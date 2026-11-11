"""Unit tests for repopulse.ingest (hourly GH Archive ingest). No network: fake fetchers only."""

from __future__ import annotations

import gzip
import hashlib
import http.client
import json
import shutil
import urllib.error
import zlib
from collections import Counter
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path

import pytest

from repopulse import ingest

FIXTURES = Path(__file__).resolve().parent / "fixtures" / "gharchive_small"
DAY = "2026-01-05"
NOW = datetime(2026, 1, 5, 6, 30, tzinfo=UTC)

# Hand counts of tests/fixtures/gharchive_small/2026-01-05-0.json (20 lines).
HOUR0_COUNTS = {
    "ForkEvent": 1,
    "PullRequestEvent": 7,
    "IssuesEvent": 4,
    "PullRequestReviewEvent": 1,
    "IssueCommentEvent": 4,
    "PushEvent": 1,
    "WatchEvent": 1,
    "CreateEvent": 1,
}
# Hand counts of 2026-01-05-1.json (14 lines, all of a kept type).
HOUR1_COUNTS = {
    "PullRequestEvent": 5,
    "ForkEvent": 2,
    "IssueCommentEvent": 4,
    "IssuesEvent": 1,
    "PullRequestReviewCommentEvent": 1,
    "PullRequestReviewEvent": 1,
}


def fixture_lines(hour: int) -> list[bytes]:
    return (FIXTURES / f"{DAY}-{hour}.json").read_bytes().splitlines()


def write_source(src_dir: Path, name: str, lines: list[bytes]) -> Path:
    src_dir.mkdir(parents=True, exist_ok=True)
    path = src_dir / name
    with gzip.open(path, "wb") as fh:
        for line in lines:
            fh.write(line + b"\n")
    return path


class FakeFetcher:
    """Copies `<src_dir>/<name>` into dest_dir; None when the source has no such file."""

    def __init__(self, src_dir: Path):
        self.src_dir = src_dir
        self.calls: list[str] = []

    def __call__(self, name: str, dest_dir: Path) -> Path | None:
        self.calls.append(name)
        src = self.src_dir / name
        if not src.exists():
            return None
        dest_dir.mkdir(parents=True, exist_ok=True)
        dest = dest_dir / name
        shutil.copyfile(src, dest)
        return dest


@pytest.fixture
def env(tmp_path):
    src = tmp_path / "server"
    write_source(src, f"{DAY}-0.json.gz", fixture_lines(0))
    write_source(src, f"{DAY}-1.json.gz", fixture_lines(1))
    return {
        "src": src,
        "landing": tmp_path / "landing",
        "work": tmp_path / "work",
        "fetcher": FakeFetcher(src),
    }


def run(env, hour=0, **kwargs):
    kwargs.setdefault("now", NOW)
    return ingest.ingest_hour(
        DAY,
        hour,
        landing_root=env["landing"],
        work_dir=env["work"],
        fetcher=env["fetcher"],
        **kwargs,
    )


def part(env, hour=0) -> Path:
    return env["landing"] / f"date={DAY}" / f"hour={hour:02d}"


def read_events(path: Path) -> list[bytes]:
    with gzip.open(path, "rb") as fh:
        return fh.read().splitlines()


def manifest(env, hour=0) -> dict:
    return json.loads((part(env, hour) / "_manifest.json").read_text(encoding="utf-8"))


def ids(lines: list[bytes]) -> list[str]:
    return [json.loads(line)["id"] for line in lines]


# 1. Idempotent re-run of the same (date, hour)


def test_rerun_same_hour_is_skipped_and_leaves_data_unchanged(env):
    first = run(env)
    data_files_1 = sorted(p.name for p in part(env).glob("events*"))
    bytes_1 = (part(env) / "events.json.gz").read_bytes()

    second = run(env, now=NOW + timedelta(hours=1))

    assert first.status == "ingested"
    assert second.status == "skipped"
    assert env["fetcher"].calls == [f"{DAY}-0.json.gz"]
    assert data_files_1 == ["events.json.gz"]
    assert sorted(p.name for p in part(env).glob("events*")) == ["events.json.gz"]
    assert (part(env) / "events.json.gz").read_bytes() == bytes_1
    lines = read_events(part(env) / "events.json.gz")
    assert len(lines) == 19
    assert len(ids(lines)) == len(set(ids(lines)))
    assert second.events_kept == first.events_kept == 19
    assert second.events_total == 20


def test_forced_rerun_of_same_source_is_byte_identical(env):
    run(env)
    bytes_1 = (part(env) / "events.json.gz").read_bytes()

    result = run(env, force=True, now=NOW + timedelta(hours=3))

    assert result.status == "ingested"
    assert len(env["fetcher"].calls) == 2
    assert (part(env) / "events.json.gz").read_bytes() == bytes_1
    assert manifest(env)["ingested_at"] == (NOW + timedelta(hours=3)).isoformat()


def test_output_gzip_header_is_deterministic(env):
    run(env)
    raw = (part(env) / "events.json.gz").read_bytes()
    assert raw[:2] == b"\x1f\x8b"
    assert raw[3] & 0x08 == 0, "gzip header must not carry a file name"
    assert raw[4:8] == b"\x00\x00\x00\x00", "gzip mtime must be 0"


# 2. force / changed event_types replace the partition


def test_force_with_new_source_replaces_partition_instead_of_appending(env):
    run(env)
    write_source(env["src"], f"{DAY}-0.json.gz", fixture_lines(0)[:3])

    result = run(env, force=True)

    lines = read_events(part(env) / "events.json.gz")
    assert result.status == "ingested"
    assert lines == fixture_lines(0)[:3]
    assert manifest(env)["events_total"] == 3
    assert manifest(env)["events_kept"] == 3


def test_changed_event_types_reingest_without_force(env):
    run(env)

    result = run(env, event_types=["PushEvent", "WatchEvent"])

    assert result.status == "ingested"
    assert len(env["fetcher"].calls) == 2
    types = [json.loads(line)["type"] for line in read_events(part(env) / "events.json.gz")]
    assert types == ["PushEvent", "WatchEvent"]
    assert manifest(env)["event_types"] == ["PushEvent", "WatchEvent"]


def test_same_event_types_in_other_order_is_still_skipped(env):
    run(env, event_types=["WatchEvent", "PushEvent"])
    result = run(env, event_types=["PushEvent", "WatchEvent"])
    assert result.status == "skipped"
    assert len(env["fetcher"].calls) == 1


def test_old_quarantine_disappears_when_new_run_has_no_bad_lines(env):
    write_source(env["src"], f"{DAY}-0.json.gz", fixture_lines(0) + [b"{broken"])
    run(env)
    assert (part(env) / "_quarantine.jsonl.gz").exists()
    write_source(env["src"], f"{DAY}-0.json.gz", fixture_lines(0))

    run(env, force=True)

    assert not (part(env) / "_quarantine.jsonl.gz").exists()
    assert manifest(env)["events_malformed"] == 0
    assert sorted(p.name for p in part(env).iterdir()) == ["_manifest.json", "events.json.gz"]


# 3. Failure mid-way leaves nothing half-written


def truncated_source(env, hour=0):
    path = env["src"] / f"{DAY}-{hour}.json.gz"
    data = path.read_bytes()
    path.write_bytes(data[: len(data) // 2])


def test_corrupt_download_leaves_no_partial_partition_and_rerun_succeeds(env):
    truncated_source(env)

    with pytest.raises((EOFError, OSError, zlib.error)):
        run(env)

    assert not (part(env) / "_manifest.json").exists()
    assert not (part(env) / "events.json.gz").exists()
    leftovers = list(part(env).iterdir()) if part(env).exists() else []
    assert leftovers == []
    assert not any(p.is_file() for p in env["work"].rglob("*"))

    write_source(env["src"], f"{DAY}-0.json.gz", fixture_lines(0))
    result = run(env)
    assert result.status == "ingested"
    assert result.events_kept == 19


def test_failure_during_forced_rerun_keeps_previous_partition(env):
    run(env)
    before = {p.name: p.read_bytes() for p in part(env).iterdir()}
    truncated_source(env)

    with pytest.raises((EOFError, OSError, zlib.error)):
        run(env, force=True)

    after = {p.name: p.read_bytes() for p in part(env).iterdir()}
    assert after == before


def test_crash_while_swapping_files_leaves_no_manifest_so_next_run_rebuilds(env, monkeypatch):
    # The manifest is what `skipped` trusts, so it must be the last file to appear: a forced
    # re-run that dies while swapping the data file must not leave a manifest behind.
    run(env)
    write_source(env["src"], f"{DAY}-0.json.gz", fixture_lines(0)[:3])
    real_replace = ingest.os.replace

    def failing_replace(src, dst):
        if Path(dst).name == "events.json.gz":
            raise OSError("disk full")
        return real_replace(src, dst)

    monkeypatch.setattr(ingest.os, "replace", failing_replace)
    with pytest.raises(OSError, match="disk full"):
        run(env, force=True)
    monkeypatch.undo()

    assert not (part(env) / "_manifest.json").exists()
    assert [p.name for p in part(env).iterdir() if p.name.startswith(".")] == []

    result = run(env)
    assert result.status == "ingested", "an incomplete partition must be rebuilt, not skipped"
    assert read_events(part(env) / "events.json.gz") == fixture_lines(0)[:3]
    assert manifest(env)["events_total"] == 3


def test_manifest_without_data_file_is_not_skipped(env):
    run(env)
    (part(env) / "events.json.gz").unlink()

    result = run(env)

    assert result.status == "ingested"
    assert len(env["fetcher"].calls) == 2
    assert len(read_events(part(env) / "events.json.gz")) == 19


def test_fetcher_error_writes_nothing(env):
    def boom(name, dest_dir):
        raise ConnectionError("network down")

    with pytest.raises(ConnectionError):
        ingest.ingest_hour(
            DAY, 0, landing_root=env["landing"], work_dir=env["work"], fetcher=boom, now=NOW
        )

    assert not part(env).exists()


# 4. Filtering and counts


def test_filters_to_default_event_types_in_original_order(env):
    result = run(env)

    expected = [line for line in fixture_lines(0) if b'"type":"CreateEvent"' not in line]
    assert read_events(part(env) / "events.json.gz") == expected
    assert result.events_total == 20
    assert result.events_kept == 19
    assert result.events_malformed == 0


def test_default_event_types_cover_every_type_used_by_q1_to_q6():
    assert set(ingest.DEFAULT_EVENT_TYPES) == {
        "PushEvent",
        "PullRequestEvent",
        "PullRequestReviewEvent",
        "PullRequestReviewCommentEvent",
        "IssuesEvent",
        "IssueCommentEvent",
        "ForkEvent",
        "WatchEvent",
        "ReleaseEvent",
    }


def test_manifest_contents(env):
    run(env)
    m = manifest(env)
    src = env["src"] / f"{DAY}-0.json.gz"

    assert m["data_date"] == DAY
    assert m["hour"] == 0
    assert m["source_name"] == f"{DAY}-0.json.gz"
    assert m["source_sha256"] == hashlib.sha256(src.read_bytes()).hexdigest()
    assert m["source_bytes"] == src.stat().st_size
    assert m["event_types"] == sorted(ingest.DEFAULT_EVENT_TYPES)
    assert m["events_total"] == 20
    assert m["events_kept"] == 19
    assert m["events_malformed"] == 0
    assert m["counts_by_type"] == HOUR0_COUNTS
    assert sum(m["counts_by_type"].values()) == m["events_total"] - m["events_malformed"]
    assert m["ingested_at"] == NOW.isoformat()
    assert datetime.fromisoformat(m["ingested_at"]).tzinfo is not None


def test_events_kept_matches_hand_count_for_custom_types(env):
    result = run(env, event_types=["PullRequestEvent", "IssuesEvent"])
    assert result.events_kept == 7 + 4
    assert manifest(env)["counts_by_type"] == HOUR0_COUNTS


# 5. Malformed lines go to quarantine


def test_malformed_lines_are_quarantined_and_counted(env):
    bad = [b"{not json", b'{"id":"x1"}', b'{"id":"x2","type":5}', b"[1, 2]", b"\xff\xfe"]
    lines = fixture_lines(0)
    mixed = lines[:5] + bad[:2] + [b""] + lines[5:] + bad[2:]
    write_source(env["src"], f"{DAY}-0.json.gz", mixed)

    result = run(env)

    assert result.events_malformed == 5
    assert result.events_total == 25  # blank lines are not events
    assert result.events_kept == 19
    assert read_events(part(env) / "_quarantine.jsonl.gz") == bad
    m = manifest(env)
    assert m["counts_by_type"] == HOUR0_COUNTS
    assert sum(m["counts_by_type"].values()) == m["events_total"] - m["events_malformed"]


def test_deeply_nested_line_is_quarantined_not_fatal(env):
    # json.loads raises RecursionError (not ValueError) on very deep nesting; one such line
    # must not fail the whole hour.
    deep = b"[" * 100_000 + b"]" * 100_000
    write_source(env["src"], f"{DAY}-0.json.gz", fixture_lines(0) + [deep])

    result = run(env)

    assert result.status == "ingested"
    assert result.events_malformed == 1
    assert result.events_kept == 19
    assert read_events(part(env) / "_quarantine.jsonl.gz") == [deep]


def test_no_quarantine_file_without_bad_lines(env):
    run(env)
    assert not (part(env) / "_quarantine.jsonl.gz").exists()


# 6. Missing source


def test_missing_source_writes_nothing(env):
    result = run(env, hour=5)
    assert result.status == "missing"
    assert result.events_kept == 0
    assert not env["landing"].exists() or list(env["landing"].rglob("*")) == []


# 7. Partitions


def test_two_hours_land_in_independent_partitions(env):
    r0 = run(env, hour=0)
    r1 = run(env, hour=1)

    assert part(env, 0).name == "hour=00"
    assert part(env, 1).name == "hour=01"
    assert part(env, 0).parent.name == "date=2026-01-05"
    assert Path(r0.partition) == part(env, 0)
    assert Path(r1.partition) == part(env, 1)
    assert manifest(env, 1)["counts_by_type"] == HOUR1_COUNTS
    assert read_events(part(env, 1) / "events.json.gz") == fixture_lines(1)
    assert len(read_events(part(env, 0) / "events.json.gz")) == 19
    assert env["fetcher"].calls == [f"{DAY}-0.json.gz", f"{DAY}-1.json.gz"]


def test_source_name_does_not_pad_hour_and_partition_does():
    assert ingest.source_name("2026-01-05", 7) == "2026-01-05-7.json.gz"
    assert ingest.source_name("2026-01-05", 17) == "2026-01-05-17.json.gz"
    assert ingest.partition_dir(Path("root"), "2026-01-05", 7) == Path(
        "root/date=2026-01-05/hour=07"
    )


# 8. Raw download is removed


def test_raw_file_is_deleted_after_ingest(env):
    run(env)
    assert env["work"].exists() is False or not any(p.is_file() for p in env["work"].rglob("*"))


# 9. Input validation


@pytest.mark.parametrize(
    ("day", "hour"),
    [("2026-01-05", 24), ("2026-01-05", -1), ("2026-1-5", 0), ("2026-02-30", 0), ("05/01/2026", 0)],
)
def test_invalid_day_or_hour_raises_before_fetch(env, day, hour):
    with pytest.raises(ValueError):
        ingest.ingest_hour(
            day, hour, landing_root=env["landing"], work_dir=env["work"], fetcher=env["fetcher"]
        )
    assert env["fetcher"].calls == []


# 10. target_hours


def test_target_hours_default_lag():
    now = datetime(2026, 11, 11, 10, 42, tzinfo=UTC)
    assert ingest.target_hours(now) == [("2026-11-11", 8)]


def test_target_hours_crosses_midnight():
    now = datetime(2026, 11, 11, 1, 5, tzinfo=UTC)
    assert ingest.target_hours(now, lag_hours=2) == [("2026-11-10", 23)]


def test_target_hours_count_is_ascending_and_ends_at_lagged_hour():
    now = datetime(2026, 1, 1, 0, 30, tzinfo=UTC)
    assert ingest.target_hours(now, lag_hours=0, count=3) == [
        ("2025-12-31", 22),
        ("2025-12-31", 23),
        ("2026-01-01", 0),
    ]


def test_target_hours_converts_to_utc():
    now = datetime(2026, 11, 11, 8, 30, tzinfo=timezone(timedelta(hours=7)))
    assert ingest.target_hours(now) == [("2026-11-10", 23)]


def test_target_hours_rejects_naive_now():
    with pytest.raises(ValueError):
        ingest.target_hours(datetime(2026, 11, 11, 10, 0))


# 11. CLI


def cli_args(env, *extra):
    return [
        "--landing-dir",
        str(env["landing"]),
        "--work-dir",
        str(env["work"]),
        *extra,
    ]


def stdout_records(capsys) -> list[dict]:
    out = capsys.readouterr().out
    return [json.loads(line) for line in out.splitlines() if line.strip()]


def test_cli_hours_end_at_given_hour_and_write_summary(env, monkeypatch, capsys, tmp_path):
    monkeypatch.setattr(ingest, "download", env["fetcher"])
    summary = tmp_path / "out" / "summary.json"

    code = ingest.main(
        cli_args(env, "--date", DAY, "--hour", "1", "--hours", "2", "--summary", str(summary))
    )

    records = stdout_records(capsys)
    assert code == 0
    assert [(r["data_date"], r["hour"], r["status"]) for r in records] == [
        (DAY, 0, "ingested"),
        (DAY, 1, "ingested"),
    ]
    assert records[0]["events_kept"] == 19
    assert json.loads(summary.read_text(encoding="utf-8")) == records


def test_cli_defaults_to_target_hours(env, monkeypatch, capsys):
    monkeypatch.setattr(ingest, "download", env["fetcher"])
    code = ingest.main(cli_args(env), now=datetime(2026, 1, 5, 3, 10, tzinfo=UTC))
    records = stdout_records(capsys)
    assert code == 0
    assert [(r["data_date"], r["hour"]) for r in records] == [(DAY, 1)]


def test_cli_rerun_skips_unless_force(env, monkeypatch, capsys):
    monkeypatch.setattr(ingest, "download", env["fetcher"])
    args = cli_args(env, "--date", DAY, "--hour", "0")
    assert ingest.main(args) == 0
    assert ingest.main(args) == 0
    assert ingest.main([*args, "--force"]) == 0
    statuses = [r["status"] for r in stdout_records(capsys)]
    assert statuses == ["ingested", "skipped", "ingested"]


def test_cli_event_types_override(env, monkeypatch, capsys):
    monkeypatch.setattr(ingest, "download", env["fetcher"])
    code = ingest.main(cli_args(env, "--date", DAY, "--hour", "0", "--event-types", "PushEvent"))
    assert code == 0
    assert stdout_records(capsys)[0]["events_kept"] == 1
    assert manifest(env)["event_types"] == ["PushEvent"]


def test_cli_missing_hour_warns_and_exits_zero(env, monkeypatch, capsys):
    monkeypatch.setattr(ingest, "download", env["fetcher"])
    code = ingest.main(cli_args(env, "--date", DAY, "--hour", "5"))
    captured = capsys.readouterr()
    assert code == 0
    assert json.loads(captured.out)["status"] == "missing"
    assert "missing" in captured.err.lower()


def test_cli_real_error_exits_nonzero(env, monkeypatch, capsys):
    def boom(name, dest_dir):
        raise ConnectionError("network down")

    monkeypatch.setattr(ingest, "download", boom)
    code = ingest.main(cli_args(env, "--date", DAY, "--hour", "0"))
    assert code != 0
    assert "network down" in capsys.readouterr().err


@pytest.mark.parametrize(
    "extra",
    [
        ["--date", DAY, "--hour", "0", "--hours", "7"],
        ["--date", DAY, "--hour", "0", "--hours", "0"],
        ["--date", DAY],
        ["--hour", "3"],
    ],
)
def test_cli_rejects_bad_arguments(env, monkeypatch, extra):
    monkeypatch.setattr(ingest, "download", env["fetcher"])
    with pytest.raises(SystemExit) as exc:
        ingest.main(cli_args(env, *extra))
    assert exc.value.code == 2
    assert env["fetcher"].calls == []


# 12. download (urlopen is faked: no network)


class FakeResponse:
    """Context manager with read(n); raises `fail_after` once that many bytes were served."""

    def __init__(self, body: bytes, fail_after: Exception | None = None):
        self._body = body
        self._pos = 0
        self._fail = fail_after

    def read(self, n: int = -1) -> bytes:
        if self._pos >= len(self._body):
            if self._fail is not None:
                raise self._fail
            return b""
        end = len(self._body) if n < 0 else self._pos + n
        chunk = self._body[self._pos : end]
        self._pos = end
        return chunk

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


def http_error(code: int) -> urllib.error.HTTPError:
    return urllib.error.HTTPError("https://example.invalid/x", code, "error", None, None)


@pytest.fixture
def net(monkeypatch):
    """Scripted urlopen: each call pops the next outcome (exception to raise or response)."""
    state = {"outcomes": [], "requests": [], "sleeps": []}

    def fake_urlopen(req, timeout=None):
        state["requests"].append(req)
        outcome = state["outcomes"].pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    monkeypatch.setattr(ingest.urllib.request, "urlopen", fake_urlopen)
    monkeypatch.setattr(ingest.time, "sleep", state["sleeps"].append)
    return state


NAME = "2026-01-05-3.json.gz"


def test_download_writes_file_and_uses_generic_user_agent(net, tmp_path):
    net["outcomes"] = [FakeResponse(b"payload-bytes")]

    path = ingest.download(NAME, tmp_path / "raw")

    assert path == tmp_path / "raw" / NAME
    assert path.read_bytes() == b"payload-bytes"
    assert [p.name for p in (tmp_path / "raw").iterdir()] == [NAME]
    req = net["requests"][0]
    assert req.full_url == f"https://data.gharchive.org/{NAME}"
    assert req.get_header("User-agent") == "gharchive-ingest/1.0"
    assert net["sleeps"] == []


def test_download_returns_none_on_404_without_retry(net, tmp_path):
    net["outcomes"] = [http_error(404)]

    assert ingest.download(NAME, tmp_path / "raw") is None

    assert len(net["requests"]) == 1
    assert list((tmp_path / "raw").iterdir()) == []


def test_download_retries_server_errors_then_succeeds(net, tmp_path):
    net["outcomes"] = [http_error(503), http_error(502), FakeResponse(b"ok")]

    path = ingest.download(NAME, tmp_path / "raw")

    assert path.read_bytes() == b"ok"
    assert len(net["requests"]) == 3
    assert len(net["sleeps"]) == 2
    assert [p.name for p in (tmp_path / "raw").iterdir()] == [NAME]


def test_download_raises_after_last_retry_and_leaves_no_part_file(net, tmp_path):
    net["outcomes"] = [urllib.error.URLError("dns") for _ in range(3)]

    with pytest.raises(urllib.error.URLError):
        ingest.download(NAME, tmp_path / "raw", retries=3)

    assert len(net["requests"]) == 3
    assert list((tmp_path / "raw").iterdir()) == []


def test_download_raises_http_error_after_last_retry(net, tmp_path):
    net["outcomes"] = [http_error(500), http_error(500)]

    with pytest.raises(urllib.error.HTTPError):
        ingest.download(NAME, tmp_path / "raw", retries=2)

    assert len(net["requests"]) == 2
    assert list((tmp_path / "raw").iterdir()) == []


def test_download_retries_when_connection_drops_mid_body(net, tmp_path):
    dropped = FakeResponse(b"half", fail_after=http.client.IncompleteRead(b"half", 100))
    net["outcomes"] = [dropped, FakeResponse(b"whole-file")]

    path = ingest.download(NAME, tmp_path / "raw")

    assert path.read_bytes() == b"whole-file"
    assert len(net["requests"]) == 2
    assert [p.name for p in (tmp_path / "raw").iterdir()] == [NAME]


def test_download_drop_mid_body_on_last_retry_leaves_no_part_file(net, tmp_path):
    net["outcomes"] = [
        FakeResponse(b"half", fail_after=http.client.IncompleteRead(b"half", 100)) for _ in range(2)
    ]

    with pytest.raises(http.client.IncompleteRead):
        ingest.download(NAME, tmp_path / "raw", retries=2)

    assert list((tmp_path / "raw").iterdir()) == []


def test_cli_404_from_real_download_is_missing_and_exits_zero(env, net, capsys):
    # End to end through the real download(): a file GH Archive has not published yet must
    # not turn the scheduled run red.
    net["outcomes"] = [http_error(404)]

    code = ingest.main(cli_args(env, "--date", DAY, "--hour", "9"))

    assert code == 0
    assert json.loads(capsys.readouterr().out)["status"] == "missing"


def test_counter_helper_matches_fixture():
    # Guards the hand counts above against fixture drift.
    types = Counter(json.loads(line)["type"] for line in fixture_lines(0))
    assert dict(types) == HOUR0_COUNTS
