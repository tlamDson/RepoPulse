"""Spark vs DuckDB scale benchmark (1, 7, 30 days of real GH Archive data): pure parts.

No network and no Spark here: the window, the filter/slim step, the per-hour prepare step (with a
fake downloader), the run-output parser, the per-cell summary, the cross-check between engines
and the CLI (with ``run_in_subprocess`` replaced). The Spark side lives in
``tests/test_spark_naive.py`` (``@pytest.mark.spark``).
"""

from __future__ import annotations

import csv
import gzip
import hashlib
import json
import re
import shutil
from pathlib import Path

import pytest

from benchmarks import naive_duckdb, scale_bench, scale_prepare, spark_naive
from benchmarks.record_run import read_artifact
from repopulse.runs_log import COLUMNS, validate_row

FIXTURE_DIR = Path(__file__).parent / "fixtures" / "gharchive_small"
FIXTURE_LINES = 34
HOUR_NAME = re.compile(r"\d{4}-\d{2}-\d{2}-([0-9]|1[0-9]|2[0-3])\.json\.gz")
QUERY_TYPE = re.compile(r"'([A-Za-z]+Event)'")


def _code(sql: str) -> str:
    """SQL without ``--`` comments."""
    return "\n".join(line.split("--", 1)[0] for line in sql.splitlines())


def _types_in(query_dir: Path) -> set[str]:
    types: set[str] = set()
    for i in range(1, 7):
        types |= set(QUERY_TYPE.findall(_code((query_dir / f"q{i}.sql").read_text("utf-8"))))
    return types


def _fixture_lines() -> list[bytes]:
    lines: list[bytes] = []
    for src in sorted(FIXTURE_DIR.glob("*.json")):
        lines += [ln for ln in src.read_bytes().splitlines() if ln.strip()]
    return lines


@pytest.fixture
def gz_dir(tmp_path: Path) -> Path:
    out = tmp_path / "raw"
    out.mkdir()
    for src in sorted(FIXTURE_DIR.glob("*.json")):
        with src.open("rb") as fin, gzip.open(out / f"{src.stem}.json.gz", "wb") as fout:
            shutil.copyfileobj(fin, fout)
    return out


@pytest.fixture
def slim_dir(gz_dir: Path, tmp_path: Path) -> Path:
    out = tmp_path / "slim"
    out.mkdir()
    types = scale_prepare.load_config()["event_types"]
    for raw in sorted(gz_dir.glob("*.json.gz")):
        scale_prepare.slim_file(raw, out / raw.name, types)
    return out


# ---------------------------------------------------------------- config and window


def test_config_pins_window_levels_and_runs():
    cfg = scale_prepare.load_config()
    assert cfg["start_date"] == "2025-06-01"
    assert cfg["levels"] == [1, 7, 30]
    assert cfg["runs"] == 3


def test_event_types_are_exactly_the_types_the_queries_read():
    cfg = scale_prepare.load_config()
    assert set(cfg["event_types"]) == _types_in(naive_duckdb.QUERY_DIR)
    assert len(cfg["event_types"]) == len(set(cfg["event_types"]))
    assert "PushEvent" not in cfg["event_types"]


@pytest.mark.parametrize("days", [1, 7, 30])
def test_window_lists_24_hour_files_per_day(days):
    names = scale_prepare.hour_files("2025-06-01", days)
    assert len(names) == 24 * days
    assert len(set(names)) == len(names)
    assert all(HOUR_NAME.fullmatch(n) for n in names), names[:3]
    assert names[0] == "2025-06-01-0.json.gz"
    assert names[23] == "2025-06-01-23.json.gz"
    assert names[-1] == scale_prepare.window_days("2025-06-01", days)[-1] + "-23.json.gz"


def test_window_days_cross_month_end():
    assert scale_prepare.window_days("2025-06-01", 30)[-1] == "2025-06-30"
    assert scale_prepare.window_days("2025-06-29", 3) == ["2025-06-29", "2025-06-30", "2025-07-01"]


def test_smaller_levels_are_prefixes_of_larger_ones():
    one, seven, thirty = (scale_prepare.hour_files("2025-06-01", n) for n in (1, 7, 30))
    assert seven[: len(one)] == one
    assert thirty[: len(seven)] == seven


def test_window_rejects_non_positive_days():
    with pytest.raises(ValueError):
        scale_prepare.hour_files("2025-06-01", 0)


# ---------------------------------------------------------------- filter / slim


def test_slim_event_keeps_only_the_fields_the_queries_read():
    event = {
        "id": "1",
        "type": "PullRequestEvent",
        "actor": {"id": 7, "login": "alice", "display_login": "alice", "url": "u"},
        "repo": {"id": 9, "name": "apache/spark", "url": "u"},
        "payload": {
            "action": "closed",
            "number": 5,
            "pull_request": {"id": 1, "number": 5, "merged": True, "body": "long", "head": {}},
            "issue": {"number": 5, "title": "t"},
            "forkee": {"full_name": "x/y"},
        },
        "public": True,
        "created_at": "2025-06-01T10:00:00Z",
        "org": {"id": 1},
    }
    assert scale_prepare.slim_event(event) == {
        "id": "1",
        "type": "PullRequestEvent",
        "actor": {"id": 7, "login": "alice"},
        "repo": {"id": 9, "name": "apache/spark"},
        "payload": {
            "action": "closed",
            "number": 5,
            "pull_request": {"number": 5, "merged": True},
            "issue": {"number": 5},
        },
        "created_at": "2025-06-01T10:00:00Z",
    }


def test_slim_event_does_not_invent_missing_keys():
    event = {
        "id": "2",
        "type": "ForkEvent",
        "actor": {"id": 7, "login": "alice"},
        "repo": {"id": 9, "name": "apache/spark"},
        "payload": {"forkee": {"id": 3}},
        "created_at": "2025-06-01T10:00:00.000Z",
    }
    slim = scale_prepare.slim_event(event)
    assert slim["payload"] == {}
    assert slim["created_at"] == "2025-06-01T10:00:00.000Z"  # original RFC 3339 text


def test_slim_file_drops_other_types_and_keeps_duplicate_lines(gz_dir, tmp_path):
    types = scale_prepare.load_config()["event_types"]
    wanted = [ln for ln in _fixture_lines() if json.loads(ln)["type"] in types]
    stats = {"kept_lines": 0, "total_lines": 0}
    for raw in sorted(gz_dir.glob("*.json.gz")):
        got = scale_prepare.slim_file(raw, tmp_path / "out" / raw.name, types)
        for key in stats:
            stats[key] += got[key]
    assert stats == {"kept_lines": len(wanted), "total_lines": FIXTURE_LINES}
    out_lines = []
    for path in sorted((tmp_path / "out").glob("*.json.gz")):
        with gzip.open(path, "rt", encoding="utf-8") as fh:
            out_lines += [json.loads(ln) for ln in fh if ln.strip()]
    assert len(out_lines) == len(wanted)
    assert {e["type"] for e in out_lines} <= set(types)
    ids = [e["id"] for e in out_lines]
    assert ids.count("9000000002") == 2  # the duplicated raw line survives (baseline: no dedup)
    assert not any(e["type"] in {"PushEvent", "CreateEvent", "WatchEvent"} for e in out_lines)


def test_slim_file_handles_lines_with_unusual_key_order(tmp_path):
    raw = tmp_path / "2025-06-01-0.json.gz"
    lines = [
        {"type": "PushEvent", "id": "1", "payload": {}, "created_at": "2025-06-01T00:00:00Z"},
        {
            "actor": {"id": 1, "login": "a", "type": "PushEvent"},
            "id": "2",
            "type": "IssuesEvent",
            "payload": {"action": "opened", "issue": {"number": 3}},
            "created_at": "2025-06-01T00:00:00Z",
        },
    ]
    raw.write_bytes(gzip.compress("\n".join(json.dumps(x) for x in lines).encode() + b"\n"))
    out = tmp_path / "slim" / raw.name
    stats = scale_prepare.slim_file(raw, out, ["IssuesEvent"])
    assert stats["kept_lines"] == 1
    with gzip.open(out, "rt", encoding="utf-8") as fh:
        assert [json.loads(ln)["id"] for ln in fh] == ["2"]


def test_queries_give_identical_results_on_slim_and_full_data(gz_dir, slim_dir):
    full = naive_duckdb.run_once(gz_dir, collect=True)
    slim = naive_duckdb.run_once(slim_dir, collect=True)
    types = scale_prepare.load_config()["event_types"]
    assert slim["row_count"] == sum(1 for ln in _fixture_lines() if json.loads(ln)["type"] in types)
    assert slim["row_count"] < full["row_count"] == FIXTURE_LINES
    for i in range(1, 7):
        q = f"q{i}"
        assert slim["results"][q]["columns"] == full["results"][q]["columns"], q
        assert slim["results"][q]["rows"] == full["results"][q]["rows"], q


# ---------------------------------------------------------------- prepare (fake downloader)


def _fake_downloader(available: dict[str, bytes], calls: list[str]):
    def fake(name: str, dest_dir: Path, **_kwargs) -> Path | None:
        calls.append(name)
        if name not in available:
            return None  # what download() returns on HTTP 404
        dest_dir.mkdir(parents=True, exist_ok=True)
        path = dest_dir / name
        path.write_bytes(available[name])
        return path

    return fake


def test_prepare_hour_slims_records_checksum_and_deletes_raw(tmp_path, gz_dir):
    blob = (gz_dir / "2026-01-05-0.json.gz").read_bytes()
    name = "2025-06-01-0.json.gz"
    calls: list[str] = []
    entry = scale_prepare.prepare_hour(
        name,
        tmp_path / "raw",
        tmp_path / "slim",
        scale_prepare.load_config()["event_types"],
        downloader=_fake_downloader({name: blob}, calls),
    )
    assert calls == [name]
    assert entry["name"] == name
    assert entry["status"] == "ok"
    assert entry["raw_bytes"] == len(blob)
    assert entry["raw_sha256"] == hashlib.sha256(blob).hexdigest()
    assert 0 < entry["kept_lines"] < entry["total_lines"]
    assert entry["slim_bytes"] == (tmp_path / "slim" / name).stat().st_size
    assert not (tmp_path / "raw" / name).exists()  # raw file deleted right away


def test_prepare_hour_records_404_as_missing(tmp_path):
    entry = scale_prepare.prepare_hour(
        "2025-06-01-5.json.gz",
        tmp_path / "raw",
        tmp_path / "slim",
        ["IssuesEvent"],
        downloader=_fake_downloader({}, []),
    )
    assert entry == {"name": "2025-06-01-5.json.gz", "status": "missing"}
    assert not (tmp_path / "slim" / "2025-06-01-5.json.gz").exists()


def _write_config(tmp_path: Path, **overrides) -> Path:
    cfg = {
        "start_date": '"2026-01-05"',
        "levels": "[1, 2]",
        "runs": "3",
        "data_dir": json.dumps((tmp_path / "scale").as_posix()),
        "workers": "1",
        "run_timeout_minutes": "5",
        "event_types": json.dumps(scale_prepare.load_config()["event_types"]),
    }
    cfg.update({k: v for k, v in overrides.items()})
    lines = [f"{k} = {v}" for k, v in cfg.items()]
    lines += ["[spark]", 'master = "local[2]"', 'driver_memory = "1g"', "shuffle_partitions = 2"]
    lines += ["[duckdb]", f"temp_directory = {json.dumps((tmp_path / 'spill').as_posix())}"]
    path = tmp_path / "scale.toml"
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def test_prepare_main_writes_manifest_and_is_resumable(tmp_path, gz_dir):
    blob = (gz_dir / "2026-01-05-0.json.gz").read_bytes()
    config = _write_config(tmp_path, levels="[1]")
    calls: list[str] = []
    fake = _fake_downloader({"2026-01-05-0.json.gz": blob}, calls)
    assert scale_prepare.main(["--config", str(config)], downloader=fake) == 0
    manifest = json.loads((tmp_path / "scale" / "manifest.json").read_text("utf-8"))
    assert manifest["start_date"] == "2026-01-05"
    assert manifest["days"] == 1
    hours = manifest["hours"]
    assert [h["name"] for h in hours] == scale_prepare.hour_files("2026-01-05", 1)
    assert [h["status"] for h in hours].count("ok") == 1
    assert [h["status"] for h in hours].count("missing") == 23
    assert (tmp_path / "scale" / "2026-01-05-0.json.gz").is_file()
    assert sorted(calls) == sorted(scale_prepare.hour_files("2026-01-05", 1))

    calls.clear()
    assert scale_prepare.main(["--config", str(config)], downloader=fake) == 0
    assert "2026-01-05-0.json.gz" not in calls  # done hours are not downloaded again
    again = json.loads((tmp_path / "scale" / "manifest.json").read_text("utf-8"))
    assert again["hours"] == hours


def test_prepare_main_exits_non_zero_when_an_hour_fails_and_keeps_the_others(tmp_path, gz_dir):
    blob = (gz_dir / "2026-01-05-0.json.gz").read_bytes()
    config = _write_config(tmp_path, levels="[1]")
    good = _fake_downloader({"2026-01-05-0.json.gz": blob}, [])

    def flaky(name: str, dest_dir: Path, **kwargs) -> Path | None:
        if name == "2026-01-05-1.json.gz":
            raise OSError("connection reset")
        return good(name, dest_dir, **kwargs)

    assert scale_prepare.main(["--config", str(config)], downloader=flaky) == 1
    manifest = json.loads((tmp_path / "scale" / "manifest.json").read_text("utf-8"))
    status = {h["name"]: h["status"] for h in manifest["hours"]}
    assert status["2026-01-05-0.json.gz"] == "ok"
    assert status["2026-01-05-2.json.gz"] == "missing"
    # the failed hour is neither ok nor missing, so the next run tries it again
    assert "2026-01-05-1.json.gz" not in status


def test_prepare_main_prepares_an_hour_again_when_its_slim_file_is_gone(tmp_path, gz_dir):
    blob = (gz_dir / "2026-01-05-0.json.gz").read_bytes()
    config = _write_config(tmp_path, levels="[1]")
    calls: list[str] = []
    fake = _fake_downloader({"2026-01-05-0.json.gz": blob}, calls)
    assert scale_prepare.main(["--config", str(config)], downloader=fake) == 0
    slim = tmp_path / "scale" / "2026-01-05-0.json.gz"
    slim.unlink()  # e.g. a partial cache restore: the manifest says ok, the file is not there

    calls.clear()
    assert scale_prepare.main(["--config", str(config)], downloader=fake) == 0
    assert "2026-01-05-0.json.gz" in calls
    assert slim.is_file()


# ---------------------------------------------------------------- DuckDB side


def test_duckdb_run_once_accepts_a_file_list_and_a_spill_directory(gz_dir, tmp_path):
    files = sorted(gz_dir.glob("*.json.gz"))
    from_dir = naive_duckdb.run_once(gz_dir)
    from_list = naive_duckdb.run_once(files, temp_directory=tmp_path / "spill")
    assert from_list["row_count"] == from_dir["row_count"] == FIXTURE_LINES
    assert from_list["bytes_read"] == from_dir["bytes_read"]
    for i in range(1, 7):
        assert from_list[f"q{i}_rows"] == from_dir[f"q{i}_rows"]


def test_level_files_lists_present_files_and_missing_hours(tmp_path):
    data = tmp_path / "scale"
    data.mkdir()
    for name in ("2026-01-05-0.json.gz", "2026-01-05-1.json.gz", "2026-01-06-0.json.gz"):
        (data / name).write_bytes(gzip.compress(b""))
    (data / "2026-01-07-0.json.gz").write_bytes(gzip.compress(b""))  # outside a 2-day window
    present, missing = scale_bench.level_files(data, "2026-01-05", 2)
    assert [p.name for p in present] == [
        "2026-01-05-0.json.gz",
        "2026-01-05-1.json.gz",
        "2026-01-06-0.json.gz",
    ]
    assert len(missing) == 48 - 3
    assert "2026-01-05-2.json.gz" in missing


# ---------------------------------------------------------------- Spark side (pure parts)


def test_spark_queries_exist_and_use_no_duckdb_only_syntax():
    for i in range(1, 7):
        path = spark_naive.QUERY_DIR / f"q{i}.sql"
        code = _code(path.read_text(encoding="utf-8"))
        for banned in ("::", "->>", "ends_with(", "arg_min(", "quantile_cont(", "epoch("):
            assert banned not in code, f"{path.name} uses {banned}"


def test_spark_queries_read_the_same_event_types_as_duckdb():
    for i in range(1, 7):
        duck = QUERY_TYPE.findall(_code((naive_duckdb.QUERY_DIR / f"q{i}.sql").read_text("utf-8")))
        spark = QUERY_TYPE.findall(_code((spark_naive.QUERY_DIR / f"q{i}.sql").read_text("utf-8")))
        assert set(spark) == set(duck), f"q{i}"


def test_spark_session_config_fits_the_runner_without_delta():
    conf = spark_naive.session_config(scale_prepare.load_config()["spark"])
    assert conf["spark.sql.session.timeZone"] == "UTC"
    assert conf["spark.master"].startswith("local[")
    assert 1 <= int(conf["spark.sql.shuffle.partitions"]) <= 64
    memory = conf["spark.driver.memory"]
    assert memory.endswith("g") and 1 <= int(memory[:-1]) <= 12  # 16 GB runner
    assert not any("delta" in k.lower() or "delta" in v.lower() for k, v in conf.items())
    assert "spark.jars.packages" not in conf


# ---------------------------------------------------------------- run output parser


def _run(total: float, *, rows: int = 100, qrows=(4, 3, 2, 5, 3, 3), rss=500.0, engine="duckdb"):
    run = {
        "total_seconds": total,
        "load_seconds": total / 2,
        **{f"q{i}_seconds": total / 20 for i in range(1, 7)},
        **{f"q{i}_rows": n for i, n in enumerate(qrows, start=1)},
        "row_count": rows,
        "bytes_read": 4096,
        "peak_rss_mb": rss,
    }
    if engine == "spark":
        run["startup_seconds"] = 3.0 + total / 100
    return run


def test_parse_run_output_reads_the_last_json_line():
    run = _run(2.0)
    out = "some log line\n" + json.dumps(run) + "\n\n"
    assert scale_bench.parse_run_output(out, "duckdb") == run


@pytest.mark.parametrize(
    ("stdout", "message"),
    [
        ("", "no output"),
        ("Traceback (most recent call last):\n  boom", "not JSON"),
        ("[1, 2]", "JSON object"),
    ],
)
def test_parse_run_output_rejects_broken_output(stdout, message):
    with pytest.raises(scale_bench.ScaleBenchError, match=message):
        scale_bench.parse_run_output(stdout, "duckdb")


@pytest.mark.parametrize(
    ("key", "value"),
    [
        ("q3_seconds", None),
        ("q3_seconds", -1.0),
        ("row_count", True),
        ("total_seconds", "fast"),
        ("q6_rows", -2),
        ("peak_rss_mb", -5.0),
    ],
)
def test_parse_run_output_rejects_bad_values(key, value):
    run = _run(2.0)
    run[key] = value
    with pytest.raises(scale_bench.ScaleBenchError, match=key):
        scale_bench.parse_run_output(json.dumps(run), "duckdb")


def test_parse_run_output_rejects_missing_keys():
    run = _run(2.0)
    del run["q4_rows"]
    with pytest.raises(scale_bench.ScaleBenchError, match="q4_rows"):
        scale_bench.parse_run_output(json.dumps(run), "duckdb")


def test_parse_run_output_requires_spark_startup_time():
    run = _run(2.0)  # no startup_seconds
    with pytest.raises(scale_bench.ScaleBenchError, match="startup_seconds"):
        scale_bench.parse_run_output(json.dumps(run), "spark")


def test_parse_run_output_allows_unknown_peak_rss():
    run = _run(2.0, rss=None)
    assert scale_bench.parse_run_output(json.dumps(run), "duckdb")["peak_rss_mb"] is None


# ---------------------------------------------------------------- summary per cell


def test_summarize_cell_builds_a_valid_runs_csv_row():
    runs = [_run(t, engine="spark", rss=r) for t, r in ((3.0, 900.0), (1.0, 700.0), (2.0, 800.0))]
    row = scale_bench.summarize_cell(
        runs,
        engine="spark",
        days=7,
        start_date="2025-06-01",
        git_sha="abc1234",
        run_at="2026-11-11T08:00:00+00:00",
    )
    validate_row(row)
    assert row["variant"] == "spark-7d"
    assert row["data_date"] == "2025-06-01"
    assert row["total_seconds"] == 2.0
    assert row["q1_seconds"] == 0.1
    assert row["peak_rss_mb"] == 800.0
    assert row["row_count"] == 100
    assert row["notes"].startswith("engine=spark days=7 runs=3 median")
    assert "startup_seconds=3.02" in row["notes"]
    assert "missing_hours" not in row["notes"]


def test_summarize_cell_notes_missing_hours_for_duckdb():
    runs = [_run(1.0), _run(2.0), _run(3.0)]
    row = scale_bench.summarize_cell(
        runs,
        engine="duckdb",
        days=30,
        start_date="2025-06-01",
        git_sha="abc1234",
        run_at="2026-11-11T08:00:00+00:00",
        missing_hours=2,
    )
    assert row["variant"] == "duckdb-30d"
    assert row["notes"] == "engine=duckdb days=30 runs=3 median missing_hours=2"


def test_summarize_cell_rejects_query_rows_that_change_between_runs():
    runs = [_run(1.0), _run(2.0, qrows=(4, 3, 2, 6, 3, 3)), _run(3.0)]
    with pytest.raises(scale_bench.ScaleBenchError, match="q4_rows"):
        scale_bench.summarize_cell(
            runs,
            engine="duckdb",
            days=1,
            start_date="2025-06-01",
            git_sha="abc1234",
            run_at="2026-11-11T08:00:00+00:00",
        )


# ---------------------------------------------------------------- cross-check


def test_cross_check_accepts_matching_engines():
    scale_bench.cross_check(7, [_run(1.0)] * 3, [_run(5.0, engine="spark")] * 3)


@pytest.mark.parametrize(
    ("spark_run", "message"),
    [
        (_run(5.0, rows=99, engine="spark"), "row_count"),
        (_run(5.0, qrows=(4, 3, 9, 5, 3, 3), engine="spark"), "q3_rows"),
    ],
)
def test_cross_check_rejects_engines_that_disagree(spark_run, message):
    with pytest.raises(scale_bench.ScaleBenchError, match=message):
        scale_bench.cross_check(7, [_run(1.0)] * 3, [spark_run] * 3)


# ---------------------------------------------------------------- CLI


@pytest.fixture
def bench_config(tmp_path: Path) -> Path:
    config = _write_config(tmp_path)
    data = tmp_path / "scale"
    data.mkdir()
    for name in ("2026-01-05-0.json.gz", "2026-01-05-1.json.gz", "2026-01-06-0.json.gz"):
        (data / name).write_bytes(gzip.compress(b""))
    return config


def _bench(config: Path, out: Path, summary: Path) -> int:
    return scale_bench.main(
        ["--config", str(config), "--git-sha", "abc1234"]
        + ["--out", str(out), "--summary", str(summary)]
    )


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8", newline="") as fh:
        rows = list(csv.reader(fh))
    assert rows[0] == list(COLUMNS)
    return [dict(zip(COLUMNS, r, strict=True)) for r in rows[1:]]


def test_main_writes_one_row_per_engine_and_level(bench_config, tmp_path, monkeypatch):
    calls: list[tuple[str, int]] = []

    def fake(engine, days, **_kwargs):
        calls.append((engine, days))
        return _run(1.0 * days, rows=10 * days, engine=engine)

    monkeypatch.setattr(scale_bench, "run_in_subprocess", fake)
    out, summary = tmp_path / "out" / "scale-rows.csv", tmp_path / "out" / "scale-summary.md"
    assert _bench(bench_config, out, summary) == 0
    assert sorted(set(calls)) == [("duckdb", 1), ("duckdb", 2), ("spark", 1), ("spark", 2)]
    assert len(calls) == 4 * 3  # runs = 3 per cell
    rows = _read_csv(out)
    assert [r["variant"] for r in rows] == ["duckdb-1d", "spark-1d", "duckdb-2d", "spark-2d"]
    assert {r["data_date"] for r in rows} == {"2026-01-05"}
    assert rows[0]["notes"] == "engine=duckdb days=1 runs=3 median missing_hours=22"
    assert rows[3]["notes"].startswith("engine=spark days=2 runs=3 median startup_seconds=")
    assert rows[3]["notes"].endswith("missing_hours=45")
    assert len(read_artifact(out)) == 4  # record_run.py can merge it
    assert "spark-2d" in summary.read_text(encoding="utf-8")


def test_main_keeps_other_cells_when_one_cell_fails(bench_config, tmp_path, monkeypatch, capsys):
    def fake(engine, days, **_kwargs):
        if (engine, days) == ("spark", 2):
            raise scale_bench.ScaleBenchError("spark run timed out after 300 s")
        return _run(1.0, engine=engine)

    monkeypatch.setattr(scale_bench, "run_in_subprocess", fake)
    out, summary = tmp_path / "scale-rows.csv", tmp_path / "scale-summary.md"
    assert _bench(bench_config, out, summary) != 0
    rows = _read_csv(out)
    assert [r["variant"] for r in rows] == ["duckdb-1d", "spark-1d", "duckdb-2d"]
    assert "not cross-checked" in rows[2]["notes"]
    err = capsys.readouterr().err
    assert "spark-2d" in err and "timed out" in err
    assert "timed out" in summary.read_text(encoding="utf-8")


def test_main_drops_a_level_whose_engines_disagree(bench_config, tmp_path, monkeypatch, capsys):
    def fake(engine, days, **_kwargs):
        rows = 99 if (engine, days) == ("spark", 2) else 100
        return _run(1.0, rows=rows, engine=engine)

    monkeypatch.setattr(scale_bench, "run_in_subprocess", fake)
    out, summary = tmp_path / "scale-rows.csv", tmp_path / "scale-summary.md"
    assert _bench(bench_config, out, summary) != 0
    assert [r["variant"] for r in _read_csv(out)] == ["duckdb-1d", "spark-1d"]
    err = capsys.readouterr().err
    assert "row_count" in err and "2d" in err


def test_main_rejects_bad_git_sha_before_running(bench_config, tmp_path, monkeypatch):
    def _must_not_run(*args, **kwargs):
        raise AssertionError("benchmark ran with an invalid git sha")

    monkeypatch.setattr(scale_bench, "run_in_subprocess", _must_not_run)
    out, summary = tmp_path / "scale-rows.csv", tmp_path / "scale-summary.md"
    code = scale_bench.main(
        ["--config", str(bench_config), "--git-sha", "NOT-A-SHA"]
        + ["--out", str(out), "--summary", str(summary)]
    )
    assert code != 0
    assert not out.exists()


def test_main_fails_cells_without_any_data(tmp_path, monkeypatch, capsys):
    config = _write_config(tmp_path, levels="[1]")
    (tmp_path / "scale").mkdir()

    def _must_not_run(*args, **kwargs):
        raise AssertionError("ran without data")

    monkeypatch.setattr(scale_bench, "run_in_subprocess", _must_not_run)
    out, summary = tmp_path / "scale-rows.csv", tmp_path / "scale-summary.md"
    assert _bench(config, out, summary) != 0
    assert _read_csv(out) == []
    assert "no prepared files" in capsys.readouterr().err
