"""Bronze -> silver incremental batch for GH Archive (PySpark + Delta, local[1]).

GH Archive is real public data; the events here are tiny hand-written look-alikes. The landing
zone is built with the real ``repopulse.ingest.ingest_hour`` and a fake fetcher, so the layout is
exactly what the ingest job writes. No network.
"""

from __future__ import annotations

import gzip
import hashlib
import json
import re
from datetime import UTC, datetime
from pathlib import Path

import pytest

from repopulse import ingest
from repopulse.spark import job, rules
from repopulse.spark.pii import hash_pii

pytestmark = pytest.mark.spark

DAY = "2026-01-05"
PREV_DAY = "2026-01-04"
SALT = "test-salt-not-a-secret"
NOW = datetime(2026, 1, 5, 12, 0, tzinfo=UTC)
EMAIL_RE = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")


# helpers


def gh_event(
    eid,
    etype="IssuesEvent",
    *,
    created_at=f"{DAY}T10:15:00Z",
    login="alice",
    actor_id=1001,
    repo_id=2001,
    repo_name="apache/spark",
    payload=None,
):
    if payload is None:
        payload = {"action": "opened", "issue": {"number": 7, "title": "t"}}
    return {
        "id": str(eid),
        "type": etype,
        "actor": {"id": actor_id, "login": login, "url": f"https://api.github.com/users/{login}"},
        "repo": {"id": repo_id, "name": repo_name},
        "payload": payload,
        "public": True,
        "created_at": created_at,
    }


def encode(obj) -> bytes:
    return json.dumps(obj, separators=(",", ":")).encode("utf-8")


def sha(line: bytes) -> str:
    return hashlib.sha256(line).hexdigest()


def land(tmp_path: Path, hour: int, lines: list[bytes], day: str = DAY) -> None:
    def fetch(name: str, dest_dir: Path) -> Path:
        dest_dir.mkdir(parents=True, exist_ok=True)
        path = dest_dir / name
        with gzip.open(path, "wb") as fh:
            for line in lines:
                fh.write(line + b"\n")
        return path

    result = ingest.ingest_hour(
        day,
        hour,
        landing_root=tmp_path / "landing",
        work_dir=tmp_path / "work",
        fetcher=fetch,
        force=True,
        now=NOW,
    )
    assert result.status == "ingested"


def run(spark, tmp_path: Path, hours, **kwargs) -> dict:
    kwargs.setdefault("late_window_hours", 24)
    summary = job.run_batch(
        spark,
        source="gharchive",
        hours=hours,
        landing_dir=tmp_path / "landing",
        lake_dir=tmp_path / "lake",
        salt=SALT,
        **kwargs,
    )
    json.dumps(summary)  # must be JSON-serialisable for --summary
    return summary


def table(spark, tmp_path: Path, name: str):
    return spark.read.format("delta").load(str(tmp_path / "lake" / name))


def silver(spark, tmp_path):
    return table(spark, tmp_path, "silver/gharchive_events")


def bronze(spark, tmp_path):
    return table(spark, tmp_path, "bronze/gharchive_events")


def quarantine(spark, tmp_path):
    return table(spark, tmp_path, "quarantine/gharchive_events")


def by_id(df) -> dict:
    return {r["event_id"]: r for r in df.collect()}


def as_json_lines(df) -> list[str]:
    return [r[0] for r in df.selectExpr("to_json(struct(*)) AS j").collect()]


def assert_conserved(summary: dict) -> None:
    assert summary["bronze_rows"] == (
        summary["silver_inserted"] + summary["duplicates_dropped"] + summary["quarantined_total"]
    ), summary
    assert summary["quarantined_total"] == sum(summary["quarantined"].values()), summary


def partition_columns(spark, path: Path) -> list[str]:
    return list(spark.sql(f"DESCRIBE DETAIL delta.`{path}`").first()["partitionColumns"])


# 1. Dedup within one hour


def test_duplicate_id_within_one_hour_keeps_first_occurrence(spark, tmp_path):
    land(
        tmp_path,
        10,
        [
            encode(gh_event(100, login="alice")),
            encode(gh_event(101)),
            encode(gh_event(100, login="mallory")),
            encode(gh_event(102)),
        ],
    )

    summary = run(spark, tmp_path, [(DAY, 10)])

    rows = by_id(silver(spark, tmp_path))
    assert sorted(rows) == ["100", "101", "102"]
    assert rows["100"]["actor_login"] == "alice"
    assert summary["bronze_rows"] == 4
    assert summary["silver_inserted"] == 3
    assert summary["duplicates_dropped"] == 1
    assert summary["quarantined_total"] == 0
    assert quarantine_count_or_zero(spark, tmp_path) == 0
    assert_conserved(summary)


def quarantine_count_or_zero(spark, tmp_path) -> int:
    path = tmp_path / "lake" / "quarantine" / "gharchive_events"
    if not path.exists():
        return 0
    return quarantine(spark, tmp_path).count()


# 2. Dedup across hours


def test_duplicate_across_two_runs_is_not_inserted_again_and_silver_wins(spark, tmp_path):
    land(tmp_path, 10, [encode(gh_event(200)), encode(gh_event(201, login="alice"))])
    land(tmp_path, 11, [encode(gh_event(201, login="mallory")), encode(gh_event(202))])

    first = run(spark, tmp_path, [(DAY, 10)])
    second = run(spark, tmp_path, [(DAY, 11)])

    rows = by_id(silver(spark, tmp_path))
    assert sorted(rows) == ["200", "201", "202"]
    assert rows["201"]["actor_login"] == "alice"
    assert rows["201"]["landing_hour"] == 10
    assert first["silver_inserted"] == 2
    assert second["bronze_rows"] == 2
    assert second["silver_inserted"] == 1
    assert second["duplicates_dropped"] == 1


def test_duplicate_across_hours_in_one_batch_keeps_earliest_hour(spark, tmp_path):
    land(tmp_path, 10, [encode(gh_event(200)), encode(gh_event(201, login="alice"))])
    land(tmp_path, 11, [encode(gh_event(201, login="mallory")), encode(gh_event(202))])

    summary = run(spark, tmp_path, [(DAY, 10), (DAY, 11)])

    rows = by_id(silver(spark, tmp_path))
    assert sorted(rows) == ["200", "201", "202"]
    assert rows["201"]["landing_hour"] == 10
    assert rows["201"]["actor_login"] == "alice"
    assert summary["bronze_rows"] == 4
    assert summary["silver_inserted"] == 3
    assert summary["duplicates_dropped"] == 1
    assert_conserved(summary)


# 3. Idempotent re-run of the same (date, hour)


def test_rerun_same_hour_leaves_every_table_unchanged(spark, tmp_path):
    land(
        tmp_path,
        10,
        [
            encode(gh_event(300)),
            encode(gh_event(301)),
            encode(gh_event(300)),
            encode({k: v for k, v in gh_event(302).items() if k != "id"}),
            encode(gh_event(303, created_at=f"{DAY[:8]}01T00:00:00Z")),  # beyond window
            b"{broken json",
        ],
    )

    first = run(spark, tmp_path, [(DAY, 10)])
    counts_1 = (
        bronze(spark, tmp_path).count(),
        silver(spark, tmp_path).count(),
        quarantine(spark, tmp_path).count(),
    )
    second = run(spark, tmp_path, [(DAY, 10)])
    counts_2 = (
        bronze(spark, tmp_path).count(),
        silver(spark, tmp_path).count(),
        quarantine(spark, tmp_path).count(),
    )

    assert counts_1 == (6, 2, 3)
    assert counts_2 == counts_1
    assert first["silver_inserted"] == 2
    assert second["silver_inserted"] == 0
    assert second["bronze_rows"] == 6
    assert sorted(by_id(silver(spark, tmp_path))) == ["300", "301"]


def test_rerun_after_reingest_replaces_bronze_and_quarantine_slice(spark, tmp_path):
    land(tmp_path, 10, [encode(gh_event(310)), b"{broken one", b"{broken two"])
    run(spark, tmp_path, [(DAY, 10)])
    land(tmp_path, 10, [encode(gh_event(310)), encode(gh_event(311)), b"{broken one"])

    run(spark, tmp_path, [(DAY, 10)])

    assert bronze(spark, tmp_path).count() == 3
    assert quarantine(spark, tmp_path).count() == 1
    assert sorted(by_id(silver(spark, tmp_path))) == ["310", "311"]


def test_other_hours_are_untouched_by_a_rerun(spark, tmp_path):
    land(tmp_path, 10, [encode(gh_event(320)), b"{broken"])
    land(tmp_path, 11, [encode(gh_event(321)), b"{broken"])
    run(spark, tmp_path, [(DAY, 10), (DAY, 11)])

    run(spark, tmp_path, [(DAY, 11)])

    hours = sorted(r["hour"] for r in bronze(spark, tmp_path).collect())
    assert hours == [10, 10, 11, 11]
    assert sorted(r["hour"] for r in quarantine(spark, tmp_path).collect()) == [10, 11]


# 4. PII: commit author emails/names only hashed in silver, never raw in silver/quarantine


COMMIT_EMAILS = ["Dev.One@Example.com", "dev.two@example.org"]
COMMIT_NAMES = ["Dev One", "Dev Two"]
LEAK_EMAIL = "leak.me@example.net"


def push_payload(emails, names):
    return {
        "push_id": 1,
        "ref": "refs/heads/main",
        "commits": [
            {"sha": f"c{i}", "author": {"email": e, "name": n}, "message": "fix", "distinct": True}
            for i, (e, n) in enumerate(zip(emails, names, strict=True))
        ],
    }


def test_push_commit_author_emails_are_hashed_and_never_raw(spark, tmp_path):
    bad = gh_event(401, "PushEvent", payload=push_payload([LEAK_EMAIL], ["Leaky Person"]))
    del bad["repo"]["name"]
    land(
        tmp_path,
        10,
        [
            encode(gh_event(400, "PushEvent", payload=push_payload(COMMIT_EMAILS, COMMIT_NAMES))),
            encode(bad),
            encode(gh_event(402)),
        ],
    )

    summary = run(spark, tmp_path, [(DAY, 10)])

    rows = by_id(silver(spark, tmp_path))
    assert list(rows["400"]["commit_author_email_hashes"]) == [
        hash_pii(e, SALT) for e in COMMIT_EMAILS
    ]
    assert list(rows["400"]["commit_author_name_hashes"]) == [
        hash_pii(n, SALT) for n in COMMIT_NAMES
    ]
    assert not rows["402"]["commit_author_email_hashes"]  # null or empty for non-push events
    assert summary["quarantined"].get(rules.REASON_SCHEMA_INVALID) == 1

    raw_values = [*COMMIT_EMAILS, *COMMIT_NAMES, LEAK_EMAIL, "Leaky Person"]
    for name, df in (
        ("silver", silver(spark, tmp_path)),
        ("quarantine", quarantine(spark, tmp_path)),
    ):
        for text in as_json_lines(df):
            for value in raw_values:
                assert value.lower() not in text.lower(), f"raw PII {value!r} found in {name}"
            assert EMAIL_RE.search(text) is None, f"email-like value in {name}: {text}"

    # Bronze is the restricted raw zone: it still holds the verbatim line.
    bronze_text = "\n".join(r["raw_json"] for r in bronze(spark, tmp_path).collect())
    assert COMMIT_EMAILS[0] in bronze_text
    assert LEAK_EMAIL in bronze_text


def test_silver_and_quarantine_hold_no_raw_payload_columns(spark, tmp_path):
    land(tmp_path, 10, [encode(gh_event(410)), b"{broken"])
    run(spark, tmp_path, [(DAY, 10)])

    for df in (silver(spark, tmp_path), quarantine(spark, tmp_path)):
        assert "payload" not in df.columns
        assert "raw_json" not in df.columns


# 5. Wrong schema -> quarantine with the right reason; nothing dropped silently


def without(event: dict, *path: str) -> dict:
    target = event
    for key in path[:-1]:
        target = target[key]
    del target[path[-1]]
    return event


def test_schema_invalid_and_malformed_lines_are_quarantined_with_reason(spark, tmp_path):
    bad_actor_id = gh_event(503)
    bad_actor_id["actor"]["id"] = "not-a-number"
    bad = {
        "id": without(gh_event(501), "id"),
        "actor.id": without(gh_event(502), "actor", "id"),
        "actor.id (type)": bad_actor_id,
        "repo.id": without(gh_event(504), "repo", "id"),
        "repo.name": without(gh_event(505), "repo", "name"),
        "created_at": gh_event(506, created_at="yesterday"),
    }
    bad_lines = {field: encode(ev) for field, ev in bad.items()}
    malformed = b'{"id":"507","type":"IssuesEvent",'
    land(tmp_path, 10, [encode(gh_event(500)), *bad_lines.values(), malformed])

    summary = run(spark, tmp_path, [(DAY, 10)])

    q = {r["raw_sha256"]: r for r in quarantine(spark, tmp_path).collect()}
    assert len(q) == 7
    for field, line in bad_lines.items():
        row = q[sha(line)]
        assert row["reason"] == rules.REASON_SCHEMA_INVALID, field
        wanted = field.split(" ")[0]
        assert any(wanted in message for message in row["errors"]), (field, row["errors"])
        assert row["event_type"] == "IssuesEvent"
        assert str(row["data_date"]) == DAY
        assert row["hour"] == 10
        assert Path(row["source_file"]).name == "events.json.gz"
    assert sorted(q[sha(line)]["line_number"] for line in bad_lines.values()) == [2, 3, 4, 5, 6, 7]

    broken = q[sha(malformed)]
    assert broken["reason"] == rules.REASON_MALFORMED_JSON
    assert Path(broken["source_file"]).name == "_quarantine.jsonl.gz"
    assert broken["line_number"] == 1

    assert sorted(by_id(silver(spark, tmp_path))) == ["500"]
    assert summary["bronze_rows"] == 8
    assert summary["silver_inserted"] == 1
    assert summary["quarantined"] == {
        rules.REASON_SCHEMA_INVALID: 6,
        rules.REASON_MALFORMED_JSON: 1,
    }
    assert summary["quarantined_total"] == 7
    assert_conserved(summary)

    bronze_hashes = {r["raw_sha256"] for r in bronze(spark, tmp_path).collect()}
    assert set(q) <= bronze_hashes, "quarantine rows must trace back to bronze by raw_sha256"


def test_conservation_on_mixed_hour(spark, tmp_path):
    land(
        tmp_path,
        10,
        [
            encode(gh_event(600)),
            encode(gh_event(600)),
            encode(gh_event(601, created_at=f"{PREV_DAY}T09:00:00Z")),  # 25h late: rejected
            encode(gh_event(602, created_at=f"{DAY}T09:00:00Z")),  # late, accepted
            encode(without(gh_event(603), "repo", "id")),
            b"not json at all",
        ],
    )

    summary = run(spark, tmp_path, [(DAY, 10)])

    assert summary["bronze_rows"] == 6 == bronze(spark, tmp_path).count()
    assert summary["silver_inserted"] == 2 == silver(spark, tmp_path).count()
    assert summary["duplicates_dropped"] == 1
    assert summary["quarantined_total"] == 3 == quarantine(spark, tmp_path).count()
    assert summary["late_accepted"] == 1
    assert_conserved(summary)


# 6. Late arrivals


def test_late_events_within_window_are_accepted_and_beyond_are_quarantined(spark, tmp_path):
    lines = {
        "on_time": encode(gh_event(700, created_at=f"{DAY}T10:15:00Z")),
        "late_same_day": encode(gh_event(701, created_at=f"{DAY}T09:30:00Z")),
        "late_prev_day": encode(gh_event(702, created_at=f"{PREV_DAY}T11:00:00Z")),
        "boundary": encode(gh_event(703, created_at=f"{PREV_DAY}T10:00:00Z")),
        "beyond": encode(gh_event(704, created_at=f"{PREV_DAY}T09:59:59Z")),
    }
    land(tmp_path, 10, list(lines.values()))

    summary = run(spark, tmp_path, [(DAY, 10)])

    rows = by_id(silver(spark, tmp_path))
    assert sorted(rows) == ["700", "701", "702", "703"]
    assert {k: r["is_late"] for k, r in rows.items()} == {
        "700": False,
        "701": True,
        "702": True,
        "703": True,
    }
    assert {k: str(r["event_date"]) for k, r in rows.items()} == {
        "700": DAY,
        "701": DAY,
        "702": PREV_DAY,
        "703": PREV_DAY,
    }
    assert {str(r["landing_date"]) for r in rows.values()} == {DAY}
    assert {r["landing_hour"] for r in rows.values()} == {10}

    (q,) = quarantine(spark, tmp_path).collect()
    assert q["reason"] == rules.REASON_LATE_BEYOND_WINDOW
    assert q["raw_sha256"] == sha(lines["beyond"])
    assert summary["late_accepted"] == 3
    assert summary["quarantined"] == {rules.REASON_LATE_BEYOND_WINDOW: 1}
    assert_conserved(summary)


def test_custom_late_window_is_honoured(spark, tmp_path):
    land(
        tmp_path,
        10,
        [
            encode(gh_event(710, created_at=f"{DAY}T09:00:00Z")),
            encode(gh_event(711, created_at=f"{DAY}T08:59:59Z")),
        ],
    )

    summary = run(spark, tmp_path, [(DAY, 10)], late_window_hours=1)

    assert sorted(by_id(silver(spark, tmp_path))) == ["710"]
    assert summary["quarantined"] == {rules.REASON_LATE_BEYOND_WINDOW: 1}


# 7. Typed silver, partitioned by event_date; bronze verbatim, partitioned by data_date


def pr_closed_merged(eid, number):
    return gh_event(
        eid,
        "PullRequestEvent",
        payload={
            "action": "closed",
            "number": number,
            "pull_request": {"number": number, "merged": True, "title": "x"},
        },
    )


def test_silver_columns_have_types_needed_by_q1_to_q6(spark, tmp_path):
    land(
        tmp_path,
        10,
        [
            encode(pr_closed_merged(800, 101)),
            encode(gh_event(801, payload={"action": "opened", "issue": {"number": 102}})),
            encode(
                gh_event(
                    802,
                    "IssueCommentEvent",
                    payload={"action": "created", "issue": {"number": 101}, "comment": {"id": 9}},
                )
            ),
            encode(gh_event(803, "ForkEvent", payload={"forkee": {"id": 3}})),
        ],
    )

    run(spark, tmp_path, [(DAY, 10)])

    df = silver(spark, tmp_path)
    types = dict(df.dtypes)
    expected = {
        "event_id": "string",
        "event_type": "string",
        "created_at": "timestamp",
        "event_date": "date",
        "actor_id": "bigint",
        "actor_login": "string",
        "repo_id": "bigint",
        "repo_name": "string",
        "action": "string",
        "pr_number": "bigint",
        "issue_number": "bigint",
        "pr_merged": "boolean",
        "commit_author_email_hashes": "array<string>",
        "commit_author_name_hashes": "array<string>",
        "landing_date": "date",
        "landing_hour": "int",
        "is_late": "boolean",
    }
    assert {k: types.get(k) for k in expected} == expected
    assert partition_columns(spark, tmp_path / "lake" / "silver" / "gharchive_events") == [
        "event_date"
    ]

    rows = by_id(df)
    assert rows["800"]["pr_number"] == 101
    assert rows["800"]["pr_merged"] is True
    assert rows["800"]["action"] == "closed"
    assert rows["800"]["event_type"] == "PullRequestEvent"
    assert rows["800"]["actor_id"] == 1001
    assert rows["800"]["repo_id"] == 2001
    assert rows["800"]["repo_name"] == "apache/spark"
    assert rows["801"]["issue_number"] == 102
    assert rows["801"]["pr_number"] is None
    assert rows["802"]["issue_number"] == 101
    assert rows["803"]["action"] is None

    created = {
        r["event_id"]: r["ts"]
        for r in df.selectExpr(
            "event_id", "date_format(created_at, \"yyyy-MM-dd'T'HH:mm:ss\") AS ts"
        ).collect()
    }
    assert created["800"] == f"{DAY}T10:15:00"


def test_bronze_keeps_every_landing_line_verbatim(spark, tmp_path):
    good = [encode(gh_event(900)), encode(gh_event(901)), encode(gh_event(900))]
    broken = [b"{broken a", b"{broken b"]
    land(tmp_path, 10, [*good, *broken])

    summary = run(spark, tmp_path, [(DAY, 10)])

    df = bronze(spark, tmp_path)
    types = dict(df.dtypes)
    expected = {
        "raw_json": "string",
        "raw_sha256": "string",
        "data_date": "date",
        "hour": "int",
        "source_file": "string",
        "line_number": "bigint",
        "ingested_at": "timestamp",
    }
    assert {k: types.get(k) for k in expected} == expected
    assert partition_columns(spark, tmp_path / "lake" / "bronze" / "gharchive_events") == [
        "data_date"
    ]

    rows = df.collect()
    assert summary["bronze_rows"] == len(rows) == 5
    assert sorted(r["raw_json"] for r in rows) == sorted(x.decode() for x in [*good, *broken])
    for r in rows:
        assert r["raw_sha256"] == sha(r["raw_json"].encode("utf-8"))
        assert str(r["data_date"]) == DAY
        assert r["hour"] == 10
        assert r["ingested_at"] is not None
    by_file: dict[str, list[int]] = {}
    for r in rows:
        by_file.setdefault(Path(r["source_file"]).name, []).append(r["line_number"])
    assert {k: sorted(v) for k, v in by_file.items()} == {
        "events.json.gz": [1, 2, 3],
        "_quarantine.jsonl.gz": [1, 2],
    }
