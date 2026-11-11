"""Bronze -> silver incremental batch for app events (PySpark + Delta, local[1]).

Every app event here is SIMULATED (trip-planner app described by ``eventspec``), not real users.
Landing layout assumed for app events: ``<landing>/date=YYYY-MM-DD/hour=HH/events.jsonl.gz``.
No network.
"""

from __future__ import annotations

import gzip
import hashlib
import json
import re
from pathlib import Path

import pytest

import eventspec
from repopulse.spark import job, rules
from repopulse.spark.pii import hash_pii

pytestmark = pytest.mark.spark

DAY = "2026-01-05"
PREV_DAY = "2026-01-04"
SALT = "test-salt-not-a-secret"
EMAIL_RE = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
APP_FILE = "events.jsonl.gz"


# helpers


def app_event(
    name,
    version=1,
    *,
    event_id=None,
    occurred_at=f"{DAY}T10:20:00Z",
    received_at=f"{DAY}T10:21:00Z",
    user_id="u-1",
    session_id="s-1",
    **fields,
):
    event = {
        "event_name": name,
        "event_version": version,
        "occurred_at": occurred_at,
        "user_id": user_id,
        "session_id": session_id,
        "simulated": True,
        **fields,
    }
    if event_id is not None:
        event["event_id"] = event_id
    if received_at is not None:
        event["received_at"] = received_at
    return event


def session_started(**kw):
    kw.setdefault("platform", "ios")
    kw.setdefault("app_version", "1.2.0")
    return app_event("session_started", **kw)


def trip_created(trip_id, **kw):
    kw.setdefault("destination", "Da Nang")
    kw.setdefault("start_date", "2026-02-01")
    kw.setdefault("end_date", "2026-02-05")
    kw.setdefault("traveler_count", 2)
    return app_event("trip_created", trip_id=trip_id, **kw)


def user_signed_up(email, version=1, **kw):
    kw.setdefault("signup_method", "email")
    if version == 2:
        kw.setdefault("marketing_opt_in", True)
    return app_event("user_signed_up", version, email=email, **kw)


def trip_shared(trip_id, recipient_email=None, **kw):
    kw.setdefault("channel", "email")
    if recipient_email is not None:
        kw["recipient_email"] = recipient_email
    return app_event("trip_shared", trip_id=trip_id, **kw)


def encode(obj) -> bytes:
    return json.dumps(obj, separators=(",", ":")).encode("utf-8")


def sha(line: bytes) -> str:
    return hashlib.sha256(line).hexdigest()


def land(tmp_path: Path, hour: int, lines: list[bytes], day: str = DAY) -> None:
    part = tmp_path / "landing" / f"date={day}" / f"hour={hour:02d}"
    part.mkdir(parents=True, exist_ok=True)
    with gzip.open(part / APP_FILE, "wb") as fh:
        for line in lines:
            fh.write(line + b"\n")


def run(spark, tmp_path: Path, hours, **kwargs) -> dict:
    kwargs.setdefault("late_window_hours", 24)
    summary = job.run_batch(
        spark,
        source="app",
        hours=hours,
        landing_dir=tmp_path / "landing",
        lake_dir=tmp_path / "lake",
        salt=SALT,
        **kwargs,
    )
    json.dumps(summary)
    return summary


def table(spark, tmp_path: Path, name: str):
    return spark.read.format("delta").load(str(tmp_path / "lake" / name))


def silver(spark, tmp_path):
    return table(spark, tmp_path, "silver/app_events")


def bronze(spark, tmp_path):
    return table(spark, tmp_path, "bronze/app_events")


def quarantine(spark, tmp_path):
    return table(spark, tmp_path, "quarantine/app_events")


def by_key(df) -> dict:
    return {r["dedup_key"]: r for r in df.collect()}


def props(row) -> dict:
    return json.loads(row["properties"])


def as_json_lines(df) -> list[str]:
    return [r[0] for r in df.selectExpr("to_json(struct(*)) AS j").collect()]


def assert_conserved(summary: dict) -> None:
    assert summary["bronze_rows"] == (
        summary["silver_inserted"] + summary["duplicates_dropped"] + summary["quarantined_total"]
    ), summary
    assert summary["quarantined_total"] == sum(summary["quarantined"].values()), summary


def partition_columns(spark, path: Path) -> list[str]:
    return list(spark.sql(f"DESCRIBE DETAIL delta.`{path}`").first()["partitionColumns"])


# 1. Dedup within one hour, both branches of eventspec.dedup_key


def test_duplicates_within_one_hour_keep_first_by_event_id_and_by_dedup_fields(spark, tmp_path):
    first_session = session_started(event_id="e-1", platform="ios")
    first_trip = trip_created("t-1", destination="Hue")
    land(
        tmp_path,
        10,
        [
            encode(first_session),
            encode(first_trip),
            encode(session_started(event_id="e-1", platform="web")),  # same event_id
            encode(trip_created("t-1", destination="Hanoi")),  # same dedup field trip_id
            encode(trip_created("t-2")),
        ],
    )

    summary = run(spark, tmp_path, [(DAY, 10)])

    rows = by_key(silver(spark, tmp_path))
    key_session = eventspec.dedup_key(first_session)
    key_trip = eventspec.dedup_key(first_trip)
    assert key_session == "event_id:e-1"
    assert key_trip.startswith("sha256:")
    assert set(rows) == {key_session, key_trip, eventspec.dedup_key(trip_created("t-2"))}
    assert props(rows[key_session])["platform"] == "ios"
    assert props(rows[key_trip])["destination"] == "Hue"
    assert rows[key_session]["event_id"] == "e-1"
    assert rows[key_trip]["event_id"] is None
    assert summary["bronze_rows"] == 5
    assert summary["silver_inserted"] == 3
    assert summary["duplicates_dropped"] == 2
    assert_conserved(summary)


# 2. Dedup across hours


def test_duplicate_across_two_runs_is_not_inserted_again(spark, tmp_path):
    land(tmp_path, 10, [encode(session_started(event_id="e-10", platform="ios"))])
    land(
        tmp_path,
        11,
        [
            encode(
                session_started(
                    event_id="e-10",
                    platform="android",
                    occurred_at=f"{DAY}T11:05:00Z",
                    received_at=f"{DAY}T11:06:00Z",
                )
            ),
            encode(
                session_started(
                    event_id="e-11", occurred_at=f"{DAY}T11:05:00Z", received_at=f"{DAY}T11:06:00Z"
                )
            ),
        ],
    )

    run(spark, tmp_path, [(DAY, 10)])
    second = run(spark, tmp_path, [(DAY, 11)])

    rows = by_key(silver(spark, tmp_path))
    assert set(rows) == {"event_id:e-10", "event_id:e-11"}
    assert props(rows["event_id:e-10"])["platform"] == "ios"
    assert rows["event_id:e-10"]["landing_hour"] == 10
    assert second["silver_inserted"] == 1
    assert second["duplicates_dropped"] == 1


# 3. Idempotent re-run


def test_rerun_same_hour_leaves_every_table_unchanged(spark, tmp_path):
    land(
        tmp_path,
        10,
        [
            encode(session_started(event_id="e-20")),
            encode(session_started(event_id="e-20")),
            encode(trip_created("t-20")),
            encode(app_event("trip_deleted", trip_id="t-20")),
            b"{not json",
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

    assert counts_1 == (5, 2, 2)
    assert counts_2 == counts_1
    assert first["silver_inserted"] == 2
    assert second["silver_inserted"] == 0


# 4. PII hashed before silver; never raw in silver or quarantine


SIGNUP_V1 = "Hoa.Tran@Example.com"
SIGNUP_V2 = "minh@example.vn"
RECIPIENT = "Friend@Example.org"
QUARANTINE_LEAK = "quarantine.leak@example.net"
LATE_LEAK = "late.leak@example.net"


def test_pii_fields_are_hashed_in_silver_and_absent_from_quarantine(spark, tmp_path):
    signup_v1 = user_signed_up(SIGNUP_V1, user_id="u-30")
    signup_v2 = user_signed_up(SIGNUP_V2, version=2, user_id="u-31")
    shared = trip_shared("t-30", RECIPIENT)
    shared_no_recipient = trip_shared("t-31", channel="link")
    invalid = user_signed_up(QUARANTINE_LEAK, user_id="u-32")
    del invalid["signup_method"]
    too_late = user_signed_up(
        LATE_LEAK,
        user_id="u-33",
        occurred_at=f"{DAY[:8]}01T00:00:00Z",
        received_at=f"{DAY}T10:00:00Z",
    )
    land(
        tmp_path,
        10,
        [encode(e) for e in (signup_v1, signup_v2, shared, shared_no_recipient, invalid, too_late)],
    )

    summary = run(spark, tmp_path, [(DAY, 10)])

    rows = by_key(silver(spark, tmp_path))
    assert props(rows[eventspec.dedup_key(signup_v1)])["email"] == hash_pii(SIGNUP_V1, SALT)
    assert props(rows[eventspec.dedup_key(signup_v2)])["email"] == hash_pii(SIGNUP_V2, SALT)
    assert props(rows[eventspec.dedup_key(shared)])["recipient_email"] == hash_pii(RECIPIENT, SALT)
    assert props(rows[eventspec.dedup_key(shared_no_recipient)]).get("recipient_email") is None
    assert summary["quarantined"] == {
        rules.REASON_SCHEMA_INVALID: 1,
        rules.REASON_LATE_BEYOND_WINDOW: 1,
    }

    for row in rows.values():
        p = props(row)
        for field in eventspec.pii_fields(row["event_name"], row["event_version"]):
            assert p.get(field) is None or re.fullmatch(r"[0-9a-f]{64}", p[field]), (field, p)

    raw = [SIGNUP_V1, SIGNUP_V2, RECIPIENT, QUARANTINE_LEAK, LATE_LEAK]
    for name, df in (
        ("silver", silver(spark, tmp_path)),
        ("quarantine", quarantine(spark, tmp_path)),
    ):
        for text in as_json_lines(df):
            for value in raw:
                assert value.lower() not in text.lower(), f"raw PII {value!r} found in {name}"
            assert EMAIL_RE.search(text) is None, f"email-like value in {name}: {text}"

    bronze_text = "\n".join(r["raw_json"] for r in bronze(spark, tmp_path).collect())
    assert SIGNUP_V1 in bronze_text
    assert QUARANTINE_LEAK in bronze_text


def test_properties_hold_only_event_specific_fields(spark, tmp_path):
    land(tmp_path, 10, [encode(trip_created("t-40"))])
    run(spark, tmp_path, [(DAY, 10)])

    (row,) = silver(spark, tmp_path).collect()
    assert props(row) == {
        "trip_id": "t-40",
        "destination": "Da Nang",
        "start_date": "2026-02-01",
        "end_date": "2026-02-05",
        "traveler_count": 2,
    }
    assert "raw_json" not in silver(spark, tmp_path).columns


# 5. Wrong schema -> quarantine with the right reason


def test_invalid_events_are_quarantined_with_reason_and_messages(spark, tmp_path):
    missing_email = user_signed_up("x@example.com", user_id="u-50")
    del missing_email["email"]
    cases = {
        "missing": (
            encode(missing_email),
            rules.REASON_SCHEMA_INVALID,
            "missing required field 'email'",
            "user_signed_up",
        ),
        "wrong_type": (
            encode(trip_created("t-50", traveler_count="two")),
            rules.REASON_SCHEMA_INVALID,
            "field 'traveler_count' must be of type integer, got string",
            "trip_created",
        ),
        "not_simulated": (
            encode({**session_started(event_id="e-51"), "simulated": False}),
            rules.REASON_SCHEMA_INVALID,
            "field 'simulated' must be one of [true], got false",
            "session_started",
        ),
        "unknown_name": (
            encode(app_event("trip_deleted", trip_id="t-50")),
            rules.REASON_UNKNOWN_EVENT,
            "unknown event_name 'trip_deleted'",
            "trip_deleted",
        ),
        "unknown_version": (
            encode(session_started(event_id="e-52", version=9)),
            rules.REASON_UNKNOWN_EVENT,
            "event_version 9 is not declared for event 'session_started'",
            "session_started",
        ),
        "not_object": (
            b"[1,2,3]",
            rules.REASON_SCHEMA_INVALID,
            "event must be an object, got array",
            None,
        ),
        "malformed": (
            b'{"event_name":"trip_created",',
            rules.REASON_MALFORMED_JSON,
            None,
            None,
        ),
    }
    valid = session_started(event_id="e-50")
    land(tmp_path, 10, [encode(valid), *(c[0] for c in cases.values())])

    summary = run(spark, tmp_path, [(DAY, 10)])

    q = {r["raw_sha256"]: r for r in quarantine(spark, tmp_path).collect()}
    assert len(q) == len(cases)
    for case, (line, reason, message, event_name) in cases.items():
        row = q[sha(line)]
        assert row["reason"] == reason, case
        if message is not None:
            assert any(message in e for e in row["errors"]), (case, row["errors"])
        if event_name is not None:
            assert row["event_name"] == event_name, case
        assert str(row["data_date"]) == DAY
        assert row["hour"] == 10
        assert Path(row["source_file"]).name == APP_FILE
    assert sorted(r["line_number"] for r in q.values()) == list(range(2, 2 + len(cases)))

    assert set(by_key(silver(spark, tmp_path))) == {"event_id:e-50"}
    assert summary["bronze_rows"] == 1 + len(cases)
    assert summary["quarantined"] == {
        rules.REASON_SCHEMA_INVALID: 4,
        rules.REASON_UNKNOWN_EVENT: 2,
        rules.REASON_MALFORMED_JSON: 1,
    }
    assert_conserved(summary)
    assert set(q) <= {r["raw_sha256"] for r in bronze(spark, tmp_path).collect()}


# 6. Late arrivals: arrival = received_at, else start of the landing hour


def test_late_app_events(spark, tmp_path):
    events = {
        "on_time": session_started(event_id="e-60"),
        "late": session_started(
            event_id="e-61", occurred_at=f"{PREV_DAY}T12:00:00Z", received_at=f"{DAY}T10:30:00Z"
        ),
        "offset": session_started(
            event_id="e-62",
            occurred_at="2026-01-05T06:30:00+07:00",
            received_at=f"{DAY}T10:05:00Z",
        ),
        "no_received_at": session_started(
            event_id="e-63", occurred_at=f"{DAY}T08:00:00Z", received_at=None
        ),
        "beyond": session_started(
            event_id="e-64", occurred_at="2026-01-03T10:00:00Z", received_at=f"{DAY}T10:00:00Z"
        ),
    }
    land(tmp_path, 10, [encode(e) for e in events.values()])

    summary = run(spark, tmp_path, [(DAY, 10)])

    df = silver(spark, tmp_path)
    rows = {r["event_id"]: r for r in df.collect()}
    assert sorted(rows) == ["e-60", "e-61", "e-62", "e-63"]
    assert {k: r["is_late"] for k, r in rows.items()} == {
        "e-60": False,
        "e-61": True,
        "e-62": True,
        "e-63": True,
    }
    assert {k: str(r["event_date"]) for k, r in rows.items()} == {
        "e-60": DAY,
        "e-61": PREV_DAY,
        "e-62": PREV_DAY,
        "e-63": DAY,
    }
    occurred = {
        r["event_id"]: r["ts"]
        for r in df.selectExpr(
            "event_id", "date_format(occurred_at, \"yyyy-MM-dd'T'HH:mm:ss\") AS ts"
        ).collect()
    }
    assert occurred["e-62"] == "2026-01-04T23:30:00"
    assert rows["e-63"]["received_at"] is None

    (q,) = quarantine(spark, tmp_path).collect()
    assert q["reason"] == rules.REASON_LATE_BEYOND_WINDOW
    assert q["raw_sha256"] == sha(encode(events["beyond"]))
    assert q["event_name"] == "session_started"
    assert summary["late_accepted"] == 3
    assert_conserved(summary)


# 7. Typed silver, simulated flag, partitioned by event_date


def test_silver_app_columns_are_typed_and_partitioned(spark, tmp_path):
    land(tmp_path, 10, [encode(session_started(event_id="e-70")), encode(trip_created("t-70"))])

    run(spark, tmp_path, [(DAY, 10)])

    df = silver(spark, tmp_path)
    types = dict(df.dtypes)
    expected = {
        "dedup_key": "string",
        "event_id": "string",
        "event_name": "string",
        "event_version": "int",
        "occurred_at": "timestamp",
        "received_at": "timestamp",
        "user_id": "string",
        "session_id": "string",
        "simulated": "boolean",
        "event_date": "date",
        "properties": "string",
        "landing_date": "date",
        "landing_hour": "int",
        "is_late": "boolean",
    }
    assert {k: types.get(k) for k in expected} == expected
    assert partition_columns(spark, tmp_path / "lake" / "silver" / "app_events") == ["event_date"]
    assert partition_columns(spark, tmp_path / "lake" / "bronze" / "app_events") == ["data_date"]
    rows = df.collect()
    assert {r["simulated"] for r in rows} == {True}
    assert {r["user_id"] for r in rows} == {"u-1"}
    assert {r["event_version"] for r in rows} == {1}
