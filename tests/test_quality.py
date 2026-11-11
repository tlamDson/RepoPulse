"""Data quality checks and alerts (repopulse.quality), pure Python: no Spark, no network, no gh.

Fault checks run on plain rows shaped like the lake tables (docs/spark-jobs.md); app events are
SIMULATED (simkit). Volume / freshness run on GH Archive hourly counts (real data in production,
hand-made counts here). Every call to ``gh`` is a fake: no test creates a real GitHub Issue.
"""

from __future__ import annotations

import ast
import hashlib
import json
import re
import tomllib
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from benchmarks.record_run import read_artifact
from repopulse import quality as q
from repopulse.pipeline import write_row
from repopulse.runs_log import COLUMNS

ROOT = Path(__file__).resolve().parents[1]
TEMPLATE = ROOT / ".github" / "ISSUE_TEMPLATE" / "data-incident.md"
CONFIG = ROOT / "repopulse" / "quality.toml"


@pytest.fixture
def cfg() -> dict:
    return q.load_config()


def write_config(tmp_path: Path, **changes) -> Path:
    """Copy of the default quality.toml with ``section__key=value`` overrides."""
    data = tomllib.loads(CONFIG.read_text(encoding="utf-8"))
    for name, value in changes.items():
        section, key = name.split("__")
        data[section][key] = value
    path = tmp_path / "quality.toml"
    path.write_text(_to_toml(data), encoding="utf-8")
    return path


def _toml_value(value) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int | float):
        return repr(value)
    if isinstance(value, str):
        return json.dumps(value)
    if isinstance(value, list):
        return "[" + ", ".join(_toml_value(v) for v in value) + "]"
    raise TypeError(value)


def _to_toml(data: dict, prefix: str = "") -> str:
    lines, tables = [], []
    for key, value in data.items():
        if isinstance(value, dict):
            tables.append((key, value))
        else:
            lines.append(f"{key} = {_toml_value(value)}")
    out = ""
    if prefix and lines:
        out += f"[{prefix}]\n"
    out += "".join(line + "\n" for line in lines)
    for key, value in tables:
        out += _to_toml(value, f"{prefix}.{key}" if prefix else key)
    return out


# ---------------------------------------------------------------- rows shaped like the lake


T0 = datetime(2026, 11, 1, 10, 0, tzinfo=UTC)


def ts(moment: datetime) -> str:
    return moment.strftime("%Y-%m-%dT%H:%M:%S.") + f"{moment.microsecond // 1000:03d}Z"


def event(event_id: str, *, user="u-00001", at=T0, latency=timedelta(seconds=1), **extra):
    return {
        "event_id": event_id,
        "event_name": "session_started",
        "event_version": 1,
        "occurred_at": ts(at),
        "received_at": ts(at + latency),
        "user_id": user,
        "session_id": "s-00001-0001",
        "simulated": True,
        "platform": "web",
        "app_version": "3.4.0",
        **extra,
    }


def line(e: dict) -> str:
    return json.dumps(e, sort_keys=True, separators=(",", ":"))


def sha(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def bronze(*events: dict) -> list[dict]:
    return [{"raw_json": line(e), "raw_sha256": sha(line(e))} for e in events]


def silver(*events: dict) -> list[dict]:
    rows = []
    for e in events:
        rows.append(
            {
                "dedup_key": f"event_id:{e['event_id']}",
                "event_id": e["event_id"],
                "user_id": e["user_id"],
                "occurred_at": datetime.fromisoformat(e["occurred_at"].replace("Z", "+00:00")),
                "received_at": datetime.fromisoformat(e["received_at"].replace("Z", "+00:00")),
                "is_late": e.get("_is_late", False),
            }
        )
    return rows


def quarantined(e: dict, reason: str, *errors: str) -> dict:
    return {"reason": reason, "errors": list(errors), "raw_sha256": sha(line(e))}


def summary(dropped: int = 0) -> dict:
    return {"duplicates_dropped": dropped}


def clean_events(n: int = 10) -> list[dict]:
    # one event every 2 minutes per user, 3 users: far below any bot rate
    return [
        event(f"e-{i}", user=f"u-{i % 3:05d}", at=T0 + timedelta(minutes=2 * i)) for i in range(n)
    ]


def checks_of(findings) -> set[str]:
    return {f.check for f in findings}


def faults_of(findings) -> set:
    return {f.fault for f in findings}


# ---------------------------------------------------------------- config


def test_default_config_has_every_threshold(cfg):
    assert cfg["faults"]["late_threshold_seconds"] > 0
    assert cfg["faults"]["bot_events_per_window"] > 0
    assert cfg["faults"]["bot_window_seconds"] > 0
    assert set(cfg["faults"]["inject"]) == {
        "duplicate",
        "null",
        "wrong_type",
        "late",
        "schema_drift",
        "bot",
    }
    vol = cfg["volume"]
    for key in ("window_hours", "min_history", "evaluate_hours", "type_min_median"):
        assert isinstance(vol[key], int) and vol[key] > 0, key
    assert 0 < vol["min_ratio"] < 1
    assert vol["table"] == cfg["freshness"]["table"] == "main_staging.stg_gharchive_events"
    assert cfg["freshness"]["max_lag_hours"] > 0
    assert cfg["alert"]["label"] == "data-incident"
    assert cfg["alert"]["template"] == ".github/ISSUE_TEMPLATE/data-incident.md"
    ref = cfg["volume"]["reference"]
    assert ref["history"] and all(h.startswith("2026-10-25T") for h in ref["history"])
    assert ref["evaluate"] == ["2026-11-03T12"]


def test_config_is_package_data():
    text = (ROOT / "pyproject.toml").read_text(encoding="utf-8")
    assert re.search(r'repopulse\s*=\s*\[[^\]]*"quality\.toml"', text)


def test_missing_key_is_a_clear_error(tmp_path):
    data = tomllib.loads(CONFIG.read_text(encoding="utf-8"))
    del data["volume"]["min_ratio"]
    path = tmp_path / "quality.toml"
    path.write_text(_to_toml(data), encoding="utf-8")
    with pytest.raises(q.ConfigError, match=r"volume\.min_ratio"):
        q.load_config(path)


def test_wrong_type_is_a_clear_error(tmp_path):
    path = write_config(tmp_path, faults__bot_events_per_window="thirty")
    with pytest.raises(q.ConfigError, match=r"faults\.bot_events_per_window"):
        q.load_config(path)


def test_missing_file_is_a_clear_error(tmp_path):
    with pytest.raises(q.ConfigError, match="cannot read"):
        q.load_config(tmp_path / "nope.toml")


def test_module_does_not_import_simkit_thresholds_or_heavy_deps_at_top_level():
    source = (ROOT / "repopulse" / "quality.py").read_text(encoding="utf-8")
    tree = ast.parse(source)
    top = []
    for node in tree.body:
        if isinstance(node, ast.Import):
            top += [a.name for a in node.names]
        elif isinstance(node, ast.ImportFrom):
            top.append(node.module or "")
    for banned in ("simkit", "pyspark", "delta", "duckdb"):
        assert not any(name.split(".")[0] == banned for name in top), banned
    for constant in ("BOT_EVENTS_PER_MINUTE", "LATE_THRESHOLD", "BOT_WINDOW"):
        assert constant not in source, constant


# ---------------------------------------------------------------- fault checks: clean rows


def test_clean_rows_raise_no_finding(cfg):
    events = clean_events()
    assert q.run_fault_checks(bronze(*events), silver(*events), [], summary(), cfg) == []


def test_clean_event_across_an_hour_boundary_is_not_late(cfg):
    # Spark marks it is_late (event hour < arrival hour) although it arrived 1.5 s later.
    e = event("e-x", at=datetime(2026, 11, 1, 10, 59, 59, 500000, tzinfo=UTC))
    e["received_at"] = ts(datetime(2026, 11, 1, 11, 0, 1, tzinfo=UTC))
    rows = silver(e)
    rows[0]["is_late"] = True
    assert q.detect_late(rows, [], cfg) == []
    assert q.run_fault_checks(bronze(e), rows, [], summary(), cfg) == []


# ---------------------------------------------------------------- duplicate


def test_duplicate_in_bronze_is_caught_once_per_key(cfg):
    events = clean_events(4)
    b = bronze(*events, events[1], events[1])
    findings = q.detect_duplicates(b, silver(*events), [], summary(2), cfg)
    assert [(f.check, f.fault, f.ref) for f in findings] == [
        ("duplicate_key_in_bronze", "duplicate", "event_id:e-1")
    ]


def test_duplicate_not_dropped_by_the_pipeline_is_flagged(cfg):
    events = clean_events(3)
    b = bronze(*events, events[0])
    s = silver(*events, events[0])  # silver kept both copies
    findings = q.detect_duplicates(b, s, [], summary(0), cfg)
    assert {"duplicate_key_in_bronze", "silver_duplicate_key", "duplicates_not_dropped"} == (
        checks_of(findings)
    )
    assert next(f for f in findings if f.check == "silver_duplicate_key").fault is None


def test_rerun_summary_dropping_more_than_bronze_copies_is_fine(cfg):
    # a re-run of the same hours drops every valid row (silver already has them)
    events = clean_events(3)
    findings = q.detect_duplicates(bronze(*events), silver(*events), [], summary(3), cfg)
    assert findings == []


# ---------------------------------------------------------------- null / wrong_type / drift


def test_null_field_in_quarantine_is_caught(cfg):
    e = event("e-n", platform=None)
    rows = [quarantined(e, "schema_invalid", "field 'platform' must be of type string, got null")]
    findings = q.detect_null(rows, cfg)
    assert [(f.check, f.fault, f.ref) for f in findings] == [
        ("quarantine_null_field", "null", f"raw_sha256:{sha(line(e))}")
    ]
    assert q.detect_wrong_type(rows, cfg) == []
    assert q.detect_schema_drift(rows, cfg) == []


def test_null_on_an_enum_field_is_still_null_not_wrong_type(cfg):
    e = event("e-n2", platform=None)
    rows = [
        quarantined(
            e,
            "schema_invalid",
            "field 'platform' must be of type string, got null",
            'field \'platform\' must be one of ["ios", "android", "web"], got null',
        )
    ]
    findings = q.run_fault_checks([], [], rows, summary(), cfg)
    assert faults_of(findings) == {"null"}


def test_wrong_type_in_quarantine_is_caught(cfg):
    e = event("e-w", app_version=341)
    rows = [
        quarantined(e, "schema_invalid", "field 'app_version' must be of type string, got integer")
    ]
    findings = q.detect_wrong_type(rows, cfg)
    assert [(f.check, f.fault) for f in findings] == [("quarantine_wrong_type", "wrong_type")]
    assert q.detect_null(rows, cfg) == []


def test_schema_drift_unexpected_field_is_caught(cfg):
    e = event("e-d", utm_source="ads")
    rows = [quarantined(e, "schema_invalid", "unexpected field 'utm_source'")]
    findings = q.detect_schema_drift(rows, cfg)
    assert [(f.check, f.fault) for f in findings] == [("quarantine_schema_drift", "schema_drift")]


def test_schema_drift_undeclared_version_is_caught(cfg):
    e = event("e-v", event_version=2)
    rows = [
        quarantined(
            e, "unknown_event", "event_version 2 is not declared for event 'session_started'"
        )
    ]
    findings = q.run_fault_checks([], [], rows, summary(), cfg)
    assert [(f.check, f.fault) for f in findings] == [("quarantine_schema_drift", "schema_drift")]


def test_other_quarantine_rows_are_reported_without_a_fault(cfg):
    e = event("e-m")
    rows = [quarantined(e, "malformed_json", "line is not valid JSON")]
    findings = q.run_fault_checks([], [], rows, summary(), cfg)
    assert [(f.check, f.fault) for f in findings] == [("quarantine_other", None)]


# ---------------------------------------------------------------- late


def test_late_event_in_silver_is_caught_by_delay_not_is_late(cfg):
    late = event("e-l", latency=timedelta(hours=1, minutes=5))
    rows = silver(late)
    rows[0]["is_late"] = False  # the check must not depend on the Spark flag
    findings = q.detect_late(rows, [], cfg)
    assert [(f.check, f.fault, f.ref) for f in findings] == [
        ("late_arrival", "late", "event_id:e-l")
    ]


def test_late_beyond_window_in_quarantine_is_caught(cfg):
    e = event("e-lb", latency=timedelta(hours=30))
    rows = [quarantined(e, "late_beyond_window", "event is more than 24 hours older")]
    findings = q.run_fault_checks([], [], rows, summary(), cfg)
    assert [(f.check, f.fault) for f in findings] == [("late_beyond_window", "late")]


def test_delay_exactly_at_threshold_is_not_late(cfg):
    threshold = timedelta(seconds=cfg["faults"]["late_threshold_seconds"])
    assert q.detect_late(silver(event("e-t", latency=threshold)), [], cfg) == []
    over = threshold + timedelta(milliseconds=1)
    assert len(q.detect_late(silver(event("e-t2", latency=over)), [], cfg)) == 1


def test_late_check_accepts_iso_strings_and_missing_received_at(cfg):
    rows = [
        {
            "dedup_key": "event_id:e-s",
            "event_id": "e-s",
            "user_id": "u-1",
            "occurred_at": "2026-11-01T10:00:00.000Z",
            "received_at": "2026-11-01T12:00:00.000Z",
        },
        {
            "dedup_key": "event_id:e-none",
            "event_id": "e-none",
            "user_id": "u-1",
            "occurred_at": "2026-11-01T10:00:00.000Z",
            "received_at": None,
        },
    ]
    assert [f.ref for f in q.detect_late(rows, [], cfg)] == ["event_id:e-s"]


# ---------------------------------------------------------------- bot


def burst(user: str, n: int, gap: timedelta, start=T0) -> list[dict]:
    return [event(f"{user}-{i}", user=user, at=start + gap * i) for i in range(n)]


def test_bot_burst_is_caught_once_per_user(cfg):
    limit = cfg["faults"]["bot_events_per_window"]
    events = burst("u-00099", limit + 1, timedelta(milliseconds=500)) + clean_events()
    findings = q.detect_bots(silver(*events), cfg)
    assert [(f.check, f.fault, f.ref) for f in findings] == [("bot_rate", "bot", "user_id:u-00099")]


def test_exactly_the_limit_in_one_window_is_not_a_bot(cfg):
    limit = cfg["faults"]["bot_events_per_window"]
    events = burst("u-00098", limit, timedelta(milliseconds=500))
    assert q.detect_bots(silver(*events), cfg) == []


def test_many_events_spread_wider_than_the_window_are_not_a_bot(cfg):
    limit = cfg["faults"]["bot_events_per_window"]
    window = cfg["faults"]["bot_window_seconds"]
    gap = timedelta(seconds=window / limit * 1.5)
    events = burst("u-00097", limit * 3, gap)
    assert q.detect_bots(silver(*events), cfg) == []


def test_bot_threshold_comes_from_the_config_file(tmp_path, cfg):
    limit = cfg["faults"]["bot_events_per_window"]
    events = burst("u-00096", limit + 1, timedelta(milliseconds=500))
    assert len(q.detect_bots(silver(*events), cfg)) == 1
    looser = q.load_config(write_config(tmp_path, faults__bot_events_per_window=limit + 5))
    assert q.detect_bots(silver(*events), looser) == []


def test_late_threshold_comes_from_the_config_file(tmp_path, cfg):
    e = event("e-cfg", latency=timedelta(minutes=90))
    assert len(q.detect_late(silver(e), [], cfg)) == 1
    looser = q.load_config(write_config(tmp_path, faults__late_threshold_seconds=7200))
    assert q.detect_late(silver(e), [], looser) == []


# ---------------------------------------------------------------- score


def manifest(**faults) -> dict:
    records = []
    for fault, entries in faults.items():
        for entry in entries:
            records.append({"fault": fault, **entry})
    counts = {t: 0 for t in q.FAULT_TYPES}
    for r in records:
        counts[r["fault"]] += 1
    return {"simulated": True, "counts": counts, "faults": records}


def test_score_matches_findings_to_the_manifest_through_raw_sha256():
    null_event = event("e-n", platform=None)
    m = manifest(
        null=[{"indices": [3], "event_ids": ["e-n"], "detail": {}}],
        duplicate=[{"indices": [5], "event_ids": ["e-1"], "detail": {}}],
        bot=[{"indices": [7, 8], "event_ids": ["b-1", "b-2"], "detail": {"user_id": "u-9"}}],
        late=[{"indices": [9], "event_ids": ["e-l"], "detail": {}}],
    )
    findings = [
        q.Finding("quarantine_null_field", "null", f"raw_sha256:{sha(line(null_event))}", ""),
        q.Finding("duplicate_key_in_bronze", "duplicate", "event_id:e-1", ""),
        q.Finding("bot_rate", "bot", "user_id:u-9", ""),
        q.Finding("bot_rate", "bot", "user_id:u-1", ""),  # not in the manifest
        q.Finding("quarantine_other", None, "raw_sha256:abc", ""),  # never a fault
    ]
    result = q.score(findings, m, {sha(line(null_event)): "e-n"})
    assert result["injected"] == {
        **{t: 0 for t in q.FAULT_TYPES},
        "null": 1,
        "duplicate": 1,
        "bot": 1,
        "late": 1,
    }
    assert result["caught"] == {
        **{t: 0 for t in q.FAULT_TYPES},
        "null": 1,
        "duplicate": 1,
        "bot": 1,
    }
    assert result["false_alarms"] == 2
    assert result["missed"] == [{"fault": "late", "event_ids": ["e-l"]}]


def test_finding_of_the_wrong_type_does_not_count_as_caught():
    m = manifest(wrong_type=[{"indices": [1], "event_ids": ["e-w"], "detail": {}}])
    findings = [q.Finding("quarantine_null_field", "null", "raw_sha256:s1", "")]
    result = q.score(findings, m, {"s1": "e-w"})
    assert result["caught"]["wrong_type"] == 0
    assert result["false_alarms"] == 1


def test_score_of_a_clean_run_with_no_finding():
    result = q.score([], manifest(), {})
    assert result["false_alarms"] == 0
    assert sum(result["injected"].values()) == 0


# ---------------------------------------------------------------- volume


def hour(n: int) -> datetime:
    return datetime(2026, 11, 1, 0, tzinfo=UTC) + timedelta(hours=n)


def series(totals: list[int], *, push_share: float = 0.6) -> dict:
    out = {}
    for i, total in enumerate(totals):
        push = int(total * push_share)
        out[hour(i)] = {"PushEvent": push, "WatchEvent": total - push}
    return out


def vol_cfg(cfg, **overrides) -> dict:
    out = json.loads(json.dumps(cfg))
    out["volume"].update(overrides)
    return out


def test_steady_volume_raises_no_alert(cfg):
    assert q.check_volume(series([1000] * 30), cfg) == []


def test_normal_fluctuation_inside_the_threshold_raises_no_alert(cfg):
    ratio = cfg["volume"]["min_ratio"]
    low = int(1000 * ratio) + 50
    assert q.check_volume(series([1000] * 27 + [low, 1500, 1000]), cfg) == []


def test_volume_drop_is_caught(cfg):
    ratio = cfg["volume"]["min_ratio"]
    low = int(1000 * ratio) - 50
    alerts = q.check_volume(series([1000] * 29 + [low]), cfg)
    assert [(a.check, a.kind, a.data_hour) for a in alerts] == [
        ("volume_drop", "volume", "2026-11-02T05")
    ]
    assert alerts[0].table == cfg["volume"]["table"]


def test_missing_event_type_is_caught_even_if_total_is_fine(cfg):
    counts = series([1000] * 30)
    counts[hour(29)] = {"WatchEvent": 1000}  # PushEvent gone, total unchanged
    alerts = q.check_volume(counts, cfg)
    assert [(a.check, a.details.get("event_type")) for a in alerts] == [
        ("volume_type_missing", "PushEvent")
    ]


def test_rare_event_type_missing_is_not_an_alert(cfg):
    small = cfg["volume"]["type_min_median"] - 1
    counts = {h: {**c, "ReleaseEvent": small} for h, c in series([1000] * 30).items()}
    counts[hour(29)].pop("ReleaseEvent")
    assert q.check_volume(counts, cfg) == []


def test_hour_without_rows_inside_the_series_counts_as_zero(cfg):
    counts = series([1000] * 30)
    del counts[hour(28)]  # landed nothing at all
    alerts = q.check_volume(counts, cfg)
    assert ("volume_drop", "2026-11-02T04") in {(a.check, a.data_hour) for a in alerts}


def test_only_the_latest_evaluate_hours_are_checked(cfg):
    n = cfg["volume"]["evaluate_hours"]
    counts = series([1000] * 30)
    counts[hour(29 - n)] = {}  # an old drop, outside the evaluated hours
    assert [a for a in q.check_volume(counts, cfg) if a.data_hour == "2026-11-02T05"] == []
    assert {a.data_hour for a in q.check_volume(counts, cfg)} <= {
        (hour(29 - i)).strftime("%Y-%m-%dT%H") for i in range(n)
    }


def test_insufficient_history_raises_no_alert_but_is_reported(cfg):
    need = cfg["volume"]["min_history"]
    counts = series([1000] * (need - 1) + [0])
    assert q.check_volume(counts, cfg) == []
    statuses = q.volume_status(counts, cfg)
    assert statuses[-1]["status"] == "insufficient_history"


def test_history_is_capped_at_window_hours(cfg):
    small = vol_cfg(cfg, window_hours=3, evaluate_hours=1)
    # a long busy past, then 3 quiet hours: the median of the last 3 hours is quiet too
    counts = series([10000] * 20 + [500, 500, 500, 400])
    assert q.check_volume(counts, small) == []
    assert q.volume_status(counts, small)[-1]["history_hours"] == 3


def test_min_ratio_comes_from_the_config_file(tmp_path, cfg):
    counts = series([1000] * 29 + [500])
    assert q.check_volume(counts, cfg) == []
    stricter = q.load_config(write_config(tmp_path, volume__min_ratio=0.6))
    assert [a.check for a in q.check_volume(counts, stricter)] == ["volume_drop"]


def test_compare_hour_against_an_explicit_reference(cfg):
    history = [{"PushEvent": 600, "WatchEvent": 400}] * 3
    alerts, status = q.compare_hour(
        history, {"WatchEvent": 380}, hour(0), cfg, table="landing (GH Archive)"
    )
    assert {a.check for a in alerts} == {"volume_type_missing"}
    assert status["median_total"] == 1000
    assert status["status"] == "alert"


# ---------------------------------------------------------------- freshness


NOW = datetime(2026, 11, 11, 12, 0, tzinfo=UTC)


def test_fresh_data_raises_no_alert(cfg):
    assert q.check_freshness(NOW - timedelta(hours=3), NOW, cfg) == []


def test_lag_exactly_at_threshold_is_not_stale(cfg):
    lag = timedelta(hours=cfg["freshness"]["max_lag_hours"])
    assert q.check_freshness(NOW - lag, NOW, cfg) == []


def test_stale_data_is_caught(cfg):
    lag = timedelta(hours=cfg["freshness"]["max_lag_hours"], minutes=1)
    (alert,) = q.check_freshness(NOW - lag, NOW, cfg)
    assert (alert.check, alert.kind) == ("freshness_stale", "freshness")
    assert alert.data_hour == (NOW - lag).strftime("%Y-%m-%dT%H")
    assert alert.table == cfg["freshness"]["table"]


def test_empty_table_is_its_own_alert(cfg):
    (alert,) = q.check_freshness(None, NOW, cfg)
    assert alert.check == "freshness_empty"


def test_freshness_needs_an_aware_now(cfg):
    with pytest.raises(ValueError, match="timezone"):
        q.check_freshness(NOW, datetime(2026, 11, 11, 12), cfg)


def test_max_lag_comes_from_the_config_file(tmp_path, cfg):
    latest = NOW - timedelta(hours=cfg["freshness"]["max_lag_hours"] + 2)
    assert len(q.check_freshness(latest, NOW, cfg)) == 1
    looser = q.load_config(
        write_config(tmp_path, freshness__max_lag_hours=cfg["freshness"]["max_lag_hours"] + 5)
    )
    assert q.check_freshness(latest, NOW, looser) == []


# ---------------------------------------------------------------- DuckDB counts


def make_staging(path: Path, rows: list[tuple]) -> None:
    import duckdb

    con = duckdb.connect(str(path))
    try:
        con.execute("CREATE SCHEMA main_staging")
        con.execute(
            "CREATE TABLE main_staging.stg_gharchive_events (event_id VARCHAR, event_type VARCHAR,"
            " created_at TIMESTAMP, landing_date DATE, landing_hour INTEGER)"
        )
        if rows:
            con.executemany(
                "INSERT INTO main_staging.stg_gharchive_events VALUES (?, ?, ?, ?, ?)", rows
            )
    finally:
        con.close()


STAGING_ROWS = [
    ("1", "PushEvent", "2026-11-01 10:05:00", "2026-11-01", 10),
    ("2", "PushEvent", "2026-11-01 10:06:00", "2026-11-01", 10),
    ("3", "WatchEvent", "2026-11-01 10:59:00", "2026-11-01", 10),
    ("4", "WatchEvent", "2026-11-01 12:30:00", "2026-11-01", 12),
]


def test_staging_hourly_counts_and_latest_event_from_duckdb(tmp_path, cfg):
    import duckdb

    db = tmp_path / "staging.duckdb"
    make_staging(db, STAGING_ROWS)
    con = duckdb.connect(str(db), read_only=True)
    try:
        counts = q.staging_hourly_counts(con, cfg)
        latest = q.latest_event_at(con, cfg)
    finally:
        con.close()
    assert counts == {
        datetime(2026, 11, 1, 10, tzinfo=UTC): {"PushEvent": 2, "WatchEvent": 1},
        datetime(2026, 11, 1, 12, tzinfo=UTC): {"WatchEvent": 1},
    }
    assert latest == datetime(2026, 11, 1, 12, 30, tzinfo=UTC)


def test_latest_event_of_an_empty_table_is_none(tmp_path, cfg):
    import duckdb

    db = tmp_path / "empty.duckdb"
    make_staging(db, [])
    con = duckdb.connect(str(db), read_only=True)
    try:
        assert q.latest_event_at(con, cfg) is None
        assert q.staging_hourly_counts(con, cfg) == {}
    finally:
        con.close()


def test_table_name_from_config_must_be_an_identifier(tmp_path):
    with pytest.raises(q.ConfigError, match="volume.table"):
        q.load_config(write_config(tmp_path, volume__table="x; DROP TABLE y"))


# ---------------------------------------------------------------- landing manifests (gap)


def land_manifest(landing: Path, at: datetime, counts: dict[str, int], kept=None) -> None:
    part = landing / f"date={at:%Y-%m-%d}" / f"hour={at:%H}"
    part.mkdir(parents=True, exist_ok=True)
    manifest = {
        "data_date": f"{at:%Y-%m-%d}",
        "hour": at.hour,
        "event_types": sorted(kept or counts),
        "counts_by_type": counts,
    }
    (part / "_manifest.json").write_text(json.dumps(manifest), encoding="utf-8")


REF_HISTORY = [datetime(2026, 10, 25, h, tzinfo=UTC) for h in (10, 11, 12)]
REF_GAP = datetime(2026, 11, 3, 12, tzinfo=UTC)
NORMAL = {"PushEvent": 60000, "IssuesEvent": 3000, "WatchEvent": 8000}


def test_landing_counts_keep_only_the_ingested_event_types(tmp_path):
    land_manifest(
        tmp_path, REF_GAP, {"PushEvent": 5, "CreateEvent": 9}, kept=["PushEvent", "WatchEvent"]
    )
    counts = q.landing_hourly_counts(tmp_path, [REF_GAP])
    assert counts == {REF_GAP: {"PushEvent": 5}}


def test_landing_hour_without_manifest_counts_as_empty(tmp_path):
    assert q.landing_hourly_counts(tmp_path, [REF_GAP]) == {REF_GAP: {}}


def test_reference_gap_hour_is_caught(tmp_path, cfg):
    for h in REF_HISTORY:
        land_manifest(tmp_path, h, NORMAL)
    land_manifest(tmp_path, REF_GAP, {"IssuesEvent": 2500, "WatchEvent": 7000}, kept=list(NORMAL))
    alerts, report = q.run_gap(cfg, tmp_path)
    assert ("volume_type_missing", "PushEvent") in {
        (a.check, a.details.get("event_type")) for a in alerts
    }
    assert {a.data_hour for a in alerts} == {"2026-11-03T12"}
    assert report["caught"] is True


def test_reference_gap_normal_hour_is_not_caught(tmp_path, cfg):
    for h in [*REF_HISTORY, REF_GAP]:
        land_manifest(tmp_path, h, NORMAL)
    alerts, report = q.run_gap(cfg, tmp_path)
    assert alerts == []
    assert report["caught"] is False


def test_run_gap_ingests_the_configured_hours_first(tmp_path, cfg):
    calls = []

    def fake_ingest(day: str, hour_: int, landing: Path) -> None:
        calls.append((day, hour_))
        at = datetime.fromisoformat(day).replace(hour=hour_, tzinfo=UTC)
        land_manifest(landing, at, NORMAL if at != REF_GAP else {"WatchEvent": 10})

    alerts, _ = q.run_gap(cfg, tmp_path, ingest=fake_ingest)
    expected = [(h.date().isoformat(), h.hour) for h in [*REF_HISTORY, REF_GAP]]
    assert calls == expected
    assert alerts


# ---------------------------------------------------------------- issue body


def template_text() -> str:
    return TEMPLATE.read_text(encoding="utf-8")


def template_headings() -> list[str]:
    body = template_text().split("---", 2)[2]
    return [ln.strip() for ln in body.splitlines() if ln.startswith("## ")]


def volume_alert(data_hour="2026-11-02T05", check="volume_drop") -> q.Alert:
    return q.Alert(
        check=check,
        kind="volume",
        data_hour=data_hour,
        table="main_staging.stg_gharchive_events",
        message="350 events vs median 1000",
        details={"total": 350, "median_total": 1000},
    )


RUN_URL = "https://github.com/o/r/actions/runs/123"


def test_issue_body_has_every_heading_of_the_template_in_order():
    assert template_headings(), "template has no ## headings"
    title, body = q.build_issue([volume_alert()], template_text(), RUN_URL)
    found = [ln.strip() for ln in body.splitlines() if ln.startswith("## ")]
    assert found == template_headings()
    assert title.startswith("[dq:volume]")
    assert "labels:" not in body and not body.startswith("---")


def test_issue_body_fills_symptom_scope_and_backfill():
    title, body = q.build_issue(
        [volume_alert(), volume_alert("2026-11-02T06", "volume_type_missing")],
        template_text(),
        RUN_URL,
    )
    sections = re.split(r"^## ", body, flags=re.M)
    symptom = next(s for s in sections if s.startswith("Triệu chứng"))
    scope = next(s for s in sections if s.startswith("Phạm vi"))
    fix = next(s for s in sections if s.startswith("Cách xử lý"))
    assert RUN_URL in symptom
    assert "volume_drop" in symptom and "volume_type_missing" in symptom
    assert "main_staging.stg_gharchive_events" in scope
    assert "2026-11-02T05" in scope and "2026-11-02T06" in scope
    assert "workflow_dispatch" in fix and "date" in fix and "hour" in fix
    assert "<!--" not in symptom.replace(q.marker(volume_alert()), "").replace(
        q.marker(volume_alert("2026-11-02T06", "volume_type_missing")), ""
    )


def test_marker_is_hidden_and_deterministic():
    a = volume_alert()
    assert q.marker(a) == "<!-- dq:volume_drop:2026-11-02T05 -->"
    assert q.marker(a) == q.marker(volume_alert())
    _, body = q.build_issue([a], template_text(), RUN_URL)
    assert q.marker(a) in body


# ---------------------------------------------------------------- publish (fake gh)


class FakeGh:
    def __init__(self, issues=None):
        self.issues = issues or []
        self.calls: list[tuple[list[str], str | None]] = []

    def __call__(self, args, stdin=None):
        self.calls.append((list(args), stdin))
        if args[:2] == ["issue", "list"]:
            return json.dumps(self.issues)
        return ""

    def commands(self) -> list[str]:
        return [" ".join(a[:2]) for a, _ in self.calls]

    def writes(self) -> list[tuple[list[str], str | None]]:
        return [(a, s) for a, s in self.calls if a[:2] != ["issue", "list"]]


def publish(alerts, gh, **kw):
    kw.setdefault("label", "data-incident")
    return q.publish(alerts, template_text=template_text(), run_url=RUN_URL, gh=gh, **kw)


def test_publish_creates_one_issue_per_kind_with_the_label():
    gh = FakeGh()
    fresh = q.Alert("freshness_stale", "freshness", "2026-11-11T01", "t", "stale", {})
    publish([volume_alert(), volume_alert("2026-11-02T06"), fresh], gh)
    creates = [(a, s) for a, s in gh.writes() if a[:2] == ["issue", "create"]]
    assert len(creates) == 2
    for args, stdin in creates:
        assert args[args.index("--label") + 1] == "data-incident"
        assert "--body-file" in args and stdin
    volume_body = next(s for a, s in creates if "[dq:volume]" in a[a.index("--title") + 1])
    assert q.marker(volume_alert()) in volume_body
    assert q.marker(volume_alert("2026-11-02T06")) in volume_body
    labels = [a for a, _ in gh.writes() if a[:2] == ["label", "create"]]
    assert labels and labels[0][2] == "data-incident" and "--force" in labels[0]
    assert gh.commands().index("label create") < gh.commands().index("issue create")


def test_publish_lists_only_open_issues_with_the_label():
    gh = FakeGh()
    publish([volume_alert()], gh)
    (list_args, _) = next(c for c in gh.calls if c[0][:2] == ["issue", "list"])
    assert list_args[list_args.index("--label") + 1] == "data-incident"
    assert list_args[list_args.index("--state") + 1] == "open"
    assert "--json" in list_args


def test_publish_comments_on_the_open_issue_for_a_new_hour():
    gh = FakeGh(
        [
            {
                "number": 7,
                "title": "[dq:volume] old",
                "body": q.marker(volume_alert()),
                "comments": [],
            }
        ]
    )
    new = volume_alert("2026-11-02T07")
    publish([volume_alert(), new], gh)
    writes = gh.writes()
    assert [a[:3] for a, _ in writes] == [["issue", "comment", "7"]]
    body = writes[0][1]
    assert q.marker(new) in body and q.marker(volume_alert()) not in body
    assert RUN_URL in body


def test_publish_skips_when_every_marker_is_already_there():
    gh = FakeGh(
        [
            {
                "number": 7,
                "title": "[dq:volume] old",
                "body": "first",
                "comments": [{"body": "x " + q.marker(volume_alert())}],
            }
        ]
    )
    actions = publish([volume_alert()], gh)
    assert gh.writes() == []
    assert any("skip" in a for a in actions)


def test_publish_ignores_open_issues_of_another_kind():
    gh = FakeGh([{"number": 3, "title": "[dq:freshness] x", "body": "", "comments": []}])
    publish([volume_alert()], gh)
    assert ["issue", "create"] in [a[:2] for a, _ in gh.writes()]


def test_publish_passes_the_repo_to_every_gh_call():
    gh = FakeGh()
    publish([volume_alert()], gh, repo="o/r")
    for args, _ in gh.calls:
        assert args[args.index("--repo") + 1] == "o/r"


def test_publish_dry_run_never_calls_gh(capsys):
    def no_gh(*_a, **_k):
        raise AssertionError("dry run must not call gh")

    actions = publish([volume_alert()], no_gh, dry_run=True)
    out = capsys.readouterr().out
    assert "[dq:volume]" in out and q.marker(volume_alert()) in out
    assert actions


def test_publish_without_alerts_does_nothing():
    gh = FakeGh()
    assert publish([], gh) == []
    assert gh.calls == []


# ---------------------------------------------------------------- runs.csv row (M4)


def fault_report(**over) -> dict:
    report = {
        "simulated": True,
        "seed": 42,
        "users": 30,
        "days": 2,
        "start": "2026-11-01",
        "events": 321,
        "injected": {
            "duplicate": 5,
            "null": 5,
            "wrong_type": 5,
            "late": 5,
            "schema_drift": 5,
            "bot": 2,
        },
        "caught": {
            "duplicate": 5,
            "null": 5,
            "wrong_type": 5,
            "late": 5,
            "schema_drift": 5,
            "bot": 2,
        },
        "false_alarms": 0,
        "clean_runs": 2,
        "clean_alarms": 0,
    }
    report.update(over)
    return report


def test_fault_row_is_a_valid_runs_csv_row_marked_simulated(tmp_path):
    row = q.fault_row(
        fault_report(),
        run_at=datetime(2026, 11, 11, 3, tzinfo=UTC),
        git_sha="abc1234",
        total_seconds=12.5,
    )
    assert tuple(row) == COLUMNS
    artifact = tmp_path / "dq-fault-row.csv"
    write_row(artifact, row)
    (parsed,) = read_artifact(artifact)  # what benchmarks/record_run.py merges into runs.csv
    assert parsed["variant"] == "dq-fault-injection"
    assert row["variant"] == "dq-fault-injection"
    assert row["data_date"] == "2026-11-01"
    assert row["row_count"] == "321"
    assert row["total_seconds"] == "12.5"
    notes = dict(item.split("=", 1) for item in row["notes"].split(" "))
    assert notes["simulated"] == "true"
    assert notes["seed"] == "42"
    assert notes["injected"] == "27" and notes["caught"] == "27"
    assert notes["false_alarms"] == "0" and notes["clean_runs"] == "2"
    assert notes["duplicate"] == "5/5" and notes["bot"] == "2/2"


def test_fault_row_counts_clean_run_alarms_as_false_alarms():
    row = q.fault_row(
        fault_report(false_alarms=1, clean_alarms=2),
        run_at=datetime(2026, 11, 11, 3, tzinfo=UTC),
        git_sha="abc1234",
        total_seconds=1.0,
    )
    assert "false_alarms=3" in row["notes"].split(" ")


def test_passed_needs_every_fault_caught_and_no_false_alarm():
    assert q.passed(fault_report())
    assert not q.passed(fault_report(caught={**fault_report()["caught"], "bot": 1}))
    assert not q.passed(fault_report(false_alarms=1))
    assert not q.passed(fault_report(clean_alarms=1))


# ---------------------------------------------------------------- CLI


def test_cli_check_writes_report_and_exits_zero_with_alerts(tmp_path, capsys):
    db = tmp_path / "staging.duckdb"
    make_staging(db, STAGING_ROWS)
    report_path = tmp_path / "out" / "dq-report.json"
    code = q.main(
        [
            "check",
            "--database",
            str(db),
            "--now",
            "2026-11-11T12:00:00+00:00",
            "--report",
            str(report_path),
        ]
    )
    assert code == 0
    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert [a["check"] for a in report["alerts"]] == ["freshness_stale"]
    assert report["freshness"]["latest_event_at"].startswith("2026-11-01T12:30:00")
    assert report["volume"]["hours"][-1]["status"] == "insufficient_history"


def test_cli_check_missing_database_exits_one(tmp_path, capsys):
    code = q.main(
        ["check", "--database", str(tmp_path / "nope.duckdb"), "--report", str(tmp_path / "r.json")]
    )
    assert code == 1
    assert "error:" in capsys.readouterr().err


def test_cli_check_rejects_now_without_time_zone(tmp_path, capsys):
    db = tmp_path / "staging.duckdb"
    make_staging(db, STAGING_ROWS)
    code = q.main(
        [
            "check",
            "--database",
            str(db),
            "--now",
            "2026-11-11T12:00:00",
            "--report",
            str(tmp_path / "r.json"),
        ]
    )
    assert code == 1


def test_cli_alert_dry_run_prints_and_never_calls_gh(tmp_path, monkeypatch, capsys):
    def no_gh(*_a, **_k):
        raise AssertionError("dry run must not call gh")

    monkeypatch.setattr(q, "run_gh", no_gh)
    report = tmp_path / "r.json"
    report.write_text(json.dumps({"alerts": [volume_alert().to_dict()]}), encoding="utf-8")
    code = q.main(["alert", "--report", str(report), "--run-url", RUN_URL, "--dry-run"])
    assert code == 0
    assert q.marker(volume_alert()) in capsys.readouterr().out


def test_cli_alert_dry_run_survives_a_non_utf8_console(tmp_path, monkeypatch):
    # Windows consoles default to cp1252, which has no Vietnamese letters of the template.
    import io
    import sys

    stream = io.TextIOWrapper(io.BytesIO(), encoding="cp1252")
    monkeypatch.setattr(sys, "stdout", stream)
    report = tmp_path / "r.json"
    report.write_text(json.dumps({"alerts": [volume_alert().to_dict()]}), encoding="utf-8")
    code = q.main(["alert", "--report", str(report), "--run-url", RUN_URL, "--dry-run"])
    stream.flush()
    assert code == 0


def test_cli_alert_uses_the_injected_gh(tmp_path, monkeypatch):
    gh = FakeGh()
    monkeypatch.setattr(q, "run_gh", gh)
    report = tmp_path / "r.json"
    report.write_text(json.dumps({"alerts": [volume_alert().to_dict()]}), encoding="utf-8")
    code = q.main(["alert", "--report", str(report), "--run-url", RUN_URL, "--repo", "o/r"])
    assert code == 0
    assert "issue create" in gh.commands()


def test_cli_alert_without_alerts_calls_nothing(tmp_path, monkeypatch):
    gh = FakeGh()
    monkeypatch.setattr(q, "run_gh", gh)
    report = tmp_path / "r.json"
    report.write_text(json.dumps({"alerts": []}), encoding="utf-8")
    assert q.main(["alert", "--report", str(report)]) == 0
    assert gh.calls == []


def test_cli_gap_exit_code_follows_caught(tmp_path):
    landing = tmp_path / "landing"
    for h in REF_HISTORY:
        land_manifest(landing, h, NORMAL)
    land_manifest(landing, REF_GAP, NORMAL)
    report = tmp_path / "gap.json"
    assert q.main(["gap", "--landing-dir", str(landing), "--report", str(report)]) == 1
    land_manifest(landing, REF_GAP, {"WatchEvent": 8000}, kept=list(NORMAL))
    assert q.main(["gap", "--landing-dir", str(landing), "--report", str(report)]) == 0
    assert json.loads(report.read_text(encoding="utf-8"))["caught"] is True


def test_cli_uses_the_config_option(tmp_path):
    landing = tmp_path / "landing"
    for h in REF_HISTORY:
        land_manifest(landing, h, NORMAL)
    land_manifest(landing, REF_GAP, {k: v // 2 for k, v in NORMAL.items()})
    args = ["gap", "--landing-dir", str(landing), "--report", str(tmp_path / "g.json")]
    assert q.main(args) == 1  # half the volume is inside the default ratio
    strict = write_config(tmp_path, volume__min_ratio=0.8)
    assert q.main(["--config", str(strict), *args]) == 0


def test_cli_faults_without_salt_exits_one_before_spark(tmp_path, monkeypatch, capsys):
    monkeypatch.delenv("PII_HASH_SALT", raising=False)

    def no_spark(*_a, **_k):
        raise AssertionError("Spark must not start without the salt")

    monkeypatch.setattr(q, "_spark_session", no_spark)
    code = q.main(
        [
            "faults",
            "--work-dir",
            str(tmp_path / "w"),
            "--git-sha",
            "abc1234",
            "--report",
            str(tmp_path / "r.json"),
            "--out",
            str(tmp_path / "row.csv"),
        ]
    )
    assert code == 1
    assert "PII_HASH_SALT" in capsys.readouterr().err


def test_cli_faults_exit_code_and_outputs(tmp_path, monkeypatch):
    monkeypatch.setenv("PII_HASH_SALT", "test-salt-not-a-secret")

    class FakeSpark:
        stopped = False

        def stop(self):
            FakeSpark.stopped = True

    monkeypatch.setattr(q, "_spark_session", lambda: FakeSpark())
    reports = {"value": fault_report()}

    def fake_run(spark, **kwargs):
        assert isinstance(spark, FakeSpark)
        assert kwargs["salt"] == "test-salt-not-a-secret"
        return reports["value"]

    monkeypatch.setattr(q, "run_fault_injection", fake_run)
    args = [
        "faults",
        "--seed",
        "42",
        "--users",
        "30",
        "--days",
        "2",
        "--clean-runs",
        "2",
        "--work-dir",
        str(tmp_path / "w"),
        "--git-sha",
        "abc1234",
        "--report",
        str(tmp_path / "r.json"),
        "--out",
        str(tmp_path / "row.csv"),
    ]
    assert q.main(args) == 0
    assert FakeSpark.stopped
    text = (tmp_path / "row.csv").read_text(encoding="utf-8")
    assert text.splitlines()[0].startswith("run_at,git_sha,variant")
    assert "dq-fault-injection" in text and "simulated=true" in text
    assert json.loads((tmp_path / "r.json").read_text(encoding="utf-8"))["simulated"] is True

    reports["value"] = fault_report(false_alarms=1)
    assert q.main(args) == 1
