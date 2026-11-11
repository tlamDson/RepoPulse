"""Data quality checks and alerts of the incremental batch (docs/data-quality.md).

Three parts, every threshold read from ``repopulse/quality.toml`` (``load_config``):

- Fault checks on the lake tables of the app events (SIMULATED data, simkit): ``detect_*`` and
  ``run_fault_checks`` take plain rows read from bronze / silver / quarantine and never look at the
  simkit manifest. ``score`` then compares the findings with the manifest (the answer key). The
  harness ``run_fault_injection`` plants the six simkit faults, lands them, runs the real Spark
  batch (``repopulse.spark.job.run_batch``) and scores the checks, plus clean runs that must raise
  nothing. pyspark and simkit are imported inside the harness functions only.
- Volume (``check_volume``: an hour against the median of the hours before it) and freshness
  (``check_freshness``: age of the newest event) of the GH Archive staging table (real data).
- Alerts: ``build_issue`` fills the ``data-incident`` issue template; ``publish`` keeps at most
  one open issue per kind of check and comments only for alerts whose hidden marker is new. ``gh``
  is injected (``run_gh`` by default); tests always pass a fake.

CLI (errors -> stderr, exit 1)::

    python -m repopulse.quality faults --seed 42 --users 30 --days 2 --clean-runs 2 \
        --work-dir data/dq-faults --git-sha <sha> --report out/dq-faults.json \
        --out out/dq-fault-row.csv          # exit 1 when a fault is missed or a false alarm
    python -m repopulse.quality check --database ci.duckdb --report out/dq-report.json
    python -m repopulse.quality gap --landing-dir data/landing/gharchive --ingest \
        --report out/dq-gap.json                        # exit 1 when the gap hour is not caught
    python -m repopulse.quality alert --report out/dq-report.json --run-url <url> [--dry-run]
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import math
import re
import statistics
import subprocess
import sys
import time
import tomllib
from collections import Counter, defaultdict
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import asdict, dataclass, field
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

from repopulse import ingest
from repopulse.pipeline import write_row
from repopulse.runs_log import validate_row
from repopulse.spark import rules
from repopulse.spark.pii import MissingSaltError, salt_from_env

DEFAULT_CONFIG = Path(__file__).with_name("quality.toml")
ROOT = Path(__file__).resolve().parents[1]
FAULT_TYPES: tuple[str, ...] = ("duplicate", "null", "wrong_type", "late", "schema_drift", "bot")
VARIANT = "dq-fault-injection"
APP_EVENTS_FILE = "events.jsonl.gz"  # app landing file name read by repopulse.spark.batch
APP_TABLE = "app_events"
DEFAULT_START = date(2026, 11, 1)
TITLES = {
    "volume": "[dq:volume] Bất thường khối lượng dữ liệu GH Archive",
    "freshness": "[dq:freshness] Dữ liệu GH Archive không còn tươi",
}

_HOUR_FMT = "%Y-%m-%dT%H"
_IDENT = re.compile(r"[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*){0,2}")
_GIT_SHA = re.compile(r"[0-9a-f]{7,40}")
_TYPE_ERROR = re.compile(r"must be of type \S+, got (\S+)$")


class QualityError(ValueError):
    """Bad input or a failed step of a data quality command."""


class ConfigError(QualityError):
    """quality.toml is missing, unreadable or has a missing / mistyped key."""


# ---------------------------------------------------------------- config


def _int(minimum: int) -> Callable[[object], bool]:
    def check(v: object) -> bool:
        return isinstance(v, int) and not isinstance(v, bool) and v >= minimum

    check.__doc__ = f"an integer >= {minimum}"
    return check


def _positive_number(v: object) -> bool:
    """a number > 0"""
    return isinstance(v, int | float) and not isinstance(v, bool) and math.isfinite(v) and v > 0


def _ratio(v: object) -> bool:
    """a number in (0, 1]"""
    return _positive_number(v) and v <= 1


def _identifier(v: object) -> bool:
    """a SQL identifier such as schema.table"""
    return isinstance(v, str) and _IDENT.fullmatch(v) is not None


def _text(v: object) -> bool:
    """a non-empty string"""
    return isinstance(v, str) and v.strip() != ""


def _hours(v: object) -> bool:
    """a non-empty list of YYYY-MM-DDTHH hours"""
    if not isinstance(v, list) or not v:
        return False
    try:
        for item in v:
            parse_hour(item)
    except (TypeError, ValueError):
        return False
    return True


_SCHEMA: dict[str, dict[str, Callable[[object], bool]]] = {
    "faults": {
        "late_threshold_seconds": _positive_number,
        "bot_events_per_window": _int(1),
        "bot_window_seconds": _positive_number,
    },
    "faults.inject": {name: _int(0) for name in FAULT_TYPES},
    "volume": {
        "table": _identifier,
        "window_hours": _int(1),
        "min_history": _int(1),
        "min_ratio": _ratio,
        "evaluate_hours": _int(1),
        "type_min_median": _int(1),
    },
    "volume.reference": {"history": _hours, "evaluate": _hours},
    "freshness": {"table": _identifier, "column": _identifier, "max_lag_hours": _positive_number},
    "alert": {"label": _text, "template": _text},
}


def load_config(path: str | Path | None = None) -> dict:
    """Validated thresholds of ``path`` (default: the packaged quality.toml) as a plain dict."""
    path = DEFAULT_CONFIG if path is None else Path(path)
    try:
        data = tomllib.loads(path.read_text(encoding="utf-8"))
    except OSError as exc:
        raise ConfigError(f"cannot read config {path}: {exc}") from None
    except tomllib.TOMLDecodeError as exc:
        raise ConfigError(f"config {path} is not valid TOML: {exc}") from None
    out: dict = {}
    for section, keys in _SCHEMA.items():
        node: object = data
        target = out
        for part in section.split("."):
            if not isinstance(node, dict) or not isinstance(node.get(part), dict):
                raise ConfigError(f"config {path}: missing table [{section}]")
            node = node[part]
            target = target.setdefault(part, {})
        for key, check in keys.items():
            name = f"{section}.{key}"
            if key not in node:
                raise ConfigError(f"config {path}: missing key {name}")
            value = node[key]
            if not check(value):
                raise ConfigError(f"config {path}: {name} must be {check.__doc__} (got {value!r})")
            target[key] = list(value) if isinstance(value, list) else value
    return out


# ---------------------------------------------------------------- time helpers


def _utc(value: object) -> datetime | None:
    """Aware UTC datetime of a datetime (naive = UTC) or an ISO-8601 string; None otherwise."""
    if isinstance(value, datetime):
        moment = value
    elif isinstance(value, str):
        try:
            moment = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
    else:
        return None
    if moment.tzinfo is None or moment.utcoffset() is None:
        return moment.replace(tzinfo=UTC)
    return moment.astimezone(UTC)


def _floor_hour(moment: datetime) -> datetime:
    return moment.astimezone(UTC).replace(minute=0, second=0, microsecond=0)


def hour_label(moment: datetime) -> str:
    return moment.astimezone(UTC).strftime(_HOUR_FMT)


def parse_hour(label: str) -> datetime:
    return datetime.strptime(label, _HOUR_FMT).replace(tzinfo=UTC)


# ---------------------------------------------------------------- fault checks (lake rows)


@dataclass(frozen=True)
class Finding:
    """One problem seen in the lake. ``fault``: the simkit fault type it signals, or None.

    ``ref`` names the culprit: ``event_id:<id>`` / dedup key, ``raw_sha256:<hex>`` (quarantine
    keeps no payload) or ``user_id:<id>``.
    """

    check: str
    fault: str | None
    ref: str
    detail: str = ""

    def to_dict(self) -> dict:
        return asdict(self)


def _bronze_key(raw: object) -> str | None:
    import eventspec  # jsonschema-backed; only the fault checks need it

    try:
        obj = json.loads(raw) if isinstance(raw, str) else None
    except (ValueError, RecursionError):
        return None
    if not isinstance(obj, dict):
        return None
    try:
        return eventspec.dedup_key(obj)
    except Exception:  # unknown event or missing dedup field: quarantined, not a duplicate
        return None


def detect_duplicates(
    bronze_rows: Iterable[Mapping],
    silver_rows: Iterable[Mapping],
    quarantine_rows: Iterable[Mapping],
    summary: Mapping,
    cfg: Mapping,
) -> list[Finding]:
    """Dedup keys landed more than once (bronze keeps every copy), plus pipeline dedup errors."""
    rejected = {r.get("raw_sha256") for r in quarantine_rows}
    keys: Counter[str] = Counter()
    valid: Counter[str] = Counter()
    for row in bronze_rows:
        key = _bronze_key(row.get("raw_json"))
        if key is None:
            continue
        keys[key] += 1
        if row.get("raw_sha256") not in rejected:
            valid[key] += 1
    findings = [
        Finding("duplicate_key_in_bronze", "duplicate", key, f"{n} copies landed")
        for key, n in sorted(keys.items())
        if n > 1
    ]
    in_silver = Counter(r.get("dedup_key") for r in silver_rows)
    findings += [
        Finding("silver_duplicate_key", None, str(key), f"{n} rows in silver")
        for key, n in sorted(in_silver.items(), key=lambda kv: str(kv[0]))
        if n > 1
    ]
    expected = sum(n - 1 for n in valid.values() if n > 1)
    dropped = summary.get("duplicates_dropped", 0) if isinstance(summary, Mapping) else 0
    if dropped < expected:
        findings.append(
            Finding(
                "duplicates_not_dropped",
                None,
                "summary",
                f"duplicates_dropped={dropped} but bronze holds {expected} extra valid copies",
            )
        )
    return findings


def _quarantine_check(row: Mapping) -> tuple[str, str | None]:
    reason = row.get("reason")
    errors = [str(e) for e in (row.get("errors") or [])]
    if reason == rules.REASON_LATE_BEYOND_WINDOW:
        return "late_beyond_window", "late"
    if reason == rules.REASON_UNKNOWN_EVENT:
        return "quarantine_schema_drift", "schema_drift"
    if reason == rules.REASON_SCHEMA_INVALID:
        got = [m.group(1) for e in errors if (m := _TYPE_ERROR.search(e))]
        if "null" in got:
            return "quarantine_null_field", "null"
        if got:
            return "quarantine_wrong_type", "wrong_type"
        if any(e.startswith("unexpected field") for e in errors):
            return "quarantine_schema_drift", "schema_drift"
    return "quarantine_other", None


def _quarantine_findings(rows: Iterable[Mapping], fault: str | None) -> list[Finding]:
    out = []
    for row in rows:
        check, kind = _quarantine_check(row)
        if kind == fault:
            detail = f"{row.get('reason')}: {'; '.join(str(e) for e in row.get('errors') or [])}"
            out.append(Finding(check, kind, f"raw_sha256:{row.get('raw_sha256')}", detail))
    return out


def detect_null(quarantine_rows: Iterable[Mapping], cfg: Mapping) -> list[Finding]:
    """Quarantined as schema_invalid with a ``... got null`` type error."""
    return _quarantine_findings(quarantine_rows, "null")


def detect_wrong_type(quarantine_rows: Iterable[Mapping], cfg: Mapping) -> list[Finding]:
    """Quarantined as schema_invalid with ``must be of type T, got U`` (U is not null)."""
    return _quarantine_findings(quarantine_rows, "wrong_type")


def detect_schema_drift(quarantine_rows: Iterable[Mapping], cfg: Mapping) -> list[Finding]:
    """Quarantined with an unexpected field or as unknown_event (undeclared version)."""
    return _quarantine_findings(quarantine_rows, "schema_drift")


def _silver_ref(row: Mapping) -> str:
    event_id = row.get("event_id")
    return f"event_id:{event_id}" if event_id else str(row.get("dedup_key"))


def detect_late(
    silver_rows: Iterable[Mapping], quarantine_rows: Iterable[Mapping], cfg: Mapping
) -> list[Finding]:
    """Silver rows with received_at - occurred_at above the threshold, or late_beyond_window.

    Spark's ``is_late`` is not used: it is true for any event in an earlier UTC hour than its
    arrival, so a clean event arriving a second after the hour boundary would be a false alarm.
    """
    threshold = timedelta(seconds=cfg["faults"]["late_threshold_seconds"])
    findings = []
    for row in silver_rows:
        occurred = _utc(row.get("occurred_at"))
        received = _utc(row.get("received_at"))
        if occurred is None or received is None:
            continue
        delay = received - occurred
        if delay > threshold:
            findings.append(
                Finding(
                    "late_arrival",
                    "late",
                    _silver_ref(row),
                    f"arrived {delay.total_seconds():g} s after it occurred",
                )
            )
    return findings + _quarantine_findings(quarantine_rows, "late")


def detect_bots(silver_rows: Iterable[Mapping], cfg: Mapping) -> list[Finding]:
    """Users with more than ``bot_events_per_window`` silver events inside any window."""
    limit = cfg["faults"]["bot_events_per_window"]
    window = timedelta(seconds=cfg["faults"]["bot_window_seconds"])
    times: dict[str, list[datetime]] = defaultdict(list)
    for row in silver_rows:
        user = row.get("user_id")
        moment = _utc(row.get("occurred_at"))
        if isinstance(user, str) and moment is not None:
            times[user].append(moment)
    findings = []
    for user in sorted(times):
        moments = sorted(times[user])
        low = best = 0
        for high, moment in enumerate(moments):
            while moment - moments[low] >= window:
                low += 1
            best = max(best, high - low + 1)
        if best > limit:
            detail = f"{best} events within {window.total_seconds():g} s"
            findings.append(Finding("bot_rate", "bot", f"user_id:{user}", detail))
    return findings


def run_fault_checks(
    bronze_rows: Sequence[Mapping],
    silver_rows: Sequence[Mapping],
    quarantine_rows: Sequence[Mapping],
    summary: Mapping,
    cfg: Mapping,
) -> list[Finding]:
    """Every fault check on one batch; an empty list means nothing suspicious."""
    return [
        *detect_duplicates(bronze_rows, silver_rows, quarantine_rows, summary, cfg),
        *detect_null(quarantine_rows, cfg),
        *detect_wrong_type(quarantine_rows, cfg),
        *detect_schema_drift(quarantine_rows, cfg),
        *detect_late(silver_rows, quarantine_rows, cfg),
        *detect_bots(silver_rows, cfg),
        *_quarantine_findings(quarantine_rows, None),
    ]


def score(
    findings: Sequence[Finding], manifest: Mapping, sha_to_event_id: Mapping[str, str]
) -> dict:
    """Compare findings with the simkit manifest (answer key).

    A fault is caught when a finding of the same fault type names one of its events (or, for a
    bot, its user). Every finding that matches no fault of its type is a false alarm.
    """
    records = list(manifest.get("faults", []))
    identities = []
    for record in records:
        ids = {f"event_id:{e}" for e in record.get("event_ids", [])}
        user = (record.get("detail") or {}).get("user_id")
        if record["fault"] == "bot" and user:
            ids.add(f"user_id:{user}")
        identities.append(ids)

    def resolve(ref: str) -> str:
        if ref.startswith("raw_sha256:"):
            event_id = sha_to_event_id.get(ref.split(":", 1)[1])
            return f"event_id:{event_id}" if event_id else ref
        return ref

    matched: set[int] = set()
    unmatched = []
    for finding in findings:
        ref = resolve(finding.ref)
        hits = [
            k
            for k, record in enumerate(records)
            if finding.fault is not None
            and record["fault"] == finding.fault
            and ref in identities[k]
        ]
        if hits:
            matched.update(hits)
        else:
            unmatched.append(finding.to_dict())
    injected = dict.fromkeys(FAULT_TYPES, 0)
    caught = dict.fromkeys(FAULT_TYPES, 0)
    missed = []
    for k, record in enumerate(records):
        injected[record["fault"]] += 1
        if k in matched:
            caught[record["fault"]] += 1
        else:
            missed.append(
                {"fault": record["fault"], "event_ids": list(record.get("event_ids", []))}
            )
    return {
        "injected": injected,
        "caught": caught,
        "false_alarms": len(unmatched),
        "missed": missed,
        "unmatched": unmatched,
    }


def passed(report: Mapping) -> bool:
    """Every injected fault caught, no false alarm in the faulted run nor in the clean runs."""
    injected, caught = report["injected"], report["caught"]
    return (
        all(caught.get(t, 0) == n for t, n in injected.items())
        and report["false_alarms"] == 0
        and report.get("clean_alarms", 0) == 0
    )


def fault_row(
    report: Mapping, *, run_at: datetime, git_sha: str, total_seconds: float
) -> dict[str, str]:
    """``runs.csv`` row (M4) of one fault-injection run; SIMULATED data, said in the notes."""
    injected, caught = report["injected"], report["caught"]
    notes = {
        "simulated": "true",
        "seed": str(report["seed"]),
        "users": str(report["users"]),
        "days": str(report["days"]),
        "injected": str(sum(injected.values())),
        "caught": str(sum(caught.get(t, 0) for t in injected)),
        "false_alarms": str(report["false_alarms"] + report.get("clean_alarms", 0)),
        "clean_runs": str(report["clean_runs"]),
        **{t: f"{caught.get(t, 0)}/{injected.get(t, 0)}" for t in FAULT_TYPES},
    }
    return validate_row(
        {
            "run_at": run_at,
            "git_sha": git_sha,
            "variant": VARIANT,
            "data_date": report["start"],
            "total_seconds": total_seconds,
            "row_count": report["events"],
            "notes": " ".join(f"{k}={v}" for k, v in notes.items()),
        }
    )


# ---------------------------------------------------------------- volume + freshness


@dataclass(frozen=True)
class Alert:
    """One volume / freshness alarm. ``data_hour``: YYYY-MM-DDTHH (UTC) or ``empty``."""

    check: str
    kind: str
    data_hour: str
    table: str
    message: str
    details: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Mapping) -> Alert:
        try:
            return cls(
                check=str(data["check"]),
                kind=str(data["kind"]),
                data_hour=str(data["data_hour"]),
                table=str(data["table"]),
                message=str(data["message"]),
                details=dict(data.get("details") or {}),
            )
        except (KeyError, TypeError) as exc:
            raise QualityError(f"bad alert in report: {exc}") from None


def _median(values: Iterable[int]) -> float | int:
    m = statistics.median(list(values))
    return int(m) if float(m).is_integer() else m


def compare_hour(
    history: Sequence[Mapping[str, int]],
    current: Mapping[str, int],
    hour: datetime,
    cfg: Mapping,
    *,
    table: str,
) -> tuple[list[Alert], dict]:
    """Alerts of one hour's counts against the hours in ``history``, and its status line."""
    vol = cfg["volume"]
    label = hour_label(hour)
    total = sum(current.values())
    status = {
        "hour": label,
        "total": total,
        "history_hours": len(history),
        "median_total": None,
        "status": "ok",
    }
    if len(history) < vol["min_history"]:
        status["status"] = "insufficient_history"
        return [], status
    median_total = _median(sum(h.values()) for h in history)
    status["median_total"] = median_total
    alerts = []
    if total < vol["min_ratio"] * median_total:
        alerts.append(
            Alert(
                "volume_drop",
                "volume",
                label,
                table,
                f"{total} events in hour {label}, below {vol['min_ratio']} x the median "
                f"{median_total} of the {len(history)} hours before",
                {"total": total, "median_total": median_total, "history_hours": len(history)},
            )
        )
    for event_type in sorted({t for h in history for t in h}):
        median_type = _median(h.get(event_type, 0) for h in history)
        if median_type >= vol["type_min_median"] and current.get(event_type, 0) == 0:
            alerts.append(
                Alert(
                    "volume_type_missing",
                    "volume",
                    label,
                    table,
                    f"no {event_type} in hour {label} (median {median_type} per hour before)",
                    {"event_type": event_type, "median": median_type},
                )
            )
    status["status"] = "alert" if alerts else "ok"
    return alerts, status


def _evaluate_volume(
    hourly_counts: Mapping[datetime, Mapping[str, int]], cfg: Mapping, table: str | None
) -> tuple[list[Alert], list[dict]]:
    if not hourly_counts:
        return [], []
    vol = cfg["volume"]
    table = vol["table"] if table is None else table
    series: dict[datetime, Mapping[str, int]] = {}
    for moment, counts in hourly_counts.items():
        series[_floor_hour(_utc(moment))] = counts
    first, last = min(series), max(series)
    hours = []
    cursor = first
    while cursor <= last:  # continuous: an hour without rows counts as 0
        hours.append(cursor)
        cursor += timedelta(hours=1)
    alerts: list[Alert] = []
    statuses: list[dict] = []
    for i in range(max(0, len(hours) - vol["evaluate_hours"]), len(hours)):
        history = [series.get(h, {}) for h in hours[max(0, i - vol["window_hours"]) : i]]
        found, status = compare_hour(history, series.get(hours[i], {}), hours[i], cfg, table=table)
        alerts += found
        statuses.append(status)
    return alerts, statuses


def check_volume(
    hourly_counts: Mapping[datetime, Mapping[str, int]], cfg: Mapping, *, table: str | None = None
) -> list[Alert]:
    """Volume alerts of the ``evaluate_hours`` newest hours of ``{hour: {event_type: count}}``."""
    return _evaluate_volume(hourly_counts, cfg, table)[0]


def volume_status(
    hourly_counts: Mapping[datetime, Mapping[str, int]], cfg: Mapping, *, table: str | None = None
) -> list[dict]:
    """Status line (ok / alert / insufficient_history) of every evaluated hour."""
    return _evaluate_volume(hourly_counts, cfg, table)[1]


def check_freshness(latest_event_at: object, now: datetime, cfg: Mapping) -> list[Alert]:
    """Alert when the newest event is older than ``max_lag_hours`` (or the table is empty)."""
    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("now must be timezone-aware")
    fresh = cfg["freshness"]
    table = fresh["table"]
    latest = _utc(latest_event_at)
    if latest is None:
        return [Alert("freshness_empty", "freshness", "empty", table, f"{table} has no rows", {})]
    lag = now - latest
    if lag <= timedelta(hours=fresh["max_lag_hours"]):
        return []
    hours = round(lag.total_seconds() / 3600, 1)
    return [
        Alert(
            "freshness_stale",
            "freshness",
            hour_label(latest),
            table,
            f"newest event {latest.isoformat()} is {hours} h old (max {fresh['max_lag_hours']} h)",
            {"latest_event_at": latest.isoformat(), "lag_hours": hours},
        )
    ]


def staging_hourly_counts(con, cfg: Mapping) -> dict[datetime, dict[str, int]]:
    """``{landing hour: {event_type: count}}`` of the volume table (DuckDB connection)."""
    table = cfg["volume"]["table"]
    rows = con.execute(
        f"SELECT landing_date, landing_hour, event_type, count(*) FROM {table} "
        "GROUP BY ALL ORDER BY ALL"
    ).fetchall()
    out: dict[datetime, dict[str, int]] = {}
    for day, hour, event_type, n in rows:
        moment = datetime(day.year, day.month, day.day, int(hour), tzinfo=UTC)
        out.setdefault(moment, {})[str(event_type)] = int(n)
    return out


def latest_event_at(con, cfg: Mapping) -> datetime | None:
    fresh = cfg["freshness"]
    value = con.execute(f"SELECT max({fresh['column']}) FROM {fresh['table']}").fetchone()[0]
    return _utc(value)


def landing_hourly_counts(
    landing_dir: str | Path, hours: Iterable[datetime]
) -> dict[datetime, dict[str, int]]:
    """Counts of the ingested event types from each hour's ``_manifest.json`` (none = empty)."""
    out: dict[datetime, dict[str, int]] = {}
    for moment in hours:
        part = ingest.partition_dir(Path(landing_dir), moment.date().isoformat(), moment.hour)
        path = part / ingest.MANIFEST_FILE
        try:
            manifest = json.loads(path.read_text(encoding="utf-8"))
        except FileNotFoundError:
            out[moment] = {}
            continue
        except ValueError as exc:
            raise QualityError(f"invalid manifest {path}: {exc}") from None
        kept = set(manifest.get("event_types") or [])
        counts = manifest.get("counts_by_type") or {}
        out[moment] = {t: int(n) for t, n in sorted(counts.items()) if not kept or t in kept}
    return out


def run_gap(
    cfg: Mapping,
    landing_dir: str | Path,
    ingest: Callable[[str, int, Path], object] | None = None,
) -> tuple[list[Alert], dict]:
    """Volume check of the known GH Archive gap hours against full reference hours (config).

    ``ingest(day, hour, landing_dir)`` lands each configured hour first when given.
    """
    ref = cfg["volume"]["reference"]
    history = [parse_hour(h) for h in ref["history"]]
    evaluate = [parse_hour(h) for h in ref["evaluate"]]
    if ingest is not None:
        for moment in [*history, *evaluate]:
            ingest(moment.date().isoformat(), moment.hour, Path(landing_dir))
    counts = landing_hourly_counts(landing_dir, [*history, *evaluate])
    past = [counts[h] for h in history]
    alerts: list[Alert] = []
    statuses = []
    for moment in evaluate:
        found, status = compare_hour(
            past, counts[moment], moment, cfg, table="GH Archive landing (_manifest.json)"
        )
        status["caught"] = bool(found)
        alerts += found
        statuses.append(status)
    report = {
        "source": "GH Archive (real data)",
        "history": list(ref["history"]),
        "evaluate": list(ref["evaluate"]),
        "history_totals": {hour_label(h): sum(counts[h].values()) for h in history},
        "hours": statuses,
        "alerts": [a.to_dict() for a in alerts],
        "caught": all(s["caught"] for s in statuses),
    }
    return alerts, report


# ---------------------------------------------------------------- GitHub issue


def marker(alert: Alert) -> str:
    """Hidden, deterministic id of an alert: one per check and data hour."""
    return f"<!-- dq:{alert.check}:{alert.data_hour} -->"


def _template_body(template_text: str) -> str:
    lines = template_text.splitlines()
    if lines and lines[0].strip() == "---":
        for i in range(1, len(lines)):
            if lines[i].strip() == "---":
                return "\n".join(lines[i + 1 :])
    return template_text


def template_headings(template_text: str) -> list[str]:
    return [
        ln.strip()[3:] for ln in _template_body(template_text).splitlines() if ln.startswith("## ")
    ]


def _symptom_lines(alerts: Sequence[Alert]) -> list[str]:
    return [
        f"- `{a.check}` (giờ dữ liệu `{a.data_hour}` UTC): {a.message} {marker(a)}" for a in alerts
    ]


def _run_line(run_url: str) -> str:
    return (
        f"Lần chạy GitHub Actions: {run_url}"
        if run_url
        else "Lần chạy GitHub Actions: (không có link)"
    )


def _sections(alerts: Sequence[Alert], run_url: str) -> dict[str, str]:
    tables = sorted({a.table for a in alerts})
    hours = sorted({a.data_hour for a in alerts if a.data_hour != "empty"})
    span = f"{hours[0]} → {hours[-1]} (UTC)" if hours else "bảng rỗng, không có giờ dữ liệu"
    kinds = {a.kind for a in alerts}
    cause = []
    if "volume" in kinds:
        cause.append(
            "- Khối lượng: kiểm tra giờ GH Archive gốc (file `YYYY-MM-DD-H.json.gz` có đủ và có "
            "đủ loại event không; từ 2026-11 có giờ mất hẳn PushEvent/CreateEvent/DeleteEvent), "
            "`_manifest.json` (`counts_by_type`) của landing và summary của bước ingest."
        )
    if "freshness" in kinds:
        cause.append(
            "- Độ tươi: pipeline theo lịch (`Pipeline (scheduled)`) có chạy và xanh không, giờ "
            "nào đang `missing` trong `out/ingest-summary.json`, secret MotherDuck còn hợp lệ."
        )
    return {
        "Triệu chứng": "\n".join(
            [
                _run_line(run_url),
                "",
                *_symptom_lines(alerts),
                "",
                "Dữ liệu GH Archive là dữ liệu thật; kiểm tra do `python -m repopulse.quality "
                "check` (incremental batch theo giờ).",
            ]
        ),
        "Phạm vi": "\n".join(
            [
                f"- Bảng hoặc mart bị ảnh hưởng: {', '.join(f'`{t}`' for t in tables)} và các "
                "mart gold dựng từ nó",
                f"- Khoảng thời gian dữ liệu: {span}",
                f"- Giờ dữ liệu: {', '.join(hours) if hours else '(không có)'}",
            ]
        ),
        "Nguyên nhân": "\n".join(cause) + "\n- Ghi `file:line` khi đã tìm ra.",
        "Điều kiện kích hoạt": (
            "Ngưỡng nằm ở `repopulse/quality.toml` (`[volume]`, `[freshness]`). Báo động lặp lại "
            "mỗi lần chạy tới khi giờ dữ liệu bình thường trở lại; cùng kiểm tra và cùng giờ chỉ "
            "được ghi một lần (marker ẩn)."
        ),
        "Cách xử lý và chạy bù": "\n".join(
            [
                "- Chạy bù bằng `workflow_dispatch` của `Pipeline (scheduled)` với input `date` "
                "(YYYY-MM-DD), `hour` (0..23, giờ cuối) và `hours` (1..6) phủ các giờ trên; "
                "staging bỏ qua event đã có nên chạy lại không sinh trùng.",
                "- Nếu nguồn GH Archive thiếu thật (không có file, hoặc mất loại event), ghi lại "
                "ở đây và đóng issue khi giờ dữ liệu bình thường trở lại.",
            ]
        ),
    }


def build_issue(alerts: Sequence[Alert], template_text: str, run_url: str) -> tuple[str, str]:
    """(title, body) of a data-incident issue: every ``## `` heading of the template, in order."""
    if not alerts:
        raise QualityError("build_issue needs at least one alert")
    kinds = sorted({a.kind for a in alerts})
    title = " + ".join(TITLES.get(k, f"[dq:{k}] Báo động chất lượng dữ liệu") for k in kinds)
    filled = _sections(alerts, run_url)
    parts = []
    for heading in template_headings(template_text):
        parts.append(f"## {heading}\n\n{filled.get(heading, '(chưa điền)')}\n")
    return title, "\n".join(parts)


def build_comment(alerts: Sequence[Alert], run_url: str) -> str:
    return "\n".join(["Báo động mới.", "", _run_line(run_url), "", *_symptom_lines(alerts)]) + "\n"


def run_gh(args: Sequence[str], stdin: str | None = None) -> str:
    """Run the GitHub CLI (token from GH_TOKEN, never printed); raise QualityError on failure."""
    proc = subprocess.run(
        ["gh", *args], input=stdin, capture_output=True, text=True, encoding="utf-8", check=False
    )
    if proc.returncode != 0:
        raise QualityError(
            f"gh {' '.join(args[:2])} failed (exit {proc.returncode}): {proc.stderr}"
        )
    return proc.stdout


def publish(
    alerts: Sequence[Alert],
    *,
    template_text: str,
    run_url: str,
    gh: Callable[..., str],
    label: str,
    repo: str | None = None,
    dry_run: bool = False,
) -> list[str]:
    """Open or update one ``label`` issue per kind of check; return what was done.

    No open issue of that kind: create one. Open issue: comment with the alerts whose marker is
    not in its body or comments yet; none new: do nothing (an hourly re-run does not spam).
    ``dry_run`` prints the issues and never calls ``gh``.
    """
    if not alerts:
        return []
    groups: dict[str, list[Alert]] = defaultdict(list)
    for alert in alerts:
        groups[alert.kind].append(alert)
    actions: list[str] = []
    if dry_run:
        for kind in sorted(groups):
            title, body = build_issue(groups[kind], template_text, run_url)
            print(f"[dry-run] would open or update issue: {title}\n\n{body}")
            actions.append(f"dry-run {kind}")
        return actions

    repo_args = ["--repo", repo] if repo else []
    listed = gh(
        [
            "issue",
            "list",
            "--label",
            label,
            "--state",
            "open",
            "--json",
            "number,title,body,comments",
            "--limit",
            "100",
            *repo_args,
        ]
    )
    issues = json.loads(listed or "[]")
    label_ready = False
    for kind in sorted(groups):
        group = groups[kind]
        prefix = f"[dq:{kind}]"
        issue = next((i for i in issues if str(i.get("title", "")).startswith(prefix)), None)
        if issue is None:
            if not label_ready:
                gh(
                    [
                        "label",
                        "create",
                        label,
                        "--force",
                        "--color",
                        "B60205",
                        "--description",
                        "Data quality alert (repopulse.quality)",
                        *repo_args,
                    ]
                )
                label_ready = True
            title, body = build_issue(group, template_text, run_url)
            gh(
                ["issue", "create", "--title", title, "--body-file", "-", "--label", label]
                + repo_args,
                stdin=body,
            )
            actions.append(f"created {kind}")
            continue
        number = str(issue["number"])
        seen = "\n".join(
            [str(issue.get("body") or "")]
            + [str(c.get("body") or "") for c in issue.get("comments") or []]
        )
        new = [a for a in group if marker(a) not in seen]
        if not new:
            actions.append(f"skipped {kind} #{number}: every alert already recorded")
            continue
        gh(
            ["issue", "comment", number, "--body-file", "-", *repo_args],
            stdin=build_comment(new, run_url),
        )
        actions.append(f"commented {kind} #{number}")
    return actions


# ---------------------------------------------------------------- fault-injection harness


def land_events(
    events: Sequence[Mapping], landing_dir: str | Path
) -> tuple[list[tuple[str, int]], dict[str, str]]:
    """Write app events to ``<landing>/date=/hour=/events.jsonl.gz`` by the hour of received_at.

    An event whose received_at is broken (null / wrong type) lands in the hour of the event
    before it. Returns the landed (date, hour) list and ``{sha256 of the line: event_id}``.
    """
    by_hour: dict[datetime, list[bytes]] = defaultdict(list)
    sha_to_event_id: dict[str, str] = {}
    previous: datetime | None = None
    for event in events:
        received = event.get("received_at")
        moment = _utc(received) if isinstance(received, str) else None
        if moment is None:
            moment = previous
        if moment is None:
            occurred = event.get("occurred_at")
            moment = _utc(occurred) if isinstance(occurred, str) else None
        if moment is None:
            raise QualityError("the first event has neither received_at nor occurred_at")
        hour = _floor_hour(moment)
        previous = hour
        text = json.dumps(event, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        raw = text.encode("utf-8")
        by_hour[hour].append(raw)
        event_id = event.get("event_id")
        if isinstance(event_id, str):
            sha_to_event_id[hashlib.sha256(raw).hexdigest()] = event_id
    hours = []
    for hour in sorted(by_hour):
        day = hour.date().isoformat()
        part = ingest.partition_dir(Path(landing_dir), day, hour.hour)
        part.mkdir(parents=True, exist_ok=True)
        with (
            (part / APP_EVENTS_FILE).open("wb") as fh,
            gzip.GzipFile(filename="", mode="wb", fileobj=fh, mtime=0) as gz,
        ):
            for raw in by_hour[hour]:
                gz.write(raw + b"\n")
        hours.append((day, hour.hour))
    return hours, sha_to_event_id


def _from_micros(value: int | None) -> datetime | None:
    if value is None:
        return None
    return datetime(1970, 1, 1, tzinfo=UTC) + timedelta(microseconds=int(value))


def _slices(hours: Sequence[tuple[str, int]], date_col: str, hour_col: str) -> str:
    return " OR ".join(
        f"({date_col} = CAST('{day}' AS DATE) AND {hour_col} = {int(hour)})" for day, hour in hours
    )


def pipeline_findings(
    spark,
    *,
    hours: Sequence[tuple[str, int]],
    landing_dir: str | Path,
    lake_dir: str | Path,
    salt: str,
    cfg: Mapping,
) -> tuple[list[Finding], dict]:
    """Run the app-event batch on ``hours`` and the fault checks on the resulting slices."""
    from pyspark.sql import functions as F

    from repopulse.spark import job

    lake = Path(lake_dir)
    summary = job.run_batch(
        spark,
        source="app",
        hours=list(hours),
        landing_dir=Path(landing_dir),
        lake_dir=lake,
        salt=salt,
    )

    def table(layer: str):
        return spark.read.format("delta").load(str(lake / layer / APP_TABLE))

    landed = _slices(hours, "data_date", "hour")
    bronze = [
        r.asDict() for r in table("bronze").where(landed).select("raw_json", "raw_sha256").collect()
    ]
    quarantine = [
        {"reason": r["reason"], "errors": list(r["errors"] or []), "raw_sha256": r["raw_sha256"]}
        for r in table("quarantine")
        .where(landed)
        .select("reason", "errors", "raw_sha256")
        .collect()
    ]
    silver = [
        {
            "dedup_key": r["dedup_key"],
            "event_id": r["event_id"],
            "user_id": r["user_id"],
            "occurred_at": _from_micros(r["occurred_us"]),
            "received_at": _from_micros(r["received_us"]),
            "is_late": r["is_late"],
        }
        for r in table("silver")
        .where(_slices(hours, "landing_date", "landing_hour"))
        .select(
            "dedup_key",
            "event_id",
            "user_id",
            F.expr("unix_micros(occurred_at)").alias("occurred_us"),
            F.expr("unix_micros(received_at)").alias("received_us"),
            "is_late",
        )
        .collect()
    ]
    return run_fault_checks(bronze, silver, quarantine, summary, cfg), summary


def run_injected(
    spark,
    *,
    seed: int,
    users: int,
    days: int,
    start: date,
    work_dir: str | Path,
    salt: str,
    cfg: Mapping,
) -> dict:
    """SIMULATED events + the configured simkit faults -> landing -> Spark batch -> score."""
    from simkit import inject_faults, simulate

    sim = simulate(seed=seed, users=users, days=days, start=start)
    result = inject_faults(sim.events, cfg["faults"]["inject"], seed=seed)
    work = Path(work_dir)
    landing, lake = work / "landing", work / "lake"
    hours, sha_to_event_id = land_events(result.events, landing)
    findings, summary = pipeline_findings(
        spark, hours=hours, landing_dir=landing, lake_dir=lake, salt=salt, cfg=cfg
    )
    manifest = result.manifest()
    return {
        "events": len(result.events),
        "manifest": manifest,
        "score": score(findings, manifest, sha_to_event_id),
        "findings": findings,
        "summary": summary,
        "hours": hours,
        "landing_dir": landing,
        "lake_dir": lake,
    }


def run_clean(
    spark,
    *,
    seed: int,
    users: int,
    days: int,
    start: date,
    work_dir: str | Path,
    salt: str,
    cfg: Mapping,
) -> list[Finding]:
    """SIMULATED events without any fault through the batch: every finding is a false alarm."""
    from simkit import simulate

    sim = simulate(seed=seed, users=users, days=days, start=start)
    work = Path(work_dir)
    hours, _ = land_events(sim.events, work / "landing")
    findings, _ = pipeline_findings(
        spark, hours=hours, landing_dir=work / "landing", lake_dir=work / "lake", salt=salt, cfg=cfg
    )
    return findings


def run_fault_injection(
    spark,
    *,
    seed: int,
    users: int,
    days: int,
    start: date,
    clean_runs: int,
    work_dir: str | Path,
    salt: str,
    cfg: Mapping,
) -> dict:
    """One faulted run (seed) + ``clean_runs`` clean runs (seed+1, seed+2, ...); JSON report."""
    work = Path(work_dir)
    common = {"users": users, "days": days, "start": start, "salt": salt, "cfg": cfg}
    faulted = run_injected(spark, seed=seed, work_dir=work / "faulted", **common)
    clean = []
    for k in range(1, clean_runs + 1):
        found = run_clean(spark, seed=seed + k, work_dir=work / f"clean-{k}", **common)
        clean.append({"seed": seed + k, "findings": [f.to_dict() for f in found]})
    sc = faulted["score"]
    return {
        "simulated": True,
        "seed": seed,
        "users": users,
        "days": days,
        "start": start.isoformat(),
        "events": faulted["events"],
        "hours": len(faulted["hours"]),
        "injected": sc["injected"],
        "caught": sc["caught"],
        "false_alarms": sc["false_alarms"],
        "missed": sc["missed"],
        "unmatched": sc["unmatched"],
        "findings": [f.to_dict() for f in faulted["findings"]],
        "summary": faulted["summary"],
        "clean_runs": clean_runs,
        "clean_alarms": sum(len(c["findings"]) for c in clean),
        "clean": clean,
    }


# ---------------------------------------------------------------- CLI


def _spark_session():
    from repopulse.spark.job import create_spark_session

    return create_spark_session("repopulse-data-quality")


def _connect(database: str):
    import duckdb

    if database.lower().startswith(("md:", "motherduck:")):
        return duckdb.connect(database)
    if not Path(database).is_file():
        raise QualityError(f"database not found: {database}")
    return duckdb.connect(database, read_only=True)


def _write_json(path: Path, data: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, default=str) + "\n", encoding="utf-8")


def _template_text(cfg: Mapping) -> str:
    configured = Path(cfg["alert"]["template"])
    for path in (configured, ROOT / configured):
        if path.is_file():
            return path.read_text(encoding="utf-8")
    raise QualityError(f"issue template not found: {configured}")


def _cmd_faults(args: argparse.Namespace, cfg: dict) -> int:
    if not _GIT_SHA.fullmatch(args.git_sha):
        raise QualityError(
            f"--git-sha must be 7-40 lowercase hex characters (got {args.git_sha!r})"
        )
    for name in ("users", "days"):
        if getattr(args, name) < 1:
            raise QualityError(f"--{name} must be >= 1")
    if args.clean_runs < 0:
        raise QualityError("--clean-runs must be >= 0")
    try:
        salt = salt_from_env()
    except MissingSaltError as exc:
        raise QualityError(str(exc)) from None
    began = time.perf_counter()
    spark = _spark_session()
    try:
        report = run_fault_injection(
            spark,
            seed=args.seed,
            users=args.users,
            days=args.days,
            start=args.start,
            clean_runs=args.clean_runs,
            work_dir=args.work_dir,
            salt=salt,
            cfg=cfg,
        )
    finally:
        spark.stop()
    elapsed = round(time.perf_counter() - began, 3)
    row = fault_row(
        report,
        run_at=datetime.now(UTC).replace(microsecond=0),
        git_sha=args.git_sha,
        total_seconds=elapsed,
    )
    _write_json(args.report, report)
    write_row(args.out, row)
    print(f"simulated fault injection: {row['notes']}")
    ok = passed(report)
    if not ok:
        print("error: a fault was missed or a check raised a false alarm", file=sys.stderr)
    return 0 if ok else 1


def _cmd_check(args: argparse.Namespace, cfg: dict) -> int:
    import duckdb

    if args.now is None:
        now = datetime.now(UTC)
    else:
        try:
            now = datetime.fromisoformat(args.now)
        except ValueError:
            raise QualityError(f"--now is not ISO-8601: {args.now!r}") from None
        if now.tzinfo is None or now.utcoffset() is None:
            raise QualityError("--now must include a time zone")
    con = _connect(args.database)
    try:
        counts = staging_hourly_counts(con, cfg)
        latest = latest_event_at(con, cfg)
    except duckdb.Error as exc:
        raise QualityError(f"cannot read {cfg['volume']['table']}: {exc}") from None
    finally:
        con.close()
    volume_alerts, statuses = _evaluate_volume(counts, cfg, None)
    freshness_alerts = check_freshness(latest, now, cfg)
    alerts = volume_alerts + freshness_alerts
    report = {
        "generated_at": now.isoformat(),
        "database": args.database,
        "volume": {"table": cfg["volume"]["table"], "hours": statuses},
        "freshness": {
            "table": cfg["freshness"]["table"],
            "latest_event_at": latest.isoformat() if latest else None,
            "max_lag_hours": cfg["freshness"]["max_lag_hours"],
        },
        "alerts": [a.to_dict() for a in alerts],
    }
    _write_json(args.report, report)
    for status in statuses:
        print(f"volume {status['hour']}: {status['status']} (total {status['total']})")
    for alert in alerts:
        print(f"ALERT {alert.check} {alert.data_hour}: {alert.message}")
    if not alerts:
        print("OK: no volume or freshness alert")
    return 0


def _ingester(work_dir: Path) -> Callable[[str, int, Path], object]:
    def land(day: str, hour: int, landing: Path) -> object:
        result = ingest.ingest_hour(day, hour, landing_root=landing, work_dir=work_dir)
        print(json.dumps(result.to_dict(), sort_keys=True), flush=True)
        return result

    return land


def _cmd_gap(args: argparse.Namespace, cfg: dict) -> int:
    ingester = _ingester(args.work_dir) if args.ingest else None
    alerts, report = run_gap(cfg, args.landing_dir, ingest=ingester)
    _write_json(args.report, report)
    for alert in alerts:
        print(f"ALERT {alert.check} {alert.data_hour}: {alert.message}")
    if report["caught"]:
        print("OK: every reference gap hour raised a volume alert")
        return 0
    print("error: a reference gap hour raised no volume alert", file=sys.stderr)
    return 1


def _cmd_alert(args: argparse.Namespace, cfg: dict) -> int:
    try:
        report = json.loads(args.report.read_text(encoding="utf-8"))
    except FileNotFoundError:
        raise QualityError(f"missing report {args.report}") from None
    except ValueError as exc:
        raise QualityError(f"invalid JSON in {args.report}: {exc}") from None
    alerts = [Alert.from_dict(a) for a in report.get("alerts", [])]
    if not alerts:
        print("OK: no alert, no issue")
        return 0
    actions = publish(
        alerts,
        template_text=_template_text(cfg),
        run_url=args.run_url,
        gh=run_gh,
        label=cfg["alert"]["label"],
        repo=args.repo,
        dry_run=args.dry_run,
    )
    for action in actions:
        print(action)
    return 0


def _parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="python -m repopulse.quality",
        description="Data quality checks and alerts of the incremental batch.",
    )
    p.add_argument("--config", type=Path, help="thresholds (default: repopulse/quality.toml)")
    sub = p.add_subparsers(dest="command", required=True)

    f = sub.add_parser("faults", help="six simkit faults (simulated) through the Spark batch")
    f.add_argument("--seed", type=int, default=42)
    f.add_argument("--users", type=int, default=30)
    f.add_argument("--days", type=int, default=2)
    f.add_argument("--start", type=date.fromisoformat, default=DEFAULT_START)
    f.add_argument("--clean-runs", type=int, default=2)
    f.add_argument("--work-dir", type=Path, default=Path("data/dq-faults"))
    f.add_argument("--git-sha", required=True)
    f.add_argument("--report", type=Path, required=True)
    f.add_argument("--out", type=Path, required=True, help="runs.csv artifact row")

    c = sub.add_parser("check", help="volume + freshness of the GH Archive staging table")
    c.add_argument("--database", required=True, help="DuckDB file or md:<database>")
    c.add_argument("--now", help="ISO-8601 with time zone (default: now, UTC)")
    c.add_argument("--report", type=Path, required=True)

    g = sub.add_parser("gap", help="volume check of the known GH Archive gap hours (config)")
    g.add_argument("--landing-dir", type=Path, default=ingest.DEFAULT_LANDING)
    g.add_argument("--ingest", action="store_true", help="ingest the configured hours first")
    g.add_argument("--work-dir", type=Path, default=ingest.DEFAULT_WORK_DIR)
    g.add_argument("--report", type=Path, required=True)

    a = sub.add_parser("alert", help="open / update the data-incident issue of a report")
    a.add_argument("--report", type=Path, required=True)
    a.add_argument("--run-url", default="")
    a.add_argument("--repo", help="owner/name passed to gh")
    a.add_argument("--dry-run", action="store_true", help="print the issues, call nothing")
    return p


_COMMANDS = {"faults": _cmd_faults, "check": _cmd_check, "gap": _cmd_gap, "alert": _cmd_alert}


def _tolerant_output() -> None:
    """Issue text is Vietnamese: never crash on a console without those letters (cp1252)."""
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(errors="backslashreplace")
        except (AttributeError, ValueError):
            pass


def main(argv: Sequence[str] | None = None) -> int:
    _tolerant_output()
    args = _parser().parse_args(argv)
    try:
        cfg = load_config(args.config)
        return _COMMANDS[args.command](args, cfg)
    except (QualityError, OSError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
