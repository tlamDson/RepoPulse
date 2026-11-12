"""Compare the two end-to-end paths of RepoPulse (D3, docs/path-comparison.md).

Path A = GitHub Actions runner (PySpark + Delta) with gold on MotherDuck; path B = Databricks
serverless bronze/silver (D1) with dbt on a Databricks SQL warehouse (D2). Both run the same
incremental batch (ingest -> bronze/silver -> gold -> Q1-Q6) on the same data window, from
``.github/workflows/path-comparison.yml``. Each run of each path writes one ``runs.csv`` row,
failed runs included, so the success rate can be computed from ``runs.csv`` alone.

Row (``variant`` = ``path-a-runner`` | ``path-b-databricks``):

- ``total_seconds``: sum of the timed pipeline steps of the run (end to end, runner setup and
  queueing excluded; a failed run sums the steps that ran).
- ``q1_seconds`` .. ``q6_seconds``: time to read the mart of each question from the path's gold.
- ``row_count``: rows the bronze/silver step inserted into silver.
- ``notes``: ``status=success|failed``, ``failed_step=<step>`` (failed runs), ``hour``, ``hours``,
  ``<step>_seconds`` for each timed step, the waits (``queue_seconds``, ``setup_seconds`` of the
  runner; on B also ``job_queue_seconds``, ``job_setup_seconds`` of the serverless job and
  ``warehouse_start_seconds``), ``missing_hours``, ``run_id``, ``trigger``.

Percentiles are **nearest-rank**: for n sorted values, the p-th percentile is the value at rank
``ceil(p / 100 * n)`` (1-based, at least 1). It is always one of the measured values; an empty
list has no percentile (``None``).

Standard library only (DuckDB is imported lazily by ``queries --engine duckdb`` and
``md-ensure``). Usage: ``python -m repopulse.pathcompare {window,runner-waits,warehouse-start,
queries,md-ensure,row,summary} --help``.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import os
import re
import sys
import time
import tomllib
import urllib.request
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from repopulse import ingest
from repopulse.probe import (
    ApiError,
    ProbeContext,
    Redactor,
    Secrets,
    _call,
    _json,
    resolve_warehouse,
    urllib_transport,
)
from repopulse.runs_log import COLUMNS, RunsLogError, validate_row

VARIANTS: dict[str, str] = {"a": "path-a-runner", "b": "path-b-databricks"}
PATH_OF_VARIANT: dict[str, str] = {v: k for k, v in VARIANTS.items()}
PATH_STEPS: dict[str, tuple[str, ...]] = {
    "a": ("ingest", "spark", "export", "dbt", "queries"),
    "b": ("ingest", "databricks", "warehouse", "dbt", "queries"),
}
WAIT_KEYS: dict[str, tuple[str, ...]] = {
    "a": ("queue", "setup"),
    "b": ("queue", "setup", "job_queue", "job_setup", "warehouse_start"),
}
OUTCOMES = frozenset({"success", "failure", "cancelled", "skipped"})
STATUSES = ("success", "failed")
MART_BY_QUERY: dict[str, str] = {
    "q1": "mart_daily_active_contributors",
    "q2": "mart_cohort_retention",
    "q3": "mart_contribution_funnel",
    "q4": "mart_responsiveness",
    "q5": "mart_repo_concentration",
    "q6": "mart_repo_health_score",
}
PATH_A_DATABASE = "repopulse_path_a"
PRODUCTION_DATABASE = "repopulse"
MAX_WINDOW_HOURS = 6  # runner disk: only a few hours per run
DEFAULT_HOURS = 1
LAG_HOURS = 2
PUSH_HOUR = 23
PERCENTILES = (50, 95)

_NOTE_WORD = re.compile(r"([a-z][a-z0-9_]*)=([^\s=]+)")
_TOKEN = re.compile(r"[^\s=]+")
_DB_NAME = re.compile(r"[a-z][a-z0-9_]*")
_SCHEMA = re.compile(r"[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)?")
_STATEMENT_WAITING = frozenset({"PENDING", "RUNNING"})
_SECONDS = "_seconds"


class PathCompareError(ValueError):
    """Bad input for a path comparison helper (row, runs.csv, window, API answer)."""


# ---------------------------------------------------------------- percentile


def percentile(values: Iterable[float], p: float) -> float | None:
    """Nearest-rank percentile: sorted value at rank ceil(p/100 * n); ``None`` when empty."""
    if isinstance(p, bool) or not 0 < p <= 100:
        raise ValueError(f"percentile p must be in (0, 100], got {p!r}")
    ordered = sorted(values)
    if not ordered:
        return None
    rank = max(1, math.ceil(p / 100 * len(ordered)))
    return ordered[rank - 1]


def _p50_p95(values: Sequence[float]) -> tuple[float | None, float | None]:
    return percentile(values, 50), percentile(values, 95)


# ---------------------------------------------------------------- row


def _path(path: str) -> str:
    if path not in VARIANTS:
        raise PathCompareError(f"path must be one of {sorted(VARIANTS)} (got {path!r})")
    return path


def _number(name: str, value: object) -> float:
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise PathCompareError(f"{name} must be a number (got {value!r})")
    if not math.isfinite(value) or value < 0:
        raise PathCompareError(f"{name} must be finite and >= 0 (got {value!r})")
    return float(value)


def _token(name: str, value: object) -> str:
    if not isinstance(value, str) or not _TOKEN.fullmatch(value):
        raise PathCompareError(f"{name} must be one token without spaces or '=' (got {value!r})")
    return value


def _status(path: str, outcomes: Mapping[str, str]) -> tuple[str, str | None]:
    steps = PATH_STEPS[path]
    unknown = sorted(set(outcomes) - set(steps))
    if unknown:
        raise PathCompareError(f"unknown step(s) for path {path}: {', '.join(unknown)}")
    for name, outcome in outcomes.items():
        if outcome not in OUTCOMES:
            raise PathCompareError(f"outcome of {name} must be one of {sorted(OUTCOMES)}")
    for name in steps:
        # A step with no outcome did not run: the job stopped before it.
        if outcomes.get(name, "skipped") != "success":
            return "failed", name
    return "success", None


def build_row(
    *,
    path: str,
    run_at: datetime,
    git_sha: str,
    data_date: str,
    hour: int,
    hours: int,
    outcomes: Mapping[str, str],
    timings: Mapping[str, object],
    waits: Mapping[str, object] | None = None,
    queries: Mapping[str, object] | None = None,
    row_count: int | None = None,
    run_id: str,
    trigger: str,
    ingest_records: Sequence[Mapping[str, Any]] | None = None,
) -> dict[str, str]:
    """The ``runs.csv`` row of one run of one path (failed runs too), as CSV-ready strings."""
    path = _path(path)
    steps = PATH_STEPS[path]
    if isinstance(hour, bool) or not isinstance(hour, int) or not 0 <= hour <= 23:
        raise PathCompareError(f"hour must be 0..23 (got {hour!r})")
    if isinstance(hours, bool) or not isinstance(hours, int) or not 1 <= hours <= 24:
        raise PathCompareError(f"hours must be 1..24 (got {hours!r})")
    status, failed_step = _status(path, outcomes)

    unknown = sorted(set(timings) - set(steps))
    if unknown:
        raise PathCompareError(f"timing of unknown step(s): {', '.join(unknown)}")
    seconds = {
        name: _number(f"timing of {name}", timings[name]) for name in steps if name in timings
    }
    if status == "success":
        missing = [name for name in steps if name not in seconds]
        if missing:
            raise PathCompareError(f"successful run without timing of: {', '.join(missing)}")

    waits = dict(waits or {})
    unknown = sorted(set(waits) - set(WAIT_KEYS[path]))
    if unknown:
        raise PathCompareError(f"wait(s) not measured on path {path}: {', '.join(unknown)}")
    wait_seconds = {k: _number(f"wait {k}", waits[k]) for k in WAIT_KEYS[path] if k in waits}

    queries = dict(queries or {})
    unknown = sorted(set(queries) - set(MART_BY_QUERY))
    if unknown:
        raise PathCompareError(f"unknown query id(s): {', '.join(unknown)}")
    if row_count is not None and (
        isinstance(row_count, bool) or not isinstance(row_count, int) or row_count < 0
    ):
        raise PathCompareError(f"row_count must be an int >= 0 (got {row_count!r})")

    notes: dict[str, str] = {"status": status}
    if failed_step is not None:
        notes["failed_step"] = failed_step
    notes["hour"] = str(hour)
    notes["hours"] = str(hours)
    for name, value in seconds.items():
        notes[f"{name}{_SECONDS}"] = str(value)
    for name, value in wait_seconds.items():
        notes[f"{name}{_SECONDS}"] = str(value)
    if ingest_records is not None:
        missing_hours = sum(1 for r in ingest_records if r.get("status") == "missing")
        notes["missing_hours"] = str(missing_hours)
    notes["run_id"] = _token("run_id", run_id)
    notes["trigger"] = _token("trigger", trigger)

    row: dict[str, object] = {
        "run_at": run_at,
        "git_sha": git_sha,
        "variant": VARIANTS[path],
        "data_date": data_date,
        "total_seconds": sum(seconds.values()),
        "row_count": row_count,
        "notes": " ".join(f"{k}={v}" for k, v in notes.items()),
    }
    for q, value in queries.items():
        row[f"{q}{_SECONDS}"] = _number(f"{q} seconds", value)
    try:
        return validate_row(row)
    except RunsLogError as exc:
        raise PathCompareError(str(exc)) from None


def write_rows(path: Path, rows: Iterable[Mapping[str, str]]) -> None:
    """Artifact CSV (header + rows) that ``benchmarks/record_run.py`` merges into runs.csv."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.writer(fh, lineterminator="\n")
        writer.writerow(COLUMNS)
        for row in rows:
            writer.writerow(row[c] for c in COLUMNS)


# ---------------------------------------------------------------- parser


def parse_notes(text: str) -> dict[str, str]:
    """``key=value`` words of ``notes``; any other word or a repeated key is an error."""
    out: dict[str, str] = {}
    for word in text.split():
        match = _NOTE_WORD.fullmatch(word)
        if not match:
            raise PathCompareError(f"notes word is not key=value: {word!r}")
        key, value = match.groups()
        if key in out:
            raise PathCompareError(f"notes key {key} appears twice")
        out[key] = value
    return out


@dataclass(frozen=True)
class PathRun:
    """One parsed ``runs.csv`` row of a path variant."""

    variant: str
    path: str
    run_at: datetime
    data_date: str
    total_seconds: float
    status: str
    failed_step: str | None
    row_count: int | None
    queries: dict[str, float]
    steps: dict[str, float]
    waits: dict[str, float]
    notes: dict[str, str]

    @property
    def day(self) -> date:
        """UTC date of ``run_at`` (the day the run counts for in runs per day)."""
        return self.run_at.astimezone(UTC).date()


def _float(name: str, text: str) -> float:
    try:
        value = float(text)
    except ValueError:
        raise PathCompareError(f"{name} is not a number: {text!r}") from None
    if not math.isfinite(value) or value < 0:
        raise PathCompareError(f"{name} must be finite and >= 0: {text!r}")
    return value


def _parse_row(cells: Mapping[str, str]) -> PathRun:
    variant = cells["variant"]
    path = PATH_OF_VARIANT[variant]
    try:
        run_at = datetime.fromisoformat(cells["run_at"])
    except ValueError:
        raise PathCompareError(f"run_at is not ISO-8601: {cells['run_at']!r}") from None
    if run_at.tzinfo is None:
        raise PathCompareError(f"run_at has no time zone: {cells['run_at']!r}")
    total = _float("total_seconds", cells["total_seconds"])
    queries = {
        q: _float(f"{q}{_SECONDS}", cells[f"{q}{_SECONDS}"])
        for q in MART_BY_QUERY
        if cells[f"{q}{_SECONDS}"] != ""
    }
    row_count = None
    if cells["row_count"] != "":
        try:
            row_count = int(cells["row_count"])
        except ValueError:
            raise PathCompareError(f"row_count is not an int: {cells['row_count']!r}") from None
    notes = parse_notes(cells["notes"])
    status = notes.get("status")
    if status not in STATUSES:
        raise PathCompareError(f"notes status must be one of {STATUSES} (got {status!r})")
    failed_step = notes.get("failed_step")
    if status == "failed" and failed_step is None:
        raise PathCompareError("failed run without failed_step in notes")
    numbers = {
        key[: -len(_SECONDS)]: _float(key, value)
        for key, value in notes.items()
        if key.endswith(_SECONDS)
    }
    return PathRun(
        variant=variant,
        path=path,
        run_at=run_at,
        data_date=cells["data_date"],
        total_seconds=total,
        status=status,
        failed_step=failed_step,
        row_count=row_count,
        queries=queries,
        steps={s: numbers[s] for s in PATH_STEPS[path] if s in numbers},
        waits={w: numbers[w] for w in WAIT_KEYS[path] if w in numbers},
        notes=notes,
    )


def read_runs(path: str | os.PathLike[str]) -> list[PathRun]:
    """Rows of ``path`` (runs.csv format) whose variant is a path variant, in file order.

    Rows of other variants are skipped; a malformed path row raises ``PathCompareError`` with
    its line number, never dropped silently.
    """
    p = Path(path)
    if not p.is_file():
        raise PathCompareError(f"runs file not found: {p}")
    out: list[PathRun] = []
    with p.open(encoding="utf-8", newline="") as fh:
        reader = csv.reader(fh)
        header = next(reader, None)
        if header != list(COLUMNS):
            raise PathCompareError(f"header of {p} does not match runs_log.COLUMNS: {header!r}")
        for line_no, values in enumerate(reader, start=2):
            if not values:
                continue
            if len(values) != len(COLUMNS):
                raise PathCompareError(
                    f"{p} line {line_no}: expected {len(COLUMNS)} cells, got {len(values)}"
                )
            cells = dict(zip(COLUMNS, values, strict=True))
            if cells["variant"] not in PATH_OF_VARIANT:
                continue
            try:
                out.append(_parse_row(cells))
            except PathCompareError as exc:
                raise PathCompareError(f"{p} line {line_no}: {exc}") from None
    return out


# ---------------------------------------------------------------- summary


@dataclass(frozen=True)
class PathSummary:
    """Aggregates of one path; percentiles are nearest-rank (see ``percentile``)."""

    variant: str
    runs: int
    successes: int
    success_rate: float | None
    e2e_p50: float | None
    e2e_p95: float | None
    steps: dict[str, tuple[float | None, float | None]] = field(default_factory=dict)
    waits: dict[str, tuple[float | None, float | None]] = field(default_factory=dict)
    failed_steps: dict[str, int] = field(default_factory=dict)
    runs_per_day: dict[date, int] = field(default_factory=dict)
    min_runs_per_day: int | None = None
    max_runs_per_day: int | None = None


def _summarize_one(path: str, runs: Sequence[PathRun]) -> PathSummary:
    ok = [r for r in runs if r.status == "success"]
    steps = {
        s: _p50_p95([r.steps[s] for r in ok if s in r.steps])
        for s in PATH_STEPS[path]
        if any(s in r.steps for r in ok)
    }
    waits = {
        w: _p50_p95([r.waits[w] for r in runs if w in r.waits])
        for w in WAIT_KEYS[path]
        if any(w in r.waits for r in runs)
    }
    failed: dict[str, int] = {}
    for r in runs:
        if r.status == "failed" and r.failed_step:
            failed[r.failed_step] = failed.get(r.failed_step, 0) + 1
    per_day: dict[date, int] = {}
    if runs:
        first, last = min(r.day for r in runs), max(r.day for r in runs)
        day = first
        while day <= last:
            per_day[day] = 0
            day += timedelta(days=1)
        for r in runs:
            per_day[r.day] += 1
    e2e_p50, e2e_p95 = _p50_p95([r.total_seconds for r in ok])
    return PathSummary(
        variant=VARIANTS[path],
        runs=len(runs),
        successes=len(ok),
        success_rate=len(ok) / len(runs) if runs else None,
        e2e_p50=e2e_p50,
        e2e_p95=e2e_p95,
        steps=steps,
        waits=waits,
        failed_steps=dict(sorted(failed.items())),
        runs_per_day=per_day,
        min_runs_per_day=min(per_day.values()) if per_day else None,
        max_runs_per_day=max(per_day.values()) if per_day else None,
    )


def summarize(runs: Iterable[PathRun]) -> dict[str, PathSummary]:
    """Per-path aggregates, keyed by variant; both paths are always present.

    - success rate = successes / runs (``None`` without runs);
    - end-to-end p50/p95 (``total_seconds``) and step p50/p95: successful runs only;
    - wait p50/p95 (queue, setup, job queue/setup, warehouse start): every run that has it;
    - runs per day: UTC day of ``run_at``, every day from the first to the last run (days
      without a run count 0); the first and the last day may be partial.
    """
    runs = list(runs)
    return {VARIANTS[p]: _summarize_one(p, [r for r in runs if r.path == p]) for p in VARIANTS}


def _fmt(value: float | int | None) -> str:
    return "—" if value is None else str(value)


def _rate(s: PathSummary) -> str:
    return "—" if s.success_rate is None else f"{100 * s.success_rate:.1f}%"


def render_summary(summaries: Mapping[str, PathSummary]) -> str:
    """Markdown tables of ``summarize``; every number is a runs.csv value or a count/ratio."""
    lines = [
        "### Path comparison (incremental batch, end to end)",
        "",
        "From `runs.csv`. p50/p95 are nearest-rank percentiles (always a measured value); "
        "end-to-end and step times over successful runs, waits over every run.",
        "",
        "| variant | runs | successes | success rate | e2e p50 s | e2e p95 s | queue p50 s "
        "| queue p95 s | runs/day min | runs/day max | days | failed steps |",
        "|---|---|---|---|---|---|---|---|---|---|---|---|",
    ]
    for variant, s in summaries.items():
        queue = s.waits.get("queue", (None, None))
        failed = ", ".join(f"{k}: {v}" for k, v in s.failed_steps.items()) or "—"
        lines.append(
            f"| {variant} | {s.runs} | {s.successes} | {_rate(s)} | {_fmt(s.e2e_p50)} "
            f"| {_fmt(s.e2e_p95)} | {_fmt(queue[0])} | {_fmt(queue[1])} "
            f"| {_fmt(s.min_runs_per_day)} | {_fmt(s.max_runs_per_day)} "
            f"| {len(s.runs_per_day)} | {failed} |"
        )
    lines += ["", "| variant | step or wait | p50 s | p95 s |", "|---|---|---|---|"]
    for variant, s in summaries.items():
        for name, (p50, p95) in s.steps.items():
            lines.append(f"| {variant} | step `{name}` | {_fmt(p50)} | {_fmt(p95)} |")
        for name, (p50, p95) in s.waits.items():
            lines.append(f"| {variant} | wait `{name}` | {_fmt(p50)} | {_fmt(p95)} |")
    return "\n".join(lines) + "\n"


# ---------------------------------------------------------------- window


def decide_window(
    event: str,
    *,
    now: datetime,
    reference_date: str,
    date: str | None = "",
    hour: str | None = "",
    hours: str | None = "",
    lag_hours: int = LAG_HOURS,
) -> tuple[str, int, int]:
    """(date of the last hour, last hour, hour count) shared by both paths of one run.

    Inputs ``date``/``hour`` win (both needed); ``push`` uses the benchmark reference day,
    hour 23; otherwise (schedule, dispatch without date) floor(now UTC) - ``lag_hours``.
    """
    hours_text = (hours or "").strip()
    try:
        n_hours = int(hours_text) if hours_text else DEFAULT_HOURS
    except ValueError:
        raise PathCompareError(f"hours must be an integer (got {hours!r})") from None
    if not 1 <= n_hours <= MAX_WINDOW_HOURS:
        raise PathCompareError(f"hours must be 1..{MAX_WINDOW_HOURS} (got {n_hours})")

    day_text, hour_text = (date or "").strip(), (hour or "").strip()
    if day_text:
        if not hour_text:
            raise PathCompareError("input hour is needed when date is given")
        try:
            last_hour = int(hour_text)
        except ValueError:
            raise PathCompareError(f"hour must be an integer (got {hour!r})") from None
        day = day_text
    elif event == "push":
        day, last_hour = reference_date, PUSH_HOUR
    else:
        end = now.astimezone(UTC).replace(minute=0, second=0, microsecond=0)
        end -= timedelta(hours=lag_hours)
        day, last_hour = end.date().isoformat(), end.hour
    try:
        ingest.validate(day, last_hour)
    except ValueError as exc:
        raise PathCompareError(str(exc)) from None
    return day, last_hour, n_hours


# ---------------------------------------------------------------- runner waits (GitHub API)


def _gh_time(value: object, what: str) -> datetime:
    if not isinstance(value, str) or not value:
        raise PathCompareError(f"job has no {what}")
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def runner_waits(payload: Mapping[str, Any], job_name: str, *, now: datetime) -> dict[str, float]:
    """``queue`` = job started_at - created_at; ``setup`` = now - started_at (seconds)."""
    jobs = [j for j in payload.get("jobs") or [] if j.get("name") == job_name]
    if not jobs:
        raise PathCompareError(f"no job named {job_name!r} in the jobs API answer")
    job = jobs[-1]
    created = _gh_time(job.get("created_at"), "created_at")
    started = _gh_time(job.get("started_at"), "started_at")
    return {
        "queue": max(0.0, (started - created).total_seconds()),
        "setup": max(0.0, (now - started).total_seconds()),
    }


def _urllib_fetch(url: str, headers: Mapping[str, str]) -> bytes:
    request = urllib.request.Request(url, headers=dict(headers))
    with urllib.request.urlopen(request, timeout=30) as resp:
        return resp.read()


# ---------------------------------------------------------------- Databricks


def databricks_waits(row_csv: str | os.PathLike[str]) -> dict[str, float]:
    """``job_queue``/``job_setup`` of the serverless run from the D1 row (``queue_s``/``setup_s``).

    The file is the artifact row of ``python -m repopulse.databricks_job run``; absent -> {}.
    """
    p = Path(row_csv)
    if not p.is_file():
        return {}
    with p.open(encoding="utf-8", newline="") as fh:
        rows = list(csv.DictReader(fh))
    if not rows:
        return {}
    notes = parse_notes(rows[-1].get("notes") or "")
    out = {}
    for src, dst in (("queue_s", "job_queue"), ("setup_s", "job_setup")):
        if src in notes:
            out[dst] = _float(src, notes[src])
    return out


def _warehouse_state(ctx: ProbeContext, warehouse_id: str) -> str:
    info = _json(_call(ctx, "GET", f"/api/2.0/sql/warehouses/{warehouse_id}"))
    return str(info.get("state") or "")


def start_warehouse(ctx: ProbeContext, *, timeout: float, poll_interval: float) -> float:
    """Seconds until the SQL warehouse is RUNNING (0.0 when it already was); starts it if not."""
    warehouse_id, _ = resolve_warehouse(ctx)
    start = ctx.clock()
    state = _warehouse_state(ctx, warehouse_id)
    if state == "RUNNING":
        return 0.0
    if state != "STARTING":
        _call(ctx, "POST", f"/api/2.0/sql/warehouses/{warehouse_id}/start", json_body={})
    deadline = start + timeout
    while state != "RUNNING":
        if ctx.clock() >= deadline:
            raise PathCompareError(f"SQL warehouse not RUNNING after {timeout}s (state {state})")
        ctx.sleep(poll_interval)
        state = _warehouse_state(ctx, warehouse_id)
    return round(ctx.clock() - start, 3)


def _statement(ctx: ProbeContext, warehouse_id: str, sql: str, *, timeout: float) -> int:
    body = {
        "statement": sql,
        "warehouse_id": warehouse_id,
        "wait_timeout": "30s",
        "on_wait_timeout": "CONTINUE",
        "disposition": "INLINE",
        "format": "JSON_ARRAY",
    }
    stmt = _json(_call(ctx, "POST", "/api/2.0/sql/statements", json_body=body))
    statement_id = str(stmt.get("statement_id") or "")
    state = (stmt.get("status") or {}).get("state", "")
    deadline = ctx.clock() + timeout
    while state in _STATEMENT_WAITING:
        if ctx.clock() >= deadline:
            try:
                _call(ctx, "POST", f"/api/2.0/sql/statements/{statement_id}/cancel", json_body={})
            except (ApiError, OSError):  # best effort; the timeout is the error reported
                pass
            raise PathCompareError(f"statement timed out (last state {state})")
        ctx.sleep(ctx.poll_interval)
        stmt = _json(_call(ctx, "GET", f"/api/2.0/sql/statements/{statement_id}"))
        state = (stmt.get("status") or {}).get("state", "")
    if state != "SUCCEEDED":
        error = ((stmt.get("status") or {}).get("error") or {}).get("message", "")
        raise PathCompareError(f"statement ended {state}: {error}")
    rows = (stmt.get("manifest") or {}).get("total_row_count")
    if isinstance(rows, bool) or not isinstance(rows, int):
        raise PathCompareError("statement answered without manifest.total_row_count")
    return rows


def _check_schema(schema: str) -> str:
    if not _SCHEMA.fullmatch(schema or ""):
        raise PathCompareError(f"schema must be name or catalog.name (got {schema!r})")
    return schema


def queries_databricks(
    ctx: ProbeContext, schema: str, *, timeout: float
) -> tuple[dict[str, float], dict[str, int]]:
    """Q1-Q6 on the SQL warehouse: ``SELECT *`` of each mart via the Statement Execution API."""
    _check_schema(schema)
    warehouse_id, _ = resolve_warehouse(ctx)
    seconds, rows = {}, {}
    for q, mart in MART_BY_QUERY.items():
        start = ctx.clock()
        rows[q] = _statement(ctx, warehouse_id, f"SELECT * FROM {schema}.{mart}", timeout=timeout)
        seconds[q] = round(ctx.clock() - start, 3)
    return seconds, rows


def queries_duckdb(
    database: str,
    schema: str,
    *,
    connect: Callable[[str], Any] | None = None,
    clock: Callable[[], float] = time.perf_counter,
) -> tuple[dict[str, float], dict[str, int]]:
    """Q1-Q6 on DuckDB/MotherDuck: ``SELECT *`` of each mart, every row fetched."""
    _check_schema(schema)
    if connect is None:
        import duckdb

        connect = duckdb.connect
    con = connect(database)
    try:
        seconds, rows = {}, {}
        for q, mart in MART_BY_QUERY.items():
            start = clock()
            try:
                rows[q] = len(con.execute(f"SELECT * FROM {schema}.{mart}").fetchall())
            except Exception as exc:  # duckdb.Error and friends, without importing duckdb here
                raise PathCompareError(f"{q} ({mart}) failed: {exc}") from None
            seconds[q] = round(clock() - start, 6)
        return seconds, rows
    finally:
        con.close()


# ---------------------------------------------------------------- MotherDuck database of path A


def ensure_database(name: str, *, connect: Callable[[str], Any] | None = None) -> None:
    """``CREATE DATABASE IF NOT EXISTS`` on MotherDuck; never the production or smoke database."""
    if not isinstance(name, str) or not _DB_NAME.fullmatch(name):
        raise PathCompareError(f"database name must match {_DB_NAME.pattern} (got {name!r})")
    if name == PRODUCTION_DATABASE or name.endswith("_smoke"):
        raise PathCompareError(f"refusing database {name!r}: used by pipeline.yml or PR smoke")
    if connect is None:
        import duckdb

        connect = duckdb.connect
    con = connect("md:")
    try:
        con.execute(f"CREATE DATABASE IF NOT EXISTS {name}")
    finally:
        con.close()


# ---------------------------------------------------------------- CLI


def _parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="python -m repopulse.pathcompare",
        description="End-to-end comparison of path A (runner + MotherDuck) and path B "
        "(Databricks serverless + SQL warehouse); incremental batch.",
    )
    sub = p.add_subparsers(dest="command", required=True)

    w = sub.add_parser("window", help="print date=/hour=/hours= for $GITHUB_OUTPUT")
    w.add_argument("--event", required=True, help="GITHUB_EVENT_NAME")
    w.add_argument("--date", default="")
    w.add_argument("--hour", default="")
    w.add_argument("--hours", default="")
    w.add_argument("--reference", type=Path, default=Path("benchmarks/reference.toml"))
    w.add_argument("--now", default="", help="ISO-8601 with time zone (default: now UTC)")

    r = sub.add_parser("runner-waits", help="queue/setup of this job from the GitHub jobs API")
    r.add_argument("--job-name", required=True)
    r.add_argument("--out", type=Path, required=True)

    s = sub.add_parser("warehouse-start", help="start the SQL warehouse, record the wait")
    s.add_argument("--out", type=Path, required=True)
    s.add_argument("--timeout", type=float, default=900.0)
    s.add_argument("--poll-interval", type=float, default=10.0)

    q = sub.add_parser("queries", help="time Q1-Q6 (SELECT * of each mart) on the gold")
    q.add_argument("--engine", choices=("duckdb", "databricks"), required=True)
    q.add_argument("--database", default="", help="duckdb: file path or md:<database>")
    q.add_argument("--schema", required=True, help="schema (duckdb) or catalog.schema")
    q.add_argument("--out", type=Path, required=True)
    q.add_argument("--timeout", type=float, default=600.0, help="seconds per statement")
    q.add_argument("--poll-interval", type=float, default=2.0)

    m = sub.add_parser("md-ensure", help="create the MotherDuck database of path A if missing")
    m.add_argument("--database", required=True)

    row = sub.add_parser("row", help="write the runs.csv artifact row of one path")
    row.add_argument("--path", required=True, choices=sorted(VARIANTS))
    row.add_argument("--run-at", required=True, help="ISO-8601 with time zone")
    row.add_argument("--git-sha", required=True)
    row.add_argument("--date", required=True)
    row.add_argument("--hour", type=int, required=True)
    row.add_argument("--hours", type=int, required=True)
    row.add_argument("--outcome", action="append", default=[], help="step=outcome, repeatable")
    row.add_argument("--timings", type=Path, required=True, help="JSON of pipeline timed")
    row.add_argument("--waits", type=Path, action="append", default=[], help="waits JSON")
    row.add_argument("--queries", type=Path, default=None, help="JSON of the queries command")
    row.add_argument("--summary-json", type=Path, default=None, help="silver_inserted summary")
    row.add_argument("--ingest-summary", type=Path, default=None)
    row.add_argument("--databricks-row", type=Path, default=None, help="D1 artifact row (B)")
    row.add_argument("--run-id", required=True)
    row.add_argument("--trigger", required=True)
    row.add_argument("--out", type=Path, required=True)

    su = sub.add_parser("summary", help="markdown p50/p95 + success rate from runs.csv")
    su.add_argument("--runs", type=Path, default=Path("runs.csv"))
    su.add_argument("--out", type=Path, default=None)
    return p


def _load_json(path: Path | None, *, default: Any = None) -> Any:
    """JSON of ``path``; ``default`` when the file is absent (the step did not run)."""
    if path is None or not Path(path).is_file():
        return default
    try:
        return json.loads(Path(path).read_text(encoding="utf-8"))
    except ValueError as exc:
        raise PathCompareError(f"invalid JSON in {path}: {exc}") from None


def _pairs(items: Sequence[str]) -> dict[str, str]:
    out = {}
    for item in items:
        key, sep, value = item.partition("=")
        if not sep or not key:
            raise PathCompareError(f"--outcome must be step=outcome (got {item!r})")
        out[key] = value
    return out


def _row(args: argparse.Namespace) -> int:
    try:
        run_at = datetime.fromisoformat(args.run_at)
    except ValueError:
        raise PathCompareError(f"--run-at is not ISO-8601: {args.run_at!r}") from None
    timings = _load_json(args.timings, default={})
    waits: dict[str, Any] = {}
    for path in args.waits:
        loaded = _load_json(path, default={})
        if not isinstance(loaded, dict):
            raise PathCompareError(f"{path} must hold a JSON object")
        waits.update(loaded)
    if args.databricks_row is not None:
        waits.update(databricks_waits(args.databricks_row))
    queries = (_load_json(args.queries, default={}) or {}).get("seconds")
    summary = _load_json(args.summary_json, default={})
    inserted = summary.get("silver_inserted") if isinstance(summary, dict) else None
    records = _load_json(args.ingest_summary, default=None)
    if not isinstance(timings, dict):
        raise PathCompareError(f"{args.timings} must hold a JSON object")
    row = build_row(
        path=args.path,
        run_at=run_at,
        git_sha=args.git_sha,
        data_date=args.date,
        hour=args.hour,
        hours=args.hours,
        outcomes=_pairs(args.outcome),
        timings=timings,
        waits=waits,
        queries=queries,
        row_count=inserted
        if isinstance(inserted, int) and not isinstance(inserted, bool)
        else None,
        run_id=args.run_id,
        trigger=args.trigger,
        ingest_records=records if isinstance(records, list) else None,
    )
    write_rows(args.out, [row])
    print(f"wrote {args.out} ({VARIANTS[args.path]}, {parse_notes(row['notes'])['status']})")
    return 0


def _write_json(path: Path, data: Any) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _databricks_ctx(env: Mapping[str, str], deps: Mapping[str, Any]) -> ProbeContext:
    secrets = Secrets.from_env(env)
    missing = [n for n in ("DATABRICKS_HOST", "DATABRICKS_TOKEN") if not (env.get(n) or "").strip()]
    if missing:
        raise PathCompareError(f"missing environment variable(s): {', '.join(missing)}")
    ctx = ProbeContext(secrets, deps.get("transport") or urllib_transport)
    # Server messages may name the workspace by its first host label (dbc-...): mask it too.
    ctx.redact.add(urlsplit(secrets.host).netloc.split(".")[0])
    if deps.get("clock") is not None:
        ctx.clock = deps["clock"]
    if deps.get("sleep") is not None:
        ctx.sleep = deps["sleep"]
    return ctx


def _runner_waits_cli(args: argparse.Namespace, env: Mapping[str, str], deps: Mapping) -> int:
    """Best effort: a failed API call is a warning and empty waits, never a failed run."""
    token = env.get("GH_TOKEN") or env.get("GITHUB_TOKEN") or ""
    redact = Redactor([token])
    try:
        api = env.get("GITHUB_API_URL") or "https://api.github.com"
        url = (
            f"{api}/repos/{env['GITHUB_REPOSITORY']}/actions/runs/{env['GITHUB_RUN_ID']}"
            f"/attempts/{env.get('GITHUB_RUN_ATTEMPT') or '1'}/jobs?per_page=100"
        )
        headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
            "User-Agent": "repopulse-pathcompare",
        }
        payload = json.loads((deps.get("fetch") or _urllib_fetch)(url, headers))
        now = (deps.get("now") or (lambda: datetime.now(UTC)))()
        waits = runner_waits(payload, args.job_name, now=now)
    except Exception as exc:  # network, missing env, unexpected answer
        print(f"::warning::runner waits not measured: {redact(f'{type(exc).__name__}: {exc}')}")
        waits = {}
    _write_json(args.out, waits)
    print(f"runner waits: {json.dumps(waits, sort_keys=True)}")
    return 0


def _run(args: argparse.Namespace, env: Mapping[str, str], deps: Mapping[str, Any]) -> int:
    if args.command == "window":
        reference = tomllib.loads(args.reference.read_text(encoding="utf-8"))["data_date"]
        now = datetime.fromisoformat(args.now) if args.now else datetime.now(UTC)
        day, hour, hours = decide_window(
            args.event,
            now=now,
            reference_date=reference,
            date=args.date,
            hour=args.hour,
            hours=args.hours,
        )
        print(f"date={day}")
        print(f"hour={hour}")
        print(f"hours={hours}")
        return 0
    if args.command == "warehouse-start":
        ctx = _databricks_ctx(env, deps)
        try:
            waited = start_warehouse(ctx, timeout=args.timeout, poll_interval=args.poll_interval)
        except (ApiError, ValueError, OSError) as exc:
            raise PathCompareError(ctx.redact(exc)) from None
        _write_json(args.out, {"warehouse_start": waited})
        print(f"SQL warehouse running; waited {waited}s")
        return 0
    if args.command == "queries":
        if args.engine == "duckdb":
            if not args.database:
                raise PathCompareError("--database is needed with --engine duckdb")
            _check_schema(args.schema)
            seconds, rows = queries_duckdb(args.database, args.schema, connect=deps.get("connect"))
        else:
            _check_schema(args.schema)
            ctx = _databricks_ctx(env, deps)
            ctx.poll_interval = args.poll_interval
            try:
                seconds, rows = queries_databricks(ctx, args.schema, timeout=args.timeout)
            except (ApiError, ValueError, OSError) as exc:
                raise PathCompareError(ctx.redact(exc)) from None
        _write_json(args.out, {"seconds": seconds, "rows": rows})
        print(f"Q1-Q6 seconds: {json.dumps(seconds, sort_keys=True)}")
        return 0
    if args.command == "md-ensure":
        ensure_database(args.database, connect=deps.get("connect"))
        print(f"MotherDuck database {args.database} ready")
        return 0
    if args.command == "row":
        return _row(args)
    if args.command == "summary":
        text = render_summary(summarize(read_runs(args.runs)))
        if args.out is not None:
            args.out.parent.mkdir(parents=True, exist_ok=True)
            args.out.write_text(text, encoding="utf-8")
        print(text, end="")
        return 0
    raise AssertionError(args.command)  # pragma: no cover


def main(
    argv: Sequence[str] | None = None,
    *,
    env: Mapping[str, str] | None = None,
    **deps: Any,
) -> int:
    """CLI entry; ``deps`` (tests): transport, clock, sleep, fetch, now, connect."""
    env = os.environ if env is None else env
    args = _parser().parse_args(list(sys.argv[1:] if argv is None else argv))
    if args.command == "runner-waits":
        return _runner_waits_cli(args, env, deps)
    redact = Secrets.from_env(env).redactor()
    try:
        return _run(args, env, deps)
    except (ValueError, OSError, KeyError) as exc:  # PathCompareError, RunsLogError, files
        print(f"error: {redact(exc)}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
