"""Helpers of the scheduled pipeline workflow (``.github/workflows/pipeline.yml``).

The workflow is an hourly incremental batch (not streaming): ingest -> Spark bronze/silver ->
silver export -> ``dbt build`` -> one ``runs.csv`` row. This module holds the glue that is easier
to test in Python than in shell:

- ``window``: last (date, hour) and hour count of the ingest summary, for the Spark job.
- ``build_row``: the ``runs.csv`` row of one run (schema of ``repopulse.runs_log``).
- ``timed``: run one step as a subprocess and record its wall time in a JSON file.
- ``counts``: row counts of staging + gold (an incremental re-run must not change them).
- ``md-reset``: recreate the MotherDuck smoke database used by pull requests, never the real one.

Usage: ``python -m repopulse.pipeline {window,row,timed,counts,md-reset} --help``.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import re
import subprocess
import sys
import time
from collections import Counter
from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, datetime, timedelta
from pathlib import Path

from repopulse import ingest
from repopulse.runs_log import COLUMNS, validate_row

VARIANT = "pipeline-incremental"
REQUIRED_STEPS: tuple[str, ...] = ("ingest", "spark", "dbt")
STEP_ORDER: tuple[str, ...] = ("ingest", "spark", "export", "dbt")
STATUSES: tuple[str, ...] = ("ingested", "skipped", "missing")
COUNT_TABLES: tuple[str, ...] = (
    "main_staging.stg_gharchive_events",
    "main_gold.fct_events",
    "main_gold.dim_user",
    "main_gold.dim_repo",
    "main_gold.mart_daily_active_contributors",
    "main_gold.mart_cohort_retention",
    "main_gold.mart_contribution_funnel",
    "main_gold.mart_responsiveness",
    "main_gold.mart_repo_concentration",
    "main_gold.mart_repo_health_score",
)

_NOTE_KEY = re.compile(r"[a-z][a-z0-9_]*")
_NOTE_VALUE = re.compile(r"[^\s=]+")
_SMOKE_DB = re.compile(r"[a-z][a-z0-9_]*_smoke")


class PipelineError(ValueError):
    """Bad input to a pipeline helper (summary, timings, note, database name)."""


# ---------------------------------------------------------------- window


def _hour_of(record: object) -> datetime:
    if not isinstance(record, Mapping):
        raise PipelineError(f"ingest record must be an object, got {record!r}")
    if "data_date" not in record:
        raise PipelineError(f"ingest record without data_date: {record!r}")
    if "hour" not in record:
        raise PipelineError(f"ingest record without hour: {record!r}")
    day, hour = record["data_date"], record["hour"]
    try:
        ingest.validate(day, hour)
    except ValueError as exc:
        raise PipelineError(str(exc)) from None
    return datetime.fromisoformat(day).replace(hour=hour, tzinfo=UTC)


def window(records: Sequence[Mapping]) -> tuple[str, int, int]:
    """(date of the last hour, last hour, number of hours) of consecutive ingest records."""
    if not records:
        raise PipelineError("ingest summary is empty")
    hours = sorted(_hour_of(r) for r in records)
    for prev, nxt in zip(hours, hours[1:], strict=False):
        if nxt - prev != timedelta(hours=1):
            raise PipelineError(f"ingest hours are not consecutive: {prev:%Y-%m-%dT%H} -> {nxt:%H}")
    last = hours[-1]
    return last.date().isoformat(), last.hour, len(hours)


# ---------------------------------------------------------------- runs.csv row


def _token(field: str, value: object) -> str:
    if not isinstance(value, str) or not _NOTE_VALUE.fullmatch(value):
        raise PipelineError(f"{field} must be one token without spaces or '=' (got {value!r})")
    return value


def _seconds(name: str, value: object) -> float:
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise PipelineError(f"timing of step {name} must be a number (got {value!r})")
    if not math.isfinite(value) or value < 0:
        raise PipelineError(f"timing of step {name} must be finite and >= 0 (got {value!r})")
    return float(value)


def build_row(
    *,
    run_at: datetime,
    git_sha: str,
    ingest_records: Sequence[Mapping],
    spark_summary: Mapping,
    timings: Mapping[str, object],
    run_id: str,
    trigger: str,
    variant: str = VARIANT,
    extra_notes: Mapping[str, str] | None = None,
) -> dict[str, str]:
    """The ``runs.csv`` row of one scheduled run, as validated CSV-ready strings.

    total_seconds = sum of the step timings; row_count = rows the Spark job inserted into this
    run's silver. Per-query columns stay empty: the pipeline does not time Q1-Q6.
    """
    data_date, last_hour, n_hours = window(ingest_records)
    for name in REQUIRED_STEPS:
        if name not in timings:
            raise PipelineError(f"timing of step {name} is missing")
    seconds = {name: _seconds(name, value) for name, value in timings.items()}
    inserted = spark_summary.get("silver_inserted") if isinstance(spark_summary, Mapping) else None
    if isinstance(inserted, bool) or not isinstance(inserted, int) or inserted < 0:
        raise PipelineError(f"spark summary silver_inserted must be an int >= 0 (got {inserted!r})")

    statuses = Counter(str(r.get("status")) for r in ingest_records)
    notes: dict[str, str] = {
        "last_hour": f"{data_date}T{last_hour:02d}",
        "hours": str(n_hours),
        **{status: str(statuses.get(status, 0)) for status in STATUSES},
    }
    ordered = [s for s in STEP_ORDER if s in seconds] + sorted(set(seconds) - set(STEP_ORDER))
    for name in ordered:
        notes[f"{_token('step name', name)}_s"] = str(seconds[name])
    notes["run_id"] = _token("run_id", run_id)
    notes["trigger"] = _token("trigger", trigger)
    for key, value in (extra_notes or {}).items():
        if not isinstance(key, str) or not _NOTE_KEY.fullmatch(key):
            raise PipelineError(f"note key must match {_NOTE_KEY.pattern} (got {key!r})")
        if key in notes:
            raise PipelineError(f"note {key} is already set by the pipeline")
        notes[key] = _token(f"note {key}", value)

    row = {
        "run_at": run_at,
        "git_sha": git_sha,
        "variant": variant,
        "data_date": data_date,
        "total_seconds": sum(seconds.values()),
        "row_count": inserted,
        "notes": " ".join(f"{k}={v}" for k, v in notes.items()),
    }
    return validate_row(row)


def write_row(path: Path, row: Mapping[str, str]) -> None:
    """Artifact CSV (header + one row) that ``benchmarks/record_run.py`` merges into runs.csv."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.writer(fh, lineterminator="\n")
        writer.writerow(COLUMNS)
        writer.writerow(row[c] for c in COLUMNS)


def _load_json(path: Path) -> object:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        raise PipelineError(f"missing file {path}") from None
    except ValueError as exc:
        raise PipelineError(f"invalid JSON in {path}: {exc}") from None


# ---------------------------------------------------------------- timed


def timed(name: str, timings_path: Path, command: Sequence[str]) -> int:
    """Run ``command``, store its wall time under ``name`` in ``timings_path``; return its code."""
    start = time.perf_counter()
    code = subprocess.run(list(command), check=False).returncode
    elapsed = round(time.perf_counter() - start, 3)
    data: dict = {}
    if timings_path.is_file():
        loaded = _load_json(timings_path)
        if isinstance(loaded, dict):
            data = loaded
    data[name] = float(elapsed)
    timings_path.parent.mkdir(parents=True, exist_ok=True)
    timings_path.write_text(json.dumps(data, indent=2) + "\n", encoding="utf-8")
    return code


# ---------------------------------------------------------------- counts


def table_counts(database: str) -> dict[str, int]:
    """Row count of every table in ``COUNT_TABLES`` (DuckDB file path or ``md:<db>``)."""
    import duckdb

    con = duckdb.connect(database)
    try:
        out = {}
        for table in COUNT_TABLES:
            try:
                out[table] = int(con.execute(f"SELECT count(*) FROM {table}").fetchone()[0])
            except duckdb.Error as exc:
                raise PipelineError(f"cannot count {table}: {exc}") from None
        return out
    finally:
        con.close()


# ---------------------------------------------------------------- MotherDuck smoke database


def reset_smoke_database(name: str, *, connect: Callable[[str], object] | None = None) -> None:
    """Drop and recreate the MotherDuck database ``name``; only ``*_smoke`` names are allowed."""
    if not isinstance(name, str) or not _SMOKE_DB.fullmatch(name):
        raise PipelineError(
            f"refusing to reset database {name!r}: only names matching {_SMOKE_DB.pattern}"
        )
    if connect is None:
        import duckdb

        connect = duckdb.connect
    con = connect("md:")
    try:
        con.execute(f"DROP DATABASE IF EXISTS {name}")
        con.execute(f"CREATE DATABASE {name}")
    finally:
        con.close()


# ---------------------------------------------------------------- CLI


def _parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="python -m repopulse.pipeline",
        description="Helpers of the scheduled pipeline (incremental batch).",
    )
    sub = p.add_subparsers(dest="command", required=True)

    w = sub.add_parser("window", help="print DATA_DATE/DATA_HOUR/DATA_HOURS for $GITHUB_ENV")
    w.add_argument("--ingest-summary", type=Path, required=True)

    r = sub.add_parser("row", help="write the runs.csv artifact row of this run")
    r.add_argument("--run-at", required=True, help="ISO-8601 with time zone")
    r.add_argument("--git-sha", required=True)
    r.add_argument("--ingest-summary", type=Path, required=True)
    r.add_argument("--spark-summary", type=Path, required=True)
    r.add_argument("--timings", type=Path, required=True)
    r.add_argument("--run-id", required=True)
    r.add_argument("--trigger", required=True)
    r.add_argument("--variant", default=VARIANT)
    r.add_argument("--note", action="append", default=[], help="extra key=value, repeatable")
    r.add_argument("--out", type=Path, required=True)

    t = sub.add_parser("timed", help="run a command and record its wall time")
    t.add_argument("--name", required=True)
    t.add_argument("--timings", type=Path, required=True)
    t.add_argument("cmd", nargs=argparse.REMAINDER, help="-- command [args...]")

    c = sub.add_parser("counts", help="row counts of staging + gold as JSON")
    c.add_argument("--database", required=True, help="DuckDB file or md:<database>")
    c.add_argument("--out", type=Path, required=True)

    m = sub.add_parser("md-reset", help="drop and recreate a MotherDuck *_smoke database")
    m.add_argument("--database", required=True)
    return p


def _notes(items: Sequence[str]) -> dict[str, str]:
    out = {}
    for item in items:
        key, sep, value = item.partition("=")
        if not sep:
            raise PipelineError(f"note must be key=value (got {item!r})")
        out[key] = value
    return out


def _run(args: argparse.Namespace) -> int:
    if args.command == "window":
        day, hour, count = window(_load_json(args.ingest_summary))
        print(f"DATA_DATE={day}")
        print(f"DATA_HOUR={hour}")
        print(f"DATA_HOURS={count}")
        return 0
    if args.command == "row":
        try:
            run_at = datetime.fromisoformat(args.run_at)
        except ValueError:
            raise PipelineError(f"--run-at is not ISO-8601: {args.run_at!r}") from None
        row = build_row(
            run_at=run_at,
            git_sha=args.git_sha,
            ingest_records=_load_json(args.ingest_summary),
            spark_summary=_load_json(args.spark_summary),
            timings=_load_json(args.timings),
            run_id=args.run_id,
            trigger=args.trigger,
            variant=args.variant,
            extra_notes=_notes(args.note),
        )
        write_row(args.out, row)
        print(f"wrote {args.out}")
        return 0
    if args.command == "counts":
        counts = table_counts(args.database)
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(json.dumps(counts, indent=2) + "\n", encoding="utf-8")
        print(json.dumps(counts, sort_keys=True))
        return 0
    if args.command == "md-reset":
        reset_smoke_database(args.database)
        print(f"reset MotherDuck database {args.database}")
        return 0
    raise AssertionError(args.command)  # pragma: no cover


def main(argv: Sequence[str] | None = None) -> int:
    parser = _parser()
    args = parser.parse_args(argv)
    if args.command == "timed":
        cmd = list(args.cmd)
        if cmd and cmd[0] == "--":
            cmd = cmd[1:]
        if not cmd:
            parser.error("timed needs a command after --")
        return timed(args.name, args.timings, cmd)
    try:
        return _run(args)
    except ValueError as exc:  # PipelineError and RunsLogError
        print(f"error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
