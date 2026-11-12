"""Spec of the Airflow DAG ``repopulse_pipeline`` and the helpers of its backfill benchmark.

The DAG (``dags/repopulse_pipeline.py``) is the Airflow alternative to the hourly cron of
``.github/workflows/pipeline.yml``: the same incremental batch (not streaming) chain
``ingest -> spark_bronze_silver -> export_silver -> dbt_build``, with per-task retries, an SLA on
the whole DAG run (an Airflow 3 Deadline Alert) and backfill over N days. Everything that can be
tested without Airflow lives here, stdlib only (this module never imports ``airflow``):

- ``settings_from_env`` / ``dag_spec``: tasks, bash commands, retries, timeouts, SLA.
- ``topological_order``: rejects cycles, unknown upstreams and duplicate task ids.
- ``backfill_window``: the logical dates to backfill for N data days.
- ``parse_runs`` / ``progress`` / ``summarize`` / ``wait``: poll ``airflow dags list-runs -o json``
  until every backfill run is done, then summarise (wall clock, success/failed, SLA misses).
- ``build_row``: the ``runs.csv`` row (``variant=airflow-backfill-<N>d``).
- ``record_sla_miss``: Deadline Alert callback, appends one JSON line to a file.

The DAG run of logical date D processes the data of D - 1 (``DATA_DATE_TEMPLATE``), i.e. the day
that is complete when the run starts; each run processes only ``hours_per_day`` hours ending at
``hour`` of that day (runner disk is 14 GB).

Usage: ``python -m repopulse.dagspec {window,wait,row,check-ingest} --help``.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shlex
import subprocess
import sys
import time
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

from repopulse import ingest
from repopulse.runs_log import validate_row

DAG_ID = "repopulse_pipeline"
SCHEDULE = "@daily"
START_DATE = datetime(2025, 1, 1, tzinfo=UTC)
MAX_ACTIVE_RUNS = 1
AIRFLOW_VERSION = "3.4.0"
ENGINE = f"airflow-{AIRFLOW_VERSION}"
TASK_ORDER: tuple[str, ...] = ("ingest", "spark_bronze_silver", "export_silver", "dbt_build")
DATA_DATE_TEMPLATE = "{{ macros.ds_add(ds, -1) }}"
MAX_BACKFILL_DAYS = 7
REPO_ROOT = Path(__file__).resolve().parents[1]

DEFAULT_HOUR = 12
DEFAULT_HOURS_PER_DAY = 1
DEFAULT_SLA_MINUTES = 30

ENV_PYTHON = "REPOPULSE_PYTHON"
ENV_HOME = "REPOPULSE_HOME"
ENV_HOUR = "REPOPULSE_AIRFLOW_HOUR"
ENV_HOURS_PER_DAY = "REPOPULSE_AIRFLOW_HOURS_PER_DAY"
ENV_SLA_MINUTES = "REPOPULSE_AIRFLOW_SLA_MINUTES"
ENV_END_DATE = "REPOPULSE_AIRFLOW_END_DATE"
ENV_SLA_MISS_FILE = "REPOPULSE_SLA_MISS_FILE"

TERMINAL_STATES = frozenset({"success", "failed"})

_DATE = re.compile(r"\d{4}-\d{2}-\d{2}")
_TOKEN = re.compile(r"[^\s=]+")


class DagSpecError(ValueError):
    """Bad DAG spec, setting, window or ``airflow`` CLI output."""


# ---------------------------------------------------------------- settings and spec


@dataclass(frozen=True)
class Settings:
    python: str
    home: Path
    hour: int
    hours_per_day: int
    sla_minutes: int
    end_date: date | None
    sla_miss_file: Path


@dataclass(frozen=True)
class TaskSpec:
    task_id: str
    command: str
    upstream: tuple[str, ...]
    retries: int
    retry_delay_seconds: int
    exponential_backoff: bool
    timeout_minutes: int


@dataclass(frozen=True)
class DagSpec:
    dag_id: str
    schedule: str
    start_date: datetime
    end_date: datetime | None
    catchup: bool
    max_active_runs: int
    sla_minutes: int
    home: Path
    sla_miss_file: Path
    tasks: tuple[TaskSpec, ...]


def _parse_date(name: str, value: str) -> date:
    if _DATE.fullmatch(value):
        try:
            return date.fromisoformat(value)
        except ValueError:
            pass
    raise DagSpecError(f"{name} must be a valid YYYY-MM-DD date (got {value!r})")


def _int_env(env: Mapping[str, str], name: str, default: int, low: int, high: int | None) -> int:
    raw = (env.get(name) or "").strip()
    if not raw:
        return default
    try:
        value = int(raw)
    except ValueError:
        raise DagSpecError(f"{name} must be an integer (got {raw!r})") from None
    if value < low or (high is not None and value > high):
        bound = f"{low}..{high}" if high is not None else f">= {low}"
        raise DagSpecError(f"{name} must be {bound} (got {value})")
    return value


def settings_from_env(env: Mapping[str, str]) -> Settings:
    """Settings of the DAG from environment variables (empty value = default)."""
    home = Path((env.get(ENV_HOME) or "").strip() or REPO_ROOT)
    try:
        sla = _int_env(env, ENV_SLA_MINUTES, DEFAULT_SLA_MINUTES, 1, None)
    except DagSpecError as exc:
        raise DagSpecError(f"SLA: {exc}") from None
    end_raw = (env.get(ENV_END_DATE) or "").strip()
    sla_file = (env.get(ENV_SLA_MISS_FILE) or "").strip()
    return Settings(
        python=(env.get(ENV_PYTHON) or "").strip() or "python",
        home=home,
        hour=_int_env(env, ENV_HOUR, DEFAULT_HOUR, 0, 23),
        hours_per_day=_int_env(env, ENV_HOURS_PER_DAY, DEFAULT_HOURS_PER_DAY, 1, ingest.MAX_HOURS),
        sla_minutes=sla,
        end_date=_parse_date(ENV_END_DATE, end_raw) if end_raw else None,
        sla_miss_file=Path(sla_file) if sla_file else home / "out" / "airflow" / "sla-miss.jsonl",
    )


def build_tasks(settings: Settings) -> tuple[TaskSpec, ...]:
    """The four tasks of ``pipeline.yml`` as bash commands run from the repo root."""
    py = shlex.quote(settings.python)
    day = DATA_DATE_TEMPLATE
    out = f"out/airflow/{day}"
    hours = f"--date {day} --hour {settings.hour} --hours {settings.hours_per_day}"
    ingest_cmd = (
        f"{py} -m repopulse.ingest {hours} --landing-dir data/landing/gharchive"
        f" --work-dir data/raw/gharchive-ingest --summary {out}/ingest-summary.json"
        f" && {py} -m repopulse.dagspec check-ingest --summary {out}/ingest-summary.json"
    )
    spark_cmd = (
        f"{py} -m repopulse.spark.job --source gharchive {hours}"
        f" --landing-dir data/landing/gharchive --lake-dir data/lake"
        f" --summary {out}/spark-summary.json"
    )
    export_cmd = (
        f"{py} -m repopulse.spark.export --lake-dir data/lake --out data/silver-export"
        f" --summary {out}/export-summary.json"
    )
    dbt_vars = shlex.quote('{silver_gharchive_parquet: "data/silver-export/*.parquet"}')
    dbt_cmd = (
        f"{py} -m dbt.cli.main build --project-dir transform --profiles-dir transform"
        f" --target ci --vars {dbt_vars} --exclude fixture_silver_gharchive_events"
    )
    return (
        TaskSpec("ingest", ingest_cmd, (), 3, 120, True, 30),
        TaskSpec("spark_bronze_silver", spark_cmd, ("ingest",), 1, 60, False, 45),
        TaskSpec("export_silver", export_cmd, ("spark_bronze_silver",), 1, 60, False, 15),
        TaskSpec("dbt_build", dbt_cmd, ("export_silver",), 1, 60, False, 30),
    )


def dag_spec(settings: Settings) -> DagSpec:
    tasks = build_tasks(settings)
    topological_order(tasks)
    end = None
    if settings.end_date is not None:
        # Noon after the last logical date: backfill runs (midnight) fit, the next scheduled one
        # does not, so an unpaused DAG creates no run of its own during the benchmark.
        end = datetime.combine(settings.end_date, datetime.min.time(), UTC) + timedelta(hours=12)
    return DagSpec(
        dag_id=DAG_ID,
        schedule=SCHEDULE,
        start_date=START_DATE,
        end_date=end,
        catchup=False,
        max_active_runs=MAX_ACTIVE_RUNS,
        sla_minutes=settings.sla_minutes,
        home=settings.home,
        sla_miss_file=settings.sla_miss_file,
        tasks=tasks,
    )


def topological_order(tasks: Iterable[TaskSpec]) -> list[str]:
    """Task ids in dependency order (ties keep the input order sorted by id)."""
    tasks = list(tasks)
    ids: list[str] = []
    for t in tasks:
        if t.task_id in ids:
            raise DagSpecError(f"duplicate task id {t.task_id!r}")
        ids.append(t.task_id)
    upstream = {t.task_id: set(t.upstream) for t in tasks}
    for t in tasks:
        unknown = sorted(set(t.upstream) - set(ids))
        if unknown:
            raise DagSpecError(f"task {t.task_id!r} has unknown upstream {', '.join(unknown)}")
    order: list[str] = []
    remaining = dict(upstream)
    while remaining:
        ready = sorted(tid for tid, ups in remaining.items() if ups <= set(order))
        if not ready:
            raise DagSpecError(f"dependency cycle among tasks {', '.join(sorted(remaining))}")
        order.append(ready[0])
        del remaining[ready[0]]
    return order


# ---------------------------------------------------------------- backfill window


@dataclass(frozen=True)
class Window:
    first_data_date: date
    last_data_date: date
    from_date: date
    to_date: date
    days: int

    @property
    def data_dates(self) -> tuple[date, ...]:
        return tuple(self.first_data_date + timedelta(days=k) for k in range(self.days))


def backfill_window(last_data_date: date, days: int) -> Window:
    """Logical dates (inclusive) whose runs process the ``days`` data days ending at the last."""
    if isinstance(days, bool) or not isinstance(days, int) or not 1 <= days <= MAX_BACKFILL_DAYS:
        raise DagSpecError(f"days must be 1..{MAX_BACKFILL_DAYS} (got {days!r})")
    first = last_data_date - timedelta(days=days - 1)
    return Window(
        first_data_date=first,
        last_data_date=last_data_date,
        from_date=first + timedelta(days=1),
        to_date=last_data_date + timedelta(days=1),
        days=days,
    )


# ---------------------------------------------------------------- list-runs output


@dataclass(frozen=True)
class DagRun:
    run_id: str
    state: str
    logical_date: datetime | None
    start_date: datetime | None
    end_date: datetime | None
    run_type: str | None = None

    @property
    def duration_seconds(self) -> float | None:
        if self.start_date is None or self.end_date is None:
            return None
        return (self.end_date - self.start_date).total_seconds()


def _dt(field: str, value: object) -> datetime | None:
    if value is None or value == "":
        return None
    if not isinstance(value, str):
        raise DagSpecError(f"{field} must be an ISO-8601 string (got {value!r})")
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        raise DagSpecError(f"{field} is not ISO-8601: {value!r}") from None
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=UTC)


def parse_runs(text: str) -> list[DagRun]:
    """Runs of ``airflow dags list-runs -o json``; log noise around the JSON array is ignored."""
    stripped = text.strip()
    if not stripped:
        return []
    try:
        data = json.loads(stripped)
    except ValueError:
        start, end = stripped.find("["), stripped.rfind("]")
        if start < 0:
            if "no data" in stripped.lower():
                return []
            raise DagSpecError(f"list-runs output is not JSON: {stripped[:200]!r}") from None
        try:
            data = json.loads(stripped[start : end + 1] if end > start else stripped[start:])
        except ValueError as exc:
            raise DagSpecError(f"invalid JSON in list-runs output: {exc}") from None
    if not isinstance(data, list):
        raise DagSpecError(f"list-runs output must be a JSON list (got {type(data).__name__})")
    runs = []
    for item in data:
        if not isinstance(item, Mapping):
            raise DagSpecError(f"list-runs item must be an object: {item!r}")
        run_id, state = item.get("run_id"), item.get("state")
        if not isinstance(run_id, str) or not run_id:
            raise DagSpecError(f"list-runs item without run_id: {dict(item)!r}")
        if not isinstance(state, str) or not state:
            raise DagSpecError(f"list-runs item without state: {dict(item)!r}")
        run_type = item.get("run_type")
        runs.append(
            DagRun(
                run_id=run_id,
                state=state.lower(),
                logical_date=_dt("logical_date", item.get("logical_date")),
                start_date=_dt("start_date", item.get("start_date")),
                end_date=_dt("end_date", item.get("end_date")),
                run_type=str(run_type) if run_type else None,
            )
        )
    return runs


def _is_backfill(run: DagRun) -> bool:
    if run.run_type:
        return "backfill" in run.run_type.lower()
    return run.run_id.startswith("backfill__")


def backfill_runs(runs: Iterable[DagRun], window: Window) -> list[DagRun]:
    """One backfill run per logical date of ``window`` (the latest started if re-run)."""
    oldest = datetime.min.replace(tzinfo=UTC)
    best: dict[date, tuple[tuple[datetime, int], DagRun]] = {}
    for i, run in enumerate(runs):
        if not _is_backfill(run) or run.logical_date is None:
            continue
        day = run.logical_date.astimezone(UTC).date()
        if not window.from_date <= day <= window.to_date:
            continue
        key = (run.start_date or oldest, i)
        if day not in best or key > best[day][0]:
            best[day] = (key, run)
    return [best[d][1] for d in sorted(best)]


@dataclass(frozen=True)
class Progress:
    done: bool
    success: int
    failed: int
    pending: int


def progress(runs: Iterable[DagRun], window: Window) -> Progress:
    picked = backfill_runs(runs, window)
    success = sum(1 for r in picked if r.state == "success")
    failed = sum(1 for r in picked if r.state == "failed")
    pending = window.days - success - failed
    return Progress(done=pending == 0, success=success, failed=failed, pending=pending)


def _iso(value: datetime | None) -> str | None:
    return value.isoformat() if value is not None else None


def summarize(
    runs: Iterable[DagRun], window: Window, *, started_at: datetime, sla_minutes: int
) -> dict:
    """JSON-ready summary of the backfill; ``status`` is success, failed or running."""
    runs = list(runs)
    p = progress(runs, window)
    picked = backfill_runs(runs, window)
    sla_seconds = sla_minutes * 60
    items = []
    for r in picked:
        duration = r.duration_seconds
        items.append(
            {
                "run_id": r.run_id,
                "logical_date": _iso(r.logical_date),
                "state": r.state,
                "start_date": _iso(r.start_date),
                "end_date": _iso(r.end_date),
                "duration_seconds": duration,
                "sla_missed": duration is not None and duration > sla_seconds,
            }
        )
    finished_at = None
    wall = None
    ends = [r.end_date for r in picked]
    if p.done and ends and all(e is not None for e in ends):
        finished_at = max(ends)
        wall = round((finished_at - started_at).total_seconds(), 3)
    if not p.done:
        status = "running"
    elif p.failed:
        status = "failed"
    else:
        status = "success"
    return {
        "status": status,
        "dag_id": DAG_ID,
        "days": window.days,
        "from_date": window.from_date.isoformat(),
        "to_date": window.to_date.isoformat(),
        "first_data_date": window.first_data_date.isoformat(),
        "last_data_date": window.last_data_date.isoformat(),
        "started_at": started_at.isoformat(),
        "finished_at": _iso(finished_at),
        "wall_seconds": float(wall) if wall is not None else None,
        "runs_success": p.success,
        "runs_failed": p.failed,
        "runs_pending": p.pending,
        "sla_minutes": sla_minutes,
        "sla_missed": sum(1 for i in items if i["sla_missed"]),
        "runs": items,
    }


def wait(
    list_runs: Callable[[], str],
    window: Window,
    *,
    started_at: datetime,
    sla_minutes: int,
    timeout_seconds: float,
    interval_seconds: float,
    clock: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
    log: Callable[[str], None] | None = None,
) -> dict:
    """Poll ``list_runs`` until every backfill run is terminal or ``timeout_seconds`` pass.

    A failing ``list_runs`` (``DagSpecError``, e.g. SQLite locked) is retried until the timeout.
    The returned summary has ``status`` success, failed or timeout.
    """
    deadline = clock() + timeout_seconds
    last_runs: list[DagRun] = []
    while True:
        last_error = None
        try:
            last_runs = parse_runs(list_runs())
        except DagSpecError as exc:
            last_error = str(exc)
        summary = summarize(last_runs, window, started_at=started_at, sla_minutes=sla_minutes)
        summary["last_error"] = last_error
        if log is not None:
            log(
                f"backfill: success={summary['runs_success']} failed={summary['runs_failed']} "
                f"pending={summary['runs_pending']}"
                + (f" error={last_error}" if last_error else "")
            )
        if last_error is None and summary["status"] != "running":
            return summary
        if clock() >= deadline:
            summary["status"] = "timeout"
            summary["wall_seconds"] = None
            summary["finished_at"] = None
            return summary
        sleep(interval_seconds)


def airflow_list_runs(airflow: str, dag_id: str) -> str:
    """Raw stdout of ``airflow dags list-runs -o json <dag_id>``."""
    cmd = [airflow, "dags", "list-runs", "-o", "json", dag_id]
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, check=False, timeout=180)
    except (OSError, subprocess.SubprocessError) as exc:
        raise DagSpecError(f"cannot run {' '.join(cmd)}: {exc}") from None
    if proc.returncode != 0:
        tail = (proc.stderr or proc.stdout or "").strip()[-500:]
        raise DagSpecError(f"airflow dags list-runs exited {proc.returncode}: {tail}")
    return proc.stdout


# ---------------------------------------------------------------- runs.csv row


def _token(field: str, value: object) -> str:
    text = str(value)
    if not _TOKEN.fullmatch(text):
        raise DagSpecError(f"{field} must be one token without spaces or '=' (got {value!r})")
    return text


def _summary_field(summary: Mapping, key: str) -> object:
    if key not in summary or summary[key] is None:
        raise DagSpecError(f"backfill summary has no {key}")
    return summary[key]


def build_row(
    summary: Mapping,
    *,
    run_at: datetime,
    git_sha: str,
    row_count: int | None,
    run_id: str,
    trigger: str,
    target: str = "ci",
) -> dict[str, str]:
    """``runs.csv`` row of a successful backfill: total_seconds = backfill wall clock."""
    status = summary.get("status")
    if status != "success":
        raise DagSpecError(f"backfill status is {status!r}: only a successful backfill is recorded")
    days = _summary_field(summary, "days")
    notes = {
        "engine": ENGINE,
        "days": days,
        "hours_per_day": _summary_field(summary, "hours_per_day"),
        "last_hour": _summary_field(summary, "hour"),
        "runs_success": _summary_field(summary, "runs_success"),
        "runs_failed": _summary_field(summary, "runs_failed"),
        "sla_minutes": _summary_field(summary, "sla_minutes"),
        "sla_missed": _summary_field(summary, "sla_missed"),
        "max_active_runs": _summary_field(summary, "max_active_runs"),
        "target": target,
        "run_id": run_id,
        "trigger": trigger,
    }
    row = {
        "run_at": run_at,
        "git_sha": git_sha,
        "variant": f"airflow-backfill-{_token('days', days)}d",
        "data_date": _summary_field(summary, "last_data_date"),
        "total_seconds": _summary_field(summary, "wall_seconds"),
        "row_count": row_count,
        "notes": " ".join(f"{k}={_token(k, v)}" for k, v in notes.items()),
    }
    return validate_row(row)


def row_count_from(summaries_dir: Path, data_dates: Iterable[date]) -> int | None:
    """Sum of ``silver_inserted`` of each data date's Spark summary; None if one is missing."""
    total = 0
    for day in data_dates:
        path = Path(summaries_dir) / day.isoformat() / "spark-summary.json"
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return None
        inserted = data.get("silver_inserted") if isinstance(data, Mapping) else None
        if isinstance(inserted, bool) or not isinstance(inserted, int) or inserted < 0:
            return None
        total += inserted
    return total


# ---------------------------------------------------------------- ingest check, SLA callback


def ingest_problems(records: object) -> list[str]:
    """Hours of an ingest summary that are neither ingested nor skipped (empty = all good)."""
    if not isinstance(records, list) or not records:
        return ["ingest summary is empty"]
    problems = []
    for r in records:
        status = r.get("status") if isinstance(r, Mapping) else None
        if status in ("ingested", "skipped"):
            continue
        day = r.get("data_date") if isinstance(r, Mapping) else None
        hour = r.get("hour") if isinstance(r, Mapping) else None
        label = f"{day}T{hour:02d}" if isinstance(hour, int) else f"{day}T{hour}"
        problems.append(f"{label}: {status}")
    return problems


async def record_sla_miss(path: str, **kwargs: object) -> None:
    """Deadline Alert callback: append one JSON line to ``path`` (runs in the Triggerer)."""
    context = kwargs.get("context")
    line = {
        "event": "sla_miss",
        "at": datetime.now(UTC).isoformat(),
        "dag_id": DAG_ID,
        "context": str(context)[:2000] if context is not None else None,
        "kwargs": sorted(kwargs),
    }
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(line, sort_keys=True) + "\n")


# ---------------------------------------------------------------- CLI


def _parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="python -m repopulse.dagspec",
        description="Helpers of the Airflow DAG repopulse_pipeline (incremental batch backfill).",
    )
    sub = p.add_subparsers(dest="command", required=True)

    w = sub.add_parser("window", help="print the backfill window as KEY=VALUE for $GITHUB_ENV")
    w.add_argument("--last-data-date", required=True, help="last data day YYYY-MM-DD (UTC)")
    w.add_argument("--days", type=int, required=True)

    t = sub.add_parser("wait", help="poll airflow dags list-runs until the backfill is done")
    t.add_argument("--last-data-date", required=True)
    t.add_argument("--days", type=int, required=True)
    t.add_argument("--started-at", required=True, help="ISO-8601 time the backfill was created")
    t.add_argument("--timeout-minutes", type=float, required=True)
    t.add_argument("--interval-seconds", type=float, default=30.0)
    t.add_argument("--airflow", default="airflow", help="path of the airflow executable")
    t.add_argument("--dag-id", default=DAG_ID)
    t.add_argument("--max-active-runs", type=int, default=MAX_ACTIVE_RUNS)
    t.add_argument("--summary", type=Path, required=True)
    t.add_argument("--raw", type=Path, help="write the last raw list-runs output here")

    r = sub.add_parser("row", help="write the runs.csv artifact row of a successful backfill")
    r.add_argument("--summary", type=Path, required=True)
    r.add_argument("--run-at", required=True, help="ISO-8601 with time zone")
    r.add_argument("--git-sha", required=True)
    r.add_argument("--run-id", required=True)
    r.add_argument("--trigger", required=True)
    r.add_argument("--summaries-dir", type=Path, default=Path("out/airflow"))
    r.add_argument("--out", type=Path, required=True)

    c = sub.add_parser("check-ingest", help="exit 1 unless every hour was ingested or skipped")
    c.add_argument("--summary", type=Path, required=True)
    return p


def _load_json(path: Path) -> object:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        raise DagSpecError(f"missing file {path}") from None
    except ValueError as exc:
        raise DagSpecError(f"invalid JSON in {path}: {exc}") from None


def _datetime_arg(name: str, value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        raise DagSpecError(f"{name} is not ISO-8601: {value!r}") from None
    if parsed.tzinfo is None:
        raise DagSpecError(f"{name} must include a time zone: {value!r}")
    return parsed


def _write_json(path: Path, data: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2) + "\n", encoding="utf-8")


def _cmd_wait(args: argparse.Namespace) -> int:
    window = backfill_window(_parse_date("--last-data-date", args.last_data_date), args.days)
    started = _datetime_arg("--started-at", args.started_at)
    settings = settings_from_env(os.environ)

    def list_runs() -> str:
        text = airflow_list_runs(args.airflow, args.dag_id)
        if args.raw is not None:
            args.raw.parent.mkdir(parents=True, exist_ok=True)
            args.raw.write_text(text, encoding="utf-8")
        return text

    summary = wait(
        list_runs,
        window,
        started_at=started,
        sla_minutes=settings.sla_minutes,
        timeout_seconds=args.timeout_minutes * 60,
        interval_seconds=args.interval_seconds,
        log=lambda line: print(line, flush=True),
    )
    summary["hour"] = settings.hour
    summary["hours_per_day"] = settings.hours_per_day
    summary["max_active_runs"] = args.max_active_runs
    _write_json(args.summary, summary)
    print(f"wrote {args.summary} (status {summary['status']})")
    if summary["status"] != "success":
        print(f"error: backfill {summary['status']}", file=sys.stderr)
        return 1
    return 0


def _cmd_row(args: argparse.Namespace) -> int:
    from repopulse.pipeline import write_row

    summary = _load_json(args.summary)
    if not isinstance(summary, Mapping):
        raise DagSpecError(f"{args.summary} must hold a JSON object")
    days = _summary_field(summary, "days")
    last = _parse_date("last_data_date", str(_summary_field(summary, "last_data_date")))
    window = backfill_window(last, days)
    row = build_row(
        summary,
        run_at=_datetime_arg("--run-at", args.run_at),
        git_sha=args.git_sha,
        row_count=row_count_from(args.summaries_dir, window.data_dates),
        run_id=args.run_id,
        trigger=args.trigger,
    )
    write_row(args.out, row)
    print(f"wrote {args.out}")
    return 0


def _run(args: argparse.Namespace) -> int:
    if args.command == "window":
        window = backfill_window(_parse_date("--last-data-date", args.last_data_date), args.days)
        print(f"BACKFILL_FROM={window.from_date.isoformat()}")
        print(f"BACKFILL_TO={window.to_date.isoformat()}")
        print(f"FIRST_DATA_DATE={window.first_data_date.isoformat()}")
        print(f"LAST_DATA_DATE={window.last_data_date.isoformat()}")
        print(f"{ENV_END_DATE}={window.to_date.isoformat()}")
        return 0
    if args.command == "wait":
        return _cmd_wait(args)
    if args.command == "row":
        return _cmd_row(args)
    if args.command == "check-ingest":
        problems = ingest_problems(_load_json(args.summary))
        for problem in problems:
            print(f"error: {problem}", file=sys.stderr)
        return 1 if problems else 0
    raise AssertionError(args.command)  # pragma: no cover


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        return _run(args)
    except ValueError as exc:  # DagSpecError and RunsLogError
        print(f"error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
