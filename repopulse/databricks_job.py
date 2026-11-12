"""Run the bronze -> silver incremental batch as a serverless Databricks job (D1).

Runs on the GitHub Actions runner, standard library only. Steps of ``run``:

1. Unity Catalog: schema ``<catalog>.<schema>`` and managed Volume (already exists = fine).
2. PII salt into a Databricks secret scope; the job only carries the scope and key names.
3. Files API: landed hours to ``landing/<source>/date=YYYY-MM-DD/hour=HH/<file>`` on the Volume
   (``overwrite=true``: re-pushing an hour replaces it) and the wheel to ``wheels/<git_sha>/``.
4. Import the notebook, submit one serverless notebook run (no cluster) with an
   ``idempotency_token`` of (source, date, hour, hours, run key), wait, read its JSON summary.
5. ``--verify-rerun``: submit the same hours again with another run key; silver must gain 0 rows.
6. One ``runs.csv`` row (``variant=databricks-bronze-silver``) as an artifact CSV.

Every HTTP call goes through an injectable ``transport``; ``sleep``/``clock`` are injectable too.
Retries: HTTP 429/5xx and network errors, deterministic exponential backoff. Every message is
passed through ``Redactor`` (host, token, user name, salt): the workflow log is public.
"""

from __future__ import annotations

import argparse
import base64
import functools
import hashlib
import json
import os
import sys
import time
import urllib.error
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlencode, urlsplit

from repopulse import ingest
from repopulse.pipeline import write_row
from repopulse.probe import (
    ApiError,
    HttpRequest,
    HttpResponse,
    Redactor,
    Secrets,
    Transport,
    _already_exists,
    _error_parts,
    urllib_transport,
)
from repopulse.runs_log import validate_row
from repopulse.spark.pii import PII_SALT_ENV
from repopulse.spark.rules import DEFAULT_LATE_WINDOW_HOURS

VARIANT = "databricks-bronze-silver"
SOURCES = ("gharchive", "app")
MAX_HOURS = 24
RETRY_STATUSES = frozenset({429, 500, 502, 503, 504})
NETWORK_ERRORS: tuple[type[BaseException], ...] = (
    urllib.error.URLError,
    TimeoutError,
    ConnectionError,
)
DEFAULT_CATALOG = "workspace"
DEFAULT_SCHEMA = "repopulse"
DEFAULT_VOLUME = "repopulse_landing"
DEFAULT_SECRET_SCOPE = "repopulse"
DEFAULT_SECRET_KEY = "pii_hash_salt"
NOTEBOOK_DIR = "repopulse"
NOTEBOOK_NAME = "repopulse_bronze_silver"
TASK_KEY = "bronze_silver"
RERUN_SUFFIX = "-rerun"
UPLOAD_TIMEOUT = 600.0

_RUN_TERMINAL = frozenset({"TERMINATED", "SKIPPED", "INTERNAL_ERROR"})
_REQUIRED_ENV = ("DATABRICKS_HOST", "DATABRICKS_TOKEN", PII_SALT_ENV)
_DURATIONS = {
    "queue": "queue_duration",
    "setup": "setup_duration",
    "execution": "execution_duration",
    "cleanup": "cleanup_duration",
    "run": "run_duration",
}

NOTEBOOK_TEMPLATE = """# Databricks notebook source
# MAGIC %pip install --quiet --force-reinstall --no-deps {wheel_path}

# COMMAND ----------

# RepoPulse D1: bronze -> silver incremental batch on serverless compute.
# Imported by `python -m repopulse.databricks_job run`; parameters come from the job.
import json

from repopulse.spark import databricks_entry

params = {{name: dbutils.widgets.get(name) for name in databricks_entry.PARAMETERS}}
salt = dbutils.secrets.get(scope=params["secret_scope"], key=params["secret_key"])
summary = databricks_entry.run(spark, params, salt=salt)
dbutils.notebook.exit(json.dumps(summary))
"""


class JobError(Exception):
    """The Databricks run failed, timed out, or returned an unusable summary."""


# --- HTTP client ------------------------------------------------------------------


class Client:
    """Databricks REST client with retry on HTTP 429/5xx and on network errors."""

    def __init__(
        self,
        host: str,
        token: str,
        *,
        transport: Transport,
        sleep: Callable[[float], None] = time.sleep,
        max_attempts: int = 5,
        backoff_seconds: float = 1.0,
    ) -> None:
        if max_attempts < 1:
            raise ValueError("max_attempts must be >= 1")
        self.host = host.rstrip("/")
        self.token = token
        self.transport = transport
        self.sleep = sleep
        self.max_attempts = max_attempts
        self.backoff_seconds = backoff_seconds

    def call(
        self,
        method: str,
        path: str,
        *,
        json_body: Any = None,
        raw: bytes | None = None,
        query: Mapping[str, Any] | None = None,
    ) -> HttpResponse:
        url = self.host + path + ("?" + urlencode(query) if query else "")
        headers = {"Authorization": f"Bearer {self.token}"}
        body = None
        if json_body is not None:
            body = json.dumps(json_body).encode()
            headers["Content-Type"] = "application/json"
        elif raw is not None:
            body = raw
            headers["Content-Type"] = "application/octet-stream"
        request = HttpRequest(method, url, headers, body)
        for attempt in range(1, self.max_attempts + 1):
            last = attempt == self.max_attempts
            try:
                resp = self.transport(request)
            except NETWORK_ERRORS:
                if last:
                    raise
                self.sleep(self.backoff_seconds * 2 ** (attempt - 1))
                continue
            if 200 <= resp.status < 300:
                return resp
            code, message = _error_parts(resp.body)
            if resp.status not in RETRY_STATUSES or last:
                raise ApiError(resp.status, code, message)
            self.sleep(self.backoff_seconds * 2 ** (attempt - 1))
        raise AssertionError("unreachable")  # pragma: no cover

    def json(self, method: str, path: str, **kwargs: Any) -> dict[str, Any]:
        resp = self.call(method, path, **kwargs)
        if not resp.body:
            return {}
        data = json.loads(resp.body)
        return data if isinstance(data, dict) else {}


# --- pure helpers -----------------------------------------------------------------


def idempotency_token(source: str, day: str, hour: int, hours: int, run_key: str) -> str:
    """Same (source, date, hour, hours, run key) -> same token; at most 64 characters."""
    digest = hashlib.sha256(f"{source}|{day}|{int(hour)}|{int(hours)}|{run_key}".encode())
    return f"rp-{source}-{day}T{int(hour):02d}-{int(hours)}h-{digest.hexdigest()[:24]}"


def volume_root(catalog: str, schema: str, volume: str) -> str:
    return f"/Volumes/{catalog}/{schema}/{volume}"


def build_submit_payload(
    *,
    source: str,
    day: str,
    hour: int,
    hours: int,
    run_key: str,
    notebook_path: str,
    wheel_path: str,
    catalog: str = DEFAULT_CATALOG,
    schema: str = DEFAULT_SCHEMA,
    volume: str = DEFAULT_VOLUME,
    late_window_hours: int = DEFAULT_LATE_WINDOW_HOURS,
    secret_scope: str = DEFAULT_SECRET_SCOPE,
    secret_key: str = DEFAULT_SECRET_KEY,
    timeout_seconds: int = 3600,
) -> dict[str, Any]:
    """One-off serverless run: one notebook task, no cluster declared, no secret value."""
    params = {
        "source": source,
        "date": day,
        "hour": str(int(hour)),
        "hours": str(int(hours)),
        "catalog": catalog,
        "schema": schema,
        "volume": volume,
        "wheel_path": wheel_path,
        "late_window_hours": str(int(late_window_hours)),
        "secret_scope": secret_scope,
        "secret_key": secret_key,
    }
    return {
        "run_name": f"repopulse-bronze-silver-{source}-{day}T{int(hour):02d}-{int(hours)}h",
        "idempotency_token": idempotency_token(source, day, hour, hours, run_key),
        "timeout_seconds": int(timeout_seconds),
        "tasks": [
            {
                "task_key": TASK_KEY,
                "notebook_task": {"notebook_path": notebook_path, "base_parameters": params},
            }
        ],
    }


def notebook_source(wheel_path: str) -> str:
    """Notebook run by the job; the wheel path is fixed per git sha, never a secret."""
    return NOTEBOOK_TEMPLATE.format(wheel_path=wheel_path)


# --- Unity Catalog, secrets, files ----------------------------------------------------


def _create(client: Client, path: str, body: Mapping[str, Any]) -> None:
    try:
        client.call("POST", path, json_body=dict(body))
    except ApiError as exc:
        if not _already_exists(exc):
            raise


def ensure_schema(client: Client, *, catalog: str, schema: str) -> None:
    _create(client, "/api/2.1/unity-catalog/schemas", {"catalog_name": catalog, "name": schema})


def ensure_volume(client: Client, *, catalog: str, schema: str, volume: str) -> None:
    body = {
        "catalog_name": catalog,
        "schema_name": schema,
        "name": volume,
        "volume_type": "MANAGED",
    }
    _create(client, "/api/2.1/unity-catalog/volumes", body)


def ensure_secret(client: Client, *, scope: str, key: str, value: str) -> None:
    """Scope (already exists = fine), then the value; the only request that carries it."""
    _create(client, "/api/2.0/secrets/scopes/create", {"scope": scope})
    client.call(
        "POST",
        "/api/2.0/secrets/put",
        json_body={"scope": scope, "key": key, "string_value": value},
    )


def _put_file(client: Client, volume_path: str, data: bytes) -> None:
    client.call("PUT", "/api/2.0/fs/files" + volume_path, raw=data, query={"overwrite": "true"})


def upload_landing(
    client: Client,
    *,
    landing_dir: Path,
    source: str,
    hours: Sequence[tuple[str, int]],
    catalog: str,
    schema: str,
    volume: str,
) -> list[str]:
    """Push every file of the landed hours; the target path depends only on (source, hour)."""
    base = f"{volume_root(catalog, schema, volume)}/landing/{source}"
    uploaded = []
    for day, hour in hours:
        part = ingest.partition_dir(Path(landing_dir), day, int(hour))
        if not part.is_dir():
            continue
        for path in sorted(p for p in part.iterdir() if p.is_file()):
            target = f"{base}/date={day}/hour={int(hour):02d}/{path.name}"
            _put_file(client, target, path.read_bytes())
            uploaded.append(target)
    return uploaded


def upload_wheel(
    client: Client, *, wheel: Path, git_sha: str, catalog: str, schema: str, volume: str
) -> str:
    target = f"{volume_root(catalog, schema, volume)}/wheels/{git_sha}/{Path(wheel).name}"
    _put_file(client, target, Path(wheel).read_bytes())
    return target


def import_notebook(client: Client, *, path: str, source: str) -> None:
    folder = path.rsplit("/", 1)[0]
    client.call("POST", "/api/2.0/workspace/mkdirs", json_body={"path": folder})
    client.call(
        "POST",
        "/api/2.0/workspace/import",
        json_body={
            "path": path,
            "format": "SOURCE",
            "language": "PYTHON",
            "overwrite": True,
            "content": base64.b64encode(source.encode()).decode(),
        },
    )


# --- jobs -------------------------------------------------------------------------------


@dataclass(frozen=True)
class RunOutcome:
    run_id: int
    summary: dict[str, Any]
    timings: dict[str, float] = field(default_factory=dict)


def submit_run(client: Client, payload: Mapping[str, Any]) -> int:
    """Submit once; a retried request carries the same idempotency token, so one run."""
    data = client.json("POST", "/api/2.1/jobs/runs/submit", json_body=dict(payload))
    run_id = data.get("run_id")
    if isinstance(run_id, bool) or not isinstance(run_id, int):
        raise JobError("runs/submit answered without a run_id")
    return run_id


def _timings(info: Mapping[str, Any]) -> dict[str, float]:
    tasks = info.get("tasks") or []
    task = tasks[0] if tasks and isinstance(tasks[0], Mapping) else {}
    out = {}
    for name, key in _DURATIONS.items():
        value = info.get(key) or task.get(key)
        if isinstance(value, int | float) and not isinstance(value, bool) and value > 0:
            out[name] = value / 1000.0
    return out


def wait_for_run(
    client: Client,
    run_id: int,
    *,
    timeout: float,
    poll_interval: float,
    clock: Callable[[], float],
    sleep: Callable[[float], None],
) -> RunOutcome:
    """Poll until the run ends; cancel it after ``timeout`` seconds. SUCCESS -> its summary."""
    deadline = clock() + timeout
    while True:
        info = client.json("GET", "/api/2.1/jobs/runs/get", query={"run_id": run_id})
        state = info.get("state") or {}
        life_cycle = state.get("life_cycle_state", "")
        if life_cycle in _RUN_TERMINAL:
            break
        if clock() >= deadline:
            try:
                client.call("POST", "/api/2.1/jobs/runs/cancel", json_body={"run_id": run_id})
            except Exception:  # best effort; the timeout is the error reported
                pass
            raise JobError(f"timed out waiting for run {run_id} (last state {life_cycle})")
        sleep(poll_interval)

    result_state = state.get("result_state", "")
    if result_state != "SUCCESS":
        message = state.get("state_message", "")
        raise JobError(f"run {run_id} ended {life_cycle}/{result_state}: {message}")

    tasks = info.get("tasks") or []
    task_run_id = tasks[0].get("run_id", run_id) if tasks else run_id
    out = client.json("GET", "/api/2.1/jobs/runs/get-output", query={"run_id": task_run_id})
    raw = (out.get("notebook_output") or {}).get("result")
    try:
        summary = json.loads(raw) if isinstance(raw, str) else None
    except ValueError:
        summary = None
    if not isinstance(summary, dict):
        raise JobError(f"run {run_id} did not return a JSON object summary")
    return RunOutcome(run_id, summary, _timings(info))


# --- runs.csv row -----------------------------------------------------------------------


def _count(summary: Mapping[str, Any], key: str) -> int:
    value = summary.get(key) if isinstance(summary, Mapping) else None
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"summary {key} must be an int >= 0 (got {value!r})")
    return value


def _token(name: str, value: object) -> str:
    text = str(value)
    if not text or any(ch.isspace() for ch in text) or "=" in text:
        raise ValueError(f"note {name} must be one token without spaces or '=' (got {value!r})")
    return text


def build_row(
    *,
    run_at: datetime,
    git_sha: str,
    source: str,
    day: str,
    hour: int,
    hours: int,
    summary: Mapping[str, Any],
    total_seconds: float,
    upload_seconds: float,
    run_id: str,
    trigger: str,
    timings: Mapping[str, float] | None = None,
    rerun_summary: Mapping[str, Any] | None = None,
) -> dict[str, str]:
    """``runs.csv`` row: total = submit -> run end (first run); row_count = silver inserted."""
    inserted = _count(summary, "silver_inserted")
    notes: dict[str, str] = {
        "source": _token("source", source),
        "last_hour": f"{day}T{int(hour):02d}",
        "hours": str(int(hours)),
        "bronze_rows": str(_count(summary, "bronze_rows")),
        "quarantined_total": str(_count(summary, "quarantined_total")),
        "duplicates_dropped": str(_count(summary, "duplicates_dropped")),
        "upload_s": str(float(upload_seconds)),
    }
    for name, seconds in (timings or {}).items():
        notes[f"{_token('timing', name)}_s"] = str(float(seconds))
    notes["run_id"] = _token("run_id", run_id)
    notes["trigger"] = _token("trigger", trigger)
    if rerun_summary is not None:
        notes["rerun_silver_inserted"] = str(_count(rerun_summary, "silver_inserted"))
    row = {
        "run_at": run_at,
        "git_sha": git_sha,
        "variant": VARIANT,
        "data_date": day,
        "total_seconds": float(total_seconds),
        "row_count": inserted,
        "notes": " ".join(f"{k}={v}" for k, v in notes.items()),
    }
    return validate_row(row)


# --- CLI ----------------------------------------------------------------------------------


def _parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="python -m repopulse.databricks_job",
        description="Bronze -> silver incremental batch as a serverless Databricks job.",
    )
    sub = p.add_subparsers(dest="command", required=True)
    r = sub.add_parser("run", help="push landed hours, run the job, write the runs.csv row")
    r.add_argument("--source", required=True, choices=SOURCES)
    r.add_argument("--date", required=True, help="data date YYYY-MM-DD (UTC) of the last hour")
    r.add_argument("--hour", required=True, type=int, help="last data hour 0..23 (UTC)")
    r.add_argument("--hours", type=int, default=1, help=f"consecutive hours, 1..{MAX_HOURS}")
    r.add_argument("--landing-dir", type=Path, required=True)
    r.add_argument("--wheel", type=Path, required=True, help="repopulse wheel to install")
    r.add_argument("--git-sha", required=True)
    r.add_argument("--summary", type=Path, required=True)
    r.add_argument("--out", type=Path, required=True, help="runs.csv artifact row")
    r.add_argument("--verify-rerun", action="store_true")
    r.add_argument("--catalog", default=DEFAULT_CATALOG)
    r.add_argument("--schema", default=DEFAULT_SCHEMA)
    r.add_argument("--volume", default=DEFAULT_VOLUME)
    r.add_argument("--secret-scope", default=DEFAULT_SECRET_SCOPE)
    r.add_argument("--secret-key", default=DEFAULT_SECRET_KEY)
    r.add_argument("--late-window-hours", type=int, default=DEFAULT_LATE_WINDOW_HOURS)
    r.add_argument("--job-timeout", type=float, default=2400.0, help="seconds per run")
    r.add_argument("--poll-interval", type=float, default=15.0)
    r.add_argument(
        "--add-mask",
        action="store_true",
        help="print ::add-mask:: for the bare hostname and the user name (GitHub Actions)",
    )
    return p


@dataclass
class _Ctx:
    client: Client
    redact: Redactor
    clock: Callable[[], float]
    sleep: Callable[[float], None]
    args: argparse.Namespace

    def say(self, text: str, *, err: bool = False) -> None:
        print(self.redact(text), file=sys.stderr if err else sys.stdout, flush=True)


def _run_once(ctx: _Ctx, payload: Mapping[str, Any]) -> tuple[RunOutcome, float]:
    start = ctx.clock()
    run_id = submit_run(ctx.client, payload)
    ctx.say(f"submitted run {run_id} ({payload['run_name']})")
    outcome = wait_for_run(
        ctx.client,
        run_id,
        timeout=ctx.args.job_timeout,
        poll_interval=ctx.args.poll_interval,
        clock=ctx.clock,
        sleep=ctx.sleep,
    )
    return outcome, ctx.clock() - start


def _run(ctx: _Ctx, env: Mapping[str, str], salt: str, hours: list[tuple[str, int]]) -> int:
    args, client = ctx.args, ctx.client
    me = client.json("GET", "/api/2.0/preview/scim/v2/Me")
    user = str(me.get("userName") or "")
    if not user:
        raise JobError("SCIM Me answered without a userName")
    ctx.redact.add(user)
    if args.add_mask:
        netloc = urlsplit(client.host).netloc
        for value in (netloc, user):
            print(f"::add-mask::{value}", flush=True)

    ensure_schema(client, catalog=args.catalog, schema=args.schema)
    ensure_volume(client, catalog=args.catalog, schema=args.schema, volume=args.volume)
    ensure_secret(client, scope=args.secret_scope, key=args.secret_key, value=salt)

    start = ctx.clock()
    uploaded = upload_landing(
        client,
        landing_dir=args.landing_dir,
        source=args.source,
        hours=hours,
        catalog=args.catalog,
        schema=args.schema,
        volume=args.volume,
    )
    wheel_path = upload_wheel(
        client,
        wheel=args.wheel,
        git_sha=args.git_sha,
        catalog=args.catalog,
        schema=args.schema,
        volume=args.volume,
    )
    upload_seconds = ctx.clock() - start
    ctx.say(f"uploaded {len(uploaded)} landing file(s) and the wheel")
    if not uploaded:
        raise JobError("no landing file found for the requested hours")

    notebook_path = f"/Users/{user}/{NOTEBOOK_DIR}/{NOTEBOOK_NAME}"
    import_notebook(client, path=notebook_path, source=notebook_source(wheel_path))

    if env.get("GITHUB_RUN_ID"):
        run_key = f"{env['GITHUB_RUN_ID']}-{env.get('GITHUB_RUN_ATTEMPT') or '1'}"
    else:
        run_key = f"local-{int(time.time())}"
    day, last_hour = hours[-1]
    common = {
        "source": args.source,
        "day": day,
        "hour": last_hour,
        "hours": len(hours),
        "notebook_path": notebook_path,
        "wheel_path": wheel_path,
        "catalog": args.catalog,
        "schema": args.schema,
        "volume": args.volume,
        "late_window_hours": args.late_window_hours,
        "secret_scope": args.secret_scope,
        "secret_key": args.secret_key,
        "timeout_seconds": int(args.job_timeout),
    }
    outcome, total = _run_once(ctx, build_submit_payload(run_key=run_key, **common))
    ctx.say(json.dumps(outcome.summary, sort_keys=True))

    rerun = None
    if args.verify_rerun:
        rerun, _ = _run_once(ctx, build_submit_payload(run_key=run_key + RERUN_SUFFIX, **common))
        ctx.say("rerun: " + json.dumps(rerun.summary, sort_keys=True))

    written = dict(outcome.summary)
    if rerun is not None:
        written["rerun"] = rerun.summary
    args.summary.parent.mkdir(parents=True, exist_ok=True)
    args.summary.write_text(json.dumps(written, indent=2) + "\n", encoding="utf-8")

    if rerun is not None and rerun.summary.get("silver_inserted") != 0:
        inserted = rerun.summary.get("silver_inserted")
        ctx.say(
            f"error: re-running the same hours inserted rows: silver_inserted={inserted!r}",
            err=True,
        )
        return 1

    row = build_row(
        run_at=datetime.now(UTC).replace(microsecond=0),
        git_sha=args.git_sha,
        source=args.source,
        day=day,
        hour=last_hour,
        hours=len(hours),
        summary=outcome.summary,
        total_seconds=round(total, 3),
        upload_seconds=round(upload_seconds, 3),
        run_id=env.get("GITHUB_RUN_ID") or "local",
        trigger=env.get("GITHUB_EVENT_NAME") or "manual",
        timings=outcome.timings,
        rerun_summary=rerun.summary if rerun is not None else None,
    )
    write_row(args.out, row)
    ctx.say(f"wrote {args.out}")
    return 0


def main(
    argv: Sequence[str] | None = None,
    *,
    env: Mapping[str, str] | None = None,
    transport: Transport | None = None,
    sleep: Callable[[float], None] | None = None,
    clock: Callable[[], float] | None = None,
) -> int:
    env = os.environ if env is None else env
    parser = _parser()
    args = parser.parse_args(list(sys.argv[1:] if argv is None else argv))
    try:
        ingest.validate(args.date, args.hour)
    except ValueError as exc:
        parser.error(str(exc))
    if not 1 <= args.hours <= MAX_HOURS:
        parser.error(f"--hours must be 1..{MAX_HOURS}")
    if args.late_window_hours < 0:
        parser.error("--late-window-hours must be >= 0")

    missing = [n for n in _REQUIRED_ENV if not (env.get(n) or "").strip()]
    if missing:
        print(f"error: missing environment variable(s): {', '.join(missing)}", file=sys.stderr)
        return 1

    secrets = Secrets.from_env(env)
    salt = env[PII_SALT_ENV]
    redact = secrets.redactor()
    redact.add(salt)
    sleep = sleep or time.sleep
    clock = clock or time.monotonic
    client = Client(
        secrets.host,
        secrets.token,
        transport=transport or functools.partial(urllib_transport, timeout=UPLOAD_TIMEOUT),
        sleep=sleep,
    )
    ctx = _Ctx(client, redact, clock, sleep, args)
    end = datetime.fromisoformat(args.date).replace(hour=args.hour, tzinfo=UTC)
    hours = ingest.target_hours(end, lag_hours=0, count=args.hours)
    try:
        for path, what in ((args.landing_dir, "--landing-dir"), (args.wheel, "--wheel")):
            if not Path(path).exists():
                raise JobError(f"{what} does not exist: {path}")
        return _run(ctx, env, salt, hours)
    except Exception as exc:
        print(f"error: {redact(f'{type(exc).__name__}: {exc}')}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
