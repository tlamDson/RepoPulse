"""Cloud probe (plan step 1): can GitHub Actions drive Databricks Free Edition, or MotherDuck?

Runs a fixed set of checks against the Databricks REST API and MotherDuck, then picks a path:

- ``primary``  - Databricks Free Edition (serverless) driven from GitHub Actions: token works,
  a file can be pushed to a Volume, a serverless job runs, a SQL warehouse answers ``SELECT 1``
  (and ``dbt debug --target databricks`` did not fail, when it ran).
- ``fallback`` - PySpark + Delta on the Actions runner with gold on MotherDuck.
- ``blocked``  - neither path is usable yet.

Whether the notebook can download ``data.gharchive.org`` is recorded for information only: the
plan already has Actions download GH Archive and push it to the Volume.

Only the standard library is used; HTTP goes through an injectable ``transport`` so unit tests
never touch the network. Secret values (host, tokens, warehouse path, user name) never reach a
result: every detail goes through :class:`Redactor`. Results are pass/fail/skipped with a short
description; no timings are recorded here (numbers only come from ``runs.csv``).
"""

from __future__ import annotations

import argparse
import base64
import json
import os
import re
import sys
import time
import urllib.error
import urllib.request
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import asdict, dataclass, field
from typing import Any
from urllib.parse import urlencode, urlsplit

PASS = "pass"
FAIL = "fail"
SKIPPED = "skipped"
STATUSES = (PASS, FAIL, SKIPPED)

SECRET_SHAPE = "databricks_secret_shape"
AUTH = "databricks_auth"
VOLUME = "databricks_volume_upload"
JOB = "databricks_serverless_job"
SQL_WAREHOUSE = "databricks_sql_warehouse"
NOTEBOOK_GHARCHIVE = "notebook_gharchive_download"
MOTHERDUCK = "motherduck_connect"
DBT_DATABRICKS = "dbt_debug_databricks"
DBT_MOTHERDUCK = "dbt_debug_motherduck"

REQUIRED_DATABRICKS: tuple[str, ...] = (AUTH, VOLUME, JOB, SQL_WAREHOUSE)
DECIDES_PATH: frozenset[str] = frozenset(
    {*REQUIRED_DATABRICKS, MOTHERDUCK, DBT_DATABRICKS, DBT_MOTHERDUCK}
)
DECISIONS = ("primary", "fallback", "blocked")

GHARCHIVE_URL = "https://data.gharchive.org/2026-10-25-12.json.gz"
PROBE_FILE_CONTENT = b"repopulse probe: hello from GitHub Actions\n"
NOTEBOOK_NAME = "repopulse_probe_notebook"
RUN_NAME = "repopulse-probe"

_SECRET_FIELDS = {
    "DATABRICKS_HOST": "host",
    "DATABRICKS_TOKEN": "token",
    "DATABRICKS_HTTP_PATH": "http_path",
    "MOTHERDUCK_TOKEN": "motherduck_token",
}
_DATABRICKS_NEEDS = ("DATABRICKS_HOST", "DATABRICKS_TOKEN")
_HTTP_PATH = re.compile(r"/sql/1\.0/warehouses/([A-Za-z0-9]+)")
_RUN_TERMINAL = frozenset({"TERMINATED", "SKIPPED", "INTERNAL_ERROR"})
_STATEMENT_WAITING = frozenset({"PENDING", "RUNNING"})
_MAX_DETAIL = 400

NOTEBOOK_SOURCE = f'''# Databricks notebook source
# RepoPulse cloud probe: can serverless compute reach data.gharchive.org?
import json
import urllib.request

URL = "{GHARCHIVE_URL}"
result = {{"gharchive_ok": False, "url": URL}}
try:
    req = urllib.request.Request(URL, headers={{"User-Agent": "repopulse-probe"}})
    with urllib.request.urlopen(req, timeout=20) as resp:
        head = resp.read(4096)
    result["gharchive_ok"] = head[:2] == b"\\x1f\\x8b"
    if not result["gharchive_ok"]:
        result["error"] = "response is not gzip"
except Exception as exc:
    result["error"] = (type(exc).__name__ + ": " + str(exc))[:300]

dbutils.notebook.exit(json.dumps(result))
'''


# --- data ---------------------------------------------------------------------


@dataclass(frozen=True)
class CheckResult:
    name: str
    status: str
    detail: str

    def __post_init__(self) -> None:
        if self.status not in STATUSES:
            raise ValueError(f"status must be one of {STATUSES}, got {self.status!r}")


@dataclass(frozen=True)
class HttpRequest:
    method: str
    url: str
    headers: Mapping[str, str]
    body: bytes | None


@dataclass(frozen=True)
class HttpResponse:
    status: int
    body: bytes


Transport = Callable[[HttpRequest], HttpResponse]


class Redactor:
    """Replace every known secret value (and anything shaped like a Databricks PAT) by ``***``."""

    MASK = "***"
    _PAT = re.compile(r"dapi[0-9A-Za-z_-]{6,}")

    def __init__(self, values: Iterable[str | None] = ()) -> None:
        self._values: set[str] = set()
        for value in values:
            self.add(value)

    def add(self, value: str | None) -> None:
        if value and len(value) >= 4:
            self._values.add(value)

    def __call__(self, text: object) -> str:
        out = str(text)
        for value in sorted(self._values, key=len, reverse=True):
            out = out.replace(value, self.MASK)
        return self._PAT.sub("dapi" + self.MASK, out)


def _with_scheme(raw: str) -> str:
    return raw if "://" in raw else "https://" + raw


def _normalise_host(raw: str) -> str:
    """Keep only ``scheme://hostname``: a pasted browser URL may carry a path or ``?o=``."""
    host = raw.strip()
    if not host:
        return ""
    parts = urlsplit(_with_scheme(host))
    if not parts.netloc:
        return _with_scheme(host).rstrip("/")
    return f"{parts.scheme}://{parts.netloc}"


@dataclass(frozen=True)
class Secrets:
    host: str = ""
    token: str = ""
    http_path: str = ""
    motherduck_token: str = ""
    host_raw: str = ""

    @classmethod
    def from_env(cls, env: Mapping[str, str]) -> Secrets:
        def get(name: str) -> str:
            return (env.get(name) or "").strip()

        return cls(
            host=_normalise_host(get("DATABRICKS_HOST")),
            token=get("DATABRICKS_TOKEN"),
            http_path=get("DATABRICKS_HTTP_PATH"),
            motherduck_token=get("MOTHERDUCK_TOKEN"),
            host_raw=get("DATABRICKS_HOST"),
        )

    def missing(self, names: Sequence[str]) -> list[str]:
        return [n for n in names if not getattr(self, _SECRET_FIELDS[n])]

    def redactor(self) -> Redactor:
        netloc = urlsplit(self.host).netloc if self.host else ""
        values = [self.host_raw, self.host, netloc, self.token, self.motherduck_token]
        return Redactor([*values, self.http_path])


@dataclass
class ProbeContext:
    secrets: Secrets
    transport: Transport
    sleep: Callable[[float], None] = time.sleep
    clock: Callable[[], float] = time.monotonic
    connect: Callable[[str], Any] | None = None
    catalog: str = "workspace"
    schema: str = "default"
    volume: str = "repopulse_probe"
    job_timeout: float = 1200.0
    statement_timeout: float = 600.0
    poll_interval: float = 10.0
    redact: Redactor = field(init=False)
    user_name: str | None = field(default=None, init=False)
    http_path: str | None = field(default=None, init=False)
    job_succeeded: bool = field(default=False, init=False)
    notebook_output: dict[str, Any] | None = field(default=None, init=False)
    notebook_error: str | None = field(default=None, init=False)

    def __post_init__(self) -> None:
        self.redact = self.secrets.redactor()


# --- HTTP helpers -------------------------------------------------------------


class ApiError(Exception):
    def __init__(self, status: int, error_code: str, message: str) -> None:
        self.status = status
        self.error_code = error_code
        self.message = message
        code = f" {error_code}" if error_code else ""
        super().__init__(f"HTTP {status}{code}: {message}")


def urllib_transport(req: HttpRequest, timeout: float = 60.0) -> HttpResponse:
    """Default transport: ``urllib``; HTTP error statuses come back as a response, not raised."""
    headers = {"User-Agent": "repopulse-probe", **dict(req.headers)}
    request = urllib.request.Request(req.url, data=req.body, headers=headers, method=req.method)
    try:
        with urllib.request.urlopen(request, timeout=timeout) as resp:
            return HttpResponse(resp.status, resp.read())
    except urllib.error.HTTPError as exc:
        try:
            body = exc.read()
        finally:
            exc.close()
        return HttpResponse(exc.code, body)


def _error_parts(body: bytes) -> tuple[str, str]:
    text = body.decode("utf-8", errors="replace")
    try:
        data = json.loads(text)
    except ValueError:
        return "", text[:300]
    if isinstance(data, dict):
        return str(data.get("error_code") or ""), str(data.get("message") or text)[:300]
    return "", text[:300]


def _call(
    ctx: ProbeContext,
    method: str,
    path: str,
    *,
    json_body: Any = None,
    raw: bytes | None = None,
    query: Mapping[str, Any] | None = None,
) -> HttpResponse:
    url = ctx.secrets.host + path + ("?" + urlencode(query) if query else "")
    headers = {"Authorization": f"Bearer {ctx.secrets.token}"}
    body = None
    if json_body is not None:
        body = json.dumps(json_body).encode()
        headers["Content-Type"] = "application/json"
    elif raw is not None:
        body = raw
        headers["Content-Type"] = "application/octet-stream"
    resp = ctx.transport(HttpRequest(method, url, headers, body))
    if not 200 <= resp.status < 300:
        code, message = _error_parts(resp.body)
        raise ApiError(resp.status, code, message)
    return resp


def _json(resp: HttpResponse) -> dict[str, Any]:
    if not resp.body:
        return {}
    data = json.loads(resp.body)
    return data if isinstance(data, dict) else {}


def _one_line(text: str) -> str:
    out = " ".join(text.split())
    return out if len(out) <= _MAX_DETAIL else out[: _MAX_DETAIL - 3] + "..."


def _check(name: str, needs: Sequence[str] = _DATABRICKS_NEEDS):
    """Turn ``fn(ctx) -> (status, detail)`` into a check that never raises and never leaks."""

    def deco(
        fn: Callable[[ProbeContext], tuple[str, str]],
    ) -> Callable[[ProbeContext], CheckResult]:
        def wrapper(ctx: ProbeContext) -> CheckResult:
            missing = ctx.secrets.missing(needs)
            if missing:
                return CheckResult(name, SKIPPED, "missing secret " + ", ".join(missing))
            try:
                status, detail = fn(ctx)
            except ApiError as exc:
                status, detail = FAIL, str(exc)
            except Exception as exc:  # a broken check must not stop the others
                status, detail = FAIL, f"{type(exc).__name__}: {exc}"
            return CheckResult(name, status, _one_line(ctx.redact(detail)))

        wrapper.__name__ = fn.__name__
        wrapper.__doc__ = fn.__doc__
        return wrapper

    return deco


# --- Databricks checks --------------------------------------------------------

_WORKSPACE_DOMAINS = (".databricks.com", ".azuredatabricks.net")


def _quoted(value: str) -> bool:
    return value[:1] in ("'", '"') or value[-1:] in ("'", '"')


def _shape_problems(host_raw: str, token: str) -> list[str]:
    """Describe what looks wrong with the two secrets, never quoting their values."""
    problems = []
    if _quoted(host_raw):
        problems.append("DATABRICKS_HOST is wrapped in quotes")
    else:
        parts = urlsplit(_with_scheme(host_raw))
        if parts.path not in ("", "/") or parts.query or parts.fragment:
            problems.append(
                "DATABRICKS_HOST has a path or query after the hostname "
                "(the probe only uses scheme and hostname)"
            )
        name = (parts.hostname or "").lower()
        if name.startswith("accounts."):
            problems.append("DATABRICKS_HOST is the account console, not a workspace URL")
        elif not name.endswith(_WORKSPACE_DOMAINS):
            problems.append("DATABRICKS_HOST is not a Databricks workspace hostname")
    if _quoted(token):
        problems.append("DATABRICKS_TOKEN is wrapped in quotes")
    elif any(ch.isspace() for ch in token):
        problems.append("DATABRICKS_TOKEN contains whitespace")
    elif not token.startswith("dapi"):
        problems.append(
            "DATABRICKS_TOKEN does not start with dapi, so it is not a personal access token"
        )
    return problems


@_check(SECRET_SHAPE)
def check_secret_shape(ctx: ProbeContext) -> tuple[str, str]:
    """Info only, no network: tells a mistyped secret apart from a refusal by the platform."""
    problems = _shape_problems(ctx.secrets.host_raw or ctx.secrets.host, ctx.secrets.token)
    if problems:
        return FAIL, "; ".join(problems)
    return PASS, "host is a workspace URL and the token has the personal access token prefix"


@_check(AUTH)
def check_auth(ctx: ProbeContext) -> tuple[str, str]:
    """Token works from outside the workspace: ``GET /api/2.0/preview/scim/v2/Me``."""
    data = _json(_call(ctx, "GET", "/api/2.0/preview/scim/v2/Me"))
    user = data.get("userName")
    if not user:
        return FAIL, "SCIM Me answered without a userName"
    ctx.redact.add(user)
    ctx.user_name = user
    return PASS, "token accepted; SCIM Me returned the current user"


def _already_exists(exc: ApiError) -> bool:
    return (
        exc.status == 409
        or exc.error_code in {"RESOURCE_ALREADY_EXISTS", "ALREADY_EXISTS"}
        or "already exists" in exc.message.lower()
    )


@_check(VOLUME)
def check_volume(ctx: ProbeContext) -> tuple[str, str]:
    """Create the managed probe Volume if needed, upload a small file and read it back."""
    spec = {
        "catalog_name": ctx.catalog,
        "schema_name": ctx.schema,
        "name": ctx.volume,
        "volume_type": "MANAGED",
    }
    created = "created"
    try:
        _call(ctx, "POST", "/api/2.1/unity-catalog/volumes", json_body=spec)
    except ApiError as exc:
        if not _already_exists(exc):
            raise
        created = "already existed"
    path = f"/api/2.0/fs/files/Volumes/{ctx.catalog}/{ctx.schema}/{ctx.volume}/probe/hello.txt"
    _call(ctx, "PUT", path, raw=PROBE_FILE_CONTENT, query={"overwrite": "true"})
    back = _call(ctx, "GET", path).body
    if back != PROBE_FILE_CONTENT:
        return FAIL, "uploaded file read back with different content"
    full = f"{ctx.catalog}.{ctx.schema}.{ctx.volume}"
    return PASS, f"volume {full} {created}; file uploaded and read back via the Files API"


def _cancel_quietly(ctx: ProbeContext, method: str, path: str, body: Any) -> None:
    try:
        _call(ctx, method, path, json_body=body)
    except Exception:  # best effort only
        pass


@_check(JOB)
def check_serverless_job(ctx: ProbeContext) -> tuple[str, str]:
    """Import the probe notebook, submit it as a serverless one-off run, read its output."""
    if not ctx.user_name:
        return FAIL, "current user unknown (auth check failed); cannot place the probe notebook"
    ctx.redact.add(ctx.user_name)
    notebook_path = f"/Users/{ctx.user_name}/{NOTEBOOK_NAME}"
    _call(
        ctx,
        "POST",
        "/api/2.0/workspace/import",
        json_body={
            "path": notebook_path,
            "format": "SOURCE",
            "language": "PYTHON",
            "overwrite": True,
            "content": base64.b64encode(NOTEBOOK_SOURCE.encode()).decode(),
        },
    )
    submit = {
        "run_name": RUN_NAME,
        "tasks": [{"task_key": "probe", "notebook_task": {"notebook_path": notebook_path}}],
    }
    run_id = _json(_call(ctx, "POST", "/api/2.1/jobs/runs/submit", json_body=submit))["run_id"]

    deadline = ctx.clock() + ctx.job_timeout
    while True:
        info = _json(_call(ctx, "GET", "/api/2.1/jobs/runs/get", query={"run_id": run_id}))
        state = info.get("state") or {}
        life_cycle = state.get("life_cycle_state", "")
        if life_cycle in _RUN_TERMINAL:
            break
        if ctx.clock() >= deadline:
            _cancel_quietly(ctx, "POST", "/api/2.1/jobs/runs/cancel", {"run_id": run_id})
            return FAIL, f"timed out waiting for the serverless run (last state {life_cycle})"
        ctx.sleep(ctx.poll_interval)

    result_state = state.get("result_state", "")
    if result_state != "SUCCESS":
        message = state.get("state_message", "")
        return FAIL, f"serverless run ended {life_cycle}/{result_state}: {message}"

    ctx.job_succeeded = True
    tasks = info.get("tasks") or []
    task_run_id = tasks[0].get("run_id", run_id) if tasks else run_id
    try:
        out = _json(
            _call(ctx, "GET", "/api/2.1/jobs/runs/get-output", query={"run_id": task_run_id})
        )
        raw = (out.get("notebook_output") or {}).get("result")
        parsed = json.loads(raw) if isinstance(raw, str) else None
        if isinstance(parsed, dict):
            ctx.notebook_output = parsed
        else:
            ctx.notebook_error = "notebook output is not a JSON object"
    except (ApiError, ValueError) as exc:
        ctx.notebook_error = f"could not read notebook output: {exc}"
    return PASS, "serverless notebook run (no cluster declared) finished with SUCCESS"


@_check(NOTEBOOK_GHARCHIVE, needs=())
def check_notebook_gharchive(ctx: ProbeContext) -> tuple[str, str]:
    """Info only: did the serverless notebook manage to read from data.gharchive.org?"""
    if not ctx.job_succeeded:
        return SKIPPED, "serverless job did not finish successfully; download not attempted"
    if ctx.notebook_error or ctx.notebook_output is None:
        return FAIL, ctx.notebook_error or "notebook returned no output"
    if ctx.notebook_output.get("gharchive_ok") is True:
        return PASS, "notebook read a gzip header from data.gharchive.org"
    error = ctx.notebook_output.get("error") or "unknown error"
    return FAIL, f"notebook could not download from data.gharchive.org: {error}"


@_check(SQL_WAREHOUSE)
def check_sql_warehouse(ctx: ProbeContext) -> tuple[str, str]:
    """Resolve the warehouse http path and run ``SELECT 1`` through the Statement API."""
    if ctx.secrets.http_path:
        match = _HTTP_PATH.fullmatch(ctx.secrets.http_path)
        if not match:
            return FAIL, "DATABRICKS_HTTP_PATH is not of the form /sql/1.0/warehouses/<id>"
        warehouse_id, http_path = match.group(1), ctx.secrets.http_path
        source = "from DATABRICKS_HTTP_PATH"
    else:
        listing = _json(_call(ctx, "GET", "/api/2.0/sql/warehouses"))
        warehouses = listing.get("warehouses") or []
        if not warehouses:
            return FAIL, "no SQL warehouse found in the workspace"
        warehouse_id = str(warehouses[0]["id"])
        odbc = warehouses[0].get("odbc_params") or {}
        http_path = odbc.get("path") or f"/sql/1.0/warehouses/{warehouse_id}"
        source = "discovered via the SQL warehouses API"
    ctx.redact.add(warehouse_id)
    ctx.redact.add(http_path)
    ctx.http_path = http_path

    body = {
        "statement": "SELECT 1",
        "warehouse_id": warehouse_id,
        "wait_timeout": "30s",
        "on_wait_timeout": "CONTINUE",
    }
    stmt = _json(_call(ctx, "POST", "/api/2.0/sql/statements", json_body=body))
    statement_id = stmt.get("statement_id", "")
    state = (stmt.get("status") or {}).get("state", "")
    deadline = ctx.clock() + ctx.statement_timeout
    while state in _STATEMENT_WAITING:
        if ctx.clock() >= deadline:
            cancel = f"/api/2.0/sql/statements/{statement_id}/cancel"
            _cancel_quietly(ctx, "POST", cancel, {})
            return FAIL, f"timed out waiting for SELECT 1 (last state {state})"
        ctx.sleep(ctx.poll_interval)
        stmt = _json(_call(ctx, "GET", f"/api/2.0/sql/statements/{statement_id}"))
        state = (stmt.get("status") or {}).get("state", "")
    if state != "SUCCEEDED":
        error = ((stmt.get("status") or {}).get("error") or {}).get("message", "")
        return FAIL, f"SELECT 1 ended {state}: {error}"
    return PASS, f"SELECT 1 succeeded on the SQL warehouse (http path {source})"


# --- MotherDuck ---------------------------------------------------------------


def _duckdb_connect(database: str) -> Any:
    try:
        import duckdb
    except ImportError as exc:
        raise RuntimeError("duckdb is not installed (pip install duckdb)") from exc
    return duckdb.connect(database)


@_check(MOTHERDUCK, needs=("MOTHERDUCK_TOKEN",))
def check_motherduck(ctx: ProbeContext) -> tuple[str, str]:
    """Connect to ``md:`` (duckdb reads MOTHERDUCK_TOKEN), ensure db ``repopulse``, ``select 1``."""
    connect = ctx.connect or _duckdb_connect
    con = connect("md:")
    try:
        con.execute("CREATE DATABASE IF NOT EXISTS repopulse")
        row = con.execute("select 1").fetchone()
    finally:
        con.close()
    if row is None or row[0] != 1:
        return FAIL, "select 1 on MotherDuck returned an unexpected row"
    return PASS, "connected to MotherDuck; database repopulse exists; select 1 ok"


# --- orchestration and decision -----------------------------------------------


CHECKS: tuple[Callable[[ProbeContext], CheckResult], ...] = (
    check_secret_shape,
    check_auth,
    check_volume,
    check_serverless_job,
    check_notebook_gharchive,
    check_sql_warehouse,
    check_motherduck,
)


def run_all(ctx: ProbeContext) -> list[CheckResult]:
    return [check(ctx) for check in CHECKS]


def decide_path(results: Iterable[CheckResult]) -> str:
    status = {r.name: r.status for r in results}
    if (
        all(status.get(n) == PASS for n in REQUIRED_DATABRICKS)
        and status.get(DBT_DATABRICKS) != FAIL
    ):
        return "primary"
    if status.get(MOTHERDUCK) == PASS and status.get(DBT_MOTHERDUCK) != FAIL:
        return "fallback"
    return "blocked"


_OUTCOMES = {
    "success": (PASS, "succeeded"),
    "failure": (FAIL, "failed (see the workflow log)"),
    "skipped": (SKIPPED, "did not run (missing secret or no warehouse http path)"),
    "cancelled": (SKIPPED, "was cancelled"),
    "": (SKIPPED, "did not run"),
}


def _dbt_result(name: str, target: str, outcome: str) -> CheckResult:
    if outcome not in _OUTCOMES:
        raise ValueError(f"unknown step outcome {outcome!r}; expected one of {sorted(_OUTCOMES)}")
    status, text = _OUTCOMES[outcome]
    return CheckResult(name, status, f"dbt debug --target {target} {text}")


def merge_dbt_outcomes(
    results: Iterable[CheckResult], *, databricks: str, motherduck: str
) -> list[CheckResult]:
    """Replace any earlier dbt debug rows with the outcomes of the two workflow steps."""
    kept = [r for r in results if r.name not in (DBT_DATABRICKS, DBT_MOTHERDUCK)]
    return kept + [
        _dbt_result(DBT_DATABRICKS, "databricks", databricks),
        _dbt_result(DBT_MOTHERDUCK, "motherduck", motherduck),
    ]


# --- reports ------------------------------------------------------------------


_DECISION_TEXT = {
    "primary": "Databricks Free Edition (serverless) driven from GitHub Actions",
    "fallback": "PySpark + Delta on the GitHub Actions runner, gold on MotherDuck",
    "blocked": "blocked: neither Databricks nor MotherDuck is usable yet",
}


def _cell(text: str) -> str:
    return " ".join(text.split()).replace("|", "\\|")


def render_markdown(results: Sequence[CheckResult], decision: str) -> str:
    lines = [
        "# RepoPulse cloud probe",
        "",
        f"**Decision: {decision}** - {_DECISION_TEXT.get(decision, decision)}",
        "",
        "| Check | Status | Detail | Role |",
        "|---|---|---|---|",
    ]
    for r in results:
        role = "decides path" if r.name in DECIDES_PATH else "info only"
        lines.append(f"| `{r.name}` | {r.status} | {_cell(r.detail)} | {role} |")
    lines += [
        "",
        "`primary` needs every Databricks check (auth, volume upload, serverless job, SQL "
        "warehouse) to pass and `dbt debug --target databricks` not to fail. `fallback` needs "
        "MotherDuck to pass and `dbt debug --target motherduck` not to fail. The GH Archive "
        "download from the notebook is informational: Actions downloads and pushes the files.",
        "",
        "Only pass/fail/skipped is reported here; measurements live in `runs.csv`.",
        "",
    ]
    return "\n".join(lines)


def to_json(results: Sequence[CheckResult], decision: str) -> str:
    checks = [{**asdict(r), "decides_path": r.name in DECIDES_PATH} for r in results]
    data = {"version": 1, "decision": decision, "checks": checks}
    return json.dumps(data, indent=2, ensure_ascii=False) + "\n"


def from_json(text: str) -> tuple[list[CheckResult], str]:
    data = json.loads(text)
    if data.get("version") != 1:
        raise ValueError(f"unsupported probe result version {data.get('version')!r}")
    decision = data.get("decision")
    if decision not in DECISIONS:
        raise ValueError(f"decision must be one of {DECISIONS}, got {decision!r}")
    results = [CheckResult(c["name"], c["status"], c["detail"]) for c in data["checks"]]
    return results, decision


# --- CLI ----------------------------------------------------------------------


def _write(path: str, text: str) -> None:
    with open(path, "w", encoding="utf-8", newline="\n") as f:
        f.write(text)


def _append(path: str, text: str) -> None:
    with open(path, "a", encoding="utf-8", newline="\n") as f:
        f.write(text)


def _publish(
    results: list[CheckResult], args: argparse.Namespace, env: Mapping[str, str], json_out: str
) -> str:
    decision = decide_path(results)
    markdown = render_markdown(results, decision)
    _write(json_out, to_json(results, decision))
    _write(args.markdown_out, markdown)
    summary = env.get("GITHUB_STEP_SUMMARY")
    if summary and not args.no_step_summary:
        _append(summary, markdown + "\n")
    for r in results:
        print(f"{r.name}: {r.status} - {r.detail}")
    print(f"decision: {decision}")
    return decision


def _main_run(argv: list[str], env: Mapping[str, str], **deps: Any) -> int:
    parser = argparse.ArgumentParser(prog="python -m repopulse.probe")
    parser.add_argument("--json-out", required=True)
    parser.add_argument("--markdown-out", required=True)
    parser.add_argument("--no-step-summary", action="store_true")
    parser.add_argument(
        "--github-env",
        action="store_true",
        help="export the resolved warehouse http path as PROBE_HTTP_PATH (masked) to $GITHUB_ENV",
    )
    args = parser.parse_args(argv)
    ctx = ProbeContext(
        Secrets.from_env(env),
        deps.get("transport") or urllib_transport,
        sleep=deps.get("sleep") or time.sleep,
        clock=deps.get("clock") or time.monotonic,
        connect=deps.get("connect"),
    )
    results = run_all(ctx)
    gh_env = env.get("GITHUB_ENV")
    if args.github_env:
        # GitHub only masks a secret exactly as stored; later steps (`dbt debug`) print the bare
        # hostname and the warehouse path, so register those forms with the log masker too.
        netloc = urlsplit(ctx.secrets.host).netloc if ctx.secrets.host else ""
        match = _HTTP_PATH.search(ctx.http_path or "")
        for value in (netloc, ctx.user_name, ctx.http_path, match.group(1) if match else None):
            if value:
                print(f"::add-mask::{value}")
    if args.github_env and gh_env and ctx.http_path:
        _append(gh_env, f"PROBE_HTTP_PATH={ctx.http_path}\n")
    _publish(results, args, env, args.json_out)
    return 0


def _main_merge(argv: list[str], env: Mapping[str, str]) -> int:
    parser = argparse.ArgumentParser(prog="python -m repopulse.probe merge-dbt")
    parser.add_argument("--json", required=True, help="probe-result.json, rewritten in place")
    parser.add_argument("--markdown-out", required=True)
    parser.add_argument("--databricks", default="", help="steps.<id>.outcome of dbt debug")
    parser.add_argument("--motherduck", default="", help="steps.<id>.outcome of dbt debug")
    parser.add_argument("--no-step-summary", action="store_true")
    args = parser.parse_args(argv)
    with open(args.json, encoding="utf-8") as f:
        results, _ = from_json(f.read())
    merged = merge_dbt_outcomes(results, databricks=args.databricks, motherduck=args.motherduck)
    _publish(merged, args, env, args.json)
    return 0


def main(
    argv: Sequence[str] | None = None,
    *,
    env: Mapping[str, str] | None = None,
    transport: Transport | None = None,
    connect: Callable[[str], Any] | None = None,
    sleep: Callable[[float], None] | None = None,
    clock: Callable[[], float] | None = None,
) -> int:
    """Exit 0 once the reports are written (failed checks are data); 1 if the script breaks."""
    env = os.environ if env is None else env
    args = list(sys.argv[1:] if argv is None else argv)
    try:
        if args and args[0] == "merge-dbt":
            return _main_merge(args[1:], env)
        return _main_run(args, env, transport=transport, connect=connect, sleep=sleep, clock=clock)
    except Exception as exc:
        redact = Secrets.from_env(env).redactor()
        print(f"probe: error: {redact(f'{type(exc).__name__}: {exc}')}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
