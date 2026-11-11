import base64
import io
import json
import re
import sys
import urllib.error
import urllib.request
from dataclasses import fields
from urllib.parse import parse_qs, urlsplit

import pytest

from repopulse import probe
from repopulse.probe import (
    AUTH,
    DBT_DATABRICKS,
    DBT_MOTHERDUCK,
    FAIL,
    JOB,
    MOTHERDUCK,
    NOTEBOOK_GHARCHIVE,
    PASS,
    REQUIRED_DATABRICKS,
    SKIPPED,
    SQL_WAREHOUSE,
    VOLUME,
    CheckResult,
    HttpRequest,
    HttpResponse,
    ProbeContext,
    Redactor,
    Secrets,
    decide_path,
    from_json,
    merge_dbt_outcomes,
    render_markdown,
    run_all,
    to_json,
)

HOST = "https://dbc-abc123.cloud.databricks.com"
NETLOC = "dbc-abc123.cloud.databricks.com"
TOKEN = "dapiSECRET123"
MD_TOKEN = "mdSECRETtoken456"
USER = "lam@example.com"
HTTP_PATH = "/sql/1.0/warehouses/wh123"
SECRET_VALUES = (HOST, NETLOC, TOKEN, MD_TOKEN)

FULL_ENV = {
    "DATABRICKS_HOST": HOST,
    "DATABRICKS_TOKEN": TOKEN,
    "MOTHERDUCK_TOKEN": MD_TOKEN,
}

VOLUME_FILE = "/api/2.0/fs/files/Volumes/workspace/default/repopulse_probe/probe/hello.txt"
SCIM_ME = "/api/2.0/preview/scim/v2/Me"
VOLUMES = "/api/2.1/unity-catalog/volumes"
IMPORT = "/api/2.0/workspace/import"
SUBMIT = "/api/2.1/jobs/runs/submit"
RUNS_GET = "/api/2.1/jobs/runs/get"
RUNS_OUTPUT = "/api/2.1/jobs/runs/get-output"
RUNS_CANCEL = "/api/2.1/jobs/runs/cancel"
WAREHOUSES = "/api/2.0/sql/warehouses"
STATEMENTS = "/api/2.0/sql/statements"

DATABRICKS_CHECKS = (AUTH, VOLUME, JOB, SQL_WAREHOUSE)
TIME_UNITS = re.compile(
    r"\d+(\.\d+)?\s?(ms|s|sec|secs|seconds|min|mins|minutes|giây|phút)\b", re.IGNORECASE
)


# --- fakes --------------------------------------------------------------------


def ok(obj, status=200):
    return HttpResponse(status, json.dumps(obj).encode())


def err(status, message="denied"):
    return HttpResponse(status, json.dumps({"error_code": "ERR", "message": message}).encode())


class FakeTransport:
    """Routes keyed by (method, path); a list is consumed in order, its last item repeats."""

    def __init__(self, routes=None):
        self.routes = {
            k: list(v) if isinstance(v, list) else [v] for k, v in (routes or {}).items()
        }
        self.calls = []

    def __call__(self, req):
        self.calls.append(req)
        key = (req.method, urlsplit(req.url).path)
        if key not in self.routes:
            return err(404, f"no route for {key}")
        seq = self.routes[key]
        item = seq.pop(0) if len(seq) > 1 else seq[0]
        if isinstance(item, BaseException):
            raise item
        if callable(item):
            item = item(req)
        return item

    def paths(self, method=None):
        return [urlsplit(c.url).path for c in self.calls if method in (None, c.method)]

    def bodies(self, method, path):
        return [
            json.loads(c.body)
            for c in self.calls
            if c.method == method and urlsplit(c.url).path == path
        ]


class FakeClock:
    def __init__(self):
        self.now = 0.0
        self.sleeps = []

    def __call__(self):
        return self.now

    def sleep(self, seconds):
        self.sleeps.append(seconds)
        self.now += seconds


class FakeCursor:
    def __init__(self, row):
        self.row = row

    def fetchone(self):
        return self.row


class FakeConnection:
    def __init__(self, select_row=(1,)):
        self.sql = []
        self.select_row = select_row
        self.closed = False

    def execute(self, sql):
        self.sql.append(sql)
        return FakeCursor(self.select_row if "select 1" in sql.lower() else None)

    def close(self):
        self.closed = True


class FakeConnect:
    def __init__(self, conn=None, exc=None):
        self.conn = conn or FakeConnection()
        self.exc = exc
        self.calls = []

    def __call__(self, database):
        self.calls.append(database)
        if self.exc is not None:
            raise self.exc
        return self.conn


def run_state(life_cycle, result=None, message="", tasks=None):
    state = {"life_cycle_state": life_cycle, "state_message": message}
    if result is not None:
        state["result_state"] = result
    body = {"run_id": 11, "state": state}
    if tasks is not None:
        body["tasks"] = tasks
    return ok(body)


def happy_routes(notebook_result=None):
    if notebook_result is None:
        notebook_result = {"gharchive_ok": True}
    return {
        ("GET", SCIM_ME): ok({"userName": USER}),
        ("POST", VOLUMES): ok({"name": "repopulse_probe"}),
        ("PUT", VOLUME_FILE): HttpResponse(204, b""),
        ("GET", VOLUME_FILE): HttpResponse(200, probe.PROBE_FILE_CONTENT),
        ("POST", IMPORT): ok({}),
        ("POST", SUBMIT): ok({"run_id": 11}),
        ("GET", RUNS_GET): [
            run_state("PENDING"),
            run_state("RUNNING"),
            run_state("TERMINATED", "SUCCESS", tasks=[{"run_id": 12, "task_key": "probe"}]),
        ],
        ("GET", RUNS_OUTPUT): ok({"notebook_output": {"result": json.dumps(notebook_result)}}),
        ("POST", RUNS_CANCEL): ok({}),
        ("GET", WAREHOUSES): ok(
            {
                "warehouses": [
                    {"id": "wh123", "name": "Serverless", "odbc_params": {"path": HTTP_PATH}}
                ]
            }
        ),
        ("POST", STATEMENTS): ok({"statement_id": "st1", "status": {"state": "SUCCEEDED"}}),
    }


def make_ctx(env=None, routes=None, connect=None, **kwargs):
    transport = FakeTransport(happy_routes() if routes is None else routes)
    clock = FakeClock()
    ctx = ProbeContext(
        Secrets.from_env(FULL_ENV if env is None else env),
        transport,
        sleep=clock.sleep,
        clock=clock,
        connect=connect if connect is not None else FakeConnect(),
        **kwargs,
    )
    return ctx, transport, clock


def results_with(**statuses):
    return [CheckResult(name, status, f"{name} detail") for name, status in statuses.items()]


def all_databricks(status=PASS, **overrides):
    statuses = {name: status for name in DATABRICKS_CHECKS}
    statuses.update(overrides)
    return statuses


def assert_no_secret(text):
    for value in SECRET_VALUES:
        assert value not in text, f"secret value leaked: {value!r} in {text!r}"


def by_name(results):
    return {r.name: r for r in results}


# --- constants and data shape -------------------------------------------------


def test_required_databricks_checks_are_the_four_cloud_capabilities():
    assert set(REQUIRED_DATABRICKS) == {AUTH, VOLUME, JOB, SQL_WAREHOUSE}
    assert NOTEBOOK_GHARCHIVE not in REQUIRED_DATABRICKS


def test_check_names_are_distinct():
    names = [AUTH, VOLUME, JOB, SQL_WAREHOUSE, NOTEBOOK_GHARCHIVE, MOTHERDUCK]
    names += [DBT_DATABRICKS, DBT_MOTHERDUCK]
    assert len(set(names)) == len(names)


def test_status_values():
    assert (PASS, FAIL, SKIPPED) == ("pass", "fail", "skipped")


def test_check_result_has_no_measurement_fields():
    assert [f.name for f in fields(CheckResult)] == ["name", "status", "detail"]


def test_check_result_rejects_unknown_status():
    with pytest.raises(ValueError, match="status"):
        CheckResult(AUTH, "ok", "x")


# --- decide_path --------------------------------------------------------------


def test_primary_when_all_four_databricks_checks_pass():
    assert decide_path(results_with(**all_databricks(), **{MOTHERDUCK: FAIL})) == "primary"


def test_primary_does_not_need_motherduck():
    assert decide_path(results_with(**all_databricks())) == "primary"


@pytest.mark.parametrize("missing", DATABRICKS_CHECKS)
@pytest.mark.parametrize("bad", [FAIL, SKIPPED])
def test_fallback_when_any_required_check_not_pass_and_motherduck_passes(missing, bad):
    statuses = all_databricks(**{missing: bad})
    statuses[MOTHERDUCK] = PASS
    assert decide_path(results_with(**statuses)) == "fallback"


@pytest.mark.parametrize("missing", DATABRICKS_CHECKS)
def test_absent_required_check_is_not_primary(missing):
    statuses = all_databricks()
    del statuses[missing]
    statuses[MOTHERDUCK] = PASS
    assert decide_path(results_with(**statuses)) == "fallback"


@pytest.mark.parametrize("md", [FAIL, SKIPPED, None])
def test_blocked_when_databricks_incomplete_and_motherduck_not_pass(md):
    statuses = all_databricks(**{JOB: FAIL})
    if md is not None:
        statuses[MOTHERDUCK] = md
    assert decide_path(results_with(**statuses)) == "blocked"


def test_blocked_when_everything_skipped():
    statuses = all_databricks(SKIPPED, **{MOTHERDUCK: SKIPPED, NOTEBOOK_GHARCHIVE: SKIPPED})
    assert decide_path(results_with(**statuses)) == "blocked"


@pytest.mark.parametrize("gh", [FAIL, SKIPPED])
def test_notebook_gharchive_download_does_not_block_primary(gh):
    statuses = all_databricks(**{NOTEBOOK_GHARCHIVE: gh})
    assert decide_path(results_with(**statuses)) == "primary"


def test_notebook_gharchive_pass_does_not_rescue_incomplete_databricks():
    statuses = all_databricks(**{VOLUME: FAIL, NOTEBOOK_GHARCHIVE: PASS, MOTHERDUCK: FAIL})
    assert decide_path(results_with(**statuses)) == "blocked"


def test_dbt_debug_databricks_failure_rules_out_primary():
    statuses = all_databricks(**{DBT_DATABRICKS: FAIL, MOTHERDUCK: PASS})
    assert decide_path(results_with(**statuses)) == "fallback"


@pytest.mark.parametrize("dbt", [PASS, SKIPPED])
def test_dbt_debug_databricks_pass_or_not_run_keeps_primary(dbt):
    statuses = all_databricks(**{DBT_DATABRICKS: dbt})
    assert decide_path(results_with(**statuses)) == "primary"


def test_dbt_debug_motherduck_failure_rules_out_fallback():
    statuses = all_databricks(**{JOB: FAIL, MOTHERDUCK: PASS, DBT_MOTHERDUCK: FAIL})
    assert decide_path(results_with(**statuses)) == "blocked"


@pytest.mark.parametrize("dbt", [PASS, SKIPPED])
def test_dbt_debug_motherduck_pass_or_not_run_keeps_fallback(dbt):
    statuses = all_databricks(**{JOB: FAIL, MOTHERDUCK: PASS, DBT_MOTHERDUCK: dbt})
    assert decide_path(results_with(**statuses)) == "fallback"


# --- secrets and redaction ----------------------------------------------------


def test_secrets_from_env_normalises_host():
    s = Secrets.from_env({"DATABRICKS_HOST": NETLOC + "/", "DATABRICKS_TOKEN": TOKEN})
    assert s.host == HOST
    assert s.token == TOKEN
    assert s.http_path == ""
    assert s.motherduck_token == ""


def test_secrets_from_env_strips_whitespace_and_reads_http_path():
    s = Secrets.from_env({"DATABRICKS_HOST": f"  {HOST}/ \n", "DATABRICKS_HTTP_PATH": HTTP_PATH})
    assert s.host == HOST
    assert s.http_path == HTTP_PATH


def test_redactor_masks_host_netloc_and_tokens():
    r = Redactor([HOST, NETLOC, TOKEN, MD_TOKEN])
    text = f"GET {HOST}/api failed for {NETLOC} with {TOKEN} and {MD_TOKEN}"
    out = r(text)
    assert_no_secret(out)
    assert "GET" in out and "failed" in out


def test_redactor_masks_unknown_databricks_token_shapes():
    out = Redactor([])("header was Bearer dapi0123abcdef4567 oops")
    assert "dapi0123abcdef4567" not in out


def test_redactor_ignores_empty_values():
    assert Redactor(["", None])("plain text") == "plain text"


def test_redactor_add_masks_values_learned_later():
    r = Redactor([])
    r.add(USER)
    assert USER not in r(f"user {USER} not allowed")


def test_secrets_redactor_covers_host_netloc_and_both_tokens():
    r = Secrets.from_env(FULL_ENV).redactor()
    assert_no_secret(r(" ".join(SECRET_VALUES)))


DATABRICKS_CHECK_FUNCS = {
    AUTH: probe.check_auth,
    VOLUME: probe.check_volume,
    JOB: probe.check_serverless_job,
    SQL_WAREHOUSE: probe.check_sql_warehouse,
}


@pytest.mark.parametrize("name", DATABRICKS_CHECKS)
def test_missing_all_databricks_secrets_skips_without_calling_transport(name):
    ctx, transport, _ = make_ctx(env={})
    result = DATABRICKS_CHECK_FUNCS[name](ctx)
    assert result.name == name
    assert result.status == SKIPPED
    assert "DATABRICKS_HOST" in result.detail
    assert "DATABRICKS_TOKEN" in result.detail
    assert transport.calls == []


@pytest.mark.parametrize("name", DATABRICKS_CHECKS)
def test_missing_token_only_names_the_token(name):
    ctx, transport, _ = make_ctx(env={"DATABRICKS_HOST": HOST})
    result = DATABRICKS_CHECK_FUNCS[name](ctx)
    assert result.status == SKIPPED
    assert "DATABRICKS_TOKEN" in result.detail
    assert "DATABRICKS_HOST" not in result.detail
    assert transport.calls == []


def test_missing_motherduck_token_skips_without_connecting():
    connect = FakeConnect()
    ctx, _, _ = make_ctx(env={"DATABRICKS_HOST": HOST, "DATABRICKS_TOKEN": TOKEN}, connect=connect)
    result = probe.check_motherduck(ctx)
    assert result.status == SKIPPED
    assert "MOTHERDUCK_TOKEN" in result.detail
    assert connect.calls == []


def test_run_all_with_no_secrets_skips_everything_and_never_calls_out():
    connect = FakeConnect()
    ctx, transport, _ = make_ctx(env={}, connect=connect)
    results = run_all(ctx)
    assert {r.name for r in results} == {
        AUTH,
        VOLUME,
        JOB,
        NOTEBOOK_GHARCHIVE,
        SQL_WAREHOUSE,
        MOTHERDUCK,
    }
    assert all(r.status == SKIPPED for r in results)
    assert transport.calls == []
    assert connect.calls == []
    assert decide_path(results) == "blocked"


def echo_secrets(req):
    return err(401, f"invalid token {TOKEN} for {HOST} ({NETLOC}) auth={req.headers}")


@pytest.mark.parametrize("name", DATABRICKS_CHECKS)
def test_server_error_echoing_secrets_is_redacted(name):
    routes = {key: echo_secrets for key in happy_routes()}
    env = dict(FULL_ENV, DATABRICKS_HTTP_PATH=HTTP_PATH)
    ctx, _, _ = make_ctx(env=env, routes=routes)
    ctx.user_name = USER
    result = DATABRICKS_CHECK_FUNCS[name](ctx)
    assert result.status == FAIL
    assert "401" in result.detail
    assert_no_secret(result.detail)
    assert USER not in result.detail


@pytest.mark.parametrize("name", DATABRICKS_CHECKS)
def test_network_error_mentioning_host_is_fail_and_redacted(name):
    boom = urllib.error.URLError(f"cannot reach {NETLOC} via {HOST}")
    routes = {key: boom for key in happy_routes()}
    ctx, _, _ = make_ctx(routes=routes)
    ctx.user_name = USER
    result = DATABRICKS_CHECK_FUNCS[name](ctx)
    assert result.status == FAIL
    assert result.detail
    assert_no_secret(result.detail)


def test_motherduck_error_echoing_token_is_redacted():
    connect = FakeConnect(exc=RuntimeError(f"Invalid token {MD_TOKEN}"))
    ctx, _, _ = make_ctx(connect=connect)
    result = probe.check_motherduck(ctx)
    assert result.status == FAIL
    assert "Invalid token" in result.detail
    assert_no_secret(result.detail)


def test_full_run_with_echoing_server_keeps_reports_clean():
    routes = {key: echo_secrets for key in happy_routes()}
    connect = FakeConnect(exc=RuntimeError(f"bad {MD_TOKEN} at {HOST}"))
    ctx, _, _ = make_ctx(routes=routes, connect=connect)
    results = run_all(ctx)
    decision = decide_path(results)
    assert_no_secret(render_markdown(results, decision))
    assert_no_secret(to_json(results, decision))


def test_user_name_from_scim_never_reaches_the_report():
    ctx, _, _ = make_ctx()
    results = run_all(ctx)
    decision = decide_path(results)
    assert USER not in render_markdown(results, decision)
    assert USER not in to_json(results, decision)


# --- auth ---------------------------------------------------------------------


def test_auth_pass_and_sends_bearer_token_to_host():
    ctx, transport, _ = make_ctx()
    result = probe.check_auth(ctx)
    assert result.status == PASS
    req = transport.calls[0]
    assert req.method == "GET"
    assert req.url == HOST + SCIM_ME
    assert req.headers["Authorization"] == "Bearer " + TOKEN
    assert ctx.user_name == USER


@pytest.mark.parametrize("status", [401, 403, 500])
def test_auth_http_error_is_fail(status):
    ctx, _, _ = make_ctx(routes={("GET", SCIM_ME): err(status)})
    result = probe.check_auth(ctx)
    assert result.status == FAIL
    assert str(status) in result.detail
    assert ctx.user_name is None


def test_auth_unexpected_exception_does_not_escape():
    ctx, _, _ = make_ctx(routes={("GET", SCIM_ME): RuntimeError("kaboom")})
    result = probe.check_auth(ctx)
    assert result.status == FAIL
    assert "kaboom" in result.detail


def test_auth_response_without_user_name_is_fail():
    ctx, _, _ = make_ctx(routes={("GET", SCIM_ME): ok({"id": "1"})})
    assert probe.check_auth(ctx).status == FAIL


def test_one_broken_check_does_not_stop_the_others():
    routes = happy_routes()
    routes[("GET", SCIM_ME)] = RuntimeError("scim exploded")
    ctx, transport, _ = make_ctx(routes=routes)
    res = by_name(run_all(ctx))
    assert res[AUTH].status == FAIL
    assert res[VOLUME].status == PASS
    assert res[SQL_WAREHOUSE].status == PASS
    assert res[MOTHERDUCK].status == PASS
    assert res[JOB].status == FAIL
    assert VOLUME_FILE in transport.paths("PUT")


# --- volume -------------------------------------------------------------------


def test_volume_pass_creates_volume_uploads_and_reads_back():
    ctx, transport, _ = make_ctx()
    result = probe.check_volume(ctx)
    assert result.status == PASS
    create = transport.bodies("POST", VOLUMES)[0]
    assert create == {
        "catalog_name": "workspace",
        "schema_name": "default",
        "name": "repopulse_probe",
        "volume_type": "MANAGED",
    }
    put = [c for c in transport.calls if c.method == "PUT"][0]
    assert urlsplit(put.url).path == VOLUME_FILE
    assert put.body == probe.PROBE_FILE_CONTENT
    assert 0 < len(probe.PROBE_FILE_CONTENT) < 1024
    assert transport.paths("GET") == [VOLUME_FILE]


@pytest.mark.parametrize(
    "existing",
    [
        err(409, "Volume 'workspace.default.repopulse_probe' already exists"),
        HttpResponse(
            400,
            json.dumps({"error_code": "RESOURCE_ALREADY_EXISTS", "message": "exists"}).encode(),
        ),
    ],
)
def test_volume_already_exists_is_ok(existing):
    routes = happy_routes()
    routes[("POST", VOLUMES)] = existing
    ctx, _, _ = make_ctx(routes=routes)
    assert probe.check_volume(ctx).status == PASS


def test_volume_create_forbidden_is_fail():
    routes = happy_routes()
    routes[("POST", VOLUMES)] = err(403, "no CREATE VOLUME privilege")
    ctx, transport, _ = make_ctx(routes=routes)
    result = probe.check_volume(ctx)
    assert result.status == FAIL
    assert "403" in result.detail
    assert transport.paths("PUT") == []


def test_volume_upload_forbidden_is_fail():
    routes = happy_routes()
    routes[("PUT", VOLUME_FILE)] = err(403)
    ctx, _, _ = make_ctx(routes=routes)
    result = probe.check_volume(ctx)
    assert result.status == FAIL
    assert "403" in result.detail


def test_volume_read_back_mismatch_is_fail():
    routes = happy_routes()
    routes[("GET", VOLUME_FILE)] = HttpResponse(200, b"something else")
    ctx, _, _ = make_ctx(routes=routes)
    result = probe.check_volume(ctx)
    assert result.status == FAIL
    assert "content" in result.detail.lower()


def test_volume_uses_configured_catalog_schema_and_name():
    path = "/api/2.0/fs/files/Volumes/main/probe_s/probe_v/probe/hello.txt"
    routes = happy_routes()
    routes[("PUT", path)] = HttpResponse(204, b"")
    routes[("GET", path)] = HttpResponse(200, probe.PROBE_FILE_CONTENT)
    ctx, transport, _ = make_ctx(routes=routes, catalog="main", schema="probe_s", volume="probe_v")
    assert probe.check_volume(ctx).status == PASS
    assert transport.bodies("POST", VOLUMES)[0]["catalog_name"] == "main"
    assert path in transport.paths("PUT")


# --- serverless job and notebook ----------------------------------------------


def job_ctx(routes=None, **kwargs):
    ctx, transport, clock = make_ctx(routes=routes, **kwargs)
    ctx.user_name = USER
    return ctx, transport, clock


def test_job_pass_imports_notebook_submits_serverless_run_and_reads_output():
    ctx, transport, clock = job_ctx()
    result = probe.check_serverless_job(ctx)
    assert result.status == PASS

    imp = transport.bodies("POST", IMPORT)[0]
    assert imp["path"] == f"/Users/{USER}/repopulse_probe_notebook"
    assert imp["format"] == "SOURCE"
    assert imp["language"] == "PYTHON"
    assert imp["overwrite"] is True
    source = base64.b64decode(imp["content"]).decode()
    assert "https://data.gharchive.org/2026-10-25-12.json.gz" in source
    assert "dbutils.notebook.exit" in source
    assert "gharchive_ok" in source

    submit = transport.bodies("POST", SUBMIT)[0]
    assert submit["run_name"] == "repopulse-probe"
    assert len(submit["tasks"]) == 1
    task = submit["tasks"][0]
    assert task["notebook_task"]["notebook_path"] == imp["path"]
    for cluster_key in ("new_cluster", "existing_cluster_id", "job_cluster_key"):
        assert cluster_key not in task
        assert cluster_key not in submit

    out_calls = [c for c in transport.calls if urlsplit(c.url).path == RUNS_OUTPUT]
    assert parse_qs(urlsplit(out_calls[0].url).query) == {"run_id": ["12"]}
    assert ctx.notebook_output == {"gharchive_ok": True}
    assert len(clock.sleeps) == 2
    assert probe.check_notebook_gharchive(ctx).status == PASS


def test_job_polls_with_run_id():
    ctx, transport, _ = job_ctx()
    probe.check_serverless_job(ctx)
    polls = [c for c in transport.calls if urlsplit(c.url).path == RUNS_GET]
    assert len(polls) == 3
    assert all(parse_qs(urlsplit(c.url).query) == {"run_id": ["11"]} for c in polls)


def test_notebook_that_cannot_reach_gharchive_fails_only_the_info_check():
    routes = happy_routes({"gharchive_ok": False, "error": "URLError: domain not allowed"})
    ctx, _, _ = job_ctx(routes=routes)
    assert probe.check_serverless_job(ctx).status == PASS
    info = probe.check_notebook_gharchive(ctx)
    assert info.status == FAIL
    assert "domain not allowed" in info.detail


def test_notebook_output_not_json_fails_info_check_but_job_passes():
    routes = happy_routes()
    routes[("GET", RUNS_OUTPUT)] = ok({"notebook_output": {"result": "not json"}})
    ctx, _, _ = job_ctx(routes=routes)
    assert probe.check_serverless_job(ctx).status == PASS
    assert probe.check_notebook_gharchive(ctx).status == FAIL


def test_job_run_failed_is_fail_with_redacted_state_message():
    routes = happy_routes()
    routes[("GET", RUNS_GET)] = run_state(
        "INTERNAL_ERROR", "FAILED", message=f"cluster denied for {HOST} token {TOKEN}"
    )
    ctx, transport, _ = job_ctx(routes=routes)
    result = probe.check_serverless_job(ctx)
    assert result.status == FAIL
    assert "FAILED" in result.detail
    assert_no_secret(result.detail)
    assert RUNS_OUTPUT not in transport.paths()
    assert ctx.notebook_output is None
    assert probe.check_notebook_gharchive(ctx).status == SKIPPED


def test_job_timeout_is_fail_cancels_and_never_sleeps_for_real():
    routes = happy_routes()
    routes[("GET", RUNS_GET)] = run_state("RUNNING")
    ctx, transport, clock = job_ctx(routes=routes, job_timeout=100.0, poll_interval=10.0)
    result = probe.check_serverless_job(ctx)
    assert result.status == FAIL
    assert "timed out" in result.detail.lower()
    assert clock.now >= 100.0
    assert len(clock.sleeps) <= 11
    assert transport.bodies("POST", RUNS_CANCEL) == [{"run_id": 11}]
    assert probe.check_notebook_gharchive(ctx).status == SKIPPED


def test_job_submit_rejected_is_fail():
    routes = happy_routes()
    routes[("POST", SUBMIT)] = err(400, "serverless compute is not enabled")
    ctx, _, _ = job_ctx(routes=routes)
    result = probe.check_serverless_job(ctx)
    assert result.status == FAIL
    assert "serverless compute is not enabled" in result.detail


def test_job_without_user_name_is_fail_and_does_not_submit():
    ctx, transport, _ = make_ctx()
    assert ctx.user_name is None
    result = probe.check_serverless_job(ctx)
    assert result.status == FAIL
    assert "user" in result.detail.lower()
    assert SUBMIT not in transport.paths()


def test_notebook_gharchive_skipped_when_job_never_ran():
    ctx, _, _ = make_ctx(env={})
    result = probe.check_notebook_gharchive(ctx)
    assert result.status == SKIPPED
    assert result.detail


# --- SQL warehouse ------------------------------------------------------------


def test_sql_uses_http_path_from_env_without_listing_warehouses():
    env = dict(FULL_ENV, DATABRICKS_HTTP_PATH="/sql/1.0/warehouses/fromenv9")
    ctx, transport, _ = make_ctx(env=env)
    result = probe.check_sql_warehouse(ctx)
    assert result.status == PASS
    assert WAREHOUSES not in transport.paths()
    stmt = transport.bodies("POST", STATEMENTS)[0]
    assert stmt["warehouse_id"] == "fromenv9"
    assert stmt["statement"] == "SELECT 1"
    assert "wait_timeout" in stmt
    assert ctx.http_path == "/sql/1.0/warehouses/fromenv9"
    assert "DATABRICKS_HTTP_PATH" in result.detail


def test_sql_discovers_http_path_when_env_missing():
    ctx, transport, _ = make_ctx()
    result = probe.check_sql_warehouse(ctx)
    assert result.status == PASS
    assert transport.paths("GET") == [WAREHOUSES]
    assert transport.bodies("POST", STATEMENTS)[0]["warehouse_id"] == "wh123"
    assert ctx.http_path == HTTP_PATH
    assert "wh123" not in result.detail


def test_sql_builds_http_path_when_odbc_params_absent():
    routes = happy_routes()
    routes[("GET", WAREHOUSES)] = ok({"warehouses": [{"id": "abc999", "name": "w"}]})
    ctx, _, _ = make_ctx(routes=routes)
    assert probe.check_sql_warehouse(ctx).status == PASS
    assert ctx.http_path == "/sql/1.0/warehouses/abc999"


@pytest.mark.parametrize("body", [{"warehouses": []}, {}])
def test_sql_without_any_warehouse_is_fail_with_reason(body):
    routes = happy_routes()
    routes[("GET", WAREHOUSES)] = ok(body)
    ctx, transport, _ = make_ctx(routes=routes)
    result = probe.check_sql_warehouse(ctx)
    assert result.status == FAIL
    assert "no sql warehouse" in result.detail.lower()
    assert STATEMENTS not in transport.paths()
    assert ctx.http_path is None


def test_sql_statement_pending_is_polled_until_succeeded():
    routes = happy_routes()
    routes[("POST", STATEMENTS)] = ok({"statement_id": "st1", "status": {"state": "PENDING"}})
    routes[("GET", STATEMENTS + "/st1")] = [
        ok({"statement_id": "st1", "status": {"state": "RUNNING"}}),
        ok({"statement_id": "st1", "status": {"state": "SUCCEEDED"}}),
    ]
    ctx, _, clock = make_ctx(routes=routes)
    assert probe.check_sql_warehouse(ctx).status == PASS
    assert len(clock.sleeps) == 2


def test_sql_statement_never_finishing_times_out():
    routes = happy_routes()
    routes[("POST", STATEMENTS)] = ok({"statement_id": "st1", "status": {"state": "PENDING"}})
    routes[("GET", STATEMENTS + "/st1")] = ok(
        {"statement_id": "st1", "status": {"state": "PENDING"}}
    )
    ctx, _, clock = make_ctx(routes=routes, statement_timeout=50.0, poll_interval=10.0)
    result = probe.check_sql_warehouse(ctx)
    assert result.status == FAIL
    assert "timed out" in result.detail.lower()
    assert clock.now >= 50.0


def test_sql_statement_failed_is_fail():
    routes = happy_routes()
    routes[("POST", STATEMENTS)] = ok(
        {"statement_id": "st1", "status": {"state": "FAILED", "error": {"message": "no access"}}}
    )
    ctx, _, _ = make_ctx(routes=routes)
    result = probe.check_sql_warehouse(ctx)
    assert result.status == FAIL
    assert "no access" in result.detail


def test_sql_bad_http_path_is_fail():
    env = dict(FULL_ENV, DATABRICKS_HTTP_PATH="not-a-path")
    ctx, transport, _ = make_ctx(env=env)
    result = probe.check_sql_warehouse(ctx)
    assert result.status == FAIL
    assert "DATABRICKS_HTTP_PATH" in result.detail
    assert transport.calls == []


# --- MotherDuck ---------------------------------------------------------------


def test_motherduck_pass_creates_database_and_selects_one():
    connect = FakeConnect()
    ctx, _, _ = make_ctx(connect=connect)
    result = probe.check_motherduck(ctx)
    assert result.status == PASS
    assert connect.calls == ["md:"]
    sql = [s.lower() for s in connect.conn.sql]
    assert "create database if not exists repopulse" in sql
    assert "select 1" in sql
    assert connect.conn.closed is True


def test_motherduck_wrong_select_result_is_fail():
    connect = FakeConnect(FakeConnection(select_row=(2,)))
    ctx, _, _ = make_ctx(connect=connect)
    assert probe.check_motherduck(ctx).status == FAIL


def test_motherduck_default_connect_without_duckdb_is_fail_not_crash(monkeypatch):
    monkeypatch.setitem(sys.modules, "duckdb", None)
    ctx = ProbeContext(Secrets.from_env(FULL_ENV), FakeTransport())
    result = probe.check_motherduck(ctx)
    assert result.status == FAIL
    assert "duckdb" in result.detail.lower()


# --- default urllib transport (no network) ------------------------------------


class _FakeUrlResponse:
    def __init__(self, status, body):
        self.status = status
        self._body = body

    def read(self):
        return self._body

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


def test_urllib_transport_returns_status_and_body(monkeypatch):
    seen = {}

    def fake_urlopen(req, timeout):
        seen["req"] = req
        seen["timeout"] = timeout
        return _FakeUrlResponse(200, b'{"a": 1}')

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)
    req = HttpRequest("PUT", HOST + "/x", {"Authorization": "Bearer t"}, b"data")
    resp = probe.urllib_transport(req)
    assert resp == HttpResponse(200, b'{"a": 1}')
    assert seen["req"].get_method() == "PUT"
    assert seen["req"].data == b"data"
    assert seen["req"].get_header("Authorization") == "Bearer t"
    assert seen["timeout"] > 0


def test_urllib_transport_turns_http_error_into_response(monkeypatch):
    def fake_urlopen(req, timeout):
        raise urllib.error.HTTPError(req.full_url, 401, "Unauthorized", None, io.BytesIO(b"no"))

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)
    resp = probe.urllib_transport(HttpRequest("GET", HOST + "/x", {}, None))
    assert resp == HttpResponse(401, b"no")


# --- report -------------------------------------------------------------------


def sample_results():
    return [
        CheckResult(AUTH, PASS, "token accepted"),
        CheckResult(VOLUME, FAIL, "HTTP 403: no | privilege\nsecond line"),
        CheckResult(JOB, SKIPPED, "missing secret DATABRICKS_TOKEN"),
        CheckResult(SQL_WAREHOUSE, PASS, "SELECT 1 succeeded"),
        CheckResult(NOTEBOOK_GHARCHIVE, SKIPPED, "job did not run"),
        CheckResult(MOTHERDUCK, PASS, "select 1 ok"),
    ]


def test_render_markdown_lists_every_check_and_the_decision():
    results = sample_results()
    md = render_markdown(results, "fallback")
    lines = md.splitlines()
    header = [i for i, line in enumerate(lines) if re.match(r"\|\s*Check\s*\|", line)]
    assert len(header) == 1
    table = []
    for line in lines[header[0] + 2 :]:
        if not line.startswith("|"):
            break
        table.append([cell.strip() for cell in line.strip("|").split("|")])
    assert [row[0] for row in table] == [f"`{r.name}`" for r in results]
    assert [row[1] for row in table] == [r.status for r in results]
    assert table[0][2] == "token accepted"
    assert re.search(r"^\*\*Decision:\s*fallback\b", md, re.MULTILINE)


def test_render_markdown_escapes_pipes_and_newlines_in_detail():
    md = render_markdown(sample_results(), "fallback")
    row = next(line for line in md.splitlines() if f"`{VOLUME}`" in line)
    assert "no \\| privilege" in row
    assert "second line" in row


@pytest.mark.parametrize(
    "decision, words",
    [("primary", "Databricks"), ("fallback", "MotherDuck"), ("blocked", "blocked")],
)
def test_render_markdown_explains_decision(decision, words):
    assert words in render_markdown(sample_results(), decision)


def test_render_markdown_marks_informational_check():
    md = render_markdown(sample_results(), "fallback")
    row = next(line for line in md.splitlines() if f"`{NOTEBOOK_GHARCHIVE}`" in line)
    assert "info" in row.lower()


def test_json_round_trip():
    results = sample_results()
    text = to_json(results, "fallback")
    data = json.loads(text)
    assert set(data) == {"version", "decision", "checks"}
    assert data["version"] == 1
    assert data["decision"] == "fallback"
    assert set(data["checks"][0]) == {"name", "status", "detail", "decides_path"}
    assert data["checks"][0]["decides_path"] is True
    nb = next(c for c in data["checks"] if c["name"] == NOTEBOOK_GHARCHIVE)
    assert nb["decides_path"] is False
    back, decision = from_json(text)
    assert back == results
    assert decision == "fallback"


def test_from_json_rejects_bad_decision():
    with pytest.raises(ValueError, match="decision"):
        from_json(json.dumps({"version": 1, "decision": "maybe", "checks": []}))


def test_reports_quote_no_performance_numbers():
    ctx, _, _ = make_ctx()
    results = run_all(ctx)
    decision = decide_path(results)
    assert decision == "primary"
    for text in (render_markdown(results, decision), to_json(results, decision)):
        assert not TIME_UNITS.search(text), TIME_UNITS.search(text)
        for word in ("duration", "elapsed", "latency", "seconds"):
            assert word not in text.lower()


# --- dbt debug outcomes -------------------------------------------------------


@pytest.mark.parametrize(
    "outcome, status",
    [
        ("success", PASS),
        ("failure", FAIL),
        ("skipped", SKIPPED),
        ("cancelled", SKIPPED),
        ("", SKIPPED),
    ],
)
def test_merge_dbt_maps_step_outcome(outcome, status):
    merged = merge_dbt_outcomes(sample_results(), databricks=outcome, motherduck=outcome)
    res = by_name(merged)
    assert res[DBT_DATABRICKS].status == status
    assert res[DBT_MOTHERDUCK].status == status
    assert "dbt debug" in res[DBT_DATABRICKS].detail


def test_merge_dbt_keeps_other_checks_and_replaces_previous_dbt_rows():
    first = merge_dbt_outcomes(sample_results(), databricks="failure", motherduck="success")
    second = merge_dbt_outcomes(first, databricks="success", motherduck="success")
    names = [r.name for r in second]
    assert names.count(DBT_DATABRICKS) == 1
    assert names.count(DBT_MOTHERDUCK) == 1
    assert names[: len(sample_results())] == [r.name for r in sample_results()]
    assert by_name(second)[DBT_DATABRICKS].status == PASS


def test_merge_dbt_rejects_unknown_outcome():
    with pytest.raises(ValueError, match="outcome"):
        merge_dbt_outcomes(sample_results(), databricks="green", motherduck="")


# --- CLI ----------------------------------------------------------------------


def run_cli(argv, env, routes=None, connect=None):
    transport = FakeTransport(happy_routes() if routes is None else routes)
    clock = FakeClock()
    code = probe.main(
        argv,
        env=env,
        transport=transport,
        connect=connect or FakeConnect(),
        sleep=clock.sleep,
        clock=clock,
    )
    return code, transport


def test_cli_without_secrets_writes_reports_and_exits_zero(tmp_path, capsys):
    j, m = tmp_path / "r.json", tmp_path / "r.md"
    code, transport = run_cli(["--json-out", str(j), "--markdown-out", str(m)], env={})
    assert code == 0
    assert transport.calls == []
    results, decision = from_json(j.read_text(encoding="utf-8"))
    assert decision == "blocked"
    assert all(r.status == SKIPPED for r in results)
    assert "Decision" in m.read_text(encoding="utf-8")


def test_cli_full_run_is_primary_and_writes_step_summary(tmp_path, capsys):
    j, m, summary = tmp_path / "r.json", tmp_path / "r.md", tmp_path / "summary.md"
    summary.write_text("earlier\n", encoding="utf-8")
    env = dict(FULL_ENV, GITHUB_STEP_SUMMARY=str(summary))
    code, _ = run_cli(["--json-out", str(j), "--markdown-out", str(m)], env=env)
    assert code == 0
    _, decision = from_json(j.read_text(encoding="utf-8"))
    assert decision == "primary"
    text = summary.read_text(encoding="utf-8")
    assert text.startswith("earlier\n")
    assert m.read_text(encoding="utf-8") in text


def test_cli_no_step_summary_flag(tmp_path):
    summary = tmp_path / "summary.md"
    env = dict(FULL_ENV, GITHUB_STEP_SUMMARY=str(summary))
    argv = ["--json-out", str(tmp_path / "r.json"), "--markdown-out", str(tmp_path / "r.md")]
    code, _ = run_cli(argv + ["--no-step-summary"], env=env)
    assert code == 0
    assert not summary.exists()


def test_cli_github_env_exports_masked_http_path(tmp_path, capsys):
    gh_env = tmp_path / "github_env"
    env = dict(FULL_ENV, GITHUB_ENV=str(gh_env))
    argv = ["--json-out", str(tmp_path / "r.json"), "--markdown-out", str(tmp_path / "r.md")]
    code, _ = run_cli(argv + ["--github-env"], env=env)
    assert code == 0
    assert gh_env.read_text(encoding="utf-8") == f"PROBE_HTTP_PATH={HTTP_PATH}\n"
    out = capsys.readouterr().out
    assert f"::add-mask::{HTTP_PATH}" in out
    assert out.index("::add-mask::") == 0 or "\n::add-mask::" in out


def test_cli_github_env_writes_nothing_without_http_path(tmp_path):
    gh_env = tmp_path / "github_env"
    env = {"GITHUB_ENV": str(gh_env)}
    argv = ["--json-out", str(tmp_path / "r.json"), "--markdown-out", str(tmp_path / "r.md")]
    code, _ = run_cli(argv + ["--github-env"], env=env)
    assert code == 0
    assert not gh_env.exists()


def test_cli_never_prints_secrets(tmp_path, capsys):
    routes = {key: echo_secrets for key in happy_routes()}
    connect = FakeConnect(exc=RuntimeError(f"bad {MD_TOKEN}"))
    argv = ["--json-out", str(tmp_path / "r.json"), "--markdown-out", str(tmp_path / "r.md")]
    code, _ = run_cli(argv, env=FULL_ENV, routes=routes, connect=connect)
    assert code == 0
    captured = capsys.readouterr()
    assert_no_secret(captured.out + captured.err)
    assert_no_secret((tmp_path / "r.json").read_text(encoding="utf-8"))
    assert_no_secret((tmp_path / "r.md").read_text(encoding="utf-8"))


def test_cli_exits_non_zero_when_report_cannot_be_written(tmp_path):
    argv = ["--json-out", str(tmp_path / "missing" / "r.json"), "--markdown-out", str(tmp_path)]
    code, _ = run_cli(argv, env={})
    assert code != 0


def test_cli_merge_dbt_updates_reports_and_decision(tmp_path):
    j, m, summary = tmp_path / "r.json", tmp_path / "r.md", tmp_path / "summary.md"
    statuses = all_databricks(**{MOTHERDUCK: PASS})
    j.write_text(to_json(results_with(**statuses), "primary"), encoding="utf-8")
    argv = ["merge-dbt", "--json", str(j), "--markdown-out", str(m)]
    argv += ["--databricks", "failure", "--motherduck", "success"]
    code, transport = run_cli(argv, env={"GITHUB_STEP_SUMMARY": str(summary)})
    assert code == 0
    assert transport.calls == []
    results, decision = from_json(j.read_text(encoding="utf-8"))
    assert decision == "fallback"
    assert by_name(results)[DBT_DATABRICKS].status == FAIL
    assert by_name(results)[DBT_MOTHERDUCK].status == PASS
    md = m.read_text(encoding="utf-8")
    assert f"`{DBT_DATABRICKS}`" in md
    assert md in summary.read_text(encoding="utf-8")
