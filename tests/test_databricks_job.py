"""Client that runs the bronze/silver incremental batch as a serverless Databricks job (D1).

No network: every HTTP request goes to ``FakeDatabricks``, an in-memory stand-in for the REST API
that records requests; ``sleep`` and ``clock`` are fakes too, so nothing waits for real. Host,
token, user and salt are obviously fake values. GH Archive is real public data; nothing here
downloads it (the landing files are a few made-up bytes).
"""

from __future__ import annotations

import base64
import csv
import gzip
import json
import urllib.error
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

import pytest

from repopulse import databricks_job as dj
from repopulse import ingest
from repopulse.probe import ApiError, HttpResponse
from repopulse.runs_log import COLUMNS, validate_row

HOST = "https://dbc-test.cloud.databricks.com"
NETLOC = "dbc-test.cloud.databricks.com"
TOKEN = "dapiTESTTOKEN0123456789"
USER = "tester@example.com"
SALT = "pii-salt-for-tests-only"
SECRET_VALUES = (HOST, NETLOC, TOKEN, USER, SALT)

DAY = "2026-01-05"
SHA = "abc1234def"
RUN_AT = datetime(2026, 1, 5, 12, 30, tzinfo=UTC)
WHEEL_NAME = "repopulse-0.0.1-py3-none-any.whl"
ROOT = "/Volumes/workspace/repopulse/repopulse_landing"

SCIM_ME = "/api/2.0/preview/scim/v2/Me"
SCHEMAS = "/api/2.1/unity-catalog/schemas"
VOLUMES = "/api/2.1/unity-catalog/volumes"
SCOPES = "/api/2.0/secrets/scopes/create"
SECRETS_PUT = "/api/2.0/secrets/put"
FILES = "/api/2.0/fs/files"
IMPORT = "/api/2.0/workspace/import"
MKDIRS = "/api/2.0/workspace/mkdirs"
SUBMIT = "/api/2.1/jobs/runs/submit"
RUNS_GET = "/api/2.1/jobs/runs/get"
RUNS_OUTPUT = "/api/2.1/jobs/runs/get-output"
RUNS_CANCEL = "/api/2.1/jobs/runs/cancel"
TASK_OFFSET = 1000

SUMMARY = {
    "source": "gharchive",
    "hours": [[DAY, 9], [DAY, 10]],
    "bronze_rows": 9,
    "silver_inserted": 5,
    "duplicates_dropped": 1,
    "quarantined": {"malformed_json": 2, "schema_invalid": 1},
    "quarantined_total": 3,
    "late_accepted": 1,
}
RERUN_SUMMARY = {**SUMMARY, "silver_inserted": 0, "duplicates_dropped": 6}


# --- fakes --------------------------------------------------------------------


def ok(obj=None, status=200):
    return HttpResponse(status, json.dumps({} if obj is None else obj).encode())


def err(status, code="ERR", message="denied"):
    return HttpResponse(status, json.dumps({"error_code": code, "message": message}).encode())


def path_of(req) -> str:
    return urlsplit(req.url).path


def query_of(req) -> dict[str, str]:
    return {k: v[0] for k, v in parse_qs(urlsplit(req.url).query).items()}


def body_of(req) -> dict:
    return json.loads(req.body) if req.body else {}


class FakeClock:
    def __init__(self):
        self.now = 0.0
        self.sleeps = []

    def __call__(self):
        return self.now

    def sleep(self, seconds):
        self.sleeps.append(seconds)
        self.now += seconds


class Scripted:
    """Transport answering from a list (response or exception); the last item repeats."""

    def __init__(self, items):
        self.items = list(items)
        self.calls = []

    def __call__(self, req):
        self.calls.append(req)
        item = self.items.pop(0) if len(self.items) > 1 else self.items[0]
        if isinstance(item, BaseException):
            raise item
        return item


class FakeDatabricks:
    """In-memory Databricks REST API: Unity Catalog, secrets, Files, workspace, Jobs.

    ``fail`` maps (method, path) to items answered first (response or exception), before the
    normal behaviour. ``runs/submit`` is idempotent on ``idempotency_token`` like the real API.
    Run n (in submit order) returns ``summaries[n]`` from ``runs/get-output``.
    """

    def __init__(
        self,
        *,
        summaries=(SUMMARY, RERUN_SUMMARY),
        pending_polls=1,
        final=("TERMINATED", "SUCCESS", ""),
        raw_output=None,
        fail=None,
        down=None,
    ):
        self.summaries = list(summaries)
        self.pending_polls = pending_polls
        self.final = final
        self.raw_output = raw_output
        self.fail = {k: list(v) for k, v in (fail or {}).items()}
        self.down = down
        self.calls = []
        self.runs: dict[int, dict] = {}
        self.by_token: dict[str, int] = {}

    # inspection
    def requests(self, method=None, path=None):
        return [
            c
            for c in self.calls
            if method in (None, c.method) and (path is None or path_of(c) == path)
        ]

    def bodies(self, method, path):
        return [body_of(c) for c in self.requests(method, path)]

    def count(self, method, path):
        return len(self.requests(method, path))

    # transport
    def __call__(self, req):
        self.calls.append(req)
        if self.down is not None:
            raise self.down
        key = (req.method, path_of(req))
        queue = self.fail.get(key)
        if queue:
            item = queue.pop(0)
            if isinstance(item, BaseException):
                raise item
            return item
        if not req.url.startswith(HOST + "/api/"):
            return err(404, "NOT_FOUND", "unknown host")
        if req.headers.get("Authorization") != f"Bearer {TOKEN}":
            return err(401, "UNAUTHENTICATED", "bad token")
        path = path_of(req)
        if key == ("GET", SCIM_ME):
            return ok({"userName": USER})
        if key in {
            ("POST", SCHEMAS),
            ("POST", VOLUMES),
            ("POST", SCOPES),
            ("POST", SECRETS_PUT),
            ("POST", IMPORT),
            ("POST", MKDIRS),
            ("POST", RUNS_CANCEL),
        }:
            return ok({})
        if req.method == "PUT" and path.startswith(FILES + "/Volumes/"):
            return HttpResponse(204, b"")
        if key == ("POST", SUBMIT):
            token = body_of(req).get("idempotency_token")
            if token in self.by_token:
                return ok({"run_id": self.by_token[token]})
            run_id = 100 + len(self.runs)
            self.runs[run_id] = {"polls": 0, "index": len(self.runs)}
            if token is not None:
                self.by_token[token] = run_id
            return ok({"run_id": run_id})
        if key == ("GET", RUNS_GET):
            run_id = int(query_of(req)["run_id"])
            run = self.runs[run_id]
            run["polls"] += 1
            if run["polls"] <= self.pending_polls:
                state = {"life_cycle_state": "RUNNING", "state_message": ""}
            else:
                life, result, message = self.final
                state = {
                    "life_cycle_state": life,
                    "result_state": result,
                    "state_message": message,
                }
            return ok(
                {
                    "run_id": run_id,
                    "state": state,
                    "tasks": [
                        {
                            "run_id": run_id + TASK_OFFSET,
                            "task_key": "bronze_silver",
                            "state": state,
                        }
                    ],
                    "setup_duration": 1000,
                    "execution_duration": 20000,
                    "cleanup_duration": 500,
                }
            )
        if key == ("GET", RUNS_OUTPUT):
            asked = int(query_of(req)["run_id"])
            run_id = asked - TASK_OFFSET if asked - TASK_OFFSET in self.runs else asked
            index = self.runs[run_id]["index"]
            if self.raw_output is not None:
                result = self.raw_output
            else:
                result = json.dumps(self.summaries[min(index, len(self.summaries) - 1)])
            return ok({"notebook_output": {"result": result}})
        return err(404, "ENDPOINT_NOT_FOUND", f"no route for {key}")


def make_client(transport, clock=None, **kwargs):
    clock = clock or FakeClock()
    return dj.Client(HOST, TOKEN, transport=transport, sleep=clock.sleep, **kwargs)


def make_landing(tmp_path: Path, hours, quarantine_hours=()) -> Path:
    landing = tmp_path / "landing" / "gharchive"
    for day, hour in hours:
        part = ingest.partition_dir(landing, day, hour)
        part.mkdir(parents=True, exist_ok=True)
        (part / ingest.EVENTS_FILE).write_bytes(gzip.compress(f"events {day} {hour}\n".encode()))
        if (day, hour) in set(quarantine_hours):
            (part / ingest.QUARANTINE_FILE).write_bytes(gzip.compress(b"{broken\n"))
    return landing


def make_wheel(tmp_path: Path) -> Path:
    wheel = tmp_path / "dist" / WHEEL_NAME
    wheel.parent.mkdir(parents=True, exist_ok=True)
    wheel.write_bytes(b"PK fake wheel bytes")
    return wheel


def payload(**overrides):
    kwargs = {
        "source": "gharchive",
        "day": DAY,
        "hour": 10,
        "hours": 2,
        "run_key": "777-2",
        "notebook_path": "/Users/someone/repopulse/bronze_silver",
        "wheel_path": f"{ROOT}/wheels/{SHA}/{WHEEL_NAME}",
        "catalog": "workspace",
        "schema": "repopulse",
        "volume": "repopulse_landing",
        "late_window_hours": 24,
        "secret_scope": "repopulse",
        "secret_key": "pii_hash_salt",
        "timeout_seconds": 3600,
    }
    kwargs.update(overrides)
    return dj.build_submit_payload(**kwargs)


def walk_keys(node):
    if isinstance(node, dict):
        for k, v in node.items():
            yield k
            yield from walk_keys(v)
    elif isinstance(node, list):
        for v in node:
            yield from walk_keys(v)


# --- HTTP client and retry ------------------------------------------------------


def test_call_sends_bearer_token_to_host_path_and_query():
    transport = Scripted([ok({"a": 1})])

    resp = make_client(transport).call("GET", RUNS_GET, query={"run_id": 5})

    (req,) = transport.calls
    assert req.method == "GET"
    assert req.url == HOST + RUNS_GET + "?run_id=5"
    assert req.headers["Authorization"] == f"Bearer {TOKEN}"
    assert json.loads(resp.body) == {"a": 1}


def test_call_sends_json_body_with_content_type():
    transport = Scripted([ok()])

    make_client(transport).call("POST", SUBMIT, json_body={"x": 1})

    (req,) = transport.calls
    assert json.loads(req.body) == {"x": 1}
    assert req.headers["Content-Type"] == "application/json"


def test_503_then_200_succeeds_after_backoff():
    transport = Scripted([err(503), err(503), err(503), ok({"done": True})])
    clock = FakeClock()

    resp = make_client(transport, clock, max_attempts=5, backoff_seconds=1.0).call("GET", RUNS_GET)

    assert json.loads(resp.body) == {"done": True}
    assert len(transport.calls) == 4
    assert clock.sleeps == [1.0, 2.0, 4.0]


@pytest.mark.parametrize("status", [429, 500, 502, 503, 504])
def test_transient_statuses_are_retried(status):
    transport = Scripted([err(status), ok({"done": True})])
    clock = FakeClock()

    make_client(transport, clock, max_attempts=5, backoff_seconds=1.0).call("GET", RUNS_GET)

    assert len(transport.calls) == 2
    assert clock.sleeps == [1.0]


@pytest.mark.parametrize(
    "exc",
    [
        urllib.error.URLError("temporary failure in name resolution"),
        TimeoutError("timed out"),
        ConnectionResetError("connection reset by peer"),
    ],
)
def test_network_errors_are_retried(exc):
    transport = Scripted([exc, ok({"done": True})])
    clock = FakeClock()

    resp = make_client(transport, clock, max_attempts=5, backoff_seconds=1.0).call("GET", RUNS_GET)

    assert json.loads(resp.body) == {"done": True}
    assert len(transport.calls) == 2
    assert clock.sleeps == [1.0]


@pytest.mark.parametrize("status", [400, 401, 403, 404, 409])
def test_client_errors_are_not_retried(status):
    transport = Scripted([err(status, "BAD", "nope"), ok()])
    clock = FakeClock()

    with pytest.raises(ApiError) as info:
        make_client(transport, clock, max_attempts=5).call("POST", SUBMIT, json_body={})

    assert info.value.status == status
    assert len(transport.calls) == 1
    assert clock.sleeps == []


def test_retries_stop_after_max_attempts_and_raise_the_last_error():
    transport = Scripted([err(502, "BAD_GATEWAY", "first"), err(503, "UNAVAILABLE", "last")])
    clock = FakeClock()

    with pytest.raises(ApiError) as info:
        make_client(transport, clock, max_attempts=3, backoff_seconds=1.0).call("GET", RUNS_GET)

    assert info.value.status == 503
    assert len(transport.calls) == 3
    assert clock.sleeps == [1.0, 2.0]


def test_network_error_after_max_attempts_is_raised():
    transport = Scripted([urllib.error.URLError("down")])
    clock = FakeClock()

    with pytest.raises(urllib.error.URLError):
        make_client(transport, clock, max_attempts=3, backoff_seconds=1.0).call("GET", RUNS_GET)

    assert len(transport.calls) == 3
    assert clock.sleeps == [1.0, 2.0]


def test_default_max_attempts_is_five():
    transport = Scripted([err(503)])

    with pytest.raises(ApiError):
        make_client(transport).call("GET", RUNS_GET)

    assert len(transport.calls) == 5


def test_unexpected_exception_is_not_retried():
    transport = Scripted([ValueError("bug in transport"), ok()])

    with pytest.raises(ValueError):
        make_client(transport).call("GET", RUNS_GET)

    assert len(transport.calls) == 1


# --- idempotency token ----------------------------------------------------------


def test_idempotency_token_is_stable_and_at_most_64_chars():
    a = dj.idempotency_token("gharchive", DAY, 10, 2, "777-2")
    b = dj.idempotency_token("gharchive", DAY, 10, 2, "777-2")

    assert a == b
    assert isinstance(a, str)
    assert 0 < len(a) <= 64


def test_idempotency_token_stays_short_for_a_long_run_key():
    token = dj.idempotency_token("gharchive", DAY, 10, 2, "9" * 200 + "-rerun")
    assert len(token) <= 64


@pytest.mark.parametrize(
    "changed",
    [
        ("app", DAY, 10, 2, "777-2"),
        ("gharchive", "2026-01-06", 10, 2, "777-2"),
        ("gharchive", DAY, 11, 2, "777-2"),
        ("gharchive", DAY, 10, 3, "777-2"),
        ("gharchive", DAY, 10, 2, "777-3"),
        ("gharchive", DAY, 10, 2, "777-2-rerun"),
    ],
)
def test_idempotency_token_changes_with_source_hour_window_or_run_key(changed):
    base = dj.idempotency_token("gharchive", DAY, 10, 2, "777-2")
    assert dj.idempotency_token(*changed) != base


# --- job payload ------------------------------------------------------------------


def test_payload_is_one_serverless_notebook_task():
    p = payload()

    assert len(p["tasks"]) == 1
    (task,) = p["tasks"]
    assert task["notebook_task"]["notebook_path"] == "/Users/someone/repopulse/bronze_silver"
    assert task.get("task_key")
    keys = set(walk_keys(p))
    for forbidden in ("new_cluster", "existing_cluster_id", "job_cluster_key", "job_clusters"):
        assert forbidden not in keys, forbidden


def test_payload_names_run_by_source_last_hour_and_hours():
    p = payload()
    assert "gharchive" in p["run_name"]
    assert "2026-01-05T10" in p["run_name"]
    assert "2h" in p["run_name"]


def test_payload_carries_idempotency_token_and_timeout():
    p = payload(timeout_seconds=1800)
    assert p["idempotency_token"] == dj.idempotency_token("gharchive", DAY, 10, 2, "777-2")
    assert p["timeout_seconds"] == 1800


def test_payload_base_parameters_are_all_strings():
    params = payload()["tasks"][0]["notebook_task"]["base_parameters"]

    assert {
        "source",
        "date",
        "hour",
        "hours",
        "catalog",
        "schema",
        "volume",
        "wheel_path",
        "late_window_hours",
        "secret_scope",
        "secret_key",
    } <= set(params)
    assert all(isinstance(v, str) for v in params.values()), params
    assert params["source"] == "gharchive"
    assert params["date"] == DAY
    assert int(params["hour"]) == 10
    assert int(params["hours"]) == 2
    assert params["catalog"] == "workspace"
    assert params["schema"] == "repopulse"
    assert params["volume"] == "repopulse_landing"
    assert params["wheel_path"] == f"{ROOT}/wheels/{SHA}/{WHEEL_NAME}"
    assert int(params["late_window_hours"]) == 24
    assert params["secret_scope"] == "repopulse"
    assert params["secret_key"] == "pii_hash_salt"


def test_payload_never_holds_the_salt():
    text = json.dumps(payload())
    assert SALT not in text
    assert "string_value" not in text


def test_payload_differs_only_by_token_for_another_run_key():
    a, b = payload(run_key="777-2"), payload(run_key="777-2-rerun")
    assert a["idempotency_token"] != b["idempotency_token"]
    assert a["tasks"] == b["tasks"]


# --- Unity Catalog, secrets ---------------------------------------------------------


def test_ensure_schema_volume_and_secret_send_the_expected_bodies():
    fake = FakeDatabricks()
    client = make_client(fake)

    dj.ensure_schema(client, catalog="workspace", schema="repopulse")
    dj.ensure_volume(client, catalog="workspace", schema="repopulse", volume="repopulse_landing")
    dj.ensure_secret(client, scope="repopulse", key="pii_hash_salt", value=SALT)

    (schema,) = fake.bodies("POST", SCHEMAS)
    assert schema["catalog_name"] == "workspace"
    assert schema["name"] == "repopulse"
    (volume,) = fake.bodies("POST", VOLUMES)
    assert volume["catalog_name"] == "workspace"
    assert volume["schema_name"] == "repopulse"
    assert volume["name"] == "repopulse_landing"
    assert volume["volume_type"] == "MANAGED"
    (scope,) = fake.bodies("POST", SCOPES)
    assert scope["scope"] == "repopulse"
    (secret,) = fake.bodies("POST", SECRETS_PUT)
    assert secret == {"scope": "repopulse", "key": "pii_hash_salt", "string_value": SALT}


def test_already_existing_schema_volume_and_scope_are_not_errors():
    fake = FakeDatabricks(
        fail={
            ("POST", SCHEMAS): [err(409, "SCHEMA_ALREADY_EXISTS", "Schema 'repopulse' exists")],
            ("POST", VOLUMES): [
                err(400, "RESOURCE_ALREADY_EXISTS", "Volume 'repopulse_landing' already exists")
            ],
            ("POST", SCOPES): [
                err(400, "RESOURCE_ALREADY_EXISTS", "Scope repopulse already exists!")
            ],
        }
    )
    client = make_client(fake)

    dj.ensure_schema(client, catalog="workspace", schema="repopulse")
    dj.ensure_volume(client, catalog="workspace", schema="repopulse", volume="repopulse_landing")
    dj.ensure_secret(client, scope="repopulse", key="pii_hash_salt", value=SALT)

    assert fake.count("POST", SCHEMAS) == 1
    assert fake.count("POST", VOLUMES) == 1
    assert fake.count("POST", SCOPES) == 1
    assert fake.count("POST", SECRETS_PUT) == 1, "an existing scope still gets the salt put"


def test_other_unity_catalog_errors_are_raised():
    fake = FakeDatabricks(fail={("POST", SCHEMAS): [err(403, "PERMISSION_DENIED", "no")]})

    with pytest.raises(ApiError) as info:
        dj.ensure_schema(make_client(fake), catalog="workspace", schema="repopulse")

    assert info.value.status == 403


# --- Files API uploads ----------------------------------------------------------------


def test_upload_landing_puts_every_file_with_overwrite_at_a_path_fixed_by_hour(tmp_path):
    hours = [(DAY, 9), (DAY, 10)]
    landing = make_landing(tmp_path, hours, quarantine_hours=[(DAY, 10)])
    fake = FakeDatabricks()

    paths = dj.upload_landing(
        make_client(fake),
        landing_dir=landing,
        source="gharchive",
        hours=hours,
        catalog="workspace",
        schema="repopulse",
        volume="repopulse_landing",
    )

    base = f"{ROOT}/landing/gharchive"
    expected = [
        f"{base}/date={DAY}/hour=09/events.json.gz",
        f"{base}/date={DAY}/hour=10/_quarantine.jsonl.gz",
        f"{base}/date={DAY}/hour=10/events.json.gz",
    ]
    assert sorted(paths) == sorted(expected)
    puts = fake.requests("PUT")
    assert sorted(path_of(r) for r in puts) == sorted(FILES + p for p in expected)
    for req in puts:
        assert query_of(req) == {"overwrite": "true"}
        rel = path_of(req).removeprefix(FILES + base + "/")
        assert req.body == (landing / rel).read_bytes()


def test_uploading_the_same_hour_twice_targets_the_same_urls(tmp_path):
    hours = [(DAY, 10)]
    landing = make_landing(tmp_path, hours, quarantine_hours=hours)
    fake = FakeDatabricks()
    kwargs = {
        "landing_dir": landing,
        "source": "gharchive",
        "hours": hours,
        "catalog": "workspace",
        "schema": "repopulse",
        "volume": "repopulse_landing",
    }

    dj.upload_landing(make_client(fake), **kwargs)
    first = sorted(r.url for r in fake.requests("PUT"))
    dj.upload_landing(make_client(fake), **kwargs)
    every = sorted(r.url for r in fake.requests("PUT"))

    assert len(first) == 2
    assert every == sorted(first * 2)


def test_upload_landing_across_midnight_uses_each_hours_own_date(tmp_path):
    hours = [("2026-01-04", 23), (DAY, 0)]
    landing = make_landing(tmp_path, hours)
    fake = FakeDatabricks()

    paths = dj.upload_landing(
        make_client(fake),
        landing_dir=landing,
        source="gharchive",
        hours=hours,
        catalog="workspace",
        schema="repopulse",
        volume="repopulse_landing",
    )

    assert sorted(paths) == [
        f"{ROOT}/landing/gharchive/date=2026-01-04/hour=23/events.json.gz",
        f"{ROOT}/landing/gharchive/date={DAY}/hour=00/events.json.gz",
    ]


def test_upload_wheel_goes_under_the_git_sha(tmp_path):
    wheel = make_wheel(tmp_path)
    fake = FakeDatabricks()

    path = dj.upload_wheel(
        make_client(fake),
        wheel=wheel,
        git_sha=SHA,
        catalog="workspace",
        schema="repopulse",
        volume="repopulse_landing",
    )

    assert path == f"{ROOT}/wheels/{SHA}/{WHEEL_NAME}"
    (req,) = fake.requests("PUT")
    assert path_of(req) == FILES + path
    assert query_of(req) == {"overwrite": "true"}
    assert req.body == wheel.read_bytes()


# --- submit and wait ------------------------------------------------------------------


def test_submit_retry_resends_the_same_idempotency_token():
    fake = FakeDatabricks(
        fail={("POST", SUBMIT): [err(503), urllib.error.URLError("connection reset")]}
    )
    p = payload()

    run_id = dj.submit_run(make_client(fake), p)

    bodies = fake.bodies("POST", SUBMIT)
    assert len(bodies) == 3
    assert all(b == p for b in bodies)
    assert {b["idempotency_token"] for b in bodies} == {p["idempotency_token"]}
    assert list(fake.runs) == [run_id], "one Databricks run for one workflow run"


def submitted(fake, clock=None):
    client = make_client(fake, clock)
    return client, dj.submit_run(client, payload())


def test_wait_for_success_returns_the_notebook_summary():
    fake = FakeDatabricks(pending_polls=2)
    clock = FakeClock()
    client, run_id = submitted(fake, clock)

    outcome = dj.wait_for_run(
        client, run_id, timeout=600, poll_interval=10, clock=clock, sleep=clock.sleep
    )

    assert outcome.summary == SUMMARY
    assert set(clock.sleeps) == {10}
    assert fake.count("GET", RUNS_OUTPUT) == 1
    assert fake.count("POST", RUNS_CANCEL) == 0


def test_failed_run_raises_with_the_state_message():
    fake = FakeDatabricks(final=("TERMINATED", "FAILED", "Notebook raised: boom"))
    clock = FakeClock()
    client, run_id = submitted(fake, clock)

    with pytest.raises(dj.JobError) as info:
        dj.wait_for_run(
            client, run_id, timeout=600, poll_interval=10, clock=clock, sleep=clock.sleep
        )

    assert "FAILED" in str(info.value)
    assert "boom" in str(info.value)
    assert fake.count("GET", RUNS_OUTPUT) == 0


def test_internal_error_life_cycle_is_a_failure():
    fake = FakeDatabricks(final=("INTERNAL_ERROR", "FAILED", "cluster went away"))
    clock = FakeClock()
    client, run_id = submitted(fake, clock)

    with pytest.raises(dj.JobError):
        dj.wait_for_run(
            client, run_id, timeout=600, poll_interval=10, clock=clock, sleep=clock.sleep
        )


def test_timeout_cancels_the_run_and_raises():
    fake = FakeDatabricks(pending_polls=10**9)
    clock = FakeClock()
    client, run_id = submitted(fake, clock)

    with pytest.raises(dj.JobError) as info:
        dj.wait_for_run(
            client, run_id, timeout=30, poll_interval=10, clock=clock, sleep=clock.sleep
        )

    assert "timed out" in str(info.value).lower()
    assert fake.bodies("POST", RUNS_CANCEL) == [{"run_id": run_id}]
    assert clock.now <= 40


@pytest.mark.parametrize("raw", ["not json", "[1, 2]", ""])
def test_output_that_is_not_a_json_object_is_an_error(raw):
    fake = FakeDatabricks(raw_output=raw)
    clock = FakeClock()
    client, run_id = submitted(fake, clock)

    with pytest.raises(dj.JobError):
        dj.wait_for_run(
            client, run_id, timeout=600, poll_interval=10, clock=clock, sleep=clock.sleep
        )


# --- runs.csv row ---------------------------------------------------------------------


def notes_of(row) -> dict[str, str]:
    return dict(token.split("=", 1) for token in row["notes"].split(" "))


def row_kwargs(**overrides):
    kwargs = {
        "run_at": RUN_AT,
        "git_sha": SHA,
        "source": "gharchive",
        "day": DAY,
        "hour": 10,
        "hours": 2,
        "summary": SUMMARY,
        "total_seconds": 42.5,
        "upload_seconds": 3.25,
        "run_id": "777",
        "trigger": "push",
    }
    kwargs.update(overrides)
    return kwargs


def test_build_row_is_a_valid_runs_csv_row_of_the_variant():
    row = dj.build_row(**row_kwargs(rerun_summary=RERUN_SUMMARY))

    assert validate_row(row) == row
    assert dj.VARIANT == "databricks-bronze-silver"
    assert row["variant"] == "databricks-bronze-silver"
    assert row["git_sha"] == SHA
    assert row["data_date"] == DAY
    assert row["total_seconds"] == "42.5"
    assert row["row_count"] == "5"
    for col in ("q1_seconds", "q6_seconds", "peak_rss_mb", "bytes_read"):
        assert row[col] == ""


def test_build_row_notes_are_key_value_tokens():
    row = dj.build_row(
        **row_kwargs(rerun_summary=RERUN_SUMMARY, timings={"setup": 1.0, "execution": 20.0})
    )

    notes = notes_of(row)
    assert notes["last_hour"] == "2026-01-05T10"
    assert notes["hours"] == "2"
    assert notes["bronze_rows"] == "9"
    assert notes["quarantined_total"] == "3"
    assert notes["duplicates_dropped"] == "1"
    assert float(notes["upload_s"]) == 3.25
    assert float(notes["setup_s"]) == 1.0
    assert float(notes["execution_s"]) == 20.0
    assert notes["run_id"] == "777"
    assert notes["trigger"] == "push"
    assert notes["rerun_silver_inserted"] == "0"


def test_build_row_without_rerun_has_no_rerun_note():
    notes = notes_of(dj.build_row(**row_kwargs()))
    assert "rerun_silver_inserted" not in notes


def test_build_row_data_date_is_the_date_of_the_last_hour():
    row = dj.build_row(**row_kwargs(day=DAY, hour=0, hours=2))
    assert row["data_date"] == DAY
    assert notes_of(row)["last_hour"] == f"{DAY}T00"


@pytest.mark.parametrize(
    "summary",
    [
        {k: v for k, v in SUMMARY.items() if k != "silver_inserted"},
        {**SUMMARY, "silver_inserted": -1},
        {**SUMMARY, "silver_inserted": "5"},
    ],
)
def test_build_row_rejects_a_summary_without_a_valid_silver_count(summary):
    with pytest.raises(ValueError):
        dj.build_row(**row_kwargs(summary=summary))


# --- CLI ------------------------------------------------------------------------------


def cli_env(**overrides):
    env = {
        "DATABRICKS_HOST": HOST,
        "DATABRICKS_TOKEN": TOKEN,
        "PII_HASH_SALT": SALT,
        "GITHUB_RUN_ID": "777",
        "GITHUB_RUN_ATTEMPT": "2",
        "GITHUB_EVENT_NAME": "push",
    }
    env.update(overrides)
    return {k: v for k, v in env.items() if v is not None}


def cli_args(tmp_path: Path, *extra: str) -> list[str]:
    landing = make_landing(tmp_path, [(DAY, 9), (DAY, 10)], quarantine_hours=[(DAY, 10)])
    wheel = make_wheel(tmp_path)
    return [
        "run",
        "--source",
        "gharchive",
        "--date",
        DAY,
        "--hour",
        "10",
        "--hours",
        "2",
        "--landing-dir",
        str(landing),
        "--wheel",
        str(wheel),
        "--git-sha",
        SHA,
        "--summary",
        str(tmp_path / "out" / "summary.json"),
        "--out",
        str(tmp_path / "out" / "databricks-row.csv"),
        *extra,
    ]


def run_cli(tmp_path, fake, *extra, env=None):
    clock = FakeClock()
    return dj.main(
        cli_args(tmp_path, *extra),
        env=cli_env() if env is None else env,
        transport=fake,
        sleep=clock.sleep,
        clock=clock,
    )


def read_csv(path: Path) -> list[list[str]]:
    with path.open(encoding="utf-8", newline="") as fh:
        return list(csv.reader(fh))


def assert_no_secret(text: str, where: str) -> None:
    for value in SECRET_VALUES:
        assert value not in text, f"secret value leaked in {where}"


def test_cli_runs_the_job_verifies_the_rerun_and_writes_the_row(tmp_path, capsys):
    fake = FakeDatabricks()

    code = run_cli(tmp_path, fake, "--verify-rerun")

    captured = capsys.readouterr()
    assert code == 0, captured.err
    tokens = [b["idempotency_token"] for b in fake.bodies("POST", SUBMIT)]
    assert tokens == [
        dj.idempotency_token("gharchive", DAY, 10, 2, "777-2"),
        dj.idempotency_token("gharchive", DAY, 10, 2, "777-2-rerun"),
    ]
    assert len(fake.runs) == 2

    rows = read_csv(tmp_path / "out" / "databricks-row.csv")
    assert rows[0] == list(COLUMNS)
    assert len(rows) == 2
    row = dict(zip(COLUMNS, rows[1], strict=True))
    assert validate_row(row) == row
    assert row["variant"] == "databricks-bronze-silver"
    assert row["git_sha"] == SHA
    assert row["data_date"] == DAY
    assert row["row_count"] == "5"
    assert float(row["total_seconds"]) > 0
    notes = notes_of(row)
    assert notes["last_hour"] == "2026-01-05T10"
    assert notes["hours"] == "2"
    assert notes["run_id"] == "777"
    assert notes["trigger"] == "push"
    assert notes["rerun_silver_inserted"] == "0"

    summary = json.loads((tmp_path / "out" / "summary.json").read_text(encoding="utf-8"))
    assert summary["silver_inserted"] == 5
    assert_no_secret((tmp_path / "out" / "databricks-row.csv").read_text("utf-8"), "row csv")
    assert_no_secret((tmp_path / "out" / "summary.json").read_text("utf-8"), "summary")
    assert_no_secret(captured.out + captured.err, "stdout/stderr")


def test_cli_uploads_landing_and_wheel_before_submitting(tmp_path):
    fake = FakeDatabricks()

    assert run_cli(tmp_path, fake) == 0

    puts = [path_of(r) for r in fake.requests("PUT")]
    base = f"{FILES}{ROOT}/landing/gharchive"
    assert sorted(p for p in puts if p.startswith(base)) == [
        f"{base}/date={DAY}/hour=09/events.json.gz",
        f"{base}/date={DAY}/hour=10/_quarantine.jsonl.gz",
        f"{base}/date={DAY}/hour=10/events.json.gz",
    ]
    wheel_path = f"{ROOT}/wheels/{SHA}/{WHEEL_NAME}"
    assert FILES + wheel_path in puts
    first_submit = fake.calls.index(fake.requests("POST", SUBMIT)[0])
    last_put = max(fake.calls.index(r) for r in fake.requests("PUT"))
    assert last_put < first_submit

    (body,) = fake.bodies("POST", SUBMIT)
    params = body["tasks"][0]["notebook_task"]["base_parameters"]
    assert params["wheel_path"] == wheel_path
    assert params["date"] == DAY and int(params["hour"]) == 10 and int(params["hours"]) == 2


def test_cli_without_verify_rerun_submits_once(tmp_path):
    fake = FakeDatabricks()

    assert run_cli(tmp_path, fake) == 0

    assert fake.count("POST", SUBMIT) == 1
    rows = read_csv(tmp_path / "out" / "databricks-row.csv")
    assert "rerun_silver_inserted" not in notes_of(dict(zip(COLUMNS, rows[1], strict=True)))


def test_cli_imports_the_notebook_that_the_job_runs(tmp_path):
    fake = FakeDatabricks()

    assert run_cli(tmp_path, fake) == 0

    imports = fake.bodies("POST", IMPORT)
    assert len(imports) == 1
    (submit,) = fake.bodies("POST", SUBMIT)
    assert imports[0]["path"] == submit["tasks"][0]["notebook_task"]["notebook_path"]
    assert imports[0].get("overwrite") is True
    source = base64.b64decode(imports[0]["content"]).decode("utf-8")
    assert "%pip install" in source
    assert "dbutils.secrets.get" in source
    assert "dbutils.notebook.exit" in source
    assert "databricks_entry" in source
    assert SALT not in source


def test_cli_salt_only_travels_in_the_secret_put(tmp_path):
    fake = FakeDatabricks()

    assert run_cli(tmp_path, fake, "--verify-rerun") == 0

    (put,) = fake.bodies("POST", SECRETS_PUT)
    assert put["string_value"] == SALT
    (submit, _rerun) = fake.bodies("POST", SUBMIT)
    params = submit["tasks"][0]["notebook_task"]["base_parameters"]
    assert put["scope"] == params["secret_scope"]
    assert put["key"] == params["secret_key"]
    for req in fake.calls:
        if path_of(req) == SECRETS_PUT:
            continue
        body = req.body or b""
        texts = [req.url, json.dumps(dict(req.headers)), body.decode("utf-8", "replace")]
        if path_of(req) == IMPORT:
            texts.append(base64.b64decode(body_of(req)["content"]).decode("utf-8", "replace"))
        for text in texts:
            assert SALT not in text, f"salt leaked in {req.method} {path_of(req)}"


def test_cli_uses_default_catalog_schema_and_volume(tmp_path):
    fake = FakeDatabricks()

    assert run_cli(tmp_path, fake) == 0

    (schema,) = fake.bodies("POST", SCHEMAS)
    assert (schema["catalog_name"], schema["name"]) == ("workspace", "repopulse")
    (volume,) = fake.bodies("POST", VOLUMES)
    assert volume["name"] == "repopulse_landing"


def test_cli_catalog_schema_and_volume_are_configurable(tmp_path):
    fake = FakeDatabricks()

    code = run_cli(
        tmp_path, fake, "--catalog", "main", "--schema", "rp_test", "--volume", "landing_test"
    )

    assert code == 0
    (schema,) = fake.bodies("POST", SCHEMAS)
    assert (schema["catalog_name"], schema["name"]) == ("main", "rp_test")
    puts = [path_of(r) for r in fake.requests("PUT")]
    assert puts and all(p.startswith(f"{FILES}/Volumes/main/rp_test/landing_test/") for p in puts)
    (submit,) = fake.bodies("POST", SUBMIT)
    params = submit["tasks"][0]["notebook_task"]["base_parameters"]
    assert (params["catalog"], params["schema"], params["volume"]) == (
        "main",
        "rp_test",
        "landing_test",
    )


def test_cli_rerun_that_inserts_rows_fails(tmp_path, capsys):
    fake = FakeDatabricks(summaries=(SUMMARY, {**SUMMARY, "silver_inserted": 5}))

    code = run_cli(tmp_path, fake, "--verify-rerun")

    assert code == 1
    assert fake.count("POST", SUBMIT) == 2
    assert "silver_inserted" in capsys.readouterr().err


@pytest.mark.parametrize("name", ["DATABRICKS_HOST", "DATABRICKS_TOKEN", "PII_HASH_SALT"])
@pytest.mark.parametrize("value", [None, "", "   "])
def test_cli_missing_secret_exits_1_naming_it_before_any_http(tmp_path, capsys, name, value):
    fake = FakeDatabricks()

    code = run_cli(tmp_path, fake, env=cli_env(**{name: value}))

    captured = capsys.readouterr()
    assert code == 1
    assert name in captured.err
    assert fake.calls == []
    assert_no_secret(captured.out + captured.err, "stdout/stderr")


def test_cli_lists_every_missing_secret(tmp_path, capsys):
    fake = FakeDatabricks()

    code = run_cli(tmp_path, fake, env={"GITHUB_RUN_ID": "777"})

    err_text = capsys.readouterr().err
    assert code == 1
    for name in ("DATABRICKS_HOST", "DATABRICKS_TOKEN", "PII_HASH_SALT"):
        assert name in err_text
    assert fake.calls == []


@pytest.mark.parametrize(
    "extra",
    [
        ["--hour", "24"],
        ["--hours", "0"],
        ["--hours", "25"],
        ["--date", "2026-02-30"],
        ["--source", "kafka"],
    ],
)
def test_cli_bad_arguments_exit_2_without_http(tmp_path, extra):
    fake = FakeDatabricks()
    clock = FakeClock()

    with pytest.raises(SystemExit) as info:
        dj.main(
            cli_args(tmp_path, *extra),
            env=cli_env(),
            transport=fake,
            sleep=clock.sleep,
            clock=clock,
        )

    assert info.value.code == 2
    assert fake.calls == []


def test_cli_failed_run_exits_1_without_leaking_secrets(tmp_path, capsys):
    message = f"Run failed at {HOST} for {USER} (host {NETLOC}, token {TOKEN}, salt {SALT})"
    fake = FakeDatabricks(final=("TERMINATED", "FAILED", message))

    code = run_cli(tmp_path, fake)

    captured = capsys.readouterr()
    assert code == 1
    assert "FAILED" in captured.err
    assert_no_secret(captured.out + captured.err, "stdout/stderr")


def test_cli_api_error_exits_1_without_leaking_secrets(tmp_path, capsys):
    message = f"User {USER} may not use {HOST} ({NETLOC}) with {TOKEN}"
    fake = FakeDatabricks(fail={("POST", SCHEMAS): [err(403, "PERMISSION_DENIED", message)]})

    code = run_cli(tmp_path, fake)

    captured = capsys.readouterr()
    assert code == 1
    assert "403" in captured.err
    assert fake.count("POST", SUBMIT) == 0
    assert_no_secret(captured.out + captured.err, "stdout/stderr")


def test_cli_network_down_exits_1_after_retries_without_leaking(tmp_path, capsys):
    fake = FakeDatabricks(down=urllib.error.URLError(f"cannot reach {NETLOC} with {TOKEN}"))

    code = run_cli(tmp_path, fake)

    captured = capsys.readouterr()
    assert code == 1
    assert len(fake.calls) == 5, "first request tried max_attempts times, then stop"
    assert_no_secret(captured.out + captured.err, "stdout/stderr")
