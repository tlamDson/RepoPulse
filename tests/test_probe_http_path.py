"""``python -m repopulse.probe http-path``: the SQL warehouse http path for dbt (D2).

Same resolution as the probe's SQL warehouse check: secret ``DATABRICKS_HTTP_PATH`` when set,
otherwise the first warehouse of ``GET /api/2.0/sql/warehouses``. The path goes to ``$GITHUB_ENV``
as ``DBT_DATABRICKS_HTTP_PATH`` and is registered with the log masker; it is never printed in
clear. Fake transport only: no network.
"""

from __future__ import annotations

import json
from urllib.parse import urlsplit

import pytest

from repopulse import probe
from repopulse.probe import HttpResponse, ProbeContext, Secrets

HOST = "https://dbc-abc123.cloud.databricks.com"
NETLOC = "dbc-abc123.cloud.databricks.com"
TOKEN = "dapiSECRET123"
WAREHOUSES = "/api/2.0/sql/warehouses"


class Transport:
    def __init__(self, routes=None):
        self.routes = routes or {}
        self.calls = []

    def __call__(self, req):
        self.calls.append(req)
        key = (req.method, urlsplit(req.url).path)
        if key not in self.routes:
            return HttpResponse(404, json.dumps({"message": f"no route {key}"}).encode())
        return self.routes[key]


def ok(obj):
    return HttpResponse(200, json.dumps(obj).encode())


def listing(*ids, with_path=True):
    items = []
    for wid in ids:
        item = {"id": wid}
        if with_path:
            item["odbc_params"] = {"path": f"/sql/1.0/warehouses/{wid}"}
        items.append(item)
    return ok({"warehouses": items})


@pytest.fixture
def gh_env(tmp_path):
    return tmp_path / "github_env"


def run(gh_env, transport, **env):
    full = {"DATABRICKS_HOST": HOST, "DATABRICKS_TOKEN": TOKEN, "GITHUB_ENV": str(gh_env), **env}
    return probe.main(["http-path", "--github-env"], env=full, transport=transport)


def test_secret_http_path_is_used_without_calling_the_api(gh_env, capsys):
    t = Transport()
    assert run(gh_env, t, DATABRICKS_HTTP_PATH="/sql/1.0/warehouses/wh123") == 0
    assert t.calls == []
    assert gh_env.read_text(encoding="utf-8") == (
        "DBT_DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/wh123\n"
    )
    out = capsys.readouterr().out
    for value in ("/sql/1.0/warehouses/wh123", "wh123", NETLOC):
        assert f"::add-mask::{value}" in out
    clear = [ln for ln in out.splitlines() if not ln.startswith("::add-mask::")]
    assert not any("wh123" in ln or NETLOC in ln for ln in clear), clear


def test_empty_secret_discovers_the_first_warehouse(gh_env, capsys):
    t = Transport({("GET", WAREHOUSES): listing("wh9", "wh10")})
    assert run(gh_env, t, DATABRICKS_HTTP_PATH="") == 0
    assert [urlsplit(c.url).path for c in t.calls] == [WAREHOUSES]
    assert t.calls[0].headers["Authorization"] == f"Bearer {TOKEN}"
    assert (
        gh_env.read_text(encoding="utf-8") == "DBT_DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/wh9\n"
    )
    assert "::add-mask::wh9" in capsys.readouterr().out


def test_warehouse_without_odbc_params_gets_the_standard_path(gh_env):
    t = Transport({("GET", WAREHOUSES): listing("abc", with_path=False)})
    assert run(gh_env, t) == 0
    assert (
        gh_env.read_text(encoding="utf-8") == "DBT_DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/abc\n"
    )


@pytest.mark.parametrize(
    ("env", "routes", "message"),
    [
        ({"DATABRICKS_HTTP_PATH": "warehouses/abc"}, {}, "not of the form"),
        ({}, {("GET", WAREHOUSES): ok({"warehouses": []})}, "no SQL warehouse"),
        ({}, {}, "HTTP 404"),
        ({"DATABRICKS_TOKEN": ""}, {}, "DATABRICKS_TOKEN"),
        ({"DATABRICKS_HOST": ""}, {}, "DATABRICKS_HOST"),
    ],
)
def test_failures_exit_1_write_nothing_and_leak_nothing(gh_env, capsys, env, routes, message):
    assert run(gh_env, Transport(routes), **env) == 1
    assert not gh_env.exists()
    captured = capsys.readouterr()
    assert message in captured.err
    for secret in (TOKEN, NETLOC, "warehouses/abc"):
        assert secret not in captured.err + captured.out


def test_github_env_is_required(capsys):
    env = {"DATABRICKS_HOST": HOST, "DATABRICKS_TOKEN": TOKEN}
    t = Transport({("GET", WAREHOUSES): listing("wh9")})
    assert probe.main(["http-path", "--github-env"], env=env, transport=t) == 1
    assert "GITHUB_ENV" in capsys.readouterr().err


def test_resolve_warehouse_is_shared_with_the_probe_check():
    ctx = ProbeContext(
        Secrets.from_env({"DATABRICKS_HOST": HOST, "DATABRICKS_TOKEN": TOKEN}),
        Transport({("GET", WAREHOUSES): listing("wh7")}),
    )
    assert probe.resolve_warehouse(ctx) == ("wh7", "/sql/1.0/warehouses/wh7")
    secret = Secrets.from_env(
        {"DATABRICKS_HOST": HOST, "DATABRICKS_TOKEN": TOKEN, "DATABRICKS_HTTP_PATH": "/x"}
    )
    with pytest.raises(ValueError, match="not of the form"):
        probe.resolve_warehouse(ProbeContext(secret, Transport()))
