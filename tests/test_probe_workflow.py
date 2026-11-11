"""Static checks on .github/workflows/probe.yml (no GitHub, no network)."""

import re
from pathlib import Path

import pytest
import yaml

WORKFLOW = Path(__file__).resolve().parents[1] / ".github" / "workflows" / "probe.yml"
REQUIRED_CHECK_NAMES = {"Lint", "Unit Tests", "dbt build (DuckDB)"}
SECRET_NAMES = ("DATABRICKS_HOST", "DATABRICKS_TOKEN", "DATABRICKS_HTTP_PATH", "MOTHERDUCK_TOKEN")
SECRET_EXPR = re.compile(r"^\$\{\{\s*secrets\.[A-Z][A-Z0-9_]*\s*\}\}$")


def load():
    assert WORKFLOW.is_file(), f"missing workflow file {WORKFLOW}"
    data = yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))
    assert isinstance(data, dict), "workflow must be a YAML mapping"
    return data


def triggers(data):
    # PyYAML (YAML 1.1) reads the bare key `on` as boolean True.
    return data.get("on", data.get(True))


def jobs(data):
    return data["jobs"]


def steps(data):
    return [s for job in jobs(data).values() for s in job.get("steps", [])]


def walk(node, path=()):
    if isinstance(node, dict):
        for k, v in node.items():
            yield from walk(v, path + (k,))
    elif isinstance(node, list):
        for i, v in enumerate(node):
            yield from walk(v, path + (i,))
    else:
        yield path, node


def test_permissions_are_minimal():
    data = load()
    assert data.get("permissions") == {"contents": "read"}
    for job in jobs(data).values():
        assert job.get("permissions", data["permissions"]) == {"contents": "read"}


def test_every_job_has_timeout():
    for name, job in jobs(load()).items():
        assert isinstance(job.get("timeout-minutes"), int), name
        assert 0 < job["timeout-minutes"] <= 60, name


def test_has_concurrency_group():
    conc = load().get("concurrency")
    assert isinstance(conc, dict) and conc.get("group")


def test_triggers_are_dispatch_and_push_to_probe_branch_only():
    on = triggers(load())
    assert isinstance(on, dict)
    assert set(on) == {"workflow_dispatch", "push"}
    assert "pull_request" not in on
    assert "pull_request_target" not in on
    assert on["push"] == {"branches": ["docs/probe-result"]}


def test_explains_push_trigger_in_comment():
    text = WORKFLOW.read_text(encoding="utf-8") if WORKFLOW.is_file() else ""
    comments = "\n".join(line for line in text.splitlines() if line.lstrip().startswith("#"))
    assert "workflow_dispatch" in comments
    assert "main" in comments


def test_job_names_do_not_shadow_required_checks():
    for key, job in jobs(load()).items():
        assert job.get("name", key) not in REQUIRED_CHECK_NAMES
        assert key not in REQUIRED_CHECK_NAMES


def test_no_job_level_if():
    for name, job in jobs(load()).items():
        assert "if" not in job, name


def test_secrets_only_appear_as_env_values():
    data = load()
    seen = set()
    for path, value in walk(data):
        if not isinstance(value, str) or "secrets." not in value:
            continue
        assert len(path) >= 2 and path[-2] == "env", f"secret outside env: {path}"
        assert SECRET_EXPR.match(value.strip()), f"secret not a plain expression: {path}"
        seen.add(path[-1])
    assert set(SECRET_NAMES) <= seen


def test_run_steps_never_echo_secrets():
    for step in steps(load()):
        run = step.get("run", "")
        assert "secrets." not in run
        for line in run.splitlines():
            if re.search(r"\b(echo|printf)\b", line):
                for name in SECRET_NAMES:
                    assert name not in line, line
        assert "set -x" not in run


def test_runs_probe_dbt_debug_and_uploads_artifact():
    all_steps = steps(load())
    runs = "\n".join(s.get("run", "") for s in all_steps)
    assert "python -m repopulse.probe" in runs
    assert "dbt-databricks" in runs
    assert "merge-dbt" in runs

    for target in ("databricks", "motherduck"):
        debug = [s for s in all_steps if f"--target {target}" in s.get("run", "")]
        assert len(debug) == 1, target
        assert "dbt debug" in debug[0]["run"]
        assert debug[0].get("continue-on-error") is True
        assert debug[0].get("id")
        assert debug[0].get("if"), "dbt debug must be gated at step level"

    upload = [s for s in all_steps if str(s.get("uses", "")).startswith("actions/upload-artifact")]
    assert len(upload) == 1
    assert upload[0]["with"]["name"] == "probe-result"
    paths = upload[0]["with"]["path"]
    assert "probe-result.json" in paths and "probe-result.md" in paths


@pytest.mark.parametrize("action", ["actions/checkout", "actions/setup-python"])
def test_uses_standard_actions(action):
    uses = [str(s.get("uses", "")) for s in steps(load())]
    assert any(u.startswith(action + "@") for u in uses)


def test_uses_python_311():
    setup = [s for s in steps(load()) if str(s.get("uses", "")).startswith("actions/setup-python")]
    assert str(setup[0]["with"]["python-version"]) == "3.11"
