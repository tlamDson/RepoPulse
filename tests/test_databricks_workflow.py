"""Static checks on .github/workflows/databricks-bronze-silver.yml (no GitHub, no Databricks)."""

import re
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = ROOT / ".github" / "workflows" / "databricks-bronze-silver.yml"
BRANCH = "feature/databricks-bronze-silver"
REQUIRED_CHECK_NAMES = {"Lint", "Unit Tests", "dbt build (DuckDB)"}
SECRET_NAMES = ("DATABRICKS_HOST", "DATABRICKS_TOKEN", "PII_HASH_SALT")
SECRET_EXPR = re.compile(r"^\$\{\{\s*secrets\.[A-Z][A-Z0-9_]*\s*\}\}$")
SECRET_EXPR_NAME = re.compile(r"^\$\{\{\s*secrets\.([A-Z][A-Z0-9_]*)\s*\}\}$")
HAS_HOST = re.compile(r"env\.DATABRICKS_HOST\s*!=\s*''")
NO_HOST = re.compile(r"env\.DATABRICKS_HOST\s*==\s*''")
CLIENT = "python -m repopulse.databricks_job run"
ROW_FILE = "out/databricks-row.csv"


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


def runs(data):
    return [s for s in steps(data) if "run" in s]


def walk(node, path=()):
    if isinstance(node, dict):
        for k, v in node.items():
            yield from walk(v, path + (k,))
    elif isinstance(node, list):
        for i, v in enumerate(node):
            yield from walk(v, path + (i,))
    else:
        yield path, node


def client_steps(data):
    return [s for s in runs(data) if CLIENT in s["run"]]


def test_triggers_are_dispatch_and_push_to_the_feature_branch_only():
    on = triggers(load())
    assert isinstance(on, dict)
    assert set(on) == {"workflow_dispatch", "push"}
    assert "pull_request" not in on and "pull_request_target" not in on
    assert on["push"]["branches"] == [BRANCH]
    paths = set(on["push"]["paths"])
    assert {
        ".github/workflows/databricks-bronze-silver.yml",
        "repopulse/databricks_job.py",
        "repopulse/spark/**",
        "pyproject.toml",
    } <= paths


def test_dispatch_inputs_date_hour_hours_are_optional():
    inputs = triggers(load())["workflow_dispatch"]["inputs"]
    assert {"date", "hour", "hours"} <= set(inputs)
    for name in ("date", "hour", "hours"):
        assert inputs[name].get("required", False) is False, name


def test_explains_push_trigger_in_comment():
    text = WORKFLOW.read_text(encoding="utf-8") if WORKFLOW.is_file() else ""
    comments = "\n".join(line for line in text.splitlines() if line.lstrip().startswith("#"))
    assert "workflow_dispatch" in comments
    assert "main" in comments
    assert BRANCH in comments
    assert "incremental batch" in comments.lower()


def test_permissions_are_minimal():
    data = load()
    assert data.get("permissions") == {"contents": "read"}
    for job in jobs(data).values():
        assert job.get("permissions", data["permissions"]) == {"contents": "read"}


def test_every_job_has_a_bounded_timeout():
    for name, job in jobs(load()).items():
        assert isinstance(job.get("timeout-minutes"), int), name
        assert 0 < job["timeout-minutes"] <= 90, name


def test_concurrency_does_not_cancel_a_running_job():
    conc = load().get("concurrency")
    assert isinstance(conc, dict) and conc.get("group")
    assert conc.get("cancel-in-progress") is False


def test_job_names_do_not_shadow_required_checks():
    for key, job in jobs(load()).items():
        assert job.get("name", key) not in REQUIRED_CHECK_NAMES
        assert key not in REQUIRED_CHECK_NAMES


def test_no_job_level_if():
    for name, job in jobs(load()).items():
        assert "if" not in job, name


def test_secrets_only_appear_as_plain_env_values():
    seen = set()
    for path, value in walk(load()):
        if not isinstance(value, str) or "secrets." not in value:
            continue
        assert len(path) >= 2 and path[-2] == "env", f"secret outside env: {path}"
        assert SECRET_EXPR.match(value.strip()), f"secret not a plain expression: {path}"
        name = SECRET_EXPR_NAME.match(value.strip()).group(1)
        assert name == path[-1], f"env name != secret name: {path}"
        seen.add(path[-1])
    assert set(SECRET_NAMES) <= seen


def test_run_steps_never_interpolate_expressions_or_print_secrets():
    for step in runs(load()):
        run = step["run"]
        assert "${{" not in run, f"expression interpolated into a shell script: {step}"
        assert "set -x" not in run
        assert not re.search(r"^\s*(env|printenv)\s*$", run, re.M), run
        for line in run.splitlines():
            if re.search(r"\b(echo|printf)\b", line):
                for name in SECRET_NAMES:
                    assert name not in line, line


def test_uses_python_311():
    setup = [s for s in steps(load()) if str(s.get("uses", "")).startswith("actions/setup-python@")]
    assert len(setup) == 1
    assert str(setup[0]["with"]["python-version"]) == "3.11"


def test_ingests_gh_archive_with_the_ingest_module():
    found = [s for s in runs(load()) if "python -m repopulse.ingest" in s["run"]]
    assert len(found) == 1
    assert "--landing-dir" in found[0]["run"]
    assert "--summary" in found[0]["run"]


def test_builds_a_wheel():
    wheel = re.compile(r"python -m build\b[^\n]*--wheel|pip wheel\b")
    assert len([s for s in runs(load()) if wheel.search(s["run"])]) == 1


def test_runs_the_client_with_rerun_check_gated_at_step_level():
    data = load()
    found = client_steps(data)
    assert len(found) == 1
    step = found[0]
    assert HAS_HOST.search(str(step.get("if", ""))), step.get("if")
    run = step["run"]
    for flag in ("--verify-rerun", "--wheel", "--git-sha", "--landing-dir", "--summary"):
        assert flag in run, flag
    assert re.search(rf"--out\s+{re.escape(ROW_FILE)}\b", run), run


def test_client_step_gets_the_three_secrets_through_env():
    data = load()
    (step,) = client_steps(data)
    job_env = next(
        job.get("env", {}) for job in jobs(data).values() if step in job.get("steps", [])
    )
    env = {**job_env, **step.get("env", {})}
    for name in SECRET_NAMES:
        assert SECRET_EXPR.match(str(env.get(name, "")).strip()), name


def test_missing_host_is_a_notice_not_a_failure():
    notices = [
        s for s in runs(load()) if "::notice" in s["run"] and NO_HOST.search(str(s.get("if")))
    ]
    assert len(notices) == 1


def test_steps_run_in_order_ingest_wheel_client():
    data = load()
    order = runs(data)
    ingest = next(i for i, s in enumerate(order) if "python -m repopulse.ingest" in s["run"])
    wheel = next(
        i for i, s in enumerate(order) if re.search(r"python -m build\b|pip wheel\b", s["run"])
    )
    client = order.index(client_steps(data)[0])
    assert ingest < client
    assert wheel < client


def test_uploads_artifact_always_and_writes_job_summary():
    data = load()
    upload = [
        s for s in steps(data) if str(s.get("uses", "")).startswith("actions/upload-artifact")
    ]
    assert len(upload) == 1
    assert upload[0].get("if") == "always()"
    assert upload[0]["with"]["name"] == "databricks-bronze-silver"
    assert ROW_FILE in upload[0]["with"]["path"]
    summary = [s for s in runs(data) if "GITHUB_STEP_SUMMARY" in s["run"]]
    assert summary and all("always()" in str(s.get("if", "")) for s in summary)
