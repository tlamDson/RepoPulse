"""Static checks on .github/workflows/path-comparison.yml (no GitHub, no network, no cloud)."""

import re
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = ROOT / ".github" / "workflows" / "path-comparison.yml"
BRANCH = "feature/pipeline-path-comparison"
REQUIRED_CHECK_NAMES = {"Lint", "Unit Tests", "dbt build (DuckDB)"}
SECRET_NAMES = (
    "DATABRICKS_HOST",
    "DATABRICKS_TOKEN",
    "DATABRICKS_HTTP_PATH",
    "MOTHERDUCK_TOKEN",
    "PII_HASH_SALT",
)
SECRET_EXPR = re.compile(r"^\$\{\{\s*secrets\.([A-Z][A-Z0-9_]*)\s*\}\}$")
JOB_A, JOB_B, JOB_WINDOW = "path-a", "path-b", "window"
PATH_OF_JOB = {JOB_A: "a", JOB_B: "b"}
STEPS = {
    "a": ("ingest", "spark", "export", "dbt", "queries"),
    "b": ("ingest", "databricks", "warehouse", "dbt", "queries"),
}
ROW_FILES = {"a": "out/path-a-row.csv", "b": "out/path-b-row.csv"}
GATE = re.compile(r"env\.HAS_SECRETS\s*==\s*'true'")
NO_GATE = re.compile(r"env\.HAS_SECRETS\s*!=\s*'true'")


def load():
    assert WORKFLOW.is_file(), f"missing workflow file {WORKFLOW}"
    data = yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))
    assert isinstance(data, dict), "workflow must be a YAML mapping"
    return data


def triggers(data):
    return data.get("on", data.get(True))  # PyYAML reads the bare key `on` as True


def job(data, key):
    return data["jobs"][key]


def runs(j):
    return [s for s in j.get("steps", []) if "run" in s]


def step_by_id(j, step_id):
    found = [s for s in j.get("steps", []) if s.get("id") == step_id]
    assert len(found) == 1, (step_id, found)
    return found[0]


def walk(node, path=()):
    if isinstance(node, dict):
        for k, v in node.items():
            yield from walk(v, path + (k,))
    elif isinstance(node, list):
        for i, v in enumerate(node):
            yield from walk(v, path + (i,))
    else:
        yield path, node


def comments():
    text = WORKFLOW.read_text(encoding="utf-8") if WORKFLOW.is_file() else ""
    return "\n".join(line for line in text.splitlines() if line.lstrip().startswith("#"))


# ---------------------------------------------------------------- triggers


def test_triggers_are_schedule_dispatch_and_push_to_this_branch():
    on = triggers(load())
    assert set(on) == {"schedule", "workflow_dispatch", "push"}
    assert "pull_request" not in on and "pull_request_target" not in on
    assert on["push"]["branches"] == [BRANCH]
    paths = set(on["push"]["paths"])
    assert {".github/workflows/path-comparison.yml", "repopulse/pathcompare.py"} <= paths


def test_cron_is_hourly_on_another_minute_than_the_pipeline():
    (entry,) = triggers(load())["schedule"]
    match = re.fullmatch(r"(\d{1,2}) \* \* \* \*", entry["cron"])
    assert match, entry["cron"]
    assert int(match.group(1)) != 23
    assert 0 <= int(match.group(1)) <= 59


def test_dispatch_inputs_are_optional():
    inputs = triggers(load())["workflow_dispatch"]["inputs"]
    assert set(inputs) >= {"date", "hour", "hours"}
    for name, spec in inputs.items():
        assert spec.get("required", False) is False, name


def test_comments_explain_main_only_triggers_and_the_batch_style():
    text = comments()
    assert "main" in text and BRANCH in text
    assert "workflow_dispatch" in text and "schedule" in text
    assert "incremental batch" in text
    assert "real-time" not in text.lower()


# ---------------------------------------------------------------- jobs


def test_three_jobs_window_then_two_independent_paths():
    data = load()
    assert set(data["jobs"]) == {JOB_WINDOW, JOB_A, JOB_B}
    for key in (JOB_A, JOB_B):
        needs = job(data, key).get("needs")
        assert needs in (JOB_WINDOW, [JOB_WINDOW]), (key, needs)


def test_both_paths_use_the_one_window_decided_once():
    data = load()
    window = job(data, JOB_WINDOW)
    assert set(window["outputs"]) >= {"date", "hour", "hours"}
    assert any("python -m repopulse.pathcompare window" in s["run"] for s in runs(window))
    for key in (JOB_A, JOB_B):
        env = job(data, key)["env"]
        for name, out in (("DATA_DATE", "date"), ("DATA_HOUR", "hour"), ("DATA_HOURS", "hours")):
            assert re.fullmatch(
                rf"\$\{{\{{\s*needs\.window\.outputs\.{out}\s*\}}\}}", str(env.get(name, ""))
            ), (key, name)


def test_jobs_have_timeouts_and_safe_names():
    for key, j in load()["jobs"].items():
        assert isinstance(j.get("timeout-minutes"), int) and 0 < j["timeout-minutes"] <= 120, key
        assert j.get("name", key) not in REQUIRED_CHECK_NAMES
        assert key not in REQUIRED_CHECK_NAMES
        assert "if" not in j, key


def test_permissions_are_read_only():
    data = load()
    assert data.get("permissions") == {"contents": "read"}
    for key in (JOB_A, JOB_B):
        assert job(data, key).get("permissions") == {"contents": "read", "actions": "read"}
    for _, value in walk(data.get("permissions")):
        assert value != "write"
    for j in data["jobs"].values():
        for _, value in walk(j.get("permissions", {})):
            assert value != "write"


def test_concurrency_never_cancels_and_b_shares_the_d1_group():
    data = load()
    conc = data.get("concurrency")
    assert isinstance(conc, dict) and conc.get("group") and conc.get("cancel-in-progress") is False
    b = job(data, JOB_B).get("concurrency")
    assert b == {"group": "databricks-bronze-silver", "cancel-in-progress": False}


# ---------------------------------------------------------------- secrets


def test_secrets_only_appear_as_plain_step_env_values():
    seen = set()
    for path, value in walk(load()):
        if not isinstance(value, str) or "secrets." not in value:
            continue
        assert path[0] == "jobs" and path[2] == "steps", f"secret outside a step: {path}"
        assert path[-2] == "env", f"secret outside env: {path}"
        match = SECRET_EXPR.match(value.strip())
        assert match, f"secret not a plain expression: {path}"
        assert match.group(1) == path[-1], f"env name != secret name: {path}"
        seen.add(path[-1])
    assert set(SECRET_NAMES) <= seen


def test_run_steps_never_interpolate_expressions_or_print_secrets():
    for j in load()["jobs"].values():
        for step in runs(j):
            run = step["run"]
            assert "${{" not in run, f"expression interpolated into a shell script: {step}"
            assert "set -x" not in run
            assert not re.search(r"^\s*(env|printenv)\s*$", run, re.M), run
            for line in run.splitlines():
                if re.search(r"\b(echo|printf)\b", line):
                    for name in SECRET_NAMES:
                        assert f"${name}" not in line and "${" + name not in line, line


@pytest.mark.parametrize("key", [JOB_A, JOB_B])
def test_missing_secrets_skip_at_step_level_with_a_notice(key):
    j = job(load(), key)
    detect = [s for s in runs(j) if "HAS_SECRETS=true" in s["run"]]
    assert len(detect) == 1
    notices = [s for s in runs(j) if "::notice::" in s["run"]]
    assert len(notices) == 1 and NO_GATE.search(str(notices[0].get("if", "")))
    for step_name in STEPS[PATH_OF_JOB[key]]:
        assert GATE.search(str(step_by_id(j, step_name).get("if", ""))), step_name


# ---------------------------------------------------------------- steps, rows, artifacts


@pytest.mark.parametrize("key", [JOB_A, JOB_B])
def test_every_step_is_timed_in_pipeline_order(key):
    j = job(load(), key)
    order = j["steps"]
    positions = []
    for name in STEPS[PATH_OF_JOB[key]]:
        step = step_by_id(j, name)
        assert re.search(
            rf"python -m repopulse\.pipeline timed --name {name} --timings out/timings\.json --",
            step["run"],
        ), name
        positions.append(order.index(step))
    assert positions == sorted(positions)


@pytest.mark.parametrize("key", [JOB_A, JOB_B])
def test_row_step_always_runs_with_every_outcome(key):
    path = PATH_OF_JOB[key]
    j = job(load(), key)
    rows = [s for s in runs(j) if "python -m repopulse.pathcompare row" in s["run"]]
    assert len(rows) == 1
    row = rows[0]
    cond = str(row.get("if", ""))
    assert "always()" in cond and GATE.search(cond), cond
    run = row["run"]
    assert re.search(rf"--path {path}\b", run)
    assert re.search(rf"--out {re.escape(ROW_FILES[path])}\b", run)
    assert "--timings out/timings.json" in run
    for name in STEPS[path]:
        env_name = f"OUTCOME_{name.upper()}"
        assert re.fullmatch(
            rf"\$\{{\{{\s*steps\.{name}\.outcome\s*\}}\}}", str(row["env"].get(env_name, ""))
        ), env_name
        assert f'--outcome "{name}=${env_name}"' in run, name
    assert j["steps"].index(row) > j["steps"].index(step_by_id(j, "queries"))


@pytest.mark.parametrize("key", [JOB_A, JOB_B])
def test_runner_queue_and_setup_come_from_the_jobs_api(key):
    j = job(load(), key)
    waits = [s for s in runs(j) if "python -m repopulse.pathcompare runner-waits" in s["run"]]
    assert len(waits) == 1
    assert f'--job-name "{j["name"]}"' in waits[0]["run"]
    assert j["steps"].index(waits[0]) < j["steps"].index(step_by_id(j, "ingest"))
    token = str(waits[0].get("env", {}).get("GH_TOKEN", ""))
    assert re.fullmatch(r"\$\{\{\s*github\.token\s*\}\}", token), token


@pytest.mark.parametrize("key", [JOB_A, JOB_B])
def test_artifact_and_job_summary_always(key):
    path = PATH_OF_JOB[key]
    j = job(load(), key)
    upload = [s for s in j["steps"] if str(s.get("uses", "")).startswith("actions/upload-artifact")]
    assert len(upload) == 1 and upload[0].get("if") == "always()"
    assert ROW_FILES[path] in upload[0]["with"]["path"]
    summary = [s for s in runs(j) if "GITHUB_STEP_SUMMARY" in s["run"]]
    assert summary and all("always()" in str(s.get("if", "")) for s in summary)


def test_path_a_gold_goes_to_its_own_motherduck_database():
    from repopulse.pathcompare import PATH_A_DATABASE

    assert PATH_A_DATABASE != "repopulse"
    j = job(load(), JOB_A)
    dbt = step_by_id(j, "dbt")
    assert re.search(r"dbt build .*--target motherduck", dbt["run"], re.S)
    assert dbt["env"]["MOTHERDUCK_DATABASE"] == PATH_A_DATABASE
    for path, value in walk(j):
        if isinstance(value, str):
            assert not re.search(r"md:repopulse(?![a-z0-9_])", value), path
        if path and path[-1] == "MOTHERDUCK_DATABASE":
            assert value == PATH_A_DATABASE, path
    ensure = [s for s in runs(j) if "python -m repopulse.pathcompare md-ensure" in s["run"]]
    assert len(ensure) == 1 and f"--database {PATH_A_DATABASE}" in ensure[0]["run"]
    queries = step_by_id(j, "queries")
    assert f"--database md:{PATH_A_DATABASE}" in queries["run"]
    assert "--engine duckdb" in queries["run"]
    installs = [s["run"] for s in runs(j) if "pip install" in s["run"]]
    assert installs and all('"duckdb==1.5.6"' in r for r in installs)
    assert "--full-refresh" not in WORKFLOW.read_text(encoding="utf-8")


def test_path_b_runs_d1_and_d2_on_databricks():
    j = job(load(), JOB_B)
    databricks = step_by_id(j, "databricks")
    assert "python -m repopulse.databricks_job run" in databricks["run"]
    assert "--verify-rerun" not in databricks["run"]
    assert "--out out/databricks-row.csv" in databricks["run"]
    assert any("python -m repopulse.probe http-path --github-env" in s["run"] for s in runs(j))
    warehouse = step_by_id(j, "warehouse")
    assert "python -m repopulse.pathcompare warehouse-start" in warehouse["run"]
    dbt = step_by_id(j, "dbt")
    assert re.search(r"dbt build .*--target databricks", dbt["run"], re.S)
    assert dbt["env"]["DBT_DATABRICKS_SCHEMA"] == "repopulse_dbt"
    queries = step_by_id(j, "queries")
    assert "--engine databricks" in queries["run"]
    assert "workspace.repopulse_dbt_gold" in queries["run"]
    (row,) = [s for s in runs(j) if "python -m repopulse.pathcompare row" in s["run"]]
    assert "--databricks-row out/databricks-row.csv" in row["run"]
