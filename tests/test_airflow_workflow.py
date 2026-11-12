"""Static checks on .github/workflows/airflow.yml (no GitHub, no network, no Airflow).

The workflow runs ``airflow standalone`` on the runner, backfills the ``repopulse_pipeline`` DAG
over N days of real GH Archive data (one hour per day) against local DuckDB (target ci) and writes
one ``runs.csv`` row (variant ``airflow-backfill-<N>d``) to an artifact. It needs no secret.
"""

from __future__ import annotations

from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = ROOT / ".github" / "workflows" / "airflow.yml"
REQUIRED_CHECK_NAMES = {"Lint", "Unit Tests", "dbt build (DuckDB)"}
CONSTRAINTS = (
    "https://raw.githubusercontent.com/apache/airflow/constraints-3.4.0/constraints-3.11.txt"
)


def load() -> dict:
    assert WORKFLOW.is_file(), f"missing workflow file {WORKFLOW}"
    data = yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))
    assert isinstance(data, dict), "workflow must be a YAML mapping"
    return data


def triggers(data):
    return data.get("on", data.get(True))  # PyYAML reads the bare key `on` as True


def only_job(data) -> dict:
    jobs = data["jobs"]
    assert len(jobs) == 1, sorted(jobs)
    return next(iter(jobs.values()))


def steps(data) -> list[dict]:
    return only_job(data)["steps"]


def runs(data) -> str:
    return "\n".join(s.get("run", "") for s in steps(data))


def step_index(data, needle: str) -> int:
    found = [i for i, s in enumerate(steps(data)) if needle in s.get("run", "")]
    assert len(found) == 1, f"expected one step running {needle!r}, found {found}"
    return found[0]


def test_workflow_name():
    assert load()["name"] == "Airflow backfill"


def test_triggers_dispatch_with_inputs_and_path_filtered_pull_request():
    on = triggers(load())
    inputs = on["workflow_dispatch"]["inputs"]
    assert {"days", "last_data_date", "hour", "hours_per_day"} <= set(inputs)
    assert str(inputs["days"]["default"]) == "3"
    pr = on["pull_request"]
    assert set(pr["branches"]) == {"develop", "main"}
    assert {
        ".github/workflows/airflow.yml",
        "dags/**",
        "repopulse/dagspec.py",
        "tests/test_airflow_dag.py",
    } <= set(pr["paths"])
    assert "schedule" not in on
    assert "push" not in on
    assert "pull_request_target" not in on


def test_permissions_read_only_and_concurrency_per_ref():
    data = load()
    assert data["permissions"] == {"contents": "read"}
    assert "permissions" not in only_job(data)
    conc = data["concurrency"]
    assert conc["group"].startswith("airflow-")
    assert "github.head_ref || github.ref" in conc["group"]


def test_single_job_bounded_and_not_a_required_check():
    data = load()
    key = next(iter(data["jobs"]))
    job = only_job(data)
    assert key not in REQUIRED_CHECK_NAMES
    assert job.get("name", key) not in REQUIRED_CHECK_NAMES
    assert "if" not in job
    assert isinstance(job["timeout-minutes"], int)
    assert 0 < job["timeout-minutes"] <= 240


def test_sets_up_python_311_java_17_and_the_project():
    data = load()
    by_uses = {str(s.get("uses", "")).split("@")[0]: s for s in steps(data)}
    assert str(by_uses["actions/setup-python"]["with"]["python-version"]) == "3.11"
    java = by_uses["actions/setup-java"]["with"]
    assert java["distribution"] == "temurin" and str(java["java-version"]) == "17"
    assert 'pip install -e ".[dev,spark,dbt]"' in runs(data)


def test_airflow_is_pinned_with_constraints_in_its_own_venv():
    text = runs(load())
    assert 'python -m venv "$AIRFLOW_VENV"' in text
    assert '"apache-airflow==3.4.0"' in text
    assert f'--constraint "{CONSTRAINTS}"' in text
    assert '"$AIRFLOW_VENV/bin/pip" install --no-deps -e .' in text


def test_airflow_environment():
    data = load()
    env = only_job(data)["env"]
    assert env["AIRFLOW__CORE__LOAD_EXAMPLES"] == "False"
    assert env["PII_HASH_SALT"] == "not-a-secret-pr-salt"
    text = runs(data)
    assert "AIRFLOW__CORE__DAGS_FOLDER=$GITHUB_WORKSPACE/dags" in text
    assert "AIRFLOW_HOME=$RUNNER_TEMP/airflow-home" in text
    assert "REPOPULSE_HOME=$GITHUB_WORKSPACE" in text
    assert "REPOPULSE_PYTHON=" in text


def test_dagbag_test_runs_with_airflow_required():
    data = load()
    i = step_index(data, "tests/test_airflow_dag.py")
    step = steps(data)[i]
    assert '"$AIRFLOW_VENV/bin/python" -m pytest tests/test_airflow_dag.py' in step["run"]
    assert step["env"]["REPOPULSE_REQUIRE_AIRFLOW"] == "1"


def test_steps_in_order():
    data = load()
    order = [
        step_index(data, "tests/test_airflow_dag.py"),
        step_index(data, "-m repopulse.dagspec window"),
        step_index(data, "db migrate"),
        step_index(data, "standalone"),
        step_index(data, "dags unpause"),
        step_index(data, "backfill create"),
        step_index(data, "-m repopulse.dagspec wait"),
        step_index(data, "-m repopulse.dagspec row"),
    ]
    assert order == sorted(order), order


def test_standalone_runs_in_background_with_a_log_file():
    data = load()
    run = steps(data)[step_index(data, "standalone")]["run"]
    assert '"$AIRFLOW" standalone >' in run
    assert "standalone.log" in run
    assert run.rstrip().endswith("&") or "&\n" in run
    # `airflow standalone` starts its components with the bare command `airflow` (run
    # 43134761449: FileNotFoundError 'airflow', nothing ran): the venv must be on PATH, after the
    # project's interpreter.
    assert 'PATH="$PATH:$AIRFLOW_VENV/bin" nohup "$AIRFLOW" standalone' in run
    # The step fails early when the components do not come up, instead of a long backfill wait.
    assert 'grep -q "Airflow is ready"' in run
    assert "exit 1" in run


def test_no_generated_password_in_the_uploaded_log():
    # With every user an admin, the simple auth manager generates and prints no password; the
    # standalone log is uploaded as an artifact.
    env = only_job(load())["env"]
    assert env["AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_ALL_ADMINS"] == "True"


def test_backfill_command():
    data = load()
    run = steps(data)[step_index(data, "backfill create")]["run"]
    assert "--dag-id repopulse_pipeline" in run
    assert '--from-date "$BACKFILL_FROM"' in run
    assert '--to-date "$BACKFILL_TO"' in run
    assert "--max-active-runs 1" in run
    assert "BACKFILL_STARTED_AT=" in run


def test_wait_and_row():
    data = load()
    wait = steps(data)[step_index(data, "-m repopulse.dagspec wait")]["run"]
    assert '--started-at "$BACKFILL_STARTED_AT"' in wait
    assert "--timeout-minutes" in wait
    assert "--summary out/airflow/backfill-summary.json" in wait
    assert "--raw out/airflow/list-runs.json" in wait
    row = steps(data)[step_index(data, "-m repopulse.dagspec row")]["run"]
    assert "--out out/airflow-backfill-row.csv" in row
    # On pull_request GITHUB_SHA is the throwaway merge commit: the row records the head commit.
    assert '--git-sha "$GIT_SHA"' in row
    text = WORKFLOW.read_text(encoding="utf-8")
    assert "GIT_SHA: ${{ github.event.pull_request.head.sha || github.sha }}" in text


def test_summary_and_artifact_always():
    data = load()
    summary = [s for s in steps(data) if "GITHUB_STEP_SUMMARY" in s.get("run", "")]
    assert summary and all(s.get("if") == "always()" for s in summary)
    uploads = [
        s for s in steps(data) if str(s.get("uses", "")).startswith("actions/upload-artifact")
    ]
    assert len(uploads) == 1
    up = uploads[0]
    assert up.get("if") == "always()"
    assert "out/" in up["with"]["path"]
    assert "airflow-home" in up["with"]["path"]


def test_no_secrets_no_motherduck_no_runs_csv():
    text = WORKFLOW.read_text(encoding="utf-8")
    assert "secrets." not in text
    assert "MOTHERDUCK" not in text
    assert "--target motherduck" not in text
    body = runs(load())
    assert "record_run" not in body
    assert "runs.csv" not in body
    for step in steps(load()):
        assert "${{" not in step.get("run", ""), step
