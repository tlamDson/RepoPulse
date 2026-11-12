"""Static checks on .github/workflows/engine-matrix.yml (no GitHub, no network, no cloud)."""

from __future__ import annotations

import re
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = ROOT / ".github" / "workflows" / "engine-matrix.yml"
BRANCH = "feature/engine-matrix"
REQUIRED_CHECK_NAMES = {"Lint", "Unit Tests", "dbt build (DuckDB)"}
SCRIPT = "benchmarks/engine_matrix.py"
ROWS = "out/engine-matrix-rows.csv"
SUMMARY = "out/engine-matrix-summary.md"
LOCAL_ENGINES = ("duckdb", "spark")
CLOUD_ENGINES = ("databricks-serverless", "databricks-sql", "motherduck")
SECRET_NAMES = ("DATABRICKS_HOST", "DATABRICKS_TOKEN", "DATABRICKS_HTTP_PATH", "MOTHERDUCK_TOKEN")
SECRET_EXPR = re.compile(r"^\$\{\{\s*secrets\.[A-Z][A-Z0-9_]*\s*\}\}$")
SECRET_EXPR_NAME = re.compile(r"^\$\{\{\s*secrets\.([A-Z][A-Z0-9_]*)\s*\}\}$")
GATES = {
    "databricks-serverless": ("DATABRICKS_HOST", "DATABRICKS_TOKEN"),
    "databricks-sql": ("DATABRICKS_HOST", "DATABRICKS_TOKEN"),
    "motherduck": ("MOTHERDUCK_TOKEN",),
}


def load():
    assert WORKFLOW.is_file(), f"missing workflow file {WORKFLOW}"
    data = yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))
    assert isinstance(data, dict), "workflow must be a YAML mapping"
    return data


def triggers(data):
    return data.get("on", data.get(True))  # PyYAML reads the bare key `on` as True


def jobs(data):
    return data["jobs"]


def steps(data):
    return [s for job in jobs(data).values() for s in job.get("steps", [])]


def runs(data):
    return [s for s in steps(data) if "run" in s]


def job_env(data):
    (job,) = jobs(data).values()
    return job.get("env", {})


def walk(node, path=()):
    if isinstance(node, dict):
        for k, v in node.items():
            yield from walk(v, path + (k,))
    elif isinstance(node, list):
        for i, v in enumerate(node):
            yield from walk(v, path + (i,))
    else:
        yield path, node


def engine_steps(data, engine):
    pattern = re.compile(rf"{re.escape(SCRIPT)}\b.*--engine\s+{re.escape(engine)}(\s|$)", re.S)
    return [s for s in runs(data) if pattern.search(s["run"])]


def test_triggers_are_dispatch_and_push_to_the_feature_branch_only():
    on = triggers(load())
    assert isinstance(on, dict)
    assert set(on) == {"workflow_dispatch", "push"}
    assert on["push"]["branches"] == [BRANCH]
    paths = set(on["push"]["paths"])
    assert {".github/workflows/engine-matrix.yml", SCRIPT} <= paths
    assert "benchmarks/queries/**" in paths and "benchmarks/queries_spark/**" in paths


def test_recording_results_does_not_trigger_a_new_measurement():
    paths = triggers(load())["push"]["paths"]
    for path in paths:
        assert not path.startswith("!"), "use an allow list of paths"
        for recorded in ("runs.csv", "README.md", "docs/engine-matrix.md"):
            assert not re.fullmatch(path.replace("**", ".*").replace("*", "[^/]*"), recorded), (
                path,
                recorded,
            )
    assert "repopulse/readme_results.py" not in paths
    assert "repopulse/engine_matrix.py" not in paths


def test_comment_explains_the_triggers():
    text = WORKFLOW.read_text(encoding="utf-8") if WORKFLOW.is_file() else ""
    comments = "\n".join(ln for ln in text.splitlines() if ln.lstrip().startswith("#"))
    assert "workflow_dispatch" in comments and "main" in comments and BRANCH in comments
    assert "quota" in comments.lower()


def test_permissions_are_minimal():
    data = load()
    assert data.get("permissions") == {"contents": "read"}
    for job in jobs(data).values():
        assert job.get("permissions", data["permissions"]) == {"contents": "read"}


def test_one_job_with_a_bounded_timeout_and_no_job_level_if():
    data = load()
    assert len(jobs(data)) == 1
    for key, job in jobs(data).items():
        assert isinstance(job.get("timeout-minutes"), int) and 0 < job["timeout-minutes"] <= 180
        assert "if" not in job, key
        assert job.get("name", key) not in REQUIRED_CHECK_NAMES and key not in REQUIRED_CHECK_NAMES


def test_concurrency_does_not_cancel_a_running_measurement():
    conc = load().get("concurrency")
    assert isinstance(conc, dict) and conc.get("group")
    assert conc.get("cancel-in-progress") is False


def test_secrets_only_appear_as_plain_env_values():
    seen = set()
    for path, value in walk(load()):
        if not isinstance(value, str) or "secrets." not in value:
            continue
        assert len(path) >= 2 and path[-2] == "env", f"secret outside env: {path}"
        assert SECRET_EXPR.match(value.strip()), f"secret not a plain expression: {path}"
        assert SECRET_EXPR_NAME.match(value.strip()).group(1) == path[-1], path
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


def test_python_311_java_17_and_duckdb_pinned_for_motherduck():
    data = load()
    setup = [s for s in steps(data) if str(s.get("uses", "")).startswith("actions/setup-python@")]
    assert len(setup) == 1 and str(setup[0]["with"]["python-version"]) == "3.11"
    java = [s for s in steps(data) if str(s.get("uses", "")).startswith("actions/setup-java@")]
    assert len(java) == 1 and str(java[0]["with"]["java-version"]) == "17"
    installs = [s["run"] for s in runs(data) if "pip install" in s["run"]]
    assert len(installs) == 1
    assert re.search(r"""\.\[[^\]]*\bspark\b[^\]]*\]""", installs[0]), installs[0]
    assert re.search(r"""["']?duckdb==1\.5\.6["']?""", installs[0]), installs[0]


def test_reference_day_is_verified_before_any_engine():
    data = load()
    order = runs(data)
    fetch = [s for s in order if "benchmarks/fetch_reference.py" in s["run"]]
    assert len(fetch) == 1
    first_engine = min(order.index(engine_steps(data, e)[0]) for e in LOCAL_ENGINES + CLOUD_ENGINES)
    assert order.index(fetch[0]) < first_engine


@pytest.mark.parametrize("engine", LOCAL_ENGINES + CLOUD_ENGINES)
def test_each_measured_engine_has_one_step_appending_to_the_shared_artifact(engine):
    data = load()
    found = engine_steps(data, engine)
    assert len(found) == 1, engine
    step = found[0]
    run = step["run"]
    assert re.search(rf"--out\s+{re.escape(ROWS)}\b", run)
    assert re.search(rf"--summary\s+{re.escape(SUMMARY)}\b", run)
    assert "--git-sha" in run and re.search(r"--runs\s+\d+", run)
    assert step.get("id"), "outcome is reported in the summary"
    assert step.get("continue-on-error") is True, "one engine must not block the others"


def test_snowflake_is_never_measured():
    for step in runs(load()):
        assert not re.search(r"--engine\s+snowflake\b", step["run"]), step


@pytest.mark.parametrize("engine", CLOUD_ENGINES)
def test_cloud_engine_steps_are_skipped_at_step_level_without_their_secrets(engine):
    data = load()
    (step,) = engine_steps(data, engine)
    cond = str(step.get("if", ""))
    for name in GATES[engine]:
        assert re.search(rf"env\.{name}\s*!=\s*''", cond), (engine, cond)
        assert SECRET_EXPR.match(str(job_env(data).get(name, ""))), name


@pytest.mark.parametrize("names", [("DATABRICKS_HOST", "DATABRICKS_TOKEN"), ("MOTHERDUCK_TOKEN",)])
def test_missing_secrets_are_a_notice(names):
    notices = [s for s in runs(load()) if "::notice" in s["run"]]
    matching = [
        s
        for s in notices
        if all(re.search(rf"env\.{n}\s*==\s*''", str(s.get("if", ""))) for n in names)
    ]
    assert len(matching) == 1, names


def test_the_warehouse_http_path_secret_reaches_only_the_databricks_sql_step():
    data = load()
    (sql_step,) = engine_steps(data, "databricks-sql")
    assert SECRET_EXPR.match(str(sql_step.get("env", {}).get("DATABRICKS_HTTP_PATH", "")))
    assert "DATABRICKS_HTTP_PATH" not in job_env(data)
    for step in steps(data):
        if step is not sql_step:
            assert "DATABRICKS_HTTP_PATH" not in step.get("env", {}), step


def test_databricks_steps_mask_derived_values():
    data = load()
    for engine in ("databricks-serverless", "databricks-sql"):
        (step,) = engine_steps(data, engine)
        assert "--add-mask" in step["run"], engine


def test_summary_reports_every_engine_outcome_and_artifact_is_always_uploaded():
    data = load()
    summary = [s for s in runs(data) if "GITHUB_STEP_SUMMARY" in s["run"]]
    assert summary and all("always()" in str(s.get("if", "")) for s in summary)
    env = {k: str(v) for s in summary for k, v in s.get("env", {}).items()}
    for engine in LOCAL_ENGINES + CLOUD_ENGINES:
        (step,) = engine_steps(data, engine)
        assert any(re.search(rf"steps\.{re.escape(step['id'])}\.outcome", v) for v in env.values())
    upload = [
        s for s in steps(data) if str(s.get("uses", "")).startswith("actions/upload-artifact")
    ]
    assert len(upload) == 1 and upload[0].get("if") == "always()"
    assert upload[0]["with"]["name"] == "engine-matrix"
    assert ROWS in upload[0]["with"]["path"] and SUMMARY in upload[0]["with"]["path"]


def test_a_failed_engine_turns_the_run_red_after_the_artifact_is_uploaded():
    data = load()
    order = steps(data)
    upload = next(s for s in order if str(s.get("uses", "")).startswith("actions/upload-artifact"))
    final = order[-1]
    assert "run" in final and "always()" in str(final.get("if", ""))
    assert "exit 1" in final["run"] and "::error" in final["run"]
    assert order.index(upload) < order.index(final)
