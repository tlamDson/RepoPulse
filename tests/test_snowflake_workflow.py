"""Static checks on .github/workflows/snowflake-port.yml (no GitHub, no network, no Snowflake)."""

import re
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = ROOT / ".github" / "workflows" / "snowflake-port.yml"
BRANCH = "feature/snowflake-port"
REQUIRED_CHECK_NAMES = {"Lint", "Unit Tests", "dbt build (DuckDB)"}
SECRET_NAMES = (
    "SNOWFLAKE_ACCOUNT",
    "SNOWFLAKE_USER",
    "SNOWFLAKE_PRIVATE_KEY",
    "SNOWFLAKE_ROLE",
    "SNOWFLAKE_WAREHOUSE",
    "SNOWFLAKE_DATABASE",
)
SECRET_EXPR = re.compile(r"^\$\{\{\s*secrets\.[A-Z][A-Z0-9_]*\s*\}\}$")
SECRET_EXPR_NAME = re.compile(r"^\$\{\{\s*secrets\.([A-Z][A-Z0-9_]*)\s*\}\}$")


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


def dbt_steps(data, command: str, target: str):
    pattern = re.compile(rf"\bdbt\s+{command}\b.*--target\s+{target}\b", re.S)
    return [s for s in runs(data) if pattern.search(s["run"])]


def test_triggers_are_dispatch_and_push_to_port_branch_only():
    on = triggers(load())
    assert isinstance(on, dict)
    assert set(on) == {"workflow_dispatch", "push"}
    assert "pull_request" not in on and "pull_request_target" not in on
    assert on["push"]["branches"] == [BRANCH]
    paths = set(on["push"]["paths"])
    assert {
        "transform/**",
        ".github/workflows/snowflake-port.yml",
        "benchmarks/snowflake_port*",
        "pyproject.toml",
    } <= paths


def test_explains_push_trigger_in_comment():
    text = WORKFLOW.read_text(encoding="utf-8") if WORKFLOW.is_file() else ""
    comments = "\n".join(line for line in text.splitlines() if line.lstrip().startswith("#"))
    assert "workflow_dispatch" in comments
    assert "main" in comments
    assert BRANCH in comments


def test_permissions_are_minimal():
    data = load()
    assert data.get("permissions") == {"contents": "read"}
    for job in jobs(data).values():
        assert job.get("permissions", data["permissions"]) == {"contents": "read"}


def test_every_job_has_timeout_of_at_most_an_hour():
    for name, job in jobs(load()).items():
        assert isinstance(job.get("timeout-minutes"), int), name
        assert 0 < job["timeout-minutes"] <= 60, name


def test_concurrency_does_not_cancel_a_running_port():
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


def test_run_steps_never_print_secrets():
    for step in runs(load()):
        run = step["run"]
        assert "secrets." not in run
        assert "set -x" not in run
        assert not re.search(r"^\s*(env|printenv)\s*$", run, re.M), run
        for line in run.splitlines():
            if re.search(r"\b(echo|printf)\b", line):
                for name in SECRET_NAMES:
                    assert name not in line, line


def test_checkout_fetches_full_history_for_the_line_count():
    checkout = [s for s in steps(load()) if str(s.get("uses", "")).startswith("actions/checkout@")]
    assert len(checkout) == 1
    assert checkout[0]["with"]["fetch-depth"] == 0


def test_uses_python_311():
    setup = [s for s in steps(load()) if str(s.get("uses", "")).startswith("actions/setup-python@")]
    assert len(setup) == 1
    assert str(setup[0]["with"]["python-version"]) == "3.11"


def test_installs_dbt_snowflake_through_the_extra():
    installs = [s["run"] for s in runs(load()) if "pip install" in s["run"]]
    assert len(installs) == 1
    assert re.search(r"""-e\s+["']?\.\[[^\]]*\bsnowflake\b[^\]]*\]""", installs[0]), installs[0]
    assert re.search(r"""\.\[[^\]]*\bdbt\b[^\]]*\]""", installs[0]), installs[0]


def test_builds_the_main_duckdb_version_on_ci_target_without_a_gate():
    data = load()
    build = dbt_steps(data, "build", "ci")
    assert len(build) == 1
    assert "if" not in build[0], "the DuckDB build is the reference: it always runs"
    assert "--project-dir transform" in build[0]["run"]


def test_snowflake_debug_and_build_are_gated_at_step_level_on_the_secret():
    data = load()
    for command in ("debug", "build"):
        found = dbt_steps(data, command, "snowflake")
        assert len(found) == 1, command
        cond = str(found[0].get("if", ""))
        assert re.search(r"env\.SNOWFLAKE_ACCOUNT\s*!=\s*''", cond), (command, cond)
        assert "--project-dir transform" in found[0]["run"]


def test_missing_secret_is_reported_as_a_notice_not_a_failure():
    data = load()
    notices = [
        s
        for s in runs(data)
        if "::notice" in s["run"]
        and re.search(r"env\.SNOWFLAKE_ACCOUNT\s*==\s*''", str(s.get("if")))
    ]
    assert len(notices) == 1


@pytest.mark.parametrize("engine", ["duckdb", "snowflake"])
def test_records_one_row_per_engine_with_the_port_script(engine):
    data = load()
    rows = [
        s
        for s in runs(data)
        if "benchmarks/snowflake_port.py" in s["run"]
        and re.search(rf"--engine\s+{engine}\b", s["run"])
    ]
    assert len(rows) == 1, engine
    run = rows[0]["run"]
    assert "--run-results" in run and "--git-sha" in run
    assert "out/snowflake-port-rows.csv" in run
    if engine == "snowflake":
        assert re.search(r"env\.SNOWFLAKE_ACCOUNT\s*!=\s*''", str(rows[0].get("if", "")))


def test_snowflake_row_is_built_after_the_snowflake_build_and_duckdb_row_before_it():
    data = load()
    order = runs(data)
    sf_build = order.index(dbt_steps(data, "build", "snowflake")[0])
    duck_build = order.index(dbt_steps(data, "build", "ci")[0])
    rows = {
        engine: next(
            i
            for i, s in enumerate(order)
            if "benchmarks/snowflake_port.py" in s["run"] and f"--engine {engine}" in s["run"]
        )
        for engine in ("duckdb", "snowflake")
    }
    assert duck_build < rows["duckdb"] < sf_build < rows["snowflake"]


def test_uploads_artifact_always_and_writes_job_summary():
    data = load()
    upload = [
        s for s in steps(data) if str(s.get("uses", "")).startswith("actions/upload-artifact")
    ]
    assert len(upload) == 1
    assert upload[0].get("if") == "always()"
    assert upload[0]["with"]["name"] == "snowflake-port"
    assert "out/snowflake-port-rows.csv" in upload[0]["with"]["path"]
    summary = [s for s in runs(data) if "GITHUB_STEP_SUMMARY" in s["run"]]
    assert summary and all("always()" in str(s.get("if", "")) for s in summary)
