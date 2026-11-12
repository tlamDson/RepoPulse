"""Static checks on .github/workflows/dbt-databricks.yml (no GitHub, no network, no Databricks)."""

import re
from pathlib import Path

import pytest
import yaml

from repopulse.spark.tables import table_names

ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = ROOT / ".github" / "workflows" / "dbt-databricks.yml"
BRANCH = "feature/dbt-databricks-target"
REQUIRED_CHECK_NAMES = {"Lint", "Unit Tests", "dbt build (DuckDB)"}
SECRET_NAMES = ("DATABRICKS_HOST", "DATABRICKS_TOKEN", "DATABRICKS_HTTP_PATH", "PII_HASH_SALT")
SECRET_EXPR = re.compile(r"^\$\{\{\s*secrets\.[A-Z][A-Z0-9_]*\s*\}\}$")
SECRET_EXPR_NAME = re.compile(r"^\$\{\{\s*secrets\.([A-Z][A-Z0-9_]*)\s*\}\}$")
HAS_SECRETS = (
    re.compile(r"env\.DATABRICKS_HOST\s*!=\s*''"),
    re.compile(r"env\.DATABRICKS_TOKEN\s*!=\s*''"),
)
NO_SECRETS = (
    re.compile(r"env\.DATABRICKS_HOST\s*==\s*''"),
    re.compile(r"env\.DATABRICKS_TOKEN\s*==\s*''"),
)
ROW_SCRIPT = "benchmarks/dbt_databricks.py"
ROW_FILE = "out/dbt-databricks-rows.csv"
SILVER_SCHEMA = table_names("gharchive", schema="repopulse", catalog="workspace")["silver"].split(
    "."
)[1]


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


def job_env(data):
    (job,) = jobs(data).values()
    return job.get("env", {})


def step_env(data, step):
    return {**job_env(data), **step.get("env", {})}


def walk(node, path=()):
    if isinstance(node, dict):
        for k, v in node.items():
            yield from walk(v, path + (k,))
    elif isinstance(node, list):
        for i, v in enumerate(node):
            yield from walk(v, path + (i,))
    else:
        yield path, node


def dbt_steps(data, command: str, target: str = "databricks"):
    pattern = re.compile(rf"\bdbt\s+{command}\b.*--target\s+{target}\b", re.S)
    return [s for s in runs(data) if pattern.search(s["run"])]


def gated_on_secrets(step) -> bool:
    cond = str(step.get("if", ""))
    return all(p.search(cond) for p in HAS_SECRETS)


def test_triggers_are_dispatch_and_push_to_the_feature_branch_only():
    on = triggers(load())
    assert isinstance(on, dict)
    assert set(on) == {"workflow_dispatch", "push"}
    assert "pull_request" not in on and "pull_request_target" not in on
    assert on["push"]["branches"] == [BRANCH]
    paths = set(on["push"]["paths"])
    assert {
        "transform/**",
        ".github/workflows/dbt-databricks.yml",
        "benchmarks/dbt_databricks*",
        "repopulse/probe.py",
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


def test_one_job_with_a_bounded_timeout():
    data = load()
    assert len(jobs(data)) == 1
    for name, job in jobs(data).items():
        assert isinstance(job.get("timeout-minutes"), int), name
        assert 0 < job["timeout-minutes"] <= 60, name


def test_concurrency_does_not_cancel_a_running_build():
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


def test_checkout_fetches_full_history_for_the_line_count():
    checkout = [s for s in steps(load()) if str(s.get("uses", "")).startswith("actions/checkout@")]
    assert len(checkout) == 1
    assert checkout[0]["with"]["fetch-depth"] == 0


def test_uses_python_311():
    setup = [s for s in steps(load()) if str(s.get("uses", "")).startswith("actions/setup-python@")]
    assert len(setup) == 1
    assert str(setup[0]["with"]["python-version"]) == "3.11"


def test_installs_dbt_databricks_through_the_extra():
    installs = [s["run"] for s in runs(load()) if "pip install" in s["run"]]
    assert len(installs) == 1
    assert re.search(r"""-e\s+["']?\.\[[^\]]*\bdatabricks\b[^\]]*\]""", installs[0]), installs[0]
    assert re.search(r"""\.\[[^\]]*\bdbt\b[^\]]*\]""", installs[0]), installs[0]


def test_missing_secrets_are_a_notice_not_a_failure():
    notices = [s for s in runs(load()) if "::notice" in s["run"]]
    assert len(notices) == 1
    cond = str(notices[0].get("if", ""))
    assert all(p.search(cond) for p in NO_SECRETS), cond
    assert "||" in cond, "either secret missing skips the build"


def test_http_path_is_resolved_like_the_probe_and_only_that_step_sees_the_secret():
    data = load()
    resolve = [s for s in runs(data) if "python -m repopulse.probe http-path" in s["run"]]
    assert len(resolve) == 1
    assert gated_on_secrets(resolve[0]), resolve[0].get("if")
    assert "--github-env" in resolve[0]["run"]
    assert SECRET_EXPR.match(str(resolve[0].get("env", {}).get("DATABRICKS_HTTP_PATH", "")))
    assert "DATABRICKS_HTTP_PATH" not in job_env(data)
    for step in steps(data):
        if step is resolve[0]:
            continue
        value = str(step.get("env", {}).get("DATABRICKS_HTTP_PATH", ""))
        assert "secrets." not in value, step


@pytest.mark.parametrize("command", ["debug", "build"])
def test_dbt_debug_and_build_run_on_databricks_gated_at_step_level(command):
    data = load()
    found = dbt_steps(data, command)
    assert len(found) == 1, command
    step = found[0]
    assert gated_on_secrets(step), (command, step.get("if"))
    assert "--project-dir transform" in step["run"] and "--profiles-dir transform" in step["run"]
    http_path = str(step_env(data, step).get("DATABRICKS_HTTP_PATH", ""))
    assert re.fullmatch(r"\$\{\{\s*env\.DBT_DATABRICKS_HTTP_PATH\s*\}\}", http_path), http_path


def test_build_writes_to_its_own_schema_not_the_silver_schema_of_d1():
    data = load()
    (build,) = dbt_steps(data, "build")
    schema = str(step_env(data, build).get("DBT_DATABRICKS_SCHEMA", ""))
    assert re.fullmatch(r"[a-z][a-z0-9_]*", schema), schema
    assert schema != SILVER_SCHEMA
    assert schema != "repopulse_probe"


def test_build_has_the_pii_salt_and_no_full_refresh():
    data = load()
    (build,) = dbt_steps(data, "build")
    assert SECRET_EXPR.match(str(step_env(data, build).get("PII_HASH_SALT", "")))
    for step in runs(data):
        assert "--full-refresh" not in step["run"], step
    assert not dbt_steps(data, "build", "ci"), "this workflow builds on Databricks only"


def test_input_stats_come_from_dbt_show_on_staging_without_overwriting_run_results():
    data = load()
    show = dbt_steps(data, "show")
    assert len(show) == 1
    run = show[0]["run"]
    assert gated_on_secrets(show[0])
    assert "--inline" in run and "ref('stg_gharchive_events')" in run
    assert re.search(r"--output\s+json\b", run) and re.search(r"--log-format\s+json\b", run)
    target_path = re.search(r"--target-path\s+(\S+)", run)
    assert target_path and target_path.group(1) not in ("target", "target/"), run
    assert re.search(r">\s*out/dbt-databricks-show\.jsonl\b", run), run


def test_row_step_runs_after_build_and_show_even_when_the_build_fails():
    data = load()
    order = runs(data)
    rows = [s for s in order if ROW_SCRIPT in s["run"]]
    assert len(rows) == 1
    row = rows[0]
    cond = str(row.get("if", ""))
    assert "always()" in cond and all(p.search(cond) for p in HAS_SECRETS), cond
    run = row["run"]
    assert "--run-results transform/target/run_results.json" in run
    assert "--show-log out/dbt-databricks-show.jsonl" in run
    assert "--git-sha" in run and re.search(rf"--out\s+{re.escape(ROW_FILE)}\b", run)
    assert "--summary" in run
    build = order.index(dbt_steps(data, "build")[0])
    show = order.index(dbt_steps(data, "show")[0])
    assert build < show < order.index(row)


def test_uploads_artifact_always_and_writes_job_summary():
    data = load()
    upload = [
        s for s in steps(data) if str(s.get("uses", "")).startswith("actions/upload-artifact")
    ]
    assert len(upload) == 1
    assert upload[0].get("if") == "always()"
    assert upload[0]["with"]["name"] == "dbt-databricks"
    assert ROW_FILE in upload[0]["with"]["path"]
    assert "out/dbt-databricks-summary.md" in upload[0]["with"]["path"]
    summary = [s for s in runs(data) if "GITHUB_STEP_SUMMARY" in s["run"]]
    assert summary and all("always()" in str(s.get("if", "")) for s in summary)
