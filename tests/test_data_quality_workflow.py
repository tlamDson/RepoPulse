"""Static checks on .github/workflows/data-quality.yml (no GitHub, no network).

The workflow checks volume + freshness of the GH Archive staging table after each scheduled
pipeline run and opens / updates one ``data-incident`` issue per kind of check. On pull requests
it runs the same checks on the CI DuckDB with a dry-run alert, the six simkit faults (SIMULATED
app events) through the Spark batch, and the known GH Archive gap hour (real data).
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = ROOT / ".github" / "workflows" / "data-quality.yml"
PIPELINE = ROOT / ".github" / "workflows" / "pipeline.yml"
PROBE = ROOT / ".github" / "workflows" / "probe.yml"
REQUIRED_CHECK_NAMES = {"Lint", "Unit Tests", "dbt build (DuckDB)"}
PR = "github.event_name == 'pull_request'"
NOT_PR = "github.event_name != 'pull_request'"


def load() -> dict:
    assert WORKFLOW.is_file(), f"missing workflow file {WORKFLOW}"
    data = yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))
    assert isinstance(data, dict), "workflow must be a YAML mapping"
    return data


def triggers(data):
    return data.get("on", data.get(True))  # PyYAML reads the bare key `on` as True


def jobs(data) -> dict:
    return data["jobs"]


def job_running(data, needle: str) -> dict:
    found = [j for j in jobs(data).values() if any(needle in s.get("run", "") for s in j["steps"])]
    assert len(found) == 1, f"expected one job running {needle!r}, got {len(found)}"
    return found[0]


def steps_running(job: dict, needle: str) -> list[dict]:
    return [s for s in job["steps"] if needle in s.get("run", "")]


def all_steps(data) -> list[dict]:
    return [s for j in jobs(data).values() for s in j["steps"]]


def comments() -> str:
    text = WORKFLOW.read_text(encoding="utf-8") if WORKFLOW.is_file() else ""
    return "\n".join(line for line in text.splitlines() if line.lstrip().startswith("#"))


def walk(node, path=()):
    if isinstance(node, dict):
        for key, value in node.items():
            yield from walk(value, (*path, str(key)))
    elif isinstance(node, list):
        for i, value in enumerate(node):
            yield from walk(value, (*path, str(i)))
    elif isinstance(node, str):
        yield path, node


# ---------------------------------------------------------------- triggers


def test_runs_after_the_scheduled_pipeline_by_its_exact_name():
    pipeline_name = yaml.safe_load(PIPELINE.read_text(encoding="utf-8"))["name"]
    run = triggers(load())["workflow_run"]
    assert run["workflows"] == [pipeline_name]
    assert run["types"] == ["completed"]


def test_other_triggers():
    on = triggers(load())
    assert "workflow_dispatch" in on
    pr = on["pull_request"]
    assert set(pr["branches"]) == {"develop", "main"}
    assert {
        ".github/workflows/data-quality.yml",
        "repopulse/quality.py",
        "repopulse/quality.toml",
        "simkit/**",
        "repopulse/spark/batch.py",
        "repopulse/spark/job.py",
    } <= set(pr["paths"])
    for banned in ("pull_request_target", "push", "schedule"):
        assert banned not in on, banned


# ---------------------------------------------------------------- permissions and job shape


def test_workflow_permissions_are_read_only():
    assert load().get("permissions") == {"contents": "read"}


def test_only_the_alert_job_can_write_issues():
    data = load()
    writers = [
        name
        for name, job in jobs(data).items()
        if (job.get("permissions") or {}).get("issues") == "write"
    ]
    alert = job_running(data, "python -m repopulse.quality alert")
    assert writers == [next(n for n, j in jobs(data).items() if j is alert)]
    assert alert["permissions"] == {"contents": "read", "issues": "write"}
    for name, job in jobs(data).items():
        perms = job.get("permissions") or {}
        assert set(perms.values()) <= {"read", "write"}, name
        if job is not alert:
            assert "write" not in perms.values(), name


def test_every_job_has_a_timeout():
    for name, job in jobs(load()).items():
        timeout = job.get("timeout-minutes")
        assert isinstance(timeout, int) and 0 < timeout <= 60, name


def test_job_names_do_not_shadow_required_checks():
    for key, job in jobs(load()).items():
        assert key not in REQUIRED_CHECK_NAMES
        assert job.get("name", key) not in REQUIRED_CHECK_NAMES


def test_concurrency_serialises_issue_writers():
    conc = load().get("concurrency")
    assert isinstance(conc, dict)
    assert conc.get("cancel-in-progress") is False
    assert PR in conc["group"]


# ---------------------------------------------------------------- checks job


def test_checks_skip_pipeline_runs_of_pull_requests():
    checks = job_running(load(), "python -m repopulse.quality check")
    cond = checks.get("if", "")
    assert "github.event.workflow_run.event" in cond
    assert "'pull_request'" in cond


def test_checks_read_motherduck_with_the_token_only_in_step_env():
    checks = job_running(load(), "python -m repopulse.quality check")
    md = [
        s for s in steps_running(checks, "python -m repopulse.quality check") if "md:" in s["run"]
    ]
    assert len(md) == 1
    step = md[0]
    assert "--database md:repopulse" in step["run"]
    assert step["if"] == NOT_PR
    assert step["env"]["MOTHERDUCK_TOKEN"] == "${{ secrets.MOTHERDUCK_TOKEN }}"
    assert "MOTHERDUCK_TOKEN" not in (checks.get("env") or {})


def test_missing_motherduck_secret_stops_early_with_an_error():
    checks = job_running(load(), "python -m repopulse.quality check")
    guard = [s for s in checks["steps"] if "::error::" in s.get("run", "")]
    assert guard, "no step stops when MOTHERDUCK_TOKEN is missing"
    assert "MOTHERDUCK_TOKEN" in guard[0]["run"] and "exit 1" in guard[0]["run"]
    assert guard[0]["if"] == NOT_PR
    assert 'echo "$MOTHERDUCK_TOKEN' not in guard[0]["run"]
    md = next(s for s in checks["steps"] if "md:repopulse" in s.get("run", ""))
    assert checks["steps"].index(guard[0]) < checks["steps"].index(md)


def test_checks_pin_the_duckdb_version_motherduck_supports():
    probe_pin = re.search(r'"duckdb==([0-9.]+)"', PROBE.read_text(encoding="utf-8"))
    assert probe_pin
    checks = job_running(load(), "python -m repopulse.quality check")
    install = [s["run"] for s in steps_running(checks, "pip install")]
    assert install and f'"duckdb=={probe_pin.group(1)}"' in install[0]
    assert '".[dev,dbt]"' in install[0]


def test_pull_request_checks_run_on_the_ci_duckdb_built_from_the_seed():
    checks = job_running(load(), "python -m repopulse.quality check")
    build = steps_running(checks, "dbt build")
    assert len(build) == 1 and build[0]["if"] == PR
    assert "--target ci" in build[0]["run"]
    local = [
        s
        for s in steps_running(checks, "python -m repopulse.quality check")
        if "md:" not in s["run"]
    ]
    assert len(local) == 1 and local[0]["if"] == PR
    assert "--database ci.duckdb" in local[0]["run"]
    assert checks["steps"].index(build[0]) < checks["steps"].index(local[0])
    for step in build + local:
        assert "secrets." not in str(step.get("env", {}))


def test_report_is_handed_to_the_alert_job():
    data = load()
    checks = job_running(data, "python -m repopulse.quality check")
    alert = job_running(data, "python -m repopulse.quality alert")
    up = [
        s for s in checks["steps"] if str(s.get("uses", "")).startswith("actions/upload-artifact@")
    ]
    down = [
        s for s in alert["steps"] if str(s.get("uses", "")).startswith("actions/download-artifact@")
    ]
    assert up and down
    assert up[0]["with"]["name"] == down[0]["with"]["name"]
    checks_key = next(n for n, j in jobs(data).items() if j is checks)
    needs = alert.get("needs")
    assert needs == checks_key or checks_key in (needs or [])


# ---------------------------------------------------------------- alert job


def test_alert_is_a_dry_run_on_pull_requests():
    alert = job_running(load(), "python -m repopulse.quality alert")
    steps = steps_running(alert, "python -m repopulse.quality alert")
    pr_steps = [s for s in steps if s.get("if") == PR]
    real = [s for s in steps if s.get("if") == NOT_PR]
    assert len(pr_steps) == 1 and len(real) == 1
    assert "--dry-run" in pr_steps[0]["run"]
    assert "GH_TOKEN" not in (pr_steps[0].get("env") or {})
    assert "--dry-run" not in real[0]["run"]
    assert real[0]["env"]["GH_TOKEN"] == "${{ github.token }}"
    assert '--repo "$GITHUB_REPOSITORY"' in real[0]["run"]
    for step in steps:
        assert "--run-url" in step["run"]


def test_no_step_calls_gh_directly():
    for step in all_steps(load()):
        run = step.get("run", "")
        assert not re.search(r"(^|\s)gh\s", run), step.get("name")


# ---------------------------------------------------------------- fault injection (simulated)


def test_fault_injection_runs_on_pr_and_dispatch_only_with_java_17():
    data = load()
    job = job_running(data, "python -m repopulse.quality faults")
    assert "github.event_name != 'workflow_run'" in job.get("if", "")
    java = [s for s in job["steps"] if str(s.get("uses", "")).startswith("actions/setup-java@")]
    assert java and str(java[0]["with"]["java-version"]) == "17"
    install = [s["run"] for s in steps_running(job, "pip install")]
    assert install and '-e ".[dev,spark,dbt]"' in install[0]


def test_fault_injection_uses_a_fixed_fake_salt_and_writes_the_row():
    job = job_running(load(), "python -m repopulse.quality faults")
    (step,) = steps_running(job, "python -m repopulse.quality faults")
    assert step["env"]["PII_HASH_SALT"] == "not-a-secret-pr-salt"
    run = step["run"]
    for arg in (
        "--seed",
        "--clean-runs",
        "--work-dir",
        "--report out/dq-faults.json",
        "--out out/dq-fault-row.csv",
        '--git-sha "$GITHUB_SHA"',
    ):
        assert arg in run, arg
    assert "out/" not in run.split("--work-dir", 1)[1].split()[0]


def test_fault_injection_uploads_out_but_not_the_lake():
    job = job_running(load(), "python -m repopulse.quality faults")
    upload = [
        s for s in job["steps"] if str(s.get("uses", "")).startswith("actions/upload-artifact@")
    ]
    assert len(upload) == 1
    with_ = upload[0]["with"]
    assert with_["name"] == "data-quality-${{ github.run_id }}"
    assert with_["path"].strip() == "out/"
    assert 0 < int(with_["retention-days"]) <= 30
    assert upload[0].get("if") == "always()"


def test_every_upload_skips_lake_and_landing():
    for step in all_steps(load()):
        if str(step.get("uses", "")).startswith("actions/upload-artifact@"):
            path = str(step["with"]["path"])
            for forbidden in ("data/", "lake", "landing", ".duckdb", "RUNNER_TEMP"):
                assert forbidden not in path, (step.get("name"), forbidden)
            assert 0 < int(step["with"]["retention-days"]) <= 30


# ---------------------------------------------------------------- reference gap hour (real data)


def test_reference_gap_hour_job_ingests_and_fails_when_not_caught():
    job = job_running(load(), "python -m repopulse.quality gap")
    assert "github.event_name != 'workflow_run'" in job.get("if", "")
    (step,) = steps_running(job, "python -m repopulse.quality gap")
    assert "--ingest" in step["run"]
    assert "--report out/" in step["run"]
    assert "continue-on-error" not in step


# ---------------------------------------------------------------- expressions, secrets, wording


def test_run_steps_do_not_interpolate_expressions():
    for step in all_steps(load()):
        run = step.get("run", "")
        assert "${{" not in run, step.get("name")
        assert "inputs." not in run, step.get("name")


def test_secrets_only_reach_steps_through_env():
    data = load()
    hits = [(p, v) for p, v in walk(data) if "secrets." in v]
    assert hits
    for path, _ in hits:
        assert "env" in path and "steps" in path, path
    names = set(re.findall(r"secrets\.([A-Z_]+)", WORKFLOW.read_text(encoding="utf-8")))
    assert names == {"MOTHERDUCK_TOKEN"}


@pytest.mark.parametrize("word", ["incremental batch", "simulated", "data-incident"])
def test_comment_explains_batch_and_simulated_data(word):
    assert word in comments().lower()


def test_comment_does_not_call_it_streaming():
    text = comments().lower()
    assert "real-time" not in text.replace("not real-time", "")
    assert "streaming" not in text.replace("not streaming", "")
