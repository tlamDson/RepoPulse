"""Static checks on .github/workflows/ingest.yml (no GitHub, no network)."""

import re
from pathlib import Path

import pytest
import yaml

WORKFLOW = Path(__file__).resolve().parents[1] / ".github" / "workflows" / "ingest.yml"
REQUIRED_CHECK_NAMES = {"Lint", "Unit Tests", "dbt build (DuckDB)"}


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


def comments():
    text = WORKFLOW.read_text(encoding="utf-8") if WORKFLOW.is_file() else ""
    return "\n".join(line for line in text.splitlines() if line.lstrip().startswith("#"))


def test_workflow_name():
    assert load()["name"] == "Ingest GH Archive"


def test_no_cron_of_its_own():
    # The hourly cron lives in pipeline.yml, which runs ingest itself; a second cron here would
    # download every hour twice.
    on = triggers(load())
    assert "schedule" not in on


def test_dispatch_inputs():
    on = triggers(load())
    inputs = on["workflow_dispatch"]["inputs"]
    assert {"date", "hour", "hours", "force"} <= set(inputs)


def test_pull_request_is_path_filtered():
    on = triggers(load())
    pr = on["pull_request"]
    assert set(pr["branches"]) == {"develop", "main"}
    assert set(pr["paths"]) == {"repopulse/ingest.py", ".github/workflows/ingest.yml"}
    assert "pull_request_target" not in on
    assert "push" not in on


def test_permissions_are_minimal():
    data = load()
    assert data.get("permissions") == {"contents": "read"}
    for job in jobs(data).values():
        assert job.get("permissions", data["permissions"]) == {"contents": "read"}


def test_every_job_has_timeout():
    for name, job in jobs(load()).items():
        assert isinstance(job.get("timeout-minutes"), int), name
        assert 0 < job["timeout-minutes"] <= 30, name


def test_concurrency_queues_per_trigger():
    # One group per trigger: GitHub keeps a single pending run per group, so a pull_request run
    # sharing the dispatch group could evict a queued manual run.
    conc = load().get("concurrency")
    assert isinstance(conc, dict)
    assert conc.get("group") == "ingest-gharchive-${{ github.event_name }}"
    assert conc.get("cancel-in-progress") is False


def test_job_names_do_not_shadow_required_checks():
    for key, job in jobs(load()).items():
        assert job.get("name", key) not in REQUIRED_CHECK_NAMES
        assert key not in REQUIRED_CHECK_NAMES


def test_no_job_level_if():
    for name, job in jobs(load()).items():
        assert "if" not in job, name


def test_runs_ingest_module():
    runs = "\n".join(s.get("run", "") for s in steps(load()))
    assert "python -m repopulse.ingest" in runs
    assert "--summary" in runs
    assert "pip install -e ." in runs


def test_uses_no_secrets():
    load()
    assert "secrets." not in WORKFLOW.read_text(encoding="utf-8")


def test_run_steps_do_not_interpolate_expressions():
    # Dispatch inputs reach the shell through env:, never by ${{ }} inside run:.
    for step in steps(load()):
        run = step.get("run", "")
        assert "inputs." not in run, run
        assert "${{" not in run, run


def test_writes_job_summary():
    runs = "\n".join(s.get("run", "") for s in steps(load()))
    assert "GITHUB_STEP_SUMMARY" in runs


def test_uploads_landing_artifact():
    upload = [
        s for s in steps(load()) if str(s.get("uses", "")).startswith("actions/upload-artifact@v7")
    ]
    assert len(upload) == 1
    with_ = upload[0]["with"]
    assert with_["overwrite"] is True
    assert 0 < int(with_["retention-days"]) <= 7
    assert with_["if-no-files-found"] == "warn"
    assert "data/landing/gharchive" in with_["path"]
    assert with_["name"].startswith("landing-gharchive")


@pytest.mark.parametrize("action", ["actions/checkout", "actions/setup-python"])
def test_uses_standard_actions(action):
    uses = [str(s.get("uses", "")) for s in steps(load())]
    assert any(u.startswith(action + "@") for u in uses)


def test_uses_python_311_with_pip_cache():
    setup = [s for s in steps(load()) if str(s.get("uses", "")).startswith("actions/setup-python")]
    assert str(setup[0]["with"]["python-version"]) == "3.11"
    assert setup[0]["with"].get("cache") == "pip"


def test_comment_explains_main_only_and_batch():
    text = comments()
    assert "main" in text
    assert "workflow_dispatch" in text
    assert "pipeline.yml" in text  # where the hourly cron lives now
    assert re.search(r"incremental batch", text, re.IGNORECASE)
    assert "artifact" in text.lower()
