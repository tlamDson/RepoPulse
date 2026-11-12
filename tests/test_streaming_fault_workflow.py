"""Static checks on .github/workflows/streaming-fault.yml and docs/streaming-fault.md.

No GitHub, no Docker, no network: the workflow is parsed as YAML, the docs as text.
"""

from __future__ import annotations

import re
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = ROOT / ".github" / "workflows" / "streaming-fault.yml"
DOCS = ROOT / "docs" / "streaming-fault.md"
COMPOSE_PATH = "benchmarks/kafka/docker-compose.yml"
DRIVER = "benchmarks/streaming_fault.py"
REQUIRED_CHECK_NAMES = {"Lint", "Unit Tests", "dbt build (DuckDB)"}


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


def _index(all_steps, predicate) -> int:
    hits = [i for i, s in enumerate(all_steps) if predicate(s)]
    assert len(hits) == 1, hits
    return hits[0]


def _uses(prefix):
    return lambda s: str(s.get("uses", "")).startswith(prefix)


def _runs(fragment):
    return lambda s: fragment in s.get("run", "")


# ---------------------------------------------------------------- workflow


def test_workflow_name():
    assert load()["name"] == "Streaming fault tolerance"


def test_triggers_dispatch_and_path_filtered_pull_request():
    on = triggers(load())
    assert "workflow_dispatch" in on
    inputs = on["workflow_dispatch"]["inputs"]
    assert {"cases", "max_hours", "kill_at_fraction"} <= set(inputs)
    pr = on["pull_request"]
    assert set(pr["branches"]) == {"develop", "main"}
    assert set(pr["paths"]) == {
        ".github/workflows/streaming-fault.yml",
        DRIVER,
        "benchmarks/kafka/**",
        "repopulse/streaming/**",
    }
    for key in ("push", "schedule", "pull_request_target"):
        assert key not in on


def test_permissions_are_read_only():
    data = load()
    assert data.get("permissions") == {"contents": "read"}
    for job in jobs(data).values():
        assert job.get("permissions", data["permissions"]) == {"contents": "read"}


def test_concurrency_cancels_per_branch_in_its_own_group():
    conc = load()["concurrency"]
    assert conc["group"].startswith("streaming-fault-")
    assert "github.head_ref || github.ref" in conc["group"]
    assert conc.get("cancel-in-progress") is True


def test_single_job_with_bounded_timeout_and_no_required_check_name():
    all_jobs = jobs(load())
    assert len(all_jobs) == 1
    for key, job in all_jobs.items():
        assert key not in REQUIRED_CHECK_NAMES
        assert job.get("name", key) not in REQUIRED_CHECK_NAMES
        assert "if" not in job
        assert isinstance(job.get("timeout-minutes"), int)
        assert 0 < job["timeout-minutes"] <= 180
        assert job["runs-on"] == "ubuntu-latest"


def test_sets_up_python_311_and_java_17_and_installs_streaming_extra():
    all_steps = steps(load())
    py = all_steps[_index(all_steps, _uses("actions/setup-python@"))]
    assert str(py["with"]["python-version"]) == "3.11"
    java = all_steps[_index(all_steps, _uses("actions/setup-java@"))]
    assert java["with"]["distribution"] == "temurin"
    assert str(java["with"]["java-version"]) == "17"
    runs = "\n".join(s.get("run", "") for s in all_steps)
    assert 'pip install -e ".[dev,spark,streaming]"' in runs


def test_reference_day_is_cached_and_verified_before_the_driver():
    all_steps = steps(load())
    cache = _index(all_steps, _uses("actions/cache@"))
    fetch = _index(all_steps, _runs("benchmarks/fetch_reference.py"))
    driver = _index(all_steps, _runs(DRIVER))
    assert cache < fetch < driver
    with_ = all_steps[cache]["with"]
    assert with_["path"] == "data/raw/gharchive"
    assert "hashFiles('benchmarks/reference.toml')" in with_["key"]


def test_kafka_is_started_with_wait_before_the_driver_and_always_torn_down():
    all_steps = steps(load())
    up = _index(all_steps, _runs("up -d --wait"))
    driver = _index(all_steps, _runs(DRIVER))
    down = _index(all_steps, _runs("down -v"))
    assert up < driver < down
    assert f"docker compose -f {COMPOSE_PATH} up -d --wait" in all_steps[up]["run"]
    assert f"docker compose -f {COMPOSE_PATH} down -v" in all_steps[down]["run"]
    assert all_steps[down].get("if") == "always()"
    assert down == len(all_steps) - 1


def test_broker_logs_are_printed_on_failure():
    all_steps = steps(load())
    logs = all_steps[_index(all_steps, _runs(" logs"))]
    assert logs.get("if") == "failure()"
    assert f"docker compose -f {COMPOSE_PATH} logs" in logs["run"]


def test_driver_writes_rows_and_summary():
    all_steps = steps(load())
    run = all_steps[_index(all_steps, _runs(DRIVER))]["run"]
    for flag in ("--bootstrap", "--cases", "--git-sha", "--max-hours", "--kill-at-fraction"):
        assert flag in run
    assert f"--compose-file {COMPOSE_PATH}" in run
    assert "--out out/streaming-fault-rows.csv" in run
    assert "--summary out/streaming-fault-summary.md" in run
    summary = [s for s in all_steps if "GITHUB_STEP_SUMMARY" in s.get("run", "")]
    assert summary and all(s.get("if") == "always()" for s in summary)


def test_uploads_rows_and_summary_under_its_own_artifact_name():
    all_steps = steps(load())
    upload = all_steps[_index(all_steps, _uses("actions/upload-artifact@v4"))]
    assert upload.get("if") == "always()"
    with_ = upload["with"]
    assert with_["name"] == "streaming-fault"
    paths = [p.strip() for p in with_["path"].splitlines() if p.strip()]
    assert sorted(paths) == ["out/streaming-fault-rows.csv", "out/streaming-fault-summary.md"]


def test_uses_no_secrets_and_no_expression_in_run():
    load()
    assert "secrets." not in WORKFLOW.read_text(encoding="utf-8")
    for step in steps(load()):
        assert "${{" not in step.get("run", ""), step


def test_does_not_touch_runs_csv():
    runs = "\n".join(s.get("run", "") for s in steps(load()))
    assert "record_run" not in runs
    assert "runs.csv" not in runs


# ---------------------------------------------------------------- docs


def docs_text() -> str:
    assert DOCS.is_file(), f"missing {DOCS}"
    return DOCS.read_text(encoding="utf-8")


def test_docs_define_every_measured_number():
    text = docs_text()
    for name in (
        "lost",
        "duplicates",
        "offset_duplicates",
        "content_duplicates",
        "failover_seconds",
        "resync_seconds",
        "max_delivery_gap_after_kill_seconds",
        "restart_first_commit_seconds",
        "restart_drain_seconds",
        "total_seconds",
        "streaming-fault-broker-kill",
        "streaming-fault-consumer-kill",
    ):
        assert f"`{name}`" in text, name


def test_docs_say_the_data_is_real_and_quote_no_measurement():
    text = docs_text()
    assert "not simulated" in text
    assert "runs.csv" in text
    assert not re.search(r"\b\d+\.\d+\s*(s|seconds|ms)\b", text)


def test_claude_md_lists_the_driver_command():
    assert DRIVER in (ROOT / "CLAUDE.md").read_text(encoding="utf-8")
