"""Static checks on .github/workflows/ci.yml: Spark tests run in `Unit Tests` on Java 17, and the
required checks stay intact (no GitHub, no network)."""

from __future__ import annotations

import re
from pathlib import Path

import yaml

WORKFLOW = Path(__file__).resolve().parents[1] / ".github" / "workflows" / "ci.yml"
REQUIRED_CHECK_NAMES = {"Lint", "Unit Tests", "dbt build (DuckDB)"}


def load() -> dict:
    data = yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))
    assert isinstance(data, dict)
    return data


def triggers(data):
    return data.get("on", data.get(True))  # PyYAML reads bare `on` as True


def job_by_name(data, name):
    matches = [job for key, job in data["jobs"].items() if job.get("name", key) == name]
    assert len(matches) == 1, f"expected exactly one job named {name!r}"
    return matches[0]


def unit_steps():
    return job_by_name(load(), "Unit Tests")["steps"]


def test_required_check_names_are_intact():
    names = {job.get("name", key) for key, job in load()["jobs"].items()}
    assert REQUIRED_CHECK_NAMES <= names


def test_pull_request_trigger_has_no_paths_filter():
    pr = triggers(load())["pull_request"]
    assert "paths" not in pr
    assert "paths-ignore" not in pr


def test_no_job_level_if():
    for key, job in load()["jobs"].items():
        assert "if" not in job, key


def test_unit_tests_set_up_java_17():
    setup = [s for s in unit_steps() if str(s.get("uses", "")).startswith("actions/setup-java@")]
    assert len(setup) == 1, "Unit Tests needs exactly one actions/setup-java step"
    with_ = setup[0]["with"]
    assert str(with_["java-version"]) == "17"
    assert with_.get("distribution"), "setup-java needs a distribution (e.g. temurin)"


def test_unit_tests_install_spark_extra():
    runs = "\n".join(str(s.get("run", "")) for s in unit_steps())
    assert re.search(r"pip install -e \"?\.\[[^\]]*\bspark\b[^\]]*\]", runs), runs
    assert re.search(r"pip install -e \"?\.\[[^\]]*\bdev\b[^\]]*\]", runs), runs


def test_java_is_set_up_before_pytest_runs():
    steps = unit_steps()
    java_at = [
        i for i, s in enumerate(steps) if str(s.get("uses", "")).startswith("actions/setup-java@")
    ]
    pytest_at = [i for i, s in enumerate(steps) if "pytest" in str(s.get("run", ""))]
    assert java_at, "no actions/setup-java step"
    assert pytest_at, "no pytest step"
    assert java_at[0] < pytest_at[0]


def test_unit_tests_timeout_leaves_room_for_spark_but_is_bounded():
    timeout = job_by_name(load(), "Unit Tests")["timeout-minutes"]
    assert isinstance(timeout, int)
    assert 15 <= timeout <= 30
