"""Tests of dags/repopulse_pipeline.py, the Airflow DAG built from repopulse.dagspec.

Two kinds of tests:

- Static (always run): the DAG file exists, uses the Airflow 3 SDK imports and Deadline Alerts,
  and no repo directory is called ``airflow`` (it would shadow the real package, because pytest
  runs with ``pythonpath = ["."]``).
- DagBag (need Airflow): load ``dags/`` and compare the DAG with the spec. Airflow is not
  installed in the ``Unit Tests`` CI job (its constraints clash with dbt/pyspark) and does not run
  on Windows, so these skip there. They run for real in .github/workflows/airflow.yml, which sets
  ``REPOPULSE_REQUIRE_AIRFLOW=1``: there a missing Airflow is a failure, never a skip.
"""

from __future__ import annotations

import ast
import importlib.util
import os
from datetime import timedelta
from pathlib import Path

import pytest

from repopulse import dagspec

ROOT = Path(__file__).resolve().parents[1]
DAGS_DIR = ROOT / "dags"
DAG_FILE = DAGS_DIR / "repopulse_pipeline.py"


def tree() -> ast.Module:
    assert DAG_FILE.is_file(), f"missing DAG file {DAG_FILE}"
    return ast.parse(DAG_FILE.read_text(encoding="utf-8"))


def imported_names() -> dict[str, set[str]]:
    out: dict[str, set[str]] = {}
    for node in ast.walk(tree()):
        if isinstance(node, ast.ImportFrom) and node.module:
            out.setdefault(node.module, set()).update(a.name for a in node.names)
    return out


# ---------------------------------------------------------------- static


def test_dag_file_uses_airflow3_sdk_and_the_spec():
    names = imported_names()
    assert {"DAG", "DeadlineAlert", "DeadlineReference", "AsyncCallback"} <= names["airflow.sdk"]
    assert "BashOperator" in names["airflow.providers.standard.operators.bash"]
    assert "dagspec" in names.get("repopulse", set())


def test_dag_file_uses_no_removed_sla_arguments():
    keywords = {
        kw.arg for node in ast.walk(tree()) if isinstance(node, ast.Call) for kw in node.keywords
    }
    assert "sla" not in keywords
    assert "sla_miss_callback" not in keywords
    assert "deadline" in keywords
    assert {"retries", "retry_delay", "retry_exponential_backoff", "execution_timeout"} <= keywords
    assert "cwd" in keywords


def test_no_repo_directory_shadows_the_airflow_package():
    assert not (ROOT / "airflow").exists()
    assert not (ROOT / "airflow.py").exists()
    assert not (DAGS_DIR / "airflow").exists()
    assert not (DAGS_DIR / "airflow.py").exists()
    assert not (DAGS_DIR / "__init__.py").exists()  # dags/ is a DAG folder, not a package


# ---------------------------------------------------------------- DagBag


@pytest.fixture(scope="module")
def dag():
    if importlib.util.find_spec("airflow") is None:
        if os.environ.get("REPOPULSE_REQUIRE_AIRFLOW") == "1":
            pytest.fail("REPOPULSE_REQUIRE_AIRFLOW=1 but apache-airflow is not installed")
        pytest.skip(
            "apache-airflow is not installed (not in the Unit Tests job, no Windows support); "
            "runs in .github/workflows/airflow.yml"
        )
    try:
        from airflow.dag_processing.dagbag import DagBag
    except ImportError:
        from airflow.models.dagbag import DagBag

    bag = DagBag(dag_folder=str(DAGS_DIR), include_examples=False)
    assert bag.import_errors == {}, bag.import_errors
    assert dagspec.DAG_ID in bag.dags, sorted(bag.dags)
    return bag.dags[dagspec.DAG_ID]


@pytest.fixture(scope="module")
def spec() -> dagspec.DagSpec:
    return dagspec.dag_spec(dagspec.settings_from_env(os.environ))


def test_dag_tasks_and_dependencies_match_the_spec(dag, spec):
    assert set(dag.task_dict) == set(dagspec.TASK_ORDER)
    for t in spec.tasks:
        op = dag.task_dict[t.task_id]
        assert set(op.upstream_task_ids) == set(t.upstream), t.task_id
        downstream = {u.task_id for u in spec.tasks if t.task_id in u.upstream}
        assert set(op.downstream_task_ids) == downstream, t.task_id


def test_dag_retries_timeouts_and_commands_match_the_spec(dag, spec):
    for t in spec.tasks:
        op = dag.task_dict[t.task_id]
        assert op.retries == t.retries, t.task_id
        assert op.retry_delay == timedelta(seconds=t.retry_delay_seconds), t.task_id
        assert bool(op.retry_exponential_backoff) is t.exponential_backoff, t.task_id
        assert op.execution_timeout == timedelta(minutes=t.timeout_minutes), t.task_id
        assert op.bash_command == t.command, t.task_id
        assert str(op.cwd) == str(spec.home), t.task_id


def test_dag_schedule_concurrency_and_deadline(dag, spec):
    assert dag.max_active_runs == 1
    assert dag.catchup is False
    assert dag.start_date == spec.start_date
    deadline = getattr(dag, "deadline", None)
    assert deadline, "the DAG needs a DeadlineAlert (Airflow 3 replacement of sla=)"
