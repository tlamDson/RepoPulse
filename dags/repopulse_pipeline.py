"""Airflow 3 DAG ``repopulse_pipeline``: the incremental batch of pipeline.yml, as a DAG.

Everything except the Airflow objects comes from ``repopulse.dagspec`` (tested without Airflow in
tests/test_dagspec.py): task ids, bash commands, retries, timeouts, schedule and SLA. The SLA is an
Airflow 3 Deadline Alert (``sla=`` was removed in Airflow 3): if a DAG run is not done
``sla_minutes`` after it was queued, the Triggerer runs ``dagspec.record_sla_miss``, which appends
one JSON line to ``REPOPULSE_SLA_MISS_FILE``. Settings come from environment variables
(``dagspec.settings_from_env``); see docs/airflow.md.
"""

from __future__ import annotations

import os
from datetime import timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, AsyncCallback, DeadlineAlert, DeadlineReference

from repopulse import dagspec

SPEC = dagspec.dag_spec(dagspec.settings_from_env(os.environ))

with DAG(
    dag_id=SPEC.dag_id,
    description="GH Archive -> Spark bronze/silver -> silver export -> dbt (incremental batch)",
    schedule=SPEC.schedule,
    start_date=SPEC.start_date,
    end_date=SPEC.end_date,
    catchup=SPEC.catchup,
    max_active_runs=SPEC.max_active_runs,
    deadline=DeadlineAlert(
        reference=DeadlineReference.DAGRUN_QUEUED_AT,
        interval=timedelta(minutes=SPEC.sla_minutes),
        callback=AsyncCallback(dagspec.record_sla_miss, kwargs={"path": str(SPEC.sla_miss_file)}),
    ),
    tags=["repopulse", "gharchive"],
) as dag:
    operators = {}
    for task in SPEC.tasks:
        operators[task.task_id] = BashOperator(
            task_id=task.task_id,
            bash_command=task.command,
            cwd=str(SPEC.home),
            retries=task.retries,
            retry_delay=timedelta(seconds=task.retry_delay_seconds),
            retry_exponential_backoff=(
                dagspec.BACKOFF_MULTIPLIER if task.exponential_backoff else 0
            ),
            execution_timeout=timedelta(minutes=task.timeout_minutes),
        )
    for task in SPEC.tasks:
        for upstream in task.upstream:
            operators[upstream] >> operators[task.task_id]
