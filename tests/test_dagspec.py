"""Tests of repopulse.dagspec: the Airflow DAG spec and the backfill helpers (no Airflow needed).

The spec is pure Python (stdlib only) so it is tested here; dags/repopulse_pipeline.py only turns
it into Airflow objects (tests/test_airflow_dag.py, run for real in .github/workflows/airflow.yml).
No test calls Airflow, GitHub or GH Archive: ``airflow dags list-runs`` output comes from
tests/fixtures/airflow_list_runs.json or from strings built in the test.
"""

from __future__ import annotations

import asyncio
import csv
import json
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pytest

from benchmarks import record_run
from repopulse import dagspec
from repopulse.runs_log import COLUMNS, validate_row

FIXTURE = Path(__file__).parent / "fixtures" / "airflow_list_runs.json"
STARTED = datetime(2026, 11, 11, 10, 0, 0, tzinfo=UTC)


def settings(**env: str) -> dagspec.Settings:
    return dagspec.settings_from_env(env)


def spec(**env: str) -> dagspec.DagSpec:
    return dagspec.dag_spec(settings(**env))


def task(spec_: dagspec.DagSpec, task_id: str) -> dagspec.TaskSpec:
    found = [t for t in spec_.tasks if t.task_id == task_id]
    assert len(found) == 1, [t.task_id for t in spec_.tasks]
    return found[0]


def run_json(logical: str, state: str, start: str | None, end: str | None, **extra) -> dict:
    return {
        "dag_id": "repopulse_pipeline",
        "run_id": extra.pop("run_id", f"backfill__{logical}T00:00:00+00:00"),
        "state": state,
        "logical_date": f"{logical}T00:00:00+00:00",
        "start_date": start,
        "end_date": end,
        **extra,
    }


# ---------------------------------------------------------------- DAG spec


def test_dag_identity_schedule_and_concurrency():
    s = spec()
    assert s.dag_id == "repopulse_pipeline" == dagspec.DAG_ID
    assert s.schedule == "@daily"
    assert s.catchup is False
    assert s.max_active_runs == 1
    assert s.start_date.tzinfo is not None
    assert s.end_date is None  # no end date unless REPOPULSE_AIRFLOW_END_DATE is set


def test_tasks_in_pipeline_yml_order():
    s = spec()
    assert dagspec.TASK_ORDER == ("ingest", "spark_bronze_silver", "export_silver", "dbt_build")
    assert dagspec.topological_order(s.tasks) == list(dagspec.TASK_ORDER)
    assert task(s, "ingest").upstream == ()
    assert task(s, "spark_bronze_silver").upstream == ("ingest",)
    assert task(s, "export_silver").upstream == ("spark_bronze_silver",)
    assert task(s, "dbt_build").upstream == ("export_silver",)


def test_every_task_retries_with_positive_delay_and_timeout():
    for t in spec().tasks:
        assert t.retries >= 1, t
        assert t.retry_delay_seconds > 0, t
        assert t.timeout_minutes > 0, t
    # GH Archive publishes an hour late now and then: the network task backs off.
    assert task(spec(), "ingest").exponential_backoff is True
    assert task(spec(), "ingest").retries >= 2


def test_sla_is_positive_and_configurable():
    assert spec().sla_minutes > 0
    assert spec(REPOPULSE_AIRFLOW_SLA_MINUTES="12").sla_minutes == 12
    with pytest.raises(dagspec.DagSpecError, match="SLA"):
        settings(REPOPULSE_AIRFLOW_SLA_MINUTES="0")


def test_commands_use_the_data_date_template_and_the_pipeline_modules():
    s = spec(REPOPULSE_PYTHON="/opt/py/bin/python", REPOPULSE_AIRFLOW_HOUR="7")
    assert dagspec.DATA_DATE_TEMPLATE == "{{ macros.ds_add(ds, -1) }}"
    expected = {
        "ingest": "-m repopulse.ingest",
        "spark_bronze_silver": "-m repopulse.spark.job --source gharchive",
        "export_silver": "-m repopulse.spark.export",
        "dbt_build": "-m dbt.cli.main build --project-dir transform --profiles-dir transform",
    }
    for task_id, fragment in expected.items():
        cmd = task(s, task_id).command
        assert cmd.startswith("/opt/py/bin/python "), cmd
        assert fragment in cmd, cmd
    for task_id in ("ingest", "spark_bronze_silver"):
        cmd = task(s, task_id).command
        assert f"--date {dagspec.DATA_DATE_TEMPLATE}" in cmd
        assert "--hour 7 --hours 1" in cmd
    dbt = task(s, "dbt_build").command
    assert "--target ci" in dbt
    assert "--exclude fixture_silver_gharchive_events" in dbt
    assert "silver_gharchive_parquet" in dbt
    assert "cd " not in " ".join(t.command for t in s.tasks)


def test_ingest_fails_the_task_when_an_hour_is_missing():
    cmd = task(spec(), "ingest").command
    assert "-m repopulse.dagspec check-ingest --summary" in cmd
    assert dagspec.DATA_DATE_TEMPLATE in cmd.split("check-ingest", 1)[1]


def test_summaries_are_written_per_data_date():
    s = spec()
    for task_id, name in (
        ("ingest", "ingest-summary.json"),
        ("spark_bronze_silver", "spark-summary.json"),
        ("export_silver", "export-summary.json"),
    ):
        assert f"out/airflow/{dagspec.DATA_DATE_TEMPLATE}/{name}" in task(s, task_id).command


def test_python_path_with_spaces_is_quoted():
    cmd = task(spec(REPOPULSE_PYTHON="C:/Program Files/py/python"), "ingest").command
    assert cmd.startswith("'C:/Program Files/py/python' -m repopulse.ingest")


def test_settings_defaults_and_validation(tmp_path):
    s = settings(REPOPULSE_HOME=str(tmp_path))
    assert s.python == "python"
    assert s.home == tmp_path
    assert s.hours_per_day == 1
    assert 0 <= s.hour <= 23
    assert s.sla_miss_file == tmp_path / "out" / "airflow" / "sla-miss.jsonl"
    assert settings().home == Path(dagspec.__file__).resolve().parents[1]
    with pytest.raises(dagspec.DagSpecError, match="HOUR"):
        settings(REPOPULSE_AIRFLOW_HOUR="24")
    with pytest.raises(dagspec.DagSpecError, match="HOURS_PER_DAY"):
        settings(REPOPULSE_AIRFLOW_HOURS_PER_DAY="0")
    with pytest.raises(dagspec.DagSpecError, match="HOURS_PER_DAY"):
        settings(REPOPULSE_AIRFLOW_HOURS_PER_DAY="7")  # ingest takes at most 6 hours
    with pytest.raises(dagspec.DagSpecError, match="END_DATE"):
        settings(REPOPULSE_AIRFLOW_END_DATE="2026-02-30")


def test_end_date_stops_scheduled_runs_after_the_backfill():
    s = spec(REPOPULSE_AIRFLOW_END_DATE="2026-01-07")
    assert s.end_date == datetime(2026, 1, 7, 12, 0, tzinfo=UTC)
    assert s.start_date < datetime(2026, 1, 1, tzinfo=UTC)


# ---------------------------------------------------------------- cycles


def mk(task_id: str, *upstream: str) -> dagspec.TaskSpec:
    return dagspec.TaskSpec(
        task_id=task_id,
        command="true",
        upstream=tuple(upstream),
        retries=1,
        retry_delay_seconds=1,
        exponential_backoff=False,
        timeout_minutes=1,
    )


def test_cycle_is_rejected():
    with pytest.raises(dagspec.DagSpecError, match="cycle") as exc:
        dagspec.topological_order([mk("a"), mk("b", "a", "c"), mk("c", "b")])
    assert "b" in str(exc.value) and "c" in str(exc.value)


def test_self_loop_is_rejected():
    with pytest.raises(dagspec.DagSpecError, match="cycle"):
        dagspec.topological_order([mk("a", "a")])


def test_unknown_upstream_is_rejected():
    with pytest.raises(dagspec.DagSpecError, match="unknown upstream"):
        dagspec.topological_order([mk("a"), mk("b", "zzz")])


def test_duplicate_task_id_is_rejected():
    with pytest.raises(dagspec.DagSpecError, match="duplicate"):
        dagspec.topological_order([mk("a"), mk("a")])


def test_topological_order_is_deterministic_for_a_diamond():
    tasks = [mk("d", "b", "c"), mk("c", "a"), mk("b", "a"), mk("a")]
    assert dagspec.topological_order(tasks) == ["a", "b", "c", "d"]


# ---------------------------------------------------------------- backfill window


def test_window_one_day():
    w = dagspec.backfill_window(date(2026, 1, 5), 1)
    assert (w.first_data_date, w.last_data_date) == (date(2026, 1, 5), date(2026, 1, 5))
    # the run of logical date D processes the data of D - 1
    assert (w.from_date, w.to_date) == (date(2026, 1, 6), date(2026, 1, 6))
    assert w.days == 1


def test_window_three_days_inclusive():
    w = dagspec.backfill_window(date(2026, 1, 6), 3)
    assert w.first_data_date == date(2026, 1, 4)
    assert (w.from_date, w.to_date) == (date(2026, 1, 5), date(2026, 1, 7))
    assert w.data_dates == (date(2026, 1, 4), date(2026, 1, 5), date(2026, 1, 6))


def test_window_across_month_and_year_end():
    w = dagspec.backfill_window(date(2026, 3, 1), 3)
    assert w.first_data_date == date(2026, 2, 27)
    assert (w.from_date, w.to_date) == (date(2026, 2, 28), date(2026, 3, 2))
    w = dagspec.backfill_window(date(2026, 12, 31), 2)
    assert (w.from_date, w.to_date) == (date(2026, 12, 31), date(2027, 1, 1))


@pytest.mark.parametrize("days", [0, -1, 8])
def test_window_rejects_bad_day_count(days):
    with pytest.raises(dagspec.DagSpecError, match="days"):
        dagspec.backfill_window(date(2026, 1, 6), days)


# ---------------------------------------------------------------- list-runs parser


def test_parse_runs_fixture():
    runs = dagspec.parse_runs(FIXTURE.read_text(encoding="utf-8"))
    assert len(runs) == 4
    first = runs[0]
    assert first.run_id == "backfill__2026-01-05T00:00:00+00:00"
    assert first.state == "success"
    assert first.logical_date == datetime(2026, 1, 5, tzinfo=UTC)
    assert first.start_date == datetime(2026, 11, 11, 10, 0, 5, tzinfo=UTC)
    assert first.end_date == datetime(2026, 11, 11, 10, 6, 5, tzinfo=UTC)
    assert first.duration_seconds == 360.0
    assert runs[2].end_date is None
    assert runs[2].duration_seconds is None


def test_parse_runs_tolerates_missing_optional_fields_and_noise():
    text = (
        'WARNING something\n[{"run_id": "backfill__x", "state": "queued", "logical_date": null}]\n'
    )
    (run,) = dagspec.parse_runs(text)
    assert run.run_id == "backfill__x"
    assert run.logical_date is None and run.start_date is None and run.end_date is None
    assert dagspec.parse_runs("") == []
    assert dagspec.parse_runs("No data found\n") == []
    assert dagspec.parse_runs("[]") == []


def test_parse_runs_skips_bracketed_log_lines_before_the_json():
    # Real stdout of `airflow dags list-runs -o json` on Airflow 3.4 (run 43134761449): a structlog
    # warning with "[warning  ]" and "[py.warnings]" comes before the JSON array.
    text = (
        "2026-11-12T02:20:42.609061Z [warning  ] Could not import graphviz. Rendering graph to "
        "the graphical format will not be possible. \n"
        "You might need to install the graphviz package and necessary system packages.\n"
        "Run `pip install graphviz` to attempt to install it. [py.warnings] category=UserWarning "
        "filename=/venv/airflow/cli/commands/dag_command.py lineno=57\n"
        '[{"dag_id": "repopulse_pipeline", "run_id": "backfill__2026-10-26T00:00:00+00:00", '
        '"state": "queued", "run_after": "2026-10-26T00:00:00+00:00", '
        '"logical_date": "2026-10-26T00:00:00+00:00", "start_date": "", "end_date": ""}, '
        '{"dag_id": "repopulse_pipeline", "run_id": "backfill__2026-10-25T00:00:00+00:00", '
        '"state": "queued", "run_after": "2026-10-25T00:00:00+00:00", '
        '"logical_date": "2026-10-25T00:00:00+00:00", "start_date": "", "end_date": ""}]\n'
    )
    runs = dagspec.parse_runs(text)
    assert [r.run_id for r in runs] == [
        "backfill__2026-10-26T00:00:00+00:00",
        "backfill__2026-10-25T00:00:00+00:00",
    ]
    assert [r.state for r in runs] == ["queued", "queued"]
    assert runs[0].logical_date == datetime(2026, 10, 26, tzinfo=UTC)
    assert runs[0].start_date is None and runs[0].end_date is None
    # No run yet: the empty array after the same warning.
    assert dagspec.parse_runs("x [warning  ] y [py.warnings] z\n[]\n") == []
    # A trailing bracketed log line does not hide the array either.
    (run,) = dagspec.parse_runs('[{"run_id": "backfill__x", "state": "running"}]\n[info ] bye\n')
    assert run.state == "running"


def test_parse_runs_accepts_z_suffix_and_space_separator():
    text = json.dumps(
        [run_json("2026-01-05", "success", "2026-11-11 10:00:00+00:00", "2026-11-11T10:01:00Z")]
    )
    (run,) = dagspec.parse_runs(text)
    assert run.duration_seconds == 60.0


@pytest.mark.parametrize(
    "text, why",
    [
        ('{"run_id": "x"}', "list"),
        ('[{"state": "success"}]', "run_id"),
        ('[{"run_id": "x"}]', "state"),
        ('[{"run_id": "x", "state": "ok", "start_date": "yesterday"}]', "start_date"),
        ("[not json", "JSON"),
    ],
)
def test_parse_runs_reports_bad_output(text, why):
    with pytest.raises(dagspec.DagSpecError, match=why):
        dagspec.parse_runs(text)


def test_backfill_runs_keeps_only_backfill_runs_in_the_window():
    runs = dagspec.parse_runs(FIXTURE.read_text(encoding="utf-8"))
    w = dagspec.backfill_window(date(2026, 1, 6), 3)
    picked = dagspec.backfill_runs(runs, w)
    assert [r.run_id for r in picked] == [
        "backfill__2026-01-05T00:00:00+00:00",
        "backfill__2026-01-06T00:00:00+00:00",
        "backfill__2026-01-07T00:00:00+00:00",
    ]
    # outside the window: dropped
    w1 = dagspec.backfill_window(date(2026, 1, 4), 1)
    assert [r.run_id for r in dagspec.backfill_runs(runs, w1)] == [
        "backfill__2026-01-05T00:00:00+00:00"
    ]


def test_backfill_runs_uses_run_type_when_present():
    text = json.dumps(
        [
            run_json("2026-01-05", "success", None, None, run_id="r1", run_type="backfill"),
            run_json("2026-01-05", "success", None, None, run_id="backfill__r2", run_type="manual"),
        ]
    )
    w = dagspec.backfill_window(date(2026, 1, 4), 1)
    assert [r.run_id for r in dagspec.backfill_runs(dagspec.parse_runs(text), w)] == ["r1"]


# ---------------------------------------------------------------- progress, SLA, summary


def test_progress_not_done_while_a_run_is_running():
    runs = dagspec.parse_runs(FIXTURE.read_text(encoding="utf-8"))
    p = dagspec.progress(runs, dagspec.backfill_window(date(2026, 1, 6), 3))
    assert (p.done, p.success, p.failed, p.pending) == (False, 2, 0, 1)


def test_progress_done_when_every_day_is_terminal():
    w = dagspec.backfill_window(date(2026, 1, 5), 2)
    text = json.dumps(
        [
            run_json("2026-01-05", "success", "2026-11-11T10:00:00Z", "2026-11-11T10:05:00Z"),
            run_json("2026-01-06", "failed", "2026-11-11T10:05:00Z", "2026-11-11T10:06:00Z"),
        ]
    )
    p = dagspec.progress(dagspec.parse_runs(text), w)
    assert (p.done, p.success, p.failed, p.pending) == (True, 1, 1, 0)


def test_progress_counts_missing_days_as_pending():
    w = dagspec.backfill_window(date(2026, 1, 6), 3)
    p = dagspec.progress([], w)
    assert (p.done, p.pending) == (False, 3)


def test_summary_wall_clock_and_sla_misses():
    w = dagspec.backfill_window(date(2026, 1, 5), 2)
    runs = dagspec.parse_runs(FIXTURE.read_text(encoding="utf-8"))[:2]
    summary = dagspec.summarize(runs, w, started_at=STARTED, sla_minutes=30)
    assert summary["status"] == "success"
    assert summary["days"] == 2
    assert summary["from_date"] == "2026-01-05" and summary["to_date"] == "2026-01-06"
    assert summary["last_data_date"] == "2026-01-05"
    assert summary["wall_seconds"] == 2770.0  # 10:00:00 -> 10:46:10
    assert summary["runs_success"] == 2 and summary["runs_failed"] == 0
    assert summary["sla_minutes"] == 30
    assert summary["sla_missed"] == 1  # the 40-minute run
    assert [r["sla_missed"] for r in summary["runs"]] == [False, True]
    json.dumps(summary)  # serialisable for the artifact


def test_summary_status_failed_when_a_run_failed():
    w = dagspec.backfill_window(date(2026, 1, 4), 1)
    text = json.dumps(
        [run_json("2026-01-05", "failed", "2026-11-11T10:00:00Z", "2026-11-11T10:01:00Z")]
    )
    summary = dagspec.summarize(dagspec.parse_runs(text), w, started_at=STARTED, sla_minutes=30)
    assert summary["status"] == "failed"
    assert summary["runs_failed"] == 1


# ---------------------------------------------------------------- wait loop


class FakeClock:
    def __init__(self) -> None:
        self.now = 0.0
        self.sleeps: list[float] = []

    def clock(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += seconds


def responses(*texts: str):
    queue = list(texts)

    def list_runs() -> str:
        return queue.pop(0) if len(queue) > 1 else queue[0]

    return list_runs


def test_wait_polls_until_every_run_is_done():
    w = dagspec.backfill_window(date(2026, 1, 5), 2)
    running = json.dumps(
        [run_json("2026-01-05", "running", "2026-11-11T10:00:00Z", None)],
    )
    done = json.dumps(
        [
            run_json("2026-01-05", "success", "2026-11-11T10:00:00Z", "2026-11-11T10:05:00Z"),
            run_json("2026-01-06", "success", "2026-11-11T10:05:00Z", "2026-11-11T10:09:00Z"),
        ]
    )
    fake = FakeClock()
    summary = dagspec.wait(
        responses("", running, done),
        w,
        started_at=STARTED,
        sla_minutes=30,
        timeout_seconds=600,
        interval_seconds=15,
        clock=fake.clock,
        sleep=fake.sleep,
    )
    assert summary["status"] == "success"
    assert fake.sleeps == [15, 15]
    assert summary["wall_seconds"] == 540.0


def test_wait_times_out():
    w = dagspec.backfill_window(date(2026, 1, 5), 2)
    fake = FakeClock()
    summary = dagspec.wait(
        responses("[]"),
        w,
        started_at=STARTED,
        sla_minutes=30,
        timeout_seconds=60,
        interval_seconds=20,
        clock=fake.clock,
        sleep=fake.sleep,
    )
    assert summary["status"] == "timeout"
    assert summary["wall_seconds"] is None
    assert summary["runs_pending"] == 2
    assert fake.now >= 60


def test_wait_reports_failed_run():
    w = dagspec.backfill_window(date(2026, 1, 4), 1)
    failed = json.dumps(
        [run_json("2026-01-05", "failed", "2026-11-11T10:00:00Z", "2026-11-11T10:02:00Z")]
    )
    fake = FakeClock()
    summary = dagspec.wait(
        responses(failed),
        w,
        started_at=STARTED,
        sla_minutes=30,
        timeout_seconds=600,
        interval_seconds=15,
        clock=fake.clock,
        sleep=fake.sleep,
    )
    assert summary["status"] == "failed"
    assert fake.sleeps == []


def test_wait_keeps_polling_through_cli_errors():
    w = dagspec.backfill_window(date(2026, 1, 4), 1)
    done = json.dumps(
        [run_json("2026-01-05", "success", "2026-11-11T10:00:00Z", "2026-11-11T10:02:00Z")]
    )
    calls = {"n": 0}

    def list_runs() -> str:
        calls["n"] += 1
        if calls["n"] == 1:
            raise dagspec.DagSpecError("airflow dags list-runs exited 1: database is locked")
        return done

    fake = FakeClock()
    summary = dagspec.wait(
        list_runs,
        w,
        started_at=STARTED,
        sla_minutes=30,
        timeout_seconds=600,
        interval_seconds=10,
        clock=fake.clock,
        sleep=fake.sleep,
    )
    assert summary["status"] == "success"
    assert calls["n"] == 2


# ---------------------------------------------------------------- runs.csv row


def success_summary(days: int = 3) -> dict:
    w = dagspec.backfill_window(date(2026, 1, 6), days)
    runs = [
        run_json(
            d.isoformat(),
            "success",
            f"2026-11-11T10:{i * 10:02d}:00Z",
            f"2026-11-11T10:{i * 10 + 5:02d}:00Z",
        )
        for i, d in enumerate(
            [w.from_date + timedelta(days=k) for k in range(days)],
        )
    ]
    summary = dagspec.summarize(
        dagspec.parse_runs(json.dumps(runs)), w, started_at=STARTED, sla_minutes=30
    )
    summary["hour"] = 15
    summary["hours_per_day"] = 1
    summary["max_active_runs"] = 1
    return summary


def test_row_matches_runs_csv_schema_and_variant():
    row = dagspec.build_row(
        success_summary(3),
        run_at=datetime(2026, 11, 11, 11, 0, tzinfo=UTC),
        git_sha="abc1234",
        row_count=1234,
        run_id="99",
        trigger="workflow_dispatch",
    )
    assert set(row) == set(COLUMNS)
    assert all(isinstance(v, str) for v in row.values())  # CSV-ready, already validated
    assert row["variant"] == "airflow-backfill-3d"
    assert row["data_date"] == "2026-01-06"
    assert row["total_seconds"] == "1500.0"  # 10:00 -> 10:25
    assert row["row_count"] == "1234"
    notes = dict(word.split("=", 1) for word in row["notes"].split())
    assert notes == {
        "engine": "airflow-3.4.0",
        "days": "3",
        "hours_per_day": "1",
        "last_hour": "15",
        "runs_success": "3",
        "runs_failed": "0",
        "sla_minutes": "30",
        "sla_missed": "0",
        "max_active_runs": "1",
        "target": "ci",
        "run_id": "99",
        "trigger": "workflow_dispatch",
    }
    for col in ("q1_seconds", "peak_rss_mb", "bytes_read"):
        assert row[col] == ""


def test_row_without_row_count_leaves_it_empty():
    row = dagspec.build_row(
        success_summary(1),
        run_at=datetime(2026, 11, 11, 11, 0, tzinfo=UTC),
        git_sha="abc1234",
        row_count=None,
        run_id="1",
        trigger="pull_request",
    )
    assert row["row_count"] == ""
    assert row["variant"] == "airflow-backfill-1d"


@pytest.mark.parametrize("status", ["failed", "timeout"])
def test_row_refuses_an_unfinished_or_failed_backfill(status):
    summary = success_summary(1)
    summary["status"] = status
    with pytest.raises(dagspec.DagSpecError, match="status"):
        dagspec.build_row(
            summary,
            run_at=datetime(2026, 11, 11, 11, 0, tzinfo=UTC),
            git_sha="abc1234",
            row_count=None,
            run_id="1",
            trigger="pull_request",
        )


def test_row_count_sums_spark_summaries_of_the_window(tmp_path):
    w = dagspec.backfill_window(date(2026, 1, 6), 2)
    for day, inserted in (("2026-01-05", 10), ("2026-01-06", 32)):
        (tmp_path / day).mkdir()
        (tmp_path / day / "spark-summary.json").write_text(
            json.dumps({"silver_inserted": inserted}), encoding="utf-8"
        )
    assert dagspec.row_count_from(tmp_path, w.data_dates) == 42
    assert (
        dagspec.row_count_from(tmp_path, dagspec.backfill_window(date(2026, 1, 7), 1).data_dates)
        is None
    )


def test_row_count_is_none_when_a_day_is_missing(tmp_path):
    (tmp_path / "2026-01-06").mkdir()
    (tmp_path / "2026-01-06" / "spark-summary.json").write_text(
        json.dumps({"silver_inserted": 5}), encoding="utf-8"
    )
    w = dagspec.backfill_window(date(2026, 1, 6), 2)
    assert dagspec.row_count_from(tmp_path, w.data_dates) is None


# ---------------------------------------------------------------- ingest check, SLA callback


def test_check_ingest_accepts_ingested_and_skipped():
    records = [
        {"data_date": "2026-01-05", "hour": 14, "status": "ingested"},
        {"data_date": "2026-01-05", "hour": 15, "status": "skipped"},
    ]
    assert dagspec.ingest_problems(records) == []


def test_check_ingest_rejects_missing_error_and_empty():
    records = [
        {"data_date": "2026-01-05", "hour": 14, "status": "missing"},
        {"data_date": "2026-01-05", "hour": 15, "status": "error"},
    ]
    assert dagspec.ingest_problems(records) == [
        "2026-01-05T14: missing",
        "2026-01-05T15: error",
    ]
    assert dagspec.ingest_problems([]) == ["ingest summary is empty"]


def test_check_ingest_cli_exit_codes(tmp_path, capsys):
    ok = tmp_path / "ok.json"
    ok.write_text(json.dumps([{"data_date": "2026-01-05", "hour": 1, "status": "ingested"}]))
    bad = tmp_path / "bad.json"
    bad.write_text(json.dumps([{"data_date": "2026-01-05", "hour": 1, "status": "missing"}]))
    assert dagspec.main(["check-ingest", "--summary", str(ok)]) == 0
    assert dagspec.main(["check-ingest", "--summary", str(bad)]) == 1
    assert "2026-01-05T01: missing" in capsys.readouterr().err
    assert dagspec.main(["check-ingest", "--summary", str(tmp_path / "nope.json")]) == 1


def test_sla_miss_callback_appends_one_json_line(tmp_path):
    path = tmp_path / "sub" / "sla-miss.jsonl"
    context = {"dag_run": {"dag_id": "repopulse_pipeline", "run_id": "backfill__x"}}
    asyncio.run(dagspec.record_sla_miss(path=str(path), context=context, other=object()))
    asyncio.run(dagspec.record_sla_miss(path=str(path)))
    lines = path.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 2
    first = json.loads(lines[0])
    assert first["event"] == "sla_miss"
    assert first["dag_id"] == dagspec.DAG_ID
    assert "at" in first
    assert "repopulse_pipeline" in first["context"]


# ---------------------------------------------------------------- CLI


def test_cli_window_prints_env_lines(capsys):
    assert dagspec.main(["window", "--last-data-date", "2026-01-06", "--days", "3"]) == 0
    out = capsys.readouterr().out.splitlines()
    assert out == [
        "BACKFILL_FROM=2026-01-05",
        "BACKFILL_TO=2026-01-07",
        "FIRST_DATA_DATE=2026-01-04",
        "LAST_DATA_DATE=2026-01-06",
        "REPOPULSE_AIRFLOW_END_DATE=2026-01-07",
    ]


def test_cli_window_rejects_bad_days(capsys):
    assert dagspec.main(["window", "--last-data-date", "2026-01-06", "--days", "0"]) == 1
    assert "days" in capsys.readouterr().err


def test_cli_wait_writes_summary_and_raw_output(tmp_path, monkeypatch):
    done = json.dumps(
        [run_json("2026-01-05", "success", "2026-11-11T10:00:00Z", "2026-11-11T10:02:00Z")]
    )
    calls: list[list[str]] = []

    def fake_list_runs(airflow: str, dag_id: str) -> str:
        calls.append([airflow, dag_id])
        return done

    monkeypatch.setattr(dagspec, "airflow_list_runs", fake_list_runs)
    summary_path = tmp_path / "summary.json"
    raw_path = tmp_path / "list-runs.json"
    code = dagspec.main(
        [
            "wait",
            "--last-data-date",
            "2026-01-04",
            "--days",
            "1",
            "--started-at",
            "2026-11-11T10:00:00+00:00",
            "--timeout-minutes",
            "5",
            "--interval-seconds",
            "1",
            "--airflow",
            "/venv/bin/airflow",
            "--summary",
            str(summary_path),
            "--raw",
            str(raw_path),
        ]
    )
    assert code == 0
    assert calls == [["/venv/bin/airflow", "repopulse_pipeline"]]
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    assert summary["status"] == "success"
    assert summary["wall_seconds"] == 120.0
    assert summary["hours_per_day"] == 1
    assert summary["max_active_runs"] == 1
    assert raw_path.read_text(encoding="utf-8") == done


def test_cli_wait_exit_1_on_failed_run(tmp_path, monkeypatch):
    failed = json.dumps(
        [run_json("2026-01-05", "failed", "2026-11-11T10:00:00Z", "2026-11-11T10:02:00Z")]
    )
    monkeypatch.setattr(dagspec, "airflow_list_runs", lambda airflow, dag_id: failed)
    code = dagspec.main(
        [
            "wait",
            "--last-data-date",
            "2026-01-04",
            "--days",
            "1",
            "--started-at",
            "2026-11-11T10:00:00+00:00",
            "--timeout-minutes",
            "1",
            "--summary",
            str(tmp_path / "s.json"),
        ]
    )
    assert code == 1
    assert json.loads((tmp_path / "s.json").read_text(encoding="utf-8"))["status"] == "failed"


def test_cli_row_writes_header_and_one_row(tmp_path):
    summary_path = tmp_path / "summary.json"
    summary_path.write_text(json.dumps(success_summary(2)), encoding="utf-8")
    out = tmp_path / "row.csv"
    code = dagspec.main(
        [
            "row",
            "--summary",
            str(summary_path),
            "--run-at",
            "2026-11-11T11:00:00+00:00",
            "--git-sha",
            "abc1234",
            "--run-id",
            "7",
            "--trigger",
            "pull_request",
            "--summaries-dir",
            str(tmp_path / "none"),
            "--out",
            str(out),
        ]
    )
    assert code == 0
    with out.open(encoding="utf-8", newline="") as fh:
        rows = list(csv.reader(fh))
    assert rows[0] == list(COLUMNS)
    assert len(rows) == 2
    assert rows[1][COLUMNS.index("variant")] == "airflow-backfill-2d"
    assert rows[1][COLUMNS.index("row_count")] == ""
    # benchmarks/record_run.py (which merges artifacts into runs.csv) accepts the file
    (merged,) = record_run.read_artifact(out)
    assert validate_row(merged)["variant"] == "airflow-backfill-2d"


def test_airflow_list_runs_calls_the_cli(monkeypatch):
    seen = {}

    class Done:
        returncode = 0
        stdout = "[]"
        stderr = ""

    def fake_run(cmd, **kwargs):
        seen["cmd"] = cmd
        return Done()

    monkeypatch.setattr(dagspec.subprocess, "run", fake_run)
    assert dagspec.airflow_list_runs("airflow", "repopulse_pipeline") == "[]"
    assert seen["cmd"] == ["airflow", "dags", "list-runs", "-o", "json", "repopulse_pipeline"]


def test_airflow_list_runs_raises_on_non_zero_exit(monkeypatch):
    class Failed:
        returncode = 1
        stdout = ""
        stderr = "boom"

    monkeypatch.setattr(dagspec.subprocess, "run", lambda cmd, **kw: Failed())
    with pytest.raises(dagspec.DagSpecError, match="boom"):
        dagspec.airflow_list_runs("airflow", "repopulse_pipeline")


def test_module_does_not_import_airflow():
    source = Path(dagspec.__file__).read_text(encoding="utf-8")
    assert "import airflow" not in source
    assert "from airflow" not in source
