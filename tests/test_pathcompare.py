"""Tests of repopulse.pathcompare (D3): rows, parser, percentile, summary, helpers, CLI.

No network: GitHub API, Databricks and MotherDuck are replaced by fakes; DuckDB runs on a local
file under tmp_path. The numbers in tests/fixtures/pathcompare/ are fake test values, never
written to the real runs.csv.
"""

from __future__ import annotations

import csv
import json
import re
from datetime import UTC, date, datetime
from pathlib import Path

import pytest

from benchmarks.record_run import _parse, read_artifact
from repopulse import pathcompare as pc
from repopulse import readme_results
from repopulse.probe import HttpResponse
from repopulse.runs_log import COLUMNS, validate_row


def revalidated(row):
    """``row`` (CSV strings) typed like record_run.py reads it, then through validate_row."""
    return validate_row({c: _parse(c, v) for c, v in row.items()})


FIXTURES = Path(__file__).parent / "fixtures" / "pathcompare"
RUNS = FIXTURES / "runs.csv"
D1_ROW = FIXTURES / "databricks-row.csv"
GH_JOBS = FIXTURES / "gh_jobs.json"
RUN_AT = datetime(2026, 11, 20, 10, 41, 30, tzinfo=UTC)
SHA = "abcdef1"


def notes_of(row):
    return dict(word.split("=", 1) for word in row["notes"].split())


def write_csv(path: Path, rows: list[list[str]], header=COLUMNS) -> Path:
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.writer(fh, lineterminator="\n")
        writer.writerow(header)
        writer.writerows(rows)
    return path


def a_success_kwargs(**over):
    kwargs = {
        "path": "a",
        "run_at": RUN_AT,
        "git_sha": SHA,
        "data_date": "2026-11-20",
        "hour": 8,
        "hours": 1,
        "outcomes": {s: "success" for s in pc.PATH_STEPS["a"]},
        "timings": {"ingest": 10.0, "spark": 50.0, "export": 5.0, "dbt": 30.0, "queries": 5.0},
        "waits": {"queue": 3.0, "setup": 60.0},
        "queries": {f"q{i}": i / 10 for i in range(1, 7)},
        "row_count": 50,
        "run_id": "101",
        "trigger": "schedule",
    }
    kwargs.update(over)
    return kwargs


# ---------------------------------------------------------------- constants


def test_variants_and_steps_of_the_two_paths():
    assert pc.VARIANTS == {"a": "path-a-runner", "b": "path-b-databricks"}
    assert pc.PATH_STEPS["a"] == ("ingest", "spark", "export", "dbt", "queries")
    assert pc.PATH_STEPS["b"] == ("ingest", "databricks", "warehouse", "dbt", "queries")
    assert pc.PATH_A_DATABASE != "repopulse"
    assert pc.MART_BY_QUERY["q1"] == "mart_daily_active_contributors"
    assert pc.MART_BY_QUERY["q6"] == "mart_repo_health_score"


# ---------------------------------------------------------------- percentile


@pytest.mark.parametrize(
    ("values", "p", "expected"),
    [
        ([], 50, None),
        ([], 95, None),
        ([7.0], 50, 7.0),
        ([7.0], 95, 7.0),
        ([3.0, 1.0], 50, 1.0),
        ([3.0, 1.0], 95, 3.0),
        ([100.0, 120.0, 110.0], 50, 110.0),
        ([100.0, 120.0, 110.0], 95, 120.0),
        ([float(i) for i in range(1, 21)], 50, 10.0),
        ([float(i) for i in range(1, 21)], 95, 19.0),
        ([float(i) for i in range(1, 21)], 100, 20.0),
        ([float(i) for i in range(1, 101)], 95, 95.0),
        # rank = ceil(p/100 * n), not round: n=5 -> rank 3 (2.5 up), n=9 -> rank 5 (4.5 up)
        ([5.0, 4.0, 3.0, 2.0, 1.0], 50, 3.0),
        ([float(i) for i in range(1, 10)], 50, 5.0),
        ([float(i) for i in range(1, 22)], 95, 20.0),
    ],
)
def test_percentile_is_nearest_rank(values, p, expected):
    assert pc.percentile(values, p) == expected


def test_percentile_returns_an_observed_value_and_does_not_mutate_input():
    values = [5.0, 1.0, 3.0, 2.0]
    assert pc.percentile(values, 50) in values
    assert values == [5.0, 1.0, 3.0, 2.0]


@pytest.mark.parametrize("p", [0, -1, 101, 150])
def test_percentile_rejects_p_outside_0_100(p):
    with pytest.raises(ValueError):
        pc.percentile([1.0], p)


# ---------------------------------------------------------------- build_row


def test_build_row_success_path_a():
    row = pc.build_row(**a_success_kwargs())
    assert row == revalidated(row)
    assert row["variant"] == "path-a-runner"
    assert row["data_date"] == "2026-11-20"
    assert float(row["total_seconds"]) == 100.0
    assert row["row_count"] == "50"
    assert [float(row[f"q{i}_seconds"]) for i in range(1, 7)] == [i / 10 for i in range(1, 7)]
    notes = notes_of(row)
    assert notes["status"] == "success"
    assert "failed_step" not in notes
    assert notes["hour"] == "8" and notes["hours"] == "1"
    assert notes["ingest_seconds"] == "10.0"
    assert notes["dbt_seconds"] == "30.0"
    assert notes["queries_seconds"] == "5.0"
    assert notes["queue_seconds"] == "3.0"
    assert notes["setup_seconds"] == "60.0"
    assert notes["run_id"] == "101" and notes["trigger"] == "schedule"
    assert "\n" not in row["notes"]


def test_build_row_notes_list_steps_in_pipeline_order():
    row = pc.build_row(**a_success_kwargs())
    keys = [w.split("=", 1)[0] for w in row["notes"].split()]
    steps = [k for k in keys if k[: -len("_seconds")] in pc.PATH_STEPS["a"]]
    assert steps == [f"{s}_seconds" for s in pc.PATH_STEPS["a"]]
    assert keys[0] == "status"


def test_build_row_failed_run_still_gives_a_valid_row():
    outcomes = {
        "ingest": "success",
        "spark": "success",
        "export": "success",
        "dbt": "failure",
        "queries": "skipped",
    }
    row = pc.build_row(
        **a_success_kwargs(
            outcomes=outcomes,
            timings={"ingest": 11.0, "spark": 52.0, "export": 4.0, "dbt": 3.0},
            queries=None,
            row_count=None,
        )
    )
    assert row == revalidated(row)
    notes = notes_of(row)
    assert notes["status"] == "failed"
    assert notes["failed_step"] == "dbt"
    assert float(row["total_seconds"]) == 70.0
    assert "queries_seconds" not in notes
    assert all(row[f"q{i}_seconds"] == "" for i in range(1, 7))
    assert row["row_count"] == ""


def test_build_row_cancelled_step_is_a_failure():
    outcomes = {s: "success" for s in pc.PATH_STEPS["a"]}
    outcomes["spark"] = "cancelled"
    outcomes.update(export="skipped", dbt="skipped", queries="skipped")
    row = pc.build_row(
        **a_success_kwargs(outcomes=outcomes, timings={"ingest": 1.0, "spark": 2.0}, queries=None)
    )
    notes = notes_of(row)
    assert notes["status"] == "failed" and notes["failed_step"] == "spark"


def test_build_row_step_without_outcome_counts_as_not_run():
    row = pc.build_row(
        **a_success_kwargs(outcomes={"ingest": "success"}, timings={"ingest": 1.0}, queries=None)
    )
    notes = notes_of(row)
    assert notes["status"] == "failed" and notes["failed_step"] == "spark"
    assert float(row["total_seconds"]) == 1.0


def test_build_row_failure_before_any_timing_has_total_zero():
    row = pc.build_row(
        **a_success_kwargs(outcomes={"ingest": "failure"}, timings={}, queries=None, waits={})
    )
    assert float(row["total_seconds"]) == 0.0
    assert notes_of(row)["failed_step"] == "ingest"


def test_build_row_success_path_b_with_databricks_waits():
    timings = {"ingest": 10.0, "databricks": 180.0, "warehouse": 20.0, "dbt": 80.0, "queries": 10}
    waits = {
        "queue": 2.0,
        "setup": 30.0,
        "job_queue": 15.0,
        "job_setup": 40.0,
        "warehouse_start": 18.0,
    }
    row = pc.build_row(
        **a_success_kwargs(
            path="b",
            outcomes={s: "success" for s in pc.PATH_STEPS["b"]},
            timings=timings,
            waits=waits,
        )
    )
    assert row["variant"] == "path-b-databricks"
    assert float(row["total_seconds"]) == 300.0
    notes = notes_of(row)
    assert notes["databricks_seconds"] == "180.0"
    assert notes["warehouse_seconds"] == "20.0"
    assert notes["job_queue_seconds"] == "15.0"
    assert notes["job_setup_seconds"] == "40.0"
    assert notes["warehouse_start_seconds"] == "18.0"


def test_build_row_success_needs_every_step_timing():
    with pytest.raises(pc.PathCompareError, match="queries"):
        pc.build_row(
            **a_success_kwargs(
                timings={"ingest": 10.0, "spark": 50.0, "export": 5.0, "dbt": 30.0},
            )
        )


@pytest.mark.parametrize(
    "over",
    [
        {"path": "c"},
        {"outcomes": {"ingest": "maybe"}},
        {"outcomes": {"nonsense": "success"}},
        {"timings": {"ingest": 1.0, "nonsense": 2.0}},
        {"timings": {"ingest": -1.0}},
        {"timings": {"ingest": "fast"}},
        {"timings": {"ingest": True}},
        {"waits": {"job_queue": 1.0}},  # Databricks job waits only exist on path B
        {"waits": {"queue": float("nan")}},
        {"queries": {"q7": 1.0}},
        {"row_count": -1},
        {"run_id": "has space"},
        {"trigger": "a=b"},
        {"hour": 24},
        {"hours": 0},
    ],
)
def test_build_row_rejects_bad_input(over):
    with pytest.raises(pc.PathCompareError):
        pc.build_row(**a_success_kwargs(**over))


def test_build_row_records_missing_hours_from_the_ingest_summary():
    records = [
        {"data_date": "2026-11-20", "hour": 8, "status": "ingested"},
        {"data_date": "2026-11-20", "hour": 9, "status": "missing"},
    ]
    row = pc.build_row(**a_success_kwargs(hours=2, ingest_records=records))
    assert notes_of(row)["missing_hours"] == "1"


def test_build_row_records_bronze_rows_so_both_paths_show_the_same_input():
    # row_count is what silver gained: 0 when the hour was already in the path's silver.
    # bronze_rows is what the run read, equal on A and B for the same data window.
    row = pc.build_row(**a_success_kwargs(row_count=0, bronze_rows=68488))
    assert row["row_count"] == "0"
    assert notes_of(row)["bronze_rows"] == "68488"


def test_build_row_without_bronze_rows_has_no_bronze_note():
    assert "bronze_rows" not in notes_of(pc.build_row(**a_success_kwargs()))


@pytest.mark.parametrize("bad", [-1, 1.5, True, "68488"])
def test_build_row_rejects_a_bad_bronze_rows(bad):
    with pytest.raises(pc.PathCompareError, match="bronze_rows"):
        pc.build_row(**a_success_kwargs(bronze_rows=bad))


# ---------------------------------------------------------------- notes + parser


def test_parse_notes_splits_key_value_words():
    assert pc.parse_notes("status=success dbt_seconds=1.5 run_id=7") == {
        "status": "success",
        "dbt_seconds": "1.5",
        "run_id": "7",
    }


@pytest.mark.parametrize(
    "text", ["status", "status=success status=failed", "=x", "Bad=1", "a==b", "a="]
)
def test_parse_notes_rejects_malformed_words(text):
    with pytest.raises(pc.PathCompareError):
        pc.parse_notes(text)


def test_read_runs_keeps_only_the_two_path_variants():
    runs = pc.read_runs(RUNS)
    assert len(runs) == 7
    assert {r.variant for r in runs} == {"path-a-runner", "path-b-databricks"}
    assert [r.path for r in runs].count("a") == 4


def test_read_runs_types_every_field():
    first = pc.read_runs(RUNS)[0]
    assert first.variant == "path-a-runner" and first.path == "a"
    assert first.run_at == datetime(2026, 11, 20, 10, 41, 30, tzinfo=UTC)
    assert first.day == date(2026, 11, 20)
    assert first.data_date == "2026-11-20"
    assert first.status == "success" and first.failed_step is None
    assert first.total_seconds == 100.0
    assert first.row_count == 50
    assert first.queries == {"q1": 0.5, "q2": 0.6, "q3": 0.7, "q4": 0.8, "q5": 0.9, "q6": 1.0}
    assert first.steps == {
        "ingest": 10.0,
        "spark": 50.0,
        "export": 5.0,
        "dbt": 30.0,
        "queries": 5.0,
    }
    assert first.waits == {"queue": 3.0, "setup": 60.0}
    assert first.notes["trigger"] == "schedule"


def test_read_runs_failed_row_has_failed_step_and_no_queries():
    failed = [r for r in pc.read_runs(RUNS) if r.status == "failed"]
    assert [(r.path, r.failed_step) for r in failed] == [("a", "dbt"), ("b", "databricks")]
    assert all(r.queries == {} and r.row_count is None for r in failed)


def test_read_runs_on_a_built_row_round_trips(tmp_path):
    row = pc.build_row(**a_success_kwargs())
    out = tmp_path / "row.csv"
    pc.write_rows(out, [row])
    (run,) = pc.read_runs(out)
    assert run.status == "success"
    assert run.steps["spark"] == 50.0
    assert run.waits == {"queue": 3.0, "setup": 60.0}


def _path_row(notes: str, total: str = "1.0") -> list[str]:
    cells = {c: "" for c in COLUMNS}
    cells.update(
        run_at="2026-11-20T10:00:00+00:00",
        git_sha=SHA,
        variant="path-a-runner",
        data_date="2026-11-20",
        total_seconds=total,
        notes=notes,
    )
    return [cells[c] for c in COLUMNS]


@pytest.mark.parametrize(
    ("notes", "total", "match"),
    [
        ("hour=8 hours=1", "1.0", "status"),
        ("status=maybe", "1.0", "status"),
        ("status=success dbt_seconds=fast", "1.0", "dbt_seconds"),
        ("status=success broken", "1.0", "broken"),
        ("status=failed", "1.0", "failed_step"),
        ("status=success", "slow", "total_seconds"),
    ],
)
def test_read_runs_reports_bad_rows_with_their_line(tmp_path, notes, total, match):
    path = write_csv(tmp_path / "runs.csv", [_path_row("status=success"), _path_row(notes, total)])
    with pytest.raises(pc.PathCompareError, match=match) as info:
        pc.read_runs(path)
    assert "line 3" in str(info.value)


def test_read_runs_ignores_bad_notes_of_other_variants(tmp_path):
    other = _path_row("free text, not key value")
    other[COLUMNS.index("variant")] = "baseline"
    path = write_csv(tmp_path / "runs.csv", [other, _path_row("status=success")])
    assert len(pc.read_runs(path)) == 1


def test_read_runs_rejects_a_wrong_header(tmp_path):
    path = write_csv(tmp_path / "runs.csv", [], header=("run_at", "variant"))
    with pytest.raises(pc.PathCompareError, match="header"):
        pc.read_runs(path)


def test_read_runs_rejects_a_short_line(tmp_path):
    path = write_csv(tmp_path / "runs.csv", [["2026-11-20T10:00:00+00:00", SHA]])
    with pytest.raises(pc.PathCompareError, match="line 2"):
        pc.read_runs(path)


def test_read_runs_missing_file(tmp_path):
    with pytest.raises(pc.PathCompareError, match="not found"):
        pc.read_runs(tmp_path / "nope.csv")


# ---------------------------------------------------------------- summarize


def test_summarize_path_a():
    a = pc.summarize(pc.read_runs(RUNS))["path-a-runner"]
    assert a.runs == 4
    assert a.successes == 3
    assert a.success_rate == 0.75
    assert a.e2e_p50 == 110.0
    assert a.e2e_p95 == 120.0
    assert a.steps["dbt"] == (35.0, 40.0)  # successful runs only: 30, 40, 35
    assert a.steps["spark"] == (55.0, 58.0)
    assert a.waits["queue"] == (4.0, 6.0)  # every run: 3, 5, 6, 4
    assert a.waits["setup"] == (60.0, 62.0)
    assert a.failed_steps == {"dbt": 1}
    assert a.runs_per_day == {date(2026, 11, 20): 3, date(2026, 11, 21): 1}
    assert (a.min_runs_per_day, a.max_runs_per_day) == (1, 3)


def test_summarize_path_b_counts_days_without_runs():
    b = pc.summarize(pc.read_runs(RUNS))["path-b-databricks"]
    assert b.runs == 3
    assert b.successes == 2
    assert b.success_rate == pytest.approx(2 / 3)
    assert (b.e2e_p50, b.e2e_p95) == (280.0, 300.0)
    assert b.waits["job_queue"] == (15.0, 25.0)
    assert b.waits["warehouse_start"] == (0.0, 18.0)
    assert b.steps["warehouse"] == (0.5, 20.0)
    assert b.failed_steps == {"databricks": 1}
    assert b.runs_per_day == {
        date(2026, 11, 20): 1,
        date(2026, 11, 21): 0,
        date(2026, 11, 22): 2,
    }
    assert (b.min_runs_per_day, b.max_runs_per_day) == (0, 2)


def test_summarize_without_runs_gives_empty_summaries_for_both_paths():
    out = pc.summarize([])
    assert set(out) == {"path-a-runner", "path-b-databricks"}
    for s in out.values():
        assert s.runs == 0 and s.successes == 0
        assert s.success_rate is None
        assert s.e2e_p50 is None and s.e2e_p95 is None
        assert s.runs_per_day == {}
        assert s.min_runs_per_day is None and s.max_runs_per_day is None


def test_summarize_only_failures_has_no_e2e_percentile():
    runs = [r for r in pc.read_runs(RUNS) if r.status == "failed"]
    a = pc.summarize(runs)["path-a-runner"]
    assert a.runs == 1 and a.successes == 0 and a.success_rate == 0.0
    assert a.e2e_p50 is None and a.e2e_p95 is None
    assert a.waits["queue"] == (6.0, 6.0)


def test_summarize_is_order_independent():
    runs = pc.read_runs(RUNS)
    assert pc.summarize(runs) == pc.summarize(list(reversed(runs)))


def test_render_summary_markdown():
    text = pc.render_summary(pc.summarize(pc.read_runs(RUNS)))
    assert "| path-a-runner |" in text and "| path-b-databricks |" in text
    assert "75.0%" in text and "66.7%" in text
    assert "110.0" in text and "280.0" in text
    assert "nearest-rank" in text
    assert "incremental batch" in text


# ---------------------------------------------------------------- window


def test_window_push_uses_the_reference_day():
    assert pc.decide_window("push", now=RUN_AT, reference_date="2026-10-25") == (
        "2026-10-25",
        23,
        1,
    )


def test_window_schedule_is_now_minus_lag():
    assert pc.decide_window("schedule", now=RUN_AT, reference_date="2026-10-25") == (
        "2026-11-20",
        8,
        1,
    )


def test_window_schedule_crosses_midnight():
    now = datetime(2026, 11, 20, 1, 41, tzinfo=UTC)
    assert pc.decide_window("schedule", now=now, reference_date="x") == ("2026-11-19", 23, 1)


def test_window_dispatch_inputs_win():
    got = pc.decide_window(
        "workflow_dispatch",
        now=RUN_AT,
        reference_date="2026-10-25",
        date="2026-11-01",
        hour="5",
        hours="3",
    )
    assert got == ("2026-11-01", 5, 3)


def test_window_dispatch_without_date_is_like_schedule_with_given_hours():
    got = pc.decide_window("workflow_dispatch", now=RUN_AT, reference_date="x", hours="2")
    assert got == ("2026-11-20", 8, 2)


@pytest.mark.parametrize(
    "inputs",
    [
        {"date": "2026-11-01"},  # hour missing
        {"date": "2026-11-01", "hour": "24"},
        {"date": "11/01/2026", "hour": "3"},
        {"hours": "0"},
        {"hours": "7"},
        {"hours": "two"},
    ],
)
def test_window_rejects_bad_inputs(inputs):
    with pytest.raises(pc.PathCompareError):
        pc.decide_window("workflow_dispatch", now=RUN_AT, reference_date="2026-10-25", **inputs)


def test_window_cli_prints_github_outputs(tmp_path, capsys):
    ref = tmp_path / "reference.toml"
    ref.write_text('data_date = "2026-10-25"\n', encoding="utf-8")
    code = pc.main(["window", "--event", "push", "--reference", str(ref)])
    assert code == 0
    assert capsys.readouterr().out.splitlines() == ["date=2026-10-25", "hour=23", "hours=1"]


# ---------------------------------------------------------------- runner waits (GitHub API)


def test_runner_waits_from_the_jobs_api():
    payload = json.loads(GH_JOBS.read_text(encoding="utf-8"))
    now = datetime(2026, 11, 20, 10, 43, 45, tzinfo=UTC)
    waits = pc.runner_waits(payload, "Path A (runner + MotherDuck)", now=now)
    assert waits == {"queue": 15.0, "setup": 120.0}


@pytest.mark.parametrize("name", ["No such job", "Path B (Databricks serverless + SQL warehouse)"])
def test_runner_waits_unknown_or_unstarted_job(name):
    payload = json.loads(GH_JOBS.read_text(encoding="utf-8"))
    with pytest.raises(pc.PathCompareError):
        pc.runner_waits(payload, name, now=RUN_AT)


def _gh_env(tmp_path):
    return {
        "GITHUB_API_URL": "https://api.example.test",
        "GITHUB_REPOSITORY": "owner/repo",
        "GITHUB_RUN_ID": "42",
        "GITHUB_RUN_ATTEMPT": "1",
        "GH_TOKEN": "gh-token-value",
    }


def test_runner_waits_cli_calls_the_attempt_jobs_endpoint(tmp_path):
    seen = {}

    def fetch(url, headers):
        seen["url"], seen["headers"] = url, headers
        return GH_JOBS.read_bytes()

    out = tmp_path / "waits.json"
    code = pc.main(
        ["runner-waits", "--job-name", "Path A (runner + MotherDuck)", "--out", str(out)],
        env=_gh_env(tmp_path),
        fetch=fetch,
        now=lambda: datetime(2026, 11, 20, 10, 42, 45, tzinfo=UTC),
    )
    assert code == 0
    assert seen["url"].startswith(
        "https://api.example.test/repos/owner/repo/actions/runs/42/attempts/1/jobs"
    )
    assert seen["headers"]["Authorization"] == "Bearer gh-token-value"
    assert json.loads(out.read_text(encoding="utf-8")) == {"queue": 15.0, "setup": 60.0}


def test_runner_waits_cli_failure_is_a_warning_with_empty_waits(tmp_path, capsys):
    def fetch(url, headers):
        raise OSError("network down gh-token-value")

    out = tmp_path / "waits.json"
    code = pc.main(
        ["runner-waits", "--job-name", "x", "--out", str(out)],
        env=_gh_env(tmp_path),
        fetch=fetch,
    )
    assert code == 0
    assert json.loads(out.read_text(encoding="utf-8")) == {}
    printed = capsys.readouterr()
    assert "::warning::" in printed.out
    assert "gh-token-value" not in printed.out + printed.err


# ---------------------------------------------------------------- Databricks job waits (D1 row)


def test_databricks_waits_from_the_d1_row():
    assert pc.databricks_waits(D1_ROW) == {"job_queue": 12.5, "job_setup": 40.0}


def test_databricks_waits_missing_file_is_empty(tmp_path):
    assert pc.databricks_waits(tmp_path / "nope.csv") == {}


# ---------------------------------------------------------------- fake Databricks transport


class FakeDatabricks:
    """Scripted answers by (method, path); records every request."""

    def __init__(self, answers):
        self.answers = {k: list(v) for k, v in answers.items()}
        self.requests = []

    def __call__(self, req):
        path = req.url.split("://", 1)[1].split("/", 1)[1]
        path = "/" + path.split("?", 1)[0]
        self.requests.append((req.method, path, json.loads(req.body) if req.body else None))
        queue = self.answers[(req.method, path)]
        body = queue.pop(0) if len(queue) > 1 else queue[0]
        return HttpResponse(200, json.dumps(body).encode())


class Clock:
    def __init__(self, step=1.0):
        self.t = 0.0
        self.step = step

    def __call__(self):
        self.t += self.step
        return self.t


DBX_ENV = {
    "DATABRICKS_HOST": "https://dbc-secret-host.cloud.databricks.com",
    "DATABRICKS_TOKEN": "dapi-secret-token-123",
    "DATABRICKS_HTTP_PATH": "/sql/1.0/warehouses/wh123abc",
}
WH = "/api/2.0/sql/warehouses/wh123abc"


def test_warehouse_start_when_already_running(tmp_path):
    fake = FakeDatabricks({("GET", WH): [{"state": "RUNNING"}]})
    out = tmp_path / "w.json"
    code = pc.main(
        ["warehouse-start", "--out", str(out)],
        env=DBX_ENV,
        transport=fake,
        clock=Clock(),
        sleep=lambda s: None,
    )
    assert code == 0
    assert json.loads(out.read_text(encoding="utf-8")) == {"warehouse_start": 0.0}
    assert [m for m, _, _ in fake.requests] == ["GET"]


def test_warehouse_start_when_stopped_starts_and_waits(tmp_path):
    fake = FakeDatabricks(
        {
            ("GET", WH): [{"state": "STOPPED"}, {"state": "STARTING"}, {"state": "RUNNING"}],
            ("POST", WH + "/start"): [{}],
        }
    )
    out = tmp_path / "w.json"
    code = pc.main(
        ["warehouse-start", "--out", str(out), "--poll-interval", "1"],
        env=DBX_ENV,
        transport=fake,
        clock=Clock(step=5.0),
        sleep=lambda s: None,
    )
    assert code == 0
    waited = json.loads(out.read_text(encoding="utf-8"))["warehouse_start"]
    assert waited > 0
    assert [(m, p) for m, p, _ in fake.requests].count(("POST", WH + "/start")) == 1


def test_warehouse_start_timeout_fails_without_leaking(tmp_path, capsys):
    fake = FakeDatabricks(
        {("GET", WH): [{"state": "STOPPED"}, {"state": "STARTING"}], ("POST", WH + "/start"): [{}]}
    )
    code = pc.main(
        ["warehouse-start", "--out", str(tmp_path / "w.json"), "--timeout", "10"],
        env=DBX_ENV,
        transport=fake,
        clock=Clock(step=5.0),
        sleep=lambda s: None,
    )
    assert code == 1
    printed = capsys.readouterr()
    for secret in ("dbc-secret-host", "dapi-secret-token-123", "wh123abc"):
        assert secret not in printed.out + printed.err


def test_warehouse_start_missing_secret(tmp_path):
    code = pc.main(["warehouse-start", "--out", str(tmp_path / "w.json")], env={})
    assert code == 1


# ---------------------------------------------------------------- queries


def _gold_duckdb(path: Path, marts=None):
    import duckdb

    con = duckdb.connect(str(path))
    con.execute("create schema main_gold")
    for mart in marts or pc.MART_BY_QUERY.values():
        con.execute(f"create table main_gold.{mart} as select range as x from range(3)")
    con.close()


def test_queries_duckdb_reads_every_mart(tmp_path):
    db = tmp_path / "gold.duckdb"
    _gold_duckdb(db)
    out = tmp_path / "q.json"
    code = pc.main(
        ["queries", "--engine", "duckdb", "--database", str(db), "--schema", "main_gold"]
        + ["--out", str(out)]
    )
    assert code == 0
    data = json.loads(out.read_text(encoding="utf-8"))
    assert set(data["seconds"]) == set(pc.MART_BY_QUERY)
    assert all(v >= 0 for v in data["seconds"].values())
    assert data["rows"] == {q: 3 for q in pc.MART_BY_QUERY}


def test_queries_duckdb_missing_mart_fails(tmp_path):
    db = tmp_path / "gold.duckdb"
    _gold_duckdb(db, marts=["mart_daily_active_contributors"])
    code = pc.main(
        ["queries", "--engine", "duckdb", "--database", str(db), "--schema", "main_gold"]
        + ["--out", str(tmp_path / "q.json")]
    )
    assert code == 1
    assert not (tmp_path / "q.json").exists()


def test_queries_refuse_an_unsafe_schema(tmp_path):
    code = pc.main(
        ["queries", "--engine", "duckdb", "--database", "x.duckdb", "--schema", "a; drop"]
        + ["--out", str(tmp_path / "q.json")]
    )
    assert code == 1


def test_queries_databricks_statement_api(tmp_path):
    done = {
        "statement_id": "s1",
        "status": {"state": "SUCCEEDED"},
        "manifest": {"total_row_count": 4},
    }
    pending = {"statement_id": "s1", "status": {"state": "PENDING"}}
    fake = FakeDatabricks(
        {
            ("POST", "/api/2.0/sql/statements"): [pending, done],
            ("GET", "/api/2.0/sql/statements/s1"): [done],
        }
    )
    out = tmp_path / "q.json"
    code = pc.main(
        ["queries", "--engine", "databricks", "--schema", "workspace.repopulse_dbt_gold"]
        + ["--out", str(out), "--poll-interval", "1"],
        env=DBX_ENV,
        transport=fake,
        clock=Clock(),
        sleep=lambda s: None,
    )
    assert code == 0
    data = json.loads(out.read_text(encoding="utf-8"))
    assert set(data["seconds"]) == set(pc.MART_BY_QUERY)
    assert data["rows"] == {q: 4 for q in pc.MART_BY_QUERY}
    statements = [b["statement"] for m, p, b in fake.requests if m == "POST"]
    assert (
        statements[0] == "SELECT * FROM workspace.repopulse_dbt_gold.mart_daily_active_contributors"
    )
    assert all(b["warehouse_id"] == "wh123abc" for m, p, b in fake.requests if m == "POST")


def test_queries_databricks_failed_statement(tmp_path, capsys):
    failed = {
        "statement_id": "s1",
        "status": {"state": "FAILED", "error": {"message": "table not found on dbc-secret-host"}},
    }
    fake = FakeDatabricks({("POST", "/api/2.0/sql/statements"): [failed]})
    code = pc.main(
        ["queries", "--engine", "databricks", "--schema", "workspace.repopulse_dbt_gold"]
        + ["--out", str(tmp_path / "q.json")],
        env=DBX_ENV,
        transport=fake,
        clock=Clock(),
        sleep=lambda s: None,
    )
    assert code == 1
    printed = capsys.readouterr()
    assert "dbc-secret-host" not in printed.out + printed.err


# ---------------------------------------------------------------- MotherDuck database of path A


class FakeCon:
    def __init__(self, log):
        self.log = log

    def execute(self, sql):
        self.log.append(sql)

    def close(self):
        self.log.append("close")


def test_ensure_database_creates_if_missing():
    log, opened = [], []

    def connect(target):
        opened.append(target)
        return FakeCon(log)

    pc.ensure_database(pc.PATH_A_DATABASE, connect=connect)
    assert opened == ["md:"]
    assert log == [f"CREATE DATABASE IF NOT EXISTS {pc.PATH_A_DATABASE}", "close"]


@pytest.mark.parametrize("name", ["repopulse", "Repopulse", "x; drop", "", "repopulse_smoke"])
def test_ensure_database_refuses_production_and_bad_names(name):
    with pytest.raises(pc.PathCompareError):
        pc.ensure_database(name, connect=lambda t: pytest.fail("must not connect"))


# ---------------------------------------------------------------- row CLI


def _write_json(path: Path, data) -> Path:
    path.write_text(json.dumps(data), encoding="utf-8")
    return path


def test_row_cli_success_path_a(tmp_path):
    timings = _write_json(
        tmp_path / "timings.json",
        {"ingest": 10.0, "spark": 50.0, "export": 5.0, "dbt": 30.0, "queries": 5.0},
    )
    waits = _write_json(tmp_path / "waits.json", {"queue": 3.0, "setup": 60.0})
    queries = _write_json(
        tmp_path / "queries.json",
        {"seconds": {f"q{i}": 0.5 for i in range(1, 7)}, "rows": {}},
    )
    spark = _write_json(tmp_path / "spark.json", {"silver_inserted": 50, "bronze_rows": 70})
    ingest = _write_json(
        tmp_path / "ingest.json", [{"data_date": "2026-11-20", "hour": 8, "status": "ingested"}]
    )
    out = tmp_path / "out" / "path-a-row.csv"
    args = ["row", "--path", "a", "--run-at", RUN_AT.isoformat(), "--git-sha", SHA]
    args += ["--date", "2026-11-20", "--hour", "8", "--hours", "1"]
    args += ["--timings", str(timings), "--waits", str(waits), "--queries", str(queries)]
    args += ["--summary-json", str(spark), "--ingest-summary", str(ingest)]
    args += ["--run-id", "101", "--trigger", "schedule", "--out", str(out)]
    for step in pc.PATH_STEPS["a"]:
        args += ["--outcome", f"{step}=success"]
    assert pc.main(args) == 0
    (row,) = read_artifact(out)
    assert row["variant"] == "path-a-runner"
    assert row["row_count"] == 50
    assert row["total_seconds"] == 100.0
    assert notes_of(row)["status"] == "success"
    assert notes_of(row)["missing_hours"] == "0"
    assert notes_of(row)["bronze_rows"] == "70"


def test_row_cli_failed_path_b_with_missing_files(tmp_path):
    timings = _write_json(tmp_path / "timings.json", {"ingest": 10.0, "databricks": 40.0})
    out = tmp_path / "path-b-row.csv"
    args = ["row", "--path", "b", "--run-at", RUN_AT.isoformat(), "--git-sha", SHA]
    args += ["--date", "2026-11-20", "--hour", "8", "--hours", "1"]
    args += ["--timings", str(timings), "--waits", str(tmp_path / "absent.json")]
    args += ["--queries", str(tmp_path / "absent-q.json")]
    args += ["--summary-json", str(tmp_path / "absent-s.json")]
    args += ["--databricks-row", str(tmp_path / "absent-row.csv")]
    args += ["--run-id", "201", "--trigger", "schedule", "--out", str(out)]
    args += ["--outcome", "ingest=success", "--outcome", "databricks=failure"]
    args += ["--outcome", "warehouse=skipped", "--outcome", "dbt=skipped"]
    args += ["--outcome", "queries=skipped"]
    assert pc.main(args) == 0
    (row,) = read_artifact(out)
    assert row["variant"] == "path-b-databricks"
    assert row["total_seconds"] == 50.0
    assert row["row_count"] is None
    notes = notes_of(row)
    assert notes["status"] == "failed" and notes["failed_step"] == "databricks"


def test_row_cli_path_b_takes_job_waits_from_the_d1_row(tmp_path):
    timings = _write_json(
        tmp_path / "timings.json",
        {"ingest": 1.0, "databricks": 2.0, "warehouse": 3.0, "dbt": 4.0, "queries": 5.0},
    )
    out = tmp_path / "row.csv"
    args = ["row", "--path", "b", "--run-at", RUN_AT.isoformat(), "--git-sha", SHA]
    args += ["--date", "2026-11-20", "--hour", "8", "--hours", "1"]
    args += ["--timings", str(timings), "--databricks-row", str(D1_ROW)]
    args += ["--run-id", "1", "--trigger", "push", "--out", str(out)]
    for step in pc.PATH_STEPS["b"]:
        args += ["--outcome", f"{step}=success"]
    assert pc.main(args) == 0
    (row,) = read_artifact(out)
    notes = notes_of(row)
    assert notes["job_queue_seconds"] == "12.5" and notes["job_setup_seconds"] == "40.0"


def test_row_cli_corrupt_json_fails(tmp_path, capsys):
    bad = tmp_path / "timings.json"
    bad.write_text("{not json", encoding="utf-8")
    args = ["row", "--path", "a", "--run-at", RUN_AT.isoformat(), "--git-sha", SHA]
    args += ["--date", "2026-11-20", "--hour", "8", "--hours", "1", "--timings", str(bad)]
    args += ["--run-id", "1", "--trigger", "push", "--out", str(tmp_path / "row.csv")]
    assert pc.main(args) == 1
    assert "timings.json" in capsys.readouterr().err
    assert not (tmp_path / "row.csv").exists()


# ---------------------------------------------------------------- summary CLI


def test_summary_cli_writes_markdown(tmp_path, capsys):
    out = tmp_path / "summary.md"
    assert pc.main(["summary", "--runs", str(RUNS), "--out", str(out)]) == 0
    text = out.read_text(encoding="utf-8")
    assert "path-a-runner" in text and "75.0%" in text
    assert capsys.readouterr().out.strip() == text.strip()


def test_summary_cli_bad_runs_exit_1(tmp_path, capsys):
    path = write_csv(tmp_path / "runs.csv", [_path_row("status=nope")])
    assert pc.main(["summary", "--runs", str(path)]) == 1
    assert "line 2" in capsys.readouterr().err


# ---------------------------------------------------------------- README generator


def test_readme_results_handles_path_rows_including_failed_ones(tmp_path):
    rows = readme_results.read_runs(RUNS)
    block = readme_results.render(rows)
    cells = {c.strip() for ln in block.splitlines() if ln.startswith("|") for c in ln.split("|")}
    assert {"path-a-runner", "path-b-databricks"} <= cells
    source = RUNS.read_text(encoding="utf-8")
    for token in re.findall(r"\d+\.\d+", block):
        assert token in source, token

    readme = tmp_path / "README.md"
    readme.write_text(
        f"# x\n\n{readme_results.START_MARKER}\n{readme_results.END_MARKER}\n", encoding="utf-8"
    )
    assert readme_results.main(["--runs", str(RUNS), "--readme", str(readme)]) == 0
    assert readme_results.main(["--runs", str(RUNS), "--readme", str(readme), "--check"]) == 0
