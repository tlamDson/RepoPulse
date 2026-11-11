"""``repopulse.pipeline``: helpers of the scheduled pipeline workflow (no network, no Spark).

- ``window``: last (date, hour) and hour count of the ingest summary, for the Spark job.
- ``build_row``: one ``runs.csv`` row per scheduled run (schema of ``repopulse.runs_log``).
- ``timed``: run one pipeline step as a subprocess and record its wall time.
- ``table_counts``: row counts of staging + gold (incremental re-run must not change them).
- ``reset_smoke_database``: recreate the MotherDuck smoke database, never the real one.
"""

from __future__ import annotations

import csv
import json
import sys
from datetime import UTC, datetime
from pathlib import Path

import duckdb
import pytest

from benchmarks import record_run
from repopulse import pipeline
from repopulse.runs_log import COLUMNS, validate_row

RUN_AT = datetime(2026, 1, 5, 3, 41, 7, tzinfo=UTC)


def rec(day: str, hour: int, status: str = "ingested", kept: int = 10) -> dict:
    return {
        "data_date": day,
        "hour": hour,
        "status": status,
        "source_name": f"{day}-{hour}.json.gz",
        "events_total": kept,
        "events_kept": kept,
        "events_malformed": 0,
        "partition": None,
    }


INGEST = [
    rec("2026-01-04", 23),
    rec("2026-01-05", 0, "skipped"),
    rec("2026-01-05", 1, "missing", 0),
]
SPARK = {
    "source": "gharchive",
    "hours": [["2026-01-04", 23], ["2026-01-05", 0], ["2026-01-05", 1]],
    "bronze_rows": 20,
    "silver_inserted": 17,
    "duplicates_dropped": 1,
    "quarantined": {"schema_invalid": 2},
    "quarantined_total": 2,
    "late_accepted": 0,
}
TIMINGS = {"ingest": 12.5, "spark": 40.25, "export": 5.0, "dbt": 30.0}


def notes_of(row: dict) -> dict[str, str]:
    return dict(item.split("=", 1) for item in row["notes"].split(" "))


def build(**overrides) -> dict:
    kwargs = {
        "run_at": RUN_AT,
        "git_sha": "abc1234",
        "ingest_records": INGEST,
        "spark_summary": SPARK,
        "timings": TIMINGS,
        "run_id": "123456",
        "trigger": "schedule",
    }
    kwargs.update(overrides)
    return pipeline.build_row(**kwargs)


# ---------------------------------------------------------------- window


def test_window_is_last_hour_and_count():
    assert pipeline.window(INGEST) == ("2026-01-05", 1, 3)


def test_window_order_of_records_does_not_matter():
    assert pipeline.window(list(reversed(INGEST))) == ("2026-01-05", 1, 3)


@pytest.mark.parametrize(
    "records, match",
    [
        ([], "empty"),
        ([rec("2026-01-05", 24)], "hour"),
        ([rec("2026-02-30", 1)], "date"),
        ([{"hour": 1}], "data_date"),
        ([rec("2026-01-05", 1), rec("2026-01-05", 3)], "consecutive"),
        ([rec("2026-01-05", 1), rec("2026-01-05", 1)], "consecutive"),
    ],
)
def test_window_rejects_bad_records(records, match):
    with pytest.raises(pipeline.PipelineError, match=match):
        pipeline.window(records)


# ---------------------------------------------------------------- build_row


def test_row_is_valid_for_runs_csv(tmp_path):
    row = build()
    assert set(row) == set(COLUMNS)
    # validate_row takes typed values (it rejects numeric strings) and returns CSV strings, so a
    # row of CSV strings is checked through the artifact round trip the workflow uses.
    artifact = tmp_path / "pipeline-row.csv"
    pipeline.write_row(artifact, row)
    (parsed,) = record_run.read_artifact(artifact)
    assert validate_row(parsed) == row


def test_row_fields():
    row = build()
    assert row["run_at"] == "2026-01-05T03:41:07+00:00"
    assert row["git_sha"] == "abc1234"
    assert row["variant"] == pipeline.VARIANT == "pipeline-incremental"
    assert row["data_date"] == "2026-01-05"  # date of the last data hour, not of the first
    assert float(row["total_seconds"]) == pytest.approx(87.75)
    assert row["row_count"] == "17"  # rows the Spark job inserted into this run's silver
    for q in range(1, 7):
        assert row[f"q{q}_seconds"] == ""
    assert row["peak_rss_mb"] == ""
    assert row["bytes_read"] == ""


def test_row_notes_are_one_line_of_key_values():
    row = build(extra_notes={"target": "motherduck", "full_refresh": "false"})
    assert "\n" not in row["notes"] and "\r" not in row["notes"]
    notes = notes_of(row)
    assert notes["last_hour"] == "2026-01-05T01"
    assert notes["hours"] == "3"
    assert notes["ingested"] == "1"
    assert notes["skipped"] == "1"
    assert notes["missing"] == "1"
    assert notes["ingest_s"] == "12.5"
    assert notes["spark_s"] == "40.25"
    assert notes["export_s"] == "5.0"
    assert notes["dbt_s"] == "30.0"
    assert notes["run_id"] == "123456"
    assert notes["trigger"] == "schedule"
    assert notes["target"] == "motherduck"
    assert notes["full_refresh"] == "false"


def test_variant_can_be_overridden_but_must_match_runs_csv_rule():
    assert build(variant="pipeline-pr-smoke")["variant"] == "pipeline-pr-smoke"
    with pytest.raises(ValueError, match="variant"):
        build(variant="Pipeline PR")


def test_naive_run_at_is_rejected():
    with pytest.raises(ValueError, match="timezone"):
        build(run_at=datetime(2026, 1, 5, 3, 41))


@pytest.mark.parametrize("missing", ["ingest", "spark", "dbt"])
def test_missing_step_timing_is_rejected(missing):
    timings = {k: v for k, v in TIMINGS.items() if k != missing}
    with pytest.raises(pipeline.PipelineError, match=missing):
        build(timings=timings)


@pytest.mark.parametrize("bad", [-1.0, float("nan"), "12", True])
def test_bad_step_timing_is_rejected(bad):
    with pytest.raises(pipeline.PipelineError, match="dbt"):
        build(timings={**TIMINGS, "dbt": bad})


@pytest.mark.parametrize("bad", [{}, {"silver_inserted": -1}, {"silver_inserted": "17"}])
def test_bad_spark_summary_is_rejected(bad):
    with pytest.raises(pipeline.PipelineError, match="silver_inserted"):
        build(spark_summary=bad)


@pytest.mark.parametrize(
    "notes",
    [{"target": "two words"}, {"target": "a\nb"}, {"bad key": "x"}, {"k": ""}, {"k": "a=b"}],
)
def test_note_values_must_be_single_tokens(notes):
    with pytest.raises(pipeline.PipelineError, match="note"):
        build(extra_notes=notes)


@pytest.mark.parametrize(
    "field, value", [("run_id", "12 3"), ("trigger", "cron\nx"), ("run_id", "")]
)
def test_run_id_and_trigger_must_be_single_tokens(field, value):
    with pytest.raises(pipeline.PipelineError, match=field):
        build(**{field: value})


# ---------------------------------------------------------------- CLI: window + row


def _write_inputs(tmp_path: Path) -> dict[str, Path]:
    paths = {
        "ingest": tmp_path / "out" / "ingest-summary.json",
        "spark": tmp_path / "out" / "spark-summary.json",
        "timings": tmp_path / "out" / "timings.json",
    }
    paths["ingest"].parent.mkdir(parents=True)
    paths["ingest"].write_text(json.dumps(INGEST), encoding="utf-8")
    paths["spark"].write_text(json.dumps(SPARK), encoding="utf-8")
    paths["timings"].write_text(json.dumps(TIMINGS), encoding="utf-8")
    return paths


def _row_argv(paths: dict[str, Path], out: Path) -> list[str]:
    return [
        "row",
        "--run-at",
        "2026-01-05T03:41:07+00:00",
        "--git-sha",
        "abc1234",
        "--ingest-summary",
        str(paths["ingest"]),
        "--spark-summary",
        str(paths["spark"]),
        "--timings",
        str(paths["timings"]),
        "--run-id",
        "123456",
        "--trigger",
        "schedule",
        "--note",
        "target=motherduck",
        "--out",
        str(out),
    ]


def test_cli_window_prints_github_env_lines(tmp_path, capsys):
    paths = _write_inputs(tmp_path)
    assert pipeline.main(["window", "--ingest-summary", str(paths["ingest"])]) == 0
    assert capsys.readouterr().out.splitlines() == [
        "DATA_DATE=2026-01-05",
        "DATA_HOUR=1",
        "DATA_HOURS=3",
    ]


def test_cli_row_writes_artifact_that_record_run_reads(tmp_path):
    paths = _write_inputs(tmp_path)
    out = tmp_path / "out" / "pipeline-row.csv"
    assert pipeline.main(_row_argv(paths, out)) == 0
    with out.open(encoding="utf-8", newline="") as fh:
        lines = list(csv.reader(fh))
    assert lines[0] == list(COLUMNS)
    assert len(lines) == 2
    rows = record_run.read_artifact(out)
    assert len(rows) == 1
    assert rows[0]["variant"] == "pipeline-incremental"
    assert rows[0]["row_count"] == 17
    assert notes_of({"notes": rows[0]["notes"]})["target"] == "motherduck"
    runs = tmp_path / "runs.csv"
    assert record_run.record(out, runs) == 1


def test_cli_row_missing_summary_fails_clearly(tmp_path, capsys):
    paths = _write_inputs(tmp_path)
    paths["spark"].unlink()
    out = tmp_path / "out" / "pipeline-row.csv"
    assert pipeline.main(_row_argv(paths, out)) == 1
    err = capsys.readouterr().err
    assert "spark-summary.json" in err
    assert not out.exists()


def test_cli_row_bad_hour_fails_clearly(tmp_path, capsys):
    paths = _write_inputs(tmp_path)
    paths["ingest"].write_text(json.dumps([rec("2026-01-05", 24)]), encoding="utf-8")
    out = tmp_path / "out" / "pipeline-row.csv"
    assert pipeline.main(_row_argv(paths, out)) == 1
    assert "hour" in capsys.readouterr().err
    assert not out.exists()


def test_cli_row_invalid_json_fails_clearly(tmp_path, capsys):
    paths = _write_inputs(tmp_path)
    paths["timings"].write_text("{not json", encoding="utf-8")
    out = tmp_path / "out" / "pipeline-row.csv"
    assert pipeline.main(_row_argv(paths, out)) == 1
    assert "timings.json" in capsys.readouterr().err


# ---------------------------------------------------------------- timed


def test_timed_records_seconds_and_returns_child_exit_code(tmp_path):
    timings = tmp_path / "out" / "timings.json"
    code = pipeline.main(
        ["timed", "--name", "ingest", "--timings", str(timings), "--", sys.executable, "-c", "0"]
    )
    assert code == 0
    code = pipeline.main(
        [
            "timed",
            "--name",
            "spark",
            "--timings",
            str(timings),
            "--",
            sys.executable,
            "-c",
            "import sys; sys.exit(3)",
        ]
    )
    assert code == 3
    data = json.loads(timings.read_text(encoding="utf-8"))
    assert set(data) == {"ingest", "spark"}
    assert all(isinstance(v, float) and v >= 0 for v in data.values())


def test_timed_needs_a_command(tmp_path):
    with pytest.raises(SystemExit) as exc:
        pipeline.main(["timed", "--name", "x", "--timings", str(tmp_path / "t.json"), "--"])
    assert exc.value.code == 2


# ---------------------------------------------------------------- counts


def test_table_counts_of_staging_and_gold(tmp_path):
    db = tmp_path / "gold.duckdb"
    con = duckdb.connect(str(db))
    try:
        for i, table in enumerate(pipeline.COUNT_TABLES):
            schema = table.split(".")[0]
            con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
            con.execute(f"CREATE TABLE {table} AS SELECT * FROM range({i + 1})")
    finally:
        con.close()
    out = tmp_path / "counts.json"
    assert pipeline.main(["counts", "--database", str(db), "--out", str(out)]) == 0
    counts = json.loads(out.read_text(encoding="utf-8"))
    assert counts == {t: i + 1 for i, t in enumerate(pipeline.COUNT_TABLES)}
    assert "main_staging.stg_gharchive_events" in counts
    assert "main_gold.fct_events" in counts
    assert "main_gold.mart_repo_health_score" in counts


def test_table_counts_missing_table_fails_clearly(tmp_path, capsys):
    db = tmp_path / "empty.duckdb"
    duckdb.connect(str(db)).close()
    code = pipeline.main(["counts", "--database", str(db), "--out", str(tmp_path / "c.json")])
    assert code == 1
    assert "stg_gharchive_events" in capsys.readouterr().err


# ---------------------------------------------------------------- MotherDuck smoke database


class FakeConnection:
    def __init__(self, log):
        self.log = log

    def execute(self, sql):
        self.log.append(sql)
        return self

    def close(self):
        self.log.append("close")


def test_reset_smoke_database_drops_and_recreates(tmp_path):
    log: list[str] = []
    targets: list[str] = []

    def connect(target):
        targets.append(target)
        return FakeConnection(log)

    pipeline.reset_smoke_database("repopulse_smoke", connect=connect)
    assert targets == ["md:"]
    assert log == [
        "DROP DATABASE IF EXISTS repopulse_smoke",
        "CREATE DATABASE repopulse_smoke",
        "close",
    ]


@pytest.mark.parametrize("name", ["repopulse", "REPOPULSE", "prod", "smoke; DROP DATABASE x", ""])
def test_reset_refuses_anything_but_a_smoke_database(name):
    def connect(target):  # pragma: no cover - must not be called
        raise AssertionError("must not connect")

    with pytest.raises(pipeline.PipelineError):
        pipeline.reset_smoke_database(name, connect=connect)


def test_cli_md_reset_refuses_production_database(capsys):
    assert pipeline.main(["md-reset", "--database", "repopulse"]) == 1
    assert "repopulse" in capsys.readouterr().err
