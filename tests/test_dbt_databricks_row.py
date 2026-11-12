"""benchmarks/dbt_databricks.py: the runs.csv row of a ``dbt build --target databricks``.

Pure core only: no dbt, no Databricks, no git network. run_results.json and ``dbt show`` logs are
built here (the show log lines have the shape dbt 1.12 writes with ``--log-format json``).
"""

from __future__ import annotations

import json
import tomllib
from pathlib import Path

import pytest

from benchmarks import dbt_databricks
from benchmarks.record_run import read_artifact
from repopulse.runs_log import COLUMNS, validate_row
from repopulse.spark.tables import table_names

ROOT = Path(__file__).resolve().parents[1]
MARTS = {
    "q1": "mart_daily_active_contributors",
    "q2": "mart_cohort_retention",
    "q3": "mart_contribution_funnel",
    "q4": "mart_responsiveness",
    "q5": "mart_repo_concentration",
    "q6": "mart_repo_health_score",
}
SILVER = table_names("gharchive", schema="repopulse", catalog="workspace")["silver"]
NUMSTAT = "10\t2\ttransform/macros/cross_db.sql\n-\t-\ttransform/seeds/blob.bin\n3\t3\tx.yml\n"
STATS = {"row_count": 1234567, "min_event_date": "2026-10-25", "max_event_date": "2026-10-26"}


def show_log(rows: list[dict], *, noise: bool = True) -> str:
    """Lines of ``dbt show --output json --log-format json`` (one JSON event per line)."""
    preview = json.dumps(rows)
    lines = []
    if noise:
        lines += [
            json.dumps({"data": {"log_version": 3}, "info": {"name": "MainReportVersion"}}),
            "not json at all",
            json.dumps({"data": {"node_count": 1}, "info": {"name": "ConcurrencyLine"}}),
        ]
    lines.append(
        json.dumps(
            {
                "data": {
                    "is_inline": True,
                    "node_name": "inline_query",
                    "output_format": "json",
                    "preview": preview,
                    "quiet": False,
                    "unique_id": "sql_operation.repopulse.inline_query",
                },
                "info": {"code": "Q041", "level": "info", "name": "ShowNode"},
            }
        )
    )
    return "\n".join(lines) + "\n"


def run_results(*, target="databricks", which="build", elapsed=95.5, statuses=None):
    statuses = statuses or {}
    results = [
        {"unique_id": "seed.repopulse.repos_data_ecosystem", "status": "success"},
        {"unique_id": "model.repopulse.stg_gharchive_events", "status": "success"},
        {"unique_id": "unit_test.repopulse.mart_responsiveness.x", "status": "pass"},
        {"unique_id": "test.repopulse.unique_fct_events_event_id", "status": "pass"},
    ]
    for r in results:
        r["execution_time"] = 0.3
    for i, (_q, mart) in enumerate(sorted(MARTS.items()), start=1):
        uid = f"model.repopulse.{mart}"
        results.append(
            {"unique_id": uid, "status": statuses.get(uid, "success"), "execution_time": i}
        )
    return {"results": results, "elapsed_time": elapsed, "args": {"which": which, "target": target}}


@pytest.fixture
def files(tmp_path):
    numstat = tmp_path / "numstat.txt"
    numstat.write_text(NUMSTAT, encoding="utf-8")
    show = tmp_path / "show.jsonl"
    show.write_text(show_log([STATS]), encoding="utf-8")

    def results(name="run_results.json", **kw) -> Path:
        path = tmp_path / name
        path.write_text(json.dumps(run_results(**kw)), encoding="utf-8")
        return path

    return {"dir": tmp_path, "numstat": numstat, "show": show, "results": results}


def cli(files, results: Path, *extra: str) -> int:
    return dbt_databricks.main(
        [
            "--run-results", str(results),
            "--show-log", str(files["show"]),
            "--git-sha", "abc1234",
            "--numstat-file", str(files["numstat"]),
            "--out", str(files["dir"] / "out" / "rows.csv"),
            *extra,
        ]
    )  # fmt: skip


# ---------------------------------------------------------------- dbt show log


def test_parse_show_log_reads_the_shownode_preview():
    assert dbt_databricks.parse_show_log(show_log([STATS])) == [STATS]


def test_parse_show_log_also_reads_plain_json_output():
    text = "Running with dbt\n" + json.dumps({"show": [STATS]}, indent=2) + "\n"
    assert dbt_databricks.parse_show_log(text) == [STATS]


def test_parse_show_log_without_a_preview_is_an_error():
    with pytest.raises(dbt_databricks.RowError, match="dbt show"):
        dbt_databricks.parse_show_log(show_log([STATS]).splitlines()[0] + "\n")


def test_input_stats_are_rows_and_event_date_range():
    assert dbt_databricks.input_stats([STATS]) == (1234567, "2026-10-25", "2026-10-26")


def test_input_stats_accept_upper_case_keys_and_numeric_strings():
    row = {"ROW_COUNT": "42", "MIN_EVENT_DATE": "2026-10-25", "MAX_EVENT_DATE": "2026-10-25"}
    assert dbt_databricks.input_stats([row]) == (42, "2026-10-25", "2026-10-25")


@pytest.mark.parametrize(
    "rows",
    [
        [],
        [{"row_count": 0, "min_event_date": None, "max_event_date": None}],
        [{"row_count": 3, "min_event_date": "25/10/2026", "max_event_date": "2026-10-25"}],
        [{"row_count": 3, "max_event_date": "2026-10-25"}],
        [STATS, STATS],
    ],
)
def test_input_stats_refuse_empty_or_malformed_input(rows):
    with pytest.raises(dbt_databricks.RowError):
        dbt_databricks.input_stats(rows)


# ---------------------------------------------------------------- row


def build(rr, **kw):
    args = {
        "git_sha": "abc1234",
        "run_at": "2026-11-12T10:00:00+00:00",
        "row_count": 1234567,
        "data_date": "2026-10-25",
        "max_event_date": "2026-10-26",
        "source": SILVER,
        "lines_changed": 7,
        "base": "629d72ec309b1afa28703d30935f7c8a714c569d",
    }
    args.update(kw)
    return dbt_databricks.build_row(rr, **args)


def test_build_row_maps_elapsed_mart_times_and_real_input():
    row = build(run_results())
    assert set(row) <= set(COLUMNS)
    assert row["variant"] == "dbt-build-databricks"
    assert row["total_seconds"] == 95.5
    for i, (q, _mart) in enumerate(sorted(MARTS.items()), start=1):
        assert row[f"{q}_seconds"] == i, q
    assert row["row_count"] == 1234567
    assert row["data_date"] == "2026-10-25"
    notes = row["notes"]
    for part in (
        "engine=databricks",
        f"source={SILVER}",
        "input=gharchive-silver-delta",
        "max_event_date=2026-10-26",
        "lines_changed=7",
        "q_seconds=mart build time",
        "base=629d72ec309b",
    ):
        assert part in notes, notes
    assert "fixture" not in notes
    assert "," not in notes and "\n" not in notes
    validate_row(row)


@pytest.mark.parametrize(
    ("kw", "match"),
    [
        ({"target": "ci"}, "target"),
        ({"target": "snowflake"}, "target"),
        ({"which": "run"}, "build"),
    ],
)
def test_build_row_refuses_run_results_of_another_target_or_command(kw, match):
    with pytest.raises(dbt_databricks.RowError, match=match):
        build(run_results(**kw))


def test_build_row_refuses_a_failed_build():
    rr = run_results(statuses={"model.repopulse.mart_responsiveness": "error"})
    rr["results"].append({"unique_id": "test.repopulse.t", "status": "fail", "execution_time": 0})
    with pytest.raises(dbt_databricks.RowError, match="mart_responsiveness"):
        build(rr)


def test_build_row_refuses_run_results_without_every_mart():
    rr = run_results()
    rr["results"] = [r for r in rr["results"] if "mart_repo_concentration" not in r["unique_id"]]
    with pytest.raises(dbt_databricks.RowError, match="mart_repo_concentration"):
        build(rr)


def test_defaults_name_the_d1_silver_table_and_the_variant():
    assert dbt_databricks.VARIANT == "dbt-build-databricks"
    assert dbt_databricks.DEFAULT_SOURCE == SILVER
    assert dbt_databricks.MART_BY_QUERY == MARTS


def test_config_pins_the_full_base_sha_of_develop_before_d2():
    with (ROOT / "benchmarks" / "dbt_databricks.toml").open("rb") as fh:
        cfg = tomllib.load(fh)
    assert cfg["base_sha"] == "629d72ec309b1afa28703d30935f7c8a714c569d"
    assert dbt_databricks.load_config()["base_sha"] == cfg["base_sha"]


# ---------------------------------------------------------------- CLI


def test_cli_writes_an_artifact_record_run_can_read(files):
    assert cli(files, files["results"]()) == 0
    rows = read_artifact(files["dir"] / "out" / "rows.csv")
    assert len(rows) == 1
    row = rows[0]
    assert row["variant"] == "dbt-build-databricks"
    assert row["git_sha"] == "abc1234"
    assert row["row_count"] == 1234567 and row["data_date"] == "2026-10-25"
    assert "lines_changed=18" in row["notes"]  # (10 + 2) + (3 + 3), binary line skipped


def test_cli_failed_build_names_the_nodes_even_without_a_show_log(files, capsys):
    summary = files["dir"] / "out" / "summary.md"
    files["show"].unlink()
    bad = files["results"](statuses={"model.repopulse.mart_cohort_retention": "error"})
    assert cli(files, bad, "--summary", str(summary)) != 0
    assert not (files["dir"] / "out" / "rows.csv").exists()
    assert "mart_cohort_retention" in capsys.readouterr().err
    assert "mart_cohort_retention" in summary.read_text(encoding="utf-8")


def test_cli_missing_run_results_or_show_log_exits_non_zero(files):
    assert cli(files, files["dir"] / "nope.json") != 0
    files["show"].unlink()
    assert cli(files, files["results"]()) != 0
    assert not (files["dir"] / "out" / "rows.csv").exists()


def test_cli_rejects_bad_git_sha_without_writing(files):
    code = dbt_databricks.main(
        [
            "--run-results", str(files["results"]()),
            "--show-log", str(files["show"]),
            "--git-sha", "NOT-A-SHA",
            "--numstat-file", str(files["numstat"]),
            "--out", str(files["dir"] / "out" / "rows.csv"),
        ]
    )  # fmt: skip
    assert code != 0
    assert not (files["dir"] / "out" / "rows.csv").exists()


def test_cli_summary_says_real_gh_archive_data_and_shows_the_row(files):
    summary = files["dir"] / "out" / "summary.md"
    assert cli(files, files["results"](), "--summary", str(summary)) == 0
    text = summary.read_text(encoding="utf-8")
    assert "dbt-build-databricks" in text
    assert "GH Archive" in text and "not simulated" in text
    assert SILVER in text
    assert "CI fixture" not in text
