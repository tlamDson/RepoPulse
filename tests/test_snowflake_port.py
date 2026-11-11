"""benchmarks/snowflake_port.py: lines changed by the port + runs.csv rows from dbt run_results.

Pure core only: no git network, no dbt, no Snowflake. run_results.json samples are built here.
"""

from __future__ import annotations

import json
import re
import subprocess
import tomllib
from pathlib import Path

import pytest

from benchmarks import snowflake_port
from benchmarks.record_run import read_artifact
from repopulse.runs_log import COLUMNS, validate_row

ROOT = Path(__file__).resolve().parents[1]
MARTS = {
    "q1": "mart_daily_active_contributors",
    "q2": "mart_cohort_retention",
    "q3": "mart_contribution_funnel",
    "q4": "mart_responsiveness",
    "q5": "mart_repo_concentration",
    "q6": "mart_repo_health_score",
}
NUMSTAT = (
    "12\t3\ttransform/models/gold/marts/mart_contribution_funnel.sql\n"
    "-\t-\ttransform/seeds/blob.bin\n"
    "4\t4\ttransform/models/{old.sql => new.sql}\n"
    "\n"
    "20\t0\ttransform/macros/cross_db.sql\n"
)
SEED_CSV = (
    "event_id,event_type,created_at,event_date,actor_id\n"
    "1,ForkEvent,2026-01-07 08:00:00,2026-01-07,1\n"
    "2,ForkEvent,2026-01-05 09:00:00,2026-01-05,2\n"
    "3,PushEvent,2026-02-01 09:00:00,2026-02-01,3\n"
)


def run_results(*, target: str = "ci", elapsed: float = 12.5, statuses: dict | None = None):
    statuses = statuses or {}
    results = [
        {
            "unique_id": "seed.repopulse.fixture_silver_gharchive_events",
            "status": "success",
            "execution_time": 0.4,
        },
        {"unique_id": "model.repopulse.fct_events", "status": "success", "execution_time": 0.9},
        {
            "unique_id": "unit_test.repopulse.mart_cohort_retention.cohort_case",
            "status": "pass",
            "execution_time": 9.0,
        },
        {
            "unique_id": "test.repopulse.unique_dim_user_user_key",
            "status": "pass",
            "execution_time": 0.1,
        },
    ]
    for i, (_q, mart) in enumerate(sorted(MARTS.items()), start=1):
        uid = f"model.repopulse.{mart}"
        results.append(
            {"unique_id": uid, "status": statuses.get(uid, "success"), "execution_time": i / 2}
        )
    return {
        "metadata": {"dbt_version": "1.10.0"},
        "results": results,
        "elapsed_time": elapsed,
        "args": {"which": "build", "target": target},
    }


@pytest.fixture
def files(tmp_path):
    seed = tmp_path / "seed.csv"
    seed.write_text(SEED_CSV, encoding="utf-8")
    numstat = tmp_path / "numstat.txt"
    numstat.write_text(NUMSTAT, encoding="utf-8")

    def results(name: str = "run_results.json", **kw) -> Path:
        path = tmp_path / name
        path.write_text(json.dumps(run_results(**kw)), encoding="utf-8")
        return path

    return {"dir": tmp_path, "seed": seed, "numstat": numstat, "results": results}


def cli(files, engine: str, results: Path, *extra: str) -> int:
    return snowflake_port.main(
        [
            "--engine", engine,
            "--run-results", str(results),
            "--git-sha", "abc1234",
            "--numstat-file", str(files["numstat"]),
            "--seed", str(files["seed"]),
            "--out", str(files["dir"] / "out" / "rows.csv"),
            *extra,
        ]
    )  # fmt: skip


# ---------------------------------------------------------------- lines changed


def test_counts_added_plus_deleted_and_skips_binary_lines():
    assert snowflake_port.count_changed_lines(NUMSTAT) == (12 + 3) + (4 + 4) + 20


def test_empty_numstat_counts_zero():
    assert snowflake_port.count_changed_lines("") == 0


def test_malformed_numstat_line_is_an_error():
    with pytest.raises(snowflake_port.PortError):
        snowflake_port.count_changed_lines("twelve\t3\ttransform/x.sql\n")


def test_git_numstat_diffs_base_against_head_on_transform_only(monkeypatch):
    seen = {}

    def fake_run(cmd, **kw):
        seen["cmd"] = cmd
        return subprocess.CompletedProcess(cmd, 0, stdout="1\t2\ttransform/a.sql\n", stderr="")

    monkeypatch.setattr(snowflake_port.subprocess, "run", fake_run)
    assert snowflake_port.git_numstat("deadbeef") == "1\t2\ttransform/a.sql\n"
    cmd = seen["cmd"]
    assert cmd[:2] == ["git", "diff"] and "--numstat" in cmd
    assert cmd[-4:] == ["deadbeef", "HEAD", "--", "transform"]


def test_config_pins_the_full_base_sha_of_develop_before_the_port():
    with (ROOT / "benchmarks" / "snowflake_port.toml").open("rb") as fh:
        cfg = tomllib.load(fh)
    assert cfg["base_sha"] == "475c2bd05c52b89e4c3dc33c4d7d32e292cc629c"
    assert snowflake_port.load_config()["base_sha"] == cfg["base_sha"]


# ---------------------------------------------------------------- seed + row


def test_seed_stats_are_row_count_and_smallest_event_date(files):
    assert snowflake_port.seed_stats(files["seed"]) == (3, "2026-01-05")


def test_default_seed_is_the_ci_fixture():
    rows, first = snowflake_port.seed_stats(snowflake_port.SEED_CSV)
    assert rows == 36
    assert first == "2026-01-05"


@pytest.mark.parametrize(
    ("engine", "variant", "target"),
    [("duckdb", "dbt-build-duckdb", "ci"), ("snowflake", "dbt-build-snowflake", "snowflake")],
)
def test_build_row_maps_elapsed_and_mart_times(engine, variant, target):
    row = snowflake_port.build_row(
        run_results(target=target),
        engine=engine,
        git_sha="abc1234",
        run_at="2026-11-11T10:00:00+00:00",
        row_count=36,
        data_date="2026-01-05",
        lines_changed=42,
    )
    assert set(row) <= set(COLUMNS)
    assert row["variant"] == variant
    assert row["total_seconds"] == 12.5
    expected = {q: i / 2 for i, (q, _) in enumerate(sorted(MARTS.items()), start=1)}
    for q, seconds in expected.items():
        assert row[f"{q}_seconds"] == seconds, q  # the model, not its unit test
    assert row["row_count"] == 36
    assert row["data_date"] == "2026-01-05"
    notes = row["notes"]
    for part in (
        f"engine={engine}",
        "fixture=seed",
        "lines_changed=42",
        "q_seconds=mart build time",
    ):
        assert part in notes, notes
    assert "," not in notes and "\n" not in notes
    validate_row(row)


def test_build_failures_lists_error_fail_and_skipped_nodes():
    rr = run_results(
        statuses={
            "model.repopulse.mart_responsiveness": "error",
            "model.repopulse.mart_repo_health_score": "skipped",
        }
    )
    rr["results"].append({"unique_id": "test.repopulse.t", "status": "fail", "execution_time": 0})
    failed = snowflake_port.build_failures(rr)
    assert set(failed) == {
        "model.repopulse.mart_responsiveness",
        "model.repopulse.mart_repo_health_score",
        "test.repopulse.t",
    }
    assert snowflake_port.build_failures(run_results()) == []


def test_build_row_refuses_a_failed_build():
    rr = run_results(statuses={"model.repopulse.mart_responsiveness": "error"})
    with pytest.raises(snowflake_port.PortError, match="mart_responsiveness"):
        snowflake_port.build_row(
            rr, engine="duckdb", git_sha="abc1234", run_at="2026-11-11T10:00:00+00:00",
            row_count=36, data_date="2026-01-05", lines_changed=1,
        )  # fmt: skip


def test_build_row_refuses_run_results_of_another_target():
    with pytest.raises(snowflake_port.PortError, match="target"):
        snowflake_port.build_row(
            run_results(target="ci"), engine="snowflake", git_sha="abc1234",
            run_at="2026-11-11T10:00:00+00:00", row_count=36, data_date="2026-01-05",
            lines_changed=1,
        )  # fmt: skip


def test_build_row_refuses_run_results_without_every_mart():
    rr = run_results()
    rr["results"] = [r for r in rr["results"] if "mart_repo_concentration" not in r["unique_id"]]
    with pytest.raises(snowflake_port.PortError, match="mart_repo_concentration"):
        snowflake_port.build_row(
            rr, engine="duckdb", git_sha="abc1234", run_at="2026-11-11T10:00:00+00:00",
            row_count=36, data_date="2026-01-05", lines_changed=1,
        )  # fmt: skip


# ---------------------------------------------------------------- CLI


def test_cli_writes_an_artifact_record_run_can_read(files):
    assert cli(files, "duckdb", files["results"]()) == 0
    rows = read_artifact(files["dir"] / "out" / "rows.csv")
    assert len(rows) == 1
    row = rows[0]
    assert row["variant"] == "dbt-build-duckdb"
    assert row["git_sha"] == "abc1234"
    assert row["row_count"] == 3 and row["data_date"] == "2026-01-05"
    assert "lines_changed=43" in row["notes"]


def test_cli_appends_both_engines_to_one_file(files):
    assert cli(files, "duckdb", files["results"]("duck.json")) == 0
    sf = files["results"]("sf.json", target="snowflake", elapsed=30.0)
    assert cli(files, "snowflake", sf) == 0
    rows = read_artifact(files["dir"] / "out" / "rows.csv")
    assert [r["variant"] for r in rows] == ["dbt-build-duckdb", "dbt-build-snowflake"]
    assert [r["total_seconds"] for r in rows] == [12.5, 30.0]


def test_cli_failed_build_writes_no_row_and_exits_non_zero(files, capsys):
    bad = files["results"](statuses={"model.repopulse.mart_cohort_retention": "error"})
    assert cli(files, "duckdb", bad) != 0
    assert not (files["dir"] / "out" / "rows.csv").exists()
    assert "mart_cohort_retention" in capsys.readouterr().err


def test_cli_missing_run_results_exits_non_zero(files):
    assert cli(files, "snowflake", files["dir"] / "nope.json") != 0
    assert not (files["dir"] / "out" / "rows.csv").exists()


def test_cli_rejects_bad_git_sha_without_writing(files):
    code = snowflake_port.main(
        [
            "--engine", "duckdb",
            "--run-results", str(files["results"]()),
            "--git-sha", "NOT-A-SHA",
            "--numstat-file", str(files["numstat"]),
            "--seed", str(files["seed"]),
            "--out", str(files["dir"] / "out" / "rows.csv"),
        ]
    )  # fmt: skip
    assert code != 0
    assert not (files["dir"] / "out" / "rows.csv").exists()


def test_cli_summary_names_engine_and_says_ci_fixture(files):
    summary = files["dir"] / "out" / "summary.md"
    assert cli(files, "duckdb", files["results"](), "--summary", str(summary)) == 0
    bad = files["results"]("bad.json", target="snowflake", statuses={
        "model.repopulse.mart_responsiveness": "error"})  # fmt: skip
    assert cli(files, "snowflake", bad, "--summary", str(summary)) != 0
    text = summary.read_text(encoding="utf-8")
    assert "dbt-build-duckdb" in text
    assert re.search(r"CI fixture", text, re.I)
    assert "snowflake" in text and "mart_responsiveness" in text
    assert len(read_artifact(files["dir"] / "out" / "rows.csv")) == 1


def test_cli_out_with_foreign_header_is_an_error(files):
    out = files["dir"] / "out" / "rows.csv"
    out.parent.mkdir()
    out.write_text("a,b\n", encoding="utf-8")
    assert cli(files, "duckdb", files["results"]()) != 0
    assert out.read_text(encoding="utf-8") == "a,b\n"


def test_queries_map_to_marts_and_engines_to_variants():
    assert snowflake_port.MART_BY_QUERY == MARTS
    assert snowflake_port.VARIANTS == {
        "duckdb": "dbt-build-duckdb",
        "snowflake": "dbt-build-snowflake",
    }
