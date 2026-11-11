"""Merge the benchmark artifact (CSV header + rows) into ``runs.csv`` through ``append_run``."""

import csv
from pathlib import Path

import pytest

from benchmarks.record_run import main, record
from repopulse.runs_log import COLUMNS, RunsLogError

HEADER = ",".join(COLUMNS)
GOOD = (
    "2026-11-11T08:00:00+00:00,abc1234,baseline,2026-10-25,"
    "12.5,1.5,2.5,3.5,1.25,0.75,0.5,2048.0,330000000,2000000,runs=5 median"
)


def _artifact(tmp_path: Path, *lines: str, header: str = HEADER) -> Path:
    path = tmp_path / "benchmark-row.csv"
    path.write_text("\n".join([header, *lines]) + "\n", encoding="utf-8")
    return path


def _read(path: Path) -> list[list[str]]:
    with path.open(encoding="utf-8", newline="") as fh:
        return list(csv.reader(fh))


def test_valid_artifact_appends_exactly_one_row(tmp_path):
    runs = tmp_path / "runs.csv"
    assert record(_artifact(tmp_path, GOOD), runs) == 1
    rows = _read(runs)
    assert rows[0] == list(COLUMNS)
    assert len(rows) == 2
    row = dict(zip(COLUMNS, rows[1], strict=True))
    assert row["variant"] == "baseline"
    assert row["total_seconds"] == "12.5"
    assert row["bytes_read"] == "330000000"
    assert row["row_count"] == "2000000"


def test_empty_optional_columns_are_kept_empty(tmp_path):
    runs = tmp_path / "runs.csv"
    line = "2026-11-11T08:00:00+00:00,abc1234,baseline,2026-10-25,12.5,,,,,,,,,,"
    record(_artifact(tmp_path, line), runs)
    row = dict(zip(COLUMNS, _read(runs)[1], strict=True))
    assert row["peak_rss_mb"] == ""
    assert row["row_count"] == ""


def test_appends_after_existing_rows(tmp_path):
    runs = tmp_path / "runs.csv"
    record(_artifact(tmp_path, GOOD), runs)
    record(_artifact(tmp_path, GOOD.replace("baseline", "incremental")), runs)
    rows = _read(runs)
    assert [r[2] for r in rows[1:]] == ["baseline", "incremental"]


@pytest.mark.parametrize(
    "bad",
    [
        GOOD.replace("baseline", "Bad Variant"),
        GOOD.replace(",12.5,", ",not-a-number,"),
        GOOD.replace(",330000000,", ",3.5,"),
        GOOD.replace("abc1234", "XYZ"),
    ],
)
def test_invalid_row_is_rejected_and_runs_csv_is_unchanged(tmp_path, bad):
    runs = tmp_path / "runs.csv"
    record(_artifact(tmp_path, GOOD), runs)
    before = runs.read_bytes()
    with pytest.raises(RunsLogError):
        record(_artifact(tmp_path, GOOD, bad), runs)  # second row bad -> nothing appended
    assert runs.read_bytes() == before


def test_artifact_with_wrong_header_is_rejected(tmp_path):
    runs = tmp_path / "runs.csv"
    with pytest.raises(RunsLogError, match="header"):
        record(_artifact(tmp_path, GOOD, header="a,b,c"), runs)
    assert not runs.exists()


def test_cli_returns_zero_and_reports_rows(tmp_path, capsys):
    runs = tmp_path / "runs.csv"
    code = main([str(_artifact(tmp_path, GOOD)), "--runs-csv", str(runs)])
    assert code == 0
    assert "1 row" in capsys.readouterr().out
    assert len(_read(runs)) == 2


def test_recording_the_same_artifact_twice_adds_nothing(tmp_path):
    runs = tmp_path / "runs.csv"
    artifact = _artifact(tmp_path, GOOD)
    assert record(artifact, runs) == 1
    before = runs.read_bytes()
    assert record(artifact, runs) == 0  # identical in every column -> skipped
    assert runs.read_bytes() == before
    assert len(_read(runs)) == 2


def test_row_that_differs_in_one_column_is_still_appended(tmp_path):
    runs = tmp_path / "runs.csv"
    record(_artifact(tmp_path, GOOD), runs)
    assert record(_artifact(tmp_path, GOOD.replace(",12.5,", ",12.6,")), runs) == 1
    assert [r[4] for r in _read(runs)[1:]] == ["12.5", "12.6"]


def test_cli_rerun_reports_skipped_duplicate_and_exits_zero(tmp_path, capsys):
    runs = tmp_path / "runs.csv"
    artifact = str(_artifact(tmp_path, GOOD))
    assert main([artifact, "--runs-csv", str(runs)]) == 0
    capsys.readouterr()
    assert main([artifact, "--runs-csv", str(runs)]) == 0
    out = capsys.readouterr().out
    assert "appended 0 row(s)" in out
    assert "skipped 1 duplicate row(s)" in out
    assert len(_read(runs)) == 2


def test_cli_records_several_artifacts_in_one_call(tmp_path, capsys):
    runs = tmp_path / "runs.csv"
    first = tmp_path / "a"
    second = tmp_path / "b"
    first.mkdir()
    second.mkdir()
    a = _artifact(first, GOOD)
    b = _artifact(second, GOOD.replace("baseline", "pipeline-incremental"))
    assert main([str(a), str(b), "--runs-csv", str(runs)]) == 0
    assert [r[2] for r in _read(runs)[1:]] == ["baseline", "pipeline-incremental"]
    assert "appended 2 row(s)" in capsys.readouterr().out


def test_cli_with_one_bad_artifact_appends_nothing(tmp_path, capsys):
    runs = tmp_path / "runs.csv"
    first = tmp_path / "a"
    second = tmp_path / "b"
    first.mkdir()
    second.mkdir()
    a = _artifact(first, GOOD)
    b = _artifact(second, GOOD.replace("baseline", "BAD"))
    assert main([str(a), str(b), "--runs-csv", str(runs)]) != 0
    assert not runs.exists()
    assert "variant" in capsys.readouterr().err


def test_cli_returns_nonzero_on_invalid_row(tmp_path, capsys):
    runs = tmp_path / "runs.csv"
    code = main(
        [str(_artifact(tmp_path, GOOD.replace("baseline", "BAD"))), "--runs-csv", str(runs)]
    )
    assert code != 0
    assert "variant" in capsys.readouterr().err
    assert not runs.exists()
