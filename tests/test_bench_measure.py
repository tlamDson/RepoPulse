"""Parser of benchmark results: N timed runs -> one median row valid for ``runs.csv``."""

import pytest

from benchmarks.measure import MeasureError, summarize_runs
from repopulse.runs_log import COLUMNS, validate_row

SECONDS = ("total_seconds", *(f"q{i}_seconds" for i in range(1, 7)))
META = {
    "variant": "baseline",
    "git_sha": "abc1234",
    "data_date": "2026-10-25",
    "run_at": "2026-11-11T08:00:00+00:00",
}


def _run(base: float, rss: float | None = 100.0, rows: int = 31, nbytes: int = 4096) -> dict:
    run = {col: base + i for i, col in enumerate(SECONDS)}
    run.update(peak_rss_mb=rss, row_count=rows, bytes_read=nbytes)
    return run


def test_odd_number_of_runs_takes_the_middle_value():
    # total_seconds of the three runs are 3.0, 1.0, 2.0 -> sorted 1, 2, 3 -> median 2.0;
    # q1 = base + 1 -> 4, 2, 3 -> median 3.0; q6 = base + 6 -> 9, 7, 8 -> median 8.0
    runs = [_run(3.0, rss=300.0), _run(1.0, rss=100.0), _run(2.0, rss=250.0)]
    row = summarize_runs(runs, **META)
    assert row["total_seconds"] == pytest.approx(2.0)
    assert row["q1_seconds"] == pytest.approx(3.0)
    assert row["q6_seconds"] == pytest.approx(8.0)
    assert row["peak_rss_mb"] == pytest.approx(250.0)  # 100, 250, 300 -> 250
    assert row["notes"] == "runs=3 median"


def test_even_number_of_runs_averages_the_two_middle_values():
    # total_seconds 1, 2, 3, 4 -> (2 + 3) / 2 = 2.5; q3 = base + 3 -> 4..7 -> (5 + 6) / 2 = 5.5
    runs = [_run(4.0), _run(1.0), _run(3.0), _run(2.0)]
    row = summarize_runs(runs, **META)
    assert row["total_seconds"] == pytest.approx(2.5)
    assert row["q3_seconds"] == pytest.approx(5.5)
    assert row["notes"] == "runs=4 median"


def test_row_passes_runs_log_schema_and_keeps_metadata():
    row = summarize_runs([_run(1.0), _run(2.0), _run(3.0), _run(4.0), _run(5.0)], **META)
    assert set(row) == set(COLUMNS)
    checked = validate_row(row)
    assert checked["variant"] == "baseline"
    assert checked["git_sha"] == "abc1234"
    assert checked["data_date"] == "2026-10-25"
    assert checked["run_at"] == "2026-11-11T08:00:00+00:00"
    assert checked["row_count"] == "31"
    assert checked["bytes_read"] == "4096"
    assert checked["notes"] == "runs=5 median"


def test_missing_peak_rss_leaves_column_empty():
    # resource.getrusage does not exist on Windows: every run reports None
    row = summarize_runs([_run(1.0, rss=None), _run(2.0, rss=None)], **META)
    assert row["peak_rss_mb"] is None
    assert validate_row(row)["peak_rss_mb"] == ""


def test_row_count_that_differs_between_runs_is_an_error():
    with pytest.raises(MeasureError, match="row_count"):
        summarize_runs([_run(1.0, rows=31), _run(2.0, rows=30)], **META)


def test_bytes_read_that_differs_between_runs_is_an_error():
    with pytest.raises(MeasureError, match="bytes_read"):
        summarize_runs([_run(1.0, nbytes=10), _run(2.0, nbytes=11)], **META)


def test_no_runs_is_an_error():
    with pytest.raises(MeasureError):
        summarize_runs([], **META)
