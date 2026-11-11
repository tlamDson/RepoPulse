"""PySpark side of the scale benchmark: Q1-Q6 in Spark SQL must match the DuckDB baseline.

Same hand-written fixture as ``tests/test_naive_duckdb.py`` (34 lines incl. one duplicate),
gzip-compressed into ``tmp_path``. Runs with the session-scoped ``spark`` fixture (local[1]).
"""

from __future__ import annotations

import gzip
import shutil
from pathlib import Path

import pytest

from benchmarks import naive_duckdb, scale_prepare, spark_naive

pytestmark = pytest.mark.spark

FIXTURE_DIR = Path(__file__).parent / "fixtures" / "gharchive_small"
FIXTURE_LINES = 34


@pytest.fixture
def gz_files(tmp_path: Path) -> list[Path]:
    out = tmp_path / "raw"
    out.mkdir()
    for src in sorted(FIXTURE_DIR.glob("*.json")):
        with src.open("rb") as fin, gzip.open(out / f"{src.stem}.json.gz", "wb") as fout:
            shutil.copyfileobj(fin, fout)
    return sorted(out.glob("*.json.gz"))


def _same(actual: list[tuple], expected: list[tuple], q: str) -> None:
    assert len(actual) == len(expected), (q, actual, expected)
    for a_row, e_row in zip(actual, expected, strict=True):
        assert len(a_row) == len(e_row), (q, a_row, e_row)
        for a, e in zip(a_row, e_row, strict=True):
            if isinstance(e, float):
                assert a == pytest.approx(e, abs=1e-6), (q, a_row, e_row)
            else:
                assert a == e, (q, a_row, e_row)


def _assert_matches_duckdb(spark_result: dict, duck_result: dict) -> None:
    assert spark_result["row_count"] == duck_result["row_count"]
    for i in range(1, 7):
        q = f"q{i}"
        s, d = spark_result["results"][q], duck_result["results"][q]
        assert list(s["columns"]) == list(d["columns"]), q
        _same([tuple(r) for r in s["rows"]], [tuple(r) for r in d["rows"]], q)
        assert spark_result[f"{q}_rows"] == duck_result[f"{q}_rows"] == len(d["rows"]), q


def test_spark_counts_every_raw_line_including_duplicates(spark, gz_files):
    result = spark_naive.run_once(spark, gz_files)
    assert result["row_count"] == FIXTURE_LINES
    assert result["bytes_read"] == sum(p.stat().st_size for p in gz_files)
    for key in ("load_seconds", "total_seconds", *(f"q{i}_seconds" for i in range(1, 7))):
        assert result[key] >= 0, key
    assert result["total_seconds"] >= result["load_seconds"]


def test_spark_q1_to_q6_match_duckdb_on_the_fixture(spark, gz_files):
    duck = naive_duckdb.run_once(gz_files, collect=True)
    spark_result = spark_naive.run_once(spark, gz_files, collect=True)
    _assert_matches_duckdb(spark_result, duck)


def test_spark_on_slim_data_matches_duckdb_on_slim_data(spark, gz_files, tmp_path):
    types = scale_prepare.load_config()["event_types"]
    slim = []
    for raw in gz_files:
        scale_prepare.slim_file(raw, tmp_path / "slim" / raw.name, types)
        slim.append(tmp_path / "slim" / raw.name)
    duck = naive_duckdb.run_once(slim, collect=True)
    spark_result = spark_naive.run_once(spark, slim, collect=True)
    _assert_matches_duckdb(spark_result, duck)


def test_spark_rerun_does_not_accumulate_rows(spark, gz_files):
    first = spark_naive.run_once(spark, gz_files)
    second = spark_naive.run_once(spark, gz_files)
    assert first["row_count"] == second["row_count"] == FIXTURE_LINES
