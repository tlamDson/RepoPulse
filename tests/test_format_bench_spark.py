"""Delta Lake vs Apache Iceberg on the same data (E2), with the session-scoped ``spark`` fixture.

Fixture ``tests/fixtures/gharchive_small`` gzip-compressed into ``tmp_path``: hour 0 has 20 lines
(20 distinct ids), hour 1 has 14 lines, one of which (``9000000002``) repeats a line of hour 0.
With two files the base table is hour 0 and the MERGE source is hours 0 + 1 (34 lines, 33 ids), so
MERGE updates the 20 base rows and inserts the 13 ids that only hour 1 has. Expected numbers are
computed from the JSON lines here, not with the code under test.
"""

from __future__ import annotations

import gzip
import json
import shutil
from pathlib import Path

import pytest

from benchmarks import format_bench, spark_naive

pytestmark = pytest.mark.spark

FIXTURE_DIR = Path(__file__).parent / "fixtures" / "gharchive_small"
QUERIES = tuple(f"q{i}" for i in range(1, 7))
BASE_LINES = 20
MERGED_ROWS = 33


@pytest.fixture
def gz_files(tmp_path: Path) -> list[Path]:
    out = tmp_path / "raw"
    out.mkdir()
    for src in sorted(FIXTURE_DIR.glob("*.json")):
        with src.open("rb") as fin, gzip.open(out / f"{src.stem}.json.gz", "wb") as fout:
            shutil.copyfileobj(fin, fout)
    return sorted(out.glob("*.json.gz"))


def _ids(path: Path) -> list[str]:
    with gzip.open(path, "rt", encoding="utf-8") as fh:
        return [json.loads(ln)["id"] for ln in fh if ln.strip()]


@pytest.fixture
def expected(gz_files) -> dict:
    """Counts of the fixture, from the raw JSON lines (independent of format_bench)."""
    base_ids = _ids(gz_files[0])
    all_ids = [i for p in gz_files for i in _ids(p)]
    return {
        "base_rows": len(base_ids),
        "base_ids": set(base_ids),
        "source_ids": set(all_ids),
        "merged_rows": len(base_ids) + len(set(all_ids) - set(base_ids)),
        "source_rows_raw": len(all_ids),
    }


def test_fixture_counts_are_what_this_file_expects(expected):
    assert expected["base_rows"] == BASE_LINES
    assert len(expected["base_ids"]) == BASE_LINES  # no duplicate inside hour 0
    assert expected["merged_rows"] == MERGED_ROWS
    assert expected["source_rows_raw"] == 34
    assert len(expected["source_ids"]) == MERGED_ROWS


def _same(actual: list[tuple], expected: list[tuple], q: str) -> None:
    assert len(actual) == len(expected), (q, actual, expected)
    for a_row, e_row in zip(actual, expected, strict=True):
        assert len(a_row) == len(e_row), (q, a_row, e_row)
        for a, e in zip(a_row, e_row, strict=True):
            if isinstance(e, float):
                assert a == pytest.approx(e, abs=1e-6), (q, a_row, e_row)
            else:
                assert a == e, (q, a_row, e_row)


def _same_results(left: dict, right: dict) -> None:
    for q in QUERIES:
        assert list(left[q]["columns"]) == list(right[q]["columns"]), q
        _same([tuple(r) for r in left[q]["rows"]], [tuple(r) for r in right[q]["rows"]], q)


def _written(spark, fmt: str, work_dir: Path, files: list[Path]):
    base, _ = format_bench.split_files(files)
    table = format_bench.open_table(spark, fmt, work_dir)
    format_bench.write_table(spark, table, format_bench.read_raw(spark, base))
    return table


def _source(spark, files: list[Path]):
    _, source = format_bench.split_files(files)
    return format_bench.dedup_by_id(format_bench.read_raw(spark, source))


def _rows_by_id(df) -> list[tuple]:
    return [tuple(r) for r in sorted(df.collect(), key=lambda r: r["id"])]


def test_session_has_delta_and_iceberg_extensions(spark):
    extensions = spark.conf.get("spark.sql.extensions")
    assert "io.delta.sql.DeltaSparkSessionExtension" in extensions
    assert "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions" in extensions


def test_written_tables_hold_every_raw_line_in_both_formats(spark, gz_files, tmp_path, expected):
    counts = {}
    for fmt in format_bench.FORMATS:
        table = _written(spark, fmt, tmp_path / fmt, gz_files)
        counts[fmt] = format_bench.read_table(spark, table).count()
        assert table.location.is_dir(), fmt
        assert format_bench.table_size(table)[1] >= 1, fmt
    assert counts["delta"] == counts["iceberg"] == expected["base_rows"]


def test_q1_to_q6_match_between_formats_and_the_raw_files(spark, gz_files, tmp_path):
    base, _ = format_bench.split_files(gz_files)
    naive = spark_naive.run_once(spark, base, collect=True)
    results = {}
    for fmt in format_bench.FORMATS:
        table = _written(spark, fmt, tmp_path / fmt, gz_files)
        got = format_bench.run_queries(spark, format_bench.read_table(spark, table), collect=True)
        for q in QUERIES:
            assert got[f"{q}_rows"] == naive[f"{q}_rows"], (fmt, q)
            assert got[f"{q}_seconds"] >= 0, (fmt, q)
        results[fmt] = got["results"]
    _same_results(results["delta"], results["iceberg"])
    _same_results(results["delta"], naive["results"])
    assert any(naive[f"{q}_rows"] > 0 for q in QUERIES)  # the comparison is not vacuous


def test_merge_is_idempotent_and_equal_between_formats(spark, gz_files, tmp_path, expected):
    source = _source(spark, gz_files).cache()
    try:
        assert source.count() == len(expected["source_ids"])
        contents = {}
        for fmt in format_bench.FORMATS:
            table = _written(spark, fmt, tmp_path / fmt, gz_files)
            format_bench.merge_into(spark, table, source)
            assert format_bench.read_table(spark, table).count() == expected["merged_rows"], fmt
            for _ in range(2):  # second and third MERGE of the same source
                format_bench.merge_into(spark, table, source)
                assert format_bench.read_table(spark, table).count() == expected["merged_rows"]
            df = format_bench.read_table(spark, table)
            per_id = {r["id"]: r["n"] for r in df.groupBy("id").count().toDF("id", "n").collect()}
            assert set(per_id) == expected["source_ids"], fmt
            assert all(per_id[i] == 1 for i in expected["source_ids"]), fmt
            contents[fmt] = _rows_by_id(df)
        assert contents["delta"] == contents["iceberg"]
        assert len(contents["delta"]) == MERGED_ROWS
    finally:
        source.unpersist()
    assert not [
        t.name for t in spark.catalog.listTables() if t.name.startswith("merge_src")
    ]  # temp views of the MERGE source are dropped


def test_time_travel_reads_the_version_before_merge(spark, gz_files, tmp_path, expected):
    source = _source(spark, gz_files)
    for fmt in format_bench.FORMATS:
        table = _written(spark, fmt, tmp_path / fmt, gz_files)
        before = format_bench.run_queries(
            spark, format_bench.read_table(spark, table), collect=True
        )
        version = format_bench.current_version(spark, table)
        format_bench.merge_into(spark, table, source)
        format_bench.merge_into(spark, table, source)

        current = format_bench.read_table(spark, table)
        old = format_bench.read_table(spark, table, version=version)
        assert current.count() == expected["merged_rows"], fmt
        assert old.count() == expected["base_rows"], fmt
        assert {r["id"] for r in old.select("id").collect()} == expected["base_ids"], fmt
        assert format_bench.current_version(spark, table) != version, fmt

        travelled = format_bench.run_queries(spark, old, collect=True)
        _same_results(travelled["results"], before["results"])


def test_run_format_reports_every_step(spark, gz_files, tmp_path, expected):
    base, _ = format_bench.split_files(gz_files)
    naive = spark_naive.run_once(spark, base, collect=True)
    results = {}
    for fmt in format_bench.FORMATS:
        r = format_bench.run_format(
            spark, fmt, gz_files, spark_naive.DEFAULT_SEED, tmp_path / fmt, collect=True
        )
        parts = ["write_seconds", *(f"{q}_seconds" for q in QUERIES)]
        parts += ["merge_seconds", "merge_rerun_seconds", "time_travel_seconds"]
        for key in [*parts, "load_seconds", "total_seconds"]:
            assert isinstance(r[key], float) and r[key] >= 0, (fmt, key)
        assert r["total_seconds"] == pytest.approx(sum(r[k] for k in parts), abs=1e-5), fmt
        assert r["base_rows"] == expected["base_rows"], fmt
        assert r["row_count"] == expected["merged_rows"], fmt
        assert r["merge_source_rows"] == len(expected["source_ids"]), fmt
        assert r["bytes_read"] == sum(p.stat().st_size for p in gz_files), fmt
        assert r["table_bytes"] > 0 and r["table_files"] >= 1, fmt
        for q in QUERIES:
            assert r[f"{q}_rows"] == naive[f"{q}_rows"], (fmt, q)
        results[fmt] = r
        # the output must pass the parser the parent process uses
        format_bench.parse_run_output(
            json.dumps({k: v for k, v in r.items() if k != "results"} | {"startup_seconds": 0.0}),
            fmt,
        )
    _same_results(results["delta"]["results"], results["iceberg"]["results"])
    _same_results(results["delta"]["results"], naive["results"])
    leftovers = {"raw_events", "repos"}
    assert not [
        t.name
        for t in spark.catalog.listTables()
        if t.name in leftovers or t.name.startswith("merge_src")
    ]
