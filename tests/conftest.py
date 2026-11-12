"""Shared pytest fixtures.

``spark``: one local SparkSession per test run (``local[1]``, UTC, 1 shuffle partition) with Delta
Lake (``spark_catalog`` = ``DeltaCatalog``) and the Apache Iceberg runtime + SQL extensions loaded
next to it (Iceberg catalogs are registered at run time by ``benchmarks/format_bench.py``, one per
work directory). On CI (``CI`` is set) a missing pyspark/delta/Java is an error, never a skip.
Locally the Spark tests skip with a reason when pyspark, delta-spark, Java 17 or (Windows) winutils
is missing.

``lake`` / ``built``: one silver parquet + one ``dbt build`` of ``transform/`` per test run, shared
by test_dbt_marts.py, test_governance.py and test_dbt_semantic_layer.py (helpers in
``dbt_helpers.py``, imported lazily so a missing dbt extra only skips the dbt tests). ``baseline``:
benchmarks/queries/q1..q6.sql on the same raw lines, what the marts and metrics are compared with.

``_no_motherduck`` (autouse): wraps ``duckdb.connect`` so any ``md:`` / ``motherduck:`` target
raises ``AssertionError`` instead of reaching the real MotherDuck account; local targets pass.
"""

from __future__ import annotations

import importlib.util
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

# Same pin as benchmarks/format_bench.py::ICEBERG_PACKAGE (Spark 3.5, Scala 2.12).
ICEBERG_PACKAGE = "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.9.2"
SQL_EXTENSIONS = (
    "io.delta.sql.DeltaSparkSessionExtension,"
    "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
)


def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "spark: needs a local SparkSession with Delta + Iceberg (Java 17); always runs on CI",
    )


def _java_major() -> int | None:
    java_home = os.environ.get("JAVA_HOME")
    java = shutil.which("java")
    if java_home:
        candidate = Path(java_home) / "bin" / ("java.exe" if sys.platform == "win32" else "java")
        if candidate.is_file():
            java = str(candidate)
    if java is None:
        return None
    try:
        proc = subprocess.run(
            [java, "-version"], capture_output=True, text=True, timeout=30, check=False
        )
    except (OSError, subprocess.SubprocessError):
        return None
    match = re.search(r'version "(\d+)(?:\.(\d+))?', proc.stderr + proc.stdout)
    if match is None:
        return None
    major = int(match.group(1))
    if major == 1 and match.group(2):  # "1.8.0_x" style
        major = int(match.group(2))
    return major


def _spark_unavailable_reason() -> str | None:
    if importlib.util.find_spec("pyspark") is None:
        return "pyspark is not installed (pip install -e '.[dev,spark]')"
    if importlib.util.find_spec("delta") is None:
        return "delta-spark is not installed (pip install -e '.[dev,spark]')"
    major = _java_major()
    if major != 17:
        return f"PySpark 3.5 tests need Java 17, found {major if major else 'no java'}"
    if sys.platform == "win32":
        hadoop_home = os.environ.get("HADOOP_HOME")
        if not hadoop_home or not (Path(hadoop_home) / "bin" / "winutils.exe").is_file():
            return "Delta on Windows needs HADOOP_HOME with bin/winutils.exe"
    return None


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    reason = _spark_unavailable_reason()
    if reason is not None:
        if os.environ.get("CI"):
            pytest.fail(f"Spark tests must run on CI: {reason}")
        pytest.skip(reason)

    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.master("local[1]")
        .appName("repopulse-tests")
        .config("spark.sql.extensions", SQL_EXTENSIONS)
        .config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.default.parallelism", "1")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.databricks.delta.snapshotPartitions", "1")
        # One JVM serves every Spark test of the run and its live heap only grows (query plans
        # Spark keeps per execution). At 1g the old generation filled up before the last Spark
        # tests and the run stalled in back-to-back full GCs until the CI job timeout.
        .config("spark.driver.memory", "2g")
        # Managed tables by name (test_spark_catalog_tables.py) live in tmp, not ./spark-warehouse.
        .config("spark.sql.warehouse.dir", str(tmp_path_factory.mktemp("spark-warehouse")))
    )
    session = configure_spark_with_delta_pip(
        builder, extra_packages=[ICEBERG_PACKAGE]
    ).getOrCreate()
    yield session
    session.stop()


_MOTHERDUCK_PREFIXES = ("md:", "motherduck:")


@pytest.fixture(autouse=True)
def _no_motherduck(monkeypatch):
    """Unit tests never call the real MotherDuck; inject a fake ``connect`` instead."""
    if sys.modules.get("duckdb", ...) is None:
        return
    try:
        import duckdb
    except ImportError:
        return

    real_connect = duckdb.connect

    def guarded_connect(database=":memory:", *args, **kwargs):
        if str(database).strip().lower().startswith(_MOTHERDUCK_PREFIXES):
            raise AssertionError(
                f"unit test tried to connect to MotherDuck ({database!r}); unit tests must not "
                "call real services: inject a fake connect or monkeypatch duckdb.connect"
            )
        return real_connect(database, *args, **kwargs)

    guarded_connect.blocks_motherduck = True
    monkeypatch.setattr(duckdb, "connect", guarded_connect)


@pytest.fixture(scope="session")
def lake(tmp_path_factory) -> dict:
    """Silver parquet built from the JSON fixtures, a DuckDB path and a ci profile, all in tmp."""
    import dbt_helpers

    return dbt_helpers.make_lake(tmp_path_factory.mktemp("dbt_marts"))


@pytest.fixture(scope="session")
def built(lake) -> dict:
    """One ``dbt build`` of transform/ on ``lake``; check ``built["proc"]`` before using the DB."""
    import dbt_helpers

    return dbt_helpers.build_lake(lake)


@pytest.fixture(scope="session")
def baseline(lake) -> dict:
    """Results of benchmarks/queries/q1..q6.sql on the raw lines of ``lake`` (naive baseline)."""
    from benchmarks import naive_duckdb

    return naive_duckdb.run_once(lake["raw"], collect=True)["results"]
