"""Shared pytest fixtures.

``spark``: one local Delta-enabled SparkSession per test run (``local[1]``, UTC, 1 shuffle
partition). On CI (``CI`` is set) a missing pyspark/delta/Java is an error, never a skip. Locally
the Spark tests skip with a reason when pyspark, delta-spark, Java 17 or (Windows) winutils is
missing.
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


def pytest_configure(config):
    config.addinivalue_line(
        "markers", "spark: needs a local SparkSession with Delta (Java 17); always runs on CI"
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
def spark():
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
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.default.parallelism", "1")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.databricks.delta.snapshotPartitions", "1")
        .config("spark.driver.memory", "1g")
    )
    session = configure_spark_with_delta_pip(builder).getOrCreate()
    yield session
    session.stop()
