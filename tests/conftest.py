"""Shared pytest fixtures.

``spark``: one local Delta-enabled SparkSession per test run (``local[1]``, UTC, 1 shuffle
partition). On CI (``CI`` is set) a missing pyspark/delta/Java is an error, never a skip. Locally
the Spark tests skip with a reason when pyspark, delta-spark, Java 17 or (Windows) winutils is
missing.

``lake`` / ``built``: one silver parquet + one ``dbt build`` of ``transform/`` per test run, shared
by test_dbt_marts.py and test_governance.py (helpers in ``dbt_helpers.py``, imported lazily so a
missing dbt extra only skips the dbt tests).

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


def pytest_configure(config):
    config.addinivalue_line(
        "markers", "spark: needs a local SparkSession with Delta (Java 17); always runs on CI"
    )
    _diag_start(config)


# TEMP-DIAG (to be removed before merge): find the Spark test that hangs on CI.
_DIAG: dict = {"config": None, "test": None, "start": 0.0, "session": None}
_DIAG_LIMIT_S = 300


def _diag_say(text: str) -> None:
    import time

    capman = _DIAG["config"].pluginmanager.getplugin("capturemanager")
    stamp = time.strftime("%H:%M:%S", time.gmtime())
    if capman is None:
        sys.stderr.write(f"\n[diag {stamp}] {text}\n")
        sys.stderr.flush()
        return
    with capman.global_and_fixture_disabled():
        sys.stderr.write(f"\n[diag {stamp}] {text}\n")
        sys.stderr.flush()


def _diag_heap() -> str:
    session = _DIAG["session"]
    if session is None:
        return "no session"
    rt = session._jvm.java.lang.Runtime.getRuntime()
    used = (rt.totalMemory() - rt.freeMemory()) >> 20
    return (
        f"jvm heap used={used}MiB total={rt.totalMemory() >> 20}MiB max={rt.maxMemory() >> 20}MiB"
    )


def _diag_tool(name: str, *args: str) -> str:
    tool = Path(os.environ.get("JAVA_HOME", "")) / "bin" / name
    try:
        proc = subprocess.run(
            [str(tool), *args], capture_output=True, text=True, timeout=60, check=False
        )
    except (OSError, subprocess.SubprocessError) as exc:
        return f"{name} failed: {exc!r}"
    return proc.stdout + proc.stderr


def _diag_watchdog() -> None:
    import faulthandler
    import time

    while True:
        time.sleep(10)
        test, start = _DIAG["test"], _DIAG["start"]
        if test is None or time.monotonic() - start < _DIAG_LIMIT_S:
            continue
        capman = _DIAG["config"].pluginmanager.getplugin("capturemanager")
        if capman is not None:
            capman.suspend_global_capture(in_=False)
        err = sys.__stderr__
        err.write(f"\n[diag] HANG: {test} has run for more than {_DIAG_LIMIT_S}s\n")
        err.flush()
        faulthandler.dump_traceback(file=err, all_threads=True)
        try:
            pids = subprocess.run(
                ["pgrep", "-x", "java"], capture_output=True, text=True, timeout=30, check=False
            ).stdout.split()
        except (OSError, subprocess.SubprocessError):
            pids = []
        for pid in pids:
            err.write(f"\n[diag] jstat -gcutil {pid}\n{_diag_tool('jstat', '-gcutil', pid)}\n")
            err.write(f"\n[diag] jstack {pid}\n{_diag_tool('jstack', pid)}\n")
        try:
            ps = subprocess.run(
                ["ps", "-eo", "pid,ppid,etimes,stat,args", "--forest"],
                capture_output=True,
                text=True,
                timeout=30,
                check=False,
            ).stdout
        except (OSError, subprocess.SubprocessError) as exc:
            ps = repr(exc)
        err.write(f"\n[diag] ps\n{ps}\n")
        err.flush()
        os._exit(3)


def _diag_start(config) -> None:
    import threading

    _DIAG["config"] = config
    if os.environ.get("CI"):
        threading.Thread(target=_diag_watchdog, name="diag-watchdog", daemon=True).start()


@pytest.hookimpl(tryfirst=True)
def pytest_runtest_setup(item):
    import time

    _DIAG["test"], _DIAG["start"] = item.nodeid, time.monotonic()
    if os.environ.get("CI") and "spark" in item.fixturenames:
        try:
            heap = _diag_heap()
        except Exception as exc:  # noqa: BLE001
            heap = f"heap unavailable: {exc!r}"
        _diag_say(f"start {item.nodeid} | {heap}")


@pytest.hookimpl(trylast=True)
def pytest_runtest_teardown(item):
    _DIAG["test"] = None


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
        # Managed tables by name (test_spark_catalog_tables.py) live in tmp, not ./spark-warehouse.
        .config("spark.sql.warehouse.dir", str(tmp_path_factory.mktemp("spark-warehouse")))
    )
    session = configure_spark_with_delta_pip(builder).getOrCreate()
    _DIAG["session"] = session
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
