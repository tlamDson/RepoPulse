"""Naive PySpark pipeline for the scale benchmark: the Spark twin of ``naive_duckdb``.

Same transformation as the DuckDB baseline: read the raw event files with an explicit schema
(``payload`` as a JSON string, Spark returns the object text verbatim for a ``StringType``
field), materialise them (``cache()`` + ``count()``, the counterpart of DuckDB's ``CREATE TABLE``),
then run Q1-Q6 translated to Spark SQL (``benchmarks/queries_spark/``). Full refresh, no
dedup, no partitioning, no Delta.

pyspark is imported lazily so the pure helpers can be imported without it.
"""

from __future__ import annotations

import sys
import time
from collections.abc import Mapping, Sequence
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
QUERY_DIR = Path(__file__).resolve().parent / "queries_spark"
QUERIES: tuple[str, ...] = tuple(f"q{i}" for i in range(1, 7))
DEFAULT_SEED = ROOT / "transform" / "seeds" / "repos_data_ecosystem.csv"

# Mirrors naive_duckdb.RAW_COLUMNS.
RAW_SCHEMA = (
    "id STRING, type STRING, actor STRUCT<id: BIGINT, login: STRING>, "
    "repo STRUCT<id: BIGINT, name: STRING>, payload STRING, created_at TIMESTAMP"
)
SEED_SCHEMA = "repo_name STRING, category STRING"


def session_config(spark_cfg: Mapping[str, object]) -> dict[str, str]:
    """Spark conf for the benchmark session (plain Spark, UTC, sized for a 4 CPU / 16 GB runner)."""
    return {
        "spark.master": str(spark_cfg.get("master", "local[*]")),
        "spark.app.name": "repopulse-scale-benchmark",
        "spark.driver.memory": str(spark_cfg.get("driver_memory", "10g")),
        "spark.sql.shuffle.partitions": str(spark_cfg.get("shuffle_partitions", 8)),
        "spark.sql.session.timeZone": "UTC",
        "spark.sql.adaptive.enabled": "true",
        "spark.ui.enabled": "false",
        "spark.ui.showConsoleProgress": "false",
    }


def build_session(spark_cfg: Mapping[str, object]):
    import os

    from pyspark.sql import SparkSession

    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    builder = SparkSession.builder
    for key, value in session_config(spark_cfg).items():
        builder = builder.config(key, value)
    return builder.getOrCreate()


def run_once(
    spark,
    files: Sequence[str | Path],
    seed_path: str | Path = DEFAULT_SEED,
    *,
    collect: bool = False,
) -> dict:
    """Load + Q1..Q6 once on ``spark``; timings in seconds (same clock as naive_duckdb)."""
    paths = [Path(p) for p in files]
    if not paths:
        raise FileNotFoundError("empty file list")
    absent = [str(p) for p in paths if not p.is_file()]
    if absent:
        raise FileNotFoundError(f"missing input file(s): {', '.join(absent)}")
    result: dict = {"bytes_read": sum(p.stat().st_size for p in paths)}
    if collect:
        result["results"] = {}
    raw = repos = None
    try:
        start = time.perf_counter()
        raw = spark.read.schema(RAW_SCHEMA).json([str(p) for p in paths]).cache()
        result["row_count"] = raw.count()
        raw.createOrReplaceTempView("raw_events")
        repos = spark.read.csv(str(seed_path), header=True, schema=SEED_SCHEMA).cache()
        repos.count()
        repos.createOrReplaceTempView("repos")
        result["load_seconds"] = round(time.perf_counter() - start, 6)
        for q in QUERIES:
            sql = (QUERY_DIR / f"{q}.sql").read_text(encoding="utf-8")
            t0 = time.perf_counter()
            df = spark.sql(sql)
            rows = df.collect()
            result[f"{q}_seconds"] = round(time.perf_counter() - t0, 6)
            result[f"{q}_rows"] = len(rows)
            if collect:
                result["results"][q] = {
                    "columns": list(df.columns),
                    "rows": [tuple(r) for r in rows],
                }
        result["total_seconds"] = round(time.perf_counter() - start, 6)
    finally:
        for df in (raw, repos):
            if df is not None:
                df.unpersist()
        spark.catalog.dropTempView("raw_events")
        spark.catalog.dropTempView("repos")
    return result


def _rss_mb(who: int) -> float:
    import resource

    kb = resource.getrusage(who).ru_maxrss
    if sys.platform == "darwin":  # bytes on macOS, KiB on Linux
        kb /= 1024
    return kb / 1024


def stop_and_measure(spark) -> float | None:
    """Stop Spark, wait for the JVM to exit, return peak RSS of Python + JVM in MiB.

    ``ru_maxrss`` of RUSAGE_CHILDREN only covers children that have exited and been waited for,
    so the JVM (child of this process) is shut down first. The value is the Python driver's
    peak plus the largest child's peak (the JVM): an upper bound of the pair's combined peak.
    None when the JVM cannot be waited for or ``resource`` is unavailable (Windows).
    """
    try:
        import resource
    except ImportError:
        spark.stop()
        return None
    from pyspark import SparkContext

    gateway = SparkContext._gateway
    proc = getattr(gateway, "proc", None)
    spark.stop()
    if proc is None:
        return None
    try:
        if proc.stdin is not None:
            proc.stdin.close()  # PythonGatewayServer exits on EOF
        proc.wait(timeout=120)
    except Exception:  # noqa: BLE001
        return None
    return round(_rss_mb(resource.RUSAGE_SELF) + _rss_mb(resource.RUSAGE_CHILDREN), 1)


def run_single(
    spark_cfg: Mapping[str, object], files: Sequence[str | Path], seed_path: str | Path
) -> dict:
    """One measured run in this (fresh) process: startup timed apart from ``total_seconds``."""
    t0 = time.perf_counter()
    spark = build_session(spark_cfg)
    spark.range(1).count()  # JVM up and an executor ready
    startup = round(time.perf_counter() - t0, 6)
    try:
        result = run_once(spark, files, seed_path)
    except BaseException:
        spark.stop()
        raise
    result["startup_seconds"] = startup
    result["peak_rss_mb"] = stop_and_measure(spark)
    return result
