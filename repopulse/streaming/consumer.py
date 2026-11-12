"""Spark Structured Streaming: Kafka topic -> bronze Delta table (needs pyspark + delta-spark).

Three parts, so the transformation and the sink are testable without Kafka:

1. ``read_kafka`` / ``kafka_source_options``: ``spark.readStream.format("kafka")`` from the
   earliest offset, ``failOnDataLoss=true`` (a gap in offsets fails the query instead of being
   skipped), ``maxOffsetsPerTrigger`` optional.
2. ``to_bronze``: any DataFrame with the Kafka source schema (``KAFKA_SCHEMA``) -> bronze rows:
   the message value verbatim (``raw_json``) + ``raw_sha256``, the event id, and the Kafka
   lineage (topic, partition, offset, timestamp). Bronze keeps the raw JSON, like the batch
   bronze in ``repopulse/spark/batch.py``; PII is hashed later, before silver.
3. ``run_stream``: Delta sink, ``checkpointLocation``, trigger ``availableNow`` (process every
   offset available at start, in one or more micro-batches, then stop). Exactly-once to the sink:
   the offsets of each micro-batch are logged in the checkpoint before it runs, and the Delta
   sink commits each batch with the query id and batch id as its transaction id, so a batch
   replayed after a crash is skipped instead of appended twice.

pyspark is imported inside the functions, so this module imports without it.
"""

from __future__ import annotations

import os
import sys
import time
from pathlib import Path

# Schema of Spark's Kafka source; names quoted because partition/offset/timestamp are keywords.
KAFKA_SCHEMA = (
    "`key` BINARY, `value` BINARY, `topic` STRING, `partition` INT, `offset` BIGINT, "
    "`timestamp` TIMESTAMP, `timestampType` INT"
)
BRONZE_COLUMNS = [
    "raw_json",
    "raw_sha256",
    "event_id",
    "kafka_key",
    "kafka_topic",
    "kafka_partition",
    "kafka_offset",
    "kafka_timestamp",
    "ingested_at",
]
SCALA_BINARY = "2.12"  # PyPI pyspark 3.5 is built for Scala 2.12
SHUFFLE_PARTITIONS = 8  # runner: 4 CPU, 16 GB RAM
RECENT_PROGRESS = 10_000  # keep every micro-batch progress of one run (default is 100)


def kafka_package(pyspark_version: str) -> str:
    """Maven coordinate of the Kafka connector that matches the installed pyspark."""
    return f"org.apache.spark:spark-sql-kafka-0-10_{SCALA_BINARY}:{pyspark_version}"


def create_spark_session(app_name: str = "repopulse-streaming", *, driver_memory: str = "6g"):
    """Local Delta + Kafka session for the benchmark (the test fixture has no Kafka connector)."""
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    import pyspark
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.master("local[*]")
        .appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        .config("spark.sql.shuffle.partitions", str(SHUFFLE_PARTITIONS))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.streaming.numRecentProgressUpdates", str(RECENT_PROGRESS))
        .config("spark.driver.memory", driver_memory)
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
    )
    packages = [kafka_package(pyspark.__version__)]
    return configure_spark_with_delta_pip(builder, extra_packages=packages).getOrCreate()


# ---------------------------------------------------------------- source


def kafka_source_options(
    bootstrap: str, topic: str, *, max_offsets_per_trigger: int | None = None
) -> dict[str, str]:
    opts = {
        "kafka.bootstrap.servers": bootstrap,
        "subscribe": topic,
        "startingOffsets": "earliest",
        "failOnDataLoss": "true",
    }
    if max_offsets_per_trigger is not None:
        if max_offsets_per_trigger < 1:
            raise ValueError("max_offsets_per_trigger must be >= 1")
        opts["maxOffsetsPerTrigger"] = str(int(max_offsets_per_trigger))
    return opts


def read_kafka(spark, bootstrap: str, topic: str, *, max_offsets_per_trigger: int | None = None):
    opts = kafka_source_options(bootstrap, topic, max_offsets_per_trigger=max_offsets_per_trigger)
    return spark.readStream.format("kafka").options(**opts).load()


# ---------------------------------------------------------------- transformation


def to_bronze(df):
    """Kafka-schema rows -> ``BRONZE_COLUMNS`` (works on batch and streaming DataFrames)."""
    from pyspark.sql import functions as F

    raw = F.col("value").cast("string")
    return (
        df.withColumn("raw_json", raw)
        .select(
            F.col("raw_json"),
            F.sha2(F.col("raw_json"), 256).alias("raw_sha256"),
            F.get_json_object(F.col("raw_json"), "$.id").alias("event_id"),
            F.col("key").cast("string").alias("kafka_key"),
            F.col("topic").alias("kafka_topic"),
            F.col("partition").cast("int").alias("kafka_partition"),
            F.col("offset").cast("bigint").alias("kafka_offset"),
            F.col("timestamp").alias("kafka_timestamp"),
            F.current_timestamp().alias("ingested_at"),
        )
        .select(*BRONZE_COLUMNS)
    )


# ---------------------------------------------------------------- sink


def run_stream(source, *, delta_path: str | Path, checkpoint_path: str | Path) -> dict:
    """Run ``to_bronze(source)`` into Delta with trigger ``availableNow``; block until it stops.

    Returns ``micro_batches`` (batches that read at least one row), ``input_rows`` (rows read by
    the source per the query progress) and ``seconds`` (query start to termination). The row
    count of record is the Delta table (``delta_counts``), not ``input_rows``.
    """
    start = time.perf_counter()
    query = (
        to_bronze(source)
        .writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", str(checkpoint_path))
        .trigger(availableNow=True)
        .start(str(delta_path))
    )
    try:
        query.awaitTermination()
    finally:
        if query.isActive:
            query.stop()
    seconds = round(time.perf_counter() - start, 6)
    progress = query.recentProgress
    batches = {int(p["batchId"]) for p in progress if int(p["numInputRows"]) > 0}
    return {
        "micro_batches": len(batches),
        "input_rows": sum(int(p["numInputRows"]) for p in progress),
        "seconds": seconds,
    }


def delta_counts(spark, delta_path: str | Path) -> dict[str, int]:
    """Rows and distinct (kafka_partition, kafka_offset) pairs of the bronze table."""
    from pyspark.sql import functions as F

    row = (
        spark.read.format("delta")
        .load(str(delta_path))
        .agg(
            F.count(F.lit(1)).alias("rows"),
            F.countDistinct("kafka_partition", "kafka_offset").alias("distinct_offsets"),
        )
        .first()
    )
    return {"rows": int(row["rows"]), "distinct_offsets": int(row["distinct_offsets"])}
