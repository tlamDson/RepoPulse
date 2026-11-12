"""Spark Structured Streaming consumer: Kafka-shaped rows -> bronze Delta (local[1], no Kafka).

The Kafka source is replaced by a file stream source over parquet files that have exactly the
schema of Spark's Kafka source (``key``/``value`` binary, ``topic``, ``partition``, ``offset``,
``timestamp``, ``timestampType``). The transformation and the Delta sink are the real ones, so
checkpointing and exactly-once to the sink are exercised for real. GH Archive is real public
data; the fixture lines are small hand-written look-alikes.
"""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from repopulse.streaming import consumer

pytestmark = pytest.mark.spark

FIXTURE = Path(__file__).resolve().parent / "fixtures" / "gharchive_small" / "2026-01-05-0.json"
TS = datetime(2026, 10, 25, 12, 0, tzinfo=UTC)


def fixture_lines() -> list[bytes]:
    return FIXTURE.read_bytes().splitlines()


def kafka_rows(lines: list[bytes], *, partitions: int = 2, start: int = 0) -> list[tuple]:
    rows = []
    for i, line in enumerate(lines, start=start):
        try:
            key = str(json.loads(line)["id"]).encode()
        except (ValueError, KeyError, TypeError):
            key = b"sha256:" + hashlib.sha256(line).hexdigest().encode()
        rows.append((key, line, "gh-test", i % partitions, i // partitions, TS, 0))
    return rows


def append_source(spark, src: Path, rows: list[tuple]) -> None:
    df = spark.createDataFrame(rows, consumer.KAFKA_SCHEMA)
    df.coalesce(1).write.mode("append").parquet(str(src))


def file_stream(spark, src: Path):
    return spark.readStream.schema(consumer.KAFKA_SCHEMA).parquet(str(src))


def delta_rows(spark, path: Path):
    return spark.read.format("delta").load(str(path))


def test_kafka_schema_matches_the_spark_kafka_source():
    names = [f.split()[0] for f in consumer.KAFKA_SCHEMA.split(",")]
    assert [n.strip().strip("`") for n in names] == [
        "key",
        "value",
        "topic",
        "partition",
        "offset",
        "timestamp",
        "timestampType",
    ]


def test_to_bronze_columns_and_values(spark):
    lines = fixture_lines()[:4] + [b"{broken json"]
    df = spark.createDataFrame(kafka_rows(lines), consumer.KAFKA_SCHEMA)
    out = consumer.to_bronze(df)
    assert out.columns == consumer.BRONZE_COLUMNS
    rows = sorted(out.collect(), key=lambda r: (r["kafka_offset"], r["kafka_partition"]))
    assert len(rows) == 5
    by_raw = {r["raw_json"]: r for r in rows}
    first = by_raw[lines[0].decode()]
    assert first["event_id"] == json.loads(lines[0])["id"]
    assert first["kafka_key"] == json.loads(lines[0])["id"]
    assert first["kafka_topic"] == "gh-test"
    assert first["kafka_partition"] == 0
    assert first["kafka_offset"] == 0
    assert first["kafka_timestamp"].replace(tzinfo=UTC) == TS
    assert first["ingested_at"] is not None
    broken = by_raw["{broken json"]
    assert broken["event_id"] is None
    assert broken["kafka_key"].startswith("sha256:")


def test_raw_sha256_is_sha256_of_the_message_value(spark):
    lines = fixture_lines()
    df = spark.createDataFrame(kafka_rows(lines), consumer.KAFKA_SCHEMA)
    got = {r["raw_json"]: r["raw_sha256"] for r in consumer.to_bronze(df).collect()}
    assert got == {ln.decode(): hashlib.sha256(ln).hexdigest() for ln in lines}


def test_source_options_read_from_earliest_and_fail_on_data_loss():
    opts = consumer.kafka_source_options("localhost:19092", "gh-p3", max_offsets_per_trigger=500)
    assert opts == {
        "kafka.bootstrap.servers": "localhost:19092",
        "subscribe": "gh-p3",
        "startingOffsets": "earliest",
        "failOnDataLoss": "true",
        "maxOffsetsPerTrigger": "500",
    }
    assert "maxOffsetsPerTrigger" not in consumer.kafka_source_options("b:1", "t")


def test_kafka_package_matches_the_pyspark_version():
    assert consumer.kafka_package("3.5.3") == "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3"


def test_stream_writes_every_row_once_to_delta(spark, tmp_path):
    src, delta, ckpt = tmp_path / "src", tmp_path / "delta", tmp_path / "ckpt"
    lines = fixture_lines()
    append_source(spark, src, kafka_rows(lines))
    metrics = consumer.run_stream(file_stream(spark, src), delta_path=delta, checkpoint_path=ckpt)
    assert metrics["input_rows"] == 20
    assert metrics["micro_batches"] >= 1
    assert metrics["seconds"] >= 0
    table = delta_rows(spark, delta)
    assert table.columns == consumer.BRONZE_COLUMNS
    assert table.count() == 20
    assert consumer.delta_counts(spark, delta) == {"rows": 20, "distinct_offsets": 20}
    assert sorted(r["raw_json"] for r in table.collect()) == sorted(ln.decode() for ln in lines)


def test_rerun_with_the_same_checkpoint_adds_no_rows(spark, tmp_path):
    src, delta, ckpt = tmp_path / "src", tmp_path / "delta", tmp_path / "ckpt"
    append_source(spark, src, kafka_rows(fixture_lines()))
    consumer.run_stream(file_stream(spark, src), delta_path=delta, checkpoint_path=ckpt)
    again = consumer.run_stream(file_stream(spark, src), delta_path=delta, checkpoint_path=ckpt)
    assert again["input_rows"] == 0
    assert consumer.delta_counts(spark, delta) == {"rows": 20, "distinct_offsets": 20}


def test_new_data_adds_exactly_the_new_rows(spark, tmp_path):
    src, delta, ckpt = tmp_path / "src", tmp_path / "delta", tmp_path / "ckpt"
    lines = fixture_lines()
    append_source(spark, src, kafka_rows(lines[:12], partitions=1))
    consumer.run_stream(file_stream(spark, src), delta_path=delta, checkpoint_path=ckpt)
    assert consumer.delta_counts(spark, delta) == {"rows": 12, "distinct_offsets": 12}
    append_source(spark, src, kafka_rows(lines[12:], partitions=1, start=12))
    metrics = consumer.run_stream(file_stream(spark, src), delta_path=delta, checkpoint_path=ckpt)
    assert metrics["input_rows"] == 8
    assert consumer.delta_counts(spark, delta) == {"rows": 20, "distinct_offsets": 20}
    offsets = sorted(r["kafka_offset"] for r in delta_rows(spark, delta).collect())
    assert offsets == list(range(20))


def test_batch_replayed_after_a_crash_before_commit_is_not_written_twice(spark, tmp_path):
    """Delta wrote batch 0 but the checkpoint commit is lost: the replay must be skipped."""
    src, delta, ckpt = tmp_path / "src", tmp_path / "delta", tmp_path / "ckpt"
    append_source(spark, src, kafka_rows(fixture_lines()))
    consumer.run_stream(file_stream(spark, src), delta_path=delta, checkpoint_path=ckpt)
    from delta.tables import DeltaTable

    versions = DeltaTable.forPath(spark, str(delta)).history().count()
    commit = ckpt / "commits" / "0"
    assert commit.is_file()
    commit.unlink()
    (ckpt / "commits" / ".0.crc").unlink(missing_ok=True)
    consumer.run_stream(file_stream(spark, src), delta_path=delta, checkpoint_path=ckpt)
    assert commit.is_file()  # batch 0 was run again and committed ...
    assert DeltaTable.forPath(spark, str(delta)).history().count() == versions  # ... no new write
    assert consumer.delta_counts(spark, delta) == {"rows": 20, "distinct_offsets": 20}


def test_cli_parser_for_a_consumer_run_in_its_own_process(tmp_path):
    args = consumer.build_parser().parse_args(
        [
            "--bootstrap",
            "localhost:19092,localhost:29092",
            "--topic",
            "gh-p3",
            "--delta",
            str(tmp_path / "delta"),
            "--checkpoint",
            str(tmp_path / "ckpt"),
            "--max-offsets-per-trigger",
            "50000",
            "--driver-memory",
            "3g",
            "--metrics-out",
            str(tmp_path / "m.json"),
        ]
    )
    assert args.bootstrap == "localhost:19092,localhost:29092"
    assert args.topic == "gh-p3"
    assert args.delta == tmp_path / "delta"
    assert args.checkpoint == tmp_path / "ckpt"
    assert args.max_offsets_per_trigger == 50000
    assert args.driver_memory == "3g"
    assert args.metrics_out == tmp_path / "m.json"


def test_cli_parser_requires_topic_and_paths():
    with pytest.raises(SystemExit):
        consumer.build_parser().parse_args(["--bootstrap", "b:1"])


def test_delta_message_counts_counts_distinct_offsets_per_content(spark, tmp_path):
    lines = fixture_lines()[:3]
    rows = kafka_rows(lines, partitions=1)
    replay = (rows[1][0], rows[1][1], "gh-test", 0, 99, TS, 0)  # same line at a new offset
    df = spark.createDataFrame(rows + rows[:1] + [replay], consumer.KAFKA_SCHEMA)
    path = tmp_path / "content"
    consumer.to_bronze(df).write.format("delta").save(str(path))
    got = dict(consumer.delta_message_counts(spark, path))
    assert got == {
        hashlib.sha256(lines[0]).digest(): 1,  # written twice at one offset: one message
        hashlib.sha256(lines[1]).digest(): 2,  # two offsets
        hashlib.sha256(lines[2]).digest(): 1,
    }


def test_delta_counts_sees_duplicate_offsets(spark, tmp_path):
    rows = kafka_rows(fixture_lines()[:3], partitions=1)
    df = spark.createDataFrame(rows + rows[:1], consumer.KAFKA_SCHEMA)
    path = tmp_path / "dup"
    consumer.to_bronze(df).write.format("delta").save(str(path))
    assert consumer.delta_counts(spark, path) == {"rows": 4, "distinct_offsets": 3}
