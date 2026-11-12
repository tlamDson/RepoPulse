# Streaming: Kafka (KRaft, 3 brokers) -> Spark Structured Streaming -> Delta

Task P2a in [`plan.md`](plan.md) (phase 2). The GH Archive reference day is produced into Apache
Kafka, read back with Spark Structured Streaming into a bronze Delta table, and reconciled with
the batch line count. Measure: `variant=streaming-throughput-p<N>` in `runs.csv`, N = number of
topic partitions.

**Numbers live only in `runs.csv`.** This page describes how they are produced; it quotes none.

**Data:** real GH Archive data (not simulated): the reference day of
[`benchmarks/reference.toml`](../benchmarks/reference.toml) (`data_date = "2026-10-25"`, 24
hour files, downloaded and checksum-verified by `benchmarks/fetch_reference.py`).

## Architecture

```
data/raw/gharchive/2026-10-25/*.json.gz
   | repopulse.streaming.producer  (confluent-kafka, one line = one message)
   v
Kafka topic gharchive-2026-10-25-p<N>   (N partitions, RF 3, min.insync.replicas 2)
   3 nodes apache/kafka, KRaft combined broker+controller  (benchmarks/kafka/docker-compose.yml)
   | repopulse.streaming.consumer   (spark.readStream.format("kafka"), trigger availableNow)
   v
bronze Delta table + checkpoint     (data/streaming/p<N>/delta, data/streaming/p<N>/checkpoint)
   | repopulse.streaming.reconcile
   v
one runs.csv row per N, only when every count matches
```

### Kafka cluster

[`benchmarks/kafka/docker-compose.yml`](../benchmarks/kafka/docker-compose.yml): three services
`kafka-1..3`, official image `apache/kafka` with a pinned version (the same tag is
`KAFKA_IMAGE` in `benchmarks/streaming_bench.py`; a test checks they agree).

- KRaft combined mode: every node is `broker,controller`; `KAFKA_CONTROLLER_QUORUM_VOTERS` lists
  the three nodes; one shared `CLUSTER_ID`. No external coordination service.
- Listeners: `CONTROLLER :9093` (quorum), `INTERNAL :9092` advertised as `kafka-N:9092`
  (broker to broker), `EXTERNAL` advertised as `localhost:19092` / `29092` / `39092` (the runner).
- Durable defaults: internal topics RF 3, `min.insync.replicas=2`, auto topic creation off,
  `message.max.bytes` 10 MiB (a few GH Archive events exceed Kafka's 1 MB default).
- Heap 1 GiB per broker; a healthcheck (`kafka-broker-api-versions.sh`) lets
  `docker compose up -d --wait` block until all three answer.
- Each broker is its own compose service without `container_name`, so the fault test (P2b) can
  stop one with `docker compose -f benchmarks/kafka/docker-compose.yml kill kafka-2`. The fault
  test that does this is described in [`streaming-fault.md`](streaming-fault.md).

### Producer (`repopulse/streaming/producer.py`)

- Every line of every hour file becomes one message; the value is the line exactly as in the
  file (without the newline). Nothing is dropped: a line that is not a JSON object with an `id`
  is still produced, with the fallback key `sha256:<hex of the line>`, and counted as `unkeyed`.
- Key = the event `id`, so all copies of one event land in the same partition (needed for a later
  per-key dedup and for the fault test).
- librdkafka settings (`producer_config`): `acks=all`, `enable.idempotence=true`,
  `compression.type=lz4`, `linger.ms=20`.
- `delivered` counts only messages with a successful delivery report (acknowledged by the
  in-sync replicas). Failed and still-queued messages are counted apart.
- The client is injected (`ProducerClient` protocol); unit tests use a fake. `confluent_kafka`
  (extra `streaming`) is imported only by `ConfluentProducer` and the admin functions in
  `repopulse/streaming/topics.py`.

### Consumer (`repopulse/streaming/consumer.py`)

- Source: `spark.readStream.format("kafka")` with `subscribe=<topic>`,
  `startingOffsets=earliest`, `failOnDataLoss=true`, `maxOffsetsPerTrigger` (default 200000, CLI
  `--max-offsets-per-trigger`).
- Sink: Delta, `outputMode("append")`, `checkpointLocation`, trigger `availableNow`.
- Session: `local[*]`, Delta extensions, the Kafka connector
  `org.apache.spark:spark-sql-kafka-0-10_2.12:<installed pyspark version>` added with
  `configure_spark_with_delta_pip(..., extra_packages=[...])` (benchmark session only, not the
  test fixture), `spark.sql.shuffle.partitions=8`, driver memory 6g, UTC.
- Spark reads one Kafka partition per task, so N also bounds the read parallelism.

**Exactly-once to the sink.** The offsets of each micro-batch are written to the checkpoint
(`offsets/<batch>`) before the batch runs; the Delta sink commits each batch with the query id
and the batch id as its transaction id and skips a batch id it has already committed. A batch
replayed after a crash (Delta written, `commits/<batch>` lost) therefore adds no rows, and a rerun
with the same checkpoint only reads offsets after the last committed batch. Both cases are tested
(`tests/test_streaming_consumer.py`, with a file stream source in place of Kafka).

#### Bronze columns

| column | type | meaning |
|---|---|---|
| `raw_json` | string | message value verbatim (one GH Archive line) |
| `raw_sha256` | string | SHA-256 of `raw_json` (same rule as the batch bronze) |
| `event_id` | string | `$.id` of the JSON, null when the line has none |
| `kafka_key` | string | message key (event id or `sha256:<hex>`) |
| `kafka_topic` | string | topic |
| `kafka_partition` | int | partition |
| `kafka_offset` | bigint | offset in the partition |
| `kafka_timestamp` | timestamp | Kafka record timestamp (produce time) |
| `ingested_at` | timestamp | time the micro-batch was written |

This is a **bronze** table: raw JSON like the batch bronze of `repopulse/spark/batch.py`. PII
is not hashed here; hashing happens before silver, and this task builds no silver or gold. The
table is deleted at the end of each N.

### Reconciliation (`repopulse/streaming/reconcile.py`)

`batch_rows` is counted from the source files themselves (lines, independent of Kafka). A run
matches only when

```
batch_rows == produced (delivered) == topic messages (sum of high - low watermarks)
           == Delta rows == distinct (kafka_partition, kafka_offset) in Delta
```

`missing = batch_rows - distinct offsets`, `duplicates = Delta rows - distinct offsets`. A
mismatch prints the problems, writes no row for that N and makes the benchmark exit 1.

## What is timed

- `produce_seconds`: from reading the first line to the last delivery report (`flush`); includes
  gzip decoding and parsing the id of each line.
- `consume_seconds`: from starting the streaming query to its termination.
- `total_seconds` = produce + consume. Not timed: topic creation, the Spark session start
  (JVM + connector download), the reconciliation.

**What the number means.** Trigger `availableNow` processes every offset present when the query
starts (in micro-batches of at most `maxOffsetsPerTrigger`) and then stops; production is
finished before consumption starts. So `consume_events_per_s` is the rate at which Spark drains
an existing backlog into Delta, not an end-to-end latency of a continuously running stream, and
`produce_events_per_s` is the rate of one Python producer into a 3-broker cluster on one runner
(all three brokers share the runner's CPU and disk).

**One run per N, no warm-up run.** Every N is measured once (not a median), in the listed order,
in one Spark session. Whatever is initialised only once in that session (JIT, the first use of
the Kafka connector and of the Delta sink) is paid by the first N in its `consume_seconds`, so
compare the first N with the others with that in mind.

## Rows

`variant` = `streaming-throughput-p1`, `-p3`, `-p6`; `data_date` = reference day;
`row_count` = Delta rows; `bytes_read` = bytes of the produced values; `notes` is one line of
`key=value`: `partitions brokers rf min_isr produce_seconds consume_seconds
produce_events_per_s consume_events_per_s batch_rows match trigger micro_batches
max_offsets_per_trigger unkeyed kafka`.

## How to run

On GitHub: workflow **Streaming** (`.github/workflows/streaming.yml`), `workflow_dispatch`
(inputs `partitions`, default `1,3,6`; `max_hours`, default `24`) or a PR into `develop`/`main`
that touches the streaming files. Not a required check. Steps: cache + verify the reference day,
`docker compose ... up -d --wait`, the benchmark, broker logs on failure, job summary, artifact,
`docker compose ... down -v`.

Locally (Linux or WSL with Docker, Java 17):

```bash
pip install -e ".[dev,spark,streaming]"
python benchmarks/fetch_reference.py
docker compose -f benchmarks/kafka/docker-compose.yml up -d --wait
python benchmarks/streaming_bench.py --git-sha <sha> --partitions 1,3,6 --out out/streaming-rows.csv --summary out/streaming-summary.md
python benchmarks/streaming_bench.py --git-sha <sha> --partitions 3 --max-hours 1   # quick try
docker compose -f benchmarks/kafka/docker-compose.yml down -v
```

Disk: the reference day is about 295 MB compressed; with RF 3 every message is stored three times
(lz4-compressed) on the same disk. Topic, Delta table and checkpoint of one N are deleted before
the next N.

## Recording the results

The workflow uploads the artifact **`streaming-throughput`** (`out/streaming-rows.csv`,
`out/streaming-summary.md`; no raw data). Merge the rows into `runs.csv` (schema-checked):

```bash
gh run download <run-id> -n streaming-throughput -D out/streaming
python benchmarks/record_run.py out/streaming/streaming-rows.csv
python -m repopulse.readme_results
```
