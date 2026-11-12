# Streaming fault tolerance: kill a broker, kill the consumer

Task P2b in [`plan.md`](plan.md) (phase 2), built on the Kafka -> Spark -> Delta path of P2a
([`streaming.md`](streaming.md)). Two faults are injected in the middle of a run, and each run
measures how many events were lost or duplicated and how long recovery took. Measure:
`variant=streaming-fault-<case>` in `runs.csv`, one row per case: `streaming-fault-broker-kill`
and `streaming-fault-consumer-kill`.

**Numbers live only in `runs.csv`.** This page describes how they are produced; it quotes none.

**Data:** real GH Archive data (not simulated): the reference day of
[`benchmarks/reference.toml`](../benchmarks/reference.toml), downloaded and checksum-verified by
`benchmarks/fetch_reference.py`. The workflow uses all 24 hour files by default (`--max-hours`,
recorded as `hours=` in `notes`).

## Code

| file | role |
|---|---|
| `repopulse/streaming/fault.py` | pure: content reconciliation, cluster-state predicates, `wait_until`, gaps, commit times, the rules for an invalid measurement |
| `benchmarks/streaming_fault.py` | driver: the two cases, the `runs.csv` rows, the real environment (`docker compose`, confluent-kafka admin, Spark) |
| `repopulse/streaming/consumer.py` | `python -m repopulse.streaming.consumer`: one `availableNow` run in its own process; `delta_message_counts` for the reconciliation |
| `repopulse/streaming/producer.py` | `produce_files(..., after_line=, after_report=)` hooks used to trigger the kill and time delivery reports |
| `.github/workflows/streaming-fault.yml` | the workflow |

Every fault control (kill/start a broker, launch/kill a consumer, read metadata, read the
checkpoint, the clock) is passed to the cases as an object, so `tests/test_streaming_fault_bench.py`
runs both cases against an in-memory fake cluster without Docker, Kafka or Spark.

## Cluster and topics

The cluster of P2a (`benchmarks/kafka/docker-compose.yml`): three `apache/kafka` nodes in KRaft
combined mode (broker + controller each), so losing one node keeps a 2/3 controller quorum.
Each case gets its own topic `gharchive-<date>-p<N>-<case>` with N partitions (default 3, one
leader per broker), RF 3, `min.insync.replicas=2`. The producer is the one of P2a: `acks=all`,
idempotent, `delivery.timeout.ms` 300000. Before a case starts, the driver waits until the
cluster metadata lists three brokers. Topic, Delta table and checkpoint are deleted after each
case (runner disk is 14 GB).

## Case `broker-kill`

```
producer ──> topic (3 brokers) ──x kill kafka-K at kill_at_fraction acknowledged
                │  (producer continues on the two other brokers)
                v
           failover observed (metadata polled every 200 ms)
producer done ──> Spark drain (availableNow, 2 brokers) ──> Delta ──> reconcile by content
docker compose start kafka-K ──> re-sync observed (every ISR back to 3)
```

1. The broker to kill is the leader of partition 0 when the case starts (`--kill-broker auto`),
   or a chosen node id. Node id K is compose service `kafka-K`.
2. The producer produces every line of the day. After each line, the driver checks the number
   of acknowledged messages; when it reaches `kill_threshold` = ceil(`kill_at_fraction` x
   source lines) (default fraction `0.3`), a background thread runs
   `docker compose -f benchmarks/kafka/docker-compose.yml kill kafka-K` (SIGKILL to the
   container) and then polls the topic metadata. The producer does not stop.
3. After the producer has flushed, the topic is drained with Spark into a fresh Delta table
   (same consumer as P2a, bootstrap list still names all three brokers), and reconciled.
4. `docker compose ... start kafka-K` restarts the same container (its log directory is kept,
   the volume is only removed by `down -v`), and the driver polls until three brokers are in
   the metadata and every partition's ISR has three members. If the case fails before this step,
   the broker is still started and the driver waits for three brokers before the next case.

Recovery numbers (`notes`, seconds, monotonic clock of the driver process):

- `failover_seconds`: from the return of `docker compose kill` to the first metadata poll where
  every partition has a leader that is a live broker other than the killed one.
- `resync_seconds`: from just before `docker compose start` to the first metadata poll where
  three brokers are registered and every ISR of the topic has three members. It includes the
  container start, Kafka's log recovery after an unclean stop, and the replica catching up with
  everything produced while it was down.
- `max_delivery_gap_after_kill_seconds`: the longest time between two consecutive successful
  delivery reports of the producer where the later report came after the kill (this includes
  the gap that spans the kill). `max_delivery_gap_before_kill_seconds` is the same before the
  kill, as a reference for the normal spacing. `none` when there is no such pair.
- `kill_at_seconds`: from the start of production to the kill; `delivered_at_kill` and
  `sent_at_kill`: messages acknowledged / handed to the client when the kill returned.

`total_seconds` = `produce_seconds` + `consume_seconds` (production including the fault, plus the
Spark drain), like P2a. Not timed: topic creation, the Spark session start, the reconciliation,
the re-sync (it has its own number).

## Case `consumer-kill`

```
producer ──> topic (no fault) ──> consumer run 1 (own process, maxOffsetsPerTrigger small)
          commits/ has kill_after_batches entries ──x SIGKILL the process group
consumer run 2 (same checkpoint, same Delta table) ──> drained, stops ──> reconcile by content
```

1. The topic is produced with no fault (a failed or undelivered message fails the case).
2. Consumer run 1 is `python -m repopulse.streaming.consumer` in a child process started in a
   new session. `maxOffsetsPerTrigger` is small (default 50000, `--max-offsets-per-trigger`), so
   the drain takes many micro-batches. The driver polls `checkpoint/commits/` every 200 ms.
3. When `kill_after_batches` (default 5) micro-batches are committed, the driver sends SIGKILL to
   the whole process group. PySpark's JVM is a child of the Python process; killing only Python
   would let the JVM exit through its shutdown hooks, so the group kill is what makes it a hard
   crash. Whatever the JVM was doing (writing a Delta commit, a checkpoint file) is cut off.
4. Consumer run 2 is the same command with the same checkpoint and Delta table; it runs until
   `availableNow` has drained the topic and the process exits.

Recovery numbers (`notes`, seconds):

- `restart_first_commit_seconds`: from launching run 2 to the first poll that sees a commit id
  that was not there at the kill. It includes the process and JVM start, Spark session, Kafka
  connector and the replay of an uncommitted batch, if any.
- `restart_drain_seconds`: from launching run 2 to the first poll that saw its last new commit.
- `run1_seconds`: launch of run 1 to the kill; `run2_seconds`: launch of run 2 to its exit.
- `micro_batches_before_kill` (commit ids present at the kill), `micro_batches_after_kill` (new
  commit ids written by run 2), `uncommitted_batches_at_kill` (ids in `offsets/` without a
  commit: a batch planned or written but not committed, replayed by run 2),
  `rows_before_restart` (Delta rows after the kill), `input_rows_after_restart` (rows run 2 read
  from Kafka, from its query progress).

Poll resolution is 200 ms, so these times are upper bounds by up to one poll.

`total_seconds` = `produce_seconds` + `run1_seconds` + `run2_seconds` (two process and JVM starts
included).

## Content reconciliation (both cases)

The count check of P2a (`reconcile.py`) compares totals, so one lost event and another event
written twice cancel out. Here the source and the sink are compared by content
(`repopulse/streaming/fault.py`):

- Source: multiset of SHA-256 of every line of the hour files, split exactly like the producer.
  A multiset, because a GH Archive file may contain the same line more than once.
- Sink: per `raw_sha256`, the number of distinct `(kafka_partition, kafka_offset)` pairs in the
  Delta table (grouped in Spark, streamed to the driver).

Definitions, for a content key k with s(k) source copies and m(k) distinct sink messages:

- `offset_duplicates` = Delta rows - distinct `(partition, offset)` pairs: the sink wrote the
  same Kafka message more than once (exactly-once to the sink broken).
- `lost` = sum of max(s(k) - m(k), 0): source lines with no message in Delta (lost by the
  producer, the broker, or the sink).
- `content_duplicates` = sum of max(m(k) - s(k), 0): extra messages with the same content at
  another offset (for example a producer retry written twice), including `unexpected` messages
  whose content is in no source line.
- `duplicates` = `offset_duplicates` + `content_duplicates`.
- Always: `delta_rows` = `source_rows` - `lost` + `duplicates`.

## Valid measurement, violation, exit code

**Invalid: no row, exit 1.** A case where the fault did not land in the middle is not a
measurement of that fault, so it writes no row (never a "0 lost, 0 duplicates" row for a fault
that did not happen):

- `broker-kill`: the threshold was never reached, nothing was acknowledged before the kill, or
  every line had already been sent when the kill returned.
- `consumer-kill`: run 1 had finished (drained the topic) before the kill, no micro-batch was
  committed before the kill, run 2 read nothing, or run 2 committed nothing.

A timeout (failover, re-sync, a consumer that never commits), a consumer process that crashes,
or a Kafka/Spark error fails the case the same way (no row, exit 1). Every wait has a timeout
(`--timeout`, `--resync-timeout`).

**Valid: always a row with the real numbers.** `lost` and `duplicates` are measured, not
assumed: with `acks=all`, RF 3, `min.insync.replicas=2`, an idempotent producer and the
checkpointed Delta sink, both are expected to be 0, but the row is written with whatever was
measured. When either is not 0 the driver prints a WARNING banner, the summary marks the case
`VIOLATION`, and the run exits 1 so the workflow shows red; the row is still in the artifact.

## Rows

`variant` = `streaming-fault-broker-kill` or `streaming-fault-consumer-kill`; `data_date` =
reference day; `row_count` = Delta rows; `bytes_read` empty; `notes` is one line of `key=value`:
`case lost duplicates offset_duplicates content_duplicates unexpected source_rows delta_rows
distinct_offsets`, then the case numbers above (and `killed_broker kill_at_fraction
kill_threshold produced produce_failed undelivered topic_messages` for broker-kill,
`kill_after_batches produced topic_messages` for consumer-kill), then `partitions brokers rf
min_isr hours max_offsets_per_trigger kafka`. A missing number is `none`.

## Limits

- One run per case, not a median; recovery times vary between runs.
- The three brokers share one runner (CPU, disk, network namespace): a broker kill here is a
  process crash, not a machine or network failure, and the re-sync copies over loopback.
- The broker is killed with SIGKILL on the container (`docker compose kill`); a graceful stop,
  a network partition, a slow disk or a controller-only failure are not tested.
- In `broker-kill` the consumer runs after the fault, with two brokers; a consumer reading while
  a broker dies is not tested (Spark local mode does not retry tasks, so that design would
  measure Spark's task failure rather than Kafka's failover).
- In `consumer-kill` there is no broker fault, and the kill lands wherever the JVM is when the
  commit count is reached (between batches, or in the middle of the next one;
  `uncommitted_batches_at_kill` tells which).
- Times come from polling (200 ms) and from the driver's clock; Kafka's own timeouts (broker
  session timeout, `replica.lag.time.max.ms`) are image defaults and shape the numbers.

## How to run

On GitHub: workflow **Streaming fault tolerance** (`.github/workflows/streaming-fault.yml`),
`workflow_dispatch` (inputs `cases`, default `broker-kill,consumer-kill`; `max_hours`, default
`24`; `kill_at_fraction`, default `0.3`) or a PR into `develop`/`main` that touches
`benchmarks/streaming_fault.py`, `benchmarks/kafka/**` or `repopulse/streaming/**`. Not a
required check. `workflow_dispatch` only works once the file is on `main` (after a release).

Locally (Linux or WSL with Docker, Java 17):

```bash
pip install -e ".[dev,spark,streaming]"
python benchmarks/fetch_reference.py
docker compose -f benchmarks/kafka/docker-compose.yml up -d --wait
python benchmarks/streaming_fault.py --git-sha <sha> --out out/streaming-fault-rows.csv --summary out/streaming-fault-summary.md
python benchmarks/streaming_fault.py --git-sha <sha> --cases consumer-kill --max-hours 2 --max-offsets-per-trigger 10000 --kill-after-batches 2   # quick try: enough batches for a mid-drain kill
docker compose -f benchmarks/kafka/docker-compose.yml down -v
```

Consumer logs are in `data/streaming-fault/logs/`.

## Recording the results

The workflow uploads the artifact **`streaming-fault`** (`out/streaming-fault-rows.csv`,
`out/streaming-fault-summary.md`; no raw data). Merge the rows into `runs.csv` (schema-checked),
then regenerate the README results (the rows land in "Other runs"):

```bash
gh run download <run-id> -n streaming-fault -D out/streaming-fault
python benchmarks/record_run.py out/streaming-fault/streaming-fault-rows.csv
python -m repopulse.readme_results
```
