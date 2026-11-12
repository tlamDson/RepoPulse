"""Streaming throughput: GH Archive reference day -> Kafka (3 brokers, KRaft) -> Spark -> Delta.

For every partition count N (default 1, 3, 6):

1. create topic ``gharchive-<date>-pN`` (N partitions, RF 3, ``min.insync.replicas=2``);
2. produce every line of the reference day (key = event id, ``acks=all``, idempotent, lz4);
3. read the topic with Spark Structured Streaming (trigger ``availableNow``: drain every offset
   present at start, then stop) into a fresh bronze Delta table with its own checkpoint;
4. reconcile: lines in the source files == messages delivered == topic offsets == Delta rows ==
   distinct (partition, offset) in Delta. A mismatch exits 1 and writes no row for that N;
5. one row in the ``runs.csv`` format (``variant=streaming-throughput-pN``), then delete the
   topic, the Delta table and the checkpoint before the next N (runner disk is 14 GB).

``total_seconds`` = produce seconds + consume seconds (topic creation, the Spark session start
and the reconciliation are not timed). Needs a running cluster
(``docker compose -f benchmarks/kafka/docker-compose.yml up -d --wait``), Java 17 and
``pip install -e ".[dev,spark,streaming]"``. What ``.github/workflows/streaming.yml`` runs::

    python benchmarks/streaming_bench.py --git-sha <sha> --out out/streaming-rows.csv \
        --summary out/streaming-summary.md

Merge the artifact with ``python benchmarks/record_run.py out/streaming-rows.csv``.
"""

from __future__ import annotations

import argparse
import shutil
import sys
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from benchmarks.reference import REFERENCE_TOML, load_reference  # noqa: E402
from repopulse.runs_log import COLUMNS, RunsLogError, append_run, validate_row  # noqa: E402
from repopulse.streaming import reconcile, topics  # noqa: E402
from repopulse.streaming.producer import ProduceSummary  # noqa: E402

KAFKA_IMAGE = "apache/kafka:4.0.0"  # must equal the image in benchmarks/kafka/docker-compose.yml
BROKERS = 3
DEFAULT_BOOTSTRAP = "localhost:19092,localhost:29092,localhost:39092"
DEFAULT_PARTITIONS = "1,3,6"
DEFAULT_MAX_OFFSETS_PER_TRIGGER = 200_000
DEFAULT_WORK_DIR = Path("data/streaming")
TRIGGER = "availableNow"


class StreamingBenchError(RuntimeError):
    """A partition count failed (Kafka, Spark or the reconciliation)."""


@dataclass
class PartitionResult:
    partitions: int
    produce: ProduceSummary
    consume: Mapping[str, object]
    recon: reconcile.Reconciliation


# ---------------------------------------------------------------- pure helpers


def variant(partitions: int) -> str:
    return f"streaming-throughput-p{int(partitions)}"


def parse_partitions(text: str) -> list[int]:
    """``"1,3,6"`` -> ``[1, 3, 6]``; positive, distinct integers only."""
    items = [item.strip() for item in text.split(",")]
    if not text.strip() or any(not item for item in items):
        raise ValueError(f"partition list must look like 1,3,6, got {text!r}")
    try:
        values = [int(item) for item in items]
    except ValueError:
        raise ValueError(f"partition counts must be integers, got {text!r}") from None
    if any(v < 1 for v in values):
        raise ValueError(f"partition counts must be >= 1, got {text!r}")
    if len(set(values)) != len(values):
        raise ValueError(f"partition counts must be distinct, got {text!r}")
    return values


def reference_files(data_dir: Path, reference: Mapping, *, max_hours: int = 24) -> list[Path]:
    """The first ``max_hours`` hour files of the reference day, in ``reference.toml`` order."""
    if max_hours < 1:
        raise ValueError(f"max_hours must be >= 1, got {max_hours}")
    return [Path(data_dir) / entry["name"] for entry in reference["files"][:max_hours]]


def _rate(rows: int, seconds: float) -> float:
    return round(rows / seconds, 1) if seconds > 0 else 0.0


def build_row(
    result: PartitionResult,
    *,
    data_date: str,
    git_sha: str,
    run_at: str,
    max_offsets_per_trigger: int | None = None,
) -> dict:
    """One ``runs.csv`` row; refuses a run whose reconciliation does not match."""
    recon = result.recon
    if not recon.match:
        raise StreamingBenchError(
            f"{variant(result.partitions)}: reconciliation failed: " + "; ".join(recon.problems())
        )
    produce_s = round(float(result.produce.seconds), 3)
    consume_s = round(float(result.consume["seconds"]), 3)
    notes = {
        "partitions": result.partitions,
        "brokers": BROKERS,
        "rf": topics.DEFAULT_REPLICATION_FACTOR,
        "min_isr": topics.DEFAULT_MIN_INSYNC_REPLICAS,
        "produce_seconds": produce_s,
        "consume_seconds": consume_s,
        "produce_events_per_s": _rate(result.produce.delivered, produce_s),
        "consume_events_per_s": _rate(recon.delta_rows, consume_s),
        "batch_rows": recon.batch_rows,
        "match": "true",
        "trigger": TRIGGER,
        "micro_batches": result.consume["micro_batches"],
        "max_offsets_per_trigger": max_offsets_per_trigger or "none",
        "unkeyed": result.produce.unkeyed,
        "kafka": KAFKA_IMAGE,
    }
    row = {
        "run_at": run_at,
        "git_sha": git_sha,
        "variant": variant(result.partitions),
        "data_date": data_date,
        "total_seconds": round(produce_s + consume_s, 3),
        "bytes_read": int(result.produce.bytes),
        "row_count": recon.delta_rows,
        "notes": " ".join(f"{k}={v}" for k, v in notes.items()),
    }
    validate_row(row)
    return row


# ---------------------------------------------------------------- driver (testable, no Kafka)


def _write_summary(path: Path, status: list[tuple[str, str]], rows: list[dict]) -> None:
    lines = ["### Streaming throughput (Kafka KRaft 3 brokers -> Spark -> Delta, GH Archive)", ""]
    lines += ["| variant | status |", "|---|---|"]
    lines += [f"| `{cell}` | {text.splitlines()[0] if text else ''} |" for cell, text in status]
    lines += ["", f"{len(rows)} row(s) for runs.csv:", "", "```csv", ",".join(COLUMNS)]
    lines += [",".join(validate_row(r)[c] for c in COLUMNS) for r in rows]
    lines += ["```", ""]
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines), encoding="utf-8")


def run_benchmark(
    partitions: Sequence[int],
    *,
    run_one: Callable[[int], PartitionResult],
    data_date: str,
    git_sha: str,
    run_at: str,
    out: Path,
    summary: Path,
    max_offsets_per_trigger: int | None = None,
) -> int:
    """Run every N; one row per matched N. Exit code 1 when any N failed or did not match."""
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(",".join(COLUMNS) + "\n", encoding="utf-8", newline="")
    rows: list[dict] = []
    status: list[tuple[str, str]] = []
    failed = False
    for n in partitions:
        cell = variant(n)
        try:
            result = run_one(n)
            recon = result.recon
            if not recon.match:
                raise StreamingBenchError(
                    f"missing={recon.missing} duplicates={recon.duplicates}: "
                    + "; ".join(recon.problems())
                )
            row = build_row(
                result,
                data_date=data_date,
                git_sha=git_sha,
                run_at=run_at,
                max_offsets_per_trigger=max_offsets_per_trigger,
            )
        except Exception as exc:  # noqa: BLE001 - one N failing must not stop the others
            failed = True
            message = f"{type(exc).__name__}: {exc}"
            status.append((cell, f"FAILED: {message}"))
            print(f"ERROR {cell}: {message}", file=sys.stderr, flush=True)
            continue
        append_run(out, row)
        rows.append(row)
        status.append((cell, f"ok, total {row['total_seconds']} s, {row['row_count']} rows"))
        print(f"{cell}: {row['notes']}", flush=True)
    _write_summary(summary, status, rows)
    print(f"wrote {len(rows)} row(s) to {out}")
    return 1 if failed else 0


# ---------------------------------------------------------------- one N against a real cluster


def run_partition(
    n: int,
    *,
    spark,
    bootstrap: str,
    files: Sequence[Path],
    batch_rows: int,
    data_date: str,
    work_dir: Path,
    max_offsets_per_trigger: int | None,
) -> PartitionResult:
    from repopulse.streaming import consumer, producer

    topic = topics.topic_name(data_date, n)
    cell_dir = work_dir / f"p{n}"
    shutil.rmtree(cell_dir, ignore_errors=True)
    topics.delete_topic(bootstrap, topic)  # leftover of an interrupted run
    topics.create_topic(bootstrap, topics.topic_config(topic, n))
    try:
        produced = producer.produce_files(producer.ConfluentProducer(bootstrap), topic, files)
        print(
            f"{variant(n)}: produced {produced.delivered}/{produced.lines} in "
            f"{produced.seconds:.1f}s (failed {produced.failed}, undelivered "
            f"{produced.undelivered}, unkeyed {produced.unkeyed}) {produced.errors[:3]}",
            flush=True,
        )
        topic_messages = topics.topic_message_count(topics.watermarks(bootstrap, topic))
        source = consumer.read_kafka(
            spark, bootstrap, topic, max_offsets_per_trigger=max_offsets_per_trigger
        )
        metrics = consumer.run_stream(
            source, delta_path=cell_dir / "delta", checkpoint_path=cell_dir / "checkpoint"
        )
        counts = consumer.delta_counts(spark, cell_dir / "delta")
    finally:
        shutil.rmtree(cell_dir, ignore_errors=True)
        try:
            topics.delete_topic(bootstrap, topic)
        except Exception as exc:  # noqa: BLE001 - keep the original error, if any
            print(f"WARNING: could not delete topic {topic}: {exc}", file=sys.stderr)
    recon = reconcile.reconcile(
        batch_rows=batch_rows,
        produced=produced.delivered,
        topic_messages=topic_messages,
        delta_rows=counts["rows"],
        distinct_offsets=counts["distinct_offsets"],
    )
    return PartitionResult(partitions=n, produce=produced, consume=metrics, recon=recon)


# ---------------------------------------------------------------- CLI


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Kafka -> Spark -> Delta streaming throughput.")
    parser.add_argument("--bootstrap", default=DEFAULT_BOOTSTRAP)
    parser.add_argument("--partitions", default=DEFAULT_PARTITIONS, help="e.g. 1,3,6")
    parser.add_argument("--git-sha", default=None)
    parser.add_argument("--reference", type=Path, default=REFERENCE_TOML)
    parser.add_argument("--data-dir", type=Path, help="default: data/raw/gharchive/<data_date>")
    parser.add_argument("--max-hours", type=int, default=24, help="first N hours only (1..24)")
    parser.add_argument(
        "--max-offsets-per-trigger", type=int, default=DEFAULT_MAX_OFFSETS_PER_TRIGGER
    )
    parser.add_argument("--work-dir", type=Path, default=DEFAULT_WORK_DIR)
    parser.add_argument("--out", type=Path, default=Path("out/streaming-rows.csv"))
    parser.add_argument("--summary", type=Path, default=Path("out/streaming-summary.md"))
    args = parser.parse_args(argv)

    try:
        partitions = parse_partitions(args.partitions)
    except ValueError as exc:
        parser.error(str(exc))
    if not 1 <= args.max_hours <= 24:
        parser.error("--max-hours must be 1..24")
    if args.max_offsets_per_trigger < 1:
        parser.error("--max-offsets-per-trigger must be >= 1")

    ref = load_reference(args.reference)
    data_date = ref["data_date"]
    run_at = datetime.now(UTC).isoformat(timespec="seconds")
    try:  # fail on bad metadata now, before Kafka and Spark
        if not args.git_sha:
            raise RunsLogError("git_sha: --git-sha is required")
        for n in partitions:
            validate_row(
                {
                    "run_at": run_at,
                    "git_sha": args.git_sha,
                    "variant": variant(n),
                    "data_date": data_date,
                    "total_seconds": 0,
                }
            )
    except RunsLogError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2

    data_dir = args.data_dir or Path("data/raw/gharchive") / data_date
    files = reference_files(data_dir, ref, max_hours=args.max_hours)
    absent = [p.name for p in files if not p.is_file()]
    if absent:
        print(
            f"ERROR: {len(absent)} reference file(s) missing in {data_dir}: {', '.join(absent)} "
            "(run python benchmarks/fetch_reference.py)",
            file=sys.stderr,
        )
        return 1
    batch_rows = reconcile.count_lines(files)
    print(f"batch: {len(files)} file(s), {batch_rows} line(s)", flush=True)

    from repopulse.streaming import consumer

    spark = consumer.create_spark_session()
    try:
        return run_benchmark(
            partitions,
            run_one=lambda n: run_partition(
                n,
                spark=spark,
                bootstrap=args.bootstrap,
                files=files,
                batch_rows=batch_rows,
                data_date=data_date,
                work_dir=args.work_dir,
                max_offsets_per_trigger=args.max_offsets_per_trigger,
            ),
            data_date=data_date,
            git_sha=args.git_sha,
            run_at=run_at,
            out=args.out,
            summary=args.summary,
            max_offsets_per_trigger=args.max_offsets_per_trigger,
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
