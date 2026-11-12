"""Streaming fault tolerance: kill a Kafka broker or the Spark consumer in the middle of a run.

Two cases on the GH Archive reference day (real data, not simulated), each on its own topic
(N partitions, RF 3, ``min.insync.replicas=2``, like ``benchmarks/streaming_bench.py``):

``broker-kill``
    produce the day; once ``--kill-at-fraction`` of the lines are acknowledged, SIGKILL the
    broker leading partition 0 (``docker compose -f benchmarks/kafka/docker-compose.yml kill``)
    while the producer keeps going on the two others; time the failover (every partition led by
    a live broker); drain the topic into Delta with Spark (trigger ``availableNow``);
    reconcile by content; ``docker compose ... start`` the broker and time the re-sync (every
    ISR back to 3).

``consumer-kill``
    produce the day (no fault); run the Spark consumer in its own process
    (``python -m repopulse.streaming.consumer``, small ``maxOffsetsPerTrigger`` so the drain
    takes many micro-batches); once ``--kill-after-batches`` micro-batches are committed,
    SIGKILL its whole process group (Python and the JVM); start it again with the same
    checkpoint and Delta table; time the recovery; reconcile by content.

A measurement is valid only when the fault really happened in the middle (see
``repopulse.streaming.fault.broker_kill_problems`` / ``consumer_kill_problems``); an invalid one
writes no row. A valid one always writes its row with the real ``lost`` / ``duplicates``, and
the run exits 1 when either is not 0. Definitions: ``docs/streaming-fault.md``. What
``.github/workflows/streaming-fault.yml`` runs::

    python benchmarks/streaming_fault.py --git-sha <sha> --out out/streaming-fault-rows.csv \
        --summary out/streaming-fault-summary.md

Merge the artifact with ``python benchmarks/record_run.py out/streaming-fault-rows.csv``.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import shutil
import signal
import subprocess
import sys
import threading
import time
from array import array
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Protocol

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from benchmarks.reference import REFERENCE_TOML, load_reference  # noqa: E402
from benchmarks.streaming_bench import (  # noqa: E402
    BROKERS,
    DEFAULT_BOOTSTRAP,
    KAFKA_IMAGE,
    reference_files,
)
from repopulse.runs_log import COLUMNS, RunsLogError, append_run, validate_row  # noqa: E402
from repopulse.streaming import fault, producer, topics  # noqa: E402

CASES = ("broker-kill", "consumer-kill")
DEFAULT_CASES = ",".join(CASES)
DEFAULT_PARTITIONS = 3
DEFAULT_KILL_AT_FRACTION = 0.3
DEFAULT_KILL_AFTER_BATCHES = 5
DEFAULT_MAX_OFFSETS_PER_TRIGGER = 50_000  # many micro-batches, so the consumer kill lands mid-drain
DEFAULT_TIMEOUT = 600.0
DEFAULT_RESYNC_TIMEOUT = 600.0
DEFAULT_INTERVAL = 0.2
DEFAULT_WORK_DIR = Path("data/streaming-fault")
COMPOSE_FILE = Path("benchmarks/kafka/docker-compose.yml")
DRIVER_MEMORY = "4g"  # this process: broker-kill drain and the reconciliation reads
CONSUMER_DRIVER_MEMORY = "2g"  # the consumer subprocess of consumer-kill


class StreamingFaultError(RuntimeError):
    """A case could not be run (setup, Kafka, Spark or a consumer process failed)."""


class InvalidMeasurement(StreamingFaultError):
    """The fault did not land in the middle of the run: there is nothing to record."""


# ---------------------------------------------------------------- names and parsing


def variant(case: str) -> str:
    return f"streaming-fault-{case}"


def parse_cases(text: str) -> list[str]:
    """``"broker-kill,consumer-kill"`` -> list; known, distinct case names only."""
    items = [item.strip() for item in text.split(",")]
    if not text.strip() or any(not item for item in items):
        raise ValueError(f"case list must look like {DEFAULT_CASES}, got {text!r}")
    unknown = [item for item in items if item not in CASES]
    if unknown:
        raise ValueError(f"unknown case(s) {unknown}; known: {', '.join(CASES)}")
    if len(set(items)) != len(items):
        raise ValueError(f"cases must be distinct, got {text!r}")
    return items


def broker_service(broker_id: int) -> str:
    """Compose service of a broker: node id N is service ``kafka-N``."""
    return f"kafka-{int(broker_id)}"


def fault_topic(data_date: str, partitions: int, case: str) -> str:
    return f"{topics.topic_name(data_date, partitions)}-{case}"


def consumer_command(
    *,
    python: str,
    bootstrap: str,
    topic: str,
    cell_dir: Path,
    max_offsets_per_trigger: int,
    driver_memory: str,
    metrics_out: Path,
) -> list[str]:
    """Command line of one consumer run (same checkpoint and Delta table for every run)."""
    return [
        python,
        "-m",
        "repopulse.streaming.consumer",
        "--bootstrap",
        bootstrap,
        "--topic",
        topic,
        "--delta",
        str(Path(cell_dir) / "delta"),
        "--checkpoint",
        str(Path(cell_dir) / "checkpoint"),
        "--max-offsets-per-trigger",
        str(int(max_offsets_per_trigger)),
        "--driver-memory",
        driver_memory,
        "--metrics-out",
        str(metrics_out),
    ]


# ---------------------------------------------------------------- background work


class Task:
    """Handle of work run by ``run_inline`` or ``run_in_thread``; ``join`` re-raises its error."""

    def __init__(self) -> None:
        self.error: BaseException | None = None
        self._thread: threading.Thread | None = None

    def join(self) -> None:
        if self._thread is not None:
            self._thread.join()
        if self.error is not None:
            raise self.error


def run_inline(fn: Callable[[], None]) -> Task:
    """Run ``fn`` now (unit tests: deterministic with a fake clock)."""
    task = Task()
    try:
        fn()
    except Exception as exc:  # noqa: BLE001 - re-raised by join()
        task.error = exc
    return task


def run_in_thread(fn: Callable[[], None]) -> Task:
    """Run ``fn`` in a thread, so the producer keeps producing while a broker is killed."""
    task = Task()

    def target() -> None:
        try:
            fn()
        except Exception as exc:  # noqa: BLE001 - re-raised by join()
            task.error = exc

    task._thread = threading.Thread(target=target, name="streaming-fault", daemon=True)
    task._thread.start()
    return task


# ---------------------------------------------------------------- environment


class ConsumerRun(Protocol):
    def poll(self) -> int | None: ...
    def kill_hard(self) -> None: ...
    def wait(self, timeout: float | None = None) -> int | None: ...
    def metrics(self) -> Mapping[str, object]: ...


class FaultEnv(Protocol):
    """What the cases need from the cluster; ``RealEnv`` here, an in-memory fake in tests."""

    def brokers(self) -> frozenset[int]: ...
    def state(self, topic: str) -> fault.TopicState: ...
    def create_topic(self, topic: str, partitions: int) -> None: ...
    def delete_topic(self, topic: str) -> None: ...
    def producer(self) -> producer.ProducerClient: ...
    def topic_messages(self, topic: str) -> int: ...
    def kill_broker(self, broker: int) -> None: ...
    def start_broker(self, broker: int) -> None: ...
    def drain(self, topic: str, cell_dir: Path) -> Mapping[str, object]: ...
    def sink(self, cell_dir: Path) -> fault.SinkProfile: ...
    def delta_rows(self, cell_dir: Path) -> int: ...
    def launch_consumer(self, topic: str, cell_dir: Path, run: int) -> ConsumerRun: ...
    def committed(self, cell_dir: Path) -> set[int]: ...
    def offset_batches(self, cell_dir: Path) -> set[int]: ...


@dataclass
class CaseConfig:
    data_date: str
    partitions: int
    files: Sequence[Path]
    source: Mapping[bytes, int]  # fault.source_counts(files), computed once for every case
    kill_at_fraction: float = DEFAULT_KILL_AT_FRACTION
    kill_broker: int | None = None  # None: the leader of partition 0 when the case starts
    kill_after_batches: int = DEFAULT_KILL_AFTER_BATCHES
    timeout: float = DEFAULT_TIMEOUT
    resync_timeout: float = DEFAULT_RESYNC_TIMEOUT
    interval: float = DEFAULT_INTERVAL
    work_dir: Path = DEFAULT_WORK_DIR
    clock: Callable[[], float] = time.monotonic
    sleep: Callable[[float], None] = time.sleep
    spawn: Callable[[Callable[[], None]], Task] = run_in_thread
    source_rows: int = field(init=False)

    def __post_init__(self) -> None:
        self.source_rows = sum(int(v) for v in self.source.values())


@dataclass
class CaseResult:
    case: str
    recon: fault.ContentReconciliation | None
    total_seconds: float
    notes: dict[str, object] = field(default_factory=dict)
    problems: list[str] = field(default_factory=list)  # not empty: invalid measurement


def _wait(cfg: CaseConfig, check, what: str, *, timeout: float | None = None) -> float:
    return fault.wait_until(
        check,
        timeout=cfg.timeout if timeout is None else timeout,
        interval=cfg.interval,
        clock=cfg.clock,
        sleep=cfg.sleep,
        what=what,
    )


def _prepare(env: FaultEnv, cfg: CaseConfig, case: str) -> tuple[str, Path]:
    topic = fault_topic(cfg.data_date, cfg.partitions, case)
    cell = Path(cfg.work_dir) / case
    _wait(cfg, lambda: len(env.brokers()) >= BROKERS, f"{BROKERS} live brokers before {case}")
    shutil.rmtree(cell, ignore_errors=True)
    env.delete_topic(topic)  # leftover of an interrupted run
    env.create_topic(topic, cfg.partitions)
    return topic, cell


def _cleanup(env: FaultEnv, topic: str, cell: Path) -> None:
    try:
        env.delete_topic(topic)
    except Exception as exc:  # noqa: BLE001 - keep the original error, if any
        print(f"WARNING: could not delete topic {topic}: {exc}", file=sys.stderr)
    shutil.rmtree(cell, ignore_errors=True)


def _reconcile(env: FaultEnv, cfg: CaseConfig, cell: Path) -> fault.ContentReconciliation:
    sink = env.sink(cell)
    return fault.reconcile_content(
        cfg.source, sink.messages, sink_rows=sink.rows, distinct_offsets=sink.distinct_offsets
    )


# ---------------------------------------------------------------- case: broker-kill


def run_broker_kill(env: FaultEnv, cfg: CaseConfig) -> CaseResult:
    case = "broker-kill"
    topic, cell = _prepare(env, cfg, case)
    kill: dict[str, object] = {}
    started = False
    try:
        if cfg.kill_broker is not None:
            dead = int(cfg.kill_broker)
        else:
            dead = fault.leader_of(env.state(topic), 0)
        threshold = max(1, math.ceil(cfg.kill_at_fraction * cfg.source_rows))
        reports = array("d")  # clock time of every successful delivery report
        task: Task | None = None

        def after_report(summary, ok: bool) -> None:
            if ok:
                reports.append(cfg.clock())

        def do_kill(summary) -> None:
            kill["broker"] = dead
            env.kill_broker(dead)
            kill["at"] = cfg.clock()
            kill["sent"] = summary.lines
            kill["delivered"] = summary.delivered
            print(
                f"{case}: killed {broker_service(dead)} after {kill['delivered']} acknowledged, "
                f"{kill['sent']} sent of {cfg.source_rows}",
                flush=True,
            )
            kill["failover_at"] = _wait(
                cfg,
                lambda: fault.leaders_alive(env.state(topic), dead),
                f"failover: every partition of {topic} led by a live broker",
            )

        def after_line(summary) -> None:
            nonlocal task
            if task is None and summary.delivered >= threshold:
                task = cfg.spawn(lambda: do_kill(summary))

        t0 = cfg.clock()
        produced = producer.produce_files(
            env.producer(), topic, cfg.files, after_line=after_line, after_report=after_report
        )
        print(
            f"{case}: produced {produced.delivered}/{produced.lines} in {produced.seconds:.1f}s "
            f"(failed {produced.failed}, undelivered {produced.undelivered}) {produced.errors[:3]}",
            flush=True,
        )
        if task is not None:
            task.join()
        notes: dict[str, object] = {
            "killed_broker": broker_service(dead),
            "kill_at_fraction": cfg.kill_at_fraction,
            "kill_threshold": threshold,
            "delivered_at_kill": kill.get("delivered"),
            "sent_at_kill": kill.get("sent"),
            "produce_seconds": produced.seconds,
            "produced": produced.delivered,
            "produce_failed": produced.failed,
            "undelivered": produced.undelivered,
        }
        problems = fault.broker_kill_problems(
            source_rows=cfg.source_rows,
            killed="at" in kill,
            delivered_at_kill=kill.get("delivered"),
            sent_at_kill=kill.get("sent"),
        )
        if problems:
            return CaseResult(case, None, 0.0, notes=notes, problems=problems)

        kill_at = float(kill["at"])
        topic_messages = env.topic_messages(topic)  # needs a leader for every partition
        metrics = env.drain(topic, cell)
        recon = _reconcile(env, cfg, cell)

        restart_at = cfg.clock()
        env.start_broker(dead)
        started = True
        resynced_at = _wait(
            cfg,
            lambda: fault.fully_replicated(
                env.state(topic), rf=topics.DEFAULT_REPLICATION_FACTOR, brokers=BROKERS
            ),
            f"re-sync: {BROKERS} brokers and every ISR of {topic} back to "
            f"{topics.DEFAULT_REPLICATION_FACTOR}",
            timeout=cfg.resync_timeout,
        )
        notes = {
            "failover_seconds": float(kill["failover_at"]) - kill_at,
            "resync_seconds": resynced_at - restart_at,
            "max_delivery_gap_after_kill_seconds": fault.max_gap(reports, ends_after=kill_at),
            "max_delivery_gap_before_kill_seconds": fault.max_gap(reports, ends_by=kill_at),
            **notes,
            "kill_at_seconds": kill_at - t0,
            "consume_seconds": float(metrics["seconds"]),
            "topic_messages": topic_messages,
            "micro_batches_before_kill": 0,  # the drain starts after the kill
            "micro_batches_after_kill": int(metrics["micro_batches"]),
        }
        total = float(produced.seconds) + float(metrics["seconds"])
        return CaseResult(case, recon, total, notes=notes)
    finally:
        if "broker" in kill and not started:
            try:  # the next case needs three brokers again
                env.start_broker(int(kill["broker"]))
                _wait(
                    cfg,
                    lambda: len(env.brokers()) >= BROKERS,
                    f"{BROKERS} live brokers after {case}",
                    timeout=cfg.resync_timeout,
                )
            except Exception as exc:  # noqa: BLE001 - keep the original error, if any
                print(f"WARNING: could not restart the killed broker: {exc}", file=sys.stderr)
        _cleanup(env, topic, cell)


# ---------------------------------------------------------------- case: consumer-kill


def run_consumer_kill(env: FaultEnv, cfg: CaseConfig) -> CaseResult:
    case = "consumer-kill"
    topic, cell = _prepare(env, cfg, case)
    try:
        produced = producer.produce_files(env.producer(), topic, cfg.files)
        if not produced.ok:
            raise StreamingFaultError(
                f"{case}: the fault-free production failed: delivered {produced.delivered}/"
                f"{produced.lines}, failed {produced.failed}, undelivered {produced.undelivered} "
                f"{produced.errors[:3]}"
            )
        topic_messages = env.topic_messages(topic)

        launched_1 = cfg.clock()
        run1 = env.launch_consumer(topic, cell, 1)
        try:
            _wait(
                cfg,
                lambda: (
                    run1.poll() is not None or len(env.committed(cell)) >= cfg.kill_after_batches
                ),
                f"{cfg.kill_after_batches} committed micro-batch(es) of consumer run 1",
            )
        except BaseException:
            run1.kill_hard()
            raise
        alive = run1.poll() is None
        if alive:
            run1.kill_hard()
        code_1 = run1.wait(timeout=120)
        killed_at = cfg.clock()
        if not alive and code_1 != 0:
            raise StreamingFaultError(f"{case}: consumer run 1 exited with code {code_1}")
        before = env.committed(cell)
        notes: dict[str, object] = {
            "kill_after_batches": cfg.kill_after_batches,
            "micro_batches_before_kill": len(before),
            "uncommitted_batches_at_kill": len(env.offset_batches(cell) - before),
            "rows_before_restart": env.delta_rows(cell),
            "run1_seconds": killed_at - launched_1,
            "produce_seconds": produced.seconds,
            "produced": produced.delivered,
            "topic_messages": topic_messages,
        }
        problems = fault.consumer_kill_problems(
            finished_before_kill=code_1 == 0, batches_before_kill=len(before)
        )
        if problems:
            return CaseResult(case, None, 0.0, notes=notes, problems=problems)
        print(f"{case}: killed consumer run 1 after {len(before)} committed batch(es)", flush=True)

        launched_2 = cfg.clock()
        run2 = env.launch_consumer(topic, cell, 2)
        samples: list[tuple[float, set[int]]] = []

        def drained() -> bool:
            samples.append((cfg.clock(), env.committed(cell)))
            return run2.poll() is not None

        try:
            _wait(cfg, drained, "consumer run 2 to drain the topic and stop")
        except BaseException:
            run2.kill_hard()
            raise
        exited_at = cfg.clock()
        code_2 = run2.wait(timeout=120)
        if code_2 != 0:
            raise StreamingFaultError(f"{case}: consumer run 2 exited with code {code_2}")
        samples.append((exited_at, env.committed(cell)))
        first, last = fault.new_commit_times(samples, before)
        after = env.committed(cell) - before
        input_rows = int(run2.metrics()["input_rows"])
        problems = fault.consumer_kill_problems(
            finished_before_kill=False,
            batches_before_kill=len(before),
            input_rows_after_restart=input_rows,
            batches_after_restart=len(after),
        )
        notes = {
            "restart_first_commit_seconds": None if first is None else first - launched_2,
            "restart_drain_seconds": None if last is None else last - launched_2,
            **notes,
            "micro_batches_after_kill": len(after),
            "input_rows_after_restart": input_rows,
            "run2_seconds": exited_at - launched_2,
        }
        if problems:
            return CaseResult(case, None, 0.0, notes=notes, problems=problems)
        recon = _reconcile(env, cfg, cell)
        total = float(produced.seconds) + float(notes["run1_seconds"]) + (exited_at - launched_2)
        return CaseResult(case, recon, total, notes=notes)
    finally:
        _cleanup(env, topic, cell)


# ---------------------------------------------------------------- rows


def _fmt(value: object) -> str:
    if value is None:
        return "none"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, float):
        return str(round(value, 3))
    return str(value)


def build_row(
    result: CaseResult,
    *,
    data_date: str,
    git_sha: str,
    run_at: str,
    partitions: int,
    hours: int,
    max_offsets_per_trigger: int,
) -> dict:
    """One ``runs.csv`` row with the real counts; refuses an invalid measurement."""
    if result.problems:
        raise InvalidMeasurement(f"{variant(result.case)}: " + "; ".join(result.problems))
    if result.recon is None:
        raise InvalidMeasurement(f"{variant(result.case)}: no reconciliation")
    notes = {
        "case": result.case,
        **result.recon.as_notes(),
        **result.notes,
        "partitions": partitions,
        "brokers": BROKERS,
        "rf": topics.DEFAULT_REPLICATION_FACTOR,
        "min_isr": topics.DEFAULT_MIN_INSYNC_REPLICAS,
        "hours": hours,
        "max_offsets_per_trigger": max_offsets_per_trigger,
        "kafka": KAFKA_IMAGE,
    }
    row = {
        "run_at": run_at,
        "git_sha": git_sha,
        "variant": variant(result.case),
        "data_date": data_date,
        "total_seconds": round(float(result.total_seconds), 3),
        "row_count": result.recon.sink_rows,
        "notes": " ".join(f"{k}={_fmt(v)}" for k, v in notes.items()),
    }
    validate_row(row)
    return row


def _write_summary(path: Path, status: list[tuple[str, str]], rows: list[dict]) -> None:
    lines = ["### Streaming fault tolerance (Kafka KRaft 3 brokers -> Spark -> Delta, GH Archive)"]
    lines += ["", "| variant | status |", "|---|---|"]
    lines += [f"| `{cell}` | {text.splitlines()[0] if text else ''} |" for cell, text in status]
    lines += ["", f"{len(rows)} row(s) for runs.csv:", "", "```csv", ",".join(COLUMNS)]
    lines += [",".join(validate_row(r)[c] for c in COLUMNS) for r in rows]
    lines += ["```", ""]
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines), encoding="utf-8")


def run_cases(
    cases: Sequence[str],
    *,
    run_one: Callable[[str], CaseResult],
    data_date: str,
    git_sha: str,
    run_at: str,
    out: Path,
    summary: Path,
    partitions: int,
    hours: int,
    max_offsets_per_trigger: int,
) -> int:
    """Run every case; a row per valid measurement. Exit 1 on any failure, invalid case, loss or
    duplicate (the row of a loss or duplicate is still written, with its numbers)."""
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(",".join(COLUMNS) + "\n", encoding="utf-8", newline="")
    rows: list[dict] = []
    status: list[tuple[str, str]] = []
    failed = False
    for case in cases:
        cell = variant(case)
        try:
            result = run_one(case)
            row = build_row(
                result,
                data_date=data_date,
                git_sha=git_sha,
                run_at=run_at,
                partitions=partitions,
                hours=hours,
                max_offsets_per_trigger=max_offsets_per_trigger,
            )
        except InvalidMeasurement as exc:
            failed = True
            status.append((cell, f"INVALID, no row: {exc}"))
            print(f"ERROR {cell}: invalid measurement, no row written: {exc}", file=sys.stderr)
            continue
        except Exception as exc:  # noqa: BLE001 - one case failing must not stop the other
            failed = True
            message = f"{type(exc).__name__}: {exc}"
            status.append((cell, f"FAILED: {message}"))
            print(f"ERROR {cell}: {message}", file=sys.stderr, flush=True)
            continue
        append_run(out, row)
        rows.append(row)
        recon = result.recon
        if recon.ok:
            status.append((cell, f"ok, lost 0, duplicates 0, total {row['total_seconds']} s"))
        else:
            failed = True
            detail = "; ".join(recon.problems())
            status.append(
                (
                    cell,
                    f"VIOLATION recorded: lost={recon.lost} duplicates={recon.duplicates}: "
                    f"{detail}",
                )
            )
            bar = "!" * 78
            print(
                f"{bar}\nWARNING {cell}: delivery guarantee VIOLATED: lost={recon.lost} "
                f"duplicates={recon.duplicates} ({detail}). The row is recorded as measured.\n"
                f"{bar}",
                file=sys.stderr,
                flush=True,
            )
        print(f"{cell}: {row['notes']}", flush=True)
    _write_summary(summary, status, rows)
    print(f"wrote {len(rows)} row(s) to {out}")
    return 1 if failed else 0


# ---------------------------------------------------------------- real cluster (Linux runner)


class DockerCompose:
    def __init__(self, compose_file: Path) -> None:
        self.compose_file = Path(compose_file)

    def _run(self, *args: str) -> None:
        cmd = ["docker", "compose", "-f", str(self.compose_file), *args]
        print("$ " + " ".join(cmd), flush=True)
        subprocess.run(cmd, check=True, timeout=180)

    def kill(self, service: str) -> None:
        self._run("kill", service)  # SIGKILL by default

    def start(self, service: str) -> None:
        self._run("start", service)


class ConsumerProcess:
    """``python -m repopulse.streaming.consumer`` in its own session (process group).

    PySpark starts the JVM as a child of the Python process; killing only the Python process
    lets the JVM see its stdin close and exit through its shutdown hooks. ``kill_hard`` sends
    SIGKILL to the whole group instead (POSIX only; this class is not used by unit tests).
    """

    def __init__(self, cmd: list[str], *, log_path: Path, metrics_path: Path) -> None:
        self.log_path = Path(log_path)
        self.metrics_path = Path(metrics_path)
        self.log_path.parent.mkdir(parents=True, exist_ok=True)
        self._log = open(self.log_path, "wb")  # closed in wait()
        kwargs = {"start_new_session": True} if os.name == "posix" else {}
        self._killed = False
        self._proc = subprocess.Popen(cmd, stdout=self._log, stderr=subprocess.STDOUT, **kwargs)

    def poll(self) -> int | None:
        return self._proc.poll()

    def kill_hard(self) -> None:
        if self._proc.poll() is not None:
            return
        self._killed = True
        if os.name == "posix":
            try:
                os.killpg(self._proc.pid, signal.SIGKILL)  # pgid == pid (new session)
            except ProcessLookupError:
                pass
        else:
            self._proc.kill()

    def wait(self, timeout: float | None = None) -> int | None:
        code = self._proc.wait(timeout)
        self._log.close()
        if code != 0 and not self._killed:
            tail = self.log_path.read_text(encoding="utf-8", errors="replace").splitlines()[-60:]
            print(f"--- last lines of {self.log_path}:", *tail, sep="\n", file=sys.stderr)
        return code

    def metrics(self) -> Mapping[str, object]:
        return json.loads(self.metrics_path.read_text(encoding="utf-8"))


def _ids(path: Path) -> set[int]:
    try:
        return fault.batch_ids(os.listdir(path))
    except FileNotFoundError:
        return set()


class RealEnv:
    """``FaultEnv`` on the compose cluster: confluent-kafka admin, docker compose, Spark."""

    def __init__(
        self,
        *,
        bootstrap: str,
        compose_file: Path,
        max_offsets_per_trigger: int,
        consumer_driver_memory: str = CONSUMER_DRIVER_MEMORY,
        log_dir: Path,
    ) -> None:
        self.bootstrap = bootstrap
        self.compose = DockerCompose(compose_file)
        self.max_offsets_per_trigger = max_offsets_per_trigger
        self.consumer_driver_memory = consumer_driver_memory
        self.log_dir = Path(log_dir)
        self._spark = None
        self._admin_client = None

    @property
    def spark(self):
        if self._spark is None:
            from repopulse.streaming import consumer

            self._spark = consumer.create_spark_session(
                "repopulse-streaming-fault", driver_memory=DRIVER_MEMORY
            )
        return self._spark

    def close(self) -> None:
        if self._spark is not None:
            self._spark.stop()

    def _admin(self):
        if self._admin_client is None:
            from confluent_kafka.admin import AdminClient

            self._admin_client = AdminClient({"bootstrap.servers": self.bootstrap})
        return self._admin_client

    def brokers(self) -> frozenset[int]:
        return frozenset(int(b) for b in self._admin().list_topics(timeout=10).brokers)

    def state(self, topic: str) -> fault.TopicState:
        meta = self._admin().list_topics(topic, timeout=10)
        tmeta = meta.topics.get(topic)
        if tmeta is None or tmeta.error is not None:
            raise RuntimeError(f"topic {topic}: {getattr(tmeta, 'error', 'no metadata')}")
        return fault.TopicState(
            brokers=frozenset(int(b) for b in meta.brokers),
            partitions={
                int(p): fault.PartitionState(
                    leader=int(pm.leader), isr=tuple(int(i) for i in pm.isrs)
                )
                for p, pm in tmeta.partitions.items()
            },
        )

    def create_topic(self, topic: str, partitions: int) -> None:
        topics.create_topic(self.bootstrap, topics.topic_config(topic, partitions))

    def delete_topic(self, topic: str) -> None:
        topics.delete_topic(self.bootstrap, topic)

    def producer(self) -> producer.ProducerClient:
        return producer.ConfluentProducer(self.bootstrap)

    def topic_messages(self, topic: str) -> int:
        return topics.topic_message_count(topics.watermarks(self.bootstrap, topic))

    def kill_broker(self, broker: int) -> None:
        self.compose.kill(broker_service(broker))

    def start_broker(self, broker: int) -> None:
        self.compose.start(broker_service(broker))

    def drain(self, topic: str, cell_dir: Path) -> Mapping[str, object]:
        from repopulse.streaming import consumer

        source = consumer.read_kafka(
            self.spark,
            self.bootstrap,
            topic,
            max_offsets_per_trigger=self.max_offsets_per_trigger,
        )
        return consumer.run_stream(
            source, delta_path=cell_dir / "delta", checkpoint_path=cell_dir / "checkpoint"
        )

    def sink(self, cell_dir: Path) -> fault.SinkProfile:
        from repopulse.streaming import consumer

        counts = consumer.delta_counts(self.spark, cell_dir / "delta")
        return fault.SinkProfile(
            rows=counts["rows"],
            distinct_offsets=counts["distinct_offsets"],
            messages=consumer.delta_message_counts(self.spark, cell_dir / "delta"),
        )

    def delta_rows(self, cell_dir: Path) -> int:
        from repopulse.streaming import consumer

        if not (cell_dir / "delta" / "_delta_log").is_dir():
            return 0
        return consumer.delta_counts(self.spark, cell_dir / "delta")["rows"]

    def launch_consumer(self, topic: str, cell_dir: Path, run: int) -> ConsumerRun:
        cell = Path(cell_dir).resolve()
        metrics = cell / f"run{run}-metrics.json"
        cmd = consumer_command(
            python=sys.executable,
            bootstrap=self.bootstrap,
            topic=topic,
            cell_dir=cell,
            max_offsets_per_trigger=self.max_offsets_per_trigger,
            driver_memory=self.consumer_driver_memory,
            metrics_out=metrics,
        )
        print("$ " + " ".join(cmd), flush=True)
        return ConsumerProcess(
            cmd, log_path=self.log_dir / f"consumer-run{run}.log", metrics_path=metrics
        )

    def committed(self, cell_dir: Path) -> set[int]:
        return _ids(Path(cell_dir) / "checkpoint" / "commits")

    def offset_batches(self, cell_dir: Path) -> set[int]:
        return _ids(Path(cell_dir) / "checkpoint" / "offsets")


# ---------------------------------------------------------------- CLI


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Kill a broker / the consumer mid-run.")
    parser.add_argument("--bootstrap", default=DEFAULT_BOOTSTRAP)
    parser.add_argument("--cases", default=DEFAULT_CASES, help=f"e.g. {DEFAULT_CASES}")
    parser.add_argument("--git-sha", default=None)
    parser.add_argument("--reference", type=Path, default=REFERENCE_TOML)
    parser.add_argument("--data-dir", type=Path, help="default: data/raw/gharchive/<data_date>")
    parser.add_argument("--max-hours", type=int, default=24, help="first N hours only (1..24)")
    parser.add_argument("--partitions", type=int, default=DEFAULT_PARTITIONS)
    parser.add_argument(
        "--kill-at-fraction",
        type=float,
        default=DEFAULT_KILL_AT_FRACTION,
        help="broker-kill: kill once this share of the lines is acknowledged (0 < f < 1)",
    )
    parser.add_argument(
        "--kill-broker",
        default="auto",
        choices=["auto", *(str(i) for i in range(1, BROKERS + 1))],
        help="broker-kill: node id to kill; auto = leader of partition 0",
    )
    parser.add_argument(
        "--kill-after-batches",
        type=int,
        default=DEFAULT_KILL_AFTER_BATCHES,
        help="consumer-kill: kill once this many micro-batches are committed",
    )
    parser.add_argument(
        "--max-offsets-per-trigger", type=int, default=DEFAULT_MAX_OFFSETS_PER_TRIGGER
    )
    parser.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT, help="seconds per wait")
    parser.add_argument("--resync-timeout", type=float, default=DEFAULT_RESYNC_TIMEOUT)
    parser.add_argument("--compose-file", type=Path, default=COMPOSE_FILE)
    parser.add_argument("--work-dir", type=Path, default=DEFAULT_WORK_DIR)
    parser.add_argument("--out", type=Path, default=Path("out/streaming-fault-rows.csv"))
    parser.add_argument("--summary", type=Path, default=Path("out/streaming-fault-summary.md"))
    args = parser.parse_args(argv)

    try:
        cases = parse_cases(args.cases)
    except ValueError as exc:
        parser.error(str(exc))
    if not 1 <= args.max_hours <= 24:
        parser.error("--max-hours must be 1..24")
    if args.partitions < 1:
        parser.error("--partitions must be >= 1")
    if not 0 < args.kill_at_fraction < 1:
        parser.error("--kill-at-fraction must be strictly between 0 and 1")
    if args.kill_after_batches < 1:
        parser.error("--kill-after-batches must be >= 1")
    if args.max_offsets_per_trigger < 1:
        parser.error("--max-offsets-per-trigger must be >= 1")
    if args.timeout <= 0 or args.resync_timeout <= 0:
        parser.error("--timeout and --resync-timeout must be > 0")

    ref = load_reference(args.reference)
    data_date = ref["data_date"]
    run_at = datetime.now(UTC).isoformat(timespec="seconds")
    try:  # fail on bad metadata now, before Kafka and Spark
        if not args.git_sha:
            raise RunsLogError("git_sha: --git-sha is required")
        for case in cases:
            validate_row(
                {
                    "run_at": run_at,
                    "git_sha": args.git_sha,
                    "variant": variant(case),
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
    source = fault.source_counts(files)
    print(f"source: {len(files)} file(s), {sum(source.values())} line(s)", flush=True)

    cfg = CaseConfig(
        data_date=data_date,
        partitions=args.partitions,
        files=files,
        source=source,
        kill_at_fraction=args.kill_at_fraction,
        kill_broker=None if args.kill_broker == "auto" else int(args.kill_broker),
        kill_after_batches=args.kill_after_batches,
        timeout=args.timeout,
        resync_timeout=args.resync_timeout,
        work_dir=args.work_dir,
    )
    env = RealEnv(
        bootstrap=args.bootstrap,
        compose_file=args.compose_file,
        max_offsets_per_trigger=args.max_offsets_per_trigger,
        log_dir=args.work_dir / "logs",
    )
    runners = {"broker-kill": run_broker_kill, "consumer-kill": run_consumer_kill}
    try:
        return run_cases(
            cases,
            run_one=lambda case: runners[case](env, cfg),
            data_date=data_date,
            git_sha=args.git_sha,
            run_at=run_at,
            out=args.out,
            summary=args.summary,
            partitions=args.partitions,
            hours=len(files),
            max_offsets_per_trigger=args.max_offsets_per_trigger,
        )
    finally:
        env.close()


if __name__ == "__main__":
    raise SystemExit(main())
