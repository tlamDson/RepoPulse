"""Produce GH Archive hour files to Kafka: one line, one message (nothing dropped silently).

- key = the event ``id`` (string or integer), so every copy of an event lands in the same
  partition; a line that is not a JSON object with an id (malformed, no id) is still produced,
  with the fallback key ``sha256:<hex of the line>``, and counted in ``unkeyed``.
- value = the line bytes exactly as in the file, without the trailing newline.
- The Kafka client is injected (``ProducerClient``): unit tests use a fake one. The real one
  (``ConfluentProducer``) imports ``confluent_kafka`` (extra ``streaming``) only when created.
- ``delivered`` counts only messages acknowledged by the brokers (delivery report without error);
  with ``acks=all`` and ``min.insync.replicas=2`` that means written to at least 2 replicas.
"""

from __future__ import annotations

import gzip
import hashlib
import json
import time
from collections.abc import Callable, Iterable, Iterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import Protocol

from repopulse.streaming.topics import MAX_MESSAGE_BYTES

COMPRESSIONS = ("lz4", "zstd")
FALLBACK_PREFIX = b"sha256:"
MAX_ERRORS_KEPT = 10

DeliveryCallback = Callable[[str | None, int | None], None]  # (error text or None, partition)


class ProducerClient(Protocol):
    def produce(
        self, topic: str, key: bytes | None, value: bytes, on_delivery: DeliveryCallback
    ) -> None: ...

    def flush(self, timeout: float) -> int:
        """Wait for delivery reports; return the number of messages still not delivered."""
        ...


@dataclass
class ProduceSummary:
    files: int = 0
    lines: int = 0
    delivered: int = 0
    failed: int = 0
    undelivered: int = 0
    unkeyed: int = 0
    bytes: int = 0
    seconds: float = 0.0
    per_partition: dict[int, int] = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return self.delivered == self.lines and not self.failed and not self.undelivered


# ---------------------------------------------------------------- lines and keys


def iter_lines(path: str | Path) -> Iterator[bytes]:
    """Lines of a plain or ``.gz`` NDJSON file, without the trailing ``\\n``."""
    path = Path(path)
    opener = gzip.open if path.suffix == ".gz" else open
    with opener(path, "rb") as fh:
        for line in fh:
            yield line[:-1] if line.endswith(b"\n") else line


def event_id(line: bytes) -> str | None:
    """The ``id`` of a GH Archive event line, or None when the line has no usable id."""
    try:
        event = json.loads(line)
    except (ValueError, RecursionError):
        return None
    if not isinstance(event, dict):
        return None
    value = event.get("id")
    if isinstance(value, bool) or not isinstance(value, str | int):
        return None
    text = str(value)
    return text or None


def event_key(line: bytes) -> bytes:
    """Message key: the event id, else ``sha256:<hex>`` of the line (deterministic)."""
    eid = event_id(line)
    if eid is not None:
        return eid.encode("utf-8")
    return FALLBACK_PREFIX + hashlib.sha256(line).hexdigest().encode("ascii")


# ---------------------------------------------------------------- produce


def produce_files(
    client: ProducerClient,
    topic: str,
    files: Iterable[str | Path],
    *,
    flush_timeout: float = 600.0,
    after_line: Callable[[ProduceSummary], None] | None = None,
    after_report: Callable[[ProduceSummary, bool], None] | None = None,
) -> ProduceSummary:
    """Produce every line of ``files`` (in order) to ``topic``; wait for every delivery report.

    Optional hooks (used by the fault test, ``benchmarks/streaming_fault.py``):
    ``after_line(summary)`` runs after each line is handed to the client, and
    ``after_report(summary, ok)`` after each delivery report has been counted.
    """
    summary = ProduceSummary()

    def on_delivery(error: str | None, partition: int | None) -> None:
        if error is not None:
            summary.failed += 1
            if len(summary.errors) < MAX_ERRORS_KEPT:
                summary.errors.append(error)
        else:
            summary.delivered += 1
            if partition is not None:
                summary.per_partition[partition] = summary.per_partition.get(partition, 0) + 1
        if after_report is not None:
            after_report(summary, error is None)

    start = time.perf_counter()
    for path in files:
        summary.files += 1
        for line in iter_lines(path):
            key = event_key(line)
            if key.startswith(FALLBACK_PREFIX):
                summary.unkeyed += 1
            summary.lines += 1
            summary.bytes += len(line)
            client.produce(topic, key, line, on_delivery)
            if after_line is not None:
                after_line(summary)
    summary.undelivered = int(client.flush(flush_timeout))
    summary.seconds = round(time.perf_counter() - start, 6)
    return summary


# ---------------------------------------------------------------- real client (confluent_kafka)


def producer_config(
    bootstrap: str, *, compression: str = "lz4", linger_ms: int = 20
) -> dict[str, object]:
    """librdkafka settings: durable (``acks=all``), idempotent, compressed."""
    if compression not in COMPRESSIONS:
        raise ValueError(f"compression must be one of {COMPRESSIONS}, got {compression!r}")
    return {
        "bootstrap.servers": bootstrap,
        "acks": "all",
        "enable.idempotence": True,
        "compression.type": compression,
        "linger.ms": linger_ms,
        "message.max.bytes": MAX_MESSAGE_BYTES,
        "queue.buffering.max.messages": 500_000,
        "delivery.timeout.ms": 300_000,
    }


class ConfluentProducer:
    """``ProducerClient`` backed by ``confluent_kafka.Producer`` (imported here, lazily)."""

    def __init__(self, bootstrap: str, **options) -> None:
        from confluent_kafka import Producer

        self._producer = Producer(producer_config(bootstrap, **options))

    def produce(
        self, topic: str, key: bytes | None, value: bytes, on_delivery: DeliveryCallback
    ) -> None:
        def report(err, msg) -> None:
            if err is not None:
                on_delivery(str(err), None)
            else:
                on_delivery(None, msg.partition())

        while True:
            try:
                self._producer.produce(topic, value=value, key=key, on_delivery=report)
                break
            except BufferError:  # local queue full: serve delivery reports, then retry
                self._producer.poll(0.5)
        self._producer.poll(0)

    def flush(self, timeout: float) -> int:
        return int(self._producer.flush(timeout))
