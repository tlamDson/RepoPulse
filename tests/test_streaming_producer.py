"""Kafka producer for GH Archive hour files, with an injected fake client (no Kafka, no network).

GH Archive is real public data; the fixture lines are small hand-written look-alikes.
"""

from __future__ import annotations

import gzip
import hashlib
import json
from pathlib import Path

import pytest

from repopulse.streaming import producer, topics

FIXTURE = Path(__file__).resolve().parent / "fixtures" / "gharchive_small" / "2026-01-05-0.json"


class FakeClient:
    """Records messages; acks them only on ``flush`` (like an async broker client)."""

    def __init__(self, partitions: int = 3, fail_keys: set[bytes] | None = None):
        self.partitions = partitions
        self.fail_keys = fail_keys or set()
        self.messages: list[tuple[str, bytes | None, bytes]] = []
        self._pending = []
        self.flushed = 0

    def produce(self, topic, key, value, on_delivery):
        self.messages.append((topic, key, value))
        self._pending.append((key, on_delivery))

    def flush(self, timeout=None):
        self.flushed += 1
        for key, cb in self._pending:
            if key in self.fail_keys:
                cb("KafkaError{code=MSG_TIMED_OUT}", None)
            else:
                cb(None, sum(key or b"") % self.partitions)
        self._pending.clear()
        return 0


def fixture_lines() -> list[bytes]:
    return FIXTURE.read_bytes().splitlines()


def write_gz(path: Path, lines: list[bytes]) -> Path:
    with gzip.open(path, "wb") as fh:
        fh.write(b"\n".join(lines) + b"\n")
    return path


def test_event_key_is_the_event_id():
    line = fixture_lines()[0]
    assert producer.event_key(line) == json.loads(line)["id"].encode("utf-8")


def test_event_key_is_stable_for_the_same_event():
    line = fixture_lines()[3]
    assert producer.event_key(line) == producer.event_key(bytes(line))


def test_event_key_falls_back_to_sha256_of_the_line_when_there_is_no_id():
    for raw in (b"{not json", b'{"type":"PushEvent"}', b"[1,2]", b'{"id":true}', b""):
        assert producer.event_id(raw) is None
        assert producer.event_key(raw) == b"sha256:" + hashlib.sha256(raw).hexdigest().encode()


def test_numeric_id_is_accepted_as_key():
    assert producer.event_key(b'{"id":12345,"type":"WatchEvent"}') == b"12345"


def test_iter_lines_reads_plain_and_gzip_without_newlines(tmp_path):
    lines = fixture_lines()
    gz = write_gz(tmp_path / "2026-01-05-0.json.gz", lines)
    assert list(producer.iter_lines(FIXTURE)) == lines
    assert list(producer.iter_lines(gz)) == lines
    assert all(not ln.endswith(b"\n") for ln in producer.iter_lines(gz))


def test_every_line_is_produced_once_with_its_id_as_key(tmp_path):
    lines = fixture_lines()
    gz = write_gz(tmp_path / "h0.json.gz", lines)
    client = FakeClient()
    summary = producer.produce_files(client, "gh-p3", [gz])
    assert [m[2] for m in client.messages] == lines
    assert {m[0] for m in client.messages} == {"gh-p3"}
    assert [m[1] for m in client.messages] == [json.loads(ln)["id"].encode() for ln in lines]
    assert client.flushed >= 1
    assert summary.lines == len(lines) == 20
    assert summary.delivered == 20
    assert summary.failed == 0
    assert summary.unkeyed == 0
    assert summary.bytes == sum(len(ln) for ln in lines)
    assert summary.seconds >= 0
    assert summary.ok is True


def test_bad_lines_are_produced_and_counted_not_dropped(tmp_path):
    good = fixture_lines()[:3]
    bad = [b"{broken json", b'{"type":"PushEvent","actor":{"id":1}}']
    src = write_gz(tmp_path / "h1.json.gz", [good[0], bad[0], good[1], bad[1], good[2]])
    client = FakeClient()
    summary = producer.produce_files(client, "t", [src])
    values = [m[2] for m in client.messages]
    assert values == [good[0], bad[0], good[1], bad[1], good[2]]
    assert summary.lines == 5
    assert summary.unkeyed == 2
    assert summary.delivered == 5
    keys = dict((m[2], m[1]) for m in client.messages)
    assert keys[bad[0]] == b"sha256:" + hashlib.sha256(bad[0]).hexdigest().encode()


def test_counts_per_partition_from_delivery_reports(tmp_path):
    client = FakeClient(partitions=3)
    summary = producer.produce_files(client, "t", [FIXTURE])
    expected: dict[int, int] = {}
    for _, key, _ in client.messages:
        p = sum(key) % 3
        expected[p] = expected.get(p, 0) + 1
    assert summary.per_partition == expected
    assert sum(summary.per_partition.values()) == 20


def test_failed_deliveries_are_not_counted_as_delivered():
    lines = fixture_lines()
    fail = {json.loads(lines[0])["id"].encode(), json.loads(lines[1])["id"].encode()}
    client = FakeClient(fail_keys=fail)
    summary = producer.produce_files(client, "t", [FIXTURE])
    assert summary.lines == 20
    assert summary.delivered == 18
    assert summary.failed == 2
    assert summary.ok is False
    assert summary.errors and "MSG_TIMED_OUT" in summary.errors[0]


def test_messages_left_in_queue_after_flush_count_as_undelivered():
    class StuckClient(FakeClient):
        def flush(self, timeout=None):
            self.flushed += 1
            return len(self._pending)

    summary = producer.produce_files(StuckClient(), "t", [FIXTURE])
    assert summary.delivered == 0
    assert summary.undelivered == 20
    assert summary.ok is False


def test_several_files_are_produced_in_order(tmp_path):
    lines = fixture_lines()
    a = write_gz(tmp_path / "a.json.gz", lines[:7])
    b = write_gz(tmp_path / "b.json.gz", lines[7:])
    client = FakeClient()
    summary = producer.produce_files(client, "t", [a, b])
    assert [m[2] for m in client.messages] == lines
    assert summary.files == 2


def test_producer_config_is_durable_idempotent_and_compressed():
    cfg = producer.producer_config("localhost:19092,localhost:29092")
    assert cfg["bootstrap.servers"] == "localhost:19092,localhost:29092"
    assert cfg["acks"] == "all"
    assert cfg["enable.idempotence"] is True
    assert cfg["compression.type"] in {"lz4", "zstd"}
    assert int(cfg["message.max.bytes"]) >= topics.MAX_MESSAGE_BYTES


def test_producer_config_rejects_unknown_compression():
    with pytest.raises(ValueError):
        producer.producer_config("localhost:19092", compression="brotli")


def test_topic_config_has_n_partitions_rf3_and_min_isr2():
    cfg = topics.topic_config("gharchive-2026-10-25-p6", 6)
    assert cfg["name"] == "gharchive-2026-10-25-p6"
    assert cfg["num_partitions"] == 6
    assert cfg["replication_factor"] == 3
    assert cfg["config"]["min.insync.replicas"] == "2"
    assert int(cfg["config"]["max.message.bytes"]) == topics.MAX_MESSAGE_BYTES


@pytest.mark.parametrize(
    "kwargs",
    [
        {"partitions": 0},
        {"partitions": -1},
        {"partitions": 3, "replication_factor": 0},
        {"partitions": 3, "replication_factor": 3, "min_insync_replicas": 4},
    ],
)
def test_topic_config_rejects_bad_values(kwargs):
    partitions = kwargs.pop("partitions")
    with pytest.raises(ValueError):
        topics.topic_config("t", partitions, **kwargs)


def test_topic_name_is_valid_for_kafka():
    name = topics.topic_name("2026-10-25", 3)
    assert name == "gharchive-2026-10-25-p3"


def test_after_line_hook_sees_the_summary_after_every_produced_line():
    class EagerClient(FakeClient):  # acks at once, like a fast broker
        def produce(self, topic, key, value, on_delivery):
            self.messages.append((topic, key, value))
            on_delivery(None, 0)

    seen = []
    producer.produce_files(
        EagerClient(), "t", [FIXTURE], after_line=lambda s: seen.append((s.lines, s.delivered))
    )
    assert seen == [(i, i) for i in range(1, 21)]


def test_after_report_hook_sees_every_delivery_report_with_its_outcome():
    lines = fixture_lines()
    fail = {json.loads(lines[0])["id"].encode()}
    reports = []
    summary = producer.produce_files(
        FakeClient(fail_keys=fail),
        "t",
        [FIXTURE],
        after_report=lambda s, ok: reports.append((ok, s.delivered, s.failed)),
    )
    assert len(reports) == 20
    assert reports[0] == (False, 0, 1)
    assert [ok for ok, _, _ in reports].count(True) == 19
    assert reports[-1][1:] == (summary.delivered, summary.failed)


def test_topic_message_count_sums_watermark_spans():
    assert topics.topic_message_count({0: (0, 10), 1: (0, 5), 2: (2, 2)}) == 15
    assert topics.topic_message_count({}) == 0
