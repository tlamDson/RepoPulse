"""Kafka topics for the streaming benchmark: pure configuration plus lazy admin calls.

``topic_config`` / ``topic_name`` / ``topic_message_count`` are pure and unit-tested. The admin
functions import ``confluent_kafka`` (extra ``streaming``) only when called and need a real
cluster (``benchmarks/kafka/docker-compose.yml``).
"""

from __future__ import annotations

import time
from collections.abc import Mapping

MAX_MESSAGE_BYTES = 10 * 1024 * 1024  # a few GH Archive events are larger than Kafka's 1 MB
DEFAULT_REPLICATION_FACTOR = 3
DEFAULT_MIN_INSYNC_REPLICAS = 2


def topic_name(data_date: str, partitions: int) -> str:
    """One topic per (reference day, partition count), e.g. ``gharchive-2026-10-25-p3``."""
    return f"gharchive-{data_date}-p{int(partitions)}"


def topic_config(
    name: str,
    partitions: int,
    *,
    replication_factor: int = DEFAULT_REPLICATION_FACTOR,
    min_insync_replicas: int = DEFAULT_MIN_INSYNC_REPLICAS,
) -> dict:
    """Arguments for ``NewTopic``: N partitions, RF 3, ``min.insync.replicas`` 2 by default."""
    if partitions < 1:
        raise ValueError(f"partitions must be >= 1, got {partitions}")
    if replication_factor < 1:
        raise ValueError(f"replication_factor must be >= 1, got {replication_factor}")
    if not 1 <= min_insync_replicas <= replication_factor:
        raise ValueError(
            f"min_insync_replicas must be 1..{replication_factor}, got {min_insync_replicas}"
        )
    return {
        "name": name,
        "num_partitions": int(partitions),
        "replication_factor": int(replication_factor),
        "config": {
            "min.insync.replicas": str(min_insync_replicas),
            "max.message.bytes": str(MAX_MESSAGE_BYTES),
        },
    }


def topic_message_count(watermarks: Mapping[int, tuple[int, int]]) -> int:
    """Messages in a topic from per-partition (low, high) watermarks.

    Valid for a non-transactional producer on a fresh topic without retention deletes: offsets
    are then contiguous, so high - low is the number of messages of the partition.
    """
    return sum(int(high) - int(low) for low, high in watermarks.values())


# ---------------------------------------------------------------- admin (needs confluent_kafka)


def _admin(bootstrap: str):
    from confluent_kafka.admin import AdminClient

    return AdminClient({"bootstrap.servers": bootstrap})


def create_topic(bootstrap: str, config: Mapping, *, timeout_s: float = 60.0) -> None:
    """Create the topic and wait until every partition has a leader."""
    from confluent_kafka.admin import NewTopic

    admin = _admin(bootstrap)
    new = NewTopic(
        config["name"],
        num_partitions=config["num_partitions"],
        replication_factor=config["replication_factor"],
        config=dict(config["config"]),
    )
    for future in admin.create_topics([new], request_timeout=timeout_s).values():
        future.result(timeout=timeout_s)
    deadline = time.monotonic() + timeout_s
    while True:
        meta = admin.list_topics(config["name"], timeout=10).topics.get(config["name"])
        ready = (
            meta is not None
            and meta.error is None
            and len(meta.partitions) == config["num_partitions"]
            and all(p.leader >= 0 for p in meta.partitions.values())
        )
        if ready:
            return
        if time.monotonic() > deadline:
            raise TimeoutError(f"topic {config['name']} has no leader for every partition")
        time.sleep(1)


def delete_topic(bootstrap: str, name: str, *, timeout_s: float = 60.0) -> None:
    """Delete the topic; a topic that does not exist is not an error."""
    from confluent_kafka import KafkaError, KafkaException

    admin = _admin(bootstrap)
    try:
        for future in admin.delete_topics([name], request_timeout=timeout_s).values():
            future.result(timeout=timeout_s)
    except KafkaException as exc:
        if exc.args and exc.args[0].code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
            return
        raise


def watermarks(bootstrap: str, topic: str, *, timeout_s: float = 30.0) -> dict[int, tuple]:
    """(low, high) offsets of every partition of ``topic`` (no consumer group is committed)."""
    from confluent_kafka import Consumer, TopicPartition

    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": "repopulse-reconcile",
            "enable.auto.commit": False,
        }
    )
    try:
        meta = consumer.list_topics(topic, timeout=timeout_s).topics[topic]
        if meta.error is not None:
            raise RuntimeError(f"topic {topic}: {meta.error}")
        return {
            p: tuple(consumer.get_watermark_offsets(TopicPartition(topic, p), timeout=timeout_s))
            for p in sorted(meta.partitions)
        }
    finally:
        consumer.close()
