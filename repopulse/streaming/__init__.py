"""Phase 2 streaming path: GH Archive events -> Kafka (KRaft, 3 brokers) -> Spark -> bronze Delta.

- ``producer``: one Kafka message per GH Archive line (key = event id), injected client.
- ``topics``: topic configuration (N partitions, RF 3, ``min.insync.replicas=2``) and admin calls.
- ``consumer``: Spark Structured Streaming from Kafka to a bronze Delta table (checkpoint,
  trigger ``availableNow``, exactly-once to the sink).
- ``reconcile``: compare batch line count, produced messages, topic offsets and Delta rows.

``confluent_kafka`` and ``pyspark`` are imported lazily, so the pure parts import without them.
Docs: ``docs/streaming.md``.
"""
