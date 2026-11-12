"""Reconcile the streaming path with the batch count (pure Python, no Kafka, no Spark).

``batch_rows`` is counted from the source files themselves (one line = one event, the same rule
as the batch bronze and the producer), so it does not depend on Kafka. The streaming run matches
when every stage saw exactly that many rows:

    batch_rows == produced == topic_messages == delta_rows == distinct_offsets

``distinct_offsets`` is the number of distinct (kafka_partition, kafka_offset) pairs in Delta:
``delta_rows - distinct_offsets`` rows are duplicates, ``batch_rows - distinct_offsets`` are
missing.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import asdict, dataclass
from pathlib import Path

from repopulse.streaming.producer import iter_lines


def count_lines(files: Iterable[str | Path]) -> int:
    """Lines of plain or ``.gz`` files; a last line without ``\\n`` counts too."""
    return sum(1 for path in files for _ in iter_lines(path))


@dataclass(frozen=True)
class Reconciliation:
    batch_rows: int
    produced: int
    topic_messages: int
    delta_rows: int
    distinct_offsets: int

    @property
    def missing(self) -> int:
        return max(self.batch_rows - self.distinct_offsets, 0)

    @property
    def duplicates(self) -> int:
        return max(self.delta_rows - self.distinct_offsets, 0)

    @property
    def match(self) -> bool:
        return not self.problems()

    def problems(self) -> list[str]:
        out = []
        if self.produced != self.batch_rows:
            out.append(f"produced {self.produced} != batch_rows {self.batch_rows}")
        if self.topic_messages != self.produced:
            out.append(f"topic offsets {self.topic_messages} != produced {self.produced}")
        if self.distinct_offsets != self.batch_rows:
            out.append(f"delta distinct offsets {self.distinct_offsets} != batch {self.batch_rows}")
        if self.duplicates:
            out.append(f"{self.duplicates} duplicate (partition, offset) row(s) in delta")
        return out

    def as_dict(self) -> dict:
        return {
            **asdict(self),
            "missing": self.missing,
            "duplicates": self.duplicates,
            "match": self.match,
        }


def reconcile(
    *, batch_rows: int, produced: int, topic_messages: int, delta_rows: int, distinct_offsets: int
) -> Reconciliation:
    return Reconciliation(
        batch_rows=int(batch_rows),
        produced=int(produced),
        topic_messages=int(topic_messages),
        delta_rows=int(delta_rows),
        distinct_offsets=int(distinct_offsets),
    )
