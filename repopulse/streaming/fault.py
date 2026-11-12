"""Fault-tolerance measurements for the streaming path (pure Python: no Kafka, no Spark).

Used by ``benchmarks/streaming_fault.py`` (task P2b, ``docs/streaming-fault.md``).

**Content reconciliation.** The count check of ``reconcile.py`` cannot tell "one event lost and
another written twice" from "nothing went wrong". Here the source lines and the Delta rows are
compared as multisets of their SHA-256 (a GH Archive file can legitimately contain the same
line twice, so it is a multiset, not a set):

- ``offset_duplicates`` = sink rows - distinct (partition, offset) pairs: the sink wrote the same
  Kafka message more than once;
- after collapsing those, every distinct message is one copy of a content; per content key
  ``k`` with ``s[k]`` source copies and ``m[k]`` distinct messages:
  ``lost = sum(max(s[k] - m[k], 0))`` and ``content_duplicates = sum(max(m[k] - s[k], 0))``
  (the producer wrote one line at two offsets, or a content that is not in the source at all,
  counted again in ``unexpected``);
- ``duplicates = offset_duplicates + content_duplicates``, and always
  ``sink_rows == source_rows - lost + duplicates``.

**Recovery times** are differences of monotonic clock readings taken by the driver; the
helpers here decide when a condition holds (``leaders_alive``, ``fully_replicated``), wait for
it with an injected clock (``wait_until``), and compute gaps and commit times from samples.
"""

from __future__ import annotations

import hashlib
import time
from collections import Counter
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path

from repopulse.streaming.producer import iter_lines

Key = bytes | str


# ---------------------------------------------------------------- source and sink


def line_key(line: bytes) -> bytes:
    """SHA-256 digest of a line (the bronze ``raw_sha256`` is the hex of the same digest)."""
    return hashlib.sha256(line).digest()


def source_counts(files: Iterable[str | Path]) -> Counter:
    """Multiset of ``line_key`` over every line of ``files`` (same line split as the producer)."""
    return Counter(line_key(line) for path in files for line in iter_lines(path))


@dataclass(frozen=True)
class SinkProfile:
    rows: int
    distinct_offsets: int
    messages: Mapping[Key, int] | Iterable[tuple[Key, int]]  # content -> distinct offsets


def profile_rows(rows: Iterable[tuple[int, int, Key]]) -> SinkProfile:
    """``SinkProfile`` of sink rows ``(partition, offset, content key)`` (small inputs, tests)."""
    total = 0
    at: dict[tuple[int, int], Key] = {}
    for partition, offset, key in rows:
        total += 1
        where = (int(partition), int(offset))
        seen = at.setdefault(where, key)
        if seen != key:
            raise ValueError(f"two different contents at partition {partition} offset {offset}")
    return SinkProfile(rows=total, distinct_offsets=len(at), messages=Counter(at.values()))


@dataclass(frozen=True)
class ContentReconciliation:
    source_rows: int
    sink_rows: int
    distinct_offsets: int
    lost: int
    offset_duplicates: int
    content_duplicates: int
    unexpected: int

    @property
    def duplicates(self) -> int:
        return self.offset_duplicates + self.content_duplicates

    @property
    def ok(self) -> bool:
        return self.lost == 0 and self.duplicates == 0

    def problems(self) -> list[str]:
        out = []
        if self.lost:
            out.append(f"{self.lost} source line(s) lost (not in delta)")
        if self.offset_duplicates:
            out.append(f"{self.offset_duplicates} duplicate row(s) of one (partition, offset)")
        if self.content_duplicates:
            out.append(
                f"{self.content_duplicates} duplicate content row(s) at another offset"
                f" ({self.unexpected} not in the source)"
            )
        return out

    def as_notes(self) -> dict[str, int]:
        return {
            "lost": self.lost,
            "duplicates": self.duplicates,
            "offset_duplicates": self.offset_duplicates,
            "content_duplicates": self.content_duplicates,
            "unexpected": self.unexpected,
            "source_rows": self.source_rows,
            "delta_rows": self.sink_rows,
            "distinct_offsets": self.distinct_offsets,
        }


def reconcile_content(
    source: Mapping[Key, int],
    messages: Mapping[Key, int] | Iterable[tuple[Key, int]],
    *,
    sink_rows: int,
    distinct_offsets: int,
) -> ContentReconciliation:
    """Compare source copies with distinct sink messages per content (see the module doc).

    ``messages`` gives, once per content key, the number of distinct (partition, offset) pairs of
    the sink holding it; an iterator is consumed in one pass, so the sink side never has to fit
    in memory. ``source`` is not modified.
    """
    sink_rows, distinct_offsets = int(sink_rows), int(distinct_offsets)
    if sink_rows < distinct_offsets:
        raise ValueError(f"sink_rows {sink_rows} < distinct_offsets {distinct_offsets}")
    items = messages.items() if isinstance(messages, Mapping) else messages
    matched = content_dup = unexpected = total = 0
    for key, count in items:
        m = int(count)
        if m < 1:
            raise ValueError(f"message count must be >= 1, got {m}")
        total += m
        s = int(source.get(key, 0))
        matched += min(s, m)
        if m > s:
            content_dup += m - s
            if s == 0:
                unexpected += m
    if total != distinct_offsets:
        raise ValueError(f"messages sum to {total}, expected distinct_offsets {distinct_offsets}")
    source_rows = sum(int(v) for v in source.values())
    return ContentReconciliation(
        source_rows=source_rows,
        sink_rows=sink_rows,
        distinct_offsets=distinct_offsets,
        lost=source_rows - matched,
        offset_duplicates=sink_rows - distinct_offsets,
        content_duplicates=content_dup,
        unexpected=unexpected,
    )


# ---------------------------------------------------------------- cluster state


@dataclass(frozen=True)
class PartitionState:
    leader: int  # broker id, -1 when the partition has no leader
    isr: tuple[int, ...]


@dataclass(frozen=True)
class TopicState:
    brokers: frozenset[int]  # brokers in the cluster metadata (live, unfenced)
    partitions: Mapping[int, PartitionState]


def leader_of(state: TopicState, partition: int = 0) -> int:
    return state.partitions[partition].leader


def leaders_alive(state: TopicState, dead: int) -> bool:
    """Every partition has a leader that is a live broker other than ``dead`` (failover done)."""
    return bool(state.partitions) and all(
        p.leader >= 0 and p.leader != dead and p.leader in state.brokers
        for p in state.partitions.values()
    )


def fully_replicated(state: TopicState, *, rf: int, brokers: int) -> bool:
    """``brokers`` brokers registered and every partition's ISR has ``rf`` members (re-sync)."""
    return (
        len(state.brokers) >= brokers
        and bool(state.partitions)
        and all(len(set(p.isr)) >= rf for p in state.partitions.values())
    )


# ---------------------------------------------------------------- waiting and timing


def wait_until(
    check: Callable[[], bool],
    *,
    timeout: float,
    interval: float,
    clock: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
    what: str = "condition",
) -> float:
    """Poll ``check`` every ``interval`` s; return ``clock()`` when it first holds.

    An exception in ``check`` counts as "not yet" (a broker may be unreachable while it fails
    over). After ``timeout`` s raise ``TimeoutError`` naming ``what`` and the last error.
    """
    start = clock()
    last_error: Exception | None = None
    while True:
        try:
            if check():
                return clock()
        except Exception as exc:  # noqa: BLE001 - retried until the timeout
            last_error = exc
        if clock() - start >= timeout:
            detail = (
                f" (last error: {type(last_error).__name__}: {last_error})" if last_error else ""
            )
            raise TimeoutError(f"{what} not reached within {timeout} s{detail}")
        sleep(interval)


def max_gap(
    times: Sequence[float], *, ends_after: float | None = None, ends_by: float | None = None
) -> float | None:
    """Largest ``b - a`` over consecutive readings ``(a, b)`` with ``ends_after < b <= ends_by``.

    With ``ends_after`` = kill time this includes the gap that spans the kill. None when the
    window holds no pair.
    """
    best: float | None = None
    for a, b in zip(times, times[1:], strict=False):
        if ends_after is not None and b <= ends_after:
            continue
        if ends_by is not None and b > ends_by:
            continue
        gap = b - a
        if best is None or gap > best:
            best = gap
    return best


def batch_ids(names: Iterable[str]) -> set[int]:
    """Micro-batch ids from the file names of a checkpoint ``commits/`` or ``offsets/`` dir."""
    return {int(name) for name in names if name.isdigit()}


def new_commit_times(
    samples: Iterable[tuple[float, set[int]]], before: set[int]
) -> tuple[float | None, float | None]:
    """(first, last) sample time at which a commit id not in ``before`` appeared.

    ``samples`` are ``(clock time, committed batch ids)`` polls; "last" is the poll that first
    saw the final new id. (None, None) when no new commit was seen.
    """
    first = last = None
    known = set(before)
    for t, ids in samples:
        fresh = set(ids) - known
        if fresh:
            if first is None:
                first = t
            last = t
            known |= fresh
    return first, last


# ---------------------------------------------------------------- valid measurement or not


def broker_kill_problems(
    *,
    source_rows: int,
    killed: bool,
    delivered_at_kill: int | None,
    sent_at_kill: int | None,
) -> list[str]:
    """Why a broker-kill run did not put the fault in the middle of production ([] = valid)."""
    if not killed:
        return ["broker was not killed (kill threshold never reached during production)"]
    out = []
    if not delivered_at_kill:
        out.append("broker killed before any message was acknowledged")
    if sent_at_kill is None or sent_at_kill >= source_rows:
        out.append(
            f"broker killed after every line was sent ({sent_at_kill}/{source_rows}): "
            "the producer did not have to continue on two brokers"
        )
    return out


def consumer_kill_problems(
    *,
    finished_before_kill: bool,
    batches_before_kill: int,
    input_rows_after_restart: int | None = None,
    batches_after_restart: int | None = None,
) -> list[str]:
    """Why a consumer-kill run did not hit the middle of the drain ([] = valid).

    The restart fields are None when checked right after the kill, before the restart.
    """
    out = []
    if finished_before_kill:
        out.append("consumer finished (drained the topic) before it was killed")
    if batches_before_kill < 1:
        out.append("no micro-batch was committed before the kill")
    if input_rows_after_restart is not None and input_rows_after_restart < 1:
        out.append("restarted consumer read nothing: the topic was drained before the kill")
    if batches_after_restart is not None and batches_after_restart < 1:
        out.append("restarted consumer committed no micro-batch")
    return out
