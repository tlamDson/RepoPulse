"""Fault-tolerance reconciliation and recovery times (pure Python: no Kafka, no Spark, no Docker).

GH Archive is real public data; the fixture lines are small hand-written look-alikes.
"""

from __future__ import annotations

import gzip
import hashlib
from collections import Counter
from pathlib import Path

import pytest

from repopulse.streaming import fault, reconcile

FIXTURE = Path(__file__).resolve().parent / "fixtures" / "gharchive_small" / "2026-01-05-0.json"


def key(text: str) -> bytes:
    return hashlib.sha256(text.encode()).digest()


def recon_rows(source_lines: list[str], sink_rows: list[tuple[int, int, str]]):
    """Reconcile source lines against sink rows (partition, offset, line)."""
    source = Counter(key(line) for line in source_lines)
    profile = fault.profile_rows((p, o, key(line)) for p, o, line in sink_rows)
    return fault.reconcile_content(
        source,
        profile.messages,
        sink_rows=profile.rows,
        distinct_offsets=profile.distinct_offsets,
    )


# ---------------------------------------------------------------- keys and source


def test_line_key_is_the_sha256_digest_of_the_line_bytes():
    assert fault.line_key(b'{"id":"1"}') == hashlib.sha256(b'{"id":"1"}').digest()


def test_source_counts_is_a_multiset_of_line_keys(tmp_path):
    gz = tmp_path / "h.json.gz"
    with gzip.open(gz, "wb") as fh:
        fh.write(b'{"id":"1"}\n{"id":"2"}\n{"id":"1"}\n')
    counts = fault.source_counts([gz])
    assert counts == Counter({fault.line_key(b'{"id":"1"}'): 2, fault.line_key(b'{"id":"2"}'): 1})
    assert sum(fault.source_counts([FIXTURE, gz]).values()) == 23


# ---------------------------------------------------------------- sink profile


def test_profile_rows_counts_rows_distinct_offsets_and_messages_per_content():
    rows = [(0, 0, key("a")), (0, 1, key("b")), (0, 0, key("a")), (1, 0, key("a"))]
    profile = fault.profile_rows(rows)
    assert profile.rows == 4
    assert profile.distinct_offsets == 3
    assert dict(profile.messages) == {key("a"): 2, key("b"): 1}


def test_profile_rows_rejects_two_contents_at_one_offset():
    with pytest.raises(ValueError):
        fault.profile_rows([(0, 0, key("a")), (0, 0, key("b"))])


# ---------------------------------------------------------------- content reconciliation


def test_identical_source_and_sink_have_no_loss_and_no_duplicate():
    r = recon_rows(["a", "b", "c"], [(0, 0, "a"), (0, 1, "b"), (1, 0, "c")])
    assert (r.lost, r.duplicates, r.offset_duplicates, r.content_duplicates) == (0, 0, 0, 0)
    assert r.ok is True
    assert r.problems() == []
    assert (r.source_rows, r.sink_rows, r.distinct_offsets) == (3, 3, 3)


def test_one_lost_and_one_duplicate_do_not_cancel_out():
    """The count-only check of P2a sees a match here; the content check must not."""
    source = ["a", "b", "c"]
    sink = [(0, 0, "a"), (0, 1, "b"), (0, 2, "b")]  # c lost, b produced twice (new offset)
    counts = reconcile.reconcile(
        batch_rows=3, produced=3, topic_messages=3, delta_rows=3, distinct_offsets=3
    )
    assert counts.match is True  # the blind spot
    r = recon_rows(source, sink)
    assert r.lost == 1
    assert r.duplicates == 1
    assert r.content_duplicates == 1
    assert r.offset_duplicates == 0
    assert r.ok is False
    assert any("lost" in p for p in r.problems())
    assert any("duplicate" in p for p in r.problems())


def test_repeated_lines_in_the_source_are_a_multiset_not_a_set():
    source = ["a", "a", "b"]  # the same line twice in the source is legitimate
    assert recon_rows(source, [(0, 0, "a"), (0, 1, "a"), (0, 2, "b")]).ok is True
    one_copy = recon_rows(source, [(0, 0, "a"), (0, 1, "b")])
    assert (one_copy.lost, one_copy.duplicates) == (1, 0)
    three = recon_rows(source, [(0, 0, "a"), (0, 1, "a"), (0, 2, "a"), (0, 3, "b")])
    assert (three.lost, three.duplicates, three.content_duplicates) == (0, 1, 1)


def test_the_sink_writing_one_message_twice_is_an_offset_duplicate():
    r = recon_rows(["a", "b"], [(0, 0, "a"), (0, 1, "b"), (0, 1, "b")])
    assert r.lost == 0
    assert r.offset_duplicates == 1
    assert r.content_duplicates == 0
    assert r.duplicates == 1


def test_offset_duplicate_does_not_hide_a_lost_copy_of_the_same_line():
    # source has "a" twice; only one copy reached Kafka but the sink wrote that message twice
    r = recon_rows(["a", "a"], [(0, 0, "a"), (0, 0, "a")])
    assert r.lost == 1
    assert r.offset_duplicates == 1
    assert r.duplicates == 1


def test_content_not_in_the_source_is_unexpected_and_counted_as_duplicate():
    r = recon_rows(["a"], [(0, 0, "a"), (0, 1, "zzz")])
    assert r.unexpected == 1
    assert r.content_duplicates == 1
    assert r.lost == 0


def test_everything_lost():
    r = recon_rows(["a", "b"], [])
    assert (r.lost, r.duplicates, r.sink_rows) == (2, 0, 0)


@pytest.mark.parametrize(
    ("source", "sink"),
    [
        (["a", "b", "c"], [(0, 0, "a"), (0, 1, "b"), (0, 2, "b")]),
        (["a", "a"], [(0, 0, "a"), (0, 0, "a")]),
        (["a", "b"], [(0, 0, "a"), (0, 1, "x"), (0, 1, "x"), (1, 5, "b")]),
        (["a"] * 5, [(0, 0, "a")]),
    ],
)
def test_sink_rows_equal_source_minus_lost_plus_duplicates(source, sink):
    r = recon_rows(source, sink)
    assert r.sink_rows == r.source_rows - r.lost + r.duplicates


def test_reconcile_content_accepts_an_iterator_of_unique_content_counts():
    source = Counter({key("a"): 1, key("b"): 1})
    r = fault.reconcile_content(
        source, iter([(key("a"), 1), (key("b"), 1)]), sink_rows=2, distinct_offsets=2
    )
    assert r.ok is True


def test_reconcile_content_rejects_inconsistent_sink_counts():
    source = Counter({key("a"): 1})
    with pytest.raises(ValueError):  # messages sum to 1 but 2 distinct offsets claimed
        fault.reconcile_content(source, {key("a"): 1}, sink_rows=2, distinct_offsets=2)
    with pytest.raises(ValueError):  # fewer rows than distinct offsets is impossible
        fault.reconcile_content(source, {key("a"): 1}, sink_rows=0, distinct_offsets=1)


def test_reconcile_content_does_not_modify_the_source_counter():
    source = Counter({key("a"): 2})
    fault.reconcile_content(source, {key("a"): 1}, sink_rows=1, distinct_offsets=1)
    assert source == Counter({key("a"): 2})


def test_as_notes_has_every_count():
    r = recon_rows(["a", "b", "c"], [(0, 0, "a"), (0, 1, "b"), (0, 2, "b")])
    assert r.as_notes() == {
        "lost": 1,
        "duplicates": 1,
        "offset_duplicates": 0,
        "content_duplicates": 1,
        "unexpected": 0,
        "source_rows": 3,
        "delta_rows": 3,
        "distinct_offsets": 3,
    }


# ---------------------------------------------------------------- cluster state


def state(brokers, parts):
    return fault.TopicState(
        brokers=frozenset(brokers),
        partitions={p: fault.PartitionState(leader=ld, isr=tuple(isr)) for p, (ld, isr) in parts},
    )


def test_leader_of_partition():
    s = state({1, 2, 3}, [(0, (2, [2, 1, 3])), (1, (3, [3, 1, 2]))])
    assert fault.leader_of(s, 0) == 2
    assert fault.leader_of(s, 1) == 3


def test_leaders_alive_after_failover():
    before = state({1, 2, 3}, [(0, (2, [2, 1, 3])), (1, (3, [3, 2, 1]))])
    assert fault.leaders_alive(before, dead=2) is False  # partition 0 still led by the dead one
    stale = state({1, 3}, [(0, (2, [2, 1, 3])), (1, (3, [3, 1]))])
    assert fault.leaders_alive(stale, dead=2) is False
    offline = state({1, 3}, [(0, (-1, [])), (1, (3, [3, 1]))])
    assert fault.leaders_alive(offline, dead=2) is False
    after = state({1, 3}, [(0, (1, [1, 3])), (1, (3, [3, 1]))])
    assert fault.leaders_alive(after, dead=2) is True
    assert fault.leaders_alive(state({1, 3}, []), dead=2) is False


def test_fully_replicated_needs_every_broker_and_a_full_isr_everywhere():
    full = state({1, 2, 3}, [(0, (1, [1, 2, 3])), (1, (3, [3, 1, 2]))])
    assert fault.fully_replicated(full, rf=3, brokers=3) is True
    short = state({1, 2, 3}, [(0, (1, [1, 3])), (1, (3, [3, 1, 2]))])
    assert fault.fully_replicated(short, rf=3, brokers=3) is False
    missing_broker = state({1, 3}, [(0, (1, [1, 2, 3]))])
    assert fault.fully_replicated(missing_broker, rf=3, brokers=3) is False
    assert fault.fully_replicated(state({1, 2, 3}, []), rf=3, brokers=3) is False


# ---------------------------------------------------------------- waiting and timing


class FakeClock:
    def __init__(self, start: float = 100.0):
        self.now = start
        self.sleeps: list[float] = []

    def __call__(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += seconds


def test_wait_until_returns_the_clock_time_when_the_check_first_passes():
    clock = FakeClock()
    answers = iter([False, False, False, True])
    t = fault.wait_until(
        lambda: next(answers), timeout=10, interval=0.5, clock=clock, sleep=clock.sleep
    )
    assert t == pytest.approx(101.5)
    assert clock.sleeps == [0.5, 0.5, 0.5]


def test_wait_until_raises_timeout_with_the_description():
    clock = FakeClock()
    with pytest.raises(TimeoutError, match="ISR back to 3"):
        fault.wait_until(
            lambda: False,
            timeout=3,
            interval=1,
            clock=clock,
            sleep=clock.sleep,
            what="ISR back to 3",
        )
    assert clock.now == pytest.approx(103.0)


def test_wait_until_treats_an_exception_in_the_check_as_not_yet():
    clock = FakeClock()
    calls = {"n": 0}

    def check():
        calls["n"] += 1
        if calls["n"] < 3:
            raise RuntimeError("broker unreachable")
        return True

    assert fault.wait_until(check, timeout=10, interval=1, clock=clock, sleep=clock.sleep) == 102.0


def test_wait_until_timeout_message_keeps_the_last_error():
    clock = FakeClock()

    def check():
        raise RuntimeError("all brokers down")

    with pytest.raises(TimeoutError, match="all brokers down"):
        fault.wait_until(check, timeout=2, interval=1, clock=clock, sleep=clock.sleep)


def test_max_gap_between_consecutive_delivery_reports():
    times = [0.0, 0.1, 0.2, 5.2, 5.3, 9.3, 9.4]
    assert fault.max_gap(times) == pytest.approx(5.0)
    assert fault.max_gap(times, ends_by=5.25) == pytest.approx(5.0)
    assert fault.max_gap(times, ends_by=5.0) == pytest.approx(0.1)
    # gaps that end after the kill, including the one that spans it
    assert fault.max_gap(times, ends_after=6.0) == pytest.approx(4.0)
    assert fault.max_gap(times, ends_after=9.35) == pytest.approx(0.1)


def test_max_gap_is_none_without_two_reports_in_the_window():
    assert fault.max_gap([]) is None
    assert fault.max_gap([1.0]) is None
    assert fault.max_gap([1.0, 2.0], ends_after=2.0) is None


def test_batch_ids_from_checkpoint_file_names():
    names = ["0", "1", ".1.crc", "12", "metadata", ".0.crc", "tmp-x"]
    assert fault.batch_ids(names) == {0, 1, 12}


def test_new_commit_times_after_a_restart():
    before = {0, 1, 2}
    samples = [
        (10.0, {0, 1, 2}),
        (10.5, {0, 1, 2}),
        (11.0, {0, 1, 2, 3}),
        (11.5, {0, 1, 2, 3}),
        (12.0, {0, 1, 2, 3, 4, 5}),
        (12.5, {0, 1, 2, 3, 4, 5}),
    ]
    assert fault.new_commit_times(samples, before) == (11.0, 12.0)
    assert fault.new_commit_times(samples[:2], before) == (None, None)


# ---------------------------------------------------------------- valid or not


def test_broker_kill_in_the_middle_of_production_is_valid():
    assert (
        fault.broker_kill_problems(
            source_rows=1000, killed=True, delivered_at_kill=300, sent_at_kill=400
        )
        == []
    )


def test_broker_kill_that_never_happened_is_invalid():
    problems = fault.broker_kill_problems(
        source_rows=1000, killed=False, delivered_at_kill=None, sent_at_kill=None
    )
    assert problems and "not killed" in problems[0]


def test_broker_kill_after_every_line_was_sent_is_invalid():
    problems = fault.broker_kill_problems(
        source_rows=1000, killed=True, delivered_at_kill=1000, sent_at_kill=1000
    )
    assert problems and any("after" in p for p in problems)


def test_broker_kill_before_any_delivery_is_invalid():
    problems = fault.broker_kill_problems(
        source_rows=1000, killed=True, delivered_at_kill=0, sent_at_kill=10
    )
    assert problems


def test_consumer_kill_between_batches_is_valid():
    assert (
        fault.consumer_kill_problems(
            finished_before_kill=False,
            batches_before_kill=3,
            input_rows_after_restart=500,
            batches_after_restart=4,
        )
        == []
    )


@pytest.mark.parametrize(
    ("kwargs", "word"),
    [
        ({"finished_before_kill": True, "batches_before_kill": 9}, "finished"),
        ({"finished_before_kill": False, "batches_before_kill": 0}, "no micro-batch"),
        (
            {
                "finished_before_kill": False,
                "batches_before_kill": 3,
                "input_rows_after_restart": 0,
                "batches_after_restart": 1,
            },
            "nothing",
        ),
        (
            {
                "finished_before_kill": False,
                "batches_before_kill": 3,
                "input_rows_after_restart": 10,
                "batches_after_restart": 0,
            },
            "no micro-batch",
        ),
    ],
)
def test_consumer_kill_that_did_not_hit_the_middle_is_invalid(kwargs, word):
    problems = fault.consumer_kill_problems(**kwargs)
    assert problems and any(word in p for p in problems)
