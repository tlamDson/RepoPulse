"""Reconciliation of the streaming path against the batch line count (pure Python)."""

from __future__ import annotations

import gzip
from pathlib import Path

from repopulse.streaming import reconcile

FIXTURE = Path(__file__).resolve().parent / "fixtures" / "gharchive_small" / "2026-01-05-0.json"


def test_count_lines_of_plain_and_gzip_files(tmp_path):
    gz = tmp_path / "a.json.gz"
    with gzip.open(gz, "wb") as fh:
        fh.write(b'{"id":"1"}\n{"id":"2"}\n{broken\n')
    assert reconcile.count_lines([FIXTURE]) == 20
    assert reconcile.count_lines([gz]) == 3
    assert reconcile.count_lines([FIXTURE, gz]) == 23


def test_count_lines_counts_a_last_line_without_newline(tmp_path):
    gz = tmp_path / "b.json.gz"
    with gzip.open(gz, "wb") as fh:
        fh.write(b'{"id":"1"}\n{"id":"2"}')
    assert reconcile.count_lines([gz]) == 2


def test_count_lines_of_empty_file_is_zero(tmp_path):
    gz = tmp_path / "c.json.gz"
    with gzip.open(gz, "wb") as fh:
        fh.write(b"")
    assert reconcile.count_lines([gz]) == 0


def test_everything_equal_is_a_match():
    r = reconcile.reconcile(
        batch_rows=100, produced=100, topic_messages=100, delta_rows=100, distinct_offsets=100
    )
    assert r.match is True
    assert r.missing == 0
    assert r.duplicates == 0
    assert r.problems() == []


def test_rows_missing_in_delta_are_reported():
    r = reconcile.reconcile(
        batch_rows=100, produced=100, topic_messages=100, delta_rows=97, distinct_offsets=97
    )
    assert r.match is False
    assert r.missing == 3
    assert r.duplicates == 0
    assert any("delta" in p for p in r.problems())


def test_duplicate_offsets_in_delta_are_reported():
    r = reconcile.reconcile(
        batch_rows=100, produced=100, topic_messages=100, delta_rows=104, distinct_offsets=100
    )
    assert r.match is False
    assert r.missing == 0
    assert r.duplicates == 4
    assert any("duplicate" in p for p in r.problems())


def test_producer_short_of_batch_is_a_mismatch():
    r = reconcile.reconcile(
        batch_rows=100, produced=99, topic_messages=99, delta_rows=99, distinct_offsets=99
    )
    assert r.match is False
    assert r.missing == 1
    assert any("produced" in p for p in r.problems())


def test_topic_offsets_different_from_produced_is_a_mismatch():
    r = reconcile.reconcile(
        batch_rows=100, produced=100, topic_messages=102, delta_rows=100, distinct_offsets=100
    )
    assert r.match is False
    assert r.missing == 0
    assert r.duplicates == 0
    assert any("topic" in p for p in r.problems())


def test_as_dict_has_every_count():
    r = reconcile.reconcile(
        batch_rows=5, produced=5, topic_messages=5, delta_rows=6, distinct_offsets=5
    )
    assert r.as_dict() == {
        "batch_rows": 5,
        "produced": 5,
        "topic_messages": 5,
        "delta_rows": 6,
        "distinct_offsets": 5,
        "missing": 0,
        "duplicates": 1,
        "match": False,
    }
