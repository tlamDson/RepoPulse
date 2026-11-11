"""Unit tests never reach MotherDuck: ``conftest.py`` wraps ``duckdb.connect`` for every test.

A broken guard in product code (e.g. the ``_SMOKE_DB`` regex of ``repopulse.pipeline``) must turn
a test red, not drop a real database. Every test that would connect to ``md:`` first asserts the
wrapper is in place, so without it the test stops before any network call.
"""

from __future__ import annotations

import duckdb
import pytest


def _guarded() -> bool:
    return getattr(duckdb.connect, "blocks_motherduck", False) is True


def test_duckdb_connect_is_guarded():
    assert getattr(duckdb.connect, "blocks_motherduck", False) is True


@pytest.mark.parametrize(
    "target", ["md:", "md:repopulse", "MD:repopulse_smoke", "motherduck:repopulse", "  md:x"]
)
def test_motherduck_targets_raise_before_connecting(target):
    assert _guarded(), "duckdb.connect is not guarded; refusing to try a MotherDuck target"
    with pytest.raises(AssertionError, match="MotherDuck"):
        duckdb.connect(target)


def test_memory_database_still_connects():
    assert _guarded()
    con = duckdb.connect(":memory:")
    try:
        assert con.execute("select 1").fetchone() == (1,)
    finally:
        con.close()


def test_default_database_still_connects():
    assert _guarded()
    con = duckdb.connect()
    try:
        assert con.execute("select 1").fetchone() == (1,)
    finally:
        con.close()


@pytest.mark.parametrize("as_path", [False, True], ids=["str", "Path"])
def test_local_file_still_connects_and_forwards_kwargs(tmp_path, as_path):
    assert _guarded()
    db = tmp_path / "local.duckdb"
    target = db if as_path else str(db)
    con = duckdb.connect(target)
    con.execute("create table t as select 1 as x")
    con.close()

    ro = duckdb.connect(target, read_only=True)
    try:
        assert ro.execute("select x from t").fetchone() == (1,)
        with pytest.raises(duckdb.Error):
            ro.execute("insert into t values (2)")
    finally:
        ro.close()
