"""dbt star schema and Q1-Q6 marts (transform/) checked against the naive baseline.

Input: the raw GH Archive fixtures ``tests/fixtures/gharchive_small`` (see test_naive_duckdb.py)
plus ``tests/fixtures/gharchive_scd2`` (actor 1004 renames dave -> dave2 on 2026-02-12 and
comments on spark issue #104; erin forks and opens PR #9 on ``DuckDB/DuckDB``, mixed case, to
exercise the case-insensitive seed match). Those two directories are exactly the CI seed
``fixture_silver_gharchive_events``.

The silver parquet also gets ``tests/fixtures/gharchive_parquet_only`` (not in the seed): a
WatchEvent of frank on spark, proving dbt really read the parquet, and actor 1010 commenting twice
in the same second as ``gina`` then ``gina2`` (zero-length first SCD2 version). The baseline runs
on the same raw lines as the parquet, so marts and queries still see the same events.

Silver dedupes by ``event_id`` while the baseline does not, so both sides get the same
deduplicated input: the duplicated line of event 9000000002 is dropped (first copy kept) before
the baseline runs and before the silver parquet is written. Silver is rebuilt here with DuckDB
using the column list of ``docs/spark-jobs.md`` (no Spark needed); dbt reads it through the var
``silver_gharchive_parquet``. Everything (DuckDB file, target, logs, profiles) lives in tmp.

dbt runs as a subprocess (``python -m dbt.cli.main``) a few times per session only: one build
(the session fixture ``built`` of conftest.py, shared with test_governance.py), one rebuild
(idempotency), and one small build per contract-violation case. Helpers: ``dbt_helpers.py``.
"""

from __future__ import annotations

import gzip
import json
import re
from datetime import datetime

import duckdb
import pytest

pytest.importorskip("dbt.cli.main", reason="dbt extra not installed: pip install -e '.[dbt]'")
pytest.importorskip("dbt.adapters.duckdb", reason="dbt-duckdb not installed")

from dbt_helpers import (  # noqa: E402
    GOLD_MODELS,
    GOLD_SCHEMA,
    MARTS,
    PROJECT,
    SEED_FIXTURE_DIRS,
    copy_project,
    dedup_lines,
    run_dbt,
    silver_select_sql,
    write_profiles,
)
from dbt_helpers import gold_counts as _gold_counts  # noqa: E402
from dbt_helpers import output as _output  # noqa: E402
from dbt_helpers import q as _q  # noqa: E402
from dbt_helpers import require_ok as _require_ok  # noqa: E402

PARQUET_ONLY_IDS = ("9000000050", "9000000051", "9000000052")
EMAIL = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")


# ---------------------------------------------------------------- session fixtures
# lake / built / baseline: conftest.py (shared with test_governance.py, test_dbt_semantic_layer.py)


@pytest.fixture
def connect_gold(built):
    """Read-only connection to the built gold DuckDB.

    Asserts the build first so a failed build reports as a test failure, not a setup error.
    """

    def connect() -> duckdb.DuckDBPyConnection:
        _require_ok(built["proc"])
        con = duckdb.connect(str(built["db"]), read_only=True)
        opened.append(con)
        return con

    opened: list[duckdb.DuckDBPyConnection] = []
    yield connect
    for con in opened:
        con.close()


# ---------------------------------------------------------------- build + marts == baseline


def test_dbt_build_succeeds_on_silver_fixture(built):
    _require_ok(built["proc"])


def test_baseline_input_has_no_duplicate_ids(lake):
    ids = [json.loads(line)["id"] for line in dedup_lines()]
    assert len(ids) == len(set(ids)) == 39  # 34 + 3 + 3 lines, minus the duplicate of 9000000002
    seed_ids = {json.loads(line)["id"] for line in dedup_lines(SEED_FIXTURE_DIRS)}
    assert len(seed_ids) == 36
    assert set(ids) - seed_ids == set(PARQUET_ONLY_IDS)


def test_ci_seed_equals_silver_built_from_fixture_json(tmp_path):
    raw = tmp_path / "events.json.gz"
    with gzip.open(raw, "wt", encoding="utf-8") as fh:
        fh.write("\n".join(dedup_lines(SEED_FIXTURE_DIRS)) + "\n")
    seed = (PROJECT / "seeds" / "fixture_silver_gharchive_events.csv").as_posix()
    con = duckdb.connect(":memory:")
    try:
        con.execute(f"CREATE TABLE expected AS {silver_select_sql(raw.as_posix())}")
        con.execute(
            "CREATE TABLE seed AS SELECT * FROM "
            f"read_csv({_q(seed)}, header = true, all_varchar = true)"
        )
        cols = [r[0] for r in con.execute("DESCRIBE expected").fetchall()]
        assert [r[0] for r in con.execute("DESCRIBE seed").fetchall()] == cols

        def rows(table: str) -> dict[str, tuple]:
            select = ", ".join(f'CAST("{c}" AS VARCHAR)' for c in cols)
            return {r[0]: r for r in con.execute(f"SELECT {select} FROM {table}").fetchall()}

        expected, actual = rows("expected"), rows("seed")
    finally:
        con.close()
    assert sorted(actual) == sorted(expected)
    for event_id, row in expected.items():
        for col, a, e in zip(cols, actual[event_id], row, strict=True):
            assert a == e, (event_id, col, a, e)


def test_staging_reads_the_parquet_var_not_the_seed(connect_gold, lake):
    gold = connect_gold()
    con = duckdb.connect(":memory:")
    try:
        parquet_rows = con.execute(
            f"SELECT count(*) FROM read_parquet({_q(lake['silver_glob'])})"
        ).fetchone()[0]
    finally:
        con.close()
    staged = gold.execute("SELECT count(*) FROM main_staging.stg_gharchive_events").fetchone()[0]
    assert staged == parquet_rows == 39  # the seed has 36
    in_fct = gold.execute(
        f"SELECT event_id FROM {GOLD_SCHEMA}.fct_events WHERE event_id IN "
        f"({', '.join(_q(i) for i in PARQUET_ONLY_IDS)}) ORDER BY event_id"
    ).fetchall()
    assert [r[0] for r in in_fct] == list(PARQUET_ONLY_IDS)


@pytest.mark.parametrize("q", sorted(MARTS))
def test_mart_matches_baseline_query(q, connect_gold, baseline):
    gold = connect_gold()
    mart, order_by = MARTS[q]
    expected_cols = list(baseline[q]["columns"])
    cur = gold.execute(f"SELECT * FROM {GOLD_SCHEMA}.{mart} LIMIT 0")
    assert [c[0] for c in cur.description] == expected_cols
    cols = ", ".join(expected_cols)
    actual = gold.execute(f"SELECT {cols} FROM {GOLD_SCHEMA}.{mart} ORDER BY {order_by}").fetchall()
    expected = [tuple(r) for r in baseline[q]["rows"]]
    assert expected, f"{q} baseline is empty on the fixture: comparison would be vacuous"
    assert len(actual) == len(expected), (actual, expected)
    for a_row, e_row in zip(actual, expected, strict=True):
        for a, e in zip(a_row, e_row, strict=True):
            if isinstance(e, float):
                assert a == pytest.approx(e, abs=1e-9), (a_row, e_row)
            else:
                assert a == e, (a_row, e_row)


# ---------------------------------------------------------------- star schema


def test_fct_events_keeps_each_seed_repo_event_once(connect_gold, lake):
    gold = connect_gold()
    con = duckdb.connect(":memory:")
    try:
        seed = (PROJECT / "seeds" / "repos_data_ecosystem.csv").as_posix()
        expected = con.execute(
            f"SELECT count(*) FROM read_parquet({_q(lake['silver_glob'])}) AS s "
            f"JOIN read_csv({_q(seed)}, header = true) AS r "
            "ON lower(s.repo_name) = lower(r.repo_name)"
        ).fetchone()[0]
    finally:
        con.close()
    total, distinct = gold.execute(
        f"SELECT count(*), count(DISTINCT event_id) FROM {GOLD_SCHEMA}.fct_events"
    ).fetchone()
    assert total == distinct == expected
    # 2 of the 39 fixture events are on alice/toy (not in the seed)
    assert total == 37


def test_dim_repo_matches_seed_case_insensitively(connect_gold):
    gold = connect_gold()
    rows = gold.execute(
        f"SELECT r.repo_name, count(*) FROM {GOLD_SCHEMA}.fct_events AS f "
        f"JOIN {GOLD_SCHEMA}.dim_repo AS r USING (repo_key) "
        "WHERE f.event_id IN ('9000000041', '9000000042') GROUP BY r.repo_name"
    ).fetchall()
    assert rows == [("duckdb/duckdb", 2)]  # silver name is 'DuckDB/DuckDB'
    seed_rows = sum(
        1 for _ in (PROJECT / "seeds" / "repos_data_ecosystem.csv").open(encoding="utf-8")
    )
    assert gold.execute(f"SELECT count(*) FROM {GOLD_SCHEMA}.dim_repo").fetchone()[0] == (
        seed_rows - 1
    )


def test_dim_user_scd2_versions_for_renamed_actor(connect_gold):
    gold = connect_gold()
    rows = gold.execute(
        "SELECT actor_login, version_number, valid_from, valid_to, is_current, is_bot "
        f"FROM {GOLD_SCHEMA}.dim_user WHERE actor_id = 1004 ORDER BY version_number"
    ).fetchall()
    assert rows == [
        # first seen: ForkEvent on spark 01-05 08:00; renamed at the 02-12 10:00 comment
        ("dave", 1, datetime(2026, 1, 5, 8), datetime(2026, 2, 12, 10), False, False),
        ("dave2", 2, datetime(2026, 2, 12, 10), None, True, False),
    ]


def test_dim_user_flags_bots_by_login_rule(connect_gold):
    gold = connect_gold()
    rows = dict(
        gold.execute(
            f"SELECT actor_login, is_bot FROM {GOLD_SCHEMA}.dim_user "
            "WHERE actor_login IN ('dependabot[bot]', 'Spark-CI-Bot', 'sparkmaint')"
        ).fetchall()
    )
    assert rows == {"dependabot[bot]": True, "Spark-CI-Bot": True, "sparkmaint": False}


def test_fct_events_point_at_user_version_valid_at_event_time(connect_gold):
    gold = connect_gold()
    rows = gold.execute(
        "SELECT f.event_id, u.actor_login FROM "
        f"{GOLD_SCHEMA}.fct_events AS f JOIN {GOLD_SCHEMA}.dim_user AS u USING (user_key) "
        "WHERE f.actor_id = 1004 ORDER BY f.created_at"
    ).fetchall()
    assert rows == [
        ("9000000021", "dave"),  # fork 01-05
        ("9000000016", "dave"),  # issue #106 01-09
        ("9000000040", "dave2"),  # comment 02-12 after the rename
    ]


def test_same_second_rename_keeps_each_event_on_its_own_login_version(connect_gold):
    gold = connect_gold()
    t = datetime(2026, 2, 13, 10)
    versions = gold.execute(
        "SELECT user_key, actor_login, valid_from, valid_to, is_current "
        f"FROM {GOLD_SCHEMA}.dim_user WHERE actor_id = 1010 ORDER BY version_number"
    ).fetchall()
    assert versions == [
        ("1010-1", "gina", t, t, False),  # zero-length: gina2 shows up in the same second
        ("1010-2", "gina2", t, None, True),
    ]
    facts = gold.execute(
        f"SELECT event_id, user_key FROM {GOLD_SCHEMA}.fct_events "
        "WHERE actor_id = 1010 ORDER BY event_id"
    ).fetchall()
    assert facts == [("9000000051", "1010-1"), ("9000000052", "1010-2")]


# ---------------------------------------------------------------- contract must fail the build


@pytest.mark.parametrize(
    "wrap",
    [
        pytest.param("SELECT *, 1 AS not_in_contract FROM ({sql}\n) AS m", id="extra-column"),
        pytest.param(
            "SELECT * REPLACE (CAST(1 AS INTEGER) AS repo_key) FROM ({sql}\n) AS m",
            id="wrong-type",
        ),
    ],
)
def test_contract_violation_fails_dbt_build(wrap, tmp_path):
    model = PROJECT / "models" / "gold" / "dim_repo.sql"
    assert model.is_file(), f"gold model {model} does not exist"
    project = copy_project(tmp_path / "transform")
    copy = project / "models" / "gold" / "dim_repo.sql"
    copy.write_text(wrap.format(sql=model.read_text(encoding="utf-8")), encoding="utf-8")
    profiles = write_profiles(tmp_path / "profiles", tmp_path / "contract.duckdb")
    proc = run_dbt(["build", "--select", "+dim_repo"], project, profiles, tmp_path)
    out = _output(proc)
    assert proc.returncode != 0, out[-4000:]
    assert "contract" in out.lower(), out[-4000:]
    assert "dim_repo" in out, out[-4000:]


# ---------------------------------------------------------------- PII + idempotency


def test_gold_has_no_email_columns_or_values(connect_gold):
    gold = connect_gold()
    tables = {
        r[0]
        for r in gold.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = ?",
            [GOLD_SCHEMA],
        ).fetchall()
    }
    assert set(GOLD_MODELS) <= tables
    cols = gold.execute(
        "SELECT table_name, column_name, data_type FROM information_schema.columns "
        "WHERE table_schema = ?",
        [GOLD_SCHEMA],
    ).fetchall()
    for table, column, _ in cols:
        assert "email" not in column.lower(), (table, column)
        assert "commit_author" not in column.lower(), (table, column)
    for table, column, data_type in cols:
        if data_type.upper() != "VARCHAR":
            continue
        values = gold.execute(
            f'SELECT DISTINCT "{column}" FROM {GOLD_SCHEMA}."{table}" WHERE "{column}" IS NOT NULL'
        ).fetchall()
        for (value,) in values:
            assert not EMAIL.search(value), (table, column, value)


def test_rebuild_with_same_input_is_idempotent(built):
    _require_ok(built["proc"])
    again = run_dbt(
        ["build"],
        PROJECT,
        built["profiles"],
        built["base"],
        {"silver_gharchive_parquet": built["silver_glob"]},
    )
    _require_ok(again)
    assert _gold_counts(built["db"]) == built["counts"]
    con = duckdb.connect(str(built["db"]), read_only=True)
    try:
        dupes = con.execute(
            f"SELECT count(*) - count(DISTINCT event_id) FROM {GOLD_SCHEMA}.fct_events"
        ).fetchone()[0]
    finally:
        con.close()
    assert dupes == 0
