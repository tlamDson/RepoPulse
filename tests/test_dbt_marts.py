"""dbt star schema and Q1-Q6 marts (transform/) checked against the naive baseline.

Input: the raw GH Archive fixtures ``tests/fixtures/gharchive_small`` (see test_naive_duckdb.py)
plus ``tests/fixtures/gharchive_scd2`` (actor 1004 renames dave -> dave2 on 2026-02-12 and
comments on spark issue #104; erin forks and opens PR #9 on ``DuckDB/DuckDB``, mixed case, to
exercise the case-insensitive seed match).

Silver dedupes by ``event_id`` while the baseline does not, so both sides get the same
deduplicated input: the duplicated line of event 9000000002 is dropped (first copy kept) before
the baseline runs and before the silver parquet is written. Silver is rebuilt here with DuckDB
using the column list of ``docs/spark-jobs.md`` (no Spark needed); dbt reads it through the var
``silver_gharchive_parquet``. Everything (DuckDB file, target, logs, profiles) lives in tmp.

dbt runs as a subprocess (``python -m dbt.cli.main``) a few times per session only: one build,
one rebuild (idempotency), and one small build per contract-violation case.
"""

from __future__ import annotations

import gzip
import json
import os
import re
import shutil
import subprocess
import sys
from datetime import datetime
from pathlib import Path

import duckdb
import pytest

from benchmarks import naive_duckdb

pytest.importorskip("dbt.cli.main", reason="dbt extra not installed: pip install -e '.[dbt]'")
pytest.importorskip("dbt.adapters.duckdb", reason="dbt-duckdb not installed")

ROOT = Path(__file__).resolve().parents[1]
PROJECT = ROOT / "transform"
FIXTURE_DIRS = (
    Path(__file__).parent / "fixtures" / "gharchive_small",
    Path(__file__).parent / "fixtures" / "gharchive_scd2",
)
GOLD_SCHEMA = "main_gold"
CORE_MODELS = ("fct_events", "dim_user", "dim_repo")
MARTS = {  # baseline query -> (mart, ORDER BY used by the baseline query)
    "q1": ("mart_daily_active_contributors", "period, scope, scope_key, period_start"),
    "q2": ("mart_cohort_retention", "cohort_week"),
    "q3": ("mart_contribution_funnel", "repo_name"),
    "q4": ("mart_responsiveness", "repo_name, item_type"),
    "q5": ("mart_repo_concentration", "repo_name"),
    "q6": ("mart_repo_health_score", "health_rank"),
}
GOLD_MODELS = CORE_MODELS + tuple(m for m, _ in MARTS.values())
EMAIL = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")


def _q(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def silver_select_sql(raw_glob: str) -> str:
    """Raw GH Archive JSON (gz) -> silver columns of docs/spark-jobs.md (no dedupe here)."""
    return f"""
        SELECT
            id AS event_id,
            type AS event_type,
            created_at,
            CAST(created_at AS DATE) AS event_date,
            actor.id AS actor_id,
            actor.login AS actor_login,
            repo.id AS repo_id,
            repo.name AS repo_name,
            payload ->> '$.action' AS action,
            TRY_CAST(
                coalesce(payload ->> '$.pull_request.number', payload ->> '$.number') AS BIGINT
            ) AS pr_number,
            TRY_CAST(payload ->> '$.issue.number' AS BIGINT) AS issue_number,
            TRY_CAST(payload ->> '$.pull_request.merged' AS BOOLEAN) AS pr_merged,
            CAST(NULL AS VARCHAR[]) AS commit_author_email_hashes,
            CAST(NULL AS VARCHAR[]) AS commit_author_name_hashes,
            CAST(created_at AS DATE) AS landing_date,
            CAST(hour(created_at) AS INTEGER) AS landing_hour,
            false AS is_late
        FROM read_json({_q(raw_glob)}, format = 'newline_delimited', compression = 'gzip',
                       columns = {naive_duckdb.RAW_COLUMNS})
        ORDER BY created_at, event_id
    """


def dedup_lines(dirs=FIXTURE_DIRS) -> list[str]:
    """All fixture lines, first copy per event id kept (what silver does)."""
    seen: set[str] = set()
    out: list[str] = []
    for d in dirs:
        for path in sorted(d.glob("*.json")):
            for line in path.read_text(encoding="utf-8").splitlines():
                if not line.strip():
                    continue
                event_id = json.loads(line)["id"]
                if event_id not in seen:
                    seen.add(event_id)
                    out.append(line)
    return out


def write_profiles(profiles_dir: Path, db_path: Path) -> Path:
    profiles_dir.mkdir(parents=True, exist_ok=True)
    (profiles_dir / "profiles.yml").write_text(
        "repopulse:\n"
        "  target: ci\n"
        "  outputs:\n"
        "    ci:\n"
        "      type: duckdb\n"
        f'      path: "{db_path.as_posix()}"\n'
        "      threads: 2\n",
        encoding="utf-8",
    )
    return profiles_dir


def run_dbt(
    args: list[str], project: Path, profiles: Path, work: Path, dbt_vars: dict | None = None
) -> subprocess.CompletedProcess:
    cmd = [
        sys.executable,
        "-m",
        "dbt.cli.main",
        *args,
        "--project-dir",
        str(project),
        "--profiles-dir",
        str(profiles),
        "--target",
        "ci",
        "--target-path",
        str(work / "target"),
        "--log-path",
        str(work / "logs"),
        "--no-use-colors",
    ]
    if dbt_vars:
        cmd += ["--vars", json.dumps(dbt_vars)]
    env = {**os.environ, "DBT_SEND_ANONYMOUS_USAGE_STATS": "false", "NO_COLOR": "1"}
    return subprocess.run(cmd, capture_output=True, text=True, env=env, check=False)


def _output(proc: subprocess.CompletedProcess) -> str:
    return (proc.stdout or "") + (proc.stderr or "")


def _require_ok(proc: subprocess.CompletedProcess) -> None:
    lines = [ln for ln in _output(proc).splitlines() if "WARNING" not in ln]
    tail = "\n".join(lines[-25:])
    assert proc.returncode == 0, f"dbt exited {proc.returncode}:\n{tail}"


def _gold_counts(db: Path) -> dict[str, int]:
    con = duckdb.connect(str(db), read_only=True)
    try:
        return {
            m: con.execute(f"SELECT count(*) FROM {GOLD_SCHEMA}.{m}").fetchone()[0]
            for m in GOLD_MODELS
        }
    finally:
        con.close()


# ---------------------------------------------------------------- session fixtures


@pytest.fixture(scope="session")
def lake(tmp_path_factory) -> dict:
    base = tmp_path_factory.mktemp("dbt_marts")
    raw = base / "raw"
    raw.mkdir()
    with gzip.open(raw / "events.json.gz", "wt", encoding="utf-8") as fh:
        fh.write("\n".join(dedup_lines()) + "\n")
    silver = base / "silver"
    silver.mkdir()
    con = duckdb.connect(":memory:")
    try:
        sql = silver_select_sql((raw / "*.json.gz").as_posix())
        con.execute(
            f"COPY ({sql}) TO {_q((silver / 'part-0.parquet').as_posix())} (FORMAT parquet)"
        )
    finally:
        con.close()
    db = base / "gold.duckdb"
    return {
        "base": base,
        "raw": raw,
        "silver_glob": (silver / "*.parquet").as_posix(),
        "db": db,
        "profiles": write_profiles(base / "profiles", db),
    }


@pytest.fixture(scope="session")
def built(lake) -> dict:
    proc = run_dbt(
        ["build"],
        PROJECT,
        lake["profiles"],
        lake["base"],
        {"silver_gharchive_parquet": lake["silver_glob"]},
    )
    counts = _gold_counts(lake["db"]) if proc.returncode == 0 else None
    return {**lake, "proc": proc, "counts": counts}


@pytest.fixture(scope="session")
def baseline(lake) -> dict:
    return naive_duckdb.run_once(lake["raw"], collect=True)["results"]


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
    assert len(ids) == len(set(ids)) == 36  # 34 + 3 lines, minus the duplicate of 9000000002


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
    # 2 of the 36 fixture events are on alice/toy (not in the seed)
    assert total == 34


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
    project = tmp_path / "transform"
    shutil.copytree(
        PROJECT, project, ignore=shutil.ignore_patterns("target", "logs", "dbt_packages", "*.yml~")
    )
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
