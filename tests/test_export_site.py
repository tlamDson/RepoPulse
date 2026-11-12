"""Tests for repopulse/export_site.py: public marts + runs.csv -> static site data + Tableau CSVs.

Every database is a small DuckDB file in ``tmp_path`` built by hand here (no MotherDuck, no
network). The build must fail, without leaving a half-written output, when a mart or a column of
the contract is missing, when the schema does not exist or when runs.csv has the wrong header.
"""

from __future__ import annotations

import csv
import json
from pathlib import Path

import duckdb
import pytest
import yaml

from repopulse import export_site, runs_log

ROOT = Path(__file__).resolve().parents[1]
PUBLIC_YML = ROOT / "transform" / "models" / "public" / "_public.yml"
GENERATED_AT = "2026-11-11T08:00:00+00:00"

# mart -> (column DDL, rows). Values are chosen so the expected order and numbers are obvious.
SPEC: dict[str, tuple[str, list[str]]] = {
    "mart_daily_active_contributors": (
        "period VARCHAR, period_start DATE, scope VARCHAR, scope_key VARCHAR, "
        "active_contributors BIGINT, new_contributors BIGINT, returning_contributors BIGINT",
        [
            "('week', DATE '2026-01-05', 'repo', 'apache/spark', 9, 4, 5)",
            "('day', DATE '2026-01-06', 'category', 'compute', 3, 1, 2)",
            "('day', DATE '2026-01-05', 'category', 'compute', 2, 2, 0)",
        ],
    ),
    "mart_cohort_retention": (
        "cohort_week DATE, cohort_size BIGINT, retained_w4 BIGINT, retained_w12 BIGINT, "
        "retention_w4_pct DOUBLE, retention_w12_pct DOUBLE",
        [
            "(DATE '2026-01-12', 1, 0, 0, 0.0, 0.0)",
            "(DATE '2026-01-05', 2, 1, 1, 50.0, NULL)",
        ],
    ),
    "mart_contribution_funnel": (
        "repo_name VARCHAR, category VARCHAR, forkers BIGINT, forkers_with_pr BIGINT, "
        "forkers_with_merge BIGINT, prs_opened BIGINT, prs_merged BIGINT, "
        "merge_hours_p50 DOUBLE, merge_hours_p90 DOUBLE",
        [
            "('org/b', 'bi', 1, 0, 0, 0, 0, NULL, NULL)",
            "('org/a', 'compute', 2, 1, 1, 3, 2, 6.0, 9.2)",
        ],
    ),
    "mart_responsiveness": (
        "repo_name VARCHAR, item_type VARCHAR, opened BIGINT, responded BIGINT, "
        "response_hours_p50 DOUBLE, response_hours_p90 DOUBLE, closed BIGINT, "
        "close_rate_pct DOUBLE",
        [
            "('org/a', 'pull_request', 3, 2, 1.5, 4.25, 1, 33.25)",
            "('org/a', 'issue', 2, 1, 0.5, 0.5, 2, 100.0)",
        ],
    ),
    "mart_repo_concentration": (
        "repo_name VARCHAR, category VARCHAR, contributions BIGINT, bot_contributions BIGINT, "
        "bot_share_pct DOUBLE, human_contributors BIGINT, top1_share_pct DOUBLE, "
        "top2_share_pct DOUBLE, top3_share_pct DOUBLE",
        ["('org/a', 'compute', 10, 1, 10.0, 3, 60.0, 80.0, 100.0)"],
    ),
    "mart_repo_health_score": (
        "health_rank BIGINT, repo_name VARCHAR, category VARCHAR, activity_score DOUBLE, "
        "retention_score DOUBLE, merge_score DOUBLE, response_score DOUBLE, close_score DOUBLE, "
        "diversity_score DOUBLE, health_score DOUBLE",
        [
            "(2, 'org/b', 'bi', 0.1, 0.0, NULL, 0.2, 0.3, 0.4, 0.25)",
            "(1, 'org/a', 'compute', 1.0, 0.5, 0.75, 0.5, 1.0, 0.5, 0.875)",
        ],
    ),
}
MARTS = tuple(SPEC)

RUNS_ROWS = [
    {
        "run_at": "2026-11-11T07:43:57+00:00",
        "git_sha": "ef8d83955bdd77e1b0d6cf93885d875cb8d51ad1",
        "variant": "baseline",
        "data_date": "2026-10-25",
        "total_seconds": "4.25",
        "q1_seconds": "0.5",
        "q2_seconds": "0.5",
        "q3_seconds": "0.5",
        "q4_seconds": "0.5",
        "q5_seconds": "0.5",
        "q6_seconds": "0.5",
        "peak_rss_mb": "100.0",
        "bytes_read": "1000",
        "row_count": "20",
        "notes": "runs=5 median",
    },
    {
        "run_at": "2026-11-11T16:20:05+00:00",
        "git_sha": "ee38cf0",
        "variant": "dbt-build-duckdb",
        "data_date": "2026-01-05",
        "total_seconds": "2.5",
        "q1_seconds": "",
        "q2_seconds": "",
        "q3_seconds": "",
        "q4_seconds": "",
        "q5_seconds": "",
        "q6_seconds": "",
        "peak_rss_mb": "",
        "bytes_read": "",
        "row_count": "36",
        "notes": "engine=duckdb; fixture=seed",
    },
]


def make_db(
    path: Path,
    schema: str = "main_public",
    skip: str | None = None,
    drop: tuple[str, str] | None = None,
    extra_sql: tuple[str, ...] = (),
    empty: str | None = None,
) -> Path:
    con = duckdb.connect(str(path))
    try:
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        for mart, (ddl, rows) in SPEC.items():
            if mart == skip:
                continue
            con.execute(f"CREATE TABLE {schema}.{mart} ({ddl})")
            if rows and mart != empty:
                con.execute(f"INSERT INTO {schema}.{mart} VALUES {', '.join(rows)}")
            if drop and drop[0] == mart:
                con.execute(f"ALTER TABLE {schema}.{mart} DROP COLUMN {drop[1]}")
        for sql in extra_sql:
            con.execute(sql)
    finally:
        con.close()
    return path


def write_runs(path: Path, header: tuple[str, ...] = runs_log.COLUMNS, rows=RUNS_ROWS) -> Path:
    with open(path, "w", encoding="utf-8", newline="") as fh:
        writer = csv.writer(fh, lineterminator="\n")
        writer.writerow(header)
        for row in rows:
            writer.writerow(row.get(c, "") for c in header)
    return path


@pytest.fixture
def site_dir(tmp_path) -> Path:
    d = tmp_path / "site-src"
    d.mkdir()
    (d / "index.html").write_text("<!doctype html><title>t</title>\n", encoding="utf-8")
    return d


@pytest.fixture
def inputs(tmp_path) -> dict:
    return {
        "db": make_db(tmp_path / "gold.duckdb"),
        "runs": write_runs(tmp_path / "runs.csv"),
    }


def run_main(inputs: dict, out: Path, site_dir: Path, *extra: str) -> int:
    argv = [
        "--db",
        str(inputs["db"]),
        "--runs",
        str(inputs["runs"]),
        "--out",
        str(out),
        "--site-dir",
        str(site_dir),
        "--generated-at",
        GENERATED_AT,
        *extra,
    ]
    return export_site.main(argv)


def load_json(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


def all_files(root: Path) -> dict[str, bytes]:
    return {p.relative_to(root).as_posix(): p.read_bytes() for p in root.rglob("*") if p.is_file()}


# ---------------------------------------------------------------- contract


def test_contract_matches_public_yml():
    doc = yaml.safe_load(PUBLIC_YML.read_text(encoding="utf-8"))
    from_yml = {
        m["config"]["alias"]: tuple(c["name"] for c in m["columns"])
        for m in doc["models"]
        if m["config"]["alias"].startswith("mart_")
    }
    assert export_site.MART_COLUMNS == from_yml


def test_contract_names_the_six_marts_in_query_order():
    assert tuple(export_site.MART_COLUMNS) == MARTS


# ---------------------------------------------------------------- failures


@pytest.mark.parametrize("mart", MARTS)
def test_missing_mart_fails_and_names_it(tmp_path, site_dir, capsys, mart):
    inputs = {
        "db": make_db(tmp_path / "gold.duckdb", skip=mart),
        "runs": write_runs(tmp_path / "runs.csv"),
    }
    out = tmp_path / "out"
    assert run_main(inputs, out, site_dir) != 0
    err = capsys.readouterr().err
    assert mart in err
    assert "missing" in err.lower()
    assert not out.exists()


@pytest.mark.parametrize(
    ("mart", "column"),
    [
        ("mart_daily_active_contributors", "new_contributors"),
        ("mart_cohort_retention", "retention_w12_pct"),
        ("mart_contribution_funnel", "merge_hours_p90"),
        ("mart_responsiveness", "close_rate_pct"),
        ("mart_repo_concentration", "top3_share_pct"),
        ("mart_repo_health_score", "health_score"),
    ],
)
def test_missing_column_fails_and_names_it(tmp_path, site_dir, capsys, mart, column):
    inputs = {
        "db": make_db(tmp_path / "gold.duckdb", drop=(mart, column)),
        "runs": write_runs(tmp_path / "runs.csv"),
    }
    out = tmp_path / "out"
    assert run_main(inputs, out, site_dir) != 0
    err = capsys.readouterr().err
    assert f"{mart}.{column}" in err
    assert not out.exists()


def test_unknown_schema_fails_and_names_it(inputs, tmp_path, site_dir, capsys):
    out = tmp_path / "out"
    assert run_main(inputs, out, site_dir, "--schema", "main_nope") != 0
    assert "main_nope" in capsys.readouterr().err
    assert not out.exists()


def test_runs_csv_with_wrong_header_fails(tmp_path, site_dir, capsys):
    header = tuple(c for c in runs_log.COLUMNS if c != "peak_rss_mb")
    inputs = {
        "db": make_db(tmp_path / "gold.duckdb"),
        "runs": write_runs(tmp_path / "runs.csv", header=header),
    }
    out = tmp_path / "out"
    assert run_main(inputs, out, site_dir) != 0
    assert "runs.csv" in capsys.readouterr().err
    assert not out.exists()


def test_missing_runs_csv_fails(tmp_path, site_dir, capsys):
    inputs = {"db": make_db(tmp_path / "gold.duckdb"), "runs": tmp_path / "nope.csv"}
    out = tmp_path / "out"
    assert run_main(inputs, out, site_dir) != 0
    assert "nope.csv" in capsys.readouterr().err
    assert not out.exists()


def test_missing_database_file_fails_without_creating_it(tmp_path, site_dir, capsys):
    db = tmp_path / "absent.duckdb"
    inputs = {"db": db, "runs": write_runs(tmp_path / "runs.csv")}
    out = tmp_path / "out"
    assert run_main(inputs, out, site_dir) != 0
    assert "absent.duckdb" in capsys.readouterr().err
    assert not db.exists()
    assert not out.exists()


def test_generated_at_without_timezone_fails(inputs, tmp_path, site_dir):
    out = tmp_path / "out"
    argv = [
        "--db",
        str(inputs["db"]),
        "--runs",
        str(inputs["runs"]),
        "--out",
        str(out),
        "--site-dir",
        str(site_dir),
        "--generated-at",
        "2026-11-11T08:00:00",
    ]
    assert export_site.main(argv) != 0
    assert not out.exists()


def test_refuses_to_overwrite_a_directory_that_is_not_a_previous_export(
    inputs, tmp_path, site_dir, capsys
):
    out = tmp_path / "out"
    out.mkdir()
    (out / "keep.txt").write_text("mine", encoding="utf-8")
    assert run_main(inputs, out, site_dir) != 0
    assert "out" in capsys.readouterr().err
    assert (out / "keep.txt").read_text(encoding="utf-8") == "mine"


def test_failed_build_keeps_the_previous_export(inputs, tmp_path, site_dir):
    out = tmp_path / "out"
    assert run_main(inputs, out, site_dir) == 0
    before = all_files(out)
    broken = {
        "db": make_db(tmp_path / "broken.duckdb", skip="mart_cohort_retention"),
        "runs": inputs["runs"],
    }
    assert run_main(broken, out, site_dir) != 0
    assert all_files(out) == before


# ---------------------------------------------------------------- success


def test_writes_site_data_and_tableau_files(inputs, tmp_path, site_dir):
    out = tmp_path / "out"
    assert run_main(inputs, out, site_dir) == 0
    files = set(all_files(out))
    expected = {"index.html", "data/meta.json", "data/runs.json", "tableau/runs.csv"}
    expected |= {f"data/{m}.json" for m in MARTS} | {f"tableau/{m}.csv" for m in MARTS}
    assert files == expected


def test_q1_json_has_the_numbers_in_a_stable_order(inputs, tmp_path, site_dir):
    out = tmp_path / "out"
    assert run_main(inputs, out, site_dir) == 0
    doc = load_json(out / "data" / "mart_daily_active_contributors.json")
    assert doc["columns"] == list(export_site.MART_COLUMNS["mart_daily_active_contributors"])
    assert doc["rows"] == [
        {
            "period": "day",
            "period_start": "2026-01-05",
            "scope": "category",
            "scope_key": "compute",
            "active_contributors": 2,
            "new_contributors": 2,
            "returning_contributors": 0,
        },
        {
            "period": "day",
            "period_start": "2026-01-06",
            "scope": "category",
            "scope_key": "compute",
            "active_contributors": 3,
            "new_contributors": 1,
            "returning_contributors": 2,
        },
        {
            "period": "week",
            "period_start": "2026-01-05",
            "scope": "repo",
            "scope_key": "apache/spark",
            "active_contributors": 9,
            "new_contributors": 4,
            "returning_contributors": 5,
        },
    ]


def test_nulls_and_floats_serialise_cleanly(inputs, tmp_path, site_dir):
    out = tmp_path / "out"
    assert run_main(inputs, out, site_dir) == 0
    q2 = load_json(out / "data" / "mart_cohort_retention.json")["rows"]
    assert [r["cohort_week"] for r in q2] == ["2026-01-05", "2026-01-12"]
    assert q2[0]["retention_w4_pct"] == 50.0
    assert q2[0]["retention_w12_pct"] is None
    q3 = load_json(out / "data" / "mart_contribution_funnel.json")["rows"]
    assert [r["repo_name"] for r in q3] == ["org/a", "org/b"]
    assert q3[0]["merge_hours_p90"] == 9.2
    assert q3[1]["merge_hours_p50"] is None
    q4 = load_json(out / "data" / "mart_responsiveness.json")["rows"]
    assert [r["item_type"] for r in q4] == ["issue", "pull_request"]
    q6 = load_json(out / "data" / "mart_repo_health_score.json")["rows"]
    assert [(r["health_rank"], r["repo_name"], r["health_score"]) for r in q6] == [
        (1, "org/a", 0.875),
        (2, "org/b", 0.25),
    ]
    assert "NaN" not in (out / "data" / "mart_repo_health_score.json").read_text(encoding="utf-8")


def test_decimal_and_nan_values_become_json_numbers_and_null(tmp_path, site_dir):
    extra = (
        "ALTER TABLE main_public.mart_responsiveness ALTER close_rate_pct TYPE DECIMAL(6, 2)",
        "UPDATE main_public.mart_responsiveness SET response_hours_p90 = 'NaN'::DOUBLE "
        "WHERE item_type = 'issue'",
    )
    inputs = {
        "db": make_db(tmp_path / "gold.duckdb", extra_sql=extra),
        "runs": write_runs(tmp_path / "runs.csv"),
    }
    out = tmp_path / "out"
    assert run_main(inputs, out, site_dir) == 0
    rows = load_json(out / "data" / "mart_responsiveness.json")["rows"]
    assert rows[0]["close_rate_pct"] == 100.0
    assert rows[1]["close_rate_pct"] == 33.25
    assert rows[0]["response_hours_p90"] is None


def test_tableau_csv_matches_the_mart(inputs, tmp_path, site_dir):
    out = tmp_path / "out"
    assert run_main(inputs, out, site_dir) == 0
    with open(out / "tableau" / "mart_contribution_funnel.csv", encoding="utf-8", newline="") as f:
        rows = list(csv.reader(f))
    assert rows[0] == list(export_site.MART_COLUMNS["mart_contribution_funnel"])
    assert rows[1] == ["org/a", "compute", "2", "1", "1", "3", "2", "6.0", "9.2"]
    assert rows[2] == ["org/b", "bi", "1", "0", "0", "0", "0", "", ""]


def test_runs_json_and_csv_match_runs_csv(inputs, tmp_path, site_dir):
    out = tmp_path / "out"
    assert run_main(inputs, out, site_dir) == 0
    doc = load_json(out / "data" / "runs.json")
    assert doc["columns"] == list(runs_log.COLUMNS)
    expected = [{c: (r[c] if r[c] != "" else None) for c in runs_log.COLUMNS} for r in RUNS_ROWS]
    assert doc["rows"] == expected
    assert (out / "tableau" / "runs.csv").read_bytes() == inputs["runs"].read_bytes()


def test_meta_comes_from_the_data_not_from_hand_typed_values(inputs, tmp_path, site_dir):
    out = tmp_path / "out"
    assert run_main(inputs, out, site_dir, "--git-sha", "abc1234") == 0
    meta = load_json(out / "data" / "meta.json")
    assert meta["generated_at"] == GENERATED_AT
    assert meta["git_sha"] == "abc1234"
    assert meta["schema"] == "main_public"
    assert meta["latest_data_date"] == "2026-01-06"  # max day of Q1, not the week row
    assert meta["source"]["name"] == "GH Archive"
    assert meta["source"]["real_data"] is True
    assert meta["simulated_sections"] == []
    assert meta["marts"]["mart_cohort_retention"] == {
        "query": "q2",
        "rows": 2,
        "json": "data/mart_cohort_retention.json",
        "csv": "tableau/mart_cohort_retention.csv",
    }
    assert meta["runs"] == {"rows": 2, "json": "data/runs.json", "csv": "tableau/runs.csv"}


def test_meta_without_git_sha_is_null(inputs, tmp_path, site_dir):
    out = tmp_path / "out"
    assert run_main(inputs, out, site_dir) == 0
    assert load_json(out / "data" / "meta.json")["git_sha"] is None


def test_bad_git_sha_fails(inputs, tmp_path, site_dir):
    out = tmp_path / "out"
    assert run_main(inputs, out, site_dir, "--git-sha", "not a sha") != 0
    assert not out.exists()


def test_empty_mart_exports_zero_rows_and_null_latest_date(tmp_path, site_dir):
    inputs = {
        "db": make_db(tmp_path / "gold.duckdb", empty="mart_daily_active_contributors"),
        "runs": write_runs(tmp_path / "runs.csv"),
    }
    out = tmp_path / "out"
    assert run_main(inputs, out, site_dir) == 0
    assert load_json(out / "data" / "mart_daily_active_contributors.json")["rows"] == []
    meta = load_json(out / "data" / "meta.json")
    assert meta["latest_data_date"] is None
    assert meta["marts"]["mart_daily_active_contributors"]["rows"] == 0


def test_same_input_twice_gives_byte_identical_output(inputs, tmp_path, site_dir):
    a, b = tmp_path / "a", tmp_path / "b"
    assert run_main(inputs, a, site_dir, "--git-sha", "abc1234") == 0
    assert run_main(inputs, b, site_dir, "--git-sha", "abc1234") == 0
    assert all_files(a) == all_files(b)


def test_re_export_into_a_previous_export_replaces_it(inputs, tmp_path, site_dir):
    out = tmp_path / "out"
    assert run_main(inputs, out, site_dir) == 0
    (out / "stale.txt").write_text("old", encoding="utf-8")
    assert run_main(inputs, out, site_dir) == 0
    assert not (out / "stale.txt").exists()
    assert (out / "data" / "meta.json").is_file()


def test_default_site_dir_is_the_repo_site_folder(inputs, tmp_path):
    out = tmp_path / "out"
    argv = ["--db", str(inputs["db"]), "--runs", str(inputs["runs"]), "--out", str(out)]
    assert export_site.main(argv) == 0
    assert (out / "index.html").read_bytes() == (ROOT / "site" / "index.html").read_bytes()


# ---------------------------------------------------------------- public schema only


def test_reads_only_the_public_schema_and_only_contract_columns(tmp_path, site_dir):
    extra = (
        "CREATE SCHEMA main_gold",
        "CREATE TABLE main_gold.mart_contribution_funnel AS "
        "SELECT 'gold/secret-repo' AS repo_name, 'x' AS category",
        "CREATE TABLE main_gold.dim_user AS SELECT 'leaked@example.com' AS actor_login",
        "ALTER TABLE main_public.mart_contribution_funnel ADD COLUMN author_email VARCHAR "
        "DEFAULT 'someone@example.com'",
    )
    inputs = {
        "db": make_db(tmp_path / "gold.duckdb", extra_sql=extra),
        "runs": write_runs(tmp_path / "runs.csv"),
    }
    out = tmp_path / "out"
    assert run_main(inputs, out, site_dir) == 0
    blob = b"\n".join(all_files(out).values())
    assert b"gold/secret-repo" not in blob
    assert b"example.com" not in blob
    assert b"author_email" not in blob


def test_local_database_is_opened_read_only(inputs, tmp_path, site_dir, monkeypatch):
    calls = []
    guarded = duckdb.connect

    def spy(database=":memory:", *args, **kwargs):
        calls.append((str(database), kwargs.get("read_only")))
        return guarded(database, *args, **kwargs)

    monkeypatch.setattr(duckdb, "connect", spy)
    assert run_main(inputs, tmp_path / "out", site_dir) == 0
    assert calls == [(str(inputs["db"]), True)]


def test_motherduck_path_is_passed_as_is_and_token_never_printed(
    inputs, tmp_path, site_dir, monkeypatch, capsys
):
    token = "tok-not-a-real-secret-123"
    monkeypatch.setenv("MOTHERDUCK_TOKEN", token)
    seen = []
    guarded = duckdb.connect
    local = str(inputs["db"])

    def fake(database=":memory:", *args, **kwargs):
        seen.append(str(database))
        return guarded(local, read_only=True)

    monkeypatch.setattr(duckdb, "connect", fake)
    out = tmp_path / "out"
    argv = [
        "--db",
        "md:repopulse",
        "--runs",
        str(inputs["runs"]),
        "--out",
        str(out),
        "--site-dir",
        str(site_dir),
        "--generated-at",
        GENERATED_AT,
    ]
    assert export_site.main(argv) == 0
    assert seen == ["md:repopulse"]
    captured = capsys.readouterr()
    assert token not in captured.out + captured.err
    assert token.encode() not in b"\n".join(all_files(out).values())


# ---------------------------------------------------------------- real dbt build


def test_exports_the_public_schema_of_a_real_dbt_build(built, tmp_path, site_dir):
    import dbt_helpers

    dbt_helpers.require_ok(built["proc"])
    runs = write_runs(tmp_path / "runs.csv")
    out = tmp_path / "out"
    inputs = {"db": built["db"], "runs": runs}
    assert run_main(inputs, out, site_dir) == 0
    meta = load_json(out / "data" / "meta.json")
    con = duckdb.connect(str(built["db"]), read_only=True)
    try:
        for mart in MARTS:
            n = con.execute(f"SELECT count(*) FROM main_public.{mart}").fetchone()[0]
            assert meta["marts"][mart]["rows"] == n, mart
            assert len(load_json(out / "data" / f"{mart}.json")["rows"]) == n, mart
        latest = con.execute(
            "SELECT max(period_start) FROM main_public.mart_daily_active_contributors "
            "WHERE period = 'day'"
        ).fetchone()[0]
    finally:
        con.close()
    assert meta["latest_data_date"] == latest.isoformat()
