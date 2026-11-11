"""Helpers shared by the dbt tests (test_dbt_marts.py, test_governance.py, test_dbt_incremental.py).

Not a test module: the session fixtures ``lake``/``built`` in conftest.py and the tests import it
as ``dbt_helpers`` (``tests/`` is on ``sys.path`` under pytest's default import mode).

Silver is rebuilt with DuckDB using the column list of ``docs/spark-jobs.md`` (no Spark needed);
dbt reads it through the var ``silver_gharchive_parquet``. dbt runs as a subprocess
(``python -m dbt.cli.main``) with its target, logs and profiles in a tmp directory.

``PII_HASH_SALT`` is removed from the dbt subprocess environment unless a test passes it, so the
``ci`` target always masks with the fixture salt of ``transform/dbt_project.yml``.
"""

from __future__ import annotations

import gzip
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

import duckdb
import yaml

from benchmarks import naive_duckdb

ROOT = Path(__file__).resolve().parents[1]
PROJECT = ROOT / "transform"
FIXTURES = Path(__file__).parent / "fixtures"
SEED_FIXTURE_DIRS = (  # = transform/seeds/fixture_silver_gharchive_events.csv
    FIXTURES / "gharchive_small",
    FIXTURES / "gharchive_scd2",
)
PARQUET_ONLY_DIR = FIXTURES / "gharchive_parquet_only"
FIXTURE_DIRS = (*SEED_FIXTURE_DIRS, PARQUET_ONLY_DIR)
GOLD_SCHEMA = "main_gold"
PUBLIC_SCHEMA = "main_public"
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
SALT_ENV = "PII_HASH_SALT"


def q(value: str) -> str:
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
        FROM read_json({q(raw_glob)}, format = 'newline_delimited', compression = 'gzip',
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


def write_raw(raw_dir: Path, dirs=FIXTURE_DIRS) -> Path:
    raw_dir.mkdir(parents=True, exist_ok=True)
    with gzip.open(raw_dir / "events.json.gz", "wt", encoding="utf-8") as fh:
        fh.write("\n".join(dedup_lines(dirs)) + "\n")
    return raw_dir


def write_silver(raw_dir: Path, silver_dir: Path, updates: tuple[str, ...] = ()) -> str:
    """Silver parquet from the raw fixture; ``updates`` are SQL run on table ``silver`` first.

    Returns the glob to pass as the dbt var ``silver_gharchive_parquet``.
    """
    silver_dir.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(":memory:")
    try:
        con.execute(
            f"CREATE TABLE silver AS {silver_select_sql((raw_dir / '*.json.gz').as_posix())}"
        )
        for sql in updates:
            con.execute(sql)
        con.execute(
            "COPY (SELECT * FROM silver ORDER BY created_at, event_id) TO "
            f"{q((silver_dir / 'part-0.parquet').as_posix())} (FORMAT parquet)"
        )
    finally:
        con.close()
    return (silver_dir / "*.parquet").as_posix()


def write_profiles(profiles_dir: Path, db_path: Path, target: str = "ci") -> Path:
    profiles_dir.mkdir(parents=True, exist_ok=True)
    (profiles_dir / "profiles.yml").write_text(
        "repopulse:\n"
        f"  target: {target}\n"
        "  outputs:\n"
        f"    {target}:\n"
        "      type: duckdb\n"
        f'      path: "{db_path.as_posix()}"\n'
        "      threads: 2\n",
        encoding="utf-8",
    )
    return profiles_dir


def run_dbt(
    args: list[str],
    project: Path,
    profiles: Path,
    work: Path,
    dbt_vars: dict | None = None,
    target: str = "ci",
    env: dict[str, str] | None = None,
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
        target,
        "--target-path",
        str(work / "target"),
        "--log-path",
        str(work / "logs"),
        "--no-use-colors",
    ]
    if dbt_vars:
        cmd += ["--vars", json.dumps(dbt_vars)]
    base = {k: v for k, v in os.environ.items() if k != SALT_ENV}
    full_env = {**base, "DBT_SEND_ANONYMOUS_USAGE_STATS": "false", "NO_COLOR": "1", **(env or {})}
    return subprocess.run(cmd, capture_output=True, text=True, env=full_env, check=False)


def output(proc: subprocess.CompletedProcess) -> str:
    return (proc.stdout or "") + (proc.stderr or "")


def require_ok(proc: subprocess.CompletedProcess) -> None:
    lines = [ln for ln in output(proc).splitlines() if "WARNING" not in ln]
    tail = "\n".join(lines[-25:])
    assert proc.returncode == 0, f"dbt exited {proc.returncode}:\n{tail}"


def gold_counts(db: Path) -> dict[str, int]:
    con = duckdb.connect(str(db), read_only=True)
    try:
        return {
            m: con.execute(f"SELECT count(*) FROM {GOLD_SCHEMA}.{m}").fetchone()[0]
            for m in GOLD_MODELS
        }
    finally:
        con.close()


def copy_project(dest: Path) -> Path:
    """Copy of transform/ (without build output) to break on purpose."""
    shutil.copytree(
        PROJECT, dest, ignore=shutil.ignore_patterns("target", "logs", "dbt_packages", "*.yml~")
    )
    return dest


def ci_salt() -> str:
    """Fixture salt of the ``ci`` target (dbt_project.yml var ``ci_pii_hash_salt``)."""
    project = yaml.safe_load((PROJECT / "dbt_project.yml").read_text(encoding="utf-8"))
    salt = (project.get("vars") or {}).get("ci_pii_hash_salt")
    assert salt, "transform/dbt_project.yml has no var ci_pii_hash_salt (fixture salt of ci)"
    return salt


def make_lake(base: Path) -> dict:
    raw = write_raw(base / "raw")
    silver_glob = write_silver(raw, base / "silver")
    db = base / "gold.duckdb"
    return {
        "base": base,
        "raw": raw,
        "silver_glob": silver_glob,
        "db": db,
        "profiles": write_profiles(base / "profiles", db),
    }


def build_lake(lake: dict) -> dict:
    proc = run_dbt(
        ["build"],
        PROJECT,
        lake["profiles"],
        lake["base"],
        {"silver_gharchive_parquet": lake["silver_glob"]},
    )
    counts = gold_counts(lake["db"]) if proc.returncode == 0 else None
    return {
        **lake,
        "proc": proc,
        "counts": counts,
        "manifest": lake["base"] / "target" / "manifest.json",
    }
