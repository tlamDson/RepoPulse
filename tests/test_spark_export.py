"""Export silver/gharchive_events (Delta) of one run's lake to plain parquet for dbt.

The scheduled pipeline gets a fresh lake per run; dbt reads that run's silver through the var
``silver_gharchive_parquet``. The export goes through Spark + Delta (never a glob over the Delta
directory, which would ignore ``_delta_log``). CLI tests replace Spark and run everywhere; tests
marked ``spark`` need Java 17 and always run on CI.
"""

from __future__ import annotations

import json
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path

import duckdb
import pytest
from test_spark_gharchive import encode, gh_event, land, run

from repopulse.spark import export

DAY = "2026-01-05"
SILVER_COLUMNS = [
    "event_id",
    "event_type",
    "created_at",
    "event_date",
    "actor_id",
    "actor_login",
    "repo_id",
    "repo_name",
    "action",
    "pr_number",
    "issue_number",
    "pr_merged",
    "commit_author_email_hashes",
    "commit_author_name_hashes",
    "landing_date",
    "landing_hour",
    "is_late",
]


def test_columns_are_the_17_silver_columns():
    assert list(export.SILVER_COLUMNS) == SILVER_COLUMNS


def test_import_does_not_pull_in_pyspark():
    code = "import sys; import repopulse.spark.export; print('pyspark' in sys.modules)"
    proc = subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
        cwd=Path(__file__).resolve().parents[1],
    )
    assert proc.returncode == 0, proc.stderr
    assert proc.stdout.strip() == "False"


# ---------------------------------------------------------------- CLI (no Spark)


class FakeSession:
    def __init__(self):
        self.stopped = False

    def stop(self):
        self.stopped = True


@pytest.fixture
def fakes(monkeypatch):
    state = {"sessions": [], "calls": []}

    def fake_session(*args, **kwargs):
        session = FakeSession()
        state["sessions"].append(session)
        return session

    def fake_export(spark, **kwargs):
        state["calls"].append({"spark": spark, **kwargs})
        return {"silver_table": "found", "rows": 5, "out": str(kwargs["out_dir"])}

    monkeypatch.setattr(export, "create_spark_session", fake_session)
    monkeypatch.setattr(export, "export_silver", fake_export)
    return state


def test_cli_exports_and_writes_summary(fakes, tmp_path):
    summary = tmp_path / "out" / "export-summary.json"
    code = export.main(
        [
            "--lake-dir",
            str(tmp_path / "lake"),
            "--out",
            str(tmp_path / "silver-export"),
            "--summary",
            str(summary),
        ]
    )
    assert code == 0
    (call,) = fakes["calls"]
    assert call["spark"] is fakes["sessions"][0]
    assert Path(call["lake_dir"]) == tmp_path / "lake"
    assert Path(call["out_dir"]) == tmp_path / "silver-export"
    assert fakes["sessions"][0].stopped
    assert json.loads(summary.read_text(encoding="utf-8"))["rows"] == 5


def test_cli_requires_out(fakes):
    with pytest.raises(SystemExit) as exc:
        export.main(["--lake-dir", "data/lake"])
    assert exc.value.code == 2
    assert fakes["sessions"] == []


# ---------------------------------------------------------------- Spark + Delta (CI)


def _parquet_rows(out_dir: Path) -> tuple[list[str], list[tuple]]:
    con = duckdb.connect(":memory:")
    try:
        con.execute("SET TimeZone = 'UTC'")
        glob = (out_dir / "*.parquet").as_posix()
        cur = con.execute(f"SELECT * FROM read_parquet('{glob}') ORDER BY event_id")
        return [c[0] for c in cur.description], cur.fetchall()
    finally:
        con.close()


@pytest.mark.spark
def test_export_writes_every_silver_row_with_the_silver_columns(spark, tmp_path):
    land(
        tmp_path,
        10,
        [
            encode(gh_event(1, created_at=f"{DAY}T10:15:00Z")),
            encode(gh_event(2, "WatchEvent", created_at=f"{DAY}T10:59:59Z", payload={})),
            encode(gh_event(2, "WatchEvent", created_at=f"{DAY}T10:59:59Z", payload={})),
        ],
    )
    summary = run(spark, tmp_path, [(DAY, 10)])
    out = tmp_path / "silver-export"
    result = export.export_silver(spark, lake_dir=tmp_path / "lake", out_dir=out)
    assert result["silver_table"] == "found"
    assert result["rows"] == summary["silver_inserted"] == 2
    cols, rows = _parquet_rows(out)
    assert cols == SILVER_COLUMNS
    assert [r[0] for r in rows] == ["1", "2"]
    created = {r[0]: r[2] for r in rows}
    as_utc = {
        k: (v.astimezone(UTC).replace(tzinfo=None) if v.tzinfo else v) for k, v in created.items()
    }
    assert as_utc == {"1": datetime(2026, 1, 5, 10, 15), "2": datetime(2026, 1, 5, 10, 59, 59)}
    assert not list(out.glob("_delta_log"))


@pytest.mark.spark
def test_export_overwrites_previous_output(spark, tmp_path):
    land(tmp_path, 10, [encode(gh_event(1))])
    run(spark, tmp_path, [(DAY, 10)])
    out = tmp_path / "silver-export"
    export.export_silver(spark, lake_dir=tmp_path / "lake", out_dir=out)
    export.export_silver(spark, lake_dir=tmp_path / "lake", out_dir=out)
    assert len(_parquet_rows(out)[1]) == 1


@pytest.mark.spark
def test_export_of_a_lake_without_silver_is_an_empty_parquet_with_the_schema(spark, tmp_path):
    out = tmp_path / "silver-export"
    result = export.export_silver(spark, lake_dir=tmp_path / "no-lake", out_dir=out)
    assert result["silver_table"] == "missing"
    assert result["rows"] == 0
    cols, rows = _parquet_rows(out)
    assert cols == SILVER_COLUMNS
    assert rows == []


@pytest.mark.spark
def test_exported_silver_feeds_dbt(spark, tmp_path):
    """Spark silver -> export -> dbt staging/fct (needs dbt; both are installed on CI)."""
    pytest.importorskip("dbt.cli.main")
    from test_dbt_marts import PROJECT, _require_ok, run_dbt, write_profiles

    land(
        tmp_path,
        0,
        [
            encode(gh_event(11, created_at=f"{DAY}T00:00:01Z")),
            encode(gh_event(12, created_at=f"{DAY}T00:30:00Z", login="alice2")),
            encode(gh_event(13, created_at=f"{DAY}T00:40:00Z", repo_name="alice/toy")),
        ],
    )
    run(spark, tmp_path, [(DAY, 0)])
    out = tmp_path / "silver-export"
    export.export_silver(spark, lake_dir=tmp_path / "lake", out_dir=out)
    db = tmp_path / "gold.duckdb"
    proc = run_dbt(
        ["build"],
        PROJECT,
        write_profiles(tmp_path / "profiles", db),
        tmp_path,
        {"silver_gharchive_parquet": (out / "*.parquet").as_posix()},
    )
    _require_ok(proc)
    con = duckdb.connect(str(db), read_only=True)
    try:
        staged = con.execute("SELECT count(*) FROM main_staging.stg_gharchive_events").fetchone()
        facts = con.execute(
            "SELECT event_id, created_at, event_date, user_key FROM main_gold.fct_events "
            "ORDER BY event_id"
        ).fetchall()
    finally:
        con.close()
    assert staged == (3,)
    assert facts == [
        ("11", datetime(2026, 1, 5, 0, 0, 1), datetime(2026, 1, 5).date(), "1001-1"),
        ("12", datetime(2026, 1, 5, 0, 30), datetime(2026, 1, 5).date(), "1001-2"),
    ]
