"""CLI: export silver/gharchive_events (Delta) of one run's lake to plain parquet for dbt.

The scheduled pipeline (``.github/workflows/pipeline.yml``) builds a fresh lake on every run, so
dbt reads that run's silver through the var ``silver_gharchive_parquet``. The table is read through
Delta (never a glob over the Delta directory, which would ignore ``_delta_log``) and written as
plain parquet with the 17 silver columns in the documented order (docs/spark-jobs.md). A lake
without silver (every hour missing) exports an empty parquet that still carries the schema, so
dbt's incremental run simply adds nothing.

Usage: ``python -m repopulse.spark.export --lake-dir data/lake --out data/silver-export``.
pyspark is imported lazily.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Sequence
from pathlib import Path

from repopulse.spark.job import DEFAULT_LAKE, create_spark_session

TABLE = "gharchive_events"
SILVER_COLUMNS: tuple[str, ...] = (
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
)
SILVER_SCHEMA = (
    "event_id string, event_type string, created_at timestamp, event_date date, "
    "actor_id bigint, actor_login string, repo_id bigint, repo_name string, action string, "
    "pr_number bigint, issue_number bigint, pr_merged boolean, "
    "commit_author_email_hashes array<string>, commit_author_name_hashes array<string>, "
    "landing_date date, landing_hour int, is_late boolean"
)


def export_silver(spark, *, lake_dir: Path, out_dir: Path) -> dict:
    """Overwrite ``out_dir`` with the silver rows as parquet; return a small summary."""
    from delta.tables import DeltaTable

    path = Path(lake_dir) / "silver" / TABLE
    if DeltaTable.isDeltaTable(spark, str(path)):
        df = spark.read.format("delta").load(str(path)).select(*SILVER_COLUMNS)
        found = "found"
    else:
        df = spark.createDataFrame([], SILVER_SCHEMA)
        found = "missing"
    # TIMESTAMP_MICROS instead of Spark's legacy INT96: DuckDB reads it as an exact UTC instant.
    spark.conf.set("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
    df = df.coalesce(1)
    df.write.mode("overwrite").parquet(str(out_dir))
    rows = spark.read.parquet(str(out_dir)).count()
    return {"silver_table": found, "rows": int(rows), "out": str(out_dir)}


def _parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="python -m repopulse.spark.export",
        description="Export silver/gharchive_events (Delta) to plain parquet for dbt.",
    )
    p.add_argument("--lake-dir", type=Path, default=DEFAULT_LAKE)
    p.add_argument("--out", type=Path, required=True, help="output directory (overwritten)")
    p.add_argument("--summary", type=Path, help="write the JSON summary here")
    return p


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    spark = create_spark_session("repopulse-silver-export")
    try:
        summary = export_silver(spark, lake_dir=args.lake_dir, out_dir=args.out)
    finally:
        spark.stop()
    print(json.dumps(summary, sort_keys=True), flush=True)
    if args.summary is not None:
        args.summary.parent.mkdir(parents=True, exist_ok=True)
        args.summary.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    sys.exit(main())
