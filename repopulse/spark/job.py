"""CLI: bronze -> silver incremental batch for landed hours (PySpark + Delta on the runner).

Usage: ``python -m repopulse.spark.job --source gharchive --date 2026-01-05 --hour 15 --hours 2``.
Needs ``PII_HASH_SALT``; checked before any SparkSession starts. pyspark is imported lazily.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections.abc import Sequence
from datetime import UTC, datetime
from pathlib import Path

from repopulse import ingest
from repopulse.spark.pii import MissingSaltError, salt_from_env
from repopulse.spark.rules import DEFAULT_LATE_WINDOW_HOURS

MAX_HOURS = 24
DEFAULT_LAKE = Path("data/lake")
DEFAULT_LANDING = {"gharchive": ingest.DEFAULT_LANDING, "app": Path("data/landing/app")}
SHUFFLE_PARTITIONS = 8  # runner: 4 CPU, 16 GB RAM; a few landed hours per run


def create_spark_session(app_name: str = "repopulse-bronze-silver"):
    """Local Delta-enabled session on the runner, UTC session time zone."""
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.master("local[*]")
        .appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        .config("spark.sql.shuffle.partitions", str(SHUFFLE_PARTITIONS))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.memory", "8g")
        .config("spark.ui.enabled", "false")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def run_batch(
    spark,
    *,
    source: str,
    hours: Sequence[tuple[str, int]],
    landing_dir: Path,
    lake_dir: Path,
    salt: str,
    late_window_hours: int = DEFAULT_LATE_WINDOW_HOURS,
) -> dict:
    """Run one incremental batch; see ``repopulse.spark.batch.run_batch``."""
    from repopulse.spark import batch

    return batch.run_batch(
        spark,
        source=source,
        hours=hours,
        landing_dir=Path(landing_dir),
        lake_dir=Path(lake_dir),
        salt=salt,
        late_window_hours=late_window_hours,
    )


def _parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="python -m repopulse.spark.job",
        description="Bronze -> silver incremental batch (PySpark + Delta) for landed hours.",
    )
    p.add_argument("--source", required=True, choices=sorted(DEFAULT_LANDING))
    p.add_argument("--date", required=True, help="data date YYYY-MM-DD (UTC) of the last hour")
    p.add_argument("--hour", required=True, type=int, help="last data hour 0..23 (UTC)")
    p.add_argument("--hours", type=int, default=1, help=f"consecutive hours, 1..{MAX_HOURS}")
    p.add_argument("--landing-dir", type=Path, help="default: data/landing/<source>")
    p.add_argument("--lake-dir", type=Path, default=DEFAULT_LAKE)
    p.add_argument("--late-window-hours", type=int, default=DEFAULT_LATE_WINDOW_HOURS)
    p.add_argument("--summary", type=Path, help="write the JSON summary here")
    return p


def main(argv: Sequence[str] | None = None) -> int:
    parser = _parser()
    args = parser.parse_args(argv)
    try:
        ingest.validate(args.date, args.hour)
    except ValueError as exc:
        parser.error(str(exc))
    if not 1 <= args.hours <= MAX_HOURS:
        parser.error(f"--hours must be 1..{MAX_HOURS}")
    if args.late_window_hours < 0:
        parser.error("--late-window-hours must be >= 0")

    try:
        salt = salt_from_env()
    except MissingSaltError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    end = datetime.fromisoformat(args.date).replace(hour=args.hour, tzinfo=UTC)
    hours = ingest.target_hours(end, lag_hours=0, count=args.hours)
    landing = args.landing_dir if args.landing_dir is not None else DEFAULT_LANDING[args.source]

    spark = create_spark_session()
    try:
        summary = run_batch(
            spark,
            source=args.source,
            hours=hours,
            landing_dir=landing,
            lake_dir=args.lake_dir,
            salt=salt,
            late_window_hours=args.late_window_hours,
        )
    finally:
        spark.stop()

    print(json.dumps(summary, sort_keys=True), flush=True)
    if args.summary is not None:
        args.summary.parent.mkdir(parents=True, exist_ok=True)
        args.summary.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    sys.exit(main())
