"""Databricks entry of the bronze -> silver incremental batch (called by the job's notebook).

The notebook (``repopulse.databricks_job.notebook_source``) installs the wheel, reads its widget
values (the job's ``base_parameters``, all strings) and the PII salt from the secret scope, then
calls :func:`run`. :func:`run` calls the shared ``repopulse.spark.batch.run_batch`` with the
landing folder on the Volume and catalog tables by name, without persist (serverless compute is
Spark Connect). pyspark is imported only when the real batch runs.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from repopulse import ingest
from repopulse.spark.tables import TABLES

MAX_HOURS = 24
PARAMETERS: tuple[str, ...] = (
    "source",
    "date",
    "hour",
    "hours",
    "catalog",
    "schema",
    "volume",
    "wheel_path",
    "late_window_hours",
    "secret_scope",
    "secret_key",
)


def landing_dir(*, catalog: str, schema: str, volume: str, source: str) -> str:
    """``/Volumes/<catalog>/<schema>/<volume>/landing/<source>`` (layout of repopulse.ingest)."""
    return f"/Volumes/{catalog}/{schema}/{volume}/landing/{source}"


def _int(params: Mapping[str, str], name: str) -> int:
    try:
        return int(str(params[name]).strip())
    except (KeyError, ValueError):
        raise ValueError(
            f"parameter {name} must be an integer (got {params.get(name)!r})"
        ) from None


def run(
    spark: Any,
    params: Mapping[str, str],
    *,
    salt: str,
    run_batch: Callable[..., dict] | None = None,
) -> dict:
    """Validate the job parameters and run one batch; return its JSON-serialisable summary."""
    source = params.get("source")
    if source not in TABLES:
        raise ValueError(f"unknown source {source!r}")
    day, hour, hours = params.get("date"), _int(params, "hour"), _int(params, "hours")
    ingest.validate(day, hour)
    if not 1 <= hours <= MAX_HOURS:
        raise ValueError(f"hours must be 1..{MAX_HOURS} (got {hours})")
    late_window = _int(params, "late_window_hours")
    if late_window < 0:
        raise ValueError("late_window_hours must be >= 0")
    if not salt or not salt.strip():
        raise ValueError("the PII salt is empty; PII cannot be hashed")
    catalog, schema, volume = params.get("catalog"), params.get("schema"), params.get("volume")
    if not (catalog and schema and volume):
        raise ValueError("catalog, schema and volume are required")

    end = datetime.fromisoformat(day).replace(hour=hour, tzinfo=UTC)
    if run_batch is None:
        from repopulse.spark.batch import run_batch
    return run_batch(
        spark,
        source=source,
        hours=ingest.target_hours(end, lag_hours=0, count=hours),
        landing_dir=Path(landing_dir(catalog=catalog, schema=schema, volume=volume, source=source)),
        salt=salt,
        catalog=catalog,
        schema=schema,
        late_window_hours=late_window,
        persist=False,
    )
