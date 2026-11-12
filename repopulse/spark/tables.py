"""Table names of the bronze/silver batch in a catalog (pure Python, no pyspark).

``[<catalog>.]<schema>.{bronze,silver,quarantine}_<table>`` with ``table`` = ``gharchive_events``
or ``app_events``. On Databricks the catalog is the Unity Catalog one (``workspace``); locally it
is left out and the session catalog is used.
"""

from __future__ import annotations

TABLES = {"gharchive": "gharchive_events", "app": "app_events"}
KINDS: tuple[str, ...] = ("bronze", "silver", "quarantine")


def table_names(source: str, *, schema: str, catalog: str | None = None) -> dict[str, str]:
    if source not in TABLES:
        raise ValueError(f"unknown source {source!r}")
    if not schema:
        raise ValueError("schema must not be empty")
    prefix = f"{catalog}.{schema}" if catalog else schema
    return {kind: f"{prefix}.{kind}_{TABLES[source]}" for kind in KINDS}
