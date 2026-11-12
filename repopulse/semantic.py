"""SQL of semantic-layer metrics (MetricFlow) for the DuckDB / MotherDuck gold.

Reads the ``semantic_manifest.json`` that ``dbt parse``/``dbt build`` writes into
``transform/target/`` and compiles metric queries to DuckDB SQL with the MetricFlow engine that
ships with dbt-core (>= 1.12). Nothing is executed here: run the SQL with ``duckdb`` against the
database the manifest was built for. Definitions: ``transform/models/public/_semantic.yml``,
docs/semantic-layer.md.

Usage::

    python -m repopulse.semantic sql --manifest transform/target/semantic_manifest.json \
        --metrics cohort_size,retention_w4_pct --group-by q2_cohort__cohort_week__week

``metricflow`` is imported lazily: the package itself has no dependency.
"""

from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import Any


class _DuckDbCompiler:
    """The part of MetricFlow's ``SqlClient`` that ``MetricFlowEngine.explain`` uses."""

    def __init__(self) -> None:
        from metricflow.protocols.sql_client import SqlEngine
        from metricflow.sql.render.duckdb_renderer import DuckDbSqlPlanRenderer

        self.sql_engine_type = SqlEngine.DUCKDB
        self.sql_plan_renderer = DuckDbSqlPlanRenderer()

    def render_bind_parameter_key(self, bind_parameter_key: str) -> str:
        return f"${bind_parameter_key}"

    def query(self, *args: Any, **kwargs: Any) -> Any:
        raise NotImplementedError("compile only: run the SQL with duckdb")

    execute = dry_run = query

    def close(self) -> None:
        return None


def load_manifest(path: str | Path) -> Any:
    """``semantic_manifest.json`` written by dbt, with MetricFlow's own load-time transforms."""
    from metricflow_semantics.model.dbt_manifest_parser import (
        parse_manifest_from_dbt_generated_manifest,
    )

    return parse_manifest_from_dbt_generated_manifest(Path(path).read_text(encoding="utf-8"))


def compile_sql(
    manifest: Any,
    metrics: Sequence[str],
    group_by: Sequence[str] = (),
    order_by: Sequence[str] | None = None,
) -> str:
    """DuckDB SQL computing ``metrics`` grouped by ``group_by`` (MetricFlow dunder names)."""
    from metricflow.engine.metricflow_engine import MetricFlowEngine, MetricFlowQueryRequest
    from metricflow_semantics.model.semantic_manifest_lookup import SemanticManifestLookup

    engine = MetricFlowEngine(SemanticManifestLookup(manifest), _DuckDbCompiler())
    request = MetricFlowQueryRequest.create(
        metric_names=list(metrics),
        group_by_names=list(group_by),
        order_by_names=list(order_by) if order_by else None,
    )
    return engine.explain(request).sql_statement.sql


def _names(value: str) -> list[str]:
    return [v.strip() for v in value.split(",") if v.strip()]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m repopulse.semantic", description="Semantic-layer metrics -> DuckDB SQL."
    )
    sub = parser.add_subparsers(dest="command", required=True)
    sql = sub.add_parser("sql", help="print the SQL of a metric query")
    sql.add_argument(
        "--manifest", type=Path, default=Path("transform/target/semantic_manifest.json")
    )
    sql.add_argument("--metrics", required=True, help="comma-separated metric names")
    sql.add_argument("--group-by", default="", help="comma-separated group-by names")
    sql.add_argument("--order-by", default="", help="comma-separated; '-' prefix = descending")
    args = parser.parse_args(argv)

    try:
        manifest = load_manifest(args.manifest)
        text = compile_sql(
            manifest, _names(args.metrics), _names(args.group_by), _names(args.order_by) or None
        )
    except Exception as exc:  # MetricFlow raises many error types for a bad query
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2
    print(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
