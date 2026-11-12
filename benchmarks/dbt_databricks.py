"""One ``runs.csv`` row for a ``dbt build --target databricks`` (D2, docs/dbt-databricks.md).

The build reads the silver Delta table of the Databricks bronze/silver job (real GH Archive data,
not simulated, not the CI seed). The row holds:

- ``total_seconds``: ``elapsed_time`` of ``target/run_results.json`` (whole ``dbt build``);
- ``q1_seconds`` .. ``q6_seconds``: ``execution_time`` of the mart model of that question;
- ``row_count`` / ``data_date``: rows and earliest ``event_date`` of ``stg_gharchive_events``
  after the build, from a ``dbt show --output json --log-format json`` log (``--show-log``);
- ``notes``: ``engine=databricks; source=<table>; input=gharchive-silver-delta;
  max_event_date=..; lines_changed=<n>; q_seconds=mart build time; ..; base=..``.

``lines_changed`` = added + deleted lines of ``git diff --numstat <base_sha> HEAD -- transform``
(``base_sha`` in ``benchmarks/dbt_databricks.toml``). A failed build (a node ``error``, ``fail``,
``runtime error`` or ``skipped``) writes no row and exits non-zero; its failing nodes go to the
summary even when the show log is missing.

Usage (what ``.github/workflows/dbt-databricks.yml`` runs after the build)::

    python benchmarks/dbt_databricks.py --run-results transform/target/run_results.json \
        --show-log out/dbt-databricks-show.jsonl --git-sha <sha> \
        --out out/dbt-databricks-rows.csv --summary out/dbt-databricks-summary.md

Merge the artifact with ``python benchmarks/record_run.py out/dbt-databricks-rows.csv``.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from benchmarks.snowflake_port import (  # noqa: E402
    MART_BY_QUERY,
    PROJECT,
    PortError,
    build_failures,
    count_changed_lines,
    git_numstat,
    load_run_results,
)
from benchmarks.snowflake_port import load_config as _load_toml  # noqa: E402
from repopulse.runs_log import RunsLogError, append_run, validate_row  # noqa: E402
from repopulse.spark.tables import table_names  # noqa: E402

CONFIG = Path(__file__).with_suffix(".toml")
VARIANT = "dbt-build-databricks"
TARGET = "databricks"
DEFAULT_SOURCE = table_names("gharchive", schema="repopulse", catalog="workspace")["silver"]
_DATE = re.compile(r"\d{4}-\d{2}-\d{2}")

__all__ = ["MART_BY_QUERY", "RowError", "build_row", "input_stats", "main", "parse_show_log"]


class RowError(PortError):
    """Bad input for the Databricks row (run_results, show log) or a failed build."""


def load_config(path: Path = CONFIG) -> dict:
    return _load_toml(path)


# ---------------------------------------------------------------- dbt show


def parse_show_log(text: str) -> list[dict]:
    """Rows of ``dbt show --output json``: the ``ShowNode`` event of a ``--log-format json`` log,
    or the ``{"show": [...]}`` document of the plain output."""
    for line in text.splitlines():
        line = line.strip()
        if not line.startswith("{"):
            continue
        try:
            event = json.loads(line)
        except ValueError:
            continue
        if isinstance(event, dict) and (event.get("info") or {}).get("name") == "ShowNode":
            preview = (event.get("data") or {}).get("preview")
            try:
                rows = json.loads(preview) if isinstance(preview, str) else None
            except ValueError:
                rows = None
            if isinstance(rows, list):
                return rows
    start = text.find("{")
    if start >= 0:
        try:
            doc, _ = json.JSONDecoder().raw_decode(text[start:])
        except ValueError:
            doc = None
        if isinstance(doc, dict) and isinstance(doc.get("show"), list):
            return doc["show"]
    raise RowError("no dbt show result in the show log (did `dbt show --output json` run?)")


def input_stats(rows: Sequence[Mapping]) -> tuple[int, str, str]:
    """(row_count, min_event_date, max_event_date) from the single row of the stats query."""
    if len(rows) != 1:
        raise RowError(f"the stats query must return one row, got {len(rows)}")
    row = {str(k).lower(): v for k, v in rows[0].items()}
    try:
        # agate may hand a count back as a decimal (1234.0): accept it only when integral
        raw = Decimal(str(row["row_count"]))
        if raw != raw.to_integral_value():
            raise ValueError(f"row_count {raw} is not an integer")
        count = int(raw)
        first, last = str(row["min_event_date"]), str(row["max_event_date"])
    except (KeyError, TypeError, ValueError, InvalidOperation) as exc:
        raise RowError(f"stats row lacks row_count/min_event_date/max_event_date: {exc}") from None
    if count <= 0:
        raise RowError("stg_gharchive_events is empty: nothing was built from silver")
    for value in (first, last):
        if not _DATE.fullmatch(value):
            raise RowError(f"event date {value!r} is not YYYY-MM-DD")
    return count, first, last


# ---------------------------------------------------------------- row


def _check_build(run_results: Mapping) -> None:
    args = run_results.get("args") or {}
    if args.get("which") != "build":
        raise RowError(f"run_results is for `dbt {args.get('which')}`, expected `dbt build`")
    if args.get("target") != TARGET:
        raise RowError(f"run_results is for target {args.get('target')!r}, expected {TARGET!r}")
    failed = build_failures(run_results)
    if failed:
        raise RowError(f"dbt build failed on {len(failed)} node(s): {', '.join(failed)}")


def build_row(
    run_results: Mapping,
    *,
    git_sha: str,
    run_at: str,
    row_count: int,
    data_date: str,
    max_event_date: str,
    source: str,
    lines_changed: int,
    base: str = "",
) -> dict[str, object]:
    """One ``runs.csv`` row from the run_results of a successful Databricks ``dbt build``."""
    _check_build(run_results)
    times = {str(r.get("unique_id")): r.get("execution_time") for r in run_results["results"]}
    row: dict[str, object] = {
        "run_at": run_at,
        "git_sha": git_sha,
        "variant": VARIANT,
        "data_date": data_date,
        "total_seconds": run_results.get("elapsed_time"),
        "row_count": row_count,
    }
    for q, mart in MART_BY_QUERY.items():
        uid = f"model.{PROJECT}.{mart}"
        if uid not in times:
            raise RowError(f"{mart} is not in run_results: the build did not run every mart")
        row[f"{q}_seconds"] = times[uid]
    notes = [
        f"engine={TARGET}",
        f"source={source}",
        "input=gharchive-silver-delta",
        f"max_event_date={max_event_date}",
        f"lines_changed={lines_changed}",
        "q_seconds=mart build time",
        "total=dbt build elapsed_time",
    ]
    if base:
        notes.append(f"base={base[:12]}")
    row["notes"] = "; ".join(notes)
    validate_row(row)
    return row


# ---------------------------------------------------------------- CLI


def _summary(path: Path, source: str, row: Mapping | None, error: str | None) -> None:
    lines = [f"### dbt build `{VARIANT}` (Databricks SQL warehouse)", ""]
    lines.append(f"Input: `{source}`, real GH Archive data (not simulated).")
    lines.append("")
    if error:
        lines += [f"**Failed, no row written:** {error}", ""]
    else:
        v = validate_row(row)
        lines += ["| column | value |", "|---|---|"]
        lines += [f"| `{c}` | {v[c]} |" for c in v if v[c] != ""]
        lines.append("")
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as fh:
        fh.write("\n".join(lines) + "\n")


def _read(path: Path, what: str) -> str:
    try:
        return Path(path).read_text(encoding="utf-8")
    except FileNotFoundError:
        raise RowError(f"{what} {path} not found") from None


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="runs.csv row for a dbt build on Databricks.")
    parser.add_argument("--run-results", type=Path, required=True)
    parser.add_argument("--show-log", type=Path, required=True)
    parser.add_argument("--git-sha", required=True)
    parser.add_argument("--source", default=DEFAULT_SOURCE, help="silver table read by staging")
    src = parser.add_mutually_exclusive_group()
    src.add_argument("--base", default=None, help="base commit (default: base_sha in config)")
    src.add_argument("--numstat-file", type=Path, default=None)
    parser.add_argument("--config", type=Path, default=CONFIG)
    parser.add_argument("--out", type=Path, default=Path("out/dbt-databricks-rows.csv"))
    parser.add_argument("--summary", type=Path, default=None)
    args = parser.parse_args(argv)

    row = None
    try:
        run_results = load_run_results(args.run_results)
        _check_build(run_results)  # a failed build is reported before the show log is needed
        row_count, first, last = input_stats(parse_show_log(_read(args.show_log, "show log")))
        if args.numstat_file is not None:
            base = ""
            numstat = _read(args.numstat_file, "numstat file")
        else:
            base = args.base or load_config(args.config)["base_sha"]
            numstat = git_numstat(base)
        row = build_row(
            run_results,
            git_sha=args.git_sha,
            run_at=datetime.now(UTC).isoformat(timespec="seconds"),
            row_count=row_count,
            data_date=first,
            max_event_date=last,
            source=args.source,
            lines_changed=count_changed_lines(numstat),
            base=base,
        )
        args.out.parent.mkdir(parents=True, exist_ok=True)
        append_run(args.out, row)
    except (PortError, RunsLogError, OSError, KeyError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        if args.summary:
            _summary(args.summary, args.source, None, str(exc))
        return 1
    if args.summary:
        _summary(args.summary, args.source, row, None)
    print(f"wrote {VARIANT} row to {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
