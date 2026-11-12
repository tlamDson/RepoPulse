"""Generate the Results block of README.md from runs.csv.

    python -m repopulse.readme_results          # rewrite the block between the markers
    python -m repopulse.readme_results --check  # exit 1 if README.md is stale, write nothing

``runs.csv`` is the only source of numbers for the README. This module copies the cells of
``runs.csv`` as written: it never rounds, never computes a ratio or a difference, and never adds a
number of its own. An empty cell is shown as ``—``.

For every (variant, data_date) the newest row (by ``run_at``) is shown, in tables grouped by what
the rows measure. A row is compared with ``variant=baseline`` only when it has the same
``data_date`` as a baseline row; every other row is listed where it belongs, and is never dropped
silently.

The engine matrix (E1, ``repopulse.engine_matrix``) is one table with one row per engine in a
fixed order: the newest ``matrix-<engine>`` row, the reused ``dbt-build-snowflake`` row for
Snowflake (labelled not comparable), or ``—`` with a warning line for a missing engine. Those
rows are not repeated under "Other runs"; ``main`` prints the warnings to stderr without changing
the exit code.

The block lives between ``<!-- results:start -->`` and ``<!-- results:end -->`` in README.md.
Output is deterministic: the same rows in any order give the same text.
"""

from __future__ import annotations

import argparse
import csv
import re
import sys
from collections.abc import Iterable, Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path

from repopulse import engine_matrix, runs_log

START_MARKER = "<!-- results:start -->"
END_MARKER = "<!-- results:end -->"
EMPTY = "—"

TITLE_LAYOUTS = "Query layouts vs naive baseline"
TITLE_SCALE = "Spark vs DuckDB at 1, 7 and 30 days"
TITLE_DBT = "dbt build: DuckDB vs Snowflake"
TITLE_DQ = "Data quality fault injection (simulated data)"
TITLE_DBT_DATABRICKS = "dbt build on a Databricks SQL warehouse (real GH Archive data)"
VARIANT_DBT_DATABRICKS = "dbt-build-databricks"
TITLE_MATRIX = "Engine matrix: Q1 to Q6 on six engines (real GH Archive data)"
TITLE_OTHER = "Other runs"

_COMMON = (
    "variant",
    "data_date",
    "total_seconds",
    "q1_seconds",
    "q2_seconds",
    "q3_seconds",
    "q4_seconds",
    "q5_seconds",
    "q6_seconds",
    "peak_rss_mb",
    "bytes_read",
    "row_count",
    "run_at",
    "git_sha",
)
_SCALE_VARIANT = re.compile(r"(duckdb|spark)-(\d+)d")
_ENGINE_ORDER = {"duckdb": 0, "spark": 1}
_DQ_COLUMNS = (
    "variant",
    "data_date",
    "total_seconds",
    "row_count",
    "simulated",
    "seed",
    "users",
    "days",
    "injected",
    "caught",
    "false_alarms",
    "clean_runs",
    "duplicate",
    "null",
    "wrong_type",
    "late",
    "schema_drift",
    "bot",
    "run_at",
    "git_sha",
)
_MATRIX_COLUMNS = (
    "engine",
    "source",
    "variant",
    "data_date",
    "total_seconds",
    "q1_seconds",
    "q2_seconds",
    "q3_seconds",
    "q4_seconds",
    "q5_seconds",
    "q6_seconds",
    "row_count",
    "timing",
    "cross_check",
    "run_at",
    "git_sha",
)
_MATRIX_SOURCE = {
    engine_matrix.MEASURED: "measured: Q1-Q6 on the reference day",
    engine_matrix.REUSED: (
        f"reused {engine_matrix.SNOWFLAKE_REUSED_VARIANT}: dbt build of the marts on the seed "
        "fixture, not comparable"
    ),
    engine_matrix.MISSING: "missing: no row in runs.csv yet",
}
_RUN_COLUMNS = frozenset(runs_log.COLUMNS)

Row = Mapping[str, str | None]


class ReadmeResultsError(ValueError):
    """runs.csv or README.md cannot be used (missing file, wrong header, missing markers)."""


# --- reading ---------------------------------------------------------------------------------


def read_runs(path: str | Path) -> list[dict[str, str | None]]:
    """Rows of runs.csv (empty cell -> None). The header must be ``runs_log.COLUMNS``."""
    p = Path(path)
    if not p.is_file():
        raise ReadmeResultsError(f"runs.csv not found: {p}")
    with open(p, encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        header = next(reader, None)
        if header != list(runs_log.COLUMNS):
            raise ReadmeResultsError(f"header of {p} does not match runs_log.COLUMNS: {header!r}")
        rows: list[dict[str, str | None]] = []
        for line_no, cells in enumerate(reader, start=2):
            if not cells:
                continue
            if len(cells) != len(header):
                raise ReadmeResultsError(f"line {line_no} of {p} has {len(cells)} cells")
            rows.append({c: (v if v != "" else None) for c, v in zip(header, cells, strict=True)})
    return rows


# --- rendering -------------------------------------------------------------------------------


def _value(row: Row, col: str) -> str | None:
    v = row.get(col)
    if v is None or v == "":
        return None
    return str(v)


def _escape(text: str) -> str:
    return text.replace("|", "\\|").replace("\r", " ").replace("\n", " ")


def _notes(row: Row) -> dict[str, str]:
    """``key=value`` words of ``notes`` (a trailing ``;`` of a value is dropped)."""
    out: dict[str, str] = {}
    for word in (_value(row, "notes") or "").split():
        key, sep, val = word.partition("=")
        if sep and key and key not in out:
            out[key] = val.rstrip(";")
    return out


def _cell(row: Row, col: str, extra: Mapping[str, str]) -> str:
    if col in _RUN_COLUMNS:
        v = _value(row, col)
        if v is not None and col == "git_sha":
            v = v[:7]
    else:
        v = extra.get(col)
    if v is None or v == "":
        return EMPTY
    return _escape(v)


def _table(rows: Sequence[Row], columns: Sequence[str]) -> str:
    """Markdown table; ``columns`` not in runs.csv are read from the ``notes`` words."""
    header = "| " + " | ".join(columns) + " |"
    rule = "| " + " | ".join("---" for _ in columns) + " |"
    lines = [header, rule]
    for row in rows:
        extra = _notes(row)
        lines.append("| " + " | ".join(_cell(row, c, extra) for c in columns) + " |")
    return "\n".join(lines)


def _when(row: Row) -> datetime:
    raw = _value(row, "run_at")
    try:
        when = datetime.fromisoformat(raw or "")
    except ValueError:
        raise ReadmeResultsError(f"run_at is not an ISO-8601 timestamp: {raw!r}") from None
    return when if when.tzinfo is not None else when.replace(tzinfo=UTC)


def _newest(rows: Iterable[Row]) -> list[Row]:
    """The newest row (by run_at) of each (variant, data_date); ties broken by content."""
    best: dict[tuple[str, str], Row] = {}
    for row in rows:
        variant, data_date = _value(row, "variant"), _value(row, "data_date")
        if variant is None or data_date is None:
            raise ReadmeResultsError(f"row without variant or data_date: {dict(row)!r}")

        def key(r: Row) -> tuple[datetime, str, str, str]:
            return (
                _when(r),
                _value(r, "git_sha") or "",
                _value(r, "total_seconds") or "",
                _value(r, "notes") or "",
            )

        current = best.get((variant, data_date))
        if current is None or key(row) > key(current):
            best[(variant, data_date)] = row
    return list(best.values())


def _section(title: str, prose: str, rows: Sequence[Row], columns: Sequence[str]) -> str:
    if not rows:
        return f"### {title}\n\nNo rows in `runs.csv` for this section yet."
    return f"### {title}\n\n{prose}\n\n{_table(rows, columns)}"


_PROSE_LAYOUTS = (
    "Real GH Archive data (not simulated). Each row has the same `data_date` as a `baseline` "
    "row: the same day of data and the same queries Q1 to Q6. `total_seconds` of `baseline` "
    "includes loading the `*.json.gz` files into a table, while the `qopt-*` rows read parquet "
    "layouts, so compare `q1_seconds` to `q6_seconds`, not `total_seconds`. `bytes_read` of "
    "`baseline` is the size of the input files; for `qopt-*` it is what Q1 to Q6 read. The "
    "layouts are described in [`docs/query-optimization.md`](docs/query-optimization.md)."
)
_PROSE_SCALE = (
    "Real GH Archive data (not simulated), read from the same files by both engines. These rows "
    "have a different data date than `baseline` and a different measured step, so they are not "
    "compared with `baseline`: compare a `spark-Nd` row with the `duckdb-Nd` row of the same N. "
    "`notes` is copied from `runs.csv`; Spark startup is outside `total_seconds`. Method: "
    "[`docs/benchmark-scale.md`](docs/benchmark-scale.md)."
)
_PROSE_DBT = (
    "Both builds run the same dbt project on the seed fixture (a small set of hand-made events "
    "for CI), not on GH Archive data, and are not compared with `baseline`. `total_seconds` is "
    "the whole `dbt build`; `q1_seconds` to `q6_seconds` are the build time of the mart for Q1 "
    "to Q6, not the time of the query; `row_count` is the number of seed rows; `lines_changed` "
    "is read from `notes`. Method: [`docs/snowflake-port.md`](docs/snowflake-port.md)."
)
_PROSE_DBT_DATABRICKS = (
    "Real GH Archive data (not simulated): the dbt project builds on a Databricks SQL warehouse "
    "from the silver Delta table written by the Databricks bronze/silver job, not from the "
    "hand-made CI events, so these rows are compared neither with the builds above nor with "
    "`baseline`. "
    "`total_seconds` is the whole `dbt build`; `q1_seconds` to `q6_seconds` are the build time "
    "of the mart for Q1 to Q6; `row_count` is the number of staged events and `data_date` the "
    "earliest `event_date`; `max_event_date` and `lines_changed` are read from `notes`. "
    "Method: [`docs/dbt-databricks.md`](docs/dbt-databricks.md)."
)
_PROSE_DQ = (
    "Simulated data: app events from `simkit` (no real users) with a known number of faults "
    "injected, run through the Spark job. `injected` and `caught` count faults; `false_alarms` "
    "counts findings that match no injected fault, including those of the clean runs "
    "(`clean_runs`). The six fault types show caught/injected. Every value after `row_count` is "
    "read from `notes`. Method: [`docs/data-quality.md`](docs/data-quality.md)."
)
_PROSE_OTHER = (
    "Not compared with `baseline`: either there is no baseline row with the same `data_date`, "
    "or the variant is not one of the groups above. `notes` is copied from `runs.csv`."
)


_PROSE_MATRIX = (
    "Real GH Archive data (not simulated): the same reference day as `baseline` and the same "
    "queries Q1 to Q6 on every engine, one row per engine in a fixed order (the newest "
    "`matrix-<engine>` run). Each engine loads the day first (not timed), runs Q1 to Q6 once as "
    "a warm-up, then several timed runs; `q1_seconds` to `q6_seconds` are medians and "
    "`total_seconds` is the median of the per-run sum of Q1 to Q6, so unlike `baseline` it "
    "does not include loading. `timing` says where the clock runs: `in-process` engines "
    "(`duckdb`, `spark`) are timed in the process that runs the query, `client` engines "
    "(`motherduck`, `databricks-sql`) are timed by the client on the runner, so their times "
    "include the network, and `notebook` (`databricks-serverless`) is timed inside the "
    "Databricks notebook. A row is written only when its results matched DuckDB on the same "
    "data (`cross_check`). The `snowflake` row is reused from `dbt-build-snowflake` (the trial "
    "is not spent again): the build time of the marts with `dbt build` on the seed fixture, a "
    "different measurement that is not comparable with the other rows. Method: "
    "[`docs/engine-matrix.md`](docs/engine-matrix.md)."
)

_FOOTER = "The full log, with every run and not only the newest, is [`runs.csv`](runs.csv)."


def _matrix_section(matrix: engine_matrix.Matrix) -> str:
    """One table, one row per engine in the fixed order; missing engines show ``—``."""
    header = "| " + " | ".join(_MATRIX_COLUMNS) + " |"
    rule = "| " + " | ".join("---" for _ in _MATRIX_COLUMNS) + " |"
    lines = [header, rule]
    for entry in matrix.entries:
        row: Row = entry.row or {}
        extra = {"engine": entry.engine, "source": _MATRIX_SOURCE[entry.source]}
        if entry.row is not None:
            notes = _notes(row)
            for key in ("timing", "cross_check"):
                if key in notes:
                    extra[key] = notes[key]
        lines.append("| " + " | ".join(_cell(row, c, extra) for c in _MATRIX_COLUMNS) + " |")
    parts = [f"### {TITLE_MATRIX}", _PROSE_MATRIX, "\n".join(lines)]
    if matrix.warnings:
        parts.append("\n".join(f"- Warning: {_escape(w)}." for w in matrix.warnings))
    return "\n\n".join(parts)


def render(rows: Iterable[Row]) -> str:
    """Markdown of the Results block for ``rows`` (dicts keyed by ``runs_log.COLUMNS``)."""
    rows = list(rows)
    newest = _newest(rows)  # checks variant, data_date and run_at of every row first
    matrix = engine_matrix.build_matrix(rows)
    # matrix-<engine> rows of the six engines live only in the matrix table
    latest = [r for r in newest if not engine_matrix.is_known_matrix_variant(_value(r, "variant"))]
    baseline_dates = {_value(r, "data_date") for r in latest if _value(r, "variant") == "baseline"}

    layouts: list[Row] = []
    scale: list[Row] = []
    dbt: list[Row] = []
    dbt_databricks: list[Row] = []
    dq: list[Row] = []
    other: list[Row] = []
    for row in latest:
        variant = _value(row, "variant") or ""
        if variant == "baseline" or (
            variant.startswith("qopt-") and _value(row, "data_date") in baseline_dates
        ):
            layouts.append(row)
        elif _SCALE_VARIANT.fullmatch(variant):
            scale.append(row)
        elif variant == VARIANT_DBT_DATABRICKS:
            dbt_databricks.append(row)
        elif variant.startswith("dbt-build-"):
            dbt.append(row)
        elif variant == "dq-fault-injection":
            dq.append(row)
        else:
            other.append(row)

    def by_date_baseline_first(r: Row) -> tuple[str, int, str]:
        variant = _value(r, "variant") or ""
        return (_value(r, "data_date") or "", 0 if variant == "baseline" else 1, variant)

    def by_days_engine(r: Row) -> tuple[int, int, str, str]:
        match = _SCALE_VARIANT.fullmatch(_value(r, "variant") or "")
        assert match is not None
        return (
            int(match.group(2)),
            _ENGINE_ORDER[match.group(1)],
            _value(r, "data_date") or "",
            match.group(0),
        )

    def by_variant_date(r: Row) -> tuple[str, str]:
        return (_value(r, "variant") or "", _value(r, "data_date") or "")

    def by_date_variant(r: Row) -> tuple[str, str]:
        return (_value(r, "data_date") or "", _value(r, "variant") or "")

    parts = [
        "Generated by `python -m repopulse.readme_results` from `runs.csv`: for each variant and "
        "data date the newest run is shown, and every cell is copied from `runs.csv` as written "
        f"(`{EMPTY}` is an empty cell). Do not edit this block by hand.",
        _section(
            TITLE_LAYOUTS, _PROSE_LAYOUTS, sorted(layouts, key=by_date_baseline_first), _COMMON
        ),
        _matrix_section(matrix),
        _section(TITLE_SCALE, _PROSE_SCALE, sorted(scale, key=by_days_engine), (*_COMMON, "notes")),
        _section(
            TITLE_DBT,
            _PROSE_DBT,
            sorted(dbt, key=by_variant_date),
            (*_COMMON[:12], "lines_changed", *_COMMON[12:]),
        ),
        _section(TITLE_DQ, _PROSE_DQ, sorted(dq, key=by_date_variant), _DQ_COLUMNS),
    ]
    # Only once a row exists, so README.md does not change before the first Databricks build.
    if dbt_databricks:
        parts.append(
            _section(
                TITLE_DBT_DATABRICKS,
                _PROSE_DBT_DATABRICKS,
                sorted(dbt_databricks, key=by_date_variant),
                (*_COMMON[:12], "max_event_date", "lines_changed", *_COMMON[12:]),
            )
        )
    if other:
        parts.append(
            _section(
                TITLE_OTHER, _PROSE_OTHER, sorted(other, key=by_variant_date), (*_COMMON, "notes")
            )
        )
    # A closing paragraph also keeps the last table apart from the end marker (a comment line
    # right after a table row can be read as part of the table).
    parts.append(_FOOTER)
    return "\n\n".join(parts) + "\n"


# --- README ----------------------------------------------------------------------------------


def update_readme(readme_text: str, block: str) -> str:
    """Replace the text between the results markers with ``block``; the rest is untouched."""
    problem = f"README needs exactly one {START_MARKER} followed by exactly one {END_MARKER}"
    if readme_text.count(START_MARKER) != 1 or readme_text.count(END_MARKER) != 1:
        raise ReadmeResultsError(problem)
    start = readme_text.index(START_MARKER)
    end = readme_text.index(END_MARKER)
    if end < start:
        raise ReadmeResultsError(problem)
    head = readme_text[: start + len(START_MARKER)]
    tail = readme_text[end:]
    body = block.strip("\n")
    return f"{head}\n{body}\n{tail}"


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m repopulse.readme_results",
        description="Rewrite the Results block of README.md from runs.csv.",
    )
    parser.add_argument("--runs", default="runs.csv", help="path of runs.csv (default: runs.csv)")
    parser.add_argument("--readme", default="README.md", help="path of README.md")
    parser.add_argument(
        "--check",
        action="store_true",
        help="write nothing; exit 1 when the block in README.md differs from runs.csv",
    )
    args = parser.parse_args(argv)

    readme_path = Path(args.readme)
    try:
        rows = read_runs(args.runs)
        block = render(rows)
        # A missing engine is reported, never an error: CI stays green before a cloud run.
        for warning in engine_matrix.build_matrix(rows).warnings:
            print(f"warning: engine matrix: {warning}", file=sys.stderr)
        if not readme_path.is_file():
            raise ReadmeResultsError(f"README not found: {readme_path}")
        current = readme_path.read_text(encoding="utf-8")
        updated = update_readme(current, block)
    except (ReadmeResultsError, UnicodeDecodeError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2

    if args.check:
        if updated == current:
            print(f"{readme_path}: results block is up to date")
            return 0
        print(
            f"{readme_path}: results block is out of date with {args.runs}; "
            "run `python -m repopulse.readme_results` and commit README.md",
            file=sys.stderr,
        )
        return 1

    if updated == current:
        print(f"{readme_path}: results block already up to date")
        return 0
    with open(readme_path, "w", encoding="utf-8", newline="\n") as f:
        f.write(updated)
    print(f"{readme_path}: results block updated from {args.runs}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
