"""Append the rows of a benchmark artifact CSV to ``runs.csv`` (schema-checked).

Usage::

    python benchmarks/record_run.py benchmark-row.csv [pipeline-row.csv ...] [--runs-csv runs.csv]

Every row of every artifact is parsed and validated before anything is written, so one bad
artifact leaves ``runs.csv`` untouched.
"""

from __future__ import annotations

import argparse
import csv
import os
import sys
from pathlib import Path

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from repopulse.runs_log import COLUMNS, RunsLogError, append_run, validate_row  # noqa: E402

FLOAT_COLUMNS = frozenset(c for c in COLUMNS if c.endswith("_seconds") or c == "peak_rss_mb")
INT_COLUMNS = frozenset({"bytes_read", "row_count"})


def _parse(col: str, text: str) -> object:
    if text == "":
        return None
    try:
        if col in FLOAT_COLUMNS:
            return float(text)
        if col in INT_COLUMNS:
            return int(text)
    except ValueError:
        raise RunsLogError(f"{col}: not a number (got {text!r})") from None
    return text


def read_artifact(path: str | os.PathLike[str]) -> list[dict[str, object]]:
    with open(path, encoding="utf-8", newline="") as fh:
        reader = csv.reader(fh)
        header = next(reader, None)
        if header != list(COLUMNS):
            raise RunsLogError(f"header of {os.fspath(path)} does not match COLUMNS: {header!r}")
        rows = []
        for values in reader:
            if not values:
                continue
            if len(values) != len(COLUMNS):
                raise RunsLogError(f"expected {len(COLUMNS)} values, got {len(values)}")
            row = {c: _parse(c, v) for c, v in zip(COLUMNS, values, strict=True)}
            validate_row(row)
            rows.append(row)
    return rows


def _existing_rows(runs_csv: str | os.PathLike[str]) -> set[tuple[str, ...]]:
    if not os.path.exists(runs_csv):
        return set()
    with open(runs_csv, encoding="utf-8", newline="") as fh:
        return {tuple(values) for values in list(csv.reader(fh))[1:] if values}


def new_rows(
    rows: list[dict[str, object]], runs_csv: str | os.PathLike[str]
) -> list[dict[str, object]]:
    """Rows not already in ``runs_csv`` (identical in every column) nor repeated earlier."""
    seen = _existing_rows(runs_csv)
    fresh = []
    for row in rows:
        key = tuple(validate_row(row)[c] for c in COLUMNS)
        if key in seen:
            continue
        seen.add(key)
        fresh.append(row)
    return fresh


def record(artifact: str | os.PathLike[str], runs_csv: str | os.PathLike[str]) -> int:
    """Validate every artifact row, then append the new ones; return how many were appended.

    Re-recording the same artifact is a no-op: rows already in ``runs_csv`` are skipped.
    """
    rows = new_rows(read_artifact(artifact), runs_csv)
    for row in rows:
        append_run(runs_csv, row)
    return len(rows)


def record_many(
    artifacts: list[str | os.PathLike[str]], runs_csv: str | os.PathLike[str]
) -> tuple[int, int]:
    """Validate the rows of every artifact first, then append the new ones.

    One bad artifact appends nothing. Returns (rows read, rows appended).
    """
    rows = [row for artifact in artifacts for row in read_artifact(artifact)]
    fresh = new_rows(rows, runs_csv)
    for row in fresh:
        append_run(runs_csv, row)
    return len(rows), len(fresh)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Append benchmark artifact rows to runs.csv.")
    parser.add_argument("artifacts", type=Path, nargs="+", metavar="artifact")
    parser.add_argument("--runs-csv", type=Path, default=Path("runs.csv"))
    args = parser.parse_args(argv)
    try:
        total, n = record_many(args.artifacts, args.runs_csv)
    except (RunsLogError, OSError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    print(f"appended {n} row(s) to {args.runs_csv}")
    if total > n:
        print(f"skipped {total - n} duplicate row(s) already in {args.runs_csv}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
