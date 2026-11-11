"""Append the rows of a benchmark artifact CSV to ``runs.csv`` (schema-checked).

Usage::

    python benchmarks/record_run.py benchmark-row.csv [--runs-csv runs.csv]

Every row is parsed and validated before anything is written, so a bad artifact leaves
``runs.csv`` untouched.
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


def record(artifact: str | os.PathLike[str], runs_csv: str | os.PathLike[str]) -> int:
    """Validate every artifact row, then append them all; return how many were appended."""
    rows = read_artifact(artifact)
    for row in rows:
        append_run(runs_csv, row)
    return len(rows)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Append benchmark artifact rows to runs.csv.")
    parser.add_argument("artifact", type=Path)
    parser.add_argument("--runs-csv", type=Path, default=Path("runs.csv"))
    args = parser.parse_args(argv)
    try:
        n = record(args.artifact, args.runs_csv)
    except RunsLogError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    print(f"appended {n} row(s) to {args.runs_csv}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
