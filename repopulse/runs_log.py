"""Append one measurement row to ``runs.csv`` with a strict schema check.

``runs.csv`` is the only source of numbers for README, PRs and reports, so every row is
validated before it touches disk and an invalid row never leaves a partial write behind.
"""

from __future__ import annotations

import csv
import math
import os
import re
from collections.abc import Callable, Mapping
from datetime import date, datetime

COLUMNS: tuple[str, ...] = (
    "run_at",
    "git_sha",
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
    "notes",
)
REQUIRED: frozenset[str] = frozenset({"run_at", "git_sha", "variant", "data_date", "total_seconds"})

_HEADER = ",".join(COLUMNS)
_GIT_SHA = re.compile(r"[0-9a-f]{7,40}")
_VARIANT = re.compile(r"[a-z0-9][a-z0-9_-]*")
_DATE = re.compile(r"\d{4}-\d{2}-\d{2}")


class RunsLogError(ValueError):
    """A row or an existing ``runs.csv`` does not match the schema."""


def _fail(col: str, value: object, why: str) -> RunsLogError:
    return RunsLogError(f"{col}: {why} (got {value!r})")


def _run_at(col: str, value: object) -> str:
    if isinstance(value, str):
        try:
            value = datetime.fromisoformat(value)
        except ValueError:
            raise _fail(col, value, "not an ISO-8601 timestamp") from None
    if not isinstance(value, datetime):
        raise _fail(col, value, "must be an ISO-8601 string or datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise _fail(col, value, "must include a timezone")
    return value.isoformat()


def _git_sha(col: str, value: object) -> str:
    if not isinstance(value, str) or not _GIT_SHA.fullmatch(value):
        raise _fail(col, value, "must be 7-40 lowercase hex characters")
    return value


def _variant(col: str, value: object) -> str:
    if not isinstance(value, str) or not _VARIANT.fullmatch(value):
        raise _fail(col, value, "must match ^[a-z0-9][a-z0-9_-]*$")
    return value


def _data_date(col: str, value: object) -> str:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, str) and _DATE.fullmatch(value):
        try:
            return date.fromisoformat(value).isoformat()
        except ValueError:
            pass
    raise _fail(col, value, "must be a valid YYYY-MM-DD date")


def _seconds(col: str, value: object) -> str:
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise _fail(col, value, "must be an int or float")
    if not math.isfinite(value) or value < 0:
        raise _fail(col, value, "must be finite and >= 0")
    return str(value)


def _count(col: str, value: object) -> str:
    if isinstance(value, bool) or not isinstance(value, int):
        raise _fail(col, value, "must be an int")
    if value < 0:
        raise _fail(col, value, "must be >= 0")
    return str(value)


def _notes(col: str, value: object) -> str:
    if not isinstance(value, str):
        raise _fail(col, value, "must be a string")
    if "\n" in value or "\r" in value:
        raise _fail(col, value, "must not contain line breaks")
    return value


_CHECKS: dict[str, Callable[[str, object], str]] = {
    "run_at": _run_at,
    "git_sha": _git_sha,
    "variant": _variant,
    "data_date": _data_date,
    **{c: _seconds for c in COLUMNS if c.endswith("_seconds") or c == "peak_rss_mb"},
    "bytes_read": _count,
    "row_count": _count,
    "notes": _notes,
}


def validate_row(row: Mapping[str, object]) -> dict[str, str]:
    """Check one row and return every column of ``COLUMNS`` as a CSV-ready string."""
    unknown = sorted(set(row) - set(COLUMNS))
    if unknown:
        raise RunsLogError(f"unknown column(s): {', '.join(unknown)}")
    out: dict[str, str] = {}
    for col in COLUMNS:
        value = row.get(col)
        if value is None or value == "":
            if col in REQUIRED:
                raise RunsLogError(f"{col}: required column is missing or empty")
            out[col] = ""
            continue
        out[col] = _CHECKS[col](col, value)
    return out


def append_run(path: str | os.PathLike[str], row: Mapping[str, object]) -> None:
    """Validate ``row`` and append it as exactly one line to the CSV at ``path``."""
    values = validate_row(row)
    write_header = True
    needs_newline = False
    if os.path.exists(path) and os.path.getsize(path) > 0:
        with open(path, "rb") as f:
            first = f.readline().decode("utf-8").rstrip("\n")
            f.seek(-1, os.SEEK_END)
            needs_newline = f.read(1) != b"\n"
        if first != _HEADER:
            raise RunsLogError(f"header of {os.fspath(path)} does not match COLUMNS: {first!r}")
        write_header = False
    with open(path, "a", encoding="utf-8", newline="") as f:
        if needs_newline:
            f.write("\n")
        writer = csv.writer(f, lineterminator="\n")
        if write_header:
            writer.writerow(COLUMNS)
        writer.writerow(values[c] for c in COLUMNS)
