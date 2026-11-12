"""Engine matrix (E1): which ``runs.csv`` row stands for each of the six engines.

Pure and light (standard library only, no ``benchmarks`` import): ``readme_results`` uses it to
render one table, ``benchmarks/engine_matrix.py`` uses the engine names and variants.

For every engine of :data:`ENGINES`, in that fixed order, the entry is the newest row (by
``run_at``) of variant ``matrix-<engine>``. Snowflake is not measured again (trial credits): when
there is no ``matrix-snowflake`` row, the newest ``dbt-build-snowflake`` row is reused and marked
:data:`REUSED`, because it measures something else (``dbt build`` of the marts on the seed
fixture, not Q1-Q6 on the reference day). An engine with no row is :data:`MISSING` and gets a
warning naming it. A ``matrix-<name>`` row whose name is not one of the six engines is not
dropped: it is returned in :attr:`Matrix.unknown` (the README lists it under "Other runs") and
gets a warning too.

Method: ``docs/engine-matrix.md``.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime

ENGINES: tuple[str, ...] = (
    "duckdb",
    "spark",
    "databricks-serverless",
    "databricks-sql",
    "motherduck",
    "snowflake",
)
MATRIX_PREFIX = "matrix-"
SNOWFLAKE_REUSED_VARIANT = "dbt-build-snowflake"

MEASURED = "measured"
REUSED = "reused"
MISSING = "missing"

Row = Mapping[str, object]


@dataclass(frozen=True)
class MatrixEntry:
    engine: str
    row: Row | None
    source: str  # MEASURED, REUSED or MISSING


@dataclass(frozen=True)
class Matrix:
    entries: tuple[MatrixEntry, ...]
    warnings: tuple[str, ...]
    unknown: tuple[Row, ...]

    @property
    def missing(self) -> tuple[str, ...]:
        return tuple(e.engine for e in self.entries if e.source == MISSING)


def matrix_variant(engine: str) -> str:
    """``matrix-<engine>`` for one of :data:`ENGINES`."""
    if engine not in ENGINES:
        raise ValueError(f"unknown engine {engine!r}; expected one of {', '.join(ENGINES)}")
    return MATRIX_PREFIX + engine


def is_matrix_variant(variant: object) -> bool:
    return isinstance(variant, str) and variant.startswith(MATRIX_PREFIX)


def is_known_matrix_variant(variant: object) -> bool:
    return is_matrix_variant(variant) and str(variant)[len(MATRIX_PREFIX) :] in ENGINES


def _text(row: Row, col: str) -> str:
    value = row.get(col)
    return "" if value is None else str(value)


def _key(row: Row) -> tuple[datetime, str, str, str]:
    raw = _text(row, "run_at")
    try:
        when = datetime.fromisoformat(raw)
    except ValueError:
        raise ValueError(f"run_at is not an ISO-8601 timestamp: {raw!r}") from None
    if when.tzinfo is None:
        when = when.replace(tzinfo=UTC)
    # ties on run_at are broken by content, so the input order never decides
    return (when, _text(row, "git_sha"), _text(row, "total_seconds"), _text(row, "notes"))


def _newest(rows: Iterable[Row]) -> Row | None:
    best: Row | None = None
    for row in rows:
        if best is None or _key(row) > _key(best):
            best = row
    return best


def build_matrix(rows: Iterable[Row]) -> Matrix:
    """One entry per engine (fixed order) plus warnings for missing engines and unknown rows."""
    by_variant: dict[str, list[Row]] = {}
    unknown: list[Row] = []
    for row in rows:
        variant = _text(row, "variant")
        _key(row)  # a bad run_at is an error even for rows that are not chosen
        if is_matrix_variant(variant) and not is_known_matrix_variant(variant):
            unknown.append(row)
        by_variant.setdefault(variant, []).append(row)

    entries: list[MatrixEntry] = []
    warnings: list[str] = []
    for engine in ENGINES:
        variant = matrix_variant(engine)
        row = _newest(by_variant.get(variant, ()))
        if row is not None:
            entries.append(MatrixEntry(engine, row, MEASURED))
            continue
        if engine == "snowflake":
            reused = _newest(by_variant.get(SNOWFLAKE_REUSED_VARIANT, ()))
            if reused is not None:
                entries.append(MatrixEntry(engine, reused, REUSED))
                continue
            warnings.append(
                f"engine snowflake is missing: no {variant} row and no "
                f"{SNOWFLAKE_REUSED_VARIANT} row to reuse in runs.csv"
            )
        else:
            warnings.append(f"engine {engine} is missing: no {variant} row in runs.csv")
        entries.append(MatrixEntry(engine, None, MISSING))

    unknown.sort(key=lambda r: (_text(r, "variant"), _key(r)))
    for variant in sorted({_text(r, "variant") for r in unknown}):
        warnings.append(
            f"{variant} is not one of the six engines ({', '.join(ENGINES)}); "
            "listed under Other runs"
        )
    return Matrix(tuple(entries), tuple(warnings), tuple(unknown))
