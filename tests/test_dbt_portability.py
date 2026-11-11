"""dbt SQL outside transform/macros/ runs on DuckDB and Snowflake (docs/snowflake-port.md).

DuckDB-only syntax is allowed only inside a cross-database macro, and every such macro dispatches
to a ``duckdb__`` and a ``snowflake__`` implementation. Comments and string literals are ignored.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
TRANSFORM = ROOT / "transform"
SQL_DIRS = (TRANSFORM / "models", TRANSFORM / "tests")
MACROS = TRANSFORM / "macros"

DUCKDB_ONLY = {
    "aggregate FILTER (WHERE ...)": r"\bfilter\s*\(\s*where\b",
    "arg_min/arg_max (use min_by/max_by)": r"\barg_(min|max)\s*\(",
    "quantile/quantile_cont (use percentile_cont within group)": r"\bquantile(_cont|_disc)?\s*\(",
    "epoch()": r"\bepoch\s*\(",
    "make_timestamp/epoch_us/epoch_ms": r"\b(make_timestamp|epoch_(us|ms))\s*\(",
    ":: cast (use cast(... as ...))": r"::",
    "list type x[]": r"\w\s*\[\s*\]",
    "bool_or/bool_and": r"\bbool_(or|and)\s*\(",
    "read_parquet/read_csv/read_json": r"\bread_(parquet|csv|json)\w*\s*\(",
    "ends_with/starts_with (Snowflake: endswith/startswith)": r"\b(ends|starts)_with\s*\(",
}
MACRO_DEF = re.compile(r"\{%-?\s*macro\s+(\w+)\s*\(")
DISPATCH = re.compile(r"adapter\.dispatch\(\s*['\"](\w+)['\"]")


def _blank(match: re.Match) -> str:
    return "\n" * match.group(0).count("\n")


def strip_comments_and_strings(text: str) -> str:
    """Drop Jinja/SQL comments and string literals, keeping line numbers."""
    text = re.sub(r"\{#.*?#\}", _blank, text, flags=re.S)
    text = re.sub(r"/\*.*?\*/", _blank, text, flags=re.S)
    text = re.sub(r"'(?:[^'\n]|'')*'", "''", text)
    text = re.sub(r'"(?:[^"\n])*"', '""', text)
    return re.sub(r"--[^\n]*", "", text)


def violations(path: Path, label: str | None = None) -> list[str]:
    """``<file>:<line>: <what>`` for each DuckDB-only construct in ``path``."""
    label = label or path.relative_to(ROOT).as_posix()
    text = strip_comments_and_strings(path.read_text(encoding="utf-8"))
    return [
        f"{label}:{lineno}: {what}"
        for lineno, line in enumerate(text.splitlines(), start=1)
        for what, pattern in DUCKDB_ONLY.items()
        if re.search(pattern, line, re.I)
    ]


def sql_files() -> list[Path]:
    return sorted(p for d in SQL_DIRS for p in d.rglob("*.sql"))


def macro_sources() -> dict[str, str]:
    """Macro name -> body text (comments/strings kept, for the dispatch name)."""
    out: dict[str, str] = {}
    for path in sorted(MACROS.rglob("*.sql")) if MACROS.is_dir() else []:
        text = path.read_text(encoding="utf-8")
        starts = list(MACRO_DEF.finditer(text))
        for i, m in enumerate(starts):
            end = starts[i + 1].start() if i + 1 < len(starts) else len(text)
            out[m.group(1)] = text[m.start() : end]
    return out


def dispatched() -> dict[str, str]:
    return {
        name: m.group(1)
        for name, body in macro_sources().items()
        if (m := DISPATCH.search(body)) is not None
    }


# ---------------------------------------------------------------- the scanner itself


@pytest.mark.parametrize(
    "sql",
    [
        "select count(*) filter (where x) from t",
        "select arg_min(a, b) from t",
        "select quantile_cont(x, 0.5) from t",
        "select epoch(ts) from t",
        "select make_timestamp(epoch_us(ts)) from t",
        "select epoch_ms(ts) from t",
        "select x::double from t",
        "select cast(x as varchar[]) from t",
        "select bool_or(x) from t",
        "select * from read_parquet('a/*.parquet')",
        "select ends_with(lower(x), 'bot') from t",
    ],
)
def test_scanner_flags_duckdb_only_syntax(sql, tmp_path):
    f = tmp_path / "m.sql"
    f.write_text("-- header\n" + sql + "\n", encoding="utf-8")
    found = violations(f, "m.sql")
    assert found and all(v.startswith("m.sql:2:") for v in found), found


@pytest.mark.parametrize(
    "sql",
    [
        "-- count(*) filter (where x) and x::double in a comment",
        "{# arg_min(a, b) in a Jinja comment #}",
        "select 'epoch(ts) :: in a string' as s",
        "select count(case when x then 1 end), min_by(a, b) from t",
        "select percentile_cont(0.5) within group (order by x) from t",
        "select {{ hours_between('a', 'b') }} from {{ ref('t') }}",
    ],
)
def test_scanner_ignores_comments_strings_and_portable_sql(sql, tmp_path):
    f = tmp_path / "m.sql"
    f.write_text(sql + "\n", encoding="utf-8")
    assert violations(f, "m.sql") == []


# ---------------------------------------------------------------- the project


def test_models_and_tests_have_sql_files():
    names = {p.name for p in sql_files()}
    assert {"stg_gharchive_events.sql", "mart_repo_health_score.sql"} <= names
    assert "assert_dim_user_scd2_intervals.sql" in names


def test_no_duckdb_only_syntax_outside_macros():
    found = [v for p in sql_files() for v in violations(p)]
    assert found == [], "DuckDB-only syntax outside transform/macros/:\n" + "\n".join(found)


def test_there_are_cross_database_macros():
    assert dispatched(), f"no adapter.dispatch macro under {MACROS}"


def test_staging_normalises_created_at_to_utc_through_a_cross_database_macro():
    staging = (TRANSFORM / "models" / "staging" / "stg_gharchive_events.sql").read_text(
        encoding="utf-8"
    )
    assert dispatched().get("utc_timestamp") == "utc_timestamp", sorted(dispatched())
    assert re.search(r"\{\{\s*utc_timestamp\(\s*'created_at'\s*\)\s*\}\}", staging)


def test_every_cross_database_macro_has_duckdb_and_snowflake_branches():
    macros = macro_sources()
    missing = [
        f"{name} -> {impl}__{target}"
        for name, target in dispatched().items()
        for impl in ("duckdb", "snowflake")
        if f"{impl}__{target}" not in macros
    ]
    assert missing == [], missing


def test_snowflake_branch_of_a_parquet_reader_raises_a_compile_error():
    macros = macro_sources()
    readers = [
        target
        for target in dispatched().values()
        if re.search(r"\bread_parquet\s*\(", macros.get(f"duckdb__{target}", ""))
    ]
    assert readers, "the parquet source of staging must be a cross-database macro"
    for target in readers:
        body = macros.get(f"snowflake__{target}", "")
        assert "exceptions.raise_compiler_error" in body, target
        assert "silver_gharchive_parquet" in body, target


def test_duckdb_only_syntax_in_macros_stays_in_duckdb_branches():
    found = []
    for name, body in macro_sources().items():
        if name.startswith("duckdb__"):
            continue
        text = strip_comments_and_strings(body)
        found += [
            f"{name}: {what}"
            for what, pattern in DUCKDB_ONLY.items()
            if re.search(pattern, text, re.I)
        ]
    assert found == [], found
