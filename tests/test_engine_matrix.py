"""Engine matrix parser (E1): one entry per engine from runs.csv rows, warnings for missing ones.

Pure: rows are dicts keyed by ``runs_log.COLUMNS`` (as ``readme_results.read_runs`` returns);
nothing reads the real runs.csv and nothing touches a cloud service.
"""

from __future__ import annotations

import pytest

from repopulse import engine_matrix as em
from repopulse import runs_log

ENGINES = ("duckdb", "spark", "databricks-serverless", "databricks-sql", "motherduck", "snowflake")


def row(variant, run_at, total="1.5", data_date="2026-10-25", git_sha="abcdef1", notes=None):
    out = dict.fromkeys(runs_log.COLUMNS)
    out.update(
        run_at=run_at,
        git_sha=git_sha,
        variant=variant,
        data_date=data_date,
        total_seconds=total,
        notes=notes,
    )
    return out


def full(run_at="2026-11-12T10:00:00+00:00"):
    return [row(f"matrix-{e}", run_at) for e in ENGINES]


def by_engine(matrix):
    return {e.engine: e for e in matrix.entries}


def test_engine_order_is_fixed():
    assert em.ENGINES == ENGINES


def test_variant_of_an_engine_is_matrix_prefix():
    assert [em.matrix_variant(e) for e in ENGINES] == [f"matrix-{e}" for e in ENGINES]
    with pytest.raises(ValueError, match="oracle"):
        em.matrix_variant("oracle")


def test_entries_follow_the_fixed_order_whatever_the_input_order():
    rows = list(reversed(full()))

    matrix = em.build_matrix(rows)

    assert [e.engine for e in matrix.entries] == list(ENGINES)


def test_newest_row_of_each_engine_is_chosen_by_run_at_not_input_order():
    old = row("matrix-duckdb", "2026-11-12T09:00:00+00:00", total="9.0")
    new = row("matrix-duckdb", "2026-11-12T11:00:00+00:00", total="2.0")
    older_spark = row("matrix-spark", "2026-11-01T00:00:00+00:00", total="7.0")
    newer_spark = row("matrix-spark", "2026-11-02T00:00:00+00:00", total="3.0")

    for rows in ([old, new, older_spark, newer_spark], [new, old, newer_spark, older_spark]):
        entries = by_engine(em.build_matrix(rows))
        assert entries["duckdb"].row is new
        assert entries["spark"].row is newer_spark
        assert entries["duckdb"].source == em.MEASURED


def test_run_at_with_offsets_is_compared_as_time():
    # 10:30+02:00 is 08:30 UTC, older than 09:00 UTC.
    a = row("matrix-motherduck", "2026-11-12T10:30:00+02:00", total="1.0")
    b = row("matrix-motherduck", "2026-11-12T09:00:00+00:00", total="2.0")

    assert by_engine(em.build_matrix([a, b]))["motherduck"].row is b


def test_six_engines_present_means_no_warning():
    matrix = em.build_matrix(full())

    assert matrix.warnings == ()
    assert all(e.source == em.MEASURED for e in matrix.entries)
    assert matrix.missing == ()


def test_each_missing_engine_is_named_in_its_own_warning():
    rows = [r for r in full() if r["variant"] not in ("matrix-spark", "matrix-databricks-sql")]

    matrix = em.build_matrix(rows)

    assert matrix.missing == ("spark", "databricks-sql")
    assert len(matrix.warnings) == 2
    assert "spark" in matrix.warnings[0] and "matrix-spark" in matrix.warnings[0]
    assert "databricks-sql" in matrix.warnings[1]
    assert "matrix-databricks-sql" in matrix.warnings[1]
    entries = by_engine(matrix)
    assert entries["spark"].row is None and entries["spark"].source == em.MISSING


def test_no_rows_means_every_engine_is_missing_and_warned():
    matrix = em.build_matrix([])

    assert matrix.missing == ENGINES
    assert len(matrix.warnings) == len(ENGINES)
    for engine, warning in zip(ENGINES, matrix.warnings, strict=True):
        assert engine in warning


def test_snowflake_reuses_the_newest_dbt_build_snowflake_row_and_is_marked_reused():
    old = row("dbt-build-snowflake", "2026-11-10T00:00:00+00:00", data_date="2026-01-05")
    new = row("dbt-build-snowflake", "2026-11-11T16:20:35+00:00", data_date="2026-01-05")
    rows = [r for r in full() if r["variant"] != "matrix-snowflake"] + [new, old]

    matrix = em.build_matrix(rows)

    snow = by_engine(matrix)["snowflake"]
    assert snow.row is new
    assert snow.source == em.REUSED
    assert snow.source != em.MEASURED
    assert "snowflake" not in matrix.missing
    assert matrix.warnings == ()


def test_a_real_matrix_snowflake_row_wins_over_the_reused_dbt_row():
    reused = row("dbt-build-snowflake", "2026-11-13T00:00:00+00:00", data_date="2026-01-05")
    rows = [*full(), reused]

    snow = by_engine(em.build_matrix(rows))["snowflake"]

    assert snow.row["variant"] == "matrix-snowflake"
    assert snow.source == em.MEASURED


def test_snowflake_without_either_row_is_missing():
    rows = [r for r in full() if r["variant"] != "matrix-snowflake"]

    matrix = em.build_matrix(rows)

    assert matrix.missing == ("snowflake",)
    assert "snowflake" in matrix.warnings[0]


def test_other_variants_are_ignored_by_the_matrix():
    rows = [
        row("baseline", "2026-11-12T00:00:00+00:00"),
        row("dbt-build-duckdb", "2026-11-12T00:00:00+00:00"),
        row("spark-1d", "2026-11-12T00:00:00+00:00"),
    ]

    matrix = em.build_matrix(rows)

    assert matrix.missing == ENGINES
    assert matrix.unknown == ()


def test_matrix_variant_of_an_unknown_engine_is_not_swallowed():
    odd = row("matrix-oracle", "2026-11-12T00:00:00+00:00")

    matrix = em.build_matrix([*full(), odd])

    assert matrix.unknown == (odd,)
    assert len(matrix.warnings) == 1
    assert "matrix-oracle" in matrix.warnings[0]


def test_is_matrix_variant_and_is_known_matrix_variant():
    assert em.is_matrix_variant("matrix-duckdb")
    assert em.is_matrix_variant("matrix-oracle")
    assert not em.is_matrix_variant("baseline")
    assert not em.is_matrix_variant(None)
    assert em.is_known_matrix_variant("matrix-snowflake")
    assert not em.is_known_matrix_variant("matrix-oracle")
    assert not em.is_known_matrix_variant("dbt-build-snowflake")


def test_bad_run_at_is_an_error_naming_the_value():
    with pytest.raises(ValueError, match="not-a-time"):
        em.build_matrix([row("matrix-duckdb", "not-a-time")])


def test_ties_on_run_at_are_broken_by_content_so_input_order_does_not_matter():
    a = row("matrix-spark", "2026-11-12T00:00:00+00:00", total="1.0", git_sha="aaaaaaa")
    b = row("matrix-spark", "2026-11-12T00:00:00+00:00", total="1.0", git_sha="bbbbbbb")

    first = by_engine(em.build_matrix([a, b]))["spark"].row
    second = by_engine(em.build_matrix([b, a]))["spark"].row

    assert first is second
