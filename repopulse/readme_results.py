"""Generate the Results block of README.md from runs.csv.

    python -m repopulse.readme_results          # rewrite the block between the markers
    python -m repopulse.readme_results --check  # exit 1 if README.md is stale, write nothing

``runs.csv`` is the only source of numbers for the README. This module copies the cells of
``runs.csv`` as written: it never rounds, never computes a ratio or a difference, and never adds a
number of its own. An empty cell is shown as ``—``. The one exception is the summary table of the
two paths (below), which counts rows and picks one cell by nearest-rank; the cell it picks is
copied as text, never re-printed through a float.

For every (variant, data_date) the newest row (by ``run_at``) is shown, in tables grouped by what
the rows measure. A row is compared with ``variant=baseline`` only when it has the same
``data_date`` as a baseline row; every other row is listed where it belongs, and is never dropped
silently.

The engine matrix (E1, ``repopulse.engine_matrix``) is one table with one row per engine in a
fixed order: the newest ``matrix-<engine>`` row, the reused ``dbt-build-snowflake`` row for
Snowflake (labelled not comparable), or ``—`` with a warning line for a missing engine. Those
rows are not repeated under "Other runs"; ``main`` prints the warnings to stderr without changing
the exit code.

Plan v2 sections (R2), each with its own title, prose and one table (the paths section has two),
and no longer listed under "Other runs": the two paths (``path-a-runner``,
``path-b-databricks``: the newest run of each path, then every run counted by variant and
``trigger`` with the nearest-rank p50 and p95 of ``total_seconds`` over the successful runs),
Delta Lake vs Apache Iceberg (``format-*``), Kafka to Spark to Delta throughput
(``streaming-throughput-p<N>``), fault tolerance (``streaming-fault-*``), Trino (``trino-*``),
Airflow backfill (``airflow-backfill-<N>d``) and the SQL agent (``sql-agent-*``). Their columns
that are not in ``runs.csv`` are read from the ``key=value`` words of ``notes``. Every other
variant, ``databricks-bronze-silver`` included, stays under "Other runs".

``REQUIRED_VARIANTS`` lists the variants those sections need. ``missing_variants(rows)`` returns
the ones with no row, in that fixed order; ``render`` does not raise for them (the section says
"No rows"), but ``main``, with or without ``--check``, prints one ``error:`` naming each missing
variant, exits 2 and writes nothing. A missing engine of the matrix is still only a warning.

The block lives between ``<!-- results:start -->`` and ``<!-- results:end -->`` in README.md.
Output is deterministic: the same rows in any order give the same text.
"""

from __future__ import annotations

import argparse
import csv
import math
import re
import sys
from collections.abc import Callable, Iterable, Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path

from repopulse import engine_matrix, pathcompare, runs_log

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
TITLE_PATHS = "Two paths, end to end: runner + MotherDuck vs Databricks (real GH Archive data)"
TITLE_FORMATS = "Delta Lake vs Apache Iceberg (real GH Archive data)"
TITLE_STREAMING = "Kafka to Spark Structured Streaming to Delta: throughput (real GH Archive data)"
TITLE_FAULT = "Streaming fault tolerance: broker kill and consumer kill (real GH Archive data)"
TITLE_TRINO = "Trino on Hive-partitioned parquet (real GH Archive data)"
TITLE_AIRFLOW = "Airflow backfill (real GH Archive data)"
TITLE_SQL_AGENT = "SQL agent on a local model (CI fixture data, not GH Archive)"
TITLE_OTHER = "Other runs"

# Variants of the Plan v2 sections, in the order of the sections; the rows of a section are
# sorted by their index here (or by the number in the name for the families).
_PATH_VARIANTS = tuple(pathcompare.VARIANTS.values())
_FORMAT_VARIANTS = ("format-delta", "format-iceberg")
_FAULT_VARIANTS = ("streaming-fault-broker-kill", "streaming-fault-consumer-kill")
_TRINO_VARIANTS = (
    "trino-flat",
    "trino-partitioned",
    "trino-clustered",
    "trino-partitioned-clustered",
)
_THROUGHPUT_VARIANT = re.compile(r"streaming-throughput-p(\d+)")
_AIRFLOW_VARIANT = re.compile(r"airflow-backfill-(\d+)d")
_SQL_AGENT_VARIANT = re.compile(r"sql-agent-.+")

# (name shown in the error, pattern a variant must fully match): one row of each is required.
_REQUIRED: tuple[tuple[str, re.Pattern[str]], ...] = (
    *((v, re.compile(re.escape(v))) for v in (*_PATH_VARIANTS, *_FORMAT_VARIANTS)),
    ("streaming-throughput-p<N>", _THROUGHPUT_VARIANT),
    *((v, re.compile(re.escape(v))) for v in (*_FAULT_VARIANTS, *_TRINO_VARIANTS)),
    ("airflow-backfill-<N>d", _AIRFLOW_VARIANT),
    ("sql-agent-*", _SQL_AGENT_VARIANT),
)
REQUIRED_VARIANTS: tuple[str, ...] = tuple(name for name, _ in _REQUIRED)

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
_Q_COLUMNS = tuple(f"q{n}_seconds" for n in range(1, 7))
# Plan v2 tables: the columns that are not in runs.csv are read from the words of ``notes``.
_PATH_COLUMNS = (
    "variant",
    "data_date",
    "total_seconds",
    *_Q_COLUMNS,
    "row_count",
    "status",
    "trigger",
    "ingest_seconds",
    "spark_seconds",
    "export_seconds",
    "databricks_seconds",
    "warehouse_seconds",
    "dbt_seconds",
    "queries_seconds",
    "queue_seconds",
    "setup_seconds",
    "job_queue_seconds",
    "job_setup_seconds",
    "warehouse_start_seconds",
    "bronze_rows",
    "run_id",
    "run_at",
    "git_sha",
)
_PATH_SUMMARY_COLUMNS = (
    "variant",
    "trigger",
    "runs",
    "successes",
    "e2e_p50_seconds",
    "e2e_p95_seconds",
)
_FORMAT_COLUMNS = (
    "variant",
    "data_date",
    "total_seconds",
    *_Q_COLUMNS,
    "peak_rss_mb",
    "row_count",
    "write_seconds",
    "merge_seconds",
    "merge_rerun_seconds",
    "time_travel_seconds",
    "load_seconds",
    "table_bytes",
    "table_files",
    "cross_check",
    "run_at",
    "git_sha",
)
_STREAMING_COLUMNS = (
    "variant",
    "data_date",
    "total_seconds",
    "bytes_read",
    "row_count",
    "partitions",
    "brokers",
    "rf",
    "min_isr",
    "produce_seconds",
    "consume_seconds",
    "produce_events_per_s",
    "consume_events_per_s",
    "batch_rows",
    "match",
    "trigger",
    "micro_batches",
    "max_offsets_per_trigger",
    "kafka",
    "run_at",
    "git_sha",
)
_FAULT_COLUMNS = (
    "variant",
    "data_date",
    "total_seconds",
    "row_count",
    "lost",
    "duplicates",
    "offset_duplicates",
    "content_duplicates",
    "unexpected",
    "source_rows",
    "delta_rows",
    "failover_seconds",
    "resync_seconds",
    "max_delivery_gap_after_kill_seconds",
    "restart_first_commit_seconds",
    "restart_drain_seconds",
    "micro_batches_before_kill",
    "micro_batches_after_kill",
    "uncommitted_batches_at_kill",
    "partitions",
    "hours",
    "run_at",
    "git_sha",
)
_TRINO_COLUMNS = (
    "variant",
    "data_date",
    "total_seconds",
    *_Q_COLUMNS,
    "bytes_read",
    "row_count",
    "partition_by",
    "sort_by",
    "files",
    "layout_bytes",
    "trino",
    "bytes_equal_across_runs",
    "cross_check",
    "run_at",
    "git_sha",
)
_AIRFLOW_COLUMNS = (
    "variant",
    "data_date",
    "total_seconds",
    "row_count",
    "engine",
    "days",
    "hours_per_day",
    "last_hour",
    "runs_success",
    "runs_failed",
    "sla_minutes",
    "sla_missed",
    "max_active_runs",
    "target",
    "trigger",
    "run_at",
    "git_sha",
)
_SQL_AGENT_COLUMNS = (
    "variant",
    "data_date",
    "total_seconds",
    "row_count",
    "accuracy",
    "p50_question_s",
    "model",
    "dataset",
    "run_at",
    "git_sha",
    "notes",
)

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


def _md_table(columns: Sequence[str], body: Iterable[Sequence[str]]) -> str:
    """Markdown table of ready-made cells."""
    header = "| " + " | ".join(columns) + " |"
    rule = "| " + " | ".join("---" for _ in columns) + " |"
    return "\n".join([header, rule, *("| " + " | ".join(cells) + " |" for cells in body)])


def _table(rows: Sequence[Row], columns: Sequence[str]) -> str:
    """Markdown table; ``columns`` not in runs.csv are read from the ``notes`` words."""
    body = []
    for row in rows:
        extra = _notes(row)
        body.append([_cell(row, c, extra) for c in columns])
    return _md_table(columns, body)


def _when(row: Row) -> datetime:
    raw = _value(row, "run_at")
    try:
        when = datetime.fromisoformat(raw or "")
    except ValueError:
        raise ReadmeResultsError(f"run_at is not an ISO-8601 timestamp: {raw!r}") from None
    return when if when.tzinfo is not None else when.replace(tzinfo=UTC)


def _newest_key(row: Row) -> tuple[datetime, str, str, str]:
    """Order of two runs of the same thing: by run_at, ties broken by content."""
    return (
        _when(row),
        _value(row, "git_sha") or "",
        _value(row, "total_seconds") or "",
        _value(row, "notes") or "",
    )


def _newest(rows: Iterable[Row]) -> list[Row]:
    """The newest row (by run_at) of each (variant, data_date); ties broken by content."""
    best: dict[tuple[str, str], Row] = {}
    for row in rows:
        variant, data_date = _value(row, "variant"), _value(row, "data_date")
        if variant is None or data_date is None:
            raise ReadmeResultsError(f"row without variant or data_date: {dict(row)!r}")
        current = best.get((variant, data_date))
        if current is None or _newest_key(row) > _newest_key(current):
            best[(variant, data_date)] = row
    return list(best.values())


_NO_ROWS = "No rows in `runs.csv` for this section yet."


def _section(title: str, prose: str, rows: Sequence[Row], columns: Sequence[str]) -> str:
    if not rows:
        return f"### {title}\n\n{_NO_ROWS}"
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

_PROSE_PATHS = (
    "Real GH Archive data (not simulated): the same incremental batch (ingest, bronze/silver, "
    "gold, then Q1 to Q6) on the same window of data, run by a GitHub Actions workflow. "
    "`path-a-runner` runs PySpark and Delta Lake on the runner and builds gold in MotherDuck; "
    "`path-b-databricks` runs bronze/silver as a serverless Databricks job and builds gold with "
    "dbt on a Databricks SQL warehouse. These rows are not compared with `baseline`: they time a "
    "whole pipeline, not Q1 to Q6 alone. `total_seconds` is the sum of the timed steps "
    "(`ingest_seconds` and the other step times in `notes`), without runner setup and queueing, "
    "which have their own columns; `q1_seconds` to `q6_seconds` are the time to read the mart of "
    "each question from the gold; `row_count` is the number of rows the bronze/silver step "
    "inserted into silver (none when the silver table already held the hours of the window, "
    "while `bronze_rows` in `notes` is the number of rows the step read); `status`, `trigger` "
    "and the step times are read from `notes`. The first "
    "table is the newest run of each path. The second counts every run by variant and `trigger`, "
    "failed runs included; `e2e_p50_seconds` and `e2e_p95_seconds` are nearest-rank percentiles "
    "of `total_seconds` over the successful runs of the group, so each is the `total_seconds` of "
    "one run, copied from `runs.csv`. Only runs with `trigger=schedule` enter the primary-path "
    "rule; a `push` run only tests the workflow. The rule and where it stands are in "
    "[`docs/probe-result.md`](docs/probe-result.md), the method in "
    "[`docs/path-comparison.md`](docs/path-comparison.md)."
)
_PATHS_SUMMARY_LEAD = "Every run, counted by variant and trigger:"
_PROSE_FORMATS = (
    "Real GH Archive data (not simulated): the reference day of `baseline`, written once as a "
    "Delta table and once as an Iceberg table with PySpark (local mode) on a GitHub Actions "
    "runner. Both tables are unpartitioned and use the default settings of their format. Each "
    "run writes the table, runs Q1 to Q6, merges the last two hours of the day into it (`MERGE "
    "INTO`, then the same statement again to check it is idempotent) and reads the version from "
    "before the merge (time travel). `total_seconds` is the write, Q1 to Q6, both merges and the "
    "time travel; it leaves out starting Spark and loading the JSON (`load_seconds` in `notes`), "
    "so it is not comparable with `baseline` or `spark-Nd`: compare `format-delta` with "
    "`format-iceberg` only. `table_bytes` and `table_files` are the data files after the write, "
    "before the merge. `row_count` is the rows of the table after the merges; `cross_check=full` "
    "means the other format gave the same row counts. Method and limits: "
    "[`docs/iceberg-vs-delta.md`](docs/iceberg-vs-delta.md)."
)
_PROSE_STREAMING = (
    "Real GH Archive data (not simulated): the reference day is produced into an Apache Kafka "
    "cluster (KRaft mode, replicated topic) that runs in Docker Compose on a GitHub Actions "
    "runner, read back with Spark Structured Streaming into a bronze Delta table, and reconciled "
    "with the line count of the source files. A row is written only when every count matches "
    "(`match=true`); `row_count` is the rows in Delta and `bytes_read` the bytes of the produced "
    "values. `total_seconds` is `produce_seconds` plus `consume_seconds`; creating the topic, "
    "starting Spark and the reconciliation are not timed. The trigger is `availableNow`: "
    "production finishes before consumption starts, so `consume_events_per_s` is the rate at "
    "which Spark drains an existing backlog into Delta, not the latency of a stream that runs "
    "continuously, and `produce_events_per_s` is one Python producer writing to brokers that "
    "share the runner's CPU and disk. Each partition count is measured once, with no warm-up, in "
    "one Spark session, so the first one also pays one-time initialisation. These rows move the "
    "day through Kafka instead of running Q1 to Q6, so they are not compared with `baseline`. "
    "The scheduled pipeline is an incremental batch, not this benchmark. Method: "
    "[`docs/streaming.md`](docs/streaming.md)."
)
_PROSE_FAULT = (
    "Real GH Archive data (not simulated): the Kafka and Spark path above with one fault injected "
    "in the middle of a run. In `broker-kill` a broker is killed (SIGKILL on its container) "
    "while the producer is writing, and the topic is then drained into Delta with the remaining "
    "brokers; in `consumer-kill` the Spark consumer process, JVM included, is killed after a few "
    "micro-batches have been committed and restarted on the same checkpoint and Delta table. "
    "After each case the source and the sink are compared by content (a multiset of the SHA-256 "
    "of every line), not only by count: `lost` is source lines with no message in Delta, "
    "`duplicates` is `offset_duplicates` (the same Kafka message written twice) plus "
    "`content_duplicates` (the same content at another offset). They are measured, not assumed, "
    "and a case whose fault did not land in the middle of the run writes no row. "
    "`failover_seconds` and `resync_seconds` (broker) and `restart_first_commit_seconds` and "
    "`restart_drain_seconds` (consumer) are read by polling, so they are upper bounds. "
    "`total_seconds` adds up the produce and consume times (or the produce time and the two "
    "consumer runs) of the case, so these rows are not comparable with each other or with the "
    "throughput table. There is one run per case, not a median, and the brokers share one runner, "
    "so a kill here is a process crash, not a machine or network failure. Method: "
    "[`docs/streaming-fault.md`](docs/streaming-fault.md)."
)
_PROSE_TRINO = (
    "Real GH Archive data (not simulated): the reference day in the four parquet layouts of the "
    "`qopt-*` rows (flat, partitioned by event type, clustered by sort order, and both), written "
    "to MinIO, registered as external tables in a Hive Metastore and queried with Q1 to Q6 on one "
    "Trino node. Everything runs in Docker Compose on a GitHub Actions runner. `qN_seconds` is "
    "the client-side wall time of query N and `total_seconds` the sum of Q1 to Q6 without any "
    "loading, as the median of several timed runs after one untimed warm-up per layout. "
    "`bytes_read` is what Trino read from storage (`physicalInputBytes`) summed over Q1 to Q6 in "
    "the first timed run; the bytes and rows of each query are in `notes`. Every result was "
    "checked against DuckDB (`cross_check`). The server stays up and the data goes over HTTP, "
    "while `baseline` loads the files in its own process, so these rows are not comparable with "
    "`baseline` or `qopt-*`: compare the `trino-*` rows with each other, `trino-flat` being the "
    "layout before. Method: [`docs/trino-hive.md`](docs/trino-hive.md)."
)
_PROSE_AIRFLOW = (
    "Real GH Archive data (not simulated): the Airflow DAG `dags/repopulse_pipeline.py` runs the "
    "chain of the scheduled pipeline (ingest, Spark bronze/silver, export, dbt build), an "
    "incremental batch, as a backfill of N daily runs with `airflow standalone` on a GitHub "
    "Actions runner. dbt builds on a local DuckDB file (`target=ci`), not on MotherDuck. Each "
    "daily run processes only `hours_per_day` hours of its day, ending at `last_hour`: a sample "
    "of the day, not the whole day. `total_seconds` is the wall-clock time from creating the "
    "backfill to the end of the last run, Airflow's scheduling delay included; `row_count` is "
    "the sum of the silver rows inserted over the days; `sla_missed` is counted from the "
    "duration of each run against `sla_minutes`, not from the Airflow callback. A row is "
    "written only when the backfill succeeded. These rows are not compared with `baseline` or "
    "with the two paths. The cron of the scheduled pipeline is unchanged: the two ways of "
    "orchestrating are measured side by side. Method: [`docs/airflow.md`](docs/airflow.md)."
)
_PROSE_SQL_AGENT = (
    "This is not GH Archive data: the questions run on the fixture that CI builds with dbt "
    "(hand-made events, no real users), so `data_date` is the latest event date of that fixture "
    "and `dataset=ci-fixture`. A language model running locally on the repo owner's machine (model "
    "and hardware in `notes`) answers each question of `repopulse/sql_agent_eval.toml` by writing "
    "DuckDB SQL against the read-only `public` schema. `accuracy` is execution accuracy, correct "
    "answers over questions: an answer is correct when its result equals the result of the "
    "reference SQL on the same database. `row_count` is the number of questions, `total_seconds` "
    "the sum of the time per question and `p50_question_s` the median time per question, as "
    "written by the evaluation. CI never calls a real model, so this row is a one-off "
    "measurement of one model on a small set of questions, not a CI result and not a comparison "
    "with other models or with `baseline`. The `hw` value in `notes` has spaces in it, so it is "
    "only shown in the `notes` column. Method: [`docs/sql-agent.md`](docs/sql-agent.md)."
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


# --- the two paths ---------------------------------------------------------------------------


def _path_status(row: Row) -> str:
    status = _notes(row).get("status")
    if status not in pathcompare.STATUSES:
        raise ReadmeResultsError(
            f"notes of {_value(row, 'variant')} run_at {_value(row, 'run_at')} need "
            f"status={'|'.join(pathcompare.STATUSES)} (got {status!r})"
        )
    return status


def _success_total(row: Row) -> str:
    """``total_seconds`` of a successful run, as the text of the cell, after a sanity check."""
    total = _value(row, "total_seconds")
    try:
        usable = total is not None and math.isfinite(float(total)) and float(total) >= 0
    except ValueError:
        usable = False
    if total is None or not usable:
        raise ReadmeResultsError(
            f"total_seconds of {_value(row, 'variant')} run_at {_value(row, 'run_at')} is not "
            f"a finite number >= 0: {total!r}"
        )
    return total


def _nearest_rank(totals: Sequence[str], p: float) -> str | None:
    """The cell text of the p-th percentile (nearest-rank, as ``pathcompare.percentile``)."""
    ordered = sorted(totals, key=lambda text: (float(text), text))
    # percentile() of the positions 0..n-1 is the position of the nearest-rank value
    position = pathcompare.percentile(range(len(ordered)), p)
    return None if position is None else ordered[int(position)]


def _trigger(row: Row) -> str:
    return _notes(row).get("trigger") or EMPTY


def _paths_summary(path_rows: Sequence[Row]) -> str:
    """Counts and nearest-rank p50/p95 of ``total_seconds`` per (variant, trigger)."""
    body = []
    for variant in _PATH_VARIANTS:
        mine = [r for r in path_rows if _value(r, "variant") == variant]
        for trigger in sorted({_trigger(r) for r in mine}):
            group = [r for r in mine if _trigger(r) == trigger]
            # a failed run counts as a run; a row without a status in its notes is an error
            statuses = [(r, _path_status(r)) for r in group]
            ok = [_success_total(r) for r, status in statuses if status == "success"]
            p50, p95 = _nearest_rank(ok, 50), _nearest_rank(ok, 95)
            body.append(
                [
                    variant,
                    _escape(trigger),
                    str(len(group)),
                    str(len(ok)),
                    EMPTY if p50 is None else _escape(p50),
                    EMPTY if p95 is None else _escape(p95),
                ]
            )
    return _md_table(_PATH_SUMMARY_COLUMNS, body)


def _paths_section(path_rows: Sequence[Row]) -> str:
    if not path_rows:
        return f"### {TITLE_PATHS}\n\n{_NO_ROWS}"
    summary = _paths_summary(path_rows)  # validates the notes of every path row
    newest = []
    for variant in _PATH_VARIANTS:
        mine = [r for r in path_rows if _value(r, "variant") == variant]
        if mine:
            newest.append(max(mine, key=_newest_key))
    parts = [
        f"### {TITLE_PATHS}",
        _PROSE_PATHS,
        _table(newest, _PATH_COLUMNS),
        _PATHS_SUMMARY_LEAD,
        summary,
    ]
    return "\n\n".join(parts)


# --- missing variants ------------------------------------------------------------------------


def missing_variants(rows: Iterable[Row]) -> list[str]:
    """Names in ``REQUIRED_VARIANTS`` order with no row in ``rows`` (``<N>``/``*`` = a family)."""
    present = {v for v in (_value(r, "variant") for r in rows) if v is not None}
    return [
        name
        for name, pattern in _REQUIRED
        if not any(pattern.fullmatch(variant) for variant in present)
    ]


def _by_order(order: Sequence[str]) -> Callable[[Row], tuple[int, str]]:
    def key(row: Row) -> tuple[int, str]:
        return (order.index(_value(row, "variant") or ""), _value(row, "data_date") or "")

    return key


def _by_number(pattern: re.Pattern[str]) -> Callable[[Row], tuple[int, str]]:
    def key(row: Row) -> tuple[int, str]:
        match = pattern.fullmatch(_value(row, "variant") or "")
        assert match is not None
        return (int(match.group(1)), _value(row, "data_date") or "")

    return key


def render(rows: Iterable[Row]) -> str:
    """Markdown of the Results block for ``rows`` (dicts keyed by ``runs_log.COLUMNS``).

    Does not raise for a variant that has no row: its section says so (``missing_variants``).
    """
    rows = list(rows)
    newest = _newest(rows)  # checks variant, data_date and run_at of every row first
    matrix = engine_matrix.build_matrix(rows)
    # matrix-<engine> rows of the six engines live only in the matrix table
    latest = [r for r in newest if not engine_matrix.is_known_matrix_variant(_value(r, "variant"))]
    baseline_dates = {_value(r, "data_date") for r in latest if _value(r, "variant") == "baseline"}
    # The paths section needs every run (counts, percentiles), not only the newest per date.
    path_rows = [r for r in rows if _value(r, "variant") in _PATH_VARIANTS]

    layouts: list[Row] = []
    scale: list[Row] = []
    dbt: list[Row] = []
    dbt_databricks: list[Row] = []
    dq: list[Row] = []
    formats: list[Row] = []
    streaming: list[Row] = []
    fault: list[Row] = []
    trino: list[Row] = []
    airflow: list[Row] = []
    sql_agent: list[Row] = []
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
        elif variant in _PATH_VARIANTS:
            pass  # shown from path_rows
        elif variant in _FORMAT_VARIANTS:
            formats.append(row)
        elif _THROUGHPUT_VARIANT.fullmatch(variant):
            streaming.append(row)
        elif variant in _FAULT_VARIANTS:
            fault.append(row)
        elif variant in _TRINO_VARIANTS:
            trino.append(row)
        elif _AIRFLOW_VARIANT.fullmatch(variant):
            airflow.append(row)
        elif _SQL_AGENT_VARIANT.fullmatch(variant):
            sql_agent.append(row)
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
    parts += [
        _paths_section(path_rows),
        _section(
            TITLE_FORMATS,
            _PROSE_FORMATS,
            sorted(formats, key=_by_order(_FORMAT_VARIANTS)),
            _FORMAT_COLUMNS,
        ),
        _section(
            TITLE_STREAMING,
            _PROSE_STREAMING,
            sorted(streaming, key=_by_number(_THROUGHPUT_VARIANT)),
            _STREAMING_COLUMNS,
        ),
        _section(
            TITLE_FAULT,
            _PROSE_FAULT,
            sorted(fault, key=_by_order(_FAULT_VARIANTS)),
            _FAULT_COLUMNS,
        ),
        _section(
            TITLE_TRINO,
            _PROSE_TRINO,
            sorted(trino, key=_by_order(_TRINO_VARIANTS)),
            _TRINO_COLUMNS,
        ),
        _section(
            TITLE_AIRFLOW,
            _PROSE_AIRFLOW,
            sorted(airflow, key=_by_number(_AIRFLOW_VARIANT)),
            _AIRFLOW_COLUMNS,
        ),
        _section(
            TITLE_SQL_AGENT,
            _PROSE_SQL_AGENT,
            sorted(sql_agent, key=by_variant_date),
            _SQL_AGENT_COLUMNS,
        ),
    ]
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
        # A variant of a v2 section with no row is an error, with or without --check: the README
        # would otherwise say "No rows" for a measurement the repo claims to have.
        missing = missing_variants(rows)
        if missing:
            raise ReadmeResultsError(
                f"{args.runs} has no row for required variant(s): {', '.join(missing)}"
            )
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
