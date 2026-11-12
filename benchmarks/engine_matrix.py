"""Engine matrix (E1): the same Q1-Q6 on the reference day, one engine per call.

Every call measures ONE engine and appends one ``runs.csv`` row (``variant=matrix-<engine>``) to
``--out``, so a failing engine never costs the row of another one. Per engine: load the
reference day of ``benchmarks/reference.toml`` (real GH Archive data, checksum-verified first)
plus the seed ``repos`` table (not timed, ``load_seconds`` in notes), run Q1-Q6 once as a warm-up,
then ``--runs`` timed runs. ``q1_seconds``..``q6_seconds`` are medians; ``total_seconds`` is the
median of the per-run sum of Q1-Q6 (load and warm-up excluded, the same on every engine).

The warm-up results are cross-checked against DuckDB on the same files (``naive_duckdb.run_once``
in this process): same ``raw_events`` row count, same columns and the same multiset of rows for
every query (floats within 1e-6). Any difference makes the measurement invalid and no row is
written; the exit code is then 1.

Engines (the six of ``repopulse.engine_matrix.ENGINES``):

- ``duckdb``: in this process, in memory, ``benchmarks/queries/``. ``timing=in-process``.
- ``spark``: PySpark ``local[*]`` in a fresh child process (Java 17, extra ``spark``),
  ``benchmarks/queries_spark/``, raw table cached as in ``spark_naive``. ``timing=in-process``.
- ``motherduck``: DuckDB client on ``md:`` (token only from ``MOTHERDUCK_TOKEN``), tables in the
  database ``repopulse_matrix`` (never ``repopulse`` or ``repopulse_smoke``) by
  ``CREATE OR REPLACE``, ``benchmarks/queries/``. ``timing=client`` (includes the network).
- ``databricks-sql``: files pushed to a Unity Catalog Volume with the Files API, tables in
  ``workspace.repopulse_matrix`` by ``CREATE OR REPLACE TABLE ... read_files``, Q1-Q6 of
  ``benchmarks/queries_spark/`` through the SQL Statement Execution API on the SQL warehouse.
  Every statement gets a unique trailing comment so the warehouse result cache cannot answer it,
  and the query history (``metrics.result_from_cache``) is checked afterwards: a cached timed
  statement makes the measurement invalid. ``timing=client`` (includes the network).
- ``databricks-serverless``: the same Volume files, a notebook (Spark SQL of
  ``benchmarks/queries_spark/`` embedded) submitted as a one-off serverless run through the Jobs
  API; the notebook loads, warms up, times each query itself and returns JSON through
  ``dbutils.notebook.exit``. ``timing=notebook``.
- ``snowflake``: never measured here (the trial credits are not spent again); the README reuses
  the ``dbt-build-snowflake`` row. ``--engine snowflake`` exits 2.

HTTP goes through an injectable ``transport`` and MotherDuck through an injectable ``connect``,
so unit tests never call a service. Every message goes through ``Redactor`` (host, tokens,
warehouse, user name): the workflow log is public. Method: ``docs/engine-matrix.md``.

Usage (what ``.github/workflows/engine-matrix.yml`` runs, once per engine)::

    python benchmarks/engine_matrix.py --engine duckdb --runs 5 --git-sha <sha> \
        --out out/engine-matrix-rows.csv --summary out/engine-matrix-summary.md
"""

from __future__ import annotations

import argparse
import datetime as dt
import functools
import hashlib
import json
import math
import os
import re
import subprocess
import sys
import time
import uuid
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any
from urllib.parse import urlencode, urlsplit

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import duckdb  # noqa: E402

from benchmarks import naive_duckdb, spark_naive  # noqa: E402
from benchmarks.measure import MeasureError, summarize_runs  # noqa: E402
from benchmarks.reference import (  # noqa: E402
    REFERENCE_TOML,
    ChecksumError,
    load_reference,
    verify_files,
)
from benchmarks.trino_bench import _norm, _same, _sort_key  # noqa: E402
from repopulse import databricks_job as dbx  # noqa: E402
from repopulse import engine_matrix  # noqa: E402
from repopulse.probe import (  # noqa: E402
    ApiError,
    ProbeContext,
    Redactor,
    Secrets,
    Transport,
    resolve_warehouse,
    urllib_transport,
)
from repopulse.runs_log import COLUMNS, RunsLogError, append_run, validate_row  # noqa: E402

ENGINES: tuple[str, ...] = engine_matrix.ENGINES
MEASURED_ENGINES: tuple[str, ...] = tuple(e for e in ENGINES if e != "snowflake")
QUERIES: tuple[str, ...] = naive_duckdb.QUERIES
TIMINGS: tuple[str, ...] = ("in-process", "client", "notebook")
TIMING_OF: dict[str, str] = {
    "duckdb": "in-process",
    "spark": "in-process",
    "motherduck": "client",
    "databricks-sql": "client",
    "databricks-serverless": "notebook",
}
DEFAULT_MD_DATABASE = "repopulse_matrix"
FORBIDDEN_MD_DATABASES = frozenset({"repopulse", "repopulse_smoke"})
DEFAULT_CATALOG = "workspace"
DEFAULT_SCHEMA = "repopulse_matrix"
DEFAULT_VOLUME = "repopulse_matrix_data"
FORBIDDEN_SCHEMAS = frozenset({"repopulse", "repopulse_probe", "default", "information_schema"})
FORBIDDEN_SCHEMA_PREFIX = "repopulse_dbt"  # D2 (dbt-databricks) schemas
NOTEBOOK_DIR = "repopulse"
NOTEBOOK_NAME = "repopulse_engine_matrix"
TASK_KEY = "engine_matrix"
DEFAULT_OUT = Path("out/engine-matrix-rows.csv")
DEFAULT_SUMMARY = Path("out/engine-matrix-summary.md")
STATEMENT_WAIT = "50s"  # the API maximum: most statements answer in the first response
UPLOAD_TIMEOUT = 600.0

IDENTIFIER = re.compile(r"[a-z_][a-z0-9_]*")
NOTE_KEY = re.compile(r"[a-z0-9_]+")
NOTE_VALUE = re.compile(r"[^\s,=]+")
_STATEMENT_WAITING = frozenset({"PENDING", "RUNNING"})
_SNOWFLAKE_REFUSAL = (
    "snowflake is not measured by the engine matrix: the Snowflake trial credits are not spent "
    "again; the README reuses the newest dbt-build-snowflake row of runs.csv (a different "
    "measurement, marked as reused). See docs/engine-matrix.md."
)


class EngineMatrixError(RuntimeError):
    """An engine, a statement, a result or a cross-check of the engine matrix failed."""


def variant(engine: str) -> str:
    try:
        return engine_matrix.matrix_variant(engine)
    except ValueError as exc:
        raise EngineMatrixError(str(exc)) from None


def _sql_str(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def query_sql(directory: Path) -> dict[str, str]:
    return {q: (directory / f"{q}.sql").read_text(encoding="utf-8") for q in QUERIES}


def tagged(sql: str, tag: str) -> str:
    """The query with a trailing comment: unique text per statement defeats a result cache."""
    return sql.rstrip().rstrip(";").rstrip() + f"\n-- repopulse engine-matrix {tag}"


# ---------------------------------------------------------------- names (pure)


def check_md_database(name: str) -> str:
    if not isinstance(name, str) or not IDENTIFIER.fullmatch(name):
        raise EngineMatrixError(f"unsafe MotherDuck database name {name!r}")
    if name in FORBIDDEN_MD_DATABASES:
        raise EngineMatrixError(
            f"MotherDuck database {name!r} is not for benchmarks (gold or smoke test); "
            f"use its own database such as {DEFAULT_MD_DATABASE!r}"
        )
    return name


def check_dbx_names(catalog: str, schema: str, volume: str) -> None:
    for kind, value in (("catalog", catalog), ("schema", schema), ("volume", volume)):
        if not isinstance(value, str) or not IDENTIFIER.fullmatch(value):
            raise EngineMatrixError(f"unsafe Databricks {kind} name {value!r}")
    if schema in FORBIDDEN_SCHEMAS or schema.startswith(FORBIDDEN_SCHEMA_PREFIX):
        raise EngineMatrixError(
            f"Databricks schema {schema!r} belongs to another job (D1 bronze/silver, D2 dbt, "
            f"probe); use its own schema such as {DEFAULT_SCHEMA!r}"
        )


# ---------------------------------------------------------------- rows (pure)


def _note(kind: str, key: str, value: object) -> str:
    text = str(value)
    if not NOTE_KEY.fullmatch(key):
        raise EngineMatrixError(f"{kind} note key {key!r} must match {NOTE_KEY.pattern}")
    if not NOTE_VALUE.fullmatch(text):
        raise EngineMatrixError(f"{kind} note {key}={text!r} must be one token (no space, , or =)")
    return f"{key}={text}"


def build_row(
    runs: Sequence[Mapping[str, object]],
    *,
    engine: str,
    git_sha: str,
    data_date: str,
    run_at: str,
    row_count: int,
    load_seconds: float,
    timing: str,
    versions: Mapping[str, str],
    extra: Mapping[str, object] | None = None,
    cross_check: str = "content",
) -> dict[str, object]:
    """Median row of one engine; ``total_seconds`` = median of the per-run sum of Q1..Q6."""
    if engine == "snowflake":
        raise EngineMatrixError(_SNOWFLAKE_REFUSAL)
    name = variant(engine)
    if timing not in TIMINGS:
        raise EngineMatrixError(f"{engine}: timing {timing!r} is not one of {', '.join(TIMINGS)}")
    if not runs:
        raise EngineMatrixError(f"{engine}: no runs to summarise")
    for q in QUERIES:
        values = {int(r[f"{q}_rows"]) for r in runs}
        if len(values) != 1:
            raise EngineMatrixError(f"{engine}: {q}_rows differs between runs: {sorted(values)}")
    per_run = []
    for r in runs:
        seconds = {f"{q}_seconds": float(r[f"{q}_seconds"]) for q in QUERIES}
        per_run.append(
            {
                **seconds,
                "total_seconds": round(sum(seconds.values()), 6),
                "row_count": int(row_count),
                "bytes_read": 0,
                "peak_rss_mb": None,
            }
        )
    try:
        row = summarize_runs(
            per_run, variant=name, git_sha=git_sha, data_date=data_date, run_at=run_at
        )
    except MeasureError as exc:
        raise EngineMatrixError(f"{engine}: {exc}") from None
    row["bytes_read"] = None  # not measured the same way on every engine; no substitute
    row["peak_rss_mb"] = None
    notes = [
        _note("engine", "engine", engine),
        _note("runs", "runs", len(runs)),
        "stat=median",
        "warmup=1",
        _note("timing", "timing", timing),
        "total=median_run_sum_q1_q6",
        _note("load", "load_seconds", round(float(load_seconds), 3)),
        _note("cross_check", "cross_check", cross_check),
    ]
    notes += [_note("version", k, v) for k, v in versions.items()]
    notes += [_note("extra", k, v) for k, v in (extra or {}).items()]
    row["notes"] = " ".join(notes)
    try:
        validate_row(row)
    except RunsLogError as exc:
        raise EngineMatrixError(f"{engine}: {exc}") from None
    return row


# ---------------------------------------------------------------- cross-check (pure)


def _compare_query(engine: str, q: str, got: Mapping, want: Mapping, abs_tol: float) -> None:
    got_cols, want_cols = list(got["columns"]), list(want["columns"])
    if got_cols != want_cols:
        raise EngineMatrixError(
            f"{engine} {q}: columns differ: {engine}={got_cols} duckdb={want_cols}"
        )
    got_rows = sorted((tuple(_norm(v) for v in r) for r in got["rows"]), key=_sort_key)
    want_rows = sorted((tuple(_norm(v) for v in r) for r in want["rows"]), key=_sort_key)
    if len(got_rows) != len(want_rows):
        raise EngineMatrixError(
            f"{engine} {q}: {engine} returned {len(got_rows)} rows, duckdb {len(want_rows)}"
        )
    for i, (g, w) in enumerate(zip(got_rows, want_rows, strict=True)):
        if len(g) != len(w) or not all(_same(a, b, abs_tol) for a, b in zip(g, w, strict=True)):
            raise EngineMatrixError(
                f"{engine} {q}: first differing row (sorted, #{i}): {engine}={g!r} duckdb={w!r}"
            )


def check_results(
    engine: str,
    measured: Mapping[str, Any],
    reference: Mapping[str, Any],
    *,
    abs_tol: float = 1e-6,
) -> None:
    """Same ``raw_events`` row count and, per query, the same columns and multiset of rows."""
    if int(measured["row_count"]) != int(reference["row_count"]):
        raise EngineMatrixError(
            f"{engine}: row_count of raw_events {measured['row_count']} != duckdb "
            f"{reference['row_count']}"
        )
    for q in QUERIES:
        got = measured["results"].get(q)
        if got is None:
            raise EngineMatrixError(f"{engine} {q}: no result to cross-check")
        _compare_query(engine, q, got, reference["results"][q], abs_tol)


def _check_runs_match_warmup(engine: str, runs: Sequence[Mapping], results: Mapping) -> None:
    for i, run in enumerate(runs, start=1):
        for q in QUERIES:
            if int(run[f"{q}_rows"]) != len(results[q]["rows"]):
                raise EngineMatrixError(
                    f"{engine} {q}: run {i} returned {run[f'{q}_rows']} rows, warm-up "
                    f"{len(results[q]['rows'])}"
                )


# ---------------------------------------------------------------- shared timing loop


Execute = Callable[[str, str, str], tuple[list[str], list[tuple]]]


def timed_queries(
    execute: Execute, sql: Mapping[str, str], runs: int
) -> tuple[dict[str, dict], list[dict]]:
    """Warm-up of Q1..Q6 (results kept), then ``runs`` timed runs (row counts kept)."""
    results: dict[str, dict] = {}
    for q in QUERIES:
        columns, rows = execute(q, sql[q], "warmup")
        results[q] = {"columns": columns, "rows": rows}
    timed: list[dict] = []
    for i in range(1, runs + 1):
        run: dict[str, object] = {}
        for q in QUERIES:
            t0 = time.perf_counter()
            _, rows = execute(q, sql[q], f"run{i}")
            run[f"{q}_seconds"] = round(time.perf_counter() - t0, 6)
            run[f"{q}_rows"] = len(rows)
        timed.append(run)
    return results, timed


def jsonable(value: object) -> object:
    if isinstance(value, dt.datetime):
        return value.isoformat(sep=" ")
    if isinstance(value, dt.date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    return value


def _is_number(value: object) -> bool:
    return isinstance(value, int | float) and not isinstance(value, bool)


def check_summary(summary: object, label: str) -> dict:
    """A measurement summary (Spark child, serverless notebook): keys, types, rows as tuples."""
    if not isinstance(summary, Mapping):
        raise EngineMatrixError(f"{label}: summary is not a JSON object")
    absent = [
        k
        for k in ("spark_version", "row_count", "load_seconds", "runs", "results")
        if k not in summary
    ]
    if absent:
        raise EngineMatrixError(f"{label}: summary misses key(s): {', '.join(absent)}")
    row_count = summary["row_count"]
    if isinstance(row_count, bool) or not isinstance(row_count, int) or row_count < 0:
        raise EngineMatrixError(f"{label}: row_count must be an integer >= 0, got {row_count!r}")
    if not _is_number(summary["load_seconds"]):
        raise EngineMatrixError(f"{label}: load_seconds is not a number")
    runs = summary["runs"]
    if not isinstance(runs, list) or not runs:
        raise EngineMatrixError(f"{label}: runs must be a non-empty list")
    for run in runs:
        if not isinstance(run, Mapping):
            raise EngineMatrixError(f"{label}: a run is not an object")
        for q in QUERIES:
            seconds, rows = run.get(f"{q}_seconds"), run.get(f"{q}_rows")
            if not _is_number(seconds) or not math.isfinite(seconds) or seconds < 0:
                raise EngineMatrixError(f"{label}: {q}_seconds must be a number >= 0")
            if isinstance(rows, bool) or not isinstance(rows, int) or rows < 0:
                raise EngineMatrixError(f"{label}: {q}_rows must be an integer >= 0")
    results = summary["results"]
    if not isinstance(results, Mapping):
        raise EngineMatrixError(f"{label}: results is not an object")
    out_results = {}
    for q in QUERIES:
        res = results.get(q)
        if (
            not isinstance(res, Mapping)
            or not isinstance(res.get("columns"), list)
            or not isinstance(res.get("rows"), list)
            or not all(isinstance(r, list) for r in res["rows"])
        ):
            raise EngineMatrixError(f"{label}: results of {q} are malformed")
        out_results[q] = {"columns": list(res["columns"]), "rows": [tuple(r) for r in res["rows"]]}
    version = summary["spark_version"]
    if not isinstance(version, str) or not NOTE_VALUE.fullmatch(version):
        raise EngineMatrixError(f"{label}: bad spark_version {version!r}")
    return {**summary, "results": out_results}


def parse_child_output(stdout: str) -> dict:
    """The last non-empty stdout line of the Spark child, as a checked summary."""
    lines = [ln for ln in stdout.splitlines() if ln.strip()]
    if not lines:
        raise EngineMatrixError("spark child: no output")
    try:
        summary = json.loads(lines[-1])
    except ValueError:
        raise EngineMatrixError(
            f"spark child: last line is not JSON: {lines[-1][:200]!r}"
        ) from None
    return check_summary(summary, "spark child")


# ---------------------------------------------------------------- DuckDB and MotherDuck


def load_tables(con: Any, files: Sequence[Path], seed: Path) -> int:
    """``CREATE OR REPLACE`` of ``raw_events`` and ``repos`` (same columns as the baseline)."""
    con.execute(
        "CREATE OR REPLACE TABLE raw_events AS SELECT * FROM read_json("
        f"{naive_duckdb._source_sql(list(files))}, format = 'newline_delimited', "
        f"compression = 'gzip', columns = {naive_duckdb.RAW_COLUMNS})"
    )
    con.execute(
        "CREATE OR REPLACE TABLE repos AS SELECT * FROM read_csv("
        f"{_sql_str(Path(seed).as_posix())}, header = true, "
        "columns = {'repo_name': 'VARCHAR', 'category': 'VARCHAR'})"
    )
    return int(con.execute("SELECT count(*) FROM raw_events").fetchone()[0])


def _duckdb_execute(con: Any, *, tag_with: str | None) -> Execute:
    def execute(q: str, sql: str, label: str) -> tuple[list[str], list[tuple]]:
        text = tagged(sql, f"{tag_with} {q} {label}") if tag_with else sql
        cur = con.execute(text)
        rows = cur.fetchall()
        return [c[0] for c in cur.description], [tuple(r) for r in rows]

    return execute


def measure_duckdb(files: Sequence[Path], seed: Path, runs: int) -> dict:
    con = duckdb.connect(":memory:")
    try:
        start = time.perf_counter()
        row_count = load_tables(con, files, seed)
        load_seconds = time.perf_counter() - start
        results, timed = timed_queries(
            _duckdb_execute(con, tag_with=None), query_sql(naive_duckdb.QUERY_DIR), runs
        )
    finally:
        con.close()
    return {
        "row_count": row_count,
        "load_seconds": load_seconds,
        "results": results,
        "runs": timed,
        "versions": {"duckdb": duckdb.__version__},
        "extra": {},
    }


def measure_motherduck(
    files: Sequence[Path],
    seed: Path,
    runs: int,
    *,
    connect: Callable[..., Any],
    database: str,
    nonce: str,
) -> dict:
    """Tables in the MotherDuck database ``database``; the client reads the token from env."""
    check_md_database(database)
    con = connect("md:")
    try:
        con.execute(f"CREATE DATABASE IF NOT EXISTS {database}")
        con.execute(f"USE {database}")
        start = time.perf_counter()
        row_count = load_tables(con, files, seed)
        load_seconds = time.perf_counter() - start
        found = con.execute(
            "SELECT type FROM duckdb_databases() WHERE database_name = ?", [database]
        ).fetchone()
        db_type = str(found[0]) if found and found[0] else "unknown"
        results, timed = timed_queries(
            _duckdb_execute(con, tag_with=nonce), query_sql(naive_duckdb.QUERY_DIR), runs
        )
    finally:
        con.close()
    extra = {"md_database": database, "md_database_type": re.sub(r"\W", "_", db_type)}
    extra["result_cache"] = "unique_comment"
    return {
        "row_count": row_count,
        "load_seconds": load_seconds,
        "results": results,
        "runs": timed,
        "versions": {"duckdb": duckdb.__version__},
        "extra": extra,
    }


# ---------------------------------------------------------------- Spark (child process)


def spark_child(files: Sequence[Path], seed: Path, runs: int, cfg: Mapping[str, object]) -> dict:
    """One Spark session in this (fresh) process: load + cache, warm-up, timed runs."""
    t0 = time.perf_counter()
    spark = spark_naive.build_session(cfg)
    spark.range(1).count()  # JVM up and an executor ready
    startup = time.perf_counter() - t0
    raw = repos = None
    try:
        start = time.perf_counter()
        raw = spark.read.schema(spark_naive.RAW_SCHEMA).json([str(p) for p in files]).cache()
        row_count = raw.count()
        raw.createOrReplaceTempView("raw_events")
        repos = spark.read.csv(str(seed), header=True, schema=spark_naive.SEED_SCHEMA).cache()
        repos.count()
        repos.createOrReplaceTempView("repos")
        load_seconds = time.perf_counter() - start

        def execute(q: str, sql: str, label: str) -> tuple[list[str], list[tuple]]:
            df = spark.sql(sql)
            rows = df.collect()
            return list(df.columns), [tuple(r) for r in rows]

        results, timed = timed_queries(execute, query_sql(spark_naive.QUERY_DIR), runs)
        version = spark.version
    finally:
        for df in (raw, repos):
            if df is not None:
                df.unpersist()
        spark.stop()
    return {
        "spark_version": version,
        "row_count": int(row_count),
        "load_seconds": round(load_seconds, 6),
        "startup_seconds": round(startup, 6),
        "warmup": 1,
        "runs": timed,
        "results": {
            q: {"columns": r["columns"], "rows": [[jsonable(v) for v in row] for row in r["rows"]]}
            for q, r in results.items()
        },
    }


def measure_spark(
    data_dir: Path, seed: Path, runs: int, cfg: Mapping[str, object], timeout_s: float
) -> dict:
    cmd = [
        sys.executable,
        str(Path(__file__).resolve()),
        "--engine",
        "spark",
        "--spark-child",
        "--data-dir",
        str(data_dir),
        "--seed",
        str(seed),
        "--runs",
        str(runs),
        "--spark-master",
        str(cfg["master"]),
        "--spark-driver-memory",
        str(cfg["driver_memory"]),
        "--spark-shuffle-partitions",
        str(cfg["shuffle_partitions"]),
    ]
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout_s, check=False)
    except subprocess.TimeoutExpired:
        raise EngineMatrixError(f"spark child timed out after {timeout_s:.0f} s") from None
    if proc.returncode != 0:
        tail = "\n".join(proc.stderr.strip().splitlines()[-20:])
        raise EngineMatrixError(f"spark child failed (exit {proc.returncode}):\n{tail}")
    summary = parse_child_output(proc.stdout)
    return {
        "row_count": summary["row_count"],
        "load_seconds": summary["load_seconds"],
        "results": summary["results"],
        "runs": summary["runs"],
        "versions": {"spark": summary["spark_version"]},
        "extra": {
            "master": cfg["master"],
            "startup_seconds": round(float(summary.get("startup_seconds", 0.0)), 3),
        },
    }


# ---------------------------------------------------------------- Databricks (REST)


_INT_TYPES = frozenset({"BYTE", "SHORT", "INT", "LONG", "TINYINT", "SMALLINT", "BIGINT"})
_FLOAT_TYPES = frozenset({"FLOAT", "DOUBLE"})


def convert_value(type_name: str, text: object) -> object:
    """One JSON_ARRAY cell of the Statement Execution API (always a string) by column type."""
    if text is None:
        return None
    kind = str(type_name or "").upper()
    try:
        if kind in _INT_TYPES:
            return int(str(text))
        if kind in _FLOAT_TYPES:
            return float(str(text))
        if kind == "DECIMAL":
            return float(Decimal(str(text)))
        if kind == "BOOLEAN":
            if str(text).lower() not in ("true", "false"):
                raise ValueError
            return str(text).lower() == "true"
        if kind == "DATE":
            return dt.date.fromisoformat(str(text))
        if kind in ("TIMESTAMP", "TIMESTAMP_NTZ"):
            value = dt.datetime.fromisoformat(str(text).replace("Z", "+00:00"))
            if value.tzinfo is not None:
                value = value.astimezone(UTC).replace(tzinfo=None)
            return value
    except (ValueError, InvalidOperation):
        raise EngineMatrixError(f"value {text!r} is not a {kind}") from None
    return text


@dataclass
class DbxContext:
    client: dbx.Client
    redact: Redactor
    sleep: Callable[[float], None]
    clock: Callable[[], float]
    catalog: str
    schema: str
    volume: str
    statement_timeout: float
    job_timeout: float
    poll_interval: float

    def say(self, text: str) -> None:
        print(self.redact(text), flush=True)


def current_user(ctx: DbxContext) -> str:
    me = ctx.client.json("GET", "/api/2.0/preview/scim/v2/Me")
    user = str(me.get("userName") or "")
    if not user:
        raise EngineMatrixError("SCIM Me answered without a userName")
    ctx.redact.add(user)
    return user


def upload_reference(
    ctx: DbxContext, files: Sequence[Path], seed: Path, data_date: str
) -> tuple[str, str]:
    """Schema, Volume, then every reference file and the seed (``overwrite=true``)."""
    dbx.ensure_schema(ctx.client, catalog=ctx.catalog, schema=ctx.schema)
    dbx.ensure_volume(ctx.client, catalog=ctx.catalog, schema=ctx.schema, volume=ctx.volume)
    base = dbx.volume_root(ctx.catalog, ctx.schema, ctx.volume)
    data_dir = f"{base}/gharchive/{data_date}"
    for path in files:
        dbx._put_file(ctx.client, f"{data_dir}/{path.name}", Path(path).read_bytes())
    seed_path = f"{base}/seed/{Path(seed).name}"
    dbx._put_file(ctx.client, seed_path, Path(seed).read_bytes())
    ctx.say(f"uploaded {len(files)} reference file(s) and the seed to the matrix Volume")
    return data_dir, seed_path


def dbx_load_statements(catalog: str, schema: str, data_dir: str, seed_path: str) -> list[str]:
    """``CREATE OR REPLACE TABLE`` of ``raw_events`` and ``repos`` from the Volume files."""
    full = f"{catalog}.{schema}"
    return [
        f"CREATE OR REPLACE TABLE {full}.raw_events AS "
        "SELECT id, type, actor, repo, payload, created_at FROM read_files("
        f"{_sql_str(data_dir)}, format => 'json', schema => {_sql_str(spark_naive.RAW_SCHEMA)})",
        f"CREATE OR REPLACE TABLE {full}.repos AS SELECT repo_name, category FROM read_files("
        f"{_sql_str(seed_path)}, format => 'csv', header => true, "
        f"schema => {_sql_str(spark_naive.SEED_SCHEMA)})",
    ]


def dbx_count_sql(catalog: str, schema: str) -> str:
    return f"SELECT count(*) AS row_count FROM {catalog}.{schema}.raw_events"


def execute_statement(ctx: DbxContext, warehouse_id: str, sql: str, label: str) -> dict:
    """One statement on the warehouse: wait, follow every result chunk, convert the values."""
    body = {
        "statement": sql,
        "warehouse_id": warehouse_id,
        "catalog": ctx.catalog,
        "schema": ctx.schema,
        "wait_timeout": STATEMENT_WAIT,
        "on_wait_timeout": "CONTINUE",
        "disposition": "INLINE",
        "format": "JSON_ARRAY",
    }
    stmt = ctx.client.json("POST", "/api/2.0/sql/statements", json_body=body)
    statement_id = str(stmt.get("statement_id") or "")
    if not statement_id:
        raise EngineMatrixError(f"{label}: statement answered without a statement_id")
    state = (stmt.get("status") or {}).get("state", "")
    deadline = ctx.clock() + ctx.statement_timeout
    while state in _STATEMENT_WAITING:
        if ctx.clock() >= deadline:
            try:
                ctx.client.call("POST", f"/api/2.0/sql/statements/{statement_id}/cancel")
            except Exception:  # best effort; the timeout is the error reported
                pass
            raise EngineMatrixError(f"{label}: timed out (last state {state})")
        ctx.sleep(ctx.poll_interval)
        stmt = ctx.client.json("GET", f"/api/2.0/sql/statements/{statement_id}")
        state = (stmt.get("status") or {}).get("state", "")
    if state != "SUCCEEDED":
        error = ((stmt.get("status") or {}).get("error") or {}).get("message", "")
        raise EngineMatrixError(f"{label}: statement ended {state}: {error}")
    columns = ((stmt.get("manifest") or {}).get("schema") or {}).get("columns") or []
    names = [str(c.get("name")) for c in columns]
    types = [str(c.get("type_name", "")) for c in columns]
    result = stmt.get("result") or {}
    data = list(result.get("data_array") or [])
    link = result.get("next_chunk_internal_link")
    while link:
        chunk = ctx.client.json("GET", str(link))
        data.extend(chunk.get("data_array") or [])
        link = chunk.get("next_chunk_internal_link")
    rows = []
    for raw in data:
        if not isinstance(raw, list) or len(raw) != len(names):
            raise EngineMatrixError(f"{label}: malformed result row")
        rows.append(tuple(convert_value(t, v) for t, v in zip(types, raw, strict=True)))
    return {"statement_id": statement_id, "columns": names, "rows": rows}


def check_result_cache(
    ctx: DbxContext, statement_ids: Sequence[str], *, attempts: int = 7, wait: float = 10.0
) -> str:
    """``query_history``: every timed statement is in the history and none came from the result
    cache. A cached one raises (the measurement is invalid). ``unavailable``/``incomplete`` when
    the history cannot confirm it (recorded in notes, the unique comments still apply)."""
    params = [("filter_by.statement_ids", i) for i in statement_ids]
    params += [("include_metrics", "true"), ("max_results", str(max(100, len(statement_ids))))]
    path = "/api/2.0/sql/history/queries?" + urlencode(params)
    for attempt in range(attempts):
        try:
            data = ctx.client.json("GET", path)
        except (ApiError, OSError) as exc:
            ctx.say(f"query history unavailable: {exc}")
            return "unavailable"
        seen = {str(r.get("query_id")): r for r in data.get("res") or [] if isinstance(r, Mapping)}
        cached = [
            i
            for i in statement_ids
            if ((seen.get(i) or {}).get("metrics") or {}).get("result_from_cache") is True
        ]
        if cached:
            raise EngineMatrixError(
                f"databricks-sql: {len(cached)} timed statement(s) were answered from the result "
                "cache; the measurement is invalid"
            )
        if all(isinstance((seen.get(i) or {}).get("metrics"), Mapping) for i in statement_ids):
            return "query_history"
        if attempt + 1 < attempts:
            ctx.sleep(wait)
    return "incomplete"


def _token(value: object) -> str:
    return re.sub(r"[^A-Za-z0-9._-]", "_", str(value)) or "unknown"


def measure_databricks_sql(
    ctx: DbxContext,
    files: Sequence[Path],
    seed: Path,
    runs: int,
    *,
    data_date: str,
    warehouse_id: str,
    nonce: str,
) -> dict:
    data_dir, seed_path = upload_reference(ctx, files, seed, data_date)
    extra: dict[str, object] = {"schema": f"{ctx.catalog}.{ctx.schema}"}
    try:
        info = ctx.client.json("GET", f"/api/2.0/sql/warehouses/{warehouse_id}")
        for key, note in (("cluster_size", "warehouse_size"), ("warehouse_type", "warehouse_type")):
            if info.get(key):
                extra[note] = _token(info[key])
        if "enable_serverless_compute" in info:
            extra["serverless"] = "true" if info["enable_serverless_compute"] else "false"
    except Exception:  # informational only
        pass
    start = time.perf_counter()
    for i, stmt in enumerate(dbx_load_statements(ctx.catalog, ctx.schema, data_dir, seed_path)):
        execute_statement(ctx, warehouse_id, tagged(stmt, f"{nonce} load{i}"), f"load {i}")
    counted = execute_statement(
        ctx, warehouse_id, dbx_count_sql(ctx.catalog, ctx.schema), "row count"
    )
    load_seconds = time.perf_counter() - start
    if len(counted["rows"]) != 1:
        raise EngineMatrixError("databricks-sql: row count answered no row")
    timed_ids: list[str] = []

    def execute(q: str, sql: str, label: str) -> tuple[list[str], list[tuple]]:
        res = execute_statement(ctx, warehouse_id, tagged(sql, f"{nonce} {q} {label}"), q)
        if label != "warmup":
            timed_ids.append(res["statement_id"])
        return res["columns"], res["rows"]

    results, timed = timed_queries(execute, query_sql(spark_naive.QUERY_DIR), runs)
    extra["result_cache"] = "unique_comment"
    extra["cache_check"] = check_result_cache(ctx, timed_ids)
    versions: dict[str, str] = {}
    try:
        ver = execute_statement(
            ctx, warehouse_id, "SELECT current_version().dbsql_version AS v", "version"
        )
        if ver["rows"] and ver["rows"][0][0]:
            versions["dbsql"] = _token(ver["rows"][0][0])
    except Exception:  # informational only
        pass
    return {
        "row_count": int(counted["rows"][0][0]),
        "load_seconds": load_seconds,
        "results": results,
        "runs": timed,
        "versions": versions,
        "extra": extra,
    }


def notebook_source(load_statements: Sequence[str], count_sql: str) -> str:
    """Notebook run by the serverless job: Q1-Q6 (Spark SQL) embedded, timed in the notebook."""
    queries = query_sql(spark_naive.QUERY_DIR)
    header = [
        "# Databricks notebook source",
        "# RepoPulse E1 engine matrix: Q1-Q6 (Spark SQL) on serverless compute, timed here.",
        "# Imported by `python benchmarks/engine_matrix.py --engine databricks-serverless`.",
        "import datetime",
        "import decimal",
        "import json",
        "import time",
        "",
        f"QUERIES_JSON = {json.dumps(json.dumps(queries))}",
        f"LOAD_JSON = {json.dumps(json.dumps(list(load_statements)))}",
        f"COUNT_SQL = {json.dumps(count_sql)}",
        f"ORDER = {json.dumps(list(QUERIES))}",
    ]
    body = """
QUERIES = json.loads(QUERIES_JSON)
LOAD = json.loads(LOAD_JSON)
params = {name: dbutils.widgets.get(name) for name in ("runs", "nonce", "catalog", "schema")}
runs = int(params["runs"])
nonce = params["nonce"]


def jsonable(value):
    if isinstance(value, datetime.datetime):
        return value.isoformat(sep=" ")
    if isinstance(value, datetime.date):
        return value.isoformat()
    if isinstance(value, decimal.Decimal):
        return float(value)
    return value


def tagged(sql, tag):
    return sql.rstrip().rstrip(";").rstrip() + "\\n-- repopulse engine-matrix " + tag


start = time.perf_counter()
for i, stmt in enumerate(LOAD):
    spark.sql(tagged(stmt, nonce + " load" + str(i))).collect()
row_count = int(spark.sql(COUNT_SQL).collect()[0][0])
load_seconds = round(time.perf_counter() - start, 6)
spark.catalog.setCurrentCatalog(params["catalog"])
spark.catalog.setCurrentDatabase(params["schema"])


def run_query(q, label):
    t0 = time.perf_counter()
    df = spark.sql(tagged(QUERIES[q], nonce + " " + q + " " + label))
    rows = df.collect()
    return time.perf_counter() - t0, list(df.columns), rows


results = {}
for q in ORDER:
    _, columns, rows = run_query(q, "warmup")
    results[q] = {"columns": columns, "rows": [[jsonable(v) for v in r] for r in rows]}
timed = []
for i in range(1, runs + 1):
    run = {}
    for q in ORDER:
        seconds, _, rows = run_query(q, "run" + str(i))
        run[q + "_seconds"] = round(seconds, 6)
        run[q + "_rows"] = len(rows)
    timed.append(run)

summary = {
    "spark_version": spark.version,
    "row_count": row_count,
    "load_seconds": load_seconds,
    "warmup": 1,
    "runs": timed,
    "results": results,
}
dbutils.notebook.exit(json.dumps(summary))
"""
    return "\n".join(header) + "\n" + body


def measure_databricks_serverless(
    ctx: DbxContext,
    files: Sequence[Path],
    seed: Path,
    runs: int,
    *,
    data_date: str,
    nonce: str,
    user: str,
) -> dict:
    data_dir, seed_path = upload_reference(ctx, files, seed, data_date)
    notebook_path = f"/Users/{user}/{NOTEBOOK_DIR}/{NOTEBOOK_NAME}"
    source = notebook_source(
        dbx_load_statements(ctx.catalog, ctx.schema, data_dir, seed_path),
        dbx_count_sql(ctx.catalog, ctx.schema),
    )
    dbx.import_notebook(ctx.client, path=notebook_path, source=source)
    digest = hashlib.sha256(f"engine-matrix|{data_date}|{nonce}".encode()).hexdigest()[:24]
    payload = {
        "run_name": f"repopulse-engine-matrix-{data_date}",
        "idempotency_token": f"rp-matrix-{digest}",
        "timeout_seconds": int(ctx.job_timeout),
        "tasks": [
            {
                "task_key": TASK_KEY,
                "notebook_task": {
                    "notebook_path": notebook_path,
                    "base_parameters": {
                        "runs": str(int(runs)),
                        "nonce": nonce,
                        "catalog": ctx.catalog,
                        "schema": ctx.schema,
                    },
                },
            }
        ],
    }
    run_id = dbx.submit_run(ctx.client, payload)
    ctx.say(f"submitted serverless run {run_id}")
    try:
        outcome = dbx.wait_for_run(
            ctx.client,
            run_id,
            timeout=ctx.job_timeout,
            poll_interval=ctx.poll_interval * 20,
            clock=ctx.clock,
            sleep=ctx.sleep,
        )
    except dbx.JobError as exc:
        raise EngineMatrixError(f"databricks-serverless: {exc}") from None
    summary = check_summary(outcome.summary, "databricks-serverless notebook")
    if len(summary["runs"]) != runs:
        raise EngineMatrixError(
            f"databricks-serverless: notebook returned {len(summary['runs'])} runs, asked {runs}"
        )
    extra: dict[str, object] = {"schema": f"{ctx.catalog}.{ctx.schema}"}
    extra["result_cache"] = "unique_comment"
    for name, seconds in outcome.timings.items():
        extra[f"job_{name}_s"] = round(float(seconds), 3)
    return {
        "row_count": summary["row_count"],
        "load_seconds": summary["load_seconds"],
        "results": summary["results"],
        "runs": summary["runs"],
        "versions": {"spark": summary["spark_version"]},
        "extra": extra,
    }


# ---------------------------------------------------------------- CLI


def _append_summary(path: Path, engine: str, status: str, row: Mapping | None) -> None:
    lines = [f"### Engine matrix: `{variant(engine)}` (GH Archive real data)", "", status, ""]
    if row is not None:
        lines += ["```csv", ",".join(COLUMNS), ",".join(validate_row(row)[c] for c in COLUMNS)]
        lines += ["```", ""]
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a", encoding="utf-8", newline="\n") as f:
        f.write("\n".join(lines) + "\n")


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Q1-Q6 on one engine of the engine matrix (E1).")
    p.add_argument("--engine", required=True, choices=ENGINES)
    p.add_argument("--runs", type=int, default=5, help="timed runs after one warm-up")
    p.add_argument("--git-sha", default=None)
    p.add_argument("--data-dir", type=Path, help="default: data/raw/gharchive/<data_date>")
    p.add_argument("--reference", type=Path, default=REFERENCE_TOML)
    p.add_argument("--seed", type=Path, default=naive_duckdb.DEFAULT_SEED)
    p.add_argument("--out", type=Path, default=DEFAULT_OUT, help="runs.csv rows (appended)")
    p.add_argument("--summary", type=Path, default=DEFAULT_SUMMARY, help="markdown (appended)")
    p.add_argument("--md-database", default=DEFAULT_MD_DATABASE)
    p.add_argument("--catalog", default=DEFAULT_CATALOG)
    p.add_argument("--schema", default=DEFAULT_SCHEMA)
    p.add_argument("--volume", default=DEFAULT_VOLUME)
    p.add_argument("--statement-timeout", type=float, default=1800.0)
    p.add_argument("--job-timeout", type=float, default=3600.0)
    p.add_argument("--poll-interval", type=float, default=0.5)
    p.add_argument("--spark-master", default="local[*]")
    p.add_argument("--spark-driver-memory", default="10g")
    p.add_argument("--spark-shuffle-partitions", type=int, default=8)
    p.add_argument("--spark-timeout", type=float, default=3600.0)
    p.add_argument(
        "--add-mask",
        action="store_true",
        help="print ::add-mask:: for the bare hostname, user name and warehouse (GitHub Actions)",
    )
    p.add_argument("--spark-child", action="store_true", help=argparse.SUPPRESS)
    return p


def _measure(
    args: argparse.Namespace,
    env: Mapping[str, str],
    files: list[Path],
    data_dir: Path,
    data_date: str,
    redact: Redactor,
    *,
    transport: Transport | None,
    connect: Callable[..., Any] | None,
    sleep: Callable[[float], None],
    clock: Callable[[], float],
) -> dict:
    nonce = f"{env.get('GITHUB_RUN_ID') or 'local'}-{uuid.uuid4().hex[:12]}"
    engine = args.engine
    if engine == "duckdb":
        return measure_duckdb(files, args.seed, args.runs)
    if engine == "spark":
        cfg = {
            "master": args.spark_master,
            "driver_memory": args.spark_driver_memory,
            "shuffle_partitions": args.spark_shuffle_partitions,
        }
        return measure_spark(data_dir, args.seed, args.runs, cfg, args.spark_timeout)
    if engine == "motherduck":
        return measure_motherduck(
            files,
            args.seed,
            args.runs,
            connect=connect or (lambda database: duckdb.connect(database)),
            database=args.md_database,
            nonce=nonce,
        )
    secrets = Secrets.from_env(env)
    client = dbx.Client(
        secrets.host,
        secrets.token,
        transport=transport or functools.partial(urllib_transport, timeout=UPLOAD_TIMEOUT),
        sleep=sleep,
    )
    ctx = DbxContext(
        client=client,
        redact=redact,
        sleep=sleep,
        clock=clock,
        catalog=args.catalog,
        schema=args.schema,
        volume=args.volume,
        statement_timeout=args.statement_timeout,
        job_timeout=args.job_timeout,
        poll_interval=args.poll_interval,
    )
    masks = [urlsplit(secrets.host).netloc]
    if engine == "databricks-sql":
        probe = ProbeContext(secrets, transport or urllib_transport)
        warehouse_id, http_path = resolve_warehouse(probe)
        redact.add(warehouse_id)
        redact.add(http_path)
        user = current_user(ctx)
        masks += [user, warehouse_id, http_path]
        if args.add_mask:
            for value in masks:
                if value:
                    print(f"::add-mask::{value}", flush=True)
        return measure_databricks_sql(
            ctx,
            files,
            args.seed,
            args.runs,
            data_date=data_date,
            warehouse_id=warehouse_id,
            nonce=nonce,
        )
    user = current_user(ctx)
    if args.add_mask:
        for value in (*masks, user):
            if value:
                print(f"::add-mask::{value}", flush=True)
    return measure_databricks_serverless(
        ctx, files, args.seed, args.runs, data_date=data_date, nonce=nonce, user=user
    )


_NEEDS = {
    "motherduck": ("MOTHERDUCK_TOKEN",),
    "databricks-sql": ("DATABRICKS_HOST", "DATABRICKS_TOKEN"),
    "databricks-serverless": ("DATABRICKS_HOST", "DATABRICKS_TOKEN"),
}


def main(
    argv: Sequence[str] | None = None,
    *,
    env: Mapping[str, str] | None = None,
    transport: Transport | None = None,
    connect: Callable[..., Any] | None = None,
    sleep: Callable[[float], None] | None = None,
    clock: Callable[[], float] | None = None,
) -> int:
    env = os.environ if env is None else env
    args = build_parser().parse_args(argv)

    if args.spark_child:
        files = naive_duckdb.input_files(args.data_dir)
        cfg = {
            "master": args.spark_master,
            "driver_memory": args.spark_driver_memory,
            "shuffle_partitions": args.spark_shuffle_partitions,
        }
        print(json.dumps(spark_child(files, args.seed, args.runs, cfg)))
        return 0

    if args.engine == "snowflake":
        print(f"ERROR: {_SNOWFLAKE_REFUSAL}", file=sys.stderr)
        return 2

    run_at = datetime.now(UTC).isoformat(timespec="seconds")
    try:  # fail on bad metadata and names now, before any load or request
        if not args.git_sha:
            raise RunsLogError("git_sha: --git-sha is required")
        if args.runs < 1:
            raise RunsLogError("runs: --runs must be >= 1")
        ref = load_reference(args.reference)
        validate_row(
            {
                "run_at": run_at,
                "git_sha": args.git_sha,
                "variant": variant(args.engine),
                "data_date": ref["data_date"],
                "total_seconds": 0,
            }
        )
        if args.engine == "motherduck":
            check_md_database(args.md_database)
        if args.engine.startswith("databricks-"):
            check_dbx_names(args.catalog, args.schema, args.volume)
    except (RunsLogError, EngineMatrixError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2
    data_dir = args.data_dir or naive_duckdb.DEFAULT_DATA_ROOT / ref["data_date"]
    try:
        files = verify_files(data_dir, ref)
        expected = {p.name for p in files}
        extra = sorted(p.name for p in Path(data_dir).glob("*.json.gz") if p.name not in expected)
        if extra:
            raise ChecksumError(f"files not listed in the reference: {', '.join(extra)}")
    except ChecksumError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2

    missing = [n for n in _NEEDS.get(args.engine, ()) if not (env.get(n) or "").strip()]
    if missing:
        message = f"missing environment variable(s): {', '.join(missing)}"
        print(f"ERROR: {args.engine}: {message}", file=sys.stderr)
        _append_summary(args.summary, args.engine, f"FAILED: {message}", None)
        return 1

    redact = Secrets.from_env(env).redactor()
    try:
        reference = naive_duckdb.run_once(files, args.seed, collect=True)
        measured = _measure(
            args,
            env,
            files,
            Path(data_dir),
            ref["data_date"],
            redact,
            transport=transport,
            connect=connect,
            sleep=sleep or time.sleep,
            clock=clock or time.monotonic,
        )
        check_results(args.engine, measured, reference)
        _check_runs_match_warmup(args.engine, measured["runs"], measured["results"])
        extra = dict(measured["extra"])
        if env.get("GITHUB_RUN_ID"):
            extra["run_id"] = env["GITHUB_RUN_ID"]
            extra["trigger"] = env.get("GITHUB_EVENT_NAME") or "unknown"
        row = build_row(
            measured["runs"],
            engine=args.engine,
            git_sha=args.git_sha,
            data_date=ref["data_date"],
            run_at=run_at,
            row_count=measured["row_count"],
            load_seconds=measured["load_seconds"],
            timing=TIMING_OF[args.engine],
            versions=measured["versions"],
            extra=extra,
        )
    except Exception as exc:  # every engine error ends here, redacted; no row is written
        message = redact(f"{type(exc).__name__}: {exc}")
        print(f"ERROR {variant(args.engine)}: {message}", file=sys.stderr, flush=True)
        first = message.splitlines()[0] if message else ""
        _append_summary(args.summary, args.engine, f"FAILED (no row written): {first}", None)
        return 1

    args.out.parent.mkdir(parents=True, exist_ok=True)
    append_run(args.out, row)
    status = f"ok: median total {row['total_seconds']} s over {args.runs} run(s), " + (
        "results match DuckDB"
    )
    _append_summary(args.summary, args.engine, status, row)
    print(redact(f"wrote one {variant(args.engine)} row to {args.out}"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
