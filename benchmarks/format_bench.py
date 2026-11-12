"""Delta Lake vs Apache Iceberg: the same data written, queried, merged and time-travelled.

For each table format (``delta``, ``iceberg``) and each of ``--runs`` runs, a fresh child process
(own JVM, own peak RSS) does, on the real GH Archive reference day of ``benchmarks/reference.toml``:

1. load the hourly ``*.json.gz`` files (``spark_naive.RAW_SCHEMA``) into two cached DataFrames,
   the base table (every file but the last) and the MERGE source (the last two files, deduplicated
   by ``id``): ``load_seconds``, not part of ``total_seconds``;
2. write the base table, unpartitioned, default settings of the format: ``write_seconds``;
3. Q1-Q6 of ``benchmarks/queries_spark/`` verbatim on a ``raw_events`` view over the table;
4. ``MERGE INTO`` the table from the source (update on matching ``id``, insert the rest):
   ``merge_seconds``; then the very same MERGE again, which must not change the row count:
   ``merge_rerun_seconds``;
5. read the version written in step 2 (Delta ``versionAsOf``, Iceberg ``VERSION AS OF <snapshot>``)
   and count it, which must equal the base table: ``time_travel_seconds``.

``total_seconds`` = write + Q1..Q6 + merge + merge rerun + time travel. The runs of one format give
one median row in the ``runs.csv`` format (variant ``format-delta`` / ``format-iceberg``); both
formats must agree on every row count, or neither row is written. Delta tables are path tables,
Iceberg tables live in a ``hadoop`` catalog registered at run time, one per work directory.
Definitions and limits: ``docs/iceberg-vs-delta.md``.

Usage (what ``.github/workflows/format-bench.yml`` runs)::

    python benchmarks/format_bench.py --runs 5 --git-sha <sha> --out out/format-rows.csv \
        --summary out/format-summary.md

pyspark and delta are imported lazily so the pure helpers can be imported without them.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import shutil
import statistics
import subprocess
import sys
import time
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from benchmarks import spark_naive  # noqa: E402
from benchmarks.measure import MeasureError, summarize_runs  # noqa: E402
from benchmarks.reference import (  # noqa: E402
    REFERENCE_TOML,
    ChecksumError,
    load_reference,
    verify_files,
)
from benchmarks.spark_naive import (  # noqa: E402
    DEFAULT_SEED,
    QUERIES,
    QUERY_DIR,
    RAW_SCHEMA,
    SEED_SCHEMA,
)
from repopulse.runs_log import COLUMNS, RunsLogError, append_run, validate_row  # noqa: E402

FORMATS: tuple[str, ...] = ("delta", "iceberg")
ICEBERG_VERSION = "1.9.2"
ICEBERG_PACKAGE = f"org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:{ICEBERG_VERSION}"
EXTRA_PACKAGES: tuple[str, ...] = (ICEBERG_PACKAGE,)
SQL_EXTENSIONS = (
    "io.delta.sql.DeltaSparkSessionExtension,"
    "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
)
DELTA_CATALOG = "org.apache.spark.sql.delta.catalog.DeltaCatalog"
ICEBERG_CATALOG = "org.apache.iceberg.spark.SparkCatalog"
NAMESPACE = "db"
TABLE_NAME = "events"
DEFAULT_DATA_ROOT = Path("data/raw/gharchive")
DEFAULT_WORK_DIR = Path("data/tmp/format-bench")
RUN_TIMEOUT_S = 30 * 60

STEP_KEYS: tuple[str, ...] = (
    "write_seconds",
    *(f"{q}_seconds" for q in QUERIES),
    "merge_seconds",
    "merge_rerun_seconds",
    "time_travel_seconds",
)
SECONDS_KEYS: tuple[str, ...] = ("total_seconds", *STEP_KEYS, "load_seconds", "startup_seconds")
COUNT_KEYS: tuple[str, ...] = (
    "row_count",
    "bytes_read",
    "base_rows",
    "merge_source_rows",
    *(f"{q}_rows" for q in QUERIES),
)
OPTIONAL_COUNT_KEYS: tuple[str, ...] = ("table_bytes", "table_files")
CHECKED_KEYS: tuple[str, ...] = (
    "base_rows",
    "row_count",
    "merge_source_rows",
    *(f"{q}_rows" for q in QUERIES),
)
NOTE_SECONDS: tuple[str, ...] = (
    "write_seconds",
    "merge_seconds",
    "merge_rerun_seconds",
    "time_travel_seconds",
    "load_seconds",
    "startup_seconds",
)


class FormatBenchError(RuntimeError):
    """A step, a run, a summary or the cross-check of the format benchmark failed."""


def variant(fmt: str) -> str:
    if fmt not in FORMATS:
        raise FormatBenchError(f"unknown table format {fmt!r} (expected one of {FORMATS})")
    return f"format-{fmt}"


# ---------------------------------------------------------------- session and catalogs


def session_config(spark_cfg: Mapping[str, object]) -> dict[str, str]:
    """``spark_naive.session_config`` plus Delta (session catalog) and the Iceberg extensions.

    ``spark.jars.packages`` is not here: ``configure_spark_with_delta_pip`` sets it to the Delta
    jar matching the installed delta-spark plus :data:`EXTRA_PACKAGES`.
    """
    conf = spark_naive.session_config(spark_cfg)
    conf["spark.app.name"] = "repopulse-format-benchmark"
    conf["spark.sql.extensions"] = SQL_EXTENSIONS
    conf["spark.sql.catalog.spark_catalog"] = DELTA_CATALOG
    return conf


def build_session(spark_cfg: Mapping[str, object]):
    import os

    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    builder = SparkSession.builder
    for key, value in session_config(spark_cfg).items():
        builder = builder.config(key, value)
    return configure_spark_with_delta_pip(
        builder, extra_packages=list(EXTRA_PACKAGES)
    ).getOrCreate()


def iceberg_catalog_name(work_dir: str | Path) -> str:
    """Catalog name derived from the work directory (a catalog cannot be re-pointed once loaded)."""
    digest = hashlib.sha256(Path(work_dir).resolve().as_posix().encode("utf-8")).hexdigest()
    return f"ice_{digest[:10]}"


def iceberg_catalog_conf(work_dir: str | Path) -> dict[str, str]:
    """Session conf of a ``hadoop`` Iceberg catalog whose warehouse is ``<work_dir>/iceberg``."""
    prefix = f"spark.sql.catalog.{iceberg_catalog_name(work_dir)}"
    return {
        prefix: ICEBERG_CATALOG,
        f"{prefix}.type": "hadoop",
        f"{prefix}.warehouse": (Path(work_dir) / "iceberg").resolve().as_posix(),
        # always read the latest metadata (no stale table after a MERGE in the same session)
        f"{prefix}.cache-enabled": "false",
    }


@dataclass(frozen=True)
class Table:
    fmt: str
    ident: str  # SQL identifier usable in MERGE / SELECT
    location: Path  # directory holding the table's files
    catalog: str | None = None  # Iceberg catalog name


def table_for(fmt: str, work_dir: str | Path) -> Table:
    """Where the table of ``fmt`` lives under ``work_dir`` (pure, touches nothing)."""
    variant(fmt)
    root = Path(work_dir).resolve()
    if "`" in root.as_posix():
        raise FormatBenchError(f"work directory must not contain a backtick: {root}")
    if fmt == "delta":
        location = root / "delta" / TABLE_NAME
        return Table(fmt, f"delta.`{location.as_posix()}`", location)
    catalog = iceberg_catalog_name(work_dir)
    location = root / "iceberg" / NAMESPACE / TABLE_NAME
    return Table(fmt, f"{catalog}.{NAMESPACE}.{TABLE_NAME}", location, catalog)


def open_table(spark, fmt: str, work_dir: str | Path) -> Table:
    """:func:`table_for`, with the Iceberg catalog and namespace registered in ``spark``."""
    table = table_for(fmt, work_dir)
    if fmt == "iceberg":
        for key, value in iceberg_catalog_conf(work_dir).items():
            spark.conf.set(key, value)
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {table.catalog}.{NAMESPACE}")
    return table


# ---------------------------------------------------------------- data


def split_files(files: Sequence[str | Path]) -> tuple[list[Path], list[Path]]:
    """Base table = every file but the last; MERGE source = the last two files.

    One source file is already in the table (rows that match on ``id``: updates), the other is
    new (inserts, plus any id that also appears in an earlier hour).
    """
    paths = [Path(p) for p in files]
    if len(paths) < 2:
        raise FormatBenchError(f"need at least 2 input files, got {len(paths)}")
    return paths[:-1], paths[-2:]


def read_raw(spark, files: Sequence[str | Path]):
    return spark.read.schema(RAW_SCHEMA).json([str(p) for p in files])


def dedup_by_id(df):
    """One row per non-null ``id``, chosen deterministically (latest ``created_at`` first).

    A MERGE source must not have two rows for one target row (Delta raises an error); rows
    without an ``id`` can never match and would be inserted again by every rerun, so they are
    left out of the source.
    """
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    order = [
        F.col("created_at").desc_nulls_last(),
        *(
            F.col(c).asc_nulls_last()
            for c in ("type", "payload", "actor.id", "actor.login", "repo.id", "repo.name")
        ),
    ]
    window = Window.partitionBy("id").orderBy(*order)
    return (
        df.where(F.col("id").isNotNull())
        .withColumn("_rn", F.row_number().over(window))
        .where(F.col("_rn") == 1)
        .drop("_rn")
    )


# ---------------------------------------------------------------- table operations


def write_table(spark, table: Table, df) -> None:
    """Create the table from ``df`` (unpartitioned, default settings); the location must be new."""
    if table.location.exists():
        raise FormatBenchError(f"{table.fmt} table location already exists: {table.location}")
    if table.fmt == "delta":
        df.write.format("delta").mode("error").save(table.location.as_posix())
    else:
        df.writeTo(table.ident).create()


def read_table(spark, table: Table, version: int | None = None):
    """The table as a DataFrame; ``version`` = Delta version or Iceberg snapshot id."""
    if table.fmt == "delta":
        reader = spark.read.format("delta")
        if version is not None:
            reader = reader.option("versionAsOf", int(version))
        return reader.load(table.location.as_posix())
    if version is None:
        return spark.table(table.ident)
    return spark.sql(f"SELECT * FROM {table.ident} VERSION AS OF {int(version)}")


def current_version(spark, table: Table) -> int:
    """Latest Delta version, or the latest Iceberg snapshot id."""
    if table.fmt == "delta":
        from delta.tables import DeltaTable

        rows = DeltaTable.forPath(spark, table.location.as_posix()).history(1).collect()
        if not rows:
            raise FormatBenchError(f"delta table {table.location} has no history")
        return int(rows[0]["version"])
    rows = spark.sql(
        f"SELECT snapshot_id FROM {table.ident}.snapshots ORDER BY committed_at DESC LIMIT 1"
    ).collect()
    if not rows:
        raise FormatBenchError(f"iceberg table {table.ident} has no snapshot")
    return int(rows[0][0])


def merge_into(spark, table: Table, source) -> None:
    """Upsert ``source`` (same schema, unique ``id``) into ``table`` on ``id``."""
    view = f"merge_src_{uuid.uuid4().hex}"
    source.createOrReplaceTempView(view)
    try:
        spark.sql(
            f"MERGE INTO {table.ident} AS t USING {view} AS s ON t.id = s.id "
            "WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *"
        )
    finally:
        spark.catalog.dropTempView(view)


def table_size(table: Table) -> tuple[int, int]:
    """Bytes and number of the table's parquet data files (Delta log checkpoints excluded)."""
    files = [
        p
        for p in table.location.rglob("*.parquet")
        if p.is_file() and "_delta_log" not in p.relative_to(table.location).parts
    ]
    return sum(p.stat().st_size for p in files), len(files)


def run_queries(spark, raw_df, seed_path: str | Path = DEFAULT_SEED, *, collect: bool = False):
    """Q1..Q6 on a ``raw_events`` view over ``raw_df``; ``qN_seconds`` includes ``collect()``.

    The ``repos`` seed view is created (and cached) before the first query, not timed.
    """
    result: dict = {}
    if collect:
        result["results"] = {}
    repos = None
    try:
        repos = spark.read.csv(str(seed_path), header=True, schema=SEED_SCHEMA).cache()
        repos.count()
        repos.createOrReplaceTempView("repos")
        raw_df.createOrReplaceTempView("raw_events")
        for q in QUERIES:
            sql = (QUERY_DIR / f"{q}.sql").read_text(encoding="utf-8")
            t0 = time.perf_counter()
            df = spark.sql(sql)
            rows = df.collect()
            result[f"{q}_seconds"] = round(time.perf_counter() - t0, 6)
            result[f"{q}_rows"] = len(rows)
            if collect:
                result["results"][q] = {
                    "columns": list(df.columns),
                    "rows": [tuple(r) for r in rows],
                }
    finally:
        if repos is not None:
            repos.unpersist()
        spark.catalog.dropTempView("raw_events")
        spark.catalog.dropTempView("repos")
    return result


def _since(t0: float) -> float:
    return round(time.perf_counter() - t0, 6)


def run_format(
    spark,
    fmt: str,
    files: Sequence[str | Path],
    seed_path: str | Path = DEFAULT_SEED,
    work_dir: str | Path = DEFAULT_WORK_DIR,
    *,
    collect: bool = False,
) -> dict:
    """Load, write, Q1..Q6, MERGE twice and time travel once for ``fmt``; seconds per step."""
    variant(fmt)
    paths = [Path(p) for p in files]
    absent = [str(p) for p in paths if not p.is_file()]
    if absent:
        raise FileNotFoundError(f"missing input file(s): {', '.join(absent)}")
    base_files, source_files = split_files(paths)
    result: dict = {"bytes_read": sum(p.stat().st_size for p in paths)}
    table = open_table(spark, fmt, work_dir)
    base = source = None
    try:
        t0 = time.perf_counter()
        base = read_raw(spark, base_files).cache()
        base.count()
        source = dedup_by_id(read_raw(spark, source_files)).cache()
        result["merge_source_rows"] = source.count()
        result["load_seconds"] = _since(t0)

        t0 = time.perf_counter()
        write_table(spark, table, base)
        result["write_seconds"] = _since(t0)
        result["base_rows"] = read_table(spark, table).count()
        result["table_bytes"], result["table_files"] = table_size(table)

        result.update(run_queries(spark, read_table(spark, table), seed_path, collect=collect))

        before_merge = current_version(spark, table)
        t0 = time.perf_counter()
        merge_into(spark, table, source)
        result["merge_seconds"] = _since(t0)
        merged = read_table(spark, table).count()

        t0 = time.perf_counter()
        merge_into(spark, table, source)
        result["merge_rerun_seconds"] = _since(t0)
        result["row_count"] = read_table(spark, table).count()
        if result["row_count"] != merged:
            raise FormatBenchError(
                f"{fmt}: MERGE is not idempotent: {merged} rows after the first MERGE, "
                f"{result['row_count']} after running it again"
            )

        t0 = time.perf_counter()
        travelled = read_table(spark, table, version=before_merge).count()
        result["time_travel_seconds"] = _since(t0)
        if travelled != result["base_rows"]:
            raise FormatBenchError(
                f"{fmt}: version {before_merge} holds {travelled} rows, the table had "
                f"{result['base_rows']} before MERGE"
            )
    finally:
        for df in (base, source):
            if df is not None:
                df.unpersist()
    result["total_seconds"] = round(sum(result[k] for k in STEP_KEYS), 6)
    return result


# ---------------------------------------------------------------- one run (child process)


def run_single(
    fmt: str,
    files: Sequence[str | Path],
    seed_path: str | Path,
    work_dir: str | Path,
    spark_cfg: Mapping[str, object] | None = None,
) -> dict:
    """One measured run in this (fresh) process; the work directory is emptied before and after."""
    from importlib.metadata import version as dist_version

    work = Path(work_dir)
    if work.exists():
        shutil.rmtree(work)
    try:
        t0 = time.perf_counter()
        spark = build_session(spark_cfg or {})
        spark.range(1).count()  # JVM up and an executor ready
        startup = _since(t0)
        try:
            result = run_format(spark, fmt, files, seed_path, work)
            result["spark_version"] = spark.version
        except BaseException:
            spark.stop()
            raise
        result["delta_version"] = dist_version("delta-spark")
        result["startup_seconds"] = startup
        result["peak_rss_mb"] = spark_naive.stop_and_measure(spark)
    finally:
        shutil.rmtree(work, ignore_errors=True)
    return result


def run_in_subprocess(
    fmt: str,
    *,
    data_dir: Path,
    reference: Path,
    seed: Path,
    work_dir: Path,
    timeout_s: float = RUN_TIMEOUT_S,
) -> dict:
    """One measured run of ``fmt`` in a fresh interpreter (own JVM, own peak RSS)."""
    cmd = [
        sys.executable,
        str(Path(__file__).resolve()),
        "--single",
        "--format",
        fmt,
        "--data-dir",
        str(data_dir),
        "--reference",
        str(reference),
        "--seed",
        str(seed),
        "--work-dir",
        str(work_dir),
    ]
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout_s, check=False)
    except subprocess.TimeoutExpired:
        raise FormatBenchError(f"{fmt} run timed out after {timeout_s:.0f} s") from None
    if proc.returncode != 0:
        tail = "\n".join(proc.stderr.strip().splitlines()[-20:])
        raise FormatBenchError(f"{fmt} run failed (exit {proc.returncode}):\n{tail}")
    return parse_run_output(proc.stdout, fmt)


# ---------------------------------------------------------------- run output


def _check_number(label: str, key: str, value: object, *, integer: bool) -> None:
    kinds = int if integer else int | float
    if isinstance(value, bool) or not isinstance(value, kinds):
        kind = "an integer" if integer else "a number"
        raise FormatBenchError(f"{label} run: {key}: expected {kind}, got {value!r}")
    if not math.isfinite(value) or value < 0:
        raise FormatBenchError(f"{label} run: {key}: must be finite and >= 0, got {value!r}")


def parse_run_output(stdout: str, fmt: str) -> dict:
    """The last non-empty stdout line of one run, as a checked dict."""
    lines = [ln for ln in stdout.splitlines() if ln.strip()]
    if not lines:
        raise FormatBenchError(f"{fmt} run: no output")
    try:
        run = json.loads(lines[-1])
    except ValueError:
        raise FormatBenchError(f"{fmt} run: last line is not JSON: {lines[-1][:200]!r}") from None
    if not isinstance(run, dict):
        raise FormatBenchError(f"{fmt} run: expected a JSON object, got {type(run).__name__}")
    absent = [k for k in (*SECONDS_KEYS, *COUNT_KEYS) if k not in run]
    if absent:
        raise FormatBenchError(f"{fmt} run: missing key(s): {', '.join(absent)}")
    for key in SECONDS_KEYS:
        _check_number(fmt, key, run[key], integer=False)
    for key in COUNT_KEYS:
        _check_number(fmt, key, run[key], integer=True)
    for key in OPTIONAL_COUNT_KEYS:
        if run.get(key) is not None:
            _check_number(fmt, key, run[key], integer=True)
    if run.get("peak_rss_mb") is not None:
        _check_number(fmt, "peak_rss_mb", run["peak_rss_mb"], integer=False)
    for key in ("spark_version", "delta_version"):
        value = run.get(key)
        if value is not None and (
            not isinstance(value, str) or not value or value.split() != [value]
        ):
            raise FormatBenchError(f"{fmt} run: {key}: expected a version string, got {value!r}")
    return run


# ---------------------------------------------------------------- rows


def _version_note(runs: Sequence[Mapping[str, object]], key: str) -> str:
    value = runs[0].get(key)
    return str(value).replace(",", ";") if value else "unknown"


def summarize_format(
    runs: Sequence[Mapping[str, object]],
    *,
    fmt: str,
    git_sha: str,
    data_date: str,
    run_at: str,
    cross_check: str = "full",
) -> dict[str, object]:
    """Median row of one format; every row count must be the same in every run."""
    label = variant(fmt)
    if not runs:
        raise FormatBenchError(f"{label}: no runs to summarise")
    for key in CHECKED_KEYS:
        values = {int(r[key]) for r in runs}
        if len(values) != 1:
            raise FormatBenchError(f"{label}: {key} differs between runs: {sorted(values)}")
    try:
        row = summarize_runs(
            runs, variant=label, git_sha=git_sha, data_date=data_date, run_at=run_at
        )
    except MeasureError as exc:
        raise FormatBenchError(f"{label}: {exc}") from None
    notes = [f"format={fmt}", f"runs={len(runs)}", "stat=median"]
    for key in NOTE_SECONDS:
        median = statistics.median(float(r[key]) for r in runs)
        notes.append(f"{key}={round(float(median), 3)}")
    notes.append(f"base_rows={int(runs[0]['base_rows'])}")
    notes.append(f"merge_source_rows={int(runs[0]['merge_source_rows'])}")
    for key in OPTIONAL_COUNT_KEYS:
        values = [int(r[key]) for r in runs if r.get(key) is not None]
        if values:
            notes.append(f"{key}={round(statistics.median(values))}")
    notes.append(f"spark={_version_note(runs, 'spark_version')}")
    notes.append(f"delta={_version_note(runs, 'delta_version')}")
    notes.append(f"iceberg={ICEBERG_VERSION}")
    notes.append(f"cross_check={cross_check}")
    row["notes"] = " ".join(notes)
    validate_row(row)
    return row


def cross_check(
    delta_runs: Sequence[Mapping[str, object]], iceberg_runs: Sequence[Mapping[str, object]]
) -> None:
    """Both formats held the same rows and every query returned as many rows."""
    if not delta_runs or not iceberg_runs:
        raise FormatBenchError("cross-check needs runs of both formats")
    d, i = delta_runs[0], iceberg_runs[0]
    diffs = [
        f"{key} delta={d[key]} iceberg={i[key]}"
        for key in CHECKED_KEYS
        if int(d[key]) != int(i[key])
    ]
    if diffs:
        raise FormatBenchError("formats disagree: " + "; ".join(diffs))


# ---------------------------------------------------------------- CLI


def _write_summary(path: Path, status: list[tuple[str, str]], rows: list[dict]) -> None:
    lines = ["### Format benchmark (Delta vs Iceberg, GH Archive real data)", ""]
    lines += ["| variant | status |", "|---|---|"]
    lines += [f"| `{cell}` | {text.splitlines()[0] if text else ''} |" for cell, text in status]
    lines += ["", f"{len(rows)} row(s) for runs.csv:", "", "```csv", ",".join(COLUMNS)]
    lines += [",".join(validate_row(r)[c] for c in COLUMNS) for r in rows]
    lines += ["```", ""]
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines), encoding="utf-8")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Delta vs Iceberg: write, Q1-Q6, MERGE, time travel."
    )
    parser.add_argument("--data-dir", type=Path, help="default: data/raw/gharchive/<data_date>")
    parser.add_argument("--reference", type=Path, default=REFERENCE_TOML)
    parser.add_argument("--seed", type=Path, default=DEFAULT_SEED)
    parser.add_argument("--work-dir", type=Path, default=DEFAULT_WORK_DIR, help="tables (temp)")
    parser.add_argument("--runs", type=int, default=5)
    parser.add_argument("--git-sha", default=None)
    parser.add_argument("--out", type=Path, default=Path("out/format-rows.csv"))
    parser.add_argument("--summary", type=Path, default=Path("out/format-summary.md"))
    parser.add_argument("--single", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--format", choices=FORMATS, help=argparse.SUPPRESS)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.single:
        if not args.format:
            parser.error("--format is required with --single")
        ref = load_reference(args.reference)
        data_dir = args.data_dir or DEFAULT_DATA_ROOT / ref["data_date"]
        files = [data_dir / entry["name"] for entry in ref["files"]]
        result = run_single(args.format, files, args.seed, args.work_dir / args.format)
        print(json.dumps(result))
        return 0

    run_at = datetime.now(UTC).isoformat(timespec="seconds")
    try:  # fail on bad metadata now, not after the runs
        if not args.git_sha:
            raise RunsLogError("git_sha: --git-sha is required")
        if args.runs < 1:
            raise RunsLogError("runs: --runs must be >= 1")
        ref = load_reference(args.reference)
        for fmt in FORMATS:
            validate_row(
                {
                    "run_at": run_at,
                    "git_sha": args.git_sha,
                    "variant": variant(fmt),
                    "data_date": ref["data_date"],
                    "total_seconds": 0,
                }
            )
    except RunsLogError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2
    data_dir = args.data_dir or DEFAULT_DATA_ROOT / ref["data_date"]
    try:
        split_files(verify_files(data_dir, ref))
    except (ChecksumError, FormatBenchError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2

    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(",".join(COLUMNS) + "\n", encoding="utf-8", newline="")
    rows: list[dict] = []
    status: list[tuple[str, str]] = []
    failed = False

    def fail(cell: str, message: str) -> None:
        nonlocal failed
        failed = True
        status.append((cell, f"FAILED: {message}"))
        print(f"ERROR {cell}: {message}", file=sys.stderr, flush=True)

    measured: dict[str, list[dict]] = {}
    for fmt in FORMATS:
        cell = variant(fmt)
        runs: list[dict] = []
        try:
            for i in range(1, args.runs + 1):
                run = run_in_subprocess(
                    fmt,
                    data_dir=data_dir,
                    reference=args.reference,
                    seed=args.seed,
                    work_dir=args.work_dir,
                    timeout_s=RUN_TIMEOUT_S,
                )
                print(
                    f"{cell} run {i}/{args.runs}: total {run['total_seconds']:.3f}s "
                    f"rows {run['base_rows']} -> {run['row_count']}",
                    flush=True,
                )
                runs.append(run)
        except FormatBenchError as exc:
            fail(cell, str(exc))
            continue
        measured[fmt] = runs

    meta = {"git_sha": args.git_sha, "data_date": ref["data_date"], "run_at": run_at}
    summarized: dict[str, dict] = {}
    for fmt, runs in measured.items():
        try:
            summarized[fmt] = summarize_format(runs, fmt=fmt, **meta)
        except (FormatBenchError, RunsLogError) as exc:
            fail(variant(fmt), str(exc))
    if len(summarized) == len(FORMATS):
        try:
            cross_check(measured["delta"], measured["iceberg"])
        except FormatBenchError as exc:
            for fmt in FORMATS:
                fail(variant(fmt), str(exc))
            summarized = {}
    else:  # the other format failed: its row is kept, marked as not cross-checked
        summarized = {
            fmt: summarize_format(measured[fmt], fmt=fmt, cross_check="none", **meta)
            for fmt in summarized
        }

    for fmt in FORMATS:
        if fmt not in summarized:
            continue
        row = summarized[fmt]
        append_run(args.out, row)
        rows.append(row)
        status.append((variant(fmt), f"ok, median total {row['total_seconds']} s"))

    _write_summary(args.summary, status, rows)
    print(f"wrote {len(rows)} row(s) to {args.out}")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
