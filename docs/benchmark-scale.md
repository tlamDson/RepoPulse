# Benchmark scale: Spark vs DuckDB at 1, 7 and 30 days

Measure M5 in [`plan.md`](plan.md): where (if anywhere) PySpark catches up with DuckDB on the same
transformation as the data grows. Levels: 1, 7, 30 days; 3 runs per level and engine; the
reference is the DuckDB naive pipeline of PR B.

**Numbers live only in `runs.csv`.** This page describes how they are produced; it quotes none.

## Data

- **Real GH Archive data** (not simulated), window and levels in
  [`benchmarks/scale.toml`](../benchmarks/scale.toml): `start_date = "2025-06-01"`, levels
  `[1, 7, 30]`. A smaller level is a prefix of the larger ones, so the 30-day window is prepared
  once and used by all three.
- 2025 rather than 2026: 2026 hour files are many times smaller, too small to find a crossover
  (`plan.md`, GH Archive risk).
- Preparation (`benchmarks/scale_prepare.py`, **not timed**): 30 raw days do not fit on the 14 GB
  runner disk, so each hour file is downloaded, filtered to the event types Q1-Q6 read
  (`event_types` in `scale.toml`; a test checks they equal the types in `benchmarks/queries/`),
  slimmed to the fields Q1-Q6 read (`id`, `type`, `actor.id/login`, `repo.id/name`, `created_at`
  as the original RFC 3339 text, and `payload` reduced to `action`, `number`,
  `pull_request.number/merged`, `issue.number`), written as a gzip NDJSON file with the same name,
  and the raw file is deleted right away. Duplicate lines are kept (the baseline does not dedupe).
  A test checks Q1-Q6 give identical results on slimmed and full data.
- `data/scale/gharchive/manifest.json`: one entry per hour with raw bytes, raw sha256, total /
  kept / unparsable line counts and slim bytes, or `"status": "missing"` when GH Archive answers
  404. Missing hours also appear in the `notes` of the rows (`missing_hours=N`).
- Both engines read exactly the same slim files. `bytes_read` is the size of those files.

## What is timed

The transformation is the naive pipeline of PR B on both engines: load the raw events into an
engine table, then run Q1-Q6.

| | DuckDB | PySpark |
|---|---|---|
| Load | `CREATE TABLE raw_events AS SELECT * FROM read_json(...)` (`naive_duckdb.load`) | `spark.read.schema(...).json(files).cache()` + `count()` |
| Queries | `benchmarks/queries/q1..q6.sql` | `benchmarks/queries_spark/q1..q6.sql` (same logic in Spark SQL) |
| Settings | in-memory DB, `temp_directory` from `scale.toml` (spill instead of OOM) | `local[*]`, driver memory and `spark.sql.shuffle.partitions` from `scale.toml`, session time zone UTC, no Delta |

- `total_seconds` = load + Q1..Q6. The clock starts when loading starts, the same for both
  engines (`naive_duckdb.run_once`, `spark_naive.run_once`).
- `qN_seconds` includes fetching the result rows to the driver (`fetchall()` / `collect()`).
- **Spark startup** (JVM + `SparkSession`, until a trivial job ran) is measured apart and is
  **not** in `total_seconds`; its median is in `notes` as `startup_seconds=…`.
- Every run is a fresh child process (own JVM for Spark). The row of a cell holds the median of
  its runs (`benchmarks/measure.py::summarize_runs`).
- `peak_rss_mb`: DuckDB = `ru_maxrss` of the run process. Spark = `ru_maxrss` of the Python
  driver (RUSAGE_SELF) **plus** the largest exited child (RUSAGE_CHILDREN, i.e. the JVM), read
  after `spark.stop()` and after the JVM has exited and been waited for. This is an upper bound of
  the combined peak (the two peaks may happen at different moments). When the JVM cannot be waited
  for, the column is left empty rather than holding a wrong number.

## Checks before a row is written

- Each run prints one JSON line; the parser rejects missing keys, non-JSON output, negative or
  non-numeric values (`scale_bench.parse_run_output`).
- `row_count`, `bytes_read` and the row count of every query must be equal across the runs of a
  cell.
- Cross-check per level: Spark and DuckDB must have the same `row_count` and the same row count
  for each of Q1-Q6. When they disagree, neither row of that level is written and the error is
  printed.
- A failing cell (crash, OOM, timeout after `run_timeout_minutes`) does not stop the other cells;
  its error is in the output and in the job summary, and the exit code is non-zero. When only one
  engine of a level succeeded, its row is written with `not cross-checked` in `notes`.

## Rows

`variant` = `duckdb-1d`, `spark-1d`, `duckdb-7d`, `spark-7d`, `duckdb-30d`, `spark-30d`;
`data_date` = first day of the window; `notes` starts with `engine=<engine> days=<N> runs=3 median`.

## How to run

On GitHub: workflow **Benchmark scale** (`.github/workflows/benchmark-scale.yml`),
`workflow_dispatch` or any PR into `develop`/`main` that touches the benchmark files. Not a
required check. The prepared data is cached (`actions/cache/restore` before preparing,
`actions/cache/save` right after, key = hash of `scale.toml`, `scale_prepare.py`, `download.py`),
so a later run does not download again even if the benchmark step failed.

Locally (needs Java 17 for Spark, `pip install -e ".[dev,spark]"`):

```bash
python benchmarks/scale_prepare.py                 # download + slim the window (resumable)
python benchmarks/scale_bench.py --git-sha <sha> --out out/scale-rows.csv --summary out/scale-summary.md
```

## Recording the results

The workflow uploads the artifact **`benchmark-scale`**: `out/scale-rows.csv` (the `runs.csv`
header plus up to 6 rows), `out/scale-summary.md` and `data/scale/gharchive/manifest.json`.
Merge the rows into `runs.csv` (schema-checked, re-recording is a no-op):

```bash
gh run download <run-id> -n benchmark-scale -D out/benchmark-scale
python benchmarks/record_run.py out/benchmark-scale/out/scale-rows.csv
```
