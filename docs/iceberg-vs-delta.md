# Delta Lake vs Apache Iceberg: write, Q1-Q6, MERGE, time travel

Task E2 in [`plan.md`](plan.md) (Plan v2): the same data written as a Delta table and as an Iceberg
table with PySpark on the GitHub Actions runner, then queried (Q1-Q6), merged and read at an older
version. Script [`benchmarks/format_bench.py`](../benchmarks/format_bench.py), workflow
[`.github/workflows/format-bench.yml`](../.github/workflows/format-bench.yml). Rows:
`variant=format-delta` and `variant=format-iceberg`.

**Numbers live only in `runs.csv`.** This page describes how they are produced; it quotes none.

## Data and setup

- **Real GH Archive data** (not simulated): the 24 hourly files of the reference day in
  [`benchmarks/reference.toml`](../benchmarks/reference.toml), checksum-verified before anything
  runs (a mismatch, or fewer than 2 files, stops with exit code 2 and writes nothing). Rows are
  read with `spark_naive.RAW_SCHEMA` (`id`, `type`, `actor STRUCT<id, login>`,
  `repo STRUCT<id, name>`, `payload` as a JSON string, `created_at TIMESTAMP`, session time zone
  UTC), the same schema as the Spark side of the scale benchmark.
- **Base table** = every file of the day but the last (hours 0-22). **MERGE source** = the last two
  files (hours 22 and 23): hour 22 is already in the table (updates), hour 23 is new (inserts).
  The source is deduplicated by `id` deterministically (latest `created_at`, then the other
  columns), because a MERGE source must not have two rows for one target row; rows without an
  `id` are left out of the source (they could never match and every rerun would insert them
  again). The base table keeps the raw lines as they are, duplicates included.
- **Spark**: PySpark 3.5 in local mode (`local[*]`, `driver_memory` 10g, 8 shuffle partitions, the
  settings of `spark_naive.session_config`), delta-spark 3.3 (`spark_catalog` = `DeltaCatalog`),
  Iceberg runtime `org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.9.2` loaded through
  `spark.jars.packages` next to Delta, with both SQL extensions. No pip dependency was added.
- **Tables**: Delta is a path table (`delta.\`<work_dir>/delta/events\``). Iceberg lives in a
  `hadoop` catalog (`org.apache.iceberg.spark.SparkCatalog`, `type=hadoop`,
  `warehouse=<work_dir>/iceberg`, `cache-enabled=false`) registered at run time with
  `spark.conf.set`, as `ice_<hash of the work dir>.db.events`. Both tables are **unpartitioned**,
  written with the **default settings** of each format (Iceberg format version, file size,
  compression and copy-on-write MERGE are whatever the library defaults to).
- **Queries**: `benchmarks/queries_spark/q1.sql` .. `q6.sql` verbatim, on a `raw_events` view over
  the table and the `repos` seed view.

## What one run does

Each run is a fresh child process (own JVM), so one run cannot warm up the next. In order:

| Step | Timed as | What |
|---|---|---|
| start Spark | `startup_seconds` (notes) | build the session, `spark.range(1).count()` |
| load | `load_seconds` (notes) | read the JSON of the base files and of the source files, `cache()` + `count()`, dedup the source |
| write | `write_seconds` (notes) | create the table from the cached base DataFrame |
| Q1..Q6 | `qN_seconds` | each query with `collect()`; `repos` is cached before the clock starts |
| MERGE | `merge_seconds` (notes) | `MERGE INTO t USING s ON t.id = s.id WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *` |
| MERGE again | `merge_rerun_seconds` (notes) | the very same statement; the row count must not change |
| time travel | `time_travel_seconds` (notes) | read the version written by "write" and `count()` it |

- The version before MERGE is taken right after the queries: Delta = latest version of
  `DeltaTable.history(1)`, Iceberg = latest `snapshot_id` of the `snapshots` metadata table.
  Time travel reads it with `versionAsOf` (Delta) or `VERSION AS OF <snapshot id>` (Iceberg).
- Untimed checks inside the run, each one fails the run: the rerun of MERGE leaves the row count
  unchanged (idempotent), and the old version holds exactly the rows of the table before MERGE.
- `total_seconds` = write + Q1..Q6 + merge + merge rerun + time travel. It does **not** include
  starting the JVM, loading the JSON, or the untimed counts between the steps.
- `table_bytes` / `table_files` are measured after the write and before MERGE: the `*.parquet`
  data files under the table directory (Delta's `_delta_log` excluded).
- `peak_rss_mb`: peak RSS of the Python driver plus the JVM, measured after Spark stopped
  (`spark_naive.stop_and_measure`), the same measure as the `spark-Nd` rows. Linux only.
- The table directory of a run is deleted before and after it; at most one table is on disk.

## Checks before a row is written

- Each child prints one JSON line; `parse_run_output` rejects missing keys, non-JSON output and
  negative / NaN / infinite / boolean / non-numeric values.
- Within a format, `row_count`, `bytes_read`, `base_rows`, `merge_source_rows` and the row count
  of every query must be equal across runs.
- Across formats (`cross_check`): `base_rows`, `row_count`, `merge_source_rows` and the row count
  of every query must be equal. If they differ, **neither row is written** and the exit code is
  non-zero; the error names every key that differs.
- A format that fails (crash, timeout after 30 minutes per run) does not stop the other one; the
  other row is written with `cross_check=none`, and the exit code is non-zero.
- The Spark tests (`tests/test_format_bench_spark.py`, on the 34-line fixture) check that both
  formats hold the same rows after writing, give exactly the Q1-Q6 results (columns and rows) of
  the raw files, give the same table after MERGE, that MERGE run two more times changes nothing,
  and that time travel returns the rows and the Q1-Q6 results from before MERGE.

## Rows

`data_date` = the reference day. The row of a format holds the median of its runs
(`benchmarks/measure.py::summarize_runs`).

| Column | Meaning |
|---|---|
| `total_seconds` | write + Q1..Q6 + merge + merge rerun + time travel (see above) |
| `qN_seconds` | query N on the table, before MERGE |
| `peak_rss_mb` | peak RSS of driver + JVM |
| `bytes_read` | size of the input `*.json.gz` files (all 24), as for the `spark-Nd` rows |
| `row_count` | rows of the table after both MERGEs |

`notes` is a space-separated list of `key=value` (no commas, no line breaks):

| Key | Meaning |
|---|---|
| `format` | `delta` or `iceberg` |
| `runs`, `stat` | number of runs, `median` |
| `write_seconds`, `merge_seconds`, `merge_rerun_seconds`, `time_travel_seconds` | median of each step (rounded to ms) |
| `load_seconds`, `startup_seconds` | median of loading the JSON and of starting Spark, not in `total_seconds` |
| `base_rows` | rows of the table after the write (= raw lines of hours 0-22) |
| `merge_source_rows` | rows of the MERGE source after deduplication |
| `table_bytes`, `table_files` | median size and number of the data files after the write |
| `spark`, `delta`, `iceberg` | library versions that produced the row |
| `cross_check` | `full` (checked against the other format) or `none` |

## Limits, stated plainly

- **One day of real data, one runner, local mode.** A single 4-CPU GitHub-hosted runner, Spark
  local mode, no cluster and no object store: file listing and commit costs of a real lake are not
  in these numbers.
- **Unpartitioned tables, default settings.** No partitioning, no Z-order / sort order, no
  compaction, no deletion vectors or merge-on-read tuning. Each format is measured as it comes.
- **MERGE shape**: one upsert of two hours into 23, on a key with no layout help. Other shapes
  (small upserts, deletes, partitioned targets) can rank the formats differently.
- **Iceberg catalog**: a `hadoop` catalog on the local file system, not a REST / Hive / Glue
  catalog.
- **Not comparable with `baseline` or `spark-Nd`.** `total_seconds` here excludes loading the JSON
  and includes writing, merging and time travel; `baseline` (DuckDB) and `spark-Nd` include the
  load and nothing else. Compare `format-delta` with `format-iceberg` only, column by column and
  with the `notes` keys.

## How to run

On GitHub: workflow **Format benchmark (Delta vs Iceberg)**, `workflow_dispatch` or any PR into
`develop`/`main` that touches the workflow, `benchmarks/format_bench.py`,
`benchmarks/spark_naive.py`, `benchmarks/queries_spark/**` or `benchmarks/measure.py`. Not a
required check. Steps: Python 3.11, Java 17, `pip install -e ".[dev,spark]"`, restore the
reference day (cache shared with the Benchmark workflow), download and verify it, run
`format_bench.py --runs 5`, job summary, upload.

Locally (needs Java 17; on Windows also `winutils`):

```bash
python benchmarks/fetch_reference.py
python benchmarks/format_bench.py --runs 5 --git-sha <sha> --out out/format-rows.csv --summary out/format-summary.md
```

Options: `--data-dir` (default `data/raw/gharchive/<data_date>`), `--reference`, `--seed`,
`--work-dir` (temporary tables, default `data/tmp/format-bench`, git-ignored). The script writes
only `--out` and `--summary`, never `runs.csv`.

## Recording the results

The workflow uploads the artifact **`format-bench`**: `format-rows.csv` (the `runs.csv` header plus
up to 2 rows) and `format-summary.md`. Merge the rows into `runs.csv` (schema-checked), then
regenerate the README results:

```bash
gh run download <run-id> -n format-bench -D out/format-bench
python benchmarks/record_run.py out/format-bench/format-rows.csv
python -m repopulse.readme_results
```
