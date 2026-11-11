# Query optimization: partitioning and clustering, before and after

Measure M10 in [`plan.md`](plan.md) (PR 10): p50 query time and amount of data scanned for Q1-Q6,
10 runs, compared with the unpartitioned table (`variant=baseline`). Script
[`benchmarks/query_opt.py`](../benchmarks/query_opt.py), workflow
[`.github/workflows/query-opt.yml`](../.github/workflows/query-opt.yml).

**Numbers live only in `runs.csv`.** This page describes how they are produced; it quotes none.

## Data and queries

- **Real GH Archive data** (not simulated): the reference day of
  [`benchmarks/reference.toml`](../benchmarks/reference.toml), checksum-verified before anything
  runs (same check as `naive_duckdb.py`; a mismatch or an unlisted file stops with exit code 2 and
  writes nothing).
- **Queries:** `benchmarks/queries/q1.sql` .. `q6.sql`, verbatim, the same files as the baseline.
  Nothing in the queries is changed to suit a layout.
- The chosen path is the fallback (DuckDB / MotherDuck, no Databricks), so "partitioning" means a
  Hive-partitioned parquet directory and "clustering" means sorting the rows when the parquet is
  written, so that DuckDB can skip files (partition pruning) or row groups (min/max statistics).

## Layouts

All four layouts hold exactly the same rows (the raw table of the baseline, duplicates included),
written with DuckDB's default parquet settings (row group size, compression).

| `variant` | Partitioned by (Hive directories) | Order of rows inside each file |
|---|---|---|
| `qopt-flat` | none, one file | arrival order (the "before", measured on the same runner and commit) |
| `qopt-partitioned` | `type` (`type=<EventType>/data_0.parquet`) | arrival order |
| `qopt-clustered` | none, one file | `type`, `repo.name`, `created_at` |
| `qopt-partitioned-clustered` | `type` | `repo.name`, `created_at` |

`raw_events` is a view over the layout with the same columns, order and types as the baseline
table (`id`, `type`, `actor STRUCT(id, login)`, `repo STRUCT(id, name)`, `payload JSON`,
`created_at TIMESTAMP`). JSON and TIMESTAMP survive the round trip through parquet, so no cast is
needed; the Hive column `type` is read as `VARCHAR` (`hive_types`). `repos` is the seed table of
the baseline. A test checks that every layout returns exactly the baseline result (columns and
rows) for Q1-Q6.

Why these keys, and what they cannot do:

- **`type`**: every query filters on `e.type IN (...)` and none reads `PushEvent`, `CreateEvent`,
  `WatchEvent` and the other types the queries never name. Partitioning on `type` lets DuckDB skip
  whole files from the path alone (`Scanning Files: k/n` in the profile). Sorting on `type`
  (clustered) can only help when the row-group statistics let DuckDB skip a row group; on DuckDB
  1.4.1 the `IN` list is pushed into the parquet scan as an *optional* filter, and whether row
  groups are actually skipped is only visible in the measured `qN_scan_rows` / `qN_bytes` of the
  real day (the test fixture is a single row group, so the tests cannot show it).
- **`repo.name`, `created_at`**: Q1-Q6 join on `lower(e.repo.name) = lower(r.repo_name)` (an
  expression, not the column) and have no time predicate (the data is one day). So there is **no
  static predicate on `repo.name` or `created_at` for row-group statistics to use**: this sort
  cannot reduce what is read. Its only possible effects are on file size (`layout_bytes`, through
  encoding/compression) and on locality during joins and aggregations. It is kept because it is the
  natural clustering key for the marts and to show what it does and does not change.
- Row group size is DuckDB's default: no layout here has a measured reason to change it.
  Observed while writing the tests: a very small `ROW_GROUP_SIZE` (4) is not honoured, the
  34-line fixture still becomes a single row group.

## What is timed

- **Preparing the layouts is not timed into the query numbers.** The `*.json.gz` files are loaded
  once (`naive_duckdb.load`), then each layout is written with `COPY ... TO ... (FORMAT parquet)`
  (`PARTITION_BY (type), OVERWRITE` for partitioned layouts, so preparing again never duplicates
  rows). The duration of that layout's `COPY` alone is `prepare_seconds` in `notes`. After writing,
  untimed checks: the layout holds as many rows as the raw table, and every file of a clustered
  layout is in key order (row position = position after sorting by the key).
- **Timed runs:** each of the `--runs` (default 10) runs is a fresh child process with an in-memory
  DuckDB. The `repos` seed table is created first (not timed); then the clock starts:
  `total_seconds` = creating the `raw_events` view + Q1..Q6, and `qN_seconds` includes
  `fetchall()`. `row_count` (`count(*)` on the view) is taken after the clock stopped. DuckDB
  settings are the defaults, so inside one run a later query may profit from data cached by an
  earlier one; every layout runs the queries in the same order.
- `peak_rss_mb`: `ru_maxrss` of the run process (as the baseline).
- The row of a layout holds the median of its runs (`benchmarks/measure.py::summarize_runs`).
- **Not comparable with the baseline:** `total_seconds` of the baseline includes loading the
  `*.json.gz` files into a table; here the data is already in parquet and `total_seconds` covers
  only the view and the queries. Compare `qN_seconds`, and `total_seconds` between the `qopt-*`
  rows. `qopt-flat` is the "before" for the layout effects; `baseline` (measured again in the same
  workflow run) is the "before" for "parquet instead of an in-memory table loaded from JSON".

## Amount of data scanned

Measured in a separate, untimed scan pass (one more child process per layout), never estimated:

1. **Warm-up:** Q1..Q6 run once on a throwaway database so that anything loaded once per process
   is loaded before measuring.
2. For each query, a **new in-memory database** (its own buffer pool and caches) with
   `enable_external_file_cache = false`, the view and `repos` created, then:
   - **bytes**: `rchar` of `/proc/self/io` read right before `execute` and right after
     `fetchall()`. `qN_bytes` = after - before - size of the "before" sample itself (the kernel
     counts that read in the next sample). The `.sql` file and the seed CSV are read before the
     first sample.
   - **rows and files**: the same query under `EXPLAIN (ANALYZE, FORMAT JSON)` in another new
     database. For every `READ_PARQUET` scan operator of the plan: `operator_cardinality` (rows the
     scan emitted) and `extra_info["Total Files Read"]`, summed over the scans of the plan.

Because every query gets a new database and the external file cache is off, the numbers of one
query do not depend on which query ran before it. `rchar` counts bytes returned by the read system
calls of all threads of the process, whether served from the OS page cache or from disk, so it
does not depend on the page cache either. A test on Linux checks two scan passes give identical
bytes.

Limits, stated plainly:

- `rchar` is **bytes the process read from files**, not bytes read from disk (`read_bytes` depends
  on the page cache and is not used). It includes parquet footers/metadata and DuckDB re-binding
  the view (globbing, reading a schema) at query time. It does not include memory-mapped reads
  (DuckDB reads parquet with ordinary reads).
- `/proc/self/io` exists only on Linux. Elsewhere (Windows, macOS) `bytes_read` is **empty** and
  `notes` say `bytes=unavailable`; no substitute number is written. The workflow runs on
  `ubuntu-latest`.
- `qN_scan_rows` counts rows **emitted** by the parquet scan, after filters DuckDB evaluates inside
  the scan, not rows decoded. Skipped files and row groups lower it; so does a filter evaluated in
  the scan.
- Not used, on purpose: the profile's `operator_rows_scanned` (on DuckDB 1.4.1 it does not equal
  the rows of the files read, e.g. a multiple of it for multi-file scans) and `total_bytes_read`
  (0 for parquet: it counts DuckDB's own database file).

## Checks before a row is written

- Each child prints one JSON line; the parsers (`parse_run_output`, `parse_scan_output`) reject
  missing keys, non-JSON output, negative / NaN / infinite / boolean / non-numeric values, and bytes
  measured for only some queries.
- Within a layout, `row_count` and the row count of every query must be equal across runs.
- Across layouts: `row_count` must equal the raw table's row count, and `row_count` plus the row
  count of every query must equal those of `qopt-flat`. A layout that disagrees gets no row; the
  error names the layout and the count. If `qopt-flat` itself failed, the other rows are checked
  against the raw row count only and say `cross_check=row_count_only`.
- A failing layout (prepare error, crash, timeout after 30 minutes) does not stop the others; the
  error is in the output and the job summary, and the exit code is non-zero.

## Rows

`variant` = `qopt-flat`, `qopt-partitioned`, `qopt-clustered`, `qopt-partitioned-clustered`;
`data_date` = the reference day.

| Column | Meaning |
|---|---|
| `total_seconds`, `qN_seconds`, `peak_rss_mb` | median of the timed runs (see above) |
| `row_count` | rows of `raw_events` (= the raw table, duplicates included) |
| `bytes_read` | **sum of `q1_bytes` .. `q6_bytes`**: bytes read from files by Q1-Q6, one fresh database per query; empty when not measurable. Different from `bytes_read` of `baseline`, which is the size of the input `*.json.gz` files |

`notes` is a space-separated list of `key=value` (no commas, no line breaks):

| Key | Meaning |
|---|---|
| `runs` | number of timed runs |
| `stat` | `median` |
| `partition_by` | Hive partition column(s), or `none` |
| `sort_by` | sort key inside each file, joined with `+`, or `none` |
| `files` | parquet files of the layout |
| `layout_bytes` | total size of those files on disk |
| `prepare_seconds` | duration of writing this layout (`COPY`), not in any query number |
| `duckdb` | DuckDB version that produced the row |
| `bytes` | `proc_rchar` (bytes measured) or `unavailable` |
| `cross_check` | `full` (checked against `qopt-flat` and the raw table) or `row_count_only` |
| `qN_bytes` | bytes read by query N (only when `bytes=proc_rchar`) |
| `qN_scan_rows` | rows emitted by the parquet scan(s) of query N |
| `qN_files_read` | parquet files opened by query N |

## How to run

On GitHub: workflow **Query optimization**, `workflow_dispatch` or any PR into `develop`/`main`
that touches the workflow, `benchmarks/query_opt.py`, `benchmarks/queries/**`,
`benchmarks/naive_duckdb.py` or `benchmarks/measure.py`. Not a required check. Steps: restore the
reference day (cache shared with the Benchmark workflow), download and verify it, measure
`baseline` again (`naive_duckdb.py --runs 10`), run `query_opt.py --runs 10`, job summary, upload.

Locally (bytes are only measured on Linux):

```bash
python benchmarks/fetch_reference.py
python benchmarks/query_opt.py --runs 10 --git-sha <sha> --out out/query-opt-rows.csv --summary out/query-opt-summary.md
```

Options: `--data-dir` (default `data/raw/gharchive/<data_date>`), `--reference`, `--seed`,
`--work-dir` (parquet layouts, default `data/tmp/query-opt`, git-ignored).

## Recording the results

The workflow uploads the artifact **`query-opt`**: `baseline-row.csv` (one `baseline` row from the
same runner and commit), `query-opt-rows.csv` (the `runs.csv` header plus up to 4 rows) and
`query-opt-summary.md`. Merge the rows into `runs.csv` (schema-checked, re-recording is a no-op):

```bash
gh run download <run-id> -n query-opt -D out/query-opt
python benchmarks/record_run.py out/query-opt/baseline-row.csv out/query-opt/query-opt-rows.csv
```
