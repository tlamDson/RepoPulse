# Engine matrix: Q1-Q6 on six engines

Task E1 in [`plan.md`](plan.md) (Plan v2): the same questions Q1-Q6 on the same day of data on
DuckDB, Spark on the GitHub runner, Databricks serverless, a Databricks SQL warehouse,
MotherDuck and Snowflake, shown in one README table. Script
[`benchmarks/engine_matrix.py`](../benchmarks/engine_matrix.py), parser
[`repopulse/engine_matrix.py`](../repopulse/engine_matrix.py), README table
[`repopulse/readme_results.py`](../repopulse/readme_results.py), workflow
[`.github/workflows/engine-matrix.yml`](../.github/workflows/engine-matrix.yml).

**Numbers live only in `runs.csv`.** This page describes how they are produced; it quotes none.

## Data and queries

- **Data:** the reference day of [`benchmarks/reference.toml`](../benchmarks/reference.toml),
  real GH Archive data (not simulated), the same `data_date` as `variant=baseline`. The files
  are checksum-verified before any engine starts; a mismatch stops the call with exit code 2.
- **Tables:** `raw_events` with the columns of the baseline (`payload` kept as JSON text) and
  the seed `repos` table from `transform/seeds/repos_data_ecosystem.csv`.
- **Queries:** DuckDB and MotherDuck run [`benchmarks/queries/`](../benchmarks/queries/); Spark
  and both Databricks engines run the Spark SQL translation
  [`benchmarks/queries_spark/`](../benchmarks/queries_spark/), unchanged. If a Databricks engine
  could not run a query as written, the change would be recorded here and in the PR; none is
  made in this task.

## Method (the same on every engine)

1. **Load** the day into the engine (not timed; `load_seconds` in `notes`).
2. **Warm-up:** Q1-Q6 once. Its results are the ones cross-checked.
3. **Timed runs:** Q1-Q6 `--runs` times (5 in the workflow), each query timed separately.
4. **Cross-check against DuckDB:** in the same call, `naive_duckdb.run_once` computes Q1-Q6 on the
   same files. The engine must return the same `raw_events` row count, and for every query the
   same column names and the same multiset of rows (dates compared as ISO text, floats within
   1e-6). Every timed run must also return as many rows per query as the warm-up. Any difference
   makes the measurement invalid: **no row is written** and the call exits 1. `cross_check=content`
   in `notes` records that the full content matched.

Each call measures one engine and appends one row to `--out`, so an engine that fails never
costs the row of another one.

## Columns of a `matrix-<engine>` row

| Column | Meaning |
|---|---|
| `variant` | `matrix-<engine>` |
| `data_date` | `data_date` of the reference day |
| `q1_seconds` ... `q6_seconds` | median over the timed runs of each query |
| `total_seconds` | median over the timed runs of the per-run sum of Q1-Q6 (`total=median_run_sum_q1_q6` in `notes`). Loading and the warm-up are excluded on every engine, so unlike `baseline` it does not include loading |
| `row_count` | rows of `raw_events` as the engine counted them (equal to DuckDB's, or no row) |
| `peak_rss_mb`, `bytes_read` | empty: not measured the same way on every engine, so not reported |
| `notes` | `key=value` words: `engine`, `runs`, `stat=median`, `warmup=1`, `timing`, `total`, `load_seconds`, `cross_check`, the engine or client version, and the engine-specific keys below; `run_id`/`trigger` of the workflow run |

`timing` says where the clock runs:

- `in-process`: in the process that runs the query (`duckdb`; `spark` in its own child process,
  around `spark.sql(...).collect()`).
- `client`: on the runner, from sending the query to receiving the last result row, so the time
  includes the network (`motherduck`, `databricks-sql`).
- `notebook`: inside the Databricks notebook, around `spark.sql(...).collect()`
  (`databricks-serverless`); job start-up and the network are outside.

## Engines

| Engine | Where it runs | Load | Engine-specific `notes` |
|---|---|---|---|
| `duckdb` | in-process, in memory | `CREATE OR REPLACE TABLE ... read_json` | `duckdb=<version>` |
| `spark` | PySpark `local[*]` in a fresh child process on the runner (Java 17, extra `spark`) | `spark.read.json` + `cache()` + `count()`, as `spark_naive` | `spark`, `master`, `startup_seconds` |
| `motherduck` | DuckDB client on `md:`, tables in MotherDuck | `CREATE OR REPLACE TABLE` in database `repopulse_matrix` from the local files | `duckdb` (client), `md_database`, `md_database_type`, `result_cache` |
| `databricks-sql` | SQL Statement Execution API on the SQL warehouse | Files API to the Volume, `CREATE OR REPLACE TABLE ... read_files` in `workspace.repopulse_matrix` | `dbsql` (when the warehouse reports it), `schema`, `warehouse_size`, `warehouse_type`, `serverless`, `result_cache`, `cache_check` |
| `databricks-serverless` | notebook submitted as a one-off serverless run (Jobs API, no cluster declared) | same Volume files, `CREATE OR REPLACE TABLE ... read_files` in the notebook | `spark` (of the serverless runtime), `schema`, `result_cache`, `job_*_s` |
| `snowflake` | not measured | - | - |

Details:

- **DuckDB client version.** MotherDuck rejects a DuckDB client newer than it supports, so the
  workflow pins `duckdb==1.5.6` (as `pipeline.yml`). `matrix-duckdb` runs in the same job with
  the same client; the version is in `notes`.
- **MotherDuck.** The token is read by the DuckDB client from `MOTHERDUCK_TOKEN`; it is never put
  in the connection string or printed. The tables live in the MotherDuck database
  `repopulse_matrix`, so the queries run on MotherDuck; the script refuses `repopulse` (gold) and
  `repopulse_smoke`. `CREATE OR REPLACE` makes a re-run replace the tables, never append.
- **Databricks SQL warehouse.** The warehouse is `DATABRICKS_HTTP_PATH`, or the first warehouse
  listed by the API (`repopulse.probe.resolve_warehouse`, as `dbt-databricks.yml`). Files go to the
  Volume `workspace.repopulse_matrix.repopulse_matrix_data` (`overwrite=true`). The script refuses
  the schemas of the other Databricks jobs (`repopulse`, `repopulse_dbt*`, `repopulse_probe`).
- **Result cache (Databricks SQL).** The warehouse can answer a repeated query from its result
  cache, which would make every timed run after the first one meaningless. The SQL Statement
  Execution API has no session settings (no `SET use_cached_result = false`), so every statement
  gets a unique trailing comment (`-- repopulse engine-matrix <nonce> <query> <run>`); the text
  of no two statements is the same. After the runs, the query history
  (`GET /api/2.0/sql/history/queries`, `include_metrics=true`) is read for the timed statements:
  if any reports `metrics.result_from_cache = true` the measurement is invalid and no row is
  written. `cache_check=query_history` means every timed statement was found and none came from
  the cache; `unavailable` or `incomplete` means the history could not confirm it (the unique
  comments still apply). MotherDuck and the serverless notebook get the same unique comments
  (`result_cache=unique_comment`); neither is known to cache query results, and no cache check is
  made for them.
- **Databricks serverless.** The notebook embeds the Spark SQL of Q1-Q6 and the load
  statements, gets `runs`, a nonce, catalog and schema as job parameters, and returns JSON (times
  per query and run, row count, load time, Spark version, warm-up results) through
  `dbutils.notebook.exit`. Serverless compute cannot `cache()` a DataFrame, so the day is loaded
  into Delta tables instead of a cached view (the Spark engine on the runner caches it).
- **Snowflake.** The plan says "lấy lại số đã có" and the trial has 30 days of credits, so
  `--engine snowflake` exits 2 and no workflow step measures it. The README reuses the newest
  `dbt-build-snowflake` row ([`snowflake-port.md`](snowflake-port.md)) and labels it reused and not
  comparable: it is the build time of the marts with `dbt build` on the seed fixture, not Q1-Q6
  on the reference day. A `matrix-snowflake` row, if one is ever recorded, wins over it.

## Limits of the comparison

- Local and remote engines are not timed the same way (`timing`): a `client` time includes the
  network round trips between the runner and the service, a `notebook` time does not include the
  job start-up. Compare rows with the same `timing` first.
- The cloud engines run on free tiers (Databricks Free Edition, MotherDuck Lite) whose hardware
  is not documented or controlled; the runner engines run on a 4 CPU GitHub runner.
- Warm data: every timed run follows a warm-up on the same tables, so the numbers are for warm
  caches of data (not of results).
- The `snowflake` row is a different measurement and is never comparable.

## README table

`python -m repopulse.readme_results` renders one table "Engine matrix" with one row per engine in
the fixed order `duckdb`, `spark`, `databricks-serverless`, `databricks-sql`, `motherduck`,
`snowflake` (the newest `matrix-<engine>` row by `run_at`). The `source` column says whether the
row was measured, reused (Snowflake) or is missing. An engine without a row keeps its line with
`—` and a warning line below the table names it; the command prints the same warnings to stderr
but does not change its exit code, so CI stays green while a cloud engine has no number yet. A
`matrix-<name>` row whose name is not one of the six engines is listed under "Other runs" with a
warning. `matrix-*` rows of the six engines are not repeated under "Other runs".

## Running and merging

```bash
python benchmarks/fetch_reference.py
python benchmarks/engine_matrix.py --engine duckdb --runs 5 --git-sha <sha> \
    --out out/engine-matrix-rows.csv --summary out/engine-matrix-summary.md
```

The cloud engines run only in the workflow (secrets `MOTHERDUCK_TOKEN`, `DATABRICKS_HOST`,
`DATABRICKS_TOKEN`, optional `DATABRICKS_HTTP_PATH`). A missing secret skips that engine at step
level with a notice. Every engine step has `continue-on-error`; the job summary lists each
outcome, and the last step fails the run when an engine failed. Merge the artifact:

```bash
python benchmarks/record_run.py <artifact>/engine-matrix-rows.csv
python -m repopulse.readme_results
```
