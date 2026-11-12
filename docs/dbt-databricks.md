# dbt on a Databricks SQL warehouse (D2)

Task D2 of Plan v2 (`docs/plan.md`): run the **same** dbt project `transform/` on a Databricks Free Edition SQL
warehouse (`dbt build --target databricks`), reading the silver Delta table that D1's
bronze/silver job writes into Unity Catalog (`docs/databricks-job.md`). The input is real GH Archive
(not simulated, not the CI seed). The main path of the current deployment is still the fallback (runner + MotherDuck,
`docs/probe-result.md`); this is a parallel gold build for comparison (D3).

## Running

Workflow `.github/workflows/dbt-databricks.yml` (run manually):

- `workflow_dispatch` (once the file is on `main`), or `push` to exactly the branch
  `feature/dbt-databricks-target` (filtered by `paths`), so it can run within the PR. There is no
  `pull_request` and no schedule: Free Edition has a daily quota.
- Path B of `.github/workflows/path-comparison.yml` (D3, `docs/path-comparison.md`) runs the same
  `dbt build --target databricks` with the same `DBT_DATABRICKS_SCHEMA=repopulse_dbt` on an hourly schedule
  (after release), so the `repopulse_dbt_*` schemas are also written from there. Staging only inserts new `event_id`s,
  so the two workflows writing alternately produce no duplicate rows.
- Missing `DATABRICKS_HOST` or `DATABRICKS_TOKEN`: the Databricks steps are skipped at step level, with a
  `::notice`, the workflow stays green, and there is no `runs.csv` row.
- The steps:
  1. `python -m repopulse.probe http-path --github-env`: the warehouse http path = the secret
     `DATABRICKS_HTTP_PATH`, or if empty the first warehouse from `GET /api/2.0/sql/warehouses` (same
     function `resolve_warehouse` as the probe's SQL warehouse check). Writes `DBT_DATABRICKS_HTTP_PATH` to
     `$GITHUB_ENV`, and `::add-mask::`s the hostname, http path and warehouse id. A stopped warehouse starts
     automatically when dbt connects.
  2. `dbt debug --target databricks`.
  3. `dbt build --target databricks`: seeds, models, data tests, unit tests; staging reads
     `workspace.repopulse.silver_gharchive_events`.
  4. `dbt show --inline … --output json --log-format json --target-path target/show`: row count and
     `event_date` range of `stg_gharchive_events` after the build (a separate `--target-path` to keep the build's
     `run_results.json`). The log stays on the runner, not in the artifact.
  5. `python benchmarks/dbt_databricks.py` builds the `variant=dbt-build-databricks` row
     (`if: always()`: a broken build still writes the failing nodes into the summary, but no row).
  6. Job summary and the artifact `dbt-databricks` (`out/dbt-databricks-rows.csv`,
     `out/dbt-databricks-summary.md`).

Do not run the `databricks` target on a local machine (no credentials, and `dbt-databricks` is not installed
in the `dbt` extra). Install the adapter with its own extra: `pip install -e ".[dbt,databricks]"`.

### Secrets and variables

| Name | Kind | Used for |
|---|---|---|
| `DATABRICKS_HOST` | secret | workspace URL; if empty, skip |
| `DATABRICKS_TOKEN` | secret | personal access token; if empty, skip |
| `DATABRICKS_HTTP_PATH` | secret, optional | `/sql/1.0/warehouses/<id>`; if empty, found automatically through the API |
| `PII_HASH_SALT` | secret | `mask_pii` of the public schema (a non-`ci` target with no salt gives a compile error) |
| `DBT_DATABRICKS_SCHEMA` | workflow variable (`repopulse_dbt`) | base dbt schema; empty/unset means `repopulse_probe` (keeping the probe's `dbt debug` behavior) |

Secrets enter only through `env:`; no step prints the values.

### Schemas

dbt combines `<target schema>_<custom schema>` in the `workspace` catalog: seeds in `repopulse_dbt`,
models in `repopulse_dbt_staging`, `repopulse_dbt_intermediate`, `repopulse_dbt_gold`,
`repopulse_dbt_public`. Nothing is written to `workspace.repopulse` (D1's bronze/silver/quarantine);
the test `tests/test_dbt_databricks_workflow.py` blocks setting a colliding schema.

## Staging source

| Var | Default | Meaning |
|---|---|---|
| `silver_gharchive_source` | `unity_catalog` when `target.type == 'databricks'`, `seed` on every other target | `unity_catalog` → dbt source `silver.silver_gharchive_events`; `seed` → seed `fixture_silver_gharchive_events` |
| `silver_gharchive_catalog` / `_schema` / `_table` | `workspace` / `repopulse` / `silver_gharchive_events` | location of the silver table (matches `repopulse/spark/tables.py`) |
| `silver_gharchive_parquet` | unset | DuckDB only (`read_parquet`); on Databricks it gives a compile error pointing at the Unity Catalog source |

On DuckDB the behavior is unchanged: seed by default, or parquet when the var is set. To build the CI seed on
Databricks (to compare the fixture): `--vars "{silver_gharchive_source: seed}"`.

The seed stores the two commit-hash array columns as text (empty in the fixture); Databricks cannot cast a
string to an array, so staging passes `from_text` to `cast_string_array` when the source is the seed
(`from_json(…, 'array<string>')`), while the Delta table with a real `array<string>` uses `cast(… as
array<string>)`.

**Idempotent:** staging is an incremental `append` that only inserts `event_id`s not yet present (`not exists`), so
re-running the workflow on the same silver adds no rows; `fct_events` uses `merge` by `event_id` on
Databricks (see below). The workflow never uses `--full-refresh`.

## Syntax changed

Like the Snowflake port (`docs/snowflake-port.md`): standard SQL in models; where no common form exists it goes through
an `adapter.dispatch` macro in `transform/macros/cross_db.sql`, each macro having `duckdb__`,
`snowflake__` and `databricks__` variants. `tests/test_dbt_databricks.py` blocks syntax Databricks does not understand
outside the engine branch, renders each `databricks__` branch with Jinja, and where it can run, runs it on DuckDB
next to the `duckdb__` branch (a Databricks-only function is replaced by a DuckDB macro with the same meaning).

| Before | After (DuckDB, Snowflake, Databricks) | Databricks | Where |
|---|---|---|---|
| `cast(x as varchar)`, contract/seed `varchar` | `cast(x as string)`, `data_type: string` | `varchar` without a length is rejected; `string` is an alias of `VARCHAR` on DuckDB and Snowflake | every model, `_gold.yml`, `_marts.yml`, `seeds/schema.yml` |
| macro `hours_between` | add `databricks__` | `cast(unix_micros(b) - unix_micros(a) as double) / 3600000000` (exact to the microsecond) | Q3, Q4 |
| macro `utc_timestamp` | add `databricks__` | `cast(x as timestamp)` | staging |
| macro `cast_string_array` | add parameter `from_text` | `from_json(cast(x as string), 'array<string>')` (seed) / `cast(x as array<string>)` (Delta) | staging |
| macro `silver_parquet_relation` | add `databricks__` | compile error, pointing at the Unity Catalog source | staging |
| `cohort_week + 28` | macro `add_days(d, n)` | `date_add(d, n)` | Q2 |
| `sha256(x)` | macro `sha256_hex(x)` | `sha2(x, 256)` (Snowflake too) | `mask_pii` |
| `trim(x, chars)` | macro `trim_chars(x, chars)` | `btrim(x, chars)` (older Spark's two-argument `trim(a, b)` reads the order the other way round) | `mask_pii` |
| `regexp_full_match(x, p)` | macro `regex_full_match(x, p)` | `x rlike '^(?:p)\z'` (backslash doubled in the literal) | `hashed_pii` |
| `regexp_matches(x, p)` | macro `regex_contains(x, p)` | `x rlike 'p'` (backslash doubled) | `no_raw_email` |
| `unnest(arr)` | macro `string_array_elements(rel, col)` | `select explode(col) as element …` | `hashed_pii` |
| `fct_events` `delete+insert` | `merge` when `target.type == 'databricks'` | the batch has one row per `event_id`, so upsert gives the same table as delete+insert | `fct_events` |

Kept as is because Databricks SQL understands them like DuckDB: `qualify`, `min_by`, `any_value`,
`percentile_cont(p) within group (order by …)`, `count(distinct case …)`, `is distinct from`,
`right()`, `date_trunc('week', …)` (Monday), `union`, `using (…)`, inline `values … as t (…)`
in unit tests.

## Same results on two adapters

The dbt `unit_tests:` in `_gold.yml` (SCD2 `dim_user`, `fct_events`) and `_marts.yml` (6 marts Q1–Q6)
have fixed `given`/`expect` and run in the `dbt build` of both targets: the CI job `dbt build (DuckDB)`
and this workflow. The same `expect` being green on both is evidence that the marts give the same result on the fixture on
the two adapters. Matching the marts against `benchmarks/queries/q*.sql` on data (`tests/test_dbt_marts.py`)
still runs on DuckDB only.

## The `runs.csv` row

`benchmarks/dbt_databricks.py` (shares the `run_results.json` reading core with
`benchmarks/snowflake_port.py`):

| Column | Value |
|---|---|
| `variant` | `dbt-build-databricks` |
| `total_seconds` | `elapsed_time` of `run_results.json` (the whole `dbt build`) |
| `q1_seconds` … `q6_seconds` | `execution_time` of the matching mart (model build time) |
| `row_count` | row count of `stg_gharchive_events` after the build (from `dbt show`) |
| `data_date` | smallest `event_date` of staging |
| `notes` | `engine=databricks; source=<silver table>; input=gharchive-silver-delta; max_event_date=…; lines_changed=<n>; q_seconds=mart build time; total=dbt build elapsed_time; base=<sha>` |

`lines_changed` = the sum of added + deleted of `git diff --numstat <base_sha> HEAD -- transform`, with `base_sha`
in `benchmarks/dbt_databricks.toml` (the head of `develop` before D2). The script rejects a `run_results.json` of a
target other than `databricks`, of a command other than `build`, or with a node `error`/`fail`/`runtime error`/
`skipped`.

The README has its own section "dbt build on a Databricks SQL warehouse (real GH Archive data)"
(`python -m repopulse.readme_results`), shown only when `runs.csv` has this row; it is not mixed with the
DuckDB/Snowflake seed fixture section.

Merge into `runs.csv` after downloading the `dbt-databricks` artifact:

```bash
python benchmarks/record_run.py out/dbt-databricks-rows.csv
python -m repopulse.readme_results
```

This document does not copy numbers; measurements are read only from `runs.csv`.

## Results

The workflow run on the branch `feature/dbt-databricks-target` (commit `4553df1`,
[run 43166502216](https://github.com/tlamDson/RepoPulse/actions/runs/43166502216)) built green on the
SQL warehouse: the same seeds, models, data tests and unit tests as target `ci`, with no node failed or
skipped. The input is D1's silver Delta (real GH Archive, not simulated). The row from that
run was merged into `runs.csv` with `variant=dbt-build-databricks`.

This is a single build, not a speed comparison with DuckDB/Snowflake (the other two rows build from the seed
fixture). Comparing the two paths end to end is task D3.

## Assumptions and limits

- **The session time zone is UTC.** Databricks `timestamp` is an instant displayed in the session time zone;
  the SQL warehouse default is UTC. If the workspace changes `TIMEZONE`, literals (`loaded_at`, seeds,
  unit tests) are interpreted in another time zone and `event_week` may shift.
- `date_trunc('week', …)` on Databricks always goes to Monday.
- Spark's `round(x, 2)` rounds HALF_UP while DuckDB rounds away from zero: identical for positive numbers; the
  edge case of exactly `.xx5` on a double may differ in the last digit (the unit tests avoid this case).
- CI has no `dbt-databricks`: the SQL of the `databricks__` branch is only truly checked when the workflow runs;
  Databricks SQL errors are in the job log, and the build result in the Job summary and `runs.csv`.
