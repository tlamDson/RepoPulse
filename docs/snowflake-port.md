# Porting the dbt project to Snowflake (PR 13)

Run the **same** dbt project `transform/` on the Snowflake trial and compare it with the primary one (DuckDB, target
`ci`) using the same `dbt build` command on the same input. The M7 measurements (`docs/plan.md`): number of lines that had to be
changed and build time. The input of both builds is the seed `fixture_silver_gharchive_events`:
**CI fixture, not measurements on real data**.

## Running

Workflow `.github/workflows/snowflake-port.yml` (run manually):

- `workflow_dispatch` (once the file is on `main`), or `push` to exactly the branch
  `feature/snowflake-port` (so it can run within the PR, before release). There is no `pull_request`;
  Snowflake is only written to in this workflow.
- Step 1: `dbt build --target ci` (DuckDB), then build the `dbt-build-duckdb` row.
- Step 2 (step-level gate, only when the secret `SNOWFLAKE_ACCOUNT` exists): `dbt debug --target snowflake`,
  `dbt build --target snowflake`, build the `dbt-build-snowflake` row. If the secret is missing there is a notice,
  the workflow stays green, and only the DuckDB row exists.
- Artifact `snowflake-port`: `out/snowflake-port-rows.csv` (`runs.csv` format) and
  `out/snowflake-port-summary.md` (also written to the Job summary).

Do not run the `snowflake` target on a local machine (no credentials; the command is in the `ask` group).

### Secrets

| Secret | Content |
|---|---|
| `SNOWFLAKE_ACCOUNT` | account identifier |
| `SNOWFLAKE_USER` | `REPOPULSE_CI` (SERVICE user) |
| `SNOWFLAKE_PRIVATE_KEY` | PKCS#8 PEM without a passphrase (key-pair login, no password) |
| `SNOWFLAKE_ROLE` | role of the CI user |
| `SNOWFLAKE_WAREHOUSE` | `REPOPULSE_WH` (XSMALL, auto-suspend 60s) |
| `SNOWFLAKE_DATABASE` | `REPOPULSE` |

The `snowflake` profile in `transform/profiles.yml` reads all 6 values with `env_var(...)`; the schema is fixed
as `repopulse` (dbt combines it into `repopulse_staging`, `repopulse_intermediate`, `repopulse_gold`).
Install the adapter with its own extra: `pip install -e ".[dbt,snowflake]"` (the CI `dbt` extra is unchanged).

### Merging into `runs.csv`

Download the `snowflake-port` artifact, then:

```bash
python benchmarks/record_run.py out/snowflake-port-rows.csv
```

## The `runs.csv` rows

`benchmarks/snowflake_port.py` builds one row from `transform/target/run_results.json`, right after each
build (the later build overwrites this file; the script checks `args.target` so it does not pick up the wrong result).

| Column | Value |
|---|---|
| `variant` | `dbt-build-duckdb` / `dbt-build-snowflake` |
| `total_seconds` | `elapsed_time` of `run_results.json` (the whole `dbt build`: seed, model, test, unit test) |
| `q1_seconds` … `q6_seconds` | `execution_time` of the matching mart: `mart_daily_active_contributors`, `mart_cohort_retention`, `mart_contribution_funnel`, `mart_responsiveness`, `mart_repo_concentration`, `mart_repo_health_score` (model build time, not the Q query on real data) |
| `row_count` | number of rows in the fixture seed |
| `data_date` | smallest `event_date` in the fixture seed (a convention; the fixture is not a GH Archive day) |
| `notes` | `engine=…; fixture=seed; lines_changed=<n>; q_seconds=mart build time; total=dbt build elapsed_time; base=<sha>` |

If a build has a node with `error`/`fail`/`runtime error`/`skipped`, no row is written; the script exits non-zero
and writes the failing nodes into the summary. The dbt log (including Snowflake SQL errors) is in the job log.

### Lines changed

`base_sha` in `benchmarks/snowflake_port.toml` is the head of `develop` before the port. Lines changed =
the sum of the added + deleted columns of `git diff --numstat <base_sha> HEAD -- transform` (binary files skipped). The
figure covers every change under `transform/` (model, test, macro, profile), not pytest tests, scripts
or workflows. The checkout in the workflow uses `fetch-depth: 0` to have the base commit.

## Syntax changed

Prefer standard SQL that runs on both engines; only where no common form exists does it go through an
`adapter.dispatch` macro in `transform/macros/cross_db.sql` (each macro has `duckdb__` and
`snowflake__` variants). The test `tests/test_dbt_portability.py` blocks DuckDB-only syntax outside `macros/`.

| DuckDB (before) | After (both engines) | Where |
|---|---|---|
| `count(*) filter (where c)` | `count(case when c then 1 end)` | Q1, Q5, Q6, `assert_dim_user_scd2_intervals` (including the window form) |
| `count(distinct x) filter (where c)` | `count(distinct case when c then x end)` | Q3 |
| `sum(n) filter (where c)` | `sum(case when c then n end)` | Q5 |
| `arg_min(x, y)` | `min_by(x, y)` (DuckDB has an alias) | Q3, Q4, Q6 |
| `quantile_cont(x, q)` | `percentile_cont(q) within group (order by x)` | Q3, Q4 |
| `(epoch(b) - epoch(a)) / 3600` | macro `hours_between(a, b)`: DuckDB keeps the expression, Snowflake `datediff(microsecond, a, b) / 3.6e9` | Q3, Q4 |
| `x::double` | `cast(x as double)` | Q2, Q4, Q5, Q6 |
| `bool_or(c)` | `max(case when c then 1 else 0 end) = 1` | Q6 |
| `ends_with(s, '[bot]')` | `right(s, 5) = '[bot]'` | `dim_user` |
| `cast(x as varchar[])` | macro `cast_string_array(x)`: Snowflake `cast(try_parse_json(...) as array)` | staging |
| `make_timestamp(epoch_us(x))` | macro `utc_timestamp(x)`: DuckDB keeps the expression (UTC time of a `TIMESTAMPTZ` from parquet), Snowflake `cast(x as timestamp)` (the source is always a seed) | staging |
| `read_parquet('<glob>')` | macro `silver_parquet_relation(glob)`: Snowflake raises a compile error (reading parquet needs a stage, out of scope) | staging |
| alias `merge`, `close` | `merge_rate`, `close_rate` (the mart's output columns are unchanged) | Q6 |

Results on DuckDB are unchanged: `tests/test_dbt_marts.py` still matches the 6 marts against
`benchmarks/queries/q*.sql`.

After this port, the Databricks target (D2, `docs/dbt-databricks.md`) changed a few more things shared by
all three engines: `varchar` became `string` (model, contract, seed), `cohort_week + n` became the macro
`add_days`, and `mask_pii`/`hashed_pii`/`no_raw_email` go through the macros `sha256_hex`, `trim_chars`,
`regex_full_match`, `regex_contains`, `string_array_elements` (their `snowflake__` branches are
`sha2(x, 256)`, `trim(x, chars)`, `regexp_like`, `regexp_instr`, `lateral flatten`). The new
`snowflake__` branches have not yet run on Snowflake (the `snowflake-port.yml` workflow is run manually).

Assumptions of the port:

- `date_trunc('week', …)` on Snowflake follows the `WEEK_START` parameter; the default (0) is a week starting on
  Monday like DuckDB. If the account changes this parameter, `event_week` shifts.
- The `timestamp` type on Snowflake is `TIMESTAMP_NTZ` (per the default `TIMESTAMP_TYPE_MAPPING`).
- The contract (`data_type` `varchar`, `bigint`, `double`, `date`, `timestamp`, `boolean`) uses type
  names both engines understand; dbt compares types using the engine itself, so aliases are normalized.

## Results

The workflow run on the branch `feature/snowflake-port` (commit `ee38cf0`) built green on both
engines, with the same seeds, models, data tests and unit tests. The two rows of that run were merged into
`runs.csv`: `variant=dbt-build-duckdb` and `variant=dbt-build-snowflake`, with the same `git_sha`. Build
time is in the `total_seconds` column, per-mart time in `q1_seconds`…`q6_seconds`, lines changed in
`lines_changed=` in `notes`. This document does not copy the numbers: read them in `runs.csv`.

This is a build on the CI fixture (seed), not measurements on real data, so it only compares the
fixed cost of one `dbt build` between the two engines and says nothing about speed at large volume.
