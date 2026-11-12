# Semantic layer (MetricFlow) for Q1–Q6

A layer of metric definitions over the 6 marts of the `public` schema (`main_public` on DuckDB/MotherDuck), so
tools read metrics by name instead of writing SQL themselves (task AI2: the SQL agent only reads `public`). The definitions
are in `transform/models/public/_semantic.yml`; the SQL-generating code is in `repopulse/semantic.py`; the tests are in
`tests/test_dbt_semantic_layer.py`. Every metric value is the mart's value, i.e. the result of
`benchmarks/queries/qN.sql`; this document contains no measurements (measurements come only from `runs.csv`).

## Spec and versions

- dbt-core 1.12 depends directly on `metricflow` (0.213 at time of writing); this package brings the semantic manifest
  validator (`metricflow_semantic_interfaces`) and the SQL generation engine. `dbt-metricflow`
  and the `mf` command are not needed.
- Uses the top-level `semantic_models:` spec (with `measures`) + `metrics:` with `type_params`. dbt
  1.12 also accepts the new spec that attaches a semantic model to a model (`semantic_model:`, `metrics:` inside the
  model), but we use the old spec: `dbt parse` on the installed version emits no deprecation warning, every dbt-core
  >= 1.8 that the workflows may install (MotherDuck pipeline, Snowflake port, Databricks probe) can
  parse it, and `_public.yml` (the file governance reads PII labels from) does not need to change.
- `dbt parse`/`dbt build` validate the semantic manifest (a validation error makes parse fail) and write
  `target/semantic_manifest.json`. MetricFlow reads that file through
  `parse_manifest_from_dbt_generated_manifest` (which turns measures into simple metrics on load).

## Semantic models

One semantic model per mart, one row per mart grain, its own primary entity (no joins between
marts). Metric name = mart column name. Group-by names follow the form
`<primary entity>__<dimension>`, with `__<granularity>` added for time dimensions.

| Semantic model | Mart (`ref`) | Primary entity | Grain = group-by |
|---|---|---|---|
| `q1_daily_active_contributors` | `pub_mart_daily_active_contributors` | `q1_activity` | `q1_activity__period`, `q1_activity__period_start__day`, `q1_activity__scope`, `q1_activity__scope_key` |
| `q2_cohort_retention` | `pub_mart_cohort_retention` | `q2_cohort` | `q2_cohort__cohort_week__week` |
| `q3_contribution_funnel` | `pub_mart_contribution_funnel` | `q3_funnel` | `q3_funnel__repo_name` (+ `q3_funnel__category`) |
| `q4_responsiveness` | `pub_mart_responsiveness` | `q4_responsiveness` | `q4_responsiveness__repo_name`, `q4_responsiveness__item_type` |
| `q5_repo_concentration` | `pub_mart_repo_concentration` | `q5_concentration` | `q5_concentration__repo_name` (+ `q5_concentration__category`) |
| `q6_repo_health_score` | `pub_mart_repo_health_score` | `q6_health` | `q6_health__repo_name` (+ `q6_health__category`) |

Semantic models only `ref` `pub_mart_*` models (schema `public`) and do not point at gold/staging/intermediate;
the test checks both `node_relation.schema_name` and `depends_on` in `manifest.json`.

## Metrics per Q

| Q | Additive (simple, `sum`) | Ratios (derived, from additive measures) | Not additive (correct at the mart grain) |
|---|---|---|---|
| Q1 | — | — | `active_contributors`, `new_contributors`, `returning_contributors` |
| Q2 | `cohort_size`, `retained_w4`, `retained_w12` | `retention_w4_pct`, `retention_w12_pct` | — |
| Q3 | `forkers`, `forkers_with_pr`, `forkers_with_merge`, `prs_opened`, `prs_merged` | — | `merge_hours_p50`, `merge_hours_p90` |
| Q4 | `opened`, `responded`, `closed` | `close_rate_pct` | `response_hours_p50`, `response_hours_p90` |
| Q5 | `contributions`, `bot_contributions`, `human_contributors` | `bot_share_pct` | `top1_share_pct`, `top2_share_pct`, `top3_share_pct` |
| Q6 | — | — | `health_rank` and 7 score columns (`*_score`, `health_score`) |

- **Ratios**: `round(100 * cast(a as double) / b, 2)` over the summed measures, the exact formula and rounding
  of `qN.sql`, so when several cohorts/repos are combined the result is the correct pooled ratio (not the average
  of ratios; test `test_ratio_metric_reaggregates_from_measures`).
- **Not additive**: Q1's distinct counts of people (summing across days/scopes would count one person many
  times), percentiles, top-N shares (the mart has no per-person counts to recompute them), and Q6's scores and
  ranks. Measures use `max` (one row per grain, so the result is exactly the mart value); the metric `description`
  says "Not additive" and gives the correct grain. Grouping wider than the grain gives only the max, which is meaningless.
- Distinct counts in Q3/Q5 (`forkers`, `human_contributors`) summed across repos give the number of (person, repo)
  pairs, noted in the `description`.

## Time spine and the conventional anchor date

- `metricflow_time_spine` (schema `public`, column `date_day`, granularity `day`, 2020-01-01 to
  2030-12-31) is built with dbt's `dbt.date_spine` macro (works on DuckDB/MotherDuck,
  Snowflake, Databricks). It holds no personal data (`pii: none`); `tests/test_governance.py` has
  added this table to the list of `main_public` relations.
- MetricFlow requires every measure to have an aggregation time dimension. Q1 uses `period_start` (day
  granularity; weekly rows have a `period_start` that is a Monday), Q2 uses `cohort_week` (week granularity).
- Q3–Q6 are all-time aggregates per repo with no time column. They use a dimension
  `all_time_anchor` declared with `expr: cast('1970-01-01' as date)`: **a conventional anchor, a constant, not an
  event time**. The `metric_time` of these metrics is always 1970-01-01; do not filter or group
  by it. The public mart columns do not change.
- The return type of time dimensions depends on the DuckDB version: MetricFlow wraps the column in
  `DATE_TRUNC`; on a `DATE` column, DuckDB 1.4.1 returns `DATE` while DuckDB 2.0.0 returns `TIMESTAMP` at midnight.
  Users of the generated SQL should cast to `DATE` when comparing with mart columns.

## Generating and running metric SQL locally

```bash
python -m dbt.cli.main build --project-dir transform --profiles-dir transform --target ci   # writes ci.duckdb + transform/target/semantic_manifest.json
python -m repopulse.semantic sql --metrics cohort_size,retention_w4_pct --group-by q2_cohort__cohort_week__week
```

`repopulse.semantic sql` prints DuckDB SQL (usable for MotherDuck) and exits 0; a wrong metric/group-by
prints the error to stderr and exits 2. The SQL points at `"<database>"."main_public"."mart_…"` with the database
of the target when dbt wrote the manifest (`ci` for `ci.duckdb`): run it with `duckdb` on that exact database.
In Python: `load_manifest(path)` then `compile_sql(manifest, metrics, group_by)`.
The module only generates SQL, it does not run it; `metricflow` is imported lazily.

## Tests

`python -m pytest tests/test_dbt_semantic_layer.py`:

- `dbt parse` (target/log in tmp) succeeds with no deprecation warning and writes a semantic manifest
  with exactly 6 semantic models pointing at public marts, a metric for every numeric column of each mart, a non-empty
  `description`, and a daily time spine in `main_public` covering every `period_start`/`cohort_week`.
- For each of Q1–Q6: the Q's metrics grouped by the full grain, SQL generated by MetricFlow, run on the DuckDB built
  from the fixture (session fixture `built`), compared with the `qN.sql` result (fixture `baseline`): same row count,
  same grain keys, every numeric column (float `abs=1e-9`).
- Ratios are derived from additive simple metrics; non-additive metrics say "Not additive";
  the CLI prints runnable SQL and exits 2 when a metric does not exist.

On a local machine without `metricflow` the test file is skipped; on CI (`CI` set) a missing install is an error.
