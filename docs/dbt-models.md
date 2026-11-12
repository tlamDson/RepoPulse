# dbt models: staging → star schema → marts Q1–Q6

The dbt project is in `transform/`. Gold is a star schema (`fct_events`, `dim_user` SCD2, `dim_repo`) and
six marts answering Q1–Q6 (`docs/plan.md`). The canonical definition of each metric is
`benchmarks/queries/qN.sql`; the mart gives the **same column names, same semantics and same results** as the
corresponding query on the same data (deduplicated by `event_id`), with a matching test
(`tests/test_dbt_marts.py`). `stg_gharchive_events` and `fct_events` are incremental (section
"Incremental" below); dims and marts are `table`s, rebuilt each time from the accumulated history.

## Diagram

```
seed fixture_silver_gharchive_events ─┐   (default, CI)
read_parquet(var silver_gharchive_parquet) ─┤   (when the var is set, DuckDB only)
source silver.silver_gharchive_events ─┤   (Unity Catalog, default on Databricks)
                                       ▼
                    staging.stg_gharchive_events (incremental, accumulating)
                         │                    │
seed repos_data_ecosystem ──► gold.dim_repo   │
                         │         │          ▼
                         │         │    gold.dim_user (SCD2)
                         ▼         ▼          │
                    gold.fct_events (incremental) ◄─┘
                         │
                         ▼
          intermediate.int_events_enriched (view: fct + login/is_bot + repo_name/category)
                         │
     ┌──────────┬────────┼─────────┬──────────────┬──────────────┐
     ▼          ▼        ▼         ▼              ▼              ▼
   mart_daily_  mart_    mart_     mart_          mart_repo_     mart_repo_
   active_      cohort_  contrib-  responsiveness concentration  health_score
   contributors retention ution_funnel
   (Q1)         (Q2)     (Q3)      (Q4)           (Q5)           (Q6)

gold.{dim_repo, dim_user, fct_events, 6 marts}
                         │  pub_* (table, alias = gold name)
                         ▼
     public.{dim_repo, 6 marts}           straight through (all columns pii: none)
     public.{dim_user, fct_events}        user_key / actor_id / actor_login masked with mask_pii
     public.metricflow_time_spine         daily calendar of the semantic layer
```

Schemas on DuckDB: `main` (seeds), `main_staging`, `main_intermediate`, `main_gold`, `main_public`
(dbt combines `<target schema>_<custom schema>`). `main_public` is the only thing the `public` role
(dashboard, Tableau Public) may read; PII labels, roles and masking: `docs/governance.md`.

Model and test SQL runs on DuckDB, Snowflake and Databricks: it does not use syntax only
DuckDB has (`filter (where …)`, `arg_min`, `quantile_cont`, `epoch`, `::`, `bool_or`, …) or that
Databricks does not understand (`varchar` without a length, `regexp_full_match`, `unnest`, `sha256`, two-argument
`trim`) outside the macros in `transform/macros/cross_db.sql`; `tests/test_dbt_portability.py` and
`tests/test_dbt_databricks.py` check this. The mapping table and workflows: `docs/snowflake-port.md`,
`docs/dbt-databricks.md`. The var `silver_gharchive_parquet` can only be used on DuckDB (Snowflake and
Databricks give a compile error).

## Choosing the source with a var

| Var | Source of `stg_gharchive_events` | Used when |
|---|---|---|
| (unset) | seed `fixture_silver_gharchive_events` | the CI job `dbt build (DuckDB)`: no lake, no data network |
| `silver_gharchive_parquet: "<glob>"` | `read_parquet('<glob>')` (macro `silver_parquet_relation`, DuckDB only) | the scheduled pipeline (`data/silver-export/*.parquet`, `docs/pipeline.md`), the matching test (`tests/test_dbt_marts.py`), manual runs on parquet files with silver columns |
| `silver_gharchive_source: unity_catalog` (default on the Databricks target) | source `silver.silver_gharchive_events` = `workspace.repopulse.silver_gharchive_events` (change with the vars `silver_gharchive_catalog`/`_schema`/`_table`) | `dbt build --target databricks` on the silver Delta table of the Databricks job (`docs/dbt-databricks.md`) |
| `silver_gharchive_source: seed` (default on other targets) | seed `fixture_silver_gharchive_events` | building the CI seed on Databricks |

```bash
python -m dbt.cli.main build --project-dir transform --profiles-dir transform --target ci
python -m dbt.cli.main build --project-dir transform --profiles-dir transform --target ci --vars "{silver_gharchive_parquet: 'path/to/silver/*.parquet'}"
```

The seed `fixture_silver_gharchive_events` is a **CI fixture, not a measurement**: 36 hand-written events
(`tests/fixtures/gharchive_small` deduplicated by `id` + `tests/fixtures/gharchive_scd2`) in exactly the
columns of `silver/gharchive_events` (`docs/spark-jobs.md`). Regenerate it from JSON with
`silver_select_sql` in `tests/dbt_helpers.py`; the test
`test_ci_seed_equals_silver_built_from_fixture_json` asserts the seed equals exactly that data (every
`event_id`, every column). The parquet pytest creates additionally includes `tests/fixtures/gharchive_parquet_only` (3
events not in the seed, including a same-second login-change case), so tests can tell the parquet read path
from the seed read path; the baseline runs on the same raw rows as the parquet.

The scheduled pipeline does not let dbt read the Delta directory: `python -m repopulse.spark.export` reads the
silver table through Delta and writes plain parquet (`docs/pipeline.md`). Spark writes `created_at` as a UTC
instant, and parquet may reach DuckDB as `TIMESTAMPTZ`; staging goes through the macro `utc_timestamp`
(`macros/cross_db.sql`; the DuckDB branch is `make_timestamp(epoch_us(…))`) so wall-clock time is always UTC
regardless of the session time zone. The two DuckDB targets (`ci`, `motherduck`) also set
`TimeZone: UTC` in `profiles.yml`.

## Incremental

The scheduled pipeline runs on a runner with a fresh lake each time, so the silver parquet handed to dbt only has
the hours of that run. History lives in gold (MotherDuck), specifically in staging:

| Model | Materialization | Each run |
|---|---|---|
| `stg_gharchive_events` | `incremental`, `append`, `full_refresh: false` | Inserts `event_id`s not yet present; an existing copy wins (like silver's MERGE). Column `loaded_at` = the `run_started_at` (UTC) of the dbt run that inserted the row |
| `fct_events` | `incremental`, `delete+insert` by `event_id` (`merge` on Databricks, same result) | Recomputes **all** facts of "touched" actors: actors that have a staging row with `loaded_at` greater than the `max(loaded_at)` of `fct_events` itself |
| `dim_user`, `dim_repo`, 6 marts | `table` | Rebuilt from all of staging |

Why recompute the whole actor: a late-arriving event (even on a repo outside the seed, which does not become a fact) can
insert an earlier login version into `dim_user`, shifting the `version_number` (and so the `user_key`) of every
later event of that actor. Recomputing an actor twice is harmless; missing one is wrong, so the watermark is taken from
`fct_events` itself.

Invariants with tests (`tests/test_dbt_incremental.py`): building batch by batch on one DuckDB file gives exactly
the same rows, column by column, as one full build over the union of the batches (except `loaded_at`); re-running an old batch,
a duplicate batch or an empty batch changes no table; no duplicate `event_id`; a late event shifts the version of
old facts; `--full-refresh` keeps the accumulated staging.

**When `--full-refresh` is needed** (the `full_refresh` input of `workflow_dispatch` in
`pipeline.yml`):

- Editing the seed `repos_data_ecosystem` (adding/removing repos): incremental only recomputes touched actors, so
  old events of a newly added repo do not become facts.
- Changing the SQL logic of `fct_events` or changing columns (the model sets `on_schema_change: fail`; changing columns
  without a full refresh makes `dbt build` fail).

`--full-refresh` rebuilds `fct_events` from staging but does **not** touch `stg_gharchive_events`
(`full_refresh: false`): staging is the only place that keeps history, the lake is gone. To rebuild staging too,
the table must be dropped by hand and the silver of every hour reloaded (no procedure yet).

## Grain and keys

| Model | Grain | Key | Notes |
|---|---|---|---|
| `stg_gharchive_events` | 1 silver event | `event_id` | All 17 silver columns, including the commit hash arrays (silver may keep hashes), plus `loaded_at` |
| `dim_repo` | 1 repo in the seed | `repo_key` = `lower(repo_name)` | `repo_name` keeps the seed's spelling; events match case-insensitively like the baseline |
| `dim_user` | 1 login version of 1 `actor_id` | `user_key` = `'<actor_id>-<version_number>'` | SCD2, see below |
| `fct_events` | 1 event on a seed repo | `event_id` | FK `user_key` → `dim_user`, `repo_key` → `dim_repo`; `event_week` = ISO week (Monday); `item_number` = `coalesce(pr_number, issue_number)`. Does **not** carry the commit email/name hash columns into gold |
| marts Q1–Q6 | as in `qN.sql` | as in `qN.sql` | |

Events on repos outside the seed do not enter `fct_events` (no Q uses them), but still contribute to the login
history of `dim_user`.

## SCD2 rules of `dim_user`

Built from event history (gaps-and-islands), deterministic, and **not** using dbt snapshot (snapshots depend on
run time and are not idempotent on backfill):

1. Order each `actor_id`'s events by (`created_at`, `event_id`). `event_id` is varchar, so ties on
   `created_at` are broken by string order (`'10' < '9'`), not numerically; still deterministic.
2. A new version starts when `actor_login` differs from the previous event (null-safe comparison). `a → b → a`
   gives 3 versions.
3. `valid_from` = the first event of the version; `valid_to` = the `valid_from` of the next version (half-open
   `[valid_from, valid_to)`), `NULL` for the current version; `is_current = valid_to IS NULL`.
4. `is_bot`: a login ending in `[bot]` or `-bot` (case-insensitive), as in `q1.sql`.
5. **A login change within the same second** (an event with login `a` then an event with login `b` for the same actor, same
   `created_at` = `t`): version `a` has `valid_from = valid_to = t` (a zero-length version). A zero-length
   version is in effect exactly at the instant `[t, t]`, not the empty interval `[t, t)`; every other
   version stays half-open. Timestamps are not shifted and versions are not merged, so `dim_user` still depends
   only on events. The singular test `assert_fct_events_user_version_matches_event` applies exactly this rule;
   `assert_dim_user_scd2_intervals` already accepted `valid_to = valid_from`.

`fct_events.user_key` is the version of the same `actor_id`, with the same login as the event, with the largest
`valid_from` `<=` `created_at` (on a tie, the largest `version_number`). In the same-second login-change case, event `a`
only matches the zero-length version `a`, and event `b` matches version `b`. Known limitation: if within the same second
there is `a → b → a`, the first event `a` points at the later version `a` (which also starts at `t`, same login) instead of
the first version; this is still deterministic, still passes the singular test and does not change the marts (marts count by login). Since the
baseline counts by `actor.login`, the marts use the login of the version in effect at the event, so results
match the baseline even when an actor is renamed.

## Marts

| Mart | Query | Question |
|---|---|---|
| `mart_daily_active_contributors` | `q1.sql` | Active contributors per day/week, by repo and category, new vs returning |
| `mart_cohort_retention` | `q2.sql` | Cohort retention at exactly W+4, W+12 |
| `mart_contribution_funnel` | `q3.sql` | Fork → PR → merge funnel, hours to merge p50/p90 |
| `mart_responsiveness` | `q4.sql` | First response time p50/p90, close rate |
| `mart_repo_concentration` | `q5.sql` | Bus factor (top-N share) and bot share |
| `mart_repo_health_score` | `q6.sql` | Composite health score and ranking |

The health score formula (components, weights, rounding, ranking order) is written at the top of
`benchmarks/queries/q6.sql`; `mart_repo_health_score` uses that formula verbatim.

Semantic layer (MetricFlow): each public mart has one semantic model and each numeric column a metric of the same
name, declared in `transform/models/public/_semantic.yml`; grain, non-additive metrics, the time spine
and how metric SQL is generated: `docs/semantic-layer.md`.

Differences from the baseline:

1. **Dedup.** The baseline does not deduplicate (raw rows with a duplicate `id` are counted twice, affecting Q5/Q6), while
   silver keeps one row per `event_id`. The matching test runs the baseline on deduplicated input.
2. **NULL `actor_login`.** The baseline computes `ends_with(lower(NULL), …)` as NULL so `NOT is_bot` excludes that
   event from every "human" set (forkers, cohort, `per_actor` of Q5/Q6, `opener_is_bot`), but Q5
   still counts it in `contributions`. The mart treats that event as human because `dim_user.is_bot` coalesces to
   `false`, so it slips into the Q2 cohort, `prs_opened` of Q3, `human_contributors`/top-N of Q5 and
   the activity/diversity of Q6. Silver only requires `actor.id`, but real GH Archive always has
   `actor.login`. This case **has no test yet** and the fixture has no NULL-login event.

## Tests

| Kind | Where | What it checks |
|---|---|---|
| Contract | `contract: {enforced: true}` for `fct_events`, `dim_user`, `dim_repo` and the 6 marts | Column names, types, `not_null` |
| Generic | `models/**/_*.yml` | `not_null`, `unique`, `relationships` (fct → dim, funnel → dim_repo, public fct → public dim_user), `accepted_values` |
| Custom generic | `transform/tests/generic/` | `hashed_pii`: the staging hash columns and masked public columns contain only 64-character SHA-256 hex (if silver forgot to hash, the build goes red and downstream models are SKIPPED); `no_raw_email`: columns labeled other than `none` in gold/public contain no email (`docs/governance.md`) |
| Singular | `transform/tests/` | SCD2 validity intervals have no gaps, no overlaps, exactly one current version; fct points at the right version; fct has every event on seed repos, each exactly once |
| Unit (`unit_tests:`) | `_gold.yml`, `_marts.yml` | SCD2 (login change → 2 versions; `a → b → a` → 3; same-second login change → zero-length version), `fct_events` picking the version by the `valid_from` bound (`a → b → a`) and in the same-second case, all 6 marts Q1–Q6; the same `given`/`expect` runs on DuckDB (CI) and Databricks (`docs/dbt-databricks.md`) |
| pytest | `tests/test_dbt_marts.py` | Matches the 6 marts against `qN.sql` row by row, column by column; a contract violation makes `dbt build` exit non-zero; gold has no email columns/values; rebuilding with the same input gives the same row counts |
| pytest | `tests/test_dbt_incremental.py` | Batch-wise incremental equals a full build over the union of batches; idempotent; late events; `--full-refresh`; UTC time zone |
| pytest | `tests/test_dbt_databricks.py` | Every macro has a `databricks__` branch, run on DuckDB next to the `duckdb__` branch where possible; no syntax Databricks does not understand; staging reads the Unity Catalog source on Databricks, seed/parquet elsewhere |
| pytest | `tests/test_governance.py` | `meta.pii` labels complete and matching the policy (`check`), no raw email in gold/public (`scan`), un-hashed silver turns the build red, masking in public follows the `hash_pii` formula, public = gold in row count and joins |
| pytest | `tests/test_dbt_semantic_layer.py` | Semantic manifest (`dbt parse`): 6 semantic models pointing at public marts, a metric for every numeric column, time spine; metrics grouped by mart grain match `qN.sql` (`docs/semantic-layer.md`) |

The build shared by the pytest files is the session fixture `built` (and `baseline` = the `qN.sql` results)
in `tests/conftest.py`; helpers
(`run_dbt`, `write_profiles`, `silver_select_sql`, …) are in `tests/dbt_helpers.py`.

There are no measurements in this document; measurements come only from `runs.csv`.
