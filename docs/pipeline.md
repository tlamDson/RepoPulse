# Scheduled pipeline: ingest → Spark → export → dbt → runs.csv

`.github/workflows/pipeline.yml` chains the existing steps into an **incremental batch** that runs every hour
(not streaming or real-time). Fallback path (`docs/probe-result.md`): Spark runs on the
runner, gold lives on MotherDuck. GH Archive is real data.

```
python -m repopulse.ingest            data/landing/gharchive   (docs: repopulse/ingest.py)
python -m repopulse.pipeline window   DATA_DATE / DATA_HOUR / DATA_HOURS -> $GITHUB_ENV
python -m repopulse.spark.job         data/lake (bronze/silver/quarantine)   docs/spark-jobs.md
python -m repopulse.spark.export      data/silver-export/*.parquet           docs/spark-jobs.md
dbt build --vars silver_gharchive_parquet=...   MotherDuck (incremental)     docs/dbt-models.md
python -m repopulse.pipeline row      out/pipeline-row.csv (artifact)
```

The four heavy steps run through `python -m repopulse.pipeline timed --name <step> --timings out/timings.json
-- <command>`, which records the wall-clock time of each step; the `runs.csv` row takes the sum of the steps as
`total_seconds`.

## Triggers

| Trigger | Data | dbt target | Secret |
|---|---|---|---|
| `schedule` (minute 23 of every hour) | 3 hours ending at now − 2h | `motherduck`, database `repopulse` | `MOTHERDUCK_TOKEN`, `PII_HASH_SALT` (if missing, stops right at the first step) |
| `workflow_dispatch` | inputs `date`/`hour`/`hours` (default 3), `force`, `full_refresh` | same as above | same as above |
| `pull_request` (filtered by `paths:`) | 1 real hour (now − 2h) | `ci` (local DuckDB), fixed fake salt | not needed; with `MOTHERDUCK_TOKEN` a smoke step is added |

`PII_HASH_SALT` goes into both the Spark step (PII hashing at silver) and every `dbt build` step: the `public` models
mask `user_key`/`actor_id`/`actor_login` with `mask_pii` using the same salt (`docs/governance.md`), and
outside the `ci` target a missing salt is a compile error. The step that runs only on pull requests uses a fixed fake salt
and receives no real secret.

`schedule` and `workflow_dispatch` only run once the file is on `main` (after release). These two triggers
share one concurrency group (`pipeline-motherduck`, queued, not cancelled): only one MotherDuck write at a time.
Each PR has its own group.

Each run takes 3 hours even though the cron runs hourly: an hour that GH Archive publishes late (still `missing` at run time)
is picked up by the next run. An hour already loaded is skipped by staging via `event_id`, so overlapping runs do not create
duplicates. The hourly cron of `ingest.yml` was removed (it now only has `workflow_dispatch` and `pull_request`), so
the same data is not downloaded twice an hour.

## Pull request

Does not touch the `repopulse` database. The chain runs on `ci.duckdb` and then:

1. A second `dbt build` on the same parquet, comparing `python -m repopulse.pipeline counts` before/after with
   `diff`: a re-run must not change the row counts of staging and gold.
2. With `MOTHERDUCK_TOKEN`: `md-reset --database repopulse_smoke` (accepts only `*_smoke` names), build
   the `motherduck` target twice with `MOTHERDUCK_DATABASE=repopulse_smoke`, compare row counts.

A PR's `runs.csv` row has `variant=pipeline-pr-smoke`; for schedule/dispatch it is `pipeline-incremental`.

## runs.csv

The workflow only reads the repo (`contents: read`) and does not commit. The row is in the artifact
`pipeline-run-<run_id>` (`out/pipeline-row.csv`, together with the ingest/Spark/export summaries and
`timings.json`; neither the lake nor landing is uploaded). Merge it into `runs.csv` with:

```bash
python benchmarks/record_run.py <artifact directory>/pipeline-row.csv [<other artifact>.csv ...]
```

Every row of every artifact is checked first; if one artifact is broken, no row is written.

| Column | Value |
|---|---|
| `data_date` | date of the last data hour |
| `total_seconds` | sum of the step times in `timings.json` |
| `row_count` | `silver_inserted` of the Spark job (number of events entering silver in the run) |
| `q1_seconds`…`q6_seconds`, `peak_rss_mb`, `bytes_read` | left empty (the pipeline does not measure queries) |
| `notes` | `key=value` pairs separated by spaces: `last_hour`, `hours`, `ingested`/`skipped`/`missing`, `<step>_s`, `run_id`, `trigger`, `target`, `full_refresh` |

This document contains no measurements; measurements come only from `runs.csv`.

After each scheduled run, `.github/workflows/data-quality.yml` (`workflow_run`) checks the volume
and freshness of staging and opens/updates the `data-incident` Issue: `docs/data-quality.md`.

## Commands

```bash
python -m repopulse.pipeline window --ingest-summary out/ingest-summary.json
python -m repopulse.pipeline timed --name dbt --timings out/timings.json -- <command>
python -m repopulse.pipeline counts --database ci.duckdb --out out/counts.json
python -m repopulse.pipeline row --run-at <ISO with time zone> --git-sha <sha> --ingest-summary out/ingest-summary.json --spark-summary out/spark-summary.json --timings out/timings.json --run-id <id> --trigger <name> --out out/pipeline-row.csv
python -m repopulse.pipeline md-reset --database repopulse_smoke   # needs MOTHERDUCK_TOKEN
```

Tests: `tests/test_pipeline.py` (helpers, no network, no Spark), `tests/test_pipeline_workflow.py`
(static check of `pipeline.yml`), `tests/test_spark_export.py`, `tests/test_dbt_incremental.py`.
