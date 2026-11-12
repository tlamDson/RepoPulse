# Comparing the two paths end to end (D3)

Task D3 of Plan v2 (`docs/plan.md`, section "Primary-path selection rule"): run the **same** incremental
batch pipeline (ingest → bronze/silver → gold → the Q1–Q6 set) on two paths, on the **same** window of
real GH Archive data (not simulated), enough times on a schedule, then aggregate p50/p95 and the
success rate only from `runs.csv`. The path-selection rule is applied by D4; this PR does not choose. D4 applies the rule with
`python -m repopulse.pathdecision --runs runs.csv --doc docs/probe-result.md` (the result and how it is measured are in
[`probe-result.md`](probe-result.md)).

| Path | `variant` | Bronze/silver | Gold | Q1–Q6 read from |
|---|---|---|---|---|
| A | `path-a-runner` | PySpark + Delta on a GitHub Actions runner | `dbt build --target motherduck`, database `repopulse_path_a` | MotherDuck `main_gold.mart_*` |
| B | `path-b-databricks` | Databricks serverless job (D1, `docs/databricks-job.md`) | `dbt build --target databricks` (D2, `docs/dbt-databricks.md`), schemas `repopulse_dbt_*` | SQL warehouse `workspace.repopulse_dbt_gold.mart_*` |

## Workflow `.github/workflows/path-comparison.yml`

- **Triggers:** `schedule` hourly at minute 41 (`pipeline.yml` runs at minute 23), `workflow_dispatch`
  (optional inputs `date`, `hour`, `hours`), and `push` only on the branch `feature/pipeline-path-comparison`
  (filtered by `paths`) so it can run in this PR. There is no `pull_request`. **Cron and dispatch only run when
  the file is on `main`**, i.e. after the next release; before that only the run caused by a push on the PR branch exists.
- **The data window is decided once** in the `window` job (`python -m repopulse.pathcompare window`);
  the two path jobs share `needs.window.outputs`:
  - inputs `date` + `hour` (must come together), `hours` 1..6, default 1;
  - `push`: the benchmark reference day `benchmarks/reference.toml`, last hour 23, 1 hour (as in D1);
  - `schedule` or dispatch without `date`: hour `floor(now UTC) - 2h`, 1 hour.
- **Two independent jobs** (`path-a`, `path-b`, both `needs: window`): one path failing does not lose
  the other path's row.
- **Does not touch A's production:** A's gold is written to the MotherDuck database `repopulse_path_a`
  (`MOTHERDUCK_DATABASE`, `transform/profiles.yml`), created by
  `python -m repopulse.pathcompare md-ensure` (`CREATE DATABASE IF NOT EXISTS`; rejects `repopulse`
  and `*_smoke`). `pipeline.yml`'s `repopulse` and the freshness check are not written to. `duckdb==1.5.6` is pinned
  as in `pipeline.yml`. This database keeps staging history across runs like `repopulse`
  (idempotent by `event_id`).
- **Path B reuses D1/D2's tables:** silver `workspace.repopulse.silver_gharchive_events` (MERGE
  by event id, idempotent by (date, hour)) and the schemas `repopulse_dbt_*`. A single D1 run,
  no `--verify-rerun` (D1 already proved a re-run adds no rows).
- **Concurrency:** the whole workflow is in one group `path-comparison` (queued, does not cancel the running one;
  GitHub keeps only the newest pending run, an older pending run is cancelled and leaves no row). Job B also joins D1's
  group `databricks-bronze-silver`, so it never writes silver at the same time as `databricks-bronze-silver.yml`.
  `dbt-databricks.yml` (D2, manual) is not in the same group: staging only inserts `event_id`s not yet present.
- **Secrets:** enter only through the `env:` of the step that needs them; the step "Detect secrets" writes `HAS_SECRETS=true` into
  `$GITHUB_ENV` when they are all present. If one path's secrets are missing, all steps of that path are skipped at step level, with a
  `::notice::`, the workflow stays green, and **no row is written**. A needs `MOTHERDUCK_TOKEN`, `PII_HASH_SALT`; B
  needs `DATABRICKS_HOST`, `DATABRICKS_TOKEN`, `PII_HASH_SALT` (`DATABRICKS_HTTP_PATH` optional). The warehouse
  http path is resolved and masked by `python -m repopulse.probe http-path --github-env` as in D2.
- **Permissions:** `contents: read`; the two path jobs add `actions: read` to read the job's own queue time through the
  GitHub API (`GH_TOKEN` is `github.token`, not a repo secret).
- **Runner disk:** each run handles 1 hour of data (at most 6); ingest filters event types before storing and deletes
  downloaded files in `$RUNNER_TEMP`.
- **Artifacts** `path-comparison-a-<run_id>` / `path-comparison-b-<run_id>`: `out/path-a-row.csv`
  or `out/path-b-row.csv` plus the measurement JSONs; the Job summary prints that row's summary table.

The run caused by a push on the PR branch uses the reference day: B's silver may already have those hours from D1 (inserting 0
rows), while `repopulse_path_a` starts empty. That run's row is a workflow trial; the comparison numbers for D4 come
from the scheduled runs (`trigger=schedule` in `notes`).

## What is measured

Each pipeline step runs through `python -m repopulse.pipeline timed --name <step>` (wall time of the
child process, written to `out/timings.json`; the command's exit code is the step's result). A's steps:
`ingest`, `spark`, `export`, `dbt`, `queries`; B's: `ingest`, `databricks`, `warehouse`, `dbt`,
`queries`.

| Measurement | Source | Where in the row |
|---|---|---|
| End-to-end | sum of the times of the steps that ran (excluding checkout, setup, queueing) | `total_seconds` |
| Per step | `pipeline timed` | `notes`: `<step>_seconds` |
| Runner queueing | GitHub jobs API for the run: the job's `started_at - created_at` | `queue_seconds` |
| Runner startup | `now - started_at` just before ingest (checkout, Python/Java setup, pip) | `setup_seconds` |
| Serverless job queueing / setup (B) | Jobs API via the D1 row (`queue_s`, `setup_s` of `databricks_job`) | `job_queue_seconds`, `job_setup_seconds` |
| SQL warehouse startup (B) | `pathcompare warehouse-start`: from the call until `RUNNING` (0 if already running) | `warehouse_start_seconds`; the step `warehouse_seconds` is the wall time of the whole step |
| Q1–Q6 | `pathcompare queries`: `SELECT *` over the whole mart of the question (A: DuckDB `fetchall`; B: Statement Execution API until `SUCCEEDED`) | `q1_seconds` … `q6_seconds` |
| Row count | `silver_inserted` of the bronze/silver step | `row_count` |
| Rows read | `bronze_rows` of the bronze/silver step | `bronze_rows` in `notes` |

`row_count` is the number of silver rows **newly added**: path A builds a fresh lake each run so it always inserts everything;
path B's silver lives permanently in Unity Catalog so an hour loaded earlier (for example an hour of the reference
day loaded by D1) gives `row_count=0`, with every row deduplicated. `bronze_rows` is the number of rows the
bronze/silver step read: when the two paths process the same data window these two numbers are equal. A scheduled run
takes a fresh hour so both paths really insert.

`notes` also has `status=success|failed`, `failed_step=<step>` (the first step with a result other than
`success`; a step that did not run counts as `skipped`), `hour`, `hours`, `missing_hours` (GH Archive hours
not yet on the server), `run_id`, `trigger`. The `run_at` time is when the job started (UTC).

**A failed run still writes a row** (a step with `if: always()`), so the success rate can be computed from
`runs.csv` alone. No row is written when: secrets are missing (a deliberate skip), the job is cancelled before it runs (concurrency
group), or an error before the row-writing step makes Python/the package unusable (a broken install). These
cases also leave no row on the other path if the cause is shared (for example a cancelled pending run).

Known limitations: the latency of GitHub's own cron (scheduled time → run creation) is not measured; Q1–Q6
on B are measured until the statement finishes at the warehouse, with the first part of the result inline, not downloading every chunk.

## Aggregation

```bash
python -m repopulse.pathcompare summary --runs runs.csv --out out/path-summary.md
```

The pure functions `read_runs` (parser) and `summarize` (aggregation) in `repopulse/pathcompare.py`; D4 calls
these two functions directly.

- `read_runs`: keeps only `path-a-runner` / `path-b-databricks` rows; splits `notes` into a dict (every
  word must be `key=value`, a repeated key is an error), converts numeric types. A broken row (missing `status`, `failed` without
  `failed_step`, an unreadable number, wrong column count) raises `PathCompareError` with the line number, never silently
  skipped. Rows of other variants are ignored.
- `summarize`, per path (always both):
  - number of runs, number of successes, success rate (`None` when there is no run yet);
  - p50/p95 end-to-end and per step: **successful runs only**;
  - p50/p95 of queueing/startup: every run that has that number;
  - count of failed steps (`failed_steps`);
  - number of runs per UTC day of `run_at`, every day from the first run to the last (a day with no run
    counts as 0), with the min/max per day — so D4 can check "24 runs/day". The first and last days may be
    partial days.

**Nearest-rank percentile:** with n sorted values, percentile p is the value at rank
`ceil(p / 100 * n)` (counted from 1, at least 1). The result is always a real measurement in `runs.csv`
(no interpolation); with n = 1, p50 = p95 = that value; an empty list has no percentile.

## Merging rows into `runs.csv`

Download the artifacts of the runs, then:

```bash
python benchmarks/record_run.py out/path-a-row.csv out/path-b-row.csv
python -m repopulse.readme_results
```

`record_run.py` skips rows already present (merging the same artifact again creates no duplicates). The README Results section
has a dedicated part for the two paths (R2): a table of each path's latest row with step columns taken from `notes`,
and a table counting every run by (variant, trigger) with the number of runs, number of successes, and nearest-rank p50/p95
of `total_seconds` over successful runs (copying the cell of the selected run verbatim,
not re-printed through a float). The README does not copy the path-selection conclusion; the conclusion is in
[`probe-result.md`](probe-result.md).

## Turning off the cron once D4 has enough runs

The workflow only runs on schedule after release to `main`. Once D4 has enough runs: disable it in the
Actions tab (or `gh workflow disable path-comparison.yml`, done by the repo owner), or open a PR removing the
`schedule:` block. Databricks Free Edition has a daily quota; when it is exceeded compute is off until the end of the day,
and those runs record `status=failed` on path B, which is exactly the data D4 needs.

This document does not copy numbers; measurements are read only from `runs.csv`.
