# Data quality: injected faults, volume, freshness, Issue alerts

`repopulse/quality.py` + `.github/workflows/data-quality.yml` check the data quality of the hourly
**incremental batch** (not streaming or real-time). Two sources, two kinds of checks:

- **App events — simulated data** (`simkit`, no real users): inject 6 known fault types,
  run them through the real Spark bronze/silver job, and each fault type must be caught by its own dedicated check;
  a clean run (no faults injected) must raise no alarms.
- **GH Archive — real data**: volume per landing hour and the freshness of the table
  `main_staging.stg_gharchive_events`; on an anomaly it opens/updates a GitHub Issue labeled
  `data-incident`.

This document contains no measurements; measurements (M4) come only from `runs.csv`.

## Where the thresholds are

All thresholds are in `repopulse/quality.toml` (packaged with the package, loaded by
`repopulse.quality.load_config`; a missing key or wrong type raises an error naming the key). The code does not
hardcode thresholds and does **not** take constants from `simkit`: the checks are independent of the fault generator. To run
with different thresholds: `python -m repopulse.quality --config <file.toml> <command> ...`.

| Section | Keys | Meaning |
|---|---|---|
| `[faults]` | `late_threshold_seconds`, `bot_events_per_window`, `bot_window_seconds` | thresholds of the fault checks |
| `[faults.inject]` | `duplicate`, `null`, `wrong_type`, `late`, `schema_drift`, `bot` | number of faults of each type the harness injects |
| `[volume]` | `table`, `window_hours`, `min_history`, `min_ratio`, `evaluate_hours`, `type_min_median` | volume check |
| `[volume.reference]` | `history`, `evaluate` | reference GH Archive hours (complete data) and the known-missing hours |
| `[freshness]` | `table`, `column`, `max_lag_hours` | freshness check |
| `[alert]` | `label`, `template` | Issue label, Issue template file |

## Six fault types → the check that catches them

The checks (`detect_*`, combined in `run_fault_checks`) only read rows of the lake tables
(`docs/spark-jobs.md`) and **never** look at simkit's manifest. The manifest is the answer key, used only in the
`score` step.

| simkit fault type | Check (`check`) | Function | Caught by |
|---|---|---|---|
| `duplicate` | `duplicate_key_in_bronze` | `detect_duplicates` | an `eventspec.dedup_key` key appearing > 1 time in bronze (bronze keeps every copy) |
| `null` | `quarantine_null_field` | `detect_null` | quarantine `schema_invalid` with an error `must be of type …, got null` |
| `wrong_type` | `quarantine_wrong_type` | `detect_wrong_type` | quarantine `schema_invalid` with `must be of type T, got U` (U other than `null`) |
| `schema_drift` | `quarantine_schema_drift` | `detect_schema_drift` | quarantine with `unexpected field …`, or reason `unknown_event` (undeclared version) |
| `late` | `late_arrival`, `late_beyond_window` | `detect_late` | silver with `received_at - occurred_at > late_threshold_seconds`; or quarantine `late_beyond_window` |
| `bot` | `bot_rate` | `detect_bots` | a silver user with > `bot_events_per_window` events in any `bot_window_seconds` window |

Auxiliary checks, tied to no fault type (any finding is a false alarm or a pipeline error):
`silver_duplicate_key` (silver still has > 1 row for a key), `duplicates_not_dropped`
(the summary's `duplicates_dropped` is smaller than the number of valid duplicates in bronze), `quarantine_other`
(quarantine for another reason, for example `malformed_json`).

`late` does **not** use silver's `is_late` column: Spark considers an event late when it falls in a UTC hour earlier than
the arrival hour, so a clean event straddling an hour boundary (a few seconds late) also gets `is_late = true` and would become a
false alarm. The check uses the delay against the threshold in the config.

**Scoring** (`score`): a fault in the manifest is "caught" when there is a finding of the **same type** pointing at
exactly its event (`event_id`), or for bots exactly its `user_id`. Quarantine does not keep the payload, so
quarantine findings carry `raw_sha256`; the harness keeps a table `sha256 of landing line → event_id` when writing
landing, to join them. A finding that matches no fault of the same type is a **false alarm**.

## The `faults` harness

```bash
PII_HASH_SALT=<salt> python -m repopulse.quality faults --seed 42 --users 30 --days 2 \
    --clean-runs 2 --work-dir data/tmp/dq-faults --git-sha <sha> \
    --report out/dq-faults.json --out out/dq-fault-row.csv
```

1. `simkit.simulate` + `inject_faults` (counts from `[faults.inject]`, fixed seed).
2. Write app landing `<work>/faulted/landing/date=YYYY-MM-DD/hour=HH/events.jsonl.gz`; the landing hour =
   the hour of `received_at` (if that field is corrupted by a null/wrong_type fault, use the hour of the preceding event).
   `late` events can arrive up to several days late, so the landing extends beyond the simulated range.
3. `repopulse.spark.job.run_batch` with source `app` over every hour written (one batch), then re-read exactly those slices of
   bronze/silver/quarantine → `run_fault_checks` → `score`.
4. `--clean-runs K` clean runs (seed+1 … seed+K, no faults injected): every finding is a false
   alarm.

Needs Java 17 + `pip install -e ".[dev,spark]"`, and `PII_HASH_SALT` (if missing, exit code 1 **before**
starting Spark). Exit 1 when a fault type is missed or there is a false alarm. The lake and landing are in
`--work-dir` (default `data/tmp/dq-faults`, gitignored; not `out/`, not uploaded).

### `runs.csv` row (M4)

`--out` writes a CSV (header + one row, same format as `repopulse.pipeline.write_row`):

| Column | Value |
|---|---|
| `variant` | `dq-fault-injection` |
| `data_date` | simulation start date (`--start`, default `2026-11-01`) |
| `total_seconds` | wall-clock time of the whole harness |
| `row_count` | number of events (after fault injection) fed into the pipeline |
| `notes` | `simulated=true seed= users= days= injected= caught= false_alarms= clean_runs=` and per type `duplicate=<caught>/<injected>` … `bot=<caught>/<injected>`; `false_alarms` includes findings from the clean runs |

Merge into `runs.csv` from the artifact `data-quality-<run_id>`:

```bash
python benchmarks/record_run.py <artifact directory>/dq-fault-row.csv
```

## Volume (GH Archive, real data)

`check_volume(hourly_counts, cfg)` with `{UTC hour: {event_type: event count}}` (counted by the staging table's
`landing_date`/`landing_hour`):

- Build a **continuous** series of hours from the first hour to the last; an hour with no rows = 0.
- Each hour among the newest `evaluate_hours` is compared with the **median** of up to `window_hours` hours
  immediately before it:
  - total < `min_ratio` × median total → `volume_drop`;
  - an event type whose median ≥ `type_min_median` but is 0 in the hour under evaluation → `volume_type_missing`.
- Fewer than `min_history` hours of history → no conclusion, no alarm (the report records
  `insufficient_history`).

**Real missing hours** (`gap`): since 2026-11 some GH Archive hours have lost PushEvent/CreateEvent/DeleteEvent entirely
and volume dropped sharply (`docs/plan.md`). `[volume.reference]` declares complete reference hours from the benchmark
reference day (`benchmarks/reference.toml`) as history and one missing hour as the hour under evaluation. `gap
--ingest` downloads those hours with `repopulse.ingest`, counts them by `counts_by_type` in `_manifest.json`
(only the event types that were ingested), compares as above; exit 1 if the missing hour is **not** caught.

## Freshness

`check_freshness(latest_event_at, now, cfg)`: `now - max(created_at)` > `max_lag_hours` →
`freshness_stale` (exactly at the threshold does not alarm yet); an empty table → `freshness_empty`. `now` is always passed
in (CLI `--now` ISO-8601 with time zone, default the system time in UTC).

```bash
python -m repopulse.quality check --database ci.duckdb --report out/dq-report.json
python -m repopulse.quality check --database md:repopulse --report out/dq-report.json   # needs MOTHERDUCK_TOKEN
```

`check` exits 0 even when there are alarms (alarms go through Issues), and 1 on a run error (no database,
missing table, `--now` without a time zone).

## GitHub Issue alerts

```bash
python -m repopulse.quality alert --report out/dq-report.json --run-url <run url> --dry-run
python -m repopulse.quality alert --report out/dq-report.json --run-url <url> --repo owner/name   # needs GH_TOKEN
```

- `build_issue`: the body has **all** `## ` headings of `.github/ISSUE_TEMPLATE/data-incident.md`
  in the same order (read from the template file, front matter dropped): Symptom (with the link to the Actions run),
  Scope (table, data hour range), Cause (investigation hints), Trigger condition (where the thresholds are),
  How to handle and backfill (`workflow_dispatch` of `Pipeline (scheduled)` with `date`/`hour`/
  `hours`).
- Each alarm has a deterministic hidden marker `<!-- dq:<check>:<data hour> -->`.
- `publish`: each check kind (`volume`, `freshness`) has at most **one** open issue labeled
  `data-incident` (title starting `[dq:volume]` / `[dq:freshness]`):
  - none yet → `gh label create data-incident --force` then `gh issue create`;
  - exists and the alarm's marker is not yet in the body/comments → `gh issue comment` with only the new
    alarms;
  - every marker already present → do nothing. Running every hour does not spam.
- `--dry-run` only prints the title and body, without calling `gh`. Unit tests always inject a fake `gh` and never
  create a real Issue. The token goes only through the environment variable `GH_TOKEN` and is not printed.

## Workflow `.github/workflows/data-quality.yml`

| Job | Trigger | What it does | Permissions |
|---|---|---|---|
| `checks` | `workflow_run` (after `Pipeline (scheduled)`), `workflow_dispatch`, `pull_request` | schedule/dispatch: `check --database md:repopulse` (if `MOTHERDUCK_TOKEN` is missing, `::error::` and stop); PR: `dbt build --target ci` (fixture seed) then `check --database ci.duckdb`. Report via artifact `data-quality-report-<run_id>` | `contents: read` |
| `alert` | after `checks` | PR: `alert --dry-run`; otherwise: `alert --repo $GITHUB_REPOSITORY` with `GH_TOKEN` | `contents: read`, **`issues: write`** (the only job) |
| `fault-injection` | `pull_request`, `workflow_dispatch` | `faults` on **simulated** data with the fixed fake salt `not-a-secret-pr-salt`; artifact `data-quality-<run_id>` (only `out/`: report + `dq-fault-row.csv`, 30 days) | `contents: read` |
| `reference-gap` | `pull_request`, `workflow_dispatch` | `gap --ingest` on real GH Archive hours; red if the missing hour is not caught | `contents: read` |

- A `workflow_run` originating from a pull request's pipeline run is skipped (job-level `if`).
- `workflow_run` and `workflow_dispatch` only run once the file is on `main` (after release).
- `pull_request` filters by `paths:` (this workflow is not a required check). No `push`, `schedule`,
  `pull_request_target`.
- Concurrency: schedule/dispatch share the group `data-quality-alerts` (queued, not cancelled) so two runs
  do not create Issues at the same time; each PR has its own group.
- DuckDB is pinned `duckdb==1.5.6` like `pipeline.yml`/`probe.yml` (MotherDuck rejects newer clients).
- No `${{ }}` in `run:`; secrets reach steps only via `env:`.

## Tests

| File | Covers |
|---|---|
| `tests/test_quality.py` | each fault check on hand-built rows (caught + clean rows, including a clean event straddling an hour boundary), `score`, volume, freshness, counts from a temporary DuckDB file, landing manifest, `build_issue` comparing headings with the template file, `publish` with a fake `gh`, a threshold changed in a temporary config file changes the conclusion, the `runs.csv` row, CLI exit codes. No Spark, no network |
| `tests/test_quality_faults_spark.py` (`@pytest.mark.spark`) | the 6 simkit fault types through a real `run_batch`: each type is fully caught by its check, 0 false alarms; a clean run alone → 0 findings; re-running the same hours → the same findings |
| `tests/test_data_quality_workflow.py` | static check of the workflow (triggers, permissions, secrets, dry-run on PRs, artifact does not contain the lake) |
