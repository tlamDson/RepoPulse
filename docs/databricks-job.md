# Bronze/silver job on Databricks serverless (D1)

Task D1 of Plan v2 (`docs/plan.md`): run the **same logic** of bronze → silver in
`repopulse/spark/` (`repopulse.spark.batch.run_batch`) as a serverless job on
Databricks, writing Delta tables in Unity Catalog. This is an **incremental batch** by (date, hour),
not streaming or real-time. The main path of the current deployment is still the fallback (Spark on
the runner, gold on MotherDuck, `docs/probe-result.md`); this job is a parallel run for comparison.

The data is real GH Archive (not simulated).

## Run path

```
GitHub Actions runner                         Databricks (serverless)
---------------------                         -----------------------
python -m repopulse.ingest  -> data/landing/gharchive/date=.../hour=.../
pip wheel --no-deps         -> dist/repopulse-<version>-py3-none-any.whl
python -m repopulse.databricks_job run
  1. Unity Catalog: schema + managed Volume  (skipped if they exist)
  2. PII salt -> secret scope                (the job carries only the scope/key names, not the value)
  3. Files API: landing + wheel -> Volume    (overwrite=true)
  4. Workspace API: import notebook          (%pip install wheel, calls databricks_entry.run)
  5. Jobs API: runs/submit (serverless, idempotency_token) -> wait -> runs/get-output (JSON summary)
  6. --verify-rerun: submit the same hour again, silver must add 0 rows
  7. one runs.csv row (variant=databricks-bronze-silver) -> out/databricks-row.csv
```

- The client (`repopulse/databricks_job.py`) uses only the standard library; all HTTP goes through an injectable `transport`
  so unit tests never call the real Databricks (`tests/test_databricks_job.py`).
- The notebook calls `repopulse.spark.databricks_entry.run`, which validates parameters and then calls
  `run_batch(..., catalog=..., schema=..., persist=False)`: serverless is Spark Connect, with no
  `persist` and no path-based Delta API, so tables are read/written by name.
- A failed run: the error message includes `state_message` and the task's `error` line (taken from
  `runs/get-output`, truncated); every message goes through `Redactor` (host, token, user, salt).

## Running

Workflow `.github/workflows/databricks-bronze-silver.yml`:

- `workflow_dispatch` (only appears in the Actions tab once the file is on `main`): inputs `date`, `hour`,
  `hours` are all optional. If `date` is empty, the benchmark reference day is used
  (`benchmarks/reference.toml`, `data_date`), last hour 23, 3 hours.
- `push` to the branch `feature/databricks-bronze-silver` (filtered by `paths`) so it can run within the PR, always
  with the reference-day window above. There is no `pull_request`. After the branch is deleted on merge this trigger
  has no effect.
- Missing `DATABRICKS_HOST`: the Databricks step is skipped at step level, with a `::notice`, the workflow stays green,
  and there is no `runs.csv` row.
- Path B of `.github/workflows/path-comparison.yml` (D3, `docs/path-comparison.md`) calls the same `run`
  command (without `--verify-rerun`) on an hourly schedule after release, on the same
  `workspace.repopulse.*` tables. That job shares the concurrency group `databricks-bronze-silver` with
  this workflow, so the two never write silver at the same time.

Manual run (needs the three environment variables below, an ingested landing directory and a wheel):

```bash
python -m repopulse.ingest --date 2026-10-25 --hour 23 --hours 3 --landing-dir data/landing/gharchive --summary out/ingest-summary.json
python -m pip wheel --no-deps --wheel-dir dist .
python -m repopulse.databricks_job run --source gharchive --date 2026-10-25 --hour 23 --hours 3 --landing-dir data/landing/gharchive --wheel dist/repopulse-0.0.1-py3-none-any.whl --git-sha <sha> --verify-rerun --summary out/databricks-summary.json --out out/databricks-row.csv
```

Other flags: `--catalog` (default `workspace`), `--schema` (`repopulse`), `--volume`
(`repopulse_landing`), `--secret-scope` (`repopulse`), `--secret-key` (`pii_hash_salt`),
`--late-window-hours`, `--job-timeout` (seconds per run; the workflow sets 1800), `--poll-interval`,
`--add-mask` (prints `::add-mask::` for the hostname and user name, used on GitHub Actions). Exit codes: 0
success, 1 error (missing variable, API error, failed run, re-run added rows), 2 bad parameters.

## Secrets

| Variable | Used for |
|---|---|
| `DATABRICKS_HOST` | workspace URL; if empty the workflow skips the Databricks step |
| `DATABRICKS_TOKEN` | personal access token (Bearer) for the REST API |
| `PII_HASH_SALT` | SHA-256 salt for hashing PII at silver; the client puts it into the secret scope, never into the job payload |

The repo owner adds the secrets. Secrets enter the workflow only through a step's `env:`, and no step prints them.

## Unity Catalog tables

Default `workspace.repopulse` (`repopulse/spark/tables.py`):

| Table | Content | Partition |
|---|---|---|
| `bronze_gharchive_events` | each landing line verbatim (`raw_json`, `raw_sha256`) + provenance | `data_date` |
| `quarantine_gharchive_events` | reason + error message + provenance, no payload | `data_date` |
| `silver_gharchive_events` | valid events, deduplicated by `event_id`, commit author email/name hashed | `event_date` |

Volume `repopulse_landing`: `landing/gharchive/date=YYYY-MM-DD/hour=HH/` (same layout as
`repopulse.ingest`) and `wheels/<git_sha>/`. The notebook lives at `/Users/<user>/repopulse/`.
The classification, dedup, late-arrival window and PII hashing rules are identical to the runner version:
`docs/spark-jobs.md`.

## Idempotent by (date, hour)

- The Files API writes with `overwrite=true` to a path that depends only on (source, date, hour, file name).
- Bronze and quarantine overwrite exactly the (date, hour) slices of the run with `replaceWhere`.
- Silver uses a MERGE that only inserts keys not yet present, so re-running the same hour adds 0 rows; `--verify-rerun`
  checks this on real Databricks, and if the re-run adds rows it exits 1 and writes no `runs.csv` row.
- `runs/submit` carries an `idempotency_token` built from (source, date, hour, number of hours, run key); the run key is
  `GITHUB_RUN_ID-GITHUB_RUN_ATTEMPT`, so a request resent because of a retry does not create a second run.

## Free Edition limits

- Serverless compute only, batch only; when the quota is exceeded compute is off until the end of the day, and the next day re-running
  the same hour is safe because the job is idempotent.
- At most 5 concurrent job tasks; the workflow uses one `concurrency` group and does not cancel a running run.
- Internet access follows a domain allow-list: that is why the runner downloads GH Archive and pushes it to the Volume, and the job
  does not download it itself.
- The 14 GB runner disk limits the number of hours per ingest (`repopulse.ingest` at most 6 hours).

## Merging measurements into `runs.csv`

The artifact `databricks-bronze-silver` contains `out/databricks-row.csv`, `out/databricks-summary.json`,
`out/ingest-summary.json`. Download the artifact, then:

```bash
python benchmarks/record_run.py out/databricks-row.csv
python -m repopulse.readme_results
```

The row has `variant=databricks-bronze-silver`; `total_seconds` is from submit until the first run ends,
`row_count` is the number of silver rows inserted; `notes` records the bronze/quarantine row counts, the number of duplicate rows
dropped, the upload time, the time of each phase of the run (if the API returns it), and `rerun_silver_inserted`.
Measurements are only quoted from `runs.csv`.
