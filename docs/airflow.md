# Airflow DAG: backfill, retry, SLA

`dags/repopulse_pipeline.py` is the orchestration option using **Airflow 3.4.0** for the same
**incremental batch** chain as `.github/workflows/pipeline.yml` (not streaming or real-time):

```
ingest -> spark_bronze_silver -> export_silver -> dbt_build
```

The cron of `pipeline.yml` stays as it is: the two orchestration approaches are measured side by side. Replacing the cron
outright (if ever) is a separate task. GH Archive is real data.

## Structure

| File | Role |
|---|---|
| `repopulse/dagspec.py` | Pure spec (stdlib, does not import `airflow`): tasks, bash commands, retry, timeout, SLA, backfill window, `list-runs` parser, wait loop, `runs.csv` row. Tests in `tests/test_dagspec.py` |
| `dags/repopulse_pipeline.py` | Builds the Airflow DAG from the spec (`BashOperator`, `DeadlineAlert`). DagBag test in `tests/test_airflow_dag.py` |
| `.github/workflows/airflow.yml` | `airflow standalone` on a runner, backfill N days, writes the measurement row to an artifact |

The directory is `dags/`, never named `airflow/` (it would shadow the real `airflow` package because pytest runs
with `pythonpath = ["."]`).

## DAG

- `schedule="@daily"`, `catchup=False`, `max_active_runs=1`, `start_date=2025-01-01`.
- **The run for logical date D processes the data of day D − 1** (`{{ macros.ds_add(ds, -1) }}`): that day is complete
  when the run starts. Airflow 3 uses `CronTriggerTimetable` by default, so `ds` is the day the run is
  triggered.
- Each run processes only `REPOPULSE_AIRFLOW_HOURS_PER_DAY` hours (default **1**) ending at hour
  `REPOPULSE_AIRFLOW_HOUR` (default 12) of the data day: the runner disk is 14 GB, and one full GH Archive hour
  is already heavy. This is a sample of the day, not the whole day; the measurement row records `hours_per_day=` and
  `last_hour=` explicitly.
- Task commands are identical to `pipeline.yml`, run with the project's Python (`REPOPULSE_PYTHON`) with
  `cwd` at the repo root (`REPOPULSE_HOME`). Each day's summaries are written to `out/airflow/<date>/`.
- Idempotent by (date, hour): ingest skips hours that already have a manifest, silver dedups by event id, dbt
  staging keeps one row per event. The DAG adds no new state.

### Retry

| Task | retries | retry_delay | backoff | timeout |
|---|---|---|---|---|
| `ingest` | 3 | 120 s | exponential | 30 minutes |
| `spark_bronze_silver` | 1 | 60 s | none | 45 minutes |
| `export_silver` | 1 | 60 s | none | 15 minutes |
| `dbt_build` | 1 | 60 s | none | 30 minutes |

`ingest` adds `python -m repopulse.dagspec check-ingest`: an hour that is `missing` (GH Archive has not published it)
or `error` makes the task fail, so a retry with backoff waits for the file to appear, instead of letting Spark run on
an empty landing.

### SLA = Deadline Alerts

Airflow 3 removed `sla=` and `sla_miss_callback`. The DAG uses a **Deadline Alert** (since 3.1, still
experimental): `DeadlineAlert(reference=DeadlineReference.DAGRUN_QUEUED_AT,
interval=REPOPULSE_AIRFLOW_SLA_MINUTES (default 30 minutes), callback=AsyncCallback(...))`.
The callback is `repopulse.dagspec.record_sla_miss` (a top-level `async` function the Triggerer can import),
which writes one JSON line to `REPOPULSE_SLA_MISS_FILE` (default `out/airflow/sla-miss.jsonl`).

`AsyncCallback` was chosen because it exists from the first version of Deadline Alerts and does not occupy an executor slot.
With a backfill, all runs are queued at the same time so `DAGRUN_QUEUED_AT` includes the time spent waiting for the
previous runs: the callback may report lateness even if a run was fast. So the **`sla_missed` count in the measurement row does not come from
the callback** but is computed from run duration (`end_date − start_date > SLA`) in
`airflow dags list-runs`.

## Backfill on a runner

`.github/workflows/airflow.yml`:

| Trigger | Data |
|---|---|
| `workflow_dispatch` (only when the file is on `main`, i.e. after release) | inputs `days` (default 3, max 7), `last_data_date` (default 2026-10-25), `hour` (15), `hours_per_day` (1) |
| `pull_request` (filtered by `paths:`, not a required check) | 2-day smoke up to 2026-10-25, hour 15, 1 hour/day (fixed so it is repeatable) |

Steps: install the project (`.[dev,spark,dbt]`), create a **separate venv** for Airflow (`apache-airflow==3.4.0`
with the 3.11 constraints, plus `pip install --no-deps -e .` so the DAG can import `repopulse.dagspec`),
test the DagBag with `REPOPULSE_REQUIRE_AIRFLOW=1` (a missing Airflow fails), `dagspec window` (sets
`BACKFILL_FROM/TO` and `REPOPULSE_AIRFLOW_END_DATE`), `airflow db migrate`, `airflow standalone`
running in the background, wait for the DAG to load, `airflow dags unpause`, `airflow backfill create --max-active-runs 1`,
`dagspec wait` (polls `airflow dags list-runs -o json`), `dagspec row`.

Three lessons from real runs on the runner (Airflow 3.4.0):

- `airflow standalone` starts the scheduler, dag processor, triggerer and API server using the bare command
  `airflow`, so the `bin/` of the Airflow venv must be in `PATH` (appended at the **end**, so `python` is still the
  project's interpreter). The startup step waits for the line `Airflow is ready` in `standalone.log` and
  fails after 5 minutes if it does not appear, instead of letting the backfill wait step hang until the timeout.
- `AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_ALL_ADMINS=True`: the temporary localhost instance needs no
  login, so no auto-generated password is printed into the uploaded log.
- `airflow dags list-runs -o json` prints log lines (with `[warning  ]`) to stdout before the JSON array;
  `dagspec.parse_runs` skips those lines. The output has no `run_type`: backfill runs are recognized
  by a `run_id` starting with `backfill__`.

`REPOPULSE_AIRFLOW_END_DATE` sets the DAG's `end_date` to noon of the last logical day: the backfill runs (at
midnight) fall within the range, while the scheduler does not create scheduled runs on its own when the DAG is unpaused, so no
run interleaves with the backfill.

dbt writes to local DuckDB (`--target ci`) with the fake salt `not-a-secret-pr-salt`; the workflow needs no
secrets and does not touch MotherDuck. The job fails when a backfill run is `failed` or the wait times out
(`25 × days` minutes).

Artifact `airflow-backfill-<run_id>`: `out/` (the measurement row, `backfill-summary.json`, the raw JSON of
`list-runs`, per-day summaries, `sla-miss.jsonl` if any) and `out/airflow/airflow-logs.tar.gz`
(`standalone.log` + Airflow task logs; packed because task log paths contain `:` which
`upload-artifact` does not accept).

Equivalent manual commands (Linux; Airflow does not run on Windows):

```bash
python -m repopulse.dagspec window --last-data-date 2026-10-25 --days 3      # prints BACKFILL_FROM/TO...
airflow backfill create --dag-id repopulse_pipeline --from-date <FROM> --to-date <TO> --max-active-runs 1
python -m repopulse.dagspec wait --last-data-date 2026-10-25 --days 3 --started-at <ISO> \
  --timeout-minutes 75 --summary out/airflow/backfill-summary.json --raw out/airflow/list-runs.json
python -m repopulse.dagspec row --summary out/airflow/backfill-summary.json --run-at <ISO> \
  --git-sha <sha> --run-id <id> --trigger manual --out out/airflow-backfill-row.csv
```

## Measurement row

`variant=airflow-backfill-<N>d`, `data_date` = last data day, `total_seconds` = wall clock from
creating the backfill until the last run finishes (including Airflow's scheduling latency), `row_count` = total
`silver_inserted` across the days (empty if a day's summary is missing), `notes`:
`engine=airflow-3.4.0 days= hours_per_day= last_hour= runs_success= runs_failed= sla_minutes=
sla_missed= max_active_runs= target=ci run_id= trigger=`. Only a successful backfill produces a row.

Merge into `runs.csv` from the artifact:

```bash
python benchmarks/record_run.py out/airflow-backfill-row.csv
python -m repopulse.readme_results
```

## Limitations

- Airflow standalone + SQLite is for development only; it is not a real deployment.
- Deadline Alerts are still experimental; the API may change between 3.x releases.
- Only 1 hour of data per day (default): the measurement is of orchestration + the processing chain, not of processing a whole
  GH Archive day.
- Airflow is not in the extras of `pyproject.toml` (constraints conflict with dbt/pyspark): the pinned version
  lives in the workflow.
