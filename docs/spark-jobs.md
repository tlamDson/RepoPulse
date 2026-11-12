# Spark jobs: bronze → silver (incremental batch)

`repopulse/spark/` turns the hours that have landed (`repopulse.ingest` for GH Archive, landing for app events)
into the Delta tables bronze / silver / quarantine. This is an **incremental batch** (each run processes a
list of landed (date, hour) pairs), not streaming or real-time. The chosen path is the fallback:
PySpark 3.5 + Delta 3.3 running locally on a GitHub Actions runner (4 CPU, 16 GB RAM, 14 GB disk),
not on Databricks.

- GH Archive is real data (public GitHub events).
- App events are **simulated** data (a simulated trip-planning app, `eventspec` catalog),
  not real users; the silver table has a `simulated` column (always `true`).

## Running

```bash
pip install -e ".[dev,spark]"     # needs Java 17 (PySpark 3.5); Windows also needs winutils
PII_HASH_SALT=<salt> python -m repopulse.spark.job --source gharchive --date 2026-01-05 --hour 15 --hours 2 --summary out/spark-summary.json
python -m repopulse.spark.job --source app --date 2026-01-05 --hour 15   # landing defaults to data/landing/app
```

| Parameter | Default | Notes |
|---|---|---|
| `--source` | (required) | `gharchive` or `app` |
| `--date`, `--hour` | (required) | the **last** hour (UTC) |
| `--hours` | 1 | 1..24 consecutive hours ending at `--hour` (same as `repopulse.ingest`) |
| `--landing-dir` | `data/landing/gharchive` / `data/landing/app` | |
| `--lake-dir` | `data/lake` | |
| `--late-window-hours` | 24 | window for accepting late events |
| `--summary` | | write a JSON summary |

`PII_HASH_SALT` is required and has no default; if missing or empty the job exits with code 1 **before**
starting Spark. The salt is never printed to the log or summary (the silver hash runs in a Python UDF,
so the salt is not in the query plan).

Session: `local[*]`, `spark.sql.shuffle.partitions=8`, session time zone UTC, 8 GB driver.

## Landing input

```
<landing>/date=YYYY-MM-DD/hour=HH/
    events.json.gz         GH Archive (repopulse.ingest), read by Spark by file path
    _quarantine.jsonl.gz   GH Archive, lines already rejected by ingest; read in Python on the driver
    events.jsonl.gz        app events (simulated)
```

Spark skips files whose names start with `_`/`.` when listing a directory, so `_quarantine.jsonl.gz` is read on the
driver. Files are read by explicit path so `date=`/`hour=` are not inferred as partition columns.
An hour with no landing file yields no rows for that hour.

## Tables (`<lake>/…`)

| Table | Partition | Columns |
|---|---|---|
| `bronze/gharchive_events`, `bronze/app_events` | `data_date` | `raw_json` (verbatim), `raw_sha256`, `data_date`, `hour`, `source_file`, `line_number` (from 1, per file), `ingested_at` |
| `silver/gharchive_events` | `event_date` | `event_id`, `event_type`, `created_at` (timestamp), `event_date`, `actor_id`, `actor_login`, `repo_id`, `repo_name`, `action`, `pr_number`, `issue_number`, `pr_merged`, `commit_author_email_hashes`, `commit_author_name_hashes`, `landing_date`, `landing_hour`, `is_late` |
| `silver/app_events` (simulated) | `event_date` | `dedup_key`, `event_id`, `event_name`, `event_version`, `occurred_at`, `received_at`, `user_id`, `session_id`, `simulated`, `event_date`, `properties` (JSON of event-specific fields, PII hashed), `landing_date`, `landing_hour`, `is_late` |
| `quarantine/gharchive_events`, `quarantine/app_events` | `data_date` | `reason`, `errors`, `raw_sha256`, `data_date`, `hour`, `source_file`, `line_number`, `event_type` / `event_name` |

Bronze is a restricted raw zone: it may keep raw PII. Silver and quarantine **do not** keep the raw payload; to
see the original line of a quarantine record, look up bronze by `raw_sha256`.

`pr_number` = `payload.pull_request.number`, falling back to `payload.number`; `issue_number` =
`payload.issue.number` (Q4/Q6 coalesce these two columns).

## Rules

**Schema → quarantine** (`reason`, with detailed `errors`):

| reason | When |
|---|---|
| `malformed_json` | GH: every line in the landing's `_quarantine.jsonl.gz`; app: a line that cannot be parsed as JSON |
| `schema_invalid` | GH: missing `id`/`type`/`actor.id`/`repo.id`/`repo.name`, id is not an integer, `created_at` is not RFC 3339; app: `eventspec.validate` reports errors (messages kept verbatim) |
| `unknown_event` | app: unknown `event_name` or undeclared `event_version` |
| `late_beyond_window` | arrived later than the window |

**Dedup.** GH by `id`; app by `eventspec.dedup_key` (`event_id`, otherwise a hash of
`dedup_fields`). Within a batch, keep the first by (`data_date`, `hour`, `source_file`,
`line_number`). Against existing silver, the copy in silver wins: MERGE only inserts keys not yet present
(GH silver prunes partitions by the batch's `event_date`s). Duplicates do not go to quarantine, they are only counted in
`duplicates_dropped`.

**Late arrival** (`repopulse.spark.rules.classify_arrival`). Arrival: GH = start of the landing hour (UTC); app =
`received_at`, or the start of the landing hour if absent. Event time: GH `created_at`, app `occurred_at`. An event is
late when it falls in an earlier UTC hour than the arrival's hour; if late and `arrival - event_time <= window` (equal to the
window is still accepted) it goes into silver in the event's `event_date` partition with `is_late = true`; beyond the window it goes to
quarantine as `late_beyond_window`.

**PII.** Hashed before writing silver: `sha256(salt + value.strip().lower())` hex
(`repopulse.spark.pii.hash_pii`). GH: PushEvent `payload.commits[].author.email` and `.name`
remain only as arrays of hashes. App: every catalog field with `pii: true` (`email`, `recipient_email`) is
replaced by its hash in `properties`.

**Idempotent.** Bronze and quarantine overwrite exactly the (date, hour) slice with `replaceWhere`; silver only
inserts new keys. Re-running the same hour does not change the row count of any table.

## Summary

```json
{"source": "gharchive", "hours": [["2026-01-05", 10]], "bronze_rows": 0, "silver_inserted": 0,
 "duplicates_dropped": 0, "quarantined": {"schema_invalid": 0}, "quarantined_total": 0,
 "late_accepted": 0}
```

(The values above only illustrate the shape, they are not measurements.) With an empty lake:
`bronze_rows == silver_inserted + duplicates_dropped + quarantined_total` — no row is dropped
silently. `late_accepted` is the number of late events inserted into silver.

## Exporting silver for dbt

```bash
python -m repopulse.spark.export --lake-dir data/lake --out data/silver-export --summary out/export-summary.json
```

Reads `silver/gharchive_events` **through Delta** (not by globbing the Delta directory directly, since that skips
`_delta_log`) and overwrites `--out` with plain parquet, exactly the 17 silver columns in the order of the table above,
timestamps as `TIMESTAMP_MICROS` (UTC). If the lake has no silver yet (every hour missing), it writes an
empty parquet that still carries the schema, with summary `{"silver_table": "missing", "rows": 0, ...}`. dbt reads the result through the var
`silver_gharchive_parquet` (`docs/dbt-models.md`); the whole scheduled chain is in `docs/pipeline.md`.

## Tests

`tests/test_spark_*.py`: the pure-Python parts (`pii`, `rules`, CLI) run on every machine; tests marked
`@pytest.mark.spark` use the `spark` fixture (`local[1]`) in `tests/conftest.py`, skip automatically locally when
pyspark/delta, Java 17 or winutils is missing, and always run on CI (the `Unit Tests` job installs JDK 17).
