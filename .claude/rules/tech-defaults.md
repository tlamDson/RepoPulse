# Tech defaults — RepoPulse

> Stack, layout, data rules, pitfalls and technical debt. The plan and the reasons for choosing the stack: `docs/plan.md`.

## Stack

| Layer | Technology | Notes |
|---|---|---|
| Language | Python 3.11 | `pyproject.toml`; lint/format with `ruff` (line-length 100) |
| Test | `pytest` | `tests/`, see `.claude/rules/tdd.md` |
| Orchestration | GitHub Actions | Cron only runs on `main`, can be delayed, and is switched off automatically after 60 days of repo inactivity |
| Orchestration (alternative measured next to cron) | Airflow 3.4.0 (`airflow standalone` on the runner, separate venv) | `dags/`, pure spec in `repopulse/dagspec.py`; Linux only (`.github/workflows/airflow.yml`), not in the `pyproject.toml` extras; `docs/airflow.md` |
| Bronze/Silver | PySpark + Delta Lake | Databricks Free Edition (serverless, batch `availableNow` only). Fallback: run on the Actions runner (4 CPU, 16 GB RAM, 14 GB disk) |
| Streaming (phase 2) | Apache Kafka (`apache/kafka`, KRaft, 3 brokers, docker compose) + Spark Structured Streaming + Delta | Only in the `streaming.yml` workflow on the runner; client `confluent-kafka` (extra `streaming`, lazy import); trigger `availableNow`; `docs/streaming.md` |
| Gold | dbt | `transform/`; CI runs on DuckDB (`--target ci`); real gold on a Databricks SQL warehouse or MotherDuck |
| Semantic layer | MetricFlow (ships with dbt-core >= 1.12) | `transform/models/public/_semantic.yml` (spec `semantic_models:` + `metrics:`) over the 6 public marts; `repopulse/semantic.py` generates DuckDB SQL; `docs/semantic-layer.md` |
| Dashboard | Static page on GitHub Pages + Tableau Public (published by hand) | Databricks dashboards cannot be shared publicly |
| SQL agent (AI2) | `repopulse/sql_agent.py`: an LLM through an OpenAI-compatible API (`urllib`, no SDK; locally usually `llama-server`), DuckDB read-only on `main_public` | Local DuckDB file only, no MotherDuck; guard based on the DuckDB parser (`json_serialize_sql`); CI only uses a fake LLM; `docs/sql-agent.md` |
| Security | pre-commit (`ruff`, `gitleaks`), secret scanning + push protection, Dependabot (`pip`, `github-actions`) | |

Probe number 1 (`docs/plan.md`) decides the primary path (Databricks) or the fallback path (runner + MotherDuck). The result is recorded in `docs/progress.md`; update the table above once it is settled.

## Layout

```
repopulse/        main Python package (runs.csv, orchestrator.py = the pure core of the orchestrator)
eventspec/        YAML event catalog → JSON Schema, event validation function   (PR 2)
simkit/           seeded simulated data generator, injects known faults          (PR 3)
benchmarks/       reference data day, naive pipeline, queries/q1..q6             (PR B)
tests/            pytest; fixtures/ holds small sample data
transform/        dbt project (dbt_project.yml, profiles.yml, models/, seeds/)
site/             static dashboard (plain HTML/CSS/JS) for GitHub Pages; export_site.py writes data/ + tableau/ into _site/ (PR 11)
scripts/          orchestrate.py (headless CLI), prs.toml (DAG of tasks → agent → gate)
dags/             Airflow DAG (repopulse_pipeline.py, built from repopulse/dagspec.py); do not name the directory airflow/ (P2d)
.github/workflows CI (ci.yml), benchmark (PR B), scheduled pipeline (PR 7), Pages deploy (PR 11), Airflow backfill (P2d)
.claude/          rules, agents, hooks/log_permission.py, auto-mode.json; state/ and worktrees/ are not committed
docs/             plan, progress, checklist, harness-check
logs/             stream-json log per PR, report per run (not committed)
runs.csv          measurement ledger, one row per run
```

Update this diagram when adding a new directory.

## Data rules

- **Measurements come only from `runs.csv`.** Do not quote estimates, "about" figures, or manual runs that were not recorded.
- **Simulated data is clearly labelled "simulated"** in the README, dashboard and PR descriptions. GH Archive is real data.
- **Hash PII before silver.** Gold never contains raw email/PII values; an automated test checks this (PR 9).
- **Idempotent per (date, hour).** Rerunning the same data hour produces no duplicate records.
- **Events with a wrong schema go to a quarantine table**, never silently dropped.
- Use the right names: phase 1 is "incremental batch", never called real-time/streaming.

## Secrets

`DATABRICKS_HOST`, `DATABRICKS_TOKEN`, `DATABRICKS_HTTP_PATH`, `MOTHERDUCK_TOKEN`, `PII_HASH_SALT` (the salt for PII hashing at silver), and for PR 13: `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER` (`REPOPULSE_CI`, a SERVICE user, logging in with a key pair), `SNOWFLAKE_PRIVATE_KEY` (PKCS#8 PEM), `SNOWFLAKE_ROLE`, `SNOWFLAKE_WAREHOUSE` (`REPOPULSE_WH`, XSMALL, auto-suspend 60s, resource monitor 20 credits), `SNOWFLAKE_DATABASE` (`REPOPULSE`) — the repo owner adds them with `gh secret set` or on the web. Agents do not read `.env`, do not print secret values, and do not run `gh secret` (that command is in the `ask` group). New variables are added to `.env.example` with an empty value.

Local environment variables (not GitHub secrets) for the orchestrator's daily report email: `REPOPULSE_SMTP_USER`, `REPOPULSE_SMTP_APP_PASSWORD` (a Gmail App password), `REPOPULSE_REPORT_TO` (optional). The repo owner sets them with `setx`; the script only hands the values to `smtplib` and never prints them. Agents do not read or print these variables.

Local environment variables for the SQL agent (`python -m repopulse.sql_agent`, `docs/sql-agent.md`): `REPOPULSE_LLM_BASE_URL` (default `http://127.0.0.1:8080/v1`), `REPOPULSE_LLM_MODEL`, `REPOPULSE_LLM_API_KEY` (optional). The module only reads from the environment, not from `.env`, and never puts the key in errors/logs/reports. Agents do not read or print `REPOPULSE_LLM_API_KEY`.

## Known pitfalls

- **Databricks Free Edition**: official documentation has not confirmed creating tokens/external control; internet access is limited to a domain list (unknown whether `data.gharchive.org` is allowed); exceeding the quota switches compute off until the end of the day; at most 5 concurrent job tasks. Keep the schedule sparse and jobs idempotent so the next day can catch up.
- **14 GB runner disk**: each run only processes a few hours of data and filters event types before storing.
- **Snowflake 30-day trial**: only sign up when PR 13 is reached.
- **Windows**: open sessions with an uppercase `D:` (see `developer-guide.md`). The `ruff format` hook needs `ruff` installed in the Python that the `python` command points to; if it is not installed, the hook silently skips.
- **Spark on Windows**: PySpark needs JDK 17 (the machine has Java 24, unsupported) and writing Delta needs `winutils`. Spark tests are marked `@pytest.mark.spark`, skip themselves locally when these are missing, and always run on Linux CI.
- **Hard-killing a PySpark process**: the JVM is a child process of Python; a SIGKILL of Python alone makes the JVM see stdin closed and exit through a shutdown hook (not a crash). For a real crash: run the child process with `start_new_session=True`, then `os.killpg(pid, SIGKILL)` (POSIX only; see `ConsumerProcess` in `benchmarks/streaming_fault.py`, `docs/streaming-fault.md`).
- **Auto mode ignores broad allow rules** (`Bash(python *)`, `npm *`, `npx *`, allowing `Agent`/`Monitor`); allow rules must be specific. `autoMode` is not read from `.claude/settings*.json` — it lives in `.claude/auto-mode.json`, loaded with `--settings`.
- **Writing into `.claude/` (outside `worktrees/`) is a protected path**: allow rules cannot open it, and auto mode blocks self-editing configuration. Harness changes are approved by the repo owner.
- **Quota**: stream-json has `rate_limit_event` (`status: rejected`, `resetsAt` epoch seconds); with `errorCode: credits_required`, waiting is pointless. The orchestrator sets `CLAUDE_CODE_RETRY_WATCHDOG=1` so the 5-hour limit is waited out inside the session, and uses `--resume <session_id>` when a session exits because of the limit. `autoContinueAtUsageLimit` does not apply to `-p`.
- **Mutation on an uncommitted file**: `git checkout --` cannot revert an untracked file; only try mutations on committed files.

## Technical debt

(each item has three parts: Symptom, Cause with `file:line`, Trigger condition — see `workflow.md`)

- None.
