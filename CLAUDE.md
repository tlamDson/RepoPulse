# RepoPulse — Claude Code Project Instructions

RepoPulse: a small data platform that measures the "health" of open source projects in the data ecosystem (Spark, dbt, DuckDB, Airflow, Kafka...) from public GitHub event data (GH Archive), together with a simulated app event stream that follows a logging spec. Stack: Python 3.11, PySpark + Delta Lake (Databricks Free Edition, or running on a GitHub Actions runner on the fallback path), dbt (DuckDB on CI, Databricks SQL or MotherDuck for gold), GitHub Actions for orchestration, a static dashboard on GitHub Pages.

**Path currently running: A (fallback)** — PySpark + Delta on a GitHub Actions runner, gold on MotherDuck (the DuckDB version is pinned to the one MotherDuck supports). The probe rerun with the correct personal access token (2026-11-11) came out `primary`: Databricks can be controlled from GitHub Actions. Path A still runs, per the Plan v2 path-selection rule (`python -m repopulse.pathdecision`), because `runs.csv` does not yet have enough scheduled runs to apply the rule. Details and when to reapply it: `docs/probe-result.md`.

Plan: `docs/plan.md`. Progress: `python scripts/orchestrate.py report` (the source of truth is merged PRs). Items waiting on the repo owner: `docs/lam-checklist.md`, `logs/run-<date>.md` and the daily report email (`logs/daily-<date>.md`; operational measurements are in `logs/orchestrator-metrics.csv`, not `runs.csv`).

The detailed rules below always apply, not only when asked:

@.claude/rules/tech-defaults.md
@.claude/rules/workflow.md

## Mandatory rules (must not be violated)

- **Git flow:** feature → `develop` (squash), release `develop` → `main` (merge commit). No direct push to `develop`/`main`, no force push, no `--admin`. `main` is the default branch (cron only runs there).
- **One PR, one task.** Each feature/fix/docs PR does exactly one thing: one task in `scripts/prs.toml` (always counted as one task, exactly as described), or one improvement/bug fix outside `prs.toml`. Other things found along the way are not merged in: a manual session opens a separate PR, a headless session records them under Technical debt or `## Assumptions`. Release PRs and Dependabot PRs are exempt. Details: `.claude/rules/developer-guide.md`.
- **Merge a feature into `develop` when CI is green on all 3 jobs** (`Lint`, `Unit Tests`, `dbt build (DuckDB)`) **and `code-reviewer` is PASS**. When both hold, merge immediately without asking again.
- **Release `develop` → `main`** at the `release_after` milestones in `scripts/prs.toml`, once the release PR's CI is green.
- **A PR that changes `.claude/settings.json`, `.claude/auto-mode.json` or a hook is reviewed by the repo owner**, never self-merged.
- **Command style follows `.claude/rules/command-style.md`** — a headless session immediately refuses any command that would normally have to ask.
- **TDD is mandatory** (Red → Green → Refactor): the failing test must be seen for real, and the red output pasted into the `## RED` section of the PR description. Details: `.claude/rules/tdd.md`.
- **Measurements come only from `runs.csv`.** The README, PR descriptions and reports must not quote any number that is not in `runs.csv`. Every optimization is compared against `variant=baseline` on the same data day and the Q1–Q6 query set.
- **Simulated data must be clearly labelled as simulated** ("simulated"). GH Archive is real data.
- **Never read or print secret values** (`DATABRICKS_*`, `MOTHERDUCK_TOKEN`, `SNOWFLAKE_*`, the `.env` file). The repo owner adds secrets themselves.
- **Update `CLAUDE.md` + `.claude/rules/*`** when a task changes a convention, tooling or process.
- **Do not create new `.md` files in the root directory** — only `README.md` and `CLAUDE.md`. Other documentation goes in `docs/`.

## Common commands

On the repo owner's Windows machine, `pytest`, `ruff` and `dbt` are not on the PATH: always call them as `python -m …` as below (Linux CI still uses the bare commands in `ci.yml`).

```bash
pip install -e ".[dev,dbt]"          # install once
pre-commit install                   # ruff + gitleaks hooks
python -m ruff check .
python -m ruff format --check .
python -m pytest
python -m dbt.cli.main build --project-dir transform --profiles-dir transform --target ci
python -m dbt.cli.main build --project-dir transform --profiles-dir transform --target ci --vars "{silver_gharchive_parquet: '<glob>/*.parquet'}"   # read silver parquet instead of the seed fixture (docs/dbt-models.md)
python -m pytest tests/test_dbt_marts.py         # Q1-Q6 marts match benchmarks/queries/q*.sql, contract, PII
python -m pytest tests/test_dbt_semantic_layer.py   # MetricFlow semantic layer: dbt parse checks the semantic manifest, Q1-Q6 metrics match q*.sql (docs/semantic-layer.md)
python -m repopulse.semantic sql --metrics cohort_size,retention_w4_pct --group-by q2_cohort__cohort_week__week   # DuckDB SQL of a metric from transform/target/semantic_manifest.json (after dbt build/parse)
python -m repopulse.governance scan --db ci.duckdb --schema main_gold --schema main_public   # raw email in gold/public? exit 0 clean, 1 has findings (docs/governance.md)
python -m repopulse.governance check --manifest transform/target/manifest.json --policy transform/governance.yml --db ci.duckdb   # meta.pii labels + role permissions
python -m repopulse.sql_agent ask --db ci.duckdb --model <model> "<question>"   # read-only SQL agent on main_public through an OpenAI-compatible LLM (REPOPULSE_LLM_BASE_URL, default llama-server 127.0.0.1:8080); exit 1 out of retries, 2 config/LLM error (docs/sql-agent.md)
python -m repopulse.sql_agent eval --db ci.duckdb --model <model> --report out/sql-agent-eval.json --out out/sql-agent-row.csv --git-sha <sha>   # question set repopulse/sql_agent_eval.toml, execution accuracy; if the LLM cannot be called, exit 2 and no row is written; merge with record_run.py
gh pr checks <PR-number> --watch --interval 30

python benchmarks/fetch_reference.py             # download + verify sha256 of the reference day (benchmarks/reference.toml)
python benchmarks/naive_duckdb.py --runs 5 --git-sha <sha> --out out/benchmark-row.csv
python benchmarks/record_run.py <benchmark-row.csv> [<pipeline-row.csv> ...]   # merge rows from artifacts into runs.csv (if one file is broken, nothing is written)
python benchmarks/scale_prepare.py               # download + filter + trim 30 days of GH Archive for the scale benchmark (docs/benchmark-scale.md)
python benchmarks/scale_bench.py --git-sha <sha> --out out/scale-rows.csv   # Spark vs DuckDB at 1/7/30 days; merge with record_run.py
python benchmarks/query_opt.py --runs 10 --git-sha <sha> --out out/query-opt-rows.csv --summary out/query-opt-summary.md   # Q1-Q6 on flat/partitioned/clustered parquet (reference day); bytes scanned can only be measured on Linux (docs/query-optimization.md)
python benchmarks/format_bench.py --runs 5 --git-sha <sha> --out out/format-rows.csv --summary out/format-summary.md   # Delta vs Iceberg on the reference day: write, Q1-Q6, MERGE (idempotent rerun), time travel; variant format-delta/format-iceberg (needs Java 17, extra spark; docs/iceberg-vs-delta.md)
python benchmarks/trino_bench.py --print-ddl     # Trino/Hive DDL of the 4 layouts (no Docker needed); the real measurement only runs in the trino-bench.yml workflow (Docker Compose on the runner, docs/trino-hive.md)
docker compose -f benchmarks/kafka/docker-compose.yml up -d --wait   # 3-broker KRaft Kafka (needs Docker; down -v to clean up)
python benchmarks/streaming_bench.py --git-sha <sha> --partitions 1,3,6 --out out/streaming-rows.csv --summary out/streaming-summary.md   # Kafka -> Spark Structured Streaming (availableNow) -> Delta, with count reconciliation; needs extra spark,streaming + Java 17 (docs/streaming.md)
python benchmarks/streaming_fault.py --git-sha <sha> --out out/streaming-fault-rows.csv --summary out/streaming-fault-summary.md   # kill a broker / kill the consumer midway: reconcile loss/duplicates by content + recovery time; an invalid measurement writes no row; needs a Kafka cluster + Docker + Java 17 (docs/streaming-fault.md)
python benchmarks/engine_matrix.py --engine duckdb --runs 5 --git-sha <sha> --out out/engine-matrix-rows.csv --summary out/engine-matrix-summary.md   # E1: Q1-Q6 on one engine, appends a matrix-<engine> row; spark needs Java 17 + extra spark; motherduck/databricks-sql/databricks-serverless only run in the engine-matrix.yml workflow; snowflake is not measured (the README reuses dbt-build-snowflake); docs/engine-matrix.md
python benchmarks/snowflake_port.py --engine duckdb --run-results transform/target/run_results.json --git-sha <sha> --out out/snowflake-port-rows.csv   # runs.csv row for a dbt build (Snowflake port, docs/snowflake-port.md); the snowflake target only runs in the snowflake-port.yml workflow
python benchmarks/dbt_databricks.py --run-results transform/target/run_results.json --show-log out/dbt-databricks-show.jsonl --git-sha <sha> --out out/dbt-databricks-rows.csv   # runs.csv row variant=dbt-build-databricks; the databricks target (D1's Unity Catalog Delta silver) only runs in the dbt-databricks.yml workflow (docs/dbt-databricks.md)
python -m repopulse.probe http-path --github-env   # SQL warehouse http path (secret DATABRICKS_HTTP_PATH or found through the API) -> DBT_DATABRICKS_HTTP_PATH in $GITHUB_ENV; only use on Actions

python -m eventspec.generate                     # regenerate eventspec/schemas/ from the catalog (docs/eventspec.md)
python -m simkit.generate --seed 42 --users 100 --days 7 --null 5 --bot 2   # simulated events + fault manifest into data/simulated/ (docs/simkit.md)

python -m repopulse.ingest --date 2026-01-05 --hour 15 --hours 2   # ingest GH Archive into data/landing/gharchive
python -m repopulse.ingest --summary out/ingest-summary.json      # hour (now UTC - 2h); --force to redo
python -m repopulse.spark.job --source gharchive --date 2026-01-05 --hour 16 --hours 2   # Delta bronze/silver (needs PII_HASH_SALT, Java 17, extra spark); docs/spark-jobs.md
python -m repopulse.spark.export --lake-dir data/lake --out data/silver-export   # silver Delta -> parquet for dbt (var silver_gharchive_parquet)
python -m repopulse.databricks_job run --source gharchive --date 2026-10-25 --hour 23 --hours 3 --landing-dir data/landing/gharchive --wheel dist/<wheel>.whl --git-sha <sha> --verify-rerun --summary out/databricks-summary.json --out out/databricks-row.csv   # the same bronze/silver running serverless on Databricks (DATABRICKS_HOST/TOKEN, PII_HASH_SALT); workflow databricks-bronze-silver.yml; docs/databricks-job.md
python -m repopulse.pipeline {window,timed,counts,row,md-reset} --help   # helpers of .github/workflows/pipeline.yml (hourly incremental batch); docs/pipeline.md
python -m repopulse.pathcompare summary --runs runs.csv --out out/path-summary.md   # D3: run count, success rate, p50/p95 (nearest-rank) end-to-end/per step/queueing of path-a-runner vs path-b-databricks, from runs.csv only
python -m repopulse.pathdecision --runs runs.csv --doc docs/probe-result.md [--check]   # D4: the Plan v2 primary-path selection rule (trigger=schedule only) -> the block between the path-decision markers of docs/probe-result.md; --check exits 1 on a mismatch; without --doc it prints to stdout
python -m repopulse.pathcompare {window,runner-waits,warehouse-start,queries,md-ensure,row} --help   # helpers of .github/workflows/path-comparison.yml (A runner+MotherDuck repopulse_path_a vs B Databricks, same data window; cron only runs after a release); docs/path-comparison.md
python -m repopulse.dagspec {window,wait,row,check-ingest} --help   # helpers of the Airflow DAG dags/repopulse_pipeline.py and .github/workflows/airflow.yml (N-day backfill, variant airflow-backfill-<N>d; Airflow only runs on a Linux runner); docs/airflow.md
python -m dbt.cli.main build --project-dir transform --profiles-dir transform --target ci --full-refresh   # rebuild fct_events (staging keeps history); when this is needed: docs/dbt-models.md
python -m repopulse.quality faults --seed 42 --users 30 --days 2 --clean-runs 2 --git-sha <sha> --report out/dq-faults.json --out out/dq-fault-row.csv   # the 6 simkit (simulated) faults through Spark; exit 1 on a missed fault/false alarm (needs PII_HASH_SALT, Java 17); docs/data-quality.md
python -m repopulse.quality check --database ci.duckdb --report out/dq-report.json   # volume + freshness of stg_gharchive_events (md:repopulse needs MOTHERDUCK_TOKEN); thresholds in repopulse/quality.toml
python -m repopulse.quality gap --ingest --report out/dq-gap.json   # a known-missing GH Archive hour must be caught (hours declared in [volume.reference])
python -m repopulse.quality alert --report out/dq-report.json --run-url <url> --dry-run   # data-incident Issue; without --dry-run it calls gh (GH_TOKEN), one open issue per check kind

python -m repopulse.export_site --db ci.duckdb --runs runs.csv --out _site   # 6 public marts + runs.csv -> site + Tableau CSV; a missing mart/column exits 1 (docs/dashboard.md)
python -m http.server 8000 --bind 127.0.0.1 --directory _site                # preview the site locally

python -m repopulse.readme_results               # regenerate the Results section of README.md (between the results:start/end markers) from runs.csv; --check exits 1 on a mismatch. If a required variant of the phase 2 sections is missing (both paths, format-delta/iceberg, streaming-throughput-p<N>, streaming-fault-*, trino-*, airflow-backfill-<N>d, sql-agent-*; the list is in `REQUIRED_VARIANTS`), it exits 2, prints an `error:` naming each missing variant and does not write the README (even with --check). A missing engine in the Engine matrix table still only prints a warning to stderr and does not change the exit code. Rerun after every time record_run.py adds rows (tests/test_readme_content.py goes red if forgotten); never edit that table by hand

python scripts/orchestrate.py run --dry-run      # see which tasks would run
python scripts/orchestrate.py run                # run headless, no human needed (default 5 sessions)
python scripts/orchestrate.py report             # progress by merged PRs
python scripts/orchestrate.py daily              # daily report: logs/daily-<date>.md + email (sent automatically at 22:00 while running)
python scripts/orchestrate.py quota              # latest quota read from the logs
python scripts/orchestrate.py clean --dry-run    # which worktrees would be cleaned
```

Stop the orchestrator: create the file `.claude/state/STOP` (running tasks finish, then it stops).

## Roles and models

| Role | Model / effort | What it does |
|---|---|---|
| `scripts/orchestrate.py` | (Python, not an agent) | Reads the DAG `scripts/prs.toml`, checks gates (secrets, harness), creates worktrees, runs up to 5 `claude -p` sessions in parallel (stops opening tasks when `develop` CI is red), waits for the quota reset then `--resume`, stops hung sessions, retries with the previous reason, runs an independent review session then auto-merges on PASS, releases, cleans worktrees, writes reports |
| PR session (headless) | Fable, high | One session per PR, in the PR's worktree: a short plan with a test that must be red, delegates to the specialist agent, waits for green CI, ends with `RESULT: ready <PR number>`. Does not self-review and does not merge |
| Review session (headless) | Opus, medium | A fresh `claude -p --agent code-reviewer` for each review, not seeing the PR session's context; posts a comment, ends with `REVIEW: PASS` or `REVIEW: FAIL <finding>` |
| Specialist agents | Opus, medium | `pipeline-ops`, `event-schema-engineer`, `spark-engineer`, `analytics-engineer`, `data-quality-governance`, `dashboard-builder` — see `.claude/agents/README.md` |
| `code-reviewer` | Opus, medium | Evidence-based review: rerun tests, first commit is tests only, mutation, PASS/FAIL |
| `release-manager`, `qa-testing`, `researcher` | Sonnet, high | Release when the script fails, results README; additional tests; reading code |

## Gates that need the repo owner

- Add the repo secrets (Databricks and/or MotherDuck) — if missing, the orchestrator does all the work that needs no cloud, then stops at `probe`.
- Approve `docs/harness-check.md` (the line `Lam đã duyệt`, a literal Vietnamese marker that the script greps for; it means "Lam approved") before PR B, or run with `--auto-approve-harness`.
- Approve and merge PRs that change `.claude/settings.json` / `.claude/auto-mode.json` / hooks.
- Sign up for the Snowflake trial when PR 13 is wanted (the 30-day clock starts immediately); if missing, PR 13 is skipped.
- Publish the dashboard to Tableau Public.
