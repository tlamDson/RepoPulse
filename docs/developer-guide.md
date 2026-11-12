# Developer guide

How to install, run and develop RepoPulse. The project overview, architecture and measured results are in the [README](../README.md).

## Prerequisites

| Tool | Needed for |
|---|---|
| Python 3.11 | everything |
| Java 17 | PySpark jobs and the Spark tests (Java 24 is not supported by PySpark) |
| Docker | the Kafka benchmark and the Trino/Hive benchmark (run locally or in the workflows) |
| `git`, `gh` | pull requests and checking CI |

Notes:

- On the Windows machine of the repo owner `pytest`, `ruff` and `dbt` are not on `PATH`: always call them as `python -m pytest`, `python -m ruff`, `python -m dbt.cli.main`. CI (Linux) uses the bare commands in `ci.yml`.
- Spark tests are marked `@pytest.mark.spark`. They skip locally when Java 17 (or `winutils` on Windows) is missing and always run on CI.
- Never commit secrets or a `.env` file. Cloud secrets (`DATABRICKS_*`, `MOTHERDUCK_TOKEN`, `PII_HASH_SALT`, `SNOWFLAKE_*`) are added by the repo owner as GitHub secrets; new variables go into `.env.example` with an empty value.

## Install

```bash
pip install -e ".[dev,dbt]"          # once; add the "spark" / "streaming" extras when you need them
pre-commit install                   # ruff + gitleaks hooks
```

## Test, lint, dbt

```bash
python -m ruff check .
python -m ruff format --check .
python -m pytest                                   # whole Python suite
python -m pytest tests/test_<module>.py -x         # one file
python -m pytest -k <test_name>                    # one case

# dbt on DuckDB (what CI runs, with the seed fixture)
python -m dbt.cli.main build --project-dir transform --profiles-dir transform --target ci
# read the silver parquet instead of the seed fixture (docs/dbt-models.md)
python -m dbt.cli.main build --project-dir transform --profiles-dir transform --target ci --vars "{silver_gharchive_parquet: '<glob>/*.parquet'}"
# rebuild fct_events from scratch (staging keeps history; see docs/dbt-models.md)
python -m dbt.cli.main build --project-dir transform --profiles-dir transform --target ci --full-refresh

python -m pytest tests/test_dbt_marts.py           # marts Q1-Q6 match benchmarks/queries/q*.sql, contract, PII
python -m pytest tests/test_dbt_semantic_layer.py  # MetricFlow semantic layer (docs/semantic-layer.md)
gh pr checks <pr-number> --watch --interval 30     # wait for CI on a PR
```

The three required CI jobs are `Lint`, `Unit Tests` and `dbt build (DuckDB)`. Do not rename them, and do not add `paths:` to `on: pull_request` or an `if:` at job level for them: they are the required status checks of `develop` and `main`.

### Governance, semantic layer and SQL agent

```bash
# SQL of a MetricFlow metric, from transform/target/semantic_manifest.json (after dbt build/parse)
python -m repopulse.semantic sql --metrics cohort_size,retention_w4_pct --group-by q2_cohort__cohort_week__week
# raw email in gold/public? exit 0 clean, 1 finding (docs/governance.md)
python -m repopulse.governance scan --db ci.duckdb --schema main_gold --schema main_public
# meta.pii labels and role permissions
python -m repopulse.governance check --manifest transform/target/manifest.json --policy transform/governance.yml --db ci.duckdb
# read-only SQL agent over main_public through an OpenAI-compatible LLM (docs/sql-agent.md)
# REPOPULSE_LLM_BASE_URL defaults to a local llama-server at 127.0.0.1:8080; exit 1 = out of attempts, 2 = config/LLM error
python -m repopulse.sql_agent ask --db ci.duckdb --model <model> "<question>"
python -m repopulse.sql_agent eval --db ci.duckdb --model <model> --report out/sql-agent-eval.json --out out/sql-agent-row.csv --git-sha <sha>
```

## Event spec and simulated data

```bash
python -m eventspec.generate                     # regenerate eventspec/schemas/ from the catalog (docs/eventspec.md)
python -m simkit.generate --seed 42 --users 100 --days 7 --null 5 --bot 2   # simulated events + fault manifest in data/simulated/ (docs/simkit.md)
```

Application events are always **simulated**; GH Archive data is real.

## Pipeline commands

```bash
python -m repopulse.ingest --date 2026-01-05 --hour 15 --hours 2   # GH Archive into data/landing/gharchive
python -m repopulse.ingest --summary out/ingest-summary.json       # default: hour (now UTC - 2h); --force redoes it
# bronze/silver Delta (needs PII_HASH_SALT, Java 17, the spark extra); docs/spark-jobs.md
python -m repopulse.spark.job --source gharchive --date 2026-01-05 --hour 16 --hours 2
# silver Delta -> parquet for dbt (var silver_gharchive_parquet)
python -m repopulse.spark.export --lake-dir data/lake --out data/silver-export
# helpers of .github/workflows/pipeline.yml (incremental batch per hour); docs/pipeline.md
python -m repopulse.pipeline {window,timed,counts,row,md-reset} --help
```

The scheduled pipeline is an **incremental batch**, not a stream. Cron workflows only run on `main` and can be delayed.

Path B (Databricks), path comparison and orchestration:

```bash
# same bronze/silver as a serverless Databricks job (DATABRICKS_HOST/TOKEN, PII_HASH_SALT); docs/databricks-job.md
python -m repopulse.databricks_job run --source gharchive --date 2026-10-25 --hour 23 --hours 3 --landing-dir data/landing/gharchive --wheel dist/<wheel>.whl --git-sha <sha> --verify-rerun --summary out/databricks-summary.json --out out/databricks-row.csv
python -m repopulse.pathcompare summary --runs runs.csv --out out/path-summary.md          # runs, success rate, p50/p95 per path, from runs.csv only
python -m repopulse.pathdecision --runs runs.csv --doc docs/probe-result.md [--check]       # primary-path rule; --check exits 1 on drift
python -m repopulse.pathcompare {window,runner-waits,warehouse-start,queries,md-ensure,row} --help   # docs/path-comparison.md
python -m repopulse.dagspec {window,wait,row,check-ingest} --help                            # Airflow DAG helpers; docs/airflow.md
```

Data quality (docs/data-quality.md):

```bash
python -m repopulse.quality faults --seed 42 --users 30 --days 2 --clean-runs 2 --git-sha <sha> --report out/dq-faults.json --out out/dq-fault-row.csv   # needs PII_HASH_SALT, Java 17
python -m repopulse.quality check --database ci.duckdb --report out/dq-report.json   # volume + freshness; thresholds in repopulse/quality.toml
python -m repopulse.quality gap --ingest --report out/dq-gap.json                     # known missing GH Archive hours must be caught
python -m repopulse.quality alert --report out/dq-report.json --run-url <url> --dry-run   # data-incident issue; drop --dry-run to call gh (GH_TOKEN)
```

Dashboard (docs/dashboard.md):

```bash
python -m repopulse.export_site --db ci.duckdb --runs runs.csv --out _site   # 6 public marts + runs.csv -> site + Tableau CSV
python -m http.server 8000 --bind 127.0.0.1 --directory _site                 # preview locally
```

## Benchmarks

Every optimisation is measured against one naive baseline, on the same day of data and the same six questions (Q1-Q6). Numbers come only from `runs.csv`.

```bash
python benchmarks/fetch_reference.py             # download + verify sha256 of the reference day (benchmarks/reference.toml)
python benchmarks/naive_duckdb.py --runs 5 --git-sha <sha> --out out/benchmark-row.csv
python benchmarks/record_run.py <benchmark-row.csv> [<pipeline-row.csv> ...]   # merge artifact rows into runs.csv (one bad file: nothing written)

python benchmarks/scale_prepare.py               # download + filter + shrink 30 days of GH Archive (docs/benchmark-scale.md)
python benchmarks/scale_bench.py --git-sha <sha> --out out/scale-rows.csv   # Spark vs DuckDB, 1/7/30 days
python benchmarks/query_opt.py --runs 10 --git-sha <sha> --out out/query-opt-rows.csv --summary out/query-opt-summary.md   # flat/partitioned/clustered parquet; bytes scanned only measurable on Linux (docs/query-optimization.md)
python benchmarks/trino_bench.py --print-ddl     # Trino/Hive DDL of the 4 layouts (no Docker); real runs only in trino-bench.yml (docs/trino-hive.md)
python benchmarks/engine_matrix.py --engine duckdb --runs 5 --git-sha <sha> --out out/engine-matrix-rows.csv --summary out/engine-matrix-summary.md   # Q1-Q6 on one engine (docs/engine-matrix.md)
python benchmarks/snowflake_port.py --engine duckdb --run-results transform/target/run_results.json --git-sha <sha> --out out/snowflake-port-rows.csv   # docs/snowflake-port.md
python benchmarks/dbt_databricks.py --run-results transform/target/run_results.json --show-log out/dbt-databricks-show.jsonl --git-sha <sha> --out out/dbt-databricks-rows.csv   # docs/dbt-databricks.md
```

Kafka and streaming (need Docker and Java 17; docs/streaming.md, docs/streaming-fault.md):

```bash
docker compose -f benchmarks/kafka/docker-compose.yml up -d --wait   # 3-broker KRaft cluster; "down -v" cleans up
python benchmarks/streaming_bench.py --git-sha <sha> --partitions 1,3,6 --out out/streaming-rows.csv --summary out/streaming-summary.md
python benchmarks/streaming_fault.py --git-sha <sha> --out out/streaming-fault-rows.csv --summary out/streaming-fault-summary.md   # kill broker / kill consumer; invalid measurements write no row
```

Engines that need credentials (`motherduck`, `databricks-sql`, `databricks-serverless`) and the Snowflake and Databricks targets only run inside their workflows.

### Baseline benchmark

- **Reference day:** one full day of real GH Archive data. The date, the selection criteria and the sha256 and size of each of the 24 hourly files are in [`benchmarks/reference.toml`](../benchmarks/reference.toml), generated by `benchmarks/select_reference_day.py`. A day qualifies when all 24 hours exist, every hour has PushEvents (some recent GH Archive hours lost whole event types), and no hour's size is more than `max_size_ratio` away from the median hour.
- **Naive pipeline:** [`benchmarks/naive_duckdb.py`](../benchmarks/naive_duckdb.py) reads every `*.json.gz` straight into one DuckDB table (full refresh, no partitioning, no deduplication), then runs `benchmarks/queries/q1.sql` to `q6.sql`. Repos are scoped by the seed [`transform/seeds/repos_data_ecosystem.csv`](../transform/seeds/repos_data_ecosystem.csv). The queries use pull requests, issues, reviews, comments and forks only, never PushEvent/CreateEvent/DeleteEvent. Each SQL file starts with the definitions it uses (bot, contribution, first response...).
- **Measurements:** total time, time per query, peak RSS, bytes read and raw row count; median of the runs on a GitHub-hosted `ubuntu-latest` runner via [`.github/workflows/benchmark.yml`](../.github/workflows/benchmark.yml). The workflow uploads one `runs.csv` row as the `benchmark-row` artifact; `python benchmarks/record_run.py benchmark-row.csv` appends it to `runs.csv` after a schema check.

Run it locally:

```bash
pip install -e ".[dev]"
python benchmarks/fetch_reference.py        # download + verify checksums; non-zero exit on mismatch
python benchmarks/naive_duckdb.py --runs 5 --variant baseline --git-sha "$(git rev-parse --short HEAD)" --out out/benchmark-row.csv
```

## Updating the README results

The Results block of the README (between `<!-- results:start -->` and `<!-- results:end -->`) is generated from `runs.csv`. Never edit it by hand.

```bash
python -m repopulse.readme_results           # regenerate the block
python -m repopulse.readme_results --check   # exit 1 when the block is out of date
```

Run it again after every `record_run.py` that adds rows; `tests/test_readme_content.py` fails if you forget. Any number quoted in the README, a PR description or a report must exist in `runs.csv`, and an optimisation is compared with `variant=baseline` on the same data date and queries only. Simulated data is always labelled "simulated".

## Git flow

- Feature branches (`feature/`, `fix/`, `test/`, `docs/`, `chore/`) start from the latest `origin/develop` and merge into `develop` with a **squash** PR.
- Releases are a PR `develop` -> `main` with a **merge commit**. `main` is the default branch; scheduled workflows only run there.
- Never push straight to `develop` or `main`, never force push, never merge with `--admin`.
- A feature PR merges when all three CI jobs (`Lint`, `Unit Tests`, `dbt build (DuckDB)`) are green and an independent review passes.
- One PR does one thing. Do not bundle unrelated fixes.
- Commit messages are one imperative line without a type prefix (`add runs.csv append with schema check`); the type prefix (`feat:`, `fix:`, `docs:`...) is for the PR title only.
- Do not commit `.env`, credentials, `*.duckdb`, `transform/target/`, raw data (`*.json.gz`, `*.parquet`) or `logs/`.
- PRs use the template in [`.github/pull_request_template.md`](../.github/pull_request_template.md).

## TDD

Every feature and fix follows Red, Green, Refactor:

1. **Red:** write the failing test first. The first commit of a PR contains only tests; run them and confirm they fail for the right reason (an assertion or a missing module, not a configuration error). Paste the failing output into the `## RED` section of the PR description.
2. **Green:** write the minimum code that passes.
3. **Refactor:** clean up and re-run the whole suite.

Test rules: never call a real service (Databricks, MotherDuck, `data.gharchive.org`, GitHub API) in a unit test; random data uses a fixed seed; reruns of the same (date, hour) must not duplicate records; PII tests assert no raw value remains after hashing. PySpark tests use a local `SparkSession`; dbt tests live in `transform/models/**/schema.yml` and `transform/tests/`.

## Documentation index

Most pages are in Vietnamese; the benchmark pages are in English.

| Page | Topic |
|---|---|
| [`plan.md`](plan.md), [`progress.md`](progress.md) | The plan (Vietnamese) and progress tracking |
| [`probe-result.md`](probe-result.md) | The cloud probe, the rule that picks the primary path, and where it stands |
| [`eventspec.md`](eventspec.md), [`simkit.md`](simkit.md) | Event catalog and schemas; the simulated event generator with known faults |
| [`spark-jobs.md`](spark-jobs.md) | Bronze, silver and quarantine tables, rules, PII hashing |
| [`dbt-models.md`](dbt-models.md) | Staging, star schema, marts Q1-Q6, incremental models |
| [`pipeline.md`](pipeline.md) | The scheduled incremental batch workflow |
| [`data-quality.md`](data-quality.md) | Fault injection, volume and freshness checks, Issue alerts |
| [`governance.md`](governance.md) | PII labels, roles, the masked `public` schema |
| [`dashboard.md`](dashboard.md) | The static site and the Tableau CSV export |
| [`query-optimization.md`](query-optimization.md) | Partitioned and clustered layouts against the baseline |
| [`benchmark-scale.md`](benchmark-scale.md) | Spark against DuckDB at 1, 7 and 30 days |
| [`iceberg-vs-delta.md`](iceberg-vs-delta.md) | Delta Lake against Apache Iceberg: write, Q1-Q6, MERGE, time travel |
| [`engine-matrix.md`](engine-matrix.md) | Q1-Q6 on the reference day on six engines |
| [`snowflake-port.md`](snowflake-port.md) | Running the dbt project on Snowflake |
| [`databricks-job.md`](databricks-job.md) | The bronze/silver incremental batch as a serverless Databricks job (path B) |
| [`dbt-databricks.md`](dbt-databricks.md) | The dbt project built on a Databricks SQL warehouse (path B) |
| [`path-comparison.md`](path-comparison.md) | The two end-to-end paths run on the same data window |
| [`streaming.md`](streaming.md), [`streaming-fault.md`](streaming-fault.md) | The Kafka benchmark and the tests that kill a broker or the consumer |
| [`trino-hive.md`](trino-hive.md) | Q1-Q6 on Trino over Hive-partitioned parquet |
| [`airflow.md`](airflow.md) | The Airflow DAG and its backfill on a runner |
| [`semantic-layer.md`](semantic-layer.md) | MetricFlow metrics on the public marts |
| [`sql-agent.md`](sql-agent.md) | The read-only SQL agent over the public schema, and its evaluation |
