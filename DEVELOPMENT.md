# Developing RepoPulse

These sections moved here from the [README](README.md), which now gives the short overview. Installing, running the pipeline and the benchmarks, environment variables, the test and lint commands, git flow and the TDD rule are in the [developer guide](docs/developer-guide.md); each part of the platform has its own page in [`docs/`](docs/).

## Status

Both phases are built. The first phase is ingest, Spark and Delta Lake, dbt, data quality checks, governance, a static dashboard and the benchmarks. The second adds a comparison of two end-to-end paths, Delta Lake against Apache Iceberg, a Kafka benchmark with fault injection, Trino on Hive-partitioned tables, an Airflow backfill and an SQL agent. These are measured on GitHub Actions runners, except the SQL agent, whose model runs on the repo owner's machine. Measured results are in [Full results](README.md#full-results), taken from `runs.csv`, the run log this repo keeps. To run or change the code, see the [developer guide](docs/developer-guide.md).

## Architecture details

The diagram is [`.github/readme/architecture.svg`](.github/readme/architecture.svg), shown at the top of the README.

- **Real and simulated data:** GH Archive is real public GitHub activity. The application events come from `simkit` and are **simulated**: there are no real users behind them. The scheduled pipeline processes GH Archive only; the simulated events go through Spark in the fault-injection harness.
- **Layers:** Spark writes bronze (raw lines kept), silver (typed, deduplicated, late events accepted inside a window, PII hashed) and quarantine (events that fail the schema, with a reason). dbt builds staging, the star schema, the six marts for Q1–Q6 and a `public` schema whose identifier columns are masked.
- **Rows for `runs.csv`:** the benchmark, pipeline and quality workflows upload a row as an artifact. `benchmarks/record_run.py` checks its schema and appends it to `runs.csv`.

**The scheduled pipeline runs on a GitHub Actions runner, with gold in MotherDuck.** PySpark and Delta Lake run on the runner and dbt builds the gold layer in MotherDuck: this is path A, and the diagram in the README draws it. Databricks is measured next to it as path B: the same bronze/silver job runs as a serverless Databricks job ([`docs/databricks-job.md`](docs/databricks-job.md)) and dbt builds on a Databricks SQL warehouse. A first cloud probe was rejected by Databricks, because the token then stored as a secret was of the wrong kind; a second probe, with a personal access token, showed that Databricks can be driven from GitHub Actions, and both paths now have rows in `runs.csv`. Which path becomes the primary one is decided by a rule fixed before measuring, applied to scheduled runs only by `python -m repopulse.pathdecision`. Until scheduled runs of both paths cover enough days the rule keeps path A: that is a lack of data, not a result against Databricks. The rule, the outcome and when to apply it again are in [`docs/probe-result.md`](docs/probe-result.md). The pipeline is an incremental batch that runs every hour, not a stream.

## Design decisions

- **Path A on the schedule, Databricks measured as path B.** The first probe reached MotherDuck from GitHub Actions but was rejected by Databricks, so Spark and Delta run on the runner and gold goes to MotherDuck. Once a second probe showed that Databricks can be driven from Actions, the choice became a measurement: both paths run the same incremental batch on the same window of data, and a rule fixed before measuring picks the primary one from the scheduled runs. Until there are enough of them the rule keeps path A. See [`docs/probe-result.md`](docs/probe-result.md) and [`docs/path-comparison.md`](docs/path-comparison.md).
- **DuckDB in CI, MotherDuck for gold.** CI builds the dbt project on DuckDB with a seed fixture, so it needs no secret and no network data. The scheduled run builds the same project on MotherDuck. MotherDuck rejects DuckDB clients newer than it supports, so the workflows pin the DuckDB version. See [`docs/dbt-models.md`](docs/dbt-models.md) and [`docs/pipeline.md`](docs/pipeline.md).
- **Spark and Delta Lake for bronze and silver.** Delta gives the merge that makes silver inserts safe to repeat. Spark runs in local mode on the runner and each run handles a few hours of data, because the runner has 14 GB of disk. See [`docs/spark-jobs.md`](docs/spark-jobs.md).
- **Idempotent per (date, hour).** Ingest skips hours it has already landed, bronze and quarantine overwrite exactly their hour, silver inserts only new event ids, and staging skips ids it has seen. Running the same hour twice does not change row counts.
- **PII is hashed before silver, and the public schema masks identifiers.** Commit author emails and names are salted SHA-256 hashes from silver on. The `public` schema masks user keys and logins, and dbt tests plus a scan flag a raw email in gold or public. GitHub logins are public, so masking them is pseudonymisation, not anonymisation. See [`docs/governance.md`](docs/governance.md).
- **Quarantine instead of silent drops.** An event that fails the schema, arrives outside the late window or cannot be parsed goes to a quarantine table with a reason. The Spark summary reconciles bronze rows with silver inserts, dropped duplicates and quarantine.
- **A fixed set of questions, Q1–Q6.** The same SQL files define the dbt marts and are the queries every benchmark runs, and a test checks that the marts return the same results. Optimisations are therefore measured on the work the product does.
- **`runs.csv` is the only source of numbers.** Rows are schema-checked before they are written, and this README's results are generated from the file and checked by a test.
- **A static dashboard on GitHub Pages.** Databricks dashboards cannot be shared publicly, and the gold layer of the scheduled pipeline is in MotherDuck. The site reads only the `public` schema, and the same data is exported as CSV for Tableau. See [`docs/dashboard.md`](docs/dashboard.md).
- **Application events are simulated.** There is no app with real users, so `simkit` generates events from the catalog with a fixed seed and injects faults that are known in advance. That is what makes fault detection measurable. Wherever these events are reported they are labelled simulated. See [`docs/simkit.md`](docs/simkit.md).
- **One dbt project for two engines.** The SQL avoids DuckDB-only syntax outside a small macro file, and a test enforces it, so the same project also builds on Snowflake. See [`docs/snowflake-port.md`](docs/snowflake-port.md).

## Q6 health score

For each seed repo with at least 5 human contributions in the data, six components in [0, 1]. A repo with fewer (including a bot-only repo) is not scored: it is listed after the ranked repos with no rank and no score (not enough data), because one or two contributions say nothing about health.

| Component | Definition | From |
|---|---|---|
| activity | human contributors / the maximum over scored repos | Q1 |
| retention | share of human contributors active in at least two distinct ISO weeks (a Q6-only measure from the same activity data as Q1; not the Q2 W+4/W+12 cohort retention) | Q1 activity |
| merge | PRs merged / human PRs opened | Q3 |
| response | issues + PRs with a first human response from someone other than the author / opened | Q4 |
| close | issues + PRs closed / opened | Q4 |
| diversity | 1 − share of human contributions made by the top contributor | Q5 |

merge, response and close only rate items opened at least 7 days before the last loaded contribution event (a full follow-up window). They are n/a (null, not 0) for a repo with no such item to rate, and for every repo when no scored repo has a single matched merge, response or close in the loaded data (the window is too short to see follow-ups). `health_score = 100 × (0.25·activity + 0.15·retention + 0.15·merge + 0.20·response + 0.10·close + 0.15·diversity) / (sum of the weights of the components that are not n/a)`, rounded to two decimals; ties are broken by repo name. Bots (logins ending in `[bot]` or `-bot`) are excluded from every component; their share is reported separately in Q5.

## Data sources and licences

- GH Archive data is real public GitHub activity, not simulated. GH Archive publishes no explicit licence for its event data. This repo downloads it at run time and does not redistribute raw files.
- Application events in the cloud phase are simulated and are labelled as simulated wherever they are reported.

## Getting started and contributing

Installing, running the pipeline and the benchmarks, the test and lint commands, git flow and the TDD rule are in the [developer guide](docs/developer-guide.md). Please read the [code of conduct](.github/CODE_OF_CONDUCT.md) before contributing.

## Licence

MIT. See [`LICENSE`](LICENSE).
