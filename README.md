# RepoPulse

RepoPulse is a small data platform that measures the health of open-source projects from public GitHub event data.

**Status:** bootstrapping. No pipeline results yet. Every number that appears in this README later will come from `runs.csv`, the run log this repo keeps.

## What it does

- **Input:** hourly public GitHub events from [GH Archive](https://www.gharchive.org/), plus application events generated from an event catalog with schemas.
- **Processing:** a scheduled batch ELT pipeline. PySpark and Delta Lake clean the raw events and hash personal data. dbt builds a star schema with tests and contracts. Volume and freshness checks raise alerts.
- **Output:** metrics such as daily active contributors, cohort retention and the fork → pull request → merge funnel, a public dashboard, and `runs.csv`.

## Plan

The work runs in two phases:

1. **Cloud phase:** everything runs on free cloud services and GitHub Actions. No local Docker is needed.
2. **Local phase:** adds what free cloud tiers cannot offer: Kafka, Trino on Hive-partitioned tables, Airflow, and fault-injection tests.

Details are in [`docs/plan.md`](docs/plan.md) (Vietnamese). Progress is tracked in [`docs/progress.md`](docs/progress.md).

## Data sources and licences

- GH Archive publishes no explicit licence for its event data. This repo downloads it at run time and does not redistribute raw files.
- Application events in the cloud phase are simulated and are labelled as simulated wherever they are reported.

## Licence

MIT. See [`LICENSE`](LICENSE).
