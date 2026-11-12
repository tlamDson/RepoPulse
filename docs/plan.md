# RepoPulse — implementation plan

## What RepoPulse does

RepoPulse is a small data platform that measures the "health" of open-source projects from public GitHub event data (GH Archive), together with an application event stream following a logging spec.

- **Input:** real hourly GitHub events; simulated app events generated from an event catalog with a schema.
- **Processing:** a scheduled batch ELT pipeline: cleaning and PII hashing with PySpark + Delta Lake, a star schema built with dbt, with tests, contracts and anomaly alerts.
- **Output:** metrics such as daily active contributors, cohort retention, the fork → PR → merge funnel; a public dashboard; and the measurement ledger `runs.csv` recording every run.

## Project outcome: which questions it answers

Gold and the dashboard focus on about 50–150 **data-ecosystem repos** (Spark, dbt, DuckDB, Airflow, Kafka, Trino, Delta, Iceberg, Polars, pandas…), listed in the seed `transform/seeds/repos_data_ecosystem.csv` with a `category` (compute, orchestration, storage-format, warehouse, streaming, dataframe, bi). Ingest still processes all GH Archive events to obtain scale numbers (M1).

The fixed query set Q1–Q6 is both the questions the project answers and the speed benchmark set reused in every measurement:

| Q | Question | Mart |
|---|---|---|
| Q1 | How many contributors are active per day and per week on each repo and each category? New versus returning people? | `mart_daily_active_contributors` |
| Q2 | Cohort retention: of the group with a first PR in week W, what % are still active at W+4, W+12? | `mart_cohort_retention` |
| Q3 | Funnel fork → open PR → merged PR; time to first merge p50/p90 | `mart_contribution_funnel` |
| Q4 | Time to first response for issues/PRs p50/p90; close rate | `mart_responsiveness` |
| Q5 | Bus factor: what % of contributions the top 1–3 contributors account for; bot share | `mart_repo_concentration` |
| Q6 | Health ranking of data-ecosystem repos (a composite score from Q1–Q5, formula in the README) | `mart_repo_health_score` |

**Value for DE/DA.** DE: each technique (incremental, partition, Spark versus DuckDB, data contract, quality alerts) has before/after numbers measured on the same Q1–Q6 set and the same data day. DA: metric definitions, cohorts, funnels, a public dashboard.

**First baseline measurement (PR B, right after S0.4).** One fixed GH Archive day (24 files, day and checksums recorded in `benchmarks/reference.toml`), the naive pipeline `benchmarks/naive_duckdb.py` (DuckDB reading `*.json.gz` directly, full refresh, no partitioning, no dedup) computes Q1–Q6 with `benchmarks/queries/q1.sql`…`q6.sql`. It measures total time, per-query time, peak RAM, data read, row counts; 5 runs, take the median; workflow `benchmark.yml` on a standard runner; writes `runs.csv` with `variant=baseline`. PRs 5, 7, 10, 12 re-run exactly this workflow with `variant=<name>`, and the README compares each variant with the baseline.

Work is done in two phases. Phase 1 runs entirely on free cloud services and GitHub Actions, with no Docker on the local machine. Phase 2 runs on the local machine, adding what free cloud cannot do: Kafka, Trino/Hive, Airflow and fault-tolerance testing.

## What was checked about free cloud services (2026-11-10)

| Service | Finding | Consequence for the design |
|---|---|---|
| Databricks Free Edition | Serverless only; 1 SQL warehouse of size 2X-Small; at most 5 concurrent job tasks; when the quota is exceeded compute is off until the end of the day; commercial use is forbidden. **Official documentation does not confirm** creating a token and controlling it from outside (CLI, GitHub Actions); only one forum post says it works | The first step is a probe step. If it fails, use the fallback path |
| Databricks: internet access | Limited by a domain allow-list; unknown whether `data.gharchive.org` is reachable | GitHub Actions downloads the data and pushes it to Databricks |
| Databricks: streaming | Runs only in batch style (`availableNow`) | Phase 1 is named honestly as "incremental batch", not real-time |
| Databricks: dashboard | Cannot be shared publicly | The public dashboard is built another way |
| Managed Airflow, free | No long-term free option | Phase 1 orchestrates with GitHub Actions. Airflow is left for phase 2 |
| Managed Kafka, free | Only a 30-day trial, or a very small tier that shuts itself off | Kafka is left for phase 2 |
| GitHub Actions (public repo) | Free; runner 4 CPU, 16 GB RAM, 14 GB disk; each job up to 6 hours; cron shortest 5 minutes, only runs on the default branch, may be delayed, turns itself off after 60 days of repo inactivity | The runner is strong enough to run PySpark. This is the fallback path for Databricks |
| MotherDuck Lite | Free long-term: 10 GB, 10 compute hours per month; works with dbt through a token | Fallback warehouse |
| Snowflake trial | 30 days, locked on expiry | Only sign up once the pipeline runs stably |
| Tableau Public | Reads CSV; only refreshes itself with Google Sheets; otherwise must be republished by hand | Publish by hand at milestones. The self-updating dashboard page is a static page |

## Phase 1 (cloud): design

**Goal:** a scheduled batch ELT pipeline on the cloud, with a logging spec, data modeling, data quality, governance and a dashboard, with real measurements recorded in `runs.csv`.

```
Data sources
  GH Archive (real, public GitHub events) ──┐
  Simulated app events (simkit, per logging spec) ──┤  GitHub Actions (cron)
  Real TravelPlan events (optional, last step) ──┘        │ download, validate schema, push
                                                             ▼
Bronze/Silver: PySpark + Delta Lake on Databricks (serverless)
   data types, dedup, late events, PII hashing, quarantine table for bad events
                                                             ▼
Gold: dbt star schema (fct_events, dim_user SCD2, dim_repo; DAU, retention, funnel marts)
   tests, contracts, freshness checks; CI runs the same dbt project on DuckDB
                                                             ▼
Dashboard: self-updating static page on GitHub Pages + Tableau Public (manual publish)
Measurement ledger: runs.csv in the repo, one row per run
```

**Fallback path if Databricks cannot be controlled from outside:** PySpark + Delta run directly on the GitHub Actions runner (16 GB RAM), with gold on MotherDuck. In that case Spark and Delta Lake are still used, only Databricks is absent; the README says this explicitly.

**Things deliberately not done in phase 1** (to keep the scope tight): a SQL question-answering agent, Terraform, Kafka, Airflow, Trino/Hive, fault-tolerance testing.

## Subplan S0: set up the repo, GitHub DevOps and harness before the real run

The repo is created with `gh`; the `.claude` harness is taken from TravelPlan and adapted for Python and dbt; the harness is checked with a sample PR that goes through the full loop. S0 must be finished and the harness check report must be clean before PR 2 onward is started.

### S0.1 Create the repo

- `gh repo create tlamDson/RepoPulse --public --clone` into `D:\Desktop\RepoPulse`, with a description and topics.
- Git flow as in TravelPlan: feature → `develop` (squash), release `develop` → `main` (merge commit). `main` is the default branch because scheduled workflows only run on the default branch; the pipeline starts running after release. The orchestrator releases automatically at the milestones after PR B, 7, 11, 14 when CI is green (updated 2026-11-11, replacing "a single `main` branch only").
- Repo configuration: allow squash merge only, auto-delete branches after merge.
- Root files: `README.md`, `LICENSE` (MIT), `.gitignore` (Python, dbt, `.env`, `.claude/settings.local.json`, `.claude/worktrees/`), `.gitattributes`, `.env.example`, `pyproject.toml` with pinned versions.

### S0.2 GitHub DevOps

| Item | What to do |
|---|---|
| CI on PRs | `.github/workflows/ci.yml`: ruff (lint and format), pytest, `dbt build` on DuckDB. Fixed job names so they can be required checks |
| `main` branch protection | Require PRs; require the CI jobs to be green; forbid force push and branch deletion; no required human approver (one-person repo, the review is done by agents and recorded in the PR) |
| Security | Enable secret scanning and push protection (enabled on TravelPlan); Dependabot for `pip` and `github-actions`; workflows declare minimal `permissions`, with `timeout-minutes` and `concurrency` |
| Templates | PR template with sections `## Summary`, `## RED` (red test output), `## Test plan`; an issue template for data incidents |
| GitHub Pages | Enable GitHub Actions as the source, for the dashboard PR to use later |
| Secrets | Lam adds them with `gh secret set` or on the web: `DATABRICKS_HOST`, `DATABRICKS_TOKEN`, `MOTHERDUCK_TOKEN`. Agents do not read or print the values |
| pre-commit | ruff and a secret scanner, run both locally and in CI |

The workflow that runs the scheduled pipeline and the workflow that deploys Pages belong to PR 7 and PR 11, not done in S0.

### S0.3 Install the `.claude` harness from TravelPlan

| From TravelPlan | To RepoPulse | Adjustment |
|---|---|---|
| `.claude/rules/tdd.md` | Keep | Switch to pytest and dbt tests; keep Red → Green → Refactor and the requirement to see a real red |
| `.claude/rules/workflow.md`, `developer-guide.md` | Keep | The base branch is `main`; a new worktree cut from `origin/main` is the correct default, no `reset --hard origin/develop` step needed; merge when CI is green and review PASSes; update branches with `gh pr update-branch`, not rebase |
| `.claude/rules/tech-defaults.md` | Write new | RepoPulse's stack; the rule "measurements come only from `runs.csv`"; the rule to label simulated data; the technical debt section |
| `.claude/agents/code-reviewer.md` | Keep | Add: re-run tests and copy the output, check that the first commit contains only tests, try mutations, verdict PASS or FAIL |
| `.claude/agents/qa-testing.md`, `researcher.md` | Keep | Switch test commands to Python and dbt |
| Format-after-edit hook | Keep the idea | Replace `prettier` with `ruff format` for `.py` files |
| Sound hooks (stop, permission prompt, waiting for input) | Keep as is | — |
| `.claude/commands/pr.md`, `ci-watch.md`, `scripts/ci-stop-guard.ps1` | **Do not carry over** | This mechanism can only track one PR and is deleted when a new session opens. The orchestrating session watches CI with `gh pr checks --watch` |
| `rules/design.md`, `debug-chrome-mcp.md`, the UI skills, `itinerary-builder`, `rag-destinations`, `travelplan-devops` | Do not carry over | Unrelated to a data project |
| `CLAUDE.md` | Write new | Overview, frequently used commands, the roles and models table, the gates needing Lam, pointers to the rules |

**The file `.claude/settings.json` needs Lam's explicit permission.** Auto mode blocks agents from writing permission and hook configuration themselves, unless Lam writes exactly that change in a message. So the opening message of S0 (pasted by Lam) states: "allow writing this repo's `.claude/settings.json` with the allow, ask, deny lists and hooks as in `docs/plan.md` section S0.3". Contents:
- `allow`: `git`, `gh`, `python`, `pip`, `uv`, `pytest`, `ruff`, `dbt`, `databricks`, `duckdb`, `npm`/`npx` (for the dashboard page), `curl` to localhost.
- `ask`: `gh secret *`, `gh repo delete*`, `databricks bundle destroy*`, commands that write to Snowflake.
- `deny`: reading `.env`, `git push` directly to `main`, `git push --force`, `gh pr merge --admin`.
- Hooks: `ruff format` after editing `.py` files; three sound alerts.
- Auto mode is enabled by a flag when opening the session, not set in this file.

### S0.4 Check the harness with a sample PR

The sample PR is something needed anyway: the `runs.csv` writing module (a function that appends one measurement row, with schema validation). It goes through the real full loop:

1. The main session writes a short plan for the PR, with a section for the tests that must be red.
2. An Opus subagent in a worktree writes the red test, commits, then writes the code and opens the PR.
3. CI runs on the PR.
4. A different Opus subagent reviews: re-runs the tests, tries mutations, reaches a verdict.
5. The main session merges when CI is green and the review PASSes.

During that, record in `docs/harness-check.md`:

| Check | Passes when |
|---|---|
| Permission prompts | List every time a prompt appeared; the goal is 0 times outside commands in `ask` |
| Subagent worktree | Can be created, from the correct base branch `main`; whether the `d:`/`D:` mismatch error occurs |
| Subagent model and effort | Exactly Opus medium and Sonnet high as assigned |
| Hook | `ruff format` runs after editing a file; the sound plays |
| Branch protection | The API reports `main` requires PRs and checks; a direct push is rejected by the `deny` rule |
| CI | Red on the test-only commit, green once the code exists |
| Review | The reviewer can copy real test output and catches mutations |
| Secret | Push protection is on; the `.env` file cannot be read |

Where a check fails, fix the harness and re-run that part. Lam reads `docs/harness-check.md` before letting PR 2 run.

### Phase 1 PR list (new repo)

| # | Branch | Content | Red test written first |
|---|---|---|---|
| S0 | (subplan above) | Create the repo, DevOps, harness, sample PR `feature/runs-log` | Tests of the `runs.csv` module |
| 1 | (probe step, no PR) | After Lam adds the secrets: from GitHub Actions try pushing a file to a Volume, run a serverless job, `dbt debug` to the SQL warehouse; in a notebook try downloading `data.gharchive.org`. Record the result in `docs/progress.md`, choose the primary or fallback path | — |
| B | `feature/baseline-naive` | Reference data day + checksum, naive DuckDB pipeline, the Q1–Q6 query set, `benchmark.yml` (5 runs, median), writes `runs.csv` `variant=baseline` | The measurement-result parser matches the `runs.csv` schema; a wrong checksum stops the run; Q1–Q6 on a small fixture give hand-computed numbers |
| 2 | `feature/event-spec` | `eventspec/`: YAML event catalog generating JSON Schema; event validation function; dedup key rules | An event missing a required field is rejected; an event with a duplicate key is recognized |
| 3 | `feature/simkit-generator` | `simkit/`: generates simulated users by persona, with a seed; injects 6 known data fault types (duplicate, null, wrong type, late, schema change, bot) | The same seed gives the same data; the number of faults injected equals the number requested |
| 4 | `feature/ingest-gharchive` | Download files by hour, filter event types, idempotent by (date, hour), push to the landing zone; cron workflow | Re-running the same hour does not create duplicate records |
| 5 | `feature/bronze-silver-spark` | PySpark job: raw bronze, typed silver, dedup, late-arrival window, PII hashing before silver, quarantine | PySpark tests on small data: duplicates dropped, emails no longer raw, bad events go to quarantine |
| 6 | `feature/dbt-star-schema` | dbt project: staging, marts, `dim_user` SCD2, DAU, retention and funnel marts; tests, contracts, unit tests | `dbt build` on DuckDB is red when a contract is violated |
| 7 | `feature/incremental-and-schedule` | Incremental models instead of full refresh; a scheduled workflow chaining ingest, the Spark job, `dbt build`; writes `runs.csv` | Row counts match between incremental and full refresh on a fixture |
| 8 | `feature/data-quality-alerts` | Run the injected-fault set through the pipeline; detect volume and freshness anomalies; alert via GitHub Issue | Each fault type has a test that catches it; a clean run raises no alarm |
| 9 | `feature/governance` | Role-based permissions, PII column labels, column masking; an automatic check that gold contains no raw email. Use Unity Catalog if Free Edition has it (not yet checked), otherwise do it in the dbt layer | The test looking for raw email in gold must be red when the hashing step is deliberately removed |
| 10 | `feature/query-optimization` | Measure the dashboard queries before and after partitioning and clustering; record the numbers | The measurement-result parser matches the schema |
| 11 | `feature/dashboard-site` | A static GitHub Pages site reading aggregated gold data, self-updating after every run; export CSVs for Tableau | The site build fails when a mart is missing |
| 12 | `feature/spark-vs-duckdb-benchmark` | Run the same transformation with PySpark and DuckDB at 1, 7, 30 days of data on the runner | Benchmark parser |
| 13 | `feature/snowflake-port` | Run the same dbt project on the Snowflake trial; record time and the number of lines to change | `dbt build` with the Snowflake profile in a manually run workflow |
| 14 | `docs/results` | README with a results table taken from `runs.csv`, diagrams, design decisions | — |

One PR in the **TravelPlan repo** (optional, done separately there) to get real events from an app:

| Branch | Content |
|---|---|
| `feature/product-event-logging` | An SDK that logs events on client and server following the `eventspec` catalog; an endpoint that receives events with schema validation; storage in a capped Mongo collection; measuring coverage of key actions. Afterwards a workflow in RepoPulse exports the real events daily (needs Lam to add a read-only secret) |

### Phase 1 measurements

| ID | Measurement | Conditions | Baseline |
|---|---|---|---|
| M1 | Number of events and size processed | Counted at bronze, silver, gold | Scale figures |
| M2 | Build time when adding one day of data | Median of 5 | Full refresh (`variant=baseline`) |
| M3 | Data freshness p50/p95 | ≥168 scheduled runs. Runs on the cloud so it accumulates on its own, no need for Lam's machine | A target set in advance |
| M4 | Injected faults caught; number of false alarms | 6 fault types; N clean runs | No tests: 0 |
| M5 | Spark and DuckDB crossover point | 1, 7, 30 days; 3 runs per level | DuckDB |
| M7 | Port to Snowflake: lines changed, time | Same `dbt build` | The primary version |
| M9 | Share of events dropped for schema errors; duplicate rate | Simulated events (clearly labeled simulated) and real events if any | Before the spec exists |
| M10 | p50 query time and data scanned | The Q1–Q6 set, 10 runs | Unpartitioned table (`variant=baseline`) |

## Plan v2 (2026-11-11): comparing two paths, widening engines, phase 2, AI

Phase 1 is done (16/16 tasks, release #45). Re-running the probe with the correct token ([run 43104609971](https://github.com/tlamDson/RepoPulse/actions/runs/43104609971), 2026-11-11) gave `primary`: all 9 steps pass. `docs/probe-result.md` still recorded the first attempt (`fallback`, 401 error); task D4 updates that file together with the comparison table. Plan v2 measures the two paths against each other, then chooses the primary path by a rule written down **before measuring**. It also adds an engine matrix, Iceberg versus Delta, phase 2 and a SQL agent running a local model. The repo owner settled on 2026-11-11: everything must be free, phase 2 runs on GitHub Actions runners, and the orchestrator runs until the quota is exhausted.

### Primary-path selection rule (fixed before measuring, applied exactly as is by PR D4)

Call A runner + MotherDuck (the current path), and B Databricks serverless + SQL warehouse. Each path runs the same end-to-end pipeline (ingest → bronze/silver → gold → the Q1–Q6 set) on the same data, with enough scheduled runs to have a p50 and a success rate.

1. Drop any path that **does not run the full 24 times/day within the free quota** (`24 lần/ngày` in the original Vietnamese), or that has a **success rate < 95%**.
2. Among the remaining paths, choose the one with the **lower end-to-end p50 time**.
3. If the two paths differ by **less than 10%**, keep the current path (A).

D4 records the data table, the rule and the conclusion in `docs/probe-result.md`. The table only uses numbers in `runs.csv`. D4 changes `pipeline.yml` only when the rule says to switch.

### Tasks (DAG in `scripts/prs.toml`)

| Task | Branch | Content | Red test written first | Measurement into `runs.csv` |
|---|---|---|---|---|
| D1 | `feature/databricks-bronze-silver` | A bronze/silver job running serverless on Databricks: Delta in Unity Catalog, sharing logic with `repopulse/spark/`, loaded with the Files API + Jobs API from GitHub Actions | API-calling logic (mock HTTP): building the job payload, retry, idempotent by (date, hour) | `variant=databricks-bronze-silver` |
| D2 | `feature/dbt-databricks-target` | The `databricks` target already exists in `transform/profiles.yml` (the probe uses it for `dbt debug`). This task builds the **missing gold part** on the SQL warehouse: a source reading D1's Delta silver table (Unity Catalog), dialect-specific macros (datetime, arrays) that work on both DuckDB and Databricks, and a workflow running `dbt build --target databricks` | Dialect macro tests on DuckDB red first; a dbt unit test asserts the marts give the same result on the fixture on both adapters | `variant=dbt-build-databricks` |
| D3 | `feature/pipeline-path-comparison` | A workflow running A and B end to end on a schedule, recording per-step time, startup/queueing time and success outcome; a script aggregating p50/p95 and success rate | Parser and aggregation function on a fixture | `variant=path-a-runner`, `variant=path-b-databricks` |
| D4 | `feature/primary-path-switch` | Apply the selection rule above; update `docs/probe-result.md`; change `pipeline.yml` if the rule says to switch | The (pure) selection rule function on sample number sets, including the case with a gap under 10% | — |
| E1 | `feature/engine-matrix` | The same Q1–Q6 set on DuckDB, Spark on the runner, Databricks serverless, Databricks SQL, MotherDuck, Snowflake (reusing the existing numbers). The README generates a single table | Matrix parser; a warning when an engine is missing | `variant=matrix-<engine>` |
| E2 | `feature/iceberg-vs-delta` | The same data written with Delta and Apache Iceberg (PySpark on the runner): write, read Q1–Q6, MERGE, time travel | On a small fixture: row counts and Q1–Q6 results equal between Delta and Iceberg; re-running MERGE creates no duplicates (idempotent); time travel reads the version before the MERGE correctly | `variant=format-delta`, `variant=format-iceberg` |
| P2a | `feature/streaming-kafka` | **Apache Kafka** 3 brokers in KRaft mode (official `apache/kafka` image, no ZooKeeper) as a Docker service in the workflow; a producer emits the reference day's GH Archive events into a topic with N partitions; Spark Structured Streaming reads Kafka and writes Delta (checkpoint, exactly-once to the sink); reconcile row counts against batch | Producer/consumer on a fixture; count reconciliation | `variant=streaming-throughput-p<N>` |
| P2b | `feature/streaming-fault-tolerance` | Kill a broker and a consumer midway; measure the number of events lost, duplicated, and recovery time | Loss/duplicate reconciliation function | `variant=streaming-fault-<case>` |
| P2c | `feature/trino-hive` | Trino + Hive Metastore + MinIO as Docker services; gold in Hive-partitioned form; Q1–Q6 on Trino before and after optimization | Partition DDL generation; result parser | `variant=trino-<layout>` |
| P2d | `feature/airflow-dags` | Airflow standalone in a workflow; a DAG with backfill, retry, SLA in place of cron; measure an N-day backfill | DAG imports with no cycle errors; task order | `variant=airflow-backfill-<N>d` |
| AI1 | `feature/dbt-semantic-layer` | Semantic models and metrics (MetricFlow) for Q1–Q6 on gold public | `dbt parse` checks the semantic manifest; metrics match the Q1–Q6 SQL on a fixture | — |
| AI2 | `feature/sql-agent` | A SQL agent calling through an OpenAI-compatible API (`base_url` and model are configuration): reads the `public` schema only, blocks every write statement, limits row count; an evaluation question set with answers; CI uses a mock LLM | Blocking write statements; generating and running SQL with a mock LLM; scoring answers | — (measured in AI3) |
| R2 | `docs/results-v2` | README results v2 generated from `runs.csv`: two-path table, engine matrix, Iceberg/Delta, phase 2, the agent | The generator raises an error when a variant is missing | — |

**AI3, run by hand on the repo owner's machine, not through the orchestrator:** the model `Qwen3.6-35B-A3B` (GGUF 18 GB) cannot run on a runner. After AI2 merges, the main session asks the repo owner for permission, then starts a local `llama-server` and runs the evaluation set. Accuracy and time per question are recorded in `runs.csv` with `variant=sql-agent-qwen3.6-35b-a3b`, brought in through a PR.

### Constraints

- **Free services only:** Databricks Free Edition (daily quota, at most 5 jobs at the same time), MotherDuck Lite, a Snowflake trial without a card, GitHub Actions runners (job ≤ 6 hours, 16 GB RAM, 14 GB disk). No paid LLM API.
- **Phase 2 runs on runners:** use `services:` or `docker compose` in a manually run or scheduled workflow, not on the repo owner's machine.
- **Measurements come only from `runs.csv`:** every optimization is compared with `variant=baseline` on the same data day and the same Q1–Q6 set.

Plan v2 is an extension. Dropping it would not affect the results of phase 1.

## Tasks for Lam

**Before starting (about 30 minutes):**

| # | Task | Notes |
|---|---|---|
| 1 | The project name is settled: **RepoPulse** | The agent creates the public repo `tlamDson/RepoPulse` with `gh` and clones it into `D:\Desktop\RepoPulse` |
| 2 | Sign up for Databricks Free Edition | No card needed |
| 3 | Sign up for MotherDuck Lite | Fallback; no card needed |
| 4 | Sign up for Tableau Public and install Tableau Desktop Public Edition | Used in the dashboard step |
| 5 | After S0: add secrets to the repo (Databricks token and host, MotherDuck token) | Lam adds them himself; the agent does not need to see the values |
| 6 | Read `docs/harness-check.md`, then let the run continue | The only gate between S0 and the rest |

**While running:**
- Sign up for the Snowflake trial **when the agent reports it has reached PR 13**, because the 30-day clock starts immediately.
- Publish the dashboard to Tableau Public (about 30–60 minutes, once).
- Add a read-only secret to the Mongo staging if real TravelPlan events are wanted.

## How to run

**Updated 2026-11-11:** run headless with `scripts/orchestrate.py` (DAG in `scripts/prs.toml`, one `claude -p` session per PR in its own worktree, up to 5 in parallel (previously 2), specialist agents per area, automatic quota waiting, automatic release). Usage: `docs/kickoff.md`. The old description below is kept for reference on the roles.

- **Main Fable session (effort high):** writes a short plan for each PR with a section for tests that must be red, assigns the work, decides merges, writes `docs/progress.md`.
- **Opus subagent (effort medium):** one agent per PR in its own worktree, writing the red test then the code; a different Opus agent reviews with evidence.
- **Sonnet subagent (effort high):** lint, branch updates, copying evidence, data-only PRs.
- Merge when CI is green and the review PASSes. Stop when everything remaining is waiting on Lam, and print the checklist.

Since everything heavy runs on GitHub Actions and Databricks, Lam's machine only runs small unit tests. No Docker needed.

## Order of S0

1. **S0.1:** create the public repo `tlamDson/RepoPulse` with `gh`, set the merge configuration, and make the first commit with the root files and the documentation in `docs/`.
2. **S0.2:** PR `chore/devops-bootstrap` (CI, PR template, Dependabot, pre-commit, Python and dbt skeleton). After the merge, enable `main` branch protection and the security settings.
3. **S0.3:** PR `chore/claude-harness` (rules, agents, `CLAUDE.md`, `.claude/settings.json`). This PR is **left open for the repo owner to review**.
4. **S0.4:** the sample PR `feature/runs-log`, done in a Claude Code session opened in this repo so the harness is loaded and checked where it actually runs. The opening message is in `docs/kickoff.md`.

## Verification

- **Step 1 (the probe step)** decides the primary or fallback path; the result is recorded in `docs/progress.md` before PR 4. Updated 2026-11-11: the probe step was done through the PR `docs/probe-result`, with the result in [`probe-result.md`](probe-result.md) — it chose the **fallback path**; Databricks could not be checked because the stored token was not a personal access token.
- Each PR: green CI (lint, tests, `dbt build` on DuckDB), red test output in the PR description, review PASS.
- Pipeline: the scheduled workflow runs green for many days in a row; `runs.csv` gains rows; the GitHub Pages site updates.
- Re-running the same data hour creates no duplicate records.
- The README only quotes numbers present in `runs.csv`.

## Risks and unverified items

| Risk | Handling |
|---|---|
| Databricks Free Edition does not allow control from outside | Fallback path: PySpark on the runner + MotherDuck; the README states clearly that Databricks is not used |
| Exceeding the Databricks quota in a day | A sparse schedule (once every few hours), idempotent jobs so the next day can catch up |
| The Databricks account is deleted for long inactivity | The code and the aggregated gold data always have a copy in the repo |
| GitHub Actions cron delayed, or turned off after 60 days of inactivity | Record the real run time in `runs.csv`; M3 reports the measured delay accurately |
| The runner disk is only 14 GB | Each run processes only a few hours of data; filter event types before storing |
| App data is simulated | The README and every report state "simulated" clearly; GH Archive is real data |
| Recent GH Archive is missing data unevenly (measured 2026-11-11, the 12:00 hour file) | There are complete hours: 2026-10-25 has 81,569 events, 82% PushEvent. There are hours that completely lack PushEvent, CreateEvent, DeleteEvent: 2026-10-15, 2026-11-03 (3,398 events), 2026-11-09, 2026-11-10; size 2.8–15 MB/hour. Volume has also been declining year by year: 2024-01 is 123 MB/hour, 2025-06 is 85 MB/hour, 2026-06 is 21 MB/hour. Consequences: (1) PR B chooses a reference day on which all 24 hours have PushEvent, checked by a script, with the criteria recorded in `benchmarks/reference.toml`; (2) PR 12 measures Spark and DuckDB at 1, 7, 30 days on 2024–2025 data, when one day is about 2–3 GB, fitting the 14 GB disk; (3) Q1–Q6 rely on PRs, issues, reviews, comments, forks, watches, which are always present, and do not rely on PushEvent; (4) PR 8 uses these very data-poor hours as real anomalies for the volume detection part. |

**Not yet verified:** tokens and the CLI on Free Edition; whether `data.gharchive.org` is on the allowed domain list; Volume capacity; whether Free Edition has Unity Catalog permissions, column masking and lineage; the daily quota figures; the number of credits in the Snowflake trial; the time each PR takes in a new repo.
