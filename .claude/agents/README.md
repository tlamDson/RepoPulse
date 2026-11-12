# Custom Subagents — RepoPulse

## Existing agents

| Agent (`name`) | Model / effort | Area | Used for |
|---|---|---|---|
| `pipeline-ops` | opus / medium | GitHub Actions, GH Archive ingest, cloud probe, `runs.csv`, Pages deploy | S0.4, 4, probe |
| `event-schema-engineer` | opus / medium | eventspec (catalog → JSON Schema), simkit (seeded simulated data, fault injection) | 2, 3 |
| `spark-engineer` | opus / medium | PySpark + Delta bronze/silver, PII, quarantine, Spark/DuckDB benchmark | 5, 12 |
| `analytics-engineer` | opus / medium | Q1–Q6 baseline, dbt star schema, marts, incremental, query optimization, Snowflake | B, 6, 7, 10, 13 |
| `data-quality-governance` | opus / medium | anomaly detection, alerts, PII, role-based access, column masking | 8, 9 |
| `dashboard-builder` | opus / medium | static Pages site, CSV export for Tableau | 11 |
| `release-manager` | sonnet / high | develop → main release, progress, results README | 14, failed releases |
| `code-reviewer` | opus / medium | evidence-based review: rerun tests, first commit is tests only, mutation, PASS/FAIL | every PR |
| `qa-testing` | sonnet / high | write/run additional tests | when assigned |
| `researcher` | sonnet / high | read code/docs, no code writing | when assigned |

The task → agent table lives in `scripts/prs.toml` (the `agent` column); `tests/test_orchestrator.py`
checks that every agent listed there has a file here. Every agent follows `.claude/rules/command-style.md`.

## Convention

Each subagent is one `.md` file in this directory. **The real identifier is the `name` field in the frontmatter, not the file name**, but keep the file name equal to `name` so the test can check it.

```markdown
---
name: agent-slug              # required, same as the file name
description: What the agent does and when to use it.
tools: Read, Grep, Glob        # optional — comma-separated string, NOT a YAML list. Omitted = inherit all tools.
model: sonnet                  # optional — sonnet, opus, haiku, fable (or a full model ID)
effort: high                   # optional — low, medium, high, xhigh, max
---

The agent's system prompt, written as instructions for a new colleague taking over the job.
```
