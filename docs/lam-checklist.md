# RepoPulse · Tasks for the repo owner

Agents cannot do the tasks below on your behalf. Do not paste tokens or passwords into chat or into files in the repo.

## Before running phase 1 (about 30 minutes)

| # | Task | Notes | Done |
|---|---|---|---|
| 1 | Review the harness PR `chore/claude-harness` | Read the allow, ask and deny lists in `.claude/settings.json` before merging | ☐ |
| 2 | Sign up for Databricks Free Edition | No card needed. Create a personal access token in User Settings → Developer | ☐ |
| 3 | Sign up for MotherDuck Lite | Fallback in case Databricks cannot be controlled from outside. No card needed | ☐ |
| 4 | Sign up for Tableau Public and install Tableau Desktop Public Edition | Used in the dashboard PR | ☐ |
| 5 | Add secrets to the repo | Commands below | ☐ |
| 6 | Read `docs/harness-check.md`, then let the run continue | Gate between S0 and phase 1 | ☐ |

Add secrets (run in the repo directory; each command prompts for the value, which is not echoed to the screen):

```
gh secret set DATABRICKS_HOST
gh secret set DATABRICKS_TOKEN
gh secret set MOTHERDUCK_TOKEN
```

## While the run is in progress

| Task | When |
|---|---|
| Sign up for the Snowflake trial, then add the `SNOWFLAKE_*` secrets | When the agent reports it has reached PR 13. The 30-day clock starts at sign-up |
| Publish the dashboard to Tableau Public | After PR 11, about 30–60 minutes, done once |
| Add a read-only secret for a real event source | Optional, if you want real events from an app |

## Phase 2 (on your machine)

Start Docker Desktop when each batch begins. Each batch needs an estimated 5–8 GB of RAM; measure the real RAM in the first batch.
