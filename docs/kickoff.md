# RepoPulse · How to run

Open a terminal from the repo directory, typing `D:` in capitals:

```
cd D:\Desktop\RepoPulse
pip install -e ".[dev,dbt]"
```

## Run headless overnight (the main way)

```
python scripts/orchestrate.py run --dry-run                          # see which tasks would run and where they are blocked
python scripts/orchestrate.py run --auto-approve-harness             # run, then walk away
```

- The script reads `scripts/prs.toml` and runs up to 5 `claude -p` sessions in parallel (`--parallel N` to change),
  one worktree `.claude/worktrees/pr-<id>` per task. The real number of sessions is limited by the width of the DAG.
  No session waits for a person: a command that needs a prompt is denied and logged.
- Each PR has two kinds of session: the PR session (Fable) writes code until CI is green and then reports `ready`; after that a new
  review session (`--agent code-reviewer`, Opus) reviews independently. On PASS the script checks the 3 required
  checks and merges; on FAIL the PR session runs again with the review verdict, at most 2 review rounds.
- CI on `develop` red (after a merge): no new tasks are opened, no release. Only a green CI run
  clears the flag (a `gh` error or a cancelled run leaves it unchanged). If no session is running at that point, the script stops
  and writes `develop-ci` into "Tasks for Lam"; fix it and run again.
- A session silent for more than 30 minutes (`--stall-minutes`, except while waiting for a quota reset) is treated as hung and stopped.
  A failed task is retried once (2 runs in total); the retry receives the reason and the tail of the log
  from the previous run.
- `--auto-approve-harness`: if S0.4 merged green and the log has no unexpectedly denied command, treat
  the harness as approved and continue from PR B. Drop this flag if you want to read `docs/harness-check.md` yourself first.
- Missing cloud secrets: do all tasks that need no cloud (S0.4, B, 2, 3, 4, 5, 6, 12) and then stop at `probe`.
- Out of quota: run until it is exhausted. 5-hour limit: the session waits by itself; if the session exits because of the limit, the script sleeps until the reset time and then
  `--resume`s that same session. Weekly limit (reset more than 6 hours away): stop and report; add `--hold-at <percent>` (integer 1–100) to stop opening new tasks when the 5-hour or 7-day quota reaches that threshold; `--hold-at-90` is equivalent to `--hold-at 90` (leaves 10% of quota).
- Release `develop → main` happens automatically after B, 7, 11, 14 when CI is green.

**Stop:** create the file `.claude/state/STOP`. Tasks in progress finish and then the script stops.

**In the morning, look at:** `logs/run-<date>.md` (tasks for Lam, result of each task, progress, list of commands
asked/denied). Detailed log per task: `logs/pr-<id>.jsonl`.

**Daily report by email:** each finished session writes one line (turns, minutes, USD, denied commands,
subagents) to `logs/orchestrator-metrics.csv`. At 22:00 (`--daily-hour`), if the orchestrator is running,
it writes `logs/daily-<date>.md` for the **24 hours up to the send time** (a session that finishes after 22:00 goes into the next day's mail;
`daily --date` takes exactly that calendar day), containing: merged PRs, failures/review FAILs with reasons, cost per task, proposed
rules from repeated denied commands, tasks for Lam. Then it sends the mail, once per day; it only marks
"sent" when the mail really went out, so setting the SMTP variables late in the evening still lets `daily` send.

- Enable mail: create a Gmail App password, then set Windows environment variables (open a new terminal afterwards):
  `setx REPOPULSE_SMTP_USER "<gmail>"`, `setx REPOPULSE_SMTP_APP_PASSWORD "<app password>"`,
  optionally `setx REPOPULSE_REPORT_TO "<recipient>"`. If missing, it only writes the file.
- When the orchestrator is not running at 22:00, set up Task Scheduler to call the `daily` command (it does not send duplicates):
  `schtasks /Create /SC DAILY /ST 22:05 /TN RepoPulseDaily /TR "python D:\Desktop\RepoPulse\scripts\orchestrate.py daily"`
- Rule proposals are only proposals: Lam reviews them and adds them to `.claude/rules/` himself (a protected path).

```
python scripts/orchestrate.py daily [--date 2026-11-11] [--no-send] [--force]
python scripts/orchestrate.py report         # progress by merged PR
python scripts/orchestrate.py quota          # latest quota
python scripts/orchestrate.py clean --dry-run
```

## Open a manual session (when you need to edit the harness or review)

```
claude --permission-mode auto --settings .claude/auto-mode.json --model fable --effort high
```

> Continue RepoPulse: run `python scripts/orchestrate.py report`, read the latest `logs/` and
> `docs/plan.md`, compare against `gh pr list` and `git worktree list`, then report the remaining work.
