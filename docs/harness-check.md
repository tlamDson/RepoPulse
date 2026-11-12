# RepoPulse · Harness check (S0.4)

The S0.4 PR session (`feature/runs-log`) fills this table with real evidence (commands, output, log paths).
After reading it, the repo owner adds a line `Lam đã duyệt` (Vietnamese for "Lam approved") at the end of the file; the orchestrator only runs PR B onward when that
line is present (or when run with `--auto-approve-harness` and the log is clean).

Filled in on 2026-11-11 in the S0.4 PR session, PR [#10](https://github.com/tlamDson/RepoPulse/pull/10).
The table records the state at check time, before the merge; the log kept growing after that point.

| Check | Passes when | Result | Evidence |
|---|---|---|---|
| Permission prompts / denials | 0 outside the deliberately banned command (`gh secret`) | Pass: 0 lines for task `S0.4` | `.claude/state/permission-log.jsonl` has 4 lines, all 4 are `"task": "manual"` (the repo owner's manual session, 05:49–06:16Z). The three sub-agents (`pipeline-ops`, `code-reviewer` two rounds) all reported no command was denied |
| Worktree | Session runs in `.claude/worktrees/pr-S0-4`, branch cut from `origin/develop` | Pass | Working directory `D:\Desktop\RepoPulse\.claude\worktrees\pr-S0-4`, branch `feature/runs-log`; `git log --oneline -1 origin/develop` → `9289337 chore: add headless orchestrator and Claude Code harness (#2)`. Sub-agents worked directly in this directory and created no child worktree. No `d:`/`D:` mismatch error |
| Model and effort | PR session Fable/high; `pipeline-ops` and `code-reviewer` Opus/medium | Model passes; effort could not be verified | `logs/pr-S0.4.jsonl`: the main session's messages carry `"model":"claude-fable-5-2"`, the sub-agents' messages carry `"model":"claude-opus-5-5"`. No effort field was found in the log (searched `effort`, `effortLevel`, `reasoning_effort`), so effort is known only from the agent frontmatter `effort: medium`, with no runtime evidence |
| Hook | `ruff format` runs after editing `.py`; the log hook writes a line | Pass | Wrote a probe file `hook_probe.py` (outside the repo) containing `x=1` / `def f( a,b ):` with the Write tool; reading it back gave `x = 1` / `def f(a, b):`, and the harness reported `PostToolUse hook modified ...hook_probe.py`. Hook log: `permission-log.jsonl` has `PermissionRequest` and `PermissionDenied` lines |
| Branch protection | `develop` and `main` require a PR and 3 checks; direct push is denied | API passes; deny rule exists but was not tried | `gh api repos/tlamDson/RepoPulse/branches/develop/protection` and `.../main/protection` both return `checks: ["Lint","Unit Tests","dbt build (DuckDB)"]`, `pr_required: true`, `enforce_admins: true`, `force_push: false`, `deletions: false`, `strict: false`. The deny rule for `git push origin develop`/`main` is at `.claude/settings.json:38-47`; this session did not try a direct push |
| CI | Red on the test-only commit, green after the code is added | Pass | Test commit `e8ff0b4`: `Unit Tests` fails with `ModuleNotFoundError: No module named 'repopulse.runs_log'`, `Lint` and `dbt build (DuckDB)` pass — [run 42972278922](https://github.com/tlamDson/RepoPulse/actions/runs/42972278922). Code commit `6e44f9b`: 3 jobs pass — [run 42972370013](https://github.com/tlamDson/RepoPulse/actions/runs/42972370013). Follow-up test commit `850d489`: 3 jobs pass — [run 42973204178](https://github.com/tlamDson/RepoPulse/actions/runs/42973204178) |
| Review | `code-reviewer` copies real test output and catches mutations | Pass, with process deviations (see notes 3, 4) | Round 1 [FAIL](https://github.com/tlamDson/RepoPulse/pull/10#issuecomment-6788864712): copied pytest/ruff output, tried 13 mutations, 11 caught, 2 survived (one blocked the merge: missing `datetime` case for `data_date`). After commit `850d489`, round 2 [PASS](https://github.com/tlamDson/RepoPulse/pull/10#issuecomment-6789118704): both mutations were caught |
| Secret | Push protection on; `.env` not readable | Push protection passes; `.env` has a deny rule but was not tried | `gh api repos/tlamDson/RepoPulse --jq .security_and_analysis` → `secret_scanning: enabled`, `secret_scanning_push_protection: enabled`. The deny rules `Read(./.env)`, `Read(./**/.env)`, `Read(./.env.local)` are at `.claude/settings.json:35-37`; this session did not try to read `.env` |
| Quota | Record the first real `rate_limit_event` seen (if any) | Events present, never blocked | At check time `logs/pr-S0.4.jsonl` had 19 lines containing `rate_limit_event`; none has `"status":"rejected"` or `"errorCode":"credits_required"` |

## Notes for the repo owner

Places where the harness behaved differently from the documentation, seen in this session. A PR session
may not edit `.claude/`, so they are only recorded here.

1. **`pytest` and `ruff` are not in the Git Bash PATH.** `pytest -q` and `ruff --version` return
   exit code 127 (`command not found`); `python -m pytest` and `python -m ruff` work. The commands
   in `CLAUDE.md` and `.claude/rules/tdd.md` are written in bare form (`pytest`, `ruff check .`), so
   every session wastes one failed attempt. The agent brief in this session spelled out the `python -m` form.
2. **A PR's first commit cannot be "test only" if the test imports a first-party module that does not exist yet.**
   With ruff 0.17.4, `from repopulse.runs_log import ...` is sorted into the third-party group while
   `repopulse/runs_log.py` does not exist, and reports `I001`; once the file exists the correct order flips.
   PR #10 added `[tool.ruff.lint.isort] known-first-party = ["repopulse"]` to `pyproject.toml` in
   the test commit itself. `code-reviewer` reproduced and confirmed it. From later PRs this configuration is already present,
   but new packages (`eventspec`, `simkit`, `benchmarks`) will hit exactly this error until they are
   added to the list.
3. **`code-reviewer` has no Edit tool**, while `command-style.md` prescribes trying mutations with
   Edit then `git checkout --`. The reviewer tried mutations in memory with `python -c` (read the file, replace
   a string, `exec` into the module, call `pytest.main`); the file on disk did not change. This works
   but departs from the letter of the "do not use `python -c`" rule.
4. **One `code-reviewer` turn hung.** The turn continuing round 2 (sent via `SendMessage` to the round 1 agent)
   stopped with `Agent stalled: no progress for 600s (stream watchdog did not recover)` before
   reaching a verdict. The orchestrating session opened a new `code-reviewer` with a shorter brief; that turn finished in about
   a minute. Counted as review round 2, not round 3.
5. **`gh pr checks <number> --watch` right after a push** may return `no checks reported on the
   'feature/runs-log' branch` with exit code 1 because the checks have not registered yet; the second call works.
   This command also returns exit code 1 when a job is red, even when red is expected.
6. **The `Co-Authored-By` line** of the two commits made by `pipeline-ops` says `Claude Opus 5.5`, the commit of the
   orchestrating session says `Claude Fable 5.2`. The squash merge combines both.
