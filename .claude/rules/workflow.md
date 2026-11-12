# Workflow — hub

> Inviolable rules + headless runs + technical debt. Details of git/PR/commits, the testing process
> and command style live in the 3 imported files below — edit them there, do not repeat them here.

@.claude/rules/developer-guide.md
@.claude/rules/tdd.md
@.claude/rules/command-style.md

## Inviolable rules

1. **Every change goes through a PR.** Feature → `develop` (squash). Release `develop` → `main` (merge commit). No direct push to `develop`/`main`, no force push, no `--admin`.
2. **Merge a feature into `develop` when two conditions hold:** CI is green on all 3 jobs (`Lint`, `Unit Tests`, `dbt build (DuckDB)`) and `code-reviewer` concludes PASS. When both hold, merge immediately without asking.
3. **Release `develop` → `main`** is done by `scripts/orchestrate.py` (or `release-manager`) at the milestones in `scripts/prs.toml` (`release_after`), once the release PR's CI is green. A real merge commit, no squash.
4. **A PR that changes `.claude/settings.json`, `.claude/auto-mode.json` or a hook** is reviewed and merged by the repo owner.
5. **TDD is mandatory**, the first commit contains only tests, seen red for real. Details: `.claude/rules/tdd.md`.
6. **Measurements come only from `runs.csv`; simulated data is clearly labelled "simulated".**
7. **Update `CLAUDE.md` + `.claude/rules/*`** when a task changes a convention, tooling or process.
8. **One PR, one task.** One task in `scripts/prs.toml`, or one improvement/bug fix; other things found along the way are split out (headless: record under Technical debt or `## Giả định`). Details: the "One PR, one task" section of `.claude/rules/developer-guide.md`.

## Running headless (`scripts/orchestrate.py`)

Each PR is a `claude -p --permission-mode auto --permission-prompts none` session running in its own worktree `.claude/worktrees/pr-<id>`, created by the script from `origin/develop`. There is no human to ask:

1. **Do not ask.** `AskUserQuestion` is removed; record every small decision as one line in the `## Giả định` (Assumptions) section of the PR description.
2. **Do not create child worktrees.** The PR session and every subagent work right in the PR's worktree; no `isolation: "worktree"`, no `EnterWorktree`.
3. **Delegate to the specialist agent** named in the `agent` column of `scripts/prs.toml`, pasting `.claude/rules/command-style.md` verbatim into the brief.
4. **Wait for CI** with `gh pr checks <number> --watch --interval 30` in the foreground, timeout 600000, looping until it finishes. If CI is red, fix and push again.
5. **Independent review, no self-review.** The PR session stops when CI is green (`RESULT: ready <number>`), does not call `code-reviewer`, and does not merge. The script runs a fresh `claude -p --agent code-reviewer` session (Opus, medium, not seeing the writing session's context); that session posts a comment and ends with `REVIEW: PASS` | `REVIEW: FAIL <finding>`. PASS: the script itself checks the 3 required checks, then runs `gh pr merge --squash`. FAIL: the PR session reruns with the review verdict; at most 2 review rounds, then it stops.
6. **If permission is denied**, follow the "If a command is refused" section of `command-style.md`.
7. **Do not edit `docs/progress.md`** in a feature PR (parallel runs would conflict); the script regenerates the progress itself.
8. **The last line** of a PR session is always `RESULT: ready <PR number>` | `RESULT: failed <reason>` | `RESULT: blocked <what the repo owner must do>`. The script reads this line to continue (`RESULT: merged` is only still understood for old sessions).

Every time a command is asked about or denied, the hook `log_permission.py` writes it to `.claude/state/permission-log.jsonl`; the nightly report is `logs/run-<date>.md`.

## Interactive sessions

When Lam opens a manual session: in the main checkout only read and edit `docs/`; code changes still go through a worktree and a PR as above. If there is a real question, ask right away with `AskUserQuestion`.

## Technical debt

A bug found outside the scope of the task → write a test asserting the **current** behavior (with `# [BUG]`), and do not fix it. Record it immediately in `.claude/rules/tech-defaults.md` under "Technical debt", each item with all three parts:

- **Symptom** — what the system/user sees.
- **Cause** — where in the code, with `file:line`.
- **Trigger condition** — when it becomes a real failure.

When it is fixed, remove it from the list in the same commit as the fix.
