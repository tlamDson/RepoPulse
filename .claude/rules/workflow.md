# Workflow — hub

> Inviolable rules + headless runs + technical debt. Details of git/PR/commits, the testing process
> and command style live in the 3 imported files below — edit them there, do not repeat them here.

@.claude/rules/developer-guide.md
@.claude/rules/tdd.md
@.claude/rules/command-style.md

## Inviolable rules

1. **Every change goes through a PR.** Feature → `develop` (squash). Release `develop` → `main` (merge commit). No direct push to `develop`/`main`, no force push, no `--admin`.
2. **Merge a feature into `develop` when two conditions hold:** CI is green on all 3 jobs (`Lint`, `Unit Tests`, `dbt build (DuckDB)`) and `code-reviewer` concludes PASS. When both hold, merge immediately without asking.
3. **Release `develop` → `main`** is done by conductor (or `release-manager`) at the milestones marked `release_after` in conductor's `projects/repopulse/tasks.toml`, once the release PR's CI is green. A real merge commit, no squash.
4. **A PR that changes `.claude/settings.json`, `.claude/auto-mode.json` or a hook** is reviewed and merged by the repo owner.
5. **TDD is mandatory**, the first commit contains only tests, seen red for real. Details: `.claude/rules/tdd.md`.
6. **Measurements come only from `runs.csv`; simulated data is clearly labelled "simulated".**
7. **Update `CLAUDE.md` + `.claude/rules/*`** when a task changes a convention, tooling or process.
8. **One PR, one task.** One task in conductor's `projects/repopulse/tasks.toml`, or one improvement/bug fix; other things found along the way are split out (headless: record under Technical debt or `## Assumptions`). Details: the "One PR, one task" section of `.claude/rules/developer-guide.md`.

## Running headless (conductor)

Headless sessions are started by conductor (D:/Desktop/Conductor), one per task, each in its own worktree `.claude/worktrees/cd-<id>` created from `origin/develop`. Conductor's prompt for the session is the authority on how it ends; in short:

1. **Do not ask.** Record every small decision as one line in the `## Assumptions` section of the PR description.
2. **Do not create child worktrees.** Work right in the task's worktree; no `isolation: "worktree"`, no `EnterWorktree`.
3. **Write the tests and the code yourself**, TDD first (`.claude/rules/tdd.md`), following `.claude/rules/command-style.md`.
4. **Push, open the PR against `develop`, then stop.** Do not wait for CI, do not review your own work, do not merge: conductor waits for the 3 required checks, runs an independent review (Codex or `code-reviewer`), sends the session back with the failing log or the review finding, and squash-merges on PASS.
5. **If permission is denied**, follow the "If a command is refused" section of `command-style.md`.
6. **Do not edit `docs/progress.md`** in a feature PR.
7. **The last line** is exactly the one conductor's prompt asks for (`RESULT: pushed <PR>` | `RESULT: failed <reason>` | `RESULT: blocked <what the repo owner must do>`).

Every time a command is asked about or denied, the hook `log_permission.py` writes it to `.claude/state/permission-log.jsonl`; the nightly report is `logs/run-<date>.md`.

## Interactive sessions

When Lam opens a manual session: in the main checkout only read and edit `docs/`; code changes still go through a worktree and a PR as above. If there is a real question, ask right away with `AskUserQuestion`.

## Technical debt

A bug found outside the scope of the task → write a test asserting the **current** behavior (with `# [BUG]`), and do not fix it. Record it immediately in `.claude/rules/tech-defaults.md` under "Technical debt", each item with all three parts:

- **Symptom** — what the system/user sees.
- **Cause** — where in the code, with `file:line`.
- **Trigger condition** — when it becomes a real failure.

When it is fixed, remove it from the list in the same commit as the fix.
