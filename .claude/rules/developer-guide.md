# Developer Guide — Git Flow, Worktree, Commit, PR

> Contribution conventions for RepoPulse. Testing process: `.claude/rules/tdd.md`. Command style:
> `.claude/rules/command-style.md`.

## Git Flow

`develop ← <type>/<description>` (PR, **squash**), `main ← develop` (release PR, **merge commit**). No direct push to `develop` or `main`.

- **`main` is the default branch** on GitHub. Scheduled workflows (`schedule`) only run on `main`, and `workflow_dispatch` only sees workflow files that exist on `main`. So the pipeline/cron only starts running after a release; the release milestones are in `scripts/prs.toml` (`release_after`): after PR B (baseline), PR 7 (cron), PR 11 (Pages), PR 14.
- **Feature branches branch off the latest `origin/develop`** and are deleted after merge.
- **Branch types:** `feature/`, `fix/`, `test/`, `docs/`, `chore/`. Lowercase, separated by `-`, with a description that names the scope specifically (`feature/ingest-gharchive`, not `feature/update`).

## Worktree

Each piece of code work has its own worktree under `.claude/worktrees/`. Reason: when several sessions run in parallel in the same directory, one session's `git checkout`/`git reset` silently deletes the other's uncommitted changes.

- **Headless:** `scripts/orchestrate.py` creates `.claude/worktrees/pr-<id>` from `origin/develop` and runs the PR session in it. The session and its subagents do not create child worktrees.
- **Manual session:** `git worktree add .claude/worktrees/<name> -b <type>/<description> origin/develop`. Do not use `EnterWorktree` (it branches from the default branch `main`, which is the wrong base).
- **Cleanup:** `python scripts/orchestrate.py clean` (or `/wt-clean`) removes worktrees whose PR is merged, tree is clean, and have no session running in them. Never `rm -rf` a worktree yourself.
- A new worktree has no Python environment of its own: it shares the already installed `pip install -e ".[dev,dbt]"`. Do not copy `.env` into a worktree.
- Windows pitfall: open the terminal with `cd D:\Desktop\RepoPulse` (uppercase `D:`). The paths `d:` and `D:` differ, which makes Claude Code treat them as two different projects (memory, settings).

## Commit Messages

Imperative mood, completing the sentence _"If applied, this commit will… [message]"_. One line, no `feat:`/`fix:` prefix (prefixes are only for PR titles). Standard verbs: `add`, `fix`, `update`, `remove`, `refactor`, `test`, `docs`, `chore`.

```
add failing tests for runs.csv schema validation
add runs.csv append with schema check
fix late-event window off-by-one in silver dedup
```

Do not commit: `.env`/credentials/tokens, `.venv/`, `transform/target/`, `*.duckdb`, raw data (`data/raw/`, `*.json.gz`, `*.parquet`), `logs/`, `.claude/state/`, files unrelated to the task.

## Pull Requests

### One PR, one task

Each PR does exactly **one** task, so that each can be reviewed, reverted and measured on its own.

- **What counts as one task:**
  - A task in `scripts/prs.toml` is always **one** task, exactly the scope of its title and its section in `docs/plan.md`, even when the title contains "and" (for example PR 7). The task scope is set by the repo owner; the reviewer does not split tasks.
  - Work outside `prs.toml` (harness improvements, bug fixes, docs): the title and the first sentence of `## Summary` describe the whole PR without needing "and", "+", or a list of several independent items.
- **Belongs to the same task** (allowed in the same PR):
  - the tests for that task;
  - updates to `CLAUDE.md`, `.claude/rules/*`, `docs/` that the task itself makes necessary;
  - fixes for review findings on that very PR;
  - an out-of-scope bug handled per the "Technical debt" section of `workflow.md`: a `# [BUG]` test asserting the current behavior + an entry in `tech-defaults.md` (do not fix the bug).
- **Does not belong** (do not merge in): fixing an out-of-scope bug, a refactor this task does not need, review suggestions about other code, CI/tooling changes this task does not require.
  - Manual session: open a separate PR.
  - Headless session (one session = one PR, a second PR cannot be opened): record under Technical debt (bug) or one line in `## Giả định` (something worth doing later).
  - If the main task *must* change something else to go green, it may be done in this PR, but state the reason in `## Giả định`.
- Several improvements approved at once: one PR each, even when they edit the same file. Branch after the previous PR merges, or use `gh pr update-branch <number>` when the branch falls behind `develop`.
- Does not apply to `develop → main` release PRs and Dependabot PRs.
- `code-reviewer` treats a PR that does more than one task as a `[chặn]` (blocking) finding.

Features target `develop`; only release PRs target `main`. Title `<type>: <short description>` (< 72 characters): `feat:`, `fix:`, `test:`, `docs:`, `chore:`, `refactor:`, `release:`. The description follows `.github/pull_request_template.md`, with a `## Giả định` (Assumptions) section added when running headless:

```markdown
## Summary
- <main change>
- <reason / impact>

## RED
<real red test output, pasted from the run on the tests-only commit>

## Giả định
- <small decision made on one's own>

## Test plan
- [ ] pytest pass locally
- [ ] dbt build --target ci pass
- [ ] ruff check + ruff format --check clean
- [ ] No secret / .env committed
```

### Merge conditions

| Target | How to merge | Condition |
|---|---|---|
| `develop` | `gh pr merge <number> --squash --delete-branch` | CI green on all 3 jobs + `code-reviewer` PASS (comment recorded on the PR) |
| `main` | `gh pr merge <number> --merge` (PR `develop → main`) | The release PR's CI is green; only at a `release_after` milestone |
| PR changing `.claude/settings.json`, `.claude/auto-mode.json`, hooks | Repo owner merges | Repo owner approves |

Never add `--admin`.

**Before pushing more commits to an open PR**, run `gh pr view <number> --json state` — if the PR is `MERGED`/`CLOSED`, a push does not trigger CI; create a new branch from `origin/develop` for the remaining work.

A branch that has fallen behind `develop`: `gh pr update-branch <number>`, no rebase, no force push.

### Editing `.github/workflows/ci.yml` — do not lose a required check

The names of the three jobs (`Lint`, `Unit Tests`, `dbt build (DuckDB)`) **are** the required status check identifiers of `develop` and `main`. Therefore:

- **Do not rename** these three jobs. If more work is needed, add a new job with a different name.
- **Do not add `paths:` to `on: pull_request`** — the workflow would not run, the check would hang at _Expected_, and the PR could not be merged.
- **Do not put `if:` at job level** on these three jobs. To skip heavy work, gate it at **step** level.
- Do not add other jobs to the required checks on your own.
