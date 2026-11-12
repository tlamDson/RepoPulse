# Command style — running headless without being blocked

> Mandatory for the main session and every subagent. The orchestrating session pastes this file
> **verbatim** into each agent's brief. Reason: headless sessions run with `--permission-prompts none`,
> so any command that would normally have to ask is **refused immediately**. In the Auto_Apply history,
> 221 of 414 permission prompts came from exactly the command styles forbidden below.

## Do

- **One command per Bash call.** Do not chain with `&&`, `;`, `|` unless really necessary. If a pipe
  is needed, keep it short and use read-only commands only (`| head`, `| tail`, `| grep`).
- **No `cd`.** Use `git -C <path>`, `dbt --project-dir transform --profiles-dir transform`,
  `pytest <path>`, absolute paths, or paths relative to the current directory.
- **Write files with the Write/Edit tools**, not `cat > file <<EOF`, `echo > file`, `tee`.
  Evidence (red test output, review results) is also written with Write.
- **Read and search with Read/Grep/Glob**, not `cat`, `grep -r`, `find`, `python -c` to probe code.
- **git and gh run through Bash**, not PowerShell.
- **Temporary files** go in the session scratchpad (absolute path), created with Write.
- **Mutation testing**: only on **committed** files, edit with Edit, run the tests, then revert with
  `git checkout -- <file>` and check that `git status --porcelain` is clean. `git checkout --`
  **cannot** revert an untracked file.
- **Call Python tools with `python -m`**: `python -m pytest`, `python -m ruff`,
  `python -m dbt.cli.main build …`. Bare `pytest`/`ruff`/`dbt` commands are not on the PATH of the
  local machine (exit code 127).
- **Waiting for CI**: run `gh pr checks <number> --watch --interval 30` in the foreground with a
  timeout of 600000; when it times out, call it again until it finishes. Right after a push the command
  may report `no checks reported` (exit 1) because the checks have not registered yet: wait and call it
  again. Exit 1 also happens when a job is red — read the output, do not look only at the exit code.

## Do not

- Do not use shell variables for paths (`S=...; ... "$S/x"`), and no `$(...)` containing a write command.
- No `rm -rf`, no deleting directories. Worktrees are cleaned by `scripts/orchestrate.py clean`.
- No `git stash` (shared across all worktrees), no `git reset --hard`, no `git clean`,
  no rebase, no force push.
- Do not push to `develop` or `main`; only push the branch of the task at hand.
- Do not change anything in `.claude/` (outside `.claude/worktrees/`), and do not edit
  `.github/workflows/ci.yml` unless the PR was assigned exactly that task.
- Do not read `.env`, do not run `gh secret`, do not print tokens.
- No `AskUserQuestion`, no `EnterPlanMode`. Record small decisions in the PR's `## Giả định` (Assumptions) section.

## If a command is refused

1. Read the reason. Rewrite the command following the rules above (usually: split the command, drop
   `cd`, replace a heredoc with Write).
2. Retry **once**.
3. Still refused: record the command and the reason in the PR (the `## Giả định` section), skip the step
   if it is not mandatory; if it is mandatory, finish with `RESULT: blocked <blocked command>`.
