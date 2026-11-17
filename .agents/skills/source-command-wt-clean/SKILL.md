---
name: "source-command-wt-clean"
description: "Clean up worktrees of merged PRs (preview first, then delete)"
---

# source-command-wt-clean

Use this skill when the user asks to run the migrated source command `wt-clean`.

## Command Template

Clean up the worktrees under `.Codex/worktrees/` with the script, never with a manual `rm -rf`:

1. Run `python scripts/orchestrate.py clean --dry-run` and show me the table: which worktrees
   will be removed (PR merged, clean tree, no session running), and which are kept and why.
2. If there is at least one `remove` row, run `python scripts/orchestrate.py clean`.
3. Report the result and the worktrees that were kept (with the reason: PR still open,
   uncommitted changes...).

Do not remove a worktree that has a `Codex` session running in it. If Windows reports
"Permission denied" while deleting, some process still has that folder open; tell me, and do not
try to delete it another way.
