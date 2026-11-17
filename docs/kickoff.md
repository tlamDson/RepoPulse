# RepoPulse · How to run

Open a terminal from the repo directory, typing `D:` in capitals:

```
cd D:\Desktop\RepoPulse
pip install -e ".[dev,dbt]"
```

## Run headless (the main way)

Headless runs are driven by conductor (`D:\Desktop\Conductor`), RepoPulse's only orchestrator. The task
DAG is `projects/repopulse/tasks.toml` in that repo; how to start, watch and stop a run is in its README.

## Open a manual session (when you need to edit the harness or review)

```
claude --permission-mode auto --settings .claude/auto-mode.json --model fable --effort high
```

> Continue RepoPulse: read `docs/plan.md`, compare against `gh pr list` and `git worktree list`,
> then report the remaining work.
