# Contributing to GitHub Analytics Data Pipeline

Thanks for helping. Keep changes small: one pull request does one thing.

## Run it locally

Install and run steps are in [docs/developer-guide.md](../docs/developer-guide.md); the project
overview is in the [README.md](../README.md). In short:

```bash
pip install -e ".[dev,dbt]"
pre-commit install
```

## Branch and pull request

1. Branch from the latest `origin/develop`, named `<type>/<short-description>`
   (`feature/`, `fix/`, `test/`, `docs/`, `chore/`).
2. Write a failing test first, then the code that makes it pass.
3. Open the pull request into `develop` (not `main`) and fill in the
   [pull request template](pull_request_template.md). Title: `<type>: <short description>`.

## Checks that must pass

CI runs three required jobs: `Lint`, `Unit Tests` and `dbt build (DuckDB)`. Run the same checks
locally before pushing:

```bash
python -m ruff check .
python -m ruff format --check .
python -m pytest
python -m dbt.cli.main build --project-dir transform --profiles-dir transform --target ci
```

Never commit secrets or a `.env` file. Label simulated data as simulated.

## Issues

Use the bug report or feature request template. Follow the [Code of Conduct](CODE_OF_CONDUCT.md).
