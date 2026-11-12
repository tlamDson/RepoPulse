# TDD (Red → Green → Refactor) — mandatory for every feature/fix

> The single source for RepoPulse's testing process. The stack and pitfalls are in
> `.claude/rules/tech-defaults.md` — this file only describes the **process**.

1. **RED** — write a failing test first, without writing the implementation. **The first commit of the PR contains only tests.** Run the tests and confirm they are really red (an assertion failure, or an `ImportError`/`ModuleNotFoundError` that genuinely comes from the module not existing yet, **not a configuration error** such as a missing fixture, a wrong path or a missing dependency). Paste the red output into the `## RED` section of the PR description.
2. **GREEN** — the minimum code to pass, with no logic that is not covered by a test.
3. **REFACTOR** — improve the code and rerun all tests; nothing may break.

```bash
python -m pytest                                   # all Python tests
python -m pytest tests/test_<module>.py -x         # one file
python -m pytest -k <test_name>                    # one case
python -m dbt.cli.main build --project-dir transform --profiles-dir transform --target ci   # dbt on DuckDB
```

Always use the `python -m` form (on the local machine `pytest`/`ruff`/`dbt` are not on the PATH;
`python -m` also guarantees importing the package of the current worktree). `pyproject.toml` already
sets `addopts = "-q"`: do not add `-q` again (it becomes `-qq` and loses the `N passed` summary line
that must be pasted as evidence).

**New first-party package** (`eventspec`, `simkit`, `benchmarks`...): when the tests-only commit imports
a package that does not exist yet, ruff (isort) classifies it as third-party and reports `I001`. Add the
package name to `[tool.ruff.lint.isort] known-first-party` in `pyproject.toml` **in the test commit
itself** and record it in the `## Assumptions` section — this is lint configuration and does not violate "the first commit contains only tests".

## Where tests live

- **Python** (`repopulse/`, `eventspec/`, `simkit/`...): `tests/test_<module>.py`, shared fixtures in `tests/conftest.py`, small sample data in `tests/fixtures/`.
- **PySpark**: tests run with a local `SparkSession` (`master("local[1]")`), a session-scoped `spark` fixture in `conftest.py`. A few dozen rows of data; never read real GH Archive files.
- **dbt**: generic and singular tests in `transform/models/**/schema.yml` and `transform/tests/`; dbt unit tests (`unit_tests:`) for transformation logic; `contract: {enforced: true}` for gold models. A dbt test is "red" when `python -m dbt.cli.main build … --target ci` fails in the right place.

## Conventions

- **Never call real services in a unit test** (Databricks, MotherDuck, `data.gharchive.org`, GitHub API). Use small fixtures in `tests/fixtures/` or `monkeypatch`/`unittest.mock`.
- Random data must use a fixed seed; a test checks "same seed gives same data".
- Idempotency tests: rerunning with the same input (same date, hour) produces no duplicate records.
- PII tests: assert that no raw email/value remains in layers after the hashing step.
- A bug found while writing tests that is outside the task's scope → write a test asserting the **current** behavior with a `# [BUG]` comment, and do not fix it. Record it in `.claude/rules/tech-defaults.md` under "Technical debt" in the 3-part format from `workflow.md`.

Tests are independent (no dependence on run order), assertions are specific to behavior, no vague `assert x`. No `pytest.mark.skip` without a reason. Do not commit new code without tests.
