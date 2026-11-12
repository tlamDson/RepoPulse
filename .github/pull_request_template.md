## Summary

- <main change>
- <reason>

## RED

<!-- Paste the real output of the RED test run, before any implementation. The first commit of the PR contains only tests. -->

```
```

## Test plan

- [ ] Tests written first and seen failing for real (assertion failure or a genuine import error)
- [ ] `ruff check .` and `ruff format --check .` are clean
- [ ] `pytest` is green
- [ ] `dbt build` on DuckDB is green (if the PR touches `transform/`)
- [ ] No `.env`, token or raw data committed
- [ ] New measurements (if any) are in `runs.csv`, not typed by hand into the README
