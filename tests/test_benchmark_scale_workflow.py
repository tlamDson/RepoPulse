"""Static checks on .github/workflows/benchmark-scale.yml (no GitHub, no network)."""

from __future__ import annotations

from pathlib import Path

import yaml

WORKFLOW = Path(__file__).resolve().parents[1] / ".github" / "workflows" / "benchmark-scale.yml"
REQUIRED_CHECK_NAMES = {"Lint", "Unit Tests", "dbt build (DuckDB)"}


def load():
    assert WORKFLOW.is_file(), f"missing workflow file {WORKFLOW}"
    data = yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))
    assert isinstance(data, dict), "workflow must be a YAML mapping"
    return data


def triggers(data):
    # PyYAML (YAML 1.1) reads the bare key `on` as boolean True.
    return data.get("on", data.get(True))


def jobs(data):
    return data["jobs"]


def steps(data):
    return [s for job in jobs(data).values() for s in job.get("steps", [])]


def _index(all_steps, predicate) -> int:
    hits = [i for i, s in enumerate(all_steps) if predicate(s)]
    assert len(hits) == 1, hits
    return hits[0]


def _uses(prefix):
    return lambda s: str(s.get("uses", "")).startswith(prefix)


def _runs(fragment):
    return lambda s: fragment in s.get("run", "")


def test_workflow_name():
    assert load()["name"] == "Benchmark scale"


def test_triggers_dispatch_and_path_filtered_pull_request():
    on = triggers(load())
    assert "workflow_dispatch" in on
    pr = on["pull_request"]
    assert set(pr["branches"]) == {"develop", "main"}
    paths = set(pr["paths"])
    assert {
        ".github/workflows/benchmark-scale.yml",
        "benchmarks/scale*",
        "benchmarks/spark_naive.py",
        "benchmarks/queries/**",
        "benchmarks/queries_spark/**",
        "benchmarks/naive_duckdb.py",
        "benchmarks/measure.py",
    } <= paths
    for banned in ("runs.csv", "docs/**", "tests/**", "benchmarks/**"):
        assert banned not in paths, banned
    assert not any(p.startswith(("docs", "tests", "runs.csv")) for p in paths)
    assert "push" not in on
    assert "schedule" not in on
    assert "pull_request_target" not in on


def test_permissions_are_read_only():
    data = load()
    assert data.get("permissions") == {"contents": "read"}
    for job in jobs(data).values():
        assert job.get("permissions", data["permissions"]) == {"contents": "read"}


def test_concurrency_cancels_per_branch():
    conc = load().get("concurrency")
    assert isinstance(conc, dict)
    assert "github.head_ref || github.ref" in conc["group"]
    assert conc["group"].startswith("benchmark-scale-")
    assert conc.get("cancel-in-progress") is True


def test_single_job_with_bounded_timeout_and_no_required_check_name():
    all_jobs = jobs(load())
    assert len(all_jobs) == 1
    for key, job in all_jobs.items():
        assert key not in REQUIRED_CHECK_NAMES
        assert job.get("name", key) not in REQUIRED_CHECK_NAMES
        assert "if" not in job
        assert isinstance(job.get("timeout-minutes"), int)
        assert 0 < job["timeout-minutes"] <= 350


def test_sets_up_python_311_and_java_17():
    all_steps = steps(load())
    py = all_steps[_index(all_steps, _uses("actions/setup-python@"))]
    assert str(py["with"]["python-version"]) == "3.11"
    java = all_steps[_index(all_steps, _uses("actions/setup-java@"))]
    assert java["with"]["distribution"] == "temurin"
    assert str(java["with"]["java-version"]) == "17"


def test_installs_the_spark_extra():
    runs = "\n".join(s.get("run", "") for s in steps(load()))
    assert 'pip install -e ".[dev,spark]"' in runs


def test_cache_is_restored_before_prepare_and_saved_before_the_benchmark():
    all_steps = steps(load())
    restore = _index(all_steps, _uses("actions/cache/restore@"))
    prepare = _index(all_steps, _runs("benchmarks/scale_prepare.py"))
    save = _index(all_steps, _uses("actions/cache/save@"))
    bench = _index(all_steps, _runs("benchmarks/scale_bench.py"))
    assert restore < prepare < save < bench
    assert save == prepare + 1  # saved right after preparing, even if the benchmark fails later
    key = all_steps[restore]["with"]["key"]
    assert "hashFiles(" in key
    assert "benchmarks/scale.toml" in key
    assert "benchmarks/scale_prepare.py" in key
    assert not any(_uses("actions/cache@")(s) for s in all_steps)  # no implicit post-job save


def test_benchmark_writes_rows_and_summary():
    all_steps = steps(load())
    bench = all_steps[_index(all_steps, _runs("benchmarks/scale_bench.py"))]["run"]
    assert "--git-sha" in bench
    assert "--out out/scale-rows.csv" in bench
    summary = [s for s in all_steps if "GITHUB_STEP_SUMMARY" in s.get("run", "")]
    assert summary and all(s.get("if") == "always()" for s in summary)


def test_uploads_rows_and_manifest_even_on_failure():
    all_steps = steps(load())
    upload = all_steps[_index(all_steps, _uses("actions/upload-artifact@v4"))]
    assert upload.get("if") == "always()"
    with_ = upload["with"]
    assert with_["name"] == "benchmark-scale"
    assert "out/scale-rows.csv" in with_["path"]
    assert "manifest.json" in with_["path"]


def test_uses_no_secrets_and_no_expression_in_run():
    load()
    assert "secrets." not in WORKFLOW.read_text(encoding="utf-8")
    for step in steps(load()):
        assert "${{" not in step.get("run", ""), step


def test_does_not_touch_runs_csv():
    runs = "\n".join(s.get("run", "") for s in steps(load()))
    assert "record_run" not in runs
    assert "runs.csv" not in runs
