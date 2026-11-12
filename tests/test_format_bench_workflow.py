"""Static checks on .github/workflows/format-bench.yml (no GitHub, no network)."""

from __future__ import annotations

from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = ROOT / ".github" / "workflows" / "format-bench.yml"
BENCHMARK = ROOT / ".github" / "workflows" / "benchmark.yml"
REQUIRED_CHECK_NAMES = {"Lint", "Unit Tests", "dbt build (DuckDB)"}


def load(path: Path = WORKFLOW):
    assert path.is_file(), f"missing workflow file {path}"
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
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


def _one_line(run: str) -> str:
    return " ".join(run.split())


def test_workflow_name():
    assert load()["name"] == "Format benchmark (Delta vs Iceberg)"


def test_triggers_dispatch_and_path_filtered_pull_request():
    on = triggers(load())
    assert "workflow_dispatch" in on
    pr = on["pull_request"]
    assert set(pr["branches"]) == {"develop", "main"}
    paths = set(pr["paths"])
    assert {
        ".github/workflows/format-bench.yml",
        "benchmarks/format_bench.py",
        "benchmarks/spark_naive.py",
        "benchmarks/queries_spark/**",
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
    assert conc["group"].startswith("format-bench-")
    assert conc.get("cancel-in-progress") is True


def test_single_job_with_bounded_timeout_and_no_required_check_name():
    all_jobs = jobs(load())
    assert len(all_jobs) == 1
    for key, job in all_jobs.items():
        assert key not in REQUIRED_CHECK_NAMES
        assert job.get("name", key) not in REQUIRED_CHECK_NAMES
        assert "if" not in job
        assert job["runs-on"] == "ubuntu-latest"
        assert isinstance(job.get("timeout-minutes"), int)
        assert 0 < job["timeout-minutes"] <= 90


def test_sets_up_python_311_java_17_and_the_spark_extra():
    all_steps = steps(load())
    py = all_steps[_index(all_steps, _uses("actions/setup-python@"))]
    assert str(py["with"]["python-version"]) == "3.11"
    java = all_steps[_index(all_steps, _uses("actions/setup-java@"))]
    assert java["with"]["distribution"] == "temurin"
    assert str(java["with"]["java-version"]) == "17"
    runs = "\n".join(s.get("run", "") for s in all_steps)
    assert 'pip install -e ".[dev,spark]"' in runs


def test_reference_day_cache_is_shared_with_the_benchmark_workflow():
    all_steps = steps(load())
    cache = all_steps[_index(all_steps, _uses("actions/cache@"))]
    bench_steps = steps(load(BENCHMARK))
    bench_cache = bench_steps[_index(bench_steps, _uses("actions/cache@"))]
    assert cache["with"]["path"] == bench_cache["with"]["path"] == "data/raw/gharchive"
    assert cache["with"]["key"] == bench_cache["with"]["key"]
    assert "hashFiles('benchmarks/reference.toml')" in cache["with"]["key"]


def test_steps_run_in_order_fetch_bench_summary_upload():
    all_steps = steps(load())
    java = _index(all_steps, _uses("actions/setup-java@"))
    cache = _index(all_steps, _uses("actions/cache@"))
    fetch = _index(all_steps, _runs("benchmarks/fetch_reference.py"))
    bench = _index(all_steps, _runs("benchmarks/format_bench.py"))
    summary = _index(all_steps, _runs("GITHUB_STEP_SUMMARY"))
    upload = _index(all_steps, _uses("actions/upload-artifact@v7"))
    assert java < bench
    assert cache < fetch < bench < summary < upload


def test_benchmark_writes_rows_and_summary():
    all_steps = steps(load())
    run = _one_line(all_steps[_index(all_steps, _runs("benchmarks/format_bench.py"))]["run"])
    assert "--runs 5" in run
    assert '--git-sha "$GIT_SHA"' in run
    assert "--out out/format-rows.csv" in run
    assert "--summary out/format-summary.md" in run
    job = next(iter(jobs(load()).values()))
    assert "github.event.pull_request.head.sha" in job["env"]["GIT_SHA"]
    assert "github.sha" in job["env"]["GIT_SHA"]


def test_job_summary_runs_always():
    summary = [s for s in steps(load()) if "GITHUB_STEP_SUMMARY" in s.get("run", "")]
    assert summary and all(s.get("if") == "always()" for s in summary)


def test_uploads_rows_and_summary_even_on_failure():
    all_steps = steps(load())
    upload = all_steps[_index(all_steps, _uses("actions/upload-artifact@v7"))]
    assert upload.get("if") == "always()"
    with_ = upload["with"]
    assert with_["name"] == "format-bench"
    assert set(with_["path"].split()) == {"out/format-rows.csv", "out/format-summary.md"}
    assert with_.get("if-no-files-found") == "warn"


def test_uses_no_secrets_and_no_expression_in_run():
    load()
    assert "secrets." not in WORKFLOW.read_text(encoding="utf-8")
    for step in steps(load()):
        assert "${{" not in step.get("run", ""), step


def test_does_not_touch_runs_csv():
    runs = "\n".join(s.get("run", "") for s in steps(load()))
    assert "record_run" not in runs
    assert "runs.csv" not in runs
