"""Static checks on the Trino benchmark: workflow, Docker Compose stack, catalog config and the
Trino translations of Q1-Q6. No Docker, no GitHub, no network: the stack only runs on the
runner (.github/workflows/trino-bench.yml)."""

from __future__ import annotations

import re
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = ROOT / ".github" / "workflows" / "trino-bench.yml"
BENCHMARK = ROOT / ".github" / "workflows" / "benchmark.yml"
STACK_DIR = ROOT / "benchmarks" / "trino"
COMPOSE = STACK_DIR / "docker-compose.yml"
QUERY_DIR = ROOT / "benchmarks" / "queries_trino"
REQUIRED_CHECK_NAMES = {"Lint", "Unit Tests", "dbt build (DuckDB)"}
COMPOSE_CMD = "docker compose -f benchmarks/trino/docker-compose.yml"
QUERIES = tuple(f"q{i}" for i in range(1, 7))


def _yaml(path: Path):
    assert path.is_file(), f"missing file {path}"
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    assert isinstance(data, dict), f"{path} must be a YAML mapping"
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
    return lambda s: fragment in _one_line(s.get("run", ""))


def _one_line(run: str) -> str:
    return " ".join(run.split())


def _step(fragment):
    all_steps = steps(_yaml(WORKFLOW))
    return all_steps[_index(all_steps, _runs(fragment))]


# ---------------------------------------------------------------- workflow


def test_workflow_name():
    assert _yaml(WORKFLOW)["name"] == "Trino benchmark"


def test_triggers_dispatch_and_path_filtered_pull_request():
    on = triggers(_yaml(WORKFLOW))
    assert "workflow_dispatch" in on
    pr = on["pull_request"]
    assert set(pr["branches"]) == {"develop", "main"}
    paths = set(pr["paths"])
    assert {
        ".github/workflows/trino-bench.yml",
        "benchmarks/trino_bench.py",
        "benchmarks/trino/**",
        "benchmarks/queries_trino/**",
        "benchmarks/query_opt.py",
    } <= paths
    for banned in ("runs.csv", "docs/**", "tests/**", "benchmarks/**"):
        assert banned not in paths, banned
    assert "push" not in on
    assert "schedule" not in on
    assert "pull_request_target" not in on


def test_permissions_are_read_only():
    data = _yaml(WORKFLOW)
    assert data.get("permissions") == {"contents": "read"}
    for job in jobs(data).values():
        assert job.get("permissions", data["permissions"]) == {"contents": "read"}


def test_concurrency_cancels_per_branch():
    conc = _yaml(WORKFLOW).get("concurrency")
    assert isinstance(conc, dict)
    assert "github.head_ref || github.ref" in conc["group"]
    assert conc["group"].startswith("trino-bench-")
    assert conc.get("cancel-in-progress") is True


def test_single_job_with_bounded_timeout_and_no_required_check_name():
    all_jobs = jobs(_yaml(WORKFLOW))
    assert len(all_jobs) == 1
    for key, job in all_jobs.items():
        assert key not in REQUIRED_CHECK_NAMES
        assert job.get("name", key) not in REQUIRED_CHECK_NAMES
        assert "if" not in job
        assert job["runs-on"] == "ubuntu-latest"
        assert isinstance(job.get("timeout-minutes"), int)
        assert 0 < job["timeout-minutes"] <= 120


def test_sets_up_python_311_and_installs_dev_extra():
    all_steps = steps(_yaml(WORKFLOW))
    py = all_steps[_index(all_steps, _uses("actions/setup-python@"))]
    assert str(py["with"]["python-version"]) == "3.11"
    runs = "\n".join(s.get("run", "") for s in all_steps)
    assert 'pip install -e ".[dev]"' in runs


def test_reference_day_cache_is_shared_with_the_benchmark_workflow():
    all_steps = steps(_yaml(WORKFLOW))
    cache = all_steps[_index(all_steps, _uses("actions/cache@"))]
    bench_steps = steps(_yaml(BENCHMARK))
    bench_cache = bench_steps[_index(bench_steps, _uses("actions/cache@"))]
    assert cache["with"]["path"] == bench_cache["with"]["path"] == "data/raw/gharchive"
    assert cache["with"]["key"] == bench_cache["with"]["key"]


def test_steps_run_in_order():
    all_steps = steps(_yaml(WORKFLOW))
    order = [
        _index(all_steps, _uses("actions/cache@")),
        _index(all_steps, _runs("benchmarks/fetch_reference.py")),
        _index(all_steps, _runs("benchmarks/naive_duckdb.py")),
        _index(all_steps, _runs(f"{COMPOSE_CMD} up")),
        _index(all_steps, _runs("benchmarks/trino_bench.py")),
        _index(all_steps, _runs("GITHUB_STEP_SUMMARY")),
        _index(all_steps, _uses("actions/upload-artifact@v7")),
        _index(all_steps, _runs(f"{COMPOSE_CMD} down")),
    ]
    assert order == sorted(order)


def test_baseline_is_measured_again_before_the_stack_starts():
    run = _one_line(_step("benchmarks/naive_duckdb.py")["run"])
    assert "--runs 10" in run
    assert "--variant baseline" in run
    assert '--git-sha "$GIT_SHA"' in run
    assert "--out out/baseline-row.csv" in run


def test_stack_starts_detached_and_waits_for_healthchecks():
    run = _one_line(_step(f"{COMPOSE_CMD} up")["run"])
    assert f"{COMPOSE_CMD} up -d --wait" in run
    assert "--wait-timeout" in run


def test_bench_writes_rows_summary_and_samples():
    run = _one_line(_step("benchmarks/trino_bench.py")["run"])
    assert "--runs 10" in run
    assert '--git-sha "$GIT_SHA"' in run
    assert "--out out/trino-rows.csv" in run
    assert "--summary out/trino-summary.md" in run
    assert "--samples-dir out/trino-samples" in run
    job = next(iter(jobs(_yaml(WORKFLOW)).values()))
    assert "github.event.pull_request.head.sha" in job["env"]["GIT_SHA"]


def test_stack_logs_are_printed_when_something_failed():
    logs = _step(f"{COMPOSE_CMD} logs")
    assert logs.get("if") == "failure()"


def test_job_summary_runs_always():
    summary = [s for s in steps(_yaml(WORKFLOW)) if "GITHUB_STEP_SUMMARY" in s.get("run", "")]
    assert summary and all(s.get("if") == "always()" for s in summary)


def test_uploads_rows_summary_and_samples_even_on_failure():
    all_steps = steps(_yaml(WORKFLOW))
    upload = all_steps[_index(all_steps, _uses("actions/upload-artifact@v7"))]
    assert upload.get("if") == "always()"
    with_ = upload["with"]
    assert with_["name"] == "trino-bench"
    assert set(with_["path"].split()) == {
        "out/baseline-row.csv",
        "out/trino-rows.csv",
        "out/trino-summary.md",
        "out/trino-samples/",
    }
    assert with_.get("if-no-files-found") == "warn"


def test_stack_is_removed_with_its_volumes_always():
    down = _step(f"{COMPOSE_CMD} down")
    assert down.get("if") == "always()"
    assert f"{COMPOSE_CMD} down -v" in _one_line(down["run"])


def test_uses_no_secrets_and_no_expression_in_run():
    assert "secrets." not in WORKFLOW.read_text(encoding="utf-8")
    for step in steps(_yaml(WORKFLOW)):
        assert "${{" not in step.get("run", ""), step


def test_does_not_touch_runs_csv():
    runs = "\n".join(s.get("run", "") for s in steps(_yaml(WORKFLOW)))
    assert "record_run" not in runs
    assert "runs.csv" not in runs


# ---------------------------------------------------------------- docker compose stack


def _services():
    return _yaml(COMPOSE)["services"]


def test_stack_has_minio_metastore_and_trino_plus_a_one_shot_upload():
    services = _services()
    always_on = {name for name, svc in services.items() if not svc.get("profiles")}
    assert always_on == {"minio", "metastore", "trino"}
    assert services["upload"]["profiles"] == ["upload"]
    assert services["upload"].get("restart", "no") == "no"


@pytest.mark.parametrize("name", ["minio", "metastore", "trino", "upload"])
def test_every_image_is_pinned_to_an_explicit_tag(name):
    image = _services()[name]["image"]
    match = re.fullmatch(r"(?P<repo>[a-z0-9./_-]+):(?P<tag>[A-Za-z0-9._-]+)", image)
    assert match, image
    tag = match["tag"].lower()
    assert tag != "latest" and "latest" not in tag and "nightly" not in tag, image


def test_metastore_runs_standalone_with_s3a_on_its_classpath():
    metastore = _services()["metastore"]
    assert metastore["image"].startswith("apache/hive:")
    env = metastore["environment"]
    assert env["SERVICE_NAME"] == "metastore"
    classpath = env["HADOOP_CLASSPATH"]
    assert "/opt/hadoop/share/hadoop/tools/lib/hadoop-aws-" in classpath
    assert "/opt/hadoop/share/hadoop/tools/lib/aws-java-sdk-bundle-" in classpath
    assert "healthcheck" in metastore
    assert "9083" in " ".join(metastore["healthcheck"]["test"])


def test_trino_waits_for_the_metastore_and_has_a_healthcheck():
    trino = _services()["trino"]
    assert trino["image"].startswith("trinodb/trino:")
    assert trino["depends_on"]["metastore"]["condition"] == "service_healthy"
    assert "healthcheck" in trino
    assert any(p.startswith("8080:8080") or p == "127.0.0.1:8080:8080" for p in trino["ports"])


def test_catalog_and_metastore_config_are_mounted():
    trino_mounts = " ".join(_services()["trino"]["volumes"])
    assert "./catalog/hive.properties:/etc/trino/catalog/hive.properties" in trino_mounts
    metastore_mounts = " ".join(_services()["metastore"]["volumes"])
    assert "./metastore/core-site.xml:" in metastore_mounts


def _properties(path: Path) -> dict[str, str]:
    assert path.is_file(), f"missing file {path}"
    props = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.strip() and not line.lstrip().startswith("#"):
            key, _, value = line.partition("=")
            props[key.strip()] = value.strip()
    return props


def test_hive_catalog_uses_the_metastore_and_native_s3_on_minio():
    props = _properties(STACK_DIR / "catalog" / "hive.properties")
    assert props["connector.name"] == "hive"
    assert props["hive.metastore.uri"] == "thrift://metastore:9083"
    assert props["fs.s3.enabled"] == "true"
    assert props["s3.endpoint"] == "http://minio:9000"
    assert props["s3.path-style-access"] == "true"
    assert props["s3.region"]
    # throwaway keys of the local stack come from the compose environment, not from the file
    assert props["s3.aws-access-key"].startswith("${ENV:")
    assert props["s3.aws-secret-key"].startswith("${ENV:")


def test_metastore_s3a_points_at_minio_with_path_style_access():
    text = (STACK_DIR / "metastore" / "core-site.xml").read_text(encoding="utf-8")
    pairs = dict(re.findall(r"<name>\s*([^<\s]+)\s*</name>\s*<value>\s*([^<]*?)\s*</value>", text))
    assert pairs["fs.s3a.endpoint"] == "http://minio:9000"
    assert pairs["fs.s3a.path.style.access"] == "true"
    assert pairs["fs.s3a.impl"] == "org.apache.hadoop.fs.s3a.S3AFileSystem"
    assert pairs["fs.s3a.access.key"].startswith("${env.")
    assert pairs["fs.s3a.secret.key"].startswith("${env.")


def test_compose_uses_no_github_secret():
    text = COMPOSE.read_text(encoding="utf-8")
    assert "secrets." not in text
    assert "${{" not in text


# ---------------------------------------------------------------- Trino translations of Q1-Q6


BANNED = {
    "approx_percentile": r"\bapprox_percentile\s*\(",  # approximate, not quantile_cont
    "ends_with": r"\bends_with\s*\(",
    "arg_min": r"\barg_min\s*\(",
    "quantile_cont": r"\bquantile_cont\s*\(",
    "epoch": r"\bepoch\s*\(",
    "->>": r"->>",
    "::": r"::",
}


def _code(sql: str) -> str:
    return "\n".join(line.split("--", 1)[0] for line in sql.splitlines())


@pytest.mark.parametrize("q", QUERIES)
def test_trino_query_exists_with_a_translation_note(q):
    path = QUERY_DIR / f"{q}.sql"
    assert path.is_file(), f"missing {path}"
    first = path.read_text(encoding="utf-8").splitlines()[0]
    assert first.startswith(f"-- {q.upper()} in Trino SQL"), first
    assert f"benchmarks/queries/{q}.sql" in first


@pytest.mark.parametrize("q", QUERIES)
def test_trino_query_uses_no_duckdb_only_or_approximate_function(q):
    code = _code((QUERY_DIR / f"{q}.sql").read_text(encoding="utf-8"))
    for name, pattern in BANNED.items():
        assert not re.search(pattern, code, flags=re.IGNORECASE), (q, name)


@pytest.mark.parametrize("q", QUERIES)
def test_trino_query_reads_raw_events_and_repos_unqualified(q):
    code = _code((QUERY_DIR / f"{q}.sql").read_text(encoding="utf-8"))
    assert re.search(r"\bFROM raw_events\b", code)
    assert re.search(r"\brepos AS r\b", code)
    assert "hive." not in code  # the session schema picks the layout


@pytest.mark.parametrize("q", ["q3", "q4"])
def test_percentiles_are_exact_linear_interpolation(q):
    code = _code((QUERY_DIR / f"{q}.sql").read_text(encoding="utf-8"))
    assert "array_sort(" in code
    assert "element_at(" in code
    assert "floor(" in code and "ceiling(" in code
