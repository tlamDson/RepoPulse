"""Static checks on .github/workflows/streaming.yml and benchmarks/kafka/docker-compose.yml.

No GitHub, no Docker, no network: the files are parsed as YAML.
"""

from __future__ import annotations

import re
import tomllib
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = ROOT / ".github" / "workflows" / "streaming.yml"
COMPOSE = ROOT / "benchmarks" / "kafka" / "docker-compose.yml"
COMPOSE_PATH = "benchmarks/kafka/docker-compose.yml"
REQUIRED_CHECK_NAMES = {"Lint", "Unit Tests", "dbt build (DuckDB)"}


def load():
    assert WORKFLOW.is_file(), f"missing workflow file {WORKFLOW}"
    data = yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))
    assert isinstance(data, dict), "workflow must be a YAML mapping"
    return data


def load_compose():
    assert COMPOSE.is_file(), f"missing compose file {COMPOSE}"
    data = yaml.safe_load(COMPOSE.read_text(encoding="utf-8"))
    assert isinstance(data, dict), "compose file must be a YAML mapping"
    return data


def triggers(data):
    return data.get("on", data.get(True))  # PyYAML reads the bare key `on` as True


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


def env_of(service) -> dict[str, str]:
    env = service.get("environment", {})
    if isinstance(env, list):
        env = dict(item.split("=", 1) for item in env)
    return {k: str(v) for k, v in env.items()}


# ---------------------------------------------------------------- workflow


def test_workflow_name():
    assert load()["name"] == "Streaming"


def test_triggers_dispatch_and_path_filtered_pull_request():
    on = triggers(load())
    assert "workflow_dispatch" in on
    pr = on["pull_request"]
    assert set(pr["branches"]) == {"develop", "main"}
    assert set(pr["paths"]) == {
        ".github/workflows/streaming.yml",
        "benchmarks/streaming*",
        "benchmarks/kafka/**",
        "repopulse/streaming/**",
    }
    for key in ("push", "schedule", "pull_request_target"):
        assert key not in on


def test_permissions_are_read_only():
    data = load()
    assert data.get("permissions") == {"contents": "read"}
    for job in jobs(data).values():
        assert job.get("permissions", data["permissions"]) == {"contents": "read"}


def test_concurrency_cancels_per_branch():
    conc = load()["concurrency"]
    assert conc["group"].startswith("streaming-")
    assert "github.head_ref || github.ref" in conc["group"]
    assert conc.get("cancel-in-progress") is True


def test_single_job_with_bounded_timeout_and_no_required_check_name():
    all_jobs = jobs(load())
    assert len(all_jobs) == 1
    for key, job in all_jobs.items():
        assert key not in REQUIRED_CHECK_NAMES
        assert job.get("name", key) not in REQUIRED_CHECK_NAMES
        assert "if" not in job
        assert isinstance(job.get("timeout-minutes"), int)
        assert 0 < job["timeout-minutes"] <= 180
        assert job["runs-on"] == "ubuntu-latest"


def test_sets_up_python_311_and_java_17_and_installs_streaming_extra():
    all_steps = steps(load())
    py = all_steps[_index(all_steps, _uses("actions/setup-python@"))]
    assert str(py["with"]["python-version"]) == "3.11"
    java = all_steps[_index(all_steps, _uses("actions/setup-java@"))]
    assert java["with"]["distribution"] == "temurin"
    assert str(java["with"]["java-version"]) == "17"
    runs = "\n".join(s.get("run", "") for s in all_steps)
    assert 'pip install -e ".[dev,spark,streaming]"' in runs


def test_reference_day_is_cached_and_verified_before_the_benchmark():
    all_steps = steps(load())
    cache = _index(all_steps, _uses("actions/cache@"))
    fetch = _index(all_steps, _runs("benchmarks/fetch_reference.py"))
    bench = _index(all_steps, _runs("benchmarks/streaming_bench.py"))
    assert cache < fetch < bench
    with_ = all_steps[cache]["with"]
    assert with_["path"] == "data/raw/gharchive"
    assert "hashFiles('benchmarks/reference.toml')" in with_["key"]


def test_kafka_is_started_with_wait_before_the_benchmark_and_always_torn_down():
    all_steps = steps(load())
    up = _index(all_steps, _runs("up -d --wait"))
    bench = _index(all_steps, _runs("benchmarks/streaming_bench.py"))
    down = _index(all_steps, _runs("down -v"))
    assert up < bench < down
    assert f"docker compose -f {COMPOSE_PATH} up -d --wait" in all_steps[up]["run"]
    assert f"docker compose -f {COMPOSE_PATH} down -v" in all_steps[down]["run"]
    assert all_steps[down].get("if") == "always()"
    assert down == len(all_steps) - 1


def test_broker_logs_are_printed_on_failure():
    all_steps = steps(load())
    logs = all_steps[_index(all_steps, _runs(" logs"))]
    assert logs.get("if") == "failure()"
    assert f"docker compose -f {COMPOSE_PATH} logs" in logs["run"]


def test_benchmark_writes_rows_and_summary():
    all_steps = steps(load())
    bench = all_steps[_index(all_steps, _runs("benchmarks/streaming_bench.py"))]["run"]
    for flag in ("--bootstrap", "--partitions", "--git-sha", "--max-hours"):
        assert flag in bench
    assert "--out out/streaming-rows.csv" in bench
    assert "--summary out/streaming-summary.md" in bench
    summary = [s for s in all_steps if "GITHUB_STEP_SUMMARY" in s.get("run", "")]
    assert summary and all(s.get("if") == "always()" for s in summary)


def test_uploads_rows_and_summary_but_no_raw_data():
    all_steps = steps(load())
    upload = all_steps[_index(all_steps, _uses("actions/upload-artifact@v4"))]
    assert upload.get("if") == "always()"
    with_ = upload["with"]
    assert with_["name"] == "streaming-throughput"
    paths = [p.strip() for p in with_["path"].splitlines() if p.strip()]
    assert sorted(paths) == ["out/streaming-rows.csv", "out/streaming-summary.md"]


def test_uses_no_secrets_and_no_expression_in_run():
    load()
    assert "secrets." not in WORKFLOW.read_text(encoding="utf-8")
    for step in steps(load()):
        assert "${{" not in step.get("run", ""), step


def test_does_not_touch_runs_csv():
    runs = "\n".join(s.get("run", "") for s in steps(load()))
    assert "record_run" not in runs
    assert "runs.csv" not in runs


def test_streaming_extra_declares_confluent_kafka():
    data = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    extra = data["project"]["optional-dependencies"]["streaming"]
    assert any(dep.startswith("confluent-kafka") for dep in extra)


# ---------------------------------------------------------------- docker compose


def brokers():
    return load_compose()["services"]


def test_three_kafka_brokers_and_no_zookeeper():
    services = brokers()
    assert len(services) == 3
    text = COMPOSE.read_text(encoding="utf-8").lower()
    assert "zookeeper" not in text


def test_official_image_with_a_pinned_version():
    for name, svc in brokers().items():
        image = svc["image"]
        assert re.fullmatch(r"apache/kafka:\d+\.\d+\.\d+", image), (name, image)


def test_kraft_combined_mode_with_unique_node_ids_and_one_cluster_id():
    services = brokers()
    node_ids, cluster_ids = set(), set()
    for svc in services.values():
        env = env_of(svc)
        roles = {r.strip() for r in env["KAFKA_PROCESS_ROLES"].split(",")}
        assert roles == {"broker", "controller"}
        node_ids.add(env["KAFKA_NODE_ID"])
        cluster_ids.add(env["CLUSTER_ID"])
    assert node_ids == {"1", "2", "3"}
    assert len(cluster_ids) == 1


def test_quorum_voters_list_all_three_nodes_by_service_name():
    services = brokers()
    voters = {env_of(svc)["KAFKA_CONTROLLER_QUORUM_VOTERS"] for svc in services.values()}
    assert len(voters) == 1
    entries = voters.pop().split(",")
    assert len(entries) == 3
    by_id = {}
    for entry in entries:
        node, host_port = entry.split("@")
        by_id[node] = host_port.split(":")[0]
    for name, svc in services.items():
        assert by_id[env_of(svc)["KAFKA_NODE_ID"]] == svc.get("hostname", name)


def test_internal_and_external_listeners():
    for name, svc in brokers().items():
        env = env_of(svc)
        host = svc.get("hostname", name)
        advertised = dict(
            item.split("://", 1) for item in env["KAFKA_ADVERTISED_LISTENERS"].split(",")
        )
        internal_name = env["KAFKA_INTER_BROKER_LISTENER_NAME"]
        assert advertised[internal_name].startswith(f"{host}:")
        external = [v for k, v in advertised.items() if k != internal_name]
        assert len(external) == 1 and external[0].startswith("localhost:")
        port = external[0].split(":")[1]
        assert any(str(p).startswith(f"{port}:") for p in svc["ports"])
        assert env["KAFKA_CONTROLLER_LISTENER_NAMES"] in env["KAFKA_LISTENER_SECURITY_PROTOCOL_MAP"]


def test_each_broker_has_its_own_external_port():
    ports = [str(p).split(":")[0] for svc in brokers().values() for p in svc["ports"]]
    assert len(ports) == 3 and len(set(ports)) == 3


def test_replication_defaults_are_durable():
    for svc in brokers().values():
        env = env_of(svc)
        assert env["KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR"] == "3"
        assert env["KAFKA_MIN_INSYNC_REPLICAS"] == "2"
        assert env["KAFKA_AUTO_CREATE_TOPICS_ENABLE"] == "false"


def test_every_broker_has_a_healthcheck_for_compose_wait():
    for name, svc in brokers().items():
        hc = svc.get("healthcheck")
        assert hc, name
        test = " ".join(hc["test"]) if isinstance(hc["test"], list) else hc["test"]
        assert "kafka" in test
        assert hc.get("retries", 0) >= 3


def test_brokers_are_separate_services_so_one_can_be_killed():
    for name, svc in brokers().items():
        assert "container_name" not in svc, name  # `docker compose kill <service>` (P2b)
        assert svc.get("hostname", name) == name
