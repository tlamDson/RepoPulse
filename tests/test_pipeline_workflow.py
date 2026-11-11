"""Static checks on .github/workflows/pipeline.yml (no GitHub, no network).

The scheduled pipeline chains ingest -> Spark bronze/silver -> silver export -> dbt build ->
runs.csv row. It writes MotherDuck on schedule / dispatch only; pull requests run the chain on one
real hour against local DuckDB (target ci) and never touch the ``repopulse`` database.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = ROOT / ".github" / "workflows" / "pipeline.yml"
PROBE = ROOT / ".github" / "workflows" / "probe.yml"
REQUIRED_CHECK_NAMES = {"Lint", "Unit Tests", "dbt build (DuckDB)"}


def load() -> dict:
    assert WORKFLOW.is_file(), f"missing workflow file {WORKFLOW}"
    data = yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))
    assert isinstance(data, dict), "workflow must be a YAML mapping"
    return data


def triggers(data):
    return data.get("on", data.get(True))  # PyYAML reads the bare key `on` as True


def only_job(data) -> dict:
    jobs = data["jobs"]
    assert len(jobs) == 1, sorted(jobs)
    return next(iter(jobs.values()))


def steps(data) -> list[dict]:
    return only_job(data)["steps"]


def runs(data) -> str:
    return "\n".join(s.get("run", "") for s in steps(data))


def step_index(data, needle: str) -> int:
    found = [i for i, s in enumerate(steps(data)) if needle in s.get("run", "")]
    assert found, f"no step runs {needle!r}"
    return found[0]


def comments() -> str:
    text = WORKFLOW.read_text(encoding="utf-8") if WORKFLOW.is_file() else ""
    return "\n".join(line for line in text.splitlines() if line.lstrip().startswith("#"))


def walk(node, path=()):
    """Yield (path, string) for every string in the YAML tree."""
    if isinstance(node, dict):
        for key, value in node.items():
            yield from walk(value, (*path, str(key)))
    elif isinstance(node, list):
        for i, value in enumerate(node):
            yield from walk(value, (*path, str(i)))
    elif isinstance(node, str):
        yield path, node


# ---------------------------------------------------------------- triggers


def test_hourly_cron_off_the_hour():
    on = triggers(load())
    (entry,) = on["schedule"]
    minute, hour, *rest = entry["cron"].split()
    assert minute.isdigit() and 0 < int(minute) < 60
    assert hour == "*" and rest == ["*", "*", "*"]


def test_dispatch_inputs():
    inputs = triggers(load())["workflow_dispatch"]["inputs"]
    assert {"date", "hour", "hours", "force", "full_refresh"} <= set(inputs)
    assert inputs["full_refresh"]["type"] == "boolean"
    assert inputs["full_refresh"]["default"] is False
    assert inputs["hours"]["default"] == "3"


def test_pull_request_is_path_filtered_to_pipeline_code():
    on = triggers(load())
    pr = on["pull_request"]
    assert set(pr["branches"]) == {"develop", "main"}
    paths = set(pr["paths"])
    assert {
        ".github/workflows/pipeline.yml",
        "repopulse/pipeline.py",
        "repopulse/spark/export.py",
    } <= paths
    assert "pull_request_target" not in on
    assert "push" not in on


def test_ingest_workflow_no_longer_has_its_own_cron():
    ingest = yaml.safe_load((ROOT / ".github/workflows/ingest.yml").read_text(encoding="utf-8"))
    on = ingest.get("on", ingest.get(True))
    assert "schedule" not in on
    assert "workflow_dispatch" in on


# ---------------------------------------------------------------- job shape


def test_permissions_are_minimal():
    data = load()
    assert data.get("permissions") == {"contents": "read"}
    assert only_job(data).get("permissions", data["permissions"]) == {"contents": "read"}


def test_job_has_timeout():
    timeout = only_job(load()).get("timeout-minutes")
    assert isinstance(timeout, int) and 0 < timeout <= 90


def test_job_name_does_not_shadow_required_checks():
    for key, job in load()["jobs"].items():
        assert key not in REQUIRED_CHECK_NAMES
        assert job.get("name", key) not in REQUIRED_CHECK_NAMES


def test_no_job_level_if():
    for name, job in load()["jobs"].items():
        assert "if" not in job, name


def test_concurrency_schedule_and_dispatch_share_one_group_pr_separate():
    conc = load().get("concurrency")
    assert isinstance(conc, dict)
    assert conc.get("cancel-in-progress") is False
    group = conc["group"]
    assert "github.event_name == 'pull_request'" in group
    assert "pipeline-motherduck" in group  # schedule + workflow_dispatch: one writer at a time
    assert "pipeline-pr-" in group


def test_uses_python_311_with_pip_cache_and_java_17():
    s = steps(load())
    setup = [x for x in s if str(x.get("uses", "")).startswith("actions/setup-python@")]
    assert str(setup[0]["with"]["python-version"]) == "3.11"
    assert setup[0]["with"].get("cache") == "pip"
    java = [x for x in s if str(x.get("uses", "")).startswith("actions/setup-java@")]
    assert java and str(java[0]["with"]["java-version"]) == "17"
    assert str(s[0].get("uses", "")).startswith("actions/checkout@")


def test_install_pins_the_duckdb_version_motherduck_supports():
    probe_pin = re.search(r'"duckdb==([0-9.]+)"', PROBE.read_text(encoding="utf-8"))
    assert probe_pin
    text = runs(load())
    install = [line for line in text.splitlines() if "pip install" in line]
    assert install, "no pip install step"
    assert any('-e ".[dev,spark,dbt]"' in line for line in install)
    assert any(f'"duckdb=={probe_pin.group(1)}"' in line for line in install)


# ---------------------------------------------------------------- chain


def test_steps_run_in_pipeline_order():
    data = load()
    order = [
        step_index(data, "python -m repopulse.ingest"),
        step_index(data, "python -m repopulse.spark.job --source gharchive"),
        step_index(data, "python -m repopulse.spark.export"),
        step_index(data, "dbt build"),
        step_index(data, "python -m repopulse.pipeline row"),
    ]
    assert order == sorted(order), order
    assert len(set(order)) == 5


def test_spark_job_uses_the_window_of_the_ingest_summary():
    text = runs(load())
    assert "python -m repopulse.pipeline window --ingest-summary out/ingest-summary.json" in text
    assert '--date "$DATA_DATE" --hour "$DATA_HOUR" --hours "$DATA_HOURS"' in text


def test_dbt_reads_the_exported_silver_and_skips_the_ci_fixture_seed():
    data = load()
    dbt = steps(data)[step_index(data, "dbt build")]["run"]
    assert "silver_gharchive_parquet" in dbt
    assert "data/silver-export/*.parquet" in dbt
    assert "--exclude fixture_silver_gharchive_events" in dbt
    assert '--target "$DBT_TARGET"' in dbt
    assert "--full-refresh" in dbt  # only when the dispatch input asks for it


def test_target_is_ci_on_pull_request_and_motherduck_otherwise():
    env = only_job(load()).get("env", {})
    assert env["DBT_TARGET"] == (
        "${{ github.event_name == 'pull_request' && 'ci' || 'motherduck' }}"
    )


def test_pull_request_uses_a_fixed_fake_salt_and_real_secret_otherwise():
    data = load()
    spark = steps(data)[step_index(data, "python -m repopulse.spark.job")]
    salt = spark["env"]["PII_HASH_SALT"]
    assert "github.event_name == 'pull_request'" in salt
    assert "not-a-secret" in salt
    assert "secrets.PII_HASH_SALT" in salt


def dbt_build_steps(data) -> list[dict]:
    return [s for s in steps(data) if "dbt build" in s.get("run", "")]


def test_every_dbt_build_step_gets_the_pii_salt():
    # The public models mask identifiers with mask_pii (docs/governance.md): on any target but ci
    # a missing PII_HASH_SALT is a compile error, so the scheduled build would never reach gold.
    found = dbt_build_steps(load())
    assert len(found) == 3, [s.get("name") for s in found]
    for step in found:
        assert step.get("env", {}).get("PII_HASH_SALT"), step.get("name")


def test_dbt_masks_with_the_salt_silver_was_hashed_with():
    data = load()
    spark_salt = steps(data)[step_index(data, "python -m repopulse.spark.job")]["env"][
        "PII_HASH_SALT"
    ]
    fake = re.search(r"'(not-a-secret[^']*)'", spark_salt).group(1)
    for step in dbt_build_steps(data):
        salt = step.get("env", {}).get("PII_HASH_SALT")
        if "github.event_name == 'pull_request'" in step.get("if", ""):
            # pull-request-only steps never see the real secret
            assert salt == fake, step.get("name")
        else:
            assert salt == spark_salt, step.get("name")


def test_missing_secrets_stop_scheduled_runs_early():
    data = load()
    s = steps(data)
    check = [x for x in s if "MOTHERDUCK_TOKEN" in x.get("run", "") and "exit 1" in x["run"]]
    assert check, "no step stops when MOTHERDUCK_TOKEN is missing"
    assert "PII_HASH_SALT" in check[0]["run"]
    assert "pull_request" in check[0]["if"]
    assert s.index(check[0]) < step_index(data, "python -m repopulse.ingest")
    assert 'echo "$MOTHERDUCK_TOKEN' not in check[0]["run"]


def test_pull_request_builds_twice_and_compares_counts():
    text = runs(load())
    assert text.count("python -m repopulse.pipeline counts") >= 2
    assert re.search(r"diff .*counts", text)


def test_motherduck_smoke_runs_on_pr_only_with_token_and_its_own_database():
    data = load()
    smoke = [x for x in steps(data) if "md-reset" in x.get("run", "")]
    assert len(smoke) == 1
    step = smoke[0]
    assert "pull_request" in step["if"]
    assert "MOTHERDUCK" in step["if"]
    assert step["env"]["MOTHERDUCK_DATABASE"] == "repopulse_smoke"
    assert "md-reset --database repopulse_smoke" in step["run"]
    assert step["run"].count("--target motherduck") == 2
    assert "MOTHERDUCK_DATABASE=" not in step["run"]


def test_production_database_is_never_named_in_pr_steps():
    # Only the smoke step talks to MotherDuck on a pull request, and it pins repopulse_smoke; no
    # step sets MOTHERDUCK_DATABASE to anything else.
    for step in steps(load()):
        db = step.get("env", {}).get("MOTHERDUCK_DATABASE")
        assert db in (None, "repopulse_smoke"), step.get("name")


def test_writes_pipeline_row_and_job_summary():
    text = runs(load())
    assert "--out out/pipeline-row.csv" in text
    assert "GITHUB_STEP_SUMMARY" in text
    assert '--run-at "$RUN_AT"' in text
    assert '--git-sha "$GITHUB_SHA"' in text
    assert '--run-id "$GITHUB_RUN_ID"' in text


def test_steps_are_timed():
    text = runs(load())
    for name in ("ingest", "spark", "export", "dbt"):
        assert f"python -m repopulse.pipeline timed --name {name} " in text, name


# ---------------------------------------------------------------- secrets + expressions


def test_run_steps_do_not_interpolate_expressions():
    for step in steps(load()):
        run = step.get("run", "")
        assert "${{" not in run, step.get("name")
        assert "inputs." not in run, step.get("name")


def test_secrets_only_reach_steps_through_env():
    data = load()
    hits = [(p, v) for p, v in walk(data) if "secrets." in v]
    assert hits, "workflow is expected to read MOTHERDUCK_TOKEN / PII_HASH_SALT secrets"
    for path, _ in hits:
        assert "env" in path, path
    names = set(re.findall(r"secrets\.([A-Z_]+)", WORKFLOW.read_text(encoding="utf-8")))
    assert names == {"MOTHERDUCK_TOKEN", "PII_HASH_SALT"}


def test_motherduck_token_is_not_job_wide():
    env = only_job(load()).get("env", {})
    assert "MOTHERDUCK_TOKEN" not in env
    assert "PII_HASH_SALT" not in env


# ---------------------------------------------------------------- artifact


def test_uploads_summaries_and_row_but_not_the_lake():
    upload = [
        s for s in steps(load()) if str(s.get("uses", "")).startswith("actions/upload-artifact@")
    ]
    assert len(upload) == 1
    with_ = upload[0]["with"]
    assert upload[0]["if"] == "always()"
    paths = with_["path"]
    assert "out/" in paths or "out/pipeline-row.csv" in paths
    for forbidden in ("data/", "lake", "silver", "landing", ".duckdb"):
        assert forbidden not in paths, forbidden
    assert with_["name"].startswith("pipeline-run-")
    assert 0 < int(with_["retention-days"]) <= 30


@pytest.mark.parametrize("word", ["incremental batch", "main", "workflow_dispatch", "fallback"])
def test_comment_explains_batch_main_only_and_fallback(word):
    assert word in comments().lower()


def test_comment_does_not_call_it_streaming():
    text = comments().lower()
    assert "real-time" not in text.replace("not real-time", "")
    assert "streaming" not in text.replace("not streaming", "")
