"""Static checks on .github/workflows/pages.yml (no GitHub, no network).

After a successful scheduled/dispatched run of ``Pipeline (scheduled)`` on ``main`` the site is
exported from MotherDuck (public schema) and deployed to GitHub Pages. A pull request only builds
a preview from the dbt fixture (target ci) and uploads it as a plain artifact: no deploy, no
secret.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = ROOT / ".github" / "workflows" / "pages.yml"
PIPELINE = ROOT / ".github" / "workflows" / "pipeline.yml"
REQUIRED_CHECK_NAMES = {"Lint", "Unit Tests", "dbt build (DuckDB)"}


def load() -> dict:
    assert WORKFLOW.is_file(), f"missing workflow file {WORKFLOW}"
    data = yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))
    assert isinstance(data, dict), "workflow must be a YAML mapping"
    return data


def triggers(data):
    return data.get("on", data.get(True))  # PyYAML reads the bare key `on` as True


def jobs(data) -> dict:
    return data["jobs"]


def uses(job: dict, prefix: str) -> list[dict]:
    return [s for s in job["steps"] if str(s.get("uses", "")).startswith(prefix)]


def runs(job: dict) -> str:
    return "\n".join(s.get("run", "") for s in job["steps"])


def job_using(data, prefix: str) -> tuple[str, dict]:
    found = [(k, j) for k, j in jobs(data).items() if uses(j, prefix)]
    assert len(found) == 1, f"expected one job using {prefix}, got {[k for k, _ in found]}"
    return found[0]


def deploy_job(data):
    return job_using(data, "actions/deploy-pages@")


def pages_build_job(data):
    return job_using(data, "actions/upload-pages-artifact@")


def preview_job(data):
    return job_using(data, "actions/upload-artifact@")


def walk(node, path=()):
    if isinstance(node, dict):
        for key, value in node.items():
            yield from walk(value, (*path, str(key)))
    elif isinstance(node, list):
        for i, value in enumerate(node):
            yield from walk(value, (*path, str(i)))
    elif isinstance(node, str):
        yield path, node


def comments() -> str:
    text = WORKFLOW.read_text(encoding="utf-8") if WORKFLOW.is_file() else ""
    return "\n".join(line for line in text.splitlines() if line.lstrip().startswith("#"))


def squash(expr: str) -> str:
    return re.sub(r"\s+", " ", expr)


# ---------------------------------------------------------------- triggers


def test_runs_after_the_scheduled_pipeline_and_on_dispatch():
    on = triggers(load())
    wr = on["workflow_run"]
    assert wr["workflows"] == ["Pipeline (scheduled)"]
    assert wr["types"] == ["completed"]
    assert wr.get("branches") == ["main"]
    assert "workflow_dispatch" in on
    assert "push" not in on
    assert "pull_request_target" not in on
    assert "schedule" not in on


def test_pipeline_name_matches_the_triggering_workflow():
    pipeline = yaml.safe_load(PIPELINE.read_text(encoding="utf-8"))
    assert triggers(load())["workflow_run"]["workflows"] == [pipeline["name"]]


def test_pull_request_is_path_filtered_to_site_code():
    pr = triggers(load())["pull_request"]
    assert set(pr["branches"]) == {"develop", "main"}
    paths = set(pr["paths"])
    assert {".github/workflows/pages.yml", "repopulse/export_site.py", "site/**"} <= paths


# ---------------------------------------------------------------- gating


def test_pages_build_runs_only_after_a_successful_non_pr_pipeline_run_on_main():
    _, job = pages_build_job(load())
    cond = squash(job["if"])
    assert "github.event.workflow_run.conclusion == 'success'" in cond
    assert "github.event.workflow_run.event != 'pull_request'" in cond
    assert "github.event.workflow_run.head_branch == 'main'" in cond
    assert "github.event_name == 'workflow_dispatch'" in cond
    assert "github.ref == 'refs/heads/main'" in cond  # dispatch from another branch: no deploy
    assert "github.event_name == 'pull_request'" not in cond


def test_deploy_needs_the_pages_build():
    data = load()
    build_key, _ = pages_build_job(data)
    _, deploy = deploy_job(data)
    needs = deploy["needs"]
    assert needs == build_key or needs == [build_key]


def test_preview_runs_only_on_pull_request():
    _, job = preview_job(load())
    assert squash(job["if"]) == "github.event_name == 'pull_request'"


def test_pull_request_never_deploys():
    data = load()
    _, preview = preview_job(data)
    for prefix in ("actions/deploy-pages@", "actions/upload-pages-artifact@"):
        assert not uses(preview, prefix)
    _, build = pages_build_job(data)
    assert "github.event_name == 'pull_request'" not in squash(build["if"])


def test_checkout_uses_the_default_branch_code_not_the_triggering_run():
    for name, job in jobs(load()).items():
        for step in uses(job, "actions/checkout@"):
            assert "ref" not in (step.get("with") or {}), name
    assert "head_sha" not in WORKFLOW.read_text(encoding="utf-8")


# ---------------------------------------------------------------- permissions


def test_default_permissions_are_read_only():
    assert load().get("permissions") == {"contents": "read"}


def test_only_the_deploy_job_can_write_pages_and_mint_tokens():
    data = load()
    deploy_key, deploy = deploy_job(data)
    assert deploy["permissions"] == {"pages": "write", "id-token": "write"}
    env = deploy["environment"]
    assert env["name"] == "github-pages"
    assert "page_url" in env.get("url", "")
    for name, job in jobs(data).items():
        if name == deploy_key:
            continue
        perms = job.get("permissions", data["permissions"])
        assert perms == {"contents": "read"}, name


def test_deploy_concurrency_is_its_own_group_and_never_cancels():
    _, deploy = deploy_job(load())
    conc = deploy["concurrency"]
    assert conc["cancel-in-progress"] is False
    assert "pages" in conc["group"]
    assert "pull_request" not in conc["group"]


def test_configure_pages_runs_before_deploy():
    _, deploy = deploy_job(load())
    steps = [str(s.get("uses", "")) for s in deploy["steps"]]
    configure = [i for i, u in enumerate(steps) if u.startswith("actions/configure-pages@")]
    deploy_at = [i for i, u in enumerate(steps) if u.startswith("actions/deploy-pages@")]
    assert configure and deploy_at and configure[0] < deploy_at[0]


# ---------------------------------------------------------------- job names


def test_job_names_do_not_shadow_required_checks():
    for key, job in jobs(load()).items():
        assert key not in REQUIRED_CHECK_NAMES
        assert job.get("name", key) not in REQUIRED_CHECK_NAMES


def test_jobs_have_timeouts():
    for name, job in jobs(load()).items():
        timeout = job.get("timeout-minutes")
        assert isinstance(timeout, int) and 0 < timeout <= 30, name


# ---------------------------------------------------------------- export steps


def test_pages_build_exports_from_motherduck_public_schema():
    _, job = pages_build_job(load())
    text = runs(job)
    assert "python -m repopulse.export_site" in text
    assert "--db md:repopulse" in text
    assert "--schema main_public" in text
    assert "--runs runs.csv" in text
    assert '--git-sha "$GITHUB_SHA"' in text
    assert "--out _site" in text
    (artifact,) = uses(job, "actions/upload-pages-artifact@")
    assert artifact["with"]["path"] == "_site"


def test_preview_builds_dbt_fixture_and_exports_ci_duckdb():
    _, job = preview_job(load())
    text = runs(job)
    assert "dbt build --project-dir transform --profiles-dir transform --target ci" in text
    assert "python -m repopulse.export_site" in text
    assert "--db ci.duckdb" in text
    assert "md:" not in text
    (artifact,) = uses(job, "actions/upload-artifact@")
    assert artifact["with"]["path"] == "_site"
    assert 0 < int(artifact["with"]["retention-days"]) <= 14


@pytest.mark.parametrize("which", ["build", "preview"])
def test_install_pins_the_same_duckdb_as_the_pipeline(which):
    pin = re.search(r'"duckdb==([0-9.]+)"', PIPELINE.read_text(encoding="utf-8"))
    assert pin
    data = load()
    _, job = pages_build_job(data) if which == "build" else preview_job(data)
    install = [line for line in runs(job).splitlines() if "pip install" in line]
    assert install, f"no pip install in the {which} job"
    assert any(f'"duckdb=={pin.group(1)}"' in line for line in install)


def test_uses_the_same_action_majors_as_the_other_workflows():
    data = load()
    for job in jobs(data).values():
        for step in job["steps"]:
            u = str(step.get("uses", ""))
            if u.startswith("actions/checkout@"):
                assert u == "actions/checkout@v4"
            if u.startswith("actions/setup-python@"):
                assert u == "actions/setup-python@v5"
                assert str(step["with"]["python-version"]) == "3.11"
            if u.startswith("actions/upload-artifact@"):
                assert u == "actions/upload-artifact@v7"


# ---------------------------------------------------------------- secrets


def test_motherduck_token_only_reaches_build_steps_through_env():
    data = load()
    hits = [(p, v) for p, v in walk(data) if "secrets." in v]
    assert hits, "the pages build reads MOTHERDUCK_TOKEN"
    for path, _ in hits:
        assert "env" in path, path
        assert "steps" in path, path  # step env only, never job- or workflow-wide
    names = set(re.findall(r"secrets\.([A-Z_]+)", WORKFLOW.read_text(encoding="utf-8")))
    assert names == {"MOTHERDUCK_TOKEN"}
    build_key, _ = pages_build_job(data)
    for path, _ in hits:
        assert path[1] == build_key, path


def test_preview_job_gets_no_secret():
    _, job = preview_job(load())
    assert not [p for p, v in walk(job) if "secrets." in v]


def test_missing_token_stops_the_build_before_export():
    _, job = pages_build_job(load())
    steps = job["steps"]
    check = [s for s in steps if "MOTHERDUCK_TOKEN" in s.get("run", "") and "exit 1" in s["run"]]
    assert check, "no step stops when MOTHERDUCK_TOKEN is missing"
    export = [s for s in steps if "repopulse.export_site" in s.get("run", "")]
    assert steps.index(check[0]) < steps.index(export[0])


def test_no_step_prints_the_token_or_interpolates_expressions():
    for name, job in jobs(load()).items():
        for step in job["steps"]:
            run = step.get("run", "")
            assert not re.search(r"echo[^\n]*\$\{?MOTHERDUCK_TOKEN", run), (name, step.get("name"))
            assert "set -x" not in run
            assert "${{" not in run, (name, step.get("name"))


# ---------------------------------------------------------------- comments


@pytest.mark.parametrize(
    "word", ["workflow_run", "main", "motherduck_token", "pull_request", "main_public"]
)
def test_header_comment_explains_triggers_secret_and_main_only(word):
    assert word in comments().lower()


def test_comment_does_not_call_it_streaming():
    text = comments().lower()
    assert "real-time" not in text.replace("not real-time", "")
    assert "streaming" not in text.replace("not streaming", "")
