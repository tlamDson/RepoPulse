"""Tests for the headless orchestrator core (pure functions, no subprocess)."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from repopulse.orchestrator import (
    Gates,
    Worktree,
    build_command,
    build_prompt,
    clean_plan,
    decide_after_exit,
    gate_for,
    harness_auto_approved,
    load_tasks,
    merged_task_ids,
    next_ready,
    parse_result,
    parse_stream_line,
    parse_worktree_porcelain,
    rate_limit_action,
    releases_due,
    render_progress,
    should_stop,
)

TOML = """
[[task]]
id = "S0.4"
branch = "feature/runs-log"
agent = "pipeline-ops"

[[task]]
id = "B"
branch = "feature/baseline-naive"
deps = ["S0.4"]
needs = ["harness"]
agent = "analytics-engineer"
release_after = true

[[task]]
id = "2"
branch = "feature/event-spec"
deps = ["B"]
agent = "event-schema-engineer"

[[task]]
id = "4"
branch = "feature/ingest-gharchive"
deps = ["B"]
agent = "pipeline-ops"

[[task]]
id = "probe"
branch = "docs/probe-result"
needs = ["cloud"]
agent = "pipeline-ops"

[[task]]
id = "7"
branch = "feature/incremental-and-schedule"
deps = ["4", "probe"]
needs = ["cloud"]
agent = "analytics-engineer"
release_after = true

[[task]]
id = "13"
branch = "feature/snowflake-port"
deps = ["2"]
needs = ["snowflake"]
optional = true
agent = "analytics-engineer"

[[task]]
id = "14"
branch = "docs/results"
deps = ["7", "13"]
agent = "release-manager"
"""

NO_SECRETS = Gates(secrets=frozenset(), harness_approved=True)
CLOUD = Gates(secrets=frozenset({"DATABRICKS_HOST", "DATABRICKS_TOKEN"}), harness_approved=True)


@pytest.fixture
def tasks(tmp_path: Path):
    p = tmp_path / "prs.toml"
    p.write_text(TOML, encoding="utf-8")
    return load_tasks(p)


def ids(ts):
    return [t.id for t in ts]


# ---------- loading ----------


def test_load_tasks_reads_fields_and_defaults(tasks):
    assert ids(tasks) == ["S0.4", "B", "2", "4", "probe", "7", "13", "14"]
    s04 = tasks[0]
    assert s04.deps == () and s04.needs == () and s04.release_after is False
    assert tasks[1].release_after is True and tasks[1].needs == ("harness",)
    assert tasks[6].optional is True


def test_load_tasks_rejects_unknown_dependency(tmp_path: Path):
    p = tmp_path / "prs.toml"
    p.write_text('[[task]]\nid="a"\nbranch="feature/a"\ndeps=["zzz"]\nagent="x"\n', "utf-8")
    with pytest.raises(ValueError, match="zzz"):
        load_tasks(p)


# ---------- gates ----------


def test_gate_cloud_blocks_without_secrets(tasks):
    g = gate_for(tasks[4], NO_SECRETS)
    assert g is not None and g.kind == "block" and "DATABRICKS" in g.reason


def test_gate_cloud_passes_with_databricks_or_motherduck(tasks):
    assert gate_for(tasks[4], CLOUD) is None
    md = Gates(secrets=frozenset({"MOTHERDUCK_TOKEN"}), harness_approved=True)
    assert gate_for(tasks[4], md) is None


def test_gate_cloud_needs_both_databricks_secrets(tasks):
    half = Gates(secrets=frozenset({"DATABRICKS_HOST"}), harness_approved=True)
    assert gate_for(tasks[4], half).kind == "block"


def test_gate_optional_snowflake_is_skipped_not_blocked(tasks):
    g = gate_for(tasks[6], NO_SECRETS)
    assert g.kind == "skip"
    sf = Gates(secrets=frozenset({"SNOWFLAKE_ACCOUNT"}), harness_approved=True)
    assert gate_for(tasks[6], sf) is None


def test_gate_harness_blocks_until_approved(tasks):
    g = gate_for(tasks[1], Gates(secrets=frozenset(), harness_approved=False))
    assert g.kind == "block" and "harness" in g.reason


# ---------- scheduling ----------


def test_next_ready_starts_with_s04_only(tasks):
    ready, blocked, skipped = next_ready(
        tasks, done=set(), running=set(), gates=NO_SECRETS, slots=2
    )
    assert ids(ready) == ["S0.4"]
    assert "probe" in blocked


def test_next_ready_stops_at_harness_gate(tasks):
    gates = Gates(secrets=frozenset(), harness_approved=False)
    ready, blocked, _ = next_ready(tasks, {"S0.4"}, set(), gates, slots=2)
    assert ready == [] and "B" in blocked


def test_next_ready_runs_two_independent_tasks_in_parallel(tasks):
    ready, _, _ = next_ready(tasks, {"S0.4", "B"}, set(), NO_SECRETS, slots=2)
    assert ids(ready) == ["2", "4"]


def test_next_ready_respects_slots_and_running(tasks):
    ready, _, _ = next_ready(tasks, {"S0.4", "B"}, {"2"}, NO_SECRETS, slots=1)
    assert ready == []
    ready, _, _ = next_ready(tasks, {"S0.4", "B"}, {"2"}, NO_SECRETS, slots=2)
    assert ids(ready) == ["4"]


def test_next_ready_without_cloud_blocks_cloud_tasks(tasks):
    done = {"S0.4", "B", "2", "4"}
    ready, blocked, skipped = next_ready(tasks, done, set(), NO_SECRETS, slots=2)
    assert ready == []
    assert "probe" in blocked and "7" not in ids(ready)
    assert ids(skipped) == ["13"]


def test_next_ready_with_cloud_runs_probe_first(tasks):
    done = {"S0.4", "B", "2", "4"}
    ready, _, _ = next_ready(tasks, done, set(), CLOUD, slots=2)
    assert ids(ready) == ["probe"]


def test_skipped_optional_task_satisfies_dependency(tasks):
    done = {"S0.4", "B", "2", "4", "probe", "7"}
    ready, _, skipped = next_ready(tasks, done, set(), CLOUD, slots=2)
    assert ids(skipped) == ["13"] and ids(ready) == ["14"]


def test_merged_task_ids_maps_branches(tasks):
    prs = [
        {"headRefName": "feature/runs-log", "state": "MERGED"},
        {"headRefName": "feature/baseline-naive", "state": "OPEN"},
        {"headRefName": "chore/other", "state": "MERGED"},
    ]
    assert merged_task_ids(tasks, prs) == {"S0.4"}


def test_releases_due_after_release_task_merged(tasks):
    assert releases_due(tasks, done={"S0.4", "B"}, released=set()) == ["B"]
    assert releases_due(tasks, done={"S0.4", "B"}, released={"B"}) == []


# ---------- stream-json and quota ----------


def test_parse_stream_line_handles_json_and_garbage():
    assert (
        parse_stream_line('{"type":"system","subtype":"init","session_id":"s1"}')["session_id"]
        == "s1"
    )
    assert parse_stream_line("not json") is None
    assert parse_stream_line("   ") is None


def _rl(status, **info):
    return {"type": "rate_limit_event", "rate_limit_info": {"status": status, **info}}


def test_rate_limit_rejected_waits_until_reset():
    assert rate_limit_action(_rl("rejected", resetsAt=1_800_000_000)) == ("wait", 1_800_000_000)


def test_rate_limit_credits_required_stops():
    assert rate_limit_action(_rl("rejected", errorCode="credits_required"))[0] == "stop"


def test_rate_limit_warning_over_90_percent_holds_new_work():
    assert rate_limit_action(_rl("allowed_warning", utilization=0.93))[0] == "warn"
    assert rate_limit_action(_rl("allowed_warning", utilization=0.5))[0] == "none"
    assert rate_limit_action({"type": "assistant"})[0] == "none"


def test_parse_result_takes_last_result_line():
    text = "blah\nRESULT: failed early\nmore\nRESULT: merged PR #12\n"
    assert parse_result(text) == ("merged", "PR #12")
    assert parse_result("no marker") is None


def test_decide_after_exit_resumes_after_rate_limit():
    d = decide_after_exit(1, _rl("rejected", resetsAt=1000), "", now=500, max_wait_s=3600)
    assert d.kind == "resume_at" and d.at == 1060


def test_decide_after_exit_stops_when_reset_too_far():
    d = decide_after_exit(1, _rl("rejected", resetsAt=100_000), "", now=0, max_wait_s=3600)
    assert d.kind == "stop"


def test_decide_after_exit_uses_result_line():
    d = decide_after_exit(0, None, "RESULT: blocked cần secret", now=0, max_wait_s=1)
    assert (d.kind, d.reason) == ("blocked", "cần secret")
    d = decide_after_exit(0, None, "nothing", now=0, max_wait_s=1)
    assert d.kind == "failed" and "RESULT" in d.reason


def test_decide_after_exit_merged_wins_over_stale_rate_event():
    d = decide_after_exit(
        0, _rl("rejected", resetsAt=10), "RESULT: merged ok", now=0, max_wait_s=99
    )
    assert d.kind == "merged"


# ---------- command and prompt ----------


def test_build_command_is_headless_auto_mode(tasks, tmp_path: Path):
    cmd = build_command(tmp_path, "PROMPT", resume=None)
    assert cmd[:3] == ["claude", "-p", "PROMPT"]
    joined = " ".join(cmd)
    assert "--permission-mode auto" in joined
    assert "--permission-prompts none" in joined
    assert "--output-format stream-json" in joined and "--verbose" in cmd
    assert str(tmp_path / ".claude" / "auto-mode.json") in cmd
    assert "--resume" not in cmd
    assert build_command(tmp_path, "P", resume="abc")[-2:] == ["--resume", "abc"]


def test_build_prompt_names_task_agent_and_result_contract(tasks):
    p = build_prompt(tasks[2])
    assert "PR 2" in p and "feature/event-spec" in p and "event-schema-engineer" in p
    assert "RESULT:" in p and "develop" in p and "command-style.md" in p


# ---------- worktree cleanup ----------

PORCELAIN = """worktree D:/Desktop/RepoPulse
HEAD aaa
branch refs/heads/develop

worktree D:/Desktop/RepoPulse/.claude/worktrees/pr-2
HEAD bbb
branch refs/heads/feature/event-spec

worktree D:/Desktop/RepoPulse/.claude/worktrees/pr-4
HEAD ccc
detached
"""


def test_parse_worktree_porcelain():
    wts = parse_worktree_porcelain(PORCELAIN)
    assert wts[1] == ("D:/Desktop/RepoPulse/.claude/worktrees/pr-2", "feature/event-spec")
    assert wts[2] == ("D:/Desktop/RepoPulse/.claude/worktrees/pr-4", None)


def test_clean_plan_removes_only_merged_clean_inactive():
    wts = [
        Worktree("w/pr-2", "feature/event-spec", dirty=False),
        Worktree("w/pr-4", "feature/ingest-gharchive", dirty=True),
        Worktree("w/pr-5", "feature/bronze-silver-spark", dirty=False),
        Worktree("w/pr-6", "feature/dbt-star-schema", dirty=False),
    ]
    states = {
        "feature/event-spec": "MERGED",
        "feature/ingest-gharchive": "MERGED",
        "feature/bronze-silver-spark": "OPEN",
        "feature/dbt-star-schema": "MERGED",
    }
    plan = {a.path: a.action for a in clean_plan(wts, states, active={"w/pr-6"})}
    assert plan == {"w/pr-2": "remove", "w/pr-4": "keep", "w/pr-5": "keep", "w/pr-6": "keep"}


# ---------- stop file and harness gate ----------


def test_should_stop(tmp_path: Path):
    assert should_stop(tmp_path) is False
    (tmp_path / "STOP").write_text("", "utf-8")
    assert should_stop(tmp_path) is True


def _log(task, cmd, event="PermissionDenied"):
    return json.dumps({"task": task, "event": event, "tool": "Bash", "input": cmd})


def test_harness_auto_approved_ignores_expected_and_other_tasks():
    lines = [_log("S0.4", "gh secret list"), _log("2", "rm -rf x")]
    assert harness_auto_approved(lines, task_id="S0.4") is True


def test_harness_auto_approved_false_on_unexpected_denial():
    lines = [_log("S0.4", "python -c 'x'"), "garbage"]
    assert harness_auto_approved(lines, task_id="S0.4") is False


def test_render_progress_lists_every_task_status(tasks):
    md = render_progress(
        tasks, done={"S0.4"}, running={"B"}, blocked={"probe": "cần secret"}, skipped={"13"}
    )
    assert "| S0.4 | `feature/runs-log` | merged |" in md
    assert "| B | `feature/baseline-naive` | running |" in md
    assert "cần secret" in md and "| 13 |" in md and "skipped" in md
    assert "| 2 | `feature/event-spec` | pending |" in md


def test_real_prs_toml_loads_and_starts_with_s04():
    real = load_tasks(Path(__file__).resolve().parents[1] / "scripts" / "prs.toml")
    assert real[0].id == "S0.4"
    ready, blocked, _ = next_ready(real, set(), set(), NO_SECRETS, slots=2)
    assert ids(ready) == ["S0.4"] and "probe" in blocked
    known_agents = {
        p.stem for p in (Path(__file__).resolve().parents[1] / ".claude" / "agents").glob("*.md")
    }
    assert {t.agent for t in real} <= known_agents
