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
    ready, blocked, _ = next_ready(real, set(), set(), CLOUD, slots=2)
    assert ids(ready) == ["S0.4"]  # cloud probe waits for the harness check (S0.4) too
    _, blocked, _ = next_ready(real, {"S0.4"}, set(), NO_SECRETS, slots=2)
    assert "probe" in blocked
    no_harness = Gates(secrets=CLOUD.secrets, harness_approved=False)
    _, blocked, _ = next_ready(real, {"S0.4"}, set(), no_harness, slots=2)
    assert "harness" in blocked["probe"]
    known_agents = {
        p.stem for p in (Path(__file__).resolve().parents[1] / ".claude" / "agents").glob("*.md")
    }
    assert {t.agent for t in real} <= known_agents


def test_harness_file_approved_needs_a_line_starting_with_marker():
    from repopulse.orchestrator import harness_file_approved

    template = (Path(__file__).resolve().parents[1] / "docs" / "harness-check.md").read_text(
        "utf-8"
    )
    assert harness_file_approved(template) is False
    assert harness_file_approved(template + "\nLam đã duyệt 2026-11-11\n") is True
    assert harness_file_approved("") is False


def test_harness_auto_approved_counts_headless_permission_requests():
    # In -p with --permission-prompts none a prompt is an automatic denial (PermissionRequest).
    lines = [_log("S0.4", "python -c 'x'", event="PermissionRequest")]
    assert harness_auto_approved(lines, task_id="S0.4") is False
    assert harness_auto_approved([_log("S0.4", "gh secret list", "PermissionRequest")], "S0.4")


def test_rate_limit_warns_on_any_unified_window_over_threshold():
    ev = {
        "type": "rate_limit_event",
        "rate_limit_info": {
            "status": "allowed",
            "unifiedWindows": {
                "five_hour": {"utilization": 0.04, "status": "allowed"},
                "seven_day": {"utilization": 0.95, "status": "allowed"},
            },
        },
    }
    assert rate_limit_action(ev) == ("warn", 0.95)
    ev["rate_limit_info"]["unifiedWindows"]["seven_day"]["utilization"] = 0.59
    assert rate_limit_action(ev) == ("none", None)


def test_trust_keys_cover_both_drive_letter_cases():
    from repopulse.orchestrator import trust_keys

    keys = trust_keys("D:\Desktop\RepoPulse\.claude\worktrees\pr-2")
    assert "d:/Desktop/RepoPulse/.claude/worktrees/pr-2" in keys
    assert "D:/Desktop/RepoPulse/.claude/worktrees/pr-2" in keys


def test_mark_trusted_adds_keys_and_keeps_other_projects():
    from repopulse.orchestrator import mark_trusted

    cfg = {"projects": {"c:/other": {"hasTrustDialogAccepted": True, "x": 1}}, "keep": 2}
    out, changed = mark_trusted(cfg, ["d:/a", "D:/a"])
    assert changed is True and out["keep"] == 2 and out["projects"]["c:/other"]["x"] == 1
    assert out["projects"]["d:/a"]["hasTrustDialogAccepted"] is True
    assert mark_trusted(out, ["d:/a"])[1] is False


def test_hold_until_returns_reset_of_hot_windows():
    from repopulse.orchestrator import hold_until

    ev = {
        "type": "rate_limit_event",
        "rate_limit_info": {
            "status": "allowed",
            "resetsAt": 100,
            "unifiedWindows": {
                "five_hour": {"utilization": 0.2, "resetsAt": 200},
                "seven_day": {"utilization": 0.93, "resetsAt": 5000},
            },
        },
    }
    assert hold_until(ev) == 5000
    ev["rate_limit_info"]["unifiedWindows"]["five_hour"]["utilization"] = 0.95
    assert hold_until(ev) == 5000  # wait for the latest hot window
    ev["rate_limit_info"]["unifiedWindows"]["seven_day"]["utilization"] = 0.1
    assert hold_until(ev) == 200
    assert hold_until({"type": "assistant"}) is None
    flat = {
        "type": "rate_limit_event",
        "rate_limit_info": {"status": "allowed_warning", "utilization": 0.95, "resetsAt": 7},
    }
    assert hold_until(flat) == 7


def test_should_exit_waits_for_refresh_after_a_merge():
    # Regression: S0.4 merged, gates were still the pre-merge snapshot (harness not yet
    # auto-approved), nothing was running, so the loop exited instead of refreshing gates.
    from repopulse.orchestrator import should_exit

    assert should_exit(running=0, waiting=0, refresh_pending=True, stopping=False) is False
    assert should_exit(running=0, waiting=0, refresh_pending=False, stopping=False) is True
    assert should_exit(running=1, waiting=0, refresh_pending=False, stopping=False) is False
    assert should_exit(running=0, waiting=1, refresh_pending=False, stopping=False) is False
    # a STOP request still ends the run once nothing is running
    assert should_exit(running=0, waiting=1, refresh_pending=True, stopping=True) is True
    assert should_exit(running=1, waiting=0, refresh_pending=False, stopping=True) is False


def test_held_task_is_not_started_and_does_not_unlock_dependants(tasks):
    # Regression 2026-11-11 03:55: PR 5 was waiting for a usage-limit reset, the run loop
    # passed waiting tasks as "done", and PR 6 (depends on 5) started on a develop without 5.
    done = {"S0.4", "B", "2", "probe"}
    ready, _, _ = next_ready(tasks, done, set(), CLOUD, slots=2, held={"4"})
    assert "4" not in ids(ready)  # held task is not restarted
    assert "7" not in ids(ready)  # 7 depends on held 4: must not start
    assert ready == []


def test_held_default_keeps_old_behaviour(tasks):
    ready, _, _ = next_ready(tasks, {"S0.4", "B"}, set(), NO_SECRETS, slots=2)
    assert ids(ready) == ["2", "4"]


def test_scheduling_sets_keeps_waiting_and_unmerged_final_out_of_done():
    from repopulse.orchestrator import Decision, scheduling_sets

    final = {
        "4": Decision("merged", "16"),
        "8": Decision("failed", "CI red"),
        "9": Decision("blocked", "needs secret"),
    }
    done, held = scheduling_sets(done={"S0.4"}, final=final, waiting={"5"})
    assert done == {"S0.4", "4"}  # merged in this run counts even if gh lags
    assert held == {"5", "8", "9"}
    assert "5" not in done and "8" not in done


def test_scheduling_sets_feeds_next_ready_correctly(tasks):
    from repopulse.orchestrator import Decision, scheduling_sets

    done, held = scheduling_sets(done={"S0.4", "B", "2", "probe"}, final={}, waiting={"4"})
    ready, _, _ = next_ready(tasks, done, set(), CLOUD, slots=2, held=held)
    assert ready == []  # 7 depends on waiting 4
    done, held = scheduling_sets(
        done={"S0.4", "B", "2", "probe"}, final={"4": Decision("merged", "16")}, waiting=set()
    )
    ready, _, _ = next_ready(tasks, done, set(), CLOUD, slots=2, held=held)
    assert ids(ready) == ["7"]


# ---------- retry with the previous failure (#1) ----------


def _line(obj: dict) -> str:
    return json.dumps(obj, ensure_ascii=False)


def test_failure_context_prefers_final_result_text():
    from repopulse.orchestrator import failure_context

    lines = [
        _line({"type": "assistant", "message": {"content": [{"type": "text", "text": "old"}]}}),
        _line({"type": "result", "result": "CI đỏ ở job Lint\nRESULT: failed lint"}),
    ]
    ctx = failure_context(lines)
    assert "CI đỏ ở job Lint" in ctx
    assert "old" not in ctx


def test_failure_context_falls_back_to_last_assistant_texts_and_truncates():
    from repopulse.orchestrator import failure_context

    long = "x" * 5000
    lines = [
        "not json",
        _line({"type": "assistant", "message": {"content": [{"type": "text", "text": "đầu"}]}}),
        _line({"type": "assistant", "message": {"content": [{"type": "tool_use", "name": "B"}]}}),
        _line({"type": "assistant", "message": {"content": [{"type": "text", "text": long}]}}),
    ]
    ctx = failure_context(lines, max_chars=800)
    assert len(ctx) <= 800
    assert ctx.endswith("x")  # keeps the tail, the most recent text
    assert failure_context([]) == ""


def test_build_prompt_without_failure_has_no_failure_section(tasks):
    assert "Lần trước thất bại" not in build_prompt(tasks[2])


def test_build_prompt_with_previous_failure_tells_session_what_went_wrong(tasks):
    p = build_prompt(tasks[2], previous_failure="treo 30 phút\nchờ gh pr checks")
    assert "Lần trước thất bại" in p
    assert "treo 30 phút" in p and "chờ gh pr checks" in p
    assert p.rstrip().endswith("RESULT: blocked <việc cần chủ repo làm>")  # contract stays last


# ---------- develop CI health (#3) ----------


def test_develop_ci_state_maps_latest_run():
    from repopulse.orchestrator import develop_ci_state

    assert develop_ci_state([]) == "unknown"
    assert develop_ci_state([{"status": "completed", "conclusion": "success"}]) == "green"
    assert develop_ci_state([{"status": "completed", "conclusion": "failure"}]) == "red"
    assert develop_ci_state([{"status": "completed", "conclusion": "cancelled"}]) == "unknown"
    assert develop_ci_state([{"status": "in_progress", "conclusion": ""}]) == "pending"
    # only the newest run counts (gh lists newest first)
    runs = [
        {"status": "completed", "conclusion": "success"},
        {"status": "completed", "conclusion": "failure"},
    ]
    assert develop_ci_state(runs) == "green"


def test_develop_ci_pending_keeps_previous_verdict():
    from repopulse.orchestrator import develop_ci_blocks

    # red blocks new work; pending/unknown keep whatever we knew before; only green releases it
    assert develop_ci_blocks("red", previously_blocked=False) is True
    assert develop_ci_blocks("pending", previously_blocked=True) is True
    assert develop_ci_blocks("pending", previously_blocked=False) is False
    assert develop_ci_blocks("green", previously_blocked=True) is False


def test_develop_ci_unknown_does_not_clear_a_red_flag():
    # a gh error or a cancelled run says nothing new: a red develop stays red until green
    from repopulse.orchestrator import develop_ci_blocks

    assert develop_ci_blocks("unknown", previously_blocked=True) is True
    assert develop_ci_blocks("unknown", previously_blocked=False) is False


def test_failure_context_keeps_only_the_five_latest_texts():
    from repopulse.orchestrator import failure_context

    lines = [
        _line({"type": "assistant", "message": {"content": [{"type": "text", "text": f"t{i}"}]}})
        for i in range(8)
    ]
    ctx = failure_context(lines)
    assert ctx.splitlines() == ["t3", "t4", "t5", "t6", "t7"]


# ---------- stall detection (#6) ----------


def test_is_stalled_after_silence_limit():
    from repopulse.orchestrator import is_stalled

    assert is_stalled(last_output=1000, now=1000 + 1799, limit_s=1800, last_rate=None) is False
    assert is_stalled(last_output=1000, now=1000 + 1800, limit_s=1800, last_rate=None) is False
    assert is_stalled(last_output=1000, now=1000 + 1801, limit_s=1800, last_rate=None) is True


def test_decide_after_exit_stalled_session_fails_even_with_stale_rejected_event():
    # Review PR 24: a stalled run whose last_rate is an old "rejected" event must not be
    # resumed as if it hit the usage limit; it is a failure with the stall as its reason.
    stale = {
        "type": "rate_limit_event",
        "rate_limit_info": {"status": "rejected", "resetsAt": 1000},
    }
    d = decide_after_exit(
        -15, stale, "", now=5000, max_wait_s=6 * 3600, stalled="treo 30 phút không có output"
    )
    assert d.kind == "failed"
    assert d.reason == "treo 30 phút không có output"


def test_decide_after_exit_stalled_but_merged_is_still_merged():
    d = decide_after_exit(0, None, "RESULT: merged 24", now=0, max_wait_s=0, stalled="treo")
    assert d.kind == "merged"


def test_is_stalled_not_while_waiting_for_quota_reset():
    from repopulse.orchestrator import is_stalled

    rejected = {
        "type": "rate_limit_event",
        "rate_limit_info": {"status": "rejected", "resetsAt": 10_000},
    }
    # silent because the in-session watchdog waits for the reset: not a stall
    assert is_stalled(last_output=1000, now=9000, limit_s=1800, last_rate=rejected) is False
    # reset passed long ago and still silent: stalled
    assert is_stalled(last_output=1000, now=10_000 + 1801, limit_s=1800, last_rate=rejected) is True


# ---------- CLI defaults ----------


def _load_cli():
    import importlib.util
    import sys

    path = Path(__file__).resolve().parents[1] / "scripts" / "orchestrate.py"
    spec = importlib.util.spec_from_file_location("orchestrate_cli", path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules["orchestrate_cli"] = mod  # dataclasses resolve annotations via sys.modules
    spec.loader.exec_module(mod)
    return mod


def test_cli_defaults_to_five_parallel_and_thirty_minute_stall():
    args = _load_cli().build_parser().parse_args(["run"])
    assert args.parallel == 5
    assert args.stall_minutes == 30


# ---------- independent review session (#2) ----------


def test_build_prompt_stops_at_ready_and_never_reviews_or_merges(tasks):
    p = build_prompt(tasks[2])
    assert "RESULT: ready <số PR>" in p
    assert "gh pr merge" not in p
    assert "code-reviewer" not in p  # review is a separate, fresh session


def test_parse_result_accepts_ready():
    assert parse_result("CI xanh\nRESULT: ready 31") == ("ready", "31")


def test_decide_after_exit_ready_carries_pr_number():
    d = decide_after_exit(0, None, "RESULT: ready 31", now=0, max_wait_s=0)
    assert d.kind == "ready"
    assert d.reason == "31"


def test_decide_after_exit_ready_keeps_only_the_digits():
    for text in ("RESULT: ready #31", "RESULT: ready 31 (CI xanh)", "RESULT: ready PR #31"):
        d = decide_after_exit(0, None, text, now=0, max_wait_s=0)
        assert (d.kind, d.reason) == ("ready", "31"), text


def test_decide_after_exit_ready_prefers_the_hash_number():
    # Review PR 25 (round 2): "ready PR 2 #31" must read the PR number, not the task id
    d = decide_after_exit(0, None, "RESULT: ready PR 2 #31", now=0, max_wait_s=0)
    assert (d.kind, d.reason) == ("ready", "31")


def test_cli_has_daily_command_and_daily_hour():
    parser = _load_cli().build_parser()
    assert parser.parse_args(["run"]).daily_hour == 22
    args = parser.parse_args(["daily", "--date", "2026-11-11", "--no-send"])
    assert args.cmd == "daily" and args.date == "2026-11-11" and args.no_send is True
    assert parser.parse_args(["daily"]).force is False


def test_decide_after_exit_ready_without_pr_number_is_a_counted_failure():
    # Review PR 25: "ready" with no number put "" in pending_review, the next start was a build
    # session again (falsy review_pr) and the loop never ended.
    d = decide_after_exit(0, None, "RESULT: ready", now=0, max_wait_s=0)
    assert d.kind == "failed"
    assert "số PR" in d.reason


def test_decide_after_exit_ready_beats_a_stale_rate_event():
    stale = {"type": "rate_limit_event", "rate_limit_info": {"status": "rejected", "resetsAt": 9}}
    d = decide_after_exit(0, stale, "RESULT: ready 31", now=0, max_wait_s=3600)
    assert (d.kind, d.reason) == ("ready", "31")


def test_build_review_command_runs_code_reviewer_agent_headless(tmp_path: Path):
    from repopulse.orchestrator import build_review_command

    cmd = build_review_command(tmp_path, "REVIEW IT", resume=None)
    assert cmd[:3] == ["claude", "-p", "REVIEW IT"]
    joined = " ".join(cmd)
    assert "--agent code-reviewer" in joined
    assert "--model opus" in joined and "--effort medium" in joined
    assert "--permission-mode auto" in joined and "--permission-prompts none" in joined
    assert "--output-format stream-json" in joined
    assert "--resume" not in cmd
    assert build_review_command(tmp_path, "R", resume="s1")[-2:] == ["--resume", "s1"]


def test_build_review_prompt_names_pr_and_verdict_contract(tasks):
    from repopulse.orchestrator import build_review_prompt

    p = build_review_prompt(tasks[2], "31")
    assert "#31" in p and "feature/event-spec" in p
    assert "gh pr comment 31" in p
    assert "REVIEW: PASS" in p and "REVIEW: FAIL" in p
    assert "không merge" in p.lower()


def test_parse_review_takes_last_verdict_line():
    from repopulse.orchestrator import parse_review

    assert parse_review("REVIEW: FAIL a\n...\nREVIEW: PASS") == ("PASS", "")
    assert parse_review("REVIEW: FAIL thiếu test cho dedup") == ("FAIL", "thiếu test cho dedup")
    assert parse_review("không có gì") is None


def test_parse_review_trusts_only_review_lines():
    # Review PR 25: the agent's template "Kết luận: PASS | FAIL" must never read as a PASS;
    # merging hangs on this, so only an explicit REVIEW: line counts.
    from repopulse.orchestrator import parse_review

    assert parse_review("## Kết luận: PASS | FAIL") is None
    assert parse_review("## Kết luận: PASS") is None
    assert parse_review("## Kết luận: PASS\n\nREVIEW: FAIL thiếu test") == ("FAIL", "thiếu test")


def test_decide_after_review_maps_verdict_quota_and_missing_line():
    from repopulse.orchestrator import decide_after_review

    assert decide_after_review(0, None, "REVIEW: PASS", now=0, max_wait_s=0).kind == "pass"
    d = decide_after_review(0, None, "REVIEW: FAIL mutation sống", now=0, max_wait_s=0)
    assert (d.kind, d.reason) == ("fail", "mutation sống")
    limit = {
        "type": "rate_limit_event",
        "rate_limit_info": {"status": "rejected", "resetsAt": 100},
    }
    assert decide_after_review(1, limit, "", now=50, max_wait_s=3600).kind == "resume_at"
    assert decide_after_review(1, None, "", now=0, max_wait_s=0).kind == "failed"
    d = decide_after_review(1, limit, "", now=50, max_wait_s=3600, stalled="treo")
    assert (d.kind, d.reason) == ("failed", "treo")


def test_merge_ready_needs_all_three_required_checks_passing():
    from repopulse.orchestrator import merge_ready

    ok = [
        {"name": "Lint", "bucket": "pass"},
        {"name": "Unit Tests", "bucket": "pass"},
        {"name": "dbt build (DuckDB)", "bucket": "pass"},
        {"name": "GitGuardian Security Checks", "bucket": "pass"},
    ]
    assert merge_ready(ok) == (True, "")
    red = [dict(c, bucket="fail") if c["name"] == "Unit Tests" else c for c in ok]
    assert merge_ready(red) == (False, "Unit Tests: fail")
    missing = [c for c in ok if c["name"] != "Lint"]
    assert merge_ready(missing) == (False, "Lint: thiếu")
    # Review PR 25: a check still running (or skipped) is not a pass
    for bucket in ("pending", "skipping", "cancel"):
        running = [dict(c, bucket=bucket) if c["name"] == "Unit Tests" else c for c in ok]
        assert merge_ready(running) == (False, f"Unit Tests: {bucket}")


def test_review_next_merges_on_pass_fixes_once_then_gives_up():
    from repopulse.orchestrator import Decision, review_next

    assert review_next(Decision("pass"), failed_rounds=0, max_rounds=2) == "merge"
    assert review_next(Decision("pass"), failed_rounds=1, max_rounds=2) == "merge"
    assert review_next(Decision("fail", "x"), failed_rounds=0, max_rounds=2) == "fix"
    assert review_next(Decision("fail", "x"), failed_rounds=1, max_rounds=2) == "give_up"


# ---------- --hold-at <percent> ----------


def _quota(five_hour, seven_day, status="allowed"):
    return _rl(
        status,
        unifiedWindows={
            "five_hour": {"utilization": five_hour, "status": status},
            "seven_day": {"utilization": seven_day, "status": status},
        },
    )


def test_quota_peak_is_the_highest_of_the_five_hour_and_seven_day_windows():
    from repopulse.orchestrator import quota_peak

    assert quota_peak(_quota(five_hour=0.04, seven_day=0.75)) == 0.75
    assert quota_peak(_quota(five_hour=0.62, seven_day=0.10)) == 0.62
    assert quota_peak(_rl("allowed_warning", utilization=0.93)) == 0.93


def test_quota_peak_is_zero_when_there_is_nothing_to_hold_on():
    from repopulse.orchestrator import quota_peak

    assert quota_peak(None) == 0.0
    assert quota_peak({"type": "assistant"}) == 0.0
    assert quota_peak(_rl("allowed")) == 0.0
    # a rejected event is handled as wait/stop by rate_limit_action, never as a hold (as before)
    assert quota_peak(_rl("rejected", utilization=1.0, resetsAt=9)) == 0.0


def test_over_hold_at_compares_the_peak_with_the_chosen_percent():
    from repopulse.orchestrator import over_hold_at

    assert over_hold_at(0.75, 70) is True
    assert over_hold_at(0.69, 70) is False
    assert over_hold_at(0.75, 80) is False
    assert over_hold_at(0.05, 1) is True
    assert over_hold_at(0.99, 100) is False
    assert over_hold_at(1.0, 100) is True


def test_over_hold_at_without_a_threshold_never_holds():
    from repopulse.orchestrator import over_hold_at

    assert over_hold_at(1.0, None) is False
    assert over_hold_at(0.0, None) is False


def test_over_hold_at_holds_exactly_at_every_percent_and_not_one_below():
    # utilization arrives as a JSON decimal; 0.29 * 100 is 28.999999999999996 in floats, so a
    # naive "peak * 100 >= percent" would miss the threshold
    from repopulse.orchestrator import over_hold_at

    for percent in range(1, 101):
        at = json.loads("1.0" if percent == 100 else f"0.{percent:02d}")
        below = json.loads(f"0.{percent - 1:02d}")
        assert over_hold_at(at, percent) is True, percent
        assert over_hold_at(below, percent) is False, percent


def test_hold_at_90_holds_exactly_when_the_old_warn_did():
    from repopulse.orchestrator import over_hold_at, quota_peak

    events = [_rl("allowed_warning", utilization=u) for u in (0.5, 0.89, 0.9, 0.93, 1.0)]
    events += [_quota(0.04, 0.95), _quota(0.04, 0.59), _rl("rejected", resetsAt=9)]
    for ev in events:
        warned = rate_limit_action(ev)[0] == "warn"
        assert over_hold_at(quota_peak(ev), 90) is warned, ev


def test_parse_hold_at_accepts_whole_percents_from_1_to_100():
    from repopulse.orchestrator import parse_hold_at

    assert parse_hold_at("1") == 1
    assert parse_hold_at("75") == 75
    assert parse_hold_at("100") == 100


@pytest.mark.parametrize("bad", ["0", "101", "-5", "7.5", "abc", "", "90%"])
def test_parse_hold_at_rejects_anything_else(bad):
    from repopulse.orchestrator import parse_hold_at

    with pytest.raises(ValueError, match="1.*100"):
        parse_hold_at(bad)


def test_cli_hold_at_takes_a_percent():
    args = _load_cli().build_parser().parse_args(["run", "--hold-at", "75"])
    assert args.hold_at == 75


def test_cli_hold_at_90_is_the_same_as_hold_at_90_percent():
    parser = _load_cli().build_parser()
    old = parser.parse_args(["run", "--hold-at-90"])
    new = parser.parse_args(["run", "--hold-at", "90"])
    assert old.hold_at == 90
    assert vars(old) == vars(new)


def test_cli_without_a_hold_flag_runs_until_the_quota_is_gone():
    assert _load_cli().build_parser().parse_args(["run"]).hold_at is None


@pytest.mark.parametrize("bad", ["0", "101", "7.5", "abc"])
def test_cli_rejects_a_hold_at_outside_1_to_100(bad, capsys):
    with pytest.raises(SystemExit) as exc:
        _load_cli().build_parser().parse_args(["run", "--hold-at", bad])
    assert exc.value.code == 2
    assert "--hold-at" in capsys.readouterr().err


def test_cli_rejects_hold_at_together_with_hold_at_90(capsys):
    with pytest.raises(SystemExit) as exc:
        _load_cli().build_parser().parse_args(["run", "--hold-at", "50", "--hold-at-90"])
    assert exc.value.code == 2
    assert "not allowed with" in capsys.readouterr().err


def test_reader_keeps_the_highest_quota_peak_a_session_reported(tasks, tmp_path: Path):
    # the run loop compares this peak with --hold-at; it stays up after a later, lower event
    from types import SimpleNamespace

    cli = _load_cli()
    lines = [
        json.dumps(ev) + "\n"
        for ev in (
            {"type": "system", "subtype": "init", "session_id": "s1"},
            _quota(five_hour=0.40, seven_day=0.10),
            _quota(five_hour=0.75, seven_day=0.10),
            _quota(five_hour=0.60, seven_day=0.10),
        )
    ]
    run = cli.Run(task=tasks[0], proc=SimpleNamespace(stdout=iter(lines)), path=tmp_path, started=0)
    assert run.peak == 0.0
    cli._reader(run, tmp_path / "pr.jsonl")
    assert run.peak == 0.75
    assert run.session_id == "s1"


def _t(needs, optional=False):
    from repopulse.orchestrator import Task

    return Task(id="x", branch="feature/x", agent="a", needs=tuple(needs), optional=optional)


def test_unknown_need_blocks_instead_of_passing_silently():
    g = gate_for(_t(["clod"]), CLOUD)
    assert g is not None and g.kind == "block" and "clod" in g.reason


def test_build_prompt_requires_foreground_agents_and_no_early_end(tasks):
    # Regression 2026-11-11 20:18: the D1 session handed the rest of the work to a
    # background subagent and ended its turn "waiting", with no RESULT line; the
    # session exit killed the background agent and the run counted as failed.
    p = build_prompt(tasks[2])
    assert "run_in_background" in p and "foreground" in p
    assert "không kết thúc lượt" in p


def test_merge_failure_reason_tells_the_retry_what_to_do():
    # 2026-11-11: four "review PASS but merge failed" retries had only "merge PR #N thất bại"
    # as context, so the retried session did not know the branch conflicted with develop.
    from repopulse.orchestrator import merge_failure_reason

    dirty = merge_failure_reason("50", {"mergeable": "CONFLICTING", "mergeStateStatus": "DIRTY"})
    assert "conflict" in dirty and "origin/develop" in dirty and "#50" in dirty
    behind = merge_failure_reason("7", {"mergeable": "MERGEABLE", "mergeStateStatus": "BEHIND"})
    assert "gh pr update-branch 7" in behind
    blocked = merge_failure_reason("9", {"mergeable": "MERGEABLE", "mergeStateStatus": "BLOCKED"})
    assert "check" in blocked
    assert "#3" in merge_failure_reason("3", {})


def test_merge_failure_reason_detects_conflict_from_either_field():
    from repopulse.orchestrator import merge_failure_reason

    for view in (
        {"mergeable": "UNKNOWN", "mergeStateStatus": "DIRTY"},
        {"mergeable": "CONFLICTING", "mergeStateStatus": "UNKNOWN"},
    ):
        assert "origin/develop" in merge_failure_reason("50", view)


def test_merge_failure_reason_names_the_blocking_state():
    from repopulse.orchestrator import merge_failure_reason

    for state in ("BLOCKED", "UNSTABLE"):
        msg = merge_failure_reason("9", {"mergeable": "MERGEABLE", "mergeStateStatus": state})
        assert state in msg and "CI" in msg
