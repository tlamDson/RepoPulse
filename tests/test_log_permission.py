"""Tests for the PermissionRequest/PermissionDenied logging hook."""

from __future__ import annotations

import importlib.util
import json
import subprocess
import sys
from pathlib import Path

HOOK = Path(__file__).resolve().parents[1] / ".claude" / "hooks" / "log_permission.py"


def _load():
    spec = importlib.util.spec_from_file_location("log_permission", HOOK)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_build_entry_extracts_command_task_and_agent():
    mod = _load()
    payload = {
        "hook_event_name": "PermissionDenied",
        "tool_name": "Bash",
        "tool_input": {"command": "gh secret list"},
        "reason": "matched ask rule",
        "agent_type": "code-reviewer",
    }
    e = mod.build_entry(payload, {"REPOPULSE_TASK": "S0.4"}, now="2026-11-11T00:00:00Z")
    assert e == {
        "ts": "2026-11-11T00:00:00Z",
        "task": "S0.4",
        "event": "PermissionDenied",
        "tool": "Bash",
        "input": "gh secret list",
        "reason": "matched ask rule",
        "agent": "code-reviewer",
    }


def test_build_entry_defaults_and_truncates():
    mod = _load()
    payload = {
        "hook_event_name": "PermissionRequest",
        "tool_name": "Write",
        "tool_input": {"file_path": "x" * 900},
    }
    e = mod.build_entry(payload, {}, now="t")
    assert e["task"] == "manual" and e["agent"] == "main"
    assert len(e["input"]) <= 500


def test_retry_only_for_classifier_errors():
    mod = _load()
    flaky = {
        "hook_event_name": "PermissionDenied",
        "reason": "The server-side auto mode classifier gave no verdict (error)",
    }
    assert mod.retry_output(flaky) == {
        "hookSpecificOutput": {"hookEventName": "PermissionDenied", "retry": True}
    }
    assert mod.retry_output({"hook_event_name": "PermissionDenied", "reason": "deny rule"}) is None
    assert (
        mod.retry_output({"hook_event_name": "PermissionRequest", "reason": "no verdict"}) is None
    )


def test_main_appends_jsonl_and_never_fails(tmp_path: Path):
    payload = json.dumps(
        {
            "hook_event_name": "PermissionDenied",
            "tool_name": "Bash",
            "tool_input": {"command": "rm -rf x"},
        }
    )
    env = {"REPOPULSE_STATE_DIR": str(tmp_path), "REPOPULSE_TASK": "2", "PATH": ""}
    r = subprocess.run(
        [sys.executable, str(HOOK)], input=payload, text=True, capture_output=True, env=env
    )
    assert r.returncode == 0
    lines = (tmp_path / "permission-log.jsonl").read_text("utf-8").splitlines()
    assert json.loads(lines[-1])["input"] == "rm -rf x"
    bad = subprocess.run(
        [sys.executable, str(HOOK)], input="not json", text=True, capture_output=True, env=env
    )
    assert bad.returncode == 0
