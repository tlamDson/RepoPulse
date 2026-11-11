"""PermissionRequest / PermissionDenied hook: log every prompt or denial, retry flaky classifier.

Writes one JSON line per event to ``$REPOPULSE_STATE_DIR/permission-log.jsonl`` (the
orchestrator sets this to the main checkout's ``.claude/state``), falling back to
``$CLAUDE_PROJECT_DIR/.claude/state``. Must never fail or block the session.
"""

from __future__ import annotations

import json
import os
import sys
from datetime import UTC, datetime
from pathlib import Path

MAX_INPUT = 500
FLAKY_MARKERS = ("no verdict", "classifier unavailable", "classifier error")


def _input_text(tool_input: object) -> str:
    if isinstance(tool_input, dict):
        for key in ("command", "file_path", "path", "url", "pattern"):
            if key in tool_input:
                return str(tool_input[key])[:MAX_INPUT]
        return json.dumps(tool_input, ensure_ascii=False)[:MAX_INPUT]
    return str(tool_input or "")[:MAX_INPUT]


def build_entry(payload: dict, env: dict, now: str) -> dict:
    return {
        "ts": now,
        "task": env.get("REPOPULSE_TASK", "manual"),
        "event": payload.get("hook_event_name", ""),
        "tool": payload.get("tool_name", ""),
        "input": _input_text(payload.get("tool_input")),
        "reason": str(payload.get("reason") or payload.get("permission_decision_reason") or ""),
        "agent": payload.get("agent_type") or payload.get("agent_id") or "main",
    }


def retry_output(payload: dict) -> dict | None:
    if payload.get("hook_event_name") != "PermissionDenied":
        return None
    reason = str(payload.get("reason", "")).lower()
    if any(m in reason for m in FLAKY_MARKERS):
        return {"hookSpecificOutput": {"hookEventName": "PermissionDenied", "retry": True}}
    return None


def _state_dir(env: dict) -> Path:
    if env.get("REPOPULSE_STATE_DIR"):
        return Path(env["REPOPULSE_STATE_DIR"])
    return Path(env.get("CLAUDE_PROJECT_DIR", ".")) / ".claude" / "state"


def main() -> int:
    try:
        payload = json.loads(sys.stdin.read() or "{}")
        if not isinstance(payload, dict):
            return 0
        env = dict(os.environ)
        now = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
        state = _state_dir(env)
        state.mkdir(parents=True, exist_ok=True)
        with (state / "permission-log.jsonl").open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(build_entry(payload, env, now), ensure_ascii=False) + "\n")
        out = retry_output(payload)
        if out:
            print(json.dumps(out))
    except Exception:  # noqa: BLE001 - a logging hook must never break the session
        pass
    return 0


if __name__ == "__main__":
    sys.exit(main())
