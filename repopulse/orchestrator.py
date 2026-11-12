"""Pure core of the headless orchestrator.

Everything here is side-effect free so it can be unit tested. Process spawning, git and gh
calls live in ``scripts/orchestrate.py``.
"""

from __future__ import annotations

import json
import re
import tomllib
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path

RESUME_GRACE_S = 60
WARN_UTILIZATION = 0.9
EXPECTED_DENIALS = ("gh secret",)

# ---------- tasks ----------


@dataclass(frozen=True)
class Task:
    id: str
    branch: str
    agent: str
    deps: tuple[str, ...] = ()
    needs: tuple[str, ...] = ()
    release_after: bool = False
    optional: bool = False
    title: str = ""


def load_tasks(path: Path) -> list[Task]:
    data = tomllib.loads(Path(path).read_text(encoding="utf-8"))
    tasks = [
        Task(
            id=str(t["id"]),
            branch=t["branch"],
            agent=t["agent"],
            deps=tuple(str(d) for d in t.get("deps", ())),
            needs=tuple(t.get("needs", ())),
            release_after=bool(t.get("release_after", False)),
            optional=bool(t.get("optional", False)),
            title=t.get("title", ""),
        )
        for t in data.get("task", [])
    ]
    known = {t.id for t in tasks}
    for t in tasks:
        missing = [d for d in t.deps if d not in known]
        if missing:
            raise ValueError(f"task {t.id} depends on unknown task(s): {', '.join(missing)}")
    return tasks


# ---------- gates ----------


@dataclass(frozen=True)
class Gates:
    secrets: frozenset[str]
    harness_approved: bool


@dataclass(frozen=True)
class Gate:
    kind: str  # "block" | "skip"
    reason: str


KNOWN_NEEDS = frozenset({"cloud", "snowflake", "harness"})


def _has_cloud(secrets: frozenset[str]) -> bool:
    databricks = {"DATABRICKS_HOST", "DATABRICKS_TOKEN"} <= secrets
    return databricks or "MOTHERDUCK_TOKEN" in secrets


def _need_reason(need: str, gates: Gates) -> str | None:
    if need == "cloud" and not _has_cloud(gates.secrets):
        return "cần secret DATABRICKS_HOST + DATABRICKS_TOKEN hoặc MOTHERDUCK_TOKEN"
    if need == "snowflake" and not any(s.startswith("SNOWFLAKE_") for s in gates.secrets):
        return "cần secret SNOWFLAKE_* (đăng ký Snowflake trial)"
    if need == "harness" and not gates.harness_approved:
        return "chờ duyệt harness (docs/harness-check.md chưa có 'Lam đã duyệt')"
    if need not in KNOWN_NEEDS:
        return f"điều kiện không rõ trong prs.toml: {need}"
    return None


def gate_for(task: Task, gates: Gates) -> Gate | None:
    reasons = [r for n in task.needs if (r := _need_reason(n, gates))]
    if not reasons:
        return None
    return Gate("skip" if task.optional else "block", "; ".join(reasons))


# ---------- scheduling ----------


def next_ready(
    tasks: list[Task],
    done: set[str],
    running: set[str],
    gates: Gates,
    slots: int,
    held: set[str] | None = None,
) -> tuple[list[Task], dict[str, str], list[Task]]:
    """Return (ready to start, blocked id -> reason, skipped optional tasks).

    Skipped optional tasks count as satisfied for their dependants. ``held`` tasks (waiting
    for a quota reset, or finished as failed/blocked) are not started and are NOT satisfied,
    so their dependants stay waiting too.
    """
    held = held or set()
    satisfied = set(done)
    skipped: list[Task] = []
    blocked: dict[str, str] = {}
    ready: list[Task] = []
    changed = True
    while changed:  # skips can unlock later tasks, so iterate to a fixed point
        changed = False
        for t in tasks:
            if (
                t.id in satisfied
                or t.id in running
                or t.id in held
                or t in ready
                or t.id in blocked
            ):
                continue
            if not all(d in satisfied for d in t.deps):
                continue
            gate = gate_for(t, gates)
            if gate and gate.kind == "skip":
                skipped.append(t)
                satisfied.add(t.id)
                changed = True
            elif gate:
                blocked[t.id] = gate.reason
            else:
                ready.append(t)
    free = max(0, slots - len(running))
    return ready[:free], blocked, skipped


def scheduling_sets(
    done: set[str], final: dict[str, Decision], waiting: Iterable[str]
) -> tuple[set[str], set[str]]:
    """Split run-loop state into (done, held) for ``next_ready``.

    Tasks merged during this run count as done even if ``gh pr list`` has not caught up.
    Tasks waiting for a quota reset, or finished failed/blocked/stopped, are held: not
    restarted and not satisfying their dependants.
    """
    merged = {k for k, d in final.items() if d.kind == "merged"}
    held = (set(final) - merged) | set(waiting)
    return set(done) | merged, held - merged


def should_exit(running: int, waiting: int, refresh_pending: bool, stopping: bool) -> bool:
    """End the run loop only when nothing is running and nothing more can start.

    ``refresh_pending`` is set right after a task merges: the gates (e.g. the harness
    auto-approval that depends on S0.4 being merged) are still the pre-merge snapshot, so
    the loop must refresh them before concluding there is no work left.
    """
    if running:
        return False
    if stopping:
        return True
    return not waiting and not refresh_pending


def merged_task_ids(tasks: list[Task], prs: Iterable[dict]) -> set[str]:
    merged = {p["headRefName"] for p in prs if p.get("state") == "MERGED"}
    return {t.id for t in tasks if t.branch in merged}


def releases_due(tasks: list[Task], done: set[str], released: set[str]) -> list[str]:
    return [t.id for t in tasks if t.release_after and t.id in done and t.id not in released]


# ---------- stream-json, quota, result ----------


def parse_stream_line(line: str) -> dict | None:
    line = line.strip()
    if not line:
        return None
    try:
        obj = json.loads(line)
    except json.JSONDecodeError:
        return None
    return obj if isinstance(obj, dict) else None


def rate_limit_action(event: dict | None) -> tuple[str, int | str | None]:
    """Map a stream event to ("none"|"warn"|"wait"|"stop", detail)."""
    if not event or event.get("type") != "rate_limit_event":
        return ("none", None)
    info = event.get("rate_limit_info") or {}
    status = info.get("status")
    if status == "rejected":
        if info.get("errorCode"):
            return ("stop", f"usage limit: {info['errorCode']}")
        return ("wait", info.get("resetsAt"))
    peak = quota_peak(event)
    if peak >= WARN_UTILIZATION:
        return ("warn", peak)
    return ("none", None)


def quota_peak(event: dict | None) -> float:
    """Highest utilization (0..1) of the 5-hour and 7-day windows while requests are allowed.

    0.0 for any other event: a rejected one is a wait/stop (``rate_limit_action``), not a hold.
    """
    if not event or event.get("type") != "rate_limit_event":
        return 0.0
    info = event.get("rate_limit_info") or {}
    if info.get("status") not in ("allowed", "allowed_warning"):
        return 0.0
    windows = info.get("unifiedWindows") or {}
    used = [info.get("utilization") or 0] + [
        (w or {}).get("utilization") or 0 for w in windows.values() if isinstance(w, dict)
    ]
    return float(max(used))


def parse_hold_at(text: str) -> int:
    """Value of ``--hold-at``: a whole percent from 1 to 100."""
    value = int(text) if re.fullmatch(r"[0-9]+", text) else 0
    if not 1 <= value <= 100:
        raise ValueError(f"ngưỡng phải là số nguyên từ 1 đến 100, nhận {text!r}")
    return value


def over_hold_at(peak: float, hold_at: int | None) -> bool:
    """True when a quota peak (0..1) has reached ``hold_at`` percent; None never holds.

    Reaching the threshold already holds, as 0.9 always did for ``--hold-at-90``. The percent
    is divided, not the peak multiplied: 0.29 * 100 is 28.999999999999996 in floats.
    """
    return hold_at is not None and peak >= hold_at / 100


def hold_until(event: dict | None) -> int | None:
    """Reset time (epoch s) of the latest quota window at or above WARN_UTILIZATION, if any."""
    if not event or event.get("type") != "rate_limit_event":
        return None
    info = event.get("rate_limit_info") or {}
    windows = [w for w in (info.get("unifiedWindows") or {}).values() if isinstance(w, dict)]
    windows.append(info)  # the flat fields describe the binding window
    resets = [
        int(w["resetsAt"])
        for w in windows
        if (w.get("utilization") or 0) >= WARN_UTILIZATION and w.get("resetsAt")
    ]
    return max(resets) if resets else None


# "merged" stays for sessions started before the independent review (they merged themselves)
_RESULT_RE = re.compile(r"RESULT:\s*(ready|merged|failed|blocked)\b[ \t]*(.*)")


def parse_result(text: str) -> tuple[str, str] | None:
    found = _RESULT_RE.findall(text or "")
    if not found:
        return None
    status, reason = found[-1]
    return status, reason.strip()


@dataclass(frozen=True)
class Decision:
    # build session: ready | merged | failed | blocked | resume_at | stop
    # review session: pass | fail | failed | resume_at | stop
    kind: str
    reason: str = ""
    at: int | None = None


def _quota_decision(last_rate_event: dict | None, now: int, max_wait_s: int) -> Decision | None:
    action, detail = rate_limit_action(last_rate_event)
    if action == "stop":
        return Decision("stop", str(detail))
    if action == "wait":
        if not isinstance(detail, int):
            return Decision("stop", "usage limit without reset time")
        at = detail + RESUME_GRACE_S
        if at - now > max_wait_s:
            return Decision("stop", f"usage limit resets at {detail}, beyond max wait")
        return Decision("resume_at", "usage limit", at=at)
    return None


def decide_after_exit(
    returncode: int,
    last_rate_event: dict | None,
    result_text: str,
    now: int,
    max_wait_s: int,
    stalled: str | None = None,
) -> Decision:
    """``stalled`` = reason when the loop killed a silent session; it beats a stale quota event."""
    result = parse_result(result_text)
    if result and result[0] == "ready":
        # "#31" wins over a bare number ("ready PR 2 #31" is PR 31, not task 2)
        number = _PR_HASH_RE.search(result[1]) or _PR_NUMBER_RE.search(result[1])
        if not number:
            return Decision("failed", f"RESULT: ready thiếu số PR ({result[1] or 'trống'})")
        return Decision("ready", number.group(1))
    if result and result[0] == "merged":
        return Decision(result[0], result[1])
    if stalled:
        return Decision("failed", stalled)
    quota = _quota_decision(last_rate_event, now, max_wait_s)
    if quota:
        return quota
    if result:
        return Decision(result[0], result[1])
    return Decision("failed", f"exit {returncode}, no RESULT line")


# ---------- independent review session ----------

REQUIRED_CHECKS = ("Lint", "Unit Tests", "dbt build (DuckDB)")
MAX_REVIEW_ROUNDS = 2

_REVIEW_RE = re.compile(r"REVIEW:\s*(PASS|FAIL)\b[ \t]*(.*)")
_PR_HASH_RE = re.compile(r"#(\d+)")
_PR_NUMBER_RE = re.compile(r"(\d+)")


def parse_review(text: str) -> tuple[str, str] | None:
    """Last explicit ``REVIEW: PASS|FAIL <why>`` line. Merging hangs on it, so nothing else
    (e.g. the agent's ``Kết luận: PASS | FAIL`` template heading) counts as a verdict."""
    found = _REVIEW_RE.findall(text or "")
    if not found:
        return None
    verdict, reason = found[-1]
    return verdict, reason.strip()


def decide_after_review(
    returncode: int,
    last_rate_event: dict | None,
    result_text: str,
    now: int,
    max_wait_s: int,
    stalled: str | None = None,
) -> Decision:
    verdict = parse_review(result_text)
    if verdict:
        return Decision("pass" if verdict[0] == "PASS" else "fail", verdict[1])
    if stalled:
        return Decision("failed", stalled)
    quota = _quota_decision(last_rate_event, now, max_wait_s)
    if quota:
        return quota
    return Decision("failed", f"review exit {returncode}, no REVIEW line")


def merge_failure_reason(pr: str, view: dict) -> str:
    """Why `gh pr merge` failed, phrased as what the retried PR session must do.

    ``view`` is ``gh pr view <n> --json mergeable,mergeStateStatus``.
    """
    state = (view or {}).get("mergeStateStatus", "")
    if view and (view.get("mergeable") == "CONFLICTING" or state == "DIRTY"):
        return (
            f"merge PR #{pr} thất bại: nhánh conflict với develop. Chạy `git fetch origin` rồi "
            "`git merge origin/develop`, gỡ conflict (giữ thay đổi của cả hai bên), chạy lại "
            "test, commit merge và push; chờ CI xanh rồi mới ready."
        )
    if state == "BEHIND":
        return (
            f"merge PR #{pr} thất bại: nhánh tụt sau develop. Chạy `gh pr update-branch {pr}`, "
            "chờ CI xanh rồi mới ready."
        )
    if state in ("BLOCKED", "UNSTABLE"):
        return f"merge PR #{pr} thất bại: check bắt buộc chưa xanh ({state}). Chờ CI xong và sửa."
    return f"merge PR #{pr} thất bại ({state or 'không rõ trạng thái'})"


def _check_bucket(checks: list[dict], name: str) -> str | None:
    """One bucket for a check name that may have several runs (push + pull_request): any
    fail/cancel wins, then anything not finished, and "pass" only when every run passed."""
    runs = [c.get("bucket") for c in checks if c.get("name") == name]
    if not runs:
        return None
    for bad in ("fail", "cancel"):
        if bad in runs:
            return bad
    return "pass" if all(b == "pass" for b in runs) else "pending"


def required_checks_state(checks: list[dict]) -> str:
    """'pending' | 'pass' | 'fail' for the required checks only (``gh pr checks --json
    name,bucket``). A required check not registered yet counts as pending, so a freshly
    created PR is never judged red; optional workflows never decide."""
    states = [_check_bucket(checks, name) for name in REQUIRED_CHECKS]
    if any(b in ("fail", "cancel") for b in states):
        return "fail"
    if all(b == "pass" for b in states):
        return "pass"
    return "pending"


def merge_ready(checks: list[dict]) -> tuple[bool, str]:
    """All required checks pass, from ``gh pr checks <n> --json name,bucket``."""
    for name in REQUIRED_CHECKS:
        bucket = _check_bucket(checks, name)
        if bucket != "pass":
            return False, f"{name}: {bucket or 'thiếu'}"
    return True, ""


def review_next(d: Decision, failed_rounds: int, max_rounds: int = MAX_REVIEW_ROUNDS) -> str:
    """merge | fix | give_up after a review verdict (``failed_rounds`` = FAILs before this one)."""
    if d.kind == "pass":
        return "merge"
    return "fix" if failed_rounds + 1 < max_rounds else "give_up"


# ---------- retry context, develop CI, stalls ----------

FAILURE_CONTEXT_CHARS = 1500


def failure_context(log_lines: Iterable[str], max_chars: int = FAILURE_CONTEXT_CHARS) -> str:
    """Tail of what a failed session last said, to brief its retry.

    The final ``result`` event is the session's own summary; without one (killed, crashed)
    fall back to its last assistant texts. Keeps the most recent ``max_chars``.
    """
    result = ""
    texts: list[str] = []
    for line in log_lines:
        ev = parse_stream_line(line)
        if not ev:
            continue
        if ev.get("type") == "result" and ev.get("result"):
            result = str(ev["result"])
        elif ev.get("type") == "assistant":
            content = (ev.get("message") or {}).get("content") or []
            texts += [c.get("text", "") for c in content if c.get("type") == "text"]
    text = result or "\n".join(t for t in texts[-5:] if t)
    return text[-max_chars:].strip()


def develop_ci_state(runs: list[dict]) -> str:
    """State of the newest CI run on develop (``gh run list`` lists newest first)."""
    if not runs:
        return "unknown"
    run = runs[0]
    if run.get("status") != "completed":
        return "pending"
    return {"success": "green", "failure": "red"}.get(run.get("conclusion") or "", "unknown")


def develop_ci_blocks(state: str, previously_blocked: bool) -> bool:
    """Red develop holds new work; only green releases it (pending/unknown keep the verdict)."""
    if state == "red":
        return True
    if state == "green":
        return False
    return previously_blocked


def is_stalled(last_output: float, now: float, limit_s: float, last_rate: dict | None) -> bool:
    """No stream output for ``limit_s``, not counting an in-session wait for a quota reset."""
    since = last_output
    info = (last_rate or {}).get("rate_limit_info") or {}
    if info.get("status") == "rejected" and info.get("resetsAt"):
        since = max(since, float(info["resetsAt"]))
    return now - since > limit_s


# ---------- command and prompt ----------


def build_command(repo_root: Path, prompt: str, resume: str | None) -> list[str]:
    """PR (build) session: Fable, high."""
    return _headless(repo_root, prompt, resume, ["--model", "fable", "--effort", "high"])


def build_review_command(repo_root: Path, prompt: str, resume: str | None) -> list[str]:
    """Independent review session: a fresh ``code-reviewer`` agent session, Opus, medium."""
    extra = ["--agent", "code-reviewer", "--model", "opus", "--effort", "medium"]
    return _headless(repo_root, prompt, resume, extra)


def _headless(repo_root: Path, prompt: str, resume: str | None, extra: list[str]) -> list[str]:
    cmd = [
        "claude",
        "-p",
        prompt,
        *extra,
        "--permission-mode",
        "auto",
        "--permission-prompts",
        "none",
        "--settings",
        str(Path(repo_root) / ".claude" / "auto-mode.json"),
        "--output-format",
        "stream-json",
        "--verbose",
    ]
    if resume:
        cmd += ["--resume", resume]
    return cmd


def build_prompt(task: Task, previous_failure: str | None = None) -> str:
    label = f"PR {task.id}" if task.id[0].isdigit() else f"việc {task.id}"
    about = task.title or task.branch
    retry = ""
    if previous_failure:
        retry = f"""
Lần trước thất bại. Đây là phần cuối lần chạy trước, đọc kỹ và đừng lặp lại cùng lỗi:
<lan-truoc>
{previous_failure.strip()}
</lan-truoc>
"""
    return f"""Bạn là phiên điều phối headless cho {label} của RepoPulse ({about}).{retry}
Thư mục hiện tại là worktree riêng của việc này, nhánh `{task.branch}` rẽ từ `origin/develop`.
Đọc trước: CLAUDE.md, docs/plan.md (mục của {label}), .claude/rules/command-style.md.
Kiểm `gh pr list --head {task.branch} --state all`: nếu PR đã mở thì làm tiếp từ đó.

Quy trình:
1. Viết kế hoạch ngắn (5-10 dòng) có mục "test phải đỏ".
2. Giao agent `{task.agent}` (subagent, KHÔNG dùng isolation worktree, làm ngay trong thư mục này)
   viết test đỏ, commit riêng, rồi code tới khi xanh. Dán nguyên văn .claude/rules/command-style.md
   vào brief của agent. Gọi agent ở foreground: KHÔNG đặt `run_in_background`, chờ nó trả kết
   quả rồi mới đi tiếp. Phiên này thoát thì mọi việc chạy nền chết theo, nên không kết thúc lượt
   khi còn agent hay lệnh nào đang chạy.
3. Push, mở PR vào `develop` theo .github/pull_request_template.md (có ## RED và ## Giả định).
4. Chờ CI: `gh pr checks <số> --watch --interval 30` (foreground), lặp tới khi xong.
   CI đỏ thì sửa và push lại.
5. CI xanh cả 3 job thì DỪNG, trả `RESULT: ready <số PR>`. Không tự review, không merge:
   orchestrator chạy một phiên review độc lập với ngữ cảnh mới, rồi tự merge khi PASS.
   Nếu ở trên có kết luận review FAIL, sửa đúng các finding đó, push, chờ CI xanh rồi mới ready.

Luật: không hỏi người dùng; mọi giả định ghi vào mục ## Giả định của PR. Không sửa gì trong
.claude/ ngoài worktrees/. Không đọc .env, không chạy gh secret. Bị từ chối quyền thì đổi cách
theo command-style; vẫn không được thì dừng.

Dòng cuối cùng của câu trả lời PHẢI là một trong:
RESULT: ready <số PR>
RESULT: failed <lý do ngắn>
RESULT: blocked <việc cần chủ repo làm>
"""


def build_review_prompt(task: Task, pr: str) -> str:
    label = f"PR {task.id}" if task.id[0].isdigit() else f"việc {task.id}"
    return f"""Review độc lập PR #{pr} ({label} của RepoPulse, nhánh `{task.branch}` → `develop`).
Bạn KHÔNG phải phiên đã viết code này và không thấy ngữ cảnh của nó: chỉ tin vào code, test và
output bạn tự chạy. Thư mục hiện tại là worktree của PR, đang ở HEAD của nhánh.

Làm đúng theo hướng dẫn agent code-reviewer của bạn: chạy lại test, kiểm commit đầu chỉ có test,
thử mutation (hoàn tác, kiểm `git status --porcelain` sạch), kiểm luật dữ liệu, convention, bảo mật.
Đăng kết luận lên PR bằng `gh pr comment {pr} --body-file <file trong scratchpad>`.
Không merge, không push, không sửa code của PR. Không hỏi người dùng.

Dòng cuối cùng của câu trả lời PHẢI là một trong:
REVIEW: PASS
REVIEW: FAIL <các finding [chặn] tóm tắt trên một dòng>
"""


# ---------- worktrees ----------


@dataclass(frozen=True)
class Worktree:
    path: str
    branch: str | None
    dirty: bool


@dataclass(frozen=True)
class CleanAction:
    path: str
    branch: str | None
    action: str  # remove | keep
    reason: str


def parse_worktree_porcelain(text: str) -> list[tuple[str, str | None]]:
    out: list[tuple[str, str | None]] = []
    path: str | None = None
    branch: str | None = None
    for line in text.splitlines() + [""]:
        if line.startswith("worktree "):
            path, branch = line[len("worktree ") :].strip(), None
        elif line.startswith("branch "):
            branch = line[len("branch ") :].strip().removeprefix("refs/heads/")
        elif not line.strip() and path is not None:
            out.append((path, branch))
            path, branch = None, None
    return out


def clean_plan(
    worktrees: list[Worktree], pr_states: dict[str, str], active: set[str]
) -> list[CleanAction]:
    plan = []
    for w in worktrees:
        state = pr_states.get(w.branch or "", "NONE")
        if w.path in active:
            plan.append(CleanAction(w.path, w.branch, "keep", "đang chạy"))
        elif w.dirty:
            plan.append(CleanAction(w.path, w.branch, "keep", "còn thay đổi chưa commit"))
        elif state != "MERGED":
            plan.append(CleanAction(w.path, w.branch, "keep", f"PR {state}"))
        else:
            plan.append(CleanAction(w.path, w.branch, "remove", "PR đã merge"))
    return plan


# ---------- workspace trust ----------
# In -p mode Claude Code ignores a project's permissions.allow unless the folder is trusted in
# ~/.claude.json (projects["<path>"].hasTrustDialogAccepted). Every worktree is a new folder.


def trust_keys(path: str | Path) -> list[str]:
    p = str(path).replace("\\", "/").rstrip("/")
    if len(p) > 1 and p[1] == ":":
        return [p[0].lower() + p[1:], p[0].upper() + p[1:]]
    return [p]


def mark_trusted(config: dict, keys: Iterable[str]) -> tuple[dict, bool]:
    out = dict(config)
    projects = dict(out.get("projects") or {})
    changed = False
    for k in keys:
        entry = dict(projects.get(k) or {})
        if entry.get("hasTrustDialogAccepted") is not True:
            entry["hasTrustDialogAccepted"] = True
            projects[k] = entry
            changed = True
    out["projects"] = projects
    return out, changed


# ---------- stop file, harness gate, progress ----------


def should_stop(state_dir: Path) -> bool:
    return (Path(state_dir) / "STOP").exists()


APPROVAL_RE = re.compile(r"^Lam đã duyệt\b", re.MULTILINE)


def harness_file_approved(text: str) -> bool:
    """True only when a line *starts* with the marker (the template mentions it mid-sentence)."""
    return bool(APPROVAL_RE.search(text or ""))


def harness_auto_approved(log_lines: Iterable[str], task_id: str) -> bool:
    for line in log_lines:
        entry = parse_stream_line(line)
        # headless (-p, prompts none): a PermissionRequest is an automatic denial too
        events = ("PermissionDenied", "PermissionRequest")
        if not entry or entry.get("task") != task_id or entry.get("event") not in events:
            continue
        if not any(e in str(entry.get("input", "")) for e in EXPECTED_DENIALS):
            return False
    return True


def render_progress(
    tasks: list[Task],
    done: set[str],
    running: set[str],
    blocked: dict[str, str],
    skipped: set[str],
) -> str:
    rows = ["| Việc | Nhánh | Trạng thái | Ghi chú |", "|---|---|---|---|"]
    for t in tasks:
        if t.id in done:
            status, note = "merged", ""
        elif t.id in running:
            status, note = "running", ""
        elif t.id in skipped:
            status, note = "skipped", _need_reason_text(t)
        elif t.id in blocked:
            status, note = "blocked", blocked[t.id]
        else:
            status, note = "pending", ""
        rows.append(f"| {t.id} | `{t.branch}` | {status} | {note} |")
    return "\n".join(rows) + "\n"


def _need_reason_text(task: Task) -> str:
    return "thiếu: " + ", ".join(task.needs) if task.needs else ""
