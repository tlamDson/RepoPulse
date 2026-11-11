"""Headless orchestrator: runs RepoPulse PRs with `claude -p`, unattended.

    python scripts/orchestrate.py run --dry-run
    python scripts/orchestrate.py run --parallel 2 [--only ID] [--auto-approve-harness]
    python scripts/orchestrate.py clean [--dry-run]
    python scripts/orchestrate.py quota
    python scripts/orchestrate.py report

The decision logic lives in ``repopulse/orchestrator.py`` (unit tested). This file only does I/O:
git, gh, spawning ``claude`` and writing logs. It never reads secret values.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import threading
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from repopulse.orchestrator import (  # noqa: E402
    Decision,
    Gates,
    Task,
    Worktree,
    build_command,
    build_prompt,
    clean_plan,
    decide_after_exit,
    harness_auto_approved,
    harness_file_approved,
    load_tasks,
    merged_task_ids,
    next_ready,
    parse_stream_line,
    parse_worktree_porcelain,
    rate_limit_action,
    releases_due,
    render_progress,
    should_stop,
)

STATE = ROOT / ".claude" / "state"
LOGS = ROOT / "logs"
WORKTREES = ROOT / ".claude" / "worktrees"
PRS_TOML = ROOT / "scripts" / "prs.toml"
POLL_S = 15
REFRESH_S = 60
MAX_FAILURES = 2


# ---------- shell helpers ----------


def sh(args: list[str], cwd: Path = ROOT, check: bool = False, timeout: int = 600) -> str:
    r = subprocess.run(
        args, cwd=cwd, capture_output=True, text=True, encoding="utf-8", timeout=timeout
    )
    if check and r.returncode != 0:
        raise RuntimeError(f"{' '.join(args)} failed: {r.stderr.strip() or r.stdout.strip()}")
    return r.stdout


def log(msg: str) -> None:
    line = f"[{datetime.now():%Y-%m-%d %H:%M:%S}] {msg}"
    print(line, flush=True)
    LOGS.mkdir(exist_ok=True)
    with (LOGS / "orchestrator.log").open("a", encoding="utf-8") as fh:
        fh.write(line + "\n")


def gh_prs() -> list[dict]:
    out = sh(
        [
            "gh",
            "pr",
            "list",
            "--state",
            "all",
            "--limit",
            "300",
            "--json",
            "number,headRefName,state,baseRefName",
        ]
    )
    return json.loads(out or "[]")


def gh_secret_names() -> frozenset[str]:
    out = sh(["gh", "secret", "list", "--json", "name"])  # names only, never values
    try:
        return frozenset(s["name"] for s in json.loads(out or "[]"))
    except json.JSONDecodeError:
        return frozenset()


def permission_log_lines() -> list[str]:
    p = STATE / "permission-log.jsonl"
    return p.read_text("utf-8").splitlines() if p.exists() else []


def harness_approved(done: set[str], auto: bool) -> bool:
    text = sh(["git", "show", "origin/develop:docs/harness-check.md"])
    if harness_file_approved(text):
        return True
    return auto and "S0.4" in done and harness_auto_approved(permission_log_lines(), "S0.4")


# ---------- worktrees ----------


def worktree_path(task: Task) -> Path:
    return WORKTREES / f"pr-{task.id.replace('.', '-')}"


def ensure_worktree(task: Task) -> Path:
    path = worktree_path(task)
    if path.exists():
        return path
    WORKTREES.mkdir(parents=True, exist_ok=True)
    remote = sh(["git", "ls-remote", "--heads", "origin", task.branch]).strip()
    local = sh(["git", "branch", "--list", task.branch]).strip()
    if remote:
        sh(
            ["git", "worktree", "add", "-B", task.branch, str(path), f"origin/{task.branch}"],
            check=True,
        )
    elif local:
        sh(["git", "worktree", "add", str(path), task.branch], check=True)
    else:
        sh(["git", "worktree", "add", "-b", task.branch, str(path), "origin/develop"], check=True)
    return path


def list_worktrees() -> list[Worktree]:
    out = []
    root = str(WORKTREES).replace("\\", "/").lower()
    for path, branch in parse_worktree_porcelain(sh(["git", "worktree", "list", "--porcelain"])):
        if not path.replace("\\", "/").lower().startswith(root):
            continue
        dirty = bool(sh(["git", "-C", path, "status", "--porcelain"]).strip())
        out.append(Worktree(path, branch, dirty))
    return out


def clean(dry_run: bool, active: set[str] | None = None) -> None:
    states = {p["headRefName"]: p["state"] for p in gh_prs()}
    for a in clean_plan(list_worktrees(), states, active or set()):
        log(f"clean {a.action}: {a.path} ({a.branch}) — {a.reason}")
        if a.action == "remove" and not dry_run:
            sh(["git", "worktree", "remove", a.path])
            if a.branch:
                sh(["git", "branch", "-D", a.branch])
    if not dry_run:
        sh(["git", "worktree", "prune"])


# ---------- running claude ----------


@dataclass
class Run:
    task: Task
    proc: subprocess.Popen
    path: Path
    started: float
    session_id: str | None = None
    last_rate: dict | None = None
    result_text: str = ""
    warn: bool = False
    reader: threading.Thread | None = field(default=None, repr=False)


def _reader(run: Run, logfile: Path) -> None:
    with logfile.open("a", encoding="utf-8") as fh:
        for line in run.proc.stdout:  # type: ignore[union-attr]
            fh.write(line)
            fh.flush()
            ev = parse_stream_line(line)
            if not ev:
                continue
            run.session_id = ev.get("session_id") or run.session_id
            if ev.get("type") == "rate_limit_event":
                run.last_rate = ev
                if rate_limit_action(ev)[0] == "warn":
                    run.warn = True
            if ev.get("type") == "result":
                run.result_text = str(ev.get("result", ""))


def start(task: Task, resume: str | None = None) -> Run:
    path = ensure_worktree(task)
    prompt = "Tiếp tục đúng việc đang dở." if resume else build_prompt(task)
    cmd = build_command(ROOT, prompt, resume)
    cmd[0] = shutil.which("claude") or "claude"
    env = dict(os.environ)
    env.update(
        REPOPULSE_TASK=task.id,
        REPOPULSE_STATE_DIR=str(STATE),
        CLAUDE_CODE_RETRY_WATCHDOG="1",
        CLAUDE_CODE_RETRY_WATCHDOG_MAX_USAGE_LIMIT_WAIT_MS=str(6 * 3600 * 1000),
    )
    LOGS.mkdir(exist_ok=True)
    proc = subprocess.Popen(
        cmd,
        cwd=path,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        stdin=subprocess.DEVNULL,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    run = Run(task, proc, path, time.time())
    run.reader = threading.Thread(
        target=_reader, args=(run, LOGS / f"pr-{task.id}.jsonl"), daemon=True
    )
    run.reader.start()
    log(f"start {task.id} ({task.branch}) in {path}" + (f" resume={resume}" if resume else ""))
    return run


# ---------- release develop -> main ----------


def release(after: str) -> str | None:
    """Open and merge a develop -> main release PR. Returns an error string or None."""
    sh(["git", "fetch", "origin", "--prune"])
    ahead = sh(["git", "rev-list", "--count", "origin/main..origin/develop"]).strip()
    if ahead in ("", "0"):
        log(f"release after {after}: develop không đi trước main, bỏ qua")
        return None
    existing = json.loads(
        sh(
            [
                "gh",
                "pr",
                "list",
                "--base",
                "main",
                "--head",
                "develop",
                "--state",
                "open",
                "--json",
                "number",
            ]
        )
        or "[]"
    )
    if existing:
        number = str(existing[0]["number"])
    else:
        url = sh(
            [
                "gh",
                "pr",
                "create",
                "--base",
                "main",
                "--head",
                "develop",
                "--title",
                f"release: develop → main sau {after}",
                "--body",
                f"Release tự động sau khi merge {after}.\n\n"
                "🤖 Generated with [Claude Code](https://claude.com/claude-code)",
            ]
        ).strip()
        number = url.rstrip("/").split("/")[-1]
        if not number.isdigit():
            return f"không tạo được PR release: {url}"
    log(f"release after {after}: chờ CI của PR #{number}")
    checks = subprocess.run(
        ["gh", "pr", "checks", number, "--watch", "--interval", "30"],
        cwd=ROOT,
        capture_output=True,
        text=True,
        timeout=3600,
    )
    if checks.returncode != 0:
        return f"CI của PR release #{number} không xanh"
    r = subprocess.run(
        ["gh", "pr", "merge", number, "--merge"], cwd=ROOT, capture_output=True, text=True
    )
    if r.returncode != 0:
        return f"merge PR release #{number} thất bại: {r.stderr.strip()}"
    log(f"release after {after}: đã merge PR #{number}")
    return None


# ---------- state ----------


def load_state() -> dict:
    p = STATE / "orchestrator.json"
    return json.loads(p.read_text("utf-8")) if p.exists() else {"released": [], "failures": {}}


def save_state(state: dict) -> None:
    STATE.mkdir(parents=True, exist_ok=True)
    (STATE / "orchestrator.json").write_text(json.dumps(state, indent=2), "utf-8")


# ---------- main loop ----------


def run(args: argparse.Namespace) -> int:
    tasks = load_tasks(PRS_TOML)
    if args.only:
        keep = set(args.only.split(","))
        unknown = keep - {t.id for t in tasks}
        if unknown:
            print(f"không có task: {', '.join(sorted(unknown))}")
            return 2
    else:
        keep = None
    state = load_state()
    released = set(state["released"])
    failures: dict[str, int] = state.get("failures", {})
    runs: dict[str, Run] = {}
    waiting: dict[str, tuple[int, str | None]] = {}
    final: dict[str, Decision] = {}
    blocked: dict[str, str] = {}
    skipped: set[str] = set()
    run_started = datetime.now()
    release_failed: set[str] = set()
    hold_new = False
    stop_all = ""
    last_refresh = 0.0
    done: set[str] = set()
    gates = Gates(frozenset(), False)

    if not args.dry_run:
        clean(dry_run=False)

    while True:
        now = time.time()
        if now - last_refresh > REFRESH_S:
            sh(["git", "fetch", "origin", "--prune"])
            done = merged_task_ids(tasks, gh_prs())
            gates = Gates(gh_secret_names(), harness_approved(done, args.auto_approve_harness))
            last_refresh = now

        # finished runs
        for tid, r in list(runs.items()):
            if r.proc.poll() is None:
                if now - r.started > args.task_hours * 3600:
                    log(f"{tid}: quá {args.task_hours}h, dừng tiến trình")
                    r.proc.terminate()
                continue
            r.reader.join(timeout=10)
            d = decide_after_exit(
                r.proc.returncode, r.last_rate, r.result_text, int(now), args.max_wait_hours * 3600
            )
            del runs[tid]
            log(f"{tid}: {d.kind} {d.reason}")
            hold_new = hold_new or r.warn
            if d.kind == "resume_at":
                waiting[tid] = (d.at or int(now), r.session_id)
            elif d.kind == "stop":
                stop_all = d.reason
                final[tid] = d
            elif d.kind == "failed":
                failures[tid] = failures.get(tid, 0) + 1
                if failures[tid] >= MAX_FAILURES:
                    final[tid] = d
            elif d.kind == "blocked":
                final[tid] = d
            else:
                done.add(tid)
                final[tid] = d
                last_refresh = 0
            state["failures"] = failures
            save_state(state)
            if d.kind == "merged":
                clean(dry_run=False, active={str(x.path) for x in runs.values()})

        stopping = should_stop(STATE) or bool(stop_all)

        # releases happen only when nothing is running, so develop is stable
        due = [
            t
            for t in releases_due(tasks, done, released | release_failed)
            if keep is None or t in keep
        ]
        if due and not runs and not args.dry_run:
            err = release(due[0])
            if err:
                blocked[f"release-{due[0]}"] = err
                release_failed.add(due[0])
                stop_all = stop_all or err
            else:
                released.add(due[0])
                state["released"] = sorted(released)
                save_state(state)
            continue

        # resume after usage-limit reset
        for tid, (at, sid) in list(waiting.items()):
            if time.time() >= at and len(runs) < args.parallel and not stopping:
                del waiting[tid]
                task = next(t for t in tasks if t.id == tid)
                runs[tid] = start(task, resume=sid)

        # new work
        exclude = set(final) | set(waiting)
        ready, blk, skp = next_ready(tasks, done | exclude, set(runs), gates, args.parallel)
        blocked.update(blk)
        skipped |= {t.id for t in skp}
        if keep is not None:
            ready = [t for t in ready if t.id in keep]
        if args.dry_run:
            print(render_progress(tasks, done, set(runs), blocked, skipped))
            for t in ready:
                print(f"SẼ CHẠY {t.id}: cwd={worktree_path(t)}")
                print("  " + " ".join(build_command(ROOT, "<prompt>", None)))
            if not ready:
                print("Không có việc nào sẵn sàng. Bị chặn: " + json.dumps(blk, ensure_ascii=False))
            return 0
        if not stopping and not hold_new:
            for t in ready:
                runs[t.id] = start(t)

        if not runs and not waiting:
            break
        if stopping and not runs:
            break
        time.sleep(POLL_S)

    write_report(tasks, done, blocked, skipped, final, run_started, stop_all, hold_new)
    _chime()
    return 0


def write_report(tasks, done, blocked, skipped, final, started, stop_all, hold_new) -> Path:
    LOGS.mkdir(exist_ok=True)
    path = LOGS / f"run-{started:%Y-%m-%d-%H%M}.md"
    lines = [f"# Orchestrator run {started:%Y-%m-%d %H:%M}", ""]
    todo = [f"- [ ] {k}: {v}" for k, v in blocked.items()]
    todo += [f"- [ ] {k}: {d.reason}" for k, d in final.items() if d.kind in ("blocked", "stop")]
    if stop_all:
        todo.append(f"- [ ] Dừng sớm: {stop_all}")
    if hold_new:
        todo.append("- [ ] Quota trên 90%: không mở việc mới, chạy lại sau khi reset")
    lines += ["## Việc cần Lam", *(todo or ["- Không có"]), ""]
    lines += ["## Kết quả từng việc", ""]
    lines += [f"- {k}: {d.kind} {d.reason}" for k, d in final.items()] or ["- (không có)"]
    lines += ["", "## Tiến độ", "", render_progress(tasks, done, set(), blocked, skipped)]
    since = started.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")  # hook writes UTC "Z" stamps
    denials = [
        ln
        for ln in permission_log_lines()
        if (e := parse_stream_line(ln)) and e.get("ts", "") >= since
    ]
    lines += ["## Lệnh bị hỏi hoặc bị từ chối", "", f"Tổng: {len(denials)}", ""]
    lines += [
        f"- `{json.loads(d).get('task')}` {json.loads(d).get('event')}: "
        f"`{json.loads(d).get('input')}` — {json.loads(d).get('reason')}"
        for d in denials
    ]
    path.write_text("\n".join(lines) + "\n", "utf-8")
    log(f"report: {path}")
    return path


def _chime() -> None:
    if os.name == "nt":
        subprocess.run(
            [
                "powershell",
                "-NoProfile",
                "-Command",
                "(New-Object Media.SoundPlayer 'C:\\Windows\\Media\\chimes.wav').PlaySync()",
            ],
            capture_output=True,
        )


# ---------- other commands ----------


def quota(_: argparse.Namespace) -> int:
    latest: tuple[float, dict] | None = None
    for f in LOGS.glob("pr-*.jsonl"):
        for line in f.read_text("utf-8", errors="replace").splitlines():
            ev = parse_stream_line(line)
            if ev and ev.get("type") == "rate_limit_event":
                latest = (
                    (f.stat().st_mtime, ev)
                    if not latest or f.stat().st_mtime >= latest[0]
                    else latest
                )
    if not latest:
        print("Chưa có rate_limit_event nào trong logs/.")
        return 0
    info = latest[1].get("rate_limit_info", {})
    reset = info.get("resetsAt")
    resets = f"{datetime.fromtimestamp(reset):%Y-%m-%d %H:%M}" if reset else "?"
    print(
        f"status={info.get('status')} type={info.get('rateLimitType')} "
        f"utilization={info.get('utilization')} resets={resets}"
    )
    return 0


def report(_: argparse.Namespace) -> int:
    tasks = load_tasks(PRS_TOML)
    done = merged_task_ids(tasks, gh_prs())
    gates = Gates(gh_secret_names(), harness_approved(done, auto=False))
    _, blocked, skipped = next_ready(tasks, done, set(), gates, slots=0)
    print(render_progress(tasks, done, set(), blocked, {t.id for t in skipped}))
    return 0


def main(argv: list[str] | None = None) -> int:
    for stream in (sys.stdout, sys.stderr):  # Windows consoles default to cp1252
        if hasattr(stream, "reconfigure"):
            stream.reconfigure(encoding="utf-8", errors="replace")
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("run")
    r.add_argument("--dry-run", action="store_true")
    r.add_argument("--parallel", type=int, default=2)
    r.add_argument("--only", help="comma-separated task ids")
    r.add_argument(
        "--auto-approve-harness",
        action="store_true",
        help="coi harness đã duyệt nếu S0.4 merge và log không có lệnh bị từ chối lạ",
    )
    r.add_argument("--task-hours", type=float, default=4)
    r.add_argument("--max-wait-hours", type=float, default=30)
    r.set_defaults(fn=run)
    c = sub.add_parser("clean")
    c.add_argument("--dry-run", action="store_true")
    c.set_defaults(fn=lambda a: clean(a.dry_run) or 0)
    sub.add_parser("quota").set_defaults(fn=quota)
    sub.add_parser("report").set_defaults(fn=report)
    args = p.parse_args(argv)
    return args.fn(args)


if __name__ == "__main__":
    sys.exit(main())
