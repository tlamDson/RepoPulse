"""Headless orchestrator: runs RepoPulse PRs with `claude -p`, unattended.

    python scripts/orchestrate.py run --dry-run
    python scripts/orchestrate.py run [--parallel 5] [--only ID] [--auto-approve-harness]
    python scripts/orchestrate.py clean [--dry-run]
    python scripts/orchestrate.py quota
    python scripts/orchestrate.py report
    python scripts/orchestrate.py daily [--date YYYY-MM-DD] [--no-send] [--force]

The decision logic lives in ``repopulse/orchestrator.py`` and ``repopulse/daily_report.py``
(unit tested). This file only does I/O: git, gh, spawning ``claude``, writing logs, sending the
daily e-mail. It never reads repo secret values; the SMTP app password for the daily e-mail is
taken from the environment (REPOPULSE_SMTP_*) and only handed to smtplib, never printed.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import smtplib
import subprocess
import sys
import threading
import time
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from repopulse.daily_report import (  # noqa: E402
    append_metrics,
    build_daily_report,
    build_email,
    daily_due,
    denials_between,
    metrics_row,
    needs_lam,
    needs_lam_section,
    read_metrics,
    rows_between,
    run_metrics,
    send_email,
    smtp_config,
    utc_window,
)
from repopulse.orchestrator import (  # noqa: E402
    Decision,
    Gates,
    Task,
    Worktree,
    build_command,
    build_prompt,
    build_review_command,
    build_review_prompt,
    clean_plan,
    decide_after_exit,
    decide_after_review,
    develop_ci_blocks,
    develop_ci_state,
    failure_context,
    harness_auto_approved,
    harness_file_approved,
    is_stalled,
    load_tasks,
    mark_trusted,
    merge_failure_reason,
    merge_ready,
    merged_task_ids,
    next_ready,
    over_hold_at,
    parse_hold_at,
    parse_stream_line,
    parse_worktree_porcelain,
    quota_peak,
    releases_due,
    render_progress,
    required_checks_state,
    review_next,
    scheduling_sets,
    should_exit,
    should_stop,
    trust_keys,
)

STATE = ROOT / ".claude" / "state"
LOGS = ROOT / "logs"
WORKTREES = ROOT / ".claude" / "worktrees"
PRS_TOML = ROOT / "scripts" / "prs.toml"
POLL_S = 15
REFRESH_S = 60
MAX_FAILURES = 2
RELEASE_WAIT_S = 2 * 3600  # release PRs run every workflow; heavy ones take long
METRICS = LOGS / "orchestrator-metrics.csv"


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


def gh_develop_ci() -> str:
    """green | red | pending | unknown for the newest CI run on a push to develop."""
    out = sh(
        [
            "gh",
            "run",
            "list",
            "--workflow",
            "ci.yml",
            "--branch",
            "develop",
            "--event",
            "push",
            "--limit",
            "1",
            "--json",
            "status,conclusion,headSha",
        ]
    )
    try:
        return develop_ci_state(json.loads(out or "[]"))
    except json.JSONDecodeError:
        return "unknown"


def permission_log_lines() -> list[str]:
    p = STATE / "permission-log.jsonl"
    return p.read_text("utf-8").splitlines() if p.exists() else []


def harness_approved(done: set[str], auto: bool) -> bool:
    text = sh(["git", "show", "origin/develop:docs/harness-check.md"])
    if harness_file_approved(text):
        return True
    return auto and "S0.4" in done and harness_auto_approved(permission_log_lines(), "S0.4")


# ---------- workspace trust (~/.claude.json) ----------

CLAUDE_JSON = Path.home() / ".claude.json"


def ensure_trusted(paths: list[Path]) -> None:
    """Pre-accept the trust dialog for the repo and every worktree path, so -p sessions keep
    the project's permissions.allow rules. Writes atomically; keeps one backup."""
    keys = [k for p in paths for k in trust_keys(p)]
    try:
        cfg = json.loads(CLAUDE_JSON.read_text("utf-8")) if CLAUDE_JSON.exists() else {}
    except (OSError, json.JSONDecodeError) as exc:
        log(f"trust: cannot read {CLAUDE_JSON} ({exc}); skipping")
        return
    new, changed = mark_trusted(cfg, keys)
    if not changed:
        return
    backup = CLAUDE_JSON.with_name(".claude.json.repopulse-backup")
    if not backup.exists() and CLAUDE_JSON.exists():
        shutil.copy2(CLAUDE_JSON, backup)
    tmp = CLAUDE_JSON.with_name(".claude.json.repopulse-tmp")
    tmp.write_text(json.dumps(new, indent=2, ensure_ascii=False), "utf-8")
    os.replace(tmp, CLAUDE_JSON)
    log(f"trust: marked {len(paths)} folders as trusted")


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
    peak: float = 0.0  # highest quota utilization (0..1) this session reported, for --hold-at
    stalled: bool = False
    last_output: float = 0.0
    log_offset: int = 0  # where this attempt starts in logs/pr-<id>.jsonl
    review_pr: str | None = None  # set = independent review session of this PR number
    review_sha: str = ""  # HEAD the review session saw; the merge must be exactly this commit
    reader: threading.Thread | None = field(default=None, repr=False)


def task_log(task: Task) -> Path:
    return LOGS / f"pr-{task.id}.jsonl"


def attempt_lines(run: Run) -> list[str]:
    """Stream lines written by this attempt only (the log file is appended across attempts)."""
    p = task_log(run.task)
    if not p.exists():
        return []
    with p.open("rb") as fh:
        fh.seek(run.log_offset)
        return fh.read().decode("utf-8", errors="replace").splitlines()


def _reader(run: Run, logfile: Path) -> None:
    with logfile.open("a", encoding="utf-8") as fh:
        for line in run.proc.stdout:  # type: ignore[union-attr]
            fh.write(line)
            fh.flush()
            run.last_output = time.time()
            ev = parse_stream_line(line)
            if not ev:
                continue
            run.session_id = ev.get("session_id") or run.session_id
            if ev.get("type") == "rate_limit_event":
                run.last_rate = ev
                run.peak = max(run.peak, quota_peak(ev))
            if ev.get("type") == "result":
                run.result_text = str(ev.get("result", ""))


def start(
    task: Task,
    resume: str | None = None,
    previous_failure: str | None = None,
    review_pr: str | None = None,
) -> Run:
    """Start the PR (build) session, or with ``review_pr`` a fresh independent review session."""
    path = ensure_worktree(task)
    ensure_trusted([path])  # re-check: a running claude may have rewritten ~/.claude.json
    if resume:
        prompt = "Continue exactly the work that was left unfinished."
    elif review_pr:
        prompt = build_review_prompt(task, review_pr)
    else:
        prompt = build_prompt(task, previous_failure=previous_failure)
    builder = build_review_command if review_pr else build_command
    cmd = builder(ROOT, prompt, resume)
    cmd[0] = shutil.which("claude") or "claude"
    env = dict(os.environ)
    env.update(
        REPOPULSE_TASK=task.id,
        REPOPULSE_STATE_DIR=str(STATE),
        CLAUDE_CODE_RETRY_WATCHDOG="1",
        CLAUDE_CODE_RETRY_WATCHDOG_MAX_USAGE_LIMIT_WAIT_MS=str(6 * 3600 * 1000),
    )
    LOGS.mkdir(exist_ok=True)
    logfile = task_log(task)
    offset = logfile.stat().st_size if logfile.exists() else 0
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
    now = time.time()
    run = Run(task, proc, path, now, last_output=now, log_offset=offset, review_pr=review_pr)
    if review_pr:
        run.review_sha = sh(["git", "-C", str(path), "rev-parse", "HEAD"]).strip()
    run.reader = threading.Thread(target=_reader, args=(run, logfile), daemon=True)
    run.reader.start()
    kind = f"review PR #{review_pr}" if review_pr else "build"
    note = f" resume={resume}" if resume else (" (retry with context)" if previous_failure else "")
    log(f"start {task.id} [{kind}] ({task.branch}) in {path}{note}")
    return run


def merge_pr(pr: str, head_sha: str) -> str | None:
    """Merge after an independent PASS, only if the required checks pass and the PR head is
    still the commit that was reviewed. Returns an error."""
    if not head_sha:
        return f"unknown reviewed commit of PR #{pr}"
    out = sh(["gh", "pr", "checks", pr, "--json", "name,bucket"])
    try:
        checks = json.loads(out or "[]")
    except json.JSONDecodeError:
        return f"cannot read the checks of PR #{pr}"
    ok, why = merge_ready(checks)
    if not ok:
        return f"CI is not green ({why})"
    # --delete-branch also tries the local branch, which fails while the worktree holds it;
    # the merge itself is judged by the PR state below, the worktree is removed by clean().
    sh(["gh", "pr", "merge", pr, "--squash", "--delete-branch", "--match-head-commit", head_sha])
    state = sh(["gh", "pr", "view", pr, "--json", "state"])
    if '"MERGED"' not in state:
        out = sh(["gh", "pr", "view", pr, "--json", "mergeable,mergeStateStatus"])
        try:
            view = json.loads(out or "{}")
        except json.JSONDecodeError:
            view = {}
        return merge_failure_reason(pr, view)
    return None


# ---------- release develop -> main ----------


def release(after: str) -> str | None:
    """Open and merge a develop -> main release PR. Returns an error string or None."""
    sh(["git", "fetch", "origin", "--prune"])
    ahead = sh(["git", "rev-list", "--count", "origin/main..origin/develop"]).strip()
    if ahead in ("", "0"):
        log(f"release after {after}: develop is not ahead of main, skipping")
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
                f"release: develop → main after {after}",
                "--body",
                f"Automatic release after merging {after}.\n\n"
                "🤖 Generated with [Claude Code](https://claude.com/claude-code)",
            ]
        ).strip()
        number = url.rstrip("/").split("/")[-1]
        if not number.isdigit():
            return f"could not create the release PR: {url}"
    log(f"release after {after}: waiting for CI of PR #{number}")
    deadline = time.time() + RELEASE_WAIT_S
    state = "pending"
    while time.time() < deadline:
        try:
            checks = json.loads(sh(["gh", "pr", "checks", number, "--json", "name,bucket"]) or "[]")
        except json.JSONDecodeError:
            checks = []
        state = required_checks_state(checks)
        if state != "pending":
            break
        time.sleep(30)
    if state != "pass":
        return f"required CI of release PR #{number}: {state}"
    r = subprocess.run(
        ["gh", "pr", "merge", number, "--merge"], cwd=ROOT, capture_output=True, text=True
    )
    if r.returncode != 0:
        return f"merging release PR #{number} failed: {r.stderr.strip()}"
    log(f"release after {after}: merged PR #{number}")
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
            print(f"unknown task: {', '.join(sorted(unknown))}")
            return 2
    else:
        keep = None
    state = load_state()
    released = set(state["released"])
    failures: dict[str, int] = state.get("failures", {})
    last_failure: dict[str, str] = state.setdefault("last_failure", {})
    pending_review: dict[str, str] = state.setdefault("pending_review", {})  # task -> PR number
    review_fails: dict[str, int] = state.setdefault("review_fails", {})
    develop_red = False
    daily_tried = ""  # local date of this process's last daily-report attempt
    runs: dict[str, Run] = {}
    waiting: dict[str, tuple[int, str | None, str | None]] = {}  # at, session, review PR
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
        ensure_trusted([ROOT, *(worktree_path(t) for t in tasks)])
        clean(dry_run=False)

    while True:
        now = time.time()
        if now - last_refresh > REFRESH_S:
            sh(["git", "fetch", "origin", "--prune"])
            done = merged_task_ids(tasks, gh_prs())
            gates = Gates(gh_secret_names(), harness_approved(done, args.auto_approve_harness))
            red = develop_ci_blocks(gh_develop_ci(), develop_red)
            if red != develop_red:
                log(
                    "CI on develop is red: not opening new tasks, not releasing"
                    if red
                    else "CI on develop is green again: opening tasks again"
                )
            develop_red = red
            if develop_red:
                blocked["develop-ci"] = "CI on develop is red after the latest merge: needs a fix"
            else:
                blocked.pop("develop-ci", None)
            last_refresh = now

        # finished runs
        for tid, r in list(runs.items()):
            if r.proc.poll() is None:
                if now - r.started > args.task_hours * 3600:
                    log(f"{tid}: over {args.task_hours}h, stopping the process")
                    r.proc.terminate()
                elif not r.stalled and is_stalled(
                    r.last_output, now, args.stall_minutes * 60, r.last_rate
                ):
                    log(
                        f"{tid}: no output for {args.stall_minutes:g} minutes, "
                        "treating as hung, stopping"
                    )
                    r.stalled = True
                    r.proc.terminate()
                continue
            r.reader.join(timeout=10)
            decide = decide_after_review if r.review_pr else decide_after_exit
            d = decide(
                r.proc.returncode,
                r.last_rate,
                r.result_text,
                int(now),
                args.max_wait_hours * 3600,
                stalled=f"hung: no output for {args.stall_minutes:g} minutes"
                if r.stalled
                else None,
            )
            del runs[tid]
            phase = f"review #{r.review_pr}" if r.review_pr else "build"
            log(f"{tid} [{phase}]: {d.kind} {d.reason}")
            hold_new = hold_new or over_hold_at(r.peak, args.hold_at)
            if d.kind == "pass":
                # an independent session said PASS: the orchestrator merges, not the author
                err = merge_pr(r.review_pr or "", r.review_sha)
                pending_review.pop(tid, None)
                if err:
                    log(f"{tid}: review PASS but could not merge: {err}")
                    last_refresh = 0  # re-read gh: if the merge did land, the task counts as done
                    failures[tid] = failures.get(tid, 0) + 1
                    last_failure[tid] = f"Review PASS but the orchestrator could not merge: {err}"
                    if failures[tid] >= MAX_FAILURES:
                        final[tid] = Decision("failed", err)
                else:
                    d = Decision("merged", r.review_pr or "")
            elif d.kind == "fail":
                action = review_next(d, review_fails.get(tid, 0))
                review_fails[tid] = review_fails.get(tid, 0) + 1
                pending_review.pop(tid, None)
                if action == "fix":
                    last_failure[tid] = (
                        f"Independent review FAIL (round {review_fails[tid]}) PR #{r.review_pr}: "
                        f"{d.reason}\nDetails are in the review comment on the PR.\n"
                        f"{failure_context(attempt_lines(r))}"
                    )
                else:
                    final[tid] = Decision(
                        "failed", f"review FAIL after {review_fails[tid]} rounds: {d.reason}"
                    )
            elif d.kind == "ready":
                pending_review[tid] = d.reason  # next start of this task is its review
            elif d.kind == "resume_at":
                waiting[tid] = (d.at or int(now), r.session_id, r.review_pr)
            elif d.kind == "stop":
                stop_all = d.reason
                final[tid] = d
            elif d.kind == "failed":
                # a crashed/stalled review keeps pending_review, so the retry is a review again
                failures[tid] = failures.get(tid, 0) + 1
                last_failure[tid] = f"Reason: {d.reason}\n{failure_context(attempt_lines(r))}"
                if failures[tid] >= MAX_FAILURES:
                    final[tid] = d
            elif d.kind == "blocked":
                final[tid] = d
            if d.kind == "merged":
                done.add(tid)
                final[tid] = d
                last_failure.pop(tid, None)
                review_fails.pop(tid, None)
                last_refresh = 0
            state["failures"] = failures
            save_state(state)
            record_metrics(r, d)
            if d.kind == "merged":
                clean(dry_run=False, active={str(x.path) for x in runs.values()})

        today = date.today().isoformat()
        if (
            not args.dry_run
            and daily_tried != today  # one attempt per day per process, sent or not
            and daily_due(datetime.now(), daily_sent(), args.daily_hour)
        ):
            daily_tried = today
            try:
                send_daily(date.today(), *rolling_window(), send=True)
            except Exception as exc:  # noqa: BLE001 - the report must never stop running sessions
                log(f"daily: error while writing the report ({type(exc).__name__}: {exc})")

        stopping = should_stop(STATE) or bool(stop_all)
        if not hold_new and any(over_hold_at(r.peak, args.hold_at) for r in runs.values()):
            hold_new = True
            log(f"quota above {args.hold_at}% (5 hours or 7 days): not opening new tasks")

        # releases happen only when nothing is running, so develop is stable
        due = [
            t
            for t in releases_due(tasks, done, released | release_failed)
            if keep is None or t in keep
        ]
        if due and not runs and not args.dry_run and not develop_red:
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
        for tid, (at, sid, review_pr) in list(waiting.items()):
            if time.time() >= at and len(runs) < args.parallel and not stopping:
                del waiting[tid]
                task = next(t for t in tasks if t.id == tid)
                runs[tid] = start(task, resume=sid, review_pr=review_pr)

        # new work
        sched_done, held = scheduling_sets(done, final, waiting)
        ready, blk, skp = next_ready(tasks, sched_done, set(runs), gates, args.parallel, held=held)
        blocked.update(blk)
        skipped |= {t.id for t in skp}
        if keep is not None:
            ready = [t for t in ready if t.id in keep]
        if args.dry_run:
            print(render_progress(tasks, done, set(runs), blocked, skipped))
            if develop_red:
                print("CI on develop is red: a real run will NOT open any of the tasks below.")
            for t in ready:
                pr = pending_review.get(t.id)
                builder = build_review_command if pr else build_command
                print(
                    f"WOULD RUN {t.id}"
                    + (f" [review #{pr}]" if pr else "")
                    + f": cwd={worktree_path(t)}"
                )
                print("  " + " ".join(builder(ROOT, "<prompt>", None)))
            if not ready:
                print("No task is ready. Blocked: " + json.dumps(blk, ensure_ascii=False))
            return 0
        if not stopping and not hold_new and not develop_red:
            for t in ready:
                runs[t.id] = start(
                    t,
                    previous_failure=last_failure.get(t.id),
                    review_pr=pending_review.get(t.id),
                )

        if should_exit(
            len(runs), len(waiting), refresh_pending=last_refresh == 0, stopping=stopping
        ):
            break
        time.sleep(POLL_S)

    held_at = args.hold_at if hold_new else None
    write_report(tasks, done, blocked, skipped, final, run_started, stop_all, held_at)
    _chime()
    return 0


def write_report(tasks, done, blocked, skipped, final, started, stop_all, held_at) -> Path:
    """``held_at`` = the --hold-at percent when the run stopped opening work on it, else None."""
    LOGS.mkdir(exist_ok=True)
    path = LOGS / f"run-{started:%Y-%m-%d-%H%M}.md"
    lines = [f"# Orchestrator run {started:%Y-%m-%d %H:%M}", ""]
    todo = [f"- [ ] {k}: {v}" for k, v in blocked.items()]
    todo += [f"- [ ] {k}: {d.reason}" for k, d in final.items() if d.kind in ("blocked", "stop")]
    if stop_all:
        todo.append(f"- [ ] Stopped early: {stop_all}")
    if held_at is not None:
        todo.append(f"- [ ] Quota above {held_at}%: no new tasks opened, rerun after the reset")
    lines += needs_lam_section(todo)
    lines += ["## Results per task", ""]
    lines += [f"- {k}: {d.kind} {d.reason}" for k, d in final.items()] or ["- (none)"]
    lines += ["", "## Progress", "", render_progress(tasks, done, set(), blocked, skipped)]
    since = started.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")  # hook writes UTC "Z" stamps
    denials = [
        ln
        for ln in permission_log_lines()
        if (e := parse_stream_line(ln)) and e.get("ts", "") >= since
    ]
    lines += ["## Commands asked about or denied", "", f"Total: {len(denials)}", ""]
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
        print("No rate_limit_event in logs/ yet.")
        return 0
    info = latest[1].get("rate_limit_info", {})
    reset = info.get("resetsAt")
    resets = f"{datetime.fromtimestamp(reset):%Y-%m-%d %H:%M}" if reset else "?"
    print(
        f"status={info.get('status')} type={info.get('rateLimitType')} "
        f"utilization={info.get('utilization')} resets={resets}"
    )
    return 0


def record_metrics(r: Run, d: Decision) -> None:
    """One row per finished session in logs/orchestrator-metrics.csv; never breaks the loop."""
    try:
        row = metrics_row(
            f"{datetime.now():%Y-%m-%d %H:%M:%S}",
            r.task.id,
            "review" if r.review_pr else "build",
            d.kind,
            d.reason,
            run_metrics(attempt_lines(r)),
        )
        append_metrics(METRICS, row)
    except Exception as exc:  # noqa: BLE001 - metrics must never stop running sessions
        log(f"metrics: could not write ({type(exc).__name__}: {exc})")


# ---------- daily report ----------

DAILY_SENT = STATE / "daily-sent"  # own file: `daily` may run while `run` holds orchestrator.json


def daily_sent() -> str | None:
    return DAILY_SENT.read_text("utf-8").strip() if DAILY_SENT.exists() else None


def calendar_window(day: date) -> tuple[datetime, datetime]:
    """Aware local midnight of ``day`` and of the next day (each converted on its own: DST)."""
    nxt = day + timedelta(days=1)
    start = datetime(day.year, day.month, day.day).astimezone()
    return start, datetime(nxt.year, nxt.month, nxt.day).astimezone()


def rolling_window() -> tuple[datetime, datetime]:
    """The 24 hours up to now, so sessions ending after the send hour are in tomorrow's mail."""
    end = datetime.now().astimezone()
    return end - timedelta(hours=24), end


def send_daily(day: date, start: datetime, end: datetime, send: bool) -> bool:
    """Write logs/daily-<day>.md for [start, end); e-mail it if ``send`` and SMTP is set.

    Returns True only when the mail went out; only then is the day marked as sent, so setting
    the SMTP variables later the same evening still gets that day's mail.
    """
    fmt = "%Y-%m-%d %H:%M:%S"
    rows = rows_between(read_metrics(METRICS), start.strftime(fmt), end.strftime(fmt))
    denials = denials_between(permission_log_lines(), *utc_window(start, end))
    reports = sorted(LOGS.glob("run-*.md"), key=lambda p: p.stat().st_mtime)
    todo = needs_lam(reports[-1].read_text("utf-8")) if reports else []
    window = f"{start:%Y-%m-%d %H:%M} → {end:%Y-%m-%d %H:%M}"
    subject, body = build_daily_report(day, rows, denials, todo, window=window)
    LOGS.mkdir(exist_ok=True)
    path = LOGS / f"daily-{day.isoformat()}.md"
    path.write_text(body, "utf-8")
    log(f"daily: {path}")
    if not send:
        return False
    cfg = smtp_config(dict(os.environ))
    if cfg is None:
        log("daily: REPOPULSE_SMTP_USER / REPOPULSE_SMTP_APP_PASSWORD not set, file only")
        return False
    try:
        send_email(build_email(subject, body, cfg), cfg)
    except (OSError, smtplib.SMTPException) as exc:
        log(f"daily: sending mail failed ({type(exc).__name__}); rerun with `daily --force`")
        return False
    log(f"daily: mail sent to {cfg['to']}")
    STATE.mkdir(parents=True, exist_ok=True)
    DAILY_SENT.write_text(day.isoformat(), "utf-8")
    return True


def daily(args: argparse.Namespace) -> int:
    """``--date`` = that calendar day; without it, the 24 hours up to now (as the run loop)."""
    day = date.fromisoformat(args.date) if args.date else date.today()
    send = not args.no_send
    if send and not args.force and daily_sent() == day.isoformat():
        print(f"The report for {day} was already sent; add --force to send again.")
        return 0
    start, end = calendar_window(day) if args.date else rolling_window()
    send_daily(day, start, end, send=send)
    print(LOGS / f"daily-{day.isoformat()}.md")
    return 0


def report(_: argparse.Namespace) -> int:
    tasks = load_tasks(PRS_TOML)
    done = merged_task_ids(tasks, gh_prs())
    gates = Gates(gh_secret_names(), harness_approved(done, auto=False))
    _, blocked, skipped = next_ready(tasks, done, set(), gates, slots=0)
    print(render_progress(tasks, done, set(), blocked, {t.id for t in skipped}))
    return 0


def _hold_at_arg(text: str) -> int:
    try:
        return parse_hold_at(text)
    except ValueError as exc:  # argparse would hide the reason behind "invalid ... value"
        raise argparse.ArgumentTypeError(str(exc)) from None


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("run")
    r.add_argument("--dry-run", action="store_true")
    r.add_argument(
        "--parallel",
        type=int,
        default=5,
        help="maximum sessions; in practice limited by the DAG width, quota and develop CI",
    )
    r.add_argument(
        "--stall-minutes",
        type=float,
        default=30,
        help="after this many minutes without stream output (except while waiting for a quota "
        "reset) the session counts as hung: stop it, retry next time with context",
    )
    r.add_argument(
        "--daily-hour",
        type=int,
        default=22,
        help="local hour to e-mail the daily report (once a day, while running)",
    )
    r.add_argument("--only", help="comma-separated task ids")
    r.add_argument(
        "--auto-approve-harness",
        action="store_true",
        help="treat the harness as approved if S0.4 merged and the log has no unexpected denial",
    )
    r.add_argument("--task-hours", type=float, default=4)
    r.add_argument(
        "--max-wait-hours",
        type=float,
        default=6,
        help="how long to wait for a limit reset before continuing; default 6h = wait out the "
        "5-hour limit, stop on the weekly limit",
    )
    hold = r.add_mutually_exclusive_group()
    hold.add_argument(
        "--hold-at",
        type=_hold_at_arg,
        metavar="PERCENT",
        help="do not open new tasks once the 5-hour or 7-day quota reaches or exceeds this "
        "threshold (whole number 1-100; default: run to the end)",
    )
    hold.add_argument(
        "--hold-at-90",
        dest="hold_at",
        action="store_const",
        const=90,
        help="same as --hold-at 90",
    )
    r.set_defaults(fn=run)
    c = sub.add_parser("clean")
    c.add_argument("--dry-run", action="store_true")
    c.set_defaults(fn=lambda a: clean(a.dry_run) or 0)
    sub.add_parser("quota").set_defaults(fn=quota)
    sub.add_parser("report").set_defaults(fn=report)
    dl = sub.add_parser("daily", help="write logs/daily-<date>.md and e-mail the daily report")
    dl.add_argument("--date", help="YYYY-MM-DD, default today (local time)")
    dl.add_argument("--no-send", action="store_true", help="write the file only, send no mail")
    dl.add_argument("--force", action="store_true", help="send again even if already sent that day")
    dl.set_defaults(fn=daily)
    return p


def main(argv: list[str] | None = None) -> int:
    for stream in (sys.stdout, sys.stderr):  # Windows consoles default to cp1252
        if hasattr(stream, "reconfigure"):
            stream.reconfigure(encoding="utf-8", errors="replace")
    args = build_parser().parse_args(argv)
    return args.fn(args)


if __name__ == "__main__":
    sys.exit(main())
