"""Orchestrator metrics, rule proposals and the daily e-mail report.

Pure functions plus two thin I/O helpers (CSV append, SMTP send) so everything is unit tested
without the network. ``scripts/orchestrate.py`` calls these.

The numbers here are operational (turns, minutes, USD per headless session) and live in
``logs/orchestrator-metrics.csv``. They are not benchmark measurements: those stay in
``runs.csv`` only, and nothing from this file goes into README or PR descriptions.
"""

from __future__ import annotations

import csv
import json
import smtplib
from collections.abc import Callable, Iterable
from datetime import UTC, date, datetime
from email.message import EmailMessage
from pathlib import Path

METRIC_FIELDS = [
    "ts",
    "task",
    "phase",
    "outcome",
    "reason",
    "turns",
    "duration_s",
    "cost_usd",
    "denials",
    "subagents",
]
DENIAL_EVENTS = ("PermissionDenied", "PermissionRequest")  # headless: a request is a denial
FAILED_OUTCOMES = ("failed", "fail")

# ---------- metrics (#4) ----------


def _json(line: str) -> dict | None:
    try:
        obj = json.loads(line)
    except (json.JSONDecodeError, TypeError):
        return None
    return obj if isinstance(obj, dict) else None


def run_metrics(log_lines: Iterable[str]) -> dict:
    """Turns, wall time, cost, denials and subagents from a session's final ``result`` event.

    A session killed before its ``result`` (stall, crash) gives empty values, not zeros.
    """
    result = None
    for line in log_lines:
        ev = _json(line)
        if ev and ev.get("type") == "result":
            result = ev
    if result is None:
        return {k: "" for k in ("turns", "duration_s", "cost_usd", "denials", "subagents")}
    return {
        "turns": int(result.get("num_turns") or 0),
        "duration_s": int((result.get("duration_ms") or 0) / 1000),
        "cost_usd": round(float(result.get("total_cost_usd") or 0), 4),
        "denials": len(result.get("permission_denials") or []),
        "subagents": int((result.get("subagent_stats") or {}).get("spawned") or 0),
    }


def metrics_row(ts: str, task: str, phase: str, outcome: str, reason: str, m: dict) -> dict:
    row = {"ts": ts, "task": task, "phase": phase, "outcome": outcome, "reason": reason, **m}
    return {k: row.get(k, "") for k in METRIC_FIELDS}


def append_metrics(path: Path, row: dict) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    new = not path.exists()
    with path.open("a", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=METRIC_FIELDS)
        if new:
            writer.writeheader()
        writer.writerow(row)


def read_metrics(path: Path) -> list[dict]:
    path = Path(path)
    if not path.exists():
        return []
    with path.open(encoding="utf-8", newline="") as fh:
        return list(csv.DictReader(fh))


def rows_for_day(rows: list[dict], day: date) -> list[dict]:
    prefix = day.isoformat()
    return [r for r in rows if str(r.get("ts", "")).startswith(prefix)]


def rows_between(rows: list[dict], start_local: str, end_local: str) -> list[dict]:
    """Rows with ``start_local <= ts < end_local`` (``YYYY-MM-DD HH:MM:SS`` local strings)."""
    return [r for r in rows if start_local <= str(r.get("ts", "")) < end_local]


def utc_window(start: datetime, end: datetime) -> tuple[str, str]:
    """Aware local bounds as the ``...Z`` strings the permission log uses.

    Each bound is converted on its own, so a window spanning a DST change keeps its real
    length (adding ``timedelta(days=1)`` to a UTC start would be an hour off that day).
    """
    fmt = "%Y-%m-%dT%H:%M:%SZ"
    return start.astimezone(UTC).strftime(fmt), end.astimezone(UTC).strftime(fmt)


# ---------- rule proposals (#5) ----------


def denials_between(log_lines: Iterable[str], start_utc: str, end_utc: str) -> list[dict]:
    """Permission-log entries with ``start_utc <= ts < end_utc`` (ISO ``...Z`` strings)."""
    out = []
    for line in log_lines:
        e = _json(line)
        if e and e.get("event") in DENIAL_EVENTS and start_utc <= str(e.get("ts", "")) < end_utc:
            out.append(e)
    return out


def _denial_key(d: dict) -> str:
    if d.get("tool") == "Bash":
        return " ".join(str(d.get("input", "")).split()[:2])
    return str(d.get("tool", ""))


def propose_rules(denials: list[dict], min_count: int = 2) -> list[str]:
    """One proposal per command prefix denied at least ``min_count`` times."""
    groups: dict[str, list[str]] = {}
    for d in denials:
        groups.setdefault(_denial_key(d), []).append(str(d.get("task", "?")))
    rules = []
    for key, tasks in groups.items():
        if len(tasks) < min_count:
            continue
        who = ", ".join(dict.fromkeys(tasks))
        rules.append(
            f'`{key}` was denied {len(tasks)} times (tasks {who}): add it to the "Do not" section '
            "of `.claude/rules/command-style.md` with a replacement, or add a specific allow rule "
            "if this command is really needed."
        )
    return rules


NEEDS_LAM_HEADING = "## Needs Lam"
# Run reports written before the switch to English used this heading.
_OLD_NEEDS_LAM_HEADINGS = frozenset({"## Việc cần Lam"})


def needs_lam_section(todo: list[str]) -> list[str]:
    """Lines of the ``## Needs Lam`` section that a run report writes."""
    return [NEEDS_LAM_HEADING, *(todo or ["- None"]), ""]


def needs_lam(run_report_md: str) -> list[str]:
    """Unticked items of the ``## Needs Lam`` section of a ``logs/run-*.md`` report."""
    items: list[str] = []
    inside = False
    for line in (run_report_md or "").splitlines():
        if line.startswith("## "):
            inside = line.strip() == NEEDS_LAM_HEADING or line.strip() in _OLD_NEEDS_LAM_HEADINGS
            continue
        if inside and line.startswith("- [ ] "):
            items.append(line[len("- [ ] ") :].strip())
    return items


# ---------- daily report ----------


def _num(v: object) -> float:
    try:
        return float(v)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return 0.0


def build_daily_report(
    day: date,
    rows: list[dict],
    denials: list[dict],
    todo: list[str],
    window: str | None = None,
) -> tuple[str, str]:
    """(subject, markdown body) for one day of orchestrator runs (``window`` = its span)."""
    merged = [r for r in rows if r.get("outcome") == "merged"]
    failed = [r for r in rows if r.get("outcome") in FAILED_OUTCOMES]
    cost = sum(_num(r.get("cost_usd")) for r in rows)
    subject = f"RepoPulse {day}: {len(merged)} merged, {len(failed)} failed, ${cost:.2f}"

    lines = [f"# RepoPulse · daily report {day}", ""]
    if window:
        lines += [f"Window: {window}", ""]
    if not rows:
        lines += ["No sessions ran today.", ""]
    else:
        lines += [
            f"{len(rows)} sessions, {len(merged)} PRs merged, {len(failed)} failed, "
            f"${cost:.2f} (operational figures from logs/orchestrator-metrics.csv).",
            "",
        ]
    lines += ["## Merged", ""]
    lines += [f"- PR #{r.get('reason')} (task {r.get('task')})" for r in merged] or ["- None"]
    lines += ["", "## Failures and review FAILs", ""]
    lines += [
        f"- {r.get('task')} [{r.get('phase')}]: {r.get('outcome')} — {r.get('reason')}"
        for r in failed
    ] or ["- None"]

    lines += ["", "## Cost per task", ""]
    if rows:
        per: dict[str, list[dict]] = {}
        for r in rows:
            per.setdefault(str(r.get("task")), []).append(r)
        lines += ["| Task | Sessions | Turns | Minutes | USD |", "|---|---|---|---|---|"]
        for task, rs in per.items():
            turns = int(sum(_num(r.get("turns")) for r in rs))
            minutes = round(sum(_num(r.get("duration_s")) for r in rs) / 60)
            usd = sum(_num(r.get("cost_usd")) for r in rs)
            lines.append(f"| {task} | {len(rs)} | {turns} | {minutes} | {usd:.2f} |")
    else:
        lines.append("- None")

    rules = propose_rules(denials)
    lines += ["", "## Commands asked about or denied", "", f"Total: {len(denials)}"]
    lines += ["", "## Proposed rules (Lam approves, then adds to `.claude/rules/`)", ""]
    lines += [f"- {r}" for r in rules] or ["- None"]
    lines += ["", NEEDS_LAM_HEADING, ""]
    lines += [f"- [ ] {t}" for t in todo] or ["- None"]
    return subject, "\n".join(lines) + "\n"


# ---------- e-mail ----------


def smtp_config(env: dict) -> dict | None:
    """Gmail SMTP from environment variables; None (do not send) when not configured."""
    user = env.get("REPOPULSE_SMTP_USER")
    password = env.get("REPOPULSE_SMTP_APP_PASSWORD")
    if not user or not password:
        return None
    return {
        "host": env.get("REPOPULSE_SMTP_HOST") or "smtp.gmail.com",
        "port": int(env.get("REPOPULSE_SMTP_PORT") or 465),
        "user": user,
        "password": password,
        "to": env.get("REPOPULSE_REPORT_TO") or user,
    }


def build_email(subject: str, body: str, cfg: dict) -> EmailMessage:
    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = cfg["user"]
    msg["To"] = cfg["to"]
    msg.set_content(body)
    return msg


def send_email(
    msg: EmailMessage, cfg: dict, smtp_factory: Callable[..., object] = smtplib.SMTP_SSL
) -> None:
    with smtp_factory(cfg["host"], cfg["port"], timeout=30) as s:  # type: ignore[attr-defined]
        s.login(cfg["user"], cfg["password"])
        s.send_message(msg)


def daily_due(now: datetime, last_sent: str | None, hour: int) -> bool:
    """Send once per local day, at or after ``hour``."""
    return now.hour >= hour and last_sent != now.date().isoformat()
