"""Tests for orchestrator metrics, rule proposals and the daily e-mail report (no network)."""

from __future__ import annotations

import json
from datetime import date, datetime
from pathlib import Path

from repopulse.daily_report import (
    METRIC_FIELDS,
    append_metrics,
    build_daily_report,
    build_email,
    daily_due,
    denials_between,
    metrics_row,
    needs_lam,
    propose_rules,
    read_metrics,
    rows_for_day,
    run_metrics,
    send_email,
    smtp_config,
)


def _result(**kw) -> str:
    ev = {
        "type": "result",
        "num_turns": 69,
        "duration_ms": 3_537_061,
        "total_cost_usd": 13.6502727,
        "permission_denials": [{"tool_name": "Bash"}],
        "subagent_stats": {"spawned": 4},
        "result": "RESULT: ready 23",
    }
    ev.update(kw)
    return json.dumps(ev)


# ---------- metrics (#4) ----------


def test_run_metrics_reads_the_final_result_event():
    lines = ["garbage", json.dumps({"type": "assistant"}), _result()]
    m = run_metrics(lines)
    assert m == {
        "turns": 69,
        "duration_s": 3537,
        "cost_usd": 13.6503,
        "denials": 1,
        "subagents": 4,
    }


def test_run_metrics_without_result_event_is_empty_not_zero():
    # a killed/stalled session has no result: unknown, not "free"
    m = run_metrics([json.dumps({"type": "assistant"})])
    assert m == {"turns": "", "duration_s": "", "cost_usd": "", "denials": "", "subagents": ""}


def test_metrics_row_has_every_field_in_order():
    row = metrics_row(
        "2026-11-11 22:01:00", "6", "review", "fail", "mutation sống", run_metrics([_result()])
    )
    assert list(row) == METRIC_FIELDS
    assert row["task"] == "6" and row["phase"] == "review" and row["outcome"] == "fail"
    assert row["cost_usd"] == 13.6503


def test_append_and_read_metrics_roundtrip(tmp_path: Path):
    p = tmp_path / "logs" / "orchestrator-metrics.csv"
    m = run_metrics([_result()])
    append_metrics(p, metrics_row("2026-11-11 09:00:00", "6", "build", "ready", "23", m))
    append_metrics(p, metrics_row("2026-11-12 01:00:00", "7", "build", "failed", "x", m))
    rows = read_metrics(p)
    assert [r["task"] for r in rows] == ["6", "7"]
    assert p.read_text("utf-8").count("ts,task,phase") == 1  # header written once
    assert read_metrics(tmp_path / "missing.csv") == []


def test_rows_between_is_half_open_on_local_timestamps():
    from repopulse.daily_report import rows_between

    rows = [
        {"ts": "2026-11-10 21:59:59"},
        {"ts": "2026-11-10 22:00:00"},
        {"ts": "2026-11-11 21:59:59"},
        {"ts": "2026-11-11 22:00:00"},
    ]
    got = rows_between(rows, "2026-11-10 22:00:00", "2026-11-11 22:00:00")
    assert [r["ts"] for r in got] == ["2026-11-10 22:00:00", "2026-11-11 21:59:59"]


def test_utc_window_converts_aware_local_bounds_including_a_dst_change():
    # Review PR 26: local midnight + timedelta(days=1) is off by an hour on a DST day.
    from datetime import timedelta, timezone

    from repopulse.daily_report import utc_window

    edt, est = timezone(timedelta(hours=-4)), timezone(timedelta(hours=-5))
    start = datetime(2026, 11, 1, 0, 0, tzinfo=edt)  # before the fall-back change
    end = datetime(2026, 11, 2, 0, 0, tzinfo=est)  # after it: a 25-hour local day
    assert utc_window(start, end) == ("2026-11-01T04:00:00Z", "2026-11-02T05:00:00Z")


def test_rows_for_day_filters_on_local_timestamp():
    rows = [{"ts": "2026-11-11 23:59:59"}, {"ts": "2026-11-12 00:00:00"}]
    assert rows_for_day(rows, date(2026, 11, 11)) == [{"ts": "2026-11-11 23:59:59"}]


# ---------- rule proposals (#5) ----------


def _denial(task: str, cmd: str, tool: str = "Bash", ts: str = "2026-11-11T10:00:00Z") -> dict:
    return {"ts": ts, "task": task, "event": "PermissionDenied", "tool": tool, "input": cmd}


def test_propose_rules_groups_repeated_denials_by_command_prefix():
    denials = [
        _denial("6", "git stash push -m wip"),
        _denial("12", "git stash pop"),
        _denial("6", "rm -rf transform/target"),
        _denial("7", "python -c 'print(1)'"),
    ]
    rules = propose_rules(denials)
    assert len(rules) == 1
    assert "`git stash`" in rules[0]
    assert "2 lần" in rules[0] and "6, 12" in rules[0]


def test_propose_rules_respects_threshold():
    denials = [_denial("6", "rm -rf a"), _denial("7", "rm -rf b"), _denial("8", "rm -rf c")]
    assert propose_rules(denials, min_count=4) == []
    assert "3 lần" in propose_rules(denials, min_count=3)[0]


def test_denials_between_filters_utc_window_and_skips_garbage():
    lines = [
        json.dumps(_denial("6", "a", ts="2026-11-11T14:59:59Z")),
        "not json",
        json.dumps(_denial("6", "b", ts="2026-11-11T15:00:00Z")),
        json.dumps(_denial("6", "c", ts="2026-11-12T15:00:00Z")),
    ]
    got = denials_between(lines, "2026-11-11T15:00:00Z", "2026-11-12T15:00:00Z")
    assert [d["input"] for d in got] == ["b"]


def test_needs_lam_reads_unticked_items_of_its_section():
    md = (
        "# Orchestrator run\n\n## Việc cần Lam\n- [ ] develop-ci: CI đỏ\n- [ ] 9: cần secret\n\n"
        "## Kết quả từng việc\n- [ ] not this\n"
    )
    assert needs_lam(md) == ["develop-ci: CI đỏ", "9: cần secret"]
    assert needs_lam("## Việc cần Lam\n- Không có\n") == []
    assert needs_lam("") == []


# ---------- daily report ----------


def test_build_daily_report_summarises_the_day():
    m = run_metrics([_result()])
    rows = [
        metrics_row("2026-11-11 09:00:00", "6", "build", "ready", "23", m),
        metrics_row("2026-11-11 10:00:00", "6", "review", "merged", "23", m),
        metrics_row("2026-11-11 11:00:00", "12", "review", "fail", "mutation sống", m),
        metrics_row("2026-11-11 12:00:00", "7", "build", "failed", "treo 30 phút", m),
    ]
    denials = [_denial("6", "git stash"), _denial("12", "git stash pop")]
    subject, body = build_daily_report(date(2026, 11, 11), rows, denials, ["9: cần secret"])
    assert subject == "RepoPulse 2026-11-11: 1 merged, 2 thất bại, $54.60"
    assert "PR #23 (việc 6)" in body
    assert "12 [review]: fail — mutation sống" in body
    assert "7 [build]: failed — treo 30 phút" in body
    assert "`git stash`" in body
    assert "- [ ] 9: cần secret" in body
    assert "| 6 | 2 | 138 |" in body  # per task: sessions, turns


def test_build_daily_report_on_an_empty_day():
    subject, body = build_daily_report(date(2026, 11, 11), [], [], [])
    assert subject == "RepoPulse 2026-11-11: 0 merged, 0 thất bại, $0.00"
    assert "Không có phiên nào" in body


# ---------- e-mail ----------

ENV = {
    "REPOPULSE_SMTP_USER": "bot@example.com",
    "REPOPULSE_SMTP_APP_PASSWORD": "app-pass",
}


def test_smtp_config_needs_user_and_app_password():
    assert smtp_config({}) is None
    assert smtp_config({"REPOPULSE_SMTP_USER": "bot@example.com"}) is None
    cfg = smtp_config(ENV)
    assert cfg == {
        "host": "smtp.gmail.com",
        "port": 465,
        "user": "bot@example.com",
        "password": "app-pass",
        "to": "bot@example.com",  # defaults to sending to yourself
    }
    other = smtp_config({**ENV, "REPOPULSE_REPORT_TO": "me@example.com"})
    assert other["to"] == "me@example.com"


def test_build_email_has_headers_and_body():
    msg = build_email("S", "B\nC", smtp_config(ENV))
    assert msg["Subject"] == "S"
    assert msg["From"] == "bot@example.com" and msg["To"] == "bot@example.com"
    assert msg.get_content().strip() == "B\nC"


def test_send_email_logs_in_and_sends_without_touching_the_network():
    calls = []

    class FakeSMTP:
        def __init__(self, host, port, timeout):
            calls.append(("connect", host, port))

        def __enter__(self):
            return self

        def __exit__(self, *a):
            calls.append(("close",))

        def login(self, user, password):
            calls.append(("login", user, password))

        def send_message(self, msg):
            calls.append(("send", msg["Subject"]))

    cfg = smtp_config(ENV)
    send_email(build_email("S", "B", cfg), cfg, smtp_factory=FakeSMTP)
    assert calls == [
        ("connect", "smtp.gmail.com", 465),
        ("login", "bot@example.com", "app-pass"),
        ("send", "S"),
        ("close",),
    ]


def test_build_daily_report_names_its_window_when_given():
    _, body = build_daily_report(
        date(2026, 11, 11), [], [], [], window="2026-11-10 22:00 → 2026-11-11 22:00"
    )
    assert "Khoảng: 2026-11-10 22:00 → 2026-11-11 22:00" in body


def test_daily_due_once_per_day_after_the_hour():
    assert daily_due(datetime(2026, 11, 11, 21, 59), None, hour=22) is False
    assert daily_due(datetime(2026, 11, 11, 22, 0), None, hour=22) is True
    assert daily_due(datetime(2026, 11, 11, 23, 0), "2026-11-11", hour=22) is False
    assert daily_due(datetime(2026, 11, 12, 22, 5), "2026-11-11", hour=22) is True
