"""Tests of repopulse.pathdecision (D4): the Plan v2 primary path rule applied to runs.csv.

Rule (docs/plan.md, primary-path selection): drop a path that does not run 24 times a day or
whose success rate is below 95%; of the rest pick the lower end-to-end p50; under 10% apart keep
the current path (A = path-a-runner).

No network. The numbers below are fake test values written to tmp_path, never to the real
runs.csv; the last tests check the real runs.csv and docs/probe-result.md of the repo.
"""

from __future__ import annotations

import csv
import random
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pytest

from repopulse import pathcompare as pc
from repopulse import pathdecision as pd
from repopulse.runs_log import COLUMNS

ROOT = Path(__file__).resolve().parents[1]
A = "path-a-runner"
B = "path-b-databricks"
D1, D2, D3, D4 = (date(2026, 12, d) for d in (1, 2, 3, 4))


# ---------------------------------------------------------------- helpers


def cells(variant, run_at, total, *, status="success", trigger="schedule"):
    """One runs.csv row (list of cells) of a path variant."""
    notes = [f"status={status}"]
    if status == "failed":
        notes.append("failed_step=dbt")
    notes += ["hour=23", "hours=1"]
    if trigger is not None:
        notes.append(f"trigger={trigger}")
    row = {c: "" for c in COLUMNS}
    row.update(
        run_at=run_at.isoformat(),
        git_sha="abcdef1",
        variant=variant,
        data_date="2026-10-25",
        total_seconds=str(total),
        notes=" ".join(notes),
    )
    return [row[c] for c in COLUMNS]


def day_runs(variant, day, n, total=100.0, *, failed=0, trigger="schedule"):
    """``n`` runs spread over the UTC ``day``; the last ``failed`` of them failed."""
    start = datetime(day.year, day.month, day.day, tzinfo=UTC)
    step = 86400 // max(n, 1)
    return [
        cells(
            variant,
            start + timedelta(seconds=i * step),
            total,
            status="failed" if i >= n - failed else "success",
            trigger=trigger,
        )
        for i in range(n)
    ]


def three_days(variant, total, *, full=24, edge=2, failed=0):
    """Scheduled runs on D1 (partial), D2 (the only full day), D3 (partial)."""
    return (
        day_runs(variant, D1, edge, total)
        + day_runs(variant, D2, full, total, failed=failed)
        + day_runs(variant, D3, edge, total)
    )


def write_runs(path: Path, rows) -> Path:
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.writer(fh, lineterminator="\n")
        writer.writerow(COLUMNS)
        writer.writerows(rows)
    return path


def load(tmp_path, rows):
    return pc.read_runs(write_runs(tmp_path / "runs.csv", rows))


def decide(tmp_path, rows, **kwargs):
    return pd.decide(load(tmp_path, rows), **kwargs)


# ---------------------------------------------------------------- constants


def test_thresholds_are_the_plan_v2_rule():
    assert pd.MIN_RUNS_PER_DAY == 24
    assert pd.MIN_SUCCESS_RATE == 0.95
    assert pd.MIN_GAP == 0.10
    assert pd.CURRENT == A


# ---------------------------------------------------------------- rule 2 and rule 3


def test_a_clearly_faster_keeps_a(tmp_path):
    d = decide(tmp_path, three_days(A, 50.0) + three_days(B, 200.0))
    assert (d.chosen, d.switch, d.outcome, d.rule) == (A, False, "faster-p50", 2)
    assert d.verdicts[A].eligible is True
    assert d.verdicts[B].eligible is True


def test_b_faster_by_more_than_ten_percent_switches(tmp_path):
    d = decide(tmp_path, three_days(A, 100.0) + three_days(B, 80.0))
    assert (d.chosen, d.switch, d.outcome, d.rule) == (B, True, "faster-p50", 2)
    assert d.relative_gap == pytest.approx(0.2)


def test_b_faster_by_under_ten_percent_keeps_a(tmp_path):
    d = decide(tmp_path, three_days(A, 100.0) + three_days(B, 90.01))
    assert (d.chosen, d.switch, d.outcome, d.rule) == (A, False, "within-gap", 3)


def test_exactly_ten_percent_switches(tmp_path):
    d = decide(tmp_path, three_days(A, 100.0) + three_days(B, 90.0))
    assert (d.chosen, d.switch, d.outcome) == (B, True, "faster-p50")


def test_exactly_ten_percent_is_not_lost_to_float_rounding(tmp_path):
    # (55.5 - 49.95) / 55.5 is 0.1 in decimal but just under 0.1 in binary floating point.
    assert (55.5 - 49.95) / 55.5 < 0.10
    d = decide(tmp_path, three_days(A, 55.5) + three_days(B, 49.95))
    assert (d.chosen, d.switch, d.outcome) == (B, True, "faster-p50")


def test_a_faster_by_under_ten_percent_keeps_a(tmp_path):
    d = decide(tmp_path, three_days(A, 100.0) + three_days(B, 105.0))
    assert (d.chosen, d.switch) == (A, False)
    assert d.outcome == "within-gap"


def test_equal_p50_keeps_a(tmp_path):
    d = decide(tmp_path, three_days(A, 100.0) + three_days(B, 100.0))
    assert (d.chosen, d.switch, d.outcome) == (A, False, "within-gap")
    assert d.relative_gap == 0.0


def test_gap_threshold_is_a_keyword(tmp_path):
    rows = three_days(A, 100.0) + three_days(B, 95.0)
    assert decide(tmp_path, rows).chosen == A
    assert decide(tmp_path, rows, min_gap=0.05).chosen == B


# ---------------------------------------------------------------- rule 1


def test_b_below_95_percent_success_is_dropped_even_if_faster(tmp_path):
    # B: 8 + 24 + 8 = 40 runs, 3 failed -> 37/40 = 92.5%.
    rows = three_days(A, 100.0, edge=8) + three_days(B, 10.0, edge=8, failed=3)
    d = decide(tmp_path, rows)
    assert (d.chosen, d.switch, d.outcome, d.rule) == (A, False, "only-eligible-path", 1)
    v = d.verdicts[B]
    assert (v.runs, v.successes, v.eligible) == (40, 37, False)
    assert v.success_rate == pytest.approx(0.925)
    assert any("success rate" in r for r in v.reasons)


def test_exactly_95_percent_success_is_eligible(tmp_path):
    # B: 40 runs, 2 failed -> 38/40 = 95%.
    rows = three_days(A, 100.0, edge=8) + three_days(B, 50.0, edge=8, failed=2)
    d = decide(tmp_path, rows)
    assert d.verdicts[B].eligible is True
    assert d.verdicts[B].reasons == ()
    assert (d.chosen, d.switch) == (B, True)


def test_a_dropped_b_eligible_picks_b_even_if_slower(tmp_path):
    rows = three_days(A, 50.0, full=23) + three_days(B, 500.0)
    d = decide(tmp_path, rows)
    assert (d.chosen, d.switch, d.outcome, d.rule) == (B, True, "only-eligible-path", 1)
    assert d.verdicts[A].eligible is False


def test_full_day_with_23_runs_is_dropped(tmp_path):
    d = decide(tmp_path, three_days(A, 100.0) + three_days(B, 10.0, full=23))
    v = d.verdicts[B]
    assert v.eligible is False
    assert v.min_runs_per_full_day == 23
    assert v.runs_per_full_day == {D2: 23}
    assert any("23" in r and "24" in r for r in v.reasons)
    assert (d.chosen, d.switch) == (A, False)


def test_path_missing_a_whole_full_day_is_dropped(tmp_path):
    # Window D1..D4 (from A), full days D2 and D3; B has no run at all on D3.
    rows = (
        day_runs(A, D1, 2)
        + day_runs(A, D2, 24)
        + day_runs(A, D3, 24)
        + day_runs(A, D4, 2)
        + day_runs(B, D1, 2, 10.0)
        + day_runs(B, D2, 24, 10.0)
    )
    d = decide(tmp_path, rows)
    assert d.full_days == (D2, D3)
    assert d.verdicts[B].runs_per_full_day == {D2: 24, D3: 0}
    assert d.verdicts[B].min_runs_per_full_day == 0
    assert d.verdicts[B].eligible is False
    assert d.verdicts[A].eligible is True
    assert (d.chosen, d.switch) == (A, False)


def test_failed_runs_count_towards_runs_per_day(tmp_path):
    # 24 runs on the full day, 1 of them failed: still 24 runs that day (rate 27/28 < 95%).
    d = decide(tmp_path, three_days(A, 100.0) + three_days(B, 100.0, failed=1))
    assert d.verdicts[B].runs_per_full_day == {D2: 24}


def test_both_dropped_keeps_a(tmp_path):
    rows = three_days(A, 100.0, full=10) + three_days(B, 10.0, full=10)
    d = decide(tmp_path, rows)
    assert (d.chosen, d.switch, d.outcome, d.rule) == (A, False, "no-eligible-path", 1)
    assert d.verdicts[A].eligible is False
    assert d.verdicts[B].eligible is False


def test_path_without_scheduled_runs_is_dropped(tmp_path):
    d = decide(tmp_path, three_days(A, 100.0))
    v = d.verdicts[B]
    assert (v.runs, v.success_rate, v.eligible) == (0, None, False)
    assert (d.chosen, d.outcome) == (A, "only-eligible-path")


# ---------------------------------------------------------------- what the rule reads


def test_only_push_runs_is_insufficient_data(tmp_path):
    rows = (
        day_runs(A, D1, 24, 100.0, trigger="push")
        + day_runs(A, D2, 24, 100.0, trigger="push")
        + day_runs(A, D3, 24, 100.0, trigger="push")
        + day_runs(B, D1, 24, 1.0, trigger="push")
        + day_runs(B, D2, 24, 1.0, trigger="push")
        + day_runs(B, D3, 24, 1.0, trigger="push")
    )
    d = decide(tmp_path, rows)
    assert (d.chosen, d.switch, d.outcome, d.rule) == (A, False, "insufficient-data", None)
    assert d.full_days == ()
    for v in d.verdicts.values():
        assert v.runs == 0
        assert v.eligible is None
        assert v.reasons == ()


def test_scheduled_runs_on_two_adjacent_days_is_insufficient_data(tmp_path):
    rows = (
        day_runs(A, D1, 24, 100.0)
        + day_runs(A, D2, 24, 100.0)
        + day_runs(B, D1, 24, 1.0)
        + day_runs(B, D2, 24, 1.0)
    )
    d = decide(tmp_path, rows)
    assert (d.chosen, d.switch, d.outcome) == (A, False, "insufficient-data")
    assert d.verdicts[B].runs == 48
    assert d.verdicts[A].eligible is None
    assert d.verdicts[B].eligible is None


def test_non_schedule_runs_are_ignored(tmp_path):
    base = three_days(A, 100.0) + three_days(B, 200.0)
    noise = (
        day_runs(B, D2, 30, 1.0, trigger="push")
        + day_runs(B, D2, 30, 1.0, trigger="workflow_dispatch")
        + day_runs(B, D2, 30, 1.0, trigger=None)
        + day_runs(A, D2, 30, 1.0, failed=30, trigger="push")
    )
    d = decide(tmp_path, base + noise)
    assert (d.chosen, d.switch, d.outcome) == (A, False, "faster-p50")
    assert (d.verdicts[A].runs, d.verdicts[A].successes) == (28, 28)
    assert d.verdicts[B].runs == 28
    assert d.verdicts[B].e2e_p50 == 200.0


def test_p50_uses_successful_runs_only(tmp_path):
    # 10 successful runs at 100 s, 10 failed runs at 1 s: p50 over every run would be 1.0.
    rows = day_runs(A, D2, 20, 100.0, failed=10)
    for i, row in enumerate(rows):
        if i >= 10:
            row[COLUMNS.index("total_seconds")] = "1.0"
    d = decide(tmp_path, rows + day_runs(A, D1, 1) + day_runs(A, D3, 1))
    assert d.verdicts[A].e2e_p50 == 100.0


def test_p50_is_nearest_rank(tmp_path):
    totals = [10.0, 20.0, 30.0, 40.0]
    rows = [
        cells(A, datetime(2026, 12, 2, h, tzinfo=UTC), t) for h, t in enumerate(totals)
    ] + day_runs(A, D1, 1, 20.0)
    rows += day_runs(A, D3, 1, 30.0)
    d = decide(tmp_path, rows)
    # 6 values 10, 20, 20, 30, 30, 40: rank ceil(0.5 * 6) = 3 -> 20.0 (no interpolation).
    assert d.verdicts[A].e2e_p50 == 20.0


# ---------------------------------------------------------------- render


def test_render_is_deterministic(tmp_path):
    rows = three_days(A, 100.0) + three_days(B, 80.0) + day_runs(A, D2, 3, 7.0, trigger="push")
    first = pd.render_decision(pd.decide(load(tmp_path, rows)), load(tmp_path, rows))
    shuffled = list(rows)
    random.Random(7).shuffle(shuffled)
    runs = load(tmp_path, shuffled)
    assert pd.render_decision(pd.decide(runs), runs) == first


def test_render_switch_names_b_and_pipeline(tmp_path):
    runs = load(tmp_path, three_days(A, 100.0) + three_days(B, 80.0))
    text = pd.render_decision(pd.decide(runs), runs)
    conclusion = [line for line in text.splitlines() if line.startswith("**Conclusion")]
    assert len(conclusion) == 1
    assert B in conclusion[0]
    assert "rule 2" in conclusion[0]
    assert "pipeline.yml" in conclusion[0]
    assert "20.0%" in text


def test_render_insufficient_data_keeps_a_and_lists_push_rows(tmp_path):
    rows = day_runs(A, D1, 2, 55.0, trigger="push") + day_runs(B, D1, 2, 160.0, trigger="push")
    runs = load(tmp_path, rows)
    text = pd.render_decision(pd.decide(runs), runs)
    conclusion = next(line for line in text.splitlines() if line.startswith("**Conclusion"))
    assert A in conclusion
    assert "insufficient data" in conclusion
    assert "not changed" in conclusion
    assert "| path-b-databricks | push | 2 | 2 | 160.0 | 160.0 |" in text
    assert "not used by the rule" in text


def test_render_shows_rule_1_reasons(tmp_path):
    runs = load(tmp_path, three_days(A, 100.0) + three_days(B, 10.0, full=23))
    text = pd.render_decision(pd.decide(runs), runs)
    assert "23" in text
    assert "dropped" in text


# ---------------------------------------------------------------- CLI


DOC = "# Doc\n\nintro\n\n<!-- path-decision:start -->\nold\n<!-- path-decision:end -->\n\nend\n"


def test_cli_writes_block_between_markers_and_is_idempotent(tmp_path):
    runs_csv = write_runs(tmp_path / "runs.csv", three_days(A, 100.0) + three_days(B, 80.0))
    doc = tmp_path / "doc.md"
    doc.write_text(DOC, encoding="utf-8")
    assert pd.main(["--runs", str(runs_csv), "--doc", str(doc)]) == 0
    text = doc.read_text(encoding="utf-8")
    runs = pc.read_runs(runs_csv)
    block = pd.render_decision(pd.decide(runs), runs)
    assert text.startswith("# Doc\n\nintro\n\n<!-- path-decision:start -->\n")
    assert text.endswith("<!-- path-decision:end -->\n\nend\n")
    assert block.strip("\n") in text
    assert "old" not in text
    assert pd.main(["--runs", str(runs_csv), "--doc", str(doc)]) == 0
    assert doc.read_text(encoding="utf-8") == text
    assert pd.main(["--runs", str(runs_csv), "--doc", str(doc), "--check"]) == 0


def test_cli_check_exits_1_on_stale_block_and_writes_nothing(tmp_path):
    runs_csv = write_runs(tmp_path / "runs.csv", three_days(A, 100.0) + three_days(B, 80.0))
    doc = tmp_path / "doc.md"
    doc.write_text(DOC, encoding="utf-8")
    assert pd.main(["--runs", str(runs_csv), "--doc", str(doc), "--check"]) == 1
    assert doc.read_text(encoding="utf-8") == DOC


def test_cli_without_doc_prints_block(tmp_path, capsys):
    runs_csv = write_runs(tmp_path / "runs.csv", three_days(A, 100.0) + three_days(B, 80.0))
    assert pd.main(["--runs", str(runs_csv)]) == 0
    runs = pc.read_runs(runs_csv)
    assert capsys.readouterr().out == pd.render_decision(pd.decide(runs), runs)


@pytest.mark.parametrize(
    "doc_text",
    [
        "# Doc\n\nno markers\n",
        "<!-- path-decision:start -->\nonly start\n",
        "<!-- path-decision:end -->\n<!-- path-decision:start -->\n",
        "<!-- path-decision:start -->\n<!-- path-decision:start -->\n<!-- path-decision:end -->\n",
    ],
)
def test_cli_bad_markers_exit_2(tmp_path, capsys, doc_text):
    runs_csv = write_runs(tmp_path / "runs.csv", three_days(A, 100.0))
    doc = tmp_path / "doc.md"
    doc.write_text(doc_text, encoding="utf-8")
    assert pd.main(["--runs", str(runs_csv), "--doc", str(doc)]) == 2
    assert "path-decision" in capsys.readouterr().err
    assert doc.read_text(encoding="utf-8") == doc_text


def test_cli_bad_runs_exit_2(tmp_path, capsys):
    doc = tmp_path / "doc.md"
    doc.write_text(DOC, encoding="utf-8")
    assert pd.main(["--runs", str(tmp_path / "missing.csv"), "--doc", str(doc)]) == 2
    bad = tmp_path / "bad.csv"
    bad.write_text("not,a,header\n", encoding="utf-8")
    assert pd.main(["--runs", str(bad), "--doc", str(doc)]) == 2
    err = capsys.readouterr().err
    assert "error:" in err
    assert "Traceback" not in err
    assert doc.read_text(encoding="utf-8") == DOC


def test_cli_missing_doc_exit_2(tmp_path):
    runs_csv = write_runs(tmp_path / "runs.csv", three_days(A, 100.0))
    assert pd.main(["--runs", str(runs_csv), "--doc", str(tmp_path / "nope.md")]) == 2


# ---------------------------------------------------------------- the real repo


def test_repo_decision_is_insufficient_data_and_keeps_a():
    d = pd.decide(pc.read_runs(ROOT / "runs.csv"))
    assert (d.chosen, d.switch, d.outcome) == (A, False, "insufficient-data")


def test_repo_probe_result_block_matches_runs_csv():
    runs = pc.read_runs(ROOT / "runs.csv")
    doc = (ROOT / "docs" / "probe-result.md").read_text(encoding="utf-8")
    block = pd.render_decision(pd.decide(runs), runs)
    assert pd.update_doc(doc, block) == doc


def test_repo_probe_result_cites_rerun_and_rule_thresholds():
    doc = (ROOT / "docs" / "probe-result.md").read_text(encoding="utf-8")
    assert "43104609971" in doc
    assert "24 lần/ngày" in doc
    assert "95%" in doc
    assert "10%" in doc
    assert "python -m repopulse.pathdecision" in doc
