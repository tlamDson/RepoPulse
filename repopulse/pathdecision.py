"""Apply the Plan v2 primary path rule to ``runs.csv`` (D4, docs/probe-result.md).

The rule (docs/plan.md, "Luật chọn đường chính", fixed before measuring):

1. Drop a path that does not run 24 times a day within the free quota, or whose success rate is
   below 95%.
2. Of the remaining paths, pick the one with the lower end-to-end p50.
3. If the two paths are less than 10% apart, keep the current path (A = ``path-a-runner``).

How it is measured (D3 rows, parsed by ``repopulse.pathcompare.read_runs``):

- only runs with ``trigger=schedule`` in ``notes``; push, ``workflow_dispatch`` or a missing
  trigger never enter the rule;
- one window of UTC days for both paths, from the first to the last scheduled run of either path;
  the *full* days are the days strictly between the two (the first and the last day may be
  partial). A full day without a run of a path counts 0 for that path;
- runs per day count every run (success or failed) on each full day; the minimum must be >= 24;
- success rate = successes / scheduled runs of the path, partial days included; >= 95% passes
  (exactly 95% passes; no run fails);
- p50 = nearest-rank (``pathcompare.percentile``) of ``total_seconds`` of the successful
  scheduled runs;
- the gap is measured against the p50 of the current path: switch only when
  ``(p50_current - p50_other) >= 10% * p50_current``; exact arithmetic on the decimal text of the
  values, so exactly 10% switches;
- no full day yet -> ``insufficient-data``; no eligible path -> ``no-eligible-path``; both keep the
  current path and change nothing.

Standard library only. Usage::

    python -m repopulse.pathdecision --runs runs.csv                     # print the block
    python -m repopulse.pathdecision --runs runs.csv --doc docs/probe-result.md   # rewrite it
    python -m repopulse.pathdecision --runs runs.csv --doc docs/probe-result.md --check
"""

from __future__ import annotations

import argparse
import sys
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from datetime import date, timedelta
from fractions import Fraction
from pathlib import Path

from repopulse.pathcompare import VARIANTS, PathCompareError, PathRun, read_runs, summarize

MIN_RUNS_PER_DAY = 24
MIN_SUCCESS_RATE = 0.95
MIN_GAP = 0.10
CURRENT = VARIANTS["a"]
SCHEDULE = "schedule"

START_MARKER = "<!-- path-decision:start -->"
END_MARKER = "<!-- path-decision:end -->"
EMPTY = "—"

RULE_OF_OUTCOME: dict[str, int | None] = {
    "insufficient-data": None,
    "no-eligible-path": 1,
    "only-eligible-path": 1,
    "faster-p50": 2,
    "within-gap": 3,
}


class PathDecisionError(ValueError):
    """The document cannot be updated (missing file, missing or misplaced markers)."""


@dataclass(frozen=True)
class PathVerdict:
    """Rule inputs of one path, from its scheduled runs.

    ``eligible`` is ``None`` when the rule could not be applied (no full day yet).
    """

    variant: str
    runs: int
    successes: int
    success_rate: float | None
    e2e_p50: float | None
    runs_per_full_day: dict[date, int] = field(default_factory=dict)
    min_runs_per_full_day: int | None = None
    eligible: bool | None = None
    reasons: tuple[str, ...] = ()


@dataclass(frozen=True)
class Decision:
    """Outcome of the rule; ``chosen`` is the path to run, ``switch`` when it is not ``current``."""

    chosen: str
    current: str
    switch: bool
    outcome: str
    rule: int | None
    relative_gap: float | None
    first_day: date | None
    last_day: date | None
    full_days: tuple[date, ...]
    verdicts: dict[str, PathVerdict]
    min_runs_per_day: int = MIN_RUNS_PER_DAY
    min_success_rate: float = MIN_SUCCESS_RATE
    min_gap: float = MIN_GAP


def _exact(value: float) -> Fraction:
    """``value`` as the exact decimal it was written as (``repr`` round-trips the CSV text)."""
    return Fraction(repr(float(value)))


def scheduled(runs: Iterable[PathRun]) -> list[PathRun]:
    """Runs that enter the rule: ``trigger=schedule`` in ``notes``."""
    return [r for r in runs if r.notes.get("trigger") == SCHEDULE]


def _verdict(
    variant: str,
    runs: Sequence[PathRun],
    full_days: Sequence[date],
    *,
    min_runs_per_day: int,
    min_success_rate: float,
) -> PathVerdict:
    summary = summarize(runs)[variant]
    per_day = dict.fromkeys(full_days, 0)
    for r in runs:
        if r.day in per_day:
            per_day[r.day] += 1
    min_day = min(per_day.values()) if per_day else None
    if not full_days:
        return PathVerdict(
            variant=variant,
            runs=summary.runs,
            successes=summary.successes,
            success_rate=summary.success_rate,
            e2e_p50=summary.e2e_p50,
        )
    reasons: list[str] = []
    if min_day is not None and min_day < min_runs_per_day:
        short = sorted(d for d, n in per_day.items() if n < min_runs_per_day)
        reasons.append(
            f"{min_day} runs on a full UTC day (needs {min_runs_per_day}); "
            f"short days: {', '.join(d.isoformat() for d in short)}"
        )
    if summary.runs == 0:
        reasons.append("no scheduled run, so no success rate")
    elif Fraction(summary.successes, summary.runs) < _exact(min_success_rate):
        reasons.append(
            f"success rate {summary.successes}/{summary.runs} is below "
            f"{_threshold(min_success_rate)}"
        )
    return PathVerdict(
        variant=variant,
        runs=summary.runs,
        successes=summary.successes,
        success_rate=summary.success_rate,
        e2e_p50=summary.e2e_p50,
        runs_per_full_day=per_day,
        min_runs_per_full_day=min_day,
        eligible=not reasons,
        reasons=tuple(reasons),
    )


def decide(
    runs: Iterable[PathRun],
    *,
    min_runs_per_day: int = MIN_RUNS_PER_DAY,
    min_success_rate: float = MIN_SUCCESS_RATE,
    min_gap: float = MIN_GAP,
    current: str = CURRENT,
) -> Decision:
    """Apply the rule to the path rows of ``runs.csv`` (see the module docstring)."""
    if current not in VARIANTS.values():
        raise ValueError(f"current must be one of {sorted(VARIANTS.values())}, got {current!r}")
    (other,) = (v for v in VARIANTS.values() if v != current)
    rows = scheduled(runs)
    first = min((r.day for r in rows), default=None)
    last = max((r.day for r in rows), default=None)
    full: list[date] = []
    if first is not None and last is not None:
        day = first + timedelta(days=1)
        while day < last:
            full.append(day)
            day += timedelta(days=1)
    verdicts = {
        v: _verdict(
            v,
            [r for r in rows if r.variant == v],
            full,
            min_runs_per_day=min_runs_per_day,
            min_success_rate=min_success_rate,
        )
        for v in VARIANTS.values()
    }
    p50_cur, p50_other = verdicts[current].e2e_p50, verdicts[other].e2e_p50
    gap = None
    if p50_cur is not None and p50_other is not None and p50_cur > 0:
        gap = float((_exact(p50_cur) - _exact(p50_other)) / _exact(p50_cur))

    def done(chosen: str, outcome: str) -> Decision:
        return Decision(
            chosen=chosen,
            current=current,
            switch=chosen != current,
            outcome=outcome,
            rule=RULE_OF_OUTCOME[outcome],
            relative_gap=gap,
            first_day=first,
            last_day=last,
            full_days=tuple(full),
            verdicts=verdicts,
            min_runs_per_day=min_runs_per_day,
            min_success_rate=min_success_rate,
            min_gap=min_gap,
        )

    if not full:
        return done(current, "insufficient-data")
    eligible = [v for v in VARIANTS.values() if verdicts[v].eligible]
    if not eligible:
        return done(current, "no-eligible-path")
    if len(eligible) == 1:
        return done(eligible[0], "only-eligible-path")
    assert p50_cur is not None and p50_other is not None
    a, b = _exact(p50_cur), _exact(p50_other)
    if abs(a - b) < _exact(min_gap) * a:
        return done(current, "within-gap")
    return done(other if b < a else current, "faster-p50")


# ---------------------------------------------------------------- render


def _fmt(value: float | int | None) -> str:
    return EMPTY if value is None else str(value)


def _percent(value: float | None) -> str:
    return EMPTY if value is None else f"{100 * value:.1f}%"


def _threshold(value: float) -> str:
    """A rule threshold as a percent, exactly as written (0.95 -> ``95%``)."""
    pct = 100 * _exact(value)
    return f"{pct.numerator}%" if pct.denominator == 1 else f"{float(pct)}%"


def _gap(d: Decision) -> str:
    return _percent(None if d.relative_gap is None else abs(d.relative_gap))


def _eligible(v: PathVerdict) -> str:
    if v.eligible is None:
        return "not applied"
    return "kept" if v.eligible else "dropped"


def _conclusion(d: Decision) -> str:
    if d.outcome == "insufficient-data":
        how = (
            "no rule applied, insufficient data: the scheduled runs do not cover a full UTC day "
            "yet (that needs scheduled runs on at least 3 UTC days)"
        )
    elif d.outcome == "no-eligible-path":
        how = f"rule {d.rule}: both paths are dropped, so the current path stays"
    elif d.outcome == "only-eligible-path":
        how = f"rule {d.rule}: the only path that is kept"
    elif d.outcome == "faster-p50":
        how = f"rule {d.rule}: lower end-to-end p50, the p50 differ by {_gap(d)} of {d.current}'s"
    else:
        how = (
            f"rule {d.rule}: the p50 differ by {_gap(d)} of {d.current}'s, under "
            f"{_threshold(d.min_gap)}, so the current path stays"
        )
    if d.switch:
        head = f"switch to {d.chosen}"
        action = "open a separate PR that changes `.github/workflows/pipeline.yml`"
    else:
        head = f"keep {d.chosen} (current path)"
        action = "`.github/workflows/pipeline.yml` is not changed"
    return f"**Conclusion:** {head}; {how}; {action}."


def render_decision(decision: Decision, runs: Iterable[PathRun]) -> str:
    """Markdown block of ``decision``; every number is a runs.csv value or a count/ratio of it.

    ``runs`` are all path rows of runs.csv (every trigger), for the per-trigger table.
    """
    d = decision
    lines = [
        "### Primary path rule (Plan v2) applied to `runs.csv`",
        "",
        "Generated by `python -m repopulse.pathdecision` from `runs.csv`; do not edit by hand. "
        f"Rule 1 needs at least {d.min_runs_per_day} runs on every full UTC day and a success "
        f"rate of at least {_threshold(d.min_success_rate)}; rule 2 picks the lower end-to-end "
        f"p50; rule 3 keeps {d.current} when the gap is under {_threshold(d.min_gap)} of its p50. "
        "Only scheduled runs (`trigger=schedule`) enter the rule; p50 is nearest-rank over "
        "successful runs.",
        "",
    ]
    if d.first_day is None:
        lines.append("Scheduled runs: none in `runs.csv` yet.")
    else:
        full = ", ".join(x.isoformat() for x in d.full_days) or "none"
        lines.append(
            f"Scheduled runs: UTC days {d.first_day.isoformat()} to {d.last_day.isoformat()} "
            f"(first and last day may be partial); full days: {full}."
        )
    lines += [
        "",
        "| variant | scheduled runs | successes | success rate | full UTC days "
        "| min runs per full day | e2e p50 s | rule 1 | reason |",
        "|---|---|---|---|---|---|---|---|---|",
    ]
    for variant in VARIANTS.values():
        v = d.verdicts[variant]
        reason = "; ".join(v.reasons) or EMPTY
        lines.append(
            f"| {variant} | {v.runs} | {v.successes} | {_percent(v.success_rate)} "
            f"| {len(v.runs_per_full_day)} | {_fmt(v.min_runs_per_full_day)} "
            f"| {_fmt(v.e2e_p50)} | {_eligible(v)} | {reason} |"
        )
    lines += [
        "",
        "Every path run by trigger. Rows whose trigger is not `schedule` are not used by the rule "
        "(push runs only test the workflow).",
        "",
        "| variant | trigger | runs | successes | e2e p50 s | e2e p95 s |",
        "|---|---|---|---|---|---|",
    ]
    all_runs = list(runs)
    triggers = sorted({r.notes.get("trigger", EMPTY) for r in all_runs})
    for variant in VARIANTS.values():
        for trigger in triggers:
            group = [
                r
                for r in all_runs
                if r.variant == variant and r.notes.get("trigger", EMPTY) == trigger
            ]
            if not group:
                continue
            s = summarize(group)[variant]
            lines.append(
                f"| {variant} | {trigger} | {s.runs} | {s.successes} "
                f"| {_fmt(s.e2e_p50)} | {_fmt(s.e2e_p95)} |"
            )
    if not all_runs:
        lines.append(f"| {EMPTY} | {EMPTY} | 0 | 0 | {EMPTY} | {EMPTY} |")
    lines += ["", _conclusion(d)]
    return "\n".join(lines) + "\n"


# ---------------------------------------------------------------- document


def update_doc(text: str, block: str) -> str:
    """Replace the text between the path-decision markers with ``block``; the rest is untouched."""
    problem = f"document needs exactly one {START_MARKER} followed by exactly one {END_MARKER}"
    if text.count(START_MARKER) != 1 or text.count(END_MARKER) != 1:
        raise PathDecisionError(problem)
    start = text.index(START_MARKER)
    end = text.index(END_MARKER)
    if end < start:
        raise PathDecisionError(problem)
    head = text[: start + len(START_MARKER)]
    tail = text[end:]
    body = block.strip("\n")
    return f"{head}\n{body}\n{tail}"


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m repopulse.pathdecision",
        description="Apply the Plan v2 primary path rule to the path rows of runs.csv.",
    )
    parser.add_argument("--runs", default="runs.csv", help="path of runs.csv (default: runs.csv)")
    parser.add_argument(
        "--doc", help="rewrite the block between the path-decision markers of this file"
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="with --doc: write nothing; exit 1 when the block differs from runs.csv",
    )
    args = parser.parse_args(argv)
    if args.check and not args.doc:
        print("error: --check needs --doc", file=sys.stderr)
        return 2
    try:
        runs = read_runs(args.runs)
        block = render_decision(decide(runs), runs)
        if not args.doc:
            sys.stdout.write(block)
            return 0
        doc = Path(args.doc)
        if not doc.is_file():
            raise PathDecisionError(f"document not found: {doc}")
        current = doc.read_text(encoding="utf-8")
        updated = update_doc(current, block)
    except (PathCompareError, PathDecisionError, UnicodeDecodeError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2

    if args.check:
        if updated == current:
            print(f"{doc}: path-decision block is up to date")
            return 0
        print(
            f"{doc}: path-decision block is out of date with {args.runs}; run "
            f"`python -m repopulse.pathdecision --runs {args.runs} --doc {doc}` and commit it",
            file=sys.stderr,
        )
        return 1
    if updated == current:
        print(f"{doc}: path-decision block already up to date")
        return 0
    with open(doc, "w", encoding="utf-8", newline="\n") as fh:
        fh.write(updated)
    print(f"{doc}: path-decision block updated from {args.runs}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
