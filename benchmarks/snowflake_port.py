"""Snowflake port: one ``runs.csv`` row per ``dbt build`` (DuckDB main version, Snowflake port).

Both builds run the same dbt project on the CI seed fixture (a CI fixture, not a measurement on
real data). For each engine the row holds:

- ``total_seconds``: ``elapsed_time`` of ``target/run_results.json`` (whole ``dbt build``);
- ``q1_seconds`` .. ``q6_seconds``: ``execution_time`` of the mart model of that question;
- ``row_count``: rows of the seed fixture; ``data_date``: smallest ``event_date`` of the seed;
- ``notes``: ``engine=..; fixture=seed; lines_changed=<n>; q_seconds=mart build time; base=..``.

``lines_changed`` = added + deleted lines of ``git diff --numstat <base_sha> HEAD -- transform``
(``base_sha`` in ``benchmarks/snowflake_port.toml``). A failed build (any node ``error``,
``fail``, ``runtime error`` or ``skipped``) writes no row and exits non-zero.

Usage (what ``.github/workflows/snowflake-port.yml`` runs right after each build)::

    python benchmarks/snowflake_port.py --engine duckdb \
        --run-results transform/target/run_results.json --git-sha <sha> \
        --out out/snowflake-port-rows.csv --summary out/snowflake-port-summary.md

Merge the artifact with ``python benchmarks/record_run.py out/snowflake-port-rows.csv``.
"""

from __future__ import annotations

import argparse
import csv
import json
import subprocess
import sys
import tomllib
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from repopulse.runs_log import RunsLogError, append_run, validate_row  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
CONFIG = Path(__file__).with_suffix(".toml")
SEED_CSV = ROOT / "transform" / "seeds" / "fixture_silver_gharchive_events.csv"
PROJECT = "repopulse"
MART_BY_QUERY: dict[str, str] = {
    "q1": "mart_daily_active_contributors",
    "q2": "mart_cohort_retention",
    "q3": "mart_contribution_funnel",
    "q4": "mart_responsiveness",
    "q5": "mart_repo_concentration",
    "q6": "mart_repo_health_score",
}
VARIANTS: dict[str, str] = {"duckdb": "dbt-build-duckdb", "snowflake": "dbt-build-snowflake"}
TARGETS: dict[str, str] = {"duckdb": "ci", "snowflake": "snowflake"}
FAILED_STATUSES = frozenset({"error", "fail", "runtime error", "skipped"})


class PortError(RuntimeError):
    """Bad input for the port row (numstat, run_results, seed) or a failed build."""


# ---------------------------------------------------------------- lines changed


def count_changed_lines(numstat: str) -> int:
    """Sum of added + deleted lines in ``git diff --numstat`` output; binary lines (``-``) skip."""
    total = 0
    for line in numstat.splitlines():
        if not line.strip():
            continue
        parts = line.split("\t", 2)
        if len(parts) != 3:
            raise PortError(f"not a numstat line: {line!r}")
        added, deleted, _path = parts
        if added == "-" and deleted == "-":
            continue
        if not (added.isdigit() and deleted.isdigit()):
            raise PortError(f"not a numstat line: {line!r}")
        total += int(added) + int(deleted)
    return total


def git_numstat(base: str, repo: Path = ROOT) -> str:
    cmd = ["git", "diff", "--numstat", base, "HEAD", "--", "transform"]
    proc = subprocess.run(cmd, capture_output=True, text=True, check=False, cwd=repo)
    if proc.returncode != 0:
        raise PortError(f"git diff failed ({proc.returncode}): {proc.stderr.strip()}")
    return proc.stdout


def load_config(path: Path = CONFIG) -> dict:
    with Path(path).open("rb") as fh:
        return tomllib.load(fh)


# ---------------------------------------------------------------- seed + run_results


def seed_stats(seed_csv: Path = SEED_CSV) -> tuple[int, str]:
    """(rows, smallest event_date) of the seed fixture."""
    with Path(seed_csv).open(encoding="utf-8", newline="") as fh:
        rows = list(csv.DictReader(fh))
    dates = sorted(r["event_date"] for r in rows if r.get("event_date"))
    if not rows or not dates:
        raise PortError(f"seed {seed_csv} has no rows with event_date")
    return len(rows), dates[0]


def load_run_results(path: Path) -> dict:
    try:
        data = json.loads(Path(path).read_text(encoding="utf-8"))
    except FileNotFoundError:
        raise PortError(f"{path} not found: dbt build did not run to the end") from None
    except json.JSONDecodeError as exc:
        raise PortError(f"{path} is not JSON: {exc}") from None
    if not isinstance(data, dict) or not isinstance(data.get("results"), list):
        raise PortError(f"{path} has no results list")
    return data


def build_failures(run_results: Mapping) -> list[str]:
    """unique_id of every node whose status is a failure."""
    return [
        str(r.get("unique_id"))
        for r in run_results.get("results", [])
        if str(r.get("status", "")).lower() in FAILED_STATUSES
    ]


def build_row(
    run_results: Mapping,
    *,
    engine: str,
    git_sha: str,
    run_at: str,
    row_count: int,
    data_date: str,
    lines_changed: int,
    base: str = "",
) -> dict[str, object]:
    """One ``runs.csv`` row from the run_results of a successful ``dbt build``."""
    if engine not in VARIANTS:
        raise PortError(f"unknown engine {engine!r}")
    args = run_results.get("args") or {}
    if args.get("which") != "build":
        raise PortError(f"run_results is for `dbt {args.get('which')}`, expected `dbt build`")
    if args.get("target") != TARGETS[engine]:
        raise PortError(
            f"run_results is for target {args.get('target')!r}, expected {TARGETS[engine]!r}"
        )
    failed = build_failures(run_results)
    if failed:
        raise PortError(f"dbt build failed on {len(failed)} node(s): {', '.join(failed)}")
    times = {str(r.get("unique_id")): r.get("execution_time") for r in run_results["results"]}
    row: dict[str, object] = {
        "run_at": run_at,
        "git_sha": git_sha,
        "variant": VARIANTS[engine],
        "data_date": data_date,
        "total_seconds": run_results.get("elapsed_time"),
        "row_count": row_count,
    }
    for q, mart in MART_BY_QUERY.items():
        uid = f"model.{PROJECT}.{mart}"
        if uid not in times:
            raise PortError(f"{mart} is not in run_results: the build did not run every mart")
        row[f"{q}_seconds"] = times[uid]
    notes = [
        f"engine={engine}",
        "fixture=seed",
        f"lines_changed={lines_changed}",
        "q_seconds=mart build time",
        "total=dbt build elapsed_time",
    ]
    if base:
        notes.append(f"base={base[:12]}")
    row["notes"] = "; ".join(notes)
    validate_row(row)
    return row


# ---------------------------------------------------------------- CLI


def _summary(path: Path, engine: str, row: Mapping | None, error: str | None) -> None:
    lines = [f"### dbt build `{VARIANTS.get(engine, engine)}` ({engine})", ""]
    lines.append("Input: CI fixture (seed), not a measurement on real data.")
    lines.append("")
    if error:
        lines += [f"**Failed, no row written:** {error}", ""]
    else:
        v = validate_row(row)
        lines += ["| column | value |", "|---|---|"]
        lines += [f"| `{c}` | {v[c]} |" for c in v if v[c] != ""]
        lines.append("")
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as fh:
        fh.write("\n".join(lines) + "\n")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="runs.csv row for a dbt build (Snowflake port).")
    parser.add_argument("--engine", choices=sorted(VARIANTS), required=True)
    parser.add_argument("--run-results", type=Path, required=True)
    parser.add_argument("--git-sha", required=True)
    src = parser.add_mutually_exclusive_group()
    src.add_argument("--base", default=None, help="base commit (default: base_sha in config)")
    src.add_argument("--numstat-file", type=Path, default=None)
    parser.add_argument("--config", type=Path, default=CONFIG)
    parser.add_argument("--seed", type=Path, default=SEED_CSV)
    parser.add_argument("--out", type=Path, default=Path("out/snowflake-port-rows.csv"))
    parser.add_argument("--summary", type=Path, default=None)
    args = parser.parse_args(argv)

    row = None
    try:
        if args.numstat_file is not None:
            base = ""
            numstat = args.numstat_file.read_text(encoding="utf-8")
        else:
            base = args.base or load_config(args.config)["base_sha"]
            numstat = git_numstat(base)
        row_count, data_date = seed_stats(args.seed)
        row = build_row(
            load_run_results(args.run_results),
            engine=args.engine,
            git_sha=args.git_sha,
            run_at=datetime.now(UTC).isoformat(timespec="seconds"),
            row_count=row_count,
            data_date=data_date,
            lines_changed=count_changed_lines(numstat),
            base=base,
        )
        args.out.parent.mkdir(parents=True, exist_ok=True)
        append_run(args.out, row)
    except (PortError, RunsLogError, OSError, KeyError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        if args.summary:
            _summary(args.summary, args.engine, None, str(exc))
        return 1
    if args.summary:
        _summary(args.summary, args.engine, row, None)
    print(f"wrote {VARIANTS[args.engine]} row to {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
