"""Spark vs DuckDB scale benchmark: 1, 7, 30 days of real GH Archive data, Q1-Q6.

For every level (``levels`` in ``benchmarks/scale.toml``) and engine (DuckDB, then PySpark) the
naive pipeline runs ``runs`` times, each run in a fresh child process that prints one JSON line.
The runs of one cell become one median row in the ``runs.csv`` format (variant
``<engine>-<N>d``, ``data_date`` = first day of the window). Both engines of a level must agree
on ``row_count`` and on the row count of every query; when they do not, neither row of that
level is written. A failing cell (crash, OOM, timeout) does not stop the other cells, but the
exit code is non-zero.

Usage (what ``.github/workflows/benchmark-scale.yml`` runs, after ``scale_prepare.py``)::

    python benchmarks/scale_bench.py --git-sha <sha> --out out/scale-rows.csv \
        --summary out/scale-summary.md

Merge the artifact into ``runs.csv`` with ``python benchmarks/record_run.py out/scale-rows.csv``.
"""

from __future__ import annotations

import argparse
import json
import math
import statistics
import subprocess
import sys
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from benchmarks.measure import MeasureError, summarize_runs  # noqa: E402
from benchmarks.scale_prepare import SCALE_TOML, hour_files, load_config  # noqa: E402
from repopulse.runs_log import COLUMNS, RunsLogError, append_run, validate_row  # noqa: E402

ENGINES: tuple[str, ...] = ("duckdb", "spark")
QUERIES: tuple[str, ...] = tuple(f"q{i}" for i in range(1, 7))
SECONDS_KEYS: tuple[str, ...] = ("total_seconds", *(f"{q}_seconds" for q in QUERIES))
COUNT_KEYS: tuple[str, ...] = ("row_count", "bytes_read", *(f"{q}_rows" for q in QUERIES))
CHECKED_KEYS: tuple[str, ...] = ("row_count", *(f"{q}_rows" for q in QUERIES))


class ScaleBenchError(RuntimeError):
    """A run, a cell or a cross-check of the scale benchmark failed."""


# ---------------------------------------------------------------- data


def level_files(data_dir: str | Path, start: str, days: int) -> tuple[list[Path], list[str]]:
    """Prepared files of the window (in order) and the hour names that are not there."""
    present: list[Path] = []
    missing: list[str] = []
    for name in hour_files(start, days):
        path = Path(data_dir) / name
        if path.is_file():
            present.append(path)
        else:
            missing.append(name)
    return present, missing


# ---------------------------------------------------------------- run output


def _check_number(key: str, value: object, *, integer: bool) -> None:
    kinds = int if integer else int | float
    if isinstance(value, bool) or not isinstance(value, kinds):
        raise ScaleBenchError(
            f"{key}: expected a {'n integer' if integer else ' number'}, got {value!r}"
        )
    if not math.isfinite(value) or value < 0:
        raise ScaleBenchError(f"{key}: must be finite and >= 0, got {value!r}")


def parse_run_output(stdout: str, engine: str) -> dict:
    """The last non-empty stdout line of one run, as a checked dict."""
    lines = [ln for ln in stdout.splitlines() if ln.strip()]
    if not lines:
        raise ScaleBenchError(f"{engine} run: no output")
    try:
        run = json.loads(lines[-1])
    except ValueError:
        raise ScaleBenchError(f"{engine} run: last line is not JSON: {lines[-1][:200]!r}") from None
    if not isinstance(run, dict):
        raise ScaleBenchError(f"{engine} run: expected a JSON object, got {type(run).__name__}")
    required = list(SECONDS_KEYS) + list(COUNT_KEYS)
    if engine == "spark":
        required.append("startup_seconds")
    absent = [k for k in required if k not in run]
    if absent:
        raise ScaleBenchError(f"{engine} run: missing key(s): {', '.join(absent)}")
    for key in required:
        _check_number(key, run[key], integer=key in COUNT_KEYS)
    if run.get("peak_rss_mb") is not None:
        _check_number("peak_rss_mb", run["peak_rss_mb"], integer=False)
    return run


# ---------------------------------------------------------------- cells


def variant(engine: str, days: int) -> str:
    return f"{engine}-{days}d"


def summarize_cell(
    runs: Sequence[Mapping[str, object]],
    *,
    engine: str,
    days: int,
    start_date: str,
    git_sha: str,
    run_at: str,
    missing_hours: int = 0,
    extra_notes: str = "",
) -> dict[str, object]:
    """Median row of one (engine, level) cell; counts must not change between runs."""
    for key in (f"{q}_rows" for q in QUERIES):
        values = {int(r[key]) for r in runs}
        if len(values) != 1:
            raise ScaleBenchError(
                f"{variant(engine, days)}: {key} differs between runs: {sorted(values)}"
            )
    try:
        row = summarize_runs(
            runs,
            variant=variant(engine, days),
            git_sha=git_sha,
            data_date=start_date,
            run_at=run_at,
        )
    except MeasureError as exc:
        raise ScaleBenchError(f"{variant(engine, days)}: {exc}") from None
    notes = [f"engine={engine} days={days} runs={len(runs)} median"]
    if engine == "spark":
        startup = statistics.median(float(r["startup_seconds"]) for r in runs)
        notes.append(f"startup_seconds={round(startup, 3)}")
    if missing_hours:
        notes.append(f"missing_hours={missing_hours}")
    if extra_notes:
        notes.append(extra_notes)
    row["notes"] = " ".join(notes)
    validate_row(row)
    return row


def cross_check(
    days: int,
    duckdb_runs: Sequence[Mapping[str, object]],
    spark_runs: Sequence[Mapping[str, object]],
) -> None:
    """Both engines saw the same rows and every query returned as many rows."""
    diffs = [
        f"{key} duckdb={duckdb_runs[0][key]} spark={spark_runs[0][key]}"
        for key in CHECKED_KEYS
        if int(duckdb_runs[0][key]) != int(spark_runs[0][key])
    ]
    if diffs:
        raise ScaleBenchError(f"{days}d: engines disagree: " + "; ".join(diffs))


# ---------------------------------------------------------------- one run (child process)


def run_single(engine: str, days: int, config: Path, data_dir: Path | None = None) -> dict:
    cfg = load_config(config)
    files, _missing = level_files(data_dir or Path(cfg["data_dir"]), cfg["start_date"], days)
    if not files:
        raise ScaleBenchError(f"{variant(engine, days)}: no prepared files")
    seed = Path(cfg.get("seed", "")) if cfg.get("seed") else None
    if engine == "duckdb":
        from benchmarks import naive_duckdb

        temp = cfg.get("duckdb", {}).get("temp_directory")
        return naive_duckdb.run_once(files, seed or naive_duckdb.DEFAULT_SEED, temp_directory=temp)
    if engine == "spark":
        from benchmarks import spark_naive

        return spark_naive.run_single(cfg.get("spark", {}), files, seed or spark_naive.DEFAULT_SEED)
    raise ScaleBenchError(f"unknown engine {engine!r}")


def run_in_subprocess(
    engine: str, days: int, *, config: Path, data_dir: Path, timeout_s: float
) -> dict:
    """One measured run in a fresh interpreter (own JVM for Spark, own peak RSS)."""
    cmd = [
        sys.executable,
        str(Path(__file__).resolve()),
        "--single",
        "--engine",
        engine,
        "--days",
        str(days),
        "--config",
        str(config),
        "--data-dir",
        str(data_dir),
    ]
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout_s, check=False)
    except subprocess.TimeoutExpired:
        raise ScaleBenchError(f"{engine} run timed out after {timeout_s:.0f} s") from None
    if proc.returncode != 0:
        tail = "\n".join(proc.stderr.strip().splitlines()[-20:])
        raise ScaleBenchError(f"{engine} run failed (exit {proc.returncode}):\n{tail}")
    return parse_run_output(proc.stdout, engine)


# ---------------------------------------------------------------- CLI


def _write_summary(path: Path, status: list[tuple[str, str]], rows: list[dict]) -> None:
    lines = ["### Benchmark scale (Spark vs DuckDB, GH Archive real data)", ""]
    lines += ["| cell | status |", "|---|---|"]
    lines += [f"| `{cell}` | {text.splitlines()[0] if text else ''} |" for cell, text in status]
    lines += ["", f"{len(rows)} row(s) for runs.csv:", "", "```csv", ",".join(COLUMNS)]
    lines += [",".join(validate_row(r)[c] for c in COLUMNS) for r in rows]
    lines += ["```", ""]
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines), encoding="utf-8")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Spark vs DuckDB at 1, 7, 30 days (Q1-Q6).")
    parser.add_argument("--config", type=Path, default=SCALE_TOML)
    parser.add_argument("--data-dir", type=Path, default=None, help="default: data_dir in config")
    parser.add_argument("--git-sha", default=None)
    parser.add_argument("--out", type=Path, default=Path("out/scale-rows.csv"))
    parser.add_argument("--summary", type=Path, default=Path("out/scale-summary.md"))
    parser.add_argument("--single", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--engine", choices=ENGINES, help=argparse.SUPPRESS)
    parser.add_argument("--days", type=int, help=argparse.SUPPRESS)
    args = parser.parse_args(argv)

    if args.single:
        print(json.dumps(run_single(args.engine, args.days, args.config, args.data_dir)))
        return 0

    cfg = load_config(args.config)
    start, runs_per_cell = cfg["start_date"], int(cfg["runs"])
    data_dir = args.data_dir or Path(cfg["data_dir"])
    timeout_s = float(cfg.get("run_timeout_minutes", 45)) * 60
    run_at = datetime.now(UTC).isoformat(timespec="seconds")
    try:  # fail on bad metadata now, not after hours of runs
        if not args.git_sha:
            raise RunsLogError("git_sha: --git-sha is required")
        for days in cfg["levels"]:
            for engine in ENGINES:
                validate_row(
                    {
                        "run_at": run_at,
                        "git_sha": args.git_sha,
                        "variant": variant(engine, days),
                        "data_date": start,
                        "total_seconds": 0,
                    }
                )
    except RunsLogError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2

    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(",".join(COLUMNS) + "\n", encoding="utf-8", newline="")
    rows: list[dict] = []
    status: list[tuple[str, str]] = []
    failed = False

    def fail(cell: str, message: str) -> None:
        nonlocal failed
        failed = True
        status.append((cell, f"FAILED: {message}"))
        print(f"ERROR {cell}: {message}", file=sys.stderr, flush=True)

    for days in cfg["levels"]:
        files, missing = level_files(data_dir, start, days)
        if not files:
            for engine in ENGINES:
                fail(variant(engine, days), f"no prepared files in {data_dir}")
            continue
        cell_runs: dict[str, list[dict]] = {}
        for engine in ENGINES:
            cell = variant(engine, days)
            runs: list[dict] = []
            try:
                for i in range(1, runs_per_cell + 1):
                    run = run_in_subprocess(
                        engine, days, config=args.config, data_dir=data_dir, timeout_s=timeout_s
                    )
                    print(
                        f"{cell} run {i}/{runs_per_cell}: total {run['total_seconds']:.3f}s "
                        f"rows {run['row_count']}",
                        flush=True,
                    )
                    runs.append(run)
                cell_runs[engine] = runs
            except ScaleBenchError as exc:
                fail(cell, str(exc))
        if len(cell_runs) == len(ENGINES):
            try:
                cross_check(days, cell_runs["duckdb"], cell_runs["spark"])
            except ScaleBenchError as exc:
                for engine in ENGINES:
                    fail(variant(engine, days), str(exc))
                continue
            extra = ""
        else:
            extra = "not cross-checked"
        for engine, runs in cell_runs.items():
            cell = variant(engine, days)
            try:
                row = summarize_cell(
                    runs,
                    engine=engine,
                    days=days,
                    start_date=start,
                    git_sha=args.git_sha,
                    run_at=run_at,
                    missing_hours=len(missing),
                    extra_notes=extra,
                )
            except (ScaleBenchError, RunsLogError) as exc:
                fail(cell, str(exc))
                continue
            append_run(args.out, row)
            rows.append(row)
            status.append((cell, f"ok, median total {row['total_seconds']} s"))

    _write_summary(args.summary, status, rows)
    print(f"wrote {len(rows)} row(s) to {args.out}")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
