"""Naive DuckDB baseline: read every ``*.json.gz`` into one raw table, then run Q1-Q6.

Deliberately naive so later variants have something to beat: full refresh (the raw table is
dropped and rebuilt on every run), no partitioning, no deduplication, no incremental state.

Usage (what ``.github/workflows/benchmark.yml`` runs)::

    python benchmarks/naive_duckdb.py --runs 5 --variant baseline --git-sha <sha> \
        --out out/benchmark-row.csv

The reference files are checksum-verified first; on mismatch nothing runs and the exit code is
non-zero. Each run executes in its own child process so peak RSS is measured per run. The output
CSV holds the ``runs.csv`` header plus exactly one row (median of the runs).
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from collections.abc import Sequence
from datetime import UTC, datetime
from pathlib import Path

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import duckdb  # noqa: E402

from benchmarks.measure import summarize_runs  # noqa: E402
from benchmarks.reference import (  # noqa: E402
    REFERENCE_TOML,
    ChecksumError,
    load_reference,
    verify_files,
)
from repopulse.runs_log import COLUMNS, RunsLogError, append_run, validate_row  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
QUERY_DIR = Path(__file__).resolve().parent / "queries"
QUERIES: tuple[str, ...] = tuple(f"q{i}" for i in range(1, 7))
DEFAULT_SEED = ROOT / "transform" / "seeds" / "repos_data_ecosystem.csv"
DEFAULT_DATA_ROOT = Path("data/raw/gharchive")

# Explicit columns: payload is kept as raw JSON because its shape differs per event type.
RAW_COLUMNS = (
    "{'id': 'VARCHAR', 'type': 'VARCHAR', "
    "'actor': 'STRUCT(id BIGINT, login VARCHAR)', "
    "'repo': 'STRUCT(id BIGINT, name VARCHAR)', "
    "'payload': 'JSON', 'created_at': 'TIMESTAMP'}"
)


def _sql_str(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _peak_rss_mb() -> float | None:
    try:
        import resource
    except ImportError:  # Windows
        return None
    kb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    if sys.platform == "darwin":  # bytes on macOS, KiB on Linux
        kb /= 1024
    return round(kb / 1024, 1)


Source = str | Path | Sequence[str | Path]


def input_files(data: Source) -> list[Path]:
    """``*.json.gz`` of a directory, or an explicit list of files (all must exist)."""
    if isinstance(data, str | Path):
        files = sorted(Path(data).glob("*.json.gz"))
        if not files:
            raise FileNotFoundError(f"no *.json.gz files in {data}")
        return files
    files = [Path(p) for p in data]
    if not files:
        raise FileNotFoundError("empty file list")
    absent = [str(p) for p in files if not p.is_file()]
    if absent:
        raise FileNotFoundError(f"missing input file(s): {', '.join(absent)}")
    return files


def _source_sql(data: Source) -> str:
    if isinstance(data, str | Path):
        return _sql_str((Path(data) / "*.json.gz").as_posix())
    return "[" + ", ".join(_sql_str(Path(p).as_posix()) for p in data) + "]"


def load(con: duckdb.DuckDBPyConnection, data: Source, seed_path: Path) -> int:
    """Full refresh of ``raw_events`` and ``repos``; return the raw row count."""
    con.execute("DROP TABLE IF EXISTS raw_events")
    con.execute(
        "CREATE TABLE raw_events AS SELECT * FROM read_json("
        f"{_source_sql(data)}, format = 'newline_delimited', compression = 'gzip', "
        f"columns = {RAW_COLUMNS})"
    )
    con.execute("DROP TABLE IF EXISTS repos")
    con.execute(
        "CREATE TABLE repos AS SELECT * FROM read_csv("
        f"{_sql_str(Path(seed_path).as_posix())}, header = true, "
        "columns = {'repo_name': 'VARCHAR', 'category': 'VARCHAR'})"
    )
    return con.execute("SELECT count(*) FROM raw_events").fetchone()[0]


def run_once(
    data: Source,
    seed_path: str | Path = DEFAULT_SEED,
    *,
    collect: bool = False,
    temp_directory: str | Path | None = None,
) -> dict:
    """Load + Q1..Q6 once in this process; timings in seconds.

    ``data`` is a directory (every ``*.json.gz``) or a list of files. ``temp_directory`` lets
    DuckDB spill to disk instead of failing when the data does not fit in memory.
    """
    files = input_files(data)
    source = data if isinstance(data, str | Path) else files
    result: dict = {"bytes_read": sum(p.stat().st_size for p in files)}
    if collect:
        result["results"] = {}
    con = duckdb.connect(":memory:")
    try:
        if temp_directory is not None:
            Path(temp_directory).mkdir(parents=True, exist_ok=True)
            con.execute(f"SET temp_directory = {_sql_str(Path(temp_directory).as_posix())}")
        start = time.perf_counter()
        result["row_count"] = load(con, source, Path(seed_path))
        result["load_seconds"] = round(time.perf_counter() - start, 6)
        for q in QUERIES:
            sql = (QUERY_DIR / f"{q}.sql").read_text(encoding="utf-8")
            t0 = time.perf_counter()
            cur = con.execute(sql)
            rows = cur.fetchall()
            result[f"{q}_seconds"] = round(time.perf_counter() - t0, 6)
            result[f"{q}_rows"] = len(rows)
            if collect:
                columns = [c[0] for c in cur.description]
                result["results"][q] = {"columns": columns, "rows": rows}
        result["total_seconds"] = round(time.perf_counter() - start, 6)
    finally:
        con.close()
    result["peak_rss_mb"] = _peak_rss_mb()
    return result


def run_in_subprocess(data_dir: Path, seed_path: Path) -> dict:
    """One measured run in a fresh interpreter so peak RSS belongs to that run only."""
    cmd = [
        sys.executable,
        str(Path(__file__).resolve()),
        "--single",
        "--data-dir",
        str(data_dir),
        "--seed",
        str(seed_path),
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
    if proc.returncode != 0:
        raise RuntimeError(f"benchmark run failed ({proc.returncode}):\n{proc.stderr}")
    return json.loads(proc.stdout.strip().splitlines()[-1])


def _summary(row: dict) -> str:
    return "\n".join(f"{col:>14}: {row[col] if row[col] is not None else ''}" for col in COLUMNS)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Naive DuckDB baseline for Q1-Q6.")
    parser.add_argument("--data-dir", type=Path, help="default: data/raw/gharchive/<data_date>")
    parser.add_argument("--reference", type=Path, default=REFERENCE_TOML)
    parser.add_argument("--seed", type=Path, default=DEFAULT_SEED)
    parser.add_argument("--runs", type=int, default=5)
    parser.add_argument("--variant", default="baseline")
    parser.add_argument("--git-sha", default=None)
    parser.add_argument("--out", type=Path, default=Path("benchmark-row.csv"))
    parser.add_argument("--single", action="store_true", help=argparse.SUPPRESS)
    args = parser.parse_args(argv)

    if args.single:
        print(json.dumps(run_once(args.data_dir, args.seed)))
        return 0

    if not args.git_sha:
        parser.error("--git-sha is required")
    if args.runs < 1:
        parser.error("--runs must be >= 1")
    ref = load_reference(args.reference)
    data_dir = args.data_dir or DEFAULT_DATA_ROOT / ref["data_date"]
    run_at = datetime.now(UTC).isoformat(timespec="seconds")
    try:  # fail on bad metadata now, not after N runs
        validate_row(
            {
                "run_at": run_at,
                "git_sha": args.git_sha,
                "variant": args.variant,
                "data_date": ref["data_date"],
                "total_seconds": 0,
            }
        )
    except RunsLogError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2
    try:
        expected = {p.name for p in verify_files(data_dir, ref)}
        extra = sorted(p.name for p in Path(data_dir).glob("*.json.gz") if p.name not in expected)
        if extra:
            raise ChecksumError(f"files not listed in the reference: {', '.join(extra)}")
    except ChecksumError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2

    runs = []
    for i in range(1, args.runs + 1):
        run = run_in_subprocess(data_dir, args.seed)
        print(f"run {i}/{args.runs}: total {run['total_seconds']:.3f}s rows {run['row_count']}")
        runs.append(run)
    row = summarize_runs(
        runs,
        variant=args.variant,
        git_sha=args.git_sha,
        data_date=ref["data_date"],
        run_at=run_at,
    )
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.unlink(missing_ok=True)
    append_run(args.out, row)
    print(_summary(row))
    print(f"wrote {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
