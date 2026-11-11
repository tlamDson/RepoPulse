"""Query optimization benchmark: Q1-Q6 verbatim on four parquet layouts of the reference day.

The same real GH Archive day (``benchmarks/reference.toml``) is written once per layout, then
``benchmarks/queries/q1..q6.sql`` run unchanged against a ``raw_events`` view over the layout:

====================================  =====================  ==================================
variant                               partition (Hive dirs)  sort order inside each file
====================================  =====================  ==================================
``qopt-flat``                         none                   arrival order (the "before")
``qopt-partitioned``                  ``type``               arrival order
``qopt-clustered``                    none                   ``type, repo.name, created_at``
``qopt-partitioned-clustered``        ``type``               ``repo.name, created_at``
====================================  =====================  ==================================

Preparing the layouts (read ``*.json.gz``, write parquet) is not timed into the query numbers; its
duration goes to ``notes`` (``prepare_seconds``). Per layout, ``runs`` timed runs (each a fresh
child process: in-memory DuckDB, create the view, Q1..Q6 with ``fetchall()``) give one median row,
and one untimed scan pass (another child) measures what each query read: bytes from
``/proc/self/io`` (Linux only) and rows / files from DuckDB's ``EXPLAIN (ANALYZE, FORMAT JSON)``.
Every layout must give the same ``row_count`` and the same row count per query as the flat layout
(and the raw table); a layout that disagrees or fails gets no row and the exit code is non-zero,
the other layouts go on. Definitions: ``docs/query-optimization.md``.

Usage (what ``.github/workflows/query-opt.yml`` runs)::

    python benchmarks/query_opt.py --runs 10 --git-sha <sha> --out out/query-opt-rows.csv \
        --summary out/query-opt-summary.md
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import duckdb  # noqa: E402

from benchmarks.measure import MeasureError, summarize_runs  # noqa: E402
from benchmarks.naive_duckdb import (  # noqa: E402
    DEFAULT_DATA_ROOT,
    DEFAULT_SEED,
    QUERIES,
    QUERY_DIR,
    Source,
    _peak_rss_mb,
    _sql_str,
    input_files,
    load,
)
from benchmarks.reference import (  # noqa: E402
    REFERENCE_TOML,
    ChecksumError,
    load_reference,
    verify_files,
)
from benchmarks.scale_bench import ScaleBenchError, _check_number  # noqa: E402
from repopulse.runs_log import COLUMNS, RunsLogError, append_run, validate_row  # noqa: E402


@dataclass(frozen=True)
class Layout:
    variant: str
    partition_by: tuple[str, ...]
    sort_by: tuple[str, ...]


LAYOUTS: tuple[Layout, ...] = (
    Layout("qopt-flat", (), ()),
    Layout("qopt-partitioned", ("type",), ()),
    Layout("qopt-clustered", (), ("type", "repo.name", "created_at")),
    Layout("qopt-partitioned-clustered", ("type",), ("repo.name", "created_at")),
)
VARIANTS: tuple[str, ...] = tuple(layout.variant for layout in LAYOUTS)
FLAT = "qopt-flat"
DEFAULT_WORK_DIR = Path("data/tmp/query-opt")
PROC_IO = Path("/proc/self/io")
RUN_TIMEOUT_S = 30 * 60
# Same columns, order and types as naive_duckdb.RAW_COLUMNS; parquet keeps JSON and TIMESTAMP.
VIEW_COLUMNS = "id, type, actor, repo, payload, created_at"

SECONDS_KEYS: tuple[str, ...] = ("total_seconds", *(f"{q}_seconds" for q in QUERIES))
CHECKED_KEYS: tuple[str, ...] = ("row_count", *(f"{q}_rows" for q in QUERIES))
SCAN_COUNT_KEYS: tuple[str, ...] = tuple(
    f"{q}_{kind}" for q in QUERIES for kind in ("scan_rows", "files_read")
)
BYTES_KEYS: tuple[str, ...] = tuple(f"{q}_bytes" for q in QUERIES)
ROW_GROUP_KEYS: tuple[str, ...] = tuple(
    f"{q}_row_groups_{kind}" for q in QUERIES for kind in ("scanned", "total")
)
SAFE_PARTITION_VALUE = re.compile(r"[A-Za-z0-9_]+")


class QueryOptError(RuntimeError):
    """A layout, a run, a measurement or a cross-check of the benchmark failed."""


def _layout(variant: str) -> Layout:
    for layout in LAYOUTS:
        if layout.variant == variant:
            return layout
    raise QueryOptError(f"unknown layout {variant!r}")


def layout_path(work_dir: str | Path, variant: str) -> Path:
    return Path(work_dir) / variant


def layout_files(work_dir: str | Path, variant: str) -> list[Path]:
    return sorted(layout_path(work_dir, variant).rglob("*.parquet"))


# ---------------------------------------------------------------- layouts (not timed)


def _source_sql(work_dir: str | Path, layout: Layout) -> str:
    root = layout_path(work_dir, layout.variant).as_posix()
    if layout.partition_by:
        glob = root + "/*" * len(layout.partition_by) + "/*.parquet"
        types = ", ".join(f"{_sql_str(c)}: 'VARCHAR'" for c in layout.partition_by)
        return f"read_parquet({_sql_str(glob)}, hive_partitioning = true, hive_types = {{{types}}})"
    return f"read_parquet({_sql_str(root + '/*.parquet')}, hive_partitioning = false)"


def _unsorted_rows(con: duckdb.DuckDBPyConnection, path: Path, keys: Sequence[str]) -> int:
    """Rows of ``path`` whose position differs from their position when sorted by ``keys``."""
    order = ", ".join(keys)
    return con.execute(
        "SELECT count(*) FROM (SELECT file_row_number AS pos, "
        f"row_number() OVER (ORDER BY {order}, file_row_number) - 1 AS sorted_pos "
        f"FROM read_parquet({_sql_str(path.as_posix())}, file_row_number = true, "
        "hive_partitioning = false)) WHERE pos <> sorted_pos"
    ).fetchone()[0]


def _partition_values(con: duckdb.DuckDBPyConnection, column: str) -> list[str]:
    """Distinct values of ``column``; each must be safe as a directory name."""
    sql = f"SELECT DISTINCT {column} FROM raw_events ORDER BY 1"
    values = [r[0] for r in con.execute(sql).fetchall()]
    unsafe = [v for v in values if v is None or not SAFE_PARTITION_VALUE.fullmatch(v)]
    if unsafe:
        raise QueryOptError(
            f"{column} value(s) not safe as a partition directory name: "
            + ", ".join(repr(v) for v in unsafe)
        )
    return values


def _remove_old_partitions(root: Path, column: str) -> None:
    """Delete the parquet files of a previous preparation (and their now empty directories)."""
    for part in sorted(root.glob(f"{column}=*")):
        if not part.is_dir():
            continue
        for path in part.glob("*.parquet"):
            path.unlink()
        part.rmdir()  # fails loudly if something else lives there


def _copy(con: duckdb.DuckDBPyConnection, select: str, path: Path) -> None:
    # One COPY into one file keeps the order of the SELECT (DuckDB preserves insertion order for
    # single-file writes); a PARTITION_BY write does not (it is written by several threads).
    con.execute(f"COPY ({select}) TO {_sql_str(path.as_posix())} (FORMAT parquet)")


def _write_layout(
    con: duckdb.DuckDBPyConnection, work_dir: Path, layout: Layout, raw_rows: int
) -> dict:
    root = layout_path(work_dir, layout.variant)
    root.mkdir(parents=True, exist_ok=True)
    order = f" ORDER BY {', '.join(layout.sort_by)}" if layout.sort_by else ""
    start = time.perf_counter()
    if layout.partition_by:
        if len(layout.partition_by) != 1:
            raise QueryOptError(f"{layout.variant}: only one partition column is supported")
        (column,) = layout.partition_by
        values = _partition_values(con, column)
        _remove_old_partitions(root, column)
        for value in values:
            part = root / f"{column}={value}"
            part.mkdir()
            _copy(
                con,
                f"SELECT * EXCLUDE ({column}) FROM raw_events "
                f"WHERE {column} = {_sql_str(value)}{order}",
                part / "data_0.parquet",
            )
    else:
        _copy(con, f"SELECT * FROM raw_events{order}", root / "data.parquet")
    seconds = time.perf_counter() - start
    files = layout_files(work_dir, layout.variant)
    if not files:
        raise QueryOptError(f"{layout.variant}: no parquet file written under {root}")
    rows = con.execute(f"SELECT count(*) FROM {_source_sql(work_dir, layout)}").fetchone()[0]
    if rows != raw_rows:
        raise QueryOptError(f"{layout.variant}: layout holds {rows} rows, raw table {raw_rows}")
    if layout.sort_by:
        for path in files:
            bad = _unsorted_rows(con, path, layout.sort_by)
            if bad:
                raise QueryOptError(
                    f"{layout.variant}: {path.name} not sorted by {', '.join(layout.sort_by)} "
                    f"({bad} rows out of place)"
                )
    return {
        "prepare_seconds": round(seconds, 6),
        "layout_bytes": sum(p.stat().st_size for p in files),
        "files": len(files),
    }


def prepare_layouts(
    data: Source,
    work_dir: str | Path,
    layouts: Sequence[Layout] = LAYOUTS,
    seed: str | Path = DEFAULT_SEED,
) -> dict:
    """Load the raw events once, write every layout; a failing layout does not stop the others.

    Returns ``{"raw_rows": int, "layouts": {variant: info}, "errors": {variant: message}}`` where
    ``info`` holds ``prepare_seconds`` (writing that layout only), ``layout_bytes`` and ``files``.
    """
    files = input_files(data)
    source = data if isinstance(data, str | Path) else files
    work = Path(work_dir)
    infos: dict[str, dict] = {}
    errors: dict[str, str] = {}
    con = duckdb.connect(":memory:")
    try:
        raw_rows = load(con, source, Path(seed))
        for layout in layouts:
            try:
                infos[layout.variant] = _write_layout(con, work, layout, raw_rows)
            except (duckdb.Error, OSError, QueryOptError) as exc:
                errors[layout.variant] = str(exc)
    finally:
        con.close()
    return {"raw_rows": raw_rows, "layouts": infos, "errors": errors}


def create_views(
    con: duckdb.DuckDBPyConnection,
    work_dir: str | Path,
    variant: str,
    seed: str | Path = DEFAULT_SEED,
) -> None:
    """``raw_events`` view over the layout and the ``repos`` seed table (as the baseline)."""
    create_repos(con, seed)
    create_raw_view(con, work_dir, variant)


def create_raw_view(con: duckdb.DuckDBPyConnection, work_dir: str | Path, variant: str) -> None:
    con.execute(
        f"CREATE OR REPLACE VIEW raw_events AS SELECT {VIEW_COLUMNS} "
        f"FROM {_source_sql(work_dir, _layout(variant))}"
    )


def create_repos(con: duckdb.DuckDBPyConnection, seed: str | Path = DEFAULT_SEED) -> None:
    # same table as naive_duckdb.load builds
    con.execute(
        "CREATE OR REPLACE TABLE repos AS SELECT * FROM read_csv("
        f"{_sql_str(Path(seed).as_posix())}, header = true, "
        "columns = {'repo_name': 'VARCHAR', 'category': 'VARCHAR'})"
    )


def _query_sql() -> dict[str, str]:
    return {q: (QUERY_DIR / f"{q}.sql").read_text(encoding="utf-8") for q in QUERIES}


# ---------------------------------------------------------------- timed run (child process)


def run_once(
    work_dir: str | Path,
    variant: str,
    seed: str | Path = DEFAULT_SEED,
    *,
    collect: bool = False,
) -> dict:
    """Create the view + Q1..Q6 once in this process; timings in seconds.

    ``total_seconds`` = creating the ``raw_events`` view + Q1..Q6 (each including ``fetchall()``).
    The ``repos`` seed table is created before the clock starts; ``row_count`` is counted after it
    stopped.
    """
    sql = _query_sql()
    result: dict = {}
    if collect:
        result["results"] = {}
    con = duckdb.connect(":memory:")
    try:
        create_repos(con, seed)
        start = time.perf_counter()
        create_raw_view(con, work_dir, variant)
        for q in QUERIES:
            t0 = time.perf_counter()
            cur = con.execute(sql[q])
            rows = cur.fetchall()
            result[f"{q}_seconds"] = round(time.perf_counter() - t0, 6)
            result[f"{q}_rows"] = len(rows)
            if collect:
                result["results"][q] = {"columns": [c[0] for c in cur.description], "rows": rows}
        result["total_seconds"] = round(time.perf_counter() - start, 6)
        result["row_count"] = con.execute("SELECT count(*) FROM raw_events").fetchone()[0]
    finally:
        con.close()
    result["peak_rss_mb"] = _peak_rss_mb()
    return result


# ---------------------------------------------------------------- scan pass (child process)


def parse_proc_io(text: str) -> dict[str, int]:
    """Counters of a ``/proc/<pid>/io`` text (``name: value`` per line); ``rchar`` required."""
    counters: dict[str, int] = {}
    for line in text.splitlines():
        if not line.strip():
            continue
        match = re.fullmatch(r"([a-z_]+):\s*(\d+)\s*", line)
        if match is None:
            raise QueryOptError(f"/proc io: unparsable line {line!r}")
        name, value = match.group(1), int(match.group(2))
        if name in counters:
            raise QueryOptError(f"/proc io: duplicate counter {name!r}")
        counters[name] = value
    if "rchar" not in counters:
        raise QueryOptError("/proc io: no rchar counter")
    return counters


def read_proc_io(path: str | Path = PROC_IO) -> tuple[dict[str, int], int] | None:
    """Counters of ``path`` and the number of bytes this read itself returned; None if absent.

    The kernel adds a ``read()`` to ``rchar`` after building its text, so the next sample also
    counts the bytes of this one: subtract the returned size from the next delta.
    """
    p = Path(path)
    if not p.is_file():
        return None
    blob = p.read_bytes()
    return parse_proc_io(blob.decode("ascii")), len(blob)


def _is_parquet_scan(node: Mapping) -> bool:
    # DuckDB 1.x: operator_type / operator_name; DuckDB 2.x: type (no operator_name)
    extra = node.get("extra_info") or {}
    kind = node.get("operator_type", node.get("type"))
    name = str(node.get("operator_name", "")).strip()
    return kind == "TABLE_SCAN" and (
        name == "READ_PARQUET"
        or (isinstance(extra, Mapping) and extra.get("Function") == "READ_PARQUET")
    )


def _count(node: Mapping, key: str) -> int:
    value = node.get(key)
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise QueryOptError(f"profile: bad {key} {value!r} on a parquet scan")
    return value


def parse_profile(text: str) -> dict[str, int | None]:
    """Summed over every parquet scan of an ``EXPLAIN (ANALYZE, FORMAT JSON)`` plan.

    ``scan_rows``: rows the scan emitted (``operator_cardinality`` on DuckDB 1.x,
    ``intermediate_rows`` on 2.x). ``files_read``: ``extra_info["Total Files Read"]``.
    ``row_groups_scanned`` / ``row_groups_total``: ``row_groups_scanned`` /
    ``total_row_groups_to_scan`` (DuckDB 2.x only; None when the plan has no such fields).
    """
    try:
        plan = json.loads(text)
    except ValueError:
        raise QueryOptError(f"profile is not JSON: {text[:200]!r}") from None
    if not isinstance(plan, dict):
        raise QueryOptError(f"profile: expected a JSON object, got {type(plan).__name__}")
    scan_rows = files_read = found = 0
    groups: list[tuple[int, int] | None] = []
    stack: list = [plan]
    while stack:
        node = stack.pop()
        if not isinstance(node, Mapping):
            raise QueryOptError(f"profile: plan node is not an object: {node!r}"[:200])
        if _is_parquet_scan(node):
            found += 1
            rows_key = (
                "operator_cardinality" if "operator_cardinality" in node else "intermediate_rows"
            )
            scan_rows += _count(node, rows_key)
            files = (node.get("extra_info") or {}).get("Total Files Read")
            if files is None:
                raise QueryOptError("profile: parquet scan without 'Total Files Read'")
            if not re.fullmatch(r"\d+", str(files)):
                raise QueryOptError(f"profile: bad 'Total Files Read' {files!r}")
            files_read += int(str(files))
            present = [k in node for k in ("row_groups_scanned", "total_row_groups_to_scan")]
            if all(present):
                groups.append(
                    (_count(node, "row_groups_scanned"), _count(node, "total_row_groups_to_scan"))
                )
            elif any(present):
                raise QueryOptError("profile: parquet scan with only one of the row_groups fields")
            else:
                groups.append(None)
        children = node.get("children") or []
        if "operator" in node and node is plan:  # DuckDB 2.x: the operator tree sits here
            children = [*children, *(node.get("operator") or [])]
        stack.extend(children)
    if not found:
        raise QueryOptError("profile: no parquet scan in the plan")
    if all(g is None for g in groups):
        scanned = total = None
    elif any(g is None for g in groups):
        raise QueryOptError("profile: row_groups reported for only some parquet scans")
    else:
        scanned = sum(g[0] for g in groups)
        total = sum(g[1] for g in groups)
    return {
        "scan_rows": scan_rows,
        "files_read": files_read,
        "row_groups_scanned": scanned,
        "row_groups_total": total,
    }


def _fresh_connection(work_dir: str | Path, variant: str, seed: str | Path):
    """New in-memory database (own caches) with DuckDB's external file cache off."""
    con = duckdb.connect(":memory:")
    has_cache = con.execute(
        "SELECT count(*) FROM duckdb_settings() WHERE name = 'enable_external_file_cache'"
    ).fetchone()[0]
    if has_cache:
        con.execute("SET enable_external_file_cache = false")
    create_views(con, work_dir, variant, seed)
    return con


def scan_once(work_dir: str | Path, variant: str, seed: str | Path = DEFAULT_SEED) -> dict:
    """What each of Q1..Q6 reads from the layout, one fresh database per query (not timed).

    ``qN_bytes``: ``rchar`` of ``/proc/self/io`` around ``execute`` + ``fetchall`` (None when the
    file does not exist). ``qN_scan_rows`` / ``qN_files_read``: :func:`parse_profile` of the same
    query under ``EXPLAIN (ANALYZE, FORMAT JSON)`` in another fresh database.
    """
    sql = _query_sql()
    # Warm-up: anything loaded once per process (code, extensions) is loaded before measuring,
    # so the first query does not pay for it.
    con = _fresh_connection(work_dir, variant, seed)
    try:
        for q in QUERIES:
            con.execute(sql[q]).fetchall()
    finally:
        con.close()
    result: dict = {}
    for q in QUERIES:
        con = _fresh_connection(work_dir, variant, seed)
        try:
            before = read_proc_io()
            con.execute(sql[q]).fetchall()
            after = read_proc_io()
        finally:
            con.close()
        if before is None or after is None:
            result[f"{q}_bytes"] = None
        else:
            result[f"{q}_bytes"] = after[0]["rchar"] - before[0]["rchar"] - before[1]
        con = _fresh_connection(work_dir, variant, seed)
        try:
            kind, text = con.execute("EXPLAIN (ANALYZE, FORMAT JSON) " + sql[q]).fetchone()
        finally:
            con.close()
        if kind != "analyzed_plan":
            raise QueryOptError(f"{variant} {q}: unexpected EXPLAIN output {kind!r}")
        profile = parse_profile(text)
        result[f"{q}_scan_rows"] = profile["scan_rows"]
        result[f"{q}_files_read"] = profile["files_read"]
        result[f"{q}_row_groups_scanned"] = profile["row_groups_scanned"]
        result[f"{q}_row_groups_total"] = profile["row_groups_total"]
    return result


# ---------------------------------------------------------------- child output parsers


def _last_json_object(stdout: str, label: str) -> dict:
    lines = [ln for ln in stdout.splitlines() if ln.strip()]
    if not lines:
        raise QueryOptError(f"{label} run: no output")
    try:
        obj = json.loads(lines[-1])
    except ValueError:
        raise QueryOptError(f"{label} run: last line is not JSON: {lines[-1][:200]!r}") from None
    if not isinstance(obj, dict):
        raise QueryOptError(f"{label} run: expected a JSON object, got {type(obj).__name__}")
    return obj


def _check(label: str, obj: Mapping, keys: Sequence[str], *, integer: bool) -> None:
    absent = [k for k in keys if k not in obj]
    if absent:
        raise QueryOptError(f"{label} run: missing key(s): {', '.join(absent)}")
    for key in keys:
        try:
            _check_number(key, obj[key], integer=integer)
        except ScaleBenchError as exc:
            raise QueryOptError(f"{label} run: {exc}") from None


def parse_run_output(stdout: str, label: str) -> dict:
    """The last non-empty stdout line of one timed run, as a checked dict."""
    run = _last_json_object(stdout, label)
    _check(label, run, SECONDS_KEYS, integer=False)
    _check(label, run, CHECKED_KEYS, integer=True)
    if run.get("peak_rss_mb") is not None:
        _check(label, run, ("peak_rss_mb",), integer=False)
    return run


def parse_scan_output(stdout: str, label: str) -> dict:
    """The last non-empty stdout line of one scan pass, as a checked dict."""
    scan = _last_json_object(stdout, label)
    _check(label, scan, SCAN_COUNT_KEYS, integer=True)
    for keys, what in ((BYTES_KEYS, "bytes"), (ROW_GROUP_KEYS, "row_groups")):
        absent = [k for k in keys if k not in scan]
        if absent:
            raise QueryOptError(f"{label} run: missing key(s): {', '.join(absent)}")
        measured = [k for k in keys if scan[k] is not None]
        if measured and len(measured) != len(keys):
            raise QueryOptError(f"{label} run: {what} measured for {', '.join(measured)} only")
        if measured:
            _check(label, scan, keys, integer=True)
    return scan


def parse_prepare_output(stdout: str) -> dict:
    """The last non-empty stdout line of the prepare child, as a checked dict.

    Every layout of :data:`LAYOUTS` must appear exactly once, in ``layouts`` (with
    ``prepare_seconds``, ``layout_bytes``, ``files``) or in ``errors`` (with a message).
    """
    label = "prepare"
    info = _last_json_object(stdout, label)
    absent = [k for k in ("raw_rows", "layouts", "errors") if k not in info]
    if absent:
        raise QueryOptError(f"{label} run: missing key(s): {', '.join(absent)}")
    _check(label, info, ("raw_rows",), integer=True)
    layouts, errors = info["layouts"], info["errors"]
    if not isinstance(layouts, dict) or not isinstance(errors, dict):
        raise QueryOptError(f"{label} run: layouts and errors must be JSON objects")
    unknown = sorted((set(layouts) | set(errors)) - set(VARIANTS))
    if unknown:
        raise QueryOptError(f"{label} run: unknown layout(s): {', '.join(unknown)}")
    both = sorted(set(layouts) & set(errors))
    if both:
        raise QueryOptError(f"{label} run: layout(s) both prepared and failed: {', '.join(both)}")
    missing = [v for v in VARIANTS if v not in layouts and v not in errors]
    if missing:
        raise QueryOptError(f"{label} run: layout(s) not reported: {', '.join(missing)}")
    for variant, layout in layouts.items():
        if not isinstance(layout, dict):
            raise QueryOptError(f"{label} run: {variant}: expected a JSON object")
        _check(f"{label} {variant}", layout, ("prepare_seconds",), integer=False)
        _check(f"{label} {variant}", layout, ("layout_bytes", "files"), integer=True)
    for variant, message in errors.items():
        if not isinstance(message, str):
            raise QueryOptError(f"{label} run: {variant}: error must be a string, got {message!r}")
    return info


def run_in_subprocess(
    mode: str,
    variant: str,
    *,
    work_dir: Path,
    seed: Path,
    timeout_s: float = RUN_TIMEOUT_S,
) -> dict:
    """One timed run (``mode="run"``) or scan pass (``"scan"``) in a fresh interpreter."""
    if mode not in ("run", "scan"):
        raise QueryOptError(f"unknown mode {mode!r}")
    stdout = _run_child(
        ["--single" if mode == "run" else "--scan", "--layout", variant],
        work_dir=work_dir,
        seed=seed,
        label=f"{variant} {mode}",
        timeout_s=timeout_s,
    )
    if mode == "run":
        return parse_run_output(stdout, variant)
    return parse_scan_output(stdout, variant)


def prepare_in_subprocess(
    data_dir: Path, *, work_dir: Path, seed: Path, timeout_s: float = RUN_TIMEOUT_S
) -> dict:
    """:func:`prepare_layouts` in a fresh interpreter, so the parent never holds the data.

    On Linux a child's ``ru_maxrss`` starts at the high-water mark of the process it was started
    from (the kernel keeps the old address space's peak across ``exec``); a parent that had
    loaded the day would therefore show up as the ``peak_rss_mb`` of every run.
    """
    stdout = _run_child(
        ["--prepare", "--data-dir", str(data_dir)],
        work_dir=work_dir,
        seed=seed,
        label="prepare",
        timeout_s=timeout_s,
    )
    return parse_prepare_output(stdout)


def _run_child(args: list[str], *, work_dir: Path, seed: Path, label: str, timeout_s: float) -> str:
    cmd = [
        sys.executable,
        str(Path(__file__).resolve()),
        *args,
        "--work-dir",
        str(work_dir),
        "--seed",
        str(seed),
    ]
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout_s, check=False)
    except subprocess.TimeoutExpired:
        raise QueryOptError(f"{label} timed out after {timeout_s:.0f} s") from None
    if proc.returncode != 0:
        tail = "\n".join(proc.stderr.strip().splitlines()[-20:])
        raise QueryOptError(f"{label} failed (exit {proc.returncode}):\n{tail}")
    return proc.stdout


# ---------------------------------------------------------------- rows


def check_against(
    variant: str, run: Mapping[str, object], reference: Mapping[str, object], *, reference_name: str
) -> None:
    """``run`` saw as many rows as ``reference`` and every query returned as many rows."""
    diffs = [
        f"{key} {variant}={run[key]} {reference_name}={reference[key]}"
        for key in CHECKED_KEYS
        if int(run[key]) != int(reference[key])
    ]
    if diffs:
        raise QueryOptError(f"{variant}: counts differ from {reference_name}: " + "; ".join(diffs))


def _keys_note(keys: Sequence[str]) -> str:
    return "+".join(keys) if keys else "none"


def summarize_layout(
    runs: Sequence[Mapping[str, object]],
    scan: Mapping[str, object],
    *,
    variant: str,
    git_sha: str,
    data_date: str,
    run_at: str,
    layout: Mapping[str, object],
    cross_check: str = "full",
) -> dict[str, object]:
    """Median row of one layout; ``bytes_read`` = bytes read by Q1..Q6 in the scan pass."""
    if not runs:
        raise QueryOptError(f"{variant}: no runs to summarise")
    for key in CHECKED_KEYS:
        values = {int(r[key]) for r in runs}
        if len(values) != 1:
            raise QueryOptError(f"{variant}: {key} differs between runs: {sorted(values)}")
    try:
        row = summarize_runs(
            [{**r, "bytes_read": 0} for r in runs],  # bytes come from the scan pass, below
            variant=variant,
            git_sha=git_sha,
            data_date=data_date,
            run_at=run_at,
        )
    except MeasureError as exc:
        raise QueryOptError(f"{variant}: {exc}") from None
    measured = [scan[k] for k in BYTES_KEYS if scan.get(k) is not None]
    row["bytes_read"] = sum(int(v) for v in measured) if measured else None
    shape = _layout(variant)
    notes = [
        f"runs={len(runs)}",
        "stat=median",
        f"partition_by={_keys_note(shape.partition_by)}",
        f"sort_by={_keys_note(shape.sort_by)}",
        f"files={int(layout['files'])}",
        f"layout_bytes={int(layout['layout_bytes'])}",
        f"prepare_seconds={round(float(layout['prepare_seconds']), 3)}",
        f"duckdb={duckdb.__version__}",
        f"bytes={'proc_rchar' if measured else 'unavailable'}",
        f"cross_check={cross_check}",
    ]
    for q in QUERIES:
        if measured:
            notes.append(f"{q}_bytes={int(scan[f'{q}_bytes'])}")
        notes.append(f"{q}_scan_rows={int(scan[f'{q}_scan_rows'])}")
        notes.append(f"{q}_files_read={int(scan[f'{q}_files_read'])}")
        for kind in ("scanned", "total"):  # DuckDB 2.x only
            value = scan.get(f"{q}_row_groups_{kind}")
            if value is not None:
                notes.append(f"{q}_row_groups_{kind}={int(value)}")
    row["notes"] = " ".join(notes)
    validate_row(row)
    return row


# ---------------------------------------------------------------- CLI


def _write_summary(path: Path, status: list[tuple[str, str]], rows: list[dict]) -> None:
    lines = ["### Query optimization (Q1-Q6 on parquet layouts, GH Archive real data)", ""]
    lines += ["| layout | status |", "|---|---|"]
    lines += [f"| `{cell}` | {text.splitlines()[0] if text else ''} |" for cell, text in status]
    lines += ["", f"{len(rows)} row(s) for runs.csv:", "", "```csv", ",".join(COLUMNS)]
    lines += [",".join(validate_row(r)[c] for c in COLUMNS) for r in rows]
    lines += ["```", ""]
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines), encoding="utf-8")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Q1-Q6 on partitioned / clustered parquet.")
    parser.add_argument("--data-dir", type=Path, help="default: data/raw/gharchive/<data_date>")
    parser.add_argument("--reference", type=Path, default=REFERENCE_TOML)
    parser.add_argument("--seed", type=Path, default=DEFAULT_SEED)
    parser.add_argument("--work-dir", type=Path, default=DEFAULT_WORK_DIR, help="parquet layouts")
    parser.add_argument("--runs", type=int, default=10)
    parser.add_argument("--git-sha", default=None)
    parser.add_argument("--out", type=Path, default=Path("out/query-opt-rows.csv"))
    parser.add_argument("--summary", type=Path, default=Path("out/query-opt-summary.md"))
    parser.add_argument("--single", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--scan", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--prepare", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--layout", choices=VARIANTS, help=argparse.SUPPRESS)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.prepare:
        if not args.data_dir:
            parser.error("--data-dir is required with --prepare")
        print(json.dumps(prepare_layouts(args.data_dir, args.work_dir, seed=args.seed)))
        return 0
    if args.single or args.scan:
        if not args.layout:
            parser.error("--layout is required with --single/--scan")
        fn = run_once if args.single else scan_once
        print(json.dumps(fn(args.work_dir, args.layout, args.seed)))
        return 0

    run_at = datetime.now(UTC).isoformat(timespec="seconds")
    try:  # fail on bad metadata now, not after the runs
        if not args.git_sha:
            raise RunsLogError("git_sha: --git-sha is required")
        if args.runs < 1:
            raise RunsLogError("runs: --runs must be >= 1")
        ref = load_reference(args.reference)
        for variant in VARIANTS:
            validate_row(
                {
                    "run_at": run_at,
                    "git_sha": args.git_sha,
                    "variant": variant,
                    "data_date": ref["data_date"],
                    "total_seconds": 0,
                }
            )
    except RunsLogError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2
    data_dir = args.data_dir or DEFAULT_DATA_ROOT / ref["data_date"]
    try:
        expected = {p.name for p in verify_files(data_dir, ref)}
        extra = sorted(p.name for p in Path(data_dir).glob("*.json.gz") if p.name not in expected)
        if extra:
            raise ChecksumError(f"files not listed in the reference: {', '.join(extra)}")
    except ChecksumError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2

    try:  # in a child of its own: the parent must stay small (see prepare_in_subprocess)
        prepared = prepare_in_subprocess(data_dir, work_dir=args.work_dir, seed=args.seed)
        print(f"prepared layouts from {prepared['raw_rows']} raw rows", flush=True)
    except QueryOptError as exc:
        prepared = {"raw_rows": 0, "layouts": {}, "errors": dict.fromkeys(VARIANTS, str(exc))}

    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(",".join(COLUMNS) + "\n", encoding="utf-8", newline="")
    rows: list[dict] = []
    status: list[tuple[str, str]] = []
    failed = False

    def fail(variant: str, message: str) -> None:
        nonlocal failed
        failed = True
        status.append((variant, f"FAILED: {message}"))
        print(f"ERROR {variant}: {message}", file=sys.stderr, flush=True)

    measured: dict[str, tuple[list[dict], dict]] = {}
    for variant in VARIANTS:
        if variant in prepared["errors"]:
            fail(variant, f"prepare: {prepared['errors'][variant]}")
            continue
        kwargs = {"work_dir": args.work_dir, "seed": args.seed, "timeout_s": RUN_TIMEOUT_S}
        try:
            runs = []
            for i in range(1, args.runs + 1):
                run = run_in_subprocess("run", variant, **kwargs)
                print(
                    f"{variant} run {i}/{args.runs}: total {run['total_seconds']:.3f}s "
                    f"rows {run['row_count']}",
                    flush=True,
                )
                runs.append(run)
            scan = run_in_subprocess("scan", variant, **kwargs)
        except QueryOptError as exc:
            fail(variant, str(exc))
            continue
        measured[variant] = (runs, scan)

    flat = measured.get(FLAT)
    for variant, (runs, scan) in measured.items():
        try:
            if int(runs[0]["row_count"]) != prepared["raw_rows"]:
                raise QueryOptError(
                    f"{variant}: row_count {runs[0]['row_count']} != raw table "
                    f"{prepared['raw_rows']}"
                )
            if flat is not None and variant != FLAT:
                check_against(variant, runs[0], flat[0][0], reference_name=FLAT)
            row = summarize_layout(
                runs,
                scan,
                variant=variant,
                git_sha=args.git_sha,
                data_date=ref["data_date"],
                run_at=run_at,
                layout=prepared["layouts"][variant],
                cross_check="full" if flat is not None else "row_count_only",
            )
        except (QueryOptError, RunsLogError) as exc:
            fail(variant, str(exc))
            continue
        append_run(args.out, row)
        rows.append(row)
        status.append((variant, f"ok, median total {row['total_seconds']} s"))

    _write_summary(args.summary, status, rows)
    print(f"wrote {len(rows)} row(s) to {args.out}")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
