"""Export the public Q1-Q6 marts and runs.csv for the static dashboard and Tableau.

    python -m repopulse.export_site --db ci.duckdb --runs runs.csv --out _site
    python -m repopulse.export_site --db md:repopulse --runs runs.csv --out _site --git-sha <sha>

Reads only the ``public`` schema (``main_public``; the ``public`` role of transform/governance.yml)
and only the columns of ``MART_COLUMNS``. Writes ``<out>/`` = a copy of ``site/`` plus
``data/<mart>.json``, ``data/runs.json``, ``data/meta.json`` and ``tableau/<mart>.csv``,
``tableau/runs.csv``. Everything is read and checked before anything is written: a missing schema,
mart or column, or a runs.csv whose header is not ``runs_log.COLUMNS``, fails the build (exit 1)
and leaves ``<out>`` as it was. The output is deterministic: same input and ``--generated-at``
give byte-identical files.

A local database is opened read-only. ``md:`` paths go to ``duckdb.connect`` unchanged; DuckDB reads
``MOTHERDUCK_TOKEN`` from the environment itself, and this module never reads or prints it.
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import math
import os
import re
import shutil
import sys
import tempfile
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path

import duckdb

from repopulse import runs_log

SITE_DIR = Path(__file__).resolve().parents[1] / "site"
DEFAULT_SCHEMA = "main_public"

# Column contract of the six public marts = transform/models/public/_public.yml (a test keeps the
# two equal). Key order is Q1..Q6.
MART_COLUMNS: dict[str, tuple[str, ...]] = {
    "mart_daily_active_contributors": (
        "period",
        "period_start",
        "scope",
        "scope_key",
        "active_contributors",
        "new_contributors",
        "returning_contributors",
    ),
    "mart_cohort_retention": (
        "cohort_week",
        "cohort_size",
        "retained_w4",
        "retained_w12",
        "retention_w4_pct",
        "retention_w12_pct",
    ),
    "mart_contribution_funnel": (
        "repo_name",
        "category",
        "forkers",
        "forkers_with_pr",
        "forkers_with_merge",
        "prs_opened",
        "prs_merged",
        "merge_hours_p50",
        "merge_hours_p90",
    ),
    "mart_responsiveness": (
        "repo_name",
        "item_type",
        "opened",
        "responded",
        "response_hours_p50",
        "response_hours_p90",
        "closed",
        "close_rate_pct",
    ),
    "mart_repo_concentration": (
        "repo_name",
        "category",
        "contributions",
        "bot_contributions",
        "bot_share_pct",
        "human_contributors",
        "top1_share_pct",
        "top2_share_pct",
        "top3_share_pct",
    ),
    "mart_repo_health_score": (
        "health_rank",
        "repo_name",
        "category",
        "activity_score",
        "retention_score",
        "merge_score",
        "response_score",
        "close_score",
        "diversity_score",
        "health_score",
    ),
}
QUERY = {mart: f"q{i}" for i, mart in enumerate(MART_COLUMNS, start=1)}
# Unique key of each mart (= ORDER BY of benchmarks/queries/qN.sql), so the row order is stable.
ORDER_BY: dict[str, tuple[str, ...]] = {
    "mart_daily_active_contributors": ("period", "scope", "scope_key", "period_start"),
    "mart_cohort_retention": ("cohort_week",),
    "mart_contribution_funnel": ("repo_name",),
    "mart_responsiveness": ("repo_name", "item_type"),
    "mart_repo_concentration": ("repo_name",),
    "mart_repo_health_score": ("health_rank",),
}
SOURCE = {
    "name": "GH Archive",
    "url": "https://www.gharchive.org/",
    "real_data": True,
    "description": "Public GitHub events (real data), loaded hourly by an incremental batch.",
}
_GIT_SHA = re.compile(r"[0-9a-f]{7,40}")
_MOTHERDUCK = ("md:", "motherduck:")


class ExportError(RuntimeError):
    """The inputs do not satisfy the contract; nothing was written."""


def _ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _value(v: object) -> object:
    """DuckDB value -> JSON value (NaN/inf -> null, Decimal -> float, dates -> ISO text)."""
    if v is None or isinstance(v, bool | int | str):
        return v
    if isinstance(v, float | Decimal):
        f = float(v)
        return f if math.isfinite(f) else None
    if isinstance(v, datetime | date):
        return v.isoformat()
    return str(v)


def _csv_cell(v: object) -> str:
    if v is None:
        return ""
    if isinstance(v, bool):
        return "true" if v else "false"
    return str(v)


def _connect(db: str):
    if db.strip().lower().startswith(_MOTHERDUCK):
        return duckdb.connect(db)
    if not Path(db).is_file():
        raise ExportError(f"database file not found: {db}")
    return duckdb.connect(db, read_only=True)


def _check_contract(con, schema: str) -> None:
    found = con.execute(
        "SELECT count(*) FROM information_schema.schemata "
        "WHERE schema_name = ? AND catalog_name = current_database()",
        [schema],
    ).fetchone()[0]
    if not found:
        raise ExportError(f"schema {schema} does not exist in the database")
    columns: dict[str, set[str]] = {}
    for table, column in con.execute(
        "SELECT table_name, column_name FROM information_schema.columns "
        "WHERE table_schema = ? AND table_catalog = current_database()",
        [schema],
    ).fetchall():
        columns.setdefault(table, set()).add(column)
    missing_marts = [m for m in MART_COLUMNS if m not in columns]
    missing_cols = [
        f"{m}.{c}"
        for m, cols in MART_COLUMNS.items()
        if m in columns
        for c in cols
        if c not in columns[m]
    ]
    problems = []
    if missing_marts:
        problems.append(f"missing mart(s) in schema {schema}: {', '.join(missing_marts)}")
    if missing_cols:
        problems.append(f"missing column(s): {', '.join(missing_cols)}")
    if problems:
        raise ExportError("; ".join(problems))


def read_marts(db: str, schema: str) -> dict[str, list[dict[str, object]]]:
    """Check the contract and return the contract columns of every mart, in a stable order."""
    try:
        con = _connect(db)
    except duckdb.Error as exc:
        raise ExportError(f"cannot open database {db}: {exc}") from None
    try:
        _check_contract(con, schema)
        out: dict[str, list[dict[str, object]]] = {}
        for mart, cols in MART_COLUMNS.items():
            sql = (
                f"SELECT {', '.join(_ident(c) for c in cols)} "
                f"FROM {_ident(schema)}.{_ident(mart)} "
                f"ORDER BY {', '.join(_ident(c) for c in ORDER_BY[mart])}"
            )
            out[mart] = [
                {c: _value(v) for c, v in zip(cols, row, strict=True)}
                for row in con.execute(sql).fetchall()
            ]
        return out
    except duckdb.Error as exc:
        raise ExportError(f"reading schema {schema} failed: {exc}") from None
    finally:
        con.close()


def read_runs(path: str | os.PathLike[str]) -> tuple[bytes, list[dict[str, str | None]]]:
    """Raw bytes of runs.csv and its rows (empty cell -> None); the header must be COLUMNS."""
    p = Path(path)
    if not p.is_file():
        raise ExportError(f"runs.csv not found: {p}")
    raw = p.read_bytes()
    reader = csv.reader(io.StringIO(raw.decode("utf-8"), newline=""))
    header = next(reader, None)
    if header != list(runs_log.COLUMNS):
        raise ExportError(f"runs.csv header of {p} does not match runs_log.COLUMNS: {header!r}")
    rows = []
    for line_no, cells in enumerate(reader, start=2):
        if not cells:
            continue
        if len(cells) != len(header):
            raise ExportError(f"runs.csv line {line_no} of {p} has {len(cells)} cells")
        rows.append({c: (v if v != "" else None) for c, v in zip(header, cells, strict=True)})
    return raw, rows


def _generated_at(value: str | None) -> str:
    if value is None:
        return datetime.now(UTC).replace(microsecond=0).isoformat()
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        raise ExportError(f"--generated-at is not an ISO-8601 timestamp: {value!r}") from None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ExportError(f"--generated-at must include a timezone: {value!r}")
    return parsed.astimezone(UTC).isoformat()


def _latest_data_date(q1_rows: list[dict[str, object]]) -> str | None:
    days = [r["period_start"] for r in q1_rows if r["period"] == "day" and r["period_start"]]
    return max(days) if days else None


def _json_bytes(doc: object) -> bytes:
    text = json.dumps(doc, ensure_ascii=False, separators=(",", ":"), allow_nan=False)
    return (text + "\n").encode("utf-8")


def _csv_bytes(columns: tuple[str, ...], rows: list[dict[str, object]]) -> bytes:
    buf = io.StringIO()
    writer = csv.writer(buf, lineterminator="\n")
    writer.writerow(columns)
    for row in rows:
        writer.writerow(_csv_cell(row[c]) for c in columns)
    return buf.getvalue().encode("utf-8")


def _check_out(out: Path) -> None:
    if not out.exists():
        return
    if not out.is_dir():
        raise ExportError(f"--out {out} exists and is not a directory")
    if any(out.iterdir()) and not (out / "data" / "meta.json").is_file():
        raise ExportError(f"--out {out} is not empty and is not a previous export; not touching it")


def build_files(
    marts: dict[str, list[dict[str, object]]],
    runs_raw: bytes,
    runs_rows: list[dict[str, str | None]],
    schema: str,
    generated_at: str,
    git_sha: str | None,
) -> dict[str, bytes]:
    """Relative path -> content of every generated (non-site) file."""
    files: dict[str, bytes] = {}
    meta_marts = {}
    for mart, rows in marts.items():
        cols = MART_COLUMNS[mart]
        files[f"data/{mart}.json"] = _json_bytes(
            {"mart": mart, "query": QUERY[mart], "columns": list(cols), "rows": rows}
        )
        files[f"tableau/{mart}.csv"] = _csv_bytes(cols, rows)
        meta_marts[mart] = {
            "query": QUERY[mart],
            "rows": len(rows),
            "json": f"data/{mart}.json",
            "csv": f"tableau/{mart}.csv",
        }
    files["data/runs.json"] = _json_bytes({"columns": list(runs_log.COLUMNS), "rows": runs_rows})
    files["tableau/runs.csv"] = runs_raw
    files["data/meta.json"] = _json_bytes(
        {
            "generated_at": generated_at,
            "git_sha": git_sha,
            "schema": schema,
            "source": SOURCE,
            "simulated_sections": [],
            "latest_data_date": _latest_data_date(marts["mart_daily_active_contributors"]),
            "marts": meta_marts,
            "runs": {"rows": len(runs_rows), "json": "data/runs.json", "csv": "tableau/runs.csv"},
        }
    )
    return files


def export(
    db: str,
    runs: str | os.PathLike[str],
    out: str | os.PathLike[str],
    schema: str = DEFAULT_SCHEMA,
    site_dir: str | os.PathLike[str] = SITE_DIR,
    generated_at: str | None = None,
    git_sha: str | None = None,
) -> dict[str, int]:
    """Read and check everything, then replace ``out`` in one move. Returns rows per mart."""
    out_path = Path(out)
    site = Path(site_dir)
    stamp = _generated_at(generated_at)
    if git_sha is not None and not _GIT_SHA.fullmatch(git_sha):
        raise ExportError(f"--git-sha must be 7-40 lowercase hex characters: {git_sha!r}")
    if not (site / "index.html").is_file():
        raise ExportError(f"site directory has no index.html: {site}")
    _check_out(out_path)
    runs_raw, runs_rows = read_runs(runs)
    marts = read_marts(str(db), schema)
    files = build_files(marts, runs_raw, runs_rows, schema, stamp, git_sha)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = Path(tempfile.mkdtemp(prefix=f".{out_path.name}.tmp-", dir=out_path.parent))
    try:
        shutil.copytree(site, tmp, dirs_exist_ok=True)
        for rel, content in files.items():
            target = tmp / rel
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(content)
        if out_path.exists():
            old = Path(tempfile.mkdtemp(prefix=f".{out_path.name}.old-", dir=out_path.parent))
            old.rmdir()
            out_path.rename(old)
            tmp.rename(out_path)
            shutil.rmtree(old)
        else:
            tmp.rename(out_path)
    except BaseException:
        shutil.rmtree(tmp, ignore_errors=True)
        raise
    return {mart: len(rows) for mart, rows in marts.items()}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m repopulse.export_site",
        description="Export the public Q1-Q6 marts and runs.csv for the dashboard and Tableau.",
    )
    parser.add_argument("--db", required=True, help="DuckDB file or md:<database> (MotherDuck)")
    parser.add_argument("--runs", default="runs.csv", help="runs.csv to publish")
    parser.add_argument("--out", required=True, help="output directory (replaced as a whole)")
    parser.add_argument("--schema", default=DEFAULT_SCHEMA, help="public schema to read")
    parser.add_argument("--site-dir", default=str(SITE_DIR), help="static page sources")
    parser.add_argument("--generated-at", help="ISO-8601 timestamp with timezone (default: now)")
    parser.add_argument("--git-sha", help="commit the site was built from")
    args = parser.parse_args(argv)
    try:
        counts = export(
            db=args.db,
            runs=args.runs,
            out=args.out,
            schema=args.schema,
            site_dir=args.site_dir,
            generated_at=args.generated_at,
            git_sha=args.git_sha,
        )
    except ExportError as exc:
        print(f"export_site: {exc}", file=sys.stderr)
        return 1
    summary = ", ".join(f"{QUERY[m]}={n}" for m, n in counts.items())
    print(f"export_site: wrote {args.out} (rows: {summary})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
