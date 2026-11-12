"""Trino + Hive Metastore + MinIO benchmark: Q1-Q6 on Trino over the four query_opt layouts.

The reference day (``benchmarks/reference.toml``, real GH Archive data) is written once per
layout by :func:`benchmarks.query_opt.prepare_layouts` (same parquet files, same names), the seed
``repos`` table is written as one parquet file, everything is uploaded unchanged to a MinIO bucket
and registered in the Hive Metastore as external Parquet tables, one schema per layout:

- ``trino-flat`` (the "before"): schema ``trino_flat``, parquet ``qopt-flat``
- ``trino-partitioned``: schema ``trino_partitioned``, parquet ``qopt-partitioned``
- ``trino-clustered``: schema ``trino_clustered``, parquet ``qopt-clustered``
- ``trino-partitioned-clustered``: schema ``trino_partitioned_clustered``, parquet
  ``qopt-partitioned-clustered``

Every schema holds ``raw_events`` (the layout) and ``repos`` (the seed).

``benchmarks/queries_trino/q1..q6.sql`` run verbatim on every layout: the session schema picks
the layout. Per layout: one untimed warm-up of Q1..Q6 (its results are cross-checked against
DuckDB on the same parquet), then ``--runs`` timed runs, one median row. Times are client-side
(HTTP request to last page); bytes are Trino's ``physicalInputBytes``. Preparing, uploading and
the DDL are not timed. The Docker stack (``benchmarks/trino/docker-compose.yml``) runs only on
the GitHub runner. Definitions: ``docs/trino-hive.md``.

Usage (what ``.github/workflows/trino-bench.yml`` runs, after ``docker compose up -d --wait``)::

    python benchmarks/trino_bench.py --runs 10 --git-sha <sha> --out out/trino-rows.csv \
        --summary out/trino-summary.md --samples-dir out/trino-samples

``--print-ddl`` prints the statements of every layout and exits (no Docker needed).
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import math
import os
import re
import subprocess
import sys
import time
import urllib.error
import urllib.request
from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import duckdb  # noqa: E402

from benchmarks import query_opt  # noqa: E402
from benchmarks.measure import MeasureError, summarize_runs  # noqa: E402
from benchmarks.naive_duckdb import (  # noqa: E402
    DEFAULT_DATA_ROOT,
    DEFAULT_SEED,
    QUERIES,
    _sql_str,
)
from benchmarks.reference import (  # noqa: E402
    REFERENCE_TOML,
    ChecksumError,
    load_reference,
    verify_files,
)
from repopulse.runs_log import COLUMNS, RunsLogError, append_run, validate_row  # noqa: E402

QUERY_DIR = Path(__file__).resolve().parent / "queries_trino"
COMPOSE_FILE = Path(__file__).resolve().parent / "trino" / "docker-compose.yml"
LAYOUT_OF: dict[str, str] = {
    "trino-flat": "qopt-flat",
    "trino-partitioned": "qopt-partitioned",
    "trino-clustered": "qopt-clustered",
    "trino-partitioned-clustered": "qopt-partitioned-clustered",
}
VARIANTS: tuple[str, ...] = tuple(LAYOUT_OF)
SCHEMAS: tuple[str, ...] = tuple(v.replace("-", "_") for v in VARIANTS)
REFERENCE_LAYOUT = "qopt-flat"  # DuckDB answers on this layout are the expected results
SAMPLE_QUERY = "q3"
SAMPLE_VARIANTS = ("trino-flat", "trino-partitioned")
CATALOG = "hive"
DEFAULT_BUCKET = "repopulse-bench"
DEFAULT_URL = "http://localhost:8080"
DEFAULT_WORK_DIR = Path("data/tmp/trino-bench")
REPOS_DIR = "repos"
HTTP_TIMEOUT_S = 30 * 60
READY_TIMEOUT_S = 10 * 60
RETRY_STATUSES = (502, 503, 504)
MAX_RETRIES = 20

# Same columns, order and types as the parquet DuckDB writes (naive_duckdb.RAW_COLUMNS): the JSON
# payload is stored as a string, so Trino reads it as varchar; created_at has whole seconds.
RAW_EVENTS_COLUMNS: tuple[tuple[str, str], ...] = (
    ("id", "varchar"),
    ("type", "varchar"),
    ("actor", "ROW(id bigint, login varchar)"),
    ("repo", "ROW(id bigint, name varchar)"),
    ("payload", "varchar"),
    ("created_at", "timestamp(3)"),
)
REPOS_COLUMNS: tuple[tuple[str, str], ...] = (("repo_name", "varchar"), ("category", "varchar"))
COUNT_SQL = "SELECT count(*) FROM raw_events"
PARTITION_COUNT_SQL = 'SELECT count(*) FROM "raw_events$partitions"'
STAT_KEYS: tuple[str, ...] = (
    "state",
    "elapsedTimeMillis",
    "cpuTimeMillis",
    "processedRows",
    "processedBytes",
    "physicalInputBytes",
    "peakMemoryBytes",
)
INPUT_KEYS: tuple[str, ...] = ("physicalInputBytes", "processedRows")  # per query, into notes
CHECKED_KEYS: tuple[str, ...] = ("row_count", *(f"{q}_rows" for q in QUERIES))

IDENTIFIER = re.compile(r"[a-z_][a-z0-9_]*")
BUCKET = re.compile(r"[a-z0-9][a-z0-9-]{1,61}[a-z0-9]")
LOCATION = re.compile(r"s3a://[a-z0-9][a-z0-9-]{1,61}[a-z0-9](/[A-Za-z0-9_-]+)*")
PATH_PART = re.compile(r"[A-Za-z0-9_-]+")
NOTE_VALUE = re.compile(r"[^\s,=]+")

Transport = Callable[[str, str, Mapping[str, str], bytes | None, float], tuple[int, bytes]]


class TrinoBenchError(RuntimeError):
    """A layout, a statement, a response or a cross-check of the Trino benchmark failed."""


def schema_name(variant: str) -> str:
    if variant not in LAYOUT_OF:
        raise TrinoBenchError(f"unknown Trino variant {variant!r}")
    return SCHEMAS[VARIANTS.index(variant)]


def query_sql() -> dict[str, str]:
    return {q: (QUERY_DIR / f"{q}.sql").read_text(encoding="utf-8") for q in QUERIES}


# ---------------------------------------------------------------- DDL (pure)


def _identifier(kind: str, value: str) -> str:
    if not isinstance(value, str) or not IDENTIFIER.fullmatch(value):
        raise TrinoBenchError(f"unsafe {kind} name {value!r}: must match {IDENTIFIER.pattern}")
    return value


def s3_location(bucket: str, *parts: str) -> str:
    if not isinstance(bucket, str) or not BUCKET.fullmatch(bucket):
        raise TrinoBenchError(f"unsafe bucket name {bucket!r}: must match {BUCKET.pattern}")
    for part in parts:
        if not isinstance(part, str) or not PATH_PART.fullmatch(part):
            raise TrinoBenchError(f"unsafe path part {part!r}: must match {PATH_PART.pattern}")
    return "/".join([f"s3a://{bucket}", *parts])


def _location(value: str) -> str:
    if not isinstance(value, str) or not LOCATION.fullmatch(value):
        raise TrinoBenchError(f"unsafe location {value!r}: must match {LOCATION.pattern}")
    return value


def create_table_sql(
    schema: str,
    table: str,
    columns: Sequence[tuple[str, str]],
    *,
    location: str,
    partition_by: Sequence[str] = (),
    catalog: str = CATALOG,
) -> str:
    """``CREATE TABLE`` of an external Parquet table; partition columns go last (Trino rule)."""
    name = ".".join(
        (
            _identifier("catalog", catalog),
            _identifier("schema", schema),
            _identifier("table", table),
        )
    )
    names = [_identifier("column", c) for c, _ in columns]
    parts = [_identifier("partition column", p) for p in partition_by]
    absent = [p for p in parts if p not in names]
    if absent:
        raise TrinoBenchError(
            f"partition column(s) not among the columns of {table}: "
            + ", ".join(repr(p) for p in absent)
        )
    ordered = [c for c in columns if c[0] not in parts] + [
        next(c for c in columns if c[0] == p) for p in parts
    ]
    props = [f"external_location = {_sql_str(_location(location))}", "format = 'PARQUET'"]
    if parts:
        props.append("partitioned_by = ARRAY[" + ", ".join(_sql_str(p) for p in parts) + "]")
    cols = ", ".join(f"{c} {kind}" for c, kind in ordered)
    return f"CREATE TABLE {name} ({cols}) WITH ({', '.join(props)})"


def sync_partitions_sql(schema: str, table: str, *, catalog: str = CATALOG) -> str:
    """Register every ``<col>=<value>/`` directory under the table location (mode ``ADD``)."""
    return (
        f"CALL {_identifier('catalog', catalog)}.system.sync_partition_metadata("
        f"{_sql_str(_identifier('schema', schema))}, {_sql_str(_identifier('table', table))}, "
        "'ADD')"
    )


def layout_ddl(variant: str, *, bucket: str = DEFAULT_BUCKET, catalog: str = CATALOG) -> list[str]:
    """Statements that (re)register one layout: schema, ``raw_events``, ``repos``, partitions."""
    schema = schema_name(variant)
    layout = query_opt._layout(LAYOUT_OF[variant])
    full = f"{_identifier('catalog', catalog)}.{schema}"
    stmts = [
        f"CREATE SCHEMA IF NOT EXISTS {full} "
        f"WITH (location = {_sql_str(s3_location(bucket, 'schemas', schema))})",
        f"DROP TABLE IF EXISTS {full}.raw_events",
        f"DROP TABLE IF EXISTS {full}.repos",
        create_table_sql(
            schema,
            "raw_events",
            RAW_EVENTS_COLUMNS,
            location=s3_location(bucket, layout.variant),
            partition_by=layout.partition_by,
            catalog=catalog,
        ),
        create_table_sql(
            schema, "repos", REPOS_COLUMNS, location=s3_location(bucket, REPOS_DIR), catalog=catalog
        ),
    ]
    if layout.partition_by:
        stmts.append(sync_partitions_sql(schema, "raw_events", catalog=catalog))
    return stmts


def partition_values(root: str | Path, column: str) -> list[str]:
    """Values of the ``<column>=<value>`` directories under ``root``; anything else is an error."""
    values: list[str] = []
    for path in sorted(Path(root).iterdir()):
        if not path.is_dir():
            continue
        prefix, sep, value = path.name.partition("=")
        if prefix != column or not sep:
            raise TrinoBenchError(f"{root}: directory {path.name!r} is not a {column}= partition")
        if not query_opt.SAFE_PARTITION_VALUE.fullmatch(value):
            raise TrinoBenchError(f"{root}: unsafe partition value {value!r}")
        values.append(value)
    return values


# ---------------------------------------------------------------- REST responses (pure)


def _count(label: str, key: str, value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise TrinoBenchError(f"{label}: stats {key} must be an integer >= 0, got {value!r}")
    return value


def _error_text(error: object) -> str:
    if not isinstance(error, Mapping):
        return repr(error)
    return f"{error.get('errorName', 'UNKNOWN_ERROR')}: {error.get('message', '')}"


def _convert(label: str, column: str, kind: str, value: object) -> object:
    if value is None:
        return None
    base = kind.split("(", 1)[0].strip().lower()
    try:
        if base in ("bigint", "integer", "smallint", "tinyint"):
            if isinstance(value, bool) or not isinstance(value, int):
                raise TypeError
            return value
        if base in ("double", "real"):
            if isinstance(value, str) and value in ("NaN", "Infinity", "-Infinity"):
                return float(value)
            if isinstance(value, bool) or not isinstance(value, int | float):
                raise TypeError
            return float(value)
        if base == "decimal":
            return Decimal(str(value))
        if base == "boolean":
            if not isinstance(value, bool):
                raise TypeError
            return value
        if base == "date":
            return dt.date.fromisoformat(value)
        if base == "timestamp" and "with time zone" not in kind:
            return dt.datetime.fromisoformat(value)
    except (TypeError, ValueError):
        raise TrinoBenchError(
            f"{label}: column {column!r} of type {kind} has value {value!r}"
        ) from None
    return value


def parse_pages(pages: Sequence[object], label: str) -> dict:
    """Columns, rows and final stats of one statement from its ``QueryResults`` pages.

    Rows of every page are joined in order and converted by column type (``date``, ``timestamp``,
    numbers, ``boolean``). The last page must have no ``nextUri``, ``stats`` with every key of
    :data:`STAT_KEYS` (integers >= 0) and state ``FINISHED``; any ``error`` raises.
    """
    if not isinstance(pages, Sequence) or not pages:
        raise TrinoBenchError(f"{label}: no response pages")
    columns: list[dict] | None = None
    rows: list[tuple] = []
    for i, page in enumerate(pages):
        if not isinstance(page, Mapping):
            raise TrinoBenchError(f"{label}: page {i} is not a JSON object")
        if page.get("error") is not None:
            raise TrinoBenchError(f"{label}: Trino error {_error_text(page['error'])}")
        if "columns" in page:
            cols = page["columns"]
            if not isinstance(cols, list) or not all(
                isinstance(c, Mapping) and isinstance(c.get("name"), str) for c in cols
            ):
                raise TrinoBenchError(f"{label}: page {i} has malformed columns")
            if columns is not None and [(c["name"], c.get("type")) for c in cols] != [
                (c["name"], c.get("type")) for c in columns
            ]:
                raise TrinoBenchError(f"{label}: columns change between pages")
            columns = list(cols)
        data = page.get("data")
        if data is None:
            continue
        if isinstance(data, Mapping):
            raise TrinoBenchError(
                f"{label}: data is an object (spooling protocol), only the direct protocol "
                "is supported"
            )
        if not isinstance(data, list):
            raise TrinoBenchError(f"{label}: page {i} data is not a list")
        if columns is None:
            raise TrinoBenchError(f"{label}: page {i} has data before any columns")
        for row in data:
            if not isinstance(row, list) or len(row) != len(columns):
                raise TrinoBenchError(f"{label}: page {i} has a malformed row {row!r}"[:300])
            rows.append(
                tuple(
                    _convert(label, c["name"], str(c.get("type", "")), v)
                    for c, v in zip(columns, row, strict=True)
                )
            )
    last = pages[-1]
    if last.get("nextUri"):
        raise TrinoBenchError(f"{label}: last page still has a nextUri (response incomplete)")
    raw = last.get("stats")
    if not isinstance(raw, Mapping):
        raise TrinoBenchError(f"{label}: last page has no stats")
    absent = [k for k in STAT_KEYS if k not in raw]
    if absent:
        raise TrinoBenchError(f"{label}: stats missing key(s): {', '.join(absent)}")
    stats: dict[str, object] = {"state": raw["state"]}
    for key in STAT_KEYS[1:]:
        stats[key] = _count(label, key, raw[key])
    if raw["state"] != "FINISHED":
        raise TrinoBenchError(f"{label}: query ended in state {raw['state']!r}, not FINISHED")
    return {
        "columns": [c["name"] for c in columns or []],
        "types": [str(c.get("type", "")) for c in columns or []],
        "rows": rows,
        "stats": stats,
    }


def parse_info(info: object) -> dict:
    """``version`` and ``starting`` of a ``GET /v1/info`` response."""
    if not isinstance(info, Mapping):
        raise TrinoBenchError("/v1/info: not a JSON object")
    node = info.get("nodeVersion")
    version = node.get("version") if isinstance(node, Mapping) else None
    if not isinstance(version, str) or not NOTE_VALUE.fullmatch(version):
        raise TrinoBenchError(f"/v1/info: bad nodeVersion.version {version!r}")
    starting = info.get("starting")
    if not isinstance(starting, bool):
        raise TrinoBenchError(f"/v1/info: bad starting {starting!r}")
    return {"version": version, "starting": starting}


# ---------------------------------------------------------------- REST client (I/O)


def http_transport(
    method: str, url: str, headers: Mapping[str, str], body: bytes | None, timeout: float
) -> tuple[int, bytes]:
    """One HTTP request with the standard library; HTTP errors are returned, not raised."""
    request = urllib.request.Request(url, data=body, headers=dict(headers), method=method)
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:  # noqa: S310
            return response.status, response.read()
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read()


class TrinoClient:
    """Minimal client of the Trino REST protocol: ``POST /v1/statement``, then every ``nextUri``."""

    def __init__(
        self,
        base_url: str = DEFAULT_URL,
        *,
        user: str = "repopulse-bench",
        catalog: str = CATALOG,
        transport: Transport | None = None,
        sleep: Callable[[float], object] = time.sleep,
        timeout: float = HTTP_TIMEOUT_S,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.user = user
        self.catalog = catalog
        self.transport = transport
        self.sleep = sleep
        self.timeout = timeout

    def _request(self, method: str, url: str, headers: Mapping[str, str], body: bytes | None):
        transport = self.transport or http_transport  # looked up per call (tests patch it)
        for attempt in range(MAX_RETRIES + 1):
            try:
                status, payload = transport(method, url, headers, body, self.timeout)
            except (OSError, urllib.error.URLError) as exc:
                raise TrinoBenchError(f"{method} {url}: {exc}") from None
            if status in RETRY_STATUSES and attempt < MAX_RETRIES:
                self.sleep(0.1)  # client protocol: try again in 50-100 ms
                continue
            if status != 200:
                text = payload[:300].decode("utf-8", "replace")
                raise TrinoBenchError(f"{method} {url}: HTTP {status}: {text}")
            try:
                return json.loads(payload)
            except ValueError:
                raise TrinoBenchError(
                    f"{method} {url}: response is not JSON: {payload[:200]!r}"
                ) from None
        raise TrinoBenchError(f"{method} {url}: still failing after {MAX_RETRIES} retries")

    def info(self) -> dict:
        return parse_info(self._request("GET", f"{self.base_url}/v1/info", {}, None))

    def execute(self, sql: str, *, schema: str | None = None, label: str | None = None) -> dict:
        """Run one statement; ``seconds`` = from sending it until the last page arrived."""
        headers = {
            "X-Trino-User": self.user,
            "X-Trino-Source": "repopulse-trino-bench",
            "X-Trino-Catalog": self.catalog,
            "X-Trino-Time-Zone": "UTC",
            "Content-Type": "text/plain; charset=utf-8",
        }
        if schema is not None:
            headers["X-Trino-Schema"] = schema
        start = time.perf_counter()
        page = self._request("POST", f"{self.base_url}/v1/statement", headers, sql.encode())
        pages = [page]
        while isinstance(page, Mapping) and page.get("nextUri") and page.get("error") is None:
            page = self._request("GET", str(page["nextUri"]), {"X-Trino-User": self.user}, None)
            pages.append(page)
        seconds = time.perf_counter() - start
        result = parse_pages(pages, label or sql.strip().splitlines()[0][:80])
        result["pages"] = pages
        result["seconds"] = round(seconds, 6)
        return result


def wait_ready(client: TrinoClient, timeout_s: float = READY_TIMEOUT_S, sleep=time.sleep) -> str:
    """Poll ``/v1/info`` until the coordinator is no longer starting; return its version."""
    deadline = time.monotonic() + timeout_s
    last = "no answer"
    while True:
        try:
            info = client.info()
            if not info["starting"]:
                return info["version"]
            last = "starting"
        except TrinoBenchError as exc:
            last = str(exc)
        if time.monotonic() > deadline:
            raise TrinoBenchError(f"Trino not ready after {timeout_s:.0f} s ({last})")
        sleep(2)


# ---------------------------------------------------------------- cross-check (pure)


def _norm(value: object) -> object:
    if isinstance(value, dt.datetime):
        return value.isoformat(sep=" ")
    if isinstance(value, dt.date):
        return value.isoformat()
    if isinstance(value, str):
        try:  # Trino timestamps come as "YYYY-MM-DD HH:MM:SS.fff"
            if len(value) > 10 and value[4] == "-" and value[10] == " ":
                return dt.datetime.fromisoformat(value).isoformat(sep=" ")
        except ValueError:
            pass
        return value
    if isinstance(value, Decimal):
        return float(value)
    return value


def _is_number(value: object) -> bool:
    return isinstance(value, int | float) and not isinstance(value, bool)


def _sort_key(row: Sequence[object]) -> tuple:
    key = []
    for value in row:
        if value is None:
            key.append((0, ""))
        elif isinstance(value, bool):
            key.append((1, str(value)))
        elif _is_number(value):
            # equal for values within the tolerance; exact for integers (counts) up to 2**53
            f = float(value)
            r = f if math.isnan(f) or math.isinf(f) else round(f, 6)
            key.append((2, str(int(r)) if r.is_integer() else repr(r)))
        else:
            key.append((3, str(value)))
    return tuple(key)


def _same(a: object, b: object, abs_tol: float) -> bool:
    if a is None or b is None:
        return a is None and b is None
    if isinstance(a, bool) or isinstance(b, bool):
        return isinstance(a, bool) and isinstance(b, bool) and a == b
    if _is_number(a) and _is_number(b):
        fa, fb = float(a), float(b)
        if math.isnan(fa) or math.isnan(fb):
            return math.isnan(fa) and math.isnan(fb)
        return math.isclose(fa, fb, rel_tol=1e-9, abs_tol=abs_tol)
    return a == b


def compare_results(
    variant: str,
    query: str,
    trino: Mapping[str, object],
    duckdb_result: Mapping[str, object],
    *,
    abs_tol: float = 1e-6,
) -> None:
    """Same column names in order and the same multiset of rows (floats within ``abs_tol``)."""
    label = f"{variant} {query}"
    t_cols, d_cols = list(trino["columns"]), list(duckdb_result["columns"])
    if t_cols != d_cols:
        raise TrinoBenchError(f"{label}: columns differ: trino={t_cols} duckdb={d_cols}")
    t_rows = sorted((tuple(_norm(v) for v in r) for r in trino["rows"]), key=_sort_key)
    d_rows = sorted((tuple(_norm(v) for v in r) for r in duckdb_result["rows"]), key=_sort_key)
    if len(t_rows) != len(d_rows):
        raise TrinoBenchError(f"{label}: trino returned {len(t_rows)} rows, duckdb {len(d_rows)}")
    for i, (t_row, d_row) in enumerate(zip(t_rows, d_rows, strict=True)):
        if not all(_same(a, b, abs_tol) for a, b in zip(t_row, d_row, strict=True)):
            raise TrinoBenchError(
                f"{label}: first differing row (sorted, #{i}): trino={t_row!r} duckdb={d_row!r}"
            )


# ---------------------------------------------------------------- rows (pure)


def build_row(
    runs: Sequence[Mapping[str, object]],
    *,
    variant: str,
    git_sha: str,
    data_date: str,
    run_at: str,
    layout: Mapping[str, object],
    trino_version: str,
    inputs: Mapping[str, Mapping[str, object]],
    bytes_equal: bool,
    cross_check: str = "full",
) -> dict[str, object]:
    """Median row of one layout; ``bytes_read`` = ``physicalInputBytes`` of Q1..Q6 (run 1)."""
    if not runs:
        raise TrinoBenchError(f"{variant}: no runs to summarise")
    if not isinstance(trino_version, str) or not NOTE_VALUE.fullmatch(trino_version):
        raise TrinoBenchError(f"{variant}: Trino version {trino_version!r} cannot go into notes")
    for key in CHECKED_KEYS:
        values = {int(r[key]) for r in runs}
        if len(values) != 1:
            raise TrinoBenchError(f"{variant}: {key} differs between runs: {sorted(values)}")
    absent = [q for q in QUERIES if q not in inputs]
    if absent:
        raise TrinoBenchError(f"{variant}: no input stats for {', '.join(absent)}")
    per_query = {
        q: {k: _count(f"{variant} {q}", k, inputs[q].get(k)) for k in INPUT_KEYS} for q in QUERIES
    }
    try:
        row = summarize_runs(
            [{**r, "bytes_read": 0, "peak_rss_mb": None} for r in runs],
            variant=variant,
            git_sha=git_sha,
            data_date=data_date,
            run_at=run_at,
        )
    except MeasureError as exc:
        raise TrinoBenchError(f"{variant}: {exc}") from None
    row["bytes_read"] = sum(per_query[q]["physicalInputBytes"] for q in QUERIES)
    row["peak_rss_mb"] = None  # the client cannot measure the server's memory; no substitute
    shape = query_opt._layout(LAYOUT_OF[variant])
    notes = [
        f"runs={len(runs)}",
        "stat=median",
        "warmup=1",
        "engine=trino",
        f"trino={trino_version}",
        f"partition_by={query_opt._keys_note(shape.partition_by)}",
        f"sort_by={query_opt._keys_note(shape.sort_by)}",
        f"files={int(layout['files'])}",
        f"layout_bytes={int(layout['layout_bytes'])}",
        "bytes=physical_input_bytes",
        "bytes_from=run1",
        f"bytes_equal_across_runs={'true' if bytes_equal else 'false'}",
        f"cross_check={cross_check}",
    ]
    for q in QUERIES:
        notes.append(f"{q}_input_bytes={per_query[q]['physicalInputBytes']}")
        notes.append(f"{q}_input_rows={per_query[q]['processedRows']}")
    row["notes"] = " ".join(notes)
    validate_row(row)
    return row


# ---------------------------------------------------------------- preparation (I/O, not timed)


def write_repos_parquet(seed: str | Path, work_dir: str | Path) -> Path:
    """The ``repos`` seed table as ``<work_dir>/repos/data.parquet`` (same columns as DuckDB)."""
    out_dir = Path(work_dir) / REPOS_DIR
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / "data.parquet"
    con = duckdb.connect(":memory:")
    try:
        query_opt.create_repos(con, seed)
        con.execute(f"COPY repos TO {_sql_str(path.as_posix())} (FORMAT parquet)")
    finally:
        con.close()
    return path


def upload_layouts(
    work_dir: str | Path,
    *,
    compose_file: str | Path = COMPOSE_FILE,
    bucket: str = DEFAULT_BUCKET,
    timeout_s: float = 30 * 60,
) -> None:
    """Copy ``work_dir`` unchanged into the bucket with the compose ``upload`` service."""
    s3_location(bucket)  # validates the name
    cmd = [
        "docker",
        "compose",
        "-f",
        str(compose_file),
        "--profile",
        "upload",
        "run",
        "--rm",
        "upload",
    ]
    env = {
        **os.environ,
        "TRINO_BENCH_WORK_DIR": str(Path(work_dir).resolve()),
        "TRINO_BENCH_BUCKET": bucket,
    }
    try:
        proc = subprocess.run(
            cmd, capture_output=True, text=True, env=env, timeout=timeout_s, check=False
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise TrinoBenchError(f"upload failed: {exc}") from None
    if proc.returncode != 0:
        tail = "\n".join((proc.stdout + proc.stderr).strip().splitlines()[-20:])
        raise TrinoBenchError(f"upload failed (exit {proc.returncode}):\n{tail}")


# ---------------------------------------------------------------- one layout on Trino


def register_layout(
    client: TrinoClient, variant: str, *, bucket: str, work_dir: Path, raw_rows: int
) -> int:
    """Run the DDL, check partitions and row count as Trino sees them; return the row count."""
    schema = schema_name(variant)
    for stmt in layout_ddl(variant, bucket=bucket):
        client.execute(stmt, schema=schema, label=f"{variant} DDL")
    layout = query_opt._layout(LAYOUT_OF[variant])
    if layout.partition_by:
        (column,) = layout.partition_by
        expected = len(partition_values(query_opt.layout_path(work_dir, layout.variant), column))
        seen = client.execute(PARTITION_COUNT_SQL, schema=schema, label=f"{variant} partitions")
        if seen["rows"] != [(expected,)]:
            raise TrinoBenchError(
                f"{variant}: Trino sees {seen['rows']} partition(s), the layout has {expected} "
                "partition directories"
            )
    counted = client.execute(COUNT_SQL, schema=schema, label=f"{variant} count")
    if counted["rows"] != [(raw_rows,)]:
        raise TrinoBenchError(f"{variant}: Trino counts {counted['rows']}, raw table {raw_rows}")
    return raw_rows


def run_queries(client: TrinoClient, variant: str, sql: Mapping[str, str]) -> dict:
    """Q1..Q6 once, in order; ``total_seconds`` = sum of the client-side query times."""
    schema = schema_name(variant)
    run: dict = {"results": {}, "inputs": {}, "pages": {}}
    for q in QUERIES:
        result = client.execute(sql[q], schema=schema, label=f"{variant} {q}")
        run[f"{q}_seconds"] = result["seconds"]
        run[f"{q}_rows"] = len(result["rows"])
        run["results"][q] = result
        run["inputs"][q] = result["stats"]
        run["pages"][q] = result["pages"]
    run["total_seconds"] = round(sum(run[f"{q}_seconds"] for q in QUERIES), 6)
    return run


def _save_sample(samples_dir: Path, variant: str, version: str, pages: list) -> None:
    samples_dir.mkdir(parents=True, exist_ok=True)
    sample = {
        "trino_version": version,
        "query": SAMPLE_QUERY,
        "layout": variant,
        "note": "real response pages of the Trino REST API, saved by benchmarks/trino_bench.py",
        "pages": pages,
    }
    path = samples_dir / f"{SAMPLE_QUERY}_{variant}.json"
    path.write_text(json.dumps(sample, indent=2) + "\n", encoding="utf-8")


def measure_layout(
    client: TrinoClient,
    variant: str,
    *,
    runs: int,
    sql: Mapping[str, str],
    reference: Mapping[str, Mapping[str, object]],
    raw_rows: int,
    bucket: str,
    work_dir: Path,
) -> tuple[list[dict], dict, bool, dict]:
    """Register, warm up + cross-check, then the timed runs of one layout."""
    row_count = register_layout(
        client, variant, bucket=bucket, work_dir=work_dir, raw_rows=raw_rows
    )
    warmup = run_queries(client, variant, sql)  # JIT, metastore cache: not timed
    for q in QUERIES:
        compare_results(variant, q, warmup["results"][q], reference[q])
    timed: list[dict] = []
    for i in range(1, runs + 1):
        run = run_queries(client, variant, sql)
        for q in QUERIES:
            if run[f"{q}_rows"] != warmup[f"{q}_rows"]:
                raise TrinoBenchError(
                    f"{variant} {q}: run {i} returned {run[f'{q}_rows']} rows, warm-up "
                    f"{warmup[f'{q}_rows']}"
                )
        run["row_count"] = row_count
        print(f"{variant} run {i}/{runs}: total {run['total_seconds']:.3f}s", flush=True)
        timed.append(run)
    bytes_equal = all(
        r["inputs"][q]["physicalInputBytes"] == timed[0]["inputs"][q]["physicalInputBytes"]
        for r in timed
        for q in QUERIES
    )
    return timed, timed[0]["inputs"], bytes_equal, timed[-1]["pages"]


# ---------------------------------------------------------------- CLI


def _write_summary(path: Path, status: list[tuple[str, str]], rows: list[dict]) -> None:
    lines = ["### Trino + Hive Metastore (Q1-Q6 on the query_opt layouts, GH Archive real data)"]
    lines += ["", "| layout | status |", "|---|---|"]
    lines += [f"| `{cell}` | {text.splitlines()[0] if text else ''} |" for cell, text in status]
    lines += ["", f"{len(rows)} row(s) for runs.csv:", "", "```csv", ",".join(COLUMNS)]
    lines += [",".join(validate_row(r)[c] for c in COLUMNS) for r in rows]
    lines += ["```", ""]
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines), encoding="utf-8")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Q1-Q6 on Trino + Hive Metastore + MinIO.")
    parser.add_argument("--data-dir", type=Path, help="default: data/raw/gharchive/<data_date>")
    parser.add_argument("--reference", type=Path, default=REFERENCE_TOML)
    parser.add_argument("--seed", type=Path, default=DEFAULT_SEED)
    parser.add_argument("--work-dir", type=Path, default=DEFAULT_WORK_DIR, help="parquet layouts")
    parser.add_argument("--runs", type=int, default=10)
    parser.add_argument("--git-sha", default=None)
    parser.add_argument("--out", type=Path, default=Path("out/trino-rows.csv"))
    parser.add_argument("--summary", type=Path, default=Path("out/trino-summary.md"))
    parser.add_argument("--samples-dir", type=Path, default=Path("out/trino-samples"))
    parser.add_argument("--trino-url", default=DEFAULT_URL)
    parser.add_argument("--bucket", default=DEFAULT_BUCKET, help="MinIO bucket of the layouts")
    parser.add_argument("--compose-file", type=Path, default=COMPOSE_FILE)
    parser.add_argument(
        "--print-ddl", action="store_true", help="print the DDL of every layout and exit"
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.print_ddl:
        try:
            for variant in VARIANTS:
                for stmt in layout_ddl(variant, bucket=args.bucket):
                    print(stmt + ";")
        except TrinoBenchError as exc:
            print(f"ERROR: {exc}", file=sys.stderr)
            return 2
        return 0

    run_at = datetime.now(UTC).isoformat(timespec="seconds")
    try:  # fail on bad metadata now, not after the runs
        if not args.git_sha:
            raise RunsLogError("git_sha: --git-sha is required")
        if args.runs < 1:
            raise RunsLogError("runs: --runs must be >= 1")
        s3_location(args.bucket)
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
    except (RunsLogError, TrinoBenchError) as exc:
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

    def fail_all(message: str) -> int:
        for variant in VARIANTS:
            fail(variant, message)
        _write_summary(args.summary, status, rows)
        return 1

    # Not timed: parquet layouts (same code and files as query_opt), seed, DuckDB answers, upload.
    try:
        prepared = query_opt.prepare_in_subprocess(data_dir, work_dir=args.work_dir, seed=args.seed)
        print(f"prepared layouts from {prepared['raw_rows']} raw rows", flush=True)
        if REFERENCE_LAYOUT in prepared["errors"]:
            raise TrinoBenchError(
                f"no DuckDB reference: {REFERENCE_LAYOUT} failed: "
                f"{prepared['errors'][REFERENCE_LAYOUT]}"
            )
        write_repos_parquet(args.seed, args.work_dir)
        reference = query_opt.run_once(args.work_dir, REFERENCE_LAYOUT, args.seed, collect=True)
        upload_layouts(args.work_dir, compose_file=args.compose_file, bucket=args.bucket)
        client = TrinoClient(args.trino_url)
        version = wait_ready(client)
        print(f"Trino {version} ready at {args.trino_url}", flush=True)
    except (query_opt.QueryOptError, TrinoBenchError, duckdb.Error, OSError) as exc:
        return fail_all(str(exc))

    sql = query_sql()
    for variant in VARIANTS:
        layout_name = LAYOUT_OF[variant]
        if layout_name in prepared["errors"]:
            fail(variant, f"prepare: {prepared['errors'][layout_name]}")
            continue
        try:
            timed, inputs, bytes_equal, pages = measure_layout(
                client,
                variant,
                runs=args.runs,
                sql=sql,
                reference=reference["results"],
                raw_rows=prepared["raw_rows"],
                bucket=args.bucket,
                work_dir=args.work_dir,
            )
            row = build_row(
                [
                    {k: v for k, v in r.items() if k not in ("results", "inputs", "pages")}
                    for r in timed
                ],
                variant=variant,
                git_sha=args.git_sha,
                data_date=ref["data_date"],
                run_at=run_at,
                layout=prepared["layouts"][layout_name],
                trino_version=version,
                inputs=inputs,
                bytes_equal=bytes_equal,
            )
        except (TrinoBenchError, RunsLogError) as exc:
            fail(variant, str(exc))
            continue
        if variant in SAMPLE_VARIANTS:
            _save_sample(args.samples_dir, variant, version, pages[SAMPLE_QUERY])
        append_run(args.out, row)
        rows.append(row)
        status.append((variant, f"ok, median total {row['total_seconds']} s"))

    _write_summary(args.summary, status, rows)
    print(f"wrote {len(rows)} row(s) to {args.out}")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
