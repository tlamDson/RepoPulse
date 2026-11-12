"""Engine matrix benchmark (E1): rows, cross-check, and every engine through fakes.

No network and no cloud: MotherDuck is a fake ``connect`` over a local in-memory DuckDB, the
Databricks REST API is a fake ``transport``. The reference day is ``tests/fixtures/gharchive_small``
(34 lines, gzip-compressed into tmp at test time) with a tmp ``reference.toml`` of its checksums.
Spark runs only on CI (``@pytest.mark.spark``).
"""

from __future__ import annotations

import ast
import base64
import datetime as dt
import gzip
import hashlib
import json
import re
import shutil
from decimal import Decimal
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

import duckdb
import pytest

from benchmarks import engine_matrix as bench
from benchmarks import naive_duckdb
from benchmarks.record_run import read_artifact
from repopulse.probe import HttpRequest, HttpResponse
from repopulse.runs_log import COLUMNS, validate_row

ROOT = Path(__file__).resolve().parents[1]
FIXTURE_DIR = Path(__file__).parent / "fixtures" / "gharchive_small"
SPARK_DIR = ROOT / "benchmarks" / "queries_spark"
FIXTURE_LINES = 34
FIXTURE_DATE = "2026-01-05"
QUERIES = tuple(f"q{i}" for i in range(1, 7))
SHA = "abcdef1234567"
ENGINES = ("duckdb", "spark", "databricks-serverless", "databricks-sql", "motherduck", "snowflake")

HOST = "https://fake-ws.cloud.databricks.com"
TOKEN = "dapiFAKEtoken0000000000"
USER = "someone@example.com"
WAREHOUSE = "abcd1234ef"
MD_TOKEN = "md-fake-token-0000000000"


# ---------------------------------------------------------------- fixtures


def make_day(tmp_path: Path) -> tuple[Path, Path]:
    """Gzipped fixture hours and a reference.toml with their sha256."""
    data = tmp_path / "data"
    data.mkdir()
    lines = [f'data_date = "{FIXTURE_DATE}"', ""]
    for src in sorted(FIXTURE_DIR.glob("*.json")):
        dst = data / f"{src.stem}.json.gz"
        with src.open("rb") as fin, gzip.GzipFile(dst, "wb", mtime=0) as fout:
            shutil.copyfileobj(fin, fout)
        digest = hashlib.sha256(dst.read_bytes()).hexdigest()
        lines += ["[[files]]", f'name = "{dst.name}"', f'sha256 = "{digest}"', ""]
    ref = tmp_path / "reference.toml"
    ref.write_text("\n".join(lines), encoding="utf-8")
    return data, ref


@pytest.fixture
def day(tmp_path):
    return make_day(tmp_path)


@pytest.fixture
def reference(day):
    data, _ = day
    return naive_duckdb.run_once(data, naive_duckdb.DEFAULT_SEED, collect=True)


def cli(engine, day, tmp_path, *extra, runs="2"):
    data, ref = day
    return [
        "--engine",
        engine,
        "--runs",
        runs,
        "--git-sha",
        SHA,
        "--data-dir",
        str(data),
        "--reference",
        str(ref),
        "--out",
        str(tmp_path / "rows.csv"),
        "--summary",
        str(tmp_path / "summary.md"),
        *extra,
    ]


def written_rows(tmp_path):
    path = tmp_path / "rows.csv"
    if not path.is_file():
        return []
    return read_artifact(path)


def notes_of(row) -> dict[str, str]:
    out = {}
    for word in str(row["notes"]).split():
        key, sep, value = word.partition("=")
        assert sep and key and value, f"notes word {word!r} is not key=value"
        out[key] = value
    return out


def timed_run(scale: float, rows: int = 3) -> dict:
    run = {}
    for i, q in enumerate(QUERIES, start=1):
        run[f"{q}_seconds"] = round(scale * i / 10, 6)
        run[f"{q}_rows"] = rows
    return run


# ---------------------------------------------------------------- engines and variants


def test_six_engines_in_fixed_order_and_snowflake_is_not_measured():
    assert bench.ENGINES == ENGINES
    assert "snowflake" not in bench.MEASURED_ENGINES
    assert set(bench.MEASURED_ENGINES) == set(ENGINES) - {"snowflake"}


def test_variant_is_matrix_engine():
    assert bench.variant("databricks-sql") == "matrix-databricks-sql"
    with pytest.raises(bench.EngineMatrixError):
        bench.variant("oracle")


# ---------------------------------------------------------------- rows


def test_build_row_is_valid_with_median_per_query_and_median_of_the_run_sums():
    runs = [timed_run(1.0), timed_run(3.0), timed_run(2.0)]

    row = bench.build_row(
        runs,
        engine="duckdb",
        git_sha=SHA,
        data_date="2026-10-25",
        run_at="2026-11-12T00:00:00+00:00",
        row_count=1670400,
        load_seconds=2.5,
        timing="in-process",
        versions={"duckdb": "1.5.6"},
        extra={"md_database": "x"},
    )

    validate_row(row)
    assert row["variant"] == "matrix-duckdb"
    assert row["data_date"] == "2026-10-25"
    assert row["row_count"] == 1670400
    assert row["q1_seconds"] == 0.2
    assert row["q6_seconds"] == 1.2
    # run sums: 2.1, 6.3, 4.2 -> median 4.2 (load and warm-up are not in it)
    assert row["total_seconds"] == 4.2
    assert row["bytes_read"] is None and row["peak_rss_mb"] is None
    notes = notes_of(row)
    assert notes["engine"] == "duckdb"
    assert notes["runs"] == "3"
    assert notes["stat"] == "median"
    assert notes["warmup"] == "1"
    assert notes["timing"] == "in-process"
    assert notes["total"] == "median_run_sum_q1_q6"
    assert notes["load_seconds"] == "2.5"
    assert notes["cross_check"] == "content"
    assert notes["duckdb"] == "1.5.6"
    assert notes["md_database"] == "x"


@pytest.mark.parametrize("engine", ["snowflake", "oracle"])
def test_build_row_refuses_snowflake_and_unknown_engines(engine):
    with pytest.raises(bench.EngineMatrixError, match=engine):
        bench.build_row(
            [timed_run(1.0)],
            engine=engine,
            git_sha=SHA,
            data_date="2026-10-25",
            run_at="2026-11-12T00:00:00+00:00",
            row_count=1,
            load_seconds=1.0,
            timing="client",
            versions={},
        )


def test_build_row_refuses_query_row_counts_that_change_between_runs():
    with pytest.raises(bench.EngineMatrixError, match="q1_rows"):
        bench.build_row(
            [timed_run(1.0, rows=3), timed_run(1.0, rows=4)],
            engine="spark",
            git_sha=SHA,
            data_date="2026-10-25",
            run_at="2026-11-12T00:00:00+00:00",
            row_count=1,
            load_seconds=1.0,
            timing="in-process",
            versions={"spark": "3.5.3"},
        )


@pytest.mark.parametrize(
    "kwargs",
    [
        {"timing": "wall clock"},
        {"timing": "guess"},
        {"versions": {"spark": "3.5 beta"}},
        {"extra": {"bad key": "x"}},
        {"extra": {"k": "a=b"}},
    ],
)
def test_build_row_refuses_notes_that_would_not_parse(kwargs):
    base = {
        "engine": "spark",
        "git_sha": SHA,
        "data_date": "2026-10-25",
        "run_at": "2026-11-12T00:00:00+00:00",
        "row_count": 1,
        "load_seconds": 1.0,
        "timing": "in-process",
        "versions": {"spark": "3.5.3"},
    }
    with pytest.raises(bench.EngineMatrixError):
        bench.build_row([timed_run(1.0)], **{**base, **kwargs})


# ---------------------------------------------------------------- cross-check against DuckDB


def _ref(rows_by_query=None, row_count=3):
    rows_by_query = rows_by_query or {}
    return {
        "row_count": row_count,
        "results": {
            q: {"columns": ["k", "n"], "rows": rows_by_query.get(q, [("a", 1), ("b", 2.5)])}
            for q in QUERIES
        },
    }


def test_check_results_accepts_the_same_rows_in_any_order_and_iso_dates():
    reference = _ref({"q2": [(dt.date(2026, 1, 5), 1)]})
    measured = _ref({"q2": [("2026-01-05", 1)], "q1": [("b", 2.5), ("a", 1)]})

    bench.check_results("databricks-sql", measured, reference)


def test_check_results_rejects_another_row_count_of_raw_events():
    with pytest.raises(bench.EngineMatrixError, match="row_count"):
        bench.check_results("motherduck", _ref(row_count=4), _ref())


def test_check_results_rejects_a_different_value_naming_engine_and_query():
    measured = _ref({"q4": [("a", 1), ("b", 2.6)]})

    with pytest.raises(bench.EngineMatrixError, match=r"spark.*q4"):
        bench.check_results("spark", measured, _ref())


def test_check_results_rejects_a_different_number_of_rows():
    measured = _ref({"q6": [("a", 1)]})

    with pytest.raises(bench.EngineMatrixError, match="q6"):
        bench.check_results("spark", measured, _ref())


def test_check_results_rejects_a_missing_query():
    measured = _ref()
    del measured["results"]["q5"]

    with pytest.raises(bench.EngineMatrixError, match="q5"):
        bench.check_results("databricks-serverless", measured, _ref())


# ---------------------------------------------------------------- CLI: metadata and refusals


def test_snowflake_is_refused_and_nothing_is_written(day, tmp_path, capsys):
    code = bench.main(cli("snowflake", day, tmp_path))

    assert code == 2
    err = capsys.readouterr().err
    assert "dbt-build-snowflake" in err
    assert not (tmp_path / "rows.csv").exists()


def test_git_sha_is_required(day, tmp_path, capsys):
    argv = cli("duckdb", day, tmp_path)
    i = argv.index("--git-sha")
    del argv[i : i + 2]

    assert bench.main(argv) == 2
    assert "git" in capsys.readouterr().err.lower()


def test_checksum_mismatch_stops_before_any_run(day, tmp_path, capsys):
    data, _ = day
    first = sorted(data.glob("*.json.gz"))[0]
    first.write_bytes(first.read_bytes() + b"x")

    assert bench.main(cli("duckdb", day, tmp_path)) == 2
    assert "sha256" in capsys.readouterr().err
    assert written_rows(tmp_path) == []


# ---------------------------------------------------------------- CLI: duckdb (in-process)


def test_duckdb_engine_writes_one_valid_row_on_the_reference_day(day, tmp_path):
    assert bench.main(cli("duckdb", day, tmp_path)) == 0

    (row,) = written_rows(tmp_path)
    assert row["variant"] == "matrix-duckdb"
    assert row["data_date"] == FIXTURE_DATE
    assert row["row_count"] == FIXTURE_LINES
    assert row["git_sha"] == SHA
    for q in QUERIES:
        assert row[f"{q}_seconds"] >= 0
    notes = notes_of(row)
    assert notes["engine"] == "duckdb"
    assert notes["runs"] == "2"
    assert notes["timing"] == "in-process"
    assert notes["cross_check"] == "content"
    assert notes["duckdb"] == duckdb.__version__
    assert float(notes["load_seconds"]) >= 0
    summary = (tmp_path / "summary.md").read_text(encoding="utf-8")
    assert "matrix-duckdb" in summary


def test_each_call_appends_one_row_so_engines_do_not_overwrite_each_other(day, tmp_path):
    assert bench.main(cli("duckdb", day, tmp_path)) == 0
    assert bench.main(cli("duckdb", day, tmp_path)) == 0

    rows = written_rows(tmp_path)
    assert [r["variant"] for r in rows] == ["matrix-duckdb", "matrix-duckdb"]
    header = (tmp_path / "rows.csv").read_text(encoding="utf-8").splitlines()[0]
    assert header == ",".join(COLUMNS)


# ---------------------------------------------------------------- CLI: MotherDuck (fake connect)


class FakeMotherDuck:
    """``connect`` for ``md:`` backed by one local in-memory DuckDB; databases are attached."""

    def __init__(self, *, tamper: bool = False) -> None:
        self.databases: list[str] = []
        self.statements: list[str] = []
        self.db = duckdb.connect(":memory:")
        self.attached: set[str] = set()
        self.tamper = tamper

    def __call__(self, database, *args, **kwargs):
        self.databases.append(str(database))
        assert not args and not kwargs.get("config"), "token must not be passed in a config"
        return _FakeMDConnection(self)


class _FakeMDConnection:
    def __init__(self, owner: FakeMotherDuck) -> None:
        self.owner = owner

    def execute(self, sql, params=None):
        owner = self.owner
        owner.statements.append(sql)
        create_db = re.fullmatch(r"\s*CREATE DATABASE IF NOT EXISTS (\w+)\s*", sql, re.I)
        if create_db:
            name = create_db.group(1)
            if name not in owner.attached:
                owner.db.execute(f"ATTACH ':memory:' AS {name}")
                owner.attached.add(name)
            return owner.db
        result = owner.db.execute(sql) if params is None else owner.db.execute(sql, params)
        if owner.tamper and re.match(r"\s*CREATE OR REPLACE TABLE raw_events\b", sql, re.I):
            owner.db.execute("DELETE FROM raw_events WHERE id = (SELECT min(id) FROM raw_events)")
            result = owner.db
        return result

    def close(self):
        pass


def md_env():
    return {"MOTHERDUCK_TOKEN": MD_TOKEN}


def test_motherduck_loads_its_own_database_and_writes_a_client_timed_row(day, tmp_path, capsys):
    fake = FakeMotherDuck()

    code = bench.main(cli("motherduck", day, tmp_path), env=md_env(), connect=fake)

    assert code == 0, capsys.readouterr().err
    (row,) = written_rows(tmp_path)
    assert row["variant"] == "matrix-motherduck"
    assert row["row_count"] == FIXTURE_LINES
    notes = notes_of(row)
    assert notes["timing"] == "client"
    assert notes["cross_check"] == "content"
    assert notes["md_database"] == "repopulse_matrix"
    assert notes["duckdb"] == duckdb.__version__
    assert fake.databases == ["md:"]
    joined = "\n".join(fake.statements)
    assert "CREATE DATABASE IF NOT EXISTS repopulse_matrix" in joined
    assert re.search(r"CREATE OR REPLACE TABLE raw_events\b", joined)
    assert re.search(r"CREATE OR REPLACE TABLE repos\b", joined)
    assert not re.search(r"\brepopulse(_smoke)?\.", joined), "never writes gold or smoke"
    out = capsys.readouterr()
    assert MD_TOKEN not in out.out + out.err + joined


def test_motherduck_rerun_replaces_the_tables_without_duplicates(day, tmp_path):
    fake = FakeMotherDuck()
    assert bench.main(cli("motherduck", day, tmp_path), env=md_env(), connect=fake) == 0

    assert bench.main(cli("motherduck", day, tmp_path), env=md_env(), connect=fake) == 0

    count = fake.db.execute("SELECT count(*) FROM repopulse_matrix.main.raw_events").fetchone()
    assert count == (FIXTURE_LINES,)
    assert len(written_rows(tmp_path)) == 2


def test_motherduck_without_token_fails_before_connecting(day, tmp_path, capsys):
    fake = FakeMotherDuck()

    assert bench.main(cli("motherduck", day, tmp_path), env={}, connect=fake) != 0

    assert fake.databases == []
    assert "MOTHERDUCK_TOKEN" in capsys.readouterr().err
    assert written_rows(tmp_path) == []


@pytest.mark.parametrize("name", ["repopulse", "repopulse_smoke", "bad-name", "x;drop"])
def test_motherduck_refuses_the_gold_and_smoke_databases_and_unsafe_names(
    day, tmp_path, name, capsys
):
    fake = FakeMotherDuck()

    code = bench.main(
        cli("motherduck", day, tmp_path, "--md-database", name), env=md_env(), connect=fake
    )

    assert code == 2
    assert fake.databases == []
    assert name in capsys.readouterr().err


def test_motherduck_result_that_differs_from_duckdb_writes_no_row(day, tmp_path, capsys):
    fake = FakeMotherDuck(tamper=True)

    code = bench.main(cli("motherduck", day, tmp_path), env=md_env(), connect=fake)

    assert code == 1
    assert written_rows(tmp_path) == []
    err = capsys.readouterr().err
    assert "row_count" in err
    assert "FAILED" in (tmp_path / "summary.md").read_text(encoding="utf-8")


# ---------------------------------------------------------------- Databricks (fake transport)


def _jsonable(value):
    if isinstance(value, dt.datetime):
        return value.isoformat(sep=" ")
    if isinstance(value, dt.date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    return value


def _type_name(values) -> str:
    for v in values:
        if v is None:
            continue
        if isinstance(v, bool):
            return "BOOLEAN"
        if isinstance(v, int):
            return "LONG"
        if isinstance(v, float | Decimal):
            return "DOUBLE"
        if isinstance(v, dt.datetime):
            return "TIMESTAMP"
        if isinstance(v, dt.date):
            return "DATE"
        return "STRING"
    return "STRING"


def _text(value) -> str | None:
    """A value as the Statement Execution API sends it in JSON_ARRAY (always a string)."""
    if value is None:
        return None
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, dt.datetime):
        return value.strftime("%Y-%m-%dT%H:%M:%S.000Z")
    if isinstance(value, dt.date):
        return value.isoformat()
    return str(value)


def ok(body) -> HttpResponse:
    return HttpResponse(200, json.dumps(body).encode())


class FakeDatabricks:
    """Databricks REST API on the fixture: Statement API answers with DuckDB's own results.

    The result cache is modelled as the real warehouse showed it (run 43173868610: unique
    trailing comments did not stop 26 of 30 timed statements from being cache hits): a query
    is answered from the cache when the same query ran before and neither table was updated
    since. ``ALTER TABLE ... SET TBLPROPERTIES`` is an update (a new Delta table version), as
    "both caches are invalidated when the underlying tables are updated" (Databricks docs,
    query caching). ``cache_hit=True`` forces every query to be a cache hit.
    """

    def __init__(self, reference, *, cache_hit=False, tamper=None, me_status=200) -> None:
        self.reference = reference
        self.cache_hit = cache_hit
        self.table_versions = {"raw_events": 0, "repos": 0}
        self.cached_at: dict[str, tuple[int, int]] = {}  # query -> table versions of its result
        self.from_cache: dict[str, bool] = {}  # statement id -> answered from the cache
        self.alters: list[tuple[int, str]] = []  # (statement index, table)
        self.tamper = tamper
        self.me_status = me_status
        self.requests: list[HttpRequest] = []
        self.created: list[tuple[str, dict]] = []
        self.uploads: dict[str, bytes] = {}
        self.statements: list[dict] = []
        self.query_ids: dict[str, str] = {}
        self.imports: list[dict] = []
        self.submits: list[dict] = []
        self.pending: dict[str, dict] = {}
        self.chunks: dict[tuple[str, int], dict] = {}
        self.history_queries: list[list[str]] = []
        self.spark_sql = {
            q: (SPARK_DIR / f"{q}.sql").read_text(encoding="utf-8").rstrip() for q in QUERIES
        }

    # -- helpers
    def _rows(self, q):
        rows = [list(r) for r in self.reference["results"][q]["rows"]]
        if self.tamper == q and rows:
            first = rows[0]
            idx = next(i for i, v in enumerate(first) if isinstance(v, int | float))
            first[idx] = first[idx] + 1000
        return rows

    def _statement(self, body):
        assert body["warehouse_id"] == WAREHOUSE
        sql = body["statement"]
        self.statements.append(body)
        sid = f"stmt-{len(self.statements)}"
        done = {"statement_id": sid, "status": {"state": "SUCCEEDED"}}
        if re.match(r"\s*CREATE OR REPLACE TABLE workspace\.repopulse_matrix\.\w+ AS", sql):
            return done
        alter = re.match(
            r"\s*ALTER TABLE workspace\.repopulse_matrix\.(raw_events|repos) SET TBLPROPERTIES "
            r"\('repopulse\.matrix_run' = '[^']+'\)",
            sql,
        )
        if alter:
            self.table_versions[alter.group(1)] += 1
            self.alters.append((len(self.statements), alter.group(1)))
            return done
        if re.fullmatch(
            r"\s*SELECT count\(\*\) AS row_count FROM workspace\.repopulse_matrix\.raw_events\s*",
            sql,
        ):
            return {
                **done,
                "manifest": {"schema": {"columns": [{"name": "row_count", "type_name": "LONG"}]}},
                "result": {"data_array": [[str(self.reference["row_count"])]]},
            }
        for q, text in self.spark_sql.items():
            if sql.startswith(text):
                self.query_ids[sid] = q
                versions = (self.table_versions["raw_events"], self.table_versions["repos"])
                self.from_cache[sid] = self.cache_hit or self.cached_at.get(q) == versions
                self.cached_at[q] = versions
                ref = self.reference["results"][q]
                rows = self._rows(q)
                columns = [
                    {"name": name, "type_name": _type_name([r[i] for r in rows]), "position": i}
                    for i, name in enumerate(ref["columns"])
                ]
                data = [[_text(v) for v in r] for r in rows]
                half = len(data) // 2
                result = {**done, "manifest": {"schema": {"columns": columns}}}
                if q == "q1" and half:
                    link = f"/api/2.0/sql/statements/{sid}/result/chunks/1"
                    result["result"] = {
                        "chunk_index": 0,
                        "data_array": data[:half],
                        "next_chunk_index": 1,
                        "next_chunk_internal_link": link,
                    }
                    self.chunks[(sid, 1)] = {"chunk_index": 1, "data_array": data[half:]}
                else:
                    result["result"] = {"chunk_index": 0, "data_array": data}
                if len(self.query_ids) == 1:  # first query: still running, must be polled
                    self.pending[sid] = result
                    return {"statement_id": sid, "status": {"state": "PENDING"}}
                return result
        raise AssertionError(f"unexpected statement {sql[:120]!r}")

    def _notebook_summary(self):
        params = self.submits[-1]["tasks"][0]["notebook_task"]["base_parameters"]
        runs = int(params["runs"])
        results = {}
        for q in QUERIES:
            ref = self.reference["results"][q]
            results[q] = {
                "columns": list(ref["columns"]),
                "rows": [[_jsonable(v) for v in r] for r in self._rows(q)],
            }
        timed = [timed_run(0.5 + i, rows=0) for i in range(runs)]
        for run in timed:
            for q in QUERIES:
                run[f"{q}_rows"] = len(results[q]["rows"])
        return {
            "spark_version": "4.0.0",
            "row_count": self.reference["row_count"],
            "load_seconds": 2.5,
            "warmup": 1,
            "runs": timed,
            "results": results,
        }

    # -- transport
    def __call__(self, req: HttpRequest) -> HttpResponse:
        self.requests.append(req)
        assert req.headers.get("Authorization") == f"Bearer {TOKEN}"
        parts = urlsplit(req.url)
        assert f"{parts.scheme}://{parts.netloc}" == HOST
        path, query = parts.path, parse_qs(parts.query)
        body = json.loads(req.body) if req.body and req.method != "PUT" else None
        if path == "/api/2.0/preview/scim/v2/Me":
            if self.me_status != 200:
                message = f"token {TOKEN} for {HOST} is not allowed"
                err = {"error_code": "PERMISSION_DENIED", "message": message}
                return HttpResponse(self.me_status, json.dumps(err).encode())
            return ok({"userName": USER})
        if path in ("/api/2.1/unity-catalog/schemas", "/api/2.1/unity-catalog/volumes"):
            self.created.append((path, body))
            return ok({})
        if path.startswith("/api/2.0/fs/files/") and req.method == "PUT":
            self.uploads[path[len("/api/2.0/fs/files") :]] = req.body
            return HttpResponse(204, b"")
        if path == "/api/2.0/sql/warehouses":
            odbc = {"path": f"/sql/1.0/warehouses/{WAREHOUSE}"}
            return ok({"warehouses": [{"id": WAREHOUSE, "odbc_params": odbc}]})
        if path == f"/api/2.0/sql/warehouses/{WAREHOUSE}":
            return ok(
                {
                    "id": WAREHOUSE,
                    "cluster_size": "2X-Small",
                    "warehouse_type": "PRO",
                    "enable_serverless_compute": True,
                }
            )
        if path == "/api/2.0/sql/statements" and req.method == "POST":
            return ok(self._statement(body))
        chunk = re.fullmatch(r"/api/2\.0/sql/statements/([^/]+)/result/chunks/(\d+)", path)
        if chunk and req.method == "GET":
            return ok(self.chunks[(chunk.group(1), int(chunk.group(2)))])
        poll = re.fullmatch(r"/api/2\.0/sql/statements/([^/]+)", path)
        if poll and req.method == "GET":
            return ok(self.pending.pop(poll.group(1)))
        if path == "/api/2.0/sql/history/queries" and req.method == "GET":
            ids = query.get("filter_by.statement_ids", [])
            self.history_queries.append(ids)
            res = [
                {
                    "query_id": i,
                    "status": "FINISHED",
                    "metrics": {"result_from_cache": self.from_cache.get(i, False)},
                }
                for i in ids
            ]
            return ok({"res": res, "has_next_page": False})
        if path == "/api/2.0/workspace/mkdirs":
            return ok({})
        if path == "/api/2.0/workspace/import":
            self.imports.append(body)
            return ok({})
        if path == "/api/2.1/jobs/runs/submit":
            self.submits.append(body)
            return ok({"run_id": 101})
        if path == "/api/2.1/jobs/runs/get":
            state = {"life_cycle_state": "TERMINATED", "result_state": "SUCCESS"}
            return ok({"state": state, "tasks": [{"run_id": 102}]})
        if path == "/api/2.1/jobs/runs/get-output":
            return ok({"notebook_output": {"result": json.dumps(self._notebook_summary())}})
        raise AssertionError(f"unexpected request {req.method} {path}")


def dbx_env(**extra):
    return {"DATABRICKS_HOST": HOST, "DATABRICKS_TOKEN": TOKEN, **extra}


def no_sleep(_seconds):
    return None


def run_dbx(engine, day, tmp_path, fake, *extra, env=None):
    return bench.main(
        cli(engine, day, tmp_path, *extra),
        env=env if env is not None else dbx_env(),
        transport=fake,
        sleep=no_sleep,
    )


def test_databricks_sql_uploads_loads_its_own_schema_and_writes_a_row(
    day, tmp_path, reference, capsys
):
    fake = FakeDatabricks(reference)

    code = run_dbx("databricks-sql", day, tmp_path, fake)

    out = capsys.readouterr()
    assert code == 0, out.err
    (row,) = written_rows(tmp_path)
    assert row["variant"] == "matrix-databricks-sql"
    assert row["row_count"] == FIXTURE_LINES
    notes = notes_of(row)
    assert notes["timing"] == "client"
    assert notes["cross_check"] == "content"
    assert notes["schema"] == "workspace.repopulse_matrix"
    assert notes["result_cache"] == "table_version_bump"
    assert notes["cache_check"] == "query_history"
    assert notes["warehouse_size"] == "2X-Small"
    # data and seed went to the matrix Volume only
    assert fake.uploads
    for path in fake.uploads:
        assert path.startswith("/Volumes/workspace/repopulse_matrix/"), path
    names = {Path(p).name for p in fake.uploads}
    assert {p.name for p in Path(day[0]).glob("*.json.gz")} <= names
    assert "repos_data_ecosystem.csv" in names
    assert (
        "/api/2.1/unity-catalog/schemas",
        {"catalog_name": "workspace", "name": "repopulse_matrix"},
    ) in fake.created
    # every write is CREATE OR REPLACE into workspace.repopulse_matrix
    texts = [s["statement"] for s in fake.statements]
    creates = [t for t in texts if t.lstrip().upper().startswith("CREATE")]
    assert len(creates) == 2
    assert all(
        t.lstrip().startswith("CREATE OR REPLACE TABLE workspace.repopulse_matrix.")
        for t in creates
    )
    for t in texts:
        assert "workspace.repopulse." not in t and "repopulse_dbt" not in t
    # timed statements are never byte-identical (result cache), and each run is checked
    queries = [t for t in texts if any(t.startswith(s) for s in fake.spark_sql.values())]
    assert len(queries) == 6 * 3  # warm-up + 2 timed runs
    assert len(set(queries)) == len(queries)
    assert fake.history_queries and len(fake.history_queries[-1]) == 6 * 2
    # right before every timed run both tables get a new version (metadata only), so the
    # warehouse result cache cannot answer a timed query; the history confirms no hit
    assert sorted(t for _, t in fake.alters) == ["raw_events", "raw_events", "repos", "repos"]
    q1_at = [i for i, t in enumerate(texts, start=1) if t.startswith(fake.spark_sql["q1"])]
    alter_at = {i for i, _ in fake.alters}
    assert len(q1_at) == 3
    for pos in q1_at[1:]:
        assert {pos - 2, pos - 1} <= alter_at, (pos, sorted(alter_at))
    assert not any(fake.from_cache[i] for i in fake.history_queries[-1])
    for secret in (TOKEN, "fake-ws.cloud.databricks.com", WAREHOUSE, USER):
        assert secret not in out.out + out.err


def test_databricks_sql_result_from_cache_makes_the_measurement_invalid(
    day, tmp_path, reference, capsys
):
    fake = FakeDatabricks(reference, cache_hit=True)

    assert run_dbx("databricks-sql", day, tmp_path, fake) == 1

    assert written_rows(tmp_path) == []
    assert "cache" in capsys.readouterr().err


def test_databricks_sql_result_that_differs_from_duckdb_writes_no_row(
    day, tmp_path, reference, capsys
):
    fake = FakeDatabricks(reference, tamper="q2")

    assert run_dbx("databricks-sql", day, tmp_path, fake) == 1

    assert written_rows(tmp_path) == []
    assert "q2" in capsys.readouterr().err


def test_databricks_without_secrets_fails_without_any_request(day, tmp_path, reference, capsys):
    fake = FakeDatabricks(reference)

    assert run_dbx("databricks-sql", day, tmp_path, fake, env={}) != 0

    assert fake.requests == []
    assert "DATABRICKS_HOST" in capsys.readouterr().err


@pytest.mark.parametrize("schema", ["repopulse", "repopulse_dbt", "repopulse_dbt_gold", "a b"])
def test_databricks_refuses_the_schemas_of_d1_and_d2(day, tmp_path, reference, schema, capsys):
    fake = FakeDatabricks(reference)

    assert run_dbx("databricks-sql", day, tmp_path, fake, "--schema", schema) == 2

    assert fake.requests == []
    assert schema in capsys.readouterr().err


def test_databricks_errors_are_redacted(day, tmp_path, reference, capsys):
    fake = FakeDatabricks(reference, me_status=403)

    assert run_dbx("databricks-sql", day, tmp_path, fake) == 1

    out = capsys.readouterr()
    text = out.out + out.err + (tmp_path / "summary.md").read_text(encoding="utf-8")
    assert TOKEN not in text
    assert "fake-ws.cloud.databricks.com" not in text
    assert "***" in text
    assert written_rows(tmp_path) == []


def test_databricks_serverless_runs_a_notebook_without_cluster_and_writes_a_row(
    day, tmp_path, reference, capsys
):
    fake = FakeDatabricks(reference)

    code = run_dbx("databricks-serverless", day, tmp_path, fake)

    out = capsys.readouterr()
    assert code == 0, out.err
    (row,) = written_rows(tmp_path)
    assert row["variant"] == "matrix-databricks-serverless"
    assert row["row_count"] == FIXTURE_LINES
    notes = notes_of(row)
    assert notes["timing"] == "notebook"
    assert notes["cross_check"] == "content"
    assert notes["spark"] == "4.0.0"
    assert notes["load_seconds"] == "2.5"
    assert notes["schema"] == "workspace.repopulse_matrix"
    # one serverless run: no cluster of any kind declared
    (submit,) = fake.submits
    (task,) = submit["tasks"]
    for key in ("new_cluster", "existing_cluster_id", "job_cluster_key"):
        assert key not in task and key not in submit
    assert task["notebook_task"]["base_parameters"]["runs"] == "2"
    # the notebook carries the Spark SQL of Q1-Q6 verbatim
    (imported,) = fake.imports
    source = base64.b64decode(imported["content"]).decode()
    tree = ast.parse(source)
    embedded = None
    for node in tree.body:
        if isinstance(node, ast.Assign) and getattr(node.targets[0], "id", "") == "QUERIES_JSON":
            embedded = json.loads(ast.literal_eval(node.value))
    assert embedded == {q: (SPARK_DIR / f"{q}.sql").read_text(encoding="utf-8") for q in QUERIES}
    assert "dbutils.notebook.exit" in source
    assert "CREATE OR REPLACE TABLE" in source
    for path in fake.uploads:
        assert path.startswith("/Volumes/workspace/repopulse_matrix/"), path
    for secret in (TOKEN, "fake-ws.cloud.databricks.com", USER):
        assert secret not in out.out + out.err


def test_databricks_serverless_result_that_differs_from_duckdb_writes_no_row(
    day, tmp_path, reference, capsys
):
    fake = FakeDatabricks(reference, tamper="q5")

    assert run_dbx("databricks-serverless", day, tmp_path, fake) == 1

    assert written_rows(tmp_path) == []
    assert "q5" in capsys.readouterr().err


# ---------------------------------------------------------------- Statement API values


@pytest.mark.parametrize(
    ("type_name", "text", "expected"),
    [
        ("LONG", "5", 5),
        ("INT", "-3", -3),
        ("SHORT", "2", 2),
        ("DOUBLE", "1.5", 1.5),
        ("FLOAT", "0.25", 0.25),
        ("DECIMAL", "1.25", 1.25),
        ("BOOLEAN", "true", True),
        ("BOOLEAN", "false", False),
        ("DATE", "2026-01-05", dt.date(2026, 1, 5)),
        ("TIMESTAMP", "2026-01-05T10:00:00.000Z", dt.datetime(2026, 1, 5, 10)),
        ("STRING", "dbt-labs/dbt-core", "dbt-labs/dbt-core"),
        ("LONG", None, None),
    ],
)
def test_statement_values_are_converted_by_column_type(type_name, text, expected):
    assert bench.convert_value(type_name, text) == expected


def test_statement_value_of_the_wrong_shape_is_an_error():
    with pytest.raises(bench.EngineMatrixError, match="LONG"):
        bench.convert_value("LONG", "five")


# ---------------------------------------------------------------- Spark (child process)


def test_spark_child_output_is_the_last_json_line():
    payload = {
        "spark_version": "3.5.3",
        "row_count": 34,
        "load_seconds": 1.0,
        "startup_seconds": 4.0,
        "warmup": 1,
        "runs": [timed_run(1.0)],
        "results": {q: {"columns": ["a"], "rows": [[1]]} for q in QUERIES},
    }

    parsed = bench.parse_child_output("log line\n" + json.dumps(payload) + "\n")

    assert parsed["row_count"] == 34
    assert parsed["results"]["q1"]["rows"] == [(1,)]


@pytest.mark.parametrize("stdout", ["", "not json", "[1, 2]", json.dumps({"row_count": 3})])
def test_spark_child_output_that_is_broken_is_an_error(stdout):
    with pytest.raises(bench.EngineMatrixError):
        bench.parse_child_output(stdout)


@pytest.mark.spark
def test_spark_engine_on_the_fixture_matches_duckdb_and_writes_a_row(spark, day, tmp_path):
    del spark  # only for the skip/fail policy of conftest; the benchmark starts its own JVM
    argv = cli(
        "spark",
        day,
        tmp_path,
        "--spark-master",
        "local[1]",
        "--spark-driver-memory",
        "1g",
        "--spark-shuffle-partitions",
        "1",
    )

    assert bench.main(argv) == 0

    (row,) = written_rows(tmp_path)
    assert row["variant"] == "matrix-spark"
    assert row["row_count"] == FIXTURE_LINES
    notes = notes_of(row)
    assert notes["timing"] == "in-process"
    assert notes["cross_check"] == "content"
    assert notes["spark"].startswith("3.5")
