"""Trino + Hive Metastore benchmark (P2c): DDL, REST result parser, cross-check, rows, CLI.

No network, no Docker, no Trino: HTTP goes through a fake transport. The REST fixtures in
``tests/fixtures/trino_rest/`` are HAND-WRITTEN from the Trino client protocol documentation
(not real server output; each file says so). The layouts are built from
``tests/fixtures/gharchive_small`` (34 lines, gzip-compressed into tmp at test time).
"""

from __future__ import annotations

import csv
import datetime as dt
import gzip
import hashlib
import json
import math
import re
import shutil
from pathlib import Path

import pytest

from benchmarks import query_opt, trino_bench
from benchmarks.record_run import read_artifact
from repopulse.runs_log import COLUMNS, validate_row

FIXTURE_DIR = Path(__file__).parent / "fixtures" / "gharchive_small"
REST_DIR = Path(__file__).parent / "fixtures" / "trino_rest"
FIXTURE_LINES = 34
QUERIES = tuple(f"q{i}" for i in range(1, 7))
VARIANTS = ("trino-flat", "trino-partitioned", "trino-clustered", "trino-partitioned-clustered")
SCHEMAS = ("trino_flat", "trino_partitioned", "trino_clustered", "trino_partitioned_clustered")
NOTE_TOKEN = re.compile(r"[a-z0-9_]+=[^\s,=]+")
STAT_KEYS = (
    "state",
    "elapsedTimeMillis",
    "cpuTimeMillis",
    "processedRows",
    "processedBytes",
    "physicalInputBytes",
    "peakMemoryBytes",
)
RAW_COLUMNS_DDL = (
    "id varchar, type varchar, actor ROW(id bigint, login varchar), "
    "repo ROW(id bigint, name varchar), payload varchar, created_at timestamp(3)"
)
PARTITIONED_COLUMNS_DDL = (
    "id varchar, actor ROW(id bigint, login varchar), repo ROW(id bigint, name varchar), "
    "payload varchar, created_at timestamp(3), type varchar"
)


def _rest(name: str) -> list[dict]:
    return json.loads((REST_DIR / name).read_text(encoding="utf-8"))["pages"]


def _gzip_fixture(out: Path) -> Path:
    out.mkdir(parents=True)
    for src in sorted(FIXTURE_DIR.glob("*.json")):
        with src.open("rb") as fin, gzip.open(out / f"{src.stem}.json.gz", "wb") as fout:
            shutil.copyfileobj(fin, fout)
    return out


def _stats(state: str = "FINISHED", **overrides) -> dict:
    stats = {
        "state": state,
        "queued": False,
        "scheduled": True,
        "nodes": 1,
        "cpuTimeMillis": 30,
        "wallTimeMillis": 40,
        "queuedTimeMillis": 1,
        "elapsedTimeMillis": 50,
        "processedRows": 34,
        "processedBytes": 9000,
        "physicalInputBytes": 4096,
        "peakMemoryBytes": 1024,
        "spilledBytes": 0,
    }
    stats.update(overrides)
    return stats


def _final(columns=None, data=None, **stats) -> dict:
    page: dict = {"id": "q", "stats": _stats(**stats)}
    if columns is not None:
        page["columns"] = [{"name": n, "type": t} for n, t in columns]
    if data is not None:
        page["data"] = data
    return page


# ---------------------------------------------------------------- variants and names


def test_four_variants_map_one_to_one_to_the_query_opt_layouts():
    assert trino_bench.VARIANTS == VARIANTS
    assert tuple(trino_bench.LAYOUT_OF[v] for v in VARIANTS) == query_opt.VARIANTS
    for variant in VARIANTS:  # every name is a valid runs.csv variant
        validate_row(
            {
                "run_at": "2026-11-11T10:00:00+00:00",
                "git_sha": "abc1234",
                "variant": variant,
                "data_date": "2026-10-25",
                "total_seconds": 0,
            }
        )


def test_schema_name_per_variant():
    assert tuple(trino_bench.schema_name(v) for v in VARIANTS) == SCHEMAS
    with pytest.raises(trino_bench.TrinoBenchError, match="trino-other"):
        trino_bench.schema_name("trino-other")


# ---------------------------------------------------------------- DDL


def _create(stmts: list[str], table: str) -> str:
    (hit,) = [s for s in stmts if s.startswith("CREATE TABLE ") and f".{table} (" in s]
    return hit


def test_flat_layout_ddl_is_an_external_parquet_table_without_partitions():
    stmts = trino_bench.layout_ddl("trino-flat", bucket="repopulse-bench")
    assert stmts[0] == (
        "CREATE SCHEMA IF NOT EXISTS hive.trino_flat "
        "WITH (location = 's3a://repopulse-bench/schemas/trino_flat')"
    )
    assert _create(stmts, "raw_events") == (
        f"CREATE TABLE hive.trino_flat.raw_events ({RAW_COLUMNS_DDL}) "
        "WITH (external_location = 's3a://repopulse-bench/qopt-flat', format = 'PARQUET')"
    )
    assert not any("partitioned_by" in s for s in stmts)
    assert not any(s.startswith("CALL ") for s in stmts)


def test_partitioned_layout_ddl_puts_the_partition_column_last_and_registers_partitions():
    stmts = trino_bench.layout_ddl("trino-partitioned", bucket="repopulse-bench")
    assert _create(stmts, "raw_events") == (
        f"CREATE TABLE hive.trino_partitioned.raw_events ({PARTITIONED_COLUMNS_DDL}) "
        "WITH (external_location = 's3a://repopulse-bench/qopt-partitioned', "
        "format = 'PARQUET', partitioned_by = ARRAY['type'])"
    )
    assert stmts[-1] == (
        "CALL hive.system.sync_partition_metadata('trino_partitioned', 'raw_events', 'ADD')"
    )


def test_clustered_layouts_point_at_their_own_parquet_directory():
    clustered = trino_bench.layout_ddl("trino-clustered", bucket="b-1")
    assert "external_location = 's3a://b-1/qopt-clustered'" in _create(clustered, "raw_events")
    assert "partitioned_by" not in _create(clustered, "raw_events")
    both = trino_bench.layout_ddl("trino-partitioned-clustered", bucket="b-1")
    raw = _create(both, "raw_events")
    assert "external_location = 's3a://b-1/qopt-partitioned-clustered'" in raw
    assert "partitioned_by = ARRAY['type']" in raw
    assert both[-1] == (
        "CALL hive.system.sync_partition_metadata("
        "'trino_partitioned_clustered', 'raw_events', 'ADD')"
    )


@pytest.mark.parametrize(("variant", "schema"), list(zip(VARIANTS, SCHEMAS, strict=True)))
def test_every_layout_has_the_repos_seed_table(variant, schema):
    stmts = trino_bench.layout_ddl(variant, bucket="repopulse-bench")
    assert _create(stmts, "repos") == (
        f"CREATE TABLE hive.{schema}.repos (repo_name varchar, category varchar) "
        "WITH (external_location = 's3a://repopulse-bench/repos', format = 'PARQUET')"
    )


@pytest.mark.parametrize("variant", VARIANTS)
def test_ddl_creates_the_schema_first_and_drops_old_tables_before_creating(variant):
    stmts = trino_bench.layout_ddl(variant, bucket="repopulse-bench")
    schema = trino_bench.schema_name(variant)
    assert stmts[0].startswith(f"CREATE SCHEMA IF NOT EXISTS hive.{schema} ")
    for table in ("raw_events", "repos"):
        drop = stmts.index(f"DROP TABLE IF EXISTS hive.{schema}.{table}")
        assert drop < stmts.index(_create(stmts, table))
    assert all(";" not in s for s in stmts)  # one statement per REST call


def test_create_table_sql_moves_partition_columns_to_the_end():
    sql = trino_bench.create_table_sql(
        "s",
        "t",
        (("a", "varchar"), ("p", "varchar"), ("b", "bigint")),
        location="s3a://bucket-x/t",
        partition_by=("p",),
    )
    assert sql == (
        "CREATE TABLE hive.s.t (a varchar, b bigint, p varchar) "
        "WITH (external_location = 's3a://bucket-x/t', format = 'PARQUET', "
        "partitioned_by = ARRAY['p'])"
    )


def test_create_table_sql_rejects_a_partition_column_that_does_not_exist():
    with pytest.raises(trino_bench.TrinoBenchError, match="'kind'"):
        trino_bench.create_table_sql(
            "s", "t", (("a", "varchar"),), location="s3a://bucket-x/t", partition_by=("kind",)
        )


@pytest.mark.parametrize(
    ("kwargs", "bad"),
    [
        ({"schema": "trino flat"}, "trino flat"),
        ({"schema": "x;drop schema y"}, "x;drop schema y"),
        ({"schema": "Trino"}, "Trino"),
        ({"schema": ""}, ""),
        ({"table": "raw-events"}, "raw-events"),
        ({"table": "t'--"}, "t'--"),
        ({"columns": (('a"b', "varchar"),)}, 'a"b'),
        ({"partition_by": ("ty pe",), "columns": (("ty pe", "varchar"),)}, "ty pe"),
        ({"location": "s3a://bucket-x/t' OR 1=1"}, "s3a://bucket-x/t' OR 1=1"),
        ({"location": "file:///etc/passwd"}, "file:///etc/passwd"),
    ],
)
def test_create_table_sql_rejects_unsafe_names_naming_the_value(kwargs, bad):
    args = {
        "schema": "s",
        "table": "t",
        "columns": (("a", "varchar"),),
        "location": "s3a://bucket-x/t",
        "partition_by": (),
    }
    args.update(kwargs)
    with pytest.raises(trino_bench.TrinoBenchError) as err:
        trino_bench.create_table_sql(
            args["schema"],
            args["table"],
            args["columns"],
            location=args["location"],
            partition_by=args["partition_by"],
        )
    assert repr(bad) in str(err.value)


@pytest.mark.parametrize("bucket", ["Bad_Bucket", "a", "bucket'--", "bucket/x", "-bucket"])
def test_layout_ddl_rejects_an_unsafe_bucket(bucket):
    with pytest.raises(trino_bench.TrinoBenchError) as err:
        trino_bench.layout_ddl("trino-flat", bucket=bucket)
    assert repr(bucket) in str(err.value)


def test_sync_partitions_sql_rejects_unsafe_names():
    with pytest.raises(trino_bench.TrinoBenchError, match="x'y"):
        trino_bench.sync_partitions_sql("x'y", "raw_events")


def test_partition_values_lists_the_hive_directories(tmp_path):
    for name in ("type=PushEvent", "type=IssuesEvent", "type=ForkEvent"):
        (tmp_path / name).mkdir()
    (tmp_path / "type=ForkEvent" / "data_0.parquet").write_bytes(b"x")
    assert trino_bench.partition_values(tmp_path, "type") == [
        "ForkEvent",
        "IssuesEvent",
        "PushEvent",
    ]


@pytest.mark.parametrize("name", ["type=Evil Event", "type=a'b", "type="])
def test_partition_values_rejects_unsafe_values_naming_them(tmp_path, name):
    (tmp_path / "type=PushEvent").mkdir()
    (tmp_path / name).mkdir()
    with pytest.raises(trino_bench.TrinoBenchError) as err:
        trino_bench.partition_values(tmp_path, "type")
    assert repr(name.split("=", 1)[1]) in str(err.value)


def test_partition_values_rejects_a_directory_that_is_not_a_partition(tmp_path):
    (tmp_path / "type=PushEvent").mkdir()
    (tmp_path / "stray").mkdir()
    with pytest.raises(trino_bench.TrinoBenchError, match="stray"):
        trino_bench.partition_values(tmp_path, "type")


def test_print_ddl_needs_no_git_sha_and_no_docker(capsys, monkeypatch):
    def _no_http(*args, **kwargs):
        raise AssertionError("--print-ddl must not call Trino")

    monkeypatch.setattr(trino_bench, "http_transport", _no_http)
    assert trino_bench.main(["--print-ddl", "--bucket", "repopulse-bench"]) == 0
    out = capsys.readouterr().out
    for schema in SCHEMAS:
        assert f"CREATE SCHEMA IF NOT EXISTS hive.{schema} " in out
        assert f"CREATE TABLE hive.{schema}.raw_events (" in out
    statements = [s.strip() for s in out.split(";") if s.strip()]
    expected = [s for v in VARIANTS for s in trino_bench.layout_ddl(v, bucket="repopulse-bench")]
    assert statements == expected


# ---------------------------------------------------------------- REST result parser


def test_parse_pages_joins_columns_and_data_over_pages():
    result = trino_bench.parse_pages(_rest("select_three_pages.json"), "q1 trino-flat")
    assert result["columns"] == ["repo_name", "period_start", "contributors", "share_pct"]
    assert result["types"] == ["varchar", "date", "bigint", "double"]
    assert result["rows"] == [
        ("apache/spark", dt.date(2026, 10, 19), 12, 33.33),
        ("duckdb/duckdb", dt.date(2026, 10, 19), 7, None),
        ("trinodb/trino", dt.date(2026, 10, 19), 3, 100.0),
    ]


def test_parse_pages_takes_the_stats_of_the_last_page():
    stats = trino_bench.parse_pages(_rest("select_three_pages.json"), "q1")["stats"]
    assert stats == {
        "state": "FINISHED",
        "elapsedTimeMillis": 150,
        "cpuTimeMillis": 350,
        "processedRows": 1000,
        "processedBytes": 60000,
        "physicalInputBytes": 48000,
        "peakMemoryBytes": 70000,
    }


def test_parse_pages_of_a_statement_without_result_columns():
    result = trino_bench.parse_pages([_final()], "ddl")
    assert result["columns"] == []
    assert result["rows"] == []
    assert result["stats"]["state"] == "FINISHED"


def test_parse_pages_raises_the_trino_error_with_name_and_message():
    with pytest.raises(trino_bench.TrinoBenchError) as err:
        trino_bench.parse_pages(_rest("error_page.json"), "q1 trino-flat")
    message = str(err.value)
    assert "q1 trino-flat" in message
    assert "FUNCTION_NOT_FOUND" in message
    assert "Function 'ends_with' not registered" in message


def test_parse_pages_raises_on_an_error_in_a_middle_page():
    pages = _rest("select_three_pages.json")
    pages[1]["error"] = {"message": "boom", "errorName": "GENERIC_INTERNAL_ERROR"}
    with pytest.raises(trino_bench.TrinoBenchError, match="GENERIC_INTERNAL_ERROR"):
        trino_bench.parse_pages(pages, "q2")


@pytest.mark.parametrize("state", ["FAILED", "RUNNING", "QUEUED", "CANCELED"])
def test_parse_pages_requires_the_finished_state(state):
    with pytest.raises(trino_bench.TrinoBenchError, match=state):
        trino_bench.parse_pages([_final(state=state)], "q3")


def test_parse_pages_rejects_a_last_page_that_still_has_a_next_uri():
    pages = _rest("select_three_pages.json")[:2]
    pages[-1]["stats"]["state"] = "FINISHED"
    with pytest.raises(trino_bench.TrinoBenchError, match="nextUri"):
        trino_bench.parse_pages(pages, "q3")


@pytest.mark.parametrize("key", STAT_KEYS)
def test_parse_pages_rejects_a_missing_stats_key(key):
    page = _final()
    del page["stats"][key]
    with pytest.raises(trino_bench.TrinoBenchError, match=key):
        trino_bench.parse_pages([page], "q4")


def test_parse_pages_rejects_a_last_page_without_stats():
    page = _final()
    del page["stats"]
    with pytest.raises(trino_bench.TrinoBenchError, match="stats"):
        trino_bench.parse_pages([page], "q4")


@pytest.mark.parametrize(
    "bad",
    [
        {"physicalInputBytes": -1},
        {"processedRows": math.nan},
        {"elapsedTimeMillis": math.inf},
        {"cpuTimeMillis": True},
        {"processedBytes": "9000"},
        {"peakMemoryBytes": 1.5},
        {"physicalInputBytes": None},
    ],
)
def test_parse_pages_rejects_bad_numbers(bad):
    with pytest.raises(trino_bench.TrinoBenchError):
        trino_bench.parse_pages([_final(**bad)], "q5")


@pytest.mark.parametrize(
    "pages",
    [
        [],
        ["not a page"],
        [{"id": "q", "data": [[1]], "stats": _stats()}],  # data without columns
        [_final(columns=[("a", "bigint")], data=[[1, 2]])],  # row longer than columns
        [_final(columns=[("a", "bigint")], data=[1])],  # row is not a list
        [_final(columns=[("a", "bigint")], data={"encoding": "json", "segments": []})],
    ],
)
def test_parse_pages_rejects_broken_responses(pages):
    with pytest.raises(trino_bench.TrinoBenchError):
        trino_bench.parse_pages(pages, "q6")


def test_parse_pages_rejects_the_spooling_protocol_by_name():
    page = _final(columns=[("a", "bigint")], data={"encoding": "json+zstd", "segments": []})
    with pytest.raises(trino_bench.TrinoBenchError, match="spool"):
        trino_bench.parse_pages([page], "q6")


def test_parse_pages_rejects_columns_that_change_between_pages():
    pages = _rest("select_three_pages.json")
    pages[2]["columns"] = pages[2]["columns"][:3]
    pages[2]["data"] = [["trinodb/trino", "2026-10-19", 3]]
    with pytest.raises(trino_bench.TrinoBenchError, match="columns"):
        trino_bench.parse_pages(pages, "q1")


def test_parse_pages_converts_values_by_column_type():
    columns = [
        ("d", "double"),
        ("e", "double"),
        ("n", "bigint"),
        ("i", "integer"),
        ("b", "boolean"),
        ("ts", "timestamp(3)"),
        ("s", "varchar(8)"),
    ]
    data = [
        ["NaN", 2, 5, 6, True, "2026-10-25 01:02:03.000", "x"],
        ["Infinity", -1.5, None, 0, False, None, None],
    ]
    rows = trino_bench.parse_pages([_final(columns=columns, data=data)], "q")["rows"]
    assert math.isnan(rows[0][0])
    assert rows[0][1:] == (2.0, 5, 6, True, dt.datetime(2026, 10, 25, 1, 2, 3), "x")
    assert rows[1][0] == math.inf
    assert rows[1][1:] == (-1.5, None, 0, False, None, None)


@pytest.mark.parametrize(
    ("kind", "value"),
    [("bigint", "5"), ("bigint", 1.5), ("bigint", True), ("date", "25/10/2026"), ("double", "x")],
)
def test_parse_pages_rejects_a_value_of_the_wrong_type(kind, value):
    with pytest.raises(trino_bench.TrinoBenchError, match="'c'"):
        trino_bench.parse_pages([_final(columns=[("c", kind)], data=[[value]])], "q")


def test_parse_info_reads_version_and_starting():
    info = {
        "nodeVersion": {"version": "484"},
        "environment": "docker",
        "coordinator": True,
        "starting": False,
        "uptime": "1.00m",
    }
    assert trino_bench.parse_info(info) == {"version": "484", "starting": False}


@pytest.mark.parametrize(
    "info",
    [
        {},
        {"nodeVersion": {"version": "484"}},  # no starting
        {"nodeVersion": {}, "starting": False},
        {"nodeVersion": {"version": "48 4"}, "starting": False},
        {"nodeVersion": {"version": "484,1"}, "starting": False},
        {"nodeVersion": {"version": "484"}, "starting": "no"},
    ],
)
def test_parse_info_rejects_broken_info(info):
    with pytest.raises(trino_bench.TrinoBenchError):
        trino_bench.parse_info(info)


# ---------------------------------------------------------------- REST client


class ScriptedTransport:
    """Answers POST /v1/statement and every nextUri from a list of (status, body) replies."""

    def __init__(self, replies):
        self.replies = list(replies)
        self.calls: list[tuple[str, str, dict, bytes | None]] = []

    def __call__(self, method, url, headers, body, timeout):
        self.calls.append((method, url, dict(headers), body))
        status, payload = self.replies.pop(0)
        return status, payload if isinstance(payload, bytes) else json.dumps(payload).encode()


def test_client_posts_the_sql_and_follows_next_uri_to_the_end():
    pages = _rest("select_three_pages.json")
    transport = ScriptedTransport([(200, p) for p in pages])
    client = trino_bench.TrinoClient("http://localhost:8080", transport=transport)
    result = client.execute("SELECT 1", schema="trino_flat")
    method, url, headers, body = transport.calls[0]
    assert (method, url, body) == ("POST", "http://localhost:8080/v1/statement", b"SELECT 1")
    assert headers["X-Trino-User"]
    assert headers["X-Trino-Catalog"] == "hive"
    assert headers["X-Trino-Schema"] == "trino_flat"
    assert headers["X-Trino-Time-Zone"] == "UTC"
    assert [(c[0], c[1]) for c in transport.calls[1:]] == [
        ("GET", pages[0]["nextUri"]),
        ("GET", pages[1]["nextUri"]),
    ]
    assert len(result["rows"]) == 3
    assert result["stats"]["physicalInputBytes"] == 48000
    assert result["pages"] == pages
    assert result["seconds"] >= 0


def test_client_retries_503_and_then_succeeds():
    sleeps: list[float] = []
    transport = ScriptedTransport([(503, b"busy"), (200, _final())])
    client = trino_bench.TrinoClient(
        "http://localhost:8080", transport=transport, sleep=sleeps.append
    )
    assert client.execute("SELECT 1")["stats"]["state"] == "FINISHED"
    assert len(transport.calls) == 2
    assert sleeps and all(0 < s <= 1 for s in sleeps)


def test_client_raises_on_an_http_error_status():
    transport = ScriptedTransport([(500, b"internal error")])
    client = trino_bench.TrinoClient("http://localhost:8080", transport=transport)
    with pytest.raises(trino_bench.TrinoBenchError, match="500"):
        client.execute("SELECT 1")


def test_client_raises_on_a_body_that_is_not_json():
    transport = ScriptedTransport([(200, b"<html>")])
    client = trino_bench.TrinoClient("http://localhost:8080", transport=transport)
    with pytest.raises(trino_bench.TrinoBenchError, match="JSON"):
        client.execute("SELECT 1")


def test_client_raises_the_query_error():
    transport = ScriptedTransport([(200, p) for p in _rest("error_page.json")])
    client = trino_bench.TrinoClient("http://localhost:8080", transport=transport)
    with pytest.raises(trino_bench.TrinoBenchError, match="FUNCTION_NOT_FOUND"):
        client.execute("SELECT ends_with('a', 'b')")


# ---------------------------------------------------------------- cross-check with DuckDB


def _result(columns, rows) -> dict:
    return {"columns": list(columns), "rows": [tuple(r) for r in rows]}


DUCK = _result(
    ["period_start", "scope_key", "active", "pct"],
    [
        (dt.date(2026, 10, 19), "apache/spark", 3, 33.33),
        (dt.date(2026, 10, 19), "duckdb/duckdb", 1, None),
        (dt.date(2026, 10, 26), "apache/spark", 2, 100.0),
    ],
)


def test_compare_accepts_the_same_rows_in_another_order():
    trino = _result(DUCK["columns"], list(reversed(DUCK["rows"])))
    trino_bench.compare_results("trino-flat", "q1", trino, DUCK)


def test_compare_names_layout_query_and_columns_that_differ():
    trino = _result(["period_start", "scope_key", "pct", "active"], DUCK["rows"])
    with pytest.raises(trino_bench.TrinoBenchError) as err:
        trino_bench.compare_results("trino-partitioned", "q2", trino, DUCK)
    message = str(err.value)
    assert "trino-partitioned" in message
    assert "q2" in message
    assert "columns" in message


def test_compare_names_the_first_row_that_differs():
    rows = list(DUCK["rows"])
    rows[1] = (dt.date(2026, 10, 19), "duckdb/duckdb", 2, None)
    with pytest.raises(trino_bench.TrinoBenchError) as err:
        trino_bench.compare_results("trino-clustered", "q3", _result(DUCK["columns"], rows), DUCK)
    message = str(err.value)
    assert "trino-clustered" in message
    assert "q3" in message
    assert "duckdb/duckdb" in message


def test_compare_treats_rows_as_a_multiset():
    rows = [*DUCK["rows"][:2], DUCK["rows"][0]]  # one row twice, one missing
    with pytest.raises(trino_bench.TrinoBenchError, match="q4"):
        trino_bench.compare_results("trino-flat", "q4", _result(DUCK["columns"], rows), DUCK)
    with pytest.raises(trino_bench.TrinoBenchError, match="3"):
        trino_bench.compare_results(
            "trino-flat", "q4", _result(DUCK["columns"], DUCK["rows"][:2]), DUCK
        )


def test_compare_accepts_floats_within_the_tolerance_only():
    close = [(r[0], r[1], r[2], None if r[3] is None else r[3] + 1e-9) for r in DUCK["rows"]]
    trino_bench.compare_results("trino-flat", "q5", _result(DUCK["columns"], close), DUCK)
    far = [(r[0], r[1], r[2], None if r[3] is None else r[3] + 0.01) for r in DUCK["rows"]]
    with pytest.raises(trino_bench.TrinoBenchError, match="q5"):
        trino_bench.compare_results("trino-flat", "q5", _result(DUCK["columns"], far), DUCK)


def test_compare_treats_int_and_float_of_the_same_value_as_equal():
    trino = _result(["n"], [(3.0,), (0,)])
    trino_bench.compare_results("trino-flat", "q6", trino, _result(["n"], [(3,), (0.0,)]))


def test_compare_dates_and_timestamps_with_iso_strings():
    duck = _result(["d", "ts"], [(dt.date(2026, 10, 19), dt.datetime(2026, 10, 25, 1, 2, 3))])
    same = _result(["d", "ts"], [("2026-10-19", "2026-10-25 01:02:03.000")])
    trino_bench.compare_results("trino-flat", "q1", same, duck)
    other = _result(["d", "ts"], [("2026-10-20", "2026-10-25 01:02:03.000")])
    with pytest.raises(trino_bench.TrinoBenchError, match="2026-10-20"):
        trino_bench.compare_results("trino-flat", "q1", other, duck)


@pytest.mark.parametrize(("trino", "duck"), [(None, 0), (0, None), (True, 1), (False, 0)])
def test_compare_keeps_null_and_bool_apart_from_numbers(trino, duck):
    with pytest.raises(trino_bench.TrinoBenchError):
        trino_bench.compare_results(
            "trino-flat", "q2", _result(["x"], [(trino,)]), _result(["x"], [(duck,)])
        )


def test_compare_accepts_equal_nulls_and_bools():
    rows = [(None, True), (None, False)]
    trino_bench.compare_results(
        "trino-flat", "q2", _result(["a", "b"], rows), _result(["a", "b"], rows)
    )


# ---------------------------------------------------------------- one row per layout


def _run(**overrides) -> dict:
    run = {"total_seconds": 0.6, "row_count": FIXTURE_LINES}
    for q in QUERIES:
        run[f"{q}_seconds"] = 0.1
        run[f"{q}_rows"] = 3
    run.update(overrides)
    return run


def _inputs() -> dict:
    return {
        q: {"physicalInputBytes": 1000 * i, "processedRows": 10 * i}
        for i, q in enumerate(QUERIES, start=1)
    }


def _layout_info(**overrides) -> dict:
    info = {"prepare_seconds": 0.25, "layout_bytes": 4096, "files": 9}
    info.update(overrides)
    return info


def _row(runs=None, variant="trino-partitioned", **kwargs) -> dict:
    args = {
        "variant": variant,
        "git_sha": "abc1234",
        "data_date": "2026-10-25",
        "run_at": "2026-11-11T10:00:00+00:00",
        "layout": _layout_info(),
        "trino_version": "484",
        "inputs": _inputs(),
        "bytes_equal": True,
    }
    args.update(kwargs)
    runs = runs if runs is not None else [_run(), _run(total_seconds=0.8), _run(total_seconds=0.7)]
    return trino_bench.build_row(runs, **args)


def _notes(row: dict) -> dict[str, str]:
    tokens = row["notes"].split(" ")
    for token in tokens:
        assert NOTE_TOKEN.fullmatch(token), (token, row["notes"])
    return dict(t.split("=", 1) for t in tokens)


def test_row_is_valid_with_median_times_and_no_peak_rss():
    row = _row()
    validate_row(row)
    assert row["variant"] == "trino-partitioned"
    assert row["data_date"] == "2026-10-25"
    assert row["total_seconds"] == 0.7  # median of 0.6, 0.8, 0.7
    assert row["q3_seconds"] == 0.1
    assert row["row_count"] == FIXTURE_LINES
    assert row["peak_rss_mb"] is None  # the client cannot measure the server's RSS
    assert validate_row(row)["peak_rss_mb"] == ""


def test_bytes_read_is_the_sum_of_physical_input_bytes_of_q1_to_q6():
    row = _row()
    assert row["bytes_read"] == sum(1000 * i for i in range(1, 7))


def test_notes_hold_engine_layout_and_per_query_input():
    notes = _notes(_row())
    assert notes["runs"] == "3"
    assert notes["stat"] == "median"
    assert notes["warmup"] == "1"
    assert notes["engine"] == "trino"
    assert notes["trino"] == "484"
    assert notes["partition_by"] == "type"
    assert notes["sort_by"] == "none"
    assert notes["files"] == "9"
    assert notes["layout_bytes"] == "4096"
    assert notes["bytes"] == "physical_input_bytes"
    assert notes["bytes_from"] == "run1"
    assert notes["bytes_equal_across_runs"] == "true"
    assert notes["cross_check"] == "full"
    for i, q in enumerate(QUERIES, start=1):
        assert notes[f"{q}_input_bytes"] == str(1000 * i)
        assert notes[f"{q}_input_rows"] == str(10 * i)
    assert "," not in _row()["notes"]


def test_notes_say_when_input_bytes_changed_between_runs():
    assert _notes(_row(bytes_equal=False))["bytes_equal_across_runs"] == "false"


def test_notes_sort_keys_of_the_clustered_layout():
    notes = _notes(_row(variant="trino-clustered"))
    assert notes["sort_by"] == "type+repo.name+created_at"
    assert notes["partition_by"] == "none"


@pytest.mark.parametrize("key", ["q2_rows", "row_count"])
def test_counts_that_change_between_runs_are_an_error(key):
    with pytest.raises(trino_bench.TrinoBenchError, match=key):
        _row(runs=[_run(), _run(**{key: 4})])


def test_build_row_rejects_no_runs():
    with pytest.raises(trino_bench.TrinoBenchError):
        _row(runs=[])


@pytest.mark.parametrize("version", ["48 4", "484,1", "", "a=b"])
def test_build_row_rejects_a_version_that_breaks_notes(version):
    with pytest.raises(trino_bench.TrinoBenchError):
        _row(trino_version=version)


def test_build_row_rejects_missing_input_stats():
    inputs = _inputs()
    del inputs["q4"]
    with pytest.raises(trino_bench.TrinoBenchError, match="q4"):
        _row(inputs=inputs)


# ---------------------------------------------------------------- CLI with a fake Trino


def _json_value(value):
    if isinstance(value, dt.datetime):
        return value.isoformat(sep=" ", timespec="milliseconds")
    if isinstance(value, dt.date):
        return value.isoformat()
    return value


def _trino_type(values) -> str:
    for value in values:
        if value is None:
            continue
        if isinstance(value, bool):
            return "boolean"
        if isinstance(value, int):
            return "bigint"
        if isinstance(value, float):
            return "double"
        if isinstance(value, dt.date):
            return "date"
        return "varchar"
    return "varchar"


class FakeTrino:
    """A Trino coordinator in a dict: DDL succeeds, Q1..Q6 return the DuckDB results.

    ``tamper``: schema whose q4 result gets one value changed; ``missing_partition``: schema
    whose ``$partitions`` count is one short; ``fail``: schema whose q2 fails with an error.
    """

    def __init__(self, duckdb_results, work_dir, *, tamper=None, missing_partition=None, fail=None):
        self.duckdb = duckdb_results
        self.work_dir = Path(work_dir)
        self.tamper = tamper
        self.missing_partition = missing_partition
        self.fail = fail
        self.queries = {sql.strip(): q for q, sql in trino_bench.query_sql().items()}
        self.calls: list[tuple[str, str, dict, bytes | None]] = []
        self.pending: dict[str, list[dict]] = {}

    def statements(self, schema: str) -> list[str]:
        return [
            body.decode()
            for method, _, headers, body in self.calls
            if method == "POST" and headers.get("X-Trino-Schema") == schema
        ]

    def _answer(self, sql: str, schema: str):
        stripped = sql.strip()
        if stripped.startswith(("CREATE ", "DROP ", "CALL ")):
            return None, None
        if stripped == trino_bench.COUNT_SQL:
            return [("_col0", "bigint")], [[FIXTURE_LINES]]
        if stripped == trino_bench.PARTITION_COUNT_SQL:
            variant = trino_bench.VARIANTS[trino_bench.SCHEMAS.index(schema)]
            root = query_opt.layout_path(self.work_dir, trino_bench.LAYOUT_OF[variant])
            count = len([p for p in root.iterdir() if p.is_dir()])
            if schema == self.missing_partition:
                count -= 1
            return [("_col0", "bigint")], [[count]]
        q = self.queries[stripped]
        if schema == self.fail and q == "q2":
            raise LookupError("line 1:1: Table 'hive.x.raw_events' does not exist")
        result = self.duckdb[q]
        rows = [[_json_value(v) for v in row] for row in result["rows"]]
        if schema == self.tamper and q == "q4" and rows:
            rows[0][-1] = (rows[0][-1] or 0) + 1
        types = [_trino_type([r[i] for r in result["rows"]]) for i in range(len(result["columns"]))]
        return list(zip(result["columns"], types, strict=True)), rows

    def __call__(self, method, url, headers, body, timeout):
        self.calls.append((method, url, dict(headers), body))
        if method == "GET" and url == "http://localhost:8080/v1/info":
            info = {"nodeVersion": {"version": "484"}, "starting": False, "coordinator": True}
            return 200, json.dumps(info).encode()
        if method == "POST" and url == "http://localhost:8080/v1/statement":
            qid = f"q{len(self.calls)}"
            try:
                columns, rows = self._answer(body.decode(), headers.get("X-Trino-Schema"))
            except LookupError as exc:
                failed = {
                    "id": qid,
                    "stats": _stats("FAILED"),
                    "error": {"message": str(exc), "errorName": "TABLE_NOT_FOUND"},
                }
                return 200, json.dumps(failed).encode()
            last = _final(columns=columns, data=None if rows is None else rows[len(rows) // 2 :])
            last["id"] = qid
            pages = [last]
            if rows is not None:
                middle = {
                    "id": qid,
                    "columns": last["columns"],
                    "data": rows[: len(rows) // 2],
                    "nextUri": f"http://localhost:8080/v1/statement/executing/{qid}/y/2",
                    "stats": _stats("RUNNING"),
                }
                pages = [middle, last]
            self.pending[qid] = pages
            first = {
                "id": qid,
                "nextUri": f"http://localhost:8080/v1/statement/queued/{qid}/y/1",
                "stats": _stats("QUEUED"),
            }
            return 200, json.dumps(first).encode()
        if method == "GET" and "/v1/statement/" in url:
            qid = url.split("/")[-3]
            return 200, json.dumps(self.pending[qid].pop(0)).encode()
        return 404, b"not found"


def _reference_for(data_dir: Path) -> str:
    lines = ['data_date = "2026-01-05"', 'url_template = "https://data.gharchive.org/{name}"']
    for hour, path in enumerate(sorted(data_dir.glob("*.json.gz"))):
        blob = path.read_bytes()
        lines += [
            "[[files]]",
            f"hour = {hour}",
            f'name = "{path.name}"',
            f'sha256 = "{hashlib.sha256(blob).hexdigest()}"',
            f"bytes = {len(blob)}",
        ]
    return "\n".join(lines) + "\n"


@pytest.fixture(scope="module")
def duckdb_results(tmp_path_factory) -> dict:
    """What DuckDB answers for Q1..Q6 on the fixture (the fake Trino replays it)."""
    base = tmp_path_factory.mktemp("trino_duckdb")
    raw = _gzip_fixture(base / "raw")
    work = base / "layouts"
    info = query_opt.prepare_layouts(raw, work)
    assert info["errors"] == {}
    return query_opt.run_once(work, "qopt-flat", collect=True)["results"]


@pytest.fixture
def cli(tmp_path):
    raw = _gzip_fixture(tmp_path / "raw")
    ref = tmp_path / "reference.toml"
    ref.write_text(_reference_for(raw), encoding="utf-8")
    out_dir = tmp_path / "out"
    work = tmp_path / "work"
    argv = [
        "--data-dir",
        str(raw),
        "--reference",
        str(ref),
        "--work-dir",
        str(work),
        "--out",
        str(out_dir / "trino-rows.csv"),
        "--summary",
        str(out_dir / "trino-summary.md"),
        "--samples-dir",
        str(out_dir / "trino-samples"),
    ]
    return {"argv": argv, "raw": raw, "work": work, "out": out_dir}


@pytest.fixture
def no_upload(monkeypatch) -> list:
    uploads: list = []
    monkeypatch.setattr(
        trino_bench, "upload_layouts", lambda work_dir, **kwargs: uploads.append(work_dir)
    )
    return uploads


def _install(monkeypatch, fake) -> None:
    monkeypatch.setattr(trino_bench, "http_transport", fake)


def test_defaults():
    args = trino_bench.build_parser().parse_args([])
    assert args.runs == 10
    assert args.trino_url == "http://localhost:8080"
    assert args.bucket == "repopulse-bench"
    assert str(args.out).replace("\\", "/") == "out/trino-rows.csv"
    assert str(args.summary).replace("\\", "/") == "out/trino-summary.md"


def test_main_requires_git_sha(cli, monkeypatch, capsys):
    def _must_not_run(*args, **kwargs):
        raise AssertionError("ran without --git-sha")

    monkeypatch.setattr(trino_bench, "http_transport", _must_not_run)
    monkeypatch.setattr(query_opt, "prepare_in_subprocess", _must_not_run)
    assert trino_bench.main(cli["argv"]) == 2
    assert "git-sha" in capsys.readouterr().err
    assert not (cli["out"] / "trino-rows.csv").exists()
    assert not (cli["out"] / "trino-summary.md").exists()


def test_main_stops_with_exit_2_on_checksum_mismatch(cli, monkeypatch):
    victim = sorted(cli["raw"].glob("*.json.gz"))[0]
    victim.write_bytes(victim.read_bytes() + b"tampered")

    def _must_not_run(*args, **kwargs):
        raise AssertionError("ran despite a checksum mismatch")

    monkeypatch.setattr(trino_bench, "http_transport", _must_not_run)
    monkeypatch.setattr(query_opt, "prepare_in_subprocess", _must_not_run)
    assert trino_bench.main([*cli["argv"], "--git-sha", "abc1234"]) == 2
    assert not (cli["out"] / "trino-rows.csv").exists()


def test_main_end_to_end_writes_four_rows(cli, monkeypatch, duckdb_results, no_upload):
    fake = FakeTrino(duckdb_results, cli["work"])
    _install(monkeypatch, fake)
    assert trino_bench.main([*cli["argv"], "--git-sha", "abc1234", "--runs", "2"]) == 0
    out = cli["out"] / "trino-rows.csv"
    with out.open(encoding="utf-8", newline="") as fh:
        assert next(csv.reader(fh)) == list(COLUMNS)
    rows = read_artifact(out)
    assert [r["variant"] for r in rows] == list(VARIANTS)
    for row in rows:
        assert row["data_date"] == "2026-01-05"
        assert row["git_sha"] == "abc1234"
        assert row["row_count"] == FIXTURE_LINES
        assert row["peak_rss_mb"] is None
        notes = _notes(row)
        assert notes["runs"] == "2"
        assert notes["engine"] == "trino"
        assert notes["trino"] == "484"
        assert notes["cross_check"] == "full"
        assert row["bytes_read"] == sum(int(notes[f"{q}_input_bytes"]) for q in QUERIES)
    assert no_upload == [cli["work"]]
    summary = (cli["out"] / "trino-summary.md").read_text(encoding="utf-8")
    for variant in VARIANTS:
        assert f"`{variant}`" in summary


def test_main_registers_each_layout_with_its_ddl(cli, monkeypatch, duckdb_results, no_upload):
    fake = FakeTrino(duckdb_results, cli["work"])
    _install(monkeypatch, fake)
    assert trino_bench.main([*cli["argv"], "--git-sha", "abc1234", "--runs", "1"]) == 0
    for variant, schema in zip(VARIANTS, SCHEMAS, strict=True):
        sent = fake.statements(schema)
        ddl = trino_bench.layout_ddl(variant, bucket="repopulse-bench")
        assert sent[: len(ddl)] == ddl
        assert trino_bench.COUNT_SQL in sent
        assert (trino_bench.PARTITION_COUNT_SQL in sent) == ("partitioned" in variant)
        # 1 warm-up + 1 timed run of every query
        for q, sql in trino_bench.query_sql().items():
            assert sum(s.strip() == sql.strip() for s in sent) == 2, (variant, q)


def test_main_saves_raw_q3_responses_as_samples(cli, monkeypatch, duckdb_results, no_upload):
    _install(monkeypatch, FakeTrino(duckdb_results, cli["work"]))
    assert trino_bench.main([*cli["argv"], "--git-sha", "abc1234", "--runs", "1"]) == 0
    for variant in ("trino-flat", "trino-partitioned"):
        sample = json.loads(
            (cli["out"] / "trino-samples" / f"q3_{variant}.json").read_text(encoding="utf-8")
        )
        assert sample["query"] == "q3"
        assert sample["layout"] == variant
        assert sample["trino_version"] == "484"
        assert sample["pages"][-1]["stats"]["state"] == "FINISHED"
        assert "nextUri" not in sample["pages"][-1]


def test_main_drops_only_the_layout_whose_results_differ(
    cli, monkeypatch, duckdb_results, no_upload, capsys
):
    _install(monkeypatch, FakeTrino(duckdb_results, cli["work"], tamper="trino_partitioned"))
    assert trino_bench.main([*cli["argv"], "--git-sha", "abc1234", "--runs", "1"]) == 1
    rows = read_artifact(cli["out"] / "trino-rows.csv")
    assert [r["variant"] for r in rows] == [
        "trino-flat",
        "trino-clustered",
        "trino-partitioned-clustered",
    ]
    err = capsys.readouterr().err
    assert "trino-partitioned" in err
    assert "q4" in err
    assert "FAILED" in (cli["out"] / "trino-summary.md").read_text(encoding="utf-8")


def test_main_fails_a_layout_whose_partitions_are_not_all_registered(
    cli, monkeypatch, duckdb_results, no_upload, capsys
):
    fake = FakeTrino(duckdb_results, cli["work"], missing_partition="trino_partitioned_clustered")
    _install(monkeypatch, fake)
    assert trino_bench.main([*cli["argv"], "--git-sha", "abc1234", "--runs", "1"]) == 1
    rows = read_artifact(cli["out"] / "trino-rows.csv")
    assert [r["variant"] for r in rows] == list(VARIANTS[:3])
    assert "partition" in capsys.readouterr().err


def test_main_keeps_going_when_a_query_fails_on_one_layout(
    cli, monkeypatch, duckdb_results, no_upload, capsys
):
    _install(monkeypatch, FakeTrino(duckdb_results, cli["work"], fail="trino_clustered"))
    assert trino_bench.main([*cli["argv"], "--git-sha", "abc1234", "--runs", "1"]) == 1
    rows = read_artifact(cli["out"] / "trino-rows.csv")
    assert [r["variant"] for r in rows] == [
        "trino-flat",
        "trino-partitioned",
        "trino-partitioned-clustered",
    ]
    assert "TABLE_NOT_FOUND" in capsys.readouterr().err


def test_main_fails_every_layout_when_the_upload_fails(cli, monkeypatch, duckdb_results, capsys):
    def _boom(work_dir, **kwargs):
        raise trino_bench.TrinoBenchError("upload failed (exit 1): bucket unreachable")

    monkeypatch.setattr(trino_bench, "upload_layouts", _boom)
    _install(monkeypatch, FakeTrino(duckdb_results, cli["work"]))
    assert trino_bench.main([*cli["argv"], "--git-sha", "abc1234", "--runs", "1"]) == 1
    assert read_artifact(cli["out"] / "trino-rows.csv") == []
    summary = (cli["out"] / "trino-summary.md").read_text(encoding="utf-8")
    assert summary.count("FAILED") == len(VARIANTS)
    assert "bucket unreachable" in capsys.readouterr().err


def test_main_writes_the_repos_seed_as_parquet_for_the_catalog(
    cli, monkeypatch, duckdb_results, no_upload
):
    import duckdb

    _install(monkeypatch, FakeTrino(duckdb_results, cli["work"]))
    assert trino_bench.main([*cli["argv"], "--git-sha", "abc1234", "--runs", "1"]) == 0
    files = sorted((cli["work"] / "repos").glob("*.parquet"))
    assert len(files) == 1
    con = duckdb.connect()
    try:
        described = con.execute(
            f"DESCRIBE SELECT * FROM read_parquet('{files[0].as_posix()}')"
        ).fetchall()
        seed_rows = con.execute(
            f"SELECT count(*) FROM read_csv('{trino_bench.DEFAULT_SEED.as_posix()}', header = true)"
        ).fetchone()[0]
        rows = con.execute(f"SELECT count(*) FROM read_parquet('{files[0].as_posix()}')").fetchone()
    finally:
        con.close()
    assert [(name, kind) for name, kind, *_ in described] == [
        ("repo_name", "VARCHAR"),
        ("category", "VARCHAR"),
    ]
    assert rows[0] == seed_rows
