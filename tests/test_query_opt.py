"""Query optimization benchmark (PR 10): Q1-Q6 verbatim on four parquet layouts of one day.

No network. The layouts are built from ``tests/fixtures/gharchive_small`` (34 lines, gzip-compressed
into tmp at test time). The profiling fixtures in ``tests/fixtures/duckdb_profile/`` are real
``EXPLAIN (ANALYZE, FORMAT JSON)`` output of the installed DuckDB (version inside each file).
``/proc/self/io`` exists only on Linux: the parser tests run everywhere, the tests that read the
real file skip elsewhere (they always run on the Linux CI runner).
"""

from __future__ import annotations

import csv
import gzip
import hashlib
import json
import math
import re
import shutil
from pathlib import Path

import duckdb
import pytest

from benchmarks import naive_duckdb, query_opt
from benchmarks.record_run import read_artifact
from repopulse.runs_log import COLUMNS, validate_row

FIXTURE_DIR = Path(__file__).parent / "fixtures" / "gharchive_small"
PROFILE_DIR = Path(__file__).parent / "fixtures" / "duckdb_profile"
FIXTURE_LINES = 34
QUERIES = tuple(f"q{i}" for i in range(1, 7))
VARIANTS = ("qopt-flat", "qopt-partitioned", "qopt-clustered", "qopt-partitioned-clustered")
HAS_PROC_IO = Path("/proc/self/io").is_file()
NOTE_TOKEN = re.compile(r"[a-z0-9_]+=[^\s,=]+")

PROC_IO_SAMPLE = """rchar: 323934931
wchar: 323929600
syscr: 632687
syscw: 632675
read_bytes: 4096
write_bytes: 323932160
cancelled_write_bytes: 0
"""


def _gzip_fixture(out: Path) -> Path:
    out.mkdir(parents=True)
    for src in sorted(FIXTURE_DIR.glob("*.json")):
        with src.open("rb") as fin, gzip.open(out / f"{src.stem}.json.gz", "wb") as fout:
            shutil.copyfileobj(fin, fout)
    return out


@pytest.fixture
def gz_dir(tmp_path: Path) -> Path:
    return _gzip_fixture(tmp_path / "raw")


@pytest.fixture(scope="module")
def prepared(tmp_path_factory) -> dict:
    """Raw fixture, the four layouts written once, and the naive baseline results."""
    base = tmp_path_factory.mktemp("query_opt")
    raw = _gzip_fixture(base / "raw")
    work = base / "layouts"
    info = query_opt.prepare_layouts(raw, work)
    return {
        "raw": raw,
        "work": work,
        "info": info,
        "naive": naive_duckdb.run_once(raw, collect=True),
    }


def _run(**overrides) -> dict:
    run = {"total_seconds": 0.5, "row_count": FIXTURE_LINES, "peak_rss_mb": 120.5}
    for q in QUERIES:
        run[f"{q}_seconds"] = 0.05
        run[f"{q}_rows"] = 3
    run.update(overrides)
    return run


def _scan(bytes_per_query: int | None = 1000, row_groups: bool = True, **overrides) -> dict:
    scan: dict = {}
    for i, q in enumerate(QUERIES, start=1):
        scan[f"{q}_bytes"] = None if bytes_per_query is None else bytes_per_query * i
        scan[f"{q}_scan_rows"] = 10 * i
        scan[f"{q}_files_read"] = i
        # DuckDB 2.x reports row groups per scan, 1.x does not (None)
        scan[f"{q}_row_groups_scanned"] = i if row_groups else None
        scan[f"{q}_row_groups_total"] = 2 * i if row_groups else None
    scan.update(overrides)
    return scan


def _layout_info(**overrides) -> dict:
    info = {"prepare_seconds": 0.25, "layout_bytes": 4096, "files": 9}
    info.update(overrides)
    return info


def _summarize(runs=None, scan=None, variant="qopt-partitioned", **kwargs) -> dict:
    return query_opt.summarize_layout(
        runs if runs is not None else [_run(), _run(total_seconds=0.7), _run(total_seconds=0.6)],
        scan if scan is not None else _scan(),
        variant=variant,
        git_sha="abc1234",
        data_date="2026-10-25",
        run_at="2026-11-11T10:00:00+00:00",
        layout=kwargs.pop("layout", _layout_info()),
        **kwargs,
    )


def _notes(row: dict) -> dict[str, str]:
    tokens = row["notes"].split(" ")
    for token in tokens:
        assert NOTE_TOKEN.fullmatch(token), (token, row["notes"])
    return dict(t.split("=", 1) for t in tokens)


def _sorted_by(path: Path, keys: str) -> tuple[list, list]:
    con = duckdb.connect()
    try:
        rows = con.execute(
            f"SELECT {keys} FROM read_parquet('{path.as_posix()}', file_row_number = true) "
            "ORDER BY file_row_number"
        ).fetchall()
    finally:
        con.close()
    return rows, sorted(rows)


# ---------------------------------------------------------------- layouts


def test_four_layouts_before_partition_clustering_and_both():
    assert tuple(layout.variant for layout in query_opt.LAYOUTS) == VARIANTS
    by_name = {layout.variant: layout for layout in query_opt.LAYOUTS}
    assert by_name["qopt-flat"].partition_by == ()
    assert by_name["qopt-flat"].sort_by == ()
    assert by_name["qopt-partitioned"].partition_by == ("type",)
    assert by_name["qopt-partitioned"].sort_by == ()
    assert by_name["qopt-clustered"].partition_by == ()
    assert by_name["qopt-clustered"].sort_by == ("type", "repo.name", "created_at")
    assert by_name["qopt-partitioned-clustered"].partition_by == ("type",)
    assert by_name["qopt-partitioned-clustered"].sort_by == ("repo.name", "created_at")
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


# ---------------------------------------------------------------- run output parser


def test_parse_run_output_takes_the_last_json_line():
    run = _run()
    out = "some log line\n" + json.dumps(run) + "\n\n"
    assert query_opt.parse_run_output(out, "qopt-flat") == run


def test_parse_run_output_accepts_missing_peak_rss():
    run = _run(peak_rss_mb=None)
    assert query_opt.parse_run_output(json.dumps(run), "qopt-flat")["peak_rss_mb"] is None


@pytest.mark.parametrize("key", ["total_seconds", "q3_seconds", "q6_rows", "row_count"])
def test_parse_run_output_rejects_missing_key(key):
    run = _run()
    del run[key]
    with pytest.raises(query_opt.QueryOptError, match=key):
        query_opt.parse_run_output(json.dumps(run), "qopt-flat")


@pytest.mark.parametrize(
    "bad",
    [
        {"q1_seconds": -0.1},
        {"q2_seconds": math.nan},
        {"total_seconds": math.inf},
        {"q4_seconds": True},
        {"q1_rows": 2.5},
        {"row_count": False},
        {"q5_rows": -1},
        {"q6_seconds": "0.1"},
        {"peak_rss_mb": -3.0},
    ],
)
def test_parse_run_output_rejects_bad_numbers(bad):
    run = _run(**bad)
    with pytest.raises(query_opt.QueryOptError):
        query_opt.parse_run_output(json.dumps(run), "qopt-flat")


@pytest.mark.parametrize("stdout", ["", "  \n", "not json at all", "[1, 2, 3]", '"text"'])
def test_parse_run_output_rejects_non_object_output(stdout):
    with pytest.raises(query_opt.QueryOptError, match="qopt-clustered"):
        query_opt.parse_run_output(stdout, "qopt-clustered")


# ---------------------------------------------------------------- scan output parser


def test_parse_scan_output_with_bytes():
    scan = _scan()
    assert query_opt.parse_scan_output(json.dumps(scan), "qopt-flat") == scan


def test_parse_scan_output_without_bytes():
    scan = _scan(bytes_per_query=None)
    assert query_opt.parse_scan_output(json.dumps(scan), "qopt-flat") == scan


def test_parse_scan_output_rejects_bytes_for_only_some_queries():
    scan = _scan(q3_bytes=None)
    with pytest.raises(query_opt.QueryOptError, match="bytes"):
        query_opt.parse_scan_output(json.dumps(scan), "qopt-flat")


def test_parse_scan_output_without_row_groups():
    scan = _scan(row_groups=False)
    assert query_opt.parse_scan_output(json.dumps(scan), "qopt-flat") == scan


@pytest.mark.parametrize("key", ["q2_row_groups_scanned", "q6_row_groups_total"])
def test_parse_scan_output_rejects_row_groups_for_only_some_queries(key):
    with pytest.raises(query_opt.QueryOptError, match="row_groups"):
        query_opt.parse_scan_output(json.dumps(_scan(**{key: None})), "qopt-flat")


@pytest.mark.parametrize(
    "bad",
    [
        {"q1_bytes": -1},
        {"q2_scan_rows": 1.5},
        {"q3_files_read": True},
        {"q4_bytes": "9"},
        {"q5_row_groups_scanned": -2},
        {"q6_row_groups_total": 1.5},
    ],
)
def test_parse_scan_output_rejects_bad_numbers(bad):
    with pytest.raises(query_opt.QueryOptError):
        query_opt.parse_scan_output(json.dumps(_scan(**bad)), "qopt-flat")


def test_parse_scan_output_rejects_missing_key():
    scan = _scan()
    del scan["q5_files_read"]
    with pytest.raises(query_opt.QueryOptError, match="q5_files_read"):
        query_opt.parse_scan_output(json.dumps(scan), "qopt-flat")


# ---------------------------------------------------------------- prepare output parser


def _prepared_output(**overrides) -> dict:
    out = {
        "raw_rows": FIXTURE_LINES,
        "layouts": {v: _layout_info() for v in VARIANTS[:3]},
        "errors": {VARIANTS[3]: "qopt-partitioned-clustered: boom"},
    }
    out.update(overrides)
    return out


def test_parse_prepare_output_takes_the_last_json_line():
    info = _prepared_output()
    assert query_opt.parse_prepare_output("log\n" + json.dumps(info) + "\n") == info


@pytest.mark.parametrize("key", ["raw_rows", "layouts", "errors"])
def test_parse_prepare_output_rejects_missing_key(key):
    info = _prepared_output()
    del info[key]
    with pytest.raises(query_opt.QueryOptError, match=key):
        query_opt.parse_prepare_output(json.dumps(info))


@pytest.mark.parametrize(
    "bad",
    [
        {"raw_rows": -1},
        {"raw_rows": True},
        {"raw_rows": 3.5},
        {"layouts": []},
        {"errors": {"qopt-partitioned-clustered": 5}},
        {"layouts": {"qopt-flat": _layout_info(files=-1)}},
        {"layouts": {"qopt-flat": _layout_info(prepare_seconds=math.nan)}},
        {"layouts": {"qopt-flat": {"files": 1, "layout_bytes": 9}}},  # no prepare_seconds
    ],
)
def test_parse_prepare_output_rejects_bad_values(bad):
    with pytest.raises(query_opt.QueryOptError):
        query_opt.parse_prepare_output(json.dumps(_prepared_output(**bad)))


def test_parse_prepare_output_rejects_unknown_or_missing_layouts():
    unknown = _prepared_output(errors={"qopt-other": "x", VARIANTS[3]: "boom"})
    with pytest.raises(query_opt.QueryOptError, match="qopt-other"):
        query_opt.parse_prepare_output(json.dumps(unknown))
    missing = _prepared_output(errors={})
    with pytest.raises(query_opt.QueryOptError, match=VARIANTS[3]):
        query_opt.parse_prepare_output(json.dumps(missing))
    both = _prepared_output(errors={VARIANTS[0]: "x", VARIANTS[3]: "boom"})
    with pytest.raises(query_opt.QueryOptError, match=VARIANTS[0]):
        query_opt.parse_prepare_output(json.dumps(both))


@pytest.mark.parametrize("stdout", ["", "not json", "[1]"])
def test_parse_prepare_output_rejects_non_object_output(stdout):
    with pytest.raises(query_opt.QueryOptError, match="prepare"):
        query_opt.parse_prepare_output(stdout)


# ---------------------------------------------------------------- /proc/self/io


def test_parse_proc_io_reads_every_counter():
    assert query_opt.parse_proc_io(PROC_IO_SAMPLE) == {
        "rchar": 323934931,
        "wchar": 323929600,
        "syscr": 632687,
        "syscw": 632675,
        "read_bytes": 4096,
        "write_bytes": 323932160,
        "cancelled_write_bytes": 0,
    }


@pytest.mark.parametrize(
    "text",
    [
        "",
        "wchar: 1\nsyscr: 2\n",  # no rchar
        "rchar 12\n",  # no colon
        "rchar: twelve\n",
        "rchar: -5\n",
        "rchar: 1\nrchar: 2\n",  # duplicate
    ],
)
def test_parse_proc_io_rejects_broken_text(text):
    with pytest.raises(query_opt.QueryOptError):
        query_opt.parse_proc_io(text)


def test_read_proc_io_returns_none_without_the_file(tmp_path):
    assert query_opt.read_proc_io(tmp_path / "no-such-io") is None


def test_read_proc_io_returns_counters_and_the_size_of_its_own_read(tmp_path):
    path = tmp_path / "io"
    path.write_bytes(PROC_IO_SAMPLE.encode("ascii"))  # no \r\n translation on Windows
    counters, size = query_opt.read_proc_io(path)
    assert counters["rchar"] == 323934931
    assert size == len(PROC_IO_SAMPLE.encode("ascii"))


@pytest.mark.skipif(not HAS_PROC_IO, reason="/proc/self/io exists only on Linux")
def test_read_proc_io_on_linux_counts_bytes_read_by_this_process(tmp_path):
    blob = tmp_path / "blob.bin"
    blob.write_bytes(b"x" * 100_000)
    before, before_size = query_opt.read_proc_io()
    blob.read_bytes()
    after, _ = query_opt.read_proc_io()
    assert after["rchar"] - before["rchar"] - before_size >= 100_000


# ---------------------------------------------------------------- DuckDB profiling parser


# (fixture file suffix, major version): DuckDB 1.x nodes use operator_type /
# operator_cardinality; DuckDB 2.x puts the plan under "operator" and uses type /
# intermediate_rows plus row_groups_scanned / total_row_groups_to_scan.
PROFILE_VERSIONS = [("", "1"), ("_duckdb2", "2")]


def _profile_fixture(variant: str, suffix: str = "") -> dict:
    return json.loads((PROFILE_DIR / f"q3_{variant}{suffix}.json").read_text(encoding="utf-8"))


@pytest.mark.parametrize(("suffix", "major"), PROFILE_VERSIONS)
def test_profile_fixtures_are_real_duckdb_output_with_a_version(suffix, major):
    for variant in ("qopt-flat", "qopt-partitioned"):
        fixture = _profile_fixture(variant, suffix)
        assert re.fullmatch(r"\d+\.\d+\.\d+", fixture["duckdb_version"])
        assert fixture["duckdb_version"].split(".")[0] == major
        assert fixture["query"] == "q3"
        assert fixture["layout"] == variant
        assert isinstance(json.loads(fixture["explain_json"]), dict)


def test_parse_profile_flat_layout_reads_one_file():
    # q3 reads raw_events once; the flat layout is one file and every fixture line comes out of
    # the scan (the type filter is applied above it). The two scans of `repos` are not parquet.
    parsed = query_opt.parse_profile(_profile_fixture("qopt-flat")["explain_json"])
    assert parsed == {
        "scan_rows": FIXTURE_LINES,
        "files_read": 1,
        "row_groups_scanned": None,  # DuckDB 1.x does not report row groups
        "row_groups_total": None,
    }


def test_parse_profile_partitioned_layout_reads_only_the_matching_partitions():
    # q3 needs ForkEvent and PullRequestEvent: 2 of the 9 partition files, 15 fixture lines
    parsed = query_opt.parse_profile(_profile_fixture("qopt-partitioned")["explain_json"])
    assert parsed == {
        "scan_rows": 15,
        "files_read": 2,
        "row_groups_scanned": None,
        "row_groups_total": None,
    }


def test_parse_profile_duckdb2_flat_layout():
    parsed = query_opt.parse_profile(_profile_fixture("qopt-flat", "_duckdb2")["explain_json"])
    assert parsed == {
        "scan_rows": FIXTURE_LINES,
        "files_read": 1,
        "row_groups_scanned": 1,
        "row_groups_total": 1,
    }


def test_parse_profile_duckdb2_partitioned_layout():
    parsed = query_opt.parse_profile(
        _profile_fixture("qopt-partitioned", "_duckdb2")["explain_json"]
    )
    assert parsed == {
        "scan_rows": 15,
        "files_read": 2,
        "row_groups_scanned": 2,
        "row_groups_total": 2,
    }


def test_parse_profile_sums_every_parquet_scan():
    plan = json.loads(_profile_fixture("qopt-partitioned")["explain_json"])
    doubled = {"operator_type": "UNION", "extra_info": {}, "children": [plan, plan]}
    assert query_opt.parse_profile(json.dumps(doubled)) == {
        "scan_rows": 30,
        "files_read": 4,
        "row_groups_scanned": None,
        "row_groups_total": None,
    }


def test_parse_profile_duckdb2_sums_every_parquet_scan():
    plan = json.loads(_profile_fixture("qopt-partitioned", "_duckdb2")["explain_json"])
    plan["operator"] = plan["operator"] * 2
    assert query_opt.parse_profile(json.dumps(plan)) == {
        "scan_rows": 30,
        "files_read": 4,
        "row_groups_scanned": 4,
        "row_groups_total": 4,
    }


def test_parse_profile_duckdb2_rejects_a_scan_without_row_count():
    text = _profile_fixture("qopt-flat", "_duckdb2")["explain_json"].replace(
        '"intermediate_rows": 34', '"intermediate_rows": "34"'
    )
    with pytest.raises(query_opt.QueryOptError, match="rows"):
        query_opt.parse_profile(text)


def test_parse_profile_duckdb2_rejects_row_groups_on_only_some_scans():
    plan = json.loads(_profile_fixture("qopt-partitioned", "_duckdb2")["explain_json"])
    stripped = json.loads(json.dumps(plan))

    def strip(node):
        if (node.get("extra_info") or {}).get("Function") == "READ_PARQUET":
            node.pop("row_groups_scanned")
        for child in node.get("children", []):
            strip(child)

    for root in stripped["operator"]:
        strip(root)
    plan["operator"] = plan["operator"] + stripped["operator"]
    with pytest.raises(query_opt.QueryOptError, match="row_groups"):
        query_opt.parse_profile(json.dumps(plan))


def test_parse_profile_rejects_non_json():
    with pytest.raises(query_opt.QueryOptError):
        query_opt.parse_profile("Total Files Read: 2")


def test_parse_profile_rejects_a_plan_without_parquet_scan():
    plan = {"operator_type": "PROJECTION", "extra_info": {}, "children": []}
    with pytest.raises(query_opt.QueryOptError, match="parquet"):
        query_opt.parse_profile(json.dumps(plan))


@pytest.mark.parametrize(("suffix", "major"), PROFILE_VERSIONS)
def test_parse_profile_rejects_a_parquet_scan_without_file_count(suffix, major):
    text = _profile_fixture("qopt-flat", suffix)["explain_json"].replace(
        '"Total Files Read"', '"Something Else"'
    )
    with pytest.raises(query_opt.QueryOptError, match="Total Files Read"):
        query_opt.parse_profile(text)


@pytest.mark.parametrize(("suffix", "major"), PROFILE_VERSIONS)
def test_parse_profile_rejects_a_non_integer_file_count(suffix, major):
    text = _profile_fixture("qopt-flat", suffix)["explain_json"].replace(
        '"Total Files Read": "1"', '"Total Files Read": "one"'
    )
    assert text != _profile_fixture("qopt-flat", suffix)["explain_json"]
    with pytest.raises(query_opt.QueryOptError):
        query_opt.parse_profile(text)


# ---------------------------------------------------------------- one row per layout


def test_summarized_row_is_valid_and_named_after_the_layout():
    row = _summarize()
    validate_row(row)
    assert row["variant"] == "qopt-partitioned"
    assert row["data_date"] == "2026-10-25"
    assert row["total_seconds"] == 0.6  # median of 0.5, 0.7, 0.6
    assert row["row_count"] == FIXTURE_LINES
    assert row["peak_rss_mb"] == 120.5


def test_bytes_read_is_the_sum_over_q1_to_q6_and_notes_hold_every_query():
    row = _summarize()
    assert row["bytes_read"] == sum(1000 * i for i in range(1, 7))
    notes = _notes(row)
    assert notes["runs"] == "3"
    assert notes["stat"] == "median"
    assert notes["partition_by"] == "type"
    assert notes["sort_by"] == "none"
    assert notes["files"] == "9"
    assert notes["layout_bytes"] == "4096"
    assert notes["prepare_seconds"] == "0.25"
    assert notes["bytes"] == "proc_rchar"
    assert notes["duckdb"] == duckdb.__version__
    for i, q in enumerate(QUERIES, start=1):
        assert notes[f"{q}_bytes"] == str(1000 * i)
        assert notes[f"{q}_scan_rows"] == str(10 * i)
        assert notes[f"{q}_files_read"] == str(i)
    assert "," not in row["notes"]
    assert "\n" not in row["notes"]


def test_sort_keys_in_notes_have_no_comma():
    layout = {layout.variant: layout for layout in query_opt.LAYOUTS}["qopt-clustered"]
    assert layout.sort_by
    notes = _notes(_summarize(variant="qopt-clustered"))
    assert notes["sort_by"] == "type+repo.name+created_at"
    assert notes["partition_by"] == "none"


def test_bytes_read_stays_empty_when_bytes_were_not_measured():
    row = _summarize(scan=_scan(bytes_per_query=None))
    assert row["bytes_read"] is None
    assert validate_row(row)["bytes_read"] == ""
    notes = _notes(row)
    assert notes["bytes"] == "unavailable"
    assert not any(key.endswith("_bytes") and key != "layout_bytes" for key in notes)


def test_row_groups_are_noted_per_query_when_duckdb_reports_them():
    notes = _notes(_summarize())
    for i, q in enumerate(QUERIES, start=1):
        assert notes[f"{q}_row_groups_scanned"] == str(i)
        assert notes[f"{q}_row_groups_total"] == str(2 * i)


def test_row_groups_are_left_out_when_duckdb_does_not_report_them():
    notes = _notes(_summarize(scan=_scan(row_groups=False)))
    assert not any("row_groups" in key for key in notes)


def test_row_count_only_cross_checked_is_noted():
    notes = _notes(_summarize(cross_check="row_count_only"))
    assert notes["cross_check"] == "row_count_only"


@pytest.mark.parametrize("key", ["q2_rows", "row_count"])
def test_counts_that_change_between_runs_are_an_error(key):
    runs = [_run(), _run(**{key: 4})]
    with pytest.raises(query_opt.QueryOptError, match="qopt-partitioned"):
        _summarize(runs=runs)


def test_summarize_rejects_no_runs():
    with pytest.raises(query_opt.QueryOptError):
        _summarize(runs=[])


# ---------------------------------------------------------------- cross-check between layouts


def test_cross_check_passes_on_identical_counts():
    query_opt.check_against("qopt-clustered", _run(), _run(), reference_name="qopt-flat")


@pytest.mark.parametrize("key", ["row_count", "q1_rows", "q6_rows"])
def test_cross_check_names_the_layout_and_the_count_that_differ(key):
    with pytest.raises(query_opt.QueryOptError) as err:
        query_opt.check_against(
            "qopt-partitioned-clustered",
            _run(**{key: 99}),
            _run(),
            reference_name="qopt-flat",
        )
    message = str(err.value)
    assert "qopt-partitioned-clustered" in message
    assert "qopt-flat" in message
    assert key in message


# ---------------------------------------------------------------- end to end on the fixture


def test_prepare_reports_every_layout(prepared):
    info = prepared["info"]
    assert info["raw_rows"] == FIXTURE_LINES
    assert info["errors"] == {}
    assert set(info["layouts"]) == set(VARIANTS)
    for variant, layout in info["layouts"].items():
        files = sorted(query_opt.layout_path(prepared["work"], variant).rglob("*.parquet"))
        assert layout["files"] == len(files), variant
        assert layout["layout_bytes"] == sum(p.stat().st_size for p in files), variant
        assert layout["prepare_seconds"] >= 0


@pytest.mark.parametrize("variant", VARIANTS)
def test_every_layout_gives_exactly_the_naive_results(prepared, variant):
    run = query_opt.run_once(prepared["work"], variant, collect=True)
    assert run["row_count"] == FIXTURE_LINES
    for q in QUERIES:
        expected = prepared["naive"]["results"][q]
        actual = run["results"][q]
        assert list(actual["columns"]) == list(expected["columns"]), (variant, q)
        assert [tuple(r) for r in actual["rows"]] == [tuple(r) for r in expected["rows"]], (
            variant,
            q,
        )
        assert run[f"{q}_rows"] == len(expected["rows"])
        assert run[f"{q}_seconds"] >= 0
    # every value is rounded to 6 decimals on its own
    assert run["total_seconds"] >= sum(run[f"{q}_seconds"] for q in QUERIES) - 1e-5


def test_view_keeps_the_raw_column_types(prepared):
    con = duckdb.connect()
    try:
        query_opt.create_views(con, prepared["work"], "qopt-partitioned")
        types = dict(
            (name, kind) for name, kind, *_ in con.execute("DESCRIBE raw_events").fetchall()
        )
    finally:
        con.close()
    assert list(types) == ["id", "type", "actor", "repo", "payload", "created_at"]
    assert types["payload"] == "JSON"
    assert types["created_at"] == "TIMESTAMP"
    assert types["actor"] == "STRUCT(id BIGINT, login VARCHAR)"
    assert types["type"] == "VARCHAR"


def test_partitioned_layouts_write_one_directory_per_event_type(prepared):
    con = duckdb.connect()
    try:
        types = {
            r[0]
            for r in con.execute(
                "SELECT DISTINCT type FROM read_json("
                f"'{(prepared['raw'] / '*.json.gz').as_posix()}', format = 'newline_delimited')"
            ).fetchall()
        }
    finally:
        con.close()
    for variant in ("qopt-partitioned", "qopt-partitioned-clustered"):
        root = query_opt.layout_path(prepared["work"], variant)
        dirs = {p.name for p in root.iterdir() if p.is_dir()}
        assert dirs == {f"type={t}" for t in types}, variant
    for variant in ("qopt-flat", "qopt-clustered"):
        root = query_opt.layout_path(prepared["work"], variant)
        assert len(list(root.rglob("*.parquet"))) == 1, variant
        assert not [p for p in root.iterdir() if p.is_dir()], variant


def test_flat_layout_keeps_the_arrival_order(prepared):
    (flat_file,) = query_opt.layout_path(prepared["work"], "qopt-flat").rglob("*.parquet")
    rows, _ = _sorted_by(flat_file, "id")
    con = duckdb.connect()
    try:
        raw = con.execute(
            "SELECT id FROM read_json("
            f"'{(prepared['raw'] / '*.json.gz').as_posix()}', format = 'newline_delimited', "
            "columns = {'id': 'VARCHAR'})"
        ).fetchall()
    finally:
        con.close()
    assert rows == raw


def test_clustered_layout_is_sorted_by_type_repo_and_time(prepared):
    (path,) = query_opt.layout_path(prepared["work"], "qopt-clustered").rglob("*.parquet")
    rows, ordered = _sorted_by(path, "type, repo.name, created_at")
    assert len(rows) == FIXTURE_LINES
    assert rows == ordered


def test_partitioned_clustered_files_are_sorted_by_repo_and_time(prepared):
    files = sorted(
        query_opt.layout_path(prepared["work"], "qopt-partitioned-clustered").rglob("*.parquet")
    )
    total = 0
    for path in files:
        rows, ordered = _sorted_by(path, "repo.name, created_at")
        assert rows == ordered, path
        total += len(rows)
    assert total == FIXTURE_LINES


def test_flat_layout_is_not_sorted_by_the_clustering_key(prepared):
    # guards the test above: the fixture's arrival order is not already the clustered order
    (path,) = query_opt.layout_path(prepared["work"], "qopt-flat").rglob("*.parquet")
    rows, ordered = _sorted_by(path, "type, repo.name, created_at")
    assert rows != ordered


def test_partition_by_type_reads_fewer_files_and_rows_than_flat(prepared):
    flat = query_opt.scan_once(prepared["work"], "qopt-flat")
    part = query_opt.scan_once(prepared["work"], "qopt-partitioned")
    files = prepared["info"]["layouts"]["qopt-partitioned"]["files"]
    for q in QUERIES:
        assert flat[f"{q}_files_read"] == 1, q
        assert 0 < part[f"{q}_files_read"] < files, q  # never every partition: no PushEvent
        assert part[f"{q}_scan_rows"] <= flat[f"{q}_scan_rows"], q
    assert part["q3_scan_rows"] < flat["q3_scan_rows"]
    assert sum(part[f"{q}_scan_rows"] for q in QUERIES) < sum(
        flat[f"{q}_scan_rows"] for q in QUERIES
    )


@pytest.mark.skipif(HAS_PROC_IO, reason="checks the platform without /proc/self/io")
def test_scan_bytes_are_empty_without_proc_io(prepared):
    scan = query_opt.scan_once(prepared["work"], "qopt-flat")
    assert all(scan[f"{q}_bytes"] is None for q in QUERIES)


@pytest.mark.skipif(not HAS_PROC_IO, reason="/proc/self/io exists only on Linux")
def test_scan_bytes_are_measured_and_do_not_depend_on_order(prepared):
    first = query_opt.scan_once(prepared["work"], "qopt-partitioned")
    second = query_opt.scan_once(prepared["work"], "qopt-partitioned")
    for q in QUERIES:
        assert isinstance(first[f"{q}_bytes"], int) and first[f"{q}_bytes"] > 0, q
        assert first[f"{q}_bytes"] == second[f"{q}_bytes"], q
    layout_bytes = prepared["info"]["layouts"]["qopt-partitioned"]["layout_bytes"]
    for q in QUERIES:  # one pass over the files it needs, plus metadata, never the whole lake 2x
        assert first[f"{q}_bytes"] <= 2 * layout_bytes, q


def test_prepare_twice_does_not_duplicate_rows(gz_dir, tmp_path):
    work = tmp_path / "layouts"
    query_opt.prepare_layouts(gz_dir, work)
    files_before = {
        v: sorted(p.relative_to(work) for p in query_opt.layout_path(work, v).rglob("*"))
        for v in VARIANTS
    }
    info = query_opt.prepare_layouts(gz_dir, work)
    assert info["errors"] == {}
    for variant in VARIANTS:
        files_after = sorted(
            p.relative_to(work) for p in query_opt.layout_path(work, variant).rglob("*")
        )
        assert files_after == files_before[variant], variant
        assert query_opt.run_once(work, variant)["row_count"] == FIXTURE_LINES, variant


def _write_gz_lines(out: Path, lines: list[bytes]) -> Path:
    out.mkdir(parents=True)
    with gzip.open(out / "2026-01-05-0.json.gz", "wb") as fh:
        fh.write(b"\n".join(lines) + b"\n")
    return out


def _fixture_lines() -> list[bytes]:
    lines: list[bytes] = []
    for src in sorted(FIXTURE_DIR.glob("*.json")):
        lines += [ln for ln in src.read_bytes().splitlines() if ln.strip()]
    return lines


def test_prepare_again_removes_partitions_that_no_longer_exist(tmp_path):
    lines = _fixture_lines()
    full = _write_gz_lines(tmp_path / "full", lines)
    no_watch = _write_gz_lines(
        tmp_path / "no_watch", [ln for ln in lines if json.loads(ln)["type"] != "WatchEvent"]
    )
    work = tmp_path / "layouts"
    assert query_opt.prepare_layouts(full, work)["errors"] == {}
    info = query_opt.prepare_layouts(no_watch, work)
    assert info["errors"] == {}
    for variant in ("qopt-partitioned", "qopt-partitioned-clustered"):
        root = query_opt.layout_path(work, variant)
        assert not (root / "type=WatchEvent").exists(), variant
        assert all(p.name == "data_0.parquet" for p in root.rglob("*.parquet")), variant
        assert query_opt.run_once(work, variant)["row_count"] == info["raw_rows"], variant


def test_unsafe_partition_value_fails_only_the_partitioned_layouts(tmp_path):
    lines = _fixture_lines()
    evil = json.loads(lines[0])
    evil["type"] = "../Evil Event"
    raw = _write_gz_lines(tmp_path / "raw", [*lines, json.dumps(evil).encode()])
    work = tmp_path / "layouts"
    info = query_opt.prepare_layouts(raw, work)
    assert set(info["errors"]) == {"qopt-partitioned", "qopt-partitioned-clustered"}
    for message in info["errors"].values():
        assert "../Evil Event" in message
    assert set(info["layouts"]) == {"qopt-flat", "qopt-clustered"}
    assert not (tmp_path / "Evil Event").exists()
    assert not list(work.rglob("*Evil*"))


# ---------------------------------------------------------------- large input: order in files

BIG_COPIES = 5000  # 34 fixture lines x 5000 = 170,000 rows, enough for multi-threaded writes


@pytest.fixture(scope="module")
def big(tmp_path_factory) -> dict:
    """170,000 events in shuffled arrival order, varied repos and times (deterministic)."""
    base = tmp_path_factory.mktemp("query_opt_big")
    raw = _gzip_fixture(base / "fixture")
    big_raw = base / "raw"
    big_raw.mkdir()
    con = duckdb.connect()
    try:
        naive_duckdb.load(con, raw, naive_duckdb.DEFAULT_SEED)
        con.execute(
            "COPY (SELECT r.id || '-' || i AS id, r.type, r.actor, "
            "{'id': r.repo.id, 'name': r.repo.name || '-' || (hash(i, r.id) % 997)} AS repo, "
            "r.payload, r.created_at + to_seconds((hash(r.id, i) % 86400)::BIGINT) AS created_at "
            f"FROM raw_events AS r, range({BIG_COPIES}) AS t(i) ORDER BY hash(i, r.id, 7)) "
            f"TO '{(big_raw / '2026-01-05-0.json.gz').as_posix()}' "
            "(FORMAT json, COMPRESSION gzip)"
        )
    finally:
        con.close()
    work = base / "layouts"
    return {"raw": big_raw, "work": work, "info": query_opt.prepare_layouts(big_raw, work)}


def _raw_ids_by_type(raw: Path) -> dict[str, list[str]]:
    con = duckdb.connect()
    try:
        rows = con.execute(
            f"SELECT type, id FROM read_json('{(raw / '*.json.gz').as_posix()}', "
            "format = 'newline_delimited', columns = {'id': 'VARCHAR', 'type': 'VARCHAR'})"
        ).fetchall()
    finally:
        con.close()
    by_type: dict[str, list[str]] = {}
    for kind, event_id in rows:
        by_type.setdefault(kind, []).append(event_id)
    return by_type


def test_big_input_prepares_every_layout(big):
    assert big["info"]["raw_rows"] == FIXTURE_LINES * BIG_COPIES
    assert big["info"]["errors"] == {}
    assert set(big["info"]["layouts"]) == set(VARIANTS)


def test_big_partitioned_clustered_files_are_each_sorted(big):
    files = query_opt.layout_files(big["work"], "qopt-partitioned-clustered")
    assert len(files) == 9
    total = 0
    for path in files:
        rows, ordered = _sorted_by(path, "repo.name, created_at")
        assert rows == ordered, path.parent.name
        total += len(rows)
    assert total == FIXTURE_LINES * BIG_COPIES


def test_big_clustered_file_is_sorted(big):
    (path,) = query_opt.layout_files(big["work"], "qopt-clustered")
    rows, ordered = _sorted_by(path, "type, repo.name, created_at")
    assert rows == ordered


def test_big_partitioned_files_keep_the_arrival_order(big):
    expected = _raw_ids_by_type(big["raw"])
    files = query_opt.layout_files(big["work"], "qopt-partitioned")
    assert {p.parent.name for p in files} == {f"type={t}" for t in expected}
    for path in files:
        rows, _ = _sorted_by(path, "id")
        assert [r[0] for r in rows] == expected[path.parent.name.split("=", 1)[1]], path


def test_big_flat_file_keeps_the_arrival_order(big):
    (path,) = query_opt.layout_files(big["work"], "qopt-flat")
    con = duckdb.connect()
    try:
        raw = con.execute(
            f"SELECT id FROM read_json('{(big['raw'] / '*.json.gz').as_posix()}', "
            "format = 'newline_delimited', columns = {'id': 'VARCHAR'})"
        ).fetchall()
    finally:
        con.close()
    rows, _ = _sorted_by(path, "id")
    assert rows == raw


# ---------------------------------------------------------------- CLI


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


def _cli(gz_dir: Path, tmp_path: Path, *extra: str) -> tuple[list[str], Path, Path]:
    ref = tmp_path / "reference.toml"
    if not ref.exists():
        ref.write_text(_reference_for(gz_dir), encoding="utf-8")
    out = tmp_path / "out" / "query-opt-rows.csv"
    summary = tmp_path / "out" / "query-opt-summary.md"
    argv = [
        "--data-dir",
        str(gz_dir),
        "--reference",
        str(ref),
        "--work-dir",
        str(tmp_path / "work"),
        "--out",
        str(out),
        "--summary",
        str(summary),
        *extra,
    ]
    return argv, out, summary


def _in_process(mode: str, variant: str, *, work_dir: Path, seed: Path, timeout_s: float) -> dict:
    if mode == "run":
        return query_opt.run_once(work_dir, variant, seed)
    return query_opt.scan_once(work_dir, variant, seed)


def test_default_runs_is_ten():
    assert query_opt.build_parser().parse_args([]).runs == 10


def test_main_requires_git_sha(gz_dir, tmp_path, capsys):
    argv, out, summary = _cli(gz_dir, tmp_path)
    assert query_opt.main(argv) == 2
    assert "git-sha" in capsys.readouterr().err
    assert not out.exists()
    assert not summary.exists()


def test_main_stops_with_exit_2_on_checksum_mismatch(gz_dir, tmp_path, monkeypatch):
    argv, out, summary = _cli(gz_dir, tmp_path, "--git-sha", "abc1234")
    victim = sorted(gz_dir.glob("*.json.gz"))[0]
    victim.write_bytes(victim.read_bytes() + b"tampered")

    def _must_not_run(*args, **kwargs):
        raise AssertionError("ran despite a checksum mismatch")

    monkeypatch.setattr(query_opt, "prepare_layouts", _must_not_run)
    monkeypatch.setattr(query_opt, "prepare_in_subprocess", _must_not_run, raising=False)
    monkeypatch.setattr(query_opt, "run_in_subprocess", _must_not_run)
    assert query_opt.main(argv) == 2
    assert not out.exists()
    assert not summary.exists()


def test_main_refuses_files_not_listed_in_the_reference(gz_dir, tmp_path, monkeypatch, capsys):
    argv, out, _ = _cli(gz_dir, tmp_path, "--git-sha", "abc1234")
    (gz_dir / "2026-01-06-0.json.gz").write_bytes(gzip.compress(b"{}\n"))
    monkeypatch.setattr(query_opt, "prepare_layouts", lambda *a, **k: pytest.fail("prepared"))
    monkeypatch.setattr(
        query_opt, "prepare_in_subprocess", lambda *a, **k: pytest.fail("prepared"), raising=False
    )
    assert query_opt.main(argv) == 2
    assert "2026-01-06-0.json.gz" in capsys.readouterr().err
    assert not out.exists()


def test_main_never_prepares_layouts_in_its_own_process(gz_dir, tmp_path, monkeypatch):
    # On Linux a child's ru_maxrss starts at the parent's peak RSS (the kernel carries the old
    # mm's high-water mark over exec), so loading the day in the parent would show up as the
    # peak_rss_mb of every run. Preparing must happen in a child process of its own.
    def _not_in_parent(*args, **kwargs):
        raise AssertionError("prepare_layouts ran in the parent process")

    monkeypatch.setattr(query_opt, "prepare_layouts", _not_in_parent)
    monkeypatch.setattr(query_opt, "load", _not_in_parent)
    argv, out, _ = _cli(gz_dir, tmp_path, "--git-sha", "abc1234", "--runs", "1")
    assert query_opt.main(argv) == 0
    assert [r["variant"] for r in read_artifact(out)] == list(VARIANTS)


def test_prepare_mode_prints_one_json_line(gz_dir, tmp_path, capsys):
    work = tmp_path / "work"
    argv = ["--prepare", "--data-dir", str(gz_dir), "--work-dir", str(work)]
    assert query_opt.main(argv) == 0
    info = query_opt.parse_prepare_output(capsys.readouterr().out)
    assert info["raw_rows"] == FIXTURE_LINES
    assert info["errors"] == {}
    assert set(info["layouts"]) == set(VARIANTS)
    assert info["layouts"]["qopt-partitioned"]["files"] == len(
        query_opt.layout_files(work, "qopt-partitioned")
    )


def test_prepare_in_subprocess_reports_a_crash(tmp_path):
    with pytest.raises(query_opt.QueryOptError, match="prepare"):
        query_opt.prepare_in_subprocess(
            tmp_path / "no-such-dir", work_dir=tmp_path / "work", seed=naive_duckdb.DEFAULT_SEED
        )


def test_main_fails_every_layout_when_preparing_crashes(gz_dir, tmp_path, monkeypatch, capsys):
    def _crash(*args, **kwargs):
        raise query_opt.QueryOptError("prepare failed (exit 1): boom")

    monkeypatch.setattr(query_opt, "prepare_in_subprocess", _crash, raising=False)
    argv, out, summary = _cli(gz_dir, tmp_path, "--git-sha", "abc1234", "--runs", "1")
    assert query_opt.main(argv) == 1
    assert read_artifact(out) == []
    assert summary.read_text(encoding="utf-8").count("FAILED") == len(VARIANTS)
    assert "boom" in capsys.readouterr().err


def test_main_end_to_end_writes_four_rows_in_child_processes(gz_dir, tmp_path):
    argv, out, summary = _cli(gz_dir, tmp_path, "--git-sha", "abc1234", "--runs", "1")
    assert query_opt.main(argv) == 0
    with out.open(encoding="utf-8", newline="") as fh:
        assert next(csv.reader(fh)) == list(COLUMNS)
    rows = read_artifact(out)
    assert [r["variant"] for r in rows] == list(VARIANTS)
    for row in rows:
        assert row["data_date"] == "2026-01-05"
        assert row["git_sha"] == "abc1234"
        assert row["row_count"] == FIXTURE_LINES
        notes = _notes(row)
        assert notes["runs"] == "1"
        if HAS_PROC_IO:
            assert row["bytes_read"] == sum(int(notes[f"{q}_bytes"]) for q in QUERIES)
        else:
            assert row["bytes_read"] is None
    text = summary.read_text(encoding="utf-8")
    for variant in VARIANTS:
        assert f"`{variant}`" in text


def test_main_drops_only_the_layout_whose_counts_disagree(gz_dir, tmp_path, monkeypatch, capsys):
    def _fake(mode, variant, **kwargs):
        result = _in_process(mode, variant, **kwargs)
        if mode == "run" and variant == "qopt-partitioned":
            result["q4_rows"] += 1
        return result

    monkeypatch.setattr(query_opt, "run_in_subprocess", _fake)
    argv, out, summary = _cli(gz_dir, tmp_path, "--git-sha", "abc1234", "--runs", "2")
    assert query_opt.main(argv) == 1
    variants = [r["variant"] for r in read_artifact(out)]
    assert variants == ["qopt-flat", "qopt-clustered", "qopt-partitioned-clustered"]
    err = capsys.readouterr().err
    assert "qopt-partitioned" in err
    assert "q4_rows" in err
    assert "FAILED" in summary.read_text(encoding="utf-8")


def test_main_keeps_going_when_one_layout_crashes(gz_dir, tmp_path, monkeypatch, capsys):
    def _fake(mode, variant, **kwargs):
        if variant == "qopt-clustered":
            raise query_opt.QueryOptError("qopt-clustered run failed (exit 1): boom")
        return _in_process(mode, variant, **kwargs)

    monkeypatch.setattr(query_opt, "run_in_subprocess", _fake)
    argv, out, _ = _cli(gz_dir, tmp_path, "--git-sha", "abc1234", "--runs", "1")
    assert query_opt.main(argv) == 1
    variants = [r["variant"] for r in read_artifact(out)]
    assert variants == ["qopt-flat", "qopt-partitioned", "qopt-partitioned-clustered"]
    assert "boom" in capsys.readouterr().err


def test_main_without_flat_layout_checks_row_count_only(gz_dir, tmp_path, monkeypatch):
    def _fake(mode, variant, **kwargs):
        if variant == "qopt-flat":
            raise query_opt.QueryOptError("qopt-flat run failed")
        return _in_process(mode, variant, **kwargs)

    monkeypatch.setattr(query_opt, "run_in_subprocess", _fake)
    argv, out, _ = _cli(gz_dir, tmp_path, "--git-sha", "abc1234", "--runs", "1")
    assert query_opt.main(argv) == 1
    rows = read_artifact(out)
    assert [r["variant"] for r in rows] == list(VARIANTS[1:])
    assert all(_notes(r)["cross_check"] == "row_count_only" for r in rows)


def test_single_mode_prints_one_json_line(prepared, capsys):
    argv = ["--single", "--layout", "qopt-flat", "--work-dir", str(prepared["work"])]
    assert query_opt.main(argv) == 0
    run = query_opt.parse_run_output(capsys.readouterr().out, "qopt-flat")
    assert run["row_count"] == FIXTURE_LINES


def test_scan_mode_prints_one_json_line(prepared, capsys):
    argv = ["--scan", "--layout", "qopt-partitioned", "--work-dir", str(prepared["work"])]
    assert query_opt.main(argv) == 0
    scan = query_opt.parse_scan_output(capsys.readouterr().out, "qopt-partitioned")
    assert scan["q3_files_read"] == 2
