import csv
import math
from datetime import UTC, date, datetime, timedelta, timezone

import pytest

from repopulse.runs_log import COLUMNS, REQUIRED, RunsLogError, append_run, validate_row

EXPECTED_COLUMNS = (
    "run_at",
    "git_sha",
    "variant",
    "data_date",
    "total_seconds",
    "q1_seconds",
    "q2_seconds",
    "q3_seconds",
    "q4_seconds",
    "q5_seconds",
    "q6_seconds",
    "peak_rss_mb",
    "bytes_read",
    "row_count",
    "notes",
)
HEADER_LINE = ",".join(EXPECTED_COLUMNS) + "\n"
FLOAT_COLUMNS = (
    "total_seconds",
    "q1_seconds",
    "q2_seconds",
    "q3_seconds",
    "q4_seconds",
    "q5_seconds",
    "q6_seconds",
    "peak_rss_mb",
)
INT_COLUMNS = ("bytes_read", "row_count")
REQUIRED_COLS = ("run_at", "git_sha", "variant", "data_date", "total_seconds")


def minimal_row(**overrides):
    row = {
        "run_at": "2026-11-11T08:30:00+00:00",
        "git_sha": "9289337",
        "variant": "baseline",
        "data_date": "2026-11-10",
        "total_seconds": 12.5,
    }
    row.update(overrides)
    return row


def full_row(**overrides):
    row = minimal_row(
        q1_seconds=1.0,
        q2_seconds=2,
        q3_seconds=0,
        q4_seconds=0.25,
        q5_seconds=5.5,
        q6_seconds=6.0,
        peak_rss_mb=512.0,
        bytes_read=1024,
        row_count=42,
        notes="first run",
    )
    row.update(overrides)
    return row


def read_bytes(path):
    with open(path, "rb") as f:
        return f.read()


# --- schema constants ---------------------------------------------------------


def test_columns_order_is_fixed():
    assert COLUMNS == EXPECTED_COLUMNS


def test_required_columns():
    assert REQUIRED == frozenset({"run_at", "git_sha", "variant", "data_date", "total_seconds"})


def test_runs_log_error_is_value_error():
    assert issubclass(RunsLogError, ValueError)


# --- validate_row: shape ------------------------------------------------------


def test_validate_minimal_row_fills_optional_columns_with_empty_strings():
    out = validate_row(minimal_row())
    assert list(out.keys()) == list(EXPECTED_COLUMNS)
    assert out["run_at"] == "2026-11-11T08:30:00+00:00"
    assert out["git_sha"] == "9289337"
    assert out["variant"] == "baseline"
    assert out["data_date"] == "2026-11-10"
    assert out["total_seconds"] == "12.5"
    for col in EXPECTED_COLUMNS[5:]:
        assert out[col] == ""


def test_validate_full_row_converts_values_to_strings():
    out = validate_row(full_row())
    assert out["q1_seconds"] == "1.0"
    assert out["q2_seconds"] == "2"
    assert out["q3_seconds"] == "0"
    assert out["peak_rss_mb"] == "512.0"
    assert out["bytes_read"] == "1024"
    assert out["row_count"] == "42"
    assert out["notes"] == "first run"
    assert all(isinstance(v, str) for v in out.values())


def test_validate_optional_none_becomes_empty():
    out = validate_row(minimal_row(q1_seconds=None, row_count=None, notes=None))
    assert out["q1_seconds"] == ""
    assert out["row_count"] == ""
    assert out["notes"] == ""


def test_validate_does_not_mutate_input():
    row = minimal_row()
    snapshot = dict(row)
    validate_row(row)
    assert row == snapshot


def test_unknown_column_rejected():
    with pytest.raises(RunsLogError, match="duration"):
        validate_row(minimal_row(duration=3))


@pytest.mark.parametrize("col", REQUIRED_COLS)
def test_missing_required_column_rejected(col):
    row = minimal_row()
    del row[col]
    with pytest.raises(RunsLogError, match=col):
        validate_row(row)


@pytest.mark.parametrize("col", REQUIRED_COLS)
@pytest.mark.parametrize("empty", [None, ""])
def test_empty_required_column_rejected(col, empty):
    with pytest.raises(RunsLogError, match=col):
        validate_row(minimal_row(**{col: empty}))


# --- run_at -------------------------------------------------------------------


def test_run_at_accepts_aware_datetime():
    ts = datetime(2026, 11, 11, 8, 30, tzinfo=UTC)
    assert validate_row(minimal_row(run_at=ts))["run_at"] == ts.isoformat()


def test_run_at_keeps_non_utc_offset():
    ts = datetime(2026, 11, 11, 15, 30, tzinfo=timezone(timedelta(hours=7)))
    assert validate_row(minimal_row(run_at=ts))["run_at"] == "2026-11-11T15:30:00+07:00"


def test_run_at_string_is_normalised_to_isoformat():
    out = validate_row(minimal_row(run_at="2026-11-11T08:30:00Z"))
    assert out["run_at"] == "2026-11-11T08:30:00+00:00"


@pytest.mark.parametrize(
    "value",
    [
        "2026-11-11T08:30:00",
        datetime(2026, 11, 11, 8, 30),
        "not-a-date",
        "2026-13-11T08:30:00+00:00",
        date(2026, 11, 11),
        1731313800,
    ],
)
def test_run_at_invalid_rejected(value):
    with pytest.raises(RunsLogError, match="run_at"):
        validate_row(minimal_row(run_at=value))


# --- git_sha ------------------------------------------------------------------


@pytest.mark.parametrize("value", ["abcdef0", "0" * 40, "9289337abc"])
def test_git_sha_valid(value):
    assert validate_row(minimal_row(git_sha=value))["git_sha"] == value


@pytest.mark.parametrize(
    "value",
    ["abc123", "0" * 41, "ABCDEF0", "abcdefg", "abc 123", 1234567],
)
def test_git_sha_invalid_rejected(value):
    with pytest.raises(RunsLogError, match="git_sha"):
        validate_row(minimal_row(git_sha=value))


# --- variant ------------------------------------------------------------------


@pytest.mark.parametrize("value", ["baseline", "incremental", "z_order-v2", "0opt"])
def test_variant_valid(value):
    assert validate_row(minimal_row(variant=value))["variant"] == value


@pytest.mark.parametrize("value", ["Baseline", "-base", "_base", "base line", "base,x", 1])
def test_variant_invalid_rejected(value):
    with pytest.raises(RunsLogError, match="variant"):
        validate_row(minimal_row(variant=value))


# --- data_date ----------------------------------------------------------------


def test_data_date_accepts_date_object():
    assert validate_row(minimal_row(data_date=date(2026, 11, 10)))["data_date"] == "2026-11-10"


@pytest.mark.parametrize(
    "value",
    ["2026-13-01", "20261110", "2026-02-30", "2026-11-10T00:00:00", "26-11-10", 20261110],
)
def test_data_date_invalid_rejected(value):
    with pytest.raises(RunsLogError, match="data_date"):
        validate_row(minimal_row(data_date=value))


# --- float columns ------------------------------------------------------------


@pytest.mark.parametrize("col", FLOAT_COLUMNS)
@pytest.mark.parametrize("value", [0, 0.0, 3, 1.5])
def test_float_columns_accept_non_negative_numbers(col, value):
    assert validate_row(minimal_row(**{col: value}))[col] == str(value)


@pytest.mark.parametrize("col", FLOAT_COLUMNS)
@pytest.mark.parametrize(
    "value",
    [-0.1, -1, math.nan, math.inf, -math.inf, True, False, "1.5", "12"],
)
def test_float_columns_invalid_rejected(col, value):
    with pytest.raises(RunsLogError, match=col):
        validate_row(minimal_row(**{col: value}))


# --- int columns --------------------------------------------------------------


@pytest.mark.parametrize("col", INT_COLUMNS)
@pytest.mark.parametrize("value", [0, 7, 10**12])
def test_int_columns_accept_non_negative_ints(col, value):
    assert validate_row(minimal_row(**{col: value}))[col] == str(value)


@pytest.mark.parametrize("col", INT_COLUMNS)
@pytest.mark.parametrize("value", [-1, 1.0, 2.5, True, False, "10", math.nan])
def test_int_columns_invalid_rejected(col, value):
    with pytest.raises(RunsLogError, match=col):
        validate_row(minimal_row(**{col: value}))


# --- notes --------------------------------------------------------------------


@pytest.mark.parametrize("value", ["line1\nline2", "a\rb", "a\r\nb", 5])
def test_notes_invalid_rejected(value):
    with pytest.raises(RunsLogError, match="notes"):
        validate_row(minimal_row(notes=value))


# --- append_run ---------------------------------------------------------------


def test_append_creates_file_with_header_and_row(tmp_path):
    path = tmp_path / "runs.csv"
    append_run(path, minimal_row())
    assert read_bytes(path) == (
        HEADER_LINE.encode()
        + b"2026-11-11T08:30:00+00:00,9289337,baseline,2026-11-10,12.5,,,,,,,,,,\n"
    )


def test_append_accepts_str_path(tmp_path):
    path = tmp_path / "runs.csv"
    append_run(str(path), minimal_row())
    assert read_bytes(path).count(b"\n") == 2


def test_append_to_empty_file_writes_header(tmp_path):
    path = tmp_path / "runs.csv"
    path.write_bytes(b"")
    append_run(path, minimal_row())
    lines = read_bytes(path).split(b"\n")
    assert lines[0] == HEADER_LINE.rstrip("\n").encode()
    assert len(lines) == 3 and lines[2] == b""


def test_append_adds_exactly_one_line_and_keeps_old_bytes(tmp_path):
    path = tmp_path / "runs.csv"
    append_run(path, minimal_row())
    before = read_bytes(path)
    append_run(path, full_row(variant="incremental"))
    after = read_bytes(path)
    assert after.startswith(before)
    assert after.count(b"\n") == before.count(b"\n") + 1
    with open(path, encoding="utf-8", newline="") as f:
        rows = list(csv.DictReader(f))
    assert [r["variant"] for r in rows] == ["baseline", "incremental"]
    assert rows[1]["row_count"] == "42"


def test_append_uses_lf_line_endings_only(tmp_path):
    path = tmp_path / "runs.csv"
    append_run(path, minimal_row())
    append_run(path, full_row())
    assert b"\r" not in read_bytes(path)


def test_append_after_file_without_trailing_newline(tmp_path):
    path = tmp_path / "runs.csv"
    existing = (
        HEADER_LINE.encode() + b"2026-11-10T08:00:00+00:00,abcdef0,baseline,2026-11-09,1,,,,,,,,,,"
    )
    path.write_bytes(existing)
    append_run(path, minimal_row())
    data = read_bytes(path)
    assert data.startswith(existing + b"\n")
    lines = data.split(b"\n")
    assert len(lines) == 4 and lines[3] == b""
    assert lines[2].startswith(b"2026-11-11T08:30:00+00:00,")


def test_append_notes_with_comma_and_quote_roundtrip(tmp_path):
    path = tmp_path / "runs.csv"
    note = 'z-order, "hot" partitions'
    append_run(path, minimal_row(notes=note))
    with open(path, encoding="utf-8", newline="") as f:
        rows = list(csv.DictReader(f))
    assert rows[0]["notes"] == note


def test_append_writes_utf8(tmp_path):
    path = tmp_path / "runs.csv"
    append_run(path, minimal_row(notes="chạy thử"))
    assert "chạy thử".encode() in read_bytes(path)


@pytest.mark.parametrize(
    "header",
    [
        b"run_at,git_sha\n",
        HEADER_LINE.replace("notes", "note").encode(),
        HEADER_LINE.replace("run_at,git_sha", "git_sha,run_at").encode(),
        b"garbage\n",
    ],
)
def test_append_rejects_wrong_header_and_leaves_file_unchanged(tmp_path, header):
    path = tmp_path / "runs.csv"
    existing = header + b"some,old,row\n"
    path.write_bytes(existing)
    with pytest.raises(RunsLogError, match="header"):
        append_run(path, minimal_row())
    assert read_bytes(path) == existing


def test_append_invalid_row_does_not_create_file(tmp_path):
    path = tmp_path / "runs.csv"
    with pytest.raises(RunsLogError, match="git_sha"):
        append_run(path, minimal_row(git_sha="XYZ"))
    assert not path.exists()


def test_append_invalid_row_leaves_existing_file_unchanged(tmp_path):
    path = tmp_path / "runs.csv"
    append_run(path, minimal_row())
    before = read_bytes(path)
    with pytest.raises(RunsLogError, match="total_seconds"):
        append_run(path, minimal_row(total_seconds=-1))
    assert read_bytes(path) == before


def test_append_missing_parent_dir_raises_naturally(tmp_path):
    path = tmp_path / "missing" / "runs.csv"
    with pytest.raises(FileNotFoundError):
        append_run(path, minimal_row())
    assert not (tmp_path / "missing").exists()
