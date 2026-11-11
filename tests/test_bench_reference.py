"""Reference day: selection criteria, checksum check, and the committed ``reference.toml``."""

import hashlib
import re
import tomllib
from datetime import date
from pathlib import Path

import pytest

from benchmarks.reference import (
    ChecksumError,
    evaluate_day,
    hour_file_name,
    load_reference,
    verify_files,
)

ROOT = Path(__file__).resolve().parents[1]
REFERENCE_TOML = ROOT / "benchmarks" / "reference.toml"
KNOWN_INCOMPLETE_DAYS = {"2026-10-15", "2026-11-03", "2026-11-09", "2026-11-10"}


# ---------------------------------------------------------------- selection criteria


def _full_day(size: int = 1000, pushes: int = 50) -> dict[int, dict[str, int]]:
    return {h: {"bytes": size, "push_events": pushes} for h in range(24)}


def test_complete_even_day_passes():
    assert evaluate_day(_full_day(), max_size_ratio=2.0) == []


def test_missing_hour_is_rejected():
    hours = _full_day()
    del hours[5]
    problems = evaluate_day(hours, max_size_ratio=2.0)
    assert problems == ["hour 5: file missing"]


def test_hour_without_push_event_is_rejected():
    hours = _full_day()
    hours[3]["push_events"] = 0
    problems = evaluate_day(hours, max_size_ratio=2.0)
    assert problems == ["hour 3: no PushEvent"]


def test_hour_far_above_median_size_is_rejected():
    hours = _full_day(size=1000)
    hours[7]["bytes"] = 2001  # median stays 1000; 2001 / 1000 > 2.0
    problems = evaluate_day(hours, max_size_ratio=2.0)
    assert len(problems) == 1
    assert problems[0].startswith("hour 7: size 2001 bytes")


def test_hour_far_below_median_size_is_rejected():
    hours = _full_day(size=1000)
    hours[20]["bytes"] = 499  # 1000 / 499 > 2.0
    problems = evaluate_day(hours, max_size_ratio=2.0)
    assert len(problems) == 1
    assert problems[0].startswith("hour 20: size 499 bytes")


def test_size_exactly_at_the_ratio_is_accepted():
    hours = _full_day(size=1000)
    hours[1]["bytes"] = 2000  # ratio exactly 2.0 is allowed
    hours[2]["bytes"] = 500
    assert evaluate_day(hours, max_size_ratio=2.0) == []


def test_hour_file_name_has_no_zero_padding():
    assert hour_file_name("2026-10-25", 0) == "2026-10-25-0.json.gz"
    assert hour_file_name("2026-10-25", 9) == "2026-10-25-9.json.gz"
    assert hour_file_name("2026-10-25", 23) == "2026-10-25-23.json.gz"


# ---------------------------------------------------------------- checksum


def _fake_reference(tmp_path: Path, contents: dict[str, bytes]) -> dict:
    files = []
    for hour, (name, blob) in enumerate(contents.items()):
        (tmp_path / name).write_bytes(blob)
        files.append(
            {
                "hour": hour,
                "name": name,
                "sha256": hashlib.sha256(blob).hexdigest(),
                "bytes": len(blob),
            }
        )
    return {"data_date": "2026-01-05", "files": files}


def test_matching_files_pass(tmp_path):
    ref = _fake_reference(tmp_path, {"a.json.gz": b"alpha", "b.json.gz": b"beta"})
    paths = verify_files(tmp_path, ref)
    assert [p.name for p in paths] == ["a.json.gz", "b.json.gz"]


def test_wrong_checksum_names_the_file(tmp_path):
    ref = _fake_reference(tmp_path, {"a.json.gz": b"alpha", "b.json.gz": b"beta"})
    (tmp_path / "b.json.gz").write_bytes(b"BETA")  # same size, different content
    with pytest.raises(ChecksumError, match=r"b\.json\.gz"):
        verify_files(tmp_path, ref)


def test_missing_file_is_an_error(tmp_path):
    ref = _fake_reference(tmp_path, {"a.json.gz": b"alpha", "b.json.gz": b"beta"})
    (tmp_path / "a.json.gz").unlink()
    with pytest.raises(ChecksumError, match=r"a\.json\.gz"):
        verify_files(tmp_path, ref)


# ---------------------------------------------------------------- committed reference.toml


def test_reference_toml_lists_24_real_hour_files():
    ref = load_reference(REFERENCE_TOML)
    day = ref["data_date"]
    assert date.fromisoformat(day).isoformat() == day
    assert day not in KNOWN_INCOMPLETE_DAYS
    files = ref["files"]
    assert [f["hour"] for f in files] == list(range(24))
    for f in files:
        assert f["name"] == f"{day}-{f['hour']}.json.gz"
        assert re.fullmatch(r"[0-9a-f]{64}", f["sha256"]), f["name"]
        assert isinstance(f["bytes"], int) and f["bytes"] > 0, f["name"]
    assert len({f["sha256"] for f in files}) == 24


def test_reference_toml_records_selection_criteria():
    with REFERENCE_TOML.open("rb") as fh:
        raw = tomllib.load(fh)
    criteria = raw["criteria"]
    assert criteria["hours_required"] == 24
    assert criteria["push_event_every_hour"] is True
    assert criteria["max_size_ratio"] > 1
    assert criteria["description"].strip() != ""
    assert raw["url_template"] == "https://data.gharchive.org/{name}"
