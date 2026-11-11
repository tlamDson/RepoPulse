"""Reference day for every benchmark: selection criteria and checksum verification.

Pure functions only (no network) so they can be unit-tested; downloading lives in
``select_reference_day.py`` and ``fetch_reference.py``.
"""

from __future__ import annotations

import hashlib
import os
import statistics
import tomllib
from collections.abc import Mapping
from pathlib import Path

REFERENCE_TOML = Path(__file__).resolve().parent / "reference.toml"
URL_TEMPLATE = "https://data.gharchive.org/{name}"
HOURS = range(24)


class ChecksumError(RuntimeError):
    """A reference file is missing or its sha256 does not match ``reference.toml``."""


def hour_file_name(day: str, hour: int) -> str:
    """GH Archive file name for one hour; the hour is not zero-padded."""
    return f"{day}-{hour}.json.gz"


def load_reference(path: str | os.PathLike[str] = REFERENCE_TOML) -> dict:
    with open(path, "rb") as fh:
        return tomllib.load(fh)


def sha256_of(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def verify_files(data_dir: str | os.PathLike[str], reference: Mapping) -> list[Path]:
    """Return the reference files in order; raise ``ChecksumError`` naming every bad file."""
    base = Path(data_dir)
    problems: list[str] = []
    paths: list[Path] = []
    for entry in reference["files"]:
        path = base / entry["name"]
        paths.append(path)
        if not path.is_file():
            problems.append(f"{entry['name']}: missing")
            continue
        actual = sha256_of(path)
        if actual != entry["sha256"]:
            problems.append(f"{entry['name']}: sha256 {actual} != expected {entry['sha256']}")
    if problems:
        raise ChecksumError("reference data check failed: " + "; ".join(problems))
    return paths


def evaluate_day(hours: Mapping[int, Mapping[str, int]], *, max_size_ratio: float) -> list[str]:
    """Problems that disqualify a day as reference; an empty list means the day qualifies.

    ``hours`` maps hour -> {"bytes": compressed size, "push_events": count}. A day qualifies
    when all 24 hours exist, each has at least one PushEvent, and each hour's size is within
    ``[median / max_size_ratio, median * max_size_ratio]``.
    """
    problems: list[str] = []
    present = [h for h in HOURS if h in hours]
    for h in HOURS:
        if h not in hours:
            problems.append(f"hour {h}: file missing")
    if not present:
        return problems
    median = statistics.median(hours[h]["bytes"] for h in present)
    for h in present:
        stats = hours[h]
        if stats["push_events"] <= 0:
            problems.append(f"hour {h}: no PushEvent")
        size = stats["bytes"]
        if size * max_size_ratio < median or size > median * max_size_ratio:
            problems.append(
                f"hour {h}: size {size} bytes outside x{max_size_ratio} of median {median:.0f}"
            )
    return problems
