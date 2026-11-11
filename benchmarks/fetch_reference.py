"""Download the reference day listed in ``reference.toml`` and verify every sha256.

Usage::

    python benchmarks/fetch_reference.py [--reference benchmarks/reference.toml] [--data-dir DIR]

Exits non-zero (and prints which file) when a file is missing upstream or its checksum does not
match, so the benchmark workflow stops before running the pipeline.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from benchmarks.download import download  # noqa: E402
from benchmarks.reference import (  # noqa: E402
    REFERENCE_TOML,
    ChecksumError,
    load_reference,
    verify_files,
)

DEFAULT_DATA_ROOT = Path("data/raw/gharchive")


def default_data_dir(reference: dict) -> Path:
    return DEFAULT_DATA_ROOT / reference["data_date"]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--reference", type=Path, default=REFERENCE_TOML)
    parser.add_argument("--data-dir", type=Path, help="default: data/raw/gharchive/<data_date>")
    args = parser.parse_args(argv)

    ref = load_reference(args.reference)
    data_dir = args.data_dir or default_data_dir(ref)
    for entry in ref["files"]:
        path = data_dir / entry["name"]
        if path.is_file() and path.stat().st_size == entry["bytes"]:
            continue
        print(f"downloading {entry['name']}")
        download(entry["name"], data_dir)
    try:
        paths = verify_files(data_dir, ref)
    except ChecksumError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    total = sum(p.stat().st_size for p in paths)
    print(f"verified {len(paths)} files for {ref['data_date']} in {data_dir} ({total} bytes)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
