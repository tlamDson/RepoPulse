"""Write one JSON Schema file per (event, version): ``python -m eventspec.generate``."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from eventspec.schema_gen import generate_all
from eventspec.spec import SCHEMA_DIR


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--out", type=Path, default=SCHEMA_DIR, help=f"output directory (default: {SCHEMA_DIR})"
    )
    args = parser.parse_args(argv)
    args.out.mkdir(parents=True, exist_ok=True)
    for name, schema in generate_all().items():
        path = args.out / name
        with open(path, "w", encoding="utf-8", newline="\n") as f:
            f.write(json.dumps(schema, indent=2, sort_keys=True) + "\n")
        print(path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
