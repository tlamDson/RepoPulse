"""Write SIMULATED events (JSONL) and the planted-fault manifest: ``python -m simkit.generate``."""

from __future__ import annotations

import argparse
import json
from datetime import date
from pathlib import Path

from simkit.faults import FAULT_TYPES, inject_faults
from simkit.generator import simulate, write_jsonl

DEFAULT_OUT = Path("data") / "simulated"  # git-ignored; never commit generated data
DEFAULT_START = date(2026, 11, 1)
EVENTS_FILE = "events.jsonl"
MANIFEST_FILE = "manifest.json"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seed", type=int, default=42, help="random seed (default: 42)")
    parser.add_argument("--users", type=int, default=100, help="simulated users (default: 100)")
    parser.add_argument("--days", type=int, default=7, help="days of activity (default: 7)")
    parser.add_argument(
        "--start",
        type=date.fromisoformat,
        default=DEFAULT_START,
        help=f"first day, YYYY-MM-DD, UTC (default: {DEFAULT_START.isoformat()})",
    )
    for fault in FAULT_TYPES:
        parser.add_argument(
            f"--{fault.replace('_', '-')}",
            dest=fault,
            type=int,
            default=0,
            help=f"number of '{fault}' faults to plant (default: 0)",
        )
    parser.add_argument(
        "--out", type=Path, default=DEFAULT_OUT, help=f"output directory (default: {DEFAULT_OUT})"
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    counts = {fault: getattr(args, fault) for fault in FAULT_TYPES}
    try:
        sim = simulate(seed=args.seed, users=args.users, days=args.days, start=args.start)
        result = inject_faults(sim.events, counts, seed=args.seed)
    except ValueError as exc:
        parser.error(str(exc))

    params = {
        "seed": args.seed,
        "users": args.users,
        "days": args.days,
        "start": args.start.isoformat(),
    }
    manifest = {"simulated": True, "params": params, **result.manifest()}
    args.out.mkdir(parents=True, exist_ok=True)
    events_path = args.out / EVENTS_FILE
    manifest_path = args.out / MANIFEST_FILE
    write_jsonl(result.events, events_path)
    with open(manifest_path, "w", encoding="utf-8", newline="\n") as f:
        f.write(json.dumps(manifest, indent=2, ensure_ascii=False) + "\n")
    print(f"{events_path} ({len(result.events)} simulated events)")
    print(f"{manifest_path} ({len(result.faults)} planted faults)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
