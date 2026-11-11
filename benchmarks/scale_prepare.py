"""Prepare the data of the Spark vs DuckDB scale benchmark (not timed).

Thirty days of raw GH Archive (real data, not simulated) do not fit on a 14 GB runner disk, so
every hour file is handled on its own: download -> keep only the event types Q1-Q6 read -> keep
only the fields they read -> write a slim gzip NDJSON file with the same name -> delete the raw
file right away. ``manifest.json`` records, per hour, the raw size and sha256, the line counts,
or ``missing`` when GH Archive answers 404 (never silently skipped).

Usage (what ``.github/workflows/benchmark-scale.yml`` runs)::

    python benchmarks/scale_prepare.py [--config benchmarks/scale.toml]

Re-running skips hours already done (resumable). Network code lives in ``benchmarks/download.py``;
everything here takes a ``downloader`` so the tests never touch the network.
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import os
import re
import sys
import tomllib
from collections.abc import Callable, Iterable, Mapping
from concurrent.futures import ProcessPoolExecutor
from datetime import date, timedelta
from pathlib import Path

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from benchmarks.download import download  # noqa: E402

SCALE_TOML = Path(__file__).resolve().parent / "scale.toml"
MANIFEST = "manifest.json"

# GH Archive lines start with {"id":"<n>","type":"<T>", ... ; when they do, the type is read
# without parsing the (often large) line. Anything else is parsed in full.
_HEAD = re.compile(rb'\{"id":"[0-9]*","type":"([A-Za-z]+)"')
_PAYLOAD_KEYS = ("action", "number")
_NESTED_KEYS = {"pull_request": ("number", "merged"), "issue": ("number",)}

Downloader = Callable[..., Path | None]


def load_config(path: str | os.PathLike[str] = SCALE_TOML) -> dict:
    with open(path, "rb") as fh:
        return tomllib.load(fh)


def window_days(start: str, days: int) -> list[str]:
    if days < 1:
        raise ValueError(f"days must be >= 1 (got {days})")
    first = date.fromisoformat(start)
    return [(first + timedelta(days=i)).isoformat() for i in range(days)]


def hour_files(start: str, days: int) -> list[str]:
    """GH Archive hour file names of the window, day by day, hour 0..23 (not zero-padded)."""
    return [f"{day}-{hour}.json.gz" for day in window_days(start, days) for hour in range(24)]


def _pick(obj: object, keys: Iterable[str]) -> object:
    if not isinstance(obj, dict):
        return obj
    return {k: obj[k] for k in keys if k in obj}


def slim_event(event: Mapping) -> dict:
    """Only the fields Q1-Q6 read; keys absent in the input stay absent; values untouched."""
    out: dict = {k: event[k] for k in ("id", "type") if k in event}
    if "actor" in event:
        out["actor"] = _pick(event["actor"], ("id", "login"))
    if "repo" in event:
        out["repo"] = _pick(event["repo"], ("id", "name"))
    if "payload" in event:
        payload = event["payload"]
        if isinstance(payload, dict):
            slim = {k: payload[k] for k in _PAYLOAD_KEYS if k in payload}
            for key, sub in _NESTED_KEYS.items():
                if key in payload:
                    slim[key] = _pick(payload[key], sub)
            payload = slim
        out["payload"] = payload
    if "created_at" in event:
        out["created_at"] = event["created_at"]
    return out


def slim_file(raw: Path, out: Path, event_types: Iterable[str]) -> dict[str, int]:
    """Filter + slim one gzip NDJSON file into ``out`` (written atomically); return counts."""
    wanted = set(event_types)
    wanted_bytes = {t.encode() for t in wanted}
    out = Path(out)
    out.parent.mkdir(parents=True, exist_ok=True)
    part = out.with_name(out.name + ".part")
    stats = {"total_lines": 0, "kept_lines": 0, "bad_lines": 0}
    with gzip.open(raw, "rb") as fin, gzip.open(part, "wb", compresslevel=6) as fout:
        for line in fin:
            if not line.strip():
                continue
            stats["total_lines"] += 1
            head = _HEAD.match(line)
            if head is not None and head.group(1) not in wanted_bytes:
                continue
            try:
                event = json.loads(line)
            except ValueError:
                stats["bad_lines"] += 1
                continue
            if not isinstance(event, dict) or event.get("type") not in wanted:
                continue
            fout.write(json.dumps(slim_event(event), separators=(",", ":")).encode() + b"\n")
            stats["kept_lines"] += 1
    os.replace(part, out)
    return stats


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def prepare_hour(
    name: str,
    raw_dir: Path,
    out_dir: Path,
    event_types: Iterable[str],
    *,
    downloader: Downloader = download,
) -> dict:
    """Download, slim and delete one hour file; return its manifest entry."""
    raw = downloader(name, Path(raw_dir))
    if raw is None:
        return {"name": name, "status": "missing"}
    raw = Path(raw)
    try:
        entry: dict = {"name": name, "status": "ok", "raw_bytes": raw.stat().st_size}
        entry["raw_sha256"] = _sha256(raw)
        out = Path(out_dir) / name
        entry.update(slim_file(raw, out, event_types))
        entry["slim_bytes"] = out.stat().st_size
    finally:
        raw.unlink(missing_ok=True)
    return entry


def _write_manifest(path: Path, header: dict, entries: dict[str, dict], names: list[str]) -> None:
    body = {**header, "hours": [entries[n] for n in names if n in entries]}
    part = path.with_name(path.name + ".part")
    part.write_text(json.dumps(body, indent=1) + "\n", encoding="utf-8")
    os.replace(part, path)


def main(argv: list[str] | None = None, *, downloader: Downloader = download) -> int:
    parser = argparse.ArgumentParser(description="Download + slim the scale benchmark window.")
    parser.add_argument("--config", type=Path, default=SCALE_TOML)
    args = parser.parse_args(argv)

    cfg = load_config(args.config)
    days = max(cfg["levels"])
    names = hour_files(cfg["start_date"], days)
    data_dir = Path(cfg["data_dir"])
    raw_dir = data_dir.parent / (data_dir.name + "-raw")
    data_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = data_dir / MANIFEST
    header = {
        "source": "GH Archive (real data), filtered and slimmed for Q1-Q6",
        "start_date": cfg["start_date"],
        "days": days,
        "event_types": list(cfg["event_types"]),
    }

    entries: dict[str, dict] = {}
    if manifest_path.is_file():
        old = json.loads(manifest_path.read_text(encoding="utf-8"))
        same = all(old.get(k) == v for k, v in header.items() if k != "source")
        if same:
            for e in old.get("hours", []):
                if e.get("status") == "ok" and (data_dir / e["name"]).is_file():
                    entries[e["name"]] = e
    todo = [n for n in names if n not in entries]
    print(f"{len(names)} hour files in window, {len(entries)} already prepared, {len(todo)} to do")

    errors: list[str] = []
    workers = int(cfg.get("workers", 1))

    def _done(name: str, entry: dict) -> None:
        entries[name] = entry
        _write_manifest(manifest_path, header, entries, names)
        status = entry["status"]
        kept = f" kept {entry['kept_lines']}/{entry['total_lines']}" if status == "ok" else ""
        print(f"{name}: {status}{kept} ({len(entries)}/{len(names)})", flush=True)

    types = list(cfg["event_types"])
    if workers <= 1:
        for name in todo:
            try:
                _done(name, prepare_hour(name, raw_dir, data_dir, types, downloader=downloader))
            except Exception as exc:  # noqa: BLE001 - reported below, other hours go on
                errors.append(f"{name}: {exc}")
    else:
        with ProcessPoolExecutor(max_workers=workers) as pool:
            futures = {
                pool.submit(prepare_hour, n, raw_dir, data_dir, types, downloader=downloader): n
                for n in todo
            }
            for fut, name in futures.items():
                try:
                    _done(name, fut.result())
                except Exception as exc:  # noqa: BLE001
                    errors.append(f"{name}: {exc}")
    _write_manifest(manifest_path, header, entries, names)

    missing = [n for n in names if entries.get(n, {}).get("status") == "missing"]
    print(f"prepared {len(names) - len(missing) - len(errors)} hours, {len(missing)} missing (404)")
    if missing:
        print("missing hours: " + ", ".join(missing))
    if errors:
        print("ERROR: " + "; ".join(errors), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
