"""Hourly GH Archive ingest into the landing zone (incremental batch, idempotent per (date, hour)).

One GH Archive hour file (``YYYY-MM-DD-H.json.gz``) becomes one landing partition::

    <landing_root>/date=YYYY-MM-DD/hour=HH/
        events.json.gz        kept lines, verbatim, original order (deterministic gzip)
        _quarantine.jsonl.gz  lines that are not JSON objects with a string ``type`` (only if any)
        _manifest.json        written last: its presence means the partition is complete

The source file is streamed line by line (an hour is ~1 GB uncompressed) and deleted afterwards.
Re-running an hour whose manifest already lists the same event types is a no-op (``skipped``);
``force`` or different event types rebuild the partition from scratch. Duplicate ``id`` values
inside one source file are kept: deduplication belongs to silver.

Stdlib only. Usage: ``python -m repopulse.ingest --help``.
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import os
import re
import sys
import time
import urllib.error
import urllib.request
from collections import Counter
from collections.abc import Callable, Iterable, Sequence
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

URL_TEMPLATE = "https://data.gharchive.org/{name}"
DEFAULT_LANDING = Path("data/landing/gharchive")
DEFAULT_WORK_DIR = Path("data/raw/gharchive-ingest")
MAX_HOURS = 6  # runner disk is 14 GB; a raw hour is ~100+ MB compressed

# Every type used by benchmarks/queries/q1..q6 plus ReleaseEvent for release cadence.
DEFAULT_EVENT_TYPES: tuple[str, ...] = (
    "PushEvent",
    "PullRequestEvent",
    "PullRequestReviewEvent",
    "PullRequestReviewCommentEvent",
    "IssuesEvent",
    "IssueCommentEvent",
    "ForkEvent",
    "WatchEvent",
    "ReleaseEvent",
)

EVENTS_FILE = "events.json.gz"
QUARANTINE_FILE = "_quarantine.jsonl.gz"
MANIFEST_FILE = "_manifest.json"

_HEADERS = {"User-Agent": "gharchive-ingest/1.0"}
_DAY_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")

Fetcher = Callable[[str, Path], Path | None]


@dataclass(frozen=True)
class HourResult:
    data_date: str
    hour: int
    status: str  # ingested | skipped | missing
    source_name: str
    events_total: int = 0
    events_kept: int = 0
    events_malformed: int = 0
    partition: str | None = None

    def to_dict(self) -> dict:
        return asdict(self)


def source_name(day: str, hour: int) -> str:
    """GH Archive file name; the hour is not zero-padded."""
    return f"{day}-{hour}.json.gz"


def partition_dir(landing_root: Path, day: str, hour: int) -> Path:
    return Path(landing_root) / f"date={day}" / f"hour={hour:02d}"


def validate(day: str, hour: int) -> None:
    """Raise ValueError unless ``day`` is a real YYYY-MM-DD date and ``hour`` is 0..23."""
    if not isinstance(day, str) or not _DAY_RE.match(day):
        raise ValueError(f"date must be YYYY-MM-DD, got {day!r}")
    try:
        date.fromisoformat(day)
    except ValueError as exc:
        raise ValueError(f"not a calendar date: {day!r}") from exc
    if isinstance(hour, bool) or not isinstance(hour, int) or not 0 <= hour <= 23:
        raise ValueError(f"hour must be an integer 0..23, got {hour!r}")


def target_hours(now: datetime, *, lag_hours: int = 2, count: int = 1) -> list[tuple[str, int]]:
    """``count`` consecutive (date, hour) pairs, ascending, ending at floor(now UTC) - lag."""
    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("now must be timezone-aware")
    if lag_hours < 0:
        raise ValueError("lag_hours must be >= 0")
    if count < 1:
        raise ValueError("count must be >= 1")
    end = now.astimezone(UTC).replace(minute=0, second=0, microsecond=0)
    end -= timedelta(hours=lag_hours)
    hours = [end - timedelta(hours=i) for i in range(count - 1, -1, -1)]
    return [(h.date().isoformat(), h.hour) for h in hours]


def download(name: str, dest_dir: Path, *, retries: int = 3, timeout: int = 300) -> Path | None:
    """Fetch ``name`` into ``dest_dir`` via ``.part`` + rename; None when the server has no file."""
    dest_dir = Path(dest_dir)
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest = dest_dir / name
    part = dest_dir / f"{name}.part"
    url = URL_TEMPLATE.format(name=name)
    for attempt in range(1, retries + 1):
        try:
            req = urllib.request.Request(url, headers=_HEADERS)
            with urllib.request.urlopen(req, timeout=timeout) as resp, part.open("wb") as out:
                while chunk := resp.read(1 << 20):
                    out.write(chunk)
            os.replace(part, dest)
            return dest
        except urllib.error.HTTPError as exc:
            part.unlink(missing_ok=True)
            if exc.code == 404:
                return None
            if attempt == retries:
                raise
        except (urllib.error.URLError, TimeoutError, ConnectionError):
            part.unlink(missing_ok=True)
            if attempt == retries:
                raise
        time.sleep(5 * attempt)
    return None


class _DeterministicGzip:
    """Gzip writer with mtime=0 and no file name, so equal input gives equal bytes."""

    def __init__(self, path: Path):
        self._fh = path.open("wb")
        self._gz = gzip.GzipFile(filename="", mode="wb", fileobj=self._fh, mtime=0, compresslevel=6)

    def write(self, data: bytes) -> None:
        self._gz.write(data)

    def close(self) -> None:
        try:
            self._gz.close()
        finally:
            self._fh.close()

    def __enter__(self) -> _DeterministicGzip:
        return self

    def __exit__(self, *exc) -> None:
        self.close()


def _event_type(line: bytes) -> str | None:
    """The event's ``type`` or None when the line is not a JSON object with a string type."""
    try:
        obj = json.loads(line)
    except ValueError:  # JSONDecodeError and UnicodeDecodeError
        return None
    if not isinstance(obj, dict):
        return None
    etype = obj.get("type")
    return etype if isinstance(etype, str) and etype else None


def _sha256(path: Path) -> tuple[str, int]:
    digest = hashlib.sha256()
    size = 0
    with path.open("rb") as fh:
        while chunk := fh.read(1 << 20):
            digest.update(chunk)
            size += len(chunk)
    return digest.hexdigest(), size


def _read_manifest(dest: Path) -> dict | None:
    try:
        data = json.loads((dest / MANIFEST_FILE).read_text(encoding="utf-8"))
    except (FileNotFoundError, ValueError):
        return None
    return data if isinstance(data, dict) else None


def _land(
    raw: Path, dest: Path, day: str, hour: int, name: str, types: list[str], now: datetime
) -> dict:
    """Stream ``raw`` into temp files inside ``dest``, then swap them in; manifest goes last."""
    created = not dest.exists()
    dest.mkdir(parents=True, exist_ok=True)
    tmp_events = dest / f".{EVENTS_FILE}.tmp"
    tmp_quarantine = dest / f".{QUARANTINE_FILE}.tmp"
    tmp_manifest = dest / f".{MANIFEST_FILE}.tmp"
    wanted = set(types)
    try:
        sha, size = _sha256(raw)
        counts: Counter[str] = Counter()
        total = kept = malformed = 0
        quarantine: _DeterministicGzip | None = None
        try:
            with gzip.open(raw, "rb") as src, _DeterministicGzip(tmp_events) as out:
                for line in src:
                    if not line.strip():
                        continue
                    if not line.endswith(b"\n"):
                        line += b"\n"
                    total += 1
                    etype = _event_type(line)
                    if etype is None:
                        malformed += 1
                        if quarantine is None:
                            quarantine = _DeterministicGzip(tmp_quarantine)
                        quarantine.write(line)
                        continue
                    counts[etype] += 1
                    if etype in wanted:
                        kept += 1
                        out.write(line)
        finally:
            if quarantine is not None:
                quarantine.close()

        manifest = {
            "data_date": day,
            "hour": hour,
            "source_name": name,
            "source_sha256": sha,
            "source_bytes": size,
            "event_types": types,
            "events_total": total,
            "events_kept": kept,
            "events_malformed": malformed,
            "counts_by_type": dict(sorted(counts.items())),
            "ingested_at": now.isoformat(),
        }
        tmp_manifest.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")

        # Drop the old manifest first: a crash during the swap leaves the partition incomplete
        # (no manifest), so the next run rebuilds it instead of trusting mixed files.
        (dest / MANIFEST_FILE).unlink(missing_ok=True)
        os.replace(tmp_events, dest / EVENTS_FILE)
        if malformed:
            os.replace(tmp_quarantine, dest / QUARANTINE_FILE)
        else:
            (dest / QUARANTINE_FILE).unlink(missing_ok=True)
        os.replace(tmp_manifest, dest / MANIFEST_FILE)
        return manifest
    except BaseException:
        for tmp in (tmp_events, tmp_quarantine, tmp_manifest):
            tmp.unlink(missing_ok=True)
        if created:
            try:
                dest.rmdir()
            except OSError:
                pass
        raise


def ingest_hour(
    day: str,
    hour: int,
    *,
    landing_root: Path = DEFAULT_LANDING,
    work_dir: Path = DEFAULT_WORK_DIR,
    fetcher: Fetcher | None = None,
    event_types: Iterable[str] = DEFAULT_EVENT_TYPES,
    force: bool = False,
    now: datetime | None = None,
) -> HourResult:
    """Ingest one GH Archive hour into its landing partition. See the module docstring."""
    validate(day, hour)
    types = sorted(set(event_types))
    if not types:
        raise ValueError("event_types must not be empty")
    if now is None:
        now = datetime.now(UTC)
    elif now.tzinfo is None:
        raise ValueError("now must be timezone-aware")

    name = source_name(day, hour)
    dest = partition_dir(Path(landing_root), day, hour)
    existing = _read_manifest(dest)
    if existing is not None and not force and existing.get("event_types") == types:
        return HourResult(
            data_date=day,
            hour=hour,
            status="skipped",
            source_name=name,
            events_total=existing.get("events_total", 0),
            events_kept=existing.get("events_kept", 0),
            events_malformed=existing.get("events_malformed", 0),
            partition=str(dest),
        )

    fetch = fetcher if fetcher is not None else download
    raw = fetch(name, Path(work_dir))
    if raw is None:
        return HourResult(data_date=day, hour=hour, status="missing", source_name=name)
    raw = Path(raw)
    try:
        manifest = _land(raw, dest, day, hour, name, types, now)
    finally:
        raw.unlink(missing_ok=True)
    return HourResult(
        data_date=day,
        hour=hour,
        status="ingested",
        source_name=name,
        events_total=manifest["events_total"],
        events_kept=manifest["events_kept"],
        events_malformed=manifest["events_malformed"],
        partition=str(dest),
    )


def _parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="python -m repopulse.ingest",
        description="Ingest GH Archive hour files into the landing zone (incremental batch).",
    )
    p.add_argument("--date", help="data date YYYY-MM-DD (UTC); needs --hour")
    p.add_argument("--hour", type=int, help="data hour 0..23 (UTC); needs --date")
    p.add_argument(
        "--hours", type=int, default=1, help=f"number of consecutive hours, 1..{MAX_HOURS}"
    )
    p.add_argument("--lag-hours", type=int, default=2, help="without --date: hours behind now")
    p.add_argument("--landing-dir", type=Path, default=DEFAULT_LANDING)
    p.add_argument("--work-dir", type=Path, default=DEFAULT_WORK_DIR)
    p.add_argument("--event-types", help="comma-separated event types to keep")
    p.add_argument("--force", action="store_true", help="re-download and rebuild the partition")
    p.add_argument("--summary", type=Path, help="write the JSON array of results here")
    return p


def main(argv: Sequence[str] | None = None, *, now: datetime | None = None) -> int:
    parser = _parser()
    args = parser.parse_args(argv)

    if not 1 <= args.hours <= MAX_HOURS:
        parser.error(f"--hours must be 1..{MAX_HOURS} (runner disk is limited)")
    if (args.date is None) != (args.hour is None):
        parser.error("--date and --hour go together")
    if args.lag_hours < 0:
        parser.error("--lag-hours must be >= 0")
    event_types: Sequence[str] = DEFAULT_EVENT_TYPES
    if args.event_types is not None:
        event_types = [t.strip() for t in args.event_types.split(",") if t.strip()]
        if not event_types:
            parser.error("--event-types must name at least one type")

    if args.date is not None:
        try:
            validate(args.date, args.hour)
        except ValueError as exc:
            parser.error(str(exc))
        end = datetime.fromisoformat(args.date).replace(hour=args.hour, tzinfo=UTC)
        hours = target_hours(end, lag_hours=0, count=args.hours)
    else:
        hours = target_hours(now or datetime.now(UTC), lag_hours=args.lag_hours, count=args.hours)

    records: list[dict] = []
    code = 0
    for day, hour in hours:
        try:
            result = ingest_hour(
                day,
                hour,
                landing_root=args.landing_dir,
                work_dir=args.work_dir,
                event_types=event_types,
                force=args.force,
                now=now,
            )
            record = result.to_dict()
            if result.status == "missing":
                print(
                    f"warning: {result.source_name} is missing on the server (not published yet?)",
                    file=sys.stderr,
                )
        except Exception as exc:  # report the hour, keep going, exit non-zero
            code = 1
            record = {
                "data_date": day,
                "hour": hour,
                "status": "error",
                "source_name": source_name(day, hour),
                "error": f"{type(exc).__name__}: {exc}",
            }
            print(f"error: {record['source_name']}: {record['error']}", file=sys.stderr)
        records.append(record)
        print(json.dumps(record, sort_keys=True), flush=True)

    if args.summary is not None:
        args.summary.parent.mkdir(parents=True, exist_ok=True)
        args.summary.write_text(json.dumps(records, indent=2) + "\n", encoding="utf-8")
    return code


if __name__ == "__main__":
    sys.exit(main())
