"""Plant six known faults in clean SIMULATED events and return a manifest (the answer key).

One fault is:

- ``duplicate``: one extra, identical copy of a clean event, placed after the original.
- ``null``: one event with one field set to ``null``.
- ``wrong_type``: one event with one field set to a value of another JSON type.
- ``late``: one event whose ``received_at`` is more than ``LATE_THRESHOLD`` after ``occurred_at``
  (still schema-valid), moved to where it would arrive in the stream.
- ``schema_drift``: one event with an unexpected extra field or an undeclared ``event_version``.
- ``bot``: one extra simulated user that emits more than ``BOT_EVENTS_PER_MINUTE`` schema-valid
  events within one minute (a clean user never gets close).

Faults never overlap: each targeted event carries exactly one fault, every other event is kept
as is.
"""

from __future__ import annotations

import bisect
import copy
import random
import re
from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from eventspec import Catalog, dedup_key, default_catalog, validate
from simkit._common import fmt_ts, ms, new_event_id, parse_ts
from simkit.generator import (
    APP_VERSIONS,
    envelope,
    itinerary_fields,
    latency_ms,
    trip_fields,
)

FAULT_TYPES = ("duplicate", "null", "wrong_type", "late", "schema_drift", "bot")
SINGLE_EVENT_FAULTS = FAULT_TYPES[:5]

LATE_THRESHOLD = timedelta(hours=1)
BOT_EVENTS_PER_MINUTE = 30  # a user with more events than this inside any 60 s window is a bot
BOT_WINDOW = timedelta(minutes=1)
BOT_EVENTS_RANGE = (40, 80)
BOT_GAP_MS = (100, 700)  # 79 gaps * 700 ms < 60 s: the whole burst fits in one minute
LATE_EXTRA_MS = (60_000, 72 * 3_600_000)  # late events arrive 1 min to 72 h past the threshold
# A resent copy is anchored at most this many INPUT events after the original. In the output it
# can sit further away, because late and bot events are inserted in between.
DUPLICATE_MAX_DISTANCE = 20

_PROTECTED = frozenset({"event_id", "event_name", "event_version", "simulated"})
_DRIFT_FIELDS = {
    "utm_source": ("newsletter", "partner", "ads"),
    "ab_bucket": ("control", "variant_a", "variant_b"),
    "referrer": ("https://www.example.com/blog", "https://www.example.org/deals"),
    "client_build": (4120, 4121, 4200),
}
_MIN_TIME = datetime.min.replace(tzinfo=UTC)
_USER_ID = re.compile(r"u-(\d+)")


@dataclass(frozen=True)
class FaultRecord:
    fault: str
    indices: tuple[int, ...]  # positions in the faulted output
    event_ids: tuple[str, ...]  # same order as indices
    detail: dict  # fault-specific: field, value, kind, delay_seconds, user_id...

    def to_dict(self) -> dict:
        return {
            "fault": self.fault,
            "indices": list(self.indices),
            "event_ids": list(self.event_ids),
            "detail": self.detail,
        }


@dataclass(frozen=True)
class FaultResult:
    events: list[dict]
    faults: tuple[FaultRecord, ...]

    def counts(self) -> dict[str, int]:
        counts = dict.fromkeys(FAULT_TYPES, 0)
        for record in self.faults:
            counts[record.fault] += 1
        return counts

    def manifest(self) -> dict:
        return {
            "simulated": True,
            "late_threshold_seconds": LATE_THRESHOLD.total_seconds(),
            "bot_events_per_minute": BOT_EVENTS_PER_MINUTE,
            "counts": self.counts(),
            "faults": [r.to_dict() for r in self.faults],
        }


def arrival_delay(event: Mapping) -> timedelta | None:
    """``received_at - occurred_at``; ``None`` when either is missing or not a timestamp."""
    occurred = parse_ts(event.get("occurred_at"))
    received = parse_ts(event.get("received_at"))
    if occurred is None or received is None:
        return None
    return received - occurred


def is_late(event: Mapping, threshold: timedelta = LATE_THRESHOLD) -> bool:
    delay = arrival_delay(event)
    return delay is not None and delay > threshold


def bot_users(events: Sequence[Mapping], threshold: int = BOT_EVENTS_PER_MINUTE) -> list[str]:
    """User ids with more than ``threshold`` events inside some 60-second window, sorted."""
    times: dict[str, list[datetime]] = defaultdict(list)
    for event in events:
        user_id = event.get("user_id")
        moment = parse_ts(event.get("occurred_at"))
        if isinstance(user_id, str) and moment is not None:
            times[user_id].append(moment)
    bots = []
    for user_id, moments in times.items():
        moments.sort()
        low = 0
        for high, moment in enumerate(moments):
            while moment - moments[low] >= BOT_WINDOW:
                low += 1
            if high - low + 1 > threshold:
                bots.append(user_id)
                break
    return sorted(bots)


def _check_counts(counts: Mapping[str, int]) -> dict[str, int]:
    if not isinstance(counts, Mapping):
        raise ValueError(f"counts must be a mapping of fault type to count, got {counts!r}")
    requested = dict.fromkeys(FAULT_TYPES, 0)
    for key in sorted(counts, key=str):
        if key not in requested:
            raise ValueError(f"unknown fault type {key!r} (known: {', '.join(FAULT_TYPES)})")
        value = counts[key]
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise ValueError(f"count for {key!r} must be a non-negative integer, got {value!r}")
        requested[key] = value
    return requested


def _json_type(value: object) -> str:
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int):
        return "integer"
    if isinstance(value, float):
        return "number"
    return "string"


def _field_type(catalog: Catalog, event: Mapping, name: str) -> str:
    spec = catalog.event(event["event_name"]).version(event["event_version"])
    return next(f.type for f in spec.fields if f.name == name)


def _other_type_value(rng: random.Random, expected: str, value: object) -> object:
    if expected == "string":
        return rng.randint(1, 99_999)
    if expected == "boolean":
        return "true" if value else "false"
    return str(value)  # integer or number -> numeric text


class _Injector:
    def __init__(self, events: Sequence[Mapping], seed: int, catalog: Catalog) -> None:
        self.rng = random.Random(seed)
        self.catalog = catalog
        self.source = [copy.deepcopy(dict(e)) for e in events]
        self.seq = 0
        # (sort key, event, record number or None, source index or None)
        self.items: list[tuple[tuple[int, int, int], dict, int | None, int | None]] = []
        self.records: list[tuple[str, dict]] = []
        self.moved: set[int] = set()
        self.tags: dict[int, int] = {}  # source index -> record number (modified in place)
        self.used_ids = {e["event_id"] for e in self.source if isinstance(e.get("event_id"), str)}
        self.user_ids = {e["user_id"] for e in self.source if isinstance(e.get("user_id"), str)}
        numbers = (_USER_ID.fullmatch(user_id) for user_id in self.user_ids)
        self.last_user_number = max((int(m[1]) for m in numbers if m), default=0)
        prefix, latest = [], _MIN_TIME
        for e in self.source:
            moment = parse_ts(e.get("occurred_at"))
            if moment is not None and moment > latest:
                latest = moment
            prefix.append(latest)
        self.prefix_max = prefix

    def eligible(self) -> list[int]:
        """Valid events that are not already a copy of an earlier one."""
        seen: set[str] = set()
        result = []
        for i, event in enumerate(self.source):
            if validate(event, self.catalog):
                continue
            key = dedup_key(event, self.catalog)
            if key in seen:
                continue
            seen.add(key)
            result.append(i)
        return result

    def _record(self, fault: str, detail: dict) -> int:
        self.records.append((fault, detail))
        return len(self.records) - 1

    def _add(self, anchor: int, rank: int, event: dict, ref: int | None) -> None:
        self.seq += 1
        self.items.append(((anchor, rank, self.seq), event, ref, None))

    def _anchor(self, moment: datetime) -> int:
        """Index of the last source event that had happened by ``moment`` (-1 if none)."""
        return bisect.bisect_right(self.prefix_max, moment) - 1

    def _mutable_fields(self, event: Mapping) -> list[str]:
        return sorted(k for k in event if k not in _PROTECTED)

    def duplicate(self, i: int) -> None:
        last = min(len(self.source) - 1, i + DUPLICATE_MAX_DISTANCE)
        ref = self._record("duplicate", {"original_index": i})
        self._add(self.rng.randint(i, last), 1, copy.deepcopy(self.source[i]), ref)

    def null(self, i: int) -> None:
        event = self.source[i]
        name = self.rng.choice(self._mutable_fields(event))
        expected = _field_type(self.catalog, event, name)
        event[name] = None
        self.tags[i] = self._record(
            "null", {"field": name, "value": None, "expected_type": expected}
        )

    def wrong_type(self, i: int) -> None:
        event = self.source[i]
        name = self.rng.choice(self._mutable_fields(event))
        expected = _field_type(self.catalog, event, name)
        value = _other_type_value(self.rng, expected, event[name])
        event[name] = value
        detail = {
            "field": name,
            "value": value,
            "expected_type": expected,
            "actual_type": _json_type(value),
        }
        self.tags[i] = self._record("wrong_type", detail)

    def late(self, i: int) -> None:
        event = self.source[i]
        occurred = parse_ts(event["occurred_at"])
        received = occurred + LATE_THRESHOLD + ms(self.rng.randint(*LATE_EXTRA_MS))
        event["received_at"] = fmt_ts(received)
        delay = parse_ts(event["received_at"]) - occurred
        self.moved.add(i)
        ref = self._record("late", {"field": "received_at", "delay_seconds": delay.total_seconds()})
        self._add(self._anchor(received), 2, event, ref)

    def schema_drift(self, i: int) -> None:
        event = self.source[i]
        rng = self.rng
        if rng.random() < 0.5:
            name = rng.choice(sorted(_DRIFT_FIELDS))
            value = rng.choice(_DRIFT_FIELDS[name])
            event[name] = value
            detail = {"kind": "unexpected_field", "field": name, "value": value}
        else:
            declared = sorted(self.catalog.event(event["event_name"]).versions)
            value = declared[-1] + 1
            event["event_version"] = value
            detail = {
                "kind": "undeclared_version",
                "field": "event_version",
                "value": value,
                "declared": declared,
            }
        self.tags[i] = self._record("schema_drift", detail)

    def _new_user_number(self) -> int:
        """Next unused user number: bot ids use the clean id format, so only behavior tells."""
        while True:
            self.last_user_number += 1
            user_id = f"u-{self.last_user_number:05d}"
            if user_id not in self.user_ids:
                self.user_ids.add(user_id)
                return self.last_user_number

    def bot(self, first: datetime, last: datetime) -> None:
        rng = self.rng
        number = self._new_user_number()
        user_id = f"u-{number:05d}"
        session_id = f"s-{number:05d}-0001"
        itineraries = 0
        span_ms = int((last - first).total_seconds() * 1000)
        moment = first + ms(rng.randint(0, max(span_ms, 0)))
        total = rng.randint(*BOT_EVENTS_RANGE)
        ref = self._record("bot", {"user_id": user_id, "events": total})
        trips: list[dict] = []
        begin = moment
        for n in range(total):
            if n == 0:
                name = "session_started"
                fields: dict = {"platform": "web", "app_version": rng.choice(APP_VERSIONS)}
            elif trips and rng.random() < 0.5:
                name = "itinerary_generated"
                itineraries += 1
                itinerary_id = f"i-{number:05d}-{itineraries:04d}"
                fields = itinerary_fields(rng, rng.choice(trips), itinerary_id)
            else:
                name = "trip_created"
                fields = trip_fields(rng, moment, f"t-{number:05d}-{len(trips) + 1:04d}")
                trips.append(fields)
            event = {
                "event_id": new_event_id(rng, self.used_ids),
                **envelope(name, 1, moment, latency_ms(rng), user_id, session_id),
                **fields,
            }
            self._add(self._anchor(moment), 3, event, ref)
            if n < total - 1:
                moment += ms(rng.randint(*BOT_GAP_MS))
        self.records[ref][1]["span_seconds"] = (moment - begin).total_seconds()

    def build(self) -> FaultResult:
        for i, event in enumerate(self.source):
            if i not in self.moved:
                self.items.append(((i, 0, 0), event, self.tags.get(i), i))
        self.items.sort(key=lambda item: item[0])
        events = [item[1] for item in self.items]
        positions: dict[int, list[int]] = defaultdict(list)
        out_index_of_source: dict[int, int] = {}
        for out_index, (_, _, ref, src) in enumerate(self.items):
            if ref is not None:
                positions[ref].append(out_index)
            if src is not None:
                out_index_of_source[src] = out_index
        faults = []
        for ref, (fault, detail) in enumerate(self.records):
            if fault == "duplicate":
                detail = {"original_index": out_index_of_source[detail["original_index"]]}
            where = tuple(positions[ref])
            ids = tuple(events[i]["event_id"] for i in where)
            faults.append(FaultRecord(fault, where, ids, detail))
        faults.sort(key=lambda r: r.indices[0])
        return FaultResult(events, tuple(faults))


def inject_faults(
    events: Sequence[Mapping],
    counts: Mapping[str, int],
    seed: int,
    catalog: Catalog | None = None,
) -> FaultResult:
    """Copy ``events`` and plant exactly ``counts[type]`` faults of each type (missing type = 0).

    Raises ``ValueError`` for an unknown type, a negative or non-integer count, or a request for
    more single-event faults than there are eligible (valid, non-duplicate) events. ``events`` is
    not modified. With every count at 0 the output equals the input.

    Meant for clean input, in one call. Calling it again on its own output is not supported: late
    and bot events of the first call are still eligible, so a second pass can stack a fault on
    them and the counts of the two manifests no longer add up.
    """
    requested = _check_counts(counts)
    cat = default_catalog() if catalog is None else catalog
    injector = _Injector(events, seed, cat)
    if not any(requested.values()):
        return FaultResult(injector.source, ())

    eligible = injector.eligible()
    need = sum(requested[t] for t in SINGLE_EVENT_FAULTS)
    if need > len(eligible):
        asked = ", ".join(f"{t}={requested[t]}" for t in SINGLE_EVENT_FAULTS if requested[t])
        raise ValueError(
            f"requested {need} single-event faults ({asked}) but the input has only "
            f"{len(eligible)} eligible events (valid and not duplicated)"
        )
    parsed = (parse_ts(e.get("occurred_at")) for e in injector.source)
    moments = [m for m in parsed if m is not None]
    if requested["bot"] and not moments:
        raise ValueError("bot faults need at least one event with a valid occurred_at")

    pool = list(eligible)
    injector.rng.shuffle(pool)
    start = 0
    for fault in SINGLE_EVENT_FAULTS:
        for i in pool[start : start + requested[fault]]:
            getattr(injector, fault)(i)
        start += requested[fault]
    for _ in range(requested["bot"]):
        injector.bot(min(moments), max(moments))
    return injector.build()
