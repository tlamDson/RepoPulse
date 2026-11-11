"""Timestamp formatting/parsing and seeded event ids shared by the generator and fault injector."""

from __future__ import annotations

import random
import uuid
from datetime import UTC, datetime, timedelta


def fmt_ts(moment: datetime) -> str:
    """RFC 3339 in UTC with millisecond precision, e.g. ``2026-11-01T08:15:03.120Z``."""
    moment = moment.astimezone(UTC)
    return moment.strftime("%Y-%m-%dT%H:%M:%S.") + f"{moment.microsecond // 1000:03d}Z"


def parse_ts(value: object) -> datetime | None:
    """Parse an RFC 3339 timestamp with offset; ``None`` for anything else (null, wrong type)."""
    if not isinstance(value, str):
        return None
    try:
        moment = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return moment if moment.tzinfo is not None else None


def ms(milliseconds: int) -> timedelta:
    return timedelta(milliseconds=milliseconds)


def new_event_id(rng: random.Random, used: set[str]) -> str:
    """UUID-shaped id drawn from ``rng`` (reproducible), never repeating one in ``used``."""
    while True:
        event_id = str(uuid.UUID(int=rng.getrandbits(128), version=4))
        if event_id not in used:
            used.add(event_id)
            return event_id
