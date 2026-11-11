"""Pure rules shared by the Spark batch: late-arrival window and quarantine reasons."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

DEFAULT_LATE_WINDOW_HOURS = 24

ON_TIME = "on_time"
LATE_ACCEPTED = "late_accepted"
LATE_BEYOND_WINDOW = "late_beyond_window"

REASON_MALFORMED_JSON = "malformed_json"
REASON_SCHEMA_INVALID = "schema_invalid"
REASON_UNKNOWN_EVENT = "unknown_event"
REASON_LATE_BEYOND_WINDOW = "late_beyond_window"


def _floor_hour(ts: datetime) -> datetime:
    return ts.replace(minute=0, second=0, microsecond=0)


def classify_arrival(
    event_time: datetime,
    arrival_time: datetime,
    late_window_hours: int = DEFAULT_LATE_WINDOW_HOURS,
) -> str:
    """``ON_TIME`` when the event is not in an earlier UTC hour than its arrival; otherwise
    ``LATE_ACCEPTED`` if ``arrival - event <= window`` (equal is accepted), else
    ``LATE_BEYOND_WINDOW``."""
    for ts in (event_time, arrival_time):
        if ts.tzinfo is None or ts.utcoffset() is None:
            raise ValueError("event_time and arrival_time must be timezone-aware")
    if late_window_hours < 0:
        raise ValueError("late_window_hours must be >= 0")
    if event_time >= _floor_hour(arrival_time.astimezone(UTC)):
        return ON_TIME
    if arrival_time - event_time <= timedelta(hours=late_window_hours):
        return LATE_ACCEPTED
    return LATE_BEYOND_WINDOW
