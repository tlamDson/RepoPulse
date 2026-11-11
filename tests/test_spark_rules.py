"""Late-arrival window rule (pure Python, no Spark).

An event is late when its event time falls in an earlier UTC hour than its arrival time. A late
event is accepted when ``arrival - event_time <= late_window_hours`` (equal to the window is still
accepted), otherwise it goes to quarantine as ``late_beyond_window``.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta, timezone

import pytest

from repopulse.spark import rules

ARRIVAL = datetime(2026, 1, 5, 10, 0, tzinfo=UTC)


def test_constants():
    assert rules.DEFAULT_LATE_WINDOW_HOURS == 24
    assert rules.ON_TIME == "on_time"
    assert rules.LATE_ACCEPTED == "late_accepted"
    assert rules.LATE_BEYOND_WINDOW == "late_beyond_window"


def test_quarantine_reason_constants():
    assert rules.REASON_MALFORMED_JSON == "malformed_json"
    assert rules.REASON_SCHEMA_INVALID == "schema_invalid"
    assert rules.REASON_UNKNOWN_EVENT == "unknown_event"
    assert rules.REASON_LATE_BEYOND_WINDOW == "late_beyond_window"


@pytest.mark.parametrize(
    "event_time",
    [
        ARRIVAL,
        ARRIVAL + timedelta(minutes=59, seconds=59),
        ARRIVAL + timedelta(hours=3),  # clock skew into the future is not "late"
    ],
)
def test_event_in_arrival_hour_or_later_is_on_time(event_time):
    assert rules.classify_arrival(event_time, ARRIVAL) == rules.ON_TIME


def test_event_in_same_hour_as_mid_hour_arrival_is_on_time():
    arrival = datetime(2026, 1, 5, 10, 21, tzinfo=UTC)
    event = datetime(2026, 1, 5, 10, 20, tzinfo=UTC)
    assert rules.classify_arrival(event, arrival) == rules.ON_TIME


def test_event_one_second_before_arrival_hour_is_late_accepted():
    event = ARRIVAL - timedelta(seconds=1)
    assert rules.classify_arrival(event, ARRIVAL) == rules.LATE_ACCEPTED


def test_previous_day_within_default_window_is_late_accepted():
    event = ARRIVAL - timedelta(hours=23)
    assert rules.classify_arrival(event, ARRIVAL) == rules.LATE_ACCEPTED


def test_exactly_default_window_is_accepted():
    event = ARRIVAL - timedelta(hours=24)
    assert rules.classify_arrival(event, ARRIVAL) == rules.LATE_ACCEPTED


def test_one_second_beyond_default_window_is_rejected():
    event = ARRIVAL - timedelta(hours=24, seconds=1)
    assert rules.classify_arrival(event, ARRIVAL) == rules.LATE_BEYOND_WINDOW


def test_custom_window_boundary():
    assert (
        rules.classify_arrival(ARRIVAL - timedelta(hours=2), ARRIVAL, late_window_hours=2)
        == rules.LATE_ACCEPTED
    )
    assert (
        rules.classify_arrival(
            ARRIVAL - timedelta(hours=2, seconds=1), ARRIVAL, late_window_hours=2
        )
        == rules.LATE_BEYOND_WINDOW
    )


def test_zero_window_rejects_any_late_event():
    event = ARRIVAL - timedelta(seconds=1)
    assert rules.classify_arrival(event, ARRIVAL, late_window_hours=0) == rules.LATE_BEYOND_WINDOW
    assert rules.classify_arrival(ARRIVAL, ARRIVAL, late_window_hours=0) == rules.ON_TIME


def test_offsets_are_compared_in_utc():
    # 2026-01-05T06:30+07:00 is 2026-01-04T23:30Z: late, 10h30m before arrival.
    event = datetime(2026, 1, 5, 6, 30, tzinfo=timezone(timedelta(hours=7)))
    assert rules.classify_arrival(event, ARRIVAL) == rules.LATE_ACCEPTED
    assert rules.classify_arrival(event, ARRIVAL, late_window_hours=10) == rules.LATE_BEYOND_WINDOW


def test_naive_datetimes_are_rejected():
    with pytest.raises(ValueError):
        rules.classify_arrival(datetime(2026, 1, 5, 9, 0), ARRIVAL)
    with pytest.raises(ValueError):
        rules.classify_arrival(ARRIVAL, datetime(2026, 1, 5, 10, 0))


def test_negative_window_is_rejected():
    with pytest.raises(ValueError):
        rules.classify_arrival(ARRIVAL, ARRIVAL, late_window_hours=-1)
