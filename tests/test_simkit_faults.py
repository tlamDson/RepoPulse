"""simkit fault injection: six known data faults planted in SIMULATED events (not GH Archive).

Every fault is checked by an independent observation (eventspec's validate/find_duplicates, the
arrival delay, the per-minute event rate), not only by counting the manifest.
"""

import copy
import json
import random
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta

import pytest

from eventspec import default_catalog, find_duplicates, validate
from simkit import (
    BOT_EVENTS_PER_MINUTE,
    FAULT_TYPES,
    LATE_THRESHOLD,
    bot_users,
    inject_faults,
    simulate,
    to_jsonl,
)

START = date(2026, 11, 1)
ALL = {"duplicate": 4, "null": 3, "wrong_type": 3, "late": 4, "schema_drift": 3, "bot": 2}


@pytest.fixture(scope="module")
def clean():
    return simulate(seed=11, users=40, days=5, start=START).events


@pytest.fixture(scope="module")
def mixed(clean):
    return inject_faults(clean, ALL, seed=99)


def parse_ts(value):
    if not isinstance(value, str):
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def delay(event):
    occurred, received = parse_ts(event.get("occurred_at")), parse_ts(event.get("received_at"))
    return None if occurred is None or received is None else received - occurred


def field_type(event, field):
    spec = default_catalog().event(event["event_name"]).version(event["event_version"])
    return next(f.type for f in spec.fields if f.name == field)


def records(result, fault):
    return [r for r in result.faults if r.fault == fault]


def indices(result, fault):
    return sorted(i for r in records(result, fault) for i in r.indices)


def max_events_per_minute(events):
    times = defaultdict(list)
    for e in events:
        t = parse_ts(e.get("occurred_at"))
        if isinstance(e.get("user_id"), str) and t is not None:
            times[e["user_id"]].append(t)
    best = {}
    for user, ts in times.items():
        ts.sort()
        lo, top = 0, 0
        for hi, t in enumerate(ts):
            while t - ts[lo] >= timedelta(minutes=1):
                lo += 1
            top = max(top, hi - lo + 1)
        best[user] = top
    return best


# --- contract --------------------------------------------------------------------------------


def test_fault_type_names_are_stable():
    assert FAULT_TYPES == ("duplicate", "null", "wrong_type", "late", "schema_drift", "bot")


@pytest.mark.parametrize("counts", [{}, dict.fromkeys(FAULT_TYPES, 0)])
def test_zero_counts_return_input_unchanged(clean, counts):
    result = inject_faults(clean, counts, seed=1)
    assert result.events == clean
    assert result.faults == ()
    assert result.counts() == dict.fromkeys(FAULT_TYPES, 0)


def test_input_is_not_mutated(clean):
    snapshot = copy.deepcopy(clean)
    inject_faults(clean, ALL, seed=5)
    assert clean == snapshot


@pytest.mark.parametrize("fault", FAULT_TYPES)
def test_manifest_count_equals_request_for_each_type(clean, fault):
    n = 2 if fault == "bot" else 5
    result = inject_faults(clean, {fault: n}, seed=3)
    expected = dict.fromkeys(FAULT_TYPES, 0)
    expected[fault] = n
    assert result.counts() == expected
    assert len(records(result, fault)) == n


# --- each fault is observable on its own -----------------------------------------------------


def test_duplicate_seen_by_find_duplicates(clean):
    result = inject_faults(clean, {"duplicate": 6}, seed=4)
    dups = find_duplicates(result.events)
    assert len(dups) == 6
    assert sorted(d.index for d in dups) == indices(result, "duplicate")
    for d in dups:
        assert result.events[d.index] == result.events[d.first_index]
        assert d.index > d.first_index
    assert all(validate(e) == [] for e in result.events)
    assert len(result.events) == len(clean) + 6


def test_null_seen_by_validate(clean):
    result = inject_faults(clean, {"null": 7}, seed=4)
    invalid = sorted(i for i, e in enumerate(result.events) if validate(e))
    assert invalid == indices(result, "null")
    for record in records(result, "null"):
        event = result.events[record.indices[0]]
        field = record.detail["field"]
        assert event[field] is None
        assert record.detail["value"] is None
        errors = validate(event)
        assert f"field '{field}' must be of type {field_type(event, field)}, got null" in errors
        assert all(f"'{field}'" in message for message in errors)


def test_wrong_type_seen_by_validate(clean):
    result = inject_faults(clean, {"wrong_type": 7}, seed=4)
    invalid = sorted(i for i, e in enumerate(result.events) if validate(e))
    assert invalid == indices(result, "wrong_type")
    for record in records(result, "wrong_type"):
        event = result.events[record.indices[0]]
        field = record.detail["field"]
        expected = field_type(event, field)
        assert event[field] is not None
        assert event[field] == record.detail["value"]
        errors = validate(event)
        type_errors = [m for m in errors if m.startswith(f"field '{field}' must be of type ")]
        assert type_errors == [
            f"field '{field}' must be of type {expected}, got {record.detail['actual_type']}"
        ]
        assert record.detail["actual_type"] not in (expected, "null")


def test_late_seen_by_arrival_delay_and_still_valid(clean):
    result = inject_faults(clean, {"late": 6}, seed=4)
    late = sorted(
        i for i, e in enumerate(result.events) if (d := delay(e)) is not None and d > LATE_THRESHOLD
    )
    assert late == indices(result, "late")
    assert len(late) == 6
    for i in late:
        event = result.events[i]
        assert validate(event) == []
        assert delay(event).total_seconds() == records_by_index(result)[i].detail["delay_seconds"]
    assert all(validate(e) == [] for e in result.events)


def records_by_index(result):
    return {i: r for r in result.faults for i in r.indices}


def test_schema_drift_seen_by_validate(clean):
    result = inject_faults(clean, {"schema_drift": 12}, seed=4)
    invalid = sorted(i for i, e in enumerate(result.events) if validate(e))
    assert invalid == indices(result, "schema_drift")
    kinds = set()
    for record in records(result, "schema_drift"):
        event = result.events[record.indices[0]]
        errors = validate(event)
        kind = record.detail["kind"]
        kinds.add(kind)
        if kind == "unexpected_field":
            assert errors == [f"unexpected field '{record.detail['field']}'"]
        else:
            assert kind == "undeclared_version"
            assert event["event_version"] == record.detail["value"]
            assert len(errors) == 1
            assert errors[0].startswith(f"event_version {event['event_version']} is not declared")
        assert event["event_name"] in default_catalog().events
    assert kinds == {"unexpected_field", "undeclared_version"}


def test_bot_seen_by_event_rate(clean):
    result = inject_faults(clean, {"bot": 3}, seed=4)
    rates = max_events_per_minute(result.events)
    fast = sorted(u for u, rate in rates.items() if rate > BOT_EVENTS_PER_MINUTE)
    manifest_users = sorted(r.detail["user_id"] for r in records(result, "bot"))
    assert fast == manifest_users
    assert len(fast) == 3
    assert bot_users(result.events) == fast
    assert max(rates[u] for u in rates if u not in fast) <= BOT_EVENTS_PER_MINUTE // 2
    clean_users = {e["user_id"] for e in clean}
    assert not set(fast) & clean_users
    for record in records(result, "bot"):
        bot_events = [result.events[i] for i in record.indices]
        assert {e["user_id"] for e in bot_events} == {record.detail["user_id"]}
        assert len(bot_events) == record.detail["events"] > BOT_EVENTS_PER_MINUTE
    assert all(validate(e) == [] for e in result.events)
    assert find_duplicates(result.events) == []
    assert all(delay(e) <= LATE_THRESHOLD for e in result.events)


# --- all together ----------------------------------------------------------------------------


def test_all_faults_together_counts_match_observations(clean, mixed):
    assert mixed.counts() == ALL
    assert len(find_duplicates(mixed.events)) == ALL["duplicate"]
    invalid = Counter()
    for i, e in enumerate(mixed.events):
        if validate(e):
            invalid[records_by_index(mixed)[i].fault] += 1
    assert invalid == Counter({"null": 3, "wrong_type": 3, "schema_drift": 3})
    late = [e for e in mixed.events if (d := delay(e)) is not None and d > LATE_THRESHOLD]
    assert len(late) == ALL["late"]
    assert len(bot_users(mixed.events)) == ALL["bot"]


def test_faults_do_not_overlap(mixed):
    seen = Counter(i for r in mixed.faults for i in r.indices)
    assert all(n == 1 for n in seen.values())


def test_untouched_events_are_unchanged_and_valid(clean, mixed):
    by_id = {e["event_id"]: e for e in clean}
    touched = set(records_by_index(mixed))
    untouched = [e for i, e in enumerate(mixed.events) if i not in touched]
    altered = ALL["null"] + ALL["wrong_type"] + ALL["late"] + ALL["schema_drift"]
    assert len(untouched) == len(clean) - altered
    for e in untouched:
        assert e == by_id[e["event_id"]]
        assert validate(e) == []
    bot_events = sum(r.detail["events"] for r in records(mixed, "bot"))
    assert len(mixed.events) == len(clean) + ALL["duplicate"] + bot_events


def test_faulted_events_stay_simulated(mixed):
    assert all(e["simulated"] is True for e in mixed.events)


def test_manifest_points_at_events_by_index_and_event_id(mixed):
    for record in mixed.faults:
        assert record.fault in FAULT_TYPES
        assert len(record.indices) == len(record.event_ids) >= 1
        for i, event_id in zip(record.indices, record.event_ids, strict=True):
            assert mixed.events[i]["event_id"] == event_id


def test_manifest_is_json_and_marked_simulated(mixed):
    manifest = json.loads(json.dumps(mixed.manifest()))
    assert manifest["simulated"] is True
    assert manifest["counts"] == ALL
    assert manifest["late_threshold_seconds"] == LATE_THRESHOLD.total_seconds()
    assert manifest["bot_events_per_minute"] == BOT_EVENTS_PER_MINUTE
    assert len(manifest["faults"]) == sum(ALL.values())
    first = manifest["faults"][0]
    assert set(first) == {"fault", "indices", "event_ids", "detail"}


# --- determinism -----------------------------------------------------------------------------


def test_same_seed_same_faults(clean):
    a = inject_faults(clean, ALL, seed=21)
    random.seed(12345)
    b = inject_faults(clean, ALL, seed=21)
    assert to_jsonl(a.events) == to_jsonl(b.events)
    assert json.dumps(a.manifest()) == json.dumps(b.manifest())


def test_different_seed_different_faults(clean):
    a = inject_faults(clean, ALL, seed=21)
    b = inject_faults(clean, ALL, seed=22)
    assert to_jsonl(a.events) != to_jsonl(b.events)


# --- bad requests ----------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("counts", "message"),
    [
        ({"typo": 1}, "unknown fault type 'typo'"),
        ({"null": -1}, "'null' must be a non-negative integer"),
        ({"late": 1.5}, "'late' must be a non-negative integer"),
        ({"bot": True}, "'bot' must be a non-negative integer"),
    ],
)
def test_bad_counts_raise(clean, counts, message):
    with pytest.raises(ValueError, match=message):
        inject_faults(clean, counts, seed=1)


def test_more_faults_than_eligible_events_raise(clean):
    with pytest.raises(ValueError, match="eligible"):
        inject_faults(clean, {"null": len(clean) + 1}, seed=1)
    half = len(clean) // 2 + 1
    with pytest.raises(ValueError, match="eligible"):
        inject_faults(clean, {"null": half, "late": half}, seed=1)


def test_invalid_input_events_are_not_eligible(clean):
    broken = [dict(e, simulated=False) for e in clean[:-3]] + list(clean[-3:])
    result = inject_faults(broken, {"wrong_type": 3}, seed=1)
    targeted = {i for r in records(result, "wrong_type") for i in r.event_ids}
    assert targeted == {e["event_id"] for e in clean[-3:]}
    with pytest.raises(ValueError, match="eligible"):
        inject_faults(broken, {"wrong_type": 4}, seed=1)


def test_bot_needs_events_to_place_in_time():
    with pytest.raises(ValueError, match="bot"):
        inject_faults([], {"bot": 1}, seed=1)
