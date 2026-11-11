"""simkit: seeded generator of SIMULATED trip-planner events (not GH Archive data) and its CLI."""

import copy
import hashlib
import json
import random
from collections import Counter, defaultdict
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pytest

import simkit
from eventspec import find_duplicates, validate
from simkit import (
    LATE_THRESHOLD,
    MAX_NORMAL_LATENCY,
    PERSONAS,
    bot_users,
    simulate,
    to_jsonl,
)
from simkit.generate import DEFAULT_OUT
from simkit.generate import main as generate_main

REPO_ROOT = Path(__file__).resolve().parents[1]
START = date(2026, 11, 1)
RESERVED_DOMAINS = {"example.com", "example.org"}


def parse_ts(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


@pytest.fixture(scope="module")
def sim():
    return simulate(seed=42, users=60, days=7, start=START)


@pytest.fixture(scope="module")
def big_sim():
    return simulate(seed=7, users=300, days=14, start=START)


def sha(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


# --- clean data ------------------------------------------------------------------------------


def test_clean_events_are_all_valid(sim):
    assert len(sim.events) > 200
    invalid = [(i, validate(e)) for i, e in enumerate(sim.events) if validate(e)]
    assert invalid == []


def test_clean_events_have_no_duplicates(sim):
    assert find_duplicates(sim.events) == []


def test_every_event_is_simulated_with_unique_event_id(sim):
    assert all(e["simulated"] is True for e in sim.events)
    ids = [e["event_id"] for e in sim.events]
    assert all(isinstance(i, str) and i for i in ids)
    assert len(set(ids)) == len(ids)


def test_received_at_follows_occurred_at_with_small_delay(sim):
    assert timedelta(0) < MAX_NORMAL_LATENCY < LATE_THRESHOLD
    for e in sim.events:
        delay = parse_ts(e["received_at"]) - parse_ts(e["occurred_at"])
        assert timedelta(0) <= delay <= MAX_NORMAL_LATENCY, e


def test_events_fall_inside_requested_window(sim):
    begin = datetime(2026, 11, 1, tzinfo=UTC)
    end = begin + timedelta(days=7)
    for e in sim.events:
        assert begin <= parse_ts(e["occurred_at"]) < end, e["occurred_at"]


def test_events_are_ordered_by_occurred_at(sim):
    times = [parse_ts(e["occurred_at"]) for e in sim.events]
    assert times == sorted(times)


def test_users_list_matches_events(sim):
    assert len(sim.users) == 60
    assert {u.user_id for u in sim.users} == {e["user_id"] for e in sim.events}
    assert {u.persona for u in sim.users} <= {p.name for p in PERSONAS}


def test_each_user_signs_up_once_right_after_first_session_starts(sim):
    by_user = defaultdict(list)
    for e in sim.events:
        by_user[e["user_id"]].append(e)
    for user_id, events in by_user.items():
        signups = [e for e in events if e["event_name"] == "user_signed_up"]
        assert len(signups) == 1, user_id
        first, second = events[0], events[1]
        assert first["event_name"] == "session_started"
        assert second["event_name"] == "user_signed_up"
        assert second["session_id"] == first["session_id"]
        signup_at = parse_ts(second["occurred_at"])
        assert all(parse_ts(e["occurred_at"]) > signup_at for e in events[2:])


def test_session_started_is_first_and_unique_in_each_session(sim):
    by_session = defaultdict(list)
    for e in sim.events:
        by_session[e["session_id"]].append(e)
    for session_id, events in by_session.items():
        assert events[0]["event_name"] == "session_started", session_id
        assert sum(e["event_name"] == "session_started" for e in events) == 1
        assert len({e["user_id"] for e in events}) == 1
        times = [parse_ts(e["occurred_at"]) for e in events]
        assert all(a < b for a, b in zip(times, times[1:], strict=False))


def test_itineraries_and_shares_reference_an_earlier_trip_of_same_user(sim):
    trips = {}
    used = Counter()
    for e in sim.events:
        if e["event_name"] == "trip_created":
            trips[e["trip_id"]] = e["user_id"]
        elif e["event_name"] in ("itinerary_generated", "trip_shared"):
            assert trips.get(e["trip_id"]) == e["user_id"], e
            used[e["event_name"]] += 1
    assert used["itinerary_generated"] > 0
    assert used["trip_shared"] > 0


def test_trip_dates_are_consistent(sim):
    trip_days = {}
    for e in sim.events:
        if e["event_name"] == "trip_created":
            start = date.fromisoformat(e["start_date"])
            end = date.fromisoformat(e["end_date"])
            assert parse_ts(e["occurred_at"]).date() <= start <= end
            assert e["traveler_count"] >= 1
            trip_days[e["trip_id"]] = (end - start).days + 1
        elif e["event_name"] == "itinerary_generated":
            assert e["day_count"] == trip_days[e["trip_id"]]
            assert e["latency_ms"] > 0


def test_emails_use_reserved_domains_and_are_unique_per_user(sim):
    emails = []
    for e in sim.events:
        for field in ("email", "recipient_email"):
            if field in e:
                assert e[field].rsplit("@", 1)[1] in RESERVED_DOMAINS, e[field]
        if e["event_name"] == "user_signed_up":
            emails.append(e["email"])
    assert len(set(emails)) == len(emails) == 60


def test_both_signup_versions_are_generated(sim):
    versions = {e["event_version"] for e in sim.events if e["event_name"] == "user_signed_up"}
    assert versions == {1, 2}


def test_three_personas_differ_in_session_rate_and_event_mix(big_sim):
    names = [p.name for p in PERSONAS]
    assert len(set(names)) >= 3
    persona_of = {u.user_id: u.persona for u in big_sim.users}
    assert set(persona_of.values()) == set(names)

    sessions = defaultdict(set)
    actions = Counter()
    shares = Counter()
    for e in big_sim.events:
        persona = persona_of[e["user_id"]]
        sessions[persona].add(e["session_id"])
        if e["event_name"] in ("trip_created", "itinerary_generated", "trip_shared"):
            actions[persona] += 1
        if e["event_name"] == "trip_shared":
            shares[persona] += 1
    users = Counter(persona_of.values())
    sessions_per_user = {p: len(sessions[p]) / users[p] for p in names}
    actions_per_session = {p: actions[p] / len(sessions[p]) for p in names}

    assert sessions_per_user["browser"] < sessions_per_user["planner"] < sessions_per_user["power"]
    assert (
        actions_per_session["browser"]
        < actions_per_session["planner"]
        < actions_per_session["power"]
    )
    assert shares["browser"] == 0
    assert shares["power"] / actions["power"] > shares["planner"] / actions["planner"]


def test_clean_users_never_reach_bot_rate(big_sim):
    assert bot_users(big_sim.events) == []


def test_zero_users_gives_no_events():
    assert simulate(seed=1, users=0, days=3, start=START).events == []


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"users": -1, "days": 3}, "users"),
        ({"users": 5, "days": 0}, "days"),
    ],
)
def test_invalid_parameters_raise(kwargs, message):
    with pytest.raises(ValueError, match=message):
        simulate(seed=1, start=START, **kwargs)


# --- determinism -----------------------------------------------------------------------------


def test_same_seed_gives_identical_bytes():
    a = to_jsonl(simulate(seed=123, users=25, days=4, start=START).events)
    b = to_jsonl(simulate(seed=123, users=25, days=4, start=START).events)
    assert sha(a) == sha(b)


def test_different_seed_gives_different_bytes():
    a = to_jsonl(simulate(seed=123, users=25, days=4, start=START).events)
    b = to_jsonl(simulate(seed=124, users=25, days=4, start=START).events)
    assert sha(a) != sha(b)


def test_generator_ignores_and_preserves_global_random_state():
    random.seed(1)
    state = random.getstate()
    a = to_jsonl(simulate(seed=5, users=10, days=2, start=START).events)
    assert random.getstate() == state
    random.seed(999)
    b = to_jsonl(simulate(seed=5, users=10, days=2, start=START).events)
    assert a == b


def test_source_avoids_nondeterministic_calls():
    package = Path(simkit.__file__).parent
    sources = {p.name: p.read_text(encoding="utf-8") for p in sorted(package.glob("*.py"))}
    assert "generator.py" in sources
    for name, text in sources.items():
        for forbidden in ("datetime.now", "uuid4", "hash(", "time.time", "random.random("):
            assert forbidden not in text, f"{name} uses {forbidden}"


def test_jsonl_is_one_sorted_key_object_per_line(sim):
    text = to_jsonl(sim.events)
    lines = text.split("\n")
    assert lines[-1] == ""
    assert [json.loads(line) for line in lines[:-1]] == sim.events
    first = json.loads(lines[0])
    assert lines[0] == json.dumps(first, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def test_simulate_does_not_share_state_between_calls():
    a = simulate(seed=9, users=5, days=2, start=START)
    snapshot = copy.deepcopy(a.events)
    simulate(seed=9, users=5, days=2, start=START).events[0]["user_id"] = "changed"
    assert a.events == snapshot


# --- CLI -------------------------------------------------------------------------------------


def run_cli(out: Path, *extra: str) -> tuple[list[dict], dict]:
    args = ["--seed", "3", "--users", "20", "--days", "3", "--start", "2026-11-01"]
    assert generate_main([*args, *extra, "--out", str(out)]) == 0
    lines = (out / "events.jsonl").read_text(encoding="utf-8").splitlines()
    manifest = json.loads((out / "manifest.json").read_text(encoding="utf-8"))
    return [json.loads(line) for line in lines], manifest


def test_cli_writes_events_and_manifest(tmp_path):
    out = tmp_path / "simulated"
    events, manifest = run_cli(
        out,
        *("--duplicate", "2", "--null", "2", "--wrong-type", "1"),
        *("--late", "2", "--schema-drift", "1", "--bot", "1"),
    )
    assert manifest["simulated"] is True
    assert manifest["params"] == {
        "seed": 3,
        "users": 20,
        "days": 3,
        "start": "2026-11-01",
    }
    assert manifest["counts"] == {
        "duplicate": 2,
        "null": 2,
        "wrong_type": 1,
        "late": 2,
        "schema_drift": 1,
        "bot": 1,
    }
    assert sum(1 for f in manifest["faults"] if f["fault"] == "late") == 2
    assert all(e["simulated"] is True for e in events)
    for record in manifest["faults"]:
        for index, event_id in zip(record["indices"], record["event_ids"], strict=True):
            assert events[index]["event_id"] == event_id


def test_cli_without_faults_writes_clean_data(tmp_path):
    events, manifest = run_cli(tmp_path / "simulated")
    assert manifest["faults"] == []
    assert set(manifest["counts"].values()) == {0}
    assert events == simulate(seed=3, users=20, days=3, start=START).events


def test_cli_same_seed_same_bytes(tmp_path):
    for name in ("a", "b"):
        run_cli(tmp_path / name, "--null", "3", "--bot", "1", "--duplicate", "2")
    for file in ("events.jsonl", "manifest.json"):
        a = (tmp_path / "a" / file).read_bytes()
        b = (tmp_path / "b" / file).read_bytes()
        assert hashlib.sha256(a).hexdigest() == hashlib.sha256(b).hexdigest(), file


def test_cli_rejects_impossible_fault_request(tmp_path):
    with pytest.raises(SystemExit) as exc:
        run_cli(tmp_path / "simulated", "--null", "100000")
    assert exc.value.code == 2


def test_cli_default_out_dir_is_simulated_and_git_ignored():
    assert "simulated" in DEFAULT_OUT.parts
    ignored = (REPO_ROOT / ".gitignore").read_text(encoding="utf-8").splitlines()
    assert f"{DEFAULT_OUT.as_posix()}/" in ignored
