"""Seeded generator of clean SIMULATED events for the trip-planner app (not GH Archive data).

All randomness comes from one ``random.Random(seed)``; ids and timestamps are derived from it and
from the requested start date, so the same parameters always give the same bytes.
"""

from __future__ import annotations

import json
import math
import random
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

from simkit._common import fmt_ts, ms, new_event_id
from simkit.personas import ACTIONS, PERSONAS, Persona

MAX_NORMAL_LATENCY = timedelta(seconds=5)  # clean received_at - occurred_at never exceeds this
MIN_LATENCY_MS = 200
MIN_EVENT_GAP_MS = 5_000  # gap between two events of one session (caps clean rate at 13/minute)
MAX_EVENT_GAP_MS = 120_000
MAX_SESSIONS_PER_DAY = 8
DAY_MS = 86_400_000

EMAIL_DOMAINS = ("example.com", "example.org")  # reserved for documentation (RFC 2606)
PLATFORMS = ("ios", "android", "web")
DEVICES = {"ios": ("iPhone 15", "iPhone 13", "iPad Air"), "android": ("Pixel 8", "Galaxy S23")}
APP_VERSIONS = ("3.4.0", "3.4.1", "3.5.0")
SIGNUP_METHODS = ("email", "google", "apple")
COUNTRIES = (("Vietnam", "VN"), ("Japan", "JP"), ("France", "FR"), ("Brazil", "BR"))
DESTINATIONS = ("Hanoi", "Da Nang", "Kyoto", "Lisbon", "Paris", "Cusco", "Reykjavik")
SHARE_CHANNELS = ("link", "email", "whatsapp")


@dataclass(frozen=True)
class SimUser:
    user_id: str
    persona: str
    email: str
    signed_up_at: str


@dataclass(frozen=True)
class Simulation:
    users: tuple[SimUser, ...]
    events: list[dict]  # sorted by occurred_at; every event has simulated=True


def _check_int(name: str, value: object, minimum: int) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise ValueError(f"{name} must be an integer >= {minimum}, got {value!r}")


def _poisson(rng: random.Random, mean: float) -> int:
    limit, k, p = math.exp(-mean), 0, 1.0
    while True:
        p *= rng.random()
        if p <= limit:
            return k
        k += 1


def latency_ms(rng: random.Random) -> int:
    return rng.randint(MIN_LATENCY_MS, int(MAX_NORMAL_LATENCY.total_seconds() * 1000))


def envelope(
    name: str, version: int, occurred: datetime, latency: int, user_id: str, session_id: str
) -> dict:
    """Envelope without ``event_id`` (ids are assigned once the final order is known)."""
    return {
        "event_name": name,
        "event_version": version,
        "occurred_at": fmt_ts(occurred),
        "received_at": fmt_ts(occurred + ms(latency)),
        "user_id": user_id,
        "session_id": session_id,
        "simulated": True,
    }


def trip_fields(rng: random.Random, occurred: datetime, trip_id: str) -> dict:
    start = occurred.date() + timedelta(days=rng.randint(3, 90))
    length = rng.randint(1, 14)
    return {
        "trip_id": trip_id,
        "destination": rng.choice(DESTINATIONS),
        "start_date": start.isoformat(),
        "end_date": (start + timedelta(days=length - 1)).isoformat(),
        "traveler_count": rng.randint(1, 6),
    }


def itinerary_fields(rng: random.Random, trip: Mapping, itinerary_id: str) -> dict:
    start = date.fromisoformat(trip["start_date"])
    end = date.fromisoformat(trip["end_date"])
    return {
        "trip_id": trip["trip_id"],
        "itinerary_id": itinerary_id,
        "day_count": (end - start).days + 1,
        "latency_ms": round(rng.uniform(300.0, 9000.0), 1),
    }


class _User:
    """Builds the events of one simulated user, in time order."""

    def __init__(self, rng: random.Random, number: int, origin: datetime) -> None:
        self.rng = rng
        self.number = number
        self.origin = origin
        self.user_id = f"u-{number:05d}"
        self.persona: Persona = rng.choices(PERSONAS, weights=[p.weight for p in PERSONAS])[0]
        self.email = f"traveler{number:05d}@{rng.choice(EMAIL_DOMAINS)}"
        self.platform = rng.choice(PLATFORMS)
        self.device = rng.choice(DEVICES[self.platform]) if self.platform in DEVICES else None
        self.signup_version = 1 if rng.random() < 0.5 else 2
        self.trips: list[dict] = []
        self.counters = {"session": 0, "trip": 0, "itinerary": 0, "share": 0}
        self.drafts: list[tuple[int, int, int, dict]] = []
        self.signed_up_at = ""

    def _next(self, kind: str) -> int:
        self.counters[kind] += 1
        return self.counters[kind]

    def _emit(self, t: int, name: str, version: int, session_id: str, fields: dict) -> None:
        event = envelope(
            name, version, self.origin + ms(t), latency_ms(self.rng), self.user_id, session_id
        )
        event.update(fields)
        self.drafts.append((t, self.number, len(self.drafts), event))

    def _signup_fields(self) -> dict:
        rng = self.rng
        fields: dict = {"email": self.email, "signup_method": rng.choice(SIGNUP_METHODS)}
        country, code = rng.choice(COUNTRIES)
        include_country = rng.random() < 0.7
        if self.signup_version == 1:
            if include_country:
                fields["country"] = country
        else:
            if include_country:
                fields["country_code"] = code
            fields["marketing_opt_in"] = rng.random() < 0.4
        return fields

    def _action(self, t: int, session_id: str) -> None:
        rng = self.rng
        action = rng.choices(ACTIONS, weights=self.persona.action_weights)[0]
        if action != "trip_created" and not self.trips:
            action = "trip_created"
        occurred = self.origin + ms(t)
        if action == "trip_created":
            trip_id = f"t-{self.number:05d}-{self._next('trip'):04d}"
            fields = trip_fields(rng, occurred, trip_id)
            self.trips.append(fields)
        elif action == "itinerary_generated":
            itinerary_id = f"i-{self.number:05d}-{self._next('itinerary'):04d}"
            fields = itinerary_fields(rng, rng.choice(self.trips), itinerary_id)
        else:
            share = self._next("share")
            channel = rng.choice(SHARE_CHANNELS)
            fields = {"trip_id": rng.choice(self.trips)["trip_id"], "channel": channel}
            if channel == "email":
                fields["recipient_email"] = f"friend{self.number:05d}.{share:03d}@example.org"
        self._emit(t, action, 1, session_id, fields)

    def _session(self, t: int, first: bool) -> None:
        rng = self.rng
        session_id = f"s-{self.number:05d}-{self._next('session'):04d}"
        fields: dict = {"platform": self.platform, "app_version": rng.choice(APP_VERSIONS)}
        if self.device is not None:
            fields["device_model"] = self.device
        self._emit(t, "session_started", 1, session_id, fields)
        if first:
            t += rng.randint(MIN_EVENT_GAP_MS, MAX_EVENT_GAP_MS)
            self.signed_up_at = fmt_ts(self.origin + ms(t))
            self._emit(t, "user_signed_up", self.signup_version, session_id, self._signup_fields())
        for _ in range(rng.randint(*self.persona.actions_per_session)):
            t += rng.randint(MIN_EVENT_GAP_MS, MAX_EVENT_GAP_MS)
            self._action(t, session_id)

    def run(self, days: int) -> None:
        rng = self.rng
        signup_day = rng.randrange(days)
        for day in range(signup_day, days):
            count = _poisson(rng, self.persona.sessions_per_day)
            if day == signup_day:
                count = max(1, count)
            count = min(count, MAX_SESSIONS_PER_DAY)
            if count == 0:
                continue
            slot = DAY_MS // count
            for j in range(count):
                start = day * DAY_MS + j * slot + rng.randrange(slot // 2)
                self._session(start, first=(day == signup_day and j == 0))


def simulate(seed: int, users: int, days: int, start: date) -> Simulation:
    """Clean simulated events for ``users`` users over ``days`` days from ``start`` (UTC)."""
    _check_int("users", users, 0)
    _check_int("days", days, 1)
    if not isinstance(start, date):
        raise ValueError(f"start must be a date, got {start!r}")
    rng = random.Random(seed)
    origin = datetime(start.year, start.month, start.day, tzinfo=UTC)

    sim_users: list[SimUser] = []
    drafts: list[tuple[int, int, int, dict]] = []
    for number in range(1, users + 1):
        user = _User(rng, number, origin)
        user.run(days)
        sim_users.append(SimUser(user.user_id, user.persona.name, user.email, user.signed_up_at))
        drafts.extend(user.drafts)
    drafts.sort(key=lambda d: d[:3])

    used: set[str] = set()
    events = [{"event_id": new_event_id(rng, used), **draft[3]} for draft in drafts]
    return Simulation(tuple(sim_users), events)


def to_jsonl(events: Iterable[Mapping]) -> str:
    """One JSON object per line, sorted keys, no spaces, UTF-8 safe; ends with a newline."""
    return "".join(
        json.dumps(e, sort_keys=True, separators=(",", ":"), ensure_ascii=False) + "\n"
        for e in events
    )


def write_jsonl(events: Iterable[Mapping], path: str | Path) -> None:
    with open(path, "w", encoding="utf-8", newline="\n") as f:
        f.write(to_jsonl(events))
