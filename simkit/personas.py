"""Personas of the SIMULATED trip-planner app users: how often they open the app, what they do."""

from __future__ import annotations

from dataclasses import dataclass

ACTIONS = ("trip_created", "itinerary_generated", "trip_shared")


@dataclass(frozen=True)
class Persona:
    name: str
    weight: float  # share of simulated users with this persona
    sessions_per_day: float  # Poisson mean, on days after sign-up
    actions_per_session: tuple[int, int]  # inclusive range of non-session events per session
    action_weights: tuple[float, float, float]  # weights for ACTIONS, in that order


PERSONAS: tuple[Persona, ...] = (
    Persona("browser", 0.50, 0.3, (0, 1), (3.0, 1.0, 0.0)),
    Persona("planner", 0.35, 1.0, (1, 4), (3.0, 4.0, 1.0)),
    Persona("power", 0.15, 3.0, (3, 8), (2.0, 3.0, 3.0)),
)
