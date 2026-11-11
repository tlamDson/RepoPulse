"""SIMULATED event data for the trip-planner app (not GH Archive): seeded generator + known faults.

See docs/simkit.md.
"""

from simkit.faults import (
    BOT_EVENTS_PER_MINUTE,
    FAULT_TYPES,
    LATE_THRESHOLD,
    FaultRecord,
    FaultResult,
    arrival_delay,
    bot_users,
    inject_faults,
    is_late,
)
from simkit.generator import (
    MAX_NORMAL_LATENCY,
    Simulation,
    SimUser,
    simulate,
    to_jsonl,
    write_jsonl,
)
from simkit.personas import PERSONAS, Persona

__all__ = [
    "BOT_EVENTS_PER_MINUTE",
    "FAULT_TYPES",
    "LATE_THRESHOLD",
    "MAX_NORMAL_LATENCY",
    "PERSONAS",
    "FaultRecord",
    "FaultResult",
    "Persona",
    "SimUser",
    "Simulation",
    "arrival_delay",
    "bot_users",
    "inject_faults",
    "is_late",
    "simulate",
    "to_jsonl",
    "write_jsonl",
]
