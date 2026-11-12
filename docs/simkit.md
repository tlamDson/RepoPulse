# simkit — simulated event data with a seed and known faults

`simkit/` generates events of the **simulated** travel-planning app following the `eventspec/` catalog
(`docs/eventspec.md`), then injects a known number of data faults. There are no real
users; this is **not** GH Archive data. Every event carries `simulated: true`, the manifest also
has `"simulated": true`, and the default output directory is `data/simulated/` (not committed, already in
`.gitignore`). Simulated emails only use the reserved domains `example.com` / `example.org`.

Downstream users: PR 5 (bronze/silver: dedup, late-arrival window, PII hashing, quarantine) and PR 8
(running the injected faults through the pipeline; the manifest is the "answer key" to compare against — `docs/data-quality.md`).

## API

```python
from simkit import simulate, inject_faults, to_jsonl, bot_users, is_late, arrival_delay

sim = simulate(seed=42, users=100, days=7, start=date(2026, 11, 1))  # -> Simulation
sim.users  # tuple[SimUser(user_id, persona, email, signed_up_at)]
sim.events  # list[dict], sorted by occurred_at, every event passes eventspec.validate

result = inject_faults(sim.events, {"duplicate": 5, "bot": 2}, seed=42)  # -> FaultResult
result.events  # list[dict] with faults injected (the input is not modified)
result.faults  # tuple[FaultRecord(fault, indices, event_ids, detail)]
result.counts()  # {"duplicate": 5, "null": 0, ..., "bot": 2}
result.manifest()  # JSON-serializable dict, see "Manifest"
```

Public constants: `FAULT_TYPES`, `LATE_THRESHOLD` (1 hour), `MAX_NORMAL_LATENCY` (5 seconds),
`BOT_EVENTS_PER_MINUTE` (30), `PERSONAS`.

## Reproducibility

All randomness comes from a dedicated `random.Random(seed)` (generator) and a separate `random.Random(seed)`
(fault injection); no global `random`, system clock or random uuid is used. `event_id` is a
UUID built from rng bits. The same parameters + the same seed give the same bytes when written as JSONL (`to_jsonl`: one
object per line, `sort_keys=True`, no whitespace, UTF-8, newline `\n`); a different seed gives different output.

## Clean data

- **Personas** (`simkit/personas.py`):

  | persona | share | sessions/day (Poisson, after sign-up day) | action events/session | mix trip_created : itinerary_generated : trip_shared |
  |---|---|---|---|---|
  | `browser` (casual browsing) | 50% | 0.3 | 0–1 | 3 : 1 : 0 |
  | `planner` (trip planning) | 35% | 1.0 | 1–4 | 3 : 4 : 1 |
  | `power` (heavy use) | 15% | 3.0 | 3–8 | 2 : 3 : 3 |

- Each user signs up on a random day in the range; that day has at least one session.
  The first session: `session_started` then `user_signed_up` (same `session_id`, since the envelope requires
  `session_id`); every other event of that person happens after sign-up. `user_signed_up` has both v1 and v2.
- Each session starts with exactly one `session_started`; events within a session are 5–120 seconds apart,
  so a clean user has at most 12 events in one minute. Sessions within a day sit in
  separate time slots, do not overlap, and lie fully within `[start, start + days)` (UTC).
- `itinerary_generated` and `trip_shared` always point to a `trip_id` created earlier by that same person;
  if there is no trip yet, a `trip_created` is generated. `start_date` >= creation date, `day_count` matches the trip length.
- `received_at` = `occurred_at` + 0.2–5 seconds (`MAX_NORMAL_LATENCY`), UTC timestamp with millisecond
  precision (`2026-11-01T08:15:03.120Z`).

## Six fault types and counting units

An event carries at most one fault type; events not chosen stay as they are and remain valid. Targets are
chosen among **eligible** events: those that pass `validate` and are not a duplicate of an earlier event.
Fields that are never modified: `event_id`, `event_name`, `event_version` (except `schema_drift`),
`simulated` — so a faulty event still carries `simulated: true`.

| type | one fault is | independently observable by |
|---|---|---|
| `duplicate` | one exact copy (same `event_id`) of a clean event, always placed after the original; anchored at most 20 input events after the original, in the output it may be further away because late/bot events are interleaved | `eventspec.find_duplicates` returns exactly N, `index` = position of the copy |
| `null` | one event with one field (outside the 4 fields above) set to `null` | `validate` has `field '<f>' must be of type <t>, got null` |
| `wrong_type` | one event with one field changed to a different JSON type (string → integer; number → numeric string; boolean → `"true"`/`"false"`) | `validate` has `field '<f>' must be of type <t>, got <u>` |
| `late` | one event with `received_at - occurred_at` > `LATE_THRESHOLD` (1 hour; plus 1 minute–72 hours), still schema-valid; moved to the position where it "arrives" in the stream | `is_late(event)` / `arrival_delay(event)` |
| `schema_drift` | one event with an unknown field (`utm_source`, `ab_bucket`, `referrer`, `client_build`) **or** `event_version` = the largest declared version + 1 | `validate` has `unexpected field '<f>'` or `event_version N is not declared ...` |
| `bot` | **one bot user** (a new user_id of the same `u-NNNNN` form, the number following the input's largest user; `session_id`/`trip_id`/`itinerary_id` also have the same form as clean data, so a bot is only recognizable by behavior): one session with 40–80 valid events 0.1–0.7 seconds apart, the whole burst within under 60 seconds | `bot_users(events)`: a user with > `BOT_EVENTS_PER_MINUTE` (30) events in any 60-second window |

`null`/`wrong_type`/`schema_drift` faults make an event schema-invalid (PR 5 sends them to quarantine). `late` and
`bot` are schema-valid and only show through arrival time / speed. `duplicate` copies and bot events
are not late.

Constraints: a missing count = 0; if all are 0 the output equals the input exactly. An unknown type name, a negative or
non-integer count, a total of single-event faults (`duplicate` + `null` + `wrong_type` + `late` +
`schema_drift`) greater than the number of eligible events, or `bot` > 0 with no events in the input → raises
`ValueError` stating the reason (no partial injection).

Call `inject_faults` **once on clean data**. A second call on already-injected output is not
supported: the `late` and bot events of the earlier call are still eligible, so the later call may stack faults
on them and the counts of the two manifests no longer add up. If you need several fault types, put them all in
one `counts`.

## Manifest

`result.manifest()` (the CLI adds `params`):

```json
{
  "simulated": true,
  "params": {"seed": 42, "users": 100, "days": 7, "start": "2026-11-01"},
  "late_threshold_seconds": 3600.0,
  "bot_events_per_minute": 30,
  "counts": {"duplicate": 5, "null": 5, "wrong_type": 5, "late": 5, "schema_drift": 5, "bot": 2},
  "faults": [
    {"fault": "wrong_type", "indices": [298], "event_ids": ["dffa28ae-..."],
     "detail": {"field": "start_date", "value": 29431, "expected_type": "string",
                "actual_type": "integer"}}
  ]
}
```

- `indices`: position (from 0) in the output; `event_ids`: same order. A single-event fault has exactly one
  element; `bot` lists every event of that bot.
- `detail` by type: `duplicate` → `original_index`; `null` → `field`, `value` (`null`),
  `expected_type`; `wrong_type` → adds `actual_type`; `late` → `field` (`received_at`),
  `delay_seconds`; `schema_drift` → `kind` (`unexpected_field` | `undeclared_version`), `field`,
  `value` (and `declared`); `bot` → `user_id`, `events`, `span_seconds`.
- The manifest does not copy the original value of the modified field (to avoid carrying emails into the manifest).
- `faults` is sorted by first position in the output.

## Commands

```bash
python -m simkit.generate                                  # 100 users, 7 days from 2026-11-01, seed 42, no faults
python -m simkit.generate --seed 7 --users 200 --days 3 --start 2026-11-01 \
    --duplicate 5 --null 5 --wrong-type 5 --late 5 --schema-drift 5 --bot 2 \
    --out data/simulated
```

Writes `<out>/events.jsonl` and `<out>/manifest.json`. Invalid parameters (for example requesting more faults than
events) exit with code 2 and a message.
