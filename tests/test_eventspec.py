"""eventspec: validation, dedup key, PII flags and JSON Schema generation.

The catalog describes a *simulated* trip-planner app (not GH Archive data).
"""

import hashlib
import json
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator

import eventspec
from eventspec import (
    CatalogError,
    Duplicate,
    EventSpecError,
    dedup_key,
    find_duplicates,
    generate_all,
    json_schema,
    load_catalog,
    pii_fields,
    schema_filename,
    validate,
)
from eventspec.generate import main as generate_main

PACKAGE_DIR = Path(eventspec.__file__).parent
CATALOG_DIR = PACKAGE_DIR / "catalog"
SCHEMA_DIR = PACKAGE_DIR / "schemas"

EVENT_NAMES = {
    "user_signed_up",
    "session_started",
    "trip_created",
    "itinerary_generated",
    "trip_shared",
}
ENVELOPE_REQUIRED = {
    "event_name",
    "event_version",
    "occurred_at",
    "user_id",
    "session_id",
    "simulated",
}


def envelope(name, version, **overrides):
    event = {
        "event_id": "evt-0001",
        "event_name": name,
        "event_version": version,
        "occurred_at": "2026-11-10T08:15:00Z",
        "user_id": "u-001",
        "session_id": "s-001",
        "simulated": True,
    }
    event.update(overrides)
    return event


def signup_v2(**overrides):
    event = envelope(
        "user_signed_up",
        2,
        email="an.nguyen@example.com",
        signup_method="google",
        country_code="VN",
        marketing_opt_in=False,
    )
    event.update(overrides)
    return event


def signup_v1(**overrides):
    event = envelope(
        "user_signed_up",
        1,
        email="an.nguyen@example.com",
        signup_method="email",
        country="VN",
    )
    event.update(overrides)
    return event


def session_started(**overrides):
    event = envelope("session_started", 1, platform="ios", app_version="3.2.0")
    event.update(overrides)
    return event


def trip_created(**overrides):
    event = envelope(
        "trip_created",
        1,
        trip_id="t-001",
        destination="Da Nang",
        start_date="2026-12-20",
        end_date="2026-12-24",
        traveler_count=2,
    )
    event.update(overrides)
    return event


def itinerary_generated(**overrides):
    event = envelope(
        "itinerary_generated",
        1,
        trip_id="t-001",
        itinerary_id="i-001",
        day_count=5,
        latency_ms=812.5,
    )
    event.update(overrides)
    return event


def trip_shared(**overrides):
    event = envelope(
        "trip_shared",
        1,
        trip_id="t-001",
        channel="email",
        recipient_email="binh.tran@example.com",
    )
    event.update(overrides)
    return event


def without(event, *keys):
    return {k: v for k, v in event.items() if k not in keys}


# --- catalog content ----------------------------------------------------------


def test_default_catalog_lists_the_simulated_app_events():
    assert set(load_catalog().events) == EVENT_NAMES


def test_user_signed_up_declares_two_versions():
    assert sorted(load_catalog().events["user_signed_up"].versions) == [1, 2]


def test_every_catalog_file_says_it_is_simulated():
    files = sorted(CATALOG_DIR.glob("*.yaml"))
    assert len(files) == len(EVENT_NAMES) + 1  # + _envelope.yaml
    for path in files:
        assert "simulated" in path.read_text(encoding="utf-8").lower(), path.name


def test_error_hierarchy():
    assert issubclass(EventSpecError, ValueError)
    assert issubclass(CatalogError, EventSpecError)


# --- validate: accepted -------------------------------------------------------


@pytest.mark.parametrize(
    "event",
    [
        signup_v2(),
        signup_v1(),
        session_started(),
        trip_created(),
        itinerary_generated(),
        trip_shared(),
    ],
    ids=lambda e: f"{e['event_name']}.v{e['event_version']}",
)
def test_valid_event_has_no_errors(event):
    assert validate(event) == []


def test_old_declared_version_is_still_accepted():
    assert validate(signup_v1()) == []
    assert validate(signup_v2()) == []


def test_optional_fields_may_be_omitted():
    event = without(signup_v2(), "event_id", "country_code")
    assert validate(event) == []
    assert validate(without(trip_shared(), "recipient_email")) == []


def test_received_at_is_an_optional_envelope_field():
    assert validate(trip_created(received_at="2026-11-10T09:00:00+07:00")) == []


def test_validate_does_not_mutate_input():
    event = signup_v2()
    snapshot = dict(event)
    validate(event)
    assert event == snapshot


# --- validate: missing required fields ----------------------------------------


@pytest.mark.parametrize(
    "field",
    [
        "event_name",
        "event_version",
        "occurred_at",
        "user_id",
        "session_id",
        "simulated",
        "email",
        "signup_method",
        "marketing_opt_in",
    ],
)
def test_missing_required_field_is_rejected_and_named(field):
    errors = validate(without(signup_v2(), field))
    assert f"missing required field '{field}'" in errors


@pytest.mark.parametrize(
    ("builder", "field"),
    [
        (trip_created, "trip_id"),
        (trip_created, "start_date"),
        (trip_created, "traveler_count"),
        (itinerary_generated, "latency_ms"),
        (session_started, "platform"),
        (trip_shared, "channel"),
    ],
)
def test_missing_event_specific_field_is_rejected(builder, field):
    assert validate(without(builder(), field)) == [f"missing required field '{field}'"]


def test_all_missing_fields_are_reported():
    errors = validate(without(signup_v2(), "email", "user_id"))
    assert "missing required field 'email'" in errors
    assert "missing required field 'user_id'" in errors
    assert len(errors) == 2


# --- validate: wrong types and values -----------------------------------------


@pytest.mark.parametrize(
    ("builder", "field", "value", "got"),
    [
        (trip_created, "traveler_count", "3", "string"),
        (trip_created, "traveler_count", True, "boolean"),
        (trip_created, "traveler_count", 2.5, "number"),
        (signup_v2, "marketing_opt_in", "no", "string"),
        (signup_v2, "user_id", 123, "integer"),
        (signup_v2, "email", None, "null"),
        (signup_v2, "simulated", "true", "string"),
        (itinerary_generated, "latency_ms", "fast", "string"),
        (session_started, "app_version", ["3", "2"], "array"),
    ],
)
def test_wrong_type_is_rejected_and_named(builder, field, value, got):
    errors = validate(builder(**{field: value}))
    assert any(m.startswith(f"field '{field}' must be of type ") for m in errors), errors
    assert any(m.endswith(f"got {got}") for m in errors), errors


def test_number_field_accepts_integer_value():
    assert validate(itinerary_generated(latency_ms=800)) == []


def test_enum_value_outside_catalog_is_rejected():
    errors = validate(signup_v2(signup_method="facebook"))
    assert errors == [
        'field \'signup_method\' must be one of ["email", "google", "apple"], got "facebook"'
    ]


def test_simulated_flag_must_be_true():
    errors = validate(signup_v2(simulated=False))
    assert errors == ["field 'simulated' must be one of [true], got false"]


@pytest.mark.parametrize(
    ("builder", "field", "value", "fmt"),
    [
        (signup_v2, "occurred_at", "2026-11-10 08:15:00Z", "date-time"),
        (signup_v2, "occurred_at", "2026-11-10T08:15:00", "date-time"),
        (signup_v2, "occurred_at", "2026-13-10T08:15:00Z", "date-time"),
        (signup_v2, "occurred_at", "yesterday", "date-time"),
        (trip_created, "received_at", "2026-11-10", "date-time"),
        (trip_created, "start_date", "2026-02-30", "date"),
        (trip_created, "end_date", "2026-12-24T00:00:00Z", "date"),
        (signup_v2, "email", "not-an-email", "email"),
        (trip_shared, "recipient_email", "binh@", "email"),
    ],
)
def test_bad_format_is_rejected_and_named(builder, field, value, fmt):
    assert validate(builder(**{field: value})) == [f"field '{field}' is not a valid {fmt}"]


@pytest.mark.parametrize(
    "value",
    ["2026-11-10T08:15:00Z", "2026-11-10T08:15:00+07:00", "2026-11-10T08:15:00.123456+00:00"],
)
def test_date_time_with_offset_is_accepted(value):
    assert validate(signup_v2(occurred_at=value)) == []


def test_unexpected_field_is_rejected():
    assert validate(signup_v2(utm_source="ads")) == ["unexpected field 'utm_source'"]


def test_v1_payload_labelled_v2_is_rejected():
    errors = validate(signup_v1(event_version=2))
    assert "missing required field 'marketing_opt_in'" in errors
    assert "unexpected field 'country'" in errors


def test_v2_field_in_v1_event_is_rejected():
    errors = validate(signup_v1(marketing_opt_in=True))
    assert errors == ["unexpected field 'marketing_opt_in'"]


# --- validate: unknown name / version -----------------------------------------


def test_unknown_event_name_is_rejected():
    assert validate(envelope("trip_deleted", 1)) == ["unknown event_name 'trip_deleted'"]


def test_undeclared_event_version_is_rejected():
    errors = validate(signup_v2(event_version=3))
    assert errors == ["event_version 3 is not declared for event 'user_signed_up' (declared: 1, 2)"]


@pytest.mark.parametrize(
    ("value", "got"),
    [("2", "string"), (True, "boolean"), (2.0, "number"), (None, "null")],
)
def test_event_version_must_be_an_integer(value, got):
    errors = validate(signup_v2(event_version=value))
    assert errors == [f"field 'event_version' must be of type integer, got {got}"]


def test_event_name_must_be_a_string():
    errors = validate(signup_v2(event_name=["user_signed_up"]))
    assert errors == ["field 'event_name' must be of type string, got array"]


@pytest.mark.parametrize("value", [None, [], "user_signed_up", 3])
def test_non_object_event_is_rejected(value):
    errors = validate(value)
    assert len(errors) == 1
    assert errors[0].startswith("event must be an object, got ")


@pytest.mark.parametrize(
    "event",
    [
        {},
        {1: 2, "event_name": None},
        {**signup_v2(), 1: "x"},
        signup_v2(email=object()),
        signup_v2(email=float("nan")),
        signup_v2(event_version=10**30),
    ],
)
def test_arbitrary_dict_never_raises(event):
    errors = validate(event)
    assert isinstance(errors, list)
    assert errors
    assert all(isinstance(m, str) for m in errors)


# --- dedup key ----------------------------------------------------------------


def test_dedup_key_uses_event_id_when_present():
    assert dedup_key(signup_v2()) == "event_id:evt-0001"


def test_same_event_id_is_duplicate_even_if_payload_differs():
    assert dedup_key(trip_created(destination="Hue")) == dedup_key(trip_created())


def test_different_event_id_gives_different_key():
    assert dedup_key(trip_created(event_id="evt-0002")) != dedup_key(trip_created())


@pytest.mark.parametrize("event_id", [None, ""])
def test_missing_or_empty_event_id_falls_back_to_hash(event_id):
    event = trip_created(event_id=event_id)
    key = dedup_key(event)
    assert key.startswith("sha256:")
    assert len(key) == len("sha256:") + 64
    assert key == dedup_key(without(trip_created(), "event_id"))


def test_hash_key_matches_documented_serialisation():
    event = without(trip_shared(), "event_id")
    payload = json.dumps(
        {
            "event_name": "trip_shared",
            "fields": {
                "channel": "email",
                "occurred_at": "2026-11-10T08:15:00Z",
                "trip_id": "t-001",
                "user_id": "u-001",
            },
        },
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )
    assert dedup_key(event) == "sha256:" + hashlib.sha256(payload.encode("utf-8")).hexdigest()


def test_hash_key_only_depends_on_dedup_fields():
    a = without(trip_created(destination="Da Nang"), "event_id")
    b = without(trip_created(destination="Hue", occurred_at="2026-11-11T00:00:00Z"), "event_id")
    assert dedup_key(a) == dedup_key(b)


def test_hash_key_differs_when_dedup_field_differs():
    a = without(trip_created(trip_id="t-001"), "event_id")
    b = without(trip_created(trip_id="t-002"), "event_id")
    assert dedup_key(a) != dedup_key(b)


def test_hash_key_ignores_event_version():
    a = without(signup_v1(), "event_id")
    b = without(signup_v2(), "event_id")
    assert dedup_key(a) == dedup_key(b)


@pytest.mark.parametrize("builder", [trip_shared, signup_v2])
@pytest.mark.parametrize("drop_id", [True, False])
def test_key_does_not_depend_on_dict_key_order(builder, drop_id):
    event = builder()
    if drop_id:
        event = without(event, "event_id")
    reversed_event = dict(reversed(list(event.items())))
    assert list(reversed_event) != list(event)
    assert dedup_key(reversed_event) == dedup_key(event)


def test_dedup_key_for_unknown_event_without_id_raises():
    with pytest.raises(EventSpecError, match="trip_deleted"):
        dedup_key(without(envelope("trip_deleted", 1), "event_id"))


def test_dedup_key_missing_dedup_field_raises():
    with pytest.raises(EventSpecError, match="trip_id"):
        dedup_key(without(trip_created(), "event_id", "trip_id"))


def test_find_duplicates_keeps_first_and_reports_later_copies():
    a = trip_created(event_id="evt-a")
    a_again = trip_created(event_id="evt-a", destination="Hue")
    c = without(trip_created(trip_id="t-009"), "event_id")
    c_again = without(trip_created(trip_id="t-009", destination="Hue"), "event_id")
    a_third = signup_v2(event_id="evt-a")
    unique = trip_created(event_id="evt-z")
    events = [a, a_again, c, c_again, a_third, unique]

    dups = find_duplicates(events)

    assert [(d.index, d.first_index) for d in dups] == [(1, 0), (3, 2), (4, 0)]
    assert [d.key for d in dups] == [dedup_key(a), dedup_key(c), dedup_key(a)]
    assert all(isinstance(d, Duplicate) for d in dups)


def test_find_duplicates_without_duplicates_is_empty():
    events = [trip_created(event_id=f"evt-{i}") for i in range(5)]
    assert find_duplicates(events) == []


def test_find_duplicates_accepts_any_iterable():
    events = (trip_created(event_id="evt-x") for _ in range(3))
    assert [(d.index, d.first_index) for d in find_duplicates(events)] == [(1, 0), (2, 0)]


# --- PII ----------------------------------------------------------------------


@pytest.mark.parametrize(
    ("name", "version", "expected"),
    [
        ("user_signed_up", 1, ("email",)),
        ("user_signed_up", 2, ("email",)),
        ("trip_shared", 1, ("recipient_email",)),
        ("session_started", 1, ()),
        ("trip_created", 1, ()),
        ("itinerary_generated", 1, ()),
    ],
)
def test_pii_fields(name, version, expected):
    assert pii_fields(name, version) == expected


def test_pii_fields_unknown_event_raises():
    with pytest.raises(EventSpecError, match="trip_deleted"):
        pii_fields("trip_deleted", 1)


def test_pii_fields_undeclared_version_raises():
    with pytest.raises(EventSpecError, match="3"):
        pii_fields("user_signed_up", 3)


# --- JSON Schema --------------------------------------------------------------


def test_every_generated_schema_is_valid_draft_2020_12():
    catalog = load_catalog()
    count = 0
    for name, spec in catalog.events.items():
        for version in spec.versions:
            schema = json_schema(name, version)
            assert schema["$schema"] == "https://json-schema.org/draft/2020-12/schema"
            Draft202012Validator.check_schema(schema)
            assert ENVELOPE_REQUIRED <= set(schema["required"])
            count += 1
    assert count == 6


def test_schema_required_matches_catalog():
    base = ENVELOPE_REQUIRED | {"email", "signup_method"}
    assert set(json_schema("user_signed_up", 1)["required"]) == base
    assert set(json_schema("user_signed_up", 2)["required"]) == base | {"marketing_opt_in"}
    assert set(json_schema("trip_created", 1)["required"]) == ENVELOPE_REQUIRED | {
        "trip_id",
        "destination",
        "start_date",
        "end_date",
        "traveler_count",
    }


def test_schema_properties_and_strictness():
    schema = json_schema("user_signed_up", 2)
    props = schema["properties"]
    assert schema["type"] == "object"
    assert schema["additionalProperties"] is False
    assert props["event_name"] == {"const": "user_signed_up"}
    assert props["event_version"] == {"const": 2}
    assert props["email"]["type"] == "string"
    assert props["email"]["format"] == "email"
    assert props["email"]["x-pii"] is True
    assert "x-pii" not in props["signup_method"]
    assert props["signup_method"]["enum"] == ["email", "google", "apple"]
    assert props["marketing_opt_in"]["type"] == "boolean"
    assert props["occurred_at"]["format"] == "date-time"
    assert "country" not in props
    assert "simulated" in schema["description"].lower()


def test_schema_types_for_numbers():
    props = json_schema("itinerary_generated", 1)["properties"]
    assert props["day_count"]["type"] == "integer"
    assert props["latency_ms"]["type"] == "number"


def test_json_schema_unknown_event_raises():
    with pytest.raises(EventSpecError, match="trip_deleted"):
        json_schema("trip_deleted", 1)


def test_schema_filename():
    assert schema_filename("user_signed_up", 2) == "user_signed_up.v2.schema.json"


def test_generate_all_has_one_schema_per_version():
    schemas = generate_all()
    assert set(schemas) == {
        "user_signed_up.v1.schema.json",
        "user_signed_up.v2.schema.json",
        "session_started.v1.schema.json",
        "trip_created.v1.schema.json",
        "itinerary_generated.v1.schema.json",
        "trip_shared.v1.schema.json",
    }
    assert schemas["user_signed_up.v2.schema.json"] == json_schema("user_signed_up", 2)


def test_generated_schema_accepts_and_rejects_like_validate():
    validator = Draft202012Validator(json_schema("trip_created", 1))
    assert validator.is_valid(trip_created())
    assert not validator.is_valid(without(trip_created(), "trip_id"))


# --- CLI ----------------------------------------------------------------------


def test_generate_cli_writes_sorted_json_with_trailing_newline(tmp_path):
    out = tmp_path / "nested" / "schemas"
    assert generate_main(["--out", str(out)]) == 0
    expected = generate_all()
    assert {p.name for p in out.iterdir()} == set(expected)
    for name, schema in expected.items():
        raw = (out / name).read_bytes()
        assert b"\r" not in raw
        assert raw.decode("utf-8") == json.dumps(schema, indent=2, sort_keys=True) + "\n"


def test_committed_schemas_match_catalog():
    expected = generate_all()
    committed = {p.name: p for p in SCHEMA_DIR.glob("*.schema.json")}
    assert set(committed) == set(expected), "run: python -m eventspec.generate"
    for name, path in committed.items():
        with open(path, encoding="utf-8") as f:
            assert json.load(f) == expected[name], (
                f"{name} drifted; run python -m eventspec.generate"
            )
