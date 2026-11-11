"""eventspec.load_catalog: malformed catalogs are rejected with a clear message."""

import copy

import pytest
import yaml

from eventspec import CatalogError, dedup_key, json_schema, load_catalog, pii_fields, validate

BASE_ENVELOPE = {
    "fields": {
        "event_id": {"type": "string", "required": False, "pii": False},
        "event_name": {"type": "string", "required": True, "pii": False},
        "event_version": {"type": "integer", "required": True, "pii": False},
        "occurred_at": {
            "type": "string",
            "format": "date-time",
            "required": True,
            "pii": False,
        },
        "user_id": {"type": "string", "required": True, "pii": False},
    }
}
BASE_EVENT = {
    "name": "ping",
    "description": "simulated test event",
    "dedup_fields": ["user_id"],
    "versions": [
        {
            "version": 1,
            "fields": {
                "note": {"type": "string", "required": False, "pii": False},
                "contact": {
                    "type": "string",
                    "format": "email",
                    "required": True,
                    "pii": True,
                },
            },
        }
    ],
}


def write_yaml(path, data):
    path.write_text(yaml.safe_dump(data, sort_keys=False), encoding="utf-8")


def write_catalog(directory, envelope=None, events=None):
    directory.mkdir(parents=True, exist_ok=True)
    write_yaml(directory / "_envelope.yaml", BASE_ENVELOPE if envelope is None else envelope)
    for event in [BASE_EVENT] if events is None else events:
        write_yaml(directory / f"{event['name']}.yaml", event)
    return directory


def ping(**overrides):
    event = {
        "event_name": "ping",
        "event_version": 1,
        "occurred_at": "2026-11-10T08:15:00Z",
        "user_id": "u-1",
        "contact": "a@example.com",
    }
    event.update(overrides)
    return event


# --- a valid custom catalog ---------------------------------------------------


def test_custom_catalog_loads_and_is_used(tmp_path):
    catalog = load_catalog(write_catalog(tmp_path / "cat"))
    assert set(catalog.events) == {"ping"}
    assert validate(ping(), catalog=catalog) == []
    assert validate(without_contact(), catalog=catalog) == ["missing required field 'contact'"]
    assert pii_fields("ping", 1, catalog=catalog) == ("contact",)
    assert set(json_schema("ping", 1, catalog=catalog)["required"]) == {
        "event_name",
        "event_version",
        "occurred_at",
        "user_id",
        "contact",
    }


def without_contact():
    event = ping()
    del event["contact"]
    return event


def test_load_catalog_accepts_str_path(tmp_path):
    catalog = load_catalog(str(write_catalog(tmp_path / "cat")))
    assert set(catalog.events) == {"ping"}


def test_hash_key_includes_event_name(tmp_path):
    pong = copy.deepcopy(BASE_EVENT)
    pong["name"] = "pong"
    catalog = load_catalog(write_catalog(tmp_path / "cat", events=[BASE_EVENT, pong]))
    a = dedup_key(ping(), catalog=catalog)
    b = dedup_key(ping(event_name="pong"), catalog=catalog)
    assert a.startswith("sha256:") and b.startswith("sha256:")
    assert a != b


# --- malformed event files ----------------------------------------------------


def _drop(key):
    def mutate(event):
        del event[key]

    return mutate


def _set(key, value):
    def mutate(event):
        event[key] = value

    return mutate


def _field(name, **spec):
    def mutate(event):
        event["versions"][0]["fields"][name] = spec

    return mutate


def _field_drop(name, key):
    def mutate(event):
        del event["versions"][0]["fields"][name][key]

    return mutate


def _add_version(version, fields=None):
    def mutate(event):
        v = copy.deepcopy(event["versions"][0])
        v["version"] = version
        if fields is not None:
            v["fields"] = fields
        event["versions"].append(v)

    return mutate


@pytest.mark.parametrize(
    ("mutate", "match"),
    [
        (_drop("versions"), "versions"),
        (_drop("dedup_fields"), "dedup_fields"),
        (_set("versions", []), "versions"),
        (_set("versions", "v1"), "versions"),
        (_set("dedup_fields", []), "dedup_fields"),
        (_set("dedup_fields", ["missing_field"]), "missing_field"),
        (_set("dedup_fields", ["note"]), "note"),
        (_set("dedup_field", ["user_id"]), "dedup_field"),
        (_add_version(1), "duplicate version 1"),
        (
            _add_version(0, {"note2": {"type": "string", "required": False, "pii": False}}),
            "invalid version 0",
        ),
        (_add_version("2"), "invalid version '2'"),
        (_add_version(2), "identical"),
        (_field_drop("note", "type"), "type"),
        (_field_drop("note", "pii"), "pii"),
        (_field_drop("note", "required"), "required"),
        (_field("note", type="uuid", required=False, pii=False), "uuid"),
        (_field("note", type="string", required="yes", pii=False), "required"),
        (_field("note", type="string", required=False, pii="no"), "pii"),
        (_field("note", type="string", required=False, pii=False, nullable=True), "nullable"),
        (_field("note", type="integer", required=False, pii=False, format="email"), "format"),
        (_field("note", type="string", required=False, pii=False, format="phone"), "phone"),
        (_field("note", type="string", required=False, pii=False, enum=[]), "enum"),
        (_field("note", type="string", required=False, pii=False, enum=[1, 2]), "enum"),
        (_field("Note", type="string", required=False, pii=False), "Note"),
        (_field("user_id", type="string", required=True, pii=False), "user_id"),
    ],
)
def test_malformed_event_is_rejected(tmp_path, mutate, match):
    event = copy.deepcopy(BASE_EVENT)
    mutate(event)
    directory = write_catalog(tmp_path / "cat", events=[event])
    with pytest.raises(CatalogError, match=match):
        load_catalog(directory)


def test_versions_must_increase(tmp_path):
    event = copy.deepcopy(BASE_EVENT)
    v2 = copy.deepcopy(event["versions"][0])
    v2["version"] = 2
    v2["fields"]["extra"] = {"type": "boolean", "required": False, "pii": False}
    event["versions"] = [v2, event["versions"][0]]
    with pytest.raises(CatalogError, match="increasing"):
        load_catalog(write_catalog(tmp_path / "cat", events=[event]))


def test_event_name_must_match_file_name(tmp_path):
    directory = write_catalog(tmp_path / "cat")
    (directory / "ping.yaml").rename(directory / "pong.yaml")
    with pytest.raises(CatalogError, match="pong.yaml"):
        load_catalog(directory)


def test_invalid_event_name_is_rejected(tmp_path):
    directory = write_catalog(tmp_path / "cat", events=[])
    event = copy.deepcopy(BASE_EVENT)
    event["name"] = "Ping-Event"
    write_yaml(directory / "Ping-Event.yaml", event)
    with pytest.raises(CatalogError, match="Ping-Event"):
        load_catalog(directory)


def test_yaml_syntax_error_names_the_file(tmp_path):
    directory = write_catalog(tmp_path / "cat")
    (directory / "ping.yaml").write_text("name: ping\nversions: [\n", encoding="utf-8")
    with pytest.raises(CatalogError, match="ping.yaml"):
        load_catalog(directory)


def test_top_level_must_be_a_mapping(tmp_path):
    directory = write_catalog(tmp_path / "cat")
    (directory / "ping.yaml").write_text("- just\n- a list\n", encoding="utf-8")
    with pytest.raises(CatalogError, match="ping.yaml"):
        load_catalog(directory)


# --- malformed envelope / directory -------------------------------------------


def test_missing_envelope_is_rejected(tmp_path):
    directory = write_catalog(tmp_path / "cat")
    (directory / "_envelope.yaml").unlink()
    with pytest.raises(CatalogError, match="_envelope.yaml"):
        load_catalog(directory)


@pytest.mark.parametrize("field", ["event_id", "event_name", "event_version"])
def test_envelope_must_declare_core_fields(tmp_path, field):
    envelope = copy.deepcopy(BASE_ENVELOPE)
    del envelope["fields"][field]
    with pytest.raises(CatalogError, match=field):
        load_catalog(write_catalog(tmp_path / "cat", envelope=envelope))


def test_envelope_field_spec_is_checked(tmp_path):
    envelope = copy.deepcopy(BASE_ENVELOPE)
    envelope["fields"]["user_id"]["type"] = "str"
    with pytest.raises(CatalogError, match="str"):
        load_catalog(write_catalog(tmp_path / "cat", envelope=envelope))


def test_missing_directory_is_rejected(tmp_path):
    with pytest.raises(CatalogError, match="not found"):
        load_catalog(tmp_path / "nope")


def test_catalog_without_events_is_rejected(tmp_path):
    with pytest.raises(CatalogError, match="no event"):
        load_catalog(write_catalog(tmp_path / "cat", events=[]))
