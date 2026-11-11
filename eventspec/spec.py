"""Load and check the event catalog (YAML) of the simulated trip-planner app."""

from __future__ import annotations

import functools
import re
from dataclasses import dataclass
from pathlib import Path

import yaml

CATALOG_DIR = Path(__file__).parent / "catalog"
SCHEMA_DIR = Path(__file__).parent / "schemas"
ENVELOPE_FILE = "_envelope.yaml"

TYPES = {"string": str, "integer": int, "number": (int, float), "boolean": bool}
FORMATS = {"date-time", "date", "email"}
CORE_ENVELOPE_FIELDS = ("event_id", "event_name", "event_version")

_NAME_RE = re.compile(r"[a-z][a-z0-9_]*")
_FIELD_KEYS = {"type", "required", "pii", "enum", "format", "description"}
_EVENT_KEYS = {"name", "description", "dedup_fields", "versions"}
_VERSION_KEYS = {"version", "fields"}


class EventSpecError(ValueError):
    """An event or lookup does not fit the catalog."""


class CatalogError(EventSpecError):
    """The catalog itself is malformed."""


@dataclass(frozen=True)
class FieldSpec:
    name: str
    type: str
    required: bool
    pii: bool
    enum: tuple | None = None
    format: str | None = None
    description: str | None = None


@dataclass(frozen=True)
class EventVersion:
    version: int
    fields: tuple[FieldSpec, ...]  # envelope fields first, then event-specific fields

    def field_names(self) -> tuple[str, ...]:
        return tuple(f.name for f in self.fields)


@dataclass(frozen=True, eq=False)
class EventSpec:
    name: str
    description: str
    dedup_fields: tuple[str, ...]
    versions: dict[int, EventVersion]

    def version(self, version: int) -> EventVersion:
        if (
            isinstance(version, bool)
            or not isinstance(version, int)
            or version not in self.versions
        ):
            declared = ", ".join(str(v) for v in self.versions)
            raise EventSpecError(
                f"event_version {version} is not declared for event '{self.name}' "
                f"(declared: {declared})"
            )
        return self.versions[version]


@dataclass(frozen=True, eq=False)
class Catalog:
    """Identity-hashable so validators can be cached per catalog."""

    envelope: tuple[FieldSpec, ...]
    events: dict[str, EventSpec]

    def event(self, name: object) -> EventSpec:
        if not isinstance(name, str) or name not in self.events:
            raise EventSpecError(f"unknown event_name {_quote(name)}")
        return self.events[name]


def _quote(value: object) -> str:
    return f"'{value}'" if isinstance(value, str) else repr(value)


def _parse_field(where: str, name: object, spec: object) -> FieldSpec:
    if not isinstance(name, str) or not _NAME_RE.fullmatch(name):
        raise CatalogError(f"{where}: field name {_quote(name)} must be snake_case")
    where = f"{where}: field '{name}'"
    if not isinstance(spec, dict):
        raise CatalogError(f"{where} must be a mapping")
    unknown = sorted(set(spec) - _FIELD_KEYS)
    if unknown:
        raise CatalogError(f"{where} has unknown key(s) {', '.join(map(str, unknown))}")
    for key in ("type", "required", "pii"):
        if key not in spec:
            raise CatalogError(f"{where} is missing '{key}'")
    ftype = spec["type"]
    if ftype not in TYPES:
        raise CatalogError(f"{where}: unknown type {_quote(ftype)} (allowed: {', '.join(TYPES)})")
    for key in ("required", "pii"):
        if not isinstance(spec[key], bool):
            raise CatalogError(f"{where}: '{key}' must be true or false")
    fmt = spec.get("format")
    if fmt is not None:
        if fmt not in FORMATS:
            raise CatalogError(f"{where}: unknown format {_quote(fmt)}")
        if ftype != "string":
            raise CatalogError(f"{where}: format only applies to type string")
    enum = spec.get("enum")
    if enum is not None:
        if not isinstance(enum, list) or not enum:
            raise CatalogError(f"{where}: enum must be a non-empty list")
        if not all(_has_type(v, ftype) for v in enum):
            raise CatalogError(f"{where}: enum values must be of type {ftype}")
        enum = tuple(enum)
    description = spec.get("description")
    return FieldSpec(name, ftype, spec["required"], spec["pii"], enum, fmt, description)


def _has_type(value: object, ftype: str) -> bool:
    if isinstance(value, bool):
        return ftype == "boolean"
    return isinstance(value, TYPES[ftype])


def _parse_fields(where: str, fields: object) -> tuple[FieldSpec, ...]:
    if not isinstance(fields, dict):
        raise CatalogError(f"{where}: 'fields' must be a mapping")
    return tuple(_parse_field(where, name, spec) for name, spec in fields.items())


def _read_yaml(path: Path) -> dict:
    try:
        with open(path, encoding="utf-8") as f:
            data = yaml.safe_load(f)
    except yaml.YAMLError as exc:
        raise CatalogError(f"{path.name}: invalid YAML: {exc}") from exc
    if not isinstance(data, dict):
        raise CatalogError(f"{path.name}: top level must be a mapping")
    return data


def _parse_envelope(path: Path) -> tuple[FieldSpec, ...]:
    if not path.is_file():
        raise CatalogError(f"{path.name} not found in {path.parent}")
    data = _read_yaml(path)
    fields = _parse_fields(path.name, data.get("fields"))
    names = {f.name for f in fields}
    for core in CORE_ENVELOPE_FIELDS:
        if core not in names:
            raise CatalogError(f"{path.name}: envelope must declare '{core}'")
    return fields


def _parse_event(path: Path, envelope: tuple[FieldSpec, ...]) -> EventSpec:
    where = path.name
    data = _read_yaml(path)
    unknown = sorted(set(data) - _EVENT_KEYS)
    if unknown:
        raise CatalogError(f"{where}: unknown key(s) {', '.join(map(str, unknown))}")
    for key in ("name", "dedup_fields", "versions"):
        if key not in data:
            raise CatalogError(f"{where}: missing '{key}'")
    name = data["name"]
    if not isinstance(name, str) or not _NAME_RE.fullmatch(name):
        raise CatalogError(f"{where}: event name {_quote(name)} must be snake_case")
    if path.stem != name:
        raise CatalogError(f"{where}: file name must be '{name}.yaml'")

    dedup = data["dedup_fields"]
    if not isinstance(dedup, list) or not dedup or not all(isinstance(d, str) for d in dedup):
        raise CatalogError(f"{where}: dedup_fields must be a non-empty list of field names")

    raw_versions = data["versions"]
    if not isinstance(raw_versions, list) or not raw_versions:
        raise CatalogError(f"{where}: versions must be a non-empty list")
    envelope_names = {f.name for f in envelope}
    versions: dict[int, EventVersion] = {}
    for raw in raw_versions:
        if not isinstance(raw, dict) or set(raw) != _VERSION_KEYS:
            raise CatalogError(f"{where}: each version needs exactly 'version' and 'fields'")
        number = raw["version"]
        if isinstance(number, bool) or not isinstance(number, int) or number < 1:
            raise CatalogError(f"{where}: invalid version {_quote(number)} (integer >= 1)")
        if number in versions:
            raise CatalogError(f"{where}: duplicate version {number}")
        if versions and number < max(versions):
            raise CatalogError(f"{where}: versions must be listed in increasing order")
        vwhere = f"{where} v{number}"
        own = _parse_fields(vwhere, raw["fields"])
        for field in own:
            if field.name in envelope_names:
                raise CatalogError(f"{vwhere}: field '{field.name}' is an envelope field")
        for prev in versions.values():
            if prev.fields[len(envelope) :] == own:
                raise CatalogError(
                    f"{vwhere}: fields identical to v{prev.version}; "
                    "bump the version only when the schema changes"
                )
        versions[number] = EventVersion(number, envelope + own)

    for field in dedup:
        for ev in versions.values():
            spec = next((f for f in ev.fields if f.name == field), None)
            if spec is None:
                raise CatalogError(
                    f"{where}: dedup field '{field}' does not exist in v{ev.version}"
                )
            if not spec.required:
                raise CatalogError(f"{where}: dedup field '{field}' must be required")

    description = data.get("description") or ""
    return EventSpec(name, description, tuple(dedup), versions)


def load_catalog(path: str | Path | None = None) -> Catalog:
    """Read every ``*.yaml`` in ``path`` (default: the packaged catalog) and check it."""
    directory = CATALOG_DIR if path is None else Path(path)
    if not directory.is_dir():
        raise CatalogError(f"catalog directory not found: {directory}")
    envelope = _parse_envelope(directory / ENVELOPE_FILE)
    events: dict[str, EventSpec] = {}
    for file in sorted(directory.glob("*.yaml")):
        if file.name == ENVELOPE_FILE:
            continue
        spec = _parse_event(file, envelope)
        events[spec.name] = spec
    if not events:
        raise CatalogError(f"no event files in {directory}")
    return Catalog(envelope, events)


@functools.cache
def default_catalog() -> Catalog:
    """The packaged catalog, loaded once."""
    return load_catalog()


def pii_fields(event_name: str, version: int, catalog: Catalog | None = None) -> tuple[str, ...]:
    """Names of the fields flagged ``pii: true`` (silver hashes these before writing)."""
    cat = default_catalog() if catalog is None else catalog
    ev = cat.event(event_name).version(version)
    return tuple(f.name for f in ev.fields if f.pii)
