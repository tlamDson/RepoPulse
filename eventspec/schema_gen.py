"""Turn a catalog (event, version) into a JSON Schema (draft 2020-12)."""

from __future__ import annotations

from eventspec.spec import Catalog, FieldSpec, default_catalog

DRAFT = "https://json-schema.org/draft/2020-12/schema"


def schema_filename(event_name: str, version: int) -> str:
    return f"{event_name}.v{version}.schema.json"


def _property(field: FieldSpec, event_name: str, version: int) -> dict:
    if field.name == "event_name":
        return {"const": event_name}
    if field.name == "event_version":
        return {"const": version}
    prop: dict = {"type": field.type}
    if field.format is not None:
        prop["format"] = field.format
    if field.enum is not None:
        prop["enum"] = list(field.enum)
    if field.description:
        prop["description"] = field.description
    if field.pii:
        prop["x-pii"] = True
    return prop


def json_schema(event_name: str, version: int, catalog: Catalog | None = None) -> dict:
    """JSON Schema for one declared version; unknown fields are not allowed."""
    cat = default_catalog() if catalog is None else catalog
    spec = cat.event(event_name)
    ev = spec.version(version)
    return {
        "$schema": DRAFT,
        "title": f"{event_name} v{version}",
        "description": f"Simulated app event. {spec.description}".strip(),
        "type": "object",
        "properties": {f.name: _property(f, event_name, version) for f in ev.fields},
        "required": [f.name for f in ev.fields if f.required],
        "additionalProperties": False,
    }


def generate_all(catalog: Catalog | None = None) -> dict[str, dict]:
    """``{file name: schema}`` for every (event, version) in the catalog."""
    cat = default_catalog() if catalog is None else catalog
    return {
        schema_filename(name, version): json_schema(name, version, cat)
        for name, spec in cat.events.items()
        for version in spec.versions
    }
