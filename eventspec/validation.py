"""Check one event against the catalog; return readable error messages."""

from __future__ import annotations

import functools
import json
import re
from collections.abc import Mapping
from datetime import date, datetime

from jsonschema import Draft202012Validator, FormatChecker

from eventspec.schema_gen import json_schema
from eventspec.spec import Catalog, EventSpecError, default_catalog

_DATE_TIME_RE = re.compile(
    r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(\.[0-9]+)?(Z|[+-][0-9]{2}:[0-9]{2})"
)
_DATE_RE = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}")
_EMAIL_RE = re.compile(r"[^@\s]+@[^@\s]+\.[^@\s]+")

# Only the formats the catalog allows; jsonschema's built-in date-time check silently passes
# when the optional rfc3339-validator package is missing, so we implement our own.
FORMAT_CHECKER = FormatChecker(formats=())


@FORMAT_CHECKER.checks("date-time")
def _is_date_time(value: object) -> bool:
    if not isinstance(value, str):
        return True  # wrong type is reported by the "type" keyword
    if not _DATE_TIME_RE.fullmatch(value):
        return False
    try:
        datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return False
    return True


@FORMAT_CHECKER.checks("date")
def _is_date(value: object) -> bool:
    if not isinstance(value, str):
        return True
    if not _DATE_RE.fullmatch(value):
        return False
    try:
        date.fromisoformat(value)
    except ValueError:
        return False
    return True


@FORMAT_CHECKER.checks("email")
def _is_email(value: object) -> bool:
    return not isinstance(value, str) or _EMAIL_RE.fullmatch(value) is not None


def _json_type(value: object) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int):
        return "integer"
    if isinstance(value, float):
        return "number"
    if isinstance(value, str):
        return "string"
    if isinstance(value, list | tuple):
        return "array"
    if isinstance(value, Mapping):
        return "object"
    return type(value).__name__


def _show(value: object) -> str:
    try:
        return json.dumps(value, ensure_ascii=False)
    except (TypeError, ValueError):
        return repr(value)


@functools.cache
def _validator(catalog: Catalog, event_name: str, version: int) -> Draft202012Validator:
    schema = json_schema(event_name, version, catalog)
    return Draft202012Validator(schema, format_checker=FORMAT_CHECKER)


def _messages(error, schema: dict) -> list[str]:
    kind = error.validator
    instance = error.instance
    if not error.path:
        if kind == "required":
            return [
                f"missing required field '{name}'"
                for name in error.validator_value
                if name not in instance
            ]
        if kind == "additionalProperties":
            props = schema["properties"]
            return [f"unexpected field '{key}'" for key in instance if key not in props]
        return [error.message]
    field = ".".join(str(p) for p in error.path)
    if kind == "type":
        return [
            f"field '{field}' must be of type {error.validator_value}, got {_json_type(instance)}"
        ]
    if kind == "enum":
        return [
            f"field '{field}' must be one of {_show(list(error.validator_value))}, "
            f"got {_show(instance)}"
        ]
    if kind == "format":
        return [f"field '{field}' is not a valid {error.validator_value}"]
    return [f"field '{field}': {error.message}"]


def validate(event: object, catalog: Catalog | None = None) -> list[str]:
    """Return the problems with ``event``; an empty list means it is valid.

    Never raises for any input: non-dicts, unknown names and undeclared versions are reported
    as messages.
    """
    if not isinstance(event, Mapping):
        return [f"event must be an object, got {_json_type(event)}"]
    cat = default_catalog() if catalog is None else catalog

    if "event_name" not in event:
        return ["missing required field 'event_name'"]
    name = event["event_name"]
    if not isinstance(name, str):
        return [f"field 'event_name' must be of type string, got {_json_type(name)}"]
    if "event_version" not in event:
        return ["missing required field 'event_version'"]
    version = event["event_version"]
    if isinstance(version, bool) or not isinstance(version, int):
        return [f"field 'event_version' must be of type integer, got {_json_type(version)}"]
    try:
        cat.event(name).version(version)
    except EventSpecError as exc:
        return [str(exc)]

    validator = _validator(cat, name, version)
    messages: set[str] = set()
    for error in validator.iter_errors(dict(event)):
        messages.update(_messages(error, validator.schema))
    return sorted(messages)
