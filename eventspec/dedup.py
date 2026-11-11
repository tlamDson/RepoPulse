"""Dedup key: ``event_id`` when present, otherwise SHA-256 of the catalog's dedup fields."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable, Mapping
from dataclasses import dataclass

from eventspec.spec import Catalog, EventSpecError, default_catalog


@dataclass(frozen=True)
class Duplicate:
    index: int  # position of the later copy
    first_index: int  # position of the first event with the same key (the one kept)
    key: str


def dedup_key(event: Mapping, catalog: Catalog | None = None) -> str:
    """``event_id:<id>`` or ``sha256:<hex>`` over ``{"event_name", "fields": {dedup fields}}``.

    The hash input is JSON with sorted keys, no spaces, UTF-8, so dict key order never matters.
    ``event_version`` is not part of it: the same action re-sent under a newer version is a
    duplicate.
    """
    event_id = event.get("event_id")
    if isinstance(event_id, str) and event_id != "":
        return f"event_id:{event_id}"
    cat = default_catalog() if catalog is None else catalog
    name = event.get("event_name")
    spec = cat.event(name)
    fields = {}
    for field in spec.dedup_fields:
        if event.get(field) is None:
            raise EventSpecError(f"event '{name}' is missing dedup field '{field}'")
        fields[field] = event[field]
    payload = json.dumps(
        {"event_name": name, "fields": fields},
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )
    return "sha256:" + hashlib.sha256(payload.encode("utf-8")).hexdigest()


def find_duplicates(events: Iterable[Mapping], catalog: Catalog | None = None) -> list[Duplicate]:
    """Every event whose key was already seen; the first occurrence is kept (not reported)."""
    first_seen: dict[str, int] = {}
    duplicates = []
    for index, event in enumerate(events):
        key = dedup_key(event, catalog)
        if key in first_seen:
            duplicates.append(Duplicate(index, first_seen[key], key))
        else:
            first_seen[key] = index
    return duplicates
