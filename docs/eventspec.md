# eventspec — event catalog of the simulated app

`eventspec/` describes the events of a **simulated** travel-planning app. There are no
real users; the data is produced by `simkit` (PR 3, see `docs/simkit.md`) following this catalog, with
known injected faults. It is **not** GH Archive data.

## Catalog format

`eventspec/catalog/`:

- `_envelope.yaml` — fields common to every event: `event_id` (optional), `event_name`,
  `event_version`, `occurred_at` (date-time with offset), `received_at` (optional), `user_id`,
  `session_id`, `simulated` (required, accepts only `true`). `event_id`, `event_name` and
  `event_version` must be declared.
- `<event>.yaml` — one event per file, the file name equals `name`. Keys: `name` (snake_case),
  `description`, `dedup_fields`, `versions`. Each version: `version` (integer >= 1) and `fields`.

Each field: `type` (`string` | `integer` | `number` | `boolean`), `required` and `pii` (must be
stated explicitly as `true`/`false`), optional `enum`, `format` (`date-time` | `date` | `email`, only for
`string`), `description`. Unknown keys are rejected. An event's field may not share a name with an envelope
field. `null` is not valid in any field; an optional field is omitted from the event entirely.

`load_catalog()` checks everything on load and raises `CatalogError` naming the wrong file and field.

## Version rules

- Changing the schema (adding, removing, changing a field) means **adding a new version**, never editing an existing one. Versions
  are listed in ascending order without duplicates; two versions with identical fields are rejected.
- The validator accepts **every declared version**; an undeclared `event_version` is rejected.
- Example: `user_signed_up` v2 replaces `country` (free text) with `country_code` and adds a required
  `marketing_opt_in`.
- The generated schema is committed at `eventspec/schemas/<event>.v<n>.schema.json`; the test
  `test_committed_schemas_match_catalog` goes red when the catalog drifts from the committed files. After editing the catalog run
  `python -m eventspec.generate`. A diff that changes the file of an old version is a sign you forgot to bump the version.

## JSON Schema and `validate`

- Draft 2020-12, `additionalProperties: false` (unknown fields are rejected, to catch silent schema changes),
  `event_name`/`event_version` are `const`, PII fields carry the annotation `x-pii: true`.
- `validate(event) -> list[str]`: empty means valid; it never raises. Messages look like
  `missing required field 'email'`, `field 'traveler_count' must be of type integer, got string`,
  `field 'occurred_at' is not a valid date-time`, `unexpected field 'utm_source'`,
  `unknown event_name 'x'`, `event_version 3 is not declared for event 'user_signed_up' (declared: 1, 2)`.
- `format` is checked by a dedicated checker (date-time must have an offset), independent of the
  `rfc3339-validator` package.

## Dedup key

`dedup_key(event)`:

1. `event_id` is a non-empty string → `event_id:<event_id>`.
2. Otherwise → `sha256:<hex>` of the JSON
   `{"event_name": ..., "fields": {<dedup_fields>: value}}` with `sort_keys=True`,
   `separators=(",", ":")`, `ensure_ascii=False`, UTF-8 encoded. Dict key order has no
   effect. `event_version` is not part of the key: resending the same action at a new version is still a duplicate.
   A missing identifying field raises `EventSpecError`.

`dedup_fields` must be required fields in every version. `find_duplicates(events)` keeps the first event and
returns `Duplicate(index, first_index, key)` for the later copies.

## PII

Fields with `pii: true` (currently `user_signed_up.email`, `trip_shared.recipient_email`) must be
hashed before silver (PR 5). Get the list with `pii_fields(event_name, version)`.

## Commands

```bash
python -m eventspec.generate               # rewrite eventspec/schemas/
python -m eventspec.generate --out <dir>   # write to another directory
```
