"""PII governance checks: column labels against the policy, raw-email scan of built schemas.

Policy: ``transform/governance.yml`` (PII classes, layers, roles); see docs/governance.md.

- ``check_labels(manifest, policy)``: every column of every dbt model carries a known
  ``meta.pii`` class, no class forbidden in the model's layer, and every role only reads classes
  it may see raw unless the column is ``masked: true``. With the built columns
  (``check --db``), a column present in the relation but not declared in yml is an error too.
- ``scan_raw_pii(con, schemas)``: finds columns holding raw email addresses in any table or view
  of the given schemas, including inside arrays, structs and maps. Findings carry counts only,
  never the values.

CLI (exit 0 clean, 1 findings, 2 bad arguments / missing file)::

    python -m repopulse.governance scan --db ci.duckdb --schema main_gold --schema main_public
    python -m repopulse.governance check --manifest transform/target/manifest.json \
        --policy transform/governance.yml [--db ci.duckdb]
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path

# Same pattern as the dbt generic test transform/tests/generic/no_raw_email.sql (a test keeps
# them equal). A 64-hex salted hash or a GitHub login never matches it.
EMAIL_PATTERN = r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}"

# Column types that cannot hold text, at any nesting depth: skipped by the scan. Everything else
# (VARCHAR, VARCHAR[], STRUCT, MAP, JSON, UNION, ENUM, BLOB, ...) is serialised and searched.
_NON_TEXT_TYPE = re.compile(
    r"(BOOLEAN|TINYINT|SMALLINT|INTEGER|BIGINT|HUGEINT|UTINYINT|USMALLINT|UINTEGER|UBIGINT"
    r"|UHUGEINT|FLOAT|DOUBLE|DECIMAL\(\d+,\s*\d+\)|DATE|TIME|TIMESTAMP|TIMESTAMP WITH TIME ZONE"
    r"|TIMESTAMP_S|TIMESTAMP_MS|TIMESTAMP_NS|TIME WITH TIME ZONE|INTERVAL|UUID)(\[\d*\])*"
)

REQUIRED_POLICY_KEYS = ("pii_classes", "layers", "roles")


class PolicyError(ValueError):
    """The policy file is missing, unreadable or inconsistent."""


@dataclass(frozen=True)
class Finding:
    """A column holding raw email values. ``rows`` = number of rows that match, no values."""

    schema: str
    table: str
    column: str
    data_type: str
    rows: int


@dataclass(frozen=True)
class LabelError:
    """A model column that breaks the policy (``column`` is ``*`` for model-level errors)."""

    model: str
    column: str
    message: str


# ---------------------------------------------------------------- policy


def load_policy(path: str | Path) -> dict:
    """Load and validate the policy YAML; raises PolicyError on any inconsistency."""
    import yaml  # dev/dbt dependency; only needed for `check`

    path = Path(path)
    try:
        policy = yaml.safe_load(path.read_text(encoding="utf-8"))
    except OSError as exc:
        raise PolicyError(f"cannot read policy {path}: {exc}") from exc
    if not isinstance(policy, dict):
        raise PolicyError(f"policy {path} is not a mapping")
    for key in REQUIRED_POLICY_KEYS:
        if not isinstance(policy.get(key), dict) or not policy[key]:
            raise PolicyError(f"policy {path}: '{key}' must be a non-empty mapping")
    classes = set(policy["pii_classes"])
    layers = set(policy["layers"])
    for name, layer in policy["layers"].items():
        unknown = set((layer or {}).get("forbidden_classes", [])) - classes
        if unknown:
            raise PolicyError(f"layer {name}: unknown classes {sorted(unknown)}")
    for name, role in policy["roles"].items():
        role = role or {}
        for key in ("read", "write"):
            unknown = set(role.get(key, [])) - layers
            if unknown:
                raise PolicyError(f"role {name}: {key} unknown layers {sorted(unknown)}")
        unknown = set(role.get("raw_classes", [])) - classes
        if unknown:
            raise PolicyError(f"role {name}: raw_classes unknown classes {sorted(unknown)}")
    return policy


# ---------------------------------------------------------------- labels


def _column_meta(column: Mapping) -> dict:
    """meta of a manifest column: ``config.meta`` (dbt >= 1.10) merged over top-level ``meta``."""
    meta = dict(column.get("meta") or {})
    meta.update((column.get("config") or {}).get("meta") or {})
    return meta


def check_labels(
    manifest: Mapping,
    policy: Mapping,
    actual_columns: Mapping[tuple[str, str], Sequence[str]] | None = None,
) -> list[LabelError]:
    """Errors of every dbt model of ``manifest`` against ``policy`` (empty list = compliant).

    ``actual_columns``: optional ``{(db schema, relation name): [column, ...]}`` of the built
    relations; columns there but missing from the yml are reported as unlabelled.
    """
    classes = set(policy["pii_classes"])
    layers = policy["layers"]
    roles = policy["roles"]
    errors: list[LabelError] = []
    for node in manifest.get("nodes", {}).values():
        if node.get("resource_type") != "model":
            continue
        model = node["name"]
        layer = (node.get("config") or {}).get("schema")
        if layer not in layers:
            errors.append(LabelError(model, "*", f"schema {layer!r} is not a layer of the policy"))
            continue
        forbidden = set(layers[layer].get("forbidden_classes", []))
        readers = [r for r, spec in roles.items() if layer in (spec or {}).get("read", [])]
        columns = node.get("columns") or {}
        for name, column in columns.items():
            meta = _column_meta(column)
            pii = meta.get("pii")
            if pii is None:
                errors.append(LabelError(model, name, "missing meta.pii label"))
                continue
            if pii not in classes:
                errors.append(LabelError(model, name, f"unknown pii class {pii!r}"))
                continue
            if pii in forbidden:
                errors.append(LabelError(model, name, f"class {pii} not allowed in layer {layer}"))
                continue
            if meta.get("masked") is True:
                continue
            for role in readers:
                if pii not in roles[role].get("raw_classes", []):
                    errors.append(
                        LabelError(
                            model,
                            name,
                            f"role {role} reads {layer} but may not see {pii} unmasked "
                            "(mask it with mask_pii and set meta.masked: true)",
                        )
                    )
        if actual_columns is not None:
            key = (node.get("schema"), node.get("alias") or model)
            built = actual_columns.get(key)
            if built is None:
                errors.append(LabelError(model, "*", f"relation {key[0]}.{key[1]} not built"))
                continue
            declared = {c.lower() for c in columns}
            for name in built:
                if name.lower() not in declared:
                    errors.append(
                        LabelError(model, name, "built column not declared in yml (no meta.pii)")
                    )
    return errors


def relation_columns(con, schemas: Iterable[str]) -> dict[tuple[str, str], list[str]]:
    """``{(schema, relation): [columns]}`` of the given schemas (information_schema order)."""
    schemas = list(schemas)
    if not schemas:
        return {}
    marks = ", ".join("?" for _ in schemas)
    rows = con.execute(
        "SELECT table_schema, table_name, column_name FROM information_schema.columns "
        f"WHERE table_schema IN ({marks}) ORDER BY table_schema, table_name, ordinal_position",
        schemas,
    ).fetchall()
    out: dict[tuple[str, str], list[str]] = {}
    for schema, table, column in rows:
        out.setdefault((schema, table), []).append(column)
    return out


# ---------------------------------------------------------------- scan


def _ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _text_expr(column: str, data_type: str) -> str | None:
    """SQL text to search for ``column``, or None when the type cannot hold text."""
    if data_type.upper() == "VARCHAR":
        return _ident(column)
    if _NON_TEXT_TYPE.fullmatch(data_type.upper()):
        return None
    # arrays, structs, maps, unions, enums, json, blobs: serialise the whole value to JSON text
    return f"CAST(to_json({_ident(column)}) AS VARCHAR)"


def scan_raw_pii(con, schemas: Iterable[str]) -> list[Finding]:
    """Columns of tables/views in ``schemas`` holding raw email values (counts, no values)."""
    schemas = list(schemas)
    if not schemas:
        return []
    marks = ", ".join("?" for _ in schemas)
    cols = con.execute(
        "SELECT table_schema, table_name, column_name, data_type FROM information_schema.columns "
        f"WHERE table_schema IN ({marks}) ORDER BY table_schema, table_name, ordinal_position",
        schemas,
    ).fetchall()
    findings: list[Finding] = []
    for schema, table, column, data_type in cols:
        expr = _text_expr(column, data_type)
        if expr is None:
            continue
        rows = con.execute(
            f"SELECT count(*) FROM {_ident(schema)}.{_ident(table)} "
            f"WHERE regexp_matches({expr}, ?)",
            [EMAIL_PATTERN],
        ).fetchone()[0]
        if rows:
            findings.append(Finding(schema, table, column, data_type, rows))
    return findings


def _existing_schemas(con) -> set[str]:
    rows = con.execute("SELECT schema_name FROM information_schema.schemata").fetchall()
    return {r[0] for r in rows}


# ---------------------------------------------------------------- CLI


def _connect(db: str):
    import duckdb

    if not Path(db).is_file():
        raise FileNotFoundError(db)
    return duckdb.connect(db, read_only=True)


def _cmd_scan(args: argparse.Namespace) -> int:
    try:
        con = _connect(args.db)
    except FileNotFoundError:
        print(f"error: database not found: {args.db}", file=sys.stderr)
        return 2
    try:
        missing = sorted(set(args.schema) - _existing_schemas(con))
        if missing:
            print(f"error: schema not found in {args.db}: {', '.join(missing)}", file=sys.stderr)
            return 2
        findings = scan_raw_pii(con, args.schema)
    finally:
        con.close()
    if not findings:
        print(f"OK: no raw email values in {', '.join(args.schema)}")
        return 0
    print("Raw email values found (values not shown):")
    print(f"{'schema':<20} {'relation':<36} {'column':<32} rows")
    for f in findings:
        print(f"{f.schema:<20} {f.table:<36} {f.column:<32} {f.rows}")
    return 1


def _cmd_check(args: argparse.Namespace) -> int:
    manifest_path = Path(args.manifest)
    if not manifest_path.is_file():
        print(f"error: manifest not found: {manifest_path} (run dbt parse/build)", file=sys.stderr)
        return 2
    try:
        policy = load_policy(args.policy)
    except PolicyError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    actual = None
    if args.db:
        try:
            con = _connect(args.db)
        except FileNotFoundError:
            print(f"error: database not found: {args.db}", file=sys.stderr)
            return 2
        try:
            schemas = {
                n["schema"]
                for n in manifest.get("nodes", {}).values()
                if n.get("resource_type") == "model"
            }
            actual = relation_columns(con, sorted(schemas))
        finally:
            con.close()
    errors = check_labels(manifest, policy, actual)
    if not errors:
        print(f"OK: every model column labelled and allowed by {args.policy}")
        return 0
    print("Policy violations:")
    for e in errors:
        print(f"  {e.model}.{e.column}: {e.message}")
    return 1


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m repopulse.governance",
        description="PII governance checks (exit 1 on findings)",
    )
    sub = parser.add_subparsers(dest="command", required=True)
    scan = sub.add_parser("scan", help="search built schemas for raw email values")
    scan.add_argument("--db", required=True, help="DuckDB file built by dbt")
    scan.add_argument("--schema", action="append", required=True, help="schema to scan (repeat)")
    scan.set_defaults(func=_cmd_scan)
    check = sub.add_parser("check", help="check dbt column labels against the policy")
    check.add_argument("--manifest", required=True, help="dbt target/manifest.json")
    check.add_argument("--policy", default="transform/governance.yml", help="policy YAML")
    check.add_argument("--db", help="optional DuckDB file: also flag built columns missing in yml")
    check.set_defaults(func=_cmd_check)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
