"""PII governance: policy, column labels, masking of the public schema, raw-email scan.

Policy = ``transform/governance.yml`` (PII classes, layers, roles). ``repopulse.governance``
checks dbt's manifest against it (``check``) and scans built schemas for raw email values
(``scan``). dbt itself refuses unhashed silver (generic test ``hashed_pii`` on
``stg_gharchive_events``) and raw emails in gold/public (generic test ``no_raw_email``). The
``public`` schema (``main_public`` on DuckDB) re-exposes gold with ``actor_login``, ``actor_id``
and ``user_key`` masked by ``mask_pii`` = the silver formula ``repopulse.spark.pii.hash_pii``.

dbt runs as a subprocess. The shared clean build is the session fixture ``built`` (conftest.py);
the cases that must fail build their own copy in tmp. Salts here are obvious test strings, never
secrets; ``PII_HASH_SALT`` is removed from the dbt environment unless a test sets it.
"""

from __future__ import annotations

import importlib
import json
import re
import subprocess
import sys

import duckdb
import pytest
import yaml

from repopulse.spark.pii import hash_pii

pytest.importorskip("dbt.cli.main", reason="dbt extra not installed: pip install -e '.[dbt]'")
pytest.importorskip("dbt.adapters.duckdb", reason="dbt-duckdb not installed")

from dbt_helpers import (  # noqa: E402
    GOLD_SCHEMA,
    MARTS,
    PROJECT,
    PUBLIC_SCHEMA,
    ROOT,
    ci_salt,
    copy_project,
    output,
    q,
    require_ok,
    run_dbt,
    write_profiles,
    write_raw,
    write_silver,
)

POLICY = PROJECT / "governance.yml"
PUBLIC_RELATIONS = {"dim_repo", "dim_user", "fct_events", *(m for m, _ in MARTS.values())}
EMAIL_TEXT = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
SILVER_SALT = "silver-test-salt-not-a-secret"


class _Lazy:
    """Imports ``repopulse.governance`` on first attribute access, inside the test body."""

    def __getattr__(self, name: str):
        return getattr(importlib.import_module("repopulse.governance"), name)


@pytest.fixture
def gov():
    """The module under test; the import happens in the test, so each test FAILS on its own."""
    return _Lazy()


def _keys(findings) -> set[tuple[str, str, str]]:
    return {(f.schema, f.table, f.column) for f in findings}


def _run_cli(*args: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, "-m", "repopulse.governance", *args],
        capture_output=True,
        text=True,
        cwd=ROOT,
        check=False,
    )


# ---------------------------------------------------------------- scan_raw_pii (in memory)


@pytest.fixture
def dirty_con():
    """Schema ``g`` with raw emails in scalar, array and nested columns; ``clean`` without."""
    con = duckdb.connect(":memory:")
    salt = "scan-test-salt"
    con.execute("CREATE SCHEMA g")
    con.execute("CREATE SCHEMA clean")
    con.execute("CREATE SCHEMA other")
    con.execute(
        "CREATE TABLE g.users AS SELECT * FROM (VALUES "
        "('alice', 'alice@example.com'), ('bob', NULL), ('carol', 'carol at home')"
        ") AS t(login, contact)"
    )
    con.execute(
        "CREATE TABLE g.events AS SELECT * FROM (VALUES "
        "(1, ['dev@example.org', 'x']), (2, ['plain']), (3, NULL)"
        ") AS t(id, author_emails)"
    )
    con.execute(
        "CREATE TABLE g.nested AS SELECT "
        "1 AS id, "
        "{'name': 'n', 'tags': ['ok', 'bob@example.org']} AS info, "
        "[{'k': 'a', 'v': 'carol@example.net'}] AS kv, "
        "MAP {'x': 'dan@example.com'} AS m"
    )
    con.execute("CREATE VIEW g.v_contacts AS SELECT contact FROM g.users")
    con.execute(
        "CREATE TABLE clean.users AS SELECT * FROM (VALUES "
        "(?, ?, [?], 1004, TIMESTAMP '2026-01-05 08:00:00'), "
        "('dependabot[bot]', NULL, NULL, 1005, NULL)"
        ") AS t(login, login_hash, name_hashes, actor_id, seen_at)",
        [
            "dave",
            hash_pii("dave@example.com", salt),
            hash_pii("Dave Example", salt),
        ],
    )
    con.execute("CREATE TABLE other.leak AS SELECT 'eve@example.com' AS contact")
    yield con
    con.close()


def test_scan_finds_email_in_varchar_column(gov, dirty_con):
    findings = gov.scan_raw_pii(dirty_con, ["g"])
    users = [f for f in findings if (f.table, f.column) == ("users", "contact")]
    assert len(users) == 1
    assert users[0].schema == "g"
    assert users[0].rows == 1  # NULL and 'carol at home' are not emails


def test_scan_finds_email_in_varchar_array(gov, dirty_con):
    findings = gov.scan_raw_pii(dirty_con, ["g"])
    hits = [f for f in findings if (f.table, f.column) == ("events", "author_emails")]
    assert [(h.schema, h.rows) for h in hits] == [("g", 1)]


def test_scan_finds_email_in_nested_struct_list_and_map(gov, dirty_con):
    keys = _keys(gov.scan_raw_pii(dirty_con, ["g"]))
    assert {("g", "nested", "info"), ("g", "nested", "kv"), ("g", "nested", "m")} <= keys
    assert ("g", "nested", "id") not in keys


def test_scan_covers_views(gov, dirty_con):
    assert ("g", "v_contacts", "contact") in _keys(gov.scan_raw_pii(dirty_con, ["g"]))


def test_scan_reports_exactly_the_dirty_columns(gov, dirty_con):
    assert _keys(gov.scan_raw_pii(dirty_con, ["g"])) == {
        ("g", "users", "contact"),
        ("g", "events", "author_emails"),
        ("g", "nested", "info"),
        ("g", "nested", "kv"),
        ("g", "nested", "m"),
        ("g", "v_contacts", "contact"),
    }


def test_scan_ignores_hashes_logins_and_non_text_columns(gov, dirty_con):
    assert gov.scan_raw_pii(dirty_con, ["clean"]) == []


def test_scan_only_looks_at_requested_schemas(gov, dirty_con):
    assert ("other", "leak", "contact") not in _keys(gov.scan_raw_pii(dirty_con, ["g", "clean"]))
    assert _keys(gov.scan_raw_pii(dirty_con, ["other"])) == {("other", "leak", "contact")}


def test_scan_findings_do_not_carry_raw_values(gov, dirty_con):
    for finding in gov.scan_raw_pii(dirty_con, ["g"]):
        assert not EMAIL_TEXT.search(repr(finding)), finding


# ---------------------------------------------------------------- scan CLI


@pytest.fixture
def dirty_db(tmp_path):
    path = tmp_path / "dirty.duckdb"
    con = duckdb.connect(str(path))
    try:
        con.execute("CREATE SCHEMA main_gold")
        con.execute(
            "CREATE TABLE main_gold.dim_contact AS SELECT * FROM (VALUES "
            "('alice', 'alice@example.com'), ('bob', 'bob@example.org')) AS t(login, email)"
        )
        con.execute("CREATE SCHEMA main_public")
        con.execute("CREATE TABLE main_public.dim_repo AS SELECT 'apache/spark' AS repo_name")
    finally:
        con.close()
    return path


def test_cli_scan_exits_1_and_names_column_without_printing_values(gov, dirty_db, capsys):
    code = gov.main(["scan", "--db", str(dirty_db), "--schema", "main_gold"])
    out = capsys.readouterr()
    text = out.out + out.err
    assert code == 1, text
    assert "main_gold" in text
    assert "dim_contact" in text
    assert "email" in text
    assert "alice@example.com" not in text
    assert "bob@example.org" not in text
    assert not EMAIL_TEXT.search(text), text


def test_cli_scan_clean_schema_exits_0(gov, dirty_db, capsys):
    assert gov.main(["scan", "--db", str(dirty_db), "--schema", "main_public"]) == 0


def test_cli_scan_missing_db_exits_2(gov, tmp_path, capsys):
    assert gov.main(["scan", "--db", str(tmp_path / "nope.duckdb"), "--schema", "x"]) == 2


def test_cli_scan_unknown_schema_exits_2(gov, dirty_db, capsys):
    # a typo must not pass as "nothing found"
    assert gov.main(["scan", "--db", str(dirty_db), "--schema", "main_gld"]) == 2


def test_cli_bad_arguments_exit_2(gov, capsys):
    with pytest.raises(SystemExit) as exc:
        gov.main(["scan"])
    assert exc.value.code == 2
    with pytest.raises(SystemExit) as exc:
        gov.main(["frobnicate"])
    assert exc.value.code == 2


# ---------------------------------------------------------------- policy


def test_policy_declares_pii_classes_layers_and_roles(gov):
    policy = gov.load_policy(POLICY)
    assert set(policy["pii_classes"]) == {"none", "public_identifier", "hashed", "direct"}
    assert set(policy["layers"]) == {"staging", "intermediate", "gold", "public"}
    assert set(policy["roles"]) == {"pipeline", "analyst", "public"}
    for layer in policy["layers"].values():
        assert "direct" in layer["forbidden_classes"]
    for layer in ("gold", "public"):
        assert "hashed" in policy["layers"][layer]["forbidden_classes"]
    assert policy["roles"]["public"]["read"] == ["public"]
    assert policy["roles"]["public"]["raw_classes"] == ["none"]
    assert "direct" not in policy["roles"]["pipeline"]["raw_classes"]


def test_load_policy_rejects_unknown_class_in_role(gov, tmp_path):
    policy = yaml.safe_load(POLICY.read_text(encoding="utf-8"))
    policy["roles"]["analyst"]["raw_classes"].append("secret_sauce")
    bad = tmp_path / "governance.yml"
    bad.write_text(yaml.safe_dump(policy), encoding="utf-8")
    with pytest.raises(gov.PolicyError, match="secret_sauce"):
        gov.load_policy(bad)


# ---------------------------------------------------------------- check_labels (synthetic manifest)


def _node(name: str, schema: str, columns: dict[str, dict | None], db_schema: str = "") -> tuple:
    cols = {
        c: {"name": c, "meta": {}, "config": {"meta": meta} if meta is not None else {}}
        for c, meta in columns.items()
    }
    node = {
        "resource_type": "model",
        "name": name,
        "alias": name,
        "schema": db_schema or f"main_{schema}",
        "config": {"schema": schema},
        "columns": cols,
    }
    return f"model.repopulse.{name}", node


def _manifest(*nodes: tuple) -> dict:
    return {"nodes": dict(nodes)}


def _check(gov, manifest: dict, actual: dict | None = None) -> list:
    """check_labels against the real policy file."""
    return gov.check_labels(manifest, gov.load_policy(POLICY), actual)


def _errors(found) -> set[tuple[str, str]]:
    return {(e.model, e.column) for e in found}


def test_check_clean_manifest_has_no_errors(gov):
    manifest = _manifest(
        _node("stg", "staging", {"id": {"pii": "none"}, "h": {"pii": "hashed"}}),
        _node("dim", "gold", {"login": {"pii": "public_identifier"}}),
        _node("pub", "public", {"login": {"pii": "public_identifier", "masked": True}}),
    )
    assert _check(gov, manifest) == []


def test_check_flags_missing_label(gov):
    manifest = _manifest(_node("dim", "gold", {"id": {"pii": "none"}, "login": None}))
    errors = _check(gov, manifest)
    assert _errors(errors) == {("dim", "login")}
    assert "meta.pii" in errors[0].message


def test_check_flags_unknown_label(gov):
    errors = _check(gov, _manifest(_node("dim", "gold", {"x": {"pii": "secretish"}})))
    assert _errors(errors) == {("dim", "x")}
    assert "secretish" in errors[0].message


def test_check_flags_direct_from_staging_on(gov):
    manifest = _manifest(
        _node("stg", "staging", {"email": {"pii": "direct"}}),
        _node("int", "intermediate", {"name": {"pii": "direct"}}),
    )
    assert {("stg", "email"), ("int", "name")} <= _errors(_check(gov, manifest))


def test_check_flags_hashed_in_gold_and_public(gov):
    manifest = _manifest(
        _node("fct", "gold", {"h": {"pii": "hashed"}}),
        _node("pub", "public", {"h": {"pii": "hashed", "masked": True}}),
    )
    assert {("fct", "h"), ("pub", "h")} <= _errors(_check(gov, manifest))


def test_check_flags_public_role_reading_unmasked_identifier(gov):
    errors = _check(gov, _manifest(_node("pub", "public", {"login": {"pii": "public_identifier"}})))
    assert _errors(errors) == {("pub", "login")}
    assert "public" in errors[0].message


def test_check_flags_model_outside_policy_layers(gov):
    errors = _check(gov, _manifest(_node("x", "scratch", {"a": {"pii": "none"}})))
    assert [e.model for e in errors] == ["x"]
    assert "scratch" in errors[0].message


def test_check_flags_built_column_missing_from_yml(gov):
    manifest = _manifest(_node("dim", "gold", {"id": {"pii": "none"}}))
    actual = {("main_gold", "dim"): ["id", "login"]}
    assert _errors(_check(gov, manifest, actual)) == {("dim", "login")}


def test_check_reads_meta_at_column_top_level_too(gov):
    key, node = _node("dim", "gold", {"id": None})
    node["columns"]["id"]["meta"] = {"pii": "none"}
    assert _check(gov, _manifest((key, node))) == []


def test_check_ignores_seeds_and_tests(gov):
    seed_key, seed = _node("fixture", "main", {"email": {"pii": "direct"}})
    seed["resource_type"] = "seed"
    assert _check(gov, _manifest((seed_key, seed))) == []


# ---------------------------------------------------------------- dbt: unhashed silver fails


@pytest.fixture(scope="module")
def silver_builds(tmp_path_factory) -> dict:
    """Two builds of +fct_events: silver with raw commit emails/names, and the same hashed."""
    base = tmp_path_factory.mktemp("governance_silver")
    raw = write_raw(base / "raw")
    pick = (
        "WHERE event_id IN "
        "(SELECT min(event_id) FROM silver UNION SELECT max(event_id) FROM silver)"
    )
    cases = {
        "raw": ("dev@example.com", "Dev Example"),
        "hashed": (hash_pii("dev@example.com", SILVER_SALT), hash_pii("Dev Example", SILVER_SALT)),
    }
    out = {}
    for name, (email, author) in cases.items():
        glob = write_silver(
            raw,
            base / f"silver_{name}",
            (
                "UPDATE silver SET commit_author_email_hashes = "
                f"[{q(email)}, NULL], commit_author_name_hashes = [{q(author)}] {pick}",
            ),
        )
        db = base / f"{name}.duckdb"
        proc = run_dbt(
            ["build", "--select", "+fct_events", "--indirect-selection", "cautious"],
            PROJECT,
            write_profiles(base / f"profiles_{name}", db),
            base / name,
            {"silver_gharchive_parquet": glob},
        )
        out[name] = {"proc": proc, "db": db}
    return out


def _tables(db, schema: str) -> set[str]:
    con = duckdb.connect(str(db), read_only=True)
    try:
        return {
            r[0]
            for r in con.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = ?", [schema]
            ).fetchall()
        }
    finally:
        con.close()


def test_unhashed_silver_fails_dbt_build_at_hashed_pii(silver_builds):
    proc = silver_builds["raw"]["proc"]
    text = output(proc)
    assert proc.returncode != 0, text[-4000:]
    failing = [ln for ln in text.splitlines() if "hashed_pii" in ln and "FAIL" in ln]
    assert failing, text[-4000:]
    assert any("stg_gharchive_events" in ln for ln in failing), failing
    assert any("commit_author_email_hashes" in ln for ln in failing), failing


def test_unhashed_silver_never_reaches_gold(silver_builds):
    assert not {"fct_events", "dim_user"} & _tables(silver_builds["raw"]["db"], GOLD_SCHEMA)


def test_hashed_silver_builds(silver_builds):
    require_ok(silver_builds["hashed"]["proc"])
    assert {"fct_events", "dim_user", "dim_repo"} <= _tables(
        silver_builds["hashed"]["db"], GOLD_SCHEMA
    )


def test_hashed_pii_and_no_raw_email_tests_are_attached(built):
    require_ok(built["proc"])
    manifest = json.loads(built["manifest"].read_text(encoding="utf-8"))
    attached: dict[str, set[tuple[str, str]]] = {"hashed_pii": set(), "no_raw_email": set()}
    for node in manifest["nodes"].values():
        name = (node.get("test_metadata") or {}).get("name")
        if node["resource_type"] == "test" and name in attached:
            model = node["attached_node"].split(".")[-1]
            attached[name].add((model, node["column_name"]))
    assert {
        ("stg_gharchive_events", "commit_author_email_hashes"),
        ("stg_gharchive_events", "commit_author_name_hashes"),
    } <= attached["hashed_pii"]
    # every labelled (not 'none') column of gold and public carries no_raw_email
    for node in manifest["nodes"].values():
        if node["resource_type"] != "model" or node["config"].get("schema") not in (
            "gold",
            "public",
        ):
            continue
        for col in node["columns"].values():
            pii = (col.get("config") or {}).get("meta", {}).get("pii") or col["meta"].get("pii")
            if pii != "none":
                assert (node["name"], col["name"]) in attached["no_raw_email"], (node["name"], col)


def test_dbt_email_pattern_is_the_scanner_pattern(gov):
    test_sql = (PROJECT / "tests" / "generic" / "no_raw_email.sql").read_text(encoding="utf-8")
    assert gov.EMAIL_PATTERN in test_sql


# ---------------------------------------------------------------- dbt: scan + check on real builds


def test_scan_cli_on_clean_build_of_gold_and_public_exits_0(built):
    require_ok(built["proc"])
    proc = _run_cli(
        "scan", "--db", str(built["db"]), "--schema", GOLD_SCHEMA, "--schema", PUBLIC_SCHEMA
    )
    assert proc.returncode == 0, proc.stdout + proc.stderr


def test_scan_cli_flags_email_leaked_into_a_gold_model(tmp_path):
    project = copy_project(tmp_path / "transform")
    (project / "models" / "gold" / "leaky_contacts.sql").write_text(
        "-- careless model: builds contact emails from logins\n"
        "select\n"
        "    user_key,\n"
        "    actor_login || '@users.example.com' as contact_email,\n"
        "    [actor_login || '@mail.example.org'] as contact_emails\n"
        "from {{ ref('dim_user') }}\n",
        encoding="utf-8",
    )
    db = tmp_path / "leaky.duckdb"
    build = run_dbt(
        ["build", "--select", "+leaky_contacts", "--indirect-selection", "cautious"],
        project,
        write_profiles(tmp_path / "profiles", db),
        tmp_path,
    )
    require_ok(build)  # the CI seed path (NULL hash arrays) passes hashed_pii
    proc = _run_cli("scan", "--db", str(db), "--schema", GOLD_SCHEMA)
    text = proc.stdout + proc.stderr
    assert proc.returncode == 1, text
    assert "leaky_contacts" in text
    assert "contact_email" in text
    assert "contact_emails" in text
    assert "dim_user" not in text
    assert not EMAIL_TEXT.search(text), text


def test_check_cli_on_real_project_exits_0(built):
    require_ok(built["proc"])
    proc = _run_cli(
        "check",
        "--manifest",
        str(built["manifest"]),
        "--policy",
        str(POLICY),
        "--db",
        str(built["db"]),
    )
    assert proc.returncode == 0, proc.stdout + proc.stderr


def _parse_copy(tmp_path, edit) -> tuple[subprocess.CompletedProcess, str]:
    project = copy_project(tmp_path / "transform")
    edit(project)
    parsed = run_dbt(
        ["parse"], project, write_profiles(tmp_path / "profiles", tmp_path / "p.duckdb"), tmp_path
    )
    require_ok(parsed)
    proc = _run_cli(
        "check",
        "--manifest",
        str(tmp_path / "target" / "manifest.json"),
        "--policy",
        str(project / "governance.yml"),
    )
    return proc, proc.stdout + proc.stderr


def test_check_cli_fails_when_gold_column_loses_its_label(tmp_path):
    def drop_label(project):
        path = project / "models" / "gold" / "_gold.yml"
        doc = yaml.safe_load(path.read_text(encoding="utf-8"))
        dim_user = next(m for m in doc["models"] if m["name"] == "dim_user")
        col = next(c for c in dim_user["columns"] if c["name"] == "actor_login")
        col.pop("meta", None)
        (col.get("config") or {}).pop("meta", None)
        path.write_text(yaml.safe_dump(doc, sort_keys=False), encoding="utf-8")

    proc, text = _parse_copy(tmp_path, drop_label)
    assert proc.returncode == 1, text
    assert "dim_user.actor_login" in text


def test_check_cli_fails_when_public_model_exposes_login_unmasked(tmp_path):
    def add_leak(project):
        public = project / "models" / "public"
        public.mkdir(exist_ok=True)
        (public / "pub_login_leak.sql").write_text(
            "select actor_login from {{ ref('dim_user') }}\n", encoding="utf-8"
        )
        (public / "_leak.yml").write_text(
            "version: 2\n"
            "models:\n"
            "  - name: pub_login_leak\n"
            "    columns:\n"
            "      - name: actor_login\n"
            "        config: {meta: {pii: public_identifier}}\n",
            encoding="utf-8",
        )

    proc, text = _parse_copy(tmp_path, add_leak)
    assert proc.returncode == 1, text
    assert "pub_login_leak.actor_login" in text
    assert "public" in text


def test_check_cli_missing_manifest_exits_2(tmp_path):
    proc = _run_cli("check", "--manifest", str(tmp_path / "manifest.json"), "--policy", str(POLICY))
    assert proc.returncode == 2, proc.stdout + proc.stderr


# ---------------------------------------------------------------- masking of the public schema


@pytest.fixture
def gold_db(built):
    require_ok(built["proc"])
    con = duckdb.connect(str(built["db"]), read_only=True)
    yield con
    con.close()


def test_public_schema_has_marts_dims_and_fact(gold_db):
    names = {
        r[0]
        for r in gold_db.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = ?",
            [PUBLIC_SCHEMA],
        ).fetchall()
    }
    assert names == PUBLIC_RELATIONS


def test_public_dim_user_is_masked_with_the_silver_formula(gold_db):
    salt = ci_salt()
    gold = gold_db.execute(
        "SELECT user_key, actor_id, actor_login, version_number, valid_from, valid_to, "
        f"is_current, is_bot FROM {GOLD_SCHEMA}.dim_user"
    ).fetchall()
    assert gold
    expected = sorted(
        (
            hash_pii(k, salt),
            hash_pii(str(a), salt),
            None if login is None else hash_pii(login, salt),
            *rest,
        )
        for k, a, login, *rest in gold
    )
    public = sorted(
        gold_db.execute(
            "SELECT user_key, actor_id, actor_login, version_number, valid_from, valid_to, "
            f"is_current, is_bot FROM {PUBLIC_SCHEMA}.dim_user"
        ).fetchall()
    )
    assert public == expected


def test_public_fct_events_masks_user_key_and_actor_id(gold_db):
    salt = ci_salt()
    gold = dict(
        (e, (k, a))
        for e, k, a in gold_db.execute(
            f"SELECT event_id, user_key, actor_id FROM {GOLD_SCHEMA}.fct_events"
        ).fetchall()
    )
    public = dict(
        (e, (k, a))
        for e, k, a in gold_db.execute(
            f"SELECT event_id, user_key, actor_id FROM {PUBLIC_SCHEMA}.fct_events"
        ).fetchall()
    )
    assert public == {e: (hash_pii(k, salt), hash_pii(str(a), salt)) for e, (k, a) in gold.items()}


def test_public_join_fct_dim_user_keeps_gold_row_count(gold_db):
    def joined(schema: str) -> int:
        return gold_db.execute(
            f"SELECT count(*) FROM {schema}.fct_events JOIN {schema}.dim_user USING (user_key)"
        ).fetchone()[0]

    assert joined(PUBLIC_SCHEMA) == joined(GOLD_SCHEMA) > 0


def test_no_gold_login_appears_in_any_public_string(gold_db):
    logins = {
        r[0]
        for r in gold_db.execute(
            f"SELECT DISTINCT actor_login FROM {GOLD_SCHEMA}.dim_user WHERE actor_login IS NOT NULL"
        ).fetchall()
    }
    assert logins
    cols = gold_db.execute(
        "SELECT table_name, column_name FROM information_schema.columns "
        "WHERE table_schema = ? AND data_type = 'VARCHAR'",
        [PUBLIC_SCHEMA],
    ).fetchall()
    assert cols
    for table, column in cols:
        values = {
            r[0]
            for r in gold_db.execute(
                f'SELECT DISTINCT "{column}" FROM {PUBLIC_SCHEMA}."{table}"'
            ).fetchall()
        }
        assert not values & logins, (table, column, sorted(values & logins)[:3])


@pytest.mark.parametrize("model", sorted({"dim_repo", *(m for m, _ in MARTS.values())}))
def test_public_marts_and_dim_repo_equal_gold(gold_db, model):
    gold_n = gold_db.execute(f"SELECT count(*) FROM {GOLD_SCHEMA}.{model}").fetchone()[0]
    public_n = gold_db.execute(f"SELECT count(*) FROM {PUBLIC_SCHEMA}.{model}").fetchone()[0]
    assert public_n == gold_n
    diff = gold_db.execute(
        f"SELECT count(*) FROM ((SELECT * FROM {GOLD_SCHEMA}.{model} "
        f"EXCEPT ALL SELECT * FROM {PUBLIC_SCHEMA}.{model}) UNION ALL "
        f"(SELECT * FROM {PUBLIC_SCHEMA}.{model} EXCEPT ALL SELECT * FROM {GOLD_SCHEMA}.{model}))"
    ).fetchone()[0]
    assert diff == 0


def test_non_ci_target_without_salt_fails_to_compile(tmp_path):
    proc = run_dbt(
        ["compile", "--select", "pub_dim_user"],
        PROJECT,
        write_profiles(tmp_path / "profiles", tmp_path / "prod.duckdb", target="prod"),
        tmp_path,
        target="prod",
    )
    text = output(proc)
    assert proc.returncode != 0, text[-4000:]
    assert "PII_HASH_SALT" in text, text[-4000:]


def test_non_ci_target_uses_the_salt_from_the_environment(tmp_path):
    proc = run_dbt(
        ["compile", "--select", "pub_dim_user"],
        PROJECT,
        write_profiles(tmp_path / "profiles", tmp_path / "prod.duckdb", target="prod"),
        tmp_path,
        target="prod",
        env={"PII_HASH_SALT": "env-test-salt-not-a-secret"},
    )
    require_ok(proc)
    compiled = next((tmp_path / "target" / "compiled").rglob("pub_dim_user.sql"))
    assert "env-test-salt-not-a-secret" in compiled.read_text(encoding="utf-8")
    assert ci_salt() not in compiled.read_text(encoding="utf-8")
