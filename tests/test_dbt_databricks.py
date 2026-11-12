"""dbt project on a Databricks SQL warehouse (D2, docs/dbt-databricks.md), checked offline.

CI has dbt-duckdb only, so the Databricks target is checked here in three ways:

- every cross-database macro of ``transform/macros/cross_db.sql`` has a ``databricks__`` branch,
  rendered with plain Jinja; where DuckDB has the same building blocks, the ``databricks__`` SQL
  runs on DuckDB next to the ``duckdb__`` SQL (each Databricks-only function is shimmed by a DuckDB
  macro with the documented meaning) and both must give the same values;
- SQL and YAML outside the macros use nothing Databricks rejects (bare ``varchar``, DuckDB-only
  functions), like ``test_dbt_portability.py`` does for Snowflake;
- staging reads the Unity Catalog silver table of D1 on Databricks and keeps the seed / parquet
  var elsewhere (Jinja render of the model + ``dbt parse`` manifest on target ``ci``).

The marts giving the same result on both adapters is the job of the dbt ``unit_tests:`` of
``models/gold/**``: the same ``given``/``expect`` run on DuckDB (CI) and on Databricks
(``.github/workflows/dbt-databricks.yml``).
"""

from __future__ import annotations

import json
import re
import tomllib
from datetime import date, datetime
from pathlib import Path

import duckdb
import pytest
import yaml

jinja2 = pytest.importorskip("jinja2", reason="jinja2 comes with the dbt extra")

from test_dbt_portability import (  # noqa: E402
    dispatched,
    macro_sources,
    sql_files,
    strip_comments_and_strings,
)

from repopulse.governance import EMAIL_PATTERN  # noqa: E402
from repopulse.spark.pii import hash_pii  # noqa: E402
from repopulse.spark.tables import table_names  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
TRANSFORM = ROOT / "transform"
MACROS = TRANSFORM / "macros"
MACRO_FILES = ("cross_db.sql", "mask_pii.sql")
STAGING = TRANSFORM / "models" / "staging" / "stg_gharchive_events.sql"
FCT = TRANSFORM / "models" / "gold" / "fct_events.sql"
PROFILES = TRANSFORM / "profiles.yml"
PYPROJECT = ROOT / "pyproject.toml"
SALT = "d2-test-salt"
D1_SILVER = table_names("gharchive", schema="repopulse", catalog="workspace")["silver"]

# Databricks SQL functions used by databricks__ branches that DuckDB lacks under that name. Each
# shim has the meaning documented by Databricks, so running the databricks__ SQL on DuckDB checks
# the arithmetic/casts around the function, not the function itself.
DATABRICKS_SHIMS = (
    "CREATE MACRO unix_micros(ts) AS epoch_us(ts)",
    "CREATE MACRO sha2(expr, bit_length) AS "
    "CASE WHEN bit_length = 256 THEN sha256(expr) ELSE error('only sha2(x, 256) is shimmed') END",
    "CREATE MACRO btrim(str, trim_str) AS trim(str, trim_str)",
)


# ---------------------------------------------------------------- Jinja harness


class CompilerError(Exception):
    """What ``exceptions.raise_compiler_error`` raises in this harness."""


class _Exceptions:
    @staticmethod
    def raise_compiler_error(message):
        raise CompilerError(message)


class _Adapter:
    """``adapter.dispatch(name, package)`` -> the ``<prefix>__<name>`` macro, like dbt."""

    def __init__(self, prefix: str):
        self.prefix = prefix
        self.module = None

    def dispatch(self, name, package=None):
        macro = getattr(self.module, f"{self.prefix}__{name}", None)
        assert macro is not None, f"no macro {self.prefix}__{name} in {MACROS}"
        return macro


def _env_var(env: dict):
    def env_var(name, default=None):
        if name in env:
            return env[name]
        if default is None:
            raise CompilerError(f"env var {name} is not set")
        return default

    return env_var


def _var(values: dict):
    def var(name, default=None):
        return values.get(name, default)

    return var


def jinja_globals(prefix: str, *, dbt_vars: dict | None = None, env: dict | None = None) -> dict:
    return {
        "adapter": _Adapter(prefix),
        "exceptions": _Exceptions(),
        "return": lambda value: value,
        "target": {"type": prefix, "name": "ci" if prefix == "duckdb" else prefix},
        "env_var": _env_var({"PII_HASH_SALT": SALT, **(env or {})}),
        "var": _var({"ci_pii_hash_salt": "ci-fixture-salt", **(dbt_vars or {})}),
        "execute": True,
    }


def macro_module(prefix: str, **kw):
    g = jinja_globals(prefix, **kw)
    text = "\n".join((MACROS / f).read_text(encoding="utf-8") for f in MACRO_FILES)
    module = jinja2.Environment().from_string(text, globals=g).module
    g["adapter"].module = module
    return module, g


def render(prefix: str, macro: str, *args, **kwargs) -> str:
    """One macro (a dispatcher dispatches on ``prefix``) rendered on one line."""
    module, _ = macro_module(prefix)
    fn = getattr(module, macro, None)
    assert fn is not None, f"macro {macro} is not defined in {MACROS}"
    return " ".join(str(fn(*args, **kwargs)).split())


def render_model(path: Path, prefix: str, dbt_vars: dict | None = None, incremental=False):
    """(SQL, config kwargs) of a model rendered for an adapter; refs/sources become markers."""
    module, g = macro_module(prefix, dbt_vars=dbt_vars)
    captured: dict = {}

    def config(**kw):
        captured.update(kw)
        return ""

    names = set(dispatched()) | {"mask_pii"}
    context = {
        **g,
        **{name: getattr(module, name) for name in names if hasattr(module, name)},
        "config": config,
        "ref": lambda name: f"__ref__{name}",
        "source": lambda src, table: f"__source__{src}.{table}",
        "is_incremental": lambda: incremental,
        "this": "__this__",
        "run_started_at": datetime(2026, 11, 11, 10, 0, 0),
    }
    sql = jinja2.Environment().from_string(path.read_text(encoding="utf-8")).render(**context)
    return " ".join(sql.split()), captured


def duck() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(":memory:")
    for sql in DATABRICKS_SHIMS:
        con.execute(sql)
    return con


# ---------------------------------------------------------------- every macro has a branch


def test_every_cross_database_macro_has_a_databricks_branch():
    macros = macro_sources()
    assert dispatched(), f"no adapter.dispatch macro under {MACROS}"
    missing = [
        f"{name} -> databricks__{target}"
        for name, target in dispatched().items()
        if f"databricks__{target}" not in macros
    ]
    assert missing == [], missing


def test_databricks_branch_of_the_parquet_reader_points_at_unity_catalog():
    module, _ = macro_module("databricks")
    with pytest.raises(CompilerError) as info:
        str(module.silver_parquet_relation("data/silver/*.parquet"))
    message = str(info.value)
    assert "silver_gharchive_parquet" in message
    assert "Unity Catalog" in message


# ---------------------------------------------------------------- branches: SQL and DuckDB run

HOUR_PAIRS = [
    ("2026-01-01 00:00:00", "2026-01-01 01:30:00"),
    ("2026-01-01 00:00:00", "2026-01-01 00:00:00.000001"),
    ("2026-01-02 10:00:00", "2026-01-01 09:15:00.250000"),
    ("2025-12-31 23:59:59.999999", "2026-01-03 00:00:00"),
    (None, "2026-01-01 00:00:00"),
    ("2026-01-01 00:00:00", None),
]


def test_hours_between_on_databricks_is_exact_microseconds_as_double():
    sql = render("databricks", "hours_between", "s", "e")
    assert re.search(r"\bunix_micros\(\s*e\s*\)\s*-\s*unix_micros\(\s*s\s*\)", sql), sql
    assert re.search(r"\bas double\b", sql), sql
    assert "3600000000" in sql, sql


def test_hours_between_databricks_and_duckdb_branches_agree_on_duckdb():
    duck_sql = render("duckdb", "hours_between", "s", "e")
    dbx_sql = render("databricks", "hours_between", "s", "e")
    con = duck()
    try:
        con.execute("CREATE TABLE t (i INTEGER, s TIMESTAMP, e TIMESTAMP)")
        con.executemany(
            "INSERT INTO t VALUES (?, ?, ?)", [(i, s, e) for i, (s, e) in enumerate(HOUR_PAIRS)]
        )
        rows = con.execute(f"SELECT {duck_sql}, {dbx_sql} FROM t ORDER BY i").fetchall()
    finally:
        con.close()
    # The duckdb__ branch subtracts epoch() doubles (seconds since 1970, about 0.24 us apart in
    # 2026), the databricks__ one exact integer microseconds: equal to well under a microsecond.
    for (expected, got), pair in zip(rows, HOUR_PAIRS, strict=True):
        if expected is None:
            assert got is None, pair
        else:
            assert isinstance(got, float), pair
            assert got == pytest.approx(expected, rel=1e-12, abs=1e-6 / 3600), pair
    assert rows[0][1] == 1.5
    assert rows[1][1] == pytest.approx(1 / 3_600_000_000, rel=1e-12)


def test_utc_timestamp_on_databricks_is_a_plain_timestamp_cast():
    assert render("databricks", "utc_timestamp", "created_at") == "cast(created_at as timestamp)"


def test_cast_string_array_on_databricks_parses_seed_text_and_casts_real_arrays():
    assert render("databricks", "cast_string_array", "c") == "cast(c as array<string>)"
    assert (
        render("databricks", "cast_string_array", "c", from_text=True)
        == "from_json(cast(c as string), 'array<string>')"
    )


@pytest.mark.parametrize("prefix", ["duckdb", "snowflake"])
def test_cast_string_array_of_other_engines_accepts_and_ignores_from_text(prefix):
    assert render(prefix, "cast_string_array", "c", from_text=True) == render(
        prefix, "cast_string_array", "c"
    )


def test_add_days_on_databricks_is_date_add():
    assert render("databricks", "add_days", "d", 28) == "date_add(d, 28)"


def test_add_days_on_duckdb_keeps_a_date():
    sql = render("duckdb", "add_days", "d", 28)
    con = duck()
    try:
        value, kind = con.execute(
            f"SELECT {sql}, typeof({sql}) FROM (SELECT DATE '2026-01-05' AS d)"
        ).fetchone()
    finally:
        con.close()
    assert value == date(2026, 2, 2)
    assert kind == "DATE"


MASK_VALUES = ["Alice@Example.COM", "  bob@example.com\t\n", "1004", "\x0bdave\x0c\r", None]


def test_mask_pii_on_databricks_uses_sha2_and_btrim():
    module, _ = macro_module("databricks")
    sql = " ".join(str(module.mask_pii("v")).split())
    assert re.search(r"\bsha2\(.*,\s*256\s*\)", sql), sql
    assert re.search(r"\bbtrim\(", sql), sql
    assert not re.search(r"\bsha256\(|\bvarchar\b", sql), sql


def test_mask_pii_databricks_and_duckdb_agree_with_the_silver_hash_on_duckdb():
    duck_sql = " ".join(str(macro_module("duckdb")[0].mask_pii("v")).split())
    dbx_sql = " ".join(str(macro_module("databricks")[0].mask_pii("v")).split())
    con = duck()
    try:
        con.execute("CREATE TABLE t (i INTEGER, v VARCHAR)")
        con.executemany("INSERT INTO t VALUES (?, ?)", list(enumerate(MASK_VALUES)))
        rows = con.execute(f"SELECT v, {duck_sql}, {dbx_sql} FROM t ORDER BY i").fetchall()
    finally:
        con.close()
    for value, from_duck, from_dbx in rows:
        assert from_dbx == from_duck, value
        assert from_dbx == (None if value is None else hash_pii(value, SALT)), value


def _spark_literal_after(keyword: str, sql: str) -> str:
    """Body of the string literal after ``keyword``, unescaped like Spark SQL does (``\\\\``)."""
    match = re.search(rf"\b{keyword}\s+'((?:[^'\\]|\\.)*)'", sql)
    assert match, f"no {keyword} '<literal>' in {sql}"
    body = match.group(1)
    assert not re.search(r"\\[^\\]", body.replace("\\\\", "")), f"lone backslash in {body!r}"
    return body.replace("\\\\", "\\")


REGEX_SAMPLES = [
    "a" * 64,
    "0123456789abcdef" * 4,
    "A" * 64,
    "a" * 63,
    "a" * 65,
    "a" * 64 + "\n",
    "x@y.io",
    "lam@example.com",
    "see a.b@c-d.org here",
    "user@host",
    "user@hostXcom",
    "no email here",
    "",
]


@pytest.mark.parametrize(
    ("macro", "pattern"),
    [("regex_full_match", "[0-9a-f]{64}"), ("regex_contains", EMAIL_PATTERN)],
)
def test_regex_macros_on_databricks_match_like_duckdb(macro, pattern):
    dbx_sql = render("databricks", macro, "v", pattern)
    assert re.search(r"\brlike\b", dbx_sql), dbx_sql
    java = _spark_literal_after("rlike", dbx_sql)
    # Java regex -> Python re: \z (end of input) is \Z in Python; the rest is the same syntax.
    as_python = re.compile(java.replace("\\z", "\\Z"))
    duck_sql = render("duckdb", macro, "v", pattern)
    con = duck()
    try:
        for sample in REGEX_SAMPLES:
            expected = con.execute(f"SELECT {duck_sql} FROM (SELECT ? AS v)", [sample]).fetchone()
            assert (as_python.search(sample) is not None) is expected[0], (macro, sample)
    finally:
        con.close()


def test_string_array_elements_on_databricks_explodes_the_column():
    sql = render("databricks", "string_array_elements", "my_table", "hashes")
    assert re.search(r"\bexplode\(\s*hashes\s*\)\s+as\s+element\b", sql), sql
    assert "my_table" in sql and "hashes is not null" in sql
    assert "unnest" not in sql


def test_string_array_elements_on_duckdb_lists_every_element():
    sql = render("duckdb", "string_array_elements", "my_table", "hashes")
    con = duck()
    try:
        con.execute("CREATE TABLE my_table (hashes VARCHAR[])")
        con.execute("INSERT INTO my_table VALUES (['a', 'b']), (NULL), (['c'])")
        rows = sorted(con.execute(sql).fetchall())
    finally:
        con.close()
    assert rows == [("a",), ("b",), ("c",)]


# ---------------------------------------------------------------- nothing Databricks rejects

NOT_ON_DATABRICKS = {
    "bare varchar (Databricks: string, or varchar(n))": r"\bvarchar\b(?!\s*\()",
    "regexp_full_match/regexp_matches (use regex_full_match/regex_contains)": (
        r"\bregexp_(full_match|matches)\s*\("
    ),
    "unnest (use string_array_elements)": r"\bunnest\s*\(",
    "sha256() (use sha256_hex)": r"\bsha256\s*\(",
    "trim() (use trim_chars)": r"\btrim\s*\(",
}
ENGINE_BRANCHES = ("duckdb__", "snowflake__")


def databricks_violations(text: str, label: str) -> list[str]:
    stripped = strip_comments_and_strings(text)
    return [
        f"{label}:{lineno}: {what}"
        for lineno, line in enumerate(stripped.splitlines(), start=1)
        for what, pattern in NOT_ON_DATABRICKS.items()
        if re.search(pattern, line, re.I)
    ]


@pytest.mark.parametrize(
    "sql",
    [
        "select cast(x as varchar) from t",
        "select cast(x as VARCHAR[]) from t",
        "select regexp_full_match(x, 'a') from t",
        "select regexp_matches(x, 'a') from t",
        "select unnest(xs) from t",
        "select sha256(x) from t",
        "select trim(x, ' ') from t",
    ],
)
def test_databricks_scanner_flags_unsupported_syntax(sql):
    found = databricks_violations("-- header\n" + sql + "\n", "m.sql")
    assert found and all(v.startswith("m.sql:2:") for v in found), found


@pytest.mark.parametrize(
    "sql",
    [
        "select cast(x as string), cast(y as varchar(64)) from t",
        "-- cast(x as varchar) in a comment",
        "select 'unnest(x) in a string' as s",
        "select {{ trim_chars('x', \"' '\") }}, {{ sha256_hex('x') }} from t",
    ],
)
def test_databricks_scanner_ignores_portable_sql(sql):
    assert databricks_violations(sql + "\n", "m.sql") == []


def test_no_databricks_unsupported_syntax_outside_engine_branches():
    found = [v for p in sql_files() for v in databricks_violations(p.read_text("utf-8"), p.name)]
    for name, body in macro_sources().items():
        if not name.startswith(ENGINE_BRANCHES):
            found += databricks_violations(body, f"macro {name}")
    assert found == [], "SQL Databricks rejects:\n" + "\n".join(found)


def yaml_types() -> list[tuple[str, str, str]]:
    """(file, column, type) of every ``data_type`` and seed ``column_types`` under transform/."""
    out = []
    for path in sorted([*TRANSFORM.glob("models/**/*.yml"), *TRANSFORM.glob("seeds/*.yml")]):

        def walk(node, path=path):
            if isinstance(node, dict):
                if "data_type" in node:
                    out.append((path.name, str(node.get("name")), str(node["data_type"])))
                for col, kind in (node.get("column_types") or {}).items():
                    out.append((path.name, str(col), str(kind)))
                for value in node.values():
                    walk(value)
            elif isinstance(node, list):
                for value in node:
                    walk(value)

        walk(yaml.safe_load(path.read_text(encoding="utf-8")))
    return out


def test_contract_and_seed_types_are_spelled_the_same_on_every_engine():
    types = yaml_types()
    assert len(types) > 50, types
    bad = [t for t in types if re.fullmatch(r"(?i)varchar|.*\[\]", t[2])]
    assert bad == [], bad


# ---------------------------------------------------------------- staging source


def silver_source() -> dict | None:
    for path in sorted(TRANSFORM.glob("models/**/*.yml")):
        doc = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        for src in doc.get("sources") or []:
            if src.get("name") == "silver":
                return src
    return None


def _render_value(text, dbt_vars: dict | None = None) -> str:
    template = jinja2.Environment().from_string(str(text))
    return template.render(var=_var(dbt_vars or {}), env_var=_env_var({}))


def _source_fqn(dbt_vars: dict | None = None) -> str:
    src = silver_source()
    assert src is not None, "no dbt source `silver` under transform/models"
    tables = [t for t in src.get("tables") or [] if t.get("name") == "silver_gharchive_events"]
    assert len(tables) == 1, src
    parts = (src["database"], src["schema"], tables[0].get("identifier", tables[0]["name"]))
    return ".".join(_render_value(p, dbt_vars) for p in parts)


def test_silver_source_defaults_to_the_d1_unity_catalog_table():
    assert _source_fqn() == D1_SILVER == "workspace.repopulse.silver_gharchive_events"


def test_silver_source_location_is_configurable_by_vars():
    dbt_vars = {
        "silver_gharchive_catalog": "cat",
        "silver_gharchive_schema": "sch",
        "silver_gharchive_table": "tbl",
    }
    assert _source_fqn(dbt_vars) == "cat.sch.tbl"


def test_staging_on_databricks_reads_the_unity_catalog_source_by_default():
    sql, _ = render_model(STAGING, "databricks")
    assert "__source__silver.silver_gharchive_events" in sql
    assert "__ref__fixture_silver_gharchive_events" not in sql
    assert "cast(commit_author_email_hashes as array<string>)" in sql
    assert "from_json" not in sql
    assert not re.search(r"\bvarchar\b", sql), sql


def test_staging_on_databricks_can_still_read_the_seed_fixture():
    sql, _ = render_model(STAGING, "databricks", {"silver_gharchive_source": "seed"})
    assert "__ref__fixture_silver_gharchive_events" in sql
    assert "__source__" not in sql
    assert "from_json(cast(commit_author_email_hashes as string), 'array<string>')" in sql


def test_staging_on_duckdb_keeps_the_seed_default_and_the_parquet_var():
    sql, _ = render_model(STAGING, "duckdb")
    assert "__ref__fixture_silver_gharchive_events" in sql and "__source__" not in sql
    assert "cast(commit_author_email_hashes as varchar[])" in sql
    sql, _ = render_model(STAGING, "duckdb", {"silver_gharchive_parquet": "x/*.parquet"})
    assert "read_parquet('x/*.parquet')" in sql
    assert "__ref__fixture_silver_gharchive_events" not in sql and "__source__" not in sql


def test_parquet_var_on_databricks_is_a_compile_error_naming_unity_catalog():
    with pytest.raises(CompilerError, match="Unity Catalog"):
        render_model(STAGING, "databricks", {"silver_gharchive_parquet": "x/*.parquet"})


def test_unknown_silver_source_is_a_compile_error():
    with pytest.raises(CompilerError, match="silver_gharchive_source"):
        render_model(STAGING, "duckdb", {"silver_gharchive_source": "lake"})


@pytest.mark.parametrize(
    ("prefix", "strategy"),
    [("databricks", "merge"), ("duckdb", "delete+insert"), ("snowflake", "delete+insert")],
)
def test_fct_events_incremental_strategy_per_adapter(prefix, strategy):
    _, cfg = render_model(FCT, prefix)
    assert cfg.get("incremental_strategy") == strategy
    assert cfg.get("unique_key") == "event_id"


def _parse(work: Path, dbt_vars: dict | None = None) -> dict:
    pytest.importorskip("dbt.cli.main", reason="dbt extra not installed")
    pytest.importorskip("dbt.adapters.duckdb", reason="dbt-duckdb not installed")
    from dbt_helpers import PROJECT, require_ok, run_dbt, write_profiles

    profiles = write_profiles(work / "profiles", work / "parse.duckdb")
    proc = run_dbt(["parse"], PROJECT, profiles, work, dbt_vars)
    require_ok(proc)
    return json.loads((work / "target" / "manifest.json").read_text(encoding="utf-8"))


def test_manifest_staging_depends_on_seed_by_default_and_on_the_source_with_the_var(tmp_path):
    stg = "model.repopulse.stg_gharchive_events"
    src = "source.repopulse.silver.silver_gharchive_events"
    default = _parse(tmp_path / "default")
    uc = _parse(tmp_path / "uc", {"silver_gharchive_source": "unity_catalog"})
    assert default["nodes"][stg]["depends_on"]["nodes"] == [
        "seed.repopulse.fixture_silver_gharchive_events"
    ]
    assert uc["nodes"][stg]["depends_on"]["nodes"] == [src]
    node = uc["sources"][src]
    assert ".".join((node["database"], node["schema"], node["identifier"])) == D1_SILVER


# ---------------------------------------------------------------- profile + extra


def databricks_output() -> dict:
    profile = yaml.safe_load(PROFILES.read_text(encoding="utf-8"))["repopulse"]
    assert profile["target"] == "ci"
    return profile["outputs"]["databricks"]


@pytest.mark.parametrize(
    ("env", "schema"),
    [
        ({}, "repopulse_probe"),
        ({"DBT_DATABRICKS_SCHEMA": ""}, "repopulse_probe"),
        ({"DBT_DATABRICKS_SCHEMA": "repopulse_dbt"}, "repopulse_dbt"),
    ],
)
def test_databricks_schema_comes_from_env_with_the_probe_default(env, schema):
    out = databricks_output()
    assert out["type"] == "databricks"
    assert out["catalog"] == "workspace"
    rendered = jinja2.Environment().from_string(str(out["schema"])).render(env_var=_env_var(env))
    assert rendered == schema


@pytest.mark.parametrize(
    ("field", "name"),
    [
        ("host", "DATABRICKS_HOST"),
        ("http_path", "DATABRICKS_HTTP_PATH"),
        ("token", "DATABRICKS_TOKEN"),
    ],
)
def test_databricks_connection_fields_come_from_env(field, name):
    assert re.search(rf"env_var\(\s*'{name}'", str(databricks_output()[field]))


def test_pyproject_has_a_separate_databricks_extra():
    with PYPROJECT.open("rb") as fh:
        deps = tomllib.load(fh)["project"]["optional-dependencies"]
    assert "databricks" in deps
    assert any(re.match(r"dbt-databricks\s*>=\s*1\.8", d) for d in deps["databricks"]), deps
    assert not any(d.startswith("dbt-databricks") for d in deps["dbt"]), deps["dbt"]
