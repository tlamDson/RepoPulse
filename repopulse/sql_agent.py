"""Read-only SQL agent over the ``public`` schema, with an evaluation set (docs/sql-agent.md).

A question in English goes to an LLM behind an OpenAI-compatible API (``base_url`` and model are
configuration, e.g. a local ``llama-server``); the reply's SQL runs on a local DuckDB file built
by dbt. Read-only is enforced by layers that do not trust each other:

1. ``Guard`` parses the SQL with DuckDB's own parser (``extract_statements`` +
   ``json_serialize_sql``, no keyword regex) and fails closed: exactly one ``SELECT``; every
   relation must be a relation of the public schema (or a CTE in scope); no table functions
   except a few generators; no ``current_setting``/``getenv``.
2. ``open_readonly`` opens the file ``read_only=True`` with external access and extension
   autoload off, then locks the configuration.
3. ``run_query`` fetches at most ``max_rows`` rows (``truncated`` flag), whatever ``LIMIT`` the
   LLM wrote.

``ask`` runs the loop (prompt with the schema read from ``information_schema`` -> SQL -> guard ->
DuckDB; errors go back to the LLM, at most ``max_attempts`` times). ``run_eval`` scores the
questions of ``sql_agent_eval.toml`` by execution accuracy: the reference SQL runs on the same
database at evaluation time and ``score`` compares the result sets. ``runs_row`` turns a report
into one ``runs.csv`` row; tests only ever use fake LLMs, so they are not measurements.

CLI (errors -> stderr)::

    python -m repopulse.sql_agent ask --db ci.duckdb [--base-url URL --model M] "question"
        # exit 0 answered, 1 no valid SQL after the retries, 2 config / LLM / database error
    python -m repopulse.sql_agent eval --db ci.duckdb --report out/sql-agent-eval.json \
        [--out out/sql-agent-row.csv --git-sha <sha> --variant <v>]
        # exit 0 when every question ran; 2 (and no row) when the LLM cannot be reached

Environment: ``REPOPULSE_LLM_BASE_URL`` (default ``http://127.0.0.1:8080/v1``, the port of
``llama-server``), ``REPOPULSE_LLM_MODEL``, ``REPOPULSE_LLM_API_KEY`` (optional, never logged).
``duckdb`` is imported lazily.
"""

from __future__ import annotations

import argparse
import http.client
import json
import math
import os
import re
import statistics
import sys
import time
import tomllib
import urllib.error
import urllib.request
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Protocol

from repopulse.pipeline import write_row
from repopulse.runs_log import RunsLogError, validate_row

DEFAULT_SCHEMA = "main_public"
DEFAULT_MAX_ROWS = 200
DEFAULT_MAX_ATTEMPTS = 3
DEFAULT_TIMEOUT_S = 300.0
ANSWER_MAX_ROWS = 10_000  # cap of a reference answer of the evaluation set
DEFAULT_BASE_URL = "http://127.0.0.1:8080/v1"
DEFAULT_QUESTIONS = Path(__file__).with_name("sql_agent_eval.toml")
ENV_BASE_URL = "REPOPULSE_LLM_BASE_URL"
ENV_MODEL = "REPOPULSE_LLM_MODEL"
ENV_API_KEY = "REPOPULSE_LLM_API_KEY"
VARIANT_PREFIX = "sql-agent-"
FLOAT_ABS_TOL = 1e-6

_MOTHERDUCK_PREFIXES = ("md:", "motherduck:")
_HARDENED_CONFIG = {
    "enable_external_access": False,
    "autoinstall_known_extensions": False,
    "autoload_known_extensions": False,
    "python_enable_replacements": False,
}
# FROM items the guard understands; any other table reference is rejected.
_TABLE_REFS_OK = frozenset(
    {"BASE_TABLE", "TABLE_FUNCTION", "JOIN", "SUBQUERY", "EMPTY", "EXPRESSION_LIST", "PIVOT"}
)
_TABLE_REFS_KNOWN = _TABLE_REFS_OK | {"SHOW_REF", "COLUMN_DATA", "DELIM_GET", "CTE", "INVALID"}
_TABLE_FUNCTIONS_OK = frozenset({"range", "generate_series", "unnest"})
_SCALAR_DENIED = frozenset({"current_setting", "getenv", "which_secret"})
_SCALAR_DENIED_PREFIXES = ("duckdb_", "pragma_")
_THINK = re.compile(r"<think>.*?</think>", re.DOTALL | re.IGNORECASE)
_FENCE = re.compile(r"```[ \t]*(?:sql)?[ \t]*\n?(.*?)```", re.DOTALL | re.IGNORECASE)
_SQL_FENCE = re.compile(r"```[ \t]*sql[ \t]*\n?(.*?)```", re.DOTALL | re.IGNORECASE)


class AgentConfigError(ValueError):
    """Bad configuration: database target, model, question file, CLI flags."""


class SqlRejected(ValueError):
    """The guard refused a SQL text; the message says why."""


class LLMError(RuntimeError):
    """The LLM endpoint failed (connection, HTTP status, malformed reply). Never holds the key."""


class LLM(Protocol):
    def complete(self, messages: Sequence[Mapping[str, str]]) -> str: ...


# ---------------------------------------------------------------- connection


def open_readonly(path: str | os.PathLike[str], schema: str = DEFAULT_SCHEMA) -> Any:
    """Read-only, locked-down DuckDB connection on a local file; unqualified names -> ``schema``.

    MotherDuck targets (``md:``, ``motherduck:``) are refused: only local files are supported.
    """
    text = os.fspath(path)
    if text.strip().lower().startswith(_MOTHERDUCK_PREFIXES):
        raise AgentConfigError(
            f"MotherDuck target {text!r} is not supported: the SQL agent only opens a local "
            "DuckDB file"
        )
    file = Path(text)
    if text.strip() in ("", ":memory:") or not file.is_file():
        raise AgentConfigError(f"DuckDB file not found (local file required): {text}")

    import duckdb

    con = _connect_hardened(duckdb, file)
    try:
        found = con.execute(
            "SELECT count(*) FROM information_schema.schemata "
            "WHERE catalog_name = current_database() AND lower(schema_name) = lower(?)",
            [schema],
        ).fetchone()[0]
        if not found:
            raise AgentConfigError(f"schema {schema!r} not found in {file}")
        con.execute(f"SET search_path = {_literal(schema)}")
        con.execute("SET lock_configuration = true")
    except BaseException:
        con.close()
        raise
    return con


def _connect_hardened(duckdb: Any, file: Path) -> Any:
    config = dict(_HARDENED_CONFIG)
    try:
        return duckdb.connect(str(file), read_only=True, config=config)
    except duckdb.Error as exc:
        if "python_enable_replacements" not in str(exc):
            raise AgentConfigError(f"cannot open {file}: {exc}") from None
    # Replacement scans are a client option some versions may not expose; the guard rejects
    # unknown names anyway.
    config.pop("python_enable_replacements")
    try:
        return duckdb.connect(str(file), read_only=True, config=config)
    except duckdb.Error as exc:
        raise AgentConfigError(f"cannot open {file}: {exc}") from None


def _literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _ident(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


# ---------------------------------------------------------------- guard


@dataclass(frozen=True)
class Guard:
    """Fail-closed read-only check of one SQL text against the relations of ``schema``."""

    conn: Any = field(repr=False)
    schema: str
    catalog: str
    tables: frozenset[str]
    unqualified_ok: bool

    @classmethod
    def from_connection(cls, conn: Any, schema: str = DEFAULT_SCHEMA) -> Guard:
        """Allowed relations = tables and views of ``schema`` in the connection's database.

        Unqualified names are allowed only when the connection resolves them to ``schema``
        (``open_readonly`` sets the search path).
        """
        catalog, current = conn.execute("SELECT current_database(), current_schema()").fetchone()
        rows = conn.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_catalog = current_database() AND lower(table_schema) = lower(?)",
            [schema],
        ).fetchall()
        return cls(
            conn=conn,
            schema=schema.lower(),
            catalog=str(catalog).lower(),
            tables=frozenset(str(name).lower() for (name,) in rows),
            unqualified_ok=str(current).lower() == schema.lower(),
        )

    def check(self, sql: str) -> None:
        """Return when ``sql`` is one read of the allowed relations, else raise ``SqlRejected``."""
        import duckdb

        if not isinstance(sql, str) or not sql.strip():
            raise SqlRejected("empty SQL")
        try:
            statements = self.conn.extract_statements(sql)
        except duckdb.Error as exc:
            raise SqlRejected(f"SQL does not parse: {_first_line(exc)}") from None
        if len(statements) != 1:
            raise SqlRejected(f"expected exactly one statement, got {len(statements)}")
        kind = statements[0].type
        if kind != duckdb.StatementType.SELECT:
            raise SqlRejected(f"only SELECT is allowed, got {getattr(kind, 'name', kind)}")
        try:
            (text,) = self.conn.execute("SELECT json_serialize_sql(?)", [sql]).fetchone()
            tree = json.loads(text)
        except (duckdb.Error, TypeError, ValueError) as exc:
            raise SqlRejected(f"SQL cannot be analysed: {_first_line(exc)}") from None
        if not isinstance(tree, dict) or tree.get("error") is not False:
            why = tree.get("error_message") if isinstance(tree, dict) else None
            raise SqlRejected(f"SQL cannot be analysed: {why or 'unexpected parser output'}")
        found = tree.get("statements")
        if not isinstance(found, list) or len(found) != 1:
            raise SqlRejected("expected exactly one SELECT statement")
        try:
            self._walk(found[0], frozenset())
        except RecursionError:
            raise SqlRejected("SQL is nested too deeply to analyse") from None

    # A CTE is visible in the whole subtree of the query node that defines it (its body, later
    # CTEs, the main query), never outside it.
    def _walk(self, node: Any, ctes: frozenset[str]) -> None:
        if isinstance(node, list):
            for item in node:
                self._walk(item, ctes)
            return
        if not isinstance(node, dict):
            return
        if "cte_map" in node:
            ctes = ctes | _cte_names(node["cte_map"])
        if "from_table" in node and not (
            isinstance(node["from_table"], dict) and _is_table_ref(node["from_table"])
        ):
            raise SqlRejected("unrecognised FROM clause")
        if _is_table_ref(node):
            self._check_table_ref(node, ctes)
        name = node.get("function_name")
        if name is not None:
            _check_scalar(name)
        for value in node.values():
            self._walk(value, ctes)

    def _check_table_ref(self, ref: dict, ctes: frozenset[str]) -> None:
        kind = ref.get("type")
        if kind not in _TABLE_REFS_OK:
            raise SqlRejected(f"FROM item of type {kind} is not allowed")
        if kind == "BASE_TABLE":
            self._check_relation(ref, ctes)
        elif kind == "TABLE_FUNCTION":
            function = ref.get("function")
            name = function.get("function_name") if isinstance(function, dict) else None
            if not isinstance(name, str) or name.lower() not in _TABLE_FUNCTIONS_OK:
                raise SqlRejected(f"table function {name!r} is not allowed")

    def _check_relation(self, ref: dict, ctes: frozenset[str]) -> None:
        parts = [ref.get(k) for k in ("catalog_name", "schema_name", "table_name")]
        if not all(isinstance(p, str) for p in parts) or not parts[2]:
            raise SqlRejected("unrecognised table reference")
        catalog, schema, table = (p.lower() for p in parts)
        shown = ".".join(p for p in parts if p)
        if not catalog and not schema:
            if table in ctes or (self.unqualified_ok and table in self.tables):
                return
            raise SqlRejected(f"relation {shown!r} is not in schema {self.schema}")
        if schema != self.schema or (catalog and catalog != self.catalog):
            raise SqlRejected(f"relation {shown!r} is outside schema {self.schema}")
        if table not in self.tables:
            raise SqlRejected(f"relation {shown!r} does not exist in schema {self.schema}")


def _is_table_ref(node: dict) -> bool:
    kind = node.get("type")
    if not isinstance(kind, str) or "class" in node:
        return False
    return (
        kind in _TABLE_REFS_KNOWN or "table_name" in node or ("alias" in node and "sample" in node)
    )


def _cte_names(cte_map: Any) -> frozenset[str]:
    entries = cte_map.get("map") if isinstance(cte_map, dict) else None
    if not isinstance(entries, list):
        raise SqlRejected("unrecognised WITH clause")
    names = set()
    for entry in entries:
        key = entry.get("key") if isinstance(entry, dict) else None
        if not isinstance(key, str):
            raise SqlRejected("unrecognised WITH clause")
        names.add(key.lower())
    return frozenset(names)


def _check_scalar(name: Any) -> None:
    if not isinstance(name, str):
        raise SqlRejected("unrecognised function call")
    lowered = name.lower()
    if lowered in _SCALAR_DENIED or lowered.startswith(_SCALAR_DENIED_PREFIXES):
        raise SqlRejected(f"function {name!r} is not allowed")


def _first_line(exc: BaseException) -> str:
    text = str(exc).strip()
    return text.splitlines()[0] if text else type(exc).__name__


# ---------------------------------------------------------------- run


@dataclass(frozen=True)
class QueryResult:
    columns: list[str]
    rows: list[tuple]
    truncated: bool


def run_query(
    conn: Any, sql: str, max_rows: int = DEFAULT_MAX_ROWS, guard: Guard | None = None
) -> QueryResult:
    """Guard ``sql``, run it and keep at most ``max_rows`` rows (``truncated`` when more)."""
    if max_rows < 1:
        raise ValueError("max_rows must be >= 1")
    (guard or Guard.from_connection(conn)).check(sql)
    cursor = conn.execute(sql)
    columns = [d[0] for d in cursor.description] if cursor.description else []
    rows = cursor.fetchmany(max_rows + 1)
    return QueryResult(columns, [tuple(r) for r in rows[:max_rows]], len(rows) > max_rows)


# ---------------------------------------------------------------- LLM client


@dataclass(frozen=True)
class OpenAICompatClient:
    """``POST {base_url}/chat/completions`` of an OpenAI-compatible server (llama-server...)."""

    base_url: str
    model: str
    api_key: str | None = field(default=None, repr=False)
    timeout_s: float = DEFAULT_TIMEOUT_S

    def complete(self, messages: Sequence[Mapping[str, str]]) -> str:
        url = self.base_url.rstrip("/") + "/chat/completions"
        body = json.dumps(
            {"model": self.model, "messages": list(messages), "temperature": 0}
        ).encode("utf-8")
        headers = {"Content-Type": "application/json", "Accept": "application/json"}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        request = urllib.request.Request(url, data=body, headers=headers, method="POST")
        try:
            with urllib.request.urlopen(request, timeout=self.timeout_s) as response:
                raw = response.read()
        except urllib.error.HTTPError as exc:
            detail = self._redact(exc.read()[:200].decode("utf-8", "replace"))
            raise LLMError(f"{url}: HTTP {exc.code} {detail}".rstrip()) from None
        except (urllib.error.URLError, OSError, http.client.HTTPException) as exc:
            reason = getattr(exc, "reason", exc)
            raise LLMError(f"{url}: {self._redact(str(reason))}") from None
        try:
            content = json.loads(raw)["choices"][0]["message"]["content"]
        except (ValueError, KeyError, IndexError, TypeError):
            raise LLMError(f"{url}: reply is not a chat completion") from None
        if not isinstance(content, str):
            raise LLMError(f"{url}: reply has no text content")
        return content

    def _redact(self, text: str) -> str:
        return text.replace(self.api_key, "***") if self.api_key else text


def client_from_env(
    env: Mapping[str, str] | None = None,
    base_url: str | None = None,
    model: str | None = None,
    timeout_s: float = DEFAULT_TIMEOUT_S,
) -> OpenAICompatClient:
    """Client from flags, else ``REPOPULSE_LLM_*`` variables (never read from a ``.env`` file)."""
    env = os.environ if env is None else env
    chosen = model or env.get(ENV_MODEL) or ""
    if not chosen.strip():
        raise AgentConfigError(f"no model: pass --model or set {ENV_MODEL}")
    return OpenAICompatClient(
        base_url=base_url or env.get(ENV_BASE_URL) or DEFAULT_BASE_URL,
        model=chosen,
        api_key=env.get(ENV_API_KEY) or None,
        timeout_s=timeout_s,
    )


def extract_sql(text: str) -> str:
    """SQL of an LLM reply: drop ``<think>`` blocks, keep the last ```sql fence, drop ``;``."""
    text = _THINK.sub("", text)
    if "</think>" in text.lower():  # reasoning without its opening tag
        text = text[text.lower().rindex("</think>") + len("</think>") :]
    blocks = _SQL_FENCE.findall(text) or _FENCE.findall(text)
    sql = blocks[-1] if blocks else text
    return sql.strip().rstrip(";").strip()


# ---------------------------------------------------------------- agent


RULES = """\
You are a SQL assistant for RepoPulse, an analytics database of GitHub activity of open source
data projects. Answer the question with exactly one read-only DuckDB SQL query.

Rules:
- Write a single SELECT statement (WITH ... SELECT is fine), DuckDB dialect.
- Use only the tables listed below, in schema {schema}; write them as {schema}.<table>.
- Never write INSERT, UPDATE, DELETE, CREATE, DROP, ALTER, COPY, ATTACH, SET, PRAGMA or any
  statement other than SELECT. No table functions such as read_csv or read_parquet, no file
  paths, no current_setting or getenv.
- Return exactly the columns the question asks for.
- Reply with the SQL inside one ```sql code block and nothing else.
"""


def schema_description(conn: Any, schema: str = DEFAULT_SCHEMA) -> str:
    """One line per relation of ``schema``: ``schema.table(col TYPE, ...)``."""
    rows = conn.execute(
        "SELECT table_name, column_name, data_type FROM information_schema.columns "
        "WHERE table_catalog = current_database() AND lower(table_schema) = lower(?) "
        "ORDER BY table_name, ordinal_position",
        [schema],
    ).fetchall()
    tables: dict[str, list[str]] = {}
    for table, column, data_type in rows:
        tables.setdefault(table, []).append(f"{column} {data_type}")
    return "\n".join(f"- {schema}.{t}({', '.join(cols)})" for t, cols in tables.items())


def system_prompt(conn: Any, schema: str = DEFAULT_SCHEMA) -> str:
    return (
        RULES.format(schema=schema)
        + f"\nTables of {schema}:\n"
        + schema_description(conn, schema)
        + "\n"
    )


@dataclass
class AgentResult:
    question: str
    sql: str | None
    columns: list[str]
    rows: list[tuple]
    truncated: bool
    attempts: int
    error: str | None
    seconds: float


def ask(
    question: str,
    conn: Any,
    llm: LLM,
    max_attempts: int = DEFAULT_MAX_ATTEMPTS,
    max_rows: int = DEFAULT_MAX_ROWS,
    guard: Guard | None = None,
    schema: str = DEFAULT_SCHEMA,
    prompt: str | None = None,
) -> AgentResult:
    """Ask the LLM for SQL, guard and run it, feed errors back; ``LLMError`` propagates."""
    import duckdb

    if max_attempts < 1:
        raise ValueError("max_attempts must be >= 1")
    start = time.perf_counter()
    guard = guard or Guard.from_connection(conn, schema)
    messages = [
        {"role": "system", "content": prompt or system_prompt(conn, schema)},
        {"role": "user", "content": question},
    ]
    sql: str | None = None
    error: str | None = None
    for attempt in range(1, max_attempts + 1):
        sql = extract_sql(llm.complete(messages))
        try:
            result = run_query(conn, sql, max_rows=max_rows, guard=guard)
        except SqlRejected as exc:
            error = f"rejected by the read-only guard: {exc}"
        except duckdb.Error as exc:
            error = f"DuckDB error: {_first_line(exc)}"
        else:
            return AgentResult(
                question,
                sql,
                result.columns,
                result.rows,
                result.truncated,
                attempt,
                None,
                time.perf_counter() - start,
            )
        messages.append({"role": "assistant", "content": f"```sql\n{sql}\n```"})
        messages.append(
            {
                "role": "user",
                "content": f"That SQL was {error}\nReply with one corrected SELECT in a "
                "```sql block.",
            }
        )
    return AgentResult(
        question, sql, [], [], False, max_attempts, error, time.perf_counter() - start
    )


# ---------------------------------------------------------------- scoring


def _is_number(value: Any) -> bool:
    return isinstance(value, int | float | Decimal) and not isinstance(value, bool)


def _equal(a: Any, b: Any) -> bool:
    if a is None or b is None:
        return a is None and b is None
    if isinstance(a, bool) or isinstance(b, bool):
        return isinstance(a, bool) and isinstance(b, bool) and a == b
    if _is_number(a) and _is_number(b):
        if isinstance(a, int) and isinstance(b, int):
            return a == b
        x, y = float(a), float(b)
        if math.isnan(x) or math.isnan(y):
            return math.isnan(x) and math.isnan(y)
        return math.isclose(x, y, rel_tol=1e-9, abs_tol=FLOAT_ABS_TOL)
    if isinstance(a, list | tuple) and isinstance(b, list | tuple):
        return len(a) == len(b) and all(_equal(x, y) for x, y in zip(a, b, strict=True))
    if _is_number(a) or _is_number(b):
        return False
    return a == b


def _bucket(value: Any) -> Any:
    """Hashable key equal for every pair of values ``_equal`` may call equal."""
    if value is None:
        return ("null",)
    if isinstance(value, bool):
        return ("bool", value)
    if _is_number(value):
        return ("num",)
    if isinstance(value, list | tuple):
        return ("seq", len(value))
    try:
        hash(value)
    except TypeError:
        return ("other", type(value).__name__)
    return ("val", value)


def _same_multiset(left: Sequence[tuple], right: Sequence[tuple]) -> bool:
    if len(left) != len(right):
        return False
    buckets: dict[tuple, list[tuple]] = {}
    for row in right:
        buckets.setdefault(tuple(_bucket(v) for v in row), []).append(row)
    for row in left:
        candidates = buckets.get(tuple(_bucket(v) for v in row), [])
        for i, other in enumerate(candidates):
            if all(_equal(x, y) for x, y in zip(row, other, strict=True)):
                del candidates[i]
                break
        else:
            return False
    return True


def _same_sequence(left: Sequence[tuple], right: Sequence[tuple]) -> bool:
    return len(left) == len(right) and all(
        all(_equal(x, y) for x, y in zip(a, b, strict=True))
        for a, b in zip(left, right, strict=True)
    )


def score(expected: Sequence[Sequence], actual: Sequence[Sequence], ordered: bool = False) -> bool:
    """True when some one-to-one map of expected columns onto actual columns gives equal rows.

    Column names and order are ignored and extra actual columns are allowed. Rows compare as a
    multiset, or as a sequence when ``ordered``. Numbers compare with a tolerance (``1 == 1.0``),
    ``NULL`` equals ``NULL``, a bool is not a number.
    """
    exp = [tuple(r) for r in expected]
    act = [tuple(r) for r in actual]
    if len(exp) != len(act):
        return False
    if not exp:
        return True
    width, available = len(exp[0]), len(act[0])
    if width > available:
        return False
    same = _same_sequence if ordered else _same_multiset

    def column(rows: list[tuple], i: int) -> list[tuple]:
        return [(r[i],) for r in rows]

    candidates = [
        [j for j in range(available) if same(column(exp, i), column(act, j))] for i in range(width)
    ]
    if not all(candidates):
        return False

    def assign(i: int, chosen: list[int]) -> bool:
        if i == width:
            return same(exp, [tuple(r[j] for j in chosen) for r in act])
        return any(assign(i + 1, [*chosen, j]) for j in candidates[i] if j not in chosen)

    return assign(0, [])


def is_correct(expected: Sequence[Sequence], result: AgentResult, ordered: bool) -> bool:
    """Wrong when the agent failed or its rows were truncated, else ``score``."""
    if result.error is not None or result.truncated:
        return False
    return score(expected, result.rows, ordered=ordered)


# ---------------------------------------------------------------- evaluation set


@dataclass(frozen=True)
class Question:
    id: str
    question: str
    sql: str
    ordered: bool = False


_QUESTION_KEYS = {"id", "question", "sql", "ordered"}


def load_questions(path: str | os.PathLike[str] | None = None) -> list[Question]:
    """``[[question]]`` entries of a TOML file (default: ``sql_agent_eval.toml`` of the package)."""
    path = Path(path) if path is not None else DEFAULT_QUESTIONS
    try:
        data = tomllib.loads(path.read_text(encoding="utf-8"))
    except (OSError, tomllib.TOMLDecodeError) as exc:
        raise AgentConfigError(f"cannot read questions {path}: {exc}") from None
    entries = data.get("question")
    if not isinstance(entries, list) or not entries:
        raise AgentConfigError(f"{path}: no [[question]] entries")
    questions: list[Question] = []
    for n, entry in enumerate(entries, 1):
        if not isinstance(entry, dict):
            raise AgentConfigError(f"{path}: question #{n} is not a table")
        unknown = sorted(set(entry) - _QUESTION_KEYS)
        if unknown:
            raise AgentConfigError(f"{path}: question #{n} has unknown key(s) {unknown}")
        for key in ("id", "question", "sql"):
            if not isinstance(entry.get(key), str) or not entry[key].strip():
                raise AgentConfigError(f"{path}: question #{n} needs a non-empty string {key!r}")
        ordered = entry.get("ordered", False)
        if not isinstance(ordered, bool):
            raise AgentConfigError(f"{path}: question {entry['id']!r}: ordered must be a bool")
        questions.append(Question(entry["id"], entry["question"], entry["sql"], ordered))
    ids = [q.id for q in questions]
    duplicates = sorted({i for i in ids if ids.count(i) > 1})
    if duplicates:
        raise AgentConfigError(f"{path}: duplicate question id(s) {duplicates}")
    return questions


def run_eval(
    conn: Any,
    llm: LLM,
    questions: Sequence[Question],
    *,
    model: str,
    base_url: str,
    max_attempts: int = DEFAULT_MAX_ATTEMPTS,
    max_rows: int = DEFAULT_MAX_ROWS,
    schema: str = DEFAULT_SCHEMA,
    progress: Callable[[dict], None] | None = None,
) -> dict:
    """Execution accuracy of the agent on ``questions``; ``LLMError`` aborts the whole run.

    The reference SQL of each question runs (through the same guard) on ``conn`` now, so the
    answer follows the data. The agent's row cap is raised to the size of the reference answer
    so the cap alone never makes an answer wrong.
    """
    guard = Guard.from_connection(conn, schema)
    prompt = system_prompt(conn, schema)
    results = []
    for q in questions:
        expected = run_query(conn, q.sql, max_rows=ANSWER_MAX_ROWS, guard=guard)
        if expected.truncated:
            raise AgentConfigError(f"reference answer of {q.id!r} has over {ANSWER_MAX_ROWS} rows")
        answer = ask(
            q.question,
            conn,
            llm,
            max_attempts=max_attempts,
            max_rows=max(max_rows, len(expected.rows)),
            guard=guard,
            schema=schema,
            prompt=prompt,
        )
        row = {
            "id": q.id,
            "question": q.question,
            "correct": is_correct(expected.rows, answer, q.ordered),
            "sql": answer.sql,
            "error": answer.error,
            "attempts": answer.attempts,
            "seconds": answer.seconds,
        }
        results.append(row)
        if progress is not None:
            progress(row)
    seconds = [r["seconds"] for r in results]
    correct = sum(1 for r in results if r["correct"])
    return {
        "model": model,
        "base_url": base_url,
        "schema": schema,
        "max_attempts": max_attempts,
        "max_rows": max_rows,
        "results": results,
        "summary": {
            "questions": len(results),
            "correct": correct,
            "accuracy": correct / len(results) if results else 0.0,
            "total_seconds": sum(seconds),
            "p50_seconds": statistics.median(seconds) if seconds else 0.0,
        },
    }


# ---------------------------------------------------------------- runs.csv row


def variant_for_model(model: str) -> str:
    """``sql-agent-<slug>``: lower case, characters outside ``[a-z0-9_-]`` become ``-``."""
    slug = re.sub(r"-+", "-", re.sub(r"[^a-z0-9_-]", "-", model.lower())).strip("-")
    if not slug:
        raise AgentConfigError(f"cannot derive a variant from model {model!r}: pass --variant")
    return VARIANT_PREFIX + slug


def runs_row(
    report: Mapping[str, Any],
    *,
    git_sha: str,
    data_date: date | str,
    variant: str | None = None,
    run_at: datetime | str | None = None,
) -> dict[str, str]:
    """The ``runs.csv`` row of an eval report, validated by ``runs_log.validate_row``."""
    results = report["results"]
    seconds = [float(r["seconds"]) for r in results]
    correct = sum(1 for r in results if r["correct"])
    p50 = statistics.median(seconds) if seconds else 0.0
    model = str(report["model"])
    return validate_row(
        {
            "run_at": run_at or datetime.now(UTC).isoformat(timespec="seconds"),
            "git_sha": git_sha,
            "variant": variant or variant_for_model(model),
            "data_date": data_date,
            "total_seconds": round(sum(seconds), 3),
            "row_count": len(results),
            "notes": f"accuracy={correct}/{len(results)}; p50_question_s={p50:.3f}; model={model}",
        }
    )


def data_date_of(conn: Any, schema: str = DEFAULT_SCHEMA) -> date:
    """Newest ``event_date`` of ``<schema>.fct_events`` (the data day of an eval run)."""
    (value,) = conn.execute(f"SELECT max(event_date) FROM {_ident(schema)}.fct_events").fetchone()
    if value is None:
        raise AgentConfigError(f"{schema}.fct_events is empty: no data_date for the runs.csv row")
    return value


# ---------------------------------------------------------------- CLI


def _print_result(result: AgentResult, out=None) -> None:
    out = out or sys.stdout
    print(f"-- attempts: {result.attempts}, seconds: {result.seconds:.3f}", file=out)
    print(result.sql or "", file=out)
    if result.error is not None:
        return
    print("", file=out)
    print("\t".join(result.columns), file=out)
    for row in result.rows:
        print("\t".join("NULL" if v is None else str(v) for v in row), file=out)
    note = " (truncated)" if result.truncated else ""
    print(f"-- {len(result.rows)} row(s){note}", file=out)


def _add_common(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--db", required=True, help="local DuckDB file built by dbt")
    parser.add_argument("--schema", default=DEFAULT_SCHEMA)
    parser.add_argument("--base-url", default=None, help=f"default: ${ENV_BASE_URL}")
    parser.add_argument("--model", default=None, help=f"default: ${ENV_MODEL}")
    parser.add_argument("--max-rows", type=int, default=DEFAULT_MAX_ROWS)
    parser.add_argument("--max-attempts", type=int, default=DEFAULT_MAX_ATTEMPTS)
    parser.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT_S, help="seconds")


def _check_row_flags(args: argparse.Namespace) -> None:
    if args.out is None:
        return
    if not args.git_sha:
        raise AgentConfigError("--out needs --git-sha")
    validate_row(
        {
            "run_at": datetime.now(UTC),
            "git_sha": args.git_sha,
            "variant": args.variant or "sql-agent",
            "data_date": "2000-01-01",
            "total_seconds": 0.0,
        }
    )


def _cmd_ask(args: argparse.Namespace, llm: OpenAICompatClient, conn: Any) -> int:
    result = ask(
        args.question,
        conn,
        llm,
        max_attempts=args.max_attempts,
        max_rows=args.max_rows,
        schema=args.schema,
    )
    _print_result(result)
    if result.error is not None:
        print(f"ERROR: no valid SQL after {result.attempts} attempt(s): {result.error}",
              file=sys.stderr)  # fmt: skip
        return 1
    return 0


def _cmd_eval(args: argparse.Namespace, llm: OpenAICompatClient, conn: Any) -> int:
    questions = load_questions(args.questions)

    def progress(row: dict) -> None:
        mark = "ok " if row["correct"] else "BAD"
        print(f"{mark} {row['id']} ({row['seconds']:.2f}s, {row['attempts']} attempt(s))")

    report = run_eval(
        conn,
        llm,
        questions,
        model=llm.model,
        base_url=llm.base_url,
        max_attempts=args.max_attempts,
        max_rows=args.max_rows,
        schema=args.schema,
        progress=progress,
    )
    row = None
    if args.out is not None:
        row = runs_row(
            report,
            git_sha=args.git_sha,
            data_date=data_date_of(conn, args.schema),
            variant=args.variant,
        )
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(report, indent=2, default=str) + "\n", encoding="utf-8")
    if row is not None:
        write_row(args.out, row)
    summary = report["summary"]
    print(f"accuracy {summary['correct']}/{summary['questions']} -> {args.report}")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m repopulse.sql_agent",
        description="Read-only SQL agent over the public schema (OpenAI-compatible LLM).",
    )
    sub = parser.add_subparsers(dest="command", required=True)
    ask_p = sub.add_parser("ask", help="answer one question")
    _add_common(ask_p)
    ask_p.add_argument("question")
    eval_p = sub.add_parser("eval", help="score the evaluation set")
    _add_common(eval_p)
    eval_p.add_argument("--questions", type=Path, default=DEFAULT_QUESTIONS)
    eval_p.add_argument("--report", type=Path, required=True, help="JSON report to write")
    eval_p.add_argument("--out", type=Path, default=None, help="runs.csv row (artifact CSV)")
    eval_p.add_argument("--git-sha", default=None)
    eval_p.add_argument("--variant", default=None, help="default: sql-agent-<model slug>")
    args = parser.parse_args(argv)

    import duckdb

    conn = None
    try:
        if args.max_rows < 1 or args.max_attempts < 1:
            raise AgentConfigError("--max-rows and --max-attempts must be >= 1")
        if args.command == "eval":
            _check_row_flags(args)
        llm = client_from_env(base_url=args.base_url, model=args.model, timeout_s=args.timeout)
        conn = open_readonly(args.db, args.schema)
        command = _cmd_ask if args.command == "ask" else _cmd_eval
        return command(args, llm, conn)
    except (AgentConfigError, LLMError, SqlRejected, RunsLogError, duckdb.Error, OSError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2
    finally:
        if conn is not None:
            conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
