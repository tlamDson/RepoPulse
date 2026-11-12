"""Read-only SQL agent over the ``public`` schema (repopulse.sql_agent, docs/sql-agent.md).

Every test here runs on a small DuckDB file built in ``tmp_path`` (``main_public``,
``main_gold``, ``main_staging`` and a table in ``main``), so no dbt is needed. The LLM is always
a fake: either ``FakeLLM`` (scripted replies) or a stub OpenAI-compatible HTTP server on
``127.0.0.1``. No real service is called.

The module is imported in each test body (``_agent()``) so a missing module fails every test on
its own instead of breaking collection.
"""

from __future__ import annotations

import copy
import csv
import hashlib
import importlib
import json
import socket
import threading
from datetime import date
from decimal import Decimal
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import duckdb
import pytest

PUBLIC = "main_public"
DB_NAME = "agent"  # file stem = DuckDB catalog name
PUBLIC_REPOS = 3
SECRET_KEY = "sk-test-do-not-leak-1234567890"


def _agent():
    """repopulse.sql_agent, imported in the test body so a missing module fails the test."""
    return importlib.import_module("repopulse.sql_agent")


# ---------------------------------------------------------------- fixtures


def _build_db(path: Path) -> Path:
    con = duckdb.connect(str(path))
    try:
        for schema in (PUBLIC, "main_gold", "main_staging"):
            con.execute(f"CREATE SCHEMA {schema}")
        con.execute(
            f"CREATE TABLE {PUBLIC}.dim_repo (repo_key VARCHAR, repo_name VARCHAR, "
            "category VARCHAR)"
        )
        con.execute(
            f"INSERT INTO {PUBLIC}.dim_repo VALUES "
            "('apache/spark', 'apache/spark', 'compute'), "
            "('duckdb/duckdb', 'duckdb/duckdb', 'warehouse'), "
            "('apache/kafka', 'apache/kafka', 'streaming')"
        )
        con.execute(
            f"CREATE TABLE {PUBLIC}.fct_events (event_id VARCHAR, event_type VARCHAR, "
            "event_date DATE, repo_key VARCHAR, updated_at TIMESTAMP)"
        )
        con.execute(
            f"INSERT INTO {PUBLIC}.fct_events VALUES "
            "('1', 'ForkEvent', DATE '2026-01-05', 'apache/spark', TIMESTAMP '2026-01-05 10:00'), "
            "('2', 'PullRequestEvent', DATE '2026-01-06', 'apache/spark', "
            "TIMESTAMP '2026-01-06 11:00'), "
            "('3', 'IssuesEvent', DATE '2026-01-07', 'duckdb/duckdb', "
            "TIMESTAMP '2026-01-07 12:00')"
        )
        con.execute("CREATE TABLE main_gold.dim_user (user_key VARCHAR, actor_login VARCHAR)")
        con.execute("INSERT INTO main_gold.dim_user VALUES ('u1', 'raw-login-alice')")
        con.execute("CREATE TABLE main_staging.stg_events (event_id VARCHAR, raw_payload VARCHAR)")
        con.execute("CREATE TABLE main.fixture_secret (secret_value VARCHAR)")
        con.execute("INSERT INTO main.fixture_secret VALUES ('hidden')")
        # Same name as a public table, other rows: unqualified names must resolve to public.
        con.execute("CREATE TABLE main.dim_repo (repo_key VARCHAR)")
        con.execute("INSERT INTO main.dim_repo VALUES ('main-only')")
    finally:
        con.close()
    return path


@pytest.fixture
def db_path(tmp_path) -> Path:
    return _build_db(tmp_path / f"{DB_NAME}.duckdb")


@pytest.fixture
def conn(db_path):
    con = _agent().open_readonly(db_path)
    yield con
    con.close()


@pytest.fixture
def guard(conn):
    return _agent().Guard.from_connection(conn)


class FakeLLM:
    """Scripted LLM: returns ``replies`` in order (the last one repeats), records the messages."""

    def __init__(self, replies):
        self.replies = list(replies)
        self.calls: list[list[dict]] = []

    def complete(self, messages):
        self.calls.append(copy.deepcopy(list(messages)))
        index = min(len(self.calls) - 1, len(self.replies) - 1)
        return self.replies[index]


class OracleLLM:
    """Answers each question with the SQL of ``answers`` (question text -> reply)."""

    def __init__(self, answers):
        self.answers = answers
        self.calls = 0

    def complete(self, messages):
        self.calls += 1
        text = "\n".join(m["content"] for m in messages if m["role"] == "user")
        for question, reply in self.answers.items():
            if question in text:
                return reply
        raise AssertionError(f"no scripted answer for: {text!r}")


class _Stub:
    def __init__(self):
        self.requests: list[dict] = []
        self.status = 200
        self.raw: bytes | None = None
        self.reply = "```sql\nSELECT 1\n```"
        self.server: ThreadingHTTPServer | None = None

    @property
    def url(self) -> str:
        host, port = self.server.server_address[:2]
        return f"http://{host}:{port}/v1"


@pytest.fixture
def stub():
    """OpenAI-compatible chat server on 127.0.0.1 (random port) running in a thread."""
    state = _Stub()

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):  # noqa: N802 - http.server API
            length = int(self.headers.get("Content-Length", "0"))
            body = self.rfile.read(length)
            state.requests.append(
                {"path": self.path, "headers": dict(self.headers), "body": json.loads(body)}
            )
            reply = state.reply(state.requests[-1]["body"]) if callable(state.reply) else None
            if state.raw is not None:
                payload = state.raw
            else:
                content = reply if reply is not None else state.reply
                payload = json.dumps(
                    {"choices": [{"message": {"role": "assistant", "content": content}}]}
                ).encode()
            self.send_response(state.status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def log_message(self, *args):
            return None

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    state.server = server
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield state
    server.shutdown()
    server.server_close()
    thread.join(timeout=5)


def _free_port() -> int:
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    return port


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


# ---------------------------------------------------------------- guard: statement type

WRITE_OR_SESSION_STATEMENTS = [
    "INSERT INTO main_public.dim_repo VALUES ('a', 'b', 'c')",
    "UPDATE main_public.dim_repo SET category = 'x'",
    "DELETE FROM main_public.dim_repo",
    "MERGE INTO main_public.dim_repo t USING main_public.dim_repo s ON t.repo_key = s.repo_key "
    "WHEN MATCHED THEN DELETE",
    "CREATE TABLE t AS SELECT 1",
    "CREATE VIEW v AS SELECT * FROM dim_repo",
    "CREATE OR REPLACE MACRO m() AS 1",
    "DROP TABLE main_public.dim_repo",
    "ALTER TABLE main_public.dim_repo ADD COLUMN x INTEGER",
    "TRUNCATE main_public.dim_repo",
    "COPY main_public.dim_repo TO 'out.csv'",
    "COPY (SELECT * FROM dim_repo) TO 'out.csv'",
    "ATTACH 'other.duckdb' AS other",
    "DETACH agent",
    "USE main_gold",
    "SET enable_external_access = true",
    "SET search_path = 'main_gold'",
    "RESET search_path",
    "PRAGMA table_info('dim_repo')",
    "CALL pragma_version()",
    "INSTALL httpfs",
    "LOAD httpfs",
    "EXPORT DATABASE 'out_dir'",
    "IMPORT DATABASE 'out_dir'",
    "CHECKPOINT",
    "VACUUM",
    "BEGIN TRANSACTION",
    "COMMIT",
    "EXPLAIN ANALYZE SELECT * FROM dim_repo",
    "EXPLAIN SELECT * FROM dim_repo",
    "SHOW TABLES",
    "DESCRIBE dim_repo",
    "SUMMARIZE dim_repo",
    "PIVOT dim_repo ON category",
    "SELECT 1; DROP TABLE main_public.dim_repo",
    "SELECT 1; SELECT 2",
    "",
    "   ",
    "-- only a comment",
    "SELEC * FROM dim_repo",
]


@pytest.mark.parametrize("sql", WRITE_OR_SESSION_STATEMENTS)
def test_guard_rejects_anything_but_one_select(guard, sql):
    sa = _agent()
    with pytest.raises(sa.SqlRejected):
        guard.check(sql)


ALLOWED = [
    "SELECT * FROM dim_repo",
    "SELECT repo_name FROM main_public.dim_repo",
    'SELECT * FROM "MAIN_PUBLIC"."DIM_REPO"',
    "SELECT * FROM Main_Public.Dim_Repo",
    f"SELECT * FROM {DB_NAME}.main_public.dim_repo",
    f'SELECT * FROM "{DB_NAME.upper()}".main_public.dim_repo',
    "WITH c AS (SELECT * FROM dim_repo) SELECT * FROM c",
    "SELECT 'drop table x' AS s FROM dim_repo",
    "SELECT repo_name FROM dim_repo -- delete everything",
    "/* delete from dim_repo */ SELECT updated_at FROM fct_events",
    "SELECT max(updated_at) AS last_update FROM fct_events",
    "SELECT * FROM range(3)",
    "SELECT * FROM generate_series(1, 3)",
    "SELECT * FROM unnest([1, 2])",
    "SELECT e.event_id, r.category FROM fct_events e JOIN dim_repo r USING (repo_key)",
    "SELECT count(*) FROM fct_events;",
    "FROM dim_repo",
    "SELECT category FROM dim_repo UNION ALL SELECT event_type FROM main_public.fct_events",
    "SELECT * FROM dim_repo WHERE repo_key IN (SELECT repo_key FROM fct_events)",
    "SELECT (SELECT count(*) FROM fct_events) AS n FROM dim_repo",
]


@pytest.mark.parametrize("sql", ALLOWED)
def test_guard_allows_reads_of_public(guard, sql):
    guard.check(sql)


FORBIDDEN_READS = [
    "SELECT * FROM main_gold.dim_user",
    "SELECT * FROM main_staging.stg_events",
    "SELECT * FROM information_schema.tables",
    "SELECT * FROM pg_catalog.pg_tables",
    "SELECT * FROM fixture_secret",
    "SELECT * FROM main.fixture_secret",
    "SELECT * FROM main.dim_repo",
    f"SELECT * FROM {DB_NAME}.main.dim_repo",
    "SELECT * FROM other_db.main_public.dim_repo",
    "SELECT * FROM no_such_table",
    "SELECT * FROM main_public.no_such_table",
    "SELECT * FROM 'data.csv'",
    "FROM 'data.parquet'",
    "SELECT * FROM dim_repo WHERE repo_key IN (SELECT user_key FROM main_gold.dim_user)",
    "SELECT (SELECT max(actor_login) FROM main_gold.dim_user) AS x FROM dim_repo",
    "WITH u AS (SELECT * FROM main_gold.dim_user) SELECT * FROM u",
    "WITH dim_user AS (SELECT 1) SELECT * FROM main_gold.dim_user",
    "SELECT repo_key FROM dim_repo UNION ALL SELECT user_key FROM main_gold.dim_user",
    "SELECT * FROM dim_repo r JOIN main_gold.dim_user u ON true",
    "SELECT * FROM dim_repo, (SELECT * FROM main_staging.stg_events) s",
    "SELECT * FROM dim_repo WHERE EXISTS (SELECT 1 FROM main.fixture_secret)",
]


@pytest.mark.parametrize("sql", FORBIDDEN_READS)
def test_guard_rejects_relations_outside_public(guard, sql):
    sa = _agent()
    with pytest.raises(sa.SqlRejected):
        guard.check(sql)


def test_guard_rejection_names_the_relation(guard):
    sa = _agent()
    with pytest.raises(sa.SqlRejected, match="(?i)main_gold.*dim_user|dim_user.*main_gold"):
        guard.check(
            "SELECT * FROM dim_repo WHERE repo_key IN (SELECT user_key FROM main_gold.dim_user)"
        )


TABLE_FUNCTIONS = [
    "SELECT * FROM read_csv('data.csv')",
    "SELECT * FROM read_csv_auto('data.csv')",
    "SELECT * FROM read_parquet('data.parquet')",
    "SELECT * FROM read_json('data.json')",
    "SELECT * FROM read_text('notes.txt')",
    "SELECT * FROM glob('*')",
    "SELECT * FROM duckdb_tables()",
    "SELECT * FROM duckdb_settings()",
    "SELECT * FROM duckdb_secrets()",
    "SELECT * FROM pragma_table_info('dim_repo')",
    "SELECT * FROM query('SELECT * FROM main_gold.dim_user')",
    "SELECT * FROM dim_repo, read_csv('data.csv')",
    "SELECT * FROM dim_repo WHERE repo_key IN (SELECT * FROM read_text('x'))",
]


@pytest.mark.parametrize("sql", TABLE_FUNCTIONS)
def test_guard_rejects_table_functions(guard, sql):
    sa = _agent()
    with pytest.raises(sa.SqlRejected):
        guard.check(sql)


SECRET_FUNCTIONS = [
    "SELECT current_setting('motherduck_token')",
    "SELECT getenv('HOME')",
    "SELECT upper(getenv('PII_HASH_SALT')) AS s FROM dim_repo",
    "SELECT repo_name FROM dim_repo WHERE current_setting('search_path') <> ''",
]


@pytest.mark.parametrize("sql", SECRET_FUNCTIONS)
def test_guard_rejects_functions_that_leak_settings(guard, sql):
    sa = _agent()
    with pytest.raises(sa.SqlRejected):
        guard.check(sql)


def test_guard_lists_exactly_the_public_relations(guard):
    assert guard.tables == frozenset({"dim_repo", "fct_events"})


def test_guard_schema_is_configurable(conn):
    sa = _agent()
    gold = sa.Guard.from_connection(conn, schema="main_gold")
    gold.check("SELECT * FROM main_gold.dim_user")
    with pytest.raises(sa.SqlRejected):
        gold.check("SELECT * FROM main_public.dim_repo")


# ---------------------------------------------------------------- connection


@pytest.mark.parametrize("target", ["md:repopulse", "MD:repopulse", "motherduck:repopulse"])
def test_open_readonly_refuses_motherduck(target):
    sa = _agent()
    with pytest.raises(sa.AgentConfigError, match="(?i)local"):
        sa.open_readonly(target)


def test_open_readonly_refuses_missing_file(tmp_path):
    sa = _agent()
    missing = tmp_path / "missing.duckdb"
    with pytest.raises(sa.AgentConfigError):
        sa.open_readonly(missing)
    assert not missing.exists()


BYPASS_GUARD = [
    "DROP TABLE main_public.dim_repo",
    "INSERT INTO main_public.dim_repo VALUES ('a', 'b', 'c')",
    "CREATE TABLE main_public.x AS SELECT 1",
    "SELECT * FROM read_csv('pyproject.toml')",
    "COPY (SELECT 1) TO 'leak.csv'",
    "SET enable_external_access = true",
    "LOAD httpfs",
]


@pytest.mark.parametrize("sql", BYPASS_GUARD)
def test_connection_itself_refuses_writes_and_file_access(db_path, sql):
    sa = _agent()
    before = _sha256(db_path)
    con = sa.open_readonly(db_path)
    try:
        with pytest.raises(duckdb.Error):
            con.execute(sql)
        assert con.execute("SELECT count(*) FROM main_public.dim_repo").fetchone()[0] == 3
    finally:
        con.close()
    assert _sha256(db_path) == before


def test_connection_still_parses_with_locked_configuration(conn):
    sa = _agent()
    sa.Guard.from_connection(conn).check("SELECT * FROM dim_repo")


def test_unqualified_names_resolve_to_public(conn):
    sa = _agent()
    result = sa.run_query(conn, "SELECT count(*) AS n FROM dim_repo")
    assert result.rows == [(PUBLIC_REPOS,)]


# ---------------------------------------------------------------- run_query


def test_run_query_caps_rows_and_flags_truncation(conn):
    sa = _agent()
    capped = sa.run_query(conn, "SELECT repo_key FROM dim_repo ORDER BY repo_key", max_rows=2)
    assert capped.columns == ["repo_key"]
    assert capped.rows == [("apache/kafka",), ("apache/spark",)]
    assert capped.truncated is True
    full = sa.run_query(conn, "SELECT repo_key FROM dim_repo ORDER BY repo_key", max_rows=3)
    assert len(full.rows) == 3
    assert full.truncated is False


def test_run_query_cap_ignores_the_sql_limit(conn):
    sa = _agent()
    result = sa.run_query(conn, "SELECT * FROM range(1000) LIMIT 500", max_rows=10)
    assert len(result.rows) == 10
    assert result.truncated is True


def test_run_query_default_cap_is_bounded(conn):
    sa = _agent()
    result = sa.run_query(conn, "SELECT * FROM range(100000)")
    assert result.truncated is True
    assert 0 < len(result.rows) == sa.DEFAULT_MAX_ROWS <= 1000


def test_run_query_checks_guard_before_executing(db_path, conn):
    sa = _agent()
    with pytest.raises(sa.SqlRejected):
        sa.run_query(conn, "SELECT * FROM main_gold.dim_user")
    with pytest.raises(sa.SqlRejected):
        sa.run_query(conn, "DROP TABLE main_public.dim_repo")
    assert conn.execute("SELECT count(*) FROM main_public.dim_repo").fetchone()[0] == 3


# ---------------------------------------------------------------- extract_sql


@pytest.mark.parametrize(
    ("text", "sql"),
    [
        ("SELECT 1;", "SELECT 1"),
        ("  select 2 ;  \n", "select 2"),
        ("```sql\nSELECT 3;\n```", "SELECT 3"),
        ("```SQL\nSELECT 4\n```", "SELECT 4"),
        ("First:\n```sql\nSELECT 0\n```\nFixed:\n```sql\nSELECT 5\n```", "SELECT 5"),
        ("<think>maybe ```sql\nSELECT 0\n``` no</think>\n```sql\nSELECT 6\n```", "SELECT 6"),
        ("<think>\nlet me think\n</think>\n\nSELECT 7", "SELECT 7"),
    ],
)
def test_extract_sql(text, sql):
    assert _agent().extract_sql(text) == sql


# ---------------------------------------------------------------- LLM client


def test_client_posts_chat_completion(stub):
    sa = _agent()
    stub.reply = "```sql\nSELECT 42\n```"
    client = sa.OpenAICompatClient(stub.url + "/", "qwen-test", api_key=SECRET_KEY, timeout_s=5)
    messages = [{"role": "system", "content": "s"}, {"role": "user", "content": "q"}]
    assert client.complete(messages) == "```sql\nSELECT 42\n```"
    (request,) = stub.requests
    assert request["path"] == "/v1/chat/completions"
    assert request["body"]["model"] == "qwen-test"
    assert request["body"]["messages"] == messages
    assert request["body"]["temperature"] == 0
    headers = {k.lower(): v for k, v in request["headers"].items()}
    assert headers["authorization"] == f"Bearer {SECRET_KEY}"
    assert headers["content-type"].startswith("application/json")


def test_client_sends_no_authorization_without_key(stub):
    sa = _agent()
    sa.OpenAICompatClient(stub.url, "m", timeout_s=5).complete([{"role": "user", "content": "q"}])
    headers = {k.lower() for k in stub.requests[0]["headers"]}
    assert "authorization" not in headers


@pytest.mark.parametrize(
    ("status", "raw"),
    [
        (500, b'{"error": "boom"}'),
        (200, b"<html>not json</html>"),
        (200, b'{"choices": []}'),
        (200, b'{"choices": [{"message": {"content": null}}]}'),
        (200, b'{"unexpected": true}'),
    ],
)
def test_client_errors_never_leak_the_key(stub, status, raw):
    sa = _agent()
    stub.status = status
    stub.raw = raw
    client = sa.OpenAICompatClient(stub.url, "m", api_key=SECRET_KEY, timeout_s=5)
    with pytest.raises(sa.LLMError) as info:
        client.complete([{"role": "user", "content": "q"}])
    assert SECRET_KEY not in str(info.value)
    assert SECRET_KEY not in repr(info.value)


def test_client_unreachable_server_is_llm_error():
    sa = _agent()
    client = sa.OpenAICompatClient(
        f"http://127.0.0.1:{_free_port()}/v1", "m", api_key=SECRET_KEY, timeout_s=5
    )
    with pytest.raises(sa.LLMError) as info:
        client.complete([{"role": "user", "content": "q"}])
    assert SECRET_KEY not in str(info.value)


def test_client_from_env_defaults_and_overrides():
    sa = _agent()
    client = sa.client_from_env({"REPOPULSE_LLM_MODEL": "qwen"})
    assert client.base_url == "http://127.0.0.1:8080/v1"
    assert client.model == "qwen"
    assert client.api_key is None
    env = {
        "REPOPULSE_LLM_BASE_URL": "http://127.0.0.1:9999/v1",
        "REPOPULSE_LLM_MODEL": "env-model",
        "REPOPULSE_LLM_API_KEY": SECRET_KEY,
    }
    client = sa.client_from_env(env, base_url="http://127.0.0.1:7777/v1", model="flag-model")
    assert (client.base_url, client.model, client.api_key) == (
        "http://127.0.0.1:7777/v1",
        "flag-model",
        SECRET_KEY,
    )
    assert SECRET_KEY not in repr(client)


def test_client_from_env_requires_a_model():
    sa = _agent()
    with pytest.raises(sa.AgentConfigError, match="REPOPULSE_LLM_MODEL"):
        sa.client_from_env({})


# ---------------------------------------------------------------- agent loop


def test_system_prompt_describes_only_public(conn):
    sa = _agent()
    prompt = sa.system_prompt(conn)
    for name in ("main_public", "dim_repo", "fct_events", "category", "updated_at", "DATE"):
        assert name in prompt
    for name in ("main_gold", "main_staging", "dim_user", "actor_login", "stg_events",
                 "raw_payload", "fixture_secret", "secret_value"):  # fmt: skip
        assert name not in prompt
    assert "SELECT" in prompt
    assert "DuckDB" in prompt


def test_ask_answers_with_one_query(conn):
    sa = _agent()
    llm = FakeLLM(["```sql\nSELECT count(*) AS n FROM dim_repo\n```"])
    result = sa.ask("How many repositories?", conn, llm)
    assert result.question == "How many repositories?"
    assert result.sql == "SELECT count(*) AS n FROM dim_repo"
    assert result.columns == ["n"]
    assert result.rows == [(PUBLIC_REPOS,)]
    assert result.attempts == 1
    assert result.error is None
    assert result.truncated is False
    assert result.seconds >= 0
    assert llm.calls[0][0]["role"] == "system"
    assert llm.calls[0][-1] == {"role": "user", "content": "How many repositories?"}


def test_ask_never_runs_a_write_and_retries_with_the_error(db_path, conn):
    sa = _agent()
    before = _sha256(db_path)
    llm = FakeLLM(["DROP TABLE main_public.dim_repo", "SELECT repo_name FROM dim_repo"])
    result = sa.ask("List repositories", conn, llm)
    assert result.error is None
    assert result.attempts == 2
    assert sorted(result.rows) == [("apache/kafka",), ("apache/spark",), ("duckdb/duckdb",)]
    second = llm.calls[1]
    assert any("DROP TABLE main_public.dim_repo" in m["content"] for m in second)
    feedback = second[-1]
    assert feedback["role"] == "user"
    assert "reject" in feedback["content"].lower()
    assert conn.execute("SELECT count(*) FROM main_public.dim_repo").fetchone()[0] == 3
    assert _sha256(db_path) == before


def test_ask_retries_on_duckdb_error(conn):
    sa = _agent()
    llm = FakeLLM(["SELECT no_such_column FROM dim_repo", "SELECT repo_name FROM dim_repo"])
    result = sa.ask("List repositories", conn, llm)
    assert result.attempts == 2
    assert result.error is None
    assert "no_such_column" in llm.calls[1][-1]["content"]


def test_ask_gives_up_after_max_attempts(conn):
    sa = _agent()
    llm = FakeLLM(["SELECT * FROM main_gold.dim_user"])
    result = sa.ask("Show users", conn, llm, max_attempts=3)
    assert len(llm.calls) == 3
    assert result.attempts == 3
    assert result.error is not None
    assert "dim_user" in result.error
    assert result.rows == []
    assert result.columns == []


def test_ask_caps_rows(conn):
    sa = _agent()
    result = sa.ask("numbers", conn, FakeLLM(["SELECT * FROM range(50)"]), max_rows=5)
    assert len(result.rows) == 5
    assert result.truncated is True


def test_ask_propagates_llm_errors(conn):
    sa = _agent()

    class Down:
        def complete(self, messages):
            raise sa.LLMError("server down")

    with pytest.raises(sa.LLMError):
        sa.ask("anything", conn, Down())


# ---------------------------------------------------------------- score


@pytest.mark.parametrize(
    ("expected", "actual", "ordered", "correct"),
    [
        ([(1, "a"), (2, "b")], [(1, "a"), (2, "b")], False, True),
        ([(1, "a")], [("a", 1)], False, True),  # column order and names do not matter
        ([(1, "a")], [(1, "a", 99)], False, True),  # extra column
        ([(1, "a")], [(1,)], False, False),  # missing column
        ([(1,), (2,), (3,)], [(3,), (1,), (2,)], False, True),  # multiset
        ([(1,), (2,), (3,)], [(3,), (1,), (2,)], True, False),  # wrong order
        ([(1,), (2,), (3,)], [(1,), (2,), (3,)], True, True),
        ([(1,), (1,), (2,)], [(1,), (2,), (2,)], False, False),  # repeat counts differ
        ([(1,), (2,)], [(1,)], False, False),  # missing row
        ([(1,)], [(1,), (2,)], False, False),  # extra row
        ([(1, "a")], [(1, "b")], False, False),  # wrong value
        ([(0.3,)], [(0.1 + 0.2,)], False, True),  # float tolerance
        ([(1,)], [(1.0,)], False, True),
        ([(1.0,)], [(1.01,)], False, False),
        ([(Decimal("2.50"),)], [(2.5,)], False, True),
        ([(None, 1)], [(None, 1)], False, True),
        ([(None,)], [(0,)], False, False),
        ([(date(2026, 1, 5),)], [(date(2026, 1, 5),)], False, True),
        ([(True,)], [(1,)], False, False),  # bool is not a number
        ([], [], False, True),
        # Every expected column has a candidate whose values match on their own; only one
        # joint assignment reproduces the rows, and it is not the first one tried.
        ([(1, 2), (3, 4)], [(3, 2, 1), (1, 4, 3)], False, True),
        ([(1, 2), (3, 4)], [(3, 2), (1, 4)], False, False),
    ],
)
def test_score(expected, actual, ordered, correct):
    assert _agent().score(expected, actual, ordered=ordered) is correct


def test_is_correct_rejects_truncated_and_failed_answers(conn):
    sa = _agent()
    ok = sa.ask("q", conn, FakeLLM(["SELECT repo_key FROM dim_repo"]))
    expected = [("apache/spark",), ("duckdb/duckdb",), ("apache/kafka",)]
    assert sa.is_correct(expected, ok, ordered=False) is True
    cut = sa.ask("q", conn, FakeLLM(["SELECT repo_key FROM dim_repo"]), max_rows=2)
    assert sa.is_correct(expected[:2], cut, ordered=False) is False
    failed = sa.ask("q", conn, FakeLLM(["SELECT * FROM main_gold.dim_user"]), max_attempts=1)
    assert sa.is_correct([], failed, ordered=False) is False


# ---------------------------------------------------------------- eval on the tmp DB


QUESTIONS_TOML = """
[[question]]
id = "repo_count"
question = "How many repositories are there? Return one number."
sql = "SELECT count(*) FROM main_public.dim_repo"

[[question]]
id = "events_by_type"
question = "Return event_type and the number of events of that type."
sql = "SELECT event_type, count(*) FROM main_public.fct_events GROUP BY event_type"

[[question]]
id = "repos_ordered"
question = "Return repo_key of every repository sorted alphabetically."
sql = "SELECT repo_key FROM main_public.dim_repo ORDER BY repo_key"
ordered = true

[[question]]
id = "spark_events"
question = "Return event_id of the events of repo apache/spark."
sql = "SELECT event_id FROM main_public.fct_events WHERE repo_key = 'apache/spark'"
"""


@pytest.fixture
def questions_path(tmp_path) -> Path:
    path = tmp_path / "questions.toml"
    path.write_text(QUESTIONS_TOML, encoding="utf-8")
    return path


def test_load_questions(questions_path):
    sa = _agent()
    questions = sa.load_questions(questions_path)
    assert [q.id for q in questions] == [
        "repo_count",
        "events_by_type",
        "repos_ordered",
        "spark_events",
    ]
    assert [q.ordered for q in questions] == [False, False, True, False]


@pytest.mark.parametrize(
    "body",
    [
        '[[question]]\nid = "a"\nquestion = "q"\n',  # no sql
        '[[question]]\nid = "a"\nquestion = "q"\nsql = "SELECT 1"\n'
        '[[question]]\nid = "a"\nquestion = "q2"\nsql = "SELECT 2"\n',  # duplicate id
        '[[question]]\nid = "a"\nquestion = "q"\nsql = "SELECT 1"\nordered = "yes"\n',
        "title = 'no questions'\n",
    ],
)
def test_load_questions_rejects_bad_files(tmp_path, body):
    sa = _agent()
    path = tmp_path / "bad.toml"
    path.write_text(body, encoding="utf-8")
    with pytest.raises(sa.AgentConfigError):
        sa.load_questions(path)


def _oracle(questions, wrong=()):
    return OracleLLM(
        {q.question: ("SELECT -12345 AS nope" if q.id in wrong else q.sql) for q in questions}
    )


def test_run_eval_report(conn, questions_path):
    sa = _agent()
    questions = sa.load_questions(questions_path)
    report = sa.run_eval(
        conn,
        _oracle(questions, wrong={"repos_ordered"}),
        questions,
        model="fake-model",
        base_url="http://127.0.0.1:1/v1",
    )
    assert report["model"] == "fake-model"
    assert report["base_url"] == "http://127.0.0.1:1/v1"
    assert "api_key" not in json.dumps(report)
    assert report["summary"]["questions"] == 4
    assert report["summary"]["correct"] == 3
    assert report["summary"]["accuracy"] == pytest.approx(0.75)
    assert report["summary"]["total_seconds"] >= 0
    assert report["summary"]["p50_seconds"] >= 0
    by_id = {r["id"]: r for r in report["results"]}
    assert set(by_id) == {q.id for q in questions}
    assert by_id["repos_ordered"]["correct"] is False
    assert by_id["repo_count"]["correct"] is True
    for row in report["results"]:
        assert set(row) >= {"id", "question", "correct", "sql", "error", "attempts", "seconds"}
    json.dumps(report)  # serialisable as is


def test_run_eval_wrong_order_is_wrong_only_when_ordered(conn, questions_path):
    sa = _agent()
    questions = sa.load_questions(questions_path)
    answers = {q.question: q.sql for q in questions}
    answers[questions[2].question] = (
        "SELECT repo_key FROM main_public.dim_repo ORDER BY repo_key DESC"
    )
    report = sa.run_eval(conn, OracleLLM(answers), questions, model="m", base_url="u")
    by_id = {r["id"]: r["correct"] for r in report["results"]}
    assert by_id == {
        "repo_count": True,
        "events_by_type": True,
        "repos_ordered": False,
        "spark_events": True,
    }


def test_run_eval_agent_error_counts_as_wrong(conn, questions_path):
    sa = _agent()
    questions = sa.load_questions(questions_path)
    llm = FakeLLM(["SELECT * FROM main_gold.dim_user"])
    report = sa.run_eval(conn, llm, questions, model="m", base_url="u", max_attempts=2)
    assert report["summary"]["correct"] == 0
    assert all(r["error"] for r in report["results"])
    assert all(r["attempts"] == 2 for r in report["results"])


def test_run_eval_aborts_when_llm_is_down(conn, questions_path):
    sa = _agent()

    class Down:
        def complete(self, messages):
            raise sa.LLMError("server down")

    with pytest.raises(sa.LLMError):
        sa.run_eval(conn, Down(), sa.load_questions(questions_path), model="m", base_url="u")


def test_run_eval_answer_key_goes_through_the_guard(conn, tmp_path):
    sa = _agent()
    path = tmp_path / "q.toml"
    path.write_text(
        '[[question]]\nid = "x"\nquestion = "q"\nsql = "SELECT * FROM main_gold.dim_user"\n',
        encoding="utf-8",
    )
    with pytest.raises(sa.SqlRejected):
        sa.run_eval(conn, FakeLLM(["SELECT 1"]), sa.load_questions(path), model="m", base_url="u")


# ---------------------------------------------------------------- runs.csv row


@pytest.mark.parametrize(
    ("model", "variant"),
    [
        ("Qwen3.6-35B-A3B", "sql-agent-qwen3-6-35b-a3b"),
        ("qwen2.5-coder:7b", "sql-agent-qwen2-5-coder-7b"),
        ("models/Llama 3_8B.gguf", "sql-agent-models-llama-3_8b-gguf"),
    ],
)
def test_variant_for_model(model, variant):
    from repopulse.runs_log import validate_row

    sa = _agent()
    assert sa.variant_for_model(model) == variant
    validate_row(
        {
            "run_at": "2026-11-12T00:00:00+00:00",
            "git_sha": "abcdef1",
            "variant": variant,
            "data_date": "2026-01-05",
            "total_seconds": 1.0,
        }
    )


def _report(seconds=(1.0, 3.0, 2.0), correct=(True, False, True), model="Qwen3.6-35B-A3B"):
    results = [
        {
            "id": f"q{i}",
            "question": "q",
            "correct": c,
            "sql": "SELECT 1",
            "error": None,
            "attempts": 1,
            "seconds": s,
        }  # fmt: skip
        for i, (s, c) in enumerate(zip(seconds, correct, strict=True))
    ]
    return {
        "model": model,
        "base_url": "http://127.0.0.1:8080/v1",
        "results": results,
        "summary": {
            "questions": len(results),
            "correct": sum(correct),
            "accuracy": sum(correct) / len(results),
            "total_seconds": sum(seconds),
            "p50_seconds": sorted(seconds)[len(seconds) // 2],
        },
    }


def test_runs_row_from_report():
    sa = _agent()
    row = sa.runs_row(
        _report(),
        git_sha="0123abc",
        data_date=date(2026, 1, 7),
        run_at="2026-11-12T01:02:03+00:00",
    )
    assert row["variant"] == "sql-agent-qwen3-6-35b-a3b"
    assert row["git_sha"] == "0123abc"
    assert row["data_date"] == "2026-01-07"
    assert float(row["total_seconds"]) == pytest.approx(6.0)
    assert row["row_count"] == "3"
    assert row["notes"].startswith("accuracy=2/3; p50_question_s=2")
    assert row["notes"].endswith("; model=Qwen3.6-35B-A3B")
    for col in ("q1_seconds", "q6_seconds", "bytes_read", "peak_rss_mb"):
        assert row[col] == ""


def test_runs_row_variant_override_and_validation():
    from repopulse.runs_log import RunsLogError

    sa = _agent()
    row = sa.runs_row(_report(), git_sha="0123abc", data_date="2026-01-07", variant="sql-agent-x")
    assert row["variant"] == "sql-agent-x"
    with pytest.raises(RunsLogError):
        sa.runs_row(_report(), git_sha="NOT-A-SHA", data_date="2026-01-07")


# ---------------------------------------------------------------- CLI


@pytest.fixture
def no_llm_env(monkeypatch):
    for name in ("REPOPULSE_LLM_BASE_URL", "REPOPULSE_LLM_MODEL", "REPOPULSE_LLM_API_KEY"):
        monkeypatch.delenv(name, raising=False)


def test_cli_ask_prints_sql_and_rows(stub, db_path, no_llm_env, capsys):
    sa = _agent()
    stub.reply = "```sql\nSELECT repo_name FROM dim_repo WHERE category = 'streaming'\n```"
    code = sa.main(
        [
            "ask",
            "--db",
            str(db_path),
            "--base-url",
            stub.url,
            "--model",
            "fake",
            "Which repos are streaming?",
        ]  # fmt: skip
    )
    out = capsys.readouterr().out
    assert code == 0
    assert "SELECT repo_name FROM dim_repo WHERE category = 'streaming'" in out
    assert "apache/kafka" in out
    assert stub.requests[0]["body"]["model"] == "fake"


def test_cli_ask_exit_1_when_no_valid_sql(stub, db_path, no_llm_env, capsys):
    sa = _agent()
    stub.reply = "DROP TABLE main_public.dim_repo"
    before = _sha256(db_path)
    code = sa.main(
        [
            "ask",
            "--db",
            str(db_path),
            "--base-url",
            stub.url,
            "--model",
            "fake",
            "--max-attempts",
            "2",
            "Drop it",
        ]  # fmt: skip
    )
    assert code == 1
    assert len(stub.requests) == 2
    assert _sha256(db_path) == before


def test_cli_ask_reads_model_from_env(stub, db_path, no_llm_env, monkeypatch):
    sa = _agent()
    monkeypatch.setenv("REPOPULSE_LLM_BASE_URL", stub.url)
    monkeypatch.setenv("REPOPULSE_LLM_MODEL", "env-model")
    assert sa.main(["ask", "--db", str(db_path), "q"]) == 0
    assert stub.requests[0]["body"]["model"] == "env-model"


@pytest.mark.parametrize(
    "args",
    [
        ["ask", "--db", "md:repopulse", "--model", "m", "q"],
        ["ask", "--db", "{missing}", "--model", "m", "q"],
        ["ask", "--db", "{db}", "q"],  # no model
        ["ask", "--db", "{db}", "--model", "m", "--base-url", "{down}", "q"],
    ],
)
def test_cli_ask_exit_2_on_config_or_llm_errors(args, db_path, tmp_path, no_llm_env, capsys):
    sa = _agent()
    values = {
        "missing": str(tmp_path / "missing.duckdb"),
        "db": str(db_path),
        "down": f"http://127.0.0.1:{_free_port()}/v1",
    }
    assert sa.main([a.format(**values) for a in args]) == 2
    assert "ERROR" in capsys.readouterr().err


def test_cli_eval_writes_report_and_runs_row(stub, db_path, questions_path, tmp_path, no_llm_env):
    from benchmarks.record_run import read_artifact

    sa = _agent()
    questions = sa.load_questions(questions_path)
    answers = {q.question: q.sql for q in questions}

    def reply(body):
        text = body["messages"][-1]["content"]
        return answers.get(text, "SELECT -1")

    stub.reply = reply
    report_path = tmp_path / "out" / "report.json"
    out = tmp_path / "out" / "row.csv"
    code = sa.main(
        [
            "eval",
            "--db",
            str(db_path),
            "--questions",
            str(questions_path),
            "--base-url",
            stub.url,
            "--model",
            "Qwen3.6-35B-A3B",
            "--report",
            str(report_path),
            "--out",
            str(out),
            "--git-sha",
            "0123abc",
        ]  # fmt: skip
    )
    assert code == 0
    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert report["summary"]["accuracy"] == 1.0
    assert report["model"] == "Qwen3.6-35B-A3B"
    (row,) = read_artifact(out)
    assert row["variant"] == "sql-agent-qwen3-6-35b-a3b"
    assert row["data_date"] == "2026-01-07"  # max(event_date) of main_public.fct_events
    assert row["git_sha"] == "0123abc"
    assert row["row_count"] == 4
    assert row["notes"].startswith("accuracy=4/4; ")
    with out.open(encoding="utf-8", newline="") as fh:
        assert len(list(csv.reader(fh))) == 2


def test_cli_eval_variant_flag(stub, db_path, questions_path, tmp_path, no_llm_env):
    from benchmarks.record_run import read_artifact

    sa = _agent()
    out = tmp_path / "row.csv"
    code = sa.main(
        [
            "eval",
            "--db",
            str(db_path),
            "--questions",
            str(questions_path),
            "--base-url",
            stub.url,
            "--model",
            "m",
            "--report",
            str(tmp_path / "r.json"),
            "--out",
            str(out),
            "--git-sha",
            "0123abc",
            "--variant",
            "sql-agent-custom",
        ]  # fmt: skip
    )
    assert code == 0
    assert read_artifact(out)[0]["variant"] == "sql-agent-custom"


def test_cli_eval_llm_down_is_invalid_not_zero(db_path, questions_path, tmp_path, no_llm_env):
    sa = _agent()
    out = tmp_path / "row.csv"
    report_path = tmp_path / "report.json"
    code = sa.main(
        [
            "eval",
            "--db",
            str(db_path),
            "--questions",
            str(questions_path),
            "--base-url",
            f"http://127.0.0.1:{_free_port()}/v1",
            "--model",
            "m",
            "--report",
            str(report_path),
            "--out",
            str(out),
            "--git-sha",
            "0123abc",
        ]  # fmt: skip
    )
    assert code == 2
    assert not out.exists()
    assert not report_path.exists()


def test_cli_eval_llm_500_is_invalid(stub, db_path, questions_path, tmp_path, no_llm_env):
    sa = _agent()
    stub.status = 500
    stub.raw = b'{"error": "overloaded"}'
    out = tmp_path / "row.csv"
    code = sa.main(
        [
            "eval",
            "--db",
            str(db_path),
            "--questions",
            str(questions_path),
            "--base-url",
            stub.url,
            "--model",
            "m",
            "--report",
            str(tmp_path / "r.json"),
            "--out",
            str(out),
            "--git-sha",
            "0123abc",
        ]  # fmt: skip
    )
    assert code == 2
    assert not out.exists()


def test_cli_eval_out_needs_git_sha(stub, db_path, questions_path, tmp_path, no_llm_env):
    sa = _agent()
    code = sa.main(
        [
            "eval",
            "--db",
            str(db_path),
            "--questions",
            str(questions_path),
            "--base-url",
            stub.url,
            "--model",
            "m",
            "--report",
            str(tmp_path / "r.json"),
            "--out",
            str(tmp_path / "row.csv"),
        ]  # fmt: skip
    )
    assert code == 2
    assert stub.requests == []
