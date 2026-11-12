"""Evaluation set of the SQL agent (repopulse/sql_agent_eval.toml) on the dbt-built fixture DB.

Uses the session fixture ``built`` (one ``dbt build`` of transform/ on the JSON fixtures, see
conftest.py). The LLM is a fake: an "oracle" that answers with the reference SQL, or one that
answers some questions wrongly. These are tests of the harness, not measurements: the real
accuracy of a model is recorded by the ``eval`` CLI into runs.csv (docs/sql-agent.md).
"""

from __future__ import annotations

import importlib

import pytest

pytest.importorskip("dbt.cli.main", reason="dbt extra not installed: pip install -e '.[dbt]'")
pytest.importorskip("dbt.adapters.duckdb", reason="dbt-duckdb not installed")

from dbt_helpers import MARTS, PUBLIC_SCHEMA, require_ok  # noqa: E402

PUBLIC_TABLES = ("fct_events", "dim_repo", "dim_user") + tuple(m for m, _ in MARTS.values())
MIN_QUESTIONS = 12


def _agent():
    """repopulse.sql_agent, imported in the test body so a missing module fails the test."""
    return importlib.import_module("repopulse.sql_agent")


@pytest.fixture(scope="module")
def public_conn(built):
    require_ok(built["proc"])
    con = _agent().open_readonly(built["db"], schema=PUBLIC_SCHEMA)
    yield con
    con.close()


@pytest.fixture(scope="module")
def questions():
    return _agent().load_questions()


class OracleLLM:
    """Answers each question with the reference SQL, or a wrong one for ids in ``wrong``."""

    def __init__(self, questions, wrong=()):
        self.answers = {
            q.question: ("SELECT -12345 AS wrong" if q.id in wrong else q.sql) for q in questions
        }

    def complete(self, messages):
        text = messages[-1]["content"]
        return self.answers[text]


def test_default_question_file_ships_with_the_package():
    sa = _agent()
    assert sa.DEFAULT_QUESTIONS.name == "sql_agent_eval.toml"
    assert sa.DEFAULT_QUESTIONS.parent.name == "repopulse"
    assert sa.DEFAULT_QUESTIONS.is_file()


def test_question_set_size_and_ids(questions):
    assert len(questions) >= MIN_QUESTIONS
    ids = [q.id for q in questions]
    assert len(ids) == len(set(ids))
    for q in questions:
        assert q.question.strip()
        assert q.sql.strip()


def test_question_set_covers_every_public_table(questions):
    sql = "\n".join(q.sql.lower() for q in questions)
    missing = [t for t in PUBLIC_TABLES if f"{PUBLIC_SCHEMA}.{t}" not in sql]
    assert missing == []


def test_question_set_has_joins_and_ordered_questions(questions):
    assert any(" join " in q.sql.lower() for q in questions)
    assert any(q.ordered for q in questions)


def test_every_reference_query_passes_the_guard_and_returns_rows(public_conn, questions):
    sa = _agent()
    guard = sa.Guard.from_connection(public_conn, schema=PUBLIC_SCHEMA)
    for q in questions:
        guard.check(q.sql)
        result = sa.run_query(public_conn, q.sql, max_rows=sa.ANSWER_MAX_ROWS, guard=guard)
        assert result.rows, f"{q.id}: reference query returns no row on the fixture"
        assert result.truncated is False, q.id


def test_oracle_scores_full_accuracy(public_conn, questions):
    sa = _agent()
    report = sa.run_eval(
        public_conn, OracleLLM(questions), questions, model="oracle", base_url="fake://"
    )
    wrong = [r["id"] for r in report["results"] if not r["correct"]]
    assert wrong == []
    assert report["summary"]["accuracy"] == 1.0
    assert report["summary"]["questions"] == len(questions)


def test_wrong_answers_lower_accuracy_exactly(public_conn, questions):
    sa = _agent()
    wrong = {questions[0].id, questions[3].id, questions[-1].id}
    report = sa.run_eval(
        public_conn, OracleLLM(questions, wrong), questions, model="m", base_url="fake://"
    )
    assert {r["id"] for r in report["results"] if not r["correct"]} == wrong
    assert report["summary"]["correct"] == len(questions) - len(wrong)
    assert report["summary"]["accuracy"] == pytest.approx(
        (len(questions) - len(wrong)) / len(questions)
    )
