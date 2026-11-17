"""The session ``spark`` fixture retries a JVM start that failed on a transient network error.

CI run 44064181198 (push to develop, the same code as two green PR runs): ``spark-submit`` could
not fetch the Delta/Iceberg jars from Maven Central (``unresolved dependency ... not found``), so
PySpark raised ``JAVA_GATEWAY_EXITED`` and every ``@pytest.mark.spark`` test errored at setup.
``spark_retry.start_with_retry`` is what the fixture wraps ``getOrCreate`` in.
"""

import ast
from pathlib import Path

import pytest
from spark_retry import create_session, start_with_retry

GATEWAY_EXITED = RuntimeError(
    "[JAVA_GATEWAY_EXITED] Java gateway process exited before sending its port number."
)


def _flaky(failures: int, error: Exception = GATEWAY_EXITED):
    calls = {"n": 0}

    def build():
        calls["n"] += 1
        if calls["n"] <= failures:
            raise error
        return "session"

    return build, calls


def test_returns_the_result_without_sleeping_when_the_first_start_works():
    build, calls = _flaky(0)
    sleeps: list[float] = []

    assert start_with_retry(build, sleep=sleeps.append) == "session"
    assert calls["n"] == 1
    assert sleeps == []


def test_retries_a_gateway_exit_and_returns_the_session_of_the_later_attempt():
    build, calls = _flaky(2)
    sleeps: list[float] = []

    assert start_with_retry(build, attempts=3, delay=7.0, sleep=sleeps.append) == "session"
    assert calls["n"] == 3
    assert sleeps == [7.0, 7.0]


def test_raises_the_last_gateway_exit_when_every_attempt_fails():
    build, calls = _flaky(10)

    with pytest.raises(RuntimeError, match="JAVA_GATEWAY_EXITED"):
        start_with_retry(build, attempts=3, sleep=lambda _s: None)
    assert calls["n"] == 3


def test_does_not_retry_an_error_that_is_not_a_gateway_exit():
    build, calls = _flaky(10, error=ValueError("bad spark conf"))
    sleeps: list[float] = []

    with pytest.raises(ValueError, match="bad spark conf"):
        start_with_retry(build, sleep=sleeps.append)
    assert calls["n"] == 1
    assert sleeps == []


class _FakeBuilder:
    """A configured ``SparkSession.Builder`` whose ``getOrCreate`` fails ``failures`` times."""

    def __init__(self, failures: int):
        self.build, self.calls = _flaky(failures)

    def getOrCreate(self):  # noqa: N802 - the PySpark method name
        return self.build()


def test_create_session_gets_the_session_on_the_third_attempt():
    """Two gateway exits still give a session: the fixture keeps all 3 attempts."""
    builder = _FakeBuilder(failures=2)
    sleeps: list[float] = []

    assert create_session(builder, sleep=sleeps.append) == "session"
    assert builder.calls["n"] == 3
    assert sleeps == [10.0, 10.0]


def test_create_session_gives_up_after_the_third_attempt():
    builder = _FakeBuilder(failures=3)

    with pytest.raises(RuntimeError, match="JAVA_GATEWAY_EXITED"):
        create_session(builder, sleep=lambda _s: None)
    assert builder.calls["n"] == 3


def _fixture_calls() -> list[ast.Call]:
    tree = ast.parse((Path(__file__).parent / "conftest.py").read_text(encoding="utf-8"))
    fixture = next(
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.FunctionDef) and node.name == "spark"
    )
    return [
        call
        for call in ast.walk(fixture)
        if isinstance(call, ast.Call) and isinstance(call.func, (ast.Name, ast.Attribute))
    ]


def _name(call: ast.Call) -> str:
    return call.func.id if isinstance(call.func, ast.Name) else call.func.attr


def test_spark_fixture_starts_the_session_through_create_session():
    """Guard the wiring: the ``spark`` fixture must not call ``getOrCreate`` itself."""
    called = {_name(call) for call in _fixture_calls()}
    assert "create_session" in called
    assert "getOrCreate" not in called


def test_spark_fixture_keeps_the_default_retry_settings():
    """The fixture passes only the builder, so it gets the 3 attempts tested above."""
    calls = [call for call in _fixture_calls() if _name(call) == "create_session"]
    assert len(calls) == 1
    assert len(calls[0].args) == 1
    assert calls[0].keywords == []
