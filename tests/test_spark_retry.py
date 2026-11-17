"""The session ``spark`` fixture retries a JVM start that failed on a transient network error.

CI run 44064181198 (push to develop, the same code as two green PR runs): ``spark-submit`` could
not fetch the Delta/Iceberg jars from Maven Central (``unresolved dependency ... not found``), so
PySpark raised ``JAVA_GATEWAY_EXITED`` and every ``@pytest.mark.spark`` test errored at setup.
``spark_retry.start_with_retry`` is what the fixture wraps ``getOrCreate`` in.
"""

import pytest
from spark_retry import start_with_retry

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
