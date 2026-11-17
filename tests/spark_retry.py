"""Retry the start of the test SparkSession when the JVM exits on a transient network error.

Not a test module: ``conftest.py`` wraps ``getOrCreate`` in ``start_with_retry``.
``spark-submit`` resolves the Delta/Iceberg jars from Maven Central on every JVM start; when that
download fails once, PySpark only reports ``JAVA_GATEWAY_EXITED``. A real misconfiguration fails
the same way on every attempt, so the retry ends with the same error and never hides it.
"""

import time
from collections.abc import Callable
from typing import TypeVar

T = TypeVar("T")

TRANSIENT_MARKER = "JAVA_GATEWAY_EXITED"


def start_with_retry(
    build: Callable[[], T],
    *,
    attempts: int = 3,
    delay: float = 10.0,
    sleep: Callable[[float], None] = time.sleep,
) -> T:
    """Call ``build`` up to ``attempts`` times; only a Java gateway exit is retried."""
    for attempt in range(1, attempts + 1):
        try:
            return build()
        except Exception as exc:
            if TRANSIENT_MARKER not in str(exc) or attempt == attempts:
                raise
            sleep(delay)
    raise AssertionError("unreachable: attempts must be at least 1")


def create_session(configured, **retry_kwargs):
    """``configured.getOrCreate()`` of a configured ``SparkSession.Builder``, with the retry."""
    return start_with_retry(configured.getOrCreate, **retry_kwargs)
