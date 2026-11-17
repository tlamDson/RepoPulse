"""Path B skips cleanly when Databricks refuses new runs (FEATURE_DISABLED); no cloud, no network."""

import pytest

from repopulse.databricks_gate import (
    EXIT_FEATURE_DISABLED,
    FeatureDisabled,
    is_feature_disabled,
)
from repopulse.probe import ApiError

ORG_MESSAGE = "Triggering new runs for organization 123 is currently disabled temporarily"


def test_exit_code_is_distinct_from_success_and_failure():
    assert EXIT_FEATURE_DISABLED not in (0, 1, 2)


def test_feature_disabled_api_error_is_detected():
    assert is_feature_disabled(ApiError(404, "FEATURE_DISABLED", ORG_MESSAGE))


@pytest.mark.parametrize(
    "exc",
    [
        ApiError(404, "RESOURCE_DOES_NOT_EXIST", "Notebook not found"),
        ApiError(403, "PERMISSION_DENIED", "no"),
        ApiError(500, "", "boom"),
        ApiError(404, "", ORG_MESSAGE),  # the message alone is not the signal
        ValueError("FEATURE_DISABLED"),
        RuntimeError("HTTP 404 FEATURE_DISABLED"),
    ],
)
def test_every_other_error_is_not_feature_disabled(exc):
    assert not is_feature_disabled(exc)


def test_feature_disabled_is_an_exception_that_keeps_the_cause():
    err = FeatureDisabled(str(ApiError(404, "FEATURE_DISABLED", ORG_MESSAGE)))
    assert isinstance(err, Exception)
    assert "FEATURE_DISABLED" in str(err)
