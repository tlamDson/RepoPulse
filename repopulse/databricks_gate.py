"""Detect "Databricks refuses new runs" so path B can skip cleanly (path-comparison.yml).

Databricks Free Edition sometimes answers ``runs/submit`` with
``HTTP 404 FEATURE_DISABLED: Triggering new runs for organization ... is currently disabled
temporarily``. That is a platform state, not a pipeline bug: like missing secrets, the workflow
writes a notice, skips the rest of path B, writes no path-b-databricks row and stays green.
Every other Databricks error keeps failing.
"""

from __future__ import annotations

from repopulse.probe import ApiError

FEATURE_DISABLED_CODE = "FEATURE_DISABLED"
# Exit code of `databricks_job run` for this one case; 0 = ok, 1 = failure, 2 = argparse error.
EXIT_FEATURE_DISABLED = 3


class FeatureDisabled(Exception):
    """Databricks refused to start a run because the feature is disabled for the organization."""


def is_feature_disabled(exc: BaseException) -> bool:
    """True only for a Databricks API error whose ``error_code`` is ``FEATURE_DISABLED``."""
    return isinstance(exc, ApiError) and exc.error_code == FEATURE_DISABLED_CODE
