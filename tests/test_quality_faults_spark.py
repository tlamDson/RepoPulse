"""Six simkit faults through the real bronze -> silver incremental batch (PySpark + Delta).

Every app event here is SIMULATED (simkit), not real users. The checks of repopulse.quality read
the lake tables only; the simkit manifest is the answer key used by ``score``. Kept small (a few
dozen simulated users over 2 days) so the ``Unit Tests`` job stays fast. No network.
"""

from __future__ import annotations

from datetime import date

import pytest

from repopulse import quality as q

pytestmark = pytest.mark.spark

SALT = "test-salt-not-a-secret"
START = date(2026, 11, 1)
USERS = 30
DAYS = 2

# fault type -> checks allowed to catch it (each fault type has its own checks)
CHECKS = {
    "duplicate": {"duplicate_key_in_bronze"},
    "null": {"quarantine_null_field"},
    "wrong_type": {"quarantine_wrong_type"},
    "schema_drift": {"quarantine_schema_drift"},
    "late": {"late_arrival", "late_beyond_window"},
    "bot": {"bot_rate"},
}


@pytest.fixture(scope="module")
def cfg() -> dict:
    return q.load_config()


@pytest.fixture(scope="module")
def faulted(spark, cfg, tmp_path_factory) -> dict:
    work = tmp_path_factory.mktemp("dq_faulted")
    return q.run_injected(
        spark,
        seed=42,
        users=USERS,
        days=DAYS,
        start=START,
        work_dir=work,
        salt=SALT,
        cfg=cfg,
    )


def test_every_fault_type_is_injected(faulted, cfg):
    assert faulted["score"]["injected"] == cfg["faults"]["inject"]
    assert all(n > 0 for n in faulted["score"]["injected"].values())


def test_every_injected_fault_is_caught(faulted):
    score = faulted["score"]
    assert score["caught"] == score["injected"], score["missed"]
    assert score["missed"] == []


def test_faulted_run_has_no_false_alarm(faulted):
    assert faulted["score"]["false_alarms"] == 0, faulted["score"]["unmatched"]


def test_each_fault_type_is_caught_by_its_own_check(faulted):
    by_fault: dict[str, set[str]] = {}
    for f in faulted["findings"]:
        by_fault.setdefault(f.fault, set()).add(f.check)
    assert set(by_fault) == set(CHECKS)
    for fault, checks in by_fault.items():
        assert checks <= CHECKS[fault], (fault, checks)


def test_rerun_of_the_same_hours_gives_the_same_findings(spark, faulted, cfg):
    again, _ = q.pipeline_findings(
        spark,
        hours=faulted["hours"],
        landing_dir=faulted["landing_dir"],
        lake_dir=faulted["lake_dir"],
        salt=SALT,
        cfg=cfg,
    )
    assert len(again) == len(faulted["findings"])
    assert set(again) == set(faulted["findings"])


def test_clean_run_raises_no_false_alarm(spark, cfg, tmp_path):
    findings = q.run_clean(
        spark,
        seed=7,
        users=USERS,
        days=DAYS,
        start=START,
        work_dir=tmp_path,
        salt=SALT,
        cfg=cfg,
    )
    assert findings == []
