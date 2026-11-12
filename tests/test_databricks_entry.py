"""Pure-Python parts of the Databricks entry of the bronze/silver batch (no Spark, no network).

The notebook passes its widget values (the job's ``base_parameters``, all strings) to
``repopulse.spark.databricks_entry.run``, which calls the shared ``batch.run_batch`` with the
landing directory on the Volume and catalog table names. The batch runner is injected here.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

from repopulse import databricks_job as dj
from repopulse.spark import databricks_entry, tables

DAY = "2026-01-05"
SALT = "pii-salt-for-tests-only"
SHA = "abc1234def"
ROOT = Path(__file__).resolve().parents[1]


def params(**overrides) -> dict[str, str]:
    kwargs = {
        "source": "gharchive",
        "day": DAY,
        "hour": 10,
        "hours": 2,
        "run_key": "777-2",
        "notebook_path": "/Users/someone/repopulse/bronze_silver",
        "wheel_path": f"/Volumes/workspace/repopulse/repopulse_landing/wheels/{SHA}/x.whl",
        "catalog": "workspace",
        "schema": "repopulse",
        "volume": "repopulse_landing",
        "late_window_hours": 6,
        "secret_scope": "repopulse",
        "secret_key": "pii_hash_salt",
        "timeout_seconds": 3600,
    }
    kwargs.update(overrides)
    payload = dj.build_submit_payload(**kwargs)
    return payload["tasks"][0]["notebook_task"]["base_parameters"]


class FakeRunBatch:
    def __init__(self):
        self.calls = []

    def __call__(self, spark, **kwargs):
        self.calls.append({"spark": spark, **kwargs})
        return {"source": kwargs["source"], "silver_inserted": 3}


def test_landing_dir_is_the_source_folder_on_the_volume():
    path = databricks_entry.landing_dir(
        catalog="workspace", schema="repopulse", volume="repopulse_landing", source="gharchive"
    )
    assert str(path) == "/Volumes/workspace/repopulse/repopulse_landing/landing/gharchive"


def test_table_names_with_catalog_are_three_part_names():
    assert tables.table_names("gharchive", schema="repopulse", catalog="workspace") == {
        "bronze": "workspace.repopulse.bronze_gharchive_events",
        "silver": "workspace.repopulse.silver_gharchive_events",
        "quarantine": "workspace.repopulse.quarantine_gharchive_events",
    }


def test_table_names_without_catalog_are_schema_qualified():
    assert tables.table_names("app", schema="rp_local") == {
        "bronze": "rp_local.bronze_app_events",
        "silver": "rp_local.silver_app_events",
        "quarantine": "rp_local.quarantine_app_events",
    }


def test_table_names_reject_an_unknown_source():
    with pytest.raises(ValueError):
        tables.table_names("kafka", schema="repopulse")


def test_run_calls_the_shared_batch_with_volume_landing_and_catalog_tables():
    fake = FakeRunBatch()
    spark = object()

    summary = databricks_entry.run(spark, params(), salt=SALT, run_batch=fake)

    assert summary == {"source": "gharchive", "silver_inserted": 3}
    (call,) = fake.calls
    assert call["spark"] is spark
    assert call["source"] == "gharchive"
    assert [tuple(h) for h in call["hours"]] == [(DAY, 9), (DAY, 10)]
    assert Path(call["landing_dir"]).as_posix() == (
        "/Volumes/workspace/repopulse/repopulse_landing/landing/gharchive"
    )
    assert call["catalog"] == "workspace"
    assert call["schema"] == "repopulse"
    assert call["salt"] == SALT
    assert call["late_window_hours"] == 6
    assert call["persist"] is False, "serverless (Spark Connect) refuses persist/cache"
    assert call.get("lake_dir") is None


def test_run_window_crosses_midnight():
    fake = FakeRunBatch()

    databricks_entry.run(object(), params(hour=0, hours=2), salt=SALT, run_batch=fake)

    assert [tuple(h) for h in fake.calls[0]["hours"]] == [("2026-01-04", 23), (DAY, 0)]


@pytest.mark.parametrize(
    "bad",
    [
        {"source": "kafka"},
        {"hour": "24"},
        {"hours": "0"},
        {"date": "2026-02-30"},
    ],
)
def test_run_rejects_bad_parameters_before_spark_work(bad):
    fake = FakeRunBatch()

    with pytest.raises(ValueError):
        databricks_entry.run(object(), {**params(), **bad}, salt=SALT, run_batch=fake)

    assert fake.calls == []


def test_run_needs_a_salt():
    fake = FakeRunBatch()

    with pytest.raises(ValueError):
        databricks_entry.run(object(), params(), salt="  ", run_batch=fake)

    assert fake.calls == []


def test_entry_and_client_import_without_pyspark():
    code = (
        "import sys; import repopulse.databricks_job, repopulse.spark.databricks_entry, "
        "repopulse.spark.tables; print('pyspark' in sys.modules)"
    )
    proc = subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
        cwd=ROOT,
    )
    assert proc.returncode == 0, proc.stderr
    assert proc.stdout.strip() == "False"
