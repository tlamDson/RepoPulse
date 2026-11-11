"""CLI of the bronze -> silver incremental batch job. No Spark: the session factory and the batch
runner are replaced, so these tests run without pyspark or Java."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

import repopulse.spark
from repopulse.spark import job

DAY = "2026-01-05"
SALT = "test-salt-not-a-secret"


def test_package_import_does_not_pull_in_pyspark():
    # repopulse.spark, its CLI and pure rules must import on machines without pyspark; pyspark is
    # imported lazily once a session is really needed. Checked in a fresh interpreter.
    assert repopulse.spark.__name__ == "repopulse.spark"
    code = (
        "import sys; import repopulse.spark, repopulse.spark.job, repopulse.spark.pii, "
        "repopulse.spark.rules; print('pyspark' in sys.modules)"
    )
    proc = subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
        cwd=Path(__file__).resolve().parents[1],
    )
    assert proc.returncode == 0, proc.stderr
    assert proc.stdout.strip() == "False"


class FakeSession:
    def __init__(self):
        self.stopped = False

    def stop(self):
        self.stopped = True


@pytest.fixture
def fakes(monkeypatch):
    state = {"sessions": [], "runs": []}

    def fake_create_spark_session(*args, **kwargs):
        session = FakeSession()
        state["sessions"].append(session)
        return session

    def fake_run_batch(spark, **kwargs):
        state["runs"].append({"spark": spark, **kwargs})
        return {
            "source": kwargs["source"],
            "hours": [[d, h] for d, h in kwargs["hours"]],
            "bronze_rows": 3,
            "silver_inserted": 1,
            "duplicates_dropped": 1,
            "quarantined": {"malformed_json": 1},
            "quarantined_total": 1,
            "late_accepted": 0,
        }

    monkeypatch.setattr(job, "create_spark_session", fake_create_spark_session)
    monkeypatch.setattr(job, "run_batch", fake_run_batch)
    monkeypatch.setenv("PII_HASH_SALT", SALT)
    return state


def test_runs_batch_with_parsed_arguments_and_writes_summary(fakes, tmp_path):
    summary = tmp_path / "out" / "summary.json"
    code = job.main(
        [
            "--source",
            "gharchive",
            "--date",
            DAY,
            "--hour",
            "1",
            "--hours",
            "2",
            "--landing-dir",
            str(tmp_path / "landing"),
            "--lake-dir",
            str(tmp_path / "lake"),
            "--late-window-hours",
            "6",
            "--summary",
            str(summary),
        ]
    )

    assert code == 0
    assert len(fakes["sessions"]) == 1
    (run,) = fakes["runs"]
    assert run["spark"] is fakes["sessions"][0]
    assert run["source"] == "gharchive"
    assert list(run["hours"]) == [(DAY, 0), (DAY, 1)]  # --hours ends at --hour, like ingest
    assert Path(run["landing_dir"]) == tmp_path / "landing"
    assert Path(run["lake_dir"]) == tmp_path / "lake"
    assert run["salt"] == SALT
    assert run["late_window_hours"] == 6
    written = json.loads(summary.read_text(encoding="utf-8"))
    assert written["bronze_rows"] == 3
    assert written["quarantined"] == {"malformed_json": 1}
    assert written["source"] == "gharchive"


def test_hours_cross_midnight(fakes):
    code = job.main(["--source", "app", "--date", DAY, "--hour", "0", "--hours", "2"])
    assert code == 0
    assert list(fakes["runs"][0]["hours"]) == [("2026-01-04", 23), (DAY, 0)]


def test_defaults(fakes):
    code = job.main(["--source", "gharchive", "--date", DAY, "--hour", "7"])
    assert code == 0
    run = fakes["runs"][0]
    assert list(run["hours"]) == [(DAY, 7)]
    assert Path(run["lake_dir"]) == Path("data/lake")
    assert Path(run["landing_dir"]) == Path("data/landing/gharchive")
    assert run["late_window_hours"] == 24


def test_default_landing_dir_for_app_events(fakes):
    job.main(["--source", "app", "--date", DAY, "--hour", "7"])
    assert Path(fakes["runs"][0]["landing_dir"]) == Path("data/landing/app")


@pytest.mark.parametrize(
    "argv",
    [
        ["--source", "kafka", "--date", DAY, "--hour", "0"],
        ["--date", DAY, "--hour", "0"],
        ["--source", "gharchive", "--hour", "0"],
        ["--source", "gharchive", "--date", DAY],
        ["--source", "gharchive", "--date", "2026-02-30", "--hour", "0"],
        ["--source", "gharchive", "--date", "2026-1-5", "--hour", "0"],
        ["--source", "gharchive", "--date", "05/01/2026", "--hour", "0"],
        ["--source", "gharchive", "--date", DAY, "--hour", "24"],
        ["--source", "gharchive", "--date", DAY, "--hour", "-1"],
        ["--source", "gharchive", "--date", DAY, "--hour", "0", "--hours", "0"],
        ["--source", "gharchive", "--date", DAY, "--hour", "0", "--hours", "25"],
        ["--source", "gharchive", "--date", DAY, "--hour", "0", "--late-window-hours", "-1"],
    ],
)
def test_bad_arguments_exit_2_without_spark(fakes, argv):
    with pytest.raises(SystemExit) as exc:
        job.main(argv)
    assert exc.value.code == 2
    assert fakes["sessions"] == []
    assert fakes["runs"] == []


@pytest.mark.parametrize("value", [None, "", "   "])
def test_missing_salt_fails_before_spark_session(fakes, monkeypatch, capsys, value):
    if value is None:
        monkeypatch.delenv("PII_HASH_SALT", raising=False)
    else:
        monkeypatch.setenv("PII_HASH_SALT", value)

    try:
        code = job.main(["--source", "gharchive", "--date", DAY, "--hour", "0"])
    except SystemExit as exc:  # either a return code or sys.exit is fine, as long as non-zero
        code = exc.code

    assert code not in (0, None)
    assert fakes["sessions"] == [], "SparkSession must not start without a PII salt"
    assert fakes["runs"] == []
    assert "PII_HASH_SALT" in capsys.readouterr().err


def test_salt_value_is_never_printed(fakes, capsys, tmp_path):
    job.main(
        [
            "--source",
            "gharchive",
            "--date",
            DAY,
            "--hour",
            "0",
            "--summary",
            str(tmp_path / "s.json"),
        ]
    )
    captured = capsys.readouterr()
    assert SALT not in captured.out
    assert SALT not in captured.err
    assert SALT not in (tmp_path / "s.json").read_text(encoding="utf-8")
