"""Run site/app.js in Node against a fake DOM and check what the Q2 section does with the data.

test_site_static.py only greps the source; this proves the behaviour: cohorts whose W+4 / W+12 week
has not started (NULL pct from the mart) must not draw a chart, and the table says "not yet
measurable" instead of a dash or 0.0%.
"""

from __future__ import annotations

import json
import shutil
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
HARNESS = Path(__file__).with_name("site_harness.js")
NODE = shutil.which("node")
pytestmark = pytest.mark.skipif(NODE is None, reason="node is not installed")

COLUMNS = [
    "cohort_week",
    "cohort_size",
    "retained_w4",
    "retained_w12",
    "retention_w4_pct",
    "retention_w12_pct",
]
META = {
    "generated_at": "2026-11-18T00:00:00+00:00",
    "git_sha": None,
    "latest_data_date": "2026-11-17",
    "first_data_date": "2026-11-11",
    "simulated_sections": [],
}


def row(week, size, w4, w12, p4, p12):
    return dict(zip(COLUMNS, [week, size, w4, w12, p4, p12], strict=True))


def run_site(rows):
    marts = {"mart_cohort_retention": {"columns": COLUMNS, "rows": rows}}
    proc = subprocess.run(
        [NODE, str(HARNESS), str(ROOT / "site" / "app.js")],
        input=json.dumps({"meta": META, "marts": marts}),
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )
    assert proc.returncode == 0, proc.stderr
    return json.loads(proc.stdout)


def test_immature_cohorts_draw_no_chart_and_say_not_yet_measurable():
    out = run_site(
        [row("2026-11-09", 39, 0, 0, None, None), row("2026-11-16", 18, 0, 0, None, None)]
    )
    assert out["chart_has_svg"] is False
    assert out["chart_text"] == ""
    assert "not yet measurable" in out["status"]
    assert "not yet measurable" in out["table_text"]
    assert "0.0%" not in out["table_text"]


def test_the_data_window_start_comes_from_meta():
    out = run_site([row("2026-11-09", 39, 0, 0, None, None)])
    assert out["window_start"] == "2026-11-11"


def test_measurable_cohorts_still_draw_the_chart_and_immature_cells_say_so():
    out = run_site(
        [
            row("2026-01-05", 10, 5, 0, 50.0, None),
            row("2026-01-12", 10, 2, 0, 20.0, None),
            row("2026-02-02", 4, 0, 0, None, None),
        ]
    )
    assert out["chart_has_svg"] is True
    assert out["status"] == ""
    assert "50.0%" in out["table_text"]
    assert "not yet measurable" in out["table_text"]
