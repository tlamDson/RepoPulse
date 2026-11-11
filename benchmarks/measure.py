"""Turn N timed benchmark runs into one median row for ``runs.csv``."""

from __future__ import annotations

import statistics
from collections.abc import Mapping, Sequence

from repopulse.runs_log import COLUMNS

SECONDS_COLUMNS: tuple[str, ...] = (
    "total_seconds",
    *(f"q{i}_seconds" for i in range(1, 7)),
)
CONSTANT_COLUMNS: tuple[str, ...] = ("row_count", "bytes_read")


class MeasureError(ValueError):
    """The runs cannot be summarised into one row."""


def _median(values: Sequence[float]) -> float:
    return round(float(statistics.median(values)), 6)


def summarize_runs(
    runs: Sequence[Mapping[str, object]],
    *,
    variant: str,
    git_sha: str,
    data_date: str,
    run_at: str,
) -> dict[str, object]:
    """Median of every timing column; ``row_count``/``bytes_read`` must agree across runs."""
    if not runs:
        raise MeasureError("no runs to summarise")
    row: dict[str, object] = dict.fromkeys(COLUMNS)
    row.update(run_at=run_at, git_sha=git_sha, variant=variant, data_date=data_date)
    for col in SECONDS_COLUMNS:
        row[col] = _median([float(r[col]) for r in runs])
    rss = [float(r["peak_rss_mb"]) for r in runs if r.get("peak_rss_mb") is not None]
    row["peak_rss_mb"] = _median(rss) if rss else None
    for col in CONSTANT_COLUMNS:
        values = {int(r[col]) for r in runs}
        if len(values) != 1:
            raise MeasureError(f"{col} differs between runs: {sorted(values)}")
        row[col] = values.pop()
    row["notes"] = f"runs={len(runs)} median"
    return row
