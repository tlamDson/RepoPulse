"""Streaming throughput benchmark: runs.csv rows, CLI checks, reconciliation gate (no Kafka)."""

from __future__ import annotations

import csv
from pathlib import Path

import pytest
import yaml

from benchmarks import streaming_bench
from repopulse.runs_log import COLUMNS, validate_row
from repopulse.streaming import reconcile
from repopulse.streaming.producer import ProduceSummary

ROOT = Path(__file__).resolve().parents[1]
COMPOSE = ROOT / "benchmarks" / "kafka" / "docker-compose.yml"
SHA = "abc1234"
RUN_AT = "2026-11-11T00:00:00+00:00"


def produce(n: int = 1000, seconds: float = 2.0) -> ProduceSummary:
    return ProduceSummary(
        files=24,
        lines=n,
        delivered=n,
        failed=0,
        undelivered=0,
        unkeyed=1,
        bytes=123456,
        seconds=seconds,
        per_partition={0: n},
        errors=[],
    )


def consume(seconds: float = 4.0, batches: int = 5) -> dict:
    return {"seconds": seconds, "micro_batches": batches, "input_rows": 1000}


def matched(n: int = 1000) -> reconcile.Reconciliation:
    return reconcile.reconcile(
        batch_rows=n, produced=n, topic_messages=n, delta_rows=n, distinct_offsets=n
    )


def result(partitions: int, recon=None) -> streaming_bench.PartitionResult:
    return streaming_bench.PartitionResult(
        partitions=partitions, produce=produce(), consume=consume(), recon=recon or matched()
    )


def parse_notes(notes: str) -> dict[str, str]:
    return dict(item.split("=", 1) for item in notes.split())


def test_variant_name():
    assert streaming_bench.variant(1) == "streaming-throughput-p1"
    assert streaming_bench.variant(6) == "streaming-throughput-p6"


def test_build_row_is_a_valid_runs_csv_row():
    row = streaming_bench.build_row(
        result(3), data_date="2026-10-25", git_sha=SHA, run_at=RUN_AT, max_offsets_per_trigger=500
    )
    checked = validate_row(row)
    assert checked["variant"] == "streaming-throughput-p3"
    assert checked["data_date"] == "2026-10-25"
    assert checked["row_count"] == "1000"
    assert row["total_seconds"] == pytest.approx(6.0)
    notes = parse_notes(row["notes"])
    assert notes["partitions"] == "3"
    assert notes["brokers"] == "3"
    assert notes["rf"] == "3"
    assert notes["min_isr"] == "2"
    assert notes["produce_seconds"] == "2.0"
    assert notes["consume_seconds"] == "4.0"
    assert float(notes["produce_events_per_s"]) == pytest.approx(500.0)
    assert float(notes["consume_events_per_s"]) == pytest.approx(250.0)
    assert notes["batch_rows"] == "1000"
    assert notes["match"] == "true"
    assert notes["trigger"] == "availableNow"
    assert notes["micro_batches"] == "5"
    assert notes["max_offsets_per_trigger"] == "500"
    assert notes["unkeyed"] == "1"
    assert notes["kafka"] == streaming_bench.KAFKA_IMAGE


def test_build_row_refuses_a_mismatch():
    bad = reconcile.reconcile(
        batch_rows=1000, produced=1000, topic_messages=1000, delta_rows=999, distinct_offsets=999
    )
    with pytest.raises(streaming_bench.StreamingBenchError):
        streaming_bench.build_row(
            result(1, bad), data_date="2026-10-25", git_sha=SHA, run_at=RUN_AT
        )


@pytest.mark.parametrize(
    ("text", "expected"), [("1,3,6", [1, 3, 6]), ("6", [6]), (" 1, 3 ", [1, 3])]
)
def test_parse_partitions(text, expected):
    assert streaming_bench.parse_partitions(text) == expected


@pytest.mark.parametrize("text", ["", "0", "1,-3", "a", "1,1", "1,,3"])
def test_parse_partitions_rejects_bad_lists(text):
    with pytest.raises(ValueError):
        streaming_bench.parse_partitions(text)


def test_reference_files_follow_reference_order_and_max_hours(tmp_path):
    ref = {
        "data_date": "2026-10-25",
        "files": [{"hour": h, "name": f"2026-10-25-{h}.json.gz"} for h in range(24)],
    }
    files = streaming_bench.reference_files(tmp_path, ref, max_hours=3)
    assert files == [tmp_path / f"2026-10-25-{h}.json.gz" for h in range(3)]
    assert len(streaming_bench.reference_files(tmp_path, ref, max_hours=24)) == 24
    with pytest.raises(ValueError):
        streaming_bench.reference_files(tmp_path, ref, max_hours=0)


def read_rows(path: Path) -> list[dict]:
    with path.open(encoding="utf-8", newline="") as fh:
        return list(csv.DictReader(fh))


def test_run_benchmark_writes_one_row_per_matched_partition_count(tmp_path):
    out, summary = tmp_path / "rows.csv", tmp_path / "summary.md"
    code = streaming_bench.run_benchmark(
        [1, 3],
        run_one=result,
        data_date="2026-10-25",
        git_sha=SHA,
        run_at=RUN_AT,
        out=out,
        summary=summary,
    )
    assert code == 0
    rows = read_rows(out)
    assert out.read_text(encoding="utf-8").splitlines()[0] == ",".join(COLUMNS)
    assert [r["variant"] for r in rows] == ["streaming-throughput-p1", "streaming-throughput-p3"]
    text = summary.read_text(encoding="utf-8")
    assert "streaming-throughput-p1" in text and "streaming-throughput-p3" in text


def test_mismatch_exits_1_and_writes_no_row_for_that_partition_count(tmp_path):
    out, summary = tmp_path / "rows.csv", tmp_path / "summary.md"
    dup = reconcile.reconcile(
        batch_rows=1000, produced=1000, topic_messages=1000, delta_rows=1002, distinct_offsets=1000
    )

    def run_one(n):
        return result(n, dup if n == 3 else None)

    code = streaming_bench.run_benchmark(
        [1, 3, 6],
        run_one=run_one,
        data_date="2026-10-25",
        git_sha=SHA,
        run_at=RUN_AT,
        out=out,
        summary=summary,
    )
    assert code == 1
    assert [r["variant"] for r in read_rows(out)] == [
        "streaming-throughput-p1",
        "streaming-throughput-p6",
    ]
    text = summary.read_text(encoding="utf-8")
    assert "FAILED" in text and "duplicates=2" in text


def test_a_crashing_partition_count_does_not_stop_the_others(tmp_path):
    out, summary = tmp_path / "rows.csv", tmp_path / "summary.md"

    def run_one(n):
        if n == 1:
            raise RuntimeError("broker unreachable")
        return result(n)

    code = streaming_bench.run_benchmark(
        [1, 3],
        run_one=run_one,
        data_date="2026-10-25",
        git_sha=SHA,
        run_at=RUN_AT,
        out=out,
        summary=summary,
    )
    assert code == 1
    assert [r["variant"] for r in read_rows(out)] == ["streaming-throughput-p3"]
    assert "broker unreachable" in summary.read_text(encoding="utf-8")


def test_main_requires_git_sha_before_touching_kafka(capsys):
    assert streaming_bench.main(["--partitions", "1"]) == 2
    assert "git-sha" in capsys.readouterr().err


def test_main_rejects_bad_partitions():
    with pytest.raises(SystemExit) as exc:
        streaming_bench.main(["--git-sha", SHA, "--partitions", "0"])
    assert exc.value.code == 2


def test_main_fails_when_reference_files_are_missing(tmp_path, capsys):
    code = streaming_bench.main(
        ["--git-sha", SHA, "--data-dir", str(tmp_path), "--max-hours", "2", "--partitions", "1"]
    )
    assert code == 1
    assert "missing" in capsys.readouterr().err


def test_defaults_cover_three_brokers_and_partition_list():
    assert streaming_bench.DEFAULT_PARTITIONS == "1,3,6"
    assert len(streaming_bench.DEFAULT_BOOTSTRAP.split(",")) == 3


def test_kafka_image_matches_the_compose_file():
    data = yaml.safe_load(COMPOSE.read_text(encoding="utf-8"))
    images = {svc["image"] for svc in data["services"].values()}
    assert images == {streaming_bench.KAFKA_IMAGE}


def test_default_bootstrap_matches_the_compose_external_ports():
    data = yaml.safe_load(COMPOSE.read_text(encoding="utf-8"))
    published = set()
    for svc in data["services"].values():
        for port in svc["ports"]:
            published.add(str(port).split(":")[0].strip('"'))
    hosts = streaming_bench.DEFAULT_BOOTSTRAP.split(",")
    assert {h.split(":")[1] for h in hosts} == published
    assert all(h.startswith("localhost:") for h in hosts)
