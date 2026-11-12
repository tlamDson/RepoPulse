"""Delta Lake vs Apache Iceberg benchmark (E2): pure parts, no Spark and no network.

Session/catalog configuration, the split of the input files into base table and MERGE source, the
run-output parser, the per-format summary, the cross-check between formats and the CLI (with
``run_in_subprocess`` replaced). The Spark side lives in ``tests/test_format_bench_spark.py``
(``@pytest.mark.spark``).
"""

from __future__ import annotations

import csv
import gzip
import hashlib
import json
import re
import shutil
import subprocess
from pathlib import Path

import pytest

from benchmarks import format_bench, spark_naive
from benchmarks.record_run import read_artifact
from repopulse.runs_log import COLUMNS, validate_row

FIXTURE_DIR = Path(__file__).parent / "fixtures" / "gharchive_small"
QUERIES = tuple(f"q{i}" for i in range(1, 7))


# ---------------------------------------------------------------- names and configuration


def test_formats_and_variants():
    assert format_bench.FORMATS == ("delta", "iceberg")
    assert format_bench.variant("delta") == "format-delta"
    assert format_bench.variant("iceberg") == "format-iceberg"
    for fmt in format_bench.FORMATS:
        validate_row(
            {
                "run_at": "2026-11-11T08:00:00+00:00",
                "git_sha": "abc1234",
                "variant": format_bench.variant(fmt),
                "data_date": "2026-10-25",
                "total_seconds": 0,
            }
        )


def test_variant_rejects_unknown_format():
    with pytest.raises(format_bench.FormatBenchError, match="hudi"):
        format_bench.variant("hudi")


def test_iceberg_package_is_pinned_for_spark_35_scala_212():
    assert format_bench.ICEBERG_PACKAGE == (
        "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.9.2"
    )
    assert format_bench.ICEBERG_VERSION == "1.9.2"
    assert format_bench.ICEBERG_PACKAGE in format_bench.EXTRA_PACKAGES


def test_session_config_loads_delta_and_iceberg_and_fits_the_runner():
    conf = format_bench.session_config({})
    extensions = conf["spark.sql.extensions"].split(",")
    assert "io.delta.sql.DeltaSparkSessionExtension" in extensions
    assert "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions" in extensions
    assert (
        conf["spark.sql.catalog.spark_catalog"] == "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    )
    assert conf["spark.sql.session.timeZone"] == "UTC"
    assert conf["spark.master"].startswith("local[")
    assert 1 <= int(conf["spark.sql.shuffle.partitions"]) <= 64
    memory = conf["spark.driver.memory"]
    assert memory.endswith("g") and 1 <= int(memory[:-1]) <= 12  # 16 GB runner
    assert conf["spark.ui.enabled"] == "false"
    # the package list is set by delta's configure_spark_with_delta_pip (delta jar + extras)
    assert "spark.jars.packages" not in conf


def test_session_config_keeps_the_spark_naive_base_settings():
    base = spark_naive.session_config({"shuffle_partitions": 4, "driver_memory": "6g"})
    conf = format_bench.session_config({"shuffle_partitions": 4, "driver_memory": "6g"})
    for key in ("spark.master", "spark.driver.memory", "spark.sql.shuffle.partitions"):
        assert conf[key] == base[key], key
    assert conf["spark.sql.shuffle.partitions"] == "4"


def test_iceberg_catalog_name_is_a_safe_identifier_unique_per_work_dir(tmp_path):
    a = format_bench.iceberg_catalog_name(tmp_path / "a")
    b = format_bench.iceberg_catalog_name(tmp_path / "b")
    assert re.fullmatch(r"ice_[0-9a-f]{10}", a), a
    assert a != b
    assert format_bench.iceberg_catalog_name(tmp_path / "a") == a  # stable


def test_iceberg_catalog_conf_is_a_hadoop_catalog_in_the_work_dir(tmp_path):
    name = format_bench.iceberg_catalog_name(tmp_path)
    conf = format_bench.iceberg_catalog_conf(tmp_path)
    prefix = f"spark.sql.catalog.{name}"
    assert conf[prefix] == "org.apache.iceberg.spark.SparkCatalog"
    assert conf[f"{prefix}.type"] == "hadoop"
    assert conf[f"{prefix}.warehouse"] == (tmp_path / "iceberg").resolve().as_posix()
    assert conf[f"{prefix}.cache-enabled"] == "false"
    assert all(k.startswith(prefix) for k in conf)


def test_table_for_delta_is_a_path_table(tmp_path):
    table = format_bench.table_for("delta", tmp_path)
    assert table.fmt == "delta"
    assert table.location == (tmp_path / "delta" / "events").resolve()
    assert table.ident == f"delta.`{table.location.as_posix()}`"


def test_table_for_iceberg_is_a_catalog_table(tmp_path):
    table = format_bench.table_for("iceberg", tmp_path)
    name = format_bench.iceberg_catalog_name(tmp_path)
    assert table.fmt == "iceberg"
    assert table.ident == f"{name}.db.events"
    assert table.location == (tmp_path / "iceberg" / "db" / "events").resolve()


def test_table_for_rejects_a_path_with_a_backtick(tmp_path):
    with pytest.raises(format_bench.FormatBenchError, match="backtick"):
        format_bench.table_for("delta", tmp_path / "a`b")


def test_write_table_refuses_an_existing_location(tmp_path):
    table = format_bench.table_for("delta", tmp_path)
    table.location.mkdir(parents=True)
    with pytest.raises(format_bench.FormatBenchError, match="already exists"):
        format_bench.write_table(None, table, None)  # checked before Spark is touched


def test_table_size_counts_data_files_only(tmp_path):
    table = format_bench.table_for("delta", tmp_path)
    (table.location / "_delta_log").mkdir(parents=True)
    (table.location / "part-0.parquet").write_bytes(b"x" * 10)
    (table.location / "part-1.parquet").write_bytes(b"x" * 5)
    (table.location / ".part-0.parquet.crc").write_bytes(b"c")
    (table.location / "_delta_log" / "00000000000000000010.checkpoint.parquet").write_bytes(b"z")
    (table.location / "_delta_log" / "00000000000000000000.json").write_bytes(b"{}")
    assert format_bench.table_size(table) == (15, 2)


# ---------------------------------------------------------------- base table / MERGE source


def test_split_files_base_is_all_but_last_source_is_last_two():
    files = [Path(f"h{i}.json.gz") for i in range(4)]
    base, source = format_bench.split_files(files)
    assert base == files[:3]
    assert source == files[2:]


def test_split_files_with_two_files():
    files = [Path("a.json.gz"), Path("b.json.gz")]
    assert format_bench.split_files(files) == ([files[0]], files)


@pytest.mark.parametrize("count", [0, 1])
def test_split_files_needs_two_files(count):
    with pytest.raises(format_bench.FormatBenchError, match="at least 2"):
        format_bench.split_files([Path(f"h{i}.json.gz") for i in range(count)])


# ---------------------------------------------------------------- run output parser


def _run(
    total: float = 2.0,
    *,
    rows: int = 33,
    base: int = 20,
    source: int = 33,
    qrows=(4, 3, 2, 5, 3, 3),
    rss=500.0,
    table_bytes: int = 4096,
) -> dict:
    run = {
        "write_seconds": total / 4,
        **{f"{q}_seconds": total / 24 for q in QUERIES},
        "merge_seconds": total / 4,
        "merge_rerun_seconds": total / 8,
        "time_travel_seconds": total / 8,
        "load_seconds": 1.0,
        "startup_seconds": 3.0,
        **{f"q{i}_rows": n for i, n in enumerate(qrows, start=1)},
        "row_count": rows,
        "base_rows": base,
        "merge_source_rows": source,
        "bytes_read": 8192,
        "table_bytes": table_bytes,
        "table_files": 2,
        "peak_rss_mb": rss,
        "spark_version": "3.5.3",
        "delta_version": "3.3.2",
    }
    run["total_seconds"] = total
    return run


def test_parse_run_output_reads_the_last_json_line():
    run = _run()
    out = "log line\n" + json.dumps(run) + "\n\n"
    assert format_bench.parse_run_output(out, "delta") == run


@pytest.mark.parametrize(
    ("stdout", "message"),
    [
        ("", "no output"),
        ("Traceback (most recent call last):\n  boom", "not JSON"),
        (json.dumps(_run()) + "\nTraceback (most recent call last):", "not JSON"),
        ("[1, 2]", "JSON object"),
    ],
)
def test_parse_run_output_rejects_broken_output(stdout, message):
    with pytest.raises(format_bench.FormatBenchError, match=message):
        format_bench.parse_run_output(stdout, "iceberg")


@pytest.mark.parametrize(
    "key",
    [
        "total_seconds",
        "write_seconds",
        "merge_seconds",
        "merge_rerun_seconds",
        "time_travel_seconds",
        "startup_seconds",
        "q4_rows",
        "row_count",
        "base_rows",
        "merge_source_rows",
        "bytes_read",
    ],
)
def test_parse_run_output_rejects_missing_keys(key):
    run = _run()
    del run[key]
    with pytest.raises(format_bench.FormatBenchError, match=key):
        format_bench.parse_run_output(json.dumps(run), "delta")


@pytest.mark.parametrize(
    ("key", "value"),
    [
        ("q3_seconds", None),
        ("merge_seconds", -1.0),
        ("row_count", True),
        ("total_seconds", "fast"),
        ("q6_rows", -2),
        ("base_rows", 20.5),
        ("peak_rss_mb", -5.0),
        ("table_bytes", -1),
    ],
)
def test_parse_run_output_rejects_bad_values(key, value):
    run = _run()
    run[key] = value
    with pytest.raises(format_bench.FormatBenchError, match=key):
        format_bench.parse_run_output(json.dumps(run), "delta")


def test_parse_run_output_allows_unknown_peak_rss_and_table_size():
    run = _run(rss=None)
    del run["table_bytes"]
    del run["table_files"]
    parsed = format_bench.parse_run_output(json.dumps(run), "iceberg")
    assert parsed["peak_rss_mb"] is None
    assert "table_bytes" not in parsed


# ---------------------------------------------------------------- summary per format


def _notes(row: dict) -> dict[str, str]:
    tokens = row["notes"].split(" ")
    assert all("=" in t for t in tokens), row["notes"]
    pairs = [t.split("=", 1) for t in tokens]
    assert len({k for k, _ in pairs}) == len(pairs), "duplicate key in notes"
    return dict(pairs)


def _summary(runs, fmt="iceberg", **kwargs):
    return format_bench.summarize_format(
        runs,
        fmt=fmt,
        git_sha="abc1234",
        data_date="2026-10-25",
        run_at="2026-11-11T08:00:00+00:00",
        **kwargs,
    )


def test_summarize_format_builds_a_valid_runs_csv_row():
    runs = [
        _run(3.0, rss=900.0, table_bytes=4000),
        _run(1.0, rss=700.0, table_bytes=4200),
        _run(2.0, rss=800.0, table_bytes=4100),
    ]
    row = _summary(runs)
    validate_row(row)
    assert row["variant"] == "format-iceberg"
    assert row["data_date"] == "2026-10-25"
    assert row["git_sha"] == "abc1234"
    assert row["total_seconds"] == 2.0
    assert row["q1_seconds"] == round(2.0 / 24, 6)
    assert row["peak_rss_mb"] == 800.0
    assert row["row_count"] == 33
    assert row["bytes_read"] == 8192
    assert "\n" not in row["notes"] and "," not in row["notes"]
    notes = _notes(row)
    assert notes["format"] == "iceberg"
    assert notes["runs"] == "3"
    assert notes["stat"] == "median"
    assert notes["write_seconds"] == "0.5"
    assert notes["merge_seconds"] == "0.5"
    assert notes["merge_rerun_seconds"] == "0.25"
    assert notes["time_travel_seconds"] == "0.25"
    assert notes["load_seconds"] == "1.0"
    assert notes["startup_seconds"] == "3.0"
    assert notes["base_rows"] == "20"
    assert notes["merge_source_rows"] == "33"
    assert notes["table_bytes"] == "4100"
    assert notes["table_files"] == "2"
    assert notes["spark"] == "3.5.3"
    assert notes["delta"] == "3.3.2"
    assert notes["iceberg"] == "1.9.2"
    assert notes["cross_check"] == "full"


def test_summarize_format_marks_a_row_that_was_not_cross_checked():
    row = _summary([_run()], fmt="delta", cross_check="none")
    assert row["variant"] == "format-delta"
    assert _notes(row)["cross_check"] == "none"


def test_summarize_format_without_table_size_leaves_it_out_of_notes():
    run = _run()
    del run["table_bytes"]
    del run["table_files"]
    notes = _notes(_summary([run]))
    assert "table_bytes" not in notes and "table_files" not in notes


@pytest.mark.parametrize(
    ("changed", "key"),
    [
        ({"qrows": (4, 3, 2, 6, 3, 3)}, "q4_rows"),
        ({"rows": 34}, "row_count"),
        ({"base": 21}, "base_rows"),
        ({"source": 32}, "merge_source_rows"),
    ],
)
def test_summarize_format_rejects_counts_that_change_between_runs(changed, key):
    runs = [_run(1.0), _run(2.0, **changed), _run(3.0)]
    with pytest.raises(format_bench.FormatBenchError, match=key):
        _summary(runs)


def test_summarize_format_rejects_no_runs():
    with pytest.raises(format_bench.FormatBenchError):
        _summary([])


# ---------------------------------------------------------------- cross-check


def test_cross_check_accepts_equal_counts_with_different_timings():
    format_bench.cross_check([_run(1.0)] * 2, [_run(9.0, table_bytes=1)] * 2)


@pytest.mark.parametrize(
    ("iceberg_run", "key"),
    [
        (_run(rows=32), "row_count"),
        (_run(base=19), "base_rows"),
        (_run(source=30), "merge_source_rows"),
        (_run(qrows=(4, 3, 9, 5, 3, 3)), "q3_rows"),
    ],
)
def test_cross_check_rejects_formats_that_disagree(iceberg_run, key):
    with pytest.raises(format_bench.FormatBenchError, match=key) as info:
        format_bench.cross_check([_run()], [iceberg_run])
    assert "delta=" in str(info.value) and "iceberg=" in str(info.value)


# ---------------------------------------------------------------- child process


def test_run_in_subprocess_parses_the_child_output(tmp_path, monkeypatch):
    seen = {}

    def fake_run(cmd, **kwargs):
        seen["cmd"] = cmd
        seen["timeout"] = kwargs.get("timeout")
        return subprocess.CompletedProcess(cmd, 0, stdout=json.dumps(_run()) + "\n", stderr="")

    monkeypatch.setattr(format_bench.subprocess, "run", fake_run)
    run = format_bench.run_in_subprocess(
        "iceberg",
        data_dir=tmp_path / "data",
        reference=tmp_path / "reference.toml",
        seed=tmp_path / "seed.csv",
        work_dir=tmp_path / "work",
        timeout_s=60,
    )
    assert run["row_count"] == 33
    cmd = seen["cmd"]
    assert "--single" in cmd
    assert cmd[cmd.index("--format") + 1] == "iceberg"
    assert cmd[cmd.index("--data-dir") + 1] == str(tmp_path / "data")
    assert cmd[cmd.index("--work-dir") + 1] == str(tmp_path / "work")
    assert seen["timeout"] == 60


def test_run_in_subprocess_reports_a_failing_child(tmp_path, monkeypatch):
    def fake_run(cmd, **kwargs):
        return subprocess.CompletedProcess(cmd, 1, stdout="", stderr="java.lang.OutOfMemoryError")

    monkeypatch.setattr(format_bench.subprocess, "run", fake_run)
    with pytest.raises(format_bench.FormatBenchError, match="OutOfMemoryError"):
        format_bench.run_in_subprocess(
            "delta",
            data_dir=tmp_path,
            reference=tmp_path / "r.toml",
            seed=tmp_path / "s.csv",
            work_dir=tmp_path / "w",
            timeout_s=60,
        )


def test_run_in_subprocess_reports_a_timeout(tmp_path, monkeypatch):
    def fake_run(cmd, **kwargs):
        raise subprocess.TimeoutExpired(cmd, kwargs["timeout"])

    monkeypatch.setattr(format_bench.subprocess, "run", fake_run)
    with pytest.raises(format_bench.FormatBenchError, match="timed out"):
        format_bench.run_in_subprocess(
            "delta",
            data_dir=tmp_path,
            reference=tmp_path / "r.toml",
            seed=tmp_path / "s.csv",
            work_dir=tmp_path / "w",
            timeout_s=5,
        )


# ---------------------------------------------------------------- CLI


@pytest.fixture
def gz_dir(tmp_path: Path) -> Path:
    out = tmp_path / "raw"
    out.mkdir()
    for src in sorted(FIXTURE_DIR.glob("*.json")):
        with src.open("rb") as fin, gzip.open(out / f"{src.stem}.json.gz", "wb") as fout:
            shutil.copyfileobj(fin, fout)
    return out


def _reference_for(paths: list[Path]) -> str:
    lines = ['data_date = "2026-01-05"', 'url_template = "https://data.gharchive.org/{name}"']
    for hour, path in enumerate(paths):
        blob = path.read_bytes()
        lines += [
            "[[files]]",
            f"hour = {hour}",
            f'name = "{path.name}"',
            f'sha256 = "{hashlib.sha256(blob).hexdigest()}"',
            f"bytes = {len(blob)}",
        ]
    return "\n".join(lines) + "\n"


def _cli(gz_dir: Path, tmp_path: Path, *extra: str, files: list[Path] | None = None):
    ref = tmp_path / "reference.toml"
    ref.write_text(_reference_for(files or sorted(gz_dir.glob("*.json.gz"))), encoding="utf-8")
    out = tmp_path / "out" / "format-rows.csv"
    summary = tmp_path / "out" / "format-summary.md"
    argv = [
        "--data-dir",
        str(gz_dir),
        "--reference",
        str(ref),
        "--work-dir",
        str(tmp_path / "work"),
        "--out",
        str(out),
        "--summary",
        str(summary),
        *extra,
    ]
    return argv, out, summary


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8", newline="") as fh:
        rows = list(csv.reader(fh))
    assert rows[0] == list(COLUMNS)
    return [dict(zip(COLUMNS, r, strict=True)) for r in rows[1:]]


def _must_not_run(*args, **kwargs):
    raise AssertionError("benchmark ran although the input should have been rejected")


def test_default_runs_is_five():
    assert format_bench.build_parser().parse_args([]).runs == 5


def test_default_input_is_the_reference_day():
    args = format_bench.build_parser().parse_args([])
    assert args.reference == format_bench.REFERENCE_TOML
    assert args.data_dir is None  # main() uses data/raw/gharchive/<data_date>
    assert format_bench.DEFAULT_DATA_ROOT == Path("data/raw/gharchive")


@pytest.mark.parametrize("sha", [None, "NOT-A-SHA"])
def test_main_rejects_missing_or_bad_git_sha_before_running(gz_dir, tmp_path, monkeypatch, sha):
    monkeypatch.setattr(format_bench, "run_in_subprocess", _must_not_run)
    extra = () if sha is None else ("--git-sha", sha)
    argv, out, summary = _cli(gz_dir, tmp_path, *extra)
    assert format_bench.main(argv) == 2
    assert not out.exists()
    assert not summary.exists()


def test_main_rejects_zero_runs(gz_dir, tmp_path, monkeypatch):
    monkeypatch.setattr(format_bench, "run_in_subprocess", _must_not_run)
    argv, out, _ = _cli(gz_dir, tmp_path, "--git-sha", "abc1234", "--runs", "0")
    assert format_bench.main(argv) == 2
    assert not out.exists()


def test_main_stops_with_exit_2_on_checksum_mismatch(gz_dir, tmp_path, monkeypatch):
    monkeypatch.setattr(format_bench, "run_in_subprocess", _must_not_run)
    argv, out, summary = _cli(gz_dir, tmp_path, "--git-sha", "abc1234")
    victim = sorted(gz_dir.glob("*.json.gz"))[0]
    victim.write_bytes(victim.read_bytes() + b"tampered")
    assert format_bench.main(argv) == 2
    assert not out.exists()
    assert not summary.exists()


def test_main_stops_with_exit_2_when_the_reference_has_one_file(gz_dir, tmp_path, monkeypatch):
    monkeypatch.setattr(format_bench, "run_in_subprocess", _must_not_run)
    one = sorted(gz_dir.glob("*.json.gz"))[:1]
    argv, out, _ = _cli(gz_dir, tmp_path, "--git-sha", "abc1234", files=one)
    assert format_bench.main(argv) == 2
    assert not out.exists()


def test_main_writes_one_row_per_format(gz_dir, tmp_path, monkeypatch):
    calls: list[str] = []

    def fake(fmt, **kwargs):
        calls.append(fmt)
        assert kwargs["data_dir"] == gz_dir
        assert kwargs["timeout_s"] > 0
        return _run(1.0 if fmt == "delta" else 2.0)

    monkeypatch.setattr(format_bench, "run_in_subprocess", fake)
    argv, out, summary = _cli(gz_dir, tmp_path, "--git-sha", "abc1234", "--runs", "3")
    assert format_bench.main(argv) == 0
    assert calls == ["delta"] * 3 + ["iceberg"] * 3
    rows = _read_csv(out)
    assert [r["variant"] for r in rows] == ["format-delta", "format-iceberg"]
    assert {r["data_date"] for r in rows} == {"2026-01-05"}
    assert [r["total_seconds"] for r in rows] == ["1.0", "2.0"]
    assert all("cross_check=full" in r["notes"] for r in rows)
    assert len(read_artifact(out)) == 2  # record_run.py can merge it
    text = summary.read_text(encoding="utf-8")
    assert "format-delta" in text and "format-iceberg" in text


def test_main_keeps_the_other_format_when_one_fails(gz_dir, tmp_path, monkeypatch, capsys):
    def fake(fmt, **kwargs):
        if fmt == "iceberg":
            raise format_bench.FormatBenchError("iceberg run timed out after 2700 s")
        return _run()

    monkeypatch.setattr(format_bench, "run_in_subprocess", fake)
    argv, out, summary = _cli(gz_dir, tmp_path, "--git-sha", "abc1234", "--runs", "2")
    assert format_bench.main(argv) == 1
    rows = _read_csv(out)
    assert [r["variant"] for r in rows] == ["format-delta"]
    assert "cross_check=none" in rows[0]["notes"]
    err = capsys.readouterr().err
    assert "format-iceberg" in err and "timed out" in err
    assert "timed out" in summary.read_text(encoding="utf-8")


def test_main_writes_no_row_when_the_formats_disagree(gz_dir, tmp_path, monkeypatch, capsys):
    def fake(fmt, **kwargs):
        return _run(qrows=(4, 3, 9, 5, 3, 3)) if fmt == "iceberg" else _run()

    monkeypatch.setattr(format_bench, "run_in_subprocess", fake)
    argv, out, summary = _cli(gz_dir, tmp_path, "--git-sha", "abc1234", "--runs", "2")
    assert format_bench.main(argv) == 1
    assert _read_csv(out) == []
    assert "q3_rows" in capsys.readouterr().err
    assert summary.is_file()


def test_main_writes_no_row_for_a_format_whose_counts_change(gz_dir, tmp_path, monkeypatch):
    seen = {"delta": 0}

    def fake(fmt, **kwargs):
        if fmt == "delta":
            seen["delta"] += 1
            return _run(rows=33 + seen["delta"])  # not stable between runs
        return _run()

    monkeypatch.setattr(format_bench, "run_in_subprocess", fake)
    argv, out, _ = _cli(gz_dir, tmp_path, "--git-sha", "abc1234", "--runs", "2")
    assert format_bench.main(argv) == 1
    rows = _read_csv(out)
    assert [r["variant"] for r in rows] == ["format-iceberg"]
    assert "cross_check=none" in rows[0]["notes"]
