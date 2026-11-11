"""Naive DuckDB baseline: Q1-Q6 on a hand-written fixture give hand-computed numbers.

Fixture ``tests/fixtures/gharchive_small/*.json`` (32 lines, gzip-compressed into ``tmp_path``
at test time so the pipeline reads real ``*.json.gz`` files). Weeks are ISO weeks starting Monday:
W0 = 2026-01-05, W1 = 01-12, W4 = 02-02, W5 = 02-09, W12 = 03-30, W13 = 04-06.

Contribution events (PullRequest*, Issues*, IssueComment*) by humans on seed repos:
  apache/spark (compute)    alice 01-05 (+ duplicate line), bob 01-05, sparkmaint 01-05 x2,
                            sparkmaint 01-06, 01-07, 01-08; bob 01-08; dave 01-09; alice 01-09,
                            alice 02-03, bob 02-10
  duckdb/duckdb (warehouse) carol 01-07, bob 01-12, duckmaint 01-12, 01-14, bob 03-31
  trinodb/trino (compute)   carol 01-13 x2, carol 02-11 (comment on her own PR), carol 04-07
Ignored on purpose: dependabot[bot] (a bot), PushEvent/CreateEvent by alice on spark 01-06,
WatchEvent/ForkEvent (not contributions), alice/toy (not in the seed).
"""

import csv
import gzip
import hashlib
import shutil
from datetime import date
from pathlib import Path

import pytest

from benchmarks import naive_duckdb
from repopulse.runs_log import COLUMNS

FIXTURE_DIR = Path(__file__).parent / "fixtures" / "gharchive_small"
FIXTURE_LINES = 32  # 18 + 14 lines, one of them an exact duplicate of event 9000000002


def d(text: str) -> date:
    return date.fromisoformat(text)


@pytest.fixture
def gz_dir(tmp_path: Path) -> Path:
    out = tmp_path / "raw"
    out.mkdir()
    for src in sorted(FIXTURE_DIR.glob("*.json")):
        with src.open("rb") as fin, gzip.open(out / f"{src.stem}.json.gz", "wb") as fout:
            shutil.copyfileobj(fin, fout)
    return out


@pytest.fixture
def result(gz_dir: Path) -> dict:
    return naive_duckdb.run_once(gz_dir, collect=True)


def _rows(result: dict, q: str) -> list[tuple]:
    return [tuple(r) for r in result["results"][q]["rows"]]


def _cols(result: dict, q: str) -> list[str]:
    return list(result["results"][q]["columns"])


def _same(actual: list[tuple], expected: list[tuple]) -> None:
    assert len(actual) == len(expected), (actual, expected)
    for a_row, e_row in zip(actual, expected, strict=True):
        assert len(a_row) == len(e_row), (a_row, e_row)
        for a, e in zip(a_row, e_row, strict=True):
            if isinstance(e, float):
                assert a == pytest.approx(e, abs=1e-9), (a_row, e_row)
            else:
                assert a == e, (a_row, e_row)


# ---------------------------------------------------------------- load / measurements


def test_raw_table_counts_duplicate_lines_and_every_event_type(result, gz_dir):
    # baseline does not dedupe: 32 lines read, including the duplicated PR-opened line
    assert result["row_count"] == FIXTURE_LINES
    assert result["bytes_read"] == sum(p.stat().st_size for p in gz_dir.glob("*.json.gz"))
    for key in ("total_seconds", *(f"q{i}_seconds" for i in range(1, 7))):
        assert result[key] >= 0


def test_full_refresh_does_not_accumulate_rows(gz_dir):
    first = naive_duckdb.run_once(gz_dir)
    second = naive_duckdb.run_once(gz_dir)
    assert first["row_count"] == second["row_count"] == FIXTURE_LINES


def test_queries_do_not_depend_on_push_create_or_delete_events():
    for i in range(1, 7):
        sql = (naive_duckdb.QUERY_DIR / f"q{i}.sql").read_text(encoding="utf-8")
        code = "\n".join(line.split("--", 1)[0] for line in sql.splitlines())
        for banned in ("PushEvent", "CreateEvent", "DeleteEvent"):
            assert banned not in code, f"q{i}.sql uses {banned}"


# ---------------------------------------------------------------- Q1


def test_q1_active_new_and_returning_contributors(result):
    assert _cols(result, "q1") == [
        "period",
        "period_start",
        "scope",
        "scope_key",
        "active_contributors",
        "new_contributors",
        "returning_contributors",
    ]
    expected = [
        # day / category compute = spark + trino
        ("day", d("2026-01-05"), "category", "compute", 3, 3, 0),  # alice, bob, sparkmaint
        ("day", d("2026-01-06"), "category", "compute", 1, 0, 1),  # sparkmaint (alice push ignored)
        ("day", d("2026-01-07"), "category", "compute", 1, 0, 1),  # sparkmaint
        ("day", d("2026-01-08"), "category", "compute", 2, 0, 2),  # bob, sparkmaint
        ("day", d("2026-01-09"), "category", "compute", 2, 1, 1),  # dave new, alice returning
        ("day", d("2026-01-13"), "category", "compute", 1, 1, 0),  # carol first on trino
        ("day", d("2026-02-03"), "category", "compute", 1, 0, 1),  # alice
        ("day", d("2026-02-10"), "category", "compute", 1, 0, 1),  # bob
        ("day", d("2026-02-11"), "category", "compute", 1, 0, 1),  # carol (trino)
        ("day", d("2026-04-07"), "category", "compute", 1, 0, 1),  # carol
        # day / category warehouse = duckdb (bot comment 01-07 ignored)
        ("day", d("2026-01-07"), "category", "warehouse", 1, 1, 0),  # carol
        ("day", d("2026-01-12"), "category", "warehouse", 2, 2, 0),  # bob, duckmaint
        ("day", d("2026-01-14"), "category", "warehouse", 1, 0, 1),  # duckmaint
        ("day", d("2026-03-31"), "category", "warehouse", 1, 0, 1),  # bob
        # day / repo
        ("day", d("2026-01-05"), "repo", "apache/spark", 3, 3, 0),
        ("day", d("2026-01-06"), "repo", "apache/spark", 1, 0, 1),
        ("day", d("2026-01-07"), "repo", "apache/spark", 1, 0, 1),
        ("day", d("2026-01-08"), "repo", "apache/spark", 2, 0, 2),
        ("day", d("2026-01-09"), "repo", "apache/spark", 2, 1, 1),
        ("day", d("2026-02-03"), "repo", "apache/spark", 1, 0, 1),
        ("day", d("2026-02-10"), "repo", "apache/spark", 1, 0, 1),
        ("day", d("2026-01-07"), "repo", "duckdb/duckdb", 1, 1, 0),
        ("day", d("2026-01-12"), "repo", "duckdb/duckdb", 2, 2, 0),
        ("day", d("2026-01-14"), "repo", "duckdb/duckdb", 1, 0, 1),
        ("day", d("2026-03-31"), "repo", "duckdb/duckdb", 1, 0, 1),
        ("day", d("2026-01-13"), "repo", "trinodb/trino", 1, 1, 0),
        ("day", d("2026-02-11"), "repo", "trinodb/trino", 1, 0, 1),
        ("day", d("2026-04-07"), "repo", "trinodb/trino", 1, 0, 1),
        # week / category
        ("week", d("2026-01-05"), "category", "compute", 4, 4, 0),  # alice, bob, sparkmaint, dave
        ("week", d("2026-01-12"), "category", "compute", 1, 1, 0),  # carol (trino)
        ("week", d("2026-02-02"), "category", "compute", 1, 0, 1),  # alice
        ("week", d("2026-02-09"), "category", "compute", 2, 0, 2),  # bob (spark), carol (trino)
        ("week", d("2026-04-06"), "category", "compute", 1, 0, 1),  # carol
        ("week", d("2026-01-05"), "category", "warehouse", 1, 1, 0),  # carol
        ("week", d("2026-01-12"), "category", "warehouse", 2, 2, 0),  # bob new here, duckmaint
        ("week", d("2026-03-30"), "category", "warehouse", 1, 0, 1),  # bob
        # week / repo
        ("week", d("2026-01-05"), "repo", "apache/spark", 4, 4, 0),
        ("week", d("2026-02-02"), "repo", "apache/spark", 1, 0, 1),
        ("week", d("2026-02-09"), "repo", "apache/spark", 1, 0, 1),
        ("week", d("2026-01-05"), "repo", "duckdb/duckdb", 1, 1, 0),
        ("week", d("2026-01-12"), "repo", "duckdb/duckdb", 2, 2, 0),
        ("week", d("2026-03-30"), "repo", "duckdb/duckdb", 1, 0, 1),
        ("week", d("2026-01-12"), "repo", "trinodb/trino", 1, 1, 0),
        ("week", d("2026-02-09"), "repo", "trinodb/trino", 1, 0, 1),
        ("week", d("2026-04-06"), "repo", "trinodb/trino", 1, 0, 1),
    ]
    _same(_rows(result, "q1"), expected)


# ---------------------------------------------------------------- Q2


def test_q2_cohort_retention_at_w4_and_w12(result):
    assert _cols(result, "q2") == [
        "cohort_week",
        "cohort_size",
        "retained_w4",
        "retained_w12",
        "retention_w4_pct",
        "retention_w12_pct",
    ]
    # First PR opened on a seed repo: alice #101 01-05 (W0), bob #105 01-08 (W0), carol #50
    # 01-13 (W1). dependabot[bot] excluded; alice's PR on alice/toy is outside the seed.
    # W0 cohort {alice, bob}: W4 = 02-02..02-08 -> alice (02-03) yes, bob (02-10 is W5) no
    #   -> 1/2 = 50.0; W12 = 03-30..04-05 -> bob (03-31 duckdb) yes, alice only on alice/toy
    #   -> 1/2 = 50.0
    # W1 cohort {carol}: W5 = 02-09.. -> 02-11 comment on trino -> 1/1 = 100.0;
    #   W13 = 04-06.. -> 04-07 trino -> 100.0. (A W+5 window would give W0 1/2 via bob 02-10
    #   but W1 0/1, so this row pins the offset.)
    expected = [
        (d("2026-01-05"), 2, 1, 1, 50.0, 50.0),
        (d("2026-01-12"), 1, 1, 1, 100.0, 100.0),
    ]
    _same(_rows(result, "q2"), expected)


# ---------------------------------------------------------------- Q3


def test_q3_fork_pr_merge_funnel_and_time_to_merge(result):
    assert _cols(result, "q3") == [
        "repo_name",
        "category",
        "forkers",
        "forkers_with_pr",
        "forkers_with_merge",
        "prs_opened",
        "prs_merged",
        "merge_hours_p50",
        "merge_hours_p90",
    ]
    # spark: forkers alice (01-05 09:00), dave (01-05 08:00); alice opened #101 at 10:00 after
    #   the fork and it was merged -> 1 / 1. dave never opens a PR (other people's PRs after his
    #   fork do not count for him). Human PRs opened: #101, #105 (bot #103 excluded),
    #   both merged. Hours to merge: #101 01-05 10:00 -> 01-06 10:00 = 24; #105 10:00 -> 16:00 = 6
    #   -> p50 = (6 + 24) / 2 = 15.0, p90 = 6 + 0.9 * (24 - 6) = 22.2
    # duckdb: bob forked 09:00, opened #8 10:00, merged 01-14 10:00 = 48 h
    # trino: no fork; carol opened #50 and closed it without merge
    expected = [
        ("apache/spark", "compute", 2, 1, 1, 2, 2, 15.0, 22.2),
        ("duckdb/duckdb", "warehouse", 1, 1, 1, 1, 1, 48.0, 48.0),
        ("trinodb/trino", "compute", 0, 0, 0, 1, 0, None, None),
    ]
    _same(_rows(result, "q3"), expected)


# ---------------------------------------------------------------- Q4


def test_q4_first_response_and_close_rate(result):
    assert _cols(result, "q4") == [
        "repo_name",
        "item_type",
        "opened",
        "responded",
        "response_hours_p50",
        "response_hours_p90",
        "closed",
        "close_rate_pct",
    ]
    # spark issues: #102 (bob 11:00, sparkmaint 15:00 -> 4 h, closed), #106 (dave 08:00, alice
    #   18:00 -> 10 h), #104 (no response) -> p50 7.0, p90 4 + 0.9 * 6 = 9.4; closed 1/3 = 33.33
    # spark PRs: #101 review 2 h after open; #105 merged without comment/review -> no response;
    #   both merged -> closed 2/2
    # duckdb issue #7: only a bot comment -> no response, not closed
    # duckdb PR #8: duckmaint comment 6 h after open; merged
    # trino PR #50: only the author touched it (incl. her own 02-11 comment) -> no response;
    #   closed
    expected = [
        ("apache/spark", "issue", 3, 2, 7.0, 9.4, 1, 33.33),
        ("apache/spark", "pull_request", 2, 1, 2.0, 2.0, 2, 100.0),
        ("duckdb/duckdb", "issue", 1, 0, None, None, 0, 0.0),
        ("duckdb/duckdb", "pull_request", 1, 1, 6.0, 6.0, 1, 100.0),
        ("trinodb/trino", "pull_request", 1, 0, None, None, 1, 100.0),
    ]
    _same(_rows(result, "q4"), expected)


# ---------------------------------------------------------------- Q5


def test_q5_bus_factor_and_bot_share(result):
    assert _cols(result, "q5") == [
        "repo_name",
        "category",
        "contributions",
        "bot_contributions",
        "bot_share_pct",
        "human_contributors",
        "top1_share_pct",
        "top2_share_pct",
        "top3_share_pct",
    ]
    # spark events: alice 4 (incl. the duplicate line, baseline does not dedupe), bob 3,
    #   sparkmaint 5, dave 1, dependabot[bot] 1 -> 14 total, bot 1/14 = 7.14 %;
    #   human 13: top1 5/13 = 38.46, top2 9/13 = 69.23, top3 12/13 = 92.31
    # duckdb: carol 1, bot 1, bob 2, duckmaint 2 -> 6, bot 1/6 = 16.67 %; human 5:
    #   top1 2/5 = 40.0, top2 4/5 = 80.0, top3 5/5 = 100.0
    # trino: carol 4 -> bot 0.0, top1..3 = 100.0
    expected = [
        ("apache/spark", "compute", 14, 1, 7.14, 4, 38.46, 69.23, 92.31),
        ("duckdb/duckdb", "warehouse", 6, 1, 16.67, 3, 40.0, 80.0, 100.0),
        ("trinodb/trino", "compute", 4, 0, 0.0, 1, 100.0, 100.0, 100.0),
    ]
    _same(_rows(result, "q5"), expected)


# ---------------------------------------------------------------- Q6


def test_q6_health_score_ranking(result):
    assert _cols(result, "q6") == [
        "health_rank",
        "repo_name",
        "category",
        "activity_score",
        "retention_score",
        "merge_score",
        "response_score",
        "close_score",
        "diversity_score",
        "health_score",
    ]
    # score = 100 * (0.25 activity + 0.15 retention + 0.15 merge + 0.20 response
    #               + 0.10 close + 0.15 diversity)
    # spark: activity 4/4 = 1; retention 2/4 (alice W0+W4, bob W0+W5) = 0.5; merge 2/2 = 1;
    #   response (2 + 1)/(3 + 2) = 0.6; close (1 + 2)/5 = 0.6; diversity 1 - 5/13 = 0.6154
    #   -> 0.25 + 0.075 + 0.15 + 0.12 + 0.06 + 0.0923077 = 0.7473077 -> 74.73
    # duckdb: activity 3/4 = 0.75; retention 1/3 (bob W1+W12); merge 1/1; response 1/2;
    #   close 1/2; diversity 1 - 0.4 = 0.6
    #   -> 0.1875 + 0.05 + 0.15 + 0.10 + 0.05 + 0.09 = 0.6275 -> 62.75
    # trino: activity 1/4; retention 1/1 (carol W1+W5+W13); merge 0/1; response 0/1; close 1/1;
    #   diversity 1 - 1 = 0 -> 0.0625 + 0.15 + 0.10 = 0.3125 -> 31.25
    expected = [
        (1, "apache/spark", "compute", 1.0, 0.5, 1.0, 0.6, 0.6, 0.6154, 74.73),
        (2, "duckdb/duckdb", "warehouse", 0.75, 0.3333, 1.0, 0.5, 0.5, 0.6, 62.75),
        (3, "trinodb/trino", "compute", 0.25, 1.0, 0.0, 0.0, 1.0, 0.0, 31.25),
    ]
    _same(_rows(result, "q6"), expected)


# ---------------------------------------------------------------- CLI


def _reference_for(data_dir: Path) -> str:
    lines = ['data_date = "2026-01-05"', 'url_template = "https://data.gharchive.org/{name}"']
    for hour, path in enumerate(sorted(data_dir.glob("*.json.gz"))):
        blob = path.read_bytes()
        lines += [
            "[[files]]",
            f"hour = {hour}",
            f'name = "{path.name}"',
            f'sha256 = "{hashlib.sha256(blob).hexdigest()}"',
            f"bytes = {len(blob)}",
        ]
    return "\n".join(lines) + "\n"


def test_main_writes_one_median_row(gz_dir, tmp_path):
    ref = tmp_path / "reference.toml"
    ref.write_text(_reference_for(gz_dir), encoding="utf-8")
    out = tmp_path / "out" / "benchmark-row.csv"
    code = naive_duckdb.main(
        [
            "--data-dir",
            str(gz_dir),
            "--reference",
            str(ref),
            "--runs",
            "2",
            "--variant",
            "baseline",
            "--git-sha",
            "abc1234",
            "--out",
            str(out),
        ]
    )
    assert code == 0
    with out.open(encoding="utf-8", newline="") as fh:
        rows = list(csv.reader(fh))
    assert rows[0] == list(COLUMNS)
    assert len(rows) == 2
    row = dict(zip(COLUMNS, rows[1], strict=True))
    assert row["variant"] == "baseline"
    assert row["git_sha"] == "abc1234"
    assert row["data_date"] == "2026-01-05"
    assert row["row_count"] == str(FIXTURE_LINES)
    assert row["notes"] == "runs=2 median"
    assert float(row["total_seconds"]) > 0


def test_main_rejects_invalid_variant_before_running(gz_dir, tmp_path, monkeypatch, capsys):
    ref = tmp_path / "reference.toml"
    ref.write_text(_reference_for(gz_dir), encoding="utf-8")

    def _must_not_run(*args, **kwargs):
        raise AssertionError("pipeline ran with an invalid variant")

    monkeypatch.setattr(naive_duckdb, "run_in_subprocess", _must_not_run)
    out = tmp_path / "benchmark-row.csv"
    code = naive_duckdb.main(
        ["--data-dir", str(gz_dir), "--reference", str(ref), "--git-sha", "abc1234"]
        + ["--variant", "Bad Variant", "--out", str(out)]
    )
    assert code != 0
    assert "variant" in capsys.readouterr().err
    assert not out.exists()


def test_main_refuses_files_not_listed_in_the_reference(gz_dir, tmp_path, monkeypatch, capsys):
    ref = tmp_path / "reference.toml"
    ref.write_text(_reference_for(gz_dir), encoding="utf-8")
    (gz_dir / "2026-01-06-0.json.gz").write_bytes(gzip.compress(b"{}\n"))

    def _must_not_run(*args, **kwargs):
        raise AssertionError("pipeline ran on files outside the reference")

    monkeypatch.setattr(naive_duckdb, "run_in_subprocess", _must_not_run)
    out = tmp_path / "benchmark-row.csv"
    code = naive_duckdb.main(
        ["--data-dir", str(gz_dir), "--reference", str(ref), "--git-sha", "abc1234"]
        + ["--out", str(out)]
    )
    assert code != 0
    assert "2026-01-06-0.json.gz" in capsys.readouterr().err
    assert not out.exists()


def test_main_stops_before_pipeline_when_checksum_is_wrong(gz_dir, tmp_path, monkeypatch):
    ref = tmp_path / "reference.toml"
    ref.write_text(_reference_for(gz_dir), encoding="utf-8")
    victim = sorted(gz_dir.glob("*.json.gz"))[1]
    victim.write_bytes(victim.read_bytes() + b"tampered")

    def _must_not_run(*args, **kwargs):
        raise AssertionError("pipeline ran despite a checksum mismatch")

    monkeypatch.setattr(naive_duckdb, "run_once", _must_not_run)
    monkeypatch.setattr(naive_duckdb, "run_in_subprocess", _must_not_run)
    out = tmp_path / "benchmark-row.csv"
    code = naive_duckdb.main(
        ["--data-dir", str(gz_dir), "--reference", str(ref), "--git-sha", "abc1234"]
        + ["--out", str(out)]
    )
    assert code != 0
    assert not out.exists()
