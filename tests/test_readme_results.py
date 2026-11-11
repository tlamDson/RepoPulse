"""Tests for repopulse.readme_results: the README results block generated from runs.csv.

Fixture rows are built here; nothing reads the real runs.csv. Numbers in the output must be the
strings of the input cells, copied verbatim (never rounded, computed or estimated).
"""

import csv
import re

import pytest

from repopulse import readme_results, runs_log

START = "<!-- results:start -->"
END = "<!-- results:end -->"

BASELINE = "Query layouts vs naive baseline"
SCALE = "Spark vs DuckDB at 1, 7 and 30 days"
DBT = "dbt build: DuckDB vs Snowflake"
DQ = "Data quality fault injection (simulated data)"
OTHER = "Other runs"


def row(variant, data_date, run_at, git_sha, total, notes=None, **cells):
    out = dict.fromkeys(runs_log.COLUMNS)
    out.update(
        run_at=run_at,
        git_sha=git_sha,
        variant=variant,
        data_date=data_date,
        total_seconds=total,
        notes=notes,
    )
    out.update(cells)
    return out


BASE_OLD = row(
    "baseline",
    "2026-10-25",
    "2026-11-11T07:43:57+00:00",
    "ef8d83955bdd77e1b0d6cf93885d875cb8d51ad1",
    "4.197164",
    "runs=5 median",
    q1_seconds="0.079778",
    peak_rss_mb="1238.0",
    bytes_read="295319902",
    row_count="1670400",
)
BASE_NEW = row(
    "baseline",
    "2026-10-25",
    "2026-11-11T18:53:13+00:00",
    "e94e9d803f37f0d68fcc1937dc4be466d3803efb",
    "4.013378",
    "runs=10 median",
    q1_seconds="0.065736",
    peak_rss_mb="1212.05",
    bytes_read="295319902",
    row_count="1670400",
)
QOPT_FLAT_OLD = row(
    "qopt-flat",
    "2026-10-25",
    "2026-11-11T17:00:00+00:00",
    "1111111aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    "9.999999",
    "runs=10 stat=median partition_by=none",
)
QOPT_FLAT = row(
    "qopt-flat",
    "2026-10-25",
    "2026-11-11T18:53:55+00:00",
    "e94e9d803f37f0d68fcc1937dc4be466d3803efb",
    "1.828283",
    "runs=10 stat=median partition_by=none sort_by=none files=1",
    q1_seconds="0.133773",
    peak_rss_mb="1429.85",
    bytes_read="1491737160",
    row_count="1670400",
)
QOPT_NO_BASELINE = row(
    "qopt-partitioned",
    "2025-01-01",
    "2026-11-12T09:00:00+00:00",
    "bbbbbbb1111111111111111111111111111111111",
    "0.947003",
    "runs=10 stat=median partition_by=type",
)
QOPT_OTHER_DAY = row(
    "qopt-clustered",
    "2026-10-26",
    "2026-11-12T09:00:01+00:00",
    "ccccccc2222222222222222222222222222222222",
    "1.607098",
    "runs=10 stat=median sort_by=type+repo.name+created_at",
)
DUCK_1 = row(
    "duckdb-1d",
    "2025-06-01",
    "2026-11-11T14:14:02+00:00",
    "8ae25be32dd51f0a1943d1c273a8c4f29cf70b82",
    "0.585144",
    "engine=duckdb days=1 runs=3 median",
    row_count="485301",
)
SPARK_1 = row(
    "spark-1d",
    "2025-06-01",
    "2026-11-11T14:14:02+00:00",
    "8ae25be32dd51f0a1943d1c273a8c4f29cf70b82",
    "30.059834",
    "engine=spark days=1 runs=3 median startup_seconds=5.506",
    row_count="485301",
)
DUCK_7 = row(
    "duckdb-7d",
    "2025-06-01",
    "2026-11-11T14:14:02+00:00",
    "8ae25be32dd51f0a1943d1c273a8c4f29cf70b82",
    "4.328583",
    "engine=duckdb days=7 runs=3 median",
)
SPARK_7 = row(
    "spark-7d",
    "2025-06-01",
    "2026-11-11T14:14:02+00:00",
    "8ae25be32dd51f0a1943d1c273a8c4f29cf70b82",
    "96.747028",
    "engine=spark days=7 runs=3 median startup_seconds=5.464",
)
DUCK_30 = row(
    "duckdb-30d",
    "2025-06-01",
    "2026-11-11T14:14:02+00:00",
    "8ae25be32dd51f0a1943d1c273a8c4f29cf70b82",
    "18.531286",
    "engine=duckdb days=30 runs=3 median missing_hours=2",
)
SPARK_30 = row(
    "spark-30d",
    "2025-06-01",
    "2026-11-11T14:14:02+00:00",
    "8ae25be32dd51f0a1943d1c273a8c4f29cf70b82",
    "320.603835",
    "engine=spark days=30 runs=3 median startup_seconds=5.441 missing_hours=2",
)
DBT_DUCK = row(
    "dbt-build-duckdb",
    "2026-01-05",
    "2026-11-11T16:20:05+00:00",
    "ee38cf0be05ceaed3bd8a160e2ffc8fbd3472aa4",
    "2.217414379119873",
    "engine=duckdb; fixture=seed; lines_changed=190; q_seconds=mart build time; base=475c2bd05c52",
    q1_seconds="0.06964230537414551",
    row_count="36",
)
DBT_SNOW = row(
    "dbt-build-snowflake",
    "2026-01-05",
    "2026-11-11T16:20:35+00:00",
    "ee38cf0be05ceaed3bd8a160e2ffc8fbd3472aa4",
    "22.600090742111206",
    "engine=snowflake; fixture=seed; lines_changed=190; q_seconds=mart build time; base=475c2bd",
    q1_seconds="1.398242473602295",
    row_count="36",
)
DQ_ROW = row(
    "dq-fault-injection",
    "2026-11-01",
    "2026-11-11T18:22:59+00:00",
    "0bc1ad42c9574d42579e9628e6ce9027e756bd12",
    "81.907",
    "simulated=true seed=42 users=60 days=2 injected=27 caught=26 false_alarms=1 clean_runs=3 "
    "duplicate=5/5 null=5/5 wrong_type=5/5 late=4/5 schema_drift=5/5 bot=2/2",
    row_count="677",
)
PIPELINE = row(
    "pipeline-incremental",
    "2026-11-12",
    "2026-11-12T10:23:00+00:00",
    "dddddddffffffffffffffffffffffffffffffffff",
    "123.456",
    "last_hour=15 hours=3 trigger=schedule",
    row_count="1234",
)
UNKNOWN = row(
    "something-new",
    "2026-11-13",
    "2026-11-13T00:00:00+00:00",
    "eeeeeee0000000000000000000000000000000000",
    "7.5",
    "free text",
)

ALL_LATEST = [
    BASE_NEW,
    QOPT_FLAT,
    QOPT_NO_BASELINE,
    QOPT_OTHER_DAY,
    DUCK_1,
    SPARK_1,
    DUCK_7,
    SPARK_7,
    DUCK_30,
    SPARK_30,
    DBT_DUCK,
    DBT_SNOW,
    DQ_ROW,
    PIPELINE,
    UNKNOWN,
]
EVERYTHING = [BASE_OLD, QOPT_FLAT_OLD, *ALL_LATEST]


def sections(out):
    """Map the title of each ``### `` section to its body."""
    parts = re.split(r"^### (.+)$", out, flags=re.MULTILINE)
    return {parts[i].strip(): parts[i + 1] for i in range(1, len(parts), 2)}


def cells(line):
    return [c.strip() for c in line.strip().strip("|").split("|")]


def table(body):
    lines = [ln for ln in body.splitlines() if ln.startswith("|")]
    header = cells(lines[0])
    return [dict(zip(header, cells(ln), strict=True)) for ln in lines[2:]]


def haystack(rows):
    return "\n".join(str(v) for r in rows for v in r.values() if v)


# --- render --------------------------------------------------------------------------------


def test_only_newest_run_of_a_variant_and_date_is_shown():
    out = readme_results.render([BASE_OLD, BASE_NEW, QOPT_FLAT_OLD, QOPT_FLAT])

    assert "4.013378" in out
    assert "4.197164" not in out
    assert "1.828283" in out
    assert "9.999999" not in out


def test_input_order_does_not_decide_which_run_is_newest():
    out = readme_results.render([BASE_NEW, BASE_OLD])

    assert "4.013378" in out
    assert "4.197164" not in out


def test_qopt_with_baseline_on_same_date_is_in_the_baseline_table():
    out = readme_results.render([BASE_NEW, QOPT_FLAT])

    rows = table(sections(out)[BASELINE])
    assert [r["variant"] for r in rows] == ["baseline", "qopt-flat"]
    assert {r["data_date"] for r in rows} == {"2026-10-25"}


def test_qopt_without_baseline_on_its_date_is_not_in_the_baseline_table_but_not_dropped():
    out = readme_results.render([BASE_NEW, QOPT_FLAT, QOPT_NO_BASELINE, QOPT_OTHER_DAY])

    secs = sections(out)
    in_baseline = [r["variant"] for r in table(secs[BASELINE])]
    assert in_baseline == ["baseline", "qopt-flat"]
    in_other = {r["variant"]: r["data_date"] for r in table(secs[OTHER])}
    assert in_other == {"qopt-partitioned": "2025-01-01", "qopt-clustered": "2026-10-26"}


def test_other_runs_section_says_why_these_rows_are_not_compared():
    out = readme_results.render([BASE_NEW, QOPT_NO_BASELINE])

    assert "no baseline" in sections(out)[OTHER]


def test_numbers_are_copied_verbatim_from_the_cells():
    out = readme_results.render(ALL_LATEST)

    for cell in ("2.217414379119873", "22.600090742111206", "0.06964230537414551", "1212.05"):
        assert cell in out


def test_every_decimal_in_the_output_is_a_substring_of_the_input():
    out = readme_results.render(ALL_LATEST)

    source = haystack(ALL_LATEST)
    tokens = re.findall(r"\d+\.\d+", out)
    assert tokens, "fixture should produce decimals"
    for token in tokens:
        assert token in source, f"{token} is not in the input rows"


def test_empty_cell_is_a_dash_not_zero_or_none():
    out = readme_results.render([DBT_DUCK])

    (r,) = table(sections(out)[DBT])
    assert r["peak_rss_mb"] == "—"
    assert r["bytes_read"] == "—"
    assert r["q2_seconds"] == "—"
    assert "None" not in out


def test_empty_string_cell_is_also_a_dash():
    blank = dict(DBT_DUCK, peak_rss_mb="")

    (r,) = table(sections(readme_results.render([blank]))[DBT])

    assert r["peak_rss_mb"] == "—"


def test_dq_section_is_labelled_simulated_and_takes_counts_from_notes():
    out = readme_results.render([DQ_ROW])

    body = sections(out)[DQ]
    assert "simulated" in body.lower()
    (r,) = table(body)
    assert r["variant"] == "dq-fault-injection"
    assert r["injected"] == "27"
    assert r["caught"] == "26"
    assert r["false_alarms"] == "1"
    assert r["clean_runs"] == "3"
    assert r["late"] == "4/5"
    assert r["bot"] == "2/2"


def test_dq_section_title_says_simulated():
    titles = sections(readme_results.render([DQ_ROW]))

    assert DQ in titles
    assert "simulated" in DQ


def test_dbt_section_reports_lines_changed_from_notes_and_says_seed_fixture():
    body = sections(readme_results.render([DBT_DUCK, DBT_SNOW]))[DBT]

    rows = table(body)
    assert [r["variant"] for r in rows] == ["dbt-build-duckdb", "dbt-build-snowflake"]
    assert [r["lines_changed"] for r in rows] == ["190", "190"]
    assert "seed fixture" in body


def test_scale_rows_are_ordered_by_days_then_engine_and_not_compared_with_baseline():
    shuffled = [SPARK_30, DUCK_7, SPARK_1, DUCK_30, SPARK_7, DUCK_1, BASE_NEW]

    body = sections(readme_results.render(shuffled))[SCALE]

    assert [r["variant"] for r in table(body)] == [
        "duckdb-1d",
        "spark-1d",
        "duckdb-7d",
        "spark-7d",
        "duckdb-30d",
        "spark-30d",
    ]
    assert "different data date" in body


def test_unknown_variants_are_listed_in_other_runs():
    out = readme_results.render([PIPELINE, UNKNOWN])

    variants = {r["variant"] for r in table(sections(out)[OTHER])}
    assert variants == {"pipeline-incremental", "something-new"}
    assert "123.456" in out


def test_other_runs_section_is_left_out_when_nothing_is_left_over():
    out = readme_results.render([BASE_NEW, QOPT_FLAT, DUCK_1])

    assert OTHER not in sections(out)


def test_empty_input_renders_every_section_with_a_no_rows_line():
    secs = sections(readme_results.render([]))

    for title in (BASELINE, SCALE, DBT, DQ):
        assert "No rows" in secs[title]
    assert OTHER not in secs


def test_each_shown_run_can_be_traced_back_to_runs_csv():
    out = readme_results.render(ALL_LATEST)

    table_lines = [cells(ln) for ln in out.splitlines() if ln.startswith("|")]
    for r in ALL_LATEST:
        wanted = {r["variant"], r["data_date"], r["git_sha"][:7], r["run_at"]}
        assert any(wanted <= set(line) for line in table_lines), f"no table row for {wanted}"


def test_output_is_deterministic_and_independent_of_input_order():
    first = readme_results.render(EVERYTHING)

    assert readme_results.render(EVERYTHING) == first
    assert readme_results.render(list(reversed(EVERYTHING))) == first
    assert readme_results.render(EVERYTHING[3:] + EVERYTHING[:3]) == first


def test_output_starts_with_the_generated_note_and_ends_with_one_newline():
    out = readme_results.render(ALL_LATEST)

    assert out.startswith("Generated by `python -m repopulse.readme_results`")
    assert out.endswith("\n")
    assert not out.endswith("\n\n")


def test_pipe_in_a_cell_does_not_break_the_table():
    odd = dict(UNKNOWN, notes="a|b")

    body = sections(readme_results.render([odd]))[OTHER]

    lines = [ln for ln in body.splitlines() if ln.startswith("|")]
    assert "a\\|b" in lines[2]
    assert lines[2].replace("\\|", "").count("|") == lines[0].count("|")


# --- update_readme -------------------------------------------------------------------------


def readme_with(middle):
    return f"intro\n\n{START}\n{middle}\n{END}\n\noutro\n"


def test_update_readme_replaces_only_the_part_between_markers():
    out = readme_results.update_readme(readme_with("old"), "NEW BLOCK\n")

    assert out == readme_with("NEW BLOCK")


def test_update_readme_is_idempotent():
    once = readme_results.update_readme(readme_with("old"), "NEW BLOCK\n")

    assert readme_results.update_readme(once, "NEW BLOCK\n") == once


def test_update_readme_ignores_trailing_newlines_of_the_block():
    a = readme_results.update_readme(readme_with("old"), "X")
    b = readme_results.update_readme(readme_with("old"), "X\n\n")

    assert a == b == readme_with("X")


@pytest.mark.parametrize(
    "text",
    [
        "no markers at all\n",
        f"{START}\nonly start\n",
        f"only end\n{END}\n",
        f"{END}\nwrong order\n{START}\n",
        f"{START}\na\n{END}\n{START}\nb\n{END}\n",
    ],
    ids=["none", "no-end", "no-start", "reversed", "two-pairs"],
)
def test_update_readme_rejects_missing_or_ambiguous_markers(text):
    with pytest.raises(readme_results.ReadmeResultsError, match="results:"):
        readme_results.update_readme(text, "X")


# --- main ----------------------------------------------------------------------------------


def write_runs(path, rows):
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f, lineterminator="\n")
        writer.writerow(runs_log.COLUMNS)
        for r in rows:
            writer.writerow(["" if r[c] is None else r[c] for c in runs_log.COLUMNS])


def setup(tmp_path, readme_text):
    runs = tmp_path / "runs.csv"
    readme = tmp_path / "README.md"
    write_runs(runs, EVERYTHING)
    readme.write_text(readme_text, encoding="utf-8", newline="\n")
    return runs, readme


def args(runs, readme, *extra):
    return ["--runs", str(runs), "--readme", str(readme), *extra]


def test_main_writes_the_block_and_check_then_passes(tmp_path):
    runs, readme = setup(tmp_path, readme_with("stale"))

    assert readme_results.main(args(runs, readme)) == 0

    expected = readme_results.update_readme(readme_with("stale"), readme_results.render(EVERYTHING))
    assert readme.read_text(encoding="utf-8") == expected
    assert readme_results.main(args(runs, readme, "--check")) == 0


def test_main_check_fails_on_stale_readme_and_does_not_write(tmp_path, capsys):
    runs, readme = setup(tmp_path, readme_with("stale"))
    before = readme.read_bytes()

    assert readme_results.main(args(runs, readme, "--check")) == 1

    assert readme.read_bytes() == before
    assert "python -m repopulse.readme_results" in capsys.readouterr().err


def test_main_check_accepts_crlf_line_endings(tmp_path):
    runs, readme = setup(tmp_path, readme_with("stale"))
    assert readme_results.main(args(runs, readme)) == 0
    readme.write_bytes(readme.read_bytes().replace(b"\n", b"\r\n"))

    assert readme_results.main(args(runs, readme, "--check")) == 0


def test_main_without_markers_fails_and_leaves_the_file_alone(tmp_path, capsys):
    runs, readme = setup(tmp_path, "# Title\n\nno markers here\n")
    before = readme.read_bytes()

    assert readme_results.main(args(runs, readme)) != 0

    assert readme.read_bytes() == before
    assert "results:start" in capsys.readouterr().err


def test_main_check_without_markers_fails(tmp_path):
    runs, readme = setup(tmp_path, "# Title\n")

    assert readme_results.main(args(runs, readme, "--check")) != 0


def test_main_rejects_missing_runs_csv_and_wrong_header(tmp_path):
    readme = tmp_path / "README.md"
    readme.write_text(readme_with("x"), encoding="utf-8")
    missing = tmp_path / "nope.csv"
    bad = tmp_path / "bad.csv"
    bad.write_text("a,b\n1,2\n", encoding="utf-8")

    assert readme_results.main(args(missing, readme)) != 0
    assert readme_results.main(args(bad, readme)) != 0
    assert readme.read_text(encoding="utf-8") == readme_with("x")
