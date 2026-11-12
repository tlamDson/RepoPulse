"""Checks on the real README.md against the real runs.csv.

The Results block between the markers must be exactly what ``python -m repopulse.readme_results``
generates from runs.csv, so a number in the README can only be a number from runs.csv. The
generator module is imported inside the tests that need it, so this file always collects.
"""

import csv
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
START = "<!-- results:start -->"
END = "<!-- results:end -->"

# Weights of the Q6 health score formula: a definition, not a measurement.
FORMULA_NUMBERS = {"0.25", "0.15", "0.20", "0.10"}


def read_text(name):
    return (ROOT / name).read_text(encoding="utf-8").replace("\r\n", "\n")


def has_markers(readme):
    return (
        readme.count(START) == 1
        and readme.count(END) == 1
        and readme.index(START) < readme.index(END)
    )


def results_block(readme):
    assert has_markers(readme), (
        f"README.md needs one {START} and one {END}, in that order; "
        "then run `python -m repopulse.readme_results`"
    )
    return readme[readme.index(START) + len(START) : readme.index(END)]


def runs_rows():
    with open(ROOT / "runs.csv", encoding="utf-8", newline="") as f:
        return [{k: (v or None) for k, v in r.items()} for r in csv.DictReader(f)]


def test_readme_has_one_results_marker_pair_in_order():
    readme = read_text("README.md")

    assert readme.count(START) == 1
    assert readme.count(END) == 1
    assert readme.index(START) < readme.index(END)


def test_readme_has_an_architecture_diagram():
    assert "```mermaid" in read_text("README.md")


def test_readme_has_design_decisions_heading():
    assert re.search(r"^#{2,3} Design decisions\s*$", read_text("README.md"), re.MULTILINE)


def test_readme_no_longer_says_it_is_bootstrapping():
    readme = read_text("README.md")

    assert "bootstrapping" not in readme.lower()
    assert "No pipeline results yet" not in readme


def hand_written(readme):
    """README text outside the generated Results block."""
    if not has_markers(readme):
        return readme
    return readme[: readme.index(START)] + readme[readme.index(END) :]


def test_readme_says_the_schedule_runs_on_the_runner_with_motherduck_and_databricks_is_measured():
    readme = read_text("README.md")
    text = hand_written(readme)

    assert "MotherDuck" in readme
    assert "GitHub Actions" in readme
    assert re.search(r"scheduled pipeline[^.]*runner[^.]*MotherDuck", text)
    assert re.search(r"Databricks[^.]*measured next to", text)
    assert "docs/probe-result.md" in text


def test_readme_no_longer_says_databricks_is_unused_or_the_second_phase_is_unbuilt():
    text = hand_written(read_text("README.md"))

    assert "does not use Databricks" not in text
    assert "Anything on Databricks" not in text
    assert "Fallback path instead of Databricks" not in text
    assert "could not authenticate to Databricks" not in text
    assert "not a personal access token" not in text
    assert "planned, not built" not in text
    assert "Local phase (planned)" not in text


def test_readme_labels_simulated_data_and_names_the_batch_style():
    readme = read_text("README.md")

    assert "simulated" in readme
    assert "incremental batch" in readme


def test_readme_links_the_method_page_of_every_v2_measurement():
    text = hand_written(read_text("README.md"))

    for page in (
        "path-comparison",
        "iceberg-vs-delta",
        "streaming",
        "streaming-fault",
        "trino-hive",
        "airflow",
        "dbt-databricks",
        "databricks-job",
        "semantic-layer",
        "sql-agent",
        "engine-matrix",
    ):
        assert (ROOT / "docs" / f"{page}.md").is_file(), page
        assert f"docs/{page}.md" in text, f"README.md does not link docs/{page}.md"


def test_readme_never_calls_the_pipeline_real_time_or_streaming():
    # The scheduled pipeline is an incremental batch. The generated Results block is exempt: it
    # copies variant names and notes from runs.csv, and `streaming-throughput-p<N>` is a real
    # Kafka + Spark Structured Streaming measurement (docs/streaming.md). The hand-written text
    # may name the two docs pages of that benchmark, nothing else.
    text = re.sub(r"docs/streaming[a-z-]*\.md", "", hand_written(read_text("README.md")))
    lowered = text.lower()

    assert "real-time" not in lowered
    assert "streaming" not in lowered


def test_runs_csv_has_a_row_for_every_variant_the_v2_sections_require():
    from repopulse import readme_results

    assert readme_results.missing_variants(runs_rows()) == []


def test_results_block_is_what_the_generator_makes_from_runs_csv():
    from repopulse import readme_results

    expected = readme_results.render(runs_rows())

    actual = results_block(read_text("README.md"))
    assert actual.strip("\n") == expected.strip("\n"), (
        "README.md results block is out of date with runs.csv; "
        "run `python -m repopulse.readme_results` and commit README.md"
    )


def test_every_variant_in_runs_csv_has_a_row_in_the_results_block():
    block = results_block(read_text("README.md"))

    cells_in_block = {
        c.strip() for ln in block.splitlines() if ln.startswith("|") for c in ln.split("|")
    }
    for variant in sorted({r["variant"] for r in runs_rows()}):
        assert variant in cells_in_block, f"variant {variant} of runs.csv is not in the README"


def test_every_decimal_in_the_results_block_comes_from_runs_csv():
    block = results_block(read_text("README.md"))
    source = read_text("runs.csv")

    tokens = re.findall(r"\d+\.\d+", block)
    assert tokens, "results block has no numbers"
    for token in tokens:
        assert token in source, f"{token} in README.md is not in runs.csv"


def test_hand_written_readme_text_quotes_no_decimal_that_runs_csv_lacks():
    outside = hand_written(read_text("README.md"))
    source = read_text("runs.csv")

    for token in re.findall(r"\d+\.\d+", outside):
        assert token in FORMULA_NUMBERS or token in source, f"{token} in README.md has no source"
