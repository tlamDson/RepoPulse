"""Checks on the recruiter-first top of README.md, its architecture SVG and DEVELOPMENT.md.

Every number in the "Results at a glance" table is recomputed here from runs.csv (the newest
row of each variant), row by row, so the table can only quote what runs.csv holds.
"""

import csv
import re
import xml.etree.ElementTree as ET
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SVG_PATH = ".github/readme/architecture.svg"
DASHBOARD_URL = "https://tlamdson.github.io/github-analytics-data-pipeline/"
SVG_NS = "{http://www.w3.org/2000/svg}"


def read_text(name):
    return (ROOT / name).read_text(encoding="utf-8").replace("\r\n", "\n")


def section(text, heading):
    """Body of the `## heading` section, up to the next `## ` heading."""
    match = re.search(rf"^## {re.escape(heading)}\s*$", text, re.MULTILINE)
    assert match, f"README.md has no `## {heading}` section"
    rest = text[match.end() :]
    following = re.search(r"^## ", rest, re.MULTILINE)
    return rest[: following.start()] if following else rest


def newest_by_variant():
    with open(ROOT / "runs.csv", encoding="utf-8", newline="") as f:
        rows = list(csv.DictReader(f))
    best = {}
    for row in rows:
        when = datetime.fromisoformat(row["run_at"])
        if row["variant"] not in best or when > best[row["variant"]][0]:
            best[row["variant"]] = (when, row)
    return {variant: row for variant, (_, row) in best.items()}


def notes(row):
    out = {}
    for word in (row["notes"] or "").split():
        key, sep, value = word.partition("=")
        if sep and key not in out:
            out[key] = value.rstrip(";")
    return out


def expected_glance_numbers():
    """Numbers each glance row must quote, keyed by a word of its first cell."""
    runs = newest_by_variant()

    layout = runs["qopt-partitioned-clustered"]
    flat = runs["qopt-flat"]
    less_scanned = 100 * (1 - int(layout["bytes_read"]) / int(flat["bytes_read"]))

    dq = notes(runs["dq-fault-injection"])

    speedups = [
        float(runs[f"spark-{n}"]["total_seconds"]) / float(runs[f"duckdb-{n}"]["total_seconds"])
        for n in ("1d", "7d", "30d")
    ]

    broker = notes(runs["streaming-fault-broker-kill"])
    consumer = notes(runs["streaming-fault-consumer-kill"])
    for key in ("lost", "duplicates", "source_rows"):
        assert broker[key] == consumer[key], f"the two crash cases differ in {key}"

    agents = [v for v in runs if v.startswith("sql-agent-")]
    assert len(agents) == 1, agents
    correct, questions = notes(runs[agents[0]])["accuracy"].split("/")

    return {
        "dashboard": [f"{float(layout['total_seconds']):.2f}", str(int(less_scanned))],
        "Data-quality": [dq["injected"], dq["caught"], dq["false_alarms"]],
        "DuckDB": [str(int(min(speedups))), str(int(max(speedups)))],
        "Kafka": [broker["lost"], broker["duplicates"], broker["source_rows"]],
        "SQL": [correct, questions],
    }


NUMBER = re.compile(r"\d{1,3}(?:,\d{3})+|\d+(?:\.\d+)?")


def numbers_in(cell):
    # Q1-Q6 are question names, not numbers.
    return [n.replace(",", "") for n in NUMBER.findall(re.sub(r"\bQ\d\b", "", cell))]


def glance_rows():
    body = section(read_text("README.md"), "Results at a glance")
    lines = [ln.strip() for ln in body.splitlines() if ln.strip().startswith("|")]
    assert lines, "the glance section has no table"
    header = [c.strip() for c in lines[0].strip("|").split("|")]
    assert header == ["What was tested", "Result", "Why it matters"]
    assert set(lines[1].replace("|", "").replace(" ", "")) <= set("-:")
    return [[c.strip() for c in ln.strip("|").split("|")] for ln in lines[2:]]


# ---------------------------------------------------------------- top of the README


def test_readme_opens_with_title_pitch_badges_and_live_dashboard_before_the_diagram():
    readme = read_text("README.md")
    head = readme[: readme.index("## What it does")]

    assert head.startswith("# GitHub Analytics Data Pipeline\n")
    assert re.search(r"actions/workflows/ci\.yml/badge\.svg", head), "CI badge missing"
    assert re.search(r"\[!\[[^\]]*[Ll]icen[cs]e[^\]]*\]\([^)]+\)\]\(LICENSE\)", head)
    url = re.escape(DASHBOARD_URL)
    link = re.search(rf"Live dashboard[^\n]*(<{url}>|\]\({url}\))", head)
    assert link, "a 'Live dashboard' text link is missing above the first section"

    image = re.search(r"!\[([^\]\n]+)\]\(" + re.escape(SVG_PATH) + r"\)", head)
    assert image, "the architecture SVG must sit right under the pitch, before the first section"
    assert len(image.group(1).split()) >= 8, "the diagram needs a descriptive alt text"
    assert link.start() < image.start()


def test_readme_shows_no_image_but_the_diagram_and_the_badges():
    readme = read_text("README.md")

    assert "<img" not in readme.lower()
    for src in re.findall(r"!\[[^\]\n]*\]\(([^)\s]+)", readme):
        badge = (
            src.startswith("https://img.shields.io/")
            or src.endswith("/badge.svg")
            or ("/badge.svg?" in src)
        )
        assert src == SVG_PATH or badge, f"unexpected image in README.md: {src}"


def test_no_mermaid_in_readme_or_development_guide():
    for name in ("README.md", "DEVELOPMENT.md"):
        assert "mermaid" not in read_text(name).lower(), name


def test_readme_sections_come_in_the_recruiter_order():
    readme = read_text("README.md")
    order = ["What it does", "Results at a glance", "What I built", "Tech stack", "Quick start"]
    positions = []
    for heading in order:
        match = re.search(rf"^## {re.escape(heading)}\s*$", readme, re.MULTILINE)
        assert match, heading
        positions.append(match.start())
    assert positions == sorted(positions)


def bullets(body):
    return [ln for ln in body.splitlines() if ln.startswith("- ")]


def test_what_it_does_is_three_bullets():
    assert len(bullets(section(read_text("README.md"), "What it does"))) == 3


def test_what_i_built_is_four_or_five_bullets_of_a_solo_project():
    body = section(read_text("README.md"), "What I built")

    assert 4 <= len(bullets(body)) <= 5
    assert "solo" in body.lower()


def test_tech_stack_is_a_table():
    body = section(read_text("README.md"), "Tech stack")
    assert sum(1 for ln in body.splitlines() if ln.startswith("|")) >= 4


def test_quick_start_has_three_commands_and_links_the_development_guide():
    body = section(read_text("README.md"), "Quick start")
    block = re.search(r"```[a-z]*\n(.*?)```", body, re.DOTALL)

    assert block, "Quick start needs a code block"
    commands = [ln for ln in block.group(1).splitlines() if ln.strip() and not ln.startswith("#")]
    assert len(commands) == 3, commands
    assert "](DEVELOPMENT.md)" in body


# ---------------------------------------------------------------- results at a glance


def test_glance_table_has_one_row_per_checked_result():
    rows = glance_rows()
    expected = expected_glance_numbers()

    assert len(rows) == len(expected)
    for key in expected:
        matching = [r for r in rows if key in r[0]]
        assert len(matching) == 1, f"one glance row must name {key!r} in its first cell"
    for row in rows:
        assert len(row) == 3 and all(row), row


def test_every_number_in_the_glance_table_is_derived_from_runs_csv():
    expected = expected_glance_numbers()

    for row in glance_rows():
        key = next(k for k in expected if k in row[0])
        quoted = numbers_in(" ".join(row))
        assert quoted == expected[key], (
            f"glance row {key!r} quotes {quoted}, runs.csv gives {expected[key]}"
        )


def test_glance_row_of_injected_faults_says_the_data_is_simulated():
    row = next(r for r in glance_rows() if "Data-quality" in r[0])
    assert "simulated" in " ".join(row).lower()


# ---------------------------------------------------------------- architecture SVG


def svg_root():
    path = ROOT / SVG_PATH
    assert path.is_file(), f"{SVG_PATH} is missing"
    return ET.fromstring(path.read_text(encoding="utf-8"))


def test_architecture_svg_is_at_most_900_px_wide():
    root = svg_root()

    assert root.tag == f"{SVG_NS}svg"
    assert float(root.get("width")) <= 900
    view_box = [float(v) for v in root.get("viewBox").split()]
    assert view_box[2] <= 900


def test_architecture_svg_loads_nothing_external_and_runs_no_script():
    text = (ROOT / SVG_PATH).read_text(encoding="utf-8").lower()

    for banned in (
        "<script",
        "<foreignobject",
        "<image",
        "@import",
        "@font-face",
        "url(http",
    ):
        assert banned not in text, banned
    assert not re.search(r"\son[a-z]+\s*=", text), "event handler attribute"
    assert not re.search(r"href\s*=\s*\"https?:", text)


def luminance(color):
    color = color.strip().lower()
    names = {"white": "#ffffff", "black": "#000000"}
    color = names.get(color, color)
    assert re.fullmatch(r"#[0-9a-f]{3}|#[0-9a-f]{6}", color), f"use hex colors: {color}"
    if len(color) == 4:
        color = "#" + "".join(c * 2 for c in color[1:])
    r, g, b = (int(color[i : i + 2], 16) / 255 for i in (1, 3, 5))
    return 0.2126 * r + 0.7152 * g + 0.0722 * b


def test_architecture_svg_has_a_white_rounded_background_and_dark_text():
    root = svg_root()
    width, height = float(root.get("width")), float(root.get("height"))

    first = next(iter(root))
    assert first.tag == f"{SVG_NS}rect", "the first shape must be the background"
    assert float(first.get("width")) == width and float(first.get("height")) == height
    assert float(first.get("rx", "0")) > 0
    assert luminance(first.get("fill")) == 1.0

    texts = list(root.iter(f"{SVG_NS}text"))
    assert texts
    parents = {child: parent for parent in root.iter() for child in parent}
    for text in texts:
        node, fill = text, None
        while node is not None and fill is None:
            fill = node.get("fill")
            node = parents.get(node)
        assert luminance(fill or "#000") < 0.4, f"light text: {''.join(text.itertext())}"


def test_architecture_svg_draws_the_whole_flow():
    words = " ".join(" ".join(t.itertext()) for t in svg_root().iter(f"{SVG_NS}text"))

    for label in (
        "GH Archive",
        "hourly",
        "PySpark",
        "Delta Lake",
        "bronze",
        "silver",
        "gold",
        "dbt",
        "marts",
        "GitHub Pages",
        "Data quality",
        "CI",
        "DuckDB",
        "Spark",
        "runs.csv",
    ):
        assert label in words, f"the diagram does not show {label!r}"


# ---------------------------------------------------------------- DEVELOPMENT.md


def test_development_guide_holds_the_moved_sections():
    guide = read_text("DEVELOPMENT.md")

    for heading in (
        "Design decisions",
        "Q6 health score",
        "Data sources and licences",
        "Getting started and contributing",
        "Licence",
    ):
        assert re.search(rf"^## {re.escape(heading)}\s*$", guide, re.MULTILINE), heading
    assert "docs/developer-guide.md" in guide
