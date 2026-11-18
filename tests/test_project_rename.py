"""The repository was renamed to tlamDson/github-analytics-data-pipeline (2026-11-18).

Readers see the display name "GitHub Analytics Data Pipeline" and only the new GitHub and Pages
URLs; the Python package `repopulse` and every object name keep their old spelling.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
NAME = "GitHub Analytics Data Pipeline"
REPO_URL = "https://github.com/tlamDson/github-analytics-data-pipeline"
PAGES_URL = "https://tlamdson.github.io/github-analytics-data-pipeline/"
OLD_URL = re.compile(
    r"https://(?:github\.com/tlamdson/repopulse|tlamdson\.github\.io/repopulse)\b", re.IGNORECASE
)
TEXT_SUFFIXES = {".md", ".html", ".css", ".js", ".svg", ".py", ".yml", ".yaml", ".toml"}


def reader_files() -> list[Path]:
    files = [ROOT / "README.md", ROOT / "DEVELOPMENT.md", ROOT / "CLAUDE.md"]
    for folder in ("docs", "site", ".github", "tests", "repopulse", "benchmarks"):
        files += [
            p for p in (ROOT / folder).rglob("*") if p.is_file() and p.suffix in TEXT_SUFFIXES
        ]
    return files


def test_no_file_links_to_the_old_repository_or_pages_url():
    stale = [
        f"{p.relative_to(ROOT)}: {m.group(0)}"
        for p in reader_files()
        if p.name != "test_project_rename.py"
        for m in OLD_URL.finditer(p.read_text(encoding="utf-8"))
    ]
    assert stale == []


def test_readme_names_the_project_and_links_the_new_urls():
    readme = (ROOT / "README.md").read_text(encoding="utf-8")
    assert readme.startswith(f"# {NAME}\n")
    assert f"{REPO_URL}/actions/workflows/ci.yml" in readme
    assert f"<{PAGES_URL}>" in readme
    pitch = readme.split("\n\n")[2]
    assert pitch.startswith(f"{NAME} is "), pitch


@pytest.mark.parametrize(
    ("path", "heading"),
    [
        ("DEVELOPMENT.md", f"# Developing {NAME}"),
        (".github/CONTRIBUTING.md", f"# Contributing to {NAME}"),
    ],
)
def test_contributor_docs_open_with_the_new_name(path, heading):
    first = (ROOT / path).read_text(encoding="utf-8").splitlines()[0]
    assert first == heading


def test_dashboard_title_heading_and_footer_use_the_new_name():
    html = (ROOT / "site" / "index.html").read_text(encoding="utf-8")
    title = re.search(r"<title>([^<]*)</title>", html).group(1)
    h1 = re.search(r"<h1>([^<]*)</h1>", html).group(1)
    assert title.startswith(f"{NAME}:"), title
    assert h1 == NAME
    assert f'<a href="{REPO_URL}">{NAME} on GitHub</a>' in html


def test_docs_titles_use_the_new_name():
    stale = [
        str(p.relative_to(ROOT))
        for p in (ROOT / "docs").glob("*.md")
        if "RepoPulse" in p.read_text(encoding="utf-8").splitlines()[0]
    ]
    assert stale == []
