"""GitHub community profile files: CONTRIBUTING and the bug/feature issue templates."""

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CONTRIBUTING = ROOT / ".github" / "CONTRIBUTING.md"
ISSUE_TEMPLATES = ROOT / ".github" / "ISSUE_TEMPLATE"
CI_JOBS = ("Lint", "Unit Tests", "dbt build (DuckDB)")
EMAIL = re.compile(r"[\w.+-]+@[\w-]+\.[\w.-]+")
PHONE = re.compile(r"\+?\d[\d\s().-]{8,}\d")


def _front_matter(text: str) -> dict[str, str]:
    assert text.startswith("---\n"), "issue template must start with YAML front matter"
    block = text.split("---\n", 2)[1]
    return dict(line.split(":", 1) for line in block.splitlines() if ":" in line)


def test_contributing_points_to_setup_docs_and_develop_flow():
    text = CONTRIBUTING.read_text(encoding="utf-8")
    assert "docs/developer-guide.md" in text
    assert "README.md" in text
    assert "origin/develop" in text
    assert "into `develop`" in text


def test_contributing_names_required_checks_and_commands():
    text = CONTRIBUTING.read_text(encoding="utf-8")
    for job in CI_JOBS:
        assert f"`{job}`" in text, job
    for cmd in ("python -m pytest", "python -m ruff check .", "--target ci"):
        assert cmd in text, cmd


def test_issue_templates_have_front_matter():
    for name, label in (("bug_report.md", "bug"), ("feature_request.md", "enhancement")):
        meta = _front_matter((ISSUE_TEMPLATES / name).read_text(encoding="utf-8"))
        assert meta["name"].strip(), name
        assert meta["about"].strip(), name
        assert meta["labels"].strip() == label, name


def test_new_community_files_have_no_personal_contact_data():
    for path in (
        CONTRIBUTING,
        ISSUE_TEMPLATES / "bug_report.md",
        ISSUE_TEMPLATES / "feature_request.md",
    ):
        text = path.read_text(encoding="utf-8")
        assert not EMAIL.search(text), path
        assert not PHONE.search(text), path
