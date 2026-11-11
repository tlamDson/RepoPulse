"""Seed of data-ecosystem repos used by the baseline queries and dbt."""

import csv
import re
from pathlib import Path

SEED = Path(__file__).resolve().parents[1] / "transform" / "seeds" / "repos_data_ecosystem.csv"
CATEGORIES = {
    "compute",
    "orchestration",
    "storage-format",
    "warehouse",
    "streaming",
    "dataframe",
    "bi",
}


def _rows() -> list[dict[str, str]]:
    with SEED.open(encoding="utf-8", newline="") as fh:
        return list(csv.DictReader(fh))


def test_header_is_repo_name_and_category():
    with SEED.open(encoding="utf-8", newline="") as fh:
        assert next(csv.reader(fh)) == ["repo_name", "category"]


def test_categories_are_from_the_allowed_set():
    bad = {r["repo_name"]: r["category"] for r in _rows() if r["category"] not in CATEGORIES}
    assert bad == {}


def test_repo_names_are_unique_case_insensitive_and_owner_slash_name():
    names = [r["repo_name"] for r in _rows()]
    assert len(names) >= 50
    assert len({n.lower() for n in names}) == len(names)
    for n in names:
        assert re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", n), n


def test_repos_used_by_the_fixture_have_the_expected_category():
    cats = {r["repo_name"]: r["category"] for r in _rows()}
    assert cats["apache/spark"] == "compute"
    assert cats["trinodb/trino"] == "compute"
    assert cats["duckdb/duckdb"] == "warehouse"
    assert "alice/toy" not in cats
