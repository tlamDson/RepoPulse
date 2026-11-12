"""Bronze -> silver batch writing catalog tables by name (the Databricks / Unity Catalog layout).

Same shared logic as the Delta-path layout of test_spark_gharchive.py, but the three tables are
``[<catalog>.]<schema>.{bronze,silver,quarantine}_gharchive_events``. Locally the catalog is the
session catalog (managed tables under the test warehouse dir); each test gets its own schema.
Serverless Databricks is Spark Connect: persist/cache and ``DeltaTable.forPath`` are not used in
this layout, which the tests enforce by making those calls raise.

GH Archive is real public data; the events here are tiny hand-written look-alikes.
"""

from __future__ import annotations

import json
import uuid

import pytest
from test_spark_gharchive import (
    COMMIT_EMAILS,
    COMMIT_NAMES,
    DAY,
    EMAIL_RE,
    SALT,
    encode,
    gh_event,
    land,
    push_payload,
    without,
)

pytestmark = pytest.mark.spark

KINDS = ("bronze", "silver", "quarantine")


def batch():
    from repopulse.spark import batch as module  # imports pyspark: only after the spark fixture

    return module


@pytest.fixture
def new_schema(spark):
    made = []

    def make() -> str:
        name = f"rp_{uuid.uuid4().hex[:12]}"
        spark.sql(f"CREATE SCHEMA {name}")
        made.append(name)
        return name

    yield make
    for name in made:
        spark.sql(f"DROP SCHEMA IF EXISTS {name} CASCADE")


def mixed_hour(tmp_path, hour=10):
    land(
        tmp_path,
        hour,
        [
            encode(gh_event(100)),
            encode(gh_event(101)),
            encode(gh_event(100, login="mallory")),  # duplicate id
            encode(gh_event(102, "PushEvent", payload=push_payload(COMMIT_EMAILS, COMMIT_NAMES))),
            encode(without(gh_event(103), "repo", "id")),  # schema invalid
            encode(gh_event(104, created_at=f"{DAY[:8]}01T00:00:00Z")),  # late beyond window
            b"{broken json",  # malformed
        ],
    )


def run_catalog(spark, tmp_path, schema, hours, *, catalog=None, persist=True):
    summary = batch().run_batch(
        spark,
        source="gharchive",
        hours=hours,
        landing_dir=tmp_path / "landing",
        salt=SALT,
        late_window_hours=24,
        schema=schema,
        catalog=catalog,
        persist=persist,
    )
    json.dumps(summary)
    return summary


def run_path(spark, tmp_path, hours):
    return batch().run_batch(
        spark,
        source="gharchive",
        hours=hours,
        landing_dir=tmp_path / "landing",
        lake_dir=tmp_path / "lake",
        salt=SALT,
        late_window_hours=24,
    )


def name(schema, kind, catalog=None):
    prefix = f"{catalog}.{schema}" if catalog else schema
    return f"{prefix}.{kind}_gharchive_events"


def counts(spark, schema, catalog=None):
    return {kind: spark.table(name(schema, kind, catalog)).count() for kind in KINDS}


def json_lines(df, drop=()):
    return sorted(r[0] for r in df.drop(*drop).selectExpr("to_json(struct(*)) AS j").collect())


def path_table(spark, tmp_path, kind):
    return spark.read.format("delta").load(str(tmp_path / "lake" / kind / "gharchive_events"))


def forbid(monkeypatch, owner, *attrs):
    def refuse(*args, **kwargs):
        raise AssertionError(f"{owner.__name__}.{attrs} must not be called in the catalog layout")

    for attr in attrs:
        monkeypatch.setattr(owner, attr, refuse)


def test_catalog_run_creates_bronze_silver_and_quarantine_tables(spark, tmp_path, new_schema):
    schema = new_schema()
    mixed_hour(tmp_path)

    summary = run_catalog(spark, tmp_path, schema, [(DAY, 10)])

    for kind in KINDS:
        assert spark.catalog.tableExists(name(schema, kind)), kind
    assert counts(spark, schema) == {"bronze": 7, "silver": 3, "quarantine": 3}
    assert summary["bronze_rows"] == 7
    assert summary["silver_inserted"] == 3
    assert summary["duplicates_dropped"] == 1
    assert summary["quarantined_total"] == 3
    assert not (tmp_path / "lake").exists(), "catalog layout must not write Delta paths"


def test_catalog_tables_keep_the_partitioning(spark, tmp_path, new_schema):
    schema = new_schema()
    mixed_hour(tmp_path)

    run_catalog(spark, tmp_path, schema, [(DAY, 10)])

    def partitions(kind):
        detail = spark.sql(f"DESCRIBE DETAIL {name(schema, kind)}").first()
        return list(detail["partitionColumns"])

    assert partitions("bronze") == ["data_date"]
    assert partitions("quarantine") == ["data_date"]
    assert partitions("silver") == ["event_date"]


def test_rerun_same_hour_changes_no_table(spark, tmp_path, new_schema):
    schema = new_schema()
    mixed_hour(tmp_path)

    first = run_catalog(spark, tmp_path, schema, [(DAY, 10)])
    before = counts(spark, schema)
    second = run_catalog(spark, tmp_path, schema, [(DAY, 10)])

    assert counts(spark, schema) == before
    assert first["silver_inserted"] == 3
    assert second["silver_inserted"] == 0
    assert second["bronze_rows"] == first["bronze_rows"]


def test_silver_table_holds_no_raw_email(spark, tmp_path, new_schema):
    schema = new_schema()
    mixed_hour(tmp_path)

    run_catalog(spark, tmp_path, schema, [(DAY, 10)])

    for kind in ("silver", "quarantine"):
        for text in json_lines(spark.table(name(schema, kind))):
            for value in [*COMMIT_EMAILS, *COMMIT_NAMES]:
                assert value.lower() not in text.lower(), f"raw PII {value!r} in {kind}"
            assert EMAIL_RE.search(text) is None, f"email-like value in {kind}: {text}"


def test_catalog_layout_gives_the_same_result_as_the_path_layout(spark, tmp_path, new_schema):
    schema = new_schema()
    mixed_hour(tmp_path, 10)
    land(tmp_path, 11, [encode(gh_event(101)), encode(gh_event(105)), b"{broken again"])

    by_path = [run_path(spark, tmp_path, [(DAY, 10)]), run_path(spark, tmp_path, [(DAY, 11)])]
    by_name = [
        run_catalog(spark, tmp_path, schema, [(DAY, 10)]),
        run_catalog(spark, tmp_path, schema, [(DAY, 11)]),
    ]

    assert by_name == by_path
    for kind in KINDS:
        drop = ("ingested_at",) if kind == "bronze" else ()
        assert json_lines(spark.table(name(schema, kind)), drop) == json_lines(
            path_table(spark, tmp_path, kind), drop
        ), kind


def test_no_persist_switch_gives_the_same_result_without_caching(
    spark, tmp_path, new_schema, monkeypatch
):
    cached, uncached = new_schema(), new_schema()
    mixed_hour(tmp_path)
    expected = run_catalog(spark, tmp_path, cached, [(DAY, 10)])

    from pyspark.sql import DataFrame

    forbid(monkeypatch, DataFrame, "persist", "cache", "unpersist")
    summary = run_catalog(spark, tmp_path, uncached, [(DAY, 10)], persist=False)

    assert summary == expected
    for kind in KINDS:
        drop = ("ingested_at",) if kind == "bronze" else ()
        assert json_lines(spark.table(name(uncached, kind)), drop) == json_lines(
            spark.table(name(cached, kind)), drop
        ), kind


def test_catalog_layout_merges_by_table_name_not_by_path(spark, tmp_path, new_schema, monkeypatch):
    from delta.tables import DeltaTable

    schema = new_schema()
    land(tmp_path, 10, [encode(gh_event(200)), encode(gh_event(201))])
    land(tmp_path, 11, [encode(gh_event(201)), encode(gh_event(202))])
    forbid(monkeypatch, DeltaTable, "isDeltaTable", "forPath")

    first = run_catalog(spark, tmp_path, schema, [(DAY, 10)])
    second = run_catalog(spark, tmp_path, schema, [(DAY, 11)])

    ids = sorted(r["event_id"] for r in spark.table(name(schema, "silver")).collect())
    assert ids == ["200", "201", "202"]
    assert first["silver_inserted"] == 2
    assert second["silver_inserted"] == 1
    assert second["duplicates_dropped"] == 1


def test_three_part_names_with_a_catalog(spark, tmp_path, new_schema):
    schema = new_schema()
    mixed_hour(tmp_path)

    run_catalog(spark, tmp_path, schema, [(DAY, 10)], catalog="spark_catalog")

    assert counts(spark, schema, catalog="spark_catalog") == {
        "bronze": 7,
        "silver": 3,
        "quarantine": 3,
    }


def test_run_batch_needs_exactly_one_of_lake_dir_or_schema(spark, tmp_path, new_schema):
    schema = new_schema()
    mixed_hour(tmp_path)
    common = {
        "source": "gharchive",
        "hours": [(DAY, 10)],
        "landing_dir": tmp_path / "landing",
        "salt": SALT,
    }

    with pytest.raises(ValueError):
        batch().run_batch(spark, **common)
    with pytest.raises(ValueError):
        batch().run_batch(spark, **common, lake_dir=tmp_path / "lake", schema=schema)
