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
DBT_DBX = "dbt build on a Databricks SQL warehouse (real GH Archive data)"
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
DBT_DATABRICKS = row(
    "dbt-build-databricks",
    "2026-10-25",
    "2026-11-12T08:00:00+00:00",
    "fffffff3333333333333333333333333333333333",
    "95.123456",
    "engine=databricks; source=workspace.repopulse.silver_gharchive_events; "
    "input=gharchive-silver-delta; max_event_date=2026-10-26; lines_changed=321; "
    "q_seconds=mart build time; total=dbt build elapsed_time; base=629d72ec309b",
    q1_seconds="4.5",
    row_count="1234567",
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

# Plan v2 rows (R2). Notes are shortened copies of the real runs.csv rows; the path rows use totals
# that a float round trip would rewrite ("162" -> 162.0, "90.50" -> 90.5).
SHA_V2 = "52c9d56c95bd931105b3e6147451ffad8613abb9"
PATH_A_NOTES = (
    "status=success hour=23 hours=1 ingest_seconds=2.077 spark_seconds=26.93 export_seconds=12.172 "
    "dbt_seconds=12.991 queries_seconds=1.174 queue_seconds=1.0 setup_seconds=41.810507 "
    "missing_hours=0 bronze_rows=68488 run_id=43174206720 trigger=push"
)
PATH_A_PUSH_1 = row(
    "path-a-runner",
    "2026-10-25",
    "2026-11-12T07:18:12+00:00",
    SHA_V2,
    "55.344",
    PATH_A_NOTES,
    q1_seconds="0.158375",
    row_count="68488",
)
PATH_A_PUSH_2 = row(
    "path-a-runner",
    "2026-10-25",
    "2026-11-12T07:27:53+00:00",
    SHA_V2,
    "56.49300000000001",
    PATH_A_NOTES.replace("spark_seconds=26.93", "spark_seconds=24.605"),
    row_count="68488",
)
PATH_A_PUSH_3 = row(
    "path-a-runner",
    "2026-10-25",
    "2026-11-12T07:50:00+00:00",
    SHA_V2,
    "90.50",
    PATH_A_NOTES,
    row_count="68488",
)
PATH_A_PUSH_FAILED = row(
    "path-a-runner",
    "2026-10-25",
    "2026-11-12T07:00:00+00:00",
    SHA_V2,
    "1000",
    "status=failed failed_step=spark hour=23 hours=1 ingest_seconds=2.0 run_id=1 trigger=push",
)
PATH_A_SCHEDULE_OK = row(
    "path-a-runner",
    "2026-11-12",
    "2026-11-12T09:41:00+00:00",
    SHA_V2,
    "40.25",
    "status=success hour=7 hours=1 ingest_seconds=1.5 spark_seconds=20.0 export_seconds=9.0 "
    "dbt_seconds=8.0 queries_seconds=1.75 queue_seconds=3.0 setup_seconds=30.5 "
    "run_id=2 trigger=schedule",
    q1_seconds="0.2",
    row_count="1200",
)
PATH_A_SCHEDULE_FAILED = row(
    "path-a-runner",
    "2026-11-12",
    "2026-11-12T08:41:00+00:00",
    SHA_V2,
    "12.5",
    "status=failed failed_step=dbt hour=6 hours=1 ingest_seconds=1.5 run_id=3 trigger=schedule",
)
PATH_B_PUSH_1 = row(
    "path-b-databricks",
    "2026-10-25",
    "2026-11-12T07:18:13+00:00",
    SHA_V2,
    "217.882",
    "status=success hour=23 hours=1 ingest_seconds=2.641 databricks_seconds=82.087 "
    "warehouse_seconds=0.298 dbt_seconds=128.268 queries_seconds=4.588 queue_seconds=2.0 "
    "setup_seconds=39.078138 job_setup_seconds=3.0 warehouse_start_seconds=0.0 "
    "run_id=43174206720 trigger=push",
    q1_seconds="0.766",
    row_count="0",
)
PATH_B_PUSH_2 = row(
    "path-b-databricks",
    "2026-10-25",
    "2026-11-12T07:27:52+00:00",
    SHA_V2,
    "162",
    "status=success hour=23 hours=1 ingest_seconds=2.095 databricks_seconds=50.846 "
    "warehouse_seconds=0.206 dbt_seconds=104.253 queries_seconds=4.742 queue_seconds=2.0 "
    "setup_seconds=28.303435 job_setup_seconds=3.0 warehouse_start_seconds=0.0 missing_hours=0 "
    "bronze_rows=68488 run_id=43175427031 trigger=push",
    q1_seconds="0.822",
    row_count="0",
)
PATH_B_PUSH_FAILED = row(
    "path-b-databricks",
    "2026-10-25",
    "2026-11-12T07:00:01+00:00",
    SHA_V2,
    "500",
    "status=failed failed_step=databricks hour=23 hours=1 ingest_seconds=2.0 run_id=4 trigger=push",
)
FORMAT_DELTA = row(
    "format-delta",
    "2026-10-25",
    "2026-11-12T02:13:36+00:00",
    "39d76aae736b1223ac4dd6dadb8f49d9e1c81335",
    "71.87834",
    "format=delta runs=5 stat=median write_seconds=8.116 merge_seconds=8.259 "
    "merge_rerun_seconds=6.927 time_travel_seconds=1.105 load_seconds=15.958 startup_seconds=6.609 "
    "base_rows=1590485 merge_source_rows=161510 table_bytes=352367811 table_files=5 spark=3.5.9 "
    "delta=3.3.3 iceberg=1.9.2 cross_check=full",
    q1_seconds="7.64757",
    peak_rss_mb="9039.9",
    bytes_read="295319902",
    row_count="1670400",
)
FORMAT_ICEBERG = row(
    "format-iceberg",
    "2026-10-25",
    "2026-11-12T02:13:36+00:00",
    "39d76aae736b1223ac4dd6dadb8f49d9e1c81335",
    "56.041629",
    "format=iceberg runs=5 stat=median write_seconds=5.841 merge_seconds=6.147 "
    "merge_rerun_seconds=6.823 time_travel_seconds=0.068 load_seconds=16.217 "
    "startup_seconds=6.347 base_rows=1590485 merge_source_rows=161510 table_bytes=214203312 "
    "table_files=5 spark=3.5.9 delta=3.3.3 iceberg=1.9.2 cross_check=full",
    q1_seconds="7.251662",
    peak_rss_mb="8368.2",
    bytes_read="295319902",
    row_count="1670400",
)


def stream_row(partitions, total, produce, consume, produce_rate, consume_rate):
    return row(
        f"streaming-throughput-p{partitions}",
        "2026-10-25",
        "2026-11-12T01:18:30+00:00",
        "d5df6de4f7a8b0178b01eb8b90e34a3511f08b5e",
        total,
        f"partitions={partitions} brokers=3 rf=3 min_isr=2 produce_seconds={produce} "
        f"consume_seconds={consume} produce_events_per_s={produce_rate} "
        f"consume_events_per_s={consume_rate} batch_rows=1670400 match=true "
        "trigger=availableNow micro_batches=9 max_offsets_per_trigger=200000 unkeyed=0 "
        "kafka=apache/kafka:4.0.0",
        bytes_read="1371864797",
        row_count="1670400",
    )


STREAM_P1 = stream_row(1, "72.101", "24.132", "47.969", "69219.3", "34822.5")
STREAM_P3 = stream_row(3, "49.884", "25.261", "24.623", "66125.6", "67839.0")
STREAM_P6 = stream_row(6, "50.067", "26.947", "23.12", "61988.3", "72249.1")
STREAM_P12 = stream_row(12, "51.5", "27.0", "24.5", "61000.0", "68000.0")
FAULT_BROKER = row(
    "streaming-fault-broker-kill",
    "2026-10-25",
    "2026-11-12T02:40:47+00:00",
    "19eaaa2a78954298824b9efac7b3540a1fceec49",
    "105.89",
    "case=broker-kill lost=0 duplicates=0 offset_duplicates=0 content_duplicates=0 unexpected=0 "
    "source_rows=1670400 delta_rows=1670400 distinct_offsets=1670400 failover_seconds=6.071 "
    "resync_seconds=4.904 max_delivery_gap_after_kill_seconds=8.359 killed_broker=kafka-3 "
    "micro_batches_before_kill=0 micro_batches_after_kill=34 partitions=3 brokers=3 rf=3 "
    "min_isr=2 hours=24 max_offsets_per_trigger=50000 kafka=apache/kafka:4.0.0",
    row_count="1670400",
)
FAULT_CONSUMER = row(
    "streaming-fault-consumer-kill",
    "2026-10-25",
    "2026-11-12T02:40:47+00:00",
    "19eaaa2a78954298824b9efac7b3540a1fceec49",
    "138.041",
    "case=consumer-kill lost=0 duplicates=0 offset_duplicates=0 content_duplicates=0 "
    "unexpected=0 source_rows=1670400 delta_rows=1670400 distinct_offsets=1670400 "
    "restart_first_commit_seconds=18.46 restart_drain_seconds=81.363 kill_after_batches=5 "
    "micro_batches_before_kill=5 uncommitted_batches_at_kill=1 micro_batches_after_kill=29 "
    "partitions=3 brokers=3 rf=3 min_isr=2 hours=24 max_offsets_per_trigger=50000 "
    "kafka=apache/kafka:4.0.0",
    row_count="1670400",
)


def trino_row(layout, total, partition_by, sort_by, files, layout_bytes, read, q1):
    return row(
        f"trino-{layout}",
        "2026-10-25",
        "2026-11-12T01:26:14+00:00",
        "58660253fa2db4deea74455f00b2d450e7fac199",
        total,
        f"runs=10 stat=median warmup=1 engine=trino trino=484 partition_by={partition_by} "
        f"sort_by={sort_by} files={files} layout_bytes={layout_bytes} bytes=physical_input_bytes "
        "bytes_from=run1 bytes_equal_across_runs=true cross_check=full",
        q1_seconds=q1,
        bytes_read=read,
        row_count="1670400",
    )


TRINO_FLAT = trino_row(
    "flat", "11.065254", "none", "none", "1", "370155952", "5272074760", "1.0645"
)
TRINO_PARTITIONED = trino_row(
    "partitioned", "4.031387", "type", "none", "16", "350231636", "439061897", "0.378627"
)
TRINO_CLUSTERED = trino_row(
    "clustered",
    "4.732549",
    "none",
    "type+repo.name+created_at",
    "1",
    "302287930",
    "1283007819",
    "0.29",
)
TRINO_PARTITIONED_CLUSTERED = trino_row(
    "partitioned-clustered",
    "3.844165",
    "type",
    "repo.name+created_at",
    "16",
    "302247116",
    "408029209",
    "0.309468",
)
AIRFLOW_2D = row(
    "airflow-backfill-2d",
    "2026-10-25",
    "2026-11-12T02:32:40+00:00",
    "d1cc3f5c1260b86f1ed684a37d926f61e701d6ab",
    "88.716",
    "engine=airflow-3.4.0 days=2 hours_per_day=1 last_hour=15 runs_success=2 runs_failed=0 "
    "sla_minutes=30 sla_missed=0 max_active_runs=1 target=ci run_id=43142141658 "
    "trigger=pull_request",
    row_count="105070",
)
AIRFLOW_10D = row(
    "airflow-backfill-10d",
    "2026-10-25",
    "2026-11-12T03:00:00+00:00",
    "d1cc3f5c1260b86f1ed684a37d926f61e701d6ab",
    "400.5",
    "engine=airflow-3.4.0 days=10 hours_per_day=1 last_hour=15 runs_success=10 runs_failed=0 "
    "sla_minutes=30 sla_missed=1 max_active_runs=1 target=ci run_id=43142141659 "
    "trigger=workflow_dispatch",
    row_count="500000",
)
SQL_NOTES = (
    "accuracy=14/16; p50_question_s=19.037; model=Qwen3.6-35B-A3B; "
    "hw=RTX4060-laptop-8GB llama.cpp -ngl99 --cpu-moe; dataset=ci-fixture"
)
SQL_AGENT = row(
    "sql-agent-qwen3-6-35b-a3b",
    "2026-04-07",
    "2026-11-12T07:36:34+00:00",
    "13adca6",
    "354.638",
    SQL_NOTES,
    row_count="16",
)
DATABRICKS_BS = row(
    "databricks-bronze-silver",
    "2026-10-25",
    "2026-11-12T01:32:05+00:00",
    "b965514c24d6d442cc2ebdc497e56fe3423de6d7",
    "107.948",
    "source=gharchive last_hour=2026-10-25T23 hours=3 bronze_rows=206827 run_s=92.42 trigger=push",
    row_count="206827",
)

# Every variant that has its own section (R2); DATABRICKS_BS stays under "Other runs".
V2_PATH_ROWS = [
    PATH_A_PUSH_1,
    PATH_A_PUSH_2,
    PATH_A_PUSH_3,
    PATH_A_PUSH_FAILED,
    PATH_A_SCHEDULE_OK,
    PATH_A_SCHEDULE_FAILED,
    PATH_B_PUSH_1,
    PATH_B_PUSH_2,
    PATH_B_PUSH_FAILED,
]
V2_ROWS = [
    *V2_PATH_ROWS,
    FORMAT_DELTA,
    FORMAT_ICEBERG,
    STREAM_P1,
    STREAM_P3,
    STREAM_P6,
    STREAM_P12,
    FAULT_BROKER,
    FAULT_CONSUMER,
    TRINO_FLAT,
    TRINO_PARTITIONED,
    TRINO_CLUSTERED,
    TRINO_PARTITIONED_CLUSTERED,
    AIRFLOW_2D,
    AIRFLOW_10D,
    SQL_AGENT,
]
V2_VARIANTS = {r["variant"] for r in V2_ROWS}

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
    DBT_DATABRICKS,
    DQ_ROW,
    PIPELINE,
    UNKNOWN,
]
# `main` refuses a runs.csv without the required variants of the v2 sections, so every test that
# runs `main` on EVERYTHING needs the v2 rows next to the older ones.
EVERYTHING = [BASE_OLD, QOPT_FLAT_OLD, *ALL_LATEST, *V2_ROWS, DATABRICKS_BS]


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


def tables(body):
    """Every markdown table of a section body, as (header, rows of dicts), in order."""
    blocks, current = [], []
    for ln in body.splitlines():
        if ln.startswith("|"):
            current.append(ln)
        elif current:
            blocks.append(current)
            current = []
    if current:
        blocks.append(current)
    out = []
    for lines in blocks:
        header = cells(lines[0])
        out.append((header, [dict(zip(header, cells(ln), strict=True)) for ln in lines[2:]]))
    return out


def prose(body):
    return "\n".join(ln for ln in body.splitlines() if not ln.startswith("|"))


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


def test_databricks_dbt_build_has_its_own_section_saying_real_data_not_seed_fixture():
    out = readme_results.render([DBT_DUCK, DBT_SNOW, DBT_DATABRICKS, BASE_NEW])
    secs = sections(out)

    assert [r["variant"] for r in table(secs[DBT])] == ["dbt-build-duckdb", "dbt-build-snowflake"]
    body = secs[DBT_DBX]
    rows = table(body)
    assert [r["variant"] for r in rows] == ["dbt-build-databricks"]
    assert rows[0]["lines_changed"] == "321"
    assert rows[0]["max_event_date"] == "2026-10-26"
    assert rows[0]["row_count"] == "1234567"
    assert "not simulated" in body and "seed fixture" not in body
    assert "docs/dbt-databricks.md" in body
    assert "dbt-build-databricks" not in table(secs[BASELINE])[0].values()


def test_databricks_dbt_section_is_left_out_when_there_is_no_row():
    assert DBT_DBX not in sections(readme_results.render([DBT_DUCK, DBT_SNOW]))
    assert DBT_DBX not in sections(readme_results.render([]))


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

    for title in (BASELINE, SCALE, DBT, DQ, *V2_TITLES):
        assert title in secs, f"no section {title!r}"
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


# --- engine matrix (E1) ----------------------------------------------------------------------

MATRIX = "Engine matrix: Q1 to Q6 on six engines (real GH Archive data)"
MATRIX_ENGINES = (
    "duckdb",
    "spark",
    "databricks-serverless",
    "databricks-sql",
    "motherduck",
    "snowflake",
)


def matrix_row(engine, total, run_at="2026-11-12T12:00:00+00:00", timing="in-process", **cells):
    return row(
        f"matrix-{engine}",
        "2026-10-25",
        run_at,
        "abcabca1111111111111111111111111111111111",
        total,
        f"engine={engine} runs=5 stat=median warmup=1 timing={timing} cross_check=content",
        row_count="1670400",
        **cells,
    )


M_DUCK = matrix_row("duckdb", "0.411111", q1_seconds="0.051111")
M_SPARK = matrix_row("spark", "6.222222")
M_DBX_SERVERLESS = matrix_row("databricks-serverless", "8.333333", timing="notebook")
M_DBX_SQL = matrix_row("databricks-sql", "9.444444", timing="client")
M_MD = matrix_row("motherduck", "3.555555", timing="client")
M_SNOW = matrix_row("snowflake", "4.666666", timing="client")
M_ODD = matrix_row("oracle", "5.777777")


def matrix_table(out):
    body = sections(out)[MATRIX]
    lines = [ln for ln in body.splitlines() if ln.startswith("|")]
    return body, table(body), lines


def test_matrix_section_has_exactly_one_table_with_one_row_per_engine_in_fixed_order():
    out = readme_results.render([M_SNOW, M_MD, M_DUCK, M_SPARK, M_DBX_SQL, M_DBX_SERVERLESS])

    body, rows, lines = matrix_table(out)
    assert sum(1 for ln in lines if re.fullmatch(r"\|( --- \|)+", ln)) == 1, "one table only"
    assert [r["engine"] for r in rows] == list(MATRIX_ENGINES)
    assert [r["variant"] for r in rows] == [f"matrix-{e}" for e in MATRIX_ENGINES]
    duck = rows[0]
    assert duck["total_seconds"] == "0.411111"
    assert duck["q1_seconds"] == "0.051111"
    assert duck["row_count"] == "1670400"
    assert duck["timing"] == "in-process"
    assert duck["cross_check"] == "content"
    assert rows[3]["timing"] == "client"
    assert rows[2]["timing"] == "notebook"
    assert out.count(f"### {MATRIX}") == 1


def test_matrix_section_with_every_engine_has_no_warning():
    out = readme_results.render([M_DUCK, M_SPARK, M_DBX_SERVERLESS, M_DBX_SQL, M_MD, M_SNOW])

    body = sections(out)[MATRIX]
    assert "warning" not in body.lower()


def test_missing_engine_keeps_its_row_with_dashes_and_is_warned_by_name():
    out = readme_results.render([M_DUCK, M_SPARK, M_SNOW])

    body, rows, _ = matrix_table(out)
    assert [r["engine"] for r in rows] == list(MATRIX_ENGINES)
    missing = {r["engine"]: r for r in rows if r["engine"] not in ("duckdb", "spark", "snowflake")}
    for r in missing.values():
        assert r["variant"] == "—"
        assert r["total_seconds"] == "—"
        assert r["q3_seconds"] == "—"
        assert "missing" in r["source"]
    warning_lines = [ln for ln in body.splitlines() if "warning" in ln.lower()]
    assert warning_lines, "a warning line is shown with the table"
    warned = " ".join(warning_lines)
    for engine in missing:
        assert f"matrix-{engine}" in warned
    assert "matrix-duckdb" not in warned


def test_snowflake_reused_dbt_build_row_is_labelled_as_not_comparable():
    out = readme_results.render([M_DUCK, M_SPARK, M_DBX_SERVERLESS, M_DBX_SQL, M_MD, DBT_SNOW])

    body, rows, _ = matrix_table(out)
    snow = rows[-1]
    assert snow["engine"] == "snowflake"
    assert snow["variant"] == "dbt-build-snowflake"
    assert snow["total_seconds"] == "22.600090742111206"
    assert "reused" in snow["source"]
    assert "not comparable" in snow["source"]
    assert "reused" in body and "not comparable" in body
    assert "warning" not in body.lower()
    assert rows[0]["source"] != snow["source"]
    # the dbt section still lists the build it comes from
    assert "dbt-build-snowflake" in [r["variant"] for r in table(sections(out)[DBT])]


def test_matrix_prose_says_real_data_and_how_engines_are_timed():
    body = sections(readme_results.render([M_DUCK]))[MATRIX]

    assert "not simulated" in body
    assert "in the process" in body
    assert "client" in body and "network" in body
    assert "notebook" in body
    assert "reused" in body
    assert "docs/engine-matrix.md" in body


def test_matrix_rows_are_not_listed_again_in_other_runs():
    out = readme_results.render([M_DUCK, M_SPARK, PIPELINE])

    other = [r["variant"] for r in table(sections(out)[OTHER])]
    assert other == ["pipeline-incremental"]
    assert out.count("matrix-duckdb") == 1
    assert OTHER not in sections(readme_results.render([M_DUCK, M_SPARK, M_MD]))


def test_older_matrix_run_is_not_shown():
    old = matrix_row("duckdb", "0.999999", run_at="2026-11-01T00:00:00+00:00")

    out = readme_results.render([old, M_DUCK])

    assert "0.411111" in out and "0.999999" not in out


def test_matrix_variant_of_an_unknown_engine_falls_back_to_other_runs_and_is_warned():
    out = readme_results.render([M_DUCK, M_ODD])

    secs = sections(out)
    assert [r["variant"] for r in table(secs[OTHER])] == ["matrix-oracle"]
    assert "matrix-oracle" not in [r["variant"] for r in table(secs[MATRIX])]
    assert "matrix-oracle" in " ".join(ln for ln in secs[MATRIX].splitlines() if "arning" in ln)


def test_matrix_section_is_shown_even_with_no_rows_and_warns_about_every_engine():
    body, rows, _ = matrix_table(readme_results.render([]))

    assert [r["engine"] for r in rows] == list(MATRIX_ENGINES)
    assert all(r["variant"] == "—" for r in rows)
    for engine in MATRIX_ENGINES:
        assert f"matrix-{engine}" in body


def test_matrix_numbers_are_copied_verbatim():
    rows_in = [M_DUCK, M_SPARK, M_DBX_SERVERLESS, M_DBX_SQL, M_MD, DBT_SNOW]
    out = readme_results.render(rows_in)

    source = haystack(rows_in)
    for token in re.findall(r"\d+\.\d+", sections(out)[MATRIX]):
        assert token in source, token


def test_main_prints_missing_engine_warnings_to_stderr_without_changing_exit_code(tmp_path, capsys):
    runs, readme = setup(tmp_path, readme_with("stale"))

    assert readme_results.main(args(runs, readme)) == 0
    err = capsys.readouterr().err
    assert "matrix-duckdb" in err and "matrix-motherduck" in err
    assert readme_results.main(args(runs, readme, "--check")) == 0
    assert "matrix-spark" in capsys.readouterr().err


def test_main_prints_no_matrix_warning_when_every_engine_has_a_row(tmp_path, capsys):
    runs = tmp_path / "runs.csv"
    readme = tmp_path / "README.md"
    write_runs(runs, [M_DUCK, M_SPARK, M_DBX_SERVERLESS, M_DBX_SQL, M_MD, M_SNOW, *V2_ROWS])
    readme.write_text(readme_with("stale"), encoding="utf-8", newline="\n")

    assert readme_results.main(args(runs, readme)) == 0

    assert "matrix-" not in capsys.readouterr().err


# --- Plan v2 sections (R2) -------------------------------------------------------------------

PATHS = "Two paths, end to end: runner + MotherDuck vs Databricks (real GH Archive data)"
FORMATS = "Delta Lake vs Apache Iceberg (real GH Archive data)"
STREAMING = "Kafka to Spark Structured Streaming to Delta: throughput (real GH Archive data)"
FAULT = "Streaming fault tolerance: broker kill and consumer kill (real GH Archive data)"
TRINO = "Trino on Hive-partitioned parquet (real GH Archive data)"
AIRFLOW = "Airflow backfill (real GH Archive data)"
SQL_AGENT_TITLE = "SQL agent on a local model (CI fixture data, not GH Archive)"
V2_TITLES = [PATHS, FORMATS, STREAMING, FAULT, TRINO, AIRFLOW, SQL_AGENT_TITLE]


def section(out, title):
    secs = sections(out)
    assert title in secs, f"no section {title!r}; sections: {list(secs)}"
    return secs[title]


def variants_of(body):
    return [r["variant"] for r in table(body)]


def test_v2_sections_come_after_the_older_ones_and_before_other_runs_in_a_fixed_order():
    titles = list(sections(readme_results.render(EVERYTHING)))

    assert all(t in titles for t in V2_TITLES), [t for t in V2_TITLES if t not in titles]
    positions = [titles.index(t) for t in V2_TITLES]
    assert positions == sorted(positions)
    assert titles.index(DBT_DBX) < positions[0]
    assert titles.index(OTHER) > positions[-1]


def test_v2_variants_are_not_listed_again_in_other_runs_but_the_databricks_job_row_stays():
    out = readme_results.render(EVERYTHING)

    other = set(variants_of(sections(out)[OTHER]))
    assert "databricks-bronze-silver" in other
    assert not other & V2_VARIANTS, sorted(other & V2_VARIANTS)


# --- two paths ---


def paths_tables(out):
    found = tables(section(out, PATHS))
    assert len(found) == 2, "the paths section has the newest-run table and the summary table"
    return found


def test_paths_first_table_is_the_newest_run_of_each_path_with_step_columns_from_notes():
    (header, rows), _ = paths_tables(readme_results.render(EVERYTHING))

    for column in (
        "status",
        "trigger",
        "ingest_seconds",
        "spark_seconds",
        "export_seconds",
        "databricks_seconds",
        "warehouse_seconds",
        "dbt_seconds",
        "queries_seconds",
        "queue_seconds",
        "setup_seconds",
    ):
        assert column in header, column
    # one row per path, whatever the data date: the newest run of path A is a scheduled one
    assert [r["variant"] for r in rows] == ["path-a-runner", "path-b-databricks"]
    a, b = rows
    assert (a["data_date"], a["total_seconds"], a["status"], a["trigger"]) == (
        "2026-11-12",
        "40.25",
        "success",
        "schedule",
    )
    assert (a["ingest_seconds"], a["spark_seconds"], a["queue_seconds"]) == ("1.5", "20.0", "3.0")
    assert a["databricks_seconds"] == "—" and a["warehouse_seconds"] == "—"
    assert (b["total_seconds"], b["status"], b["trigger"]) == ("162", "success", "push")
    assert (b["databricks_seconds"], b["warehouse_seconds"]) == ("50.846", "0.206")
    assert (b["job_setup_seconds"], b["warehouse_start_seconds"]) == ("3.0", "0.0")
    assert b["spark_seconds"] == "—" and b["export_seconds"] == "—"


def test_paths_older_runs_of_the_same_path_are_not_in_the_first_table():
    (_, rows), _ = paths_tables(readme_results.render(EVERYTHING))

    totals = {r["total_seconds"] for r in rows}
    assert totals.isdisjoint({"55.344", "56.49300000000001", "90.50", "217.882", "1000", "500"})


def summary_by_group(out):
    _, (header, rows) = paths_tables(out)
    assert header == [
        "variant",
        "trigger",
        "runs",
        "successes",
        "e2e_p50_seconds",
        "e2e_p95_seconds",
    ]
    return {(r["variant"], r["trigger"]): r for r in rows}


def test_paths_summary_counts_every_run_by_variant_and_trigger_failed_runs_included():
    out = readme_results.render(EVERYTHING)

    groups = summary_by_group(out)
    assert list(groups) == [
        ("path-a-runner", "push"),
        ("path-a-runner", "schedule"),
        ("path-b-databricks", "push"),
    ]
    counts = {k: (r["runs"], r["successes"]) for k, r in groups.items()}
    assert counts == {
        ("path-a-runner", "push"): ("4", "3"),
        ("path-a-runner", "schedule"): ("2", "1"),
        ("path-b-databricks", "push"): ("3", "2"),
    }


def test_paths_summary_percentiles_are_nearest_rank_over_successful_runs_only():
    groups = summary_by_group(readme_results.render(EVERYTHING))

    # A push: 3 successes (55.344, 56.49300000000001, 90.50); the failed run (1000) is left out,
    # otherwise p95 would be 1000
    a = groups[("path-a-runner", "push")]
    assert (a["e2e_p50_seconds"], a["e2e_p95_seconds"]) == ("56.49300000000001", "90.50")
    # B push: 2 successes (162, 217.882); with the failed run (500) p50 would be 217.882
    b = groups[("path-b-databricks", "push")]
    assert (b["e2e_p50_seconds"], b["e2e_p95_seconds"]) == ("162", "217.882")
    # one success: p50 = p95 = that run
    s = groups[("path-a-runner", "schedule")]
    assert (s["e2e_p50_seconds"], s["e2e_p95_seconds"]) == ("40.25", "40.25")


def test_paths_summary_percentiles_are_the_cell_text_never_reprinted_through_a_float():
    groups = summary_by_group(readme_results.render(EVERYTHING))

    # float("162") prints 162.0 and float("90.50") prints 90.5
    assert groups[("path-b-databricks", "push")]["e2e_p50_seconds"] == "162"
    assert groups[("path-a-runner", "push")]["e2e_p95_seconds"] == "90.50"


def test_paths_summary_without_a_successful_run_has_dashes_not_numbers():
    only_failed = [PATH_A_PUSH_FAILED, PATH_B_PUSH_FAILED]

    groups = summary_by_group(readme_results.render(only_failed))

    for group in groups.values():
        assert (group["runs"], group["successes"]) == ("1", "0")
        assert (group["e2e_p50_seconds"], group["e2e_p95_seconds"]) == ("—", "—")


def test_paths_summary_computes_no_ratio_or_percentage():
    body = section(readme_results.render(EVERYTHING), PATHS)

    assert "%" not in "\n".join(ln for ln in body.splitlines() if ln.startswith("|"))


def test_paths_prose_says_real_data_what_enters_the_rule_and_points_to_the_docs_not_a_verdict():
    text = prose(section(readme_results.render(EVERYTHING), PATHS))

    assert "not simulated" in text
    assert "trigger=schedule" in text
    assert "nearest-rank" in text
    assert "docs/probe-result.md" in text and "docs/path-comparison.md" in text
    assert "Conclusion" not in text and "switch to" not in text and "keep path" not in text


def test_a_path_row_without_a_status_in_its_notes_is_an_error_not_a_silent_drop():
    broken = dict(PATH_A_PUSH_1, notes="trigger=push hour=23")

    with pytest.raises(readme_results.ReadmeResultsError, match="status"):
        readme_results.render([broken])


# --- Delta vs Iceberg ---


def test_format_section_lists_delta_then_iceberg_with_columns_from_notes():
    body = section(readme_results.render(EVERYTHING), FORMATS)

    delta, iceberg = table(body)
    assert (delta["variant"], iceberg["variant"]) == ("format-delta", "format-iceberg")
    assert delta["total_seconds"] == "71.87834" and iceberg["total_seconds"] == "56.041629"
    assert (delta["write_seconds"], delta["merge_seconds"]) == ("8.116", "8.259")
    assert delta["merge_rerun_seconds"] == "6.927"
    assert (delta["time_travel_seconds"], iceberg["time_travel_seconds"]) == ("1.105", "0.068")
    assert (delta["load_seconds"], iceberg["load_seconds"]) == ("15.958", "16.217")
    assert (delta["table_bytes"], iceberg["table_bytes"]) == ("352367811", "214203312")
    assert (delta["table_files"], delta["cross_check"]) == ("5", "full")
    assert delta["row_count"] == "1670400" and delta["peak_rss_mb"] == "9039.9"


def test_format_prose_says_real_data_not_comparable_with_baseline_and_links_the_method():
    text = prose(section(readme_results.render(EVERYTHING), FORMATS))

    assert "not simulated" in text
    assert "not comparable" in text and "baseline" in text
    assert "docs/iceberg-vs-delta.md" in text


# --- Kafka -> Spark -> Delta ---


def test_streaming_throughput_rows_are_ordered_by_partition_count_as_a_number():
    body = section(readme_results.render(EVERYTHING), STREAMING)

    rows = table(body)
    assert [r["variant"] for r in rows] == [
        "streaming-throughput-p1",
        "streaming-throughput-p3",
        "streaming-throughput-p6",
        "streaming-throughput-p12",
    ]
    p3 = rows[1]
    assert (p3["partitions"], p3["brokers"], p3["rf"], p3["min_isr"]) == ("3", "3", "3", "2")
    assert (p3["produce_seconds"], p3["consume_seconds"]) == ("25.261", "24.623")
    assert p3["produce_events_per_s"] == "66125.6"
    assert p3["consume_events_per_s"] == "67839.0"  # not 67839
    assert (p3["match"], p3["trigger"], p3["micro_batches"]) == ("true", "availableNow", "9")
    assert (p3["bytes_read"], p3["row_count"]) == ("1371864797", "1670400")


def test_streaming_prose_is_honest_about_what_is_measured_and_keeps_the_pipeline_a_batch():
    text = prose(section(readme_results.render(EVERYTHING), STREAMING))

    assert "not simulated" in text
    assert "availableNow" in text
    assert "backlog" in text
    assert "incremental batch" in text
    assert "docs/streaming.md" in text


# --- fault tolerance ---


def test_fault_rows_are_broker_kill_then_consumer_kill_with_loss_and_recovery_from_notes():
    body = section(readme_results.render(EVERYTHING), FAULT)

    broker, consumer = table(body)
    assert broker["variant"] == "streaming-fault-broker-kill"
    assert consumer["variant"] == "streaming-fault-consumer-kill"
    for r in (broker, consumer):
        assert (r["lost"], r["duplicates"], r["unexpected"]) == ("0", "0", "0")
        assert (r["source_rows"], r["delta_rows"]) == ("1670400", "1670400")
    assert (broker["failover_seconds"], broker["resync_seconds"]) == ("6.071", "4.904")
    assert broker["max_delivery_gap_after_kill_seconds"] == "8.359"
    assert broker["restart_first_commit_seconds"] == "—"
    assert consumer["restart_first_commit_seconds"] == "18.46"
    assert consumer["restart_drain_seconds"] == "81.363"
    assert consumer["uncommitted_batches_at_kill"] == "1"
    assert consumer["failover_seconds"] == "—"


def test_fault_prose_says_real_data_content_reconciliation_and_one_run_per_case():
    text = prose(section(readme_results.render(EVERYTHING), FAULT))

    assert "not simulated" in text
    assert "content" in text
    assert "lost" in text and "duplicates" in text
    assert "one run" in text
    assert "docs/streaming-fault.md" in text


# --- Trino ---


def test_trino_rows_follow_the_layout_order_with_layout_columns_from_notes():
    body = section(readme_results.render(EVERYTHING), TRINO)

    rows = table(body)
    assert [r["variant"] for r in rows] == [
        "trino-flat",
        "trino-partitioned",
        "trino-clustered",
        "trino-partitioned-clustered",
    ]
    assert [(r["partition_by"], r["sort_by"]) for r in rows] == [
        ("none", "none"),
        ("type", "none"),
        ("none", "type+repo.name+created_at"),
        ("type", "repo.name+created_at"),
    ]
    flat = rows[0]
    assert (flat["total_seconds"], flat["bytes_read"], flat["files"]) == (
        "11.065254",
        "5272074760",
        "1",
    )
    assert (flat["layout_bytes"], flat["trino"], flat["cross_check"]) == (
        "370155952",
        "484",
        "full",
    )
    assert flat["bytes_equal_across_runs"] == "true"


def test_trino_prose_says_real_data_and_that_it_is_not_comparable_with_baseline():
    text = prose(section(readme_results.render(EVERYTHING), TRINO))

    assert "not simulated" in text
    assert "not comparable" in text and "baseline" in text
    assert "docs/trino-hive.md" in text


# --- Airflow ---


def test_airflow_rows_are_ordered_by_day_count_as_a_number():
    body = section(readme_results.render(EVERYTHING), AIRFLOW)

    short, long = table(body)
    assert (short["variant"], long["variant"]) == ("airflow-backfill-2d", "airflow-backfill-10d")
    assert short["total_seconds"] == "88.716" and short["row_count"] == "105070"
    assert (short["days"], short["hours_per_day"], short["last_hour"]) == ("2", "1", "15")
    assert (short["runs_success"], short["runs_failed"]) == ("2", "0")
    assert (short["sla_minutes"], short["sla_missed"]) == ("30", "0")
    assert (short["engine"], short["target"]) == ("airflow-3.4.0", "ci")
    assert short["trigger"] == "pull_request" and long["trigger"] == "workflow_dispatch"
    assert long["sla_missed"] == "1"


def test_airflow_prose_says_real_data_a_sample_of_each_day_and_links_the_method():
    text = prose(section(readme_results.render(EVERYTHING), AIRFLOW))

    assert "not simulated" in text
    assert "incremental batch" in text
    assert "docs/airflow.md" in text


# --- SQL agent ---


def test_sql_agent_row_has_accuracy_model_dataset_and_the_notes_verbatim():
    body = section(readme_results.render(EVERYTHING), SQL_AGENT_TITLE)

    (r,) = table(body)
    assert r["variant"] == "sql-agent-qwen3-6-35b-a3b"
    assert (r["accuracy"], r["p50_question_s"]) == ("14/16", "19.037")
    assert (r["model"], r["dataset"]) == ("Qwen3.6-35B-A3B", "ci-fixture")
    assert (r["row_count"], r["total_seconds"], r["data_date"]) == ("16", "354.638", "2026-04-07")
    # the hardware has spaces in it, so it cannot be a column: the whole notes cell is copied
    assert r["notes"] == SQL_NOTES
    assert "hw=RTX4060-laptop-8GB llama.cpp -ngl99 --cpu-moe;" in body


def test_sql_agent_prose_says_not_gh_archive_local_model_and_what_row_count_counts():
    text = prose(section(readme_results.render(EVERYTHING), SQL_AGENT_TITLE))

    assert "not GH Archive" in text
    assert "fixture" in text
    assert "local" in text
    assert "number of questions" in text
    assert "docs/sql-agent.md" in text


# --- all v2 sections ---


@pytest.mark.parametrize("title", V2_TITLES)
def test_v2_prose_quotes_no_number_and_never_says_real_time(title):
    text = prose(section(readme_results.render(EVERYTHING), title))

    # `code spans`, Q1..Q6, p50/p95 and SHA-256 are names, not figures
    bare = re.sub(r"`[^`]*`|Q\d|\bp\d+\b|SHA-256", "", text)
    assert not re.search(r"\d", bare), f"digit in the prose of {title!r}: {bare!r}"
    lowered = text.lower()
    assert "real-time" not in lowered and "real time" not in lowered


def test_every_decimal_of_the_v2_output_is_a_substring_of_the_input():
    rows = [*ALL_LATEST, *V2_ROWS, DATABRICKS_BS]
    out = readme_results.render(rows)

    source = haystack(rows)
    tokens = re.findall(r"\d+\.\d+", out)
    assert tokens
    for token in tokens:
        assert token in source, f"{token} is not in the input rows"


def test_v2_rows_render_the_same_for_any_input_order():
    first = readme_results.render(V2_ROWS)

    assert readme_results.render(list(reversed(V2_ROWS))) == first
    assert readme_results.render(V2_ROWS[4:] + V2_ROWS[:4]) == first


# --- missing variants ---

REQUIRED = [
    "path-a-runner",
    "path-b-databricks",
    "format-delta",
    "format-iceberg",
    "streaming-throughput-p<N>",
    "streaming-fault-broker-kill",
    "streaming-fault-consumer-kill",
    "trino-flat",
    "trino-partitioned",
    "trino-clustered",
    "trino-partitioned-clustered",
    "airflow-backfill-<N>d",
    "sql-agent-*",
]


def test_required_variants_are_the_v2_names_and_families_in_a_fixed_order():
    assert list(readme_results.REQUIRED_VARIANTS) == REQUIRED


def test_required_variants_match_the_names_the_benchmark_scripts_write():
    from benchmarks import format_bench, streaming_fault, trino_bench
    from repopulse import pathcompare

    written = {
        *pathcompare.VARIANTS.values(),
        *(format_bench.variant(f) for f in format_bench.FORMATS),
        *(streaming_fault.variant(c) for c in streaming_fault.CASES),
        *trino_bench.VARIANTS,
    }

    exact = {
        name for name in readme_results.REQUIRED_VARIANTS if "<" not in name and "*" not in name
    }
    assert exact == written


def test_missing_variants_names_every_required_variant_when_there_are_no_rows():
    assert readme_results.missing_variants([]) == REQUIRED


def test_missing_variants_is_empty_when_every_required_variant_has_a_row():
    assert readme_results.missing_variants([*V2_ROWS, BASE_NEW]) == []


@pytest.mark.parametrize(
    ("dropped", "reported"),
    [
        (["path-a-runner"], "path-a-runner"),
        (["path-b-databricks"], "path-b-databricks"),
        (["format-delta"], "format-delta"),
        (["format-iceberg"], "format-iceberg"),
        (["streaming-fault-broker-kill"], "streaming-fault-broker-kill"),
        (["streaming-fault-consumer-kill"], "streaming-fault-consumer-kill"),
        (["trino-flat"], "trino-flat"),
        (["trino-partitioned"], "trino-partitioned"),
        (["trino-clustered"], "trino-clustered"),
        (["trino-partitioned-clustered"], "trino-partitioned-clustered"),
        (
            ["streaming-throughput-p1", "streaming-throughput-p3", "streaming-throughput-p6"]
            + ["streaming-throughput-p12"],
            "streaming-throughput-p<N>",
        ),
        (["airflow-backfill-2d", "airflow-backfill-10d"], "airflow-backfill-<N>d"),
        (["sql-agent-qwen3-6-35b-a3b"], "sql-agent-*"),
    ],
)
def test_missing_variants_reports_exactly_the_one_that_has_no_row(dropped, reported):
    kept = [r for r in V2_ROWS if r["variant"] not in dropped]
    assert len(kept) < len(V2_ROWS)

    assert readme_results.missing_variants(kept) == [reported]


def test_one_row_is_enough_for_a_family():
    only_p3 = [r for r in V2_ROWS if not r["variant"].startswith("streaming-throughput-")]
    only_p3.append(STREAM_P3)

    assert readme_results.missing_variants(only_p3) == []


def test_a_name_that_only_looks_like_a_family_does_not_count():
    lookalikes = [
        r
        for r in V2_ROWS
        if not r["variant"].startswith(("streaming-throughput-", "airflow-backfill-", "sql-agent-"))
    ]
    lookalikes += [
        dict(STREAM_P3, variant="streaming-throughput-p"),
        dict(AIRFLOW_2D, variant="airflow-backfill-d"),
        dict(SQL_AGENT, variant="sql-agent"),
    ]

    assert readme_results.missing_variants(lookalikes) == [
        "streaming-throughput-p<N>",
        "airflow-backfill-<N>d",
        "sql-agent-*",
    ]


def test_render_does_not_raise_when_variants_are_missing_and_says_no_rows():
    out = readme_results.render([BASE_NEW])

    assert "No rows" in section(out, TRINO)


def main_with(tmp_path, rows, *extra):
    runs = tmp_path / "runs.csv"
    readme = tmp_path / "README.md"
    write_runs(runs, rows)
    readme.write_text(readme_with("stale"), encoding="utf-8", newline="\n")
    return readme, readme_results.main(args(runs, readme, *extra))


def test_main_exits_2_names_each_missing_variant_and_leaves_the_readme_alone(tmp_path, capsys):
    rows = [
        r for r in EVERYTHING if r["variant"] not in ("trino-clustered", FAULT_CONSUMER["variant"])
    ]

    readme, code = main_with(tmp_path, rows)

    assert code == 2
    err = capsys.readouterr().err
    assert "error:" in err
    assert "trino-clustered" in err and "streaming-fault-consumer-kill" in err
    assert "trino-flat" not in err
    assert readme.read_text(encoding="utf-8") == readme_with("stale")


def test_main_check_also_exits_2_and_writes_nothing_when_a_variant_is_missing(tmp_path, capsys):
    rows = [r for r in EVERYTHING if not r["variant"].startswith("airflow-backfill-")]

    readme, code = main_with(tmp_path, rows, "--check")

    assert code == 2
    assert "airflow-backfill-<N>d" in capsys.readouterr().err
    assert readme.read_text(encoding="utf-8") == readme_with("stale")


def test_main_names_a_missing_family_by_its_pattern(tmp_path, capsys):
    rows = [r for r in EVERYTHING if not r["variant"].startswith("sql-agent-")]

    _, code = main_with(tmp_path, rows)

    assert code == 2
    assert "sql-agent-*" in capsys.readouterr().err


def test_main_with_no_v2_rows_at_all_names_all_of_them(tmp_path, capsys):
    _, code = main_with(tmp_path, [BASE_NEW, QOPT_FLAT])

    assert code == 2
    err = capsys.readouterr().err
    for name in REQUIRED:
        assert name in err, name


def test_main_writes_and_check_passes_when_every_required_variant_has_a_row(tmp_path, capsys):
    readme, code = main_with(tmp_path, EVERYTHING)

    assert code == 0
    assert "error" not in capsys.readouterr().err
    assert "Two paths, end to end" in readme.read_text(encoding="utf-8")
    assert readme_results.main(args(tmp_path / "runs.csv", readme, "--check")) == 0
