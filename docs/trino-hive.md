# Trino + Hive Metastore + MinIO: Q1-Q6 before and after partitioning

Task P2c in [`plan.md`](plan.md) (Plan v2, phase 2): Q1-Q6 on Trino reading Hive-partitioned
parquet registered in a Hive Metastore, stored on MinIO, before and after layout optimizations.
Script [`benchmarks/trino_bench.py`](../benchmarks/trino_bench.py), stack
[`benchmarks/trino/`](../benchmarks/trino/), queries
[`benchmarks/queries_trino/`](../benchmarks/queries_trino/), workflow
[`.github/workflows/trino-bench.yml`](../.github/workflows/trino-bench.yml).

**Numbers live only in `runs.csv`.** This page describes how they are produced; it quotes none.

## Where it runs

Only on the GitHub runner (`ubuntu-latest`), with Docker Compose inside the workflow; never on the
owner's machine (phase 2 rule of `plan.md`). Every service is free software in a public image; no
cloud service and no GitHub secret is used. The workflow runs on `workflow_dispatch` and on PRs
into `develop`/`main` that touch the workflow, `benchmarks/trino_bench.py`, `benchmarks/trino/**`,
`benchmarks/queries_trino/**` or `benchmarks/query_opt.py`. It is not a required check.

## Architecture

```
trino_bench.py (runner, Python)
  |-- prepares parquet (query_opt.prepare_layouts) + repos seed parquet, in data/tmp/trino-bench/
  |-- docker compose run upload  -> aws s3 sync -> MinIO bucket repopulse-bench
  |-- REST /v1/statement --------> Trino 484 (catalog hive)
                                     |-- thrift :9083 -> Hive Metastore 4.0.1 (Derby, standalone)
                                     |                     |-- S3A -> MinIO (paths of schemas/tables)
                                     |-- native S3 -------> MinIO (parquet files)
```

[`benchmarks/trino/docker-compose.yml`](../benchmarks/trino/docker-compose.yml):

| Service | Image (pinned tag) | Role |
|---|---|---|
| `minio` | `pgsty/minio:RELEASE.2026-08-04T00-00-00Z` | S3-compatible object store, data in a named volume |
| `metastore` | `apache/hive:4.0.1` with `SERVICE_NAME=metastore` | standalone Hive Metastore, embedded Derby |
| `trino` | `trinodb/trino:484` | single-node coordinator + worker, port 8080 on `127.0.0.1` only |
| `upload` (profile `upload`) | `amazon/aws-cli:2.40.1` | one-shot: creates the bucket, `aws s3 sync` of the work directory |

Tags were checked on the Docker Hub API on 2026-11-11, none is `latest`. `minio/minio` and
`minio/mc` are **no longer on Docker Hub** (the API answers "object not found"; the quay.io copy
needs a login), so MinIO is the community-maintained fork `pgsty/minio` (same server and S3 API)
and the upload uses the AWS CLI instead of `mc`.

Configuration next to the compose file:

- [`catalog/hive.properties`](../benchmarks/trino/catalog/hive.properties): `connector.name=hive`,
  `hive.metastore.uri=thrift://metastore:9083`, Trino's native S3 file system
  (`fs.s3.enabled=true`: the property name of the Trino 484 docs; older releases called it
  `fs.native-s3.enabled`), `s3.endpoint=http://minio:9000`, `s3.path-style-access=true`,
  `s3.region`, and the keys through `${ENV:...}`.
- [`metastore/core-site.xml`](../benchmarks/trino/metastore/core-site.xml): `fs.s3a.*` for the
  metastore (endpoint, path-style access, no SSL, simple credentials through `${env.NAME}`).

**Known trap, handled up front:** the Hive Metastore touches table and schema paths itself
(`create_database` / `create_table` check the location and create it when missing), even for an
external table created from Trino. It therefore needs S3A: the `hadoop-aws` jar and the AWS SDK
bundle ship in the image under `/opt/hadoop/share/hadoop/tools/lib/` but are not on the
classpath. The compose file puts them there (`HADOOP_CLASSPATH` with the two jars of Hadoop 3.3.6,
the Hadoop of `apache/hive:4.0.1`, plus `HADOOP_OPTIONAL_TOOLS=hadoop-aws`) and mounts
`core-site.xml` both through `HIVE_CUSTOM_CONF_DIR` and into `/opt/hadoop/etc/hadoop/`.
Symptoms when this is wrong: `ClassNotFoundException: org.apache.hadoop.fs.s3a.S3AFileSystem` or
`No FileSystem for scheme "s3a"` in the metastore log (printed by the workflow on failure).

**Access keys:** `MINIO_ACCESS_KEY` / `MINIO_SECRET_KEY` have defaults in the compose file. They are
throwaway keys of a single-job local stack (MinIO's port is not published, everything is removed
with `docker compose down -v`), not secrets; they can be overridden through the environment.

Healthchecks: the metastore is healthy when its thrift port 9083 accepts a connection; Trino uses
the image's `/usr/lib/trino/bin/health-check` (healthy when `/v1/info` says `"starting": false`).
Trino waits for a healthy metastore; `docker compose up -d --wait` waits for both. MinIO has no
healthcheck (no tool to call inside the image is guaranteed); the upload retries until MinIO
answers. `trino_bench.py` polls `/v1/info` again until `starting` is `false` before any DDL.

## Data, layouts and variants

- **Real GH Archive data** (not simulated): the reference day of
  [`benchmarks/reference.toml`](../benchmarks/reference.toml), checksum-verified before anything
  runs (a mismatch or an unlisted file stops with exit code 2 and writes nothing).
- The four layouts are **the same parquet files as the query optimization benchmark**: written by
  `query_opt.prepare_layouts` (called, not reimplemented; in a child process as in `query_opt.py`)
  into `data/tmp/trino-bench/qopt-*/`. See [`query-optimization.md`](query-optimization.md) for how
  they are written and checked.
- `repos` (the seed `transform/seeds/repos_data_ecosystem.csv`) is written by DuckDB as
  `data/tmp/trino-bench/repos/data.parquet` (`repo_name`, `category`, both `VARCHAR`).
- The work directory is uploaded unchanged to the bucket `repopulse-bench` (`aws s3 sync
  --delete`).

**What "gold in Hive-partitioned form" means here:** the tables Q1-Q6 read are stored as
Hive-partitioned parquet (`type=<EventType>/data_0.parquet`) on MinIO and registered in the Hive
Metastore. The tables are `raw_events` + `repos`, not the dbt marts, because the rule for every
optimization is "compared with `variant=baseline` on the same data day and the same Q1-Q6", and
Q1-Q6 are defined on `raw_events` + `repos`.

| `variant` | Trino schema | Parquet directory | Partitioned by | Sorted by |
|---|---|---|---|---|
| `trino-flat` (the "before") | `trino_flat` | `qopt-flat` | none | arrival order |
| `trino-partitioned` | `trino_partitioned` | `qopt-partitioned` | `type` | arrival order |
| `trino-clustered` | `trino_clustered` | `qopt-clustered` | none | `type`, `repo.name`, `created_at` |
| `trino-partitioned-clustered` (the "after") | `trino_partitioned_clustered` | `qopt-partitioned-clustered` | `type` | `repo.name`, `created_at` |

Each schema holds two tables with the same names, `raw_events` and `repos`, so the six query files
are used verbatim on every layout: the client sets the session schema (`X-Trino-Schema`).

## DDL

Generated by pure functions (`layout_ddl`, `create_table_sql`, `sync_partitions_sql`), printed
without Docker by `python benchmarks/trino_bench.py --print-ddl`. Per layout:

1. `CREATE SCHEMA IF NOT EXISTS hive.<schema> WITH (location = 's3a://<bucket>/schemas/<schema>')`
2. `DROP TABLE IF EXISTS` of `raw_events` and `repos` (external tables: metadata only, the files
   stay), so a second registration does not fail.
3. `CREATE TABLE hive.<schema>.raw_events (id varchar, type varchar, actor ROW(id bigint, login
   varchar), repo ROW(id bigint, name varchar), payload varchar, created_at timestamp(3)) WITH
   (external_location = 's3a://<bucket>/<qopt-layout>', format = 'PARQUET'[, partitioned_by =
   ARRAY['type']])`. Types follow the parquet DuckDB writes: the structs become `ROW`, the JSON
   payload is a string column (`varchar`), `created_at` a timestamp (GH Archive has whole
   seconds, so millisecond precision loses nothing). In a partitioned table the partition column
   must be the last column of the DDL (Trino rule), so `type` moves to the end.
4. `CREATE TABLE hive.<schema>.repos (repo_name varchar, category varchar) WITH
   (external_location = 's3a://<bucket>/repos', format = 'PARQUET')`.
5. Partitioned layouts only: `CALL hive.system.sync_partition_metadata('<schema>', 'raw_events',
   'ADD')`, which registers every `type=<X>/` directory under the table location (no partition
   value is put into SQL, and `register_partition` would need an extra catalog flag).

Names are checked before any SQL is built: schema, table and column names must match
`[a-z_][a-z0-9_]*`, the bucket `[a-z0-9][a-z0-9-]{1,61}[a-z0-9]`, locations
`s3a://<bucket>/<path parts [A-Za-z0-9_-]+>`, partition directory values `[A-Za-z0-9_]+`; anything
else raises an error naming the value. After registration, untimed checks: the number of
partitions Trino sees (`SELECT count(*) FROM "raw_events$partitions"`) equals the number of
`type=*` directories of the layout, and `count(*)` of `raw_events` equals the raw row count.

## Queries

`benchmarks/queries_trino/q1.sql` .. `q6.sql`: the same definitions and output columns as
`benchmarks/queries/q*.sql` (DuckDB); each file starts with its translation notes. Dialect
changes: `ends_with` -> `LIKE '%[bot]'` / `LIKE '%-bot'`, `arg_min` -> `min_by`, `->>` ->
`json_extract_scalar`, `::DOUBLE` -> `CAST`, `date + n` -> `date_add('day', n, date)`, `epoch(b) -
epoch(a)` -> `date_diff('millisecond', a, b) / 1000`, `count(DISTINCT x) FILTER` -> `count(DISTINCT
CASE ...)`, `USING` joins written as `ON`, and `GROUP BY` repeats an expression instead of a
select alias.

**Exact percentiles:** Trino has no exact percentile (`approx_percentile` is approximate and is
not used). `quantile_cont` (linear interpolation) is computed on the sorted, non-NULL values
`h = array_sort(array_agg(x) FILTER (WHERE x IS NOT NULL))`: rank `r = (n - 1) * q`, value
`h[floor(r) + 1] + (r - floor(r)) * (h[ceiling(r) + 1] - h[floor(r) + 1])` (`element_at`), NULL
when there is no value, which is DuckDB's formula. A static test rejects `approx_percentile`,
`ends_with`, `arg_min`, `quantile_cont`, `epoch`, `->>` and `::` in the Trino files.

## What is timed

- **Not timed:** preparing the parquet, writing the seed, the DuckDB reference run, the upload,
  waiting for Trino, the DDL and the post-registration checks.
- Per layout, **one warm-up** of Q1..Q6 (JVM/JIT, metastore and file listing caches), not timed;
  its results are the ones cross-checked. Then `--runs` (default 10) **timed runs**, each Q1..Q6
  in order on the same server.
- `qN_seconds`: client-side wall time of query N, from sending `POST /v1/statement` until the last
  page (no `nextUri`) is received and parsed. `total_seconds` = sum of `q1_seconds` .. `q6_seconds`.
  The row holds the median of the timed runs (`benchmarks/measure.py::summarize_runs`).
- The server stays up between runs and layouts, so later runs profit from whatever Trino caches
  (compiled code, metastore metadata). No result cache exists in Trino; the file system cache is
  off by default and not enabled here.

### Not comparable

- **Not comparable with `baseline` or the `qopt-*` rows:** `baseline` loads the `*.json.gz` files
  into an in-memory table inside its `total_seconds`; `qopt-*` rows run DuckDB in a fresh process
  per run, reading local files. Here the data is already in parquet on MinIO, the engine is a
  server that stays up, the data goes through HTTP (S3 API on loopback and the REST protocol), and
  `total_seconds` covers only Q1..Q6 (no view, no load). Compare the `trino-*` rows with each other
  (`trino-flat` is the "before"); `baseline` is measured again in the same workflow run only so
  that the runner and commit are the same.
- Trino's JVM, the metastore and MinIO share the runner's 4 CPUs and 16 GB with the client;
  Trino's container is limited to 8 GB.

## Amount of data scanned

From Trino's own query statistics, the `stats` object of the last response page:

- `qN_input_bytes` = `physicalInputBytes`: bytes Trino read from storage (MinIO) for query N.
- `qN_input_rows` = `processedRows`: Trino's count of input rows processed by query N.
- `bytes_read` = sum of `q1_input_bytes` .. `q6_input_bytes` **of the first timed run**
  (`bytes_from=run1`). `bytes_equal_across_runs` says whether every timed run reported the same
  bytes for every query; the row is written either way.
- `peak_rss_mb` is **empty**: the client cannot measure the server's memory and no substitute is
  written (Trino's `peakMemoryBytes` is query memory, not RSS).

## Checks before a row is written

- REST responses: `parse_pages` joins `columns` + `data` over all pages, converts values by column
  type (`date`, `timestamp`, numbers, `boolean`), and rejects an `error` on any page (message with
  `errorName` and `message`), a final state other than `FINISHED`, a last page that still has a
  `nextUri`, missing stats keys, negative / NaN / infinite / boolean / non-numeric stats, rows
  that do not match the columns, data before columns, and the spooling protocol (only the direct
  protocol is supported; the client never asks for spooling). HTTP 502/503/504 are retried after
  100 ms (client protocol); any other non-200 status fails.
- **Cross-check with DuckDB:** the warm-up result of every query must equal what DuckDB returns for
  the original query on the same parquet (`query_opt.run_once(..., "qopt-flat", collect=True)`):
  the same column names in the same order and the same multiset of rows, after normalising dates
  and timestamps to ISO strings; numbers compare with an absolute tolerance of 1e-6 (relative
  1e-9), NULL only equals NULL and booleans only booleans. A difference fails that layout with an
  error naming the layout, the query and the first differing row.
- Within a layout, the row count of every query must be the same in every timed run as in the
  warm-up, and `row_count` (Trino's `count(*)`) must equal the raw table's row count.
- A failing layout (DDL error, query error, partition count, mismatch) gets no row; the other
  layouts go on; the exit code is non-zero. A failure before the layouts (prepare, upload, Trino
  not ready) fails every layout.
- Samples: the raw response pages of Q3 (last timed run) on `trino-flat` and `trino-partitioned`
  are saved to `out/trino-samples/` and uploaded with the artifact, so that the hand-written test
  fixtures in `tests/fixtures/trino_rest/` can later be replaced by real output.

## Rows

`variant` = `trino-flat`, `trino-partitioned`, `trino-clustered`, `trino-partitioned-clustered`;
`data_date` = the reference day; `row_count` = rows of `raw_events` as Trino counts them.

`notes` is a space-separated list of `key=value` (no commas, no line breaks):

| Key | Meaning |
|---|---|
| `runs` | number of timed runs |
| `stat` | `median` |
| `warmup` | untimed warm-up runs per layout (`1`) |
| `engine` | `trino` |
| `trino` | Trino version (`/v1/info`) |
| `partition_by` | Hive partition column(s), or `none` |
| `sort_by` | sort key inside each file, joined with `+`, or `none` |
| `files` | parquet files of the layout |
| `layout_bytes` | total size of those files |
| `bytes` | `physical_input_bytes` |
| `bytes_from` | `run1` (the timed run whose input stats are in the row) |
| `bytes_equal_across_runs` | `true` / `false` |
| `cross_check` | `full` (every query checked against DuckDB) |
| `qN_input_bytes` | `physicalInputBytes` of query N |
| `qN_input_rows` | `processedRows` of query N |

## Limits, stated plainly

- The compose stack, the S3A setup of the metastore and the six Trino SQL files are only executed
  on the runner. The unit tests check the DDL text, the parser, the cross-check and the CLI with a
  fake transport, and the compose / workflow / SQL files statically; the SQL logic was compared
  with DuckDB locally by mapping the few Trino-only functions, which is not a Trino run.
- The REST fixtures in `tests/fixtures/trino_rest/` are hand-written from the client protocol
  documentation, not real output (each file says so).
- One Trino node; no tuning of Trino, the metastore or MinIO; Derby is single-user and only fit
  for this benchmark.
- `physicalInputBytes` counts what Trino read from MinIO, including parquet footers; it is not
  comparable with `bytes_read` of `baseline` (size of the `*.json.gz`) or `qopt-*` (`rchar`).

## How to run

On GitHub: workflow **Trino benchmark** (`workflow_dispatch`). Steps: restore and verify the
reference day (cache shared with the Benchmark workflow), measure `baseline` again
(`naive_duckdb.py --runs 10`), `docker compose ... up -d --wait`, run `trino_bench.py --runs 10`,
print the stack logs on failure, job summary, upload, `docker compose ... down -v`.

Only the DDL, without Docker:

```bash
python benchmarks/trino_bench.py --print-ddl
```

On a Linux machine with Docker (not the owner's machine; phase 2 runs on the runner):

```bash
python benchmarks/fetch_reference.py
docker compose -f benchmarks/trino/docker-compose.yml up -d --wait
python benchmarks/trino_bench.py --runs 10 --git-sha <sha> --out out/trino-rows.csv --summary out/trino-summary.md --samples-dir out/trino-samples
docker compose -f benchmarks/trino/docker-compose.yml down -v
```

Options: `--data-dir`, `--reference`, `--seed`, `--work-dir` (default `data/tmp/trino-bench`,
git-ignored), `--trino-url` (default `http://localhost:8080`), `--bucket` (default
`repopulse-bench`), `--compose-file`.

## Recording the results

The workflow uploads the artifact **`trino-bench`**: `baseline-row.csv`, `trino-rows.csv` (the
`runs.csv` header plus up to 4 rows), `trino-summary.md` and `trino-samples/`. Merge the rows into
`runs.csv` (schema-checked, re-recording is a no-op), then regenerate the README results:

```bash
gh run download <run-id> -n trino-bench -D out/trino-bench
python benchmarks/record_run.py out/trino-bench/baseline-row.csv out/trino-bench/trino-rows.csv
python -m repopulse.readme_results
```
