# Read-only SQL agent over the `public` schema (AI2)

The agent takes an English question, calls an LLM through an OpenAI-compatible API (`base_url` and model are
configuration, for example `llama-server` running a local model), extracts the SQL from the answer and runs it on the DuckDB file
built by dbt. The agent plays the `public` role of `docs/governance.md`: it **only** reads the `public` schema
(`main_public` on DuckDB) and writes nothing. The code is in `repopulse/sql_agent.py`, the evaluation question set in
`repopulse/sql_agent_eval.toml`, the tests in `tests/test_sql_agent.py` and
`tests/test_sql_agent_eval.py`.

This document contains no measurements. CI and all tests only use a mock LLM (a fixed answer script,
or a fake HTTP server on `127.0.0.1`), so test results are **not** accuracy measurements of
any model. The real measurements (accuracy, time per question) are recorded in `runs.csv` by task AI3 with the `eval`
command below.

## Read-only boundary (layer by layer)

The layers do not trust each other: a later layer still blocks when an earlier one lets something through.

1. **Guard** (`Guard.check`) uses DuckDB's own parser, not keyword regexes:
   `extract_statements` counts statements and gets the type; `json_serialize_sql` gives the syntax tree, walked
   recursively over every dict/list (independent of key position, so it works on both DuckDB 1.4 and 2.0).
   Fail closed: a parse error, a tree that cannot be serialized, or an unknown FROM item is rejected.
   - Exactly **one** statement of type `SELECT` (including `WITH … SELECT`). `SHOW`/`DESCRIBE`/`SUMMARIZE`
     (which DuckDB treats as a SELECT over `SHOW_REF`) and statement-form `PIVOT` are also rejected.
   - Every table must be a relation of the public schema, with the list read from the `information_schema` of the
     database itself when the guard is built. Accepts names without a schema (when the connection resolves bare names to
     `main_public`, or a CTE **in scope**) and `main_public.<table>` (optionally with the database's catalog).
     Rejects `main_gold`, `main_staging`, `main`, `information_schema`, `pg_catalog`,
     system views (`duckdb_tables`, `pg_tables`), nonexistent tables, file paths
     (`FROM 'x.csv'`), even inside a subquery in WHERE, a scalar subquery, a CTE, a UNION branch, a JOIN or
     LATERAL. A CTE only takes effect inside the query that defines it: a CTE name in a subquery cannot shadow a
     real table outside.
   - All table functions are rejected (`read_csv`, `read_parquet`, `read_json`, `read_text`, `glob`,
     `query`, `duckdb_*()`, `pragma_*`…), except `range`, `generate_series`, `unnest`.
   - Functions that expose configuration/secrets are rejected: `current_setting` (on MotherDuck
     `current_setting('motherduck_token')` leaks the token), `getenv`, `which_secret`, every `duckdb_*`,
     `pragma_*`.
   - Names are compared case-insensitively, with or without double quotes. The string `'drop table x'`, a comment
     containing `delete`, and a column `updated_at` are not wrongly blocked (tests prove the guard is not a regex).
2. **Connection** (`open_readonly`): only local DuckDB files, `read_only=True`,
   `enable_external_access=false` (no file read/write, no network), autoinstall/autoload of
   extensions off, Python replacement scans off, `search_path` = public schema, then
   `lock_configuration=true` (cannot `SET` again). Tests run `DROP`, `INSERT`, `CREATE`,
   `read_csv`, `COPY … TO`, `SET enable_external_access`, `LOAD` directly on the connection (bypassing the guard): all
   fail and the database file is unchanged (compared by sha256).
3. **Row limit** (`run_query`): fetches at most `max_rows + 1` rows, returns `max_rows` rows and a `truncated` flag,
   regardless of what `LIMIT` the LLM wrote. Default 200 (`--max-rows`).

MotherDuck targets (`md:`, `motherduck:`) are rejected with a clear error: this version only supports local files.

## LLM configuration

| Environment variable | CLI flag | Default |
|---|---|---|
| `REPOPULSE_LLM_BASE_URL` | `--base-url` | `http://127.0.0.1:8080/v1` (the default port of `llama-server`) |
| `REPOPULSE_LLM_MODEL` | `--model` | none: if missing it is a configuration error (exit 2) |
| `REPOPULSE_LLM_API_KEY` | — | no `Authorization` header is sent |

Flags override environment variables. The module does not read `.env` files; the key never appears in error messages,
the client's `repr` or reports. The client sends `POST {base_url}/chat/completions` with
`{"model", "messages", "temperature": 0}` and reads `choices[0].message.content`; HTTP errors, connection
errors and malformed JSON become `LLMError`. `extract_sql` drops `<think>…</think>` blocks (reasoning Qwen
models), takes the last ```` ```sql ```` block (or the whole answer if there is none), and drops a trailing `;`.

## Running with `llama-server`

```bash
llama-server -m <model>.gguf --port 8080          # OpenAI-compatible server on 127.0.0.1:8080
python -m dbt.cli.main build --project-dir transform --profiles-dir transform --target ci   # writes ci.duckdb
python -m repopulse.sql_agent ask --db ci.duckdb --model <model name> "Which 3 repositories have the highest health score?"
```

`ask` prints the SQL, the result table (tab-separated) and the number of attempts. Exit 0 when it can answer, 1 when attempts run out without
valid SQL, 2 for a configuration error, when the LLM cannot be called or the database cannot be opened.

The loop (`ask`): the system prompt contains the rules (a single SELECT, DuckDB dialect, only the listed tables) and the schema
description (tables + columns + types of **only** `main_public`, read by the code from `information_schema`, not
through the LLM). If the SQL is blocked by the guard or DuckDB reports an error, the error is sent back to the LLM to fix, at most
`--max-attempts` times (default 3).

## Evaluation set

```bash
python -m repopulse.sql_agent eval --db ci.duckdb --model <model name> --report out/sql-agent-eval.json \
    --out out/sql-agent-row.csv --git-sha <sha>
python benchmarks/record_run.py out/sql-agent-row.csv   # merge into runs.csv (task AI3)
```

- `repopulse/sql_agent_eval.toml`: each `[[question]]` has `id`, `question` (English, stating clearly the columns to
  return), `sql` (the reference answer on `main_public`), `ordered` (default `false`). 16 questions covering the 6 marts
  Q1–Q6, `fct_events`, `dim_repo`, `dim_user`; from single-table filters, ordered top-N and aggregation, to fact–dim
  joins.
- **Execution accuracy**: the reference answer is the result of the reference SQL run on the **same database at evaluation time**
  (through the same guard), with no hard-coded values since the data changes hourly. The agent's row limit is
  raised to the number of reference rows, so the limit alone does not turn a correct answer into a wrong one.
- **Scoring** (`score`): correct when there is an injective mapping from reference columns to result columns such that the projected
  rows are equal. Column names are ignored, extra columns and column reordering are allowed; multisets are compared when
  `ordered = false`, sequences when `ordered = true`; numbers compare with an absolute tolerance of `1e-6`
  (`1 == 1.0`, `Decimal` against `float`), `NULL == NULL`, bool is not a number. A wrong value,
  missing/extra rows, a wrong number of repeated rows, wrong order when `ordered`, a truncated result (`truncated`) or an
  agent error all count as wrong.
- JSON report: per question `{id, question, correct, sql, error, attempts, seconds}`, totals
  `{questions, correct, accuracy, total_seconds, p50_seconds}`, plus `model`, `base_url` (no
  key). `eval` exits 0 when the full set ran: accuracy is a measurement, not pass/fail.
- **If the LLM cannot be called the measurement is void**: exit 2, no report, no CSV row (it is not
  counted as "all wrong").
- `runs.csv` row (`--out`, needs `--git-sha`): `variant` = `--variant` or `sql-agent-<model slug>`
  (lowercase, characters outside `[a-z0-9_-]` become `-`, for example `Qwen3.6-35B-A3B` →
  `sql-agent-qwen3-6-35b-a3b`), `data_date` = `max(event_date)` of `main_public.fct_events`,
  `total_seconds` = total time of all questions, `row_count` = number of questions, `notes` =
  `accuracy=<correct>/<total>; p50_question_s=<seconds>; model=<model>`. A one-row file of the same shape as the other
  benchmarks, merged with `benchmarks/record_run.py`.

## Known limitations

- Only local DuckDB files; MotherDuck is not supported yet (an `md:` connection has no per-file `read_only` and
  has `current_setting('motherduck_token')`, so it needs its own design).
- No timeout yet for long-running queries (DuckDB has `conn.interrupt()`); the row limit only limits the returned
  result, not the compute.
- The guard also rejects harmless things outside the list (for example table functions other than the three sequence generators,
  `DESCRIBE`): failing closed is intentional.
- The schema description has only column names and types, no dbt column descriptions (`persist_docs` is not enabled).
- The question set is small and scored by result: an answer that is correct by luck (same result on the current
  data) still counts as correct.
