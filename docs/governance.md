# Governance: PII labels, roles, column masking

RepoPulse governance lives in the dbt + Python layer, because the chosen path is the **fallback**
(`docs/probe-result.md`): there is no Unity Catalog, and gold runs on DuckDB (CI) and MotherDuck. Databricks
tags and `GRANT`s are not used.

| Component | File |
|---|---|
| Policy (single source: PII classes, layers, roles) | `transform/governance.yml` |
| Column labels | `config: {meta: {pii: <class>}}` on every column in `transform/models/**/_*.yml` |
| Column masking | macro `transform/macros/mask_pii.sql`, models in `transform/models/public/` |
| dbt tests | `transform/tests/generic/hashed_pii.sql`, `transform/tests/generic/no_raw_email.sql` |
| Automatic checks | `repopulse/governance.py` (`python -m repopulse.governance scan` / `check`) |
| pytest | `tests/test_governance.py` |

## PII classes

| Class | Meaning | Allowed in |
|---|---|---|
| `none` | Not personal data: event id, event type, time, repo name, counts, scores | every layer |
| `public_identifier` | GitHub login or id (`actor_login`, `actor_id`, `user_key`). Public on GitHub but still personal data in pseudonymous form | staging, intermediate, gold; in `public` only when masked |
| `hashed` | Salted SHA-256 done at silver (`commit_author_email_hashes`, `commit_author_name_hashes`) | staging, intermediate. **Not** in gold/public |
| `direct` | Raw email or real name | **nowhere** from staging onward: silver hashes first (`repopulse/spark/pii.py`) |

## Role → permissions

| Role | Who | Reads | Writes | Sees raw |
|---|---|---|---|---|
| `pipeline` | Scheduled job, dbt (GitHub Actions) | staging, intermediate, gold, public | every layer | `none`, `public_identifier`, `hashed` |
| `analyst` | Analyst, reads the DuckDB file or a read-only MotherDuck database | gold, public | no | `none`, `public_identifier` |
| `public` | GitHub Pages dashboard, Tableau Public, anyone | **only** `public` | no | `none` |

Rule: if a role can read a column whose class is not in the role's "sees raw" list, that column must be
masked (`mask_pii`) and declared `masked: true`. `check` reports an error otherwise.

## Column labels (summary)

| Model | Columns that are not `none` |
|---|---|
| `stg_gharchive_events` | `actor_id`, `actor_login` (`public_identifier`); `commit_author_email_hashes`, `commit_author_name_hashes` (`hashed`, with test `hashed_pii`) |
| `int_events_enriched` | `actor_login` (`public_identifier`) |
| `dim_user` | `user_key`, `actor_id`, `actor_login` (`public_identifier`, with test `no_raw_email`) |
| `fct_events` | `user_key`, `actor_id` (`public_identifier`, with test `no_raw_email`) |
| `dim_repo`, 6 marts Q1–Q6 | none (all `none`) |
| `public.dim_user` | `user_key`, `actor_id`, `actor_login`: `public_identifier`, `masked: true`, tests `hashed_pii` + `no_raw_email` |
| `public.fct_events` | `user_key`, `actor_id`: `public_identifier`, `masked: true`, tests `hashed_pii` + `no_raw_email` |
| `public.dim_repo`, `public.mart_*` | none (all `none`, straight from gold) |

Seeds (`fixture_silver_gharchive_events`, `repos_data_ecosystem`) are outside the scope of `check`:
`check` only considers dbt models.

## The `public` schema and how masking works

Models in `transform/models/public/` (named `pub_*`, with aliases keeping the gold names) go into the `public` schema
(`main_public` on DuckDB), materialized as **table**:

| Relation | Source | Masked |
|---|---|---|
| `main_public.dim_repo` | `dim_repo` | no |
| `main_public.mart_daily_active_contributors` … `mart_repo_health_score` (6 marts) | gold marts | no |
| `main_public.dim_user` | `dim_user` | `user_key`, `actor_id`, `actor_login` |
| `main_public.fct_events` | `fct_events` | `user_key`, `actor_id` |

`mask_pii(x)` = `sha256(salt || lower(strip(cast(x as varchar))))`, 64-character lowercase hex, NULL
stays NULL: **exactly the formula** of silver's `repopulse.spark.pii.hash_pii(value, salt)`
(`tests/test_governance.py` compares row by row). The same salt is used for every masked key, so
`public.fct_events JOIN public.dim_user USING (user_key)` gives the same row count as in gold. `is_bot` and
the SCD2 columns are kept as they are.

Salt:

- `PII_HASH_SALT` set (any target): use that salt, the same secret as the silver job.
- Not set, target `ci`: use the var `ci_pii_hash_salt` in `transform/dbt_project.yml`, a fixture salt marked
  **not a secret**, only for CI and tests.
- Not set, any other target (`motherduck`, `databricks`, …): a compile error naming `PII_HASH_SALT`, never
  silently falling back to a default. `dbt parse` still works without the salt (it is only checked at execute time).

Why `table` and not `view`: a view's SQL (with the literal salt) is stored in the database; sharing the
public database/schema would expose the salt. A table stores only the masked result. The salt is still in the compiled SQL
(`target/`) and the dbt debug log: both directories are gitignored and no workflow currently uploads
them as artifacts; the scheduled job (PR 7) must keep it that way.

Limit of hash masking: GitHub logins are public, so anyone holding the salt can reverse-lookup a login from its hash.
The real salt lives only in the pipeline's secret; it is not shared with the `analyst`/`public` roles.

## Tests and automatic checks

| Check | Where | Catches |
|---|---|---|
| `hashed_pii` (generic) | `stg_gharchive_events.commit_author_*_hashes` (`is_array: true`); masked columns in `public` | The silver job forgot to hash (raw email/name in a `*_hashes` column): `dbt build` goes red and **every downstream model is SKIPPED**, so gold is not built. A public column that is not masked. NULL (CI seed) passes |
| `no_raw_email` (generic) | every column labeled other than `none` in gold and public | Raw email in an identifier column (cast to varchar so it also catches arrays/structs) |
| `scan` | `python -m repopulse.governance scan` | Raw email in **any** string, array, struct or map column of the scanned schema, including new models with no yml. It does not print values, only schema/table/column/row count. 64-hex hashes and logins are not flagged |
| `check` | `python -m repopulse.governance check` | A column missing `meta.pii`, an unknown label, `direct` from staging onward, `hashed` in gold/public, a role reading a class it may not see on a column that is not `masked: true`, a model outside the policy's layers; with `--db`: a column present in the built table but not declared in yml |
| pytest | `tests/test_governance.py` | All of the above on a real build and on copies of the project that are deliberately broken (email leaking into gold, dropping one gold column's label, a public view exposing `actor_login`), plus the masking formula, public row count = gold, and a non-`ci` target missing the salt |

The two dbt tests run in the existing CI job `dbt build (DuckDB)`; `scan`/`check` run in pytest (job
`Unit Tests`). The email regex pattern is `repopulse.governance.EMAIL_PATTERN`, copied verbatim into
`no_raw_email.sql` (a test keeps the two in sync).

## Running by hand

```bash
python -m dbt.cli.main build --project-dir transform --profiles-dir transform --target ci
python -m repopulse.governance scan --db ci.duckdb --schema main_gold --schema main_public
python -m repopulse.governance check --manifest transform/target/manifest.json --policy transform/governance.yml --db ci.duckdb
```

Exit codes: `0` clean, `1` has findings (a table is printed), `2` bad parameters, missing file, or the schema does not exist
(a mistyped schema is not treated as "clean"). `check` does not need `--db` (it only checks statically against the manifest,
`dbt parse` is enough); add `--db` to also check undeclared columns.

## Limitation: access control on DuckDB/MotherDuck

DuckDB and MotherDuck **have no column-level or schema-level `GRANT`**. The roles in the policy are a design,
enforced by sharing only the part that is needed:

- `public`: only export/share the `public` schema (the static dashboard reads files exported from `main_public`; on
  MotherDuck it is a share containing only public data). Since `public` holds tables, it does not depend on gold.
- `analyst`: a read-only share of the gold database.
- `pipeline`: the MotherDuck token in Actions secrets.

Not deployed on the cloud yet: no MotherDuck share has been created. The dashboard (PR 11) exports exactly the 6 marts of
`main_public` with `python -m repopulse.export_site` (`docs/dashboard.md`). Mapping the policy to dbt `grants` on Snowflake (a role per role, permissions per
schema, a masking policy for `public_identifier` columns) is left for PR 13.
