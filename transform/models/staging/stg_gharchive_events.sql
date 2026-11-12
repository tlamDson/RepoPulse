{#- Silver GH Archive events, columns of docs/spark-jobs.md, accumulated across runs.
    Source chosen by var (docs/dbt-models.md, docs/dbt-databricks.md):
      silver_gharchive_parquet set -> parquet glob (DuckDB only, macros/cross_db.sql);
      else silver_gharchive_source = unity_catalog -> dbt source silver.silver_gharchive_events,
        the Delta table of the Databricks job (default on target type databricks);
      else silver_gharchive_source = seed -> CI seed fixture (default on every other target).

    Incremental and protected from --full-refresh: the scheduled pipeline hands dbt the silver of
    one run's fresh lake (a few hours), so this table is the only place the history lives. A run
    inserts the event ids not in the table yet; the copy already there wins, like silver's MERGE.
    loaded_at = when the dbt run that inserted the row started (UTC); fct_events uses it to find
    the actors touched by a run. -#}
{{ config(
    materialized='incremental',
    incremental_strategy='append',
    full_refresh=false,
    on_schema_change='fail'
) }}
{%- set silver_parquet = var('silver_gharchive_parquet', none) -%}
{%- set silver_source = var(
    'silver_gharchive_source', 'unity_catalog' if target.type == 'databricks' else 'seed'
) -%}
{%- if silver_source not in ('seed', 'unity_catalog') -%}
    {{ exceptions.raise_compiler_error(
        "var silver_gharchive_source must be seed or unity_catalog, got '" ~ silver_source ~ "'"
    ) }}
{%- endif -%}
{#- The seed stores the commit author hash arrays as text (empty in the fixture). -#}
{%- set hashes_from_text = not silver_parquet and silver_source == 'seed' -%}

with source as (
{%- if silver_parquet %}
    select * from {{ silver_parquet_relation(silver_parquet) }}
{%- elif silver_source == 'unity_catalog' %}
    select * from {{ source('silver', 'silver_gharchive_events') }}
{%- else %}
    select * from {{ ref('fixture_silver_gharchive_events') }}
{%- endif %}
),

typed as (
    select
        cast(event_id as string) as event_id,
        cast(event_type as string) as event_type,
        -- Spark writes UTC; parquet may arrive as TIMESTAMPTZ. utc_timestamp (macros/cross_db.sql)
        -- keeps the UTC wall time whatever the session zone.
        {{ utc_timestamp('created_at') }} as created_at,
        cast(event_date as date) as event_date,
        cast(actor_id as bigint) as actor_id,
        cast(actor_login as string) as actor_login,
        cast(repo_id as bigint) as repo_id,
        cast(repo_name as string) as repo_name,
        cast(action as string) as action,
        cast(pr_number as bigint) as pr_number,
        cast(issue_number as bigint) as issue_number,
        cast(pr_merged as boolean) as pr_merged,
        {{ cast_string_array('commit_author_email_hashes', hashes_from_text) }}
            as commit_author_email_hashes,
        {{ cast_string_array('commit_author_name_hashes', hashes_from_text) }}
            as commit_author_name_hashes,
        cast(landing_date as date) as landing_date,
        cast(landing_hour as integer) as landing_hour,
        cast(is_late as boolean) as is_late
    from source
),

batch as (
    -- silver holds one row per event_id; keep one per id even if a batch repeats an id
    select *
    from typed
    qualify row_number() over (
        partition by event_id order by landing_date, landing_hour, created_at
    ) = 1
)

select
    b.*,
    cast('{{ run_started_at.strftime("%Y-%m-%d %H:%M:%S.%f") }}' as timestamp) as loaded_at
from batch as b
{%- if is_incremental() %}
where not exists (select 1 from {{ this }} as t where t.event_id = b.event_id)
{%- endif %}
