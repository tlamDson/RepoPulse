{#- Silver GH Archive events, columns of docs/spark-jobs.md, accumulated across runs.
    Source chosen by var: silver_gharchive_parquet set -> parquet glob (DuckDB only,
    macros/cross_db.sql); unset -> CI seed fixture.

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

with source as (
{%- if silver_parquet %}
    select * from {{ silver_parquet_relation(silver_parquet) }}
{%- else %}
    select * from {{ ref('fixture_silver_gharchive_events') }}
{%- endif %}
),

typed as (
    select
        cast(event_id as varchar) as event_id,
        cast(event_type as varchar) as event_type,
        -- Spark writes UTC; parquet may arrive as TIMESTAMPTZ. epoch_us is absolute for
        -- TIMESTAMPTZ and UTC for TIMESTAMP, so the wall time is UTC whatever the session zone.
        make_timestamp(epoch_us(created_at)) as created_at,
        cast(event_date as date) as event_date,
        cast(actor_id as bigint) as actor_id,
        cast(actor_login as varchar) as actor_login,
        cast(repo_id as bigint) as repo_id,
        cast(repo_name as varchar) as repo_name,
        cast(action as varchar) as action,
        cast(pr_number as bigint) as pr_number,
        cast(issue_number as bigint) as issue_number,
        cast(pr_merged as boolean) as pr_merged,
        cast(commit_author_email_hashes as varchar[]) as commit_author_email_hashes,
        cast(commit_author_name_hashes as varchar[]) as commit_author_name_hashes,
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
