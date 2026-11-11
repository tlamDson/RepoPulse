{#- Silver GH Archive events, columns of docs/spark-jobs.md. Source chosen by var:
    silver_gharchive_parquet set -> read_parquet(<glob>); unset -> CI seed fixture. -#}
{%- set silver_parquet = var('silver_gharchive_parquet', none) -%}

with source as (
{%- if silver_parquet %}
    select * from read_parquet('{{ silver_parquet | replace("'", "''") }}')
{%- else %}
    select * from {{ ref('fixture_silver_gharchive_events') }}
{%- endif %}
)

select
    cast(event_id as varchar) as event_id,
    cast(event_type as varchar) as event_type,
    cast(created_at as timestamp) as created_at,
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
