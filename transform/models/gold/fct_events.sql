-- Grain: one GH Archive event on a seed repo. user_key = the dim_user version of the event's
-- actor with the event's login, latest valid_from at or before the event (the version valid at
-- created_at). PII hash columns of silver are deliberately not selected.
--
-- Incremental (delete+insert on event_id). A late event can insert an earlier login version and
-- shift the version_number (hence user_key) of every later event of that actor, even when the
-- late event is on a repo outside the seed. So a run recomputes every fact of each touched actor:
-- an actor with a staging row loaded after the newest loaded_at already in this table. Reworking
-- an actor twice is harmless; skipping one is not, so the watermark comes from this table.
-- On Databricks the strategy is merge on event_id (an upsert): the batch has one row per
-- event_id, so it leaves the same table as delete+insert (docs/dbt-databricks.md).
{{ config(
    materialized='incremental',
    incremental_strategy=('merge' if target.type == 'databricks' else 'delete+insert'),
    unique_key='event_id',
    on_schema_change='fail'
) }}

with staged as (
    select * from {{ ref('stg_gharchive_events') }}
),

{%- if is_incremental() %}

touched_actors as (
    select distinct actor_id
    from staged
    where loaded_at > (
        select coalesce(max(loaded_at), cast('1970-01-01' as timestamp)) from {{ this }}
    )
),
{%- endif %}

events as (
    select s.*, r.repo_key
    from staged as s
    join {{ ref('dim_repo') }} as r on lower(s.repo_name) = r.repo_key
    {%- if is_incremental() %}
    where s.actor_id in (select actor_id from touched_actors)
    {%- endif %}
),

with_user as (
    select
        e.*,
        u.user_key,
        row_number() over (
            partition by e.event_id order by u.valid_from desc, u.version_number desc
        ) as rn
    from events as e
    join {{ ref('dim_user') }} as u
        on u.actor_id = e.actor_id
        and u.actor_login is not distinct from e.actor_login
        and u.valid_from <= e.created_at
)

select
    cast(event_id as string) as event_id,
    cast(event_type as string) as event_type,
    cast(action as string) as action,
    cast(created_at as timestamp) as created_at,
    cast(event_date as date) as event_date,
    cast(date_trunc('week', created_at) as date) as event_week,
    cast(user_key as string) as user_key,
    cast(actor_id as bigint) as actor_id,
    cast(repo_key as string) as repo_key,
    cast(repo_id as bigint) as repo_id,
    cast(pr_number as bigint) as pr_number,
    cast(issue_number as bigint) as issue_number,
    cast(coalesce(pr_number, issue_number) as bigint) as item_number,
    cast(pr_merged as boolean) as pr_merged,
    cast(is_late as boolean) as is_late,
    cast(loaded_at as timestamp) as loaded_at
from with_user
where rn = 1
