-- SCD2 per actor_id from event history (gaps-and-islands). A version is a run of consecutive
-- events (order created_at, event_id) of one actor_id with the same actor_login. Deterministic:
-- depends only on the events, not on when dbt runs.
with ordered as (
    select
        actor_id,
        actor_login,
        created_at,
        coalesce(
            actor_login is distinct from lag(actor_login) over (
                partition by actor_id order by created_at, event_id
            ),
            true
        ) as starts_version,
        event_id
    from {{ ref('stg_gharchive_events') }}
),

numbered as (
    select
        *,
        sum(case when starts_version then 1 else 0 end) over (
            partition by actor_id order by created_at, event_id
            rows between unbounded preceding and current row
        ) as version_number
    from ordered
),

versions as (
    select
        actor_id,
        version_number,
        any_value(actor_login) as actor_login,
        min(created_at) as valid_from
    from numbered
    group by actor_id, version_number
)

select
    cast(actor_id as varchar) || '-' || cast(version_number as varchar) as user_key,
    cast(actor_id as bigint) as actor_id,
    cast(actor_login as varchar) as actor_login,
    cast(version_number as bigint) as version_number,
    cast(valid_from as timestamp) as valid_from,
    cast(
        lead(valid_from) over (partition by actor_id order by version_number) as timestamp
    ) as valid_to,
    lead(valid_from) over (partition by actor_id order by version_number) is null as is_current,
    coalesce(
        ends_with(lower(actor_login), '[bot]') or ends_with(lower(actor_login), '-bot'), false
    ) as is_bot
from versions
