-- Grain: one GH Archive event on a seed repo. user_key = the dim_user version of the event's
-- actor with the event's login, latest valid_from at or before the event (the version valid at
-- created_at). PII hash columns of silver are deliberately not selected.
with events as (
    select s.*, r.repo_key
    from {{ ref('stg_gharchive_events') }} as s
    join {{ ref('dim_repo') }} as r on lower(s.repo_name) = r.repo_key
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
    cast(event_id as varchar) as event_id,
    cast(event_type as varchar) as event_type,
    cast(action as varchar) as action,
    cast(created_at as timestamp) as created_at,
    cast(event_date as date) as event_date,
    cast(date_trunc('week', created_at) as date) as event_week,
    cast(user_key as varchar) as user_key,
    cast(actor_id as bigint) as actor_id,
    cast(repo_key as varchar) as repo_key,
    cast(repo_id as bigint) as repo_id,
    cast(pr_number as bigint) as pr_number,
    cast(issue_number as bigint) as issue_number,
    cast(coalesce(pr_number, issue_number) as bigint) as item_number,
    cast(pr_merged as boolean) as pr_merged,
    cast(is_late as boolean) as is_late
from with_user
where rn = 1
