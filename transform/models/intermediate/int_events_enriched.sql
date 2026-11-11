-- fct_events with the actor login / bot flag of the dim_user version valid at the event and the
-- seed repo name / category: the "raw_events JOIN repos" of benchmarks/queries/q*.sql.
select
    f.event_id,
    f.event_type,
    f.action,
    f.created_at,
    f.event_date,
    f.event_week,
    f.pr_number,
    f.item_number,
    f.pr_merged,
    u.actor_login,
    u.is_bot,
    r.repo_name,
    r.category
from {{ ref('fct_events') }} as f
join {{ ref('dim_user') }} as u using (user_key)
join {{ ref('dim_repo') }} as r using (repo_key)
