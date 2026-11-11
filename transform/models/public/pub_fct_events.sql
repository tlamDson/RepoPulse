-- fct_events for the public role: user_key and actor_id masked with mask_pii, consistently with
-- pub_dim_user (the join on user_key gives the same rows as in gold).
select
    event_id,
    event_type,
    action,
    created_at,
    event_date,
    event_week,
    {{ mask_pii('user_key') }} as user_key,
    {{ mask_pii('actor_id') }} as actor_id,
    repo_key,
    repo_id,
    pr_number,
    issue_number,
    item_number,
    pr_merged,
    is_late
from {{ ref('fct_events') }}
