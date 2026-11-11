-- Every fact row points at the dim_user version of its own actor whose login is the event's
-- login and whose validity window contains the event time. Returns the offending events.
-- Windows are half-open [valid_from, valid_to), except a zero-length version (valid_from =
-- valid_to: the actor used two logins in the same second) which is the single instant [t, t].
select f.event_id
from {{ ref('fct_events') }} as f
join {{ ref('stg_gharchive_events') }} as s using (event_id)
left join {{ ref('dim_user') }} as u on u.user_key = f.user_key
where u.user_key is null
    or u.actor_id <> s.actor_id
    or u.actor_login is distinct from s.actor_login
    or s.created_at < u.valid_from
    or (
        u.valid_to is not null
        and (
            s.created_at > u.valid_to
            or (s.created_at = u.valid_to and u.valid_from < u.valid_to)
        )
    )
