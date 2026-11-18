-- Q2: same definitions and result as benchmarks/queries/q2.sql.
with human as (
    select actor_login, event_type as type, action, event_week as week
    from {{ ref('int_events_enriched') }}
    where event_type in (
            'PullRequestEvent', 'PullRequestReviewEvent', 'PullRequestReviewCommentEvent',
            'IssuesEvent', 'IssueCommentEvent'
        )
        and not is_bot
),

cohorts as (
    select actor_login, min(week) as cohort_week
    from human
    where type = 'PullRequestEvent' and action = 'opened'
    group by actor_login
),

activity as (
    select distinct actor_login, week from human
),

-- Latest loaded day. A retention pct is NULL ("not yet measurable") while cohort_week + 28 (or 84)
-- days is later than it: that week has no data yet, so 0.0 would be wrong. NULL day (no dates
-- given) leaves the pct measurable.
latest as (
    select cast(max(event_date) as date) as latest_day from {{ ref('int_events_enriched') }}
)

select
    cast(c.cohort_week as date) as cohort_week,
    cast(count(*) as bigint) as cohort_size,
    cast(count(a4.actor_login) as bigint) as retained_w4,
    cast(count(a12.actor_login) as bigint) as retained_w12,
    cast(
        case
            when {{ add_days('c.cohort_week', 28) }} > l.latest_day then null
            else round(100 * cast(count(a4.actor_login) as double) / count(*), 2)
        end as double
    ) as retention_w4_pct,
    cast(
        case
            when {{ add_days('c.cohort_week', 84) }} > l.latest_day then null
            else round(100 * cast(count(a12.actor_login) as double) / count(*), 2)
        end as double
    ) as retention_w12_pct
from cohorts as c
cross join latest as l
left join activity as a4
    on a4.actor_login = c.actor_login and a4.week = {{ add_days('c.cohort_week', 28) }}
left join activity as a12
    on a12.actor_login = c.actor_login and a12.week = {{ add_days('c.cohort_week', 84) }}
group by c.cohort_week, l.latest_day
