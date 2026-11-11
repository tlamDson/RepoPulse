-- Q1: same definitions and result as benchmarks/queries/q1.sql.
with contrib as (
    select
        actor_login,
        repo_name,
        category,
        event_date as day,
        event_week as week
    from {{ ref('int_events_enriched') }}
    where event_type in (
            'PullRequestEvent', 'PullRequestReviewEvent', 'PullRequestReviewCommentEvent',
            'IssuesEvent', 'IssueCommentEvent'
        )
        and not is_bot
),

scoped as (
    select 'repo' as scope, repo_name as scope_key, actor_login, day, week from contrib
    union all
    select 'category' as scope, category as scope_key, actor_login, day, week from contrib
),

firsts as (
    select scope, scope_key, actor_login, min(day) as first_day, min(week) as first_week
    from scoped
    group by scope, scope_key, actor_login
),

active as (
    select distinct 'day' as period, day as period_start, scope, scope_key, actor_login
    from scoped
    union
    select distinct 'week' as period, week as period_start, scope, scope_key, actor_login
    from scoped
),

flagged as (
    select
        a.period,
        a.period_start,
        a.scope,
        a.scope_key,
        case a.period
            when 'day' then f.first_day = a.period_start
            else f.first_week = a.period_start
        end as is_new
    from active as a
    join firsts as f using (scope, scope_key, actor_login)
)

select
    cast(period as varchar) as period,
    cast(period_start as date) as period_start,
    cast(scope as varchar) as scope,
    cast(scope_key as varchar) as scope_key,
    cast(count(*) as bigint) as active_contributors,
    cast(count(case when is_new then 1 end) as bigint) as new_contributors,
    cast(count(case when not is_new then 1 end) as bigint) as returning_contributors
from flagged
group by period, period_start, scope, scope_key
