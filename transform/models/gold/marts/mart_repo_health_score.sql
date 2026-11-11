-- Q6: same components, weights, rounding and ranking as benchmarks/queries/q6.sql:
-- health_score = 100 * (0.25 activity + 0.15 retention + 0.15 merge + 0.20 response
--                       + 0.10 close + 0.15 diversity); rank by score desc, then repo_name.
with ev as (
    select
        actor_login,
        is_bot,
        repo_name,
        event_type as type,
        action,
        pr_merged,
        item_number,
        event_week as week,
        created_at
    from {{ ref('int_events_enriched') }}
    where event_type in (
        'PullRequestEvent', 'PullRequestReviewEvent', 'PullRequestReviewCommentEvent',
        'IssuesEvent', 'IssueCommentEvent'
    )
),

per_actor as (
    select repo_name, actor_login, count(*) as n, count(distinct week) as weeks
    from ev
    where not is_bot
    group by repo_name, actor_login
),

people as (
    select
        repo_name,
        count(*) as contributors,
        count(*) filter (where weeks >= 2) as multi_week,
        max(n)::double / sum(n) as top1_share
    from per_actor
    group by repo_name
),

opened as (
    select
        repo_name,
        item_number,
        case when type = 'IssuesEvent' then 'issue' else 'pull_request' end as item_type,
        arg_min(actor_login, created_at) as opener,
        arg_min(is_bot, created_at) as opener_is_bot,
        min(created_at) as opened_at
    from ev
    where action = 'opened' and type in ('IssuesEvent', 'PullRequestEvent')
        and item_number is not null
    group by repo_name, item_number, item_type
),

item_flags as (
    select
        o.repo_name,
        o.item_type,
        bool_or(
            x.type in (
                'IssueCommentEvent', 'PullRequestReviewEvent', 'PullRequestReviewCommentEvent'
            )
            and not x.is_bot and x.actor_login <> o.opener
        ) as responded,
        bool_or(
            (o.item_type = 'issue' and x.type = 'IssuesEvent' and x.action = 'closed')
            or (
                o.item_type = 'pull_request' and x.type = 'PullRequestEvent'
                and x.action in ('closed', 'merged')
            )
        ) as closed,
        bool_or(
            o.item_type = 'pull_request' and x.type = 'PullRequestEvent'
            and (x.action = 'merged' or (x.action = 'closed' and x.pr_merged))
        ) as merged
    from opened as o
    left join ev as x
        on x.repo_name = o.repo_name
        and x.item_number = o.item_number
        and x.created_at >= o.opened_at
    where not o.opener_is_bot
    group by o.repo_name, o.item_number, o.item_type
),

items as (
    select
        repo_name,
        count(*) as opened,
        count(*) filter (where responded) as responded,
        count(*) filter (where closed) as closed,
        count(*) filter (where item_type = 'pull_request') as prs_opened,
        count(*) filter (where merged) as prs_merged
    from item_flags
    group by repo_name
),

scored_repos as (
    select distinct repo_name from ev
),

components as (
    select
        s.repo_name,
        coalesce(
            p.contributors::double / nullif(max(p.contributors) over (), 0), 0
        ) as activity,
        coalesce(p.multi_week::double / p.contributors, 0) as retention,
        coalesce(i.prs_merged::double / nullif(i.prs_opened, 0), 0) as merge,
        coalesce(i.responded::double / nullif(i.opened, 0), 0) as response,
        coalesce(i.closed::double / nullif(i.opened, 0), 0) as close,
        coalesce(1 - p.top1_share, 0) as diversity
    from scored_repos as s
    left join people as p using (repo_name)
    left join items as i using (repo_name)
),

scores as (
    select
        *,
        100 * (
            0.25 * activity + 0.15 * retention + 0.15 * merge + 0.20 * response
            + 0.10 * close + 0.15 * diversity
        ) as score
    from components
)

select
    cast(row_number() over (order by s.score desc, s.repo_name) as bigint) as health_rank,
    cast(s.repo_name as varchar) as repo_name,
    cast(r.category as varchar) as category,
    cast(round(s.activity, 4) as double) as activity_score,
    cast(round(s.retention, 4) as double) as retention_score,
    cast(round(s.merge, 4) as double) as merge_score,
    cast(round(s.response, 4) as double) as response_score,
    cast(round(s.close, 4) as double) as close_score,
    cast(round(s.diversity, 4) as double) as diversity_score,
    cast(round(s.score, 2) as double) as health_score
from scores as s
join {{ ref('dim_repo') }} as r using (repo_name)
