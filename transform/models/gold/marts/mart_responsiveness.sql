-- Q4: same definitions and result as benchmarks/queries/q4.sql. item_number is
-- coalesce(pr_number, issue_number) from fct_events.
with ev as (
    select
        actor_login,
        is_bot,
        repo_name,
        event_type as type,
        action,
        item_number,
        created_at
    from {{ ref('int_events_enriched') }}
    where event_type in (
        'IssuesEvent', 'PullRequestEvent', 'IssueCommentEvent', 'PullRequestReviewEvent',
        'PullRequestReviewCommentEvent'
    )
),

opened as (
    select
        repo_name,
        item_number,
        case when type = 'IssuesEvent' then 'issue' else 'pull_request' end as item_type,
        min_by(actor_login, created_at) as opener,
        min_by(is_bot, created_at) as opener_is_bot,
        min(created_at) as opened_at
    from ev
    where action = 'opened' and type in ('IssuesEvent', 'PullRequestEvent')
        and item_number is not null
    group by repo_name, item_number, item_type
),

responses as (
    select o.repo_name, o.item_number, min(r.created_at) as responded_at
    from opened as o
    join ev as r
        on r.repo_name = o.repo_name
        and r.item_number = o.item_number
        and r.type in (
            'IssueCommentEvent', 'PullRequestReviewEvent', 'PullRequestReviewCommentEvent'
        )
        and not r.is_bot
        and r.actor_login <> o.opener
        and r.created_at >= o.opened_at
    group by o.repo_name, o.item_number
),

closes as (
    select o.repo_name, o.item_number, min(c.created_at) as closed_at
    from opened as o
    join ev as c
        on c.repo_name = o.repo_name
        and c.item_number = o.item_number
        and c.created_at >= o.opened_at
        and (
            (o.item_type = 'issue' and c.type = 'IssuesEvent' and c.action = 'closed')
            or (
                o.item_type = 'pull_request' and c.type = 'PullRequestEvent'
                and c.action in ('closed', 'merged')
            )
        )
    group by o.repo_name, o.item_number
),

items as (
    select
        o.repo_name,
        o.item_type,
        {{ hours_between('o.opened_at', 'r.responded_at') }} as response_hours,
        c.closed_at
    from opened as o
    left join responses as r using (repo_name, item_number)
    left join closes as c using (repo_name, item_number)
    where not o.opener_is_bot
)

select
    cast(repo_name as string) as repo_name,
    cast(item_type as string) as item_type,
    cast(count(*) as bigint) as opened,
    cast(count(response_hours) as bigint) as responded,
    cast(
        round(percentile_cont(0.5) within group (order by response_hours), 2) as double
    ) as response_hours_p50,
    cast(
        round(percentile_cont(0.9) within group (order by response_hours), 2) as double
    ) as response_hours_p90,
    cast(count(closed_at) as bigint) as closed,
    cast(round(100 * cast(count(closed_at) as double) / count(*), 2) as double) as close_rate_pct
from items
group by repo_name, item_type
