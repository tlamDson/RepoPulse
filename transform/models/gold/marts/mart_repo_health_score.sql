-- Q6: same components, weights, rounding and ranking as benchmarks/queries/q6.sql:
-- health_score = 100 * (0.25 activity + 0.15 retention + 0.15 merge + 0.20 response
--                       + 0.10 close + 0.15 diversity); rank by score desc, then repo_name.
-- Items are rated only when opened >= 7 days before the last loaded contribution event.
-- merge / response / close are NULL (n/a) when the repo has no item to rate, or when no repo in
-- the loaded window has a single matched merge / response / close (the window is too short to
-- see follow-ups); the score divides by the sum of the weights of the non-NULL components.
-- A repo is scored and ranked only with >= q6_min_human_contributions (default 5) human
-- contributions; the others stay listed with NULL health_rank and health_score (not enough data).
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
        count(case when weeks >= 2 then 1 end) as multi_week,
        sum(n) as human_contributions,
        cast(max(n) as double) / sum(n) as top1_share
    from per_actor
    group by repo_name
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

item_flags as (
    select
        o.repo_name,
        o.item_type,
        -- "any row matches" as max(0/1) = 1: bool_or is DuckDB-only (Snowflake: boolor_agg).
        max(case when
            x.type in (
                'IssueCommentEvent', 'PullRequestReviewEvent', 'PullRequestReviewCommentEvent'
            )
            and not x.is_bot and x.actor_login <> o.opener
        then 1 else 0 end) = 1 as responded,
        max(case when
            (o.item_type = 'issue' and x.type = 'IssuesEvent' and x.action = 'closed')
            or (
                o.item_type = 'pull_request' and x.type = 'PullRequestEvent'
                and x.action in ('closed', 'merged')
            )
        then 1 else 0 end) = 1 as closed,
        max(case when
            o.item_type = 'pull_request' and x.type = 'PullRequestEvent'
            and (x.action = 'merged' or (x.action = 'closed' and x.pr_merged))
        then 1 else 0 end) = 1 as merged
    from opened as o
    left join ev as x
        on x.repo_name = o.repo_name
        and x.item_number = o.item_number
        and x.created_at >= o.opened_at
    where not o.opener_is_bot
        -- only items with a full 7-day follow-up window before the end of the loaded events
        and {{ hours_between('o.opened_at', '(select max(created_at) from ev)') }} >= 168
    group by o.repo_name, o.item_number, o.item_type
),

items as (
    select
        repo_name,
        count(*) as opened,
        count(case when responded then 1 end) as responded,
        count(case when closed then 1 end) as closed,
        count(case when item_type = 'pull_request' then 1 end) as prs_opened,
        count(case when merged then 1 end) as prs_merged
    from item_flags
    group by repo_name
),

scored_repos as (
    select distinct repo_name from ev
),

components as (
    select
        s.repo_name,
        coalesce(p.human_contributions, 0) as human_contributions,
        coalesce(
            cast(p.contributors as double) / nullif(max(p.contributors) over (), 0), 0
        ) as activity,
        coalesce(cast(p.multi_week as double) / p.contributors, 0) as retention,
        case when sum(i.prs_merged) over () > 0
            then cast(i.prs_merged as double) / nullif(i.prs_opened, 0)
        end as merge_rate,
        case when sum(i.responded) over () > 0
            then cast(i.responded as double) / nullif(i.opened, 0)
        end as response,
        case when sum(i.closed) over () > 0
            then cast(i.closed as double) / nullif(i.opened, 0)
        end as close_rate,
        coalesce(1 - p.top1_share, 0) as diversity
    from scored_repos as s
    left join people as p using (repo_name)
    left join items as i using (repo_name)
),

scores as (
    select
        *,
        human_contributions >= {{ var('q6_min_human_contributions', 5) }} as is_scored,
        100 * (
            0.25 * activity + 0.15 * retention + coalesce(0.15 * merge_rate, 0)
            + coalesce(0.20 * response, 0) + coalesce(0.10 * close_rate, 0) + 0.15 * diversity
        ) / (
            0.55 + case when merge_rate is null then 0 else 0.15 end
            + case when response is null then 0 else 0.20 end
            + case when close_rate is null then 0 else 0.10 end
        ) as score
    from components
)

select
    cast(
        case when s.is_scored
            then row_number() over (partition by s.is_scored order by s.score desc, s.repo_name)
        end as bigint
    ) as health_rank,
    cast(s.repo_name as string) as repo_name,
    cast(r.category as string) as category,
    cast(round(s.activity, 4) as double) as activity_score,
    cast(round(s.retention, 4) as double) as retention_score,
    cast(round(s.merge_rate, 4) as double) as merge_score,
    cast(round(s.response, 4) as double) as response_score,
    cast(round(s.close_rate, 4) as double) as close_score,
    cast(round(s.diversity, 4) as double) as diversity_score,
    cast(case when s.is_scored then round(s.score, 2) end as double) as health_score
from scores as s
join {{ ref('dim_repo') }} as r using (repo_name)
