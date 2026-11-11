-- Q5: same definitions and result as benchmarks/queries/q5.sql (on deduplicated events:
-- silver keeps one row per event_id, the naive baseline counts duplicate raw lines twice).
with ev as (
    select repo_name, category, actor_login, is_bot
    from {{ ref('int_events_enriched') }}
    where event_type in (
        'PullRequestEvent', 'PullRequestReviewEvent', 'PullRequestReviewCommentEvent',
        'IssuesEvent', 'IssueCommentEvent'
    )
),

totals as (
    select
        repo_name,
        category,
        count(*) as contributions,
        count(case when is_bot then 1 end) as bot_contributions
    from ev
    group by repo_name, category
),

per_actor as (
    select repo_name, actor_login, count(*) as n
    from ev
    where not is_bot
    group by repo_name, actor_login
),

ranked as (
    select
        repo_name,
        n,
        row_number() over (partition by repo_name order by n desc, actor_login) as rk
    from per_actor
),

human as (
    select
        repo_name,
        count(*) as human_contributors,
        sum(n) as human_n,
        sum(case when rk <= 1 then n end) as top1,
        sum(case when rk <= 2 then n end) as top2,
        sum(case when rk <= 3 then n end) as top3
    from ranked
    group by repo_name
)

select
    cast(t.repo_name as varchar) as repo_name,
    cast(t.category as varchar) as category,
    cast(t.contributions as bigint) as contributions,
    cast(t.bot_contributions as bigint) as bot_contributions,
    cast(
        round(100 * cast(t.bot_contributions as double) / t.contributions, 2) as double
    ) as bot_share_pct,
    cast(coalesce(h.human_contributors, 0) as bigint) as human_contributors,
    cast(round(100 * cast(h.top1 as double) / h.human_n, 2) as double) as top1_share_pct,
    cast(round(100 * cast(h.top2 as double) / h.human_n, 2) as double) as top2_share_pct,
    cast(round(100 * cast(h.top3 as double) / h.human_n, 2) as double) as top3_share_pct
from totals as t
left join human as h using (repo_name)
