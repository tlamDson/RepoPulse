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
        count(*) filter (where is_bot) as bot_contributions
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
        sum(n) filter (where rk <= 1) as top1,
        sum(n) filter (where rk <= 2) as top2,
        sum(n) filter (where rk <= 3) as top3
    from ranked
    group by repo_name
)

select
    cast(t.repo_name as varchar) as repo_name,
    cast(t.category as varchar) as category,
    cast(t.contributions as bigint) as contributions,
    cast(t.bot_contributions as bigint) as bot_contributions,
    cast(round(100 * t.bot_contributions::double / t.contributions, 2) as double) as bot_share_pct,
    cast(coalesce(h.human_contributors, 0) as bigint) as human_contributors,
    cast(round(100 * h.top1::double / h.human_n, 2) as double) as top1_share_pct,
    cast(round(100 * h.top2::double / h.human_n, 2) as double) as top2_share_pct,
    cast(round(100 * h.top3::double / h.human_n, 2) as double) as top3_share_pct
from totals as t
left join human as h using (repo_name)
