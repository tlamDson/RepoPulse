-- Q3: same definitions and result as benchmarks/queries/q3.sql. A merge event is action
-- 'merged' or action 'closed' with pr_merged (silver's payload.pull_request.merged).
with ev as (
    select
        actor_login,
        is_bot,
        repo_name,
        event_type as type,
        action,
        pr_merged,
        pr_number,
        created_at
    from {{ ref('int_events_enriched') }}
    where event_type in ('ForkEvent', 'PullRequestEvent')
),

forks as (
    select repo_name, actor_login, min(created_at) as forked_at
    from ev
    where type = 'ForkEvent' and not is_bot
    group by repo_name, actor_login
),

opened as (
    select
        repo_name,
        pr_number,
        arg_min(actor_login, created_at) as author,
        arg_min(is_bot, created_at) as author_is_bot,
        min(created_at) as opened_at
    from ev
    where type = 'PullRequestEvent' and action = 'opened' and pr_number is not null
    group by repo_name, pr_number
),

merges as (
    select repo_name, pr_number, created_at as merged_at
    from ev
    where type = 'PullRequestEvent'
        and (action = 'merged' or (action = 'closed' and pr_merged))
),

prs as (
    select o.repo_name, o.pr_number, o.author, o.opened_at, min(m.merged_at) as merged_at
    from opened as o
    left join merges as m
        on m.repo_name = o.repo_name and m.pr_number = o.pr_number and m.merged_at >= o.opened_at
    where not o.author_is_bot
    group by o.repo_name, o.pr_number, o.author, o.opened_at
),

funnel as (
    select
        f.repo_name,
        count(distinct f.actor_login) as forkers,
        count(distinct f.actor_login) filter (where p.pr_number is not null) as forkers_with_pr,
        count(distinct f.actor_login) filter (where p.merged_at is not null) as forkers_with_merge
    from forks as f
    left join prs as p
        on p.repo_name = f.repo_name and p.author = f.actor_login and p.opened_at >= f.forked_at
    group by f.repo_name
),

pr_stats as (
    select
        repo_name,
        count(*) as prs_opened,
        count(merged_at) as prs_merged,
        quantile_cont((epoch(merged_at) - epoch(opened_at)) / 3600, 0.5) as p50,
        quantile_cont((epoch(merged_at) - epoch(opened_at)) / 3600, 0.9) as p90
    from prs
    group by repo_name
)

select
    cast(r.repo_name as varchar) as repo_name,
    cast(r.category as varchar) as category,
    cast(coalesce(f.forkers, 0) as bigint) as forkers,
    cast(coalesce(f.forkers_with_pr, 0) as bigint) as forkers_with_pr,
    cast(coalesce(f.forkers_with_merge, 0) as bigint) as forkers_with_merge,
    cast(coalesce(s.prs_opened, 0) as bigint) as prs_opened,
    cast(coalesce(s.prs_merged, 0) as bigint) as prs_merged,
    cast(round(s.p50, 2) as double) as merge_hours_p50,
    cast(round(s.p90, 2) as double) as merge_hours_p90
from {{ ref('dim_repo') }} as r
left join funnel as f on f.repo_name = r.repo_name
left join pr_stats as s on s.repo_name = r.repo_name
where f.repo_name is not null or s.repo_name is not null
