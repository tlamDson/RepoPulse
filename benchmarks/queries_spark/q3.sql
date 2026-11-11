-- Q3 in Spark SQL (3.5): same definitions and output as benchmarks/queries/q3.sql (DuckDB).
-- Translation notes: arg_min -> min_by; quantile_cont -> percentile (linear interpolation);
-- epoch() -> unix_timestamp(); count(DISTINCT x) FILTER (WHERE c) -> count(DISTINCT CASE ...).
WITH ev AS (
    SELECT
        e.actor.login AS actor_login,
        endswith(lower(e.actor.login), '[bot]') OR endswith(lower(e.actor.login), '-bot') AS is_bot,
        r.repo_name,
        e.type,
        get_json_object(e.payload, '$.action') AS action,
        get_json_object(e.payload, '$.pull_request.merged') AS merged_flag,
        try_cast(
            coalesce(
                get_json_object(e.payload, '$.pull_request.number'),
                get_json_object(e.payload, '$.number')
            ) AS BIGINT
        ) AS pr_number,
        e.created_at
    FROM raw_events AS e
    JOIN repos AS r ON lower(e.repo.name) = lower(r.repo_name)
    WHERE e.type IN ('ForkEvent', 'PullRequestEvent')
),
forks AS (
    SELECT repo_name, actor_login, min(created_at) AS forked_at
    FROM ev
    WHERE type = 'ForkEvent' AND NOT is_bot
    GROUP BY repo_name, actor_login
),
opened AS (
    SELECT
        repo_name,
        pr_number,
        min_by(actor_login, created_at) AS author,
        min_by(is_bot, created_at) AS author_is_bot,
        min(created_at) AS opened_at
    FROM ev
    WHERE type = 'PullRequestEvent' AND action = 'opened' AND pr_number IS NOT NULL
    GROUP BY repo_name, pr_number
),
merges AS (
    SELECT repo_name, pr_number, created_at AS merged_at
    FROM ev
    WHERE type = 'PullRequestEvent'
        AND (action = 'merged' OR (action = 'closed' AND merged_flag = 'true'))
),
prs AS (
    SELECT o.repo_name, o.pr_number, o.author, o.opened_at, min(m.merged_at) AS merged_at
    FROM opened AS o
    LEFT JOIN merges AS m
        ON m.repo_name = o.repo_name AND m.pr_number = o.pr_number AND m.merged_at >= o.opened_at
    WHERE NOT o.author_is_bot
    GROUP BY o.repo_name, o.pr_number, o.author, o.opened_at
),
funnel AS (
    SELECT
        f.repo_name,
        count(DISTINCT f.actor_login) AS forkers,
        count(DISTINCT CASE WHEN p.pr_number IS NOT NULL THEN f.actor_login END)
            AS forkers_with_pr,
        count(DISTINCT CASE WHEN p.merged_at IS NOT NULL THEN f.actor_login END)
            AS forkers_with_merge
    FROM forks AS f
    LEFT JOIN prs AS p
        ON p.repo_name = f.repo_name AND p.author = f.actor_login AND p.opened_at >= f.forked_at
    GROUP BY f.repo_name
),
pr_stats AS (
    SELECT
        repo_name,
        count(*) AS prs_opened,
        count(merged_at) AS prs_merged,
        percentile((unix_timestamp(merged_at) - unix_timestamp(opened_at)) / 3600, 0.5) AS p50,
        percentile((unix_timestamp(merged_at) - unix_timestamp(opened_at)) / 3600, 0.9) AS p90
    FROM prs
    GROUP BY repo_name
)
SELECT
    r.repo_name,
    r.category,
    coalesce(f.forkers, 0) AS forkers,
    coalesce(f.forkers_with_pr, 0) AS forkers_with_pr,
    coalesce(f.forkers_with_merge, 0) AS forkers_with_merge,
    coalesce(s.prs_opened, 0) AS prs_opened,
    coalesce(s.prs_merged, 0) AS prs_merged,
    round(s.p50, 2) AS merge_hours_p50,
    round(s.p90, 2) AS merge_hours_p90
FROM repos AS r
LEFT JOIN funnel AS f ON f.repo_name = r.repo_name
LEFT JOIN pr_stats AS s ON s.repo_name = r.repo_name
WHERE f.repo_name IS NOT NULL OR s.repo_name IS NOT NULL
ORDER BY r.repo_name
