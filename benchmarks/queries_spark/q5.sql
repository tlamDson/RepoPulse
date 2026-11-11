-- Q5 in Spark SQL (3.5): same definitions and output as benchmarks/queries/q5.sql (DuckDB).
-- Translation notes: x::DOUBLE -> CAST; USING join written as ON.
WITH ev AS (
    SELECT
        r.repo_name,
        r.category,
        e.actor.login AS actor_login,
        endswith(lower(e.actor.login), '[bot]') OR endswith(lower(e.actor.login), '-bot') AS is_bot
    FROM raw_events AS e
    JOIN repos AS r ON lower(e.repo.name) = lower(r.repo_name)
    WHERE e.type IN (
        'PullRequestEvent', 'PullRequestReviewEvent', 'PullRequestReviewCommentEvent',
        'IssuesEvent', 'IssueCommentEvent'
    )
),
totals AS (
    SELECT
        repo_name,
        category,
        count(*) AS contributions,
        count(*) FILTER (WHERE is_bot) AS bot_contributions
    FROM ev
    GROUP BY repo_name, category
),
per_actor AS (
    SELECT repo_name, actor_login, count(*) AS n
    FROM ev
    WHERE NOT is_bot
    GROUP BY repo_name, actor_login
),
ranked AS (
    SELECT
        repo_name,
        n,
        row_number() OVER (PARTITION BY repo_name ORDER BY n DESC, actor_login) AS rk
    FROM per_actor
),
human AS (
    SELECT
        repo_name,
        count(*) AS human_contributors,
        sum(n) AS human_n,
        sum(n) FILTER (WHERE rk <= 1) AS top1,
        sum(n) FILTER (WHERE rk <= 2) AS top2,
        sum(n) FILTER (WHERE rk <= 3) AS top3
    FROM ranked
    GROUP BY repo_name
)
SELECT
    t.repo_name,
    t.category,
    t.contributions,
    t.bot_contributions,
    round(100 * CAST(t.bot_contributions AS DOUBLE) / t.contributions, 2) AS bot_share_pct,
    coalesce(h.human_contributors, 0) AS human_contributors,
    round(100 * CAST(h.top1 AS DOUBLE) / h.human_n, 2) AS top1_share_pct,
    round(100 * CAST(h.top2 AS DOUBLE) / h.human_n, 2) AS top2_share_pct,
    round(100 * CAST(h.top3 AS DOUBLE) / h.human_n, 2) AS top3_share_pct
FROM totals AS t
LEFT JOIN human AS h ON h.repo_name = t.repo_name
ORDER BY t.repo_name
