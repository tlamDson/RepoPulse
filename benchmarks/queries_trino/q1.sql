-- Q1 in Trino SQL (484): same definitions and output as benchmarks/queries/q1.sql (DuckDB).
-- Translation notes: ends_with(x, s) -> x LIKE '%s' ('[' and '-' are not LIKE wildcards);
-- USING joins written as ON. Session time zone UTC (timestamps have no zone).
WITH contrib AS (
    SELECT
        e.actor.login AS actor_login,
        r.repo_name,
        r.category,
        CAST(e.created_at AS DATE) AS day,
        CAST(date_trunc('week', e.created_at) AS DATE) AS week
    FROM raw_events AS e
    JOIN repos AS r ON lower(e.repo.name) = lower(r.repo_name)
    WHERE e.type IN (
            'PullRequestEvent', 'PullRequestReviewEvent', 'PullRequestReviewCommentEvent',
            'IssuesEvent', 'IssueCommentEvent'
        )
        AND NOT (lower(e.actor.login) LIKE '%[bot]' OR lower(e.actor.login) LIKE '%-bot')
),
scoped AS (
    SELECT 'repo' AS scope, repo_name AS scope_key, actor_login, day, week FROM contrib
    UNION ALL
    SELECT 'category' AS scope, category AS scope_key, actor_login, day, week FROM contrib
),
firsts AS (
    SELECT scope, scope_key, actor_login, min(day) AS first_day, min(week) AS first_week
    FROM scoped
    GROUP BY scope, scope_key, actor_login
),
active AS (
    SELECT DISTINCT 'day' AS period, day AS period_start, scope, scope_key, actor_login
    FROM scoped
    UNION
    SELECT DISTINCT 'week' AS period, week AS period_start, scope, scope_key, actor_login
    FROM scoped
),
flagged AS (
    SELECT
        a.period,
        a.period_start,
        a.scope,
        a.scope_key,
        CASE a.period
            WHEN 'day' THEN f.first_day = a.period_start
            ELSE f.first_week = a.period_start
        END AS is_new
    FROM active AS a
    JOIN firsts AS f
        ON f.scope = a.scope AND f.scope_key = a.scope_key AND f.actor_login = a.actor_login
)
SELECT
    period,
    period_start,
    scope,
    scope_key,
    count(*) AS active_contributors,
    count(*) FILTER (WHERE is_new) AS new_contributors,
    count(*) FILTER (WHERE NOT is_new) AS returning_contributors
FROM flagged
GROUP BY period, period_start, scope, scope_key
ORDER BY period, scope, scope_key, period_start
