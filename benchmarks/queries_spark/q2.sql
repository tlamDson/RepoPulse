-- Q2 in Spark SQL (3.5): same definitions and output as benchmarks/queries/q2.sql (DuckDB).
-- Translation notes: ->> -> get_json_object; x::DOUBLE -> CAST; date + n -> date_add.
WITH human AS (
    SELECT
        e.actor.login AS actor_login,
        e.type,
        get_json_object(e.payload, '$.action') AS action,
        CAST(date_trunc('week', e.created_at) AS DATE) AS week
    FROM raw_events AS e
    JOIN repos AS r ON lower(e.repo.name) = lower(r.repo_name)
    WHERE e.type IN (
            'PullRequestEvent', 'PullRequestReviewEvent', 'PullRequestReviewCommentEvent',
            'IssuesEvent', 'IssueCommentEvent'
        )
        AND NOT (endswith(lower(e.actor.login), '[bot]') OR endswith(lower(e.actor.login), '-bot'))
),
cohorts AS (
    SELECT actor_login, min(week) AS cohort_week
    FROM human
    WHERE type = 'PullRequestEvent' AND action = 'opened'
    GROUP BY actor_login
),
activity AS (
    SELECT DISTINCT actor_login, week FROM human
)
SELECT
    c.cohort_week,
    count(*) AS cohort_size,
    count(a4.actor_login) AS retained_w4,
    count(a12.actor_login) AS retained_w12,
    round(100 * CAST(count(a4.actor_login) AS DOUBLE) / count(*), 2) AS retention_w4_pct,
    round(100 * CAST(count(a12.actor_login) AS DOUBLE) / count(*), 2) AS retention_w12_pct
FROM cohorts AS c
LEFT JOIN activity AS a4
    ON a4.actor_login = c.actor_login AND a4.week = date_add(c.cohort_week, 28)
LEFT JOIN activity AS a12
    ON a12.actor_login = c.actor_login AND a12.week = date_add(c.cohort_week, 84)
GROUP BY c.cohort_week
ORDER BY c.cohort_week
