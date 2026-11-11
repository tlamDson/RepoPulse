-- Q4 in Spark SQL (3.5): same definitions and output as benchmarks/queries/q4.sql (DuckDB).
-- Translation notes: arg_min -> min_by; quantile_cont -> percentile; epoch() ->
-- unix_timestamp(); ->> -> get_json_object; USING joins written as ON.
WITH ev AS (
    SELECT
        e.actor.login AS actor_login,
        endswith(lower(e.actor.login), '[bot]') OR endswith(lower(e.actor.login), '-bot') AS is_bot,
        r.repo_name,
        e.type,
        get_json_object(e.payload, '$.action') AS action,
        try_cast(
            coalesce(
                get_json_object(e.payload, '$.pull_request.number'),
                get_json_object(e.payload, '$.issue.number'),
                get_json_object(e.payload, '$.number')
            ) AS BIGINT
        ) AS item_number,
        e.created_at
    FROM raw_events AS e
    JOIN repos AS r ON lower(e.repo.name) = lower(r.repo_name)
    WHERE e.type IN (
        'IssuesEvent', 'PullRequestEvent', 'IssueCommentEvent', 'PullRequestReviewEvent',
        'PullRequestReviewCommentEvent'
    )
),
opened AS (
    SELECT
        repo_name,
        item_number,
        CASE WHEN type = 'IssuesEvent' THEN 'issue' ELSE 'pull_request' END AS item_type,
        min_by(actor_login, created_at) AS opener,
        min_by(is_bot, created_at) AS opener_is_bot,
        min(created_at) AS opened_at
    FROM ev
    WHERE action = 'opened' AND type IN ('IssuesEvent', 'PullRequestEvent')
        AND item_number IS NOT NULL
    GROUP BY repo_name, item_number, CASE WHEN type = 'IssuesEvent' THEN 'issue' ELSE 'pull_request' END
),
responses AS (
    SELECT o.repo_name, o.item_number, min(r.created_at) AS responded_at
    FROM opened AS o
    JOIN ev AS r
        ON r.repo_name = o.repo_name
        AND r.item_number = o.item_number
        AND r.type IN (
            'IssueCommentEvent', 'PullRequestReviewEvent', 'PullRequestReviewCommentEvent'
        )
        AND NOT r.is_bot
        AND r.actor_login <> o.opener
        AND r.created_at >= o.opened_at
    GROUP BY o.repo_name, o.item_number
),
closes AS (
    SELECT o.repo_name, o.item_number, min(c.created_at) AS closed_at
    FROM opened AS o
    JOIN ev AS c
        ON c.repo_name = o.repo_name
        AND c.item_number = o.item_number
        AND c.created_at >= o.opened_at
        AND (
            (o.item_type = 'issue' AND c.type = 'IssuesEvent' AND c.action = 'closed')
            OR (
                o.item_type = 'pull_request' AND c.type = 'PullRequestEvent'
                AND c.action IN ('closed', 'merged')
            )
        )
    GROUP BY o.repo_name, o.item_number
),
items AS (
    SELECT
        o.repo_name,
        o.item_type,
        (unix_timestamp(r.responded_at) - unix_timestamp(o.opened_at)) / 3600 AS response_hours,
        c.closed_at
    FROM opened AS o
    LEFT JOIN responses AS r
        ON r.repo_name = o.repo_name AND r.item_number = o.item_number
    LEFT JOIN closes AS c
        ON c.repo_name = o.repo_name AND c.item_number = o.item_number
    WHERE NOT o.opener_is_bot
)
SELECT
    repo_name,
    item_type,
    count(*) AS opened,
    count(response_hours) AS responded,
    round(percentile(response_hours, 0.5), 2) AS response_hours_p50,
    round(percentile(response_hours, 0.9), 2) AS response_hours_p90,
    count(closed_at) AS closed,
    round(100 * CAST(count(closed_at) AS DOUBLE) / count(*), 2) AS close_rate_pct
FROM items
GROUP BY repo_name, item_type
ORDER BY repo_name, item_type
