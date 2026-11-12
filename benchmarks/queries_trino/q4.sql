-- Q4 in Trino SQL (484): same definitions and output as benchmarks/queries/q4.sql (DuckDB).
-- Translation notes: arg_min -> min_by; ->> '$.x' -> json_extract_scalar(payload, '$.x');
-- ends_with -> LIKE; epoch(b) - epoch(a) -> date_diff('millisecond', a, b) / 1000;
-- x::DOUBLE -> CAST(x AS DOUBLE); USING joins written as ON; GROUP BY repeats the item_type
-- expression (Trino does not group by a select alias).
-- quantile_cont (exact, linear interpolation) is computed on the sorted values as in q3.sql:
-- rank r = q * (n - 1), value = h[floor(r) + 1] + (r - floor(r)) * (h[ceiling(r) + 1] -
-- h[floor(r) + 1]); approx_percentile is approximate and not used.
WITH ev AS (
    SELECT
        e.actor.login AS actor_login,
        lower(e.actor.login) LIKE '%[bot]' OR lower(e.actor.login) LIKE '%-bot' AS is_bot,
        r.repo_name,
        e.type,
        json_extract_scalar(e.payload, '$.action') AS action,
        TRY_CAST(
            coalesce(
                json_extract_scalar(e.payload, '$.pull_request.number'),
                json_extract_scalar(e.payload, '$.issue.number'),
                json_extract_scalar(e.payload, '$.number')
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
    GROUP BY
        repo_name,
        item_number,
        CASE WHEN type = 'IssuesEvent' THEN 'issue' ELSE 'pull_request' END
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
        CAST(date_diff('millisecond', o.opened_at, r.responded_at) AS DOUBLE) / 1000 / 3600
            AS response_hours,
        c.closed_at
    FROM opened AS o
    LEFT JOIN responses AS r ON r.repo_name = o.repo_name AND r.item_number = o.item_number
    LEFT JOIN closes AS c ON c.repo_name = o.repo_name AND c.item_number = o.item_number
    WHERE NOT o.opener_is_bot
),
item_sorted AS (
    SELECT
        repo_name,
        item_type,
        count(*) AS opened,
        count(response_hours) AS responded,
        array_sort(array_agg(response_hours) FILTER (WHERE response_hours IS NOT NULL)) AS h,
        count(closed_at) AS closed
    FROM items
    GROUP BY repo_name, item_type
),
item_rank AS (
    SELECT
        *,
        (cardinality(h) - 1) * DOUBLE '0.5' AS r50,
        (cardinality(h) - 1) * DOUBLE '0.9' AS r90
    FROM item_sorted
)
SELECT
    repo_name,
    item_type,
    opened,
    responded,
    round(
        element_at(h, CAST(floor(r50) AS BIGINT) + 1) + (r50 - floor(r50)) * (
            element_at(h, CAST(ceiling(r50) AS BIGINT) + 1)
            - element_at(h, CAST(floor(r50) AS BIGINT) + 1)
        ),
        2
    ) AS response_hours_p50,
    round(
        element_at(h, CAST(floor(r90) AS BIGINT) + 1) + (r90 - floor(r90)) * (
            element_at(h, CAST(ceiling(r90) AS BIGINT) + 1)
            - element_at(h, CAST(floor(r90) AS BIGINT) + 1)
        ),
        2
    ) AS response_hours_p90,
    closed,
    round(100 * CAST(closed AS DOUBLE) / opened, 2) AS close_rate_pct
FROM item_rank
ORDER BY repo_name, item_type
