-- Q4: responsiveness per seed repo and item type (issue / pull_request).
--   item            (repo, number) whose IssuesEvent or PullRequestEvent 'opened' event is in
--                   the data and whose opener (actor of the earliest 'opened') is human.
--   first response  earliest IssueCommentEvent, PullRequestReviewEvent or
--                   PullRequestReviewCommentEvent on the same (repo, number), at or after the
--                   opening, by a human actor other than the opener. Merging or closing without
--                   a comment/review is not a response.
--   closed          IssuesEvent 'closed' (issues) or PullRequestEvent 'closed'/'merged' (PRs) at
--                   or after the opening; close_rate_pct = 100 * closed / opened.
-- Response times are hours, p50/p90 by linear interpolation (quantile_cont) over responded
-- items. Definitions of seed repo and bot: see q1.sql.
WITH ev AS (
    SELECT
        e.actor.login AS actor_login,
        ends_with(lower(e.actor.login), '[bot]') OR ends_with(lower(e.actor.login), '-bot') AS is_bot,
        r.repo_name,
        e.type,
        e.payload ->> '$.action' AS action,
        TRY_CAST(
            coalesce(
                e.payload ->> '$.pull_request.number',
                e.payload ->> '$.issue.number',
                e.payload ->> '$.number'
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
        arg_min(actor_login, created_at) AS opener,
        arg_min(is_bot, created_at) AS opener_is_bot,
        min(created_at) AS opened_at
    FROM ev
    WHERE action = 'opened' AND type IN ('IssuesEvent', 'PullRequestEvent')
        AND item_number IS NOT NULL
    GROUP BY repo_name, item_number, item_type
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
        (epoch(r.responded_at) - epoch(o.opened_at)) / 3600 AS response_hours,
        c.closed_at
    FROM opened AS o
    LEFT JOIN responses AS r USING (repo_name, item_number)
    LEFT JOIN closes AS c USING (repo_name, item_number)
    WHERE NOT o.opener_is_bot
)
SELECT
    repo_name,
    item_type,
    count(*) AS opened,
    count(response_hours) AS responded,
    round(quantile_cont(response_hours, 0.5), 2) AS response_hours_p50,
    round(quantile_cont(response_hours, 0.9), 2) AS response_hours_p90,
    count(closed_at) AS closed,
    round(100 * count(closed_at)::DOUBLE / count(*), 2) AS close_rate_pct
FROM items
GROUP BY repo_name, item_type
ORDER BY repo_name, item_type
