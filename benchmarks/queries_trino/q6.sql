-- Q6 in Trino SQL (484): same definitions and output as benchmarks/queries/q6.sql (DuckDB).
-- Translation notes: arg_min -> min_by; ->> '$.x' -> json_extract_scalar(payload, '$.x');
-- ends_with -> LIKE; x::DOUBLE -> CAST(x AS DOUBLE); USING joins written as ON; GROUP BY repeats
-- the item_type expression (Trino does not group by a select alias). Weights: see q6.sql.
WITH ev AS (
    SELECT
        e.actor.login AS actor_login,
        lower(e.actor.login) LIKE '%[bot]' OR lower(e.actor.login) LIKE '%-bot' AS is_bot,
        r.repo_name,
        e.type,
        json_extract_scalar(e.payload, '$.action') AS action,
        json_extract_scalar(e.payload, '$.pull_request.merged') AS merged_flag,
        TRY_CAST(
            coalesce(
                json_extract_scalar(e.payload, '$.pull_request.number'),
                json_extract_scalar(e.payload, '$.issue.number'),
                json_extract_scalar(e.payload, '$.number')
            ) AS BIGINT
        ) AS item_number,
        CAST(date_trunc('week', e.created_at) AS DATE) AS week,
        e.created_at
    FROM raw_events AS e
    JOIN repos AS r ON lower(e.repo.name) = lower(r.repo_name)
    WHERE e.type IN (
        'PullRequestEvent', 'PullRequestReviewEvent', 'PullRequestReviewCommentEvent',
        'IssuesEvent', 'IssueCommentEvent'
    )
),
-- Q1/Q5 side: contributors, weeks active, top-1 share
per_actor AS (
    SELECT repo_name, actor_login, count(*) AS n, count(DISTINCT week) AS weeks
    FROM ev
    WHERE NOT is_bot
    GROUP BY repo_name, actor_login
),
people AS (
    SELECT
        repo_name,
        count(*) AS contributors,
        count(*) FILTER (WHERE weeks >= 2) AS multi_week,
        CAST(max(n) AS DOUBLE) / sum(n) AS top1_share
    FROM per_actor
    GROUP BY repo_name
),
-- Q3/Q4 side: items opened by humans, merged / responded / closed
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
item_flags AS (
    SELECT
        o.repo_name,
        o.item_type,
        bool_or(
            x.type IN (
                'IssueCommentEvent', 'PullRequestReviewEvent', 'PullRequestReviewCommentEvent'
            )
            AND NOT x.is_bot AND x.actor_login <> o.opener
        ) AS responded,
        bool_or(
            (o.item_type = 'issue' AND x.type = 'IssuesEvent' AND x.action = 'closed')
            OR (
                o.item_type = 'pull_request' AND x.type = 'PullRequestEvent'
                AND x.action IN ('closed', 'merged')
            )
        ) AS closed,
        bool_or(
            o.item_type = 'pull_request' AND x.type = 'PullRequestEvent'
            AND (x.action = 'merged' OR (x.action = 'closed' AND x.merged_flag = 'true'))
        ) AS merged
    FROM opened AS o
    LEFT JOIN ev AS x
        ON x.repo_name = o.repo_name
        AND x.item_number = o.item_number
        AND x.created_at >= o.opened_at
    WHERE NOT o.opener_is_bot
    GROUP BY o.repo_name, o.item_number, o.item_type
),
items AS (
    SELECT
        repo_name,
        count(*) AS opened,
        count(*) FILTER (WHERE responded) AS responded,
        count(*) FILTER (WHERE closed) AS closed,
        count(*) FILTER (WHERE item_type = 'pull_request') AS prs_opened,
        count(*) FILTER (WHERE merged) AS prs_merged
    FROM item_flags
    GROUP BY repo_name
),
scored_repos AS (
    SELECT DISTINCT repo_name FROM ev
),
components AS (
    SELECT
        s.repo_name,
        coalesce(
            CAST(p.contributors AS DOUBLE) / nullif(max(p.contributors) OVER (), 0), 0
        ) AS activity,
        coalesce(CAST(p.multi_week AS DOUBLE) / p.contributors, 0) AS retention,
        coalesce(CAST(i.prs_merged AS DOUBLE) / nullif(i.prs_opened, 0), 0) AS merge,
        coalesce(CAST(i.responded AS DOUBLE) / nullif(i.opened, 0), 0) AS response,
        coalesce(CAST(i.closed AS DOUBLE) / nullif(i.opened, 0), 0) AS close,
        coalesce(1 - p.top1_share, 0) AS diversity
    FROM scored_repos AS s
    LEFT JOIN people AS p ON p.repo_name = s.repo_name
    LEFT JOIN items AS i ON i.repo_name = s.repo_name
),
scores AS (
    SELECT
        *,
        100 * (
            0.25 * activity + 0.15 * retention + 0.15 * merge + 0.20 * response
            + 0.10 * close + 0.15 * diversity
        ) AS score
    FROM components
)
SELECT
    row_number() OVER (ORDER BY s.score DESC, s.repo_name) AS health_rank,
    s.repo_name,
    r.category,
    round(s.activity, 4) AS activity_score,
    round(s.retention, 4) AS retention_score,
    round(s.merge, 4) AS merge_score,
    round(s.response, 4) AS response_score,
    round(s.close, 4) AS close_score,
    round(s.diversity, 4) AS diversity_score,
    round(s.score, 2) AS health_score
FROM scores AS s
JOIN repos AS r ON r.repo_name = s.repo_name
ORDER BY health_rank
