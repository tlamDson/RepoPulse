-- Q1: active contributors per day and per ISO week (Monday start), for each seed repo and
-- each seed category, split into new vs returning.
--
-- Definitions shared by Q1-Q6:
--   seed repo     raw_events.repo.name matches repos.repo_name (case-insensitive); other repos
--                 are ignored.
--   contribution  one event of type PullRequestEvent, PullRequestReviewEvent,
--                 PullRequestReviewCommentEvent, IssuesEvent or IssueCommentEvent (any action).
--                 Push/Create/Delete events are never used; fork and watch are not contributions.
--   bot           actor login ending in '[bot]' or '-bot' (case-insensitive).
--   active        a human (non-bot) actor with >= 1 contribution in the period and scope.
--   new           active, and the actor's first contribution in that scope (repo or category)
--                 within the loaded data falls in this period; otherwise returning.
-- The baseline does not dedupe: duplicate raw lines count twice (Q1 counts distinct actors,
-- so duplicates do not change it).
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
        AND NOT (ends_with(lower(e.actor.login), '[bot]') OR ends_with(lower(e.actor.login), '-bot'))
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
    JOIN firsts AS f USING (scope, scope_key, actor_login)
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
