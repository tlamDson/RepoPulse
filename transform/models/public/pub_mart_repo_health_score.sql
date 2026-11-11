-- Q6 mart for the public role: aggregates only (every column pii: none), passed through.
select * from {{ ref('mart_repo_health_score') }}
