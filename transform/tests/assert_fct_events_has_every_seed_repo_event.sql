-- fct_events holds exactly the staging events on seed repos (repo name matched
-- case-insensitively), each once. Returns a row when the counts differ.
with expected as (
    select count(*) as n
    from {{ ref('stg_gharchive_events') }} as s
    join {{ ref('repos_data_ecosystem') }} as r on lower(s.repo_name) = lower(r.repo_name)
),
actual as (
    select count(*) as n, count(distinct event_id) as n_distinct from {{ ref('fct_events') }}
)
select e.n as expected_rows, a.n as actual_rows, a.n_distinct
from expected as e cross join actual as a
where e.n <> a.n or a.n <> a.n_distinct
