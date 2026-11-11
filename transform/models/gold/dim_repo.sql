-- One row per seed repo; repo_key = lower(repo_name) so events match case-insensitively.
select
    cast(lower(repo_name) as varchar) as repo_key,
    cast(repo_name as varchar) as repo_name,
    cast(category as varchar) as category
from {{ ref('repos_data_ecosystem') }}
