-- dim_user SCD2: per actor_id exactly one current version (valid_to NULL); every other version
-- ends exactly where the next one starts (no gap, no overlap); valid_to never before valid_from;
-- version numbers are 1..n without holes. Returns the offending rows.
with v as (
    select
        *,
        lead(valid_from) over (partition by actor_id order by version_number) as next_from,
        count(case when is_current then 1 end) over (partition by actor_id) as n_current,
        row_number() over (partition by actor_id order by valid_from, version_number) as rn
    from {{ ref('dim_user') }}
)
select *
from v
where n_current <> 1
    or (is_current and (valid_to is not null or next_from is not null))
    or (not is_current and (valid_to is null or valid_to <> next_from))
    or valid_to < valid_from
    or version_number <> rn
