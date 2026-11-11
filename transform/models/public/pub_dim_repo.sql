-- dim_repo for the public role: no personal data (every column pii: none), passed through.
select * from {{ ref('dim_repo') }}
