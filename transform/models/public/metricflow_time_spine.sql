-- Time spine of the semantic layer (docs/semantic-layer.md): one row per day, 2020-01-01 to
-- 2030-12-31 (end date exclusive in dbt.date_spine). Calendar only, no personal data.
-- dbt.date_spine is dbt's own cross-database macro (DuckDB/MotherDuck, Snowflake, Databricks).
with days as (
    {{ dbt.date_spine("day", "cast('2020-01-01' as date)", "cast('2031-01-01' as date)") }}
)

select cast(date_day as date) as date_day
from days
