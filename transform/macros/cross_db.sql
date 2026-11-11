{#- Cross-database macros: DuckDB (target ci) and Snowflake (target snowflake, docs/snowflake-port.md).
    Models keep standard SQL; only constructs with no common spelling dispatch here. -#}

{#- Hours from start_ts to end_ts as a double (NULL when either side is NULL). -#}
{% macro hours_between(start_ts, end_ts) -%}
    {{ return(adapter.dispatch('hours_between', 'repopulse')(start_ts, end_ts)) }}
{%- endmacro %}

{% macro duckdb__hours_between(start_ts, end_ts) -%}
    (epoch({{ end_ts }}) - epoch({{ start_ts }})) / 3600
{%- endmacro %}

{% macro snowflake__hours_between(start_ts, end_ts) -%}
    cast(datediff(microsecond, {{ start_ts }}, {{ end_ts }}) as double) / 3600000000
{%- endmacro %}


{#- UTC wall time of a timestamp as a TIMESTAMP without zone. Spark writes UTC; on DuckDB the
    silver parquet may arrive as TIMESTAMPTZ, and epoch_us is absolute for TIMESTAMPTZ and UTC
    for TIMESTAMP, so the result is UTC whatever the session zone. On Snowflake the source is
    always the seed (TIMESTAMP_NTZ, already UTC). -#}
{% macro utc_timestamp(expr) -%}
    {{ return(adapter.dispatch('utc_timestamp', 'repopulse')(expr)) }}
{%- endmacro %}

{% macro duckdb__utc_timestamp(expr) -%}
    make_timestamp(epoch_us({{ expr }}))
{%- endmacro %}

{% macro snowflake__utc_timestamp(expr) -%}
    cast({{ expr }} as timestamp)
{%- endmacro %}


{#- Cast an expression to an array of strings (silver's commit_author_*_hashes). The CI seed
    stores these columns as varchar (all empty in the fixture). -#}
{% macro cast_string_array(expr) -%}
    {{ return(adapter.dispatch('cast_string_array', 'repopulse')(expr)) }}
{%- endmacro %}

{% macro duckdb__cast_string_array(expr) -%}
    cast({{ expr }} as varchar[])
{%- endmacro %}

{% macro snowflake__cast_string_array(expr) -%}
    cast(try_parse_json(cast({{ expr }} as varchar)) as array)
{%- endmacro %}


{#- Silver parquet files selected by the var silver_gharchive_parquet. -#}
{% macro silver_parquet_relation(glob) -%}
    {{ return(adapter.dispatch('silver_parquet_relation', 'repopulse')(glob)) }}
{%- endmacro %}

{% macro duckdb__silver_parquet_relation(glob) -%}
    read_parquet('{{ glob | replace("'", "''") }}')
{%- endmacro %}

{% macro snowflake__silver_parquet_relation(glob) -%}
    {{ exceptions.raise_compiler_error(
        "var silver_gharchive_parquet is not supported on Snowflake: reading parquet needs a "
        ~ "stage (out of scope of the port, docs/snowflake-port.md). Unset the var to build "
        ~ "from the CI seed fixture."
    ) }}
{%- endmacro %}
