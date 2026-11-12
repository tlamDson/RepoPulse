{#- Cross-database macros: DuckDB (target ci), Snowflake (target snowflake, docs/snowflake-port.md)
    and Databricks SQL (target databricks, docs/dbt-databricks.md). Models keep standard SQL; only
    constructs with no common spelling dispatch here. Every macro has a duckdb__, a snowflake__
    and a databricks__ branch (tests/test_dbt_portability.py, tests/test_dbt_databricks.py). -#}

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

{#- unix_micros: exact microseconds since the epoch (bigint); the difference is cast before the
    division so the result is a double whatever the ANSI mode. -#}
{% macro databricks__hours_between(start_ts, end_ts) -%}
    cast(unix_micros({{ end_ts }}) - unix_micros({{ start_ts }}) as double) / 3600000000
{%- endmacro %}


{#- UTC wall time of a timestamp as a TIMESTAMP without zone. Spark writes UTC; on DuckDB the
    silver parquet may arrive as TIMESTAMPTZ, and epoch_us is absolute for TIMESTAMPTZ and UTC
    for TIMESTAMP, so the result is UTC whatever the session zone. On Snowflake the source is
    always the seed (TIMESTAMP_NTZ, already UTC). On Databricks TIMESTAMP is an instant shown in
    the session time zone; the SQL warehouse default (and assumption, docs/dbt-databricks.md) is
    UTC, so the silver Delta column and the seed text both read as UTC wall time. -#}
{% macro utc_timestamp(expr) -%}
    {{ return(adapter.dispatch('utc_timestamp', 'repopulse')(expr)) }}
{%- endmacro %}

{% macro duckdb__utc_timestamp(expr) -%}
    make_timestamp(epoch_us({{ expr }}))
{%- endmacro %}

{% macro snowflake__utc_timestamp(expr) -%}
    cast({{ expr }} as timestamp)
{%- endmacro %}

{% macro databricks__utc_timestamp(expr) -%}
    cast({{ expr }} as timestamp)
{%- endmacro %}


{#- Cast an expression to an array of strings (silver's commit_author_*_hashes). The CI seed
    stores these columns as text (all empty in the fixture): from_text=true says the input is
    that text. DuckDB and Snowflake cast text and arrays the same way and ignore it; Databricks
    cannot cast a string to an array, so it parses the text as JSON there. -#}
{% macro cast_string_array(expr, from_text=false) -%}
    {{ return(adapter.dispatch('cast_string_array', 'repopulse')(expr, from_text)) }}
{%- endmacro %}

{% macro duckdb__cast_string_array(expr, from_text=false) -%}
    cast({{ expr }} as varchar[])
{%- endmacro %}

{% macro snowflake__cast_string_array(expr, from_text=false) -%}
    cast(try_parse_json(cast({{ expr }} as varchar)) as array)
{%- endmacro %}

{% macro databricks__cast_string_array(expr, from_text=false) -%}
    {%- if from_text -%}
        from_json(cast({{ expr }} as string), 'array<string>')
    {%- else -%}
        cast({{ expr }} as array<string>)
    {%- endif -%}
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

{% macro databricks__silver_parquet_relation(glob) -%}
    {{ exceptions.raise_compiler_error(
        "var silver_gharchive_parquet is not supported on Databricks: staging reads the silver "
        ~ "Delta table of the Databricks job from Unity Catalog (dbt source silver, vars "
        ~ "silver_gharchive_catalog/_schema/_table, docs/dbt-databricks.md). Unset the var."
    ) }}
{%- endmacro %}


{#- date_expr plus n days, still a DATE. -#}
{% macro add_days(date_expr, n) -%}
    {{ return(adapter.dispatch('add_days', 'repopulse')(date_expr, n)) }}
{%- endmacro %}

{% macro duckdb__add_days(date_expr, n) -%}
    ({{ date_expr }} + {{ n }})
{%- endmacro %}

{% macro snowflake__add_days(date_expr, n) -%}
    ({{ date_expr }} + {{ n }})
{%- endmacro %}

{% macro databricks__add_days(date_expr, n) -%}
    date_add({{ date_expr }}, {{ n }})
{%- endmacro %}


{#- SHA-256 of a string as 64 lowercase hex chars. -#}
{% macro sha256_hex(expr) -%}
    {{ return(adapter.dispatch('sha256_hex', 'repopulse')(expr)) }}
{%- endmacro %}

{% macro duckdb__sha256_hex(expr) -%}
    sha256({{ expr }})
{%- endmacro %}

{% macro snowflake__sha256_hex(expr) -%}
    sha2({{ expr }}, 256)
{%- endmacro %}

{% macro databricks__sha256_hex(expr) -%}
    sha2({{ expr }}, 256)
{%- endmacro %}


{#- expr without the leading/trailing characters of the string expression chars. -#}
{% macro trim_chars(expr, chars) -%}
    {{ return(adapter.dispatch('trim_chars', 'repopulse')(expr, chars)) }}
{%- endmacro %}

{% macro duckdb__trim_chars(expr, chars) -%}
    trim({{ expr }}, {{ chars }})
{%- endmacro %}

{% macro snowflake__trim_chars(expr, chars) -%}
    trim({{ expr }}, {{ chars }})
{%- endmacro %}

{#- trim(str, chars) means trim(trimStr, str) in old Spark: btrim has one meaning. -#}
{% macro databricks__trim_chars(expr, chars) -%}
    btrim({{ expr }}, {{ chars }})
{%- endmacro %}


{#- Regular expressions. pattern is the regex itself (RE2/Java syntax common to the engines),
    not a SQL literal: each branch writes the literal. Snowflake and Databricks read a backslash
    in a string literal as an escape, so their branches double it. -#}

{#- The whole of expr matches pattern. -#}
{% macro regex_full_match(expr, pattern) -%}
    {{ return(adapter.dispatch('regex_full_match', 'repopulse')(expr, pattern)) }}
{%- endmacro %}

{% macro duckdb__regex_full_match(expr, pattern) -%}
    regexp_full_match({{ expr }}, '{{ pattern }}')
{%- endmacro %}

{#- regexp_like is anchored on both ends on Snowflake. -#}
{% macro snowflake__regex_full_match(expr, pattern) -%}
    regexp_like({{ expr }}, '{{ pattern | replace("\\", "\\\\") }}')
{%- endmacro %}

{#- rlike finds the pattern anywhere: anchor it; \z (not $) so a trailing newline fails. -#}
{% macro databricks__regex_full_match(expr, pattern) -%}
    ({{ expr }} rlike '^(?:{{ pattern | replace("\\", "\\\\") }})\\z')
{%- endmacro %}

{#- pattern matches somewhere in expr. -#}
{% macro regex_contains(expr, pattern) -%}
    {{ return(adapter.dispatch('regex_contains', 'repopulse')(expr, pattern)) }}
{%- endmacro %}

{% macro duckdb__regex_contains(expr, pattern) -%}
    regexp_matches({{ expr }}, '{{ pattern }}')
{%- endmacro %}

{% macro snowflake__regex_contains(expr, pattern) -%}
    (regexp_instr({{ expr }}, '{{ pattern | replace("\\", "\\\\") }}') > 0)
{%- endmacro %}

{% macro databricks__regex_contains(expr, pattern) -%}
    ({{ expr }} rlike '{{ pattern | replace("\\", "\\\\") }}')
{%- endmacro %}


{#- A query with one column `element`: every element of the array column of relation (rows
    where the array is NULL give none). -#}
{% macro string_array_elements(relation, column) -%}
    {{ return(adapter.dispatch('string_array_elements', 'repopulse')(relation, column)) }}
{%- endmacro %}

{% macro duckdb__string_array_elements(relation, column) -%}
    select unnest({{ column }}) as element from {{ relation }} where {{ column }} is not null
{%- endmacro %}

{% macro snowflake__string_array_elements(relation, column) -%}
    select cast(f.value as varchar) as element
    from {{ relation }} as t, lateral flatten(input => t.{{ column }}) as f
    where t.{{ column }} is not null
{%- endmacro %}

{% macro databricks__string_array_elements(relation, column) -%}
    select explode({{ column }}) as element from {{ relation }} where {{ column }} is not null
{%- endmacro %}
