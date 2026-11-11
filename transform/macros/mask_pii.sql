{#-
  mask_pii(expr): salted SHA-256 (lowercase hex, 64 chars) of the normalised value, the same
  formula as repopulse.spark.pii.hash_pii used for silver:
      sha256(salt || lower(strip(value)))
  NULL stays NULL. Non-text values are cast to varchar first (actor_id 1004 -> '1004').
  strip = leading/trailing space, tab, newline, vertical tab, form feed, carriage return.
  Masking the same value with the same salt gives the same hash, so masked keys still join.
-#}
{% macro mask_pii(expr) -%}
    case
        when {{ expr }} is null then null
        else sha256(
            {{ pii_hash_salt() }}
            || lower(trim(cast({{ expr }} as varchar), ' ' || chr(9) || chr(10) || chr(11) || chr(12) || chr(13)))
        )
    end
{%- endmacro %}


{#-
  pii_hash_salt(): SQL string literal of the salt.
  - PII_HASH_SALT set (any target): that salt (the same secret silver uses).
  - unset on target `ci`: the fixture salt var ci_pii_hash_salt of dbt_project.yml (not a secret;
    CI and tests only).
  - unset on any other target: compile error, never a silent default.
  The check only raises when executing, so `dbt parse` works without the secret.
-#}
{% macro pii_hash_salt() -%}
    {%- set salt = env_var('PII_HASH_SALT', '') -%}
    {%- if salt | trim == '' -%}
        {%- if target.name == 'ci' -%}
            {%- set salt = var('ci_pii_hash_salt') -%}
        {%- elif execute -%}
            {{ exceptions.raise_compiler_error(
                "PII_HASH_SALT is not set: mask_pii needs the PII salt on target '"
                ~ target.name ~ "' (only target 'ci' may use the fixture salt)"
            ) }}
        {%- endif -%}
    {%- endif -%}
    '{{ salt | replace("'", "''") }}'
{%- endmacro %}
