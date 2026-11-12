{#-
  hashed_pii: every non-NULL value (every non-NULL element when is_array) must be a salted
  SHA-256 as written by silver / mask_pii: exactly 64 lowercase hex chars. Catches a silver job
  that forgot to hash (raw email or name in a *_hashes column) and a public column left unmasked.
  Failing rows carry no value (1 as failing_value), so stored failures never copy PII.
  string_array_elements and regex_full_match are cross-database macros (macros/cross_db.sql).
-#}
{% test hashed_pii(model, column_name, is_array=false) %}

{%- if is_array %}
with elements as (
    {{ string_array_elements(model, column_name) }}
)

select 1 as failing_value
from elements
where element is not null
  and not {{ regex_full_match('cast(element as string)', '[0-9a-f]{64}') }}
{%- else %}
select 1 as failing_value
from {{ model }}
where {{ column_name }} is not null
  and not {{ regex_full_match('cast(' ~ column_name ~ ' as string)', '[0-9a-f]{64}') }}
{%- endif %}

{% endtest %}
