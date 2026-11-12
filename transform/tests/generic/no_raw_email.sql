{#-
  no_raw_email: no value of the column (cast to string, so lists and structs too) contains an
  email address. Attached to every column labelled other than pii: none in gold and public.
  The pattern is repopulse.governance.EMAIL_PATTERN (tests/test_governance.py keeps them equal);
  regex_contains (macros/cross_db.sql) writes it as a literal for each engine.
  Failing rows carry no value (1 as failing_value).
-#}
{% test no_raw_email(model, column_name) %}
{%- set email_pattern -%}
[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}
{%- endset %}

select 1 as failing_value
from {{ model }}
where {{ column_name }} is not null
  and {{ regex_contains('cast(' ~ column_name ~ ' as string)', email_pattern | trim) }}

{% endtest %}
