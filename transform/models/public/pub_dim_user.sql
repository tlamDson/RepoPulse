-- dim_user for the public role: user_key, actor_id and actor_login masked with mask_pii (salted
-- SHA-256, the silver formula). Same salt on every masked key, so public fct_events still joins
-- public dim_user on user_key. is_bot and the SCD2 columns are kept as-is.
select
    {{ mask_pii('user_key') }} as user_key,
    {{ mask_pii('actor_id') }} as actor_id,
    {{ mask_pii('actor_login') }} as actor_login,
    version_number,
    valid_from,
    valid_to,
    is_current,
    is_bot
from {{ ref('dim_user') }}
