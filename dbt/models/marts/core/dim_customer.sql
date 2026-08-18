select
    user_id as customer_key,
    user_id,
    signup_date,
    user_type
from {{ ref('stg_users') }}
