select
    account_id as account_key,
    account_id,
    user_id as customer_key,
    account_type,
    currency
from {{ ref('stg_accounts') }}
