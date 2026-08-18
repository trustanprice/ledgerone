with source as (
    select * from {{ source('raw', 'accounts') }}
)

select
    account_id,
    user_id,
    account_type,
    currency
from source
