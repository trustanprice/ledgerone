with source as (
    select * from {{ source('raw', 'events') }}
)

select
    event_id,
    cast(event_ts as timestamp) as event_ts,
    user_id,
    account_id,
    event_type,
    direction,
    cast(amount as double) as amount,
    currency,
    reference_id
from source
