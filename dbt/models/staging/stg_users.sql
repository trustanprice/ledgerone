with source as (
    select * from {{ source('raw', 'users') }}
)

select
    user_id,
    cast(signup_date as timestamp) as signup_date,
    user_type
from source
