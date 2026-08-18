with source as (
    select * from {{ source('credit_risk', 'loan_originations') }}
)

select
    account_id,
    cast(origination_date as date) as origination_date,
    date_trunc('quarter', cast(origination_date as date))::date as origination_quarter,
    origination_score_band,
    cast(credit_limit as double) as credit_limit,
    cast(apr as double) as apr,
    product_type
from source
