-- Grain: one row per account per month. performance_id is a deterministic
-- surrogate key (same dbt.hash() pattern used by stg_ledger_entries in the
-- e-commerce domain) so the composite grain can carry a standard
-- unique/not_null test without a dbt_utils dependency.
with source as (
    select * from {{ source('credit_risk', 'loan_performance') }}
)

select
    {{ dbt.hash("account_id || '-' || cast(months_on_book as varchar)") }} as performance_id,
    account_id,
    cast(performance_month as date) as performance_month,
    cast(months_on_book as integer) as months_on_book,
    cast(statement_balance as double) as statement_balance,
    cast(payment_amount as double) as payment_amount,
    delinquency_bucket,
    cast(charge_off_flag as boolean) as charge_off_flag
from source
