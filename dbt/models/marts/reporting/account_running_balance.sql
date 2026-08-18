-- Business question: what is each account's balance at the end of each day
-- it had activity? Replaces the old sql/daily_account_balances.sql script.
select
    f.account_key,
    d.date_day,
    sum(f.signed_amount) over (
        partition by f.account_key
        order by d.date_day
        rows between unbounded preceding and current row
    ) as running_balance
from {{ ref('fact_transactions') }} f
join {{ ref('dim_date') }} d on f.date_key = d.date_key
qualify row_number() over (partition by f.account_key, d.date_day order by f.event_ts desc) = 1
order by f.account_key, d.date_day
