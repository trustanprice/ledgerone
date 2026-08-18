-- Business question: month over month, what fraction of accounts in each
-- delinquency bucket roll forward, cure, or stay put? This is the roll-rate
-- transition matrix — the core technique behind CECL loss forecasting.
--
-- Grain: one row per (transition_month, from_bucket, to_bucket). To get a
-- single blended matrix across all months (what credit_risk__cecl_reserve_
-- estimate does), just re-aggregate account_count over from_bucket/to_bucket
-- without transition_month and re-normalize.
--
-- Approach: LAG each account's delinquency_bucket by one month (ordered by
-- months_on_book, which is gap-free for an active account by construction
-- — see dbt/models/credit_risk/README.md), pair it with this month's
-- bucket, then count and row-normalize within each (transition_month,
-- from_bucket) group.
with performance as (
    select
        account_id,
        performance_month,
        months_on_book,
        delinquency_bucket,
        lag(delinquency_bucket) over (partition by account_id order by months_on_book) as prior_bucket
    from {{ ref('credit_risk__stg_loan_performance') }}
),

transitions as (
    select
        performance_month as transition_month,
        prior_bucket as from_bucket,
        delinquency_bucket as to_bucket
    from performance
    where prior_bucket is not null
),

counted as (
    select
        transition_month,
        from_bucket,
        to_bucket,
        count(*) as account_count
    from transitions
    group by 1, 2, 3
)

select
    {{ dbt.hash("cast(transition_month as varchar) || '-' || from_bucket || '-' || to_bucket") }} as transition_id,
    transition_month,
    from_bucket,
    to_bucket,
    account_count,
    round(account_count / sum(account_count) over (partition by transition_month, from_bucket), 4) as transition_rate
from counted
order by transition_month, from_bucket, to_bucket
