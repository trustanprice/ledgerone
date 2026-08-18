-- Business question: how does portfolio composition and performance vary
-- by the credit score band we underwrote at origination? A quick risk
-- cross-cut analysts pull constantly.
with latest_snapshot as (
    select
        account_id,
        delinquency_bucket,
        statement_balance,
        charge_off_flag,
        row_number() over (partition by account_id order by months_on_book desc) as rn
    from {{ ref('credit_risk__stg_loan_performance') }}
),

latest as (
    select * from latest_snapshot where rn = 1
)

select
    o.origination_score_band,
    count(distinct o.account_id) as account_count,
    round(sum(l.statement_balance), 2) as outstanding_balance,
    sum(case when l.delinquency_bucket in ('30-59 DPD', '60-89 DPD', '90-119 DPD') then 1 else 0 end) as currently_delinquent_accounts,
    round(
        sum(case when l.delinquency_bucket in ('30-59 DPD', '60-89 DPD', '90-119 DPD') then 1 else 0 end)
        / count(distinct o.account_id),
        4
    ) as current_delinquency_rate,
    sum(case when l.charge_off_flag then 1 else 0 end) as charged_off_accounts,
    round(sum(case when l.charge_off_flag then 1 else 0 end) / count(distinct o.account_id), 4) as lifetime_charge_off_rate,
    round(avg(o.apr), 2) as avg_apr
from {{ ref('credit_risk__stg_loan_originations') }} o
join latest l on o.account_id = l.account_id
group by 1
order by 1
