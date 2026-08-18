-- Business question: for accounts originated in quarter X, what share have
-- EVER reached 30+/60+/90+ DPD, or charged off, by month-on-book N? This is
-- classic vintage curve analysis — cohort x MOB, cumulative.
--
-- Grain: one row per (origination_quarter, months_on_book).
--
-- Note on censoring: later MOB values only have data from earlier cohorts
-- (a 2024-Q4 cohort can't have 30 months of history yet as of the current
-- observation window) — that's normal right-censoring in vintage analysis,
-- not a data quality issue. Compare cohorts at the same MOB, not the same
-- calendar month.
with performance as (
    select
        p.account_id,
        p.months_on_book,
        p.delinquency_bucket,
        p.charge_off_flag,
        o.origination_quarter
    from {{ ref('credit_risk__stg_loan_performance') }} p
    join {{ ref('credit_risk__stg_loan_originations') }} o
        on p.account_id = o.account_id
),

cohort_sizes as (
    select
        origination_quarter,
        count(distinct account_id) as cohort_accounts
    from {{ ref('credit_risk__stg_loan_originations') }}
    group by 1
),

-- Flag the single month each account FIRST reaches each severity threshold,
-- using a window MIN to find that month and comparing it to the current row.
flagged as (
    select
        account_id,
        origination_quarter,
        months_on_book,
        charge_off_flag,
        case
            when delinquency_bucket in ('30-59 DPD', '60-89 DPD', '90-119 DPD', '120+/Charged-Off')
             and months_on_book = min(case when delinquency_bucket in ('30-59 DPD', '60-89 DPD', '90-119 DPD', '120+/Charged-Off') then months_on_book end)
                 over (partition by account_id)
            then 1 else 0
        end as is_first_30plus,
        case
            when delinquency_bucket in ('60-89 DPD', '90-119 DPD', '120+/Charged-Off')
             and months_on_book = min(case when delinquency_bucket in ('60-89 DPD', '90-119 DPD', '120+/Charged-Off') then months_on_book end)
                 over (partition by account_id)
            then 1 else 0
        end as is_first_60plus,
        case
            when delinquency_bucket in ('90-119 DPD', '120+/Charged-Off')
             and months_on_book = min(case when delinquency_bucket in ('90-119 DPD', '120+/Charged-Off') then months_on_book end)
                 over (partition by account_id)
            then 1 else 0
        end as is_first_90plus
    from performance
),

monthly as (
    select
        origination_quarter,
        months_on_book,
        sum(is_first_30plus) as new_30plus_accounts,
        sum(is_first_60plus) as new_60plus_accounts,
        sum(is_first_90plus) as new_90plus_accounts,
        sum(case when charge_off_flag then 1 else 0 end) as new_charge_off_accounts
    from flagged
    group by 1, 2
),

cumulative as (
    select
        origination_quarter,
        months_on_book,
        sum(new_30plus_accounts) over w as cumulative_30plus_accounts,
        sum(new_60plus_accounts) over w as cumulative_60plus_accounts,
        sum(new_90plus_accounts) over w as cumulative_90plus_accounts,
        sum(new_charge_off_accounts) over w as cumulative_charge_off_accounts
    from monthly
    window w as (partition by origination_quarter order by months_on_book rows between unbounded preceding and current row)
)

select
    {{ dbt.hash("c.origination_quarter || '-' || cast(c.months_on_book as varchar)") }} as vintage_curve_id,
    c.origination_quarter,
    c.months_on_book,
    s.cohort_accounts,
    c.cumulative_30plus_accounts,
    c.cumulative_60plus_accounts,
    c.cumulative_90plus_accounts,
    c.cumulative_charge_off_accounts,
    round(c.cumulative_30plus_accounts / s.cohort_accounts, 4) as cumulative_30plus_rate,
    round(c.cumulative_60plus_accounts / s.cohort_accounts, 4) as cumulative_60plus_rate,
    round(c.cumulative_90plus_accounts / s.cohort_accounts, 4) as cumulative_90plus_rate,
    round(c.cumulative_charge_off_accounts / s.cohort_accounts, 4) as cumulative_charge_off_rate
from cumulative c
join cohort_sizes s on c.origination_quarter = s.origination_quarter
order by 1, 2
