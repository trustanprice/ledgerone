-- Business question: how much did we make each month, and what share of it
-- came back as refunds?
select
    d.month_start,
    sum(case when f.event_type = 'PURCHASE' then f.amount else 0 end) as gross_revenue,
    sum(case when f.event_type = 'REFUND' then f.amount else 0 end) as refunds,
    sum(case when f.event_type = 'FEE' then f.amount else 0 end) as fee_revenue,
    sum(case when f.event_type = 'PURCHASE' then f.amount else 0 end)
        - sum(case when f.event_type = 'REFUND' then f.amount else 0 end) as net_revenue,
    round(
        sum(case when f.event_type = 'REFUND' then f.amount else 0 end)
        / nullif(sum(case when f.event_type = 'PURCHASE' then f.amount else 0 end), 0),
        4
    ) as refund_rate
from {{ ref('fact_transactions') }} f
join {{ ref('dim_date') }} d on f.date_key = d.date_key
group by 1
order by 1
