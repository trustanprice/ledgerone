-- Business question: how much cash actually moved through customer accounts
-- each month? (Distinct from revenue: this is total credits minus total
-- debits across every event type, i.e. the platform's cash-flow view rather
-- than its revenue view.)
select
    d.month_start,
    sum(case when f.posting_type = 'CREDIT' then f.amount else 0 end) as total_credits,
    sum(case when f.posting_type = 'DEBIT' then f.amount else 0 end) as total_debits,
    sum(f.signed_amount) as net_payout
from {{ ref('fact_transactions') }} f
join {{ ref('dim_date') }} d on f.date_key = d.date_key
group by 1
order by 1
