-- Business question: is the average purchase size growing or shrinking over time?
select
    d.month_start,
    count(*) as purchase_count,
    round(avg(f.amount), 2) as avg_purchase_amount
from {{ ref('fact_transactions') }} f
join {{ ref('dim_date') }} d on f.date_key = d.date_key
where f.event_type = 'PURCHASE'
group by 1
order by 1
