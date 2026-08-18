-- Business question: who are our highest-volume customers by spend?
-- (No merchant/product entity exists in this event model, so this is
-- scoped to customers, as fact_transactions supports.)
select
    c.customer_key,
    c.user_type,
    count(*) filter (where f.event_type = 'PURCHASE') as purchase_count,
    sum(case when f.event_type = 'PURCHASE' then f.amount else 0 end) as total_purchase_amount
from {{ ref('fact_transactions') }} f
join {{ ref('dim_customer') }} c on f.customer_key = c.customer_key
group by 1, 2
having count(*) filter (where f.event_type = 'PURCHASE') > 0
order by total_purchase_amount desc
