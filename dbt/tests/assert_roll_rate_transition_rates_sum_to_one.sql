-- Each (transition_month, from_bucket) row group in the roll-rate matrix is
-- a row-normalized probability distribution over to_bucket outcomes — it
-- must sum to 1 (within floating-point/rounding tolerance). This fails
-- (returns rows) if any group doesn't.
select
    transition_month,
    from_bucket,
    sum(transition_rate) as total_rate
from {{ ref('credit_risk__roll_rate_transition_matrix') }}
group by 1, 2
having abs(sum(transition_rate) - 1.0) > 0.01
