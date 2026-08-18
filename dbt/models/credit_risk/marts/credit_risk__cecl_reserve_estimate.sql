-- Business question: given the accounts on the book right now, and the
-- roll-rate behavior observed historically, how much do we expect to lose
-- to charge-off over the next {{ var('cecl_forecast_horizon_months') }}
-- months — and what reserve rate does that imply against outstanding
-- balances? This is a simplified illustration of the roll-rate / Markov-
-- chain method used in real CECL reserve models: propagate today's book,
-- bucket by bucket, forward through the historical transition matrix, and
-- read off the probability mass that lands in charge-off by the horizon.
-- This is a practice exercise, not a production model — see
-- dbt/models/credit_risk/README.md for the full writeup of what a real
-- model adds on top (segmentation, macro overlays, discounting, etc).
--
-- Grain: one row per current delinquency bucket (a small reserve schedule
-- by risk segment, the way a real CECL reserve is presented).
with recursive

observed_transitions as (
    select
        from_bucket,
        to_bucket,
        sum(account_count) as account_count
    from {{ ref('credit_risk__roll_rate_transition_matrix') }}
    group by 1, 2
),

-- Row-normalize into a single blended matrix (average behavior across the
-- whole observation window, not month-specific).
transition_probs as (
    select
        from_bucket,
        to_bucket,
        account_count / sum(account_count) over (partition by from_bucket) as transition_rate
    from observed_transitions

    union all

    -- 120+/Charged-Off is terminal: the generator never writes a row after
    -- an account charges off, so it's never observed as a from_bucket
    -- above. Model it explicitly as self-absorbing (stays charged off with
    -- probability 1) so probability mass is conserved when rolling forward.
    select '120+/Charged-Off', '120+/Charged-Off', cast(1.0 as double)
),

-- Roll a probability distribution forward month by month, once per
-- possible starting bucket, via a small Markov chain matrix multiply at
-- each step: probability(state at month m+1) = sum over prior states of
-- probability(prior state at month m) * transition_rate(prior -> state).
roll_forward (starting_bucket, horizon_month, state, probability) as (
    select distinct
        from_bucket as starting_bucket,
        0 as horizon_month,
        from_bucket as state,
        cast(1.0 as double) as probability
    from transition_probs

    union all

    select
        rf.starting_bucket,
        rf.horizon_month + 1,
        tp.to_bucket,
        rf.probability * tp.transition_rate
    from roll_forward rf
    join transition_probs tp on tp.from_bucket = rf.state
    where rf.horizon_month < {{ var('cecl_forecast_horizon_months') }}
),

horizon_charge_off_prob as (
    select
        starting_bucket,
        sum(probability) as prob_charge_off_within_horizon
    from roll_forward
    where state = '120+/Charged-Off'
      and horizon_month = {{ var('cecl_forecast_horizon_months') }}
    group by 1
),

-- "Current book": latest performance snapshot per account, excluding
-- accounts that have already charged off (their loss is already realized,
-- not a reserve forecast question).
latest_snapshot as (
    select
        account_id,
        delinquency_bucket,
        statement_balance,
        row_number() over (partition by account_id order by months_on_book desc) as rn
    from {{ ref('credit_risk__stg_loan_performance') }}
),

current_book as (
    select account_id, delinquency_bucket as current_bucket, statement_balance
    from latest_snapshot
    where rn = 1
      and delinquency_bucket != '120+/Charged-Off'
)

select
    cb.current_bucket,
    count(*) as account_count,
    round(sum(cb.statement_balance), 2) as outstanding_balance,
    round(h.prob_charge_off_within_horizon, 4) as prob_charge_off_within_horizon,
    round(sum(cb.statement_balance * h.prob_charge_off_within_horizon), 2) as expected_loss_amount,
    round(sum(cb.statement_balance * h.prob_charge_off_within_horizon) / nullif(sum(cb.statement_balance), 0), 4) as reserve_rate
from current_book cb
join horizon_charge_off_prob h on cb.current_bucket = h.starting_bucket
group by 1, h.prob_charge_off_within_horizon
order by case cb.current_bucket
    when 'Current' then 1
    when '30-59 DPD' then 2
    when '60-89 DPD' then 3
    when '90-119 DPD' then 4
    else 5
end
