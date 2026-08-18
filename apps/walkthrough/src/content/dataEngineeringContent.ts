// Static reference content for the Data Engineering tab. Copied by hand
// from the actual dbt model files at dbt/models/credit_risk/ — not
// generated from a live source, so if a model's SQL changes, update it
// here too. (Small, deliberate duplication: this app never touches dbt
// or DuckDB directly, per AGENTS.md.)

export const SQL_SNIPPETS = [
  {
    id: 'roll-rate',
    title: 'credit_risk__roll_rate_transition_matrix.sql',
    source: 'marts/',
    description:
      'The core exercise: LAG each account’s bucket by one month, pair it with this month’s bucket, count, and row-normalize.',
    sql: `with performance as (
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
    transition_month,
    from_bucket,
    to_bucket,
    account_count,
    round(account_count / sum(account_count) over (partition by transition_month, from_bucket), 4) as transition_rate
from counted
order by transition_month, from_bucket, to_bucket`,
  },
  {
    id: 'cecl-reserve',
    title: 'credit_risk__cecl_reserve_estimate.sql',
    source: 'marts/',
    description:
      'A WITH RECURSIVE Markov roll-forward: propagate a probability distribution through the transition matrix N months, read off charge-off mass.',
    sql: `with recursive

observed_transitions as (
    select from_bucket, to_bucket, sum(account_count) as account_count
    from {{ ref('credit_risk__roll_rate_transition_matrix') }}
    group by 1, 2
),

transition_probs as (
    select
        from_bucket,
        to_bucket,
        account_count / sum(account_count) over (partition by from_bucket) as transition_rate
    from observed_transitions

    union all

    -- 120+/Charged-Off is terminal and never observed as a from_bucket —
    -- model it as self-absorbing so probability mass is conserved.
    select '120+/Charged-Off', '120+/Charged-Off', cast(1.0 as double)
),

roll_forward (starting_bucket, horizon_month, state, probability) as (
    select distinct from_bucket, 0, from_bucket, cast(1.0 as double)
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
)

select starting_bucket, sum(probability) as prob_charge_off_within_horizon
from roll_forward
where state = '120+/Charged-Off'
  and horizon_month = {{ var('cecl_forecast_horizon_months') }}
group by 1
-- (joined to today's book by current_bucket in the full model —
-- see the repo for the complete query)`,
  },
  {
    id: 'vintage-curves',
    title: 'credit_risk__vintage_curves.sql',
    source: 'marts/',
    description:
      'Cumulative sums/ratios over a partitioned, ordered window — flags the first month each account crosses a severity threshold, then accumulates.',
    sql: `flagged as (
    select
        account_id,
        origination_quarter,
        months_on_book,
        case
            when delinquency_bucket in ('30-59 DPD','60-89 DPD','90-119 DPD','120+/Charged-Off')
             and months_on_book = min(case when delinquency_bucket in ('30-59 DPD','60-89 DPD','90-119 DPD','120+/Charged-Off') then months_on_book end)
                 over (partition by account_id)
            then 1 else 0
        end as is_first_30plus
        -- ...is_first_60plus, is_first_90plus follow the same pattern
    from performance
),

cumulative as (
    select
        origination_quarter,
        months_on_book,
        sum(new_30plus_accounts) over w as cumulative_30plus_accounts
        -- window named once, reused for every cumulative column
    from monthly
    window w as (
        partition by origination_quarter
        order by months_on_book
        rows between unbounded preceding and current row
    )
)`,
  },
]

export interface ModelDictEntry {
  model: string
  layer: string
  grain: string
  keys: string
  tests: string
}

export const DATA_DICTIONARY: ModelDictEntry[] = [
  {
    model: 'credit_risk__stg_loan_originations',
    layer: 'staging',
    grain: '1 row / account',
    keys: 'account_id (PK)',
    tests: 'unique, not_null, accepted_values(score_band), accepted_values(product_type)',
  },
  {
    model: 'credit_risk__stg_loan_performance',
    layer: 'staging',
    grain: '1 row / account / month',
    keys: 'performance_id (PK, hash surrogate) · account_id (FK)',
    tests: 'unique, not_null, relationships(account_id), accepted_values(delinquency_bucket)',
  },
  {
    model: 'credit_risk__vintage_curves',
    layer: 'mart',
    grain: '1 row / (origination_quarter, months_on_book)',
    keys: 'vintage_curve_id (PK, hash surrogate)',
    tests: 'unique, not_null on grain + rate columns',
  },
  {
    model: 'credit_risk__roll_rate_transition_matrix',
    layer: 'mart',
    grain: '1 row / (transition_month, from_bucket, to_bucket)',
    keys: 'transition_id (PK, hash surrogate)',
    tests: 'unique, not_null, accepted_values(from_bucket/to_bucket) + singular test (rates sum to 1)',
  },
  {
    model: 'credit_risk__cecl_reserve_estimate',
    layer: 'mart',
    grain: '1 row / current_bucket',
    keys: 'current_bucket (PK)',
    tests: 'unique, not_null, accepted_values(current_bucket)',
  },
  {
    model: 'credit_risk__portfolio_summary_by_score_band',
    layer: 'mart',
    grain: '1 row / origination_score_band',
    keys: 'origination_score_band (PK)',
    tests: 'unique, not_null, accepted_values(origination_score_band)',
  },
]

// Counted directly from dbt/models/credit_risk/**/_*.yml — 33 + 6 + 7 + 1
// schema tests + 1 singular test = 48, matching `dbt build --select
// credit_risk`'s own reported "48 data tests" exactly.
export const TEST_COVERAGE_SUMMARY = [
  { type: 'not_null', count: 33, note: 'Every join key and every grain/measure column across all 6 models.' },
  { type: 'unique', count: 6, note: 'Primary key of every staging and mart model.' },
  { type: 'accepted_values', count: 7, note: 'Every categorical field: delinquency_bucket, score_band, product_type, from/to_bucket, current_bucket.' },
  { type: 'relationships', count: 1, note: 'credit_risk__stg_loan_performance.account_id → credit_risk__stg_loan_originations.account_id.' },
  {
    type: 'singular (custom)',
    count: 1,
    note: 'assert_roll_rate_transition_rates_sum_to_one — every (transition_month, from_bucket) group’s rates sum to 1.0.',
  },
]
