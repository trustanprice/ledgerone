-- Typed, one-posting-per-event ledger. This is the dbt-native replacement for
-- the old sql/ledger.sql script: same posting_type derivation (direction ->
-- CREDIT/DEBIT), but with a deterministic surrogate key instead of a random
-- uuid() so re-running the pipeline doesn't churn ledger_entry_id values.
with events as (
    select * from {{ ref('stg_events') }}
)

select
    {{ dbt.hash('event_id') }} as ledger_entry_id,
    event_id,
    event_ts,
    user_id,
    account_id,
    event_type,
    direction as posting_type,
    amount,
    currency,
    reference_id
from events
