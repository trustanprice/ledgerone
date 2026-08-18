-- Grain: one row per financial event / ledger posting.
-- Note: this is a single-entry ledger (each event produces exactly one
-- posting against the user's cash account), not full double-entry
-- bookkeeping — there is no offsetting platform-side account. amount is
-- always positive; signed_amount applies CREDIT/DEBIT direction so it can
-- be summed directly to get net cash movement.
select
    l.ledger_entry_id,
    l.event_id,
    l.event_ts,
    cast(strftime(cast(l.event_ts as date), '%Y%m%d') as integer) as date_key,
    l.user_id as customer_key,
    l.account_id as account_key,
    l.event_type,
    l.posting_type,
    l.amount,
    case when l.posting_type = 'CREDIT' then l.amount else -l.amount end as signed_amount,
    l.currency,
    l.reference_id
from {{ ref('stg_ledger_entries') }} l
