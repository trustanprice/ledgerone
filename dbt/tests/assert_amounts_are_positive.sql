-- Every ledger posting should carry a strictly positive amount; direction
-- (CREDIT/DEBIT) is what encodes sign, not the amount itself.
select ledger_entry_id, event_id, amount
from {{ ref('fact_transactions') }}
where amount <= 0
