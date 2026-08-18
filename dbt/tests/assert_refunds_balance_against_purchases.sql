-- LedgerOne is a single-entry ledger: each event posts once against the
-- user's cash account (there's no offsetting platform-side account), so a
-- global "sum(credits) = sum(debits)" invariant doesn't hold by design —
-- deposits, purchases and fees are independent economic events, not
-- journal pairs. The one place a real balance invariant *does* apply is a
-- reversal: every REFUND must exactly offset the PURCHASE it references,
-- in opposite direction and equal amount. This test fails (returns rows)
-- if any refund doesn't balance against its purchase.
select
    refund.event_id as refund_event_id,
    refund.reference_id as purchase_event_id,
    refund.amount as refund_amount,
    purchase.amount as purchase_amount,
    refund.posting_type as refund_posting_type,
    purchase.posting_type as purchase_posting_type
from {{ ref('fact_transactions') }} as refund
left join {{ ref('fact_transactions') }} as purchase
    on refund.reference_id = purchase.event_id
    and purchase.event_type = 'PURCHASE'
where refund.event_type = 'REFUND'
  and (
      purchase.event_id is null
      or refund.amount != purchase.amount
      or refund.posting_type = purchase.posting_type
  )
