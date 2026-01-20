CREATE OR REPLACE TABLE fact_transactions AS
SELECT
  event_id,
  event_ts,
  user_id,
  account_id,
  event_type,
  posting_type,
  amount,
  currency,
  reference_id
FROM ledger_entries
WHERE event_type IN ('DEPOSIT', 'PURCHASE', 'FEE', 'REFUND');
