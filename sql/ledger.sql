-- sql/ledger.sql
-- Materialize ledger entries into DuckDB (persisted data)

CREATE OR REPLACE TABLE ledger_entries AS
SELECT
  uuid() AS ledger_entry_id,
  event_id,
  event_ts,
  user_id,
  account_id,
  event_type,
  direction AS posting_type,  -- CREDIT / DEBIT
  amount,
  currency,
  reference_id
FROM read_parquet('/Users/trustanprice/Desktop/Personal/ledgerone/data/raw/events.parquet');
