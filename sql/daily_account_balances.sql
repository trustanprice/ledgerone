CREATE OR REPLACE TABLE daily_account_balances AS
WITH daily_net AS (
  SELECT
    account_id,
    CAST(event_ts AS DATE) AS dt,
    SUM(CASE WHEN posting_type = 'CREDIT' THEN amount ELSE -amount END) AS net_change
  FROM ledger_entries
  GROUP BY 1, 2
),
running AS (
  SELECT
    account_id,
    dt,
    SUM(net_change) OVER (
      PARTITION BY account_id
      ORDER BY dt
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS balance
  FROM daily_net
)
SELECT * FROM running;
