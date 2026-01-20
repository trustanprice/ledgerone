CREATE OR REPLACE TABLE monthly_revenue AS
SELECT
  DATE_TRUNC('month', event_ts) AS month,
  SUM(CASE WHEN event_type = 'PURCHASE' THEN amount ELSE 0 END) AS gross_revenue,
  SUM(CASE WHEN event_type = 'REFUND' THEN amount ELSE 0 END) AS refunds,
  SUM(CASE WHEN event_type = 'FEE' THEN amount ELSE 0 END) AS fee_revenue,
  SUM(CASE WHEN event_type = 'PURCHASE' THEN amount ELSE 0 END)
    - SUM(CASE WHEN event_type = 'REFUND' THEN amount ELSE 0 END) AS net_revenue
FROM fact_transactions
GROUP BY 1
ORDER BY 1;
