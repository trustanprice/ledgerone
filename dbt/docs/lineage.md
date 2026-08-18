# LedgerOne dbt lineage

Generated from `target/manifest.json` after `dbt docs generate` (dbt 1.8.9 / dbt-duckdb 1.8.4).
This is a static export of the same DAG the interactive `dbt docs serve` lineage graph shows —
regenerate it any time with `dbt docs generate` and re-run the extraction snippet in the repo README
if models change.

```mermaid
graph LR
  subgraph Raw sources
    events[raw.events]
    users[raw.users]
    accounts[raw.accounts]
  end

  subgraph Staging
    stg_events[stg_events]
    stg_users[stg_users]
    stg_accounts[stg_accounts]
    stg_ledger_entries[stg_ledger_entries]
  end

  subgraph "Marts: core (star schema)"
    dim_date[dim_date]
    dim_customer[dim_customer]
    dim_account[dim_account]
    fact_transactions[fact_transactions]
  end

  subgraph "Marts: reporting"
    monthly_revenue[monthly_revenue_and_refund_rate]
    net_payout[net_payout_by_period]
    top_customers[top_customers_by_transaction_volume]
    avg_txn_value[avg_transaction_value_trend]
    running_balance[account_running_balance]
  end

  events --> stg_events
  users --> stg_users
  accounts --> stg_accounts

  stg_events --> stg_ledger_entries
  stg_ledger_entries --> dim_date
  stg_ledger_entries --> fact_transactions
  stg_users --> dim_customer
  stg_accounts --> dim_account

  fact_transactions --> monthly_revenue
  fact_transactions --> net_payout
  fact_transactions --> top_customers
  fact_transactions --> avg_txn_value
  fact_transactions --> running_balance

  dim_date --> monthly_revenue
  dim_date --> net_payout
  dim_date --> avg_txn_value
  dim_date --> running_balance
  dim_customer --> top_customers
```
