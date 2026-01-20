# LedgerOne  
### A Synthetic FinTech E-Commerce & Ledger Analytics Platform

---

## Overview

**LedgerOne** is an end-to-end fintech analytics project that simulates how modern financial platforms generate, store, and analyze transactional data. The system is built around a **ledger-first architecture**, where all financial activity is represented as immutable events and balances are derived from those events rather than stored directly.

This project is designed to mirror how fintech companies operate internally, from transaction pipelines and accounting logic to analytics-ready data warehousing and business intelligence.

---

## Project Goals

The primary goals of LedgerOne are to:

- Understand how fintech platforms model money movement  
- Build a correct, append-only financial ledger  
- Practice event-driven data engineering  
- Design an analytics-ready warehouse  
- Develop intuition for finance, accounting, and fintech metrics  
- Create a portfolio-ready project that reflects real corporate systems  

---

## Core Principles

LedgerOne follows several non-negotiable financial system principles:

- **Immutability**: Financial events are never edited or deleted  
- **Append-Only Ledger**: All financial truth lives in ledger entries  
- **Derived Balances**: Account balances are calculated, not stored  
- **Explicit Direction**: All money movement is recorded as a CREDIT or DEBIT  
- **Event-Driven Design**: User actions produce financial events that flow through the system  

---

## Financial Event Model

All financial activity in LedgerOne begins as an event.

### Supported Event Types (MVP)

- `DEPOSIT` – Money entering a cash account  
- `PURCHASE` – User spending on a product or subscription  
- `FEE` – Platform fees charged to a user  
- `REFUND` – Reversal of a prior purchase  
- `INVEST` – Movement of cash into an investment account (future phase)  

### Event Schema (Raw)

| Column | Description |
|------|------------|
| event_id | Unique identifier |
| event_ts | Timestamp of the event |
| user_id | User associated with the event |
| account_id | Account impacted |
| event_type | Type of financial event |
| direction | CREDIT or DEBIT |
| amount | Positive numeric amount |
| currency | ISO currency code |
| reference_id | Links related events (refunds, fees) |

Raw events are treated as the **source of truth** and are never modified.

---

## Ledger Architecture

Raw events are transformed into a canonical ledger table.

### Ledger Entries

Each ledger entry represents a single financial posting.

| Column | Description |
|------|------------|
| ledger_entry_id | Unique identifier |
| event_id | Source event |
| event_ts | Timestamp |
| user_id | User |
| account_id | Account |
| posting_type | CREDIT or DEBIT |
| amount | Numeric value |

### Balance Calculation

Account balances are derived using:

SUM(
  CASE
    WHEN posting_type = 'CREDIT' THEN amount
    ELSE -amount
  END
)

Balances are never stored directly.

---

## Data Flow

The system follows a clear, production-style data flow:

1. Synthetic user actions generate raw financial events  
2. Events are written to the raw data layer  
3. Events are transformed into ledger entries  
4. Ledger entries feed analytics-ready fact tables  
5. Metrics and dashboards are built on top of the warehouse  

---

## Project Structure

ledgerone/
├── data/
│ ├── raw/ # Immutable raw events
│ └── processed/ # Ledger & analytics outputs
├── src/
│ └── generate_data.py
│ └── ledger_validation.py
│ └── run_pipeline.py
├── sql/
│ └── ledger.sql
│ └── fact_transactions.sql
│ └── monthly_revenue.sql
│ └── daily_account_balances.sql
├── notebooks/
│ └── 01_event_sanity_checks.ipynb
├── README.md
└── requirements.txt

---

## How to Run

1. Create a virtual environment and install dependencies.
2. Generate synthetic data:
   - `python src/generate_data.py`
3. Build the warehouse:
   - `python src/run_pipeline.py`
4. Validate ledger integrity:
   - `python src/ledger_validation.py`

---

## Tech Stack

- **Python** – Synthetic data generation  
- **DuckDB** – Analytical warehouse  
- **SQL** – Ledger and analytics logic  
- **Parquet** – Columnar storage format  
- **VS Code** – Development environment  
- **Git/GitHub** – Version control  

This stack prioritizes simplicity, performance, and realism.

---

## Metrics & Analytics (Planned)

LedgerOne is designed to support common fintech metrics such as:

- Net deposits  
- Gross vs net revenue  
- Fee revenue  
- Active users  
- Average account balance  
- Churn proxies  
- Portfolio value (future phase)  

---

## Validation & Data Integrity

Ledger correctness is validated through:

- Balance consistency checks  
- Refund reversals  
- Revenue reconciliation  
- Account-level rollups  

Incorrect balances indicate upstream data or logic errors and are treated as critical failures.

---

## Roadmap

### Phase 1 (Current)
- Synthetic event generation  
- Ledger construction  
- Balance validation  

### Phase 2
- Analytics warehouse modeling  
- Fact and dimension tables  
- Core financial metrics  

### Phase 3
- Business intelligence dashboards  
- Executive and finance reporting  

### Phase 4 (Optional)
- Investment portfolio simulation  
- Market data integration  
- Risk and volatility analytics  

---

## Why Synthetic Data?

Real fintech transaction data is proprietary and unavailable publicly. LedgerOne uses **synthetic but realistic data** to accurately reflect how production systems behave while avoiding privacy and compliance issues.

This approach mirrors how fintech teams prototype, test, and validate systems internally.

---

## Author

Built by **Trustan Price** as a portfolio-grade fintech analytics project focused on financial systems, data engineering, and business intelligence.

---

## Disclaimer

LedgerOne is a simulated system for educational and portfolio purposes only. It does not represent real financial accounts or transactions.
