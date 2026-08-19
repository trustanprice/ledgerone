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
├── AGENTS.md              # Fleet manager — map of the project for AI agents
├── Makefile                # `make all` — the one-command reproducible run
├── data/
│ ├── AGENTS.md
│ ├── raw/                  # Immutable raw events (gitignored, generated)
│ └── processed/            # ledgerone.duckdb — the entire warehouse
├── src/
│ ├── AGENTS.md
│ ├── generate_data.py      # Synthetic users/accounts/events -> data/raw/
│ └── generate_report.py    # Reads dbt marts -> reports/ (charts + CSV)
├── dbt/                     # The dbt project (staging -> star schema -> marts)
│ ├── AGENTS.md
│ ├── dbt_project.yml, profiles.yml
│ ├── models/staging/        # stg_* : typed, 1:1 with raw sources
│ ├── models/marts/core/     # dim_date, dim_customer, dim_account, fact_transactions
│ ├── models/marts/reporting/# One model per business question
│ ├── tests/                 # Singular (custom) tests
│ └── docs/lineage.md        # Static export of the dbt lineage graph
├── notebooks/
│ ├── AGENTS.md
│ └── 01_event_sanity_checks.ipynb
├── reports/                 # Output of generate_report.py — committed as evidence
│ ├── AGENTS.md
│ └── *.png, monthly_revenue_and_refund_rate.csv
├── README.md
└── requirements.txt

Each folder has its own `AGENTS.md` describing its purpose, conventions, and
how it connects to the rest of the project — start at the root
[AGENTS.md](AGENTS.md) if you're an agent working in this repo.

---

## How to Run

**One command**, from a fresh clone:

```
python -m venv venv && source venv/bin/activate
pip install -r requirements.txt
make all
```

`make all` runs the whole pipeline: generates synthetic data, builds and
tests the dbt warehouse, and produces the report output. See
[Phase 2: Analytics Layer](#phase-2-analytics-layer) below for what each
step actually does, or run the steps individually:

1. Generate synthetic data: `make generate` (`python src/generate_data.py`)
2. Build + test the warehouse: `make build` (`dbt build --project-dir dbt --profiles-dir dbt`)
3. Produce the report output: `make report` (`python src/generate_report.py`)

Ledger integrity is now validated by `dbt test` (`make test`), not a
standalone script — see below.

---

## Tech Stack

- **Python** – Synthetic data generation, reporting/charts
- **DuckDB** – Analytical warehouse (a single local file, no server)
- **dbt-core + dbt-duckdb** – Staging, dimensional modeling, marts, testing, docs
- **Parquet** – Columnar storage format for raw events
- **Matplotlib** – Lightweight reporting output (no BI tool)
- **VS Code** – Development environment
- **Git/GitHub** – Version control

This stack prioritizes simplicity, performance, and realism — no Airflow,
no cloud warehouse, no orchestration framework. Everything runs locally.

---

## Phase 2: Analytics Layer

Phase 2 adds a dbt project (`dbt/`) on top of the Phase 1 ledger — the kind
of scope an early-career analytics data engineer would actually own, not a
full production build-out. It replaced the old hand-written
`sql/*.sql` + `run_pipeline.py` + `ledger_validation.py` pipeline outright,
rather than running alongside it.

**Staging → dimensional → mart flow:**

1. **Staging** (`dbt/models/staging/`) — `stg_users`, `stg_accounts`,
   `stg_events` sit directly on the raw parquet (via a dbt source with
   `external_location`, no load step), just typed and renamed.
   `stg_ledger_entries` is the typed replacement for the old
   `sql/ledger.sql`: same CREDIT/DEBIT posting derivation, now with a
   deterministic surrogate key instead of a random `uuid()`.
2. **Star schema** (`dbt/models/marts/core/`) — one fact table,
   `fact_transactions` (one row per ledger posting), with foreign keys to
   three dimensions: `dim_date`, `dim_customer`, `dim_account`.
3. **Reporting marts** (`dbt/models/marts/reporting/`) — five single-purpose
   models, each answering one business question: monthly revenue & refund
   rate, net payout by period, top customers by transaction volume, average
   transaction value trend, and account running balance (the old
   `sql/daily_account_balances.sql`, rebuilt on the star schema).

**Testing** — `dbt test` (run as part of `dbt build`) is now the only
correctness gate. Every staging and mart model has `not_null` / `unique` /
`relationships` / `accepted_values` tests, plus two custom singular tests:
one asserting every REFUND balances exactly against the PURCHASE it
reverses (this is a single-entry ledger, so that's the one place a real
"balances" invariant applies — see `dbt/AGENTS.md` for why a global
credits=debits check would be wrong here), and one asserting all amounts
are strictly positive.

**Docs** — every model and key column has a description in `schema.yml`.
`dbt docs generate` produces the full catalog/manifest; since this
environment can't run a headless browser to screenshot the interactive
lineage UI, `dbt/docs/lineage.md` is a static Mermaid export of the same
dependency graph, generated from `manifest.json`.

**Reporting output** — `src/generate_report.py` queries the reporting marts
and writes three charts plus a CSV to `reports/` (committed to the repo as
evidence the pipeline produces something a stakeholder could actually look
at — see `reports/AGENTS.md`).

**Reproducibility** — `make all` runs the entire thing end to end: generate
synthetic data → `dbt build` (staging, star schema, marts, tests) →
generate report output. See [How to Run](#how-to-run).

Full project map: [AGENTS.md](AGENTS.md).

---

## Validation & Data Integrity

Ledger correctness is validated by `dbt test` / `dbt build` (`make test` /
`make build`), covering:

- Schema constraints (not-null, uniqueness, accepted values, foreign keys)
  across every staging and mart model
- Refund reversals — every REFUND balances exactly against its PURCHASE
- Positive amounts on every posting

Incorrect balances indicate upstream data or logic errors and are treated
as critical failures — `dbt build` fails loudly (non-zero exit) if any test
fails.

---

## Roadmap

### Phase 1 — Complete
- Synthetic event generation
- Ledger construction
- Balance validation

### Phase 2 — Complete
- dbt staging layer, schema + custom tests, star schema, business-question
  marts, docs, reporting output, reproducible one-command run. See
  [Phase 2: Analytics Layer](#phase-2-analytics-layer) above.

### Phase 3
- Business intelligence dashboards
- Executive and finance reporting

### Phase 4 (Optional, out of scope)
- Investment portfolio simulation
- Market data integration
- Risk and volatility analytics

---

## Credit Risk Practice Module (outside the roadmap)

`dbt/models/credit_risk/` is a separate, self-contained practice module —
**not** a numbered phase, and it doesn't touch Phase 3 or Phase 4 above.
It exists purely to build hands-on SQL/data-modeling reps for credit-risk /
loss-forecasting analyst interviews: a second synthetic domain (a consumer
credit/loan portfolio, generated the same seeded/deterministic way as the
e-commerce ledger) with its own staging layer and marts for **vintage
analysis** and **roll-rate / transition-matrix analysis**, plus a
simplified illustration of how a transition-matrix roll-forward becomes a
CECL-style reserve estimate.

Full writeup — the data, the technique behind each mart, and how it maps to
real CECL loss forecasting — is in
**[dbt/models/credit_risk/README.md](dbt/models/credit_risk/README.md)**,
written to double as interview review material. Module map:
[dbt/models/credit_risk/AGENTS.md](dbt/models/credit_risk/AGENTS.md).
Same stack, same `make all` — no separate command needed.

**The module also includes a real-data validation layer**: `notebooks/03_credit_risk_validation_backtest.ipynb`
backtests the vintage-curve/roll-rate methodology (not just the synthetic
numbers) against a real Freddie Mac mortgage panel with a genuine
time-based holdout — MAE of 0.19 percentage points on the cumulative
serious-delinquency forecast, with one real, explainable gap (the roll-
rate matrix isn't time-stable — see the README's Validation section for
the full result and why that's the interesting finding, not the headline
error number).

### "A day in the life" walkthrough

`apps/walkthrough/` is a static React scrollytelling site built on top of
the credit risk module above — 5 scenes walking through vintage curves,
the roll-rate transition matrix, and the CECL reserve forecast, each a
pinned chart beside scrolling narrative text. No backend: it reads static
JSON exported once from the dbt marts
(`src/export_credit_risk_walkthrough_data.py`), and deploys as a plain
static site — consistent with the rest of the project's no-orchestration,
no-cloud-services constraint. Live at
**https://trustanprice.github.io/ledgerone/walkthrough/**; see
[apps/walkthrough/README.md](apps/walkthrough/README.md) for how to run it
locally and regenerate its data.

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
