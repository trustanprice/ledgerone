# AGENTS.md — dbt/models/credit_risk/

Fleet: [../../../AGENTS.md](../../../AGENTS.md) · dbt project: [../../AGENTS.md](../../AGENTS.md)

## Purpose

A self-contained credit-risk practice module (vintage analysis, roll-rate /
transition-matrix modeling, a simplified CECL-style reserve estimate) —
built as interview prep, not a continuation of the main README's numbered
roadmap. **Not** the e-commerce ledger domain: this is a second, separate
synthetic dataset (a consumer credit/loan portfolio) with its own source,
own staging models, own marts. Nothing here joins to `stg_events`,
`fact_transactions`, or any other e-commerce-ledger model, and it never
should.

Full explanation of the data, the technique behind each mart, and how it
maps to real CECL loss forecasting: **[README.md](README.md)** — read that
first, it's written to double as interview review material.

**This module also has a validation/backtest layer that lives outside dbt
entirely**: `notebooks/03_credit_risk_validation_backtest.ipynb` backtests
the vintage-curve/roll-rate *methodology* against real Freddie Mac
mortgage data (`data/external/freddie_mac/`, manually downloaded — see
`data/AGENTS.md`), via `src/ingest_freddie_mac_validation_data.py`. It
does not read from or write to any dbt model or the DuckDB warehouse —
it's a separate, standalone validation of the technique, not of this
module's synthetic data. See README.md's "Validation" section for the
headline numbers and exactly how to reproduce the manual download.

## Layout

```
staging/
  _sources.yml                        — credit_risk source (external_location -> data/raw/credit_risk/*.parquet)
  _staging.yml                        — schema tests + descriptions
  credit_risk__stg_loan_originations.sql
  credit_risk__stg_loan_performance.sql
marts/
  _marts.yml                          — schema tests + descriptions
  credit_risk__vintage_curves.sql
  credit_risk__roll_rate_transition_matrix.sql
  credit_risk__cecl_reserve_estimate.sql
  credit_risk__portfolio_summary_by_score_band.sql
README.md                             — the interview-prep writeup
```

Materialization follows the same convention as the e-commerce domain:
staging = view, marts = table (`dbt_project.yml`'s `models.ledgerone.credit_risk`
block). Same custom schemas too (`main_staging`, `main_marts`) — separation
from the ledger domain comes from the `credit_risk__` naming prefix, not a
different schema.

## Conventions specific to this module

- **Naming prefix is mandatory**: every model in this folder is
  `credit_risk__<name>`, even staging models, so nothing collides with the
  e-commerce domain's objects in the shared `main_staging`/`main_marts`
  schemas.
- **Composite grains get a surrogate key column**: `credit_risk__stg_loan_performance`,
  `credit_risk__vintage_curves`, and `credit_risk__roll_rate_transition_matrix`
  all have a natural key spanning multiple columns, so each adds a
  `{{ dbt.hash(...) }}` surrogate key (same pattern as the ledger domain's
  `stg_ledger_entries.ledger_entry_id`) so they can carry a plain
  `unique`/`not_null` test without a dbt_utils dependency — see
  [../../AGENTS.md](../../AGENTS.md) for why this project avoids dbt
  packages entirely.
- **Delinquency bucket transitions are a Markov chain, not derived from
  balance/payment fields** — `statement_balance`/`payment_amount` are
  descriptive only. Don't write logic (in a model or a test) that assumes
  the bucket can be inferred from the balance; it can't, by design.
- **120+/Charged-Off is absorbing and never a `from_bucket`** in the raw
  performance data — the generator stops emitting rows once an account
  charges off. `credit_risk__cecl_reserve_estimate` has to add that
  self-loop back explicitly to conserve probability mass in its roll-
  forward; see its header comment and README.md for why.
- **Watch DuckDB numeric literal typing in recursive CTEs**: a bare `1.0`
  literal gets inferred as `DECIMAL(2,1)`, not `DOUBLE`, silently wrecking
  precision through repeated multiplication. Always `CAST(... AS DOUBLE)`
  literals used as the seed of an iterative/recursive calculation. This
  bit the original build of `credit_risk__cecl_reserve_estimate` — see
  README.md's "gotcha" section for the full story.

## Connects to

- Reads `data/raw/credit_risk/*.parquet`, written by
  `src/generate_credit_risk_data.py` (see [../../../src/AGENTS.md](../../../src/AGENTS.md)).
- Read (for eyeballing only, no dashboard) by
  `notebooks/02_credit_risk_vintage_and_roll_rates.ipynb`.
- `make generate` / `make build` / `make all` at the repo root already
  cover this module — no separate command exists or is needed.
- **Not** connected to `notebooks/03_credit_risk_validation_backtest.ipynb`
  — that notebook validates the same *methodology* independently, against
  real external data, and never reads these dbt models or their output.
