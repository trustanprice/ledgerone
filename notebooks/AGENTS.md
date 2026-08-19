# AGENTS.md — notebooks/

Fleet: [../AGENTS.md](../AGENTS.md)

## Purpose

Ad hoc, exploratory sanity checks — not part of the reproducible pipeline
and never run by `make all` or the Makefile. If a check here proves durably
useful, it belongs in `dbt/` as a schema test or singular test
(see [../dbt/AGENTS.md](../dbt/AGENTS.md)), not left living only in a notebook.

- **`01_event_sanity_checks.ipynb`** — inspects the raw `data/raw/*.parquet`
  files directly (event-type/direction counts, refund spot-checks, positive-
  amount check), then confirms the dbt-built ledger
  (`main_staging.stg_ledger_entries`) row count matches. The last cell
  assumes `dbt build` has already run — it reads the DuckDB file read-only
  rather than building anything itself.
- **`02_credit_risk_vintage_and_roll_rates.ipynb`** — plots vintage curves
  (cumulative charge-off rate by cohort x months-on-book), the roll-rate
  transition matrix as a heatmap, and the CECL-style reserve estimate, all
  read from `main_marts.credit_risk__*`. This is the credit-risk module's
  entire "visual" surface, by design — see
  [../dbt/models/credit_risk/AGENTS.md](../dbt/models/credit_risk/AGENTS.md):
  no dashboard app, a notebook is enough.
- **`03_credit_risk_validation_backtest.ipynb`** — a different kind of
  notebook from the other two: it doesn't read from dbt/DuckDB at all. It
  backtests the credit-risk module's *methodology* (not its synthetic
  data) against real Freddie Mac mortgage performance data
  (`data/external/freddie_mac/`, via `src/ingest_freddie_mac_validation_data.py`)
  with a genuine time-based train/holdout split. Teaching-style like the
  rest of this module (explains the *why* of each step, not just the
  code) — see the Validation section of
  [../dbt/models/credit_risk/README.md](../dbt/models/credit_risk/README.md)
  for the headline results and how to reproduce the manual data download
  this notebook depends on.

## Conventions

- Notebooks here are exploratory scratch work: it's fine for them to
  hardcode paths, print `.df()` output, and not be idempotent-by-design the
  way dbt models are.
- Don't add new ledger/warehouse-building logic in a notebook — that's dbt's
  job. Notebooks in this folder should only ever *read*.

## Connects to

- Reads `data/raw/*.parquet` (written by `src/generate_data.py`) and
  `data/processed/ledgerone.duckdb` (written by `dbt build`).
- `02_credit_risk_vintage_and_roll_rates.ipynb` additionally depends on
  `main_marts.credit_risk__*`, built from `data/raw/credit_risk/*.parquet`
  (written by `src/generate_credit_risk_data.py`).
- `03_credit_risk_validation_backtest.ipynb` depends on
  `data/external/freddie_mac/*.parquet` (written by
  `src/ingest_freddie_mac_validation_data.py`, from manually-downloaded raw
  files) — it's the one notebook in this folder that doesn't touch
  `data/processed/ledgerone.duckdb` at all.
