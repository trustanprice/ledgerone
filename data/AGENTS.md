# AGENTS.md — data/

Fleet: [../AGENTS.md](../AGENTS.md)

## Purpose

Everything here is generated or externally sourced, not authored, and
gitignored (`.gitignore`: `data/raw/`, `data/processed/*.duckdb`,
`data/external/`). A fresh clone has no `data/` directory at all until you
run the pipeline (or, for `external/`, do the one-time manual download
described below).

- **`raw/`** — `users.parquet`, `accounts.parquet`, `events.parquet`, written
  by `src/generate_data.py`. Treated as the immutable source of truth for
  everything downstream (dbt sources read these files directly, with no
  load/seed step).
- **`raw/credit_risk/`** — `loan_originations.parquet`, `loan_performance.parquet`,
  written by `src/generate_credit_risk_data.py`. A separate synthetic
  domain (consumer credit portfolio, not e-commerce) for the credit-risk
  practice module — see
  [../dbt/models/credit_risk/AGENTS.md](../dbt/models/credit_risk/AGENTS.md).
- **`external/freddie_mac/`** — real (not synthetic) data: Freddie Mac's
  Single-Family Loan-Level Dataset, 2021 vintage sample (50,000 loans),
  used by `notebooks/03_credit_risk_validation_backtest.ipynb` and
  `src/export_credit_risk_walkthrough_data.py` to backtest the
  credit-risk module's methodology. **Not reproducible by a
  script** — Freddie Mac requires manual registration at their Clarity
  Data Intelligence portal before download. Two raw pipe-delimited `.txt`
  files (`sample_orig_2021.txt`, `sample_perf_2021.txt`) plus two derived
  `.parquet` files written by `src/ingest_freddie_mac_validation_data.py`.
  See the Validation section of
  [../dbt/models/credit_risk/README.md](../dbt/models/credit_risk/README.md)
  for exactly how to get these files if they're missing.
- **`processed/ledgerone.duckdb`** — the entire warehouse, built by
  `dbt build` from `dbt/`. Contains the `main_staging` and `main_marts`
  schemas.

## Conventions

- Never hand-edit or hand-populate anything in this folder. Regenerate with
  `make generate` (raw) and `make build` (processed).
- If either subfolder is missing or stale, that's normal, not a bug — just
  run `make all` (or the relevant sub-target) from the repo root.

## Connects to

- Produced by [../src/AGENTS.md](../src/AGENTS.md) (`generate_data.py`,
  `generate_credit_risk_data.py`) and [../dbt/AGENTS.md](../dbt/AGENTS.md)
  (`dbt build`).
- Consumed by `dbt/` (raw) and `src/generate_report.py` + `notebooks/`
  (processed).
- `external/freddie_mac/` is produced by a manual download (see above) +
  `src/ingest_freddie_mac_validation_data.py`, and consumed by
  `notebooks/03_credit_risk_validation_backtest.ipynb` (the one-time
  analysis) and `src/export_credit_risk_walkthrough_data.py` (re-derives
  the same numbers into `apps/walkthrough/public/data/backtest_validation.json`)
  — it never touches dbt or the rest of the pipeline.
