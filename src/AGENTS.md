# AGENTS.md — src/

Fleet: [../AGENTS.md](../AGENTS.md)

## Purpose

Six standalone Python scripts. None orchestrates the warehouse build
anymore — that's dbt's job (see [../dbt/AGENTS.md](../dbt/AGENTS.md)).

- **`generate_data.py`** — generates synthetic users, accounts, and financial
  events (DEPOSIT/PURCHASE/FEE/REFUND) and writes them to `data/raw/*.parquet`.
  Seeded (`SEED = 42`) for reproducibility. This is the *only* place
  e-commerce-ledger synthetic data is created; nothing downstream invents data.
- **`generate_credit_risk_data.py`** — a separate domain: generates a
  synthetic consumer credit/loan portfolio (originations + monthly
  performance, with delinquency evolving as a Markov chain) and writes it to
  `data/raw/credit_risk/*.parquet`. Same seeded-for-reproducibility pattern
  as `generate_data.py`, but do not merge or share code between the two —
  see [../dbt/models/credit_risk/AGENTS.md](../dbt/models/credit_risk/AGENTS.md)
  and its README for the full modeling writeup.
- **`generate_report.py`** — read-only. Queries the e-commerce ledger's dbt
  reporting marts (`main_marts.*` in the DuckDB file) and writes charts + a
  CSV to `reports/`. **Assumes `dbt build` has already run** — it does not
  build or refresh anything itself. (The credit-risk module's own
  eyeball-only plots live in a notebook instead, per that module's scope —
  see `notebooks/AGENTS.md`.)

- **`ingest_freddie_mac_validation_data.py`** — a different kind of script
  from the other three: it doesn't *generate* anything, it parses **real**
  external data (Freddie Mac's Single-Family Loan-Level Dataset, manually
  downloaded to `data/external/freddie_mac/` — see that folder's entry in
  `data/AGENTS.md`) and maps it into this project's grain, for
  `notebooks/03_credit_risk_validation_backtest.ipynb`. Read its docstring
  before touching the column-position constants — they were verified
  against Freddie Mac's actual current file layout and each raw file's
  real value distributions, not assumed from a cached memory of the
  format (which has changed more than once). Never modifies the raw `.txt`
  files; writes derived `.parquet` alongside them.
- **`export_credit_risk_walkthrough_data.py`** — read-only, the one
  bridge between the dbt warehouse and `apps/walkthrough/` (a static site
  with no DB connection of its own — see
  [../apps/walkthrough/AGENTS.md](../apps/walkthrough/AGENTS.md)). Writes
  `apps/walkthrough/public/data/*.json`, one file per chart/table the
  site needs. Six of its seven outputs query `main_marts.*`/`main_staging.*`
  in `ledgerone.duckdb`; the seventh, `backtest_validation.json`, instead
  re-derives the validation notebook's numbers directly from
  `data/external/freddie_mac/*.parquet` via its own DuckDB connection —
  skipped with a warning, not a hard failure, if that manually-downloaded
  data isn't present locally.
- **`export_finops_data.py`** — a different kind of export from the one
  above: reads live AWS Cost Explorer (`boto3`, `ce:GetCostAndUsage`) for
  the account [../infra/](../infra/) is deployed into, not DuckDB at all.
  Writes `apps/walkthrough/public/data/finops_snapshot.json`. Run by hand
  occasionally, not part of `make walkthrough-data` or any other target —
  it's a live billing call against real infrastructure, not something a
  fresh clone can reproduce without real AWS credentials. Skips (exit 0,
  no file written) rather than crashing if Cost Explorer isn't enabled
  yet on that account, or was enabled too recently for AWS to have
  ingested data (can lag up to 24h) — both real states this project has
  actually hit, not hypothesized edge cases.

Two other scripts used to live here: `run_pipeline.py` (ran `sql/*.sql` in
order) and `ledger_validation.py` (hand-written integrity checks). Both
were retired when the Phase 2 dbt layer was added: `dbt build` replaces
`run_pipeline.py`, and `dbt test` replaces `ledger_validation.py` with the
same checks expressed as dbt schema tests + two singular tests. Don't
recreate either — extend the dbt project instead.

## Conventions

- Scripts resolve paths from `Path(__file__).resolve().parents[1]`
  (`PROJECT_ROOT`), not the current working directory — safe to run from
  anywhere.
- `generate_report.py` opens the DuckDB file `read_only=True`. Keep it that
  way — reporting must never mutate the warehouse.
- Run via `make generate` / `make report`, or directly with the repo's
  `venv/bin/python`.

## Connects to

- Writes that `dbt/` reads (`data/raw/*.parquet` via the `raw` source, and
  `data/raw/credit_risk/*.parquet` via the separate `credit_risk` source).
- Reads what `dbt/` writes (`main_marts.*` tables) to produce `reports/`.
- `export_credit_risk_walkthrough_data.py` also reads `main_marts.*`/
  `main_staging.*` and `data/external/freddie_mac/*.parquet`, and writes
  `apps/walkthrough/public/data/*.json`.
- `export_finops_data.py` reads live AWS Cost Explorer (no DuckDB
  involved) and writes `apps/walkthrough/public/data/finops_snapshot.json`
  — the only script in `src/` that talks to AWS, and the only one whose
  output can legitimately not exist yet.
