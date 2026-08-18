# AGENTS.md — src/

Fleet: [../AGENTS.md](../AGENTS.md)

## Purpose

Two standalone Python scripts. Neither orchestrates the warehouse build
anymore — that's dbt's job (see [../dbt/AGENTS.md](../dbt/AGENTS.md)).

- **`generate_data.py`** — generates synthetic users, accounts, and financial
  events (DEPOSIT/PURCHASE/FEE/REFUND) and writes them to `data/raw/*.parquet`.
  Seeded (`SEED = 42`) for reproducibility. This is the *only* place
  synthetic data is created; nothing downstream invents data.
- **`generate_report.py`** — read-only. Queries the dbt reporting marts
  (`main_marts.*` in the DuckDB file) and writes charts + a CSV to
  `reports/`. **Assumes `dbt build` has already run** — it does not build or
  refresh anything itself.

There used to be a third script, `run_pipeline.py` (ran `sql/*.sql` in
order) and `ledger_validation.py` (hand-written integrity checks). Both were
retired when the Phase 2 dbt layer was added: `dbt build` replaces
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

- Writes that `dbt/` reads (`data/raw/*.parquet`, via the `raw` source's
  `external_location`).
- Reads what `dbt/` writes (`main_marts.*` tables) to produce `reports/`.
