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

## Conventions

- Notebooks here are exploratory scratch work: it's fine for them to
  hardcode paths, print `.df()` output, and not be idempotent-by-design the
  way dbt models are.
- Don't add new ledger/warehouse-building logic in a notebook — that's dbt's
  job. Notebooks in this folder should only ever *read*.

## Connects to

- Reads `data/raw/*.parquet` (written by `src/generate_data.py`) and
  `data/processed/ledgerone.duckdb` (written by `dbt build`).
