# AGENTS.md — data/

Fleet: [../AGENTS.md](../AGENTS.md)

## Purpose

Everything here is generated, not authored, and entirely gitignored
(`.gitignore`: `data/`). A fresh clone has no `data/` directory at all until
you run the pipeline.

- **`raw/`** — `users.parquet`, `accounts.parquet`, `events.parquet`, written
  by `src/generate_data.py`. Treated as the immutable source of truth for
  everything downstream (dbt sources read these files directly, with no
  load/seed step).
- **`processed/ledgerone.duckdb`** — the entire warehouse, built by
  `dbt build` from `dbt/`. Contains the `main_staging` and `main_marts`
  schemas.

## Conventions

- Never hand-edit or hand-populate anything in this folder. Regenerate with
  `make generate` (raw) and `make build` (processed).
- If either subfolder is missing or stale, that's normal, not a bug — just
  run `make all` (or the relevant sub-target) from the repo root.

## Connects to

- Produced by [../src/AGENTS.md](../src/AGENTS.md) (`generate_data.py`) and
  [../dbt/AGENTS.md](../dbt/AGENTS.md) (`dbt build`).
- Consumed by `dbt/` (raw) and `src/generate_report.py` + `notebooks/`
  (processed).
