# AGENTS.md — LedgerOne fleet manager

LedgerOne is a synthetic fintech ledger and analytics platform. Python generates
immutable financial events; dbt (on DuckDB) transforms them into a tested,
documented star schema and a set of business-question marts; a small Python
script turns those marts into charts/CSV a stakeholder could look at.

Nothing here touches real money, a cloud warehouse, or an orchestrator —
this is intentionally a local, single-command, dbt+DuckDB project.

## Stack

- **Python** (`venv/`) — synthetic data generation, reporting.
- **DuckDB** — the entire warehouse is one file: `data/processed/ledgerone.duckdb`.
- **dbt-core 1.8.9 + dbt-duckdb 1.8.4** — all transformation, testing, and
  documentation. No dbt packages (e.g. dbt_utils) are used — this environment
  can't reliably reach the dbt package hub, so everything is either raw
  DuckDB SQL or dbt-core's built-in cross-db macros (e.g. `dbt.hash()`).
- **Parquet** — raw event storage (`data/raw/`).
- **Matplotlib** — the one BI-adjacent tool; no BI product is used.

## Data flow

```
src/generate_data.py
  -> data/raw/{users,accounts,events}.parquet   (gitignored, regenerated)
       -> dbt sources (external_location, no load step)
            -> stg_* (typed, 1:1 with source)          [dbt/models/staging]
                 -> dim_date, dim_customer, dim_account,
                    fact_transactions (star schema)      [dbt/models/marts/core]
                      -> reporting marts (business Qs)    [dbt/models/marts/reporting]
                           -> src/generate_report.py -> reports/ (charts + CSV)
```

`dbt test` runs schema tests (not_null/unique/relationships/accepted_values)
plus two custom singular tests at every layer. This is the sole correctness
gate now — the old hand-written `src/ledger_validation.py` and `sql/*.sql`
pipeline were retired when dbt subsumed their job (Phase 2, see root README).

## How to run

One command, from repo root, after `pip install -r requirements.txt` into `venv/`:

```
make all
```

This runs `generate` (synthetic data) → `build` (`dbt build`: staging, star
schema, marts, all tests) → `report` (charts + CSV). Individual targets
(`make generate`, `make build`, `make test`, `make report`) exist too. See
`Makefile` for how `LEDGERONE_DB_PATH` / `LEDGERONE_RAW_DIR` are set — every
dbt invocation needs those two env vars (or the relative-path defaults in
`dbt/profiles.yml` / `dbt/models/staging/_sources.yml`, which only work if
you `cd dbt` first).

## Fleet members

| Folder | Purpose | AGENTS.md |
|---|---|---|
| `src/` | Python: synthetic event generation, report/chart generation | [src/AGENTS.md](src/AGENTS.md) |
| `dbt/` | The dbt project: staging, star schema, reporting marts, tests, docs | [dbt/AGENTS.md](dbt/AGENTS.md) |
| `notebooks/` | Ad hoc exploratory sanity checks, not part of the reproducible pipeline | [notebooks/AGENTS.md](notebooks/AGENTS.md) |
| `data/` | Raw parquet + the DuckDB warehouse file. Gitignored, regenerated locally | [data/AGENTS.md](data/AGENTS.md) |
| `reports/` | Output of `src/generate_report.py` — charts + CSV, committed as evidence | [reports/AGENTS.md](reports/AGENTS.md) |

## Conventions that span the fleet

- **Immutability / append-only**: raw events are never edited. Every layer
  downstream is a pure derivation, always safe to `CREATE OR REPLACE`.
- **Single-entry ledger, not double-entry**: each event posts once against
  the user's cash account — there's no offsetting platform-side account. See
  `dbt/AGENTS.md` for what this means for the "ledger balances" test.
- **No secrets, no real data**: everything is synthetic (seeded, `SEED = 42`
  in `src/generate_data.py`), so there is nothing here to protect.
- **Out of scope**: investment/portfolio simulation and market data
  (README's Phase 4) are explicitly not being built. Don't add them.
