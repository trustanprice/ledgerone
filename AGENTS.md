# AGENTS.md — LedgerOne fleet manager

LedgerOne is a synthetic fintech ledger and analytics platform. Python generates
immutable financial events; dbt (on DuckDB) transforms them into a tested,
documented star schema and a set of business-question marts; a small Python
script turns those marts into charts/CSV a stakeholder could look at.

A second, separate domain lives alongside the e-commerce ledger: a
synthetic consumer credit/loan portfolio, built purely as SQL/data-modeling
practice for credit-risk-analyst interviews (vintage analysis, roll-rate /
transition-matrix modeling, a simplified CECL reserve estimate). It's not
part of the numbered roadmap below and never joins to the ledger domain —
see [dbt/models/credit_risk/AGENTS.md](dbt/models/credit_risk/AGENTS.md).

A third piece, `apps/walkthrough/`, is a static, multi-tab React site
covering the credit-risk module from five angles behind one nav: a
teaching walkthrough (the original scroll-driven scenes), a data-
engineering reference (dbt lineage, SQL, data dictionary), a forecasting
dashboard (the same charts, reference mode), an infrastructure/
architecture writeup (documents the real AWS/MotherDuck deployment in
`infra/`, but the page itself makes no live cloud calls), and an
"Emerging Techniques" tab — currently scaffolding only (structure and
placeholder cards, no real content), tracking a set of stochastic-process
methods and a SAS exercise not built yet; see
`apps/walkthrough/README.md`'s "Planned: a sixth tab" section. A
portfolio artifact, not a dashboard app, and not part of the numbered
roadmap either. It's a pure read-only consumer of static JSON exported
from the credit-risk marts; it never queries DuckDB directly and never
modifies dbt. See [apps/walkthrough/AGENTS.md](apps/walkthrough/AGENTS.md).

Nothing here touches real money or an orchestrator, and the local,
single-command `make all` path is still the default and the one every
contributor uses day to day. It's no longer the *only* path, though: a
real (small, portfolio-scale) cloud deployment exists alongside it —
`infra/` (CloudFormation: an S3 raw-data bucket + a GitHub OIDC deploy
role, per environment) and `.github/workflows/dbt-build.yml` run the same
pipeline against MotherDuck instead of a local `.duckdb` file, dispatched
manually, not on a schedule — **manual dispatch is the current state, not
the goal**: the intent is an eventual scheduled/unattended run against
`prod`, held back deliberately until there's alerting on a failed run and
more dispatch history to trust it unwatched (see
[infra/AGENTS.md](infra/AGENTS.md)'s "Planned" section). See `infra/`'s
own docs and `dbt/profiles.yml`'s `ci_dev`/`prod` targets. The other
exception, as before, is `apps/walkthrough/`, a static site with no
backend of its own — see "Out of scope" below for what that does and
doesn't permit.

## Stack

- **Python** (`venv/`) — synthetic data generation, reporting.
- **DuckDB** — the entire warehouse is one file: `data/processed/ledgerone.duckdb`.
- **dbt-core 1.8.9 + dbt-duckdb 1.8.4** — all transformation, testing, and
  documentation. No dbt packages (e.g. dbt_utils) are used — this environment
  can't reliably reach the dbt package hub, so everything is either raw
  DuckDB SQL or dbt-core's built-in cross-db macros (e.g. `dbt.hash()`).
- **Parquet** — raw event storage (`data/raw/`).
- **Matplotlib** — the one BI-adjacent tool; no BI product is used.
- **React + Vite + TypeScript** (`apps/walkthrough/` only) — a static
  site, not a general frontend stack for the project. See its own
  AGENTS.md before adding any other JS/TS code elsewhere in the repo.

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

src/generate_credit_risk_data.py
  -> data/raw/credit_risk/{loan_originations,loan_performance}.parquet
       -> dbt sources -> credit_risk__stg_*                [dbt/models/credit_risk/staging]
            -> credit_risk__vintage_curves,
               credit_risk__roll_rate_transition_matrix,
               credit_risk__cecl_reserve_estimate, etc.     [dbt/models/credit_risk/marts]
                 -> notebooks/02_..._vintage_and_roll_rates.ipynb (eyeball only)

src/export_credit_risk_walkthrough_data.py
  (reads main_marts.credit_risk__* / main_staging.credit_risk__stg_*)
       -> apps/walkthrough/public/data/*.json (static, checked in)
            -> apps/walkthrough (React/Vite) -> deployed static site

data/external/freddie_mac/ (real data, manual download, gitignored)
  -> src/ingest_freddie_mac_validation_data.py
       -> notebooks/03_credit_risk_validation_backtest.ipynb (standalone — no dbt/DuckDB involved)
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
| `dbt/models/credit_risk/` | Credit-risk practice module: vintage analysis, roll-rate/transition matrix, CECL reserve estimate | [dbt/models/credit_risk/AGENTS.md](dbt/models/credit_risk/AGENTS.md) |
| `notebooks/` | Ad hoc exploratory sanity checks, not part of the reproducible pipeline | [notebooks/AGENTS.md](notebooks/AGENTS.md) |
| `data/` | Raw parquet + the DuckDB warehouse file. Gitignored, regenerated locally | [data/AGENTS.md](data/AGENTS.md) |
| `reports/` | Output of `src/generate_report.py` — charts + CSV, committed as evidence | [reports/AGENTS.md](reports/AGENTS.md) |
| `apps/walkthrough/` | Static multi-tab React site: teaching walkthrough + data-eng/forecasting/infra reference tabs, no backend | [apps/walkthrough/AGENTS.md](apps/walkthrough/AGENTS.md) |
| `infra/` | CloudFormation for the real (small-scale) AWS/MotherDuck deployment: S3 raw bucket + GitHub OIDC deploy role, per environment | [infra/AGENTS.md](infra/AGENTS.md) |

## Conventions that span the fleet

- **Immutability / append-only**: raw events are never edited. Every layer
  downstream is a pure derivation, always safe to `CREATE OR REPLACE`.
- **Single-entry ledger, not double-entry**: each event posts once against
  the user's cash account — there's no offsetting platform-side account. See
  `dbt/AGENTS.md` for what this means for the "ledger balances" test.
- **Synthetic data, real cloud secrets**: every dataset generated by this
  project is synthetic (seeded, `SEED = 42` in `src/generate_data.py`) —
  nothing in `data/` or `dbt/` needs protecting. `infra/`'s cloud path is
  the one exception: `MOTHERDUCK_TOKEN` is a real secret (GitHub
  Environment secrets, `dev`/`prod`, never committed), though AWS access
  itself uses short-lived OIDC-assumed credentials, never a stored key.
- **Out of scope**: investment/portfolio simulation and market data
  (README's Phase 4) are explicitly not being built. Don't add them.
  Same for a live BI dashboard app (README's Phase 3) —
  `apps/walkthrough/` is a static, pre-computed-data site, not that; don't
  extend it into one, and don't add a database connection, API route, or
  auth to it.
- **Two domains, never blended**: the e-commerce ledger and the credit-risk
  practice module are separate synthetic datasets with separate raw
  folders (`data/raw/` vs `data/raw/credit_risk/`) and separate model
  prefixes (`stg_*`/`dim_*`/`fact_*` vs `credit_risk__*`). Don't join
  across them or reuse one domain's generator/models for the other.
