# A day in the life of a credit risk analyst

A static, scroll-driven React walkthrough of LedgerOne's
[credit-risk practice module](../../dbt/models/credit_risk/) — 5 scenes,
each pairing a pinned chart with scrolling narrative text, covering
vintage curves, the roll-rate transition matrix, and a CECL-style reserve
forecast. Built as a portfolio/interview-review artifact for
Trustan and anyone he shares the link with. See
[AGENTS.md](AGENTS.md) for the technical/agent-facing map of this folder.

## What it is (and isn't)

It's a **static site** — no backend, no database connection, no auth. It
reads a handful of pre-computed JSON files at runtime via `fetch`. That
was a deliberate constraint, not a shortcut: it keeps this app consistent
with the rest of LedgerOne's "no orchestration, no cloud services, no
hosted backend" scope, and it means hosting is just "put static files
somewhere reachable by URL."

It is **not** a BI dashboard (that's the project's separate, still-open
Phase 3) and it does not modify the dbt project or the credit-risk module
it visualizes — strictly a read-only consumer of that module's output.

## How the static data export works

The credit-risk dbt marts live in DuckDB, not in a form a static site can
query directly. `src/export_credit_risk_walkthrough_data.py` (at the repo
root, alongside `src/generate_credit_risk_data.py`) runs six queries
against the built warehouse and writes the results to
`public/data/*.json`:

| File | Source |
|---|---|
| `originations_by_cohort_and_band.json` | Light aggregation over `credit_risk__stg_loan_originations` (not its own mart — just a `GROUP BY` for this chart) |
| `current_bucket_distribution.json` | `credit_risk__cecl_reserve_estimate`'s `account_count`/`outstanding_balance` columns, reused directly |
| `vintage_curves.json` | `credit_risk__vintage_curves`, full table |
| `roll_rate_matrix.json` | `credit_risk__roll_rate_transition_matrix`, aggregated across all months and row-normalized into one blended matrix |
| `reserve_forecast_trajectory.json` | **Not** sourced directly from `credit_risk__cecl_reserve_estimate`, see below |
| `portfolio_summary_by_score_band.json` | `credit_risk__portfolio_summary_by_score_band`, as-is |

**On `reserve_forecast_trajectory.json`**: the dbt mart
`credit_risk__cecl_reserve_estimate` only exposes the *final* forecast
month's expected loss — that's all the underlying business question
needs. Scene 4 of this walkthrough wants to *plot* the buildup across
all 12 months, which the mart doesn't expose. Rather than modify that
mart (out of scope — this app consumes dbt output, it doesn't change it),
the export script re-runs the same blended-transition-matrix roll-forward
the mart does, but keeps every intermediate month instead of filtering to
the last one. This does duplicate a small amount of SQL logic between the
dbt model and this script — a real, acknowledged trade-off. (Building
this also surfaced a genuine bug: early months' `outstanding_balance`
came out wrong because a bucket that can't yet mathematically reach
charge-off is *absent*, not zero, in the recursive CTE's output at that
horizon — fixed with an explicit `(bucket, month)` grid and a `LEFT JOIN`.
See the script's comments for the full explanation.)

Regenerate after any credit-risk mart changes:

```bash
# from the repo root
make walkthrough-data
# equivalent to: source venv/bin/activate && python src/export_credit_risk_walkthrough_data.py
```

Then rebuild/redeploy this app (see below) — the JSON files are checked
into `public/data/`, so a stale export is a real staleness risk, not just
a local dev inconvenience.

## Running it locally

```bash
cd apps/walkthrough
npm install
npm run dev
```

Opens at `http://localhost:5173/ledgerone/walkthrough/` (the dev server
respects the same `/ledgerone/walkthrough/` base path used in production,
so relative asset/data URLs behave identically in dev and prod).

```bash
npm run build     # production build -> dist/
npm run preview   # serve that build locally
```

## Stack, and why

- **React + Vite + TypeScript** — matches nothing else in this
  Python/dbt-first repo, but it's the standard, low-ceremony choice for a
  scroll-driven static site with per-scene interactive state.
- **Recharts** for the line/area charts (vintage curves, reserve
  forecast) — handles multi-series lines, tooltips, and responsive
  sizing out of the box.
- **The roll-rate heatmap is a hand-rolled CSS Grid component**, not
  Recharts or D3. Recharts has no first-class matrix/heatmap chart type,
  and a 4×5 cell grid with per-cell hover state and severity-based
  highlighting is well within plain CSS Grid + React state — pulling in
  D3 (or a second charting library) for one component wasn't worth the
  dependency weight.
- **Scrollytelling is a ~30-line custom `IntersectionObserver` hook**
  (`useScrollySteps`), not a dedicated library like scrollama. With only
  5 scenes and simple discrete "which paragraph is active" state (not
  continuous scroll-progress binding), a library would have added a
  dependency to replace less code than the dependency's own API surface.
- **Plain CSS**, one shared `theme.ts` for color — no design system, no
  CSS-in-JS, no Tailwind. Five scenes don't need one.

## Deployment / hosting

Static build deployed to **GitHub Pages** via a GitHub Actions workflow
(`.github/workflows/deploy-walkthrough.yml` at the repo root): on every
push to `main` that touches `apps/walkthrough/**`, it builds the app and
publishes `dist/` to Pages. Live at:

**https://trustanprice.github.io/ledgerone/walkthrough/**

`vite.config.ts`'s `base: '/ledgerone/walkthrough/'` matches that
subpath — if this ever moves to a different host or path, update `base`
to match or every asset/data URL will 404.

To deploy manually instead: `npm run build`, then publish the `dist/`
folder's contents to any static host (Netlify, Vercel, S3+CloudFront,
etc.) — there's nothing GitHub-Pages-specific about the build itself.

## Judgment calls / known limitations

- **Reserve forecast trajectory duplicates roll-forward logic** between
  the dbt mart and the export script (see above) — the one place data
  logic in this app isn't single-sourced from dbt.
- **The bucket-distribution and originations charts intentionally don't
  use a log scale**, even though delinquent-bucket bars are tiny next to
  the Current bar. That's the real shape of the data (delinquency really
  is rare), and the narrative text leans into it rather than around it —
  a log scale would visually flatter the data at the cost of honesty.
- **No automated tests.** For a 5-scene static portfolio site reading
  fixed JSON, the numbers were spot-checked by hand against the dbt
  marts' own (already-tested) output instead of writing a test suite —
  proportionate for the scope, but a real gap if this app ever grows.
