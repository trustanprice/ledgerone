# LedgerOne Credit Risk — mission control

A static React site covering LedgerOne's
[credit-risk practice module](../../dbt/models/credit_risk/) from four
angles: a teaching walkthrough, a data-engineering reference, a
forecasting dashboard, and an infrastructure/architecture writeup — one
persistent top-level nav, one visual system, client-side tab switching.
Built as a portfolio/interview-review artifact for Trustan and anyone he
shares the link with. See [AGENTS.md](AGENTS.md) for the technical/
agent-facing map of this folder.

## Tabs

| Tab | Style | What it shows |
|---|---|---|
| **Overview** | Landing | Front door: what this is, cards linking into the other four tabs. |
| **Day in the Life** | Teaching | The original 5-scene scrollytelling walkthrough — sticky chart + scrolling narrative, unchanged in content from before this became a multi-tab site. Explains vintage curves, the roll-rate transition matrix, and the CECL reserve forecast from first principles. |
| **Database & Data Engineering** | Reference | How this is actually built: a lineage diagram for both dbt domains (ledger + credit risk, kept visually distinct), the three most interesting model SQL queries verbatim, a data dictionary (grain/keys/tests per model), and an exact test-coverage breakdown (48 tests, by type). |
| **Forecasting & Data Science** | Reference | The same chart components as the walkthrough, reused in "reference mode" — every cohort visible at once, no scene-by-scene reveal — plus a portfolio-summary table and a placeholder section for backtest/validation metrics (not yet available; see below). |
| **Infrastructure & Productionization** | Architecture doc | A diagram + writeup of what productionizing this pipeline would look like (S3 → MotherDuck → scheduled GitHub Actions → OIDC/IAM → CloudFormation). Explicitly labeled as design documentation, not a live deployment — no AWS account or cloud resources involved anywhere in this repo. |

**The Day in the Life tab stays teaching-oriented; every other tab is
reference-grade** — dense, all-data-visible, captions rather than
explanations. This split is deliberate; see AGENTS.md before changing
either tab's tone.

## What it is (and isn't)

It's a **static site** — no backend, no database connection, no auth, no
real cloud calls anywhere (including on the Infrastructure tab, which is
content, not code that talks to AWS). All data comes from pre-computed
JSON read at runtime via `fetch`. That's a deliberate constraint, not a
shortcut: it keeps this app consistent with the rest of LedgerOne's
"no orchestration, no cloud services, no hosted backend" scope, and it
means hosting is just "put static files somewhere reachable by URL."

It does not modify the dbt project or the credit-risk module it
visualizes — strictly a read-only consumer of that module's output.

## How the static data export works

The credit-risk dbt marts live in DuckDB, not in a form a static site can
query directly. `src/export_credit_risk_walkthrough_data.py` (at the repo
root, alongside `src/generate_credit_risk_data.py`) runs six queries
against the built warehouse and writes the results to `public/data/*.json`.
Both the Day in the Life and Forecasting tabs read the same six files
(via the shared `useCreditRiskData` hook) — no new export step was needed
for the multi-tab restructuring; the Data Engineering and Infrastructure
tabs are static reference content with no data dependency at all.

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
needs. The reserve-forecast chart (Day in the Life scene 4, and the
Forecasting tab's reference view) wants to *plot* the buildup across all
12 months, which the mart doesn't expose. Rather than modify that mart
(out of scope — this app consumes dbt output, it doesn't change it), the
export script re-runs the same blended-transition-matrix roll-forward the
mart does, but keeps every intermediate month instead of filtering to the
last one. This does duplicate a small amount of SQL logic between the dbt
model and this script — a real, acknowledged trade-off. (Building this
also surfaced a genuine bug: early months' `outstanding_balance` came out
wrong because a bucket that can't yet mathematically reach charge-off is
*absent*, not zero, in the recursive CTE's output at that horizon — fixed
with an explicit `(bucket, month)` grid and a `LEFT JOIN`. See the
script's comments for the full explanation.)

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

Opens at `http://localhost:5173/`. Direct-link a tab with a URL hash, e.g.
`http://localhost:5173/#/data-engineering` — see "Routing" below.

```bash
npm run build     # production build -> dist/
npm run preview   # serve that build locally
```

## Visual system: dark liquid glass

A full reskin applied consistently across every tab, including Day in the
Life — the site should read as one product. See `src/theme.ts` for the
color tokens and `src/index.css` for the `.glass-panel` primitive.

- **Glassmorphism**: translucent panels (`backdrop-filter: blur(22px)
  saturate(160%)` over a two-layer fill — a light gradient sheen plus a
  ~62% opaque dark base) floating over a fixed, softly-gradiented dark
  background, so there's always something worth blurring behind a panel.
  Dark-first, not a light theme with a toggle — this genre of product
  (Kalshi, Polymarket) is dark by default.
- **One accent hue** (`#4FD8FF`, a vivid cyan), used deliberately for
  interactive/active state — the active nav tab, chart accents, highlight
  rings. Green/red (`--good`/`--bad`) are reserved for genuine directional
  meaning, never decoration.
- **Sequential ramp re-anchored for dark surfaces**: `sequentialBlue(t)`
  in `theme.ts` goes dim → vivid (not light → dark, the way a light-mode
  ramp would). On a dark background, "dark" and "invisible" are the same
  thing, so the ramp had to put visual salience at the HIGH-magnitude end
  instead — which also happens to be correct semantically: the highest
  roll-rate percentages are exactly the cells that should pop.
- **Tabular numbers**: `.mono-nums` / `code` set
  `font-variant-numeric: tabular-nums` with a monospace stack, so columns
  of figures (the data dictionary, stat tiles) align.
- **Contrast was checked against the actual composited surface, not the
  nominal panel color** — every glass panel has a solid-ish (~62-72%
  opaque) dark base *underneath* the light gradient sheen specifically so
  text stays legible regardless of what's blurring through from the
  background gradients. Verified visually via full-page screenshots
  across all 5 tabs at multiple scroll positions (desktop + mobile), not
  just assumed.

## Routing: URL hash, no router library

Tabs are plain React state, synced to `window.location.hash`
(`useHashTab.ts`) — `#/overview`, `#/walkthrough`, `#/data-engineering`,
`#/forecasting`, `#/infrastructure`. No `react-router` or similar. Hash
changes never hit the server, so a direct link to a tab works identically
on both deploy targets (GitHub Pages project subpath, Vercel root)
without any server-side rewrite/fallback configuration — the thing a
History-API router would need on at least one of those two hosts.

## Stack, and why

- **React + Vite + TypeScript** — matches nothing else in this
  Python/dbt-first repo, but it's the standard, low-ceremony choice for a
  small interactive static site.
- **Recharts** for the line/area charts (vintage curves, reserve
  forecast) — handles multi-series lines, tooltips, and responsive
  sizing out of the box. Reused as-is between the Day in the Life and
  Forecasting tabs (same components, different props — no rebuild needed).
- **The roll-rate heatmap is a hand-rolled CSS Grid component**, not
  Recharts or D3 — a 4×5 cell grid with per-cell hover/highlight state is
  well within plain CSS Grid + React state.
- **The dbt lineage diagram and infra architecture diagram share one
  component** (`FlowDiagram.tsx`) — both are just labeled boxes with
  arrows between columns. Hand-built rather than mermaid.js: static,
  small (4-6 columns), never changes at runtime, so a diagramming library
  would be 500kb+ to replace what a CSS Grid renders instantly.
- **SQL syntax highlighting is a ~50-line regex tokenizer**
  (`SqlBlock.tsx`), not Prism/Shiki. A handful of static reference
  snippets don't need a real parser — comments, strings, numbers, and a
  keyword list cover it.
- **Scrollytelling is a ~30-line custom `IntersectionObserver` hook**
  (`useScrollySteps`), not a dedicated library like scrollama. With only
  5 scenes and simple discrete "which paragraph is active" state (not
  continuous scroll-progress binding), a library would have added a
  dependency to replace less code than the dependency's own API surface.
- **Plain CSS**, one shared `theme.ts` for color — no design system, no
  CSS-in-JS, no Tailwind.

## Deployment / hosting

Two live deploy targets, both from the same build:

- **Vercel** — root directory set to `apps/walkthrough` in the Vercel
  project settings; auto-detects Vite (`npm run build`, output `dist`).
  Served from the root of its own domain, so `base` is left at its
  default (`"/"`).
- **GitHub Pages** — a GitHub Actions workflow
  (`.github/workflows/deploy-walkthrough.yml` at the repo root) builds
  and publishes `dist/` on every push to `main` that touches
  `apps/walkthrough/**`. Live at
  **https://trustanprice.github.io/ledgerone/walkthrough/**.

The one thing that has to stay correct across both: **`base` must match
where the app is actually served from, or the page renders blank** (every
asset and `public/data/*.json` fetch 404s, with no error beyond failed
network requests — check the browser Network tab first if this ever
happens after a deploy). `vite.config.ts` defaults `base` to `"/"`
(correct for Vercel and local dev); the GitHub Actions workflow overrides
it to `/ledgerone/walkthrough/` via the `VITE_BASE_PATH` env var at build
time, since Pages serves this as a subpath project site. If you add a
third static host that serves from a subpath, set `VITE_BASE_PATH` for
that build the same way — don't hardcode a new default.

## Judgment calls / known limitations

- **Reserve forecast trajectory duplicates roll-forward logic** between
  the dbt mart and the export script (see above) — the one place data
  logic in this app isn't single-sourced from dbt.
- **The bucket-distribution and originations charts intentionally don't
  use a log scale**, even though delinquent-bucket bars are tiny next to
  the Current bar. That's the real shape of the data (delinquency really
  is rare), and the narrative leans into it rather than around it — a log
  scale would visually flatter the data at the cost of honesty.
- **The Forecasting tab's vintage-curve reference view doesn't add
  click-to-filter cohort toggling.** All cohorts are visible at once
  (satisfying the actual requirement), but a "click a legend entry to
  hide that series" interaction — a natural fit for a reference
  dashboard — wasn't built, to keep scope bounded given everything else
  in this expansion. Would be a reasonable follow-up.
- **Backtest/validation metrics are a labeled placeholder**, not
  fabricated numbers. That work depends on a real loan-performance
  dataset that hasn't been sourced yet (blocked on a manual Freddie Mac
  registration/download) — the Forecasting tab says so explicitly rather
  than implying an accuracy result that doesn't exist.
- **The Infrastructure tab's content is illustrative, not a real
  proposal reviewed by anyone** — reasonable, defensible architecture
  choices (MotherDuck, OIDC, CloudFormation) explained with real
  reasoning, but written for this portfolio site, not vetted the way an
  actual production design doc would be.
- **No automated tests.** Numbers are spot-checked by hand against the
  dbt marts' own (already-tested) output, and the whole site was verified
  in an actual headless-Chromium browser (desktop + mobile, all 5 tabs,
  zero console errors) rather than just a type check — but there's no
  regression-catching test suite. Proportionate for the scope, a real gap
  if this app keeps growing.
