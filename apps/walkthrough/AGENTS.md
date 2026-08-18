# AGENTS.md — apps/walkthrough/

Fleet: [../../AGENTS.md](../../AGENTS.md)

## Purpose

A static React site: a scroll-driven "day in the life of a credit risk
analyst" walkthrough of the credit-risk practice module
([dbt/models/credit_risk/](../../dbt/models/credit_risk/)) — 5 scenes,
each a pinned visualization beside scrolling narrative text. Built as a
portfolio/interview-review artifact, not a dashboard app and not part of
the numbered roadmap's Phase 3 (BI dashboards) — see the root AGENTS.md.

**This app has no backend and never queries DuckDB.** It's a pure
consumer of static JSON exported once (or whenever the credit-risk marts
change) by `src/export_credit_risk_walkthrough_data.py` at the repo root.
It does not modify the dbt project, the credit-risk module, or its data —
read-only, one direction, source of truth stays in `dbt/`.

## Layout

```
public/data/*.json        — static data, written by the export script (not hand-edited)
src/
  types.ts                 — TS types mirroring the JSON shape (kept in sync by hand)
  theme.ts                 — the one shared color system: cohort palette + sequential ramp
  hooks/
    useJsonData.ts          — fetch(public/data/*.json) at runtime
    useScrollySteps.ts       — IntersectionObserver-driven "active narrative step"
  components/
    Scene.tsx                — shared layout: sticky visual panel + narrative column
    charts/                  — one component per chart type, all sharing theme.ts
  scenes/
    Scene1Morning.tsx ... Scene5SoWhat.tsx
  App.tsx                   — fetches all datasets, renders hero + 5 scenes + footer
```

## Conventions

- **Static data only.** If a scene needs a number that isn't in
  `public/data/*.json`, add it to the export script (mirroring an existing
  mart's query, or a light aggregation over a staging table) — never add a
  live DB connection or an API route to this app.
- **One shared visual system** (`theme.ts`): a fixed 12-color categorical
  palette for vintage cohorts (assigned by index, never reassigned), and a
  single-hue sequential blue ramp for every magnitude encoding (the
  roll-rate heatmap, the bucket-distribution bars). Don't introduce a
  second color scheme for a new chart — reuse `theme.ts`.
- **Scrollytelling via `useScrollySteps`**, not a dedicated library —
  IntersectionObserver with a centered `rootMargin` tracks which narrative
  paragraph is active; each scene's `renderVisual(activeStep)` maps that
  index to a visualization state (which chart, what's highlighted). See
  the hook's docstring for why a library wasn't worth it here.
- **CSS Grid/Flex `min-width: 0` discipline**: grid/flex children default
  to `min-width: auto`, which refuses to shrink below their content's
  min-content width — this genuinely broke the mobile layout once (see
  git history / the walkthrough build notes) until `minmax(0, 1fr)` and
  `min-width: 0` were added throughout. Keep that pattern when adding new
  grid/flex layouts here, especially anything with long unbreakable
  tokens (`<code>`, long labels).
- **A `<p>` that's `display: flex` for vertical centering needs exactly
  one child.** `Scene.tsx` wraps each narrative step's rich content
  (text + `<strong>`/`<code>`) in a single `<span>` — without that, each
  text run becomes its own flex item and normal text wrapping breaks. See
  the comment in `Scene.tsx` if this pattern is reused elsewhere.

## Running it

```
cd apps/walkthrough
npm install
npm run dev        # local dev server
npm run build       # production build to dist/
```

Regenerating the data after a credit-risk mart changes:

```
make walkthrough-data   # from the repo root
```

Full details (including the reserve-forecast trajectory query, which
duplicates a small amount of the `credit_risk__cecl_reserve_estimate`
mart's logic on purpose — see that script's docstring) are in
[README.md](README.md).

## Hosting

Deployed as a static site via GitHub Pages (GitHub Actions workflow at
`../../.github/workflows/deploy-walkthrough.yml`), at
`https://trustanprice.github.io/ledgerone/walkthrough/`. No server, no
database, no auth — "hosted" means "a static build is deployed and
reachable by URL," consistent with the rest of the project's
no-orchestration, no-cloud-services constraint. See README.md for the
deploy trigger and how to redeploy.

## Connects to

- Reads `public/data/*.json`, written by
  `../../src/export_credit_risk_walkthrough_data.py`, which reads
  `main_marts.credit_risk__*` and `main_staging.credit_risk__stg_*`
  (see [../../dbt/models/credit_risk/AGENTS.md](../../dbt/models/credit_risk/AGENTS.md)).
- Nothing else in the repo reads from this app — it's a leaf.
