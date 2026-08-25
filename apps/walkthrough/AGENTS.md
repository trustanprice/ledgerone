# AGENTS.md — apps/walkthrough/

Fleet: [../../AGENTS.md](../../AGENTS.md)

## Purpose

A static, multi-tab React site covering the credit-risk practice module
([dbt/models/credit_risk/](../../dbt/models/credit_risk/)) from four
angles behind one persistent top nav: **Overview** (landing), **Day in
the Life** (the original 5-scene scrollytelling walkthrough — teaching-
oriented), **Database & Data Engineering** (dbt lineage, SQL, data
dictionary, test coverage — reference-oriented), **Forecasting & Data
Science** (the same charts as the walkthrough, reused in reference mode —
all cohorts visible, no scene-by-scene reveal), and **Infrastructure &
Productionization** (a writeup of the real, deployed AWS/MotherDuck
infrastructure in [../../infra/](../../infra/) — real, but this tab still
renders static content and makes no live AWS calls of its own; see below).
Built as a portfolio/interview-review artifact, not a dashboard app and
not part of the numbered roadmap's Phase 3 (BI dashboards) — see the root
AGENTS.md.

**Important tonal split, don't blur it**: Day in the Life teaches concepts
to someone new to them (step-by-step narrative). Every other tab is a
dense reference for someone who already knows the domain (captions and
definitions, not explanations, with everything visible at once). If a new
addition to a reference tab starts reading like a tutorial, or the
walkthrough starts reading like a spec sheet, that's a sign it's in the
wrong tab.

**This app has no backend and never queries DuckDB, and the Infrastructure
tab makes no live calls to AWS or any cloud provider at runtime** — the
AWS/MotherDuck deployment it documents is real (see
[../../infra/AGENTS.md](../../infra/AGENTS.md)), but this site only ever
reads pre-computed static JSON, same as every other tab. It's a pure
consumer of static JSON exported once (or whenever the credit-risk marts
change) by `src/export_credit_risk_walkthrough_data.py`, plus — for the
Infrastructure tab's cost section — `src/export_finops_data.py`, exported
occasionally from AWS Cost Explorer, not on every build. The Data
Engineering tab is still pure static reference content with no data
dependency. It does not modify the dbt project, the credit-risk module,
its data, or the real AWS infrastructure — read-only, one direction,
source of truth stays in `dbt/` and `infra/`.

**Overview is a dense bento dashboard, not a card grid.** A row of
evenly-sized cards (label + title + paragraph) is the single most common
AI-generated landing-page pattern there is — the fix wasn't a theme
change, it was an asymmetric grid where each tile previews real content
(a live sparkline, an actual lineage snippet, real SQL, scene-progress
dots, a mini diagram) plus a real-number stat strip above it. If you're
tempted to add a fifth tile or "simplify" this back to uniform cards,
don't — see README.md's Visual System section for the reasoning, and keep
new tiles asymmetric (varied `grid-column`/`grid-row` spans) rather than
uniform.

## Layout

```
public/data/*.json        — static data, written by the export script (not hand-edited)
src/
  types.ts                 — TS types mirroring the JSON shape (kept in sync by hand)
  theme.ts                 — the one shared color system: cohort palette + sequential ramp + chart chrome
  tabConfig.ts              — TabId union + nav labels (the tab list itself)
  hooks/
    useJsonData.ts          — fetch(public/data/*.json) at runtime
    useCreditRiskData.ts     — fetches all 7 datasets; shared by Walkthrough + Forecasting tabs
    useScrollySteps.ts       — IntersectionObserver-driven "active narrative step" (walkthrough only)
    useHashTab.ts            — URL-hash-based tab state, no router library
  components/
    Nav.tsx                  — persistent top nav, one button per tab
    GlassPanel.tsx            — the one reusable glass-surface wrapper
    Scene.tsx                 — walkthrough-only layout: sticky visual panel + narrative column
    SqlBlock.tsx               — ~50-line regex SQL tokenizer + syntax-highlighted <pre>
    FlowDiagram.tsx             — generic box-and-arrow diagram (dbt lineage, infra architecture,
                                   AND the compact previews on the Overview tiles via .mini-diagram)
    DataTable.tsx                — generic reference table
    Sparkline.tsx                 — dependency-free inline SVG line+area, Overview's Forecasting tile
    StatStrip.tsx                  — ticker-row stat component, Overview only (so far)
    charts/                       — one component per chart type, all sharing theme.ts;
                                     reused as-is (no scrolly wrapper) by the Forecasting tab
  content/
    dataEngineeringContent.ts     — SQL snippets, data dictionary, test-coverage counts (static)
    infrastructureContent.ts      — architecture diagram data + CI/CD writeup copy (static)
  scenes/
    Scene1Morning.tsx ... Scene5SoWhat.tsx   — walkthrough-tab-only, content unchanged
  tabs/
    OverviewTab.tsx, WalkthroughTab.tsx, DataEngineeringTab.tsx,
    ForecastingTab.tsx, InfrastructureTab.tsx
  App.tsx                   — shell: <Nav/> + whichever tab is active (useHashTab)
```

## Conventions

- **Static data only.** If a tab needs a number that isn't in
  `public/data/*.json`, add it to an export script (mirroring an existing
  mart's query, or a light aggregation over a staging table) — never add a
  live DB connection or an API route to this app itself. The rule is
  about what this React app does at runtime, not about where an export
  script's data comes from: `src/export_finops_data.py` calls live AWS
  Cost Explorer, but only when run by hand, server-side, to produce a
  static JSON snapshot — the app still only ever `fetch`es that file, the
  same as every other dataset. The Data Engineering tab's content
  (`src/content/dataEngineeringContent.ts`) is static and hand-written by
  design, no data dependency at all. The Infrastructure tab's
  (`src/content/infrastructureContent.ts`) documents a real, deployed
  architecture (see [../../infra/AGENTS.md](../../infra/AGENTS.md)) —
  still hand-written prose, not derived from a query, but no longer
  describing something hypothetical.
- **One shared visual system** (`theme.ts` + `.glass-panel` in
  `index.css`): a fixed 12-color categorical palette for vintage cohorts
  (assigned by index, never reassigned), a single-hue sequential ramp for
  every magnitude encoding (roll-rate heatmap, bucket-distribution bars,
  originations-by-band bars) — re-anchored for dark surfaces (dim → vivid,
  not light → dark; see `sequentialBlue`'s docstring for why), and shared
  Recharts chrome tokens (`CHART_CHROME`) so axis/gridline colors don't
  drift per-chart. Don't introduce a second color system or hardcode a hex
  value in a chart component — extend `theme.ts`.
- **Reference tabs reuse the walkthrough's chart components directly** —
  `VintageCurvesChart`, `RollRateHeatmap`, `ReserveForecastChart` all
  accept a "no highlight" state (`null` / `'none'`) that renders them as
  a plain reference view. Don't fork a second copy of a chart component
  for a reference tab; if it needs a genuinely different capability, add
  a prop.
- **Scrollytelling via `useScrollySteps`** (Day in the Life only, not the
  reference tabs) — IntersectionObserver with a centered `rootMargin`
  tracks which narrative paragraph is active; each scene's
  `renderVisual(activeStep)` maps that index to a visualization state.
- **No fabricated trend indicators.** A `StatStrip` value only gets an
  up/down delta or arrow if there's a genuine prior-period or target
  value to compare against — none of the current Overview stats have
  one (this is a static snapshot, not a time series with a "yesterday").
  Don't add a decorative trend arrow just because that's a common ticker
  convention; it implies a comparison that doesn't exist here. If a stat
  genuinely has a trajectory (like the reserve forecast), show that as an
  actual `Sparkline` of the real data instead.
- **Tab state via `useHashTab`**, not a router library — see README's
  "Routing" section for why (works identically on both deploy targets
  with zero server config).
- **CSS Grid/Flex `min-width: 0` discipline — this bit the app twice.**
  Grid/flex children default to `min-width: auto`, which refuses to
  shrink below their content's min-content width, EVEN IF a descendant
  has `overflow-x: auto`: an inner scroll container does not save an
  outer grid/flex item from this unless every item in the ancestor chain
  also has `min-width: 0`. First hit: `.nav-tabs` (the tab bar itself)
  pushing the page wider than the viewport on mobile. Second hit, same
  session: `.ref-panel` (a `.glass-panel` grid item in `.ref-grid`)
  refusing to shrink below the lineage diagram's intrinsic width, again
  only on mobile — fixed by adding `min-width: 0` to `.glass-panel`
  itself so every panel gets it automatically. When adding a new
  grid/flex layout here, default to `minmax(0, 1fr)` for grid tracks and
  `min-width: 0` on grid/flex items from the start, don't wait to
  discover the bug on a narrow viewport.
- **Long unbroken tokens need `overflow-wrap: break-word` explicitly.**
  Normal CSS text wrapping only breaks at spaces/hyphens — a single long
  identifier with underscores (a dbt test name, a SQL keyword run) has no
  natural break point and will overflow its box even inside a properly
  `min-width: 0` container, since the box can shrink but the *text run*
  inside it still won't wrap mid-word without this. Applied to
  `.stat-sub`, `.section-desc`, `.ref-panel p` — extend the same pattern
  to any new prose-in-a-narrow-box element.
- **A `<p>` that's `display: flex` for vertical centering needs exactly
  one child.** `Scene.tsx` wraps each narrative step's rich content
  (text + `<strong>`/`<code>`) in a single `<span>` — without that, each
  text run becomes its own flex item and normal text wrapping breaks. See
  the comment in `Scene.tsx` if this pattern is reused elsewhere.
- **Every layout bug above was caught by actually screenshotting the
  built site in a headless browser at desktop AND mobile viewports**, not
  by reading the CSS. `tsc -b && vite build` passing proves nothing about
  whether a grid overflows on a 390px viewport — verify visually before
  calling a UI change done.

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

Deployed as a static site to two targets: **Vercel** (root directory =
`apps/walkthrough`, served from its own domain root) and **GitHub Pages**
(GitHub Actions workflow at `../../.github/workflows/deploy-walkthrough.yml`,
served under `https://trustanprice.github.io/ledgerone/walkthrough/`). No
server, no database, no auth — "hosted" means "a static build is deployed
and reachable by URL," consistent with the rest of the project's
no-orchestration, no-cloud-services constraint.

`vite.config.ts`'s `base` defaults to `"/"` (Vercel/local dev) and is
overridden to `/ledgerone/walkthrough/` only when the GitHub Actions build
sets `VITE_BASE_PATH` — get this wrong for whatever host is actually
serving the build and the page renders blank (every asset/data fetch
404s). See README.md for the
deploy trigger and how to redeploy.

## Connects to

- Reads `public/data/*.json`, written by
  `../../src/export_credit_risk_walkthrough_data.py`, which reads
  `main_marts.credit_risk__*` and `main_staging.credit_risk__stg_*`
  (see [../../dbt/models/credit_risk/AGENTS.md](../../dbt/models/credit_risk/AGENTS.md)).
- Also reads `public/data/finops_snapshot.json`, written by the separate
  `../../src/export_finops_data.py` against live AWS Cost Explorer — see
  [../../infra/AGENTS.md](../../infra/AGENTS.md) for the AWS side.
- Nothing else in the repo reads from this app — it's a leaf.
