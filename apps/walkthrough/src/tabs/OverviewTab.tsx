import { GlassPanel } from '../components/GlassPanel'
import { TABS, type TabId } from '../tabConfig'

interface Props {
  onNavigate: (tab: TabId) => void
}

const CARD_COPY: Record<Exclude<TabId, 'overview'>, { kicker: string; body: string }> = {
  walkthrough: {
    kicker: 'Teaching',
    body: 'The original scroll-driven narrative: vintage curves, the roll-rate transition matrix, and a CECL-style reserve estimate, explained from first principles.',
  },
  'data-engineering': {
    kicker: 'Reference',
    body: 'How this is actually built: the dbt lineage, the model SQL worth reading (roll-rate + the recursive CECL roll-forward), a data dictionary, and test coverage.',
  },
  forecasting: {
    kicker: 'Reference',
    body: 'The model outputs as a dense analytical dashboard — every cohort visible at once, plus (once available) backtest accuracy against real loan performance.',
  },
  infrastructure: {
    kicker: 'Architecture doc',
    body: 'What productionizing this pipeline would look like — a static architecture writeup, not a live deployment. No AWS account or real cloud resources involved.',
  },
}

export function OverviewTab({ onNavigate }: Props) {
  const linkTabs = TABS.filter((t) => t.id !== 'overview')

  return (
    <>
      <header className="hero">
        <span className="eyebrow">LedgerOne — Credit Risk Module</span>
        <h1>Credit risk, end to end</h1>
        <p className="lede">
          A synthetic consumer credit portfolio, modeled with the same techniques a real credit-risk or
          loss-forecasting team uses — vintage analysis, roll-rate transition matrices, a CECL-style reserve
          estimate — surfaced four ways: a teaching walkthrough, a data-engineering reference, a forecasting
          dashboard, and an architecture writeup for what productionizing it would take.
        </p>
        <div className="hero-meta">
          <span>Static site, no backend</span>
          <span>Synthetic data only</span>
          <span>
            <a href="https://github.com/trustanprice/ledgerone" target="_blank" rel="noreferrer">
              Source: LedgerOne
            </a>
          </span>
        </div>
      </header>

      <div className="tab-card-grid">
        {linkTabs.map((tab) => (
          <GlassPanel key={tab.id} className="tab-card" onClick={() => onNavigate(tab.id)}>
            <div className="tab-card-kicker">{CARD_COPY[tab.id as Exclude<TabId, 'overview'>].kicker}</div>
            <h3>{tab.label}</h3>
            <p>{CARD_COPY[tab.id as Exclude<TabId, 'overview'>].body}</p>
          </GlassPanel>
        ))}
      </div>

      <footer className="site-footer">
        <p>
          Everything here is synthetic — no real accounts, no real losses. See{' '}
          <a href="https://github.com/trustanprice/ledgerone/blob/main/dbt/models/credit_risk/README.md" target="_blank" rel="noreferrer">
            dbt/models/credit_risk/README.md
          </a>{' '}
          for the full technical writeup.
        </p>
      </footer>
    </>
  )
}
