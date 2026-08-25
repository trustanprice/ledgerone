import { useCreditRiskData } from '../hooks/useCreditRiskData'
import { Scene1Morning } from '../scenes/Scene1Morning'
import { Scene2Vintage } from '../scenes/Scene2Vintage'
import { Scene3RollRate } from '../scenes/Scene3RollRate'
import { Scene4Reserve } from '../scenes/Scene4Reserve'
import { Scene5SoWhat } from '../scenes/Scene5SoWhat'

/**
 * The original "day in the life" scrollytelling walkthrough — teaching-
 * oriented, content unchanged from before the multi-tab restructuring.
 * Every other tab is reference-oriented; this one stays a narrative. See
 * AGENTS.md's "Important distinction between the tabs" note.
 */
export function WalkthroughTab() {
  const { originations, distribution, vintage, rollRate, reserve, portfolioSummary, allLoaded, error } =
    useCreditRiskData()

  return (
    <>
      <header className="hero">
        <span className="eyebrow">LedgerOne — Credit Risk Practice Module</span>
        <h1>A day in the life of a credit risk analyst</h1>
        <p className="lede">
          A scroll-driven walkthrough of vintage analysis, roll-rate transition matrices, and a CECL-style reserve
          estimate — built on a synthetic consumer credit portfolio, using the same techniques that show up in real
          credit-risk / loss-forecasting analyst work.
        </p>
        <div className="hero-meta">
          <span>5 scenes</span>
          <span>Static data, no backend</span>
          <span>
            <a href="https://github.com/trustanprice/ledgerone" target="_blank" rel="noreferrer">
              Source: LedgerOne
            </a>
          </span>
        </div>
        <p className="scroll-cue">Scroll to begin ↓</p>
      </header>

      {error && (
        <div className="scene" style={{ borderTop: 'none' }}>
          <p style={{ color: 'var(--bad)' }}>
            Couldn't load walkthrough data ({error}). Run <code>python src/export_credit_risk_walkthrough_data.py</code>{' '}
            from the repo root, then rebuild this app.
          </p>
        </div>
      )}

      {!error && !allLoaded && (
        <div className="scene" style={{ borderTop: 'none' }}>
          <p className="chart-status">Loading…</p>
        </div>
      )}

      {allLoaded && (
        <>
          <Scene1Morning originations={originations!} distribution={distribution!} />
          <Scene2Vintage rows={vintage!} />
          <Scene3RollRate matrix={rollRate!} />
          <Scene4Reserve trajectory={reserve!} />
          <Scene5SoWhat distribution={distribution!} trajectory={reserve!} vintage={vintage!} portfolioSummary={portfolioSummary!} />
        </>
      )}

      {allLoaded && (
        <div className="scene" style={{ borderTop: 'none', paddingTop: 0 }}>
          <p style={{ color: 'var(--ink-secondary)', fontSize: '0.95rem' }}>
            Want to see where this goes next — regime-switching matrices instead of one static one, survival
            analysis, Monte Carlo loss simulation, beyond what this walkthrough covers? →{' '}
            <a href="#/emerging-techniques">Emerging Techniques</a>
          </p>
        </div>
      )}

      <footer className="site-footer">
        <p>
          Every number here traces back to a tested dbt model in{' '}
          <a href="https://github.com/trustanprice/ledgerone/tree/main/dbt/models/credit_risk" target="_blank" rel="noreferrer">
            dbt/models/credit_risk/
          </a>
          , built on synthetic data (no real accounts, no real losses) — see the{' '}
          <a
            href="https://github.com/trustanprice/ledgerone/blob/main/dbt/models/credit_risk/README.md"
            target="_blank"
            rel="noreferrer"
          >
            module README
          </a>{' '}
          for the full technical writeup this walkthrough is based on.
        </p>
      </footer>
    </>
  )
}
