import { useJsonData } from './hooks/useJsonData'
import { Scene1Morning } from './scenes/Scene1Morning'
import { Scene2Vintage } from './scenes/Scene2Vintage'
import { Scene3RollRate } from './scenes/Scene3RollRate'
import { Scene4Reserve } from './scenes/Scene4Reserve'
import { Scene5SoWhat } from './scenes/Scene5SoWhat'
import type {
  BucketDistributionRow,
  OriginationCohortBand,
  PortfolioSummaryRow,
  ReserveForecastTrajectory,
  RollRateMatrix,
  VintageCurveRow,
} from './types'

function App() {
  const originations = useJsonData<OriginationCohortBand[]>('originations_by_cohort_and_band.json')
  const distribution = useJsonData<BucketDistributionRow[]>('current_bucket_distribution.json')
  const vintage = useJsonData<VintageCurveRow[]>('vintage_curves.json')
  const rollRate = useJsonData<RollRateMatrix>('roll_rate_matrix.json')
  const reserve = useJsonData<ReserveForecastTrajectory>('reserve_forecast_trajectory.json')
  const portfolioSummary = useJsonData<PortfolioSummaryRow[]>('portfolio_summary_by_score_band.json')

  const allLoaded =
    originations.data && distribution.data && vintage.data && rollRate.data && reserve.data && portfolioSummary.data

  const anyError =
    originations.error || distribution.error || vintage.error || rollRate.error || reserve.error || portfolioSummary.error

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

      {anyError && (
        <div className="scene" style={{ borderTop: 'none' }}>
          <p style={{ color: '#E45756' }}>
            Couldn't load walkthrough data ({anyError}). Run <code>python src/export_credit_risk_walkthrough_data.py</code>{' '}
            from the repo root, then rebuild this app.
          </p>
        </div>
      )}

      {!anyError && !allLoaded && (
        <div className="scene" style={{ borderTop: 'none' }}>
          <p className="chart-status">Loading…</p>
        </div>
      )}

      {allLoaded && (
        <>
          <Scene1Morning originations={originations.data!} distribution={distribution.data!} />
          <Scene2Vintage rows={vintage.data!} />
          <Scene3RollRate matrix={rollRate.data!} />
          <Scene4Reserve trajectory={reserve.data!} />
          <Scene5SoWhat
            distribution={distribution.data!}
            trajectory={reserve.data!}
            vintage={vintage.data!}
            portfolioSummary={portfolioSummary.data!}
          />
        </>
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

export default App
