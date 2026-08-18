import { useCreditRiskData } from '../hooks/useCreditRiskData'
import { GlassPanel } from '../components/GlassPanel'
import { VintageCurvesChart } from '../components/charts/VintageCurvesChart'
import { RollRateHeatmap } from '../components/charts/RollRateHeatmap'
import { ReserveForecastChart } from '../components/charts/ReserveForecastChart'
import { DataTable } from '../components/DataTable'
import type { PortfolioSummaryRow } from '../types'

const summaryColumns = [
  { key: 'band', label: 'Score band', render: (r: PortfolioSummaryRow) => r.origination_score_band },
  { key: 'accounts', label: 'Accounts', numeric: true, render: (r: PortfolioSummaryRow) => r.account_count.toLocaleString() },
  {
    key: 'balance',
    label: 'Outstanding balance',
    numeric: true,
    render: (r: PortfolioSummaryRow) => `$${r.outstanding_balance.toLocaleString(undefined, { maximumFractionDigits: 0 })}`,
  },
  {
    key: 'delinq',
    label: 'Current delinquency rate',
    numeric: true,
    render: (r: PortfolioSummaryRow) => `${(r.current_delinquency_rate * 100).toFixed(2)}%`,
  },
  {
    key: 'co',
    label: 'Lifetime charge-off rate',
    numeric: true,
    render: (r: PortfolioSummaryRow) => `${(r.lifetime_charge_off_rate * 100).toFixed(2)}%`,
  },
  { key: 'apr', label: 'Avg APR', numeric: true, render: (r: PortfolioSummaryRow) => `${r.avg_apr.toFixed(2)}%` },
]

export function ForecastingTab() {
  const { vintage, rollRate, reserve, portfolioSummary, allLoaded, error } = useCreditRiskData()

  if (error) {
    return (
      <div className="tab-page">
        <p style={{ color: 'var(--bad)', marginTop: '2rem' }}>Couldn't load data ({error}).</p>
      </div>
    )
  }

  if (!allLoaded) {
    return (
      <div className="tab-page">
        <p className="chart-status" style={{ marginTop: '2rem' }}>
          Loading…
        </p>
      </div>
    )
  }

  const finalReserve = reserve!.points[reserve!.points.length - 1]

  return (
    <div className="tab-page">
      <div className="tab-header">
        <span className="eyebrow">Reference</span>
        <h1>Forecasting & Data Science</h1>
        <p className="lede">
          The model outputs, dense and unfiltered — every cohort visible at once, not revealed scene by scene. This
          is the "how good is this, actually" view: the same underlying data as the walkthrough tab, presented as a
          reference dashboard instead of a narrative.
        </p>
      </div>

      <section className="ref-section">
        <h2>Vintage curves — all cohorts</h2>
        <p className="section-desc">Cumulative charge-off rate by origination cohort × months-on-book, no highlighting.</p>
        <GlassPanel className="ref-panel" style={{ height: 420, display: 'flex', flexDirection: 'column' }}>
          <VintageCurvesChart rows={vintage!} highlightedCohort={null} />
        </GlassPanel>
      </section>

      <section className="ref-section">
        <h2>Roll-rate transition matrix</h2>
        <p className="section-desc">Blended across the full observation window. Hover a cell for the exact count.</p>
        <GlassPanel className="ref-panel" style={{ height: 420, display: 'flex', flexDirection: 'column' }}>
          <RollRateHeatmap matrix={rollRate!} highlight="none" />
        </GlassPanel>
      </section>

      <section className="ref-section">
        <h2>CECL reserve forecast</h2>
        <p className="section-desc">
          {reserve!.horizon_months}-month roll-forward. Final: ${finalReserve.expected_loss_amount.toLocaleString(undefined, { maximumFractionDigits: 0 })}{' '}
          expected loss, {(finalReserve.reserve_rate * 100).toFixed(2)}% blended reserve rate.
        </p>
        <GlassPanel className="ref-panel" style={{ height: 380, display: 'flex', flexDirection: 'column' }}>
          <ReserveForecastChart points={reserve!.points} highlightMonth={null} />
        </GlassPanel>
      </section>

      <section className="ref-section">
        <h2>Portfolio summary by score band</h2>
        <p className="section-desc">Current composition and lifetime performance, cut by origination score band.</p>
        <GlassPanel className="ref-panel">
          <DataTable columns={summaryColumns} rows={portfolioSummary!} getRowKey={(r) => r.origination_score_band} />
        </GlassPanel>
      </section>

      <section className="ref-section">
        <h2>Backtest / validation against real loan data</h2>
        <p className="section-desc">
          Not yet available. Everything above is validated for internal consistency (dbt tests, sanity checks against
          the transition matrix) but has not been backtested against real, historical loan-performance data — that
          validation work is a separate, planned module (time-based train/holdout split against a public
          loan-performance dataset, MAE/MAPE on the vintage forecast, a roll-rate calibration comparison). This
          section will report those numbers once that module lands rather than imply an accuracy result that doesn't
          exist yet.
        </p>
      </section>
    </div>
  )
}
