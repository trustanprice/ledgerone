import { useCreditRiskData } from '../hooks/useCreditRiskData'
import { GlassPanel } from '../components/GlassPanel'
import { VintageCurvesChart } from '../components/charts/VintageCurvesChart'
import { RollRateHeatmap } from '../components/charts/RollRateHeatmap'
import { ReserveForecastChart } from '../components/charts/ReserveForecastChart'
import { BacktestCurveChart } from '../components/charts/BacktestCurveChart'
import { DataTable } from '../components/DataTable'
import type { BacktestCalibrationRow, PortfolioSummaryRow } from '../types'

const calibrationColumns = [
  { key: 'from', label: 'From', render: (r: BacktestCalibrationRow) => r.from_bucket },
  { key: 'to', label: 'To', render: (r: BacktestCalibrationRow) => r.to_bucket },
  { key: 'train', label: 'Training rate', numeric: true, render: (r: BacktestCalibrationRow) => `${(r.train_rate * 100).toFixed(1)}%` },
  { key: 'holdout', label: 'Holdout rate', numeric: true, render: (r: BacktestCalibrationRow) => `${(r.holdout_rate * 100).toFixed(1)}%` },
  {
    key: 'diff',
    label: 'Δ (pp)',
    numeric: true,
    render: (r: BacktestCalibrationRow) => (
      <span style={{ color: Math.abs(r.rate_diff_pp) >= 3 ? 'var(--bad)' : undefined }}>
        {r.rate_diff_pp > 0 ? '+' : ''}
        {r.rate_diff_pp.toFixed(2)}
      </span>
    ),
  },
]

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
  const { vintage, rollRate, reserve, portfolioSummary, backtest, allLoaded, error } = useCreditRiskData()

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
          Everything above proves the SQL is internally consistent — it doesn't prove the vintage-curve / roll-rate{' '}
          <em>methodology</em> actually forecasts real credit performance. This backtests it against a real, public
          loan-performance panel ({backtest!.data_source}) with a genuine time-based train/holdout split: fit on data
          through {backtest!.train_cutoff}, forecast the next {backtest!.horizon_months} months, compare against what
          actually happened. It's mortgage data, not credit cards, so the loss <em>levels</em> won't match a card
          portfolio — what transfers is whether the technique forecasts accurately, not the specific numbers. Full
          writeup: <code>dbt/models/credit_risk/README.md</code>'s Validation section.
        </p>
        <div className="stat-grid cols-4">
          <div className="stat-tile">
            <div className="stat-label">MAE</div>
            <div className="stat-value">{(backtest!.accuracy.mae * 100).toFixed(2)}pp</div>
            <div className="stat-sub">cumulative serious-delinquency forecast</div>
          </div>
          <div className="stat-tile">
            <div className="stat-label">MAPE</div>
            <div className="stat-value">{(backtest!.accuracy.mape * 100).toFixed(1)}%</div>
            <div className="stat-sub">noisy at these base rates — see below</div>
          </div>
          <div className="stat-tile">
            <div className="stat-label">Mean bias</div>
            <div className="stat-value">
              {backtest!.accuracy.bias_pp > 0 ? '+' : ''}
              {backtest!.accuracy.bias_pp.toFixed(2)}pp
            </div>
            <div className="stat-sub">{backtest!.accuracy.bias_pp > 0 ? 'over' : 'under'}-predicts, on average</div>
          </div>
          <div className="stat-tile">
            <div className="stat-label">Lifetime ever-90+</div>
            <div className="stat-value">{(backtest!.sanity_check.mortgage_lifetime_ever90plus_rate * 100).toFixed(2)}%</div>
            <div className="stat-sub">this vintage, full history</div>
          </div>
        </div>
      </section>

      <section className="ref-section">
        <h2>Predicted vs. actual, holdout window</h2>
        <p className="section-desc">
          Solid = what actually happened; dashed = what the training-window transition matrix, rolled forward,
          predicted. Five cohorts (2021-Q1 – 2022-Q1), each with up to {backtest!.horizon_months} months of genuine
          out-of-sample holdout.
        </p>
        <GlassPanel className="ref-panel" style={{ height: 380, display: 'flex', flexDirection: 'column' }}>
          <BacktestCurveChart rows={backtest!.curve} />
        </GlassPanel>
      </section>

      <section className="ref-section">
        <h2>Roll-rate calibration: training window vs. holdout</h2>
        <p className="section-desc">
          The same transition matrix, fit twice — once on training-period transitions, once on holdout-period
          transitions — compared cell by cell. This is the more interesting result than the MAE above: it shows{' '}
          <em>where</em> the forecast drifts, not just how much.
        </p>
        <div className="ref-grid">
          <GlassPanel className="ref-panel" style={{ height: 380, display: 'flex', flexDirection: 'column' }}>
            <h3>Training window (through {backtest!.train_cutoff})</h3>
            <RollRateHeatmap matrix={backtest!.train_matrix} highlight="none" />
          </GlassPanel>
          <GlassPanel className="ref-panel" style={{ height: 380, display: 'flex', flexDirection: 'column' }}>
            <h3>Holdout window (after {backtest!.train_cutoff})</h3>
            <RollRateHeatmap matrix={backtest!.holdout_matrix} highlight="none" />
          </GlassPanel>
        </div>
        <GlassPanel className="ref-panel" style={{ marginTop: '1.25rem' }}>
          <DataTable
            columns={calibrationColumns}
            rows={backtest!.calibration}
            getRowKey={(r) => `${r.from_bucket}|${r.to_bucket}`}
          />
        </GlassPanel>
      </section>

      <section className="ref-section">
        <h2>Directional sanity check (not a validation)</h2>
        <p className="section-desc">
          A mortgage portfolio's serious-delinquency rate against credit-card benchmarks — different products,
          different loss definitions, different risk profiles. Only direction and rough magnitude are meaningful
          here, not a precise match: this vintage ({(backtest!.sanity_check.mortgage_lifetime_ever90plus_rate * 100).toFixed(2)}%)
          sits below both FRED's aggregate card delinquency rate ({(backtest!.sanity_check.fred_card_delinquency_rate * 100).toFixed(1)}%)
          and Synchrony's disclosed net charge-off rate ({(backtest!.sanity_check.synchrony_nco_rate * 100).toFixed(2)}%),
          consistent with secured mortgage debt running lower-loss than unsecured revolving credit.
        </p>
        <p className="section-desc">
          <strong>Honest read: mostly yes, with one real, explainable gap.</strong> The core mechanic — LAG-based
          roll-rate matrix, Markov roll-forward, cumulative vintage tracking — forecast a real 50,000-loan mortgage
          vintage within about {(backtest!.accuracy.mae * 100).toFixed(2)} percentage points, out of sample. But the
          calibration comparison above shows the transition matrix isn't time-stable: cure rates out of early-stage
          delinquency measurably worsened between the training and holdout windows, a macro/rate-environment shift a
          single blended matrix has no way to see coming. That's not a bug — it's the textbook reason production
          CECL models carry a macro overlay instead of extrapolating one historical matrix forward indefinitely.
        </p>
      </section>
    </div>
  )
}
