import { Scene } from '../components/Scene'
import type { BucketDistributionRow, PortfolioSummaryRow, ReserveForecastTrajectory, VintageCurveRow } from '../types'

interface Props {
  distribution: BucketDistributionRow[]
  trajectory: ReserveForecastTrajectory
  vintage: VintageCurveRow[]
  portfolioSummary: PortfolioSummaryRow[]
}

function worstCohort(rows: VintageCurveRow[]) {
  const latest = new Map<string, VintageCurveRow>()
  for (const row of rows) {
    const current = latest.get(row.origination_quarter)
    if (!current || row.months_on_book > current.months_on_book) latest.set(row.origination_quarter, row)
  }
  return Array.from(latest.entries()).sort((a, b) => b[1].cumulative_charge_off_rate - a[1].cumulative_charge_off_rate)[0]
}

export function Scene5SoWhat({ distribution, trajectory, vintage, portfolioSummary }: Props) {
  const finalPoint = trajectory.points[trajectory.points.length - 1]
  const [worstCohortName, worstCohortRow] = worstCohort(vintage)
  const riskiestBand = [...portfolioSummary].sort((a, b) => b.lifetime_charge_off_rate - a.lifetime_charge_off_rate)[0]
  const totalDelinquent = distribution.reduce((s, r) => (r.bucket === 'Current' ? s : s + r.account_count), 0)
  const totalAccounts = distribution.reduce((s, r) => s + r.account_count, 0)

  const steps = [
    <>
      Pulling it together: this is the readout a risk analyst would actually bring to a stand-up or a monthly
      portfolio review — not the charts themselves, but the two or three sentences they support.
    </>,
    <>
      <strong>{worstCohortName.slice(0, 7)}</strong> is underperforming its peers, reaching{' '}
      {(worstCohortRow.cumulative_charge_off_rate * 100).toFixed(1)}% cumulative charge-off — worth flagging to
      underwriting as a candidate for a closer look at what changed that quarter.
    </>,
    <>
      <strong>{riskiestBand.origination_score_band}</strong> carries the highest lifetime charge-off rate at{' '}
      {(riskiestBand.lifetime_charge_off_rate * 100).toFixed(1)}% — a reminder that portfolio-level averages can hide
      meaningfully different risk by segment.
    </>,
    <>
      And the number that ties it all together: a projected{' '}
      <strong>{(finalPoint.reserve_rate * 100).toFixed(2)}% reserve rate</strong> over the next{' '}
      {trajectory.horizon_months} months, driven almost entirely by the{' '}
      {((totalDelinquent / totalAccounts) * 100).toFixed(1)}% of the book that's already delinquent today. That's
      the whole arc of this walkthrough in one sentence: a small, closely-watched slice of the book drives almost
      all of the forward-looking loss forecast.
    </>,
  ]

  return (
    <Scene
      id="scene-5"
      eyebrow="Scene 5 — Afternoon"
      title="So what"
      steps={steps}
      renderVisual={() => (
        <div className="stat-grid">
          <div className="stat-tile">
            <div className="stat-label">Worst-performing cohort</div>
            <div className="stat-value">{worstCohortName.slice(0, 7)}</div>
            <div className="stat-sub">{(worstCohortRow.cumulative_charge_off_rate * 100).toFixed(1)}% cumulative charge-off</div>
          </div>
          <div className="stat-tile">
            <div className="stat-label">Riskiest score band</div>
            <div className="stat-value">{riskiestBand.origination_score_band}</div>
            <div className="stat-sub">{(riskiestBand.lifetime_charge_off_rate * 100).toFixed(1)}% lifetime charge-off</div>
          </div>
          <div className="stat-tile">
            <div className="stat-label">Active book delinquent</div>
            <div className="stat-value">{((totalDelinquent / totalAccounts) * 100).toFixed(1)}%</div>
            <div className="stat-sub">{totalDelinquent.toLocaleString()} of {totalAccounts.toLocaleString()} accounts</div>
          </div>
          <div className="stat-tile">
            <div className="stat-label">{trajectory.horizon_months}-month reserve rate</div>
            <div className="stat-value">{(finalPoint.reserve_rate * 100).toFixed(2)}%</div>
            <div className="stat-sub">${finalPoint.expected_loss_amount.toLocaleString(undefined, { maximumFractionDigits: 0 })} expected loss</div>
          </div>
        </div>
      )}
    />
  )
}
