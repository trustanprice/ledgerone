import { Scene } from '../components/Scene'
import { ReserveForecastChart } from '../components/charts/ReserveForecastChart'
import type { ReserveForecastTrajectory } from '../types'

interface Props {
  trajectory: ReserveForecastTrajectory
}

export function Scene4Reserve({ trajectory }: Props) {
  const finalPoint = trajectory.points[trajectory.points.length - 1]
  const midpoint = trajectory.points[Math.floor(trajectory.points.length / 2)]

  const steps = [
    <>
      Here's where the transition matrix stops being descriptive and starts being <strong>predictive</strong>. Take
      the matrix from the last scene, blended into one average monthly behavior, and roll it forward — repeatedly
      multiply today's distribution of accounts across buckets by that matrix, one month at a time, for a{' '}
      {trajectory.horizon_months}-month horizon. This is a Markov chain roll-forward, computed here with a{' '}
      <code>WITH RECURSIVE</code> CTE.
    </>,
    <>
      By month {midpoint.horizon_month}, the projected expected loss has reached{' '}
      <strong>${midpoint.expected_loss_amount.toLocaleString(undefined, { maximumFractionDigits: 0 })}</strong>. Most
      of that is coming from accounts that are <em>already</em> delinquent today — a currently-Current account needs
      several consecutive bad months to reach charge-off, so its contribution builds in slowly. That's the whole
      reason Scene 1's small "currently delinquent" slice matters so much: it's disproportionately responsible for
      near-term losses.
    </>,
    <>
      At the full {trajectory.horizon_months}-month horizon: <strong>
        ${finalPoint.expected_loss_amount.toLocaleString(undefined, { maximumFractionDigits: 0 })}
      </strong>{' '}
      in expected charge-offs against ${finalPoint.outstanding_balance.toLocaleString(undefined, { maximumFractionDigits: 0 })}{' '}
      in outstanding balance — a blended reserve rate of{' '}
      <strong>{(finalPoint.reserve_rate * 100).toFixed(2)}%</strong>. That single number is, in miniature, what a
      CECL reserve estimate is: not "how much have we lost," but "how much do we reasonably expect to lose on the
      book we're carrying right now."
    </>,
    <>
      A real production version of this adds a lot on top: transition matrices segmented by risk band and vintage
      instead of one blended matrix, forward-looking macroeconomic overlays (CECL explicitly requires "reasonable
      and supportable" forecasts, not just historical averages), discounting, qualitative overlays, and a full model
      governance and validation process under SR 11-7. This module deliberately isolates just the core mechanic —
      knowing exactly what's simplified here, and why, is the useful part to be able to explain out loud.
    </>,
  ]

  return (
    <Scene
      id="scene-4"
      eyebrow="Scene 4 — Rolling it forward"
      title="The reserve forecast"
      steps={steps}
      renderVisual={(activeStep) => {
        const highlightMonth = activeStep === 1 ? midpoint.horizon_month : activeStep >= 2 ? finalPoint.horizon_month : null
        return <ReserveForecastChart points={trajectory.points} highlightMonth={highlightMonth} />
      }}
    />
  )
}
