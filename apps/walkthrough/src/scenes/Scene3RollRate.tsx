import { Scene } from '../components/Scene'
import { RollRateHeatmap, type HeatmapHighlight } from '../components/charts/RollRateHeatmap'
import type { RollRateMatrix } from '../types'

interface Props {
  matrix: RollRateMatrix
}

const HIGHLIGHT_BY_STEP: HeatmapHighlight[] = ['none', 'stable', 'roll-forward', 'cure', 'none']

export function Scene3RollRate({ matrix }: Props) {
  const steps = [
    <>
      This is the <strong>roll-rate transition matrix</strong> — the single most important table in this whole
      module, and the one exercise that shows up over and over in loss-forecasting take-home tests. Every row is a
      delinquency bucket an account could be in <em>this</em> month; every column is where it could land{' '}
      <em>next</em> month. Built with one pattern: <code>LAG(delinquency_bucket)</code> partitioned by account,
      ordered by month, compared against this month's bucket, then grouped and row-normalized so each row sums to
      100%.
    </>,
    <>
      Read the <strong>Current</strong> row: the vast majority of accounts stay Current month over month. That's
      expected and it's the whole reason a bank can extend credit profitably at all — most borrowers just pay their
      bill.
    </>,
    <>
      The cells above the diagonal are <strong>roll-forward</strong> transitions — an account getting worse.
      30-59 DPD rolling to 60-89, 60-89 to 90-119, and so on. These are the cells that ultimately feed charge-off,
      and they're the ones a reserve model has to take seriously even though they're individually small percentages.
    </>,
    <>
      The cells below the diagonal are <strong>cures</strong> — an account getting better without ever charging off.
      Notice cure probability drops the deeper an account is delinquent: it's realistic that a 30-59 DPD account
      cures fairly often, but a 90-119 DPD account curing all the way back is much rarer. That asymmetry is exactly
      what a real transition matrix should show.
    </>,
    <>
      One detail worth having on screen: the underlying transition probabilities in this synthetic data were{' '}
      <strong>deliberately retuned</strong> partway through building this module. The first draft's monthly
      "Current → 30-59 DPD" rate was small on paper but compounded, over three years of monthly history, into an
      unrealistic <strong>32.5% lifetime charge-off rate</strong>. Retuning it down produced a much more
      industry-plausible <strong>~5% blended lifetime charge-off rate</strong> — a good reminder that in loss
      forecasting, small monthly assumptions compound hard, and it's worth sanity-checking the compounded result,
      not just the monthly input.
    </>,
  ]

  return (
    <Scene
      id="scene-3"
      eyebrow="Scene 3 — The core task"
      title="Roll-rate transition matrix"
      steps={steps}
      renderVisual={(activeStep) => (
        <>
          <RollRateHeatmap matrix={matrix} highlight={HIGHLIGHT_BY_STEP[activeStep] ?? 'none'} />
          {activeStep === 4 && (
            <p className="callout">
              Retuned from an unrealistic 32.5% to a realistic ~5% blended lifetime charge-off rate — see the module
              README for the full before/after.
            </p>
          )}
        </>
      )}
    />
  )
}
