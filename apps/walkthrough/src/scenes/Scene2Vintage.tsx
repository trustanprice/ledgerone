import { Scene } from '../components/Scene'
import { VintageCurvesChart } from '../components/charts/VintageCurvesChart'
import type { VintageCurveRow } from '../types'

interface Props {
  rows: VintageCurveRow[]
}

function finalRatesByCohort(rows: VintageCurveRow[]) {
  const latest = new Map<string, VintageCurveRow>()
  for (const row of rows) {
    const current = latest.get(row.origination_quarter)
    if (!current || row.months_on_book > current.months_on_book) {
      latest.set(row.origination_quarter, row)
    }
  }
  return latest
}

export function Scene2Vintage({ rows }: Props) {
  const finals = finalRatesByCohort(rows)
  const sorted = Array.from(finals.entries()).sort(
    (a, b) => b[1].cumulative_charge_off_rate - a[1].cumulative_charge_off_rate,
  )
  const worst = sorted[0]
  const best = sorted[sorted.length - 1]

  const steps = [
    <>
      This is a <strong>vintage curve</strong>: each line is one origination cohort, plotted by cumulative charge-off
      rate against <strong>months on book</strong> — not calendar date. That distinction is the whole point of the
      technique. A cohort originated in 2024 hasn't had time to season yet, so comparing it to a 2022 cohort at the
      same calendar month would be comparing apples to a fetus. Comparing them at the same age is what actually
      tells you whether underwriting quality is improving or slipping.
    </>,
    <>
      The <strong>{worst[0].slice(0, 7)}</strong> cohort is the worst performer at comparable age, reaching{' '}
      <strong>{(worst[1].cumulative_charge_off_rate * 100).toFixed(1)}%</strong> cumulative charge-off. In a real
      shop, a curve pulling away from the pack like this is the first signal something changed — underwriting
      standards, the mix of score bands originated, or the macro environment those borrowers faced early in the
      loan's life.
    </>,
    <>
      By contrast <strong>{best[0].slice(0, 7)}</strong> is the cleanest cohort, at{' '}
      <strong>{(best[1].cumulative_charge_off_rate * 100).toFixed(1)}%</strong>. Seeing the full spread across
      cohorts — not just one average number — is what vintage analysis is actually for: it isolates whether a
      problem is portfolio-wide or concentrated in specific quarters.
    </>,
    <>
      Notice the curves flatten as they age — most of a cohort's lifetime losses show up in the first 18-24 months,
      then the survivors are mostly the ones who were always going to be fine. That shape (steep, then flattening)
      is a useful sanity check on its own: a curve that's still climbing steeply at month 36 usually means something
      is still actively going wrong.
    </>,
  ]

  return (
    <Scene
      id="scene-2"
      eyebrow="Scene 2 — Reviewing the book"
      title="Vintage curves"
      steps={steps}
      renderVisual={(activeStep) => {
        const highlightedCohort = activeStep === 1 ? worst[0] : activeStep === 2 ? best[0] : null
        return <VintageCurvesChart rows={rows} highlightedCohort={highlightedCohort} />
      }}
    />
  )
}
