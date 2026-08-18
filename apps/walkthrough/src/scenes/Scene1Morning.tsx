import { Scene } from '../components/Scene'
import { OriginationsChart } from '../components/charts/OriginationsChart'
import { BucketDistributionChart } from '../components/charts/BucketDistributionChart'
import type { BucketDistributionRow, OriginationCohortBand } from '../types'

interface Props {
  originations: OriginationCohortBand[]
  distribution: BucketDistributionRow[]
}

export function Scene1Morning({ originations, distribution }: Props) {
  const delinquentShare =
    distribution.reduce((sum, r) => (r.bucket === 'Current' ? sum : sum + r.account_count), 0) /
    distribution.reduce((sum, r) => sum + r.account_count, 0)

  const steps = [
    <>
      Before touching a spreadsheet, a credit risk analyst's first move is usually the same: pull up the book and look
      at it. <strong>LedgerOne's synthetic portfolio</strong> spans 12 quarterly origination cohorts — 2022-Q1 through
      2024-Q4 — so there's enough history for the analysis later in this walkthrough to actually mean something.
    </>,
    <>
      Each stacked bar is one origination quarter, split by the credit score band assigned at underwriting. The mix
      matters: a cohort skewed toward Subprime will behave very differently a year later than one skewed toward Super
      Prime, and that difference is exactly what the next scene digs into.
    </>,
    <>
      Now the same book, cut a different way — every active account's <strong>current delinquency bucket</strong>{' '}
      today. This is the single most-checked number in a risk analyst's morning: not "how did we do," but "where do
      we stand right now."
    </>,
    <>
      Only <strong>{(delinquentShare * 100).toFixed(1)}%</strong> of active accounts are delinquent at all. That's a
      reassuringly small slice — but it's also the slice this whole walkthrough is about. A small share of accounts
      drives almost all of the loss forecast, which is exactly why risk teams watch it so closely.
    </>,
  ]

  return (
    <Scene
      id="scene-1"
      eyebrow="Scene 1 — Morning"
      title="The book, at a glance"
      steps={steps}
      renderVisual={(activeStep) =>
        activeStep < 2 ? <OriginationsChart rows={originations} /> : <BucketDistributionChart rows={distribution} />
      }
    />
  )
}
