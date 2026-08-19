import { CartesianGrid, Line, LineChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts'
import type { BacktestCurvePoint } from '../../types'
import { CHART_CHROME, cohortColor } from '../../theme'

interface Props {
  rows: BacktestCurvePoint[]
}

function pivotByHorizonMonth(rows: BacktestCurvePoint[], cohorts: string[]) {
  const byHorizon = new Map<number, Record<string, number>>()
  for (const row of rows) {
    const existing = byHorizon.get(row.horizon_month) ?? { horizon_month: row.horizon_month }
    existing[`${row.cohort}__actual`] = row.actual
    existing[`${row.cohort}__predicted`] = row.predicted
    byHorizon.set(row.horizon_month, existing)
  }
  void cohorts
  return Array.from(byHorizon.values()).sort((a, b) => a.horizon_month - b.horizon_month)
}

/**
 * Actual (solid) vs. predicted (dashed) cumulative serious-delinquency rate
 * per cohort, over months into the holdout window. X axis is horizon month
 * (1..N since the cutoff) rather than months-on-book, since cohorts hit
 * their train/holdout cutoff at different months-on-book (they were
 * originated at different times) — horizon month is the one axis every
 * cohort shares, so the overlay lines up cleanly.
 */
export function BacktestCurveChart({ rows }: Props) {
  const cohorts = Array.from(new Set(rows.map((r) => r.cohort))).sort()
  const data = pivotByHorizonMonth(rows, cohorts)

  return (
    <div style={{ flex: 1, minHeight: 0 }}>
      <ResponsiveContainer width="100%" height="100%" minHeight={280}>
        <LineChart data={data} margin={{ top: 8, right: 8, left: 0, bottom: 0 }}>
          <CartesianGrid stroke={CHART_CHROME.grid} vertical={false} />
          <XAxis
            dataKey="horizon_month"
            tick={{ fontSize: 11, fill: CHART_CHROME.tick }}
            axisLine={{ stroke: CHART_CHROME.axisLine }}
            tickLine={false}
            label={{ value: 'Months into holdout', position: 'insideBottom', offset: -2, fontSize: 11, fill: CHART_CHROME.tick }}
          />
          <YAxis
            tickFormatter={(v: number) => `${(v * 100).toFixed(1)}%`}
            tick={{ fontSize: 11, fill: CHART_CHROME.tick }}
            axisLine={false}
            tickLine={false}
            width={44}
          />
          <Tooltip
            content={({ active, payload, label }) => {
              if (!active || !payload?.length) return null
              return (
                <div className="chart-tooltip">
                  <div className="tt-title">Month {label} into holdout</div>
                  {cohorts.map((cohort) => {
                    const point = payload.find((p) => p.dataKey === `${cohort}__actual`)?.payload as
                      | Record<string, number>
                      | undefined
                    if (!point) return null
                    const actual = point[`${cohort}__actual`]
                    const predicted = point[`${cohort}__predicted`]
                    if (actual === undefined) return null
                    return (
                      <div key={cohort}>
                        {cohort.slice(0, 7)}: actual {(actual * 100).toFixed(2)}% · predicted {(predicted * 100).toFixed(2)}%
                      </div>
                    )
                  })}
                </div>
              )
            }}
          />
          {cohorts.map((cohort, i) => (
            <Line
              key={`${cohort}-actual`}
              type="monotone"
              dataKey={`${cohort}__actual`}
              stroke={cohortColor(i)}
              strokeWidth={2}
              dot={false}
              isAnimationActive={false}
            />
          ))}
          {cohorts.map((cohort, i) => (
            <Line
              key={`${cohort}-predicted`}
              type="monotone"
              dataKey={`${cohort}__predicted`}
              stroke={cohortColor(i)}
              strokeWidth={1.5}
              strokeDasharray="4 3"
              dot={false}
              isAnimationActive={false}
            />
          ))}
        </LineChart>
      </ResponsiveContainer>
      <div className="chart-legend">
        {cohorts.map((cohort, i) => (
          <span className="legend-item" key={cohort}>
            <span className="legend-swatch" style={{ background: cohortColor(i) }} />
            {cohort.slice(0, 7)}
          </span>
        ))}
        <span className="legend-item" style={{ opacity: 0.7 }}>
          — actual &nbsp;·&nbsp; ⋯ predicted
        </span>
      </div>
    </div>
  )
}
