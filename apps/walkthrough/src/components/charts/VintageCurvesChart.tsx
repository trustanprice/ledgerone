import { CartesianGrid, Line, LineChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts'
import type { VintageCurveRow } from '../../types'
import { CHART_CHROME, cohortColor } from '../../theme'

interface Props {
  rows: VintageCurveRow[]
  highlightedCohort: string | null
}

function pivotByMonthsOnBook(rows: VintageCurveRow[], cohorts: string[]) {
  const byMonth = new Map<number, Record<string, number>>()
  for (const row of rows) {
    const existing = byMonth.get(row.months_on_book) ?? { months_on_book: row.months_on_book }
    existing[row.origination_quarter] = row.cumulative_charge_off_rate
    byMonth.set(row.months_on_book, existing)
  }
  void cohorts
  return Array.from(byMonth.values()).sort((a, b) => a.months_on_book - b.months_on_book)
}

export function VintageCurvesChart({ rows, highlightedCohort }: Props) {
  const cohorts = Array.from(new Set(rows.map((r) => r.origination_quarter))).sort()
  const data = pivotByMonthsOnBook(rows, cohorts)

  return (
    <div style={{ flex: 1, minHeight: 0 }}>
      <ResponsiveContainer width="100%" height="100%" minHeight={280}>
        <LineChart data={data} margin={{ top: 8, right: 8, left: 0, bottom: 0 }}>
          <CartesianGrid stroke={CHART_CHROME.grid} vertical={false} />
          <XAxis
            dataKey="months_on_book"
            tick={{ fontSize: 11, fill: CHART_CHROME.tick }}
            axisLine={{ stroke: CHART_CHROME.axisLine }}
            tickLine={false}
            label={{ value: 'Months on book', position: 'insideBottom', offset: -2, fontSize: 11, fill: CHART_CHROME.tick }}
          />
          <YAxis
            tickFormatter={(v: number) => `${(v * 100).toFixed(0)}%`}
            tick={{ fontSize: 11, fill: CHART_CHROME.tick }}
            axisLine={false}
            tickLine={false}
            width={38}
          />
          <Tooltip
            content={({ active, payload, label }) => {
              if (!active || !payload?.length) return null
              const sorted = [...payload].sort((a, b) => (b.value as number) - (a.value as number)).slice(0, 6)
              return (
                <div className="chart-tooltip">
                  <div className="tt-title">Month {label}</div>
                  {sorted.map((p) => (
                    <div key={p.dataKey as string}>
                      {(p.dataKey as string).slice(0, 7)}: {((p.value as number) * 100).toFixed(1)}%
                    </div>
                  ))}
                </div>
              )
            }}
          />
          {cohorts.map((cohort, i) => {
            const isHighlighted = highlightedCohort === cohort
            const isDimmed = highlightedCohort !== null && !isHighlighted
            return (
              <Line
                key={cohort}
                type="monotone"
                dataKey={cohort}
                stroke={cohortColor(i)}
                strokeWidth={isHighlighted ? 3 : 1.5}
                strokeOpacity={isDimmed ? 0.15 : 1}
                dot={false}
                isAnimationActive={false}
              />
            )
          })}
        </LineChart>
      </ResponsiveContainer>
      <div className="chart-legend">
        {cohorts.map((cohort, i) => (
          <span className="legend-item" key={cohort} style={{ opacity: highlightedCohort && highlightedCohort !== cohort ? 0.35 : 1 }}>
            <span className="legend-swatch" style={{ background: cohortColor(i) }} />
            {cohort.slice(0, 7)}
          </span>
        ))}
      </div>
    </div>
  )
}
