import { Bar, BarChart, CartesianGrid, Legend, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts'
import type { OriginationCohortBand, ScoreBand } from '../../types'
import { SCORE_BAND_ORDER } from '../../types'
import { CHART_CHROME, sequentialBlue } from '../../theme'

interface Props {
  rows: OriginationCohortBand[]
}

// Worse-to-better score band maps onto the same severity ramp the rest of
// the site uses for magnitude, so "risk" reads the same way everywhere.
const BAND_COLOR: Record<ScoreBand, string> = {
  Subprime: sequentialBlue(1),
  'Near Prime': sequentialBlue(0.66),
  Prime: sequentialBlue(0.33),
  'Super Prime': sequentialBlue(0.08),
}

function pivot(rows: OriginationCohortBand[]) {
  const byQuarter = new Map<string, Record<string, number | string>>()
  for (const row of rows) {
    const existing = byQuarter.get(row.origination_quarter) ?? { quarter: row.origination_quarter }
    existing[row.score_band] = row.account_count
    byQuarter.set(row.origination_quarter, existing)
  }
  return Array.from(byQuarter.values()).sort((a, b) => String(a.quarter).localeCompare(String(b.quarter)))
}

export function OriginationsChart({ rows }: Props) {
  const data = pivot(rows)

  return (
    <div style={{ flex: 1, minHeight: 0 }}>
      <ResponsiveContainer width="100%" height="100%" minHeight={280}>
        <BarChart data={data} margin={{ top: 8, right: 8, left: 0, bottom: 0 }}>
          <CartesianGrid stroke={CHART_CHROME.grid} vertical={false} />
          <XAxis
            dataKey="quarter"
            tickFormatter={(v: string) => v.slice(0, 7)}
            tick={{ fontSize: 11, fill: CHART_CHROME.tick }}
            interval={2}
            axisLine={{ stroke: CHART_CHROME.axisLine }}
            tickLine={false}
          />
          <YAxis tick={{ fontSize: 11, fill: CHART_CHROME.tick }} axisLine={false} tickLine={false} width={34} />
          <Tooltip
            content={({ active, payload, label }) => {
              if (!active || !payload?.length) return null
              return (
                <div className="chart-tooltip">
                  <div className="tt-title">{String(label).slice(0, 7)}</div>
                  {payload.map((p) => (
                    <div key={String(p.dataKey)}>
                      {String(p.dataKey)}: {p.value}
                    </div>
                  ))}
                </div>
              )
            }}
          />
          <Legend
            content={() => (
              <div className="chart-legend">
                {SCORE_BAND_ORDER.map((band) => (
                  <span className="legend-item" key={band}>
                    <span className="legend-swatch" style={{ background: BAND_COLOR[band] }} />
                    {band}
                  </span>
                ))}
              </div>
            )}
          />
          {SCORE_BAND_ORDER.map((band) => (
            <Bar key={band} dataKey={band} stackId="originations" fill={BAND_COLOR[band]} />
          ))}
        </BarChart>
      </ResponsiveContainer>
    </div>
  )
}
