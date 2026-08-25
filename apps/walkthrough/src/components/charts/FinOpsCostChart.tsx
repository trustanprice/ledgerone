import { Bar, BarChart, Cell, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts'
import type { FinOpsServiceCost } from '../../types'
import { CHART_CHROME, sequentialBlue } from '../../theme'

interface Props {
  rows: FinOpsServiceCost[]
}

const money = (v: number) => (v < 0.01 && v > 0 ? '<$0.01' : `$${v.toFixed(2)}`)

/**
 * Cost by AWS service, ranked highest first -- same shape and same
 * sequentialBlue ramp as BucketDistributionChart (a single magnitude
 * series, ranked, not a multi-entity comparison), just a different
 * domain.
 */
export function FinOpsCostChart({ rows }: Props) {
  const ordered = [...rows].sort((a, b) => b.amount - a.amount)

  return (
    <div style={{ flex: 1, minHeight: 0 }}>
      <ResponsiveContainer width="100%" height="100%" minHeight={220}>
        <BarChart data={ordered} layout="vertical" margin={{ top: 8, right: 24, left: 8, bottom: 0 }}>
          <XAxis type="number" tickFormatter={money} tick={{ fontSize: 11, fill: CHART_CHROME.tick }} axisLine={false} tickLine={false} />
          <YAxis
            type="category"
            dataKey="service"
            width={140}
            tick={{ fontSize: 11, fill: CHART_CHROME.tick }}
            axisLine={false}
            tickLine={false}
          />
          <Tooltip
            content={({ active, payload }) => {
              if (!active || !payload?.length) return null
              const row = payload[0].payload as FinOpsServiceCost
              return (
                <div className="chart-tooltip">
                  <div className="tt-title">{row.service}</div>
                  <div>{money(row.amount)}</div>
                </div>
              )
            }}
          />
          <Bar dataKey="amount" radius={[0, 4, 4, 0]}>
            {ordered.map((row, i) => (
              <Cell key={row.service} fill={sequentialBlue(1 - i / Math.max(ordered.length - 1, 1))} />
            ))}
          </Bar>
        </BarChart>
      </ResponsiveContainer>
    </div>
  )
}
