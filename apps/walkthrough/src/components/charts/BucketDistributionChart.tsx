import { Bar, BarChart, Cell, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts'
import type { BucketDistributionRow } from '../../types'
import { BUCKET_ORDER } from '../../types'
import { CHART_CHROME, sequentialBlue } from '../../theme'

interface Props {
  rows: BucketDistributionRow[]
}

export function BucketDistributionChart({ rows }: Props) {
  const ordered = BUCKET_ORDER.filter((b) => b !== '120+/Charged-Off')
    .map((bucket) => rows.find((r) => r.bucket === bucket))
    .filter((r): r is BucketDistributionRow => Boolean(r))

  return (
    <div style={{ flex: 1, minHeight: 0 }}>
      <ResponsiveContainer width="100%" height="100%" minHeight={280}>
        <BarChart data={ordered} layout="vertical" margin={{ top: 8, right: 24, left: 8, bottom: 0 }}>
          <XAxis type="number" tick={{ fontSize: 11, fill: CHART_CHROME.tick }} axisLine={false} tickLine={false} />
          <YAxis
            type="category"
            dataKey="bucket"
            width={78}
            tick={{ fontSize: 11, fill: CHART_CHROME.tick }}
            axisLine={false}
            tickLine={false}
          />
          <Tooltip
            content={({ active, payload }) => {
              if (!active || !payload?.length) return null
              const row = payload[0].payload as BucketDistributionRow
              return (
                <div className="chart-tooltip">
                  <div className="tt-title">{row.bucket}</div>
                  <div>{row.account_count.toLocaleString()} accounts</div>
                  <div>${row.outstanding_balance.toLocaleString(undefined, { maximumFractionDigits: 0 })} balance</div>
                </div>
              )
            }}
          />
          <Bar dataKey="account_count" radius={[0, 4, 4, 0]}>
            {ordered.map((row, i) => (
              <Cell key={row.bucket} fill={sequentialBlue(i / Math.max(ordered.length - 1, 1))} />
            ))}
          </Bar>
        </BarChart>
      </ResponsiveContainer>
      <p className="chart-footnote">Active book only (excludes accounts already charged off).</p>
    </div>
  )
}
