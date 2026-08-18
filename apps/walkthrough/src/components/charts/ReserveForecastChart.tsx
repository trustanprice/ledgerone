import { Area, AreaChart, CartesianGrid, ReferenceDot, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts'
import type { ReserveForecastPoint } from '../../types'
import { CHART_CHROME, COLORS } from '../../theme'

interface Props {
  points: ReserveForecastPoint[]
  highlightMonth: number | null
}

const money = (v: number) => `$${(v / 1000).toFixed(0)}k`

export function ReserveForecastChart({ points, highlightMonth }: Props) {
  const highlighted = highlightMonth !== null ? points.find((p) => p.horizon_month === highlightMonth) : undefined

  return (
    <div style={{ flex: 1, minHeight: 0 }}>
      <ResponsiveContainer width="100%" height="100%" minHeight={280}>
        <AreaChart data={points} margin={{ top: 8, right: 12, left: 0, bottom: 0 }}>
          <defs>
            <linearGradient id="reserveFill" x1="0" y1="0" x2="0" y2="1">
              <stop offset="0%" stopColor={COLORS.accent} stopOpacity={0.35} />
              <stop offset="100%" stopColor={COLORS.accent} stopOpacity={0.03} />
            </linearGradient>
          </defs>
          <CartesianGrid stroke={CHART_CHROME.grid} vertical={false} />
          <XAxis
            dataKey="horizon_month"
            tick={{ fontSize: 11, fill: CHART_CHROME.tick }}
            axisLine={{ stroke: CHART_CHROME.axisLine }}
            tickLine={false}
            label={{ value: 'Months forward', position: 'insideBottom', offset: -2, fontSize: 11, fill: CHART_CHROME.tick }}
          />
          <YAxis tickFormatter={money} tick={{ fontSize: 11, fill: CHART_CHROME.tick }} axisLine={false} tickLine={false} width={44} />
          <Tooltip
            content={({ active, payload }) => {
              if (!active || !payload?.length) return null
              const p = payload[0].payload as ReserveForecastPoint
              return (
                <div className="chart-tooltip">
                  <div className="tt-title">Month {p.horizon_month}</div>
                  <div>Expected loss: ${p.expected_loss_amount.toLocaleString(undefined, { maximumFractionDigits: 0 })}</div>
                  <div>Reserve rate: {(p.reserve_rate * 100).toFixed(2)}%</div>
                </div>
              )
            }}
          />
          <Area
            type="monotone"
            dataKey="expected_loss_amount"
            stroke={COLORS.accent}
            strokeWidth={2}
            fill="url(#reserveFill)"
            isAnimationActive={false}
          />
          {highlighted && (
            <ReferenceDot
              x={highlighted.horizon_month}
              y={highlighted.expected_loss_amount}
              r={5}
              fill={COLORS.accent}
              stroke="#fff"
              strokeWidth={2}
            />
          )}
        </AreaChart>
      </ResponsiveContainer>
    </div>
  )
}
