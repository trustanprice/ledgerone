interface Props {
  values: number[]
  width?: number
  height?: number
  color?: string
}

/**
 * A minimal inline SVG line+area sparkline — no axes, no library. Used on
 * the Overview tab to give tile previews a real shape instead of prose;
 * the data is the same precomputed static JSON as the rest of the site,
 * not a live feed, so this deliberately has no "updating" affordance.
 */
export function Sparkline({ values, width = 120, height = 32, color = 'var(--accent)' }: Props) {
  if (values.length < 2) return null

  const min = Math.min(...values)
  const max = Math.max(...values)
  const range = max - min || 1
  const stepX = width / (values.length - 1)

  const points = values.map((v, i) => {
    const x = i * stepX
    const y = height - ((v - min) / range) * height
    return [x, y] as const
  })

  const linePath = points.map(([x, y], i) => `${i === 0 ? 'M' : 'L'}${x.toFixed(1)},${y.toFixed(1)}`).join(' ')
  const areaPath = `${linePath} L${width},${height} L0,${height} Z`
  const [lastX, lastY] = points[points.length - 1]

  return (
    <svg width={width} height={height} viewBox={`0 0 ${width} ${height}`} className="sparkline" role="img" aria-hidden="true">
      <path d={areaPath} fill={color} fillOpacity={0.16} stroke="none" />
      <path d={linePath} fill="none" stroke={color} strokeWidth={1.75} strokeLinecap="round" strokeLinejoin="round" />
      <circle cx={lastX} cy={lastY} r={2.5} fill={color} />
    </svg>
  )
}
