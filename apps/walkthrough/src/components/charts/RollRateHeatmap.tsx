import { useState } from 'react'
import type { DelinquencyBucket, RollRateCell, RollRateMatrix } from '../../types'
import { BUCKET_ORDER } from '../../types'
import { sequentialBlue } from '../../theme'

export type HeatmapHighlight = 'none' | 'stable' | 'roll-forward' | 'cure'

interface Props {
  matrix: RollRateMatrix
  highlight: HeatmapHighlight
}

const severityIndex = (bucket: DelinquencyBucket) => BUCKET_ORDER.indexOf(bucket)

function matchesHighlight(cell: RollRateCell, mode: HeatmapHighlight): boolean {
  if (mode === 'none') return false
  const fromIdx = severityIndex(cell.from_bucket)
  const toIdx = severityIndex(cell.to_bucket)
  if (mode === 'stable') return fromIdx === toIdx
  if (mode === 'roll-forward') return toIdx > fromIdx
  return toIdx < fromIdx // 'cure'
}

/**
 * A from-bucket x to-bucket transition matrix as a colored grid — the
 * centerpiece visualization of the whole site. Built as a CSS grid of
 * cells rather than pulling in D3 or a matrix-charting library: it's
 * fundamentally a colored, labeled table, and a CSS grid gives full
 * control over per-cell hover/highlight states and responsive reflow for
 * free.
 */
export function RollRateHeatmap({ matrix, highlight }: Props) {
  const [hovered, setHovered] = useState<RollRateCell | null>(null)
  const cellMap = new Map(matrix.cells.map((c) => [`${c.from_bucket}|${c.to_bucket}`, c]))

  return (
    <div className="heatmap-wrap">
      <div
        className="heatmap-grid"
        style={{ gridTemplateColumns: `72px repeat(${matrix.to_bucket_order.length}, minmax(0, 1fr))` }}
      >
        <div className="heatmap-corner" />
        {matrix.to_bucket_order.map((to) => (
          <div key={to} className="heatmap-col-label">
            {to}
          </div>
        ))}
        {matrix.from_bucket_order.map((from) => (
          <FromRow key={from} from={from} matrix={matrix} cellMap={cellMap} highlight={highlight} onHover={setHovered} />
        ))}
      </div>

      <div className="heatmap-footer">
        <div className="heatmap-scale">
          <span>0%</span>
          <div className="heatmap-scale-bar" />
          <span>100%</span>
        </div>
        <div className="chart-tooltip heatmap-tooltip" style={{ visibility: hovered ? 'visible' : 'hidden' }}>
          {hovered && (
            <>
              <div className="tt-title">
                {hovered.from_bucket} → {hovered.to_bucket}
              </div>
              <div>
                {(hovered.pct * 100).toFixed(1)}% of accounts ({hovered.account_count.toLocaleString()})
              </div>
            </>
          )}
        </div>
      </div>
    </div>
  )
}

function FromRow({
  from,
  matrix,
  cellMap,
  highlight,
  onHover,
}: {
  from: DelinquencyBucket
  matrix: RollRateMatrix
  cellMap: Map<string, RollRateCell>
  highlight: HeatmapHighlight
  onHover: (cell: RollRateCell | null) => void
}) {
  return (
    <>
      <div className="heatmap-row-label">{from}</div>
      {matrix.to_bucket_order.map((to) => {
        const cell = cellMap.get(`${from}|${to}`)
        const pct = cell?.pct ?? 0
        const isMatch = cell ? matchesHighlight(cell, highlight) : false
        const isDimmed = highlight !== 'none' && cell !== undefined && !isMatch
        const textColor = pct > 0.55 ? '#FFFFFF' : '#1A1D23'

        return (
          <div
            key={to}
            className={`heatmap-cell${isMatch ? ' is-highlighted' : ''}${isDimmed ? ' is-dimmed' : ''}`}
            style={{ background: cell ? sequentialBlue(pct) : 'transparent', color: textColor }}
            onMouseEnter={() => cell && onHover(cell)}
            onMouseLeave={() => onHover(null)}
          >
            {cell ? `${(pct * 100).toFixed(1)}%` : '—'}
          </div>
        )
      })}
    </>
  )
}
