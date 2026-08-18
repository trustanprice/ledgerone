import type { ReactNode } from 'react'

export interface StatStripItem {
  label: string
  value: ReactNode
  sub?: ReactNode
}

/**
 * A dense ticker row — small label, bold tabular figure, optional sub-line
 * — not individually-boxed stat cards. This is the "landing on the
 * homepage and seeing real numbers immediately" strip; see Overview tab.
 */
export function StatStrip({ items }: { items: StatStripItem[] }) {
  return (
    <div className="stat-strip glass-panel">
      {items.map((item, i) => (
        <div className="stat-strip-item" key={i}>
          <div className="stat-strip-label">{item.label}</div>
          <div className="stat-strip-value mono-nums">{item.value}</div>
          {item.sub && <div className="stat-strip-sub">{item.sub}</div>}
        </div>
      ))}
    </div>
  )
}
