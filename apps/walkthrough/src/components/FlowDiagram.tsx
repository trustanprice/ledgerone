import { Fragment } from 'react'

export interface FlowNode {
  label: string
  kicker?: string
  accent?: boolean
}

export interface FlowColumn {
  label: string
  nodes: FlowNode[]
}

interface Props {
  columns: FlowColumn[]
}

/**
 * A generic left-to-right box-and-arrow diagram — used for both the dbt
 * lineage view (Data Engineering tab) and the infra architecture view
 * (Infrastructure tab). Hand-built rather than mermaid.js or a diagramming
 * library: these are static, small (4-6 columns), and a plain CSS-grid
 * box layout renders instantly with zero extra bundle weight, unlike
 * pulling in a ~500kb+ diagram-rendering library for content that never
 * changes at runtime.
 */
export function FlowDiagram({ columns }: Props) {
  return (
    <div className="diagram-wrap">
      <div className="diagram-row">
        {columns.map((col, i) => (
          <Fragment key={col.label}>
            <div className="diagram-col">
              <div className="diagram-group-label">{col.label}</div>
              {col.nodes.map((node) => (
                <div key={node.label} className={`diagram-node${node.accent ? ' is-accent' : ''}`}>
                  {node.kicker && <span className="node-kicker">{node.kicker}</span>}
                  {node.label}
                </div>
              ))}
            </div>
            {i < columns.length - 1 && (
              <span className="diagram-arrow" aria-hidden="true">
                →
              </span>
            )}
          </Fragment>
        ))}
      </div>
    </div>
  )
}
