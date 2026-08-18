import type { ReactNode } from 'react'

export interface DataTableColumn<T> {
  key: string
  label: string
  numeric?: boolean
  render: (row: T) => ReactNode
}

interface Props<T> {
  columns: DataTableColumn<T>[]
  rows: T[]
  caption?: string
  getRowKey: (row: T) => string
}

export function DataTable<T>({ columns, rows, caption, getRowKey }: Props<T>) {
  return (
    <div className="data-table-wrap">
      <table className="data-table">
        {caption && <caption>{caption}</caption>}
        <thead>
          <tr>
            {columns.map((col) => (
              <th key={col.key} style={col.numeric ? { textAlign: 'right' } : undefined}>
                {col.label}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((row) => (
            <tr key={getRowKey(row)}>
              {columns.map((col) => (
                <td key={col.key} className={col.numeric ? 'is-numeric' : undefined}>
                  {col.render(row)}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}
