import type { ReactNode } from 'react'

const KEYWORDS = new Set([
  'select', 'from', 'where', 'group', 'by', 'order', 'having', 'join', 'left', 'right', 'inner',
  'outer', 'on', 'as', 'with', 'recursive', 'union', 'all', 'case', 'when', 'then', 'else', 'end',
  'and', 'or', 'not', 'in', 'is', 'null', 'partition', 'over', 'window', 'rows', 'between',
  'unbounded', 'preceding', 'current', 'row', 'distinct', 'filter', 'desc', 'asc', 'limit',
])

// Lightweight regex tokenizer, not a full SQL parser: comments, strings,
// numbers, and a keyword list, everything else passed through verbatim.
// Deliberately not a dependency (Prism/Shiki/etc) — a handful of static
// reference snippets don't need a full tokenizer, and this keeps the
// bundle small.
function tokenize(sql: string): ReactNode[] {
  const pattern = /(--[^\n]*)|('(?:[^']|'')*')|(\b\d+\.?\d*\b)|([A-Za-z_][A-Za-z0-9_]*)|([\s\S])/g
  const nodes: ReactNode[] = []
  let match: RegExpExecArray | null
  let key = 0

  while ((match = pattern.exec(sql))) {
    const [, comment, str, num, word, other] = match
    if (comment) {
      nodes.push(
        <span className="sql-comment" key={key++}>
          {comment}
        </span>,
      )
    } else if (str) {
      nodes.push(
        <span className="sql-str" key={key++}>
          {str}
        </span>,
      )
    } else if (num) {
      nodes.push(
        <span className="sql-num" key={key++}>
          {num}
        </span>,
      )
    } else if (word) {
      if (KEYWORDS.has(word.toLowerCase())) {
        nodes.push(
          <span className="sql-kw" key={key++}>
            {word}
          </span>,
        )
      } else {
        nodes.push(word)
      }
    } else if (other) {
      nodes.push(other)
    }
  }
  return nodes
}

interface Props {
  title: string
  source: string
  sql: string
}

export function SqlBlock({ title, source, sql }: Props) {
  return (
    <div className="sql-block">
      <div className="sql-block-header">
        <span>{title}</span>
        <span>{source}</span>
      </div>
      <pre>
        <code>{tokenize(sql.trim())}</code>
      </pre>
    </div>
  )
}
