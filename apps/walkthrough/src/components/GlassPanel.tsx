import type { HTMLAttributes, ReactNode } from 'react'

interface Props extends HTMLAttributes<HTMLDivElement> {
  children: ReactNode
  elevated?: boolean
}

/** The one reusable glass-surface wrapper — see index.css `.glass-panel`. */
export function GlassPanel({ children, elevated, className = '', ...rest }: Props) {
  return (
    <div className={`glass-panel${elevated ? ' is-elevated' : ''} ${className}`.trim()} {...rest}>
      {children}
    </div>
  )
}
