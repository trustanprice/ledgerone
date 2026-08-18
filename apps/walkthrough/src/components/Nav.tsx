import { TABS, type TabId } from '../tabConfig'

interface Props {
  active: TabId
  onSelect: (tab: TabId) => void
}

export function Nav({ active, onSelect }: Props) {
  return (
    <nav className="site-nav">
      <span className="nav-brand">
        <span className="brand-dot" aria-hidden="true" />
        LedgerOne <span style={{ color: 'var(--ink-tertiary)', fontWeight: 500 }}>/ Credit Risk</span>
      </span>
      <div className="nav-tabs" role="tablist" aria-label="Site sections">
        {TABS.map((tab) => (
          <button
            key={tab.id}
            role="tab"
            aria-selected={active === tab.id}
            className={`nav-tab${active === tab.id ? ' is-active' : ''}`}
            onClick={() => onSelect(tab.id)}
          >
            {tab.navLabel}
          </button>
        ))}
      </div>
    </nav>
  )
}
