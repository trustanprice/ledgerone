export type TabId = 'overview' | 'walkthrough' | 'data-engineering' | 'forecasting' | 'infrastructure'

export interface TabDef {
  id: TabId
  label: string
  navLabel: string
}

export const TABS: TabDef[] = [
  { id: 'overview', label: 'Overview', navLabel: 'Overview' },
  { id: 'walkthrough', label: 'Day in the Life', navLabel: 'Day in the Life' },
  { id: 'data-engineering', label: 'Database & Data Engineering', navLabel: 'Data Engineering' },
  { id: 'forecasting', label: 'Forecasting & Data Science', navLabel: 'Forecasting' },
  { id: 'infrastructure', label: 'Infrastructure & Productionization', navLabel: 'Infrastructure' },
]

export const DEFAULT_TAB: TabId = 'overview'

export function isTabId(value: string): value is TabId {
  return TABS.some((t) => t.id === value)
}
