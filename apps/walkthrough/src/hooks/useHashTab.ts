import { useEffect, useState } from 'react'
import { DEFAULT_TAB, isTabId, type TabId } from '../tabConfig'

function readTabFromHash(): TabId {
  const raw = window.location.hash.replace(/^#\/?/, '')
  return isTabId(raw) ? raw : DEFAULT_TAB
}

/**
 * URL-hash-based tab state — no router library. Hash routing (not the
 * History API) is deliberate: this is a static site with two deploy
 * targets (GitHub Pages project subpath, Vercel root), and hash changes
 * never hit the server, so there's no server-side rewrite/fallback
 * config needed on either host for a direct link to a tab to work.
 */
export function useHashTab(): [TabId, (tab: TabId) => void] {
  const [tab, setTabState] = useState<TabId>(readTabFromHash)

  useEffect(() => {
    const onHashChange = () => setTabState(readTabFromHash())
    window.addEventListener('hashchange', onHashChange)
    return () => window.removeEventListener('hashchange', onHashChange)
  }, [])

  const setTab = (next: TabId) => {
    window.location.hash = `/${next}`
    setTabState(next)
    window.scrollTo({ top: 0 })
  }

  return [tab, setTab]
}
