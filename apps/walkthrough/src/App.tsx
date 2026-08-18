import { Nav } from './components/Nav'
import { useHashTab } from './hooks/useHashTab'
import { OverviewTab } from './tabs/OverviewTab'
import { WalkthroughTab } from './tabs/WalkthroughTab'

function App() {
  const [tab, setTab] = useHashTab()

  return (
    <>
      <Nav active={tab} onSelect={setTab} />
      {tab === 'overview' && <OverviewTab onNavigate={setTab} />}
      {tab === 'walkthrough' && <WalkthroughTab />}
    </>
  )
}

export default App
