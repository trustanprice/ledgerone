import { describe, expect, it, beforeEach } from 'vitest'
import { render, screen } from '@testing-library/react'
import { installFetchMock } from '../test/fixtures'
import { OverviewTab } from './OverviewTab'
import { WalkthroughTab } from './WalkthroughTab'
import { DataEngineeringTab } from './DataEngineeringTab'
import { ForecastingTab } from './ForecastingTab'
import { InfrastructureTab } from './InfrastructureTab'
import { EmergingTechniquesTab } from './EmergingTechniquesTab'

// One smoke test per tab: mount it against the real, checked-in
// public/data/*.json fixtures (via installFetchMock) and confirm it
// reaches real content without throwing — not a snapshot or a visual
// check, just "does this tab render past its own loading/error states."

beforeEach(() => {
  installFetchMock()
})

describe('tab smoke tests', () => {
  it('OverviewTab renders its heading', async () => {
    render(<OverviewTab onNavigate={() => {}} />)
    expect(await screen.findByRole('heading', { name: 'Credit risk, end to end' })).toBeTruthy()
  })

  it('WalkthroughTab renders its heading', async () => {
    render(<WalkthroughTab />)
    expect(await screen.findByRole('heading', { name: 'A day in the life of a credit risk analyst' })).toBeTruthy()
  })

  it('DataEngineeringTab renders its heading', async () => {
    render(<DataEngineeringTab />)
    expect(await screen.findByRole('heading', { name: 'Database & Data Engineering' })).toBeTruthy()
  })

  it('ForecastingTab renders its heading and the backtest section', async () => {
    render(<ForecastingTab />)
    expect(await screen.findByRole('heading', { name: 'Forecasting & Data Science' })).toBeTruthy()
    expect(await screen.findByText(/Backtest \/ validation against real loan data/)).toBeTruthy()
  })

  it('InfrastructureTab renders its heading (finops_snapshot.json absent, real 404 path)', async () => {
    render(<InfrastructureTab />)
    expect(await screen.findByRole('heading', { name: 'Infrastructure & Productionization' })).toBeTruthy()
  })

  it('EmergingTechniquesTab renders its heading', async () => {
    render(<EmergingTechniquesTab />)
    expect(await screen.findByRole('heading', { name: 'Emerging Techniques' })).toBeTruthy()
  })
})
