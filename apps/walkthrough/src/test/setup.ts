import { afterEach } from 'vitest'
import { cleanup } from '@testing-library/react'

afterEach(() => {
  cleanup()
})

// jsdom implements neither observer API. Recharts' ResponsiveContainer
// needs the first to mount; useScrollySteps (Day in the Life) needs the
// second. No-ops are fine here — these tests check that real data
// renders into real DOM text/roles, not chart pixel dimensions or actual
// scroll-driven intersection.
class ResizeObserverStub {
  observe() {}
  unobserve() {}
  disconnect() {}
}

class IntersectionObserverStub {
  observe() {}
  unobserve() {}
  disconnect() {}
  takeRecords() {
    return []
  }
}

globalThis.ResizeObserver ??= ResizeObserverStub
globalThis.IntersectionObserver ??= IntersectionObserverStub as unknown as typeof IntersectionObserver
