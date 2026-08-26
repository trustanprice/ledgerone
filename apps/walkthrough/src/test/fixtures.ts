import { readFileSync, existsSync } from 'node:fs'
import path from 'node:path'
import { vi } from 'vitest'

// process.cwd() is apps/walkthrough — vitest.config.ts doesn't override
// root, and both `npm test` and CI always invoke from this directory.
const PUBLIC_DATA_DIR = path.resolve(process.cwd(), 'public/data')

// The real, checked-in export output — same files useJsonData fetches at
// runtime. Loading these instead of hand-written mocks means a smoke test
// exercises the same shape the site actually ships, and can't silently
// drift from src/export_credit_risk_walkthrough_data.py's real output.
export function loadFixture<T>(filename: string): T {
  return JSON.parse(readFileSync(path.join(PUBLIC_DATA_DIR, filename), 'utf-8')) as T
}

export function fixtureExists(filename: string): boolean {
  return existsSync(path.join(PUBLIC_DATA_DIR, filename))
}

/**
 * Stubs global fetch to serve public/data/<filename> for any request
 * whose URL ends in "data/<filename>" (matches useJsonData's
 * `${BASE_URL}data/${filename}` regardless of BASE_URL). A filename with
 * no file on disk resolves like a real 404 — this is deliberate for
 * finops_snapshot.json, which is genuinely allowed to not exist yet (see
 * useFinOpsData.ts) and should exercise that real "not found" path, not
 * a fabricated success.
 */
export function installFetchMock() {
  vi.stubGlobal(
    'fetch',
    vi.fn(async (input: string | URL) => {
      const url = typeof input === 'string' ? input : input.toString()
      const filename = url.split('/').pop() ?? ''

      if (!fixtureExists(filename)) {
        return { ok: false, status: 404 } as Response
      }

      const body = readFileSync(path.join(PUBLIC_DATA_DIR, filename), 'utf-8')
      return { ok: true, status: 200, json: async () => JSON.parse(body) } as Response
    }),
  )
}
