import { useEffect, useState } from 'react'

interface JsonDataState<T> {
  data: T | null
  loading: boolean
  error: string | null
}

/**
 * Fetches a static JSON file from public/data/ (written by
 * src/export_credit_risk_walkthrough_data.py at the repo root — this app
 * never queries DuckDB directly, see AGENTS.md). Uses import.meta.env.BASE_URL
 * so it resolves correctly both in local dev (base "/") and the deployed
 * GitHub Pages subpath (base "/ledgerone/walkthrough/").
 */
export function useJsonData<T>(filename: string): JsonDataState<T> {
  const [state, setState] = useState<JsonDataState<T>>({ data: null, loading: true, error: null })

  useEffect(() => {
    let cancelled = false
    const url = `${import.meta.env.BASE_URL}data/${filename}`

    fetch(url)
      .then((res) => {
        if (!res.ok) throw new Error(`${url} responded ${res.status}`)
        return res.json() as Promise<T>
      })
      .then((data) => {
        if (!cancelled) setState({ data, loading: false, error: null })
      })
      .catch((err: unknown) => {
        if (!cancelled) {
          setState({ data: null, loading: false, error: err instanceof Error ? err.message : String(err) })
        }
      })

    return () => {
      cancelled = true
    }
  }, [filename])

  return state
}
