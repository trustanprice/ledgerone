import { useJsonData } from './useJsonData'
import type { FinOpsSnapshot } from '../types'

/**
 * Infrastructure-tab-only, unlike useCreditRiskData: this is the one
 * dataset in the site that's genuinely allowed to not exist yet.
 * finops_snapshot.json is written by src/export_finops_data.py against a
 * live AWS Cost Explorer call, run occasionally by hand, not on every
 * build -- a fresh account (or one where Cost Explorer was only just
 * enabled) legitimately has no file to fetch. A 404 here means "not
 * available yet," not "the site is broken" -- see InfrastructureTab's
 * handling of `notFound`.
 */
export function useFinOpsData() {
  const { data, loading, error } = useJsonData<FinOpsSnapshot>('finops_snapshot.json')
  // Any fetch/parse failure here means "the file doesn't exist yet" -- the
  // only two states a static, checked-in JSON file can be in. (Not a
  // string match on "404": a dev-server SPA fallback can return 200 with
  // HTML for a missing path, which fails JSON parsing instead -- both are
  // the same "not available yet" case from this hook's point of view.)
  const notFound = Boolean(error) && !data
  return { data, loading, error: null, notFound }
}
