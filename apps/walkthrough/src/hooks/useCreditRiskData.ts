import { useJsonData } from './useJsonData'
import type {
  BacktestValidation,
  BucketDistributionRow,
  OriginationCohortBand,
  PortfolioSummaryRow,
  ReserveForecastTrajectory,
  RollRateMatrix,
  VintageCurveRow,
} from '../types'

/**
 * Fetches every static dataset the credit-risk tabs need. Both the Day in
 * the Life and Forecasting tabs consume the same seven JSON files — this is
 * the one place that lists them, so a future tab needing the same data
 * doesn't duplicate the fetch list. (Each tab mount does re-request via
 * fetch(), same as any tab switch would; the browser's own HTTP cache
 * makes repeat requests for these small static files effectively free
 * after the first load, so there's no need for an in-memory cache here.)
 */
export function useCreditRiskData() {
  const originations = useJsonData<OriginationCohortBand[]>('originations_by_cohort_and_band.json')
  const distribution = useJsonData<BucketDistributionRow[]>('current_bucket_distribution.json')
  const vintage = useJsonData<VintageCurveRow[]>('vintage_curves.json')
  const rollRate = useJsonData<RollRateMatrix>('roll_rate_matrix.json')
  const reserve = useJsonData<ReserveForecastTrajectory>('reserve_forecast_trajectory.json')
  const portfolioSummary = useJsonData<PortfolioSummaryRow[]>('portfolio_summary_by_score_band.json')
  const backtest = useJsonData<BacktestValidation>('backtest_validation.json')

  const allLoaded = Boolean(
    originations.data &&
      distribution.data &&
      vintage.data &&
      rollRate.data &&
      reserve.data &&
      portfolioSummary.data &&
      backtest.data,
  )

  const error =
    originations.error ||
    distribution.error ||
    vintage.error ||
    rollRate.error ||
    reserve.error ||
    portfolioSummary.error ||
    backtest.error

  return {
    originations: originations.data,
    distribution: distribution.data,
    vintage: vintage.data,
    rollRate: rollRate.data,
    reserve: reserve.data,
    portfolioSummary: portfolioSummary.data,
    backtest: backtest.data,
    allLoaded,
    error,
  }
}
