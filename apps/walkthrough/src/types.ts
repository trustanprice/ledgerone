export type DelinquencyBucket =
  | 'Current'
  | '30-59 DPD'
  | '60-89 DPD'
  | '90-119 DPD'
  | '120+/Charged-Off'

export const BUCKET_ORDER: DelinquencyBucket[] = [
  'Current',
  '30-59 DPD',
  '60-89 DPD',
  '90-119 DPD',
  '120+/Charged-Off',
]

export const SCORE_BAND_ORDER = ['Subprime', 'Near Prime', 'Prime', 'Super Prime'] as const
export type ScoreBand = (typeof SCORE_BAND_ORDER)[number]

export interface OriginationCohortBand {
  origination_quarter: string
  score_band: ScoreBand
  account_count: number
}

export interface BucketDistributionRow {
  bucket: DelinquencyBucket
  account_count: number
  outstanding_balance: number
}

export interface VintageCurveRow {
  origination_quarter: string
  months_on_book: number
  cohort_accounts: number
  cumulative_30plus_rate: number
  cumulative_60plus_rate: number
  cumulative_90plus_rate: number
  cumulative_charge_off_rate: number
}

export interface RollRateCell {
  from_bucket: DelinquencyBucket
  to_bucket: DelinquencyBucket
  account_count: number
  pct: number
}

export interface RollRateMatrix {
  from_bucket_order: DelinquencyBucket[]
  to_bucket_order: DelinquencyBucket[]
  cells: RollRateCell[]
}

export interface ReserveForecastPoint {
  horizon_month: number
  expected_loss_amount: number
  outstanding_balance: number
  reserve_rate: number
}

export interface ReserveForecastTrajectory {
  horizon_months: number
  points: ReserveForecastPoint[]
}

export interface PortfolioSummaryRow {
  origination_score_band: ScoreBand
  account_count: number
  outstanding_balance: number
  currently_delinquent_accounts: number
  current_delinquency_rate: number
  charged_off_accounts: number
  lifetime_charge_off_rate: number
  avg_apr: number
}

export interface BacktestCurvePoint {
  cohort: string
  months_on_book: number
  horizon_month: number
  predicted: number
  actual: number
  cohort_n: number
}

export interface BacktestAccuracy {
  mae: number
  mape: number
  bias_pp: number
  n_points: number
}

export interface BacktestCalibrationRow {
  from_bucket: DelinquencyBucket
  to_bucket: DelinquencyBucket
  train_rate: number
  holdout_rate: number
  rate_diff_pp: number
  train_n: number
  holdout_n: number
}

export interface BacktestSanityCheck {
  mortgage_lifetime_ever90plus_rate: number
  fred_card_delinquency_rate: number
  synchrony_nco_rate: number
}

export interface BacktestValidation {
  data_source: string
  train_cutoff: string
  horizon_months: number
  curve: BacktestCurvePoint[]
  accuracy: BacktestAccuracy
  calibration: BacktestCalibrationRow[]
  train_matrix: RollRateMatrix
  holdout_matrix: RollRateMatrix
  sanity_check: BacktestSanityCheck
}

export interface FinOpsServiceCost {
  service: string
  amount: number
}

export interface FinOpsDailyCost {
  date: string
  amount: number
}

export interface FinOpsSnapshot {
  as_of: string
  window_days: number
  total_unblended_cost: number
  by_service: FinOpsServiceCost[]
  daily_total: FinOpsDailyCost[]
}
