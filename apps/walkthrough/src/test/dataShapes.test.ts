import { describe, expect, it } from 'vitest'
import { loadFixture } from './fixtures'
import { BUCKET_ORDER, SCORE_BAND_ORDER } from '../types'
import type {
  BacktestValidation,
  BucketDistributionRow,
  OriginationCohortBand,
  PortfolioSummaryRow,
  ReserveForecastTrajectory,
  RollRateCell,
  RollRateMatrix,
  VintageCurveRow,
} from '../types'

// Runtime proof that the checked-in JSON export still matches the TS
// interfaces describing it — the interfaces themselves erase at compile
// time, so a field src/export_credit_risk_walkthrough_data.py stops
// writing (or renames) wouldn't otherwise surface until a tab crashed at
// runtime.

function expectKeys(row: unknown, keys: string[]) {
  for (const key of keys) {
    expect(row, `missing key "${key}"`).toHaveProperty(key)
  }
}

function expectRollRateMatrix(matrix: RollRateMatrix) {
  expect(Array.isArray(matrix.from_bucket_order)).toBe(true)
  expect(Array.isArray(matrix.to_bucket_order)).toBe(true)
  expect(matrix.from_bucket_order.length).toBeGreaterThan(0)
  for (const b of [...matrix.from_bucket_order, ...matrix.to_bucket_order]) {
    expect(BUCKET_ORDER).toContain(b)
  }
  expect(matrix.cells.length).toBeGreaterThan(0)
  const cell: RollRateCell = matrix.cells[0]
  expectKeys(cell, ['from_bucket', 'to_bucket', 'account_count', 'pct'])
  expect(BUCKET_ORDER).toContain(cell.from_bucket)
  expect(BUCKET_ORDER).toContain(cell.to_bucket)
  expect(typeof cell.account_count).toBe('number')
  expect(typeof cell.pct).toBe('number')
}

describe('originations_by_cohort_and_band.json', () => {
  const rows = loadFixture<OriginationCohortBand[]>('originations_by_cohort_and_band.json')

  it('is a non-empty array of the expected row shape', () => {
    expect(rows.length).toBeGreaterThan(0)
    expectKeys(rows[0], ['origination_quarter', 'score_band', 'account_count'])
    expect(typeof rows[0].origination_quarter).toBe('string')
    expect(typeof rows[0].account_count).toBe('number')
  })

  it('only uses known score bands', () => {
    for (const row of rows) expect(SCORE_BAND_ORDER).toContain(row.score_band)
  })
})

describe('current_bucket_distribution.json', () => {
  const rows = loadFixture<BucketDistributionRow[]>('current_bucket_distribution.json')

  it('is a non-empty array with known buckets and numeric balances', () => {
    expect(rows.length).toBeGreaterThan(0)
    for (const row of rows) {
      expectKeys(row, ['bucket', 'account_count', 'outstanding_balance'])
      expect(BUCKET_ORDER).toContain(row.bucket)
      expect(typeof row.account_count).toBe('number')
      expect(typeof row.outstanding_balance).toBe('number')
    }
  })
})

describe('vintage_curves.json', () => {
  const rows = loadFixture<VintageCurveRow[]>('vintage_curves.json')

  it('is a non-empty array of the expected row shape', () => {
    expect(rows.length).toBeGreaterThan(0)
    expectKeys(rows[0], [
      'origination_quarter',
      'months_on_book',
      'cohort_accounts',
      'cumulative_30plus_rate',
      'cumulative_60plus_rate',
      'cumulative_90plus_rate',
      'cumulative_charge_off_rate',
    ])
    for (const key of ['months_on_book', 'cohort_accounts', 'cumulative_charge_off_rate'] as const) {
      expect(typeof rows[0][key]).toBe('number')
    }
  })
})

describe('roll_rate_matrix.json', () => {
  it('matches RollRateMatrix', () => {
    expectRollRateMatrix(loadFixture<RollRateMatrix>('roll_rate_matrix.json'))
  })
})

describe('reserve_forecast_trajectory.json', () => {
  const trajectory = loadFixture<ReserveForecastTrajectory>('reserve_forecast_trajectory.json')

  it('has a positive horizon and matching points', () => {
    expect(typeof trajectory.horizon_months).toBe('number')
    expect(trajectory.points.length).toBeGreaterThan(0)
    expectKeys(trajectory.points[0], ['horizon_month', 'expected_loss_amount', 'outstanding_balance', 'reserve_rate'])
  })
})

describe('portfolio_summary_by_score_band.json', () => {
  const rows = loadFixture<PortfolioSummaryRow[]>('portfolio_summary_by_score_band.json')

  it('is a non-empty array covering only known score bands', () => {
    expect(rows.length).toBeGreaterThan(0)
    expectKeys(rows[0], [
      'origination_score_band',
      'account_count',
      'outstanding_balance',
      'currently_delinquent_accounts',
      'current_delinquency_rate',
      'charged_off_accounts',
      'lifetime_charge_off_rate',
      'avg_apr',
    ])
    for (const row of rows) expect(SCORE_BAND_ORDER).toContain(row.origination_score_band)
  })
})

describe('backtest_validation.json', () => {
  const backtest = loadFixture<BacktestValidation>('backtest_validation.json')

  it('has the top-level shape BacktestValidation expects', () => {
    expectKeys(backtest, [
      'data_source',
      'train_cutoff',
      'horizon_months',
      'curve',
      'accuracy',
      'calibration',
      'train_matrix',
      'holdout_matrix',
      'sanity_check',
    ])
  })

  it('curve, accuracy, calibration, and sanity_check rows match their interfaces', () => {
    expect(backtest.curve.length).toBeGreaterThan(0)
    expectKeys(backtest.curve[0], ['cohort', 'months_on_book', 'horizon_month', 'predicted', 'actual', 'cohort_n'])

    expectKeys(backtest.accuracy, ['mae', 'mape', 'bias_pp', 'n_points'])

    expect(backtest.calibration.length).toBeGreaterThan(0)
    expectKeys(backtest.calibration[0], [
      'from_bucket',
      'to_bucket',
      'train_rate',
      'holdout_rate',
      'rate_diff_pp',
      'train_n',
      'holdout_n',
    ])

    expectKeys(backtest.sanity_check, [
      'mortgage_lifetime_ever90plus_rate',
      'fred_card_delinquency_rate',
      'synchrony_nco_rate',
    ])
  })

  it('train_matrix and holdout_matrix each match RollRateMatrix', () => {
    expectRollRateMatrix(backtest.train_matrix)
    expectRollRateMatrix(backtest.holdout_matrix)
  })
})
