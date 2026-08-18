"""
export_credit_risk_walkthrough_data.py

Purpose:
- Static data export for apps/walkthrough/ (the "day in the life" React
  scrollytelling site). The site is a static app with no backend and no
  live DuckDB connection, per apps/walkthrough/AGENTS.md — this script is
  the one place that bridges the dbt warehouse to that app's static JSON.
- Read-only. Does not modify any dbt model, table, or the credit risk
  module itself.

Usage:
    python src/export_credit_risk_walkthrough_data.py

Assumes dbt build has already run (main_marts.credit_risk__* must exist).

Output: apps/walkthrough/public/data/*.json

Note on reserve_forecast_trajectory.json: credit_risk__cecl_reserve_estimate
(the dbt mart) only exposes the FINAL forecast-horizon month's expected
loss, by design — that's all the "how much do we expect to lose" business
question needs. The walkthrough site wants to *plot* the buildup over the
full 12-month horizon, which the mart doesn't expose. Rather than modify
the dbt model (out of scope for this app — see apps/walkthrough/AGENTS.md),
this script re-runs the same blended-transition-matrix roll-forward that
credit_risk__cecl_reserve_estimate.sql does, but keeps every intermediate
horizon_month instead of filtering to just the last one. This does
duplicate a small amount of SQL logic between the dbt model and this
script — a real, acknowledged trade-off; see the module's judgment-call
notes for why it was made this way instead of adding a new mart.
"""

from __future__ import annotations

import json
from pathlib import Path

import duckdb

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DB_PATH = PROJECT_ROOT / "data" / "processed" / "ledgerone.duckdb"
OUT_DIR = PROJECT_ROOT / "apps" / "walkthrough" / "public" / "data"
FORECAST_HORIZON_MONTHS = 12

BUCKET_ORDER = ["Current", "30-59 DPD", "60-89 DPD", "90-119 DPD", "120+/Charged-Off"]


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(DB_PATH), read_only=True)

    # --- Scene 1: originations by cohort x score band ---
    originations = con.execute("""
        SELECT
            origination_quarter,
            origination_score_band AS score_band,
            COUNT(*) AS account_count
        FROM main_staging.credit_risk__stg_loan_originations
        GROUP BY 1, 2
        ORDER BY 1, 2
    """).df()
    originations["origination_quarter"] = originations["origination_quarter"].astype(str)
    write_json("originations_by_cohort_and_band.json", originations.to_dict(orient="records"))

    # --- Scene 1: current delinquency bucket distribution (active book) ---
    distribution = con.execute("""
        SELECT
            current_bucket AS bucket,
            account_count,
            outstanding_balance
        FROM main_marts.credit_risk__cecl_reserve_estimate
        ORDER BY CASE current_bucket
            WHEN 'Current' THEN 1 WHEN '30-59 DPD' THEN 2
            WHEN '60-89 DPD' THEN 3 WHEN '90-119 DPD' THEN 4 ELSE 5 END
    """).df()
    write_json("current_bucket_distribution.json", distribution.to_dict(orient="records"))

    # --- Scene 2: vintage curves (full mart) ---
    vintage = con.execute("""
        SELECT origination_quarter, months_on_book, cohort_accounts,
               cumulative_30plus_rate, cumulative_60plus_rate,
               cumulative_90plus_rate, cumulative_charge_off_rate
        FROM main_marts.credit_risk__vintage_curves
        ORDER BY origination_quarter, months_on_book
    """).df()
    vintage["origination_quarter"] = vintage["origination_quarter"].astype(str)
    write_json("vintage_curves.json", vintage.to_dict(orient="records"))

    # --- Scene 3: blended roll-rate matrix (aggregate across all months, row-normalize) ---
    roll_rate = con.execute("""
        WITH agg AS (
            SELECT from_bucket, to_bucket, SUM(account_count) AS account_count
            FROM main_marts.credit_risk__roll_rate_transition_matrix
            GROUP BY 1, 2
        )
        SELECT
            from_bucket,
            to_bucket,
            account_count,
            round(account_count / SUM(account_count) OVER (PARTITION BY from_bucket), 4) AS pct
        FROM agg
        ORDER BY from_bucket, to_bucket
    """).df()
    write_json("roll_rate_matrix.json", {
        "from_bucket_order": BUCKET_ORDER[:-1],  # 120+/Charged-Off never observed as from_bucket
        "to_bucket_order": BUCKET_ORDER,
        "cells": roll_rate.to_dict(orient="records"),
    })

    # --- Scene 4: reserve forecast trajectory, month 0..12 (full roll-forward, not just the final month) ---
    trajectory = con.execute(f"""
        WITH RECURSIVE
        observed_transitions AS (
            SELECT from_bucket, to_bucket, SUM(account_count) AS account_count
            FROM main_marts.credit_risk__roll_rate_transition_matrix
            GROUP BY 1, 2
        ),
        transition_probs AS (
            SELECT
                from_bucket,
                to_bucket,
                account_count / SUM(account_count) OVER (PARTITION BY from_bucket) AS transition_rate
            FROM observed_transitions
            UNION ALL
            SELECT '120+/Charged-Off', '120+/Charged-Off', CAST(1.0 AS DOUBLE)
        ),
        roll_forward (starting_bucket, horizon_month, state, probability) AS (
            SELECT DISTINCT from_bucket, 0, from_bucket, CAST(1.0 AS DOUBLE)
            FROM transition_probs
            UNION ALL
            SELECT rf.starting_bucket, rf.horizon_month + 1, tp.to_bucket, rf.probability * tp.transition_rate
            FROM roll_forward rf
            JOIN transition_probs tp ON tp.from_bucket = rf.state
            WHERE rf.horizon_month < {FORECAST_HORIZON_MONTHS}
        ),
        -- A starting bucket that can't yet mathematically reach charge-off
        -- within `horizon_month` steps (e.g. Current needs >= 4 steps: Current
        -- -> 30-59 -> 60-89 -> 90-119 -> Charged-Off) has NO row in
        -- roll_forward with state = '120+/Charged-Off' at that horizon_month
        -- — it's genuinely absent, not zero. Without this explicit grid +
        -- LEFT JOIN, those accounts silently drop out of early months'
        -- aggregates entirely (their balance disappears rather than
        -- counting with $0 expected loss), which corrupts outstanding_balance
        -- for every month before every bucket has a reachable path.
        starting_buckets AS (
            SELECT DISTINCT from_bucket AS starting_bucket FROM transition_probs
        ),
        horizon_grid AS (
            SELECT starting_bucket, UNNEST(RANGE(0, {FORECAST_HORIZON_MONTHS} + 1)) AS horizon_month
            FROM starting_buckets
        ),
        charge_off_prob_by_month AS (
            SELECT
                g.starting_bucket,
                g.horizon_month,
                COALESCE(SUM(rf.probability), 0) AS prob_charge_off_by_month
            FROM horizon_grid g
            LEFT JOIN roll_forward rf
                ON rf.starting_bucket = g.starting_bucket
               AND rf.horizon_month = g.horizon_month
               AND rf.state = '120+/Charged-Off'
            GROUP BY 1, 2
        ),
        latest_snapshot AS (
            SELECT account_id, delinquency_bucket, statement_balance,
                   ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY months_on_book DESC) AS rn
            FROM main_staging.credit_risk__stg_loan_performance
        ),
        current_book AS (
            SELECT account_id, delinquency_bucket AS current_bucket, statement_balance
            FROM latest_snapshot
            WHERE rn = 1 AND delinquency_bucket != '120+/Charged-Off'
        )
        SELECT
            c.horizon_month,
            round(SUM(cb.statement_balance * c.prob_charge_off_by_month), 2) AS expected_loss_amount,
            round(SUM(cb.statement_balance), 2) AS outstanding_balance,
            round(SUM(cb.statement_balance * c.prob_charge_off_by_month) / SUM(cb.statement_balance), 4) AS reserve_rate
        FROM current_book cb
        JOIN charge_off_prob_by_month c ON cb.current_bucket = c.starting_bucket
        GROUP BY 1
        ORDER BY 1
    """).df()
    write_json("reserve_forecast_trajectory.json", {
        "horizon_months": FORECAST_HORIZON_MONTHS,
        "points": trajectory.to_dict(orient="records"),
    })

    # --- Scene 5 support: portfolio summary by score band (already a mart) ---
    portfolio_summary = con.execute("""
        SELECT * FROM main_marts.credit_risk__portfolio_summary_by_score_band
        ORDER BY origination_score_band
    """).df()
    write_json("portfolio_summary_by_score_band.json", portfolio_summary.to_dict(orient="records"))

    con.close()
    print(f"\nWalkthrough data export complete -> {OUT_DIR}")


def write_json(filename: str, data) -> None:
    path = OUT_DIR / filename
    with open(path, "w") as f:
        json.dump(data, f, indent=2, default=str)
    print(f"Wrote {path}")


if __name__ == "__main__":
    main()
