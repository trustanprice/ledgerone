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

Note on backtest_validation.json: this re-derives the same numbers as
notebooks/03_credit_risk_validation_backtest.ipynb, via the same DuckDB
SQL, rather than reading the notebook's output — the notebook is the
one-time analysis + writeup, this script is what actually feeds the
site. It reads data/external/freddie_mac/*.parquet directly (via a
separate, in-memory DuckDB connection — that data was never loaded into
ledgerone.duckdb and never will be, see dbt/models/credit_risk/AGENTS.md
on why the backtest stays outside dbt entirely) and is skipped with a
warning, not a hard failure, if that manually-downloaded data isn't
present locally.
"""

from __future__ import annotations

import json
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DB_PATH = PROJECT_ROOT / "data" / "processed" / "ledgerone.duckdb"
OUT_DIR = PROJECT_ROOT / "apps" / "walkthrough" / "public" / "data"
FORECAST_HORIZON_MONTHS = 12

BUCKET_ORDER = ["Current", "30-59 DPD", "60-89 DPD", "90-119 DPD", "120+/Charged-Off"]

# --- Backtest validation (Freddie Mac real-data notebook, re-derived) ---
FREDDIE_DIR = PROJECT_ROOT / "data" / "external" / "freddie_mac"
FREDDIE_ORIG = FREDDIE_DIR / "loan_originations.parquet"
FREDDIE_PERF = FREDDIE_DIR / "loan_performance.parquet"
BACKTEST_TRAIN_CUTOFF = "2024-09-01"
BACKTEST_HORIZON_MONTHS = 18
BACKTEST_SEVERE_BUCKETS = ("90-119 DPD", "120+/Charged-Off")
BACKTEST_COHORTS = ["2021-01-01", "2021-04-01", "2021-07-01", "2021-10-01", "2022-01-01"]


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

    # --- Backtest / validation against real Freddie Mac data (Forecasting tab) ---
    export_backtest_validation()

    print(f"\nWalkthrough data export complete -> {OUT_DIR}")


def export_backtest_validation() -> None:
    if not FREDDIE_ORIG.exists() or not FREDDIE_PERF.exists():
        print(
            f"Skipping backtest_validation.json -- {FREDDIE_DIR} not found. "
            "That data is a manual download, not generated by this repo -- see "
            "dbt/models/credit_risk/README.md's Validation section."
        )
        return

    bcon = duckdb.connect()
    cohorts_sql = ",".join(f"'{c}'" for c in BACKTEST_COHORTS)
    severe = BACKTEST_SEVERE_BUCKETS
    cutoff = BACKTEST_TRAIN_CUTOFF

    cutoff_mob = bcon.execute(f"""
        select o.origination_quarter, date_diff('month', o.origination_quarter, date '{cutoff}') as cutoff_mob
        from read_parquet('{FREDDIE_ORIG}') o
        where o.origination_quarter in ({cohorts_sql})
        group by 1 order by 1
    """).df()

    # Ground truth: same technique as credit_risk__vintage_curves, full history.
    actual_curve = bcon.execute(f"""
        with perf as (
            select p.account_id, p.months_on_book, p.delinquency_bucket, o.origination_quarter
            from read_parquet('{FREDDIE_PERF}') p join read_parquet('{FREDDIE_ORIG}') o on p.account_id = o.account_id
        ),
        flagged as (
            select account_id, origination_quarter, months_on_book,
                case when delinquency_bucket in {severe}
                     and months_on_book = min(case when delinquency_bucket in {severe} then months_on_book end) over (partition by account_id)
                     then 1 else 0 end as is_first_90plus
            from perf
        ),
        monthly as (select origination_quarter, months_on_book, sum(is_first_90plus) as new_90plus from flagged group by 1, 2),
        cohort_sizes as (select origination_quarter, count(distinct account_id) as n from read_parquet('{FREDDIE_ORIG}') group by 1)
        select m.origination_quarter, m.months_on_book,
            sum(m.new_90plus) over (partition by m.origination_quarter order by m.months_on_book rows between unbounded preceding and current row) / c.n as actual_cum_90plus_rate,
            c.n as cohort_n
        from monthly m join cohort_sizes c on m.origination_quarter = c.origination_quarter
        order by 1, 2
    """).df()

    # Training-window 4-state matrix (90-119 and 120+ collapsed into one absorbing "90+" state).
    train_matrix4 = bcon.execute(f"""
        with perf as (
            select account_id, months_on_book, performance_month,
                case when delinquency_bucket in {severe} then '90+' else delinquency_bucket end as state4
            from read_parquet('{FREDDIE_PERF}') where performance_month <= date '{cutoff}'
        ),
        lagged as (
            select account_id, state4, lag(state4) over (partition by account_id order by months_on_book) as prior_state4
            from perf
        ),
        transitions as (select prior_state4 as from_state, state4 as to_state from lagged where prior_state4 is not null and prior_state4 != '90+'),
        counted as (select from_state, to_state, count(*) as n from transitions group by 1, 2)
        select from_state, to_state, n, n::double / sum(n) over (partition by from_state) as rate
        from counted order by from_state, to_state
    """).df()

    states = ["Current", "30-59 DPD", "60-89 DPD", "90+"]
    P = pd.DataFrame(0.0, index=states, columns=states)
    for _, r in train_matrix4.iterrows():
        P.loc[r.from_state, r.to_state] = r.rate
    P.loc["90+", "90+"] = 1.0  # absorbing

    # Roll the training matrix forward: P(state = 90+ at horizon h) = P(first reached 90+ by h).
    roll = {s: np.zeros(BACKTEST_HORIZON_MONTHS + 1) for s in states}
    for s in states:
        vec = pd.Series(0.0, index=states)
        vec[s] = 1.0
        roll[s][0] = vec["90+"]
        for h in range(1, BACKTEST_HORIZON_MONTHS + 1):
            vec = vec @ P
            roll[s][h] = vec["90+"]
    prob_absorbed = pd.DataFrame(roll)  # index = horizon month h, columns = starting state

    # Bucket distribution at cutoff, restricted to accounts that NEVER reached
    # severe delinquency through the cutoff -- not just "not currently severe,"
    # which would wrongly include accounts that reached 90+ earlier and cured
    # before the cutoff snapshot (the bug caught while building the notebook).
    never_severe_state_at_cutoff = bcon.execute(f"""
        with perf as (
            select p.account_id, p.months_on_book, o.origination_quarter, p.delinquency_bucket
            from read_parquet('{FREDDIE_PERF}') p join read_parquet('{FREDDIE_ORIG}') o on p.account_id = o.account_id
        ),
        upto_cutoff as (
            select *, date_diff('month', origination_quarter, date '{cutoff}') as cutoff_mob
            from perf
            where months_on_book <= date_diff('month', origination_quarter, date '{cutoff}')
        ),
        ever_severe as (
            select account_id, max(case when delinquency_bucket in {severe} then 1 else 0 end) as was_ever_severe
            from upto_cutoff group by 1
        ),
        latest as (
            select *, row_number() over (partition by account_id order by months_on_book desc) as rn
            from upto_cutoff
        )
        select l.origination_quarter, l.delinquency_bucket, count(*) as n
        from latest l join ever_severe e on l.account_id = e.account_id
        where l.rn = 1 and e.was_ever_severe = 0
        group by 1, 2
    """).df()

    cohort_ts = [pd.Timestamp(c) for c in BACKTEST_COHORTS]
    rows = []
    for cohort in cohort_ts:
        cohort_cutoff = int(cutoff_mob.loc[cutoff_mob.origination_quarter == cohort, "cutoff_mob"].iloc[0])
        total_n = int(actual_curve.loc[actual_curve.origination_quarter == cohort, "cohort_n"].iloc[0])
        base_rate = actual_curve.loc[
            (actual_curve.origination_quarter == cohort) & (actual_curve.months_on_book == cohort_cutoff),
            "actual_cum_90plus_rate",
        ].iloc[0]
        sub = never_severe_state_at_cutoff[never_severe_state_at_cutoff.origination_quarter == cohort]
        bucket_n = dict(zip(sub.delinquency_bucket, sub.n))
        for h in range(1, BACKTEST_HORIZON_MONTHS + 1):
            mob = cohort_cutoff + h
            pred = base_rate
            for b in ["Current", "30-59 DPD", "60-89 DPD"]:
                pred += (bucket_n.get(b, 0) / total_n) * prob_absorbed.loc[h, b]
            actual_row = actual_curve[(actual_curve.origination_quarter == cohort) & (actual_curve.months_on_book == mob)]
            if len(actual_row) == 0:
                continue
            rows.append({
                "cohort": cohort.strftime("%Y-%m-%d"),
                "months_on_book": mob,
                "horizon_month": h,
                "predicted": pred,
                "actual": actual_row.actual_cum_90plus_rate.iloc[0],
                "cohort_n": total_n,
            })
    results = pd.DataFrame(rows)
    results["abs_err"] = (results.predicted - results.actual).abs()
    results["ape"] = np.where(results.actual > 0, results.abs_err / results.actual, np.nan)

    mae = float(results.abs_err.mean())
    mape = float(pd.Series(results.ape).dropna().mean())
    bias_pp = float((results.predicted - results.actual).mean() * 100)

    # Calibration: same 5-state matrix, fit once on training transitions and
    # once on holdout transitions, compared cell by cell.
    holdout_matrix = bcon.execute(f"""
        with perf as (
            select account_id, months_on_book, delinquency_bucket
            from read_parquet('{FREDDIE_PERF}') where performance_month > date '{cutoff}'
        ),
        lagged as (
            select account_id, delinquency_bucket as to_bucket,
                lag(delinquency_bucket) over (partition by account_id order by months_on_book) as from_bucket
            from perf
        ),
        transitions as (select from_bucket, to_bucket from lagged where from_bucket is not null),
        counted as (select from_bucket, to_bucket, count(*) as n from transitions group by 1, 2)
        select from_bucket, to_bucket, n as holdout_n, round(n::double / sum(n) over (partition by from_bucket), 4) as holdout_rate
        from counted order by from_bucket, to_bucket
    """).df()

    train_matrix5 = bcon.execute(f"""
        with perf as (
            select account_id, months_on_book, delinquency_bucket
            from read_parquet('{FREDDIE_PERF}') where performance_month <= date '{cutoff}'
        ),
        lagged as (
            select account_id, delinquency_bucket as to_bucket,
                lag(delinquency_bucket) over (partition by account_id order by months_on_book) as from_bucket
            from perf
        ),
        transitions as (select from_bucket, to_bucket from lagged where from_bucket is not null),
        counted as (select from_bucket, to_bucket, count(*) as n from transitions group by 1, 2)
        select from_bucket, to_bucket, n as train_n, round(n::double / sum(n) over (partition by from_bucket), 4) as train_rate
        from counted order by from_bucket, to_bucket
    """).df()

    calib = train_matrix5.merge(holdout_matrix, on=["from_bucket", "to_bucket"], how="outer").fillna(0)
    calib["rate_diff_pp"] = (calib.holdout_rate - calib.train_rate) * 100
    calib = calib.sort_values(["from_bucket", "to_bucket"])

    lifetime = bcon.execute(f"""
        with perf as (select account_id, delinquency_bucket from read_parquet('{FREDDIE_PERF}')),
        flagged as (
            select account_id,
                max(case when delinquency_bucket in {severe} then 1 else 0 end) as ever_severe
            from perf group by 1
        )
        select count(*) as n, sum(ever_severe) as ever_severe_n from flagged
    """).df()
    mortgage_lifetime_rate = float(lifetime.ever_severe_n[0] / lifetime.n[0])

    bcon.close()

    # Unlike the synthetic module's roll_rate_matrix.json, 120+/Charged-Off
    # IS observed as a from_bucket here -- real mortgages can reinstate/cure
    # out of serious delinquency (11.8% do, in the training window), unlike
    # the synthetic generator's stylized charge-off, which is terminal. Keep
    # the full 5-state grid rather than truncating it away.
    write_json("backtest_validation.json", {
        "data_source": "Freddie Mac Single-Family Loan-Level Dataset, 2021 vintage sample",
        "train_cutoff": BACKTEST_TRAIN_CUTOFF,
        "horizon_months": BACKTEST_HORIZON_MONTHS,
        "curve": results[["cohort", "months_on_book", "horizon_month", "predicted", "actual", "cohort_n"]].to_dict(orient="records"),
        "accuracy": {
            "mae": round(mae, 5),
            "mape": round(mape, 4),
            "bias_pp": round(bias_pp, 4),
            "n_points": len(results),
        },
        "calibration": calib[
            ["from_bucket", "to_bucket", "train_rate", "holdout_rate", "rate_diff_pp", "train_n", "holdout_n"]
        ].to_dict(orient="records"),
        "train_matrix": {
            "from_bucket_order": BUCKET_ORDER,
            "to_bucket_order": BUCKET_ORDER,
            "cells": [
                {"from_bucket": r.from_bucket, "to_bucket": r.to_bucket, "account_count": int(r.train_n), "pct": r.train_rate}
                for r in calib.itertuples()
            ],
        },
        "holdout_matrix": {
            "from_bucket_order": BUCKET_ORDER,
            "to_bucket_order": BUCKET_ORDER,
            "cells": [
                {"from_bucket": r.from_bucket, "to_bucket": r.to_bucket, "account_count": int(r.holdout_n), "pct": r.holdout_rate}
                for r in calib.itertuples()
            ],
        },
        "sanity_check": {
            "mortgage_lifetime_ever90plus_rate": round(mortgage_lifetime_rate, 4),
            "fred_card_delinquency_rate": 0.029,
            "synchrony_nco_rate": 0.0542,
        },
    })


def write_json(filename: str, data) -> None:
    path = OUT_DIR / filename
    with open(path, "w") as f:
        json.dump(data, f, indent=2, default=str)
    print(f"Wrote {path}")


if __name__ == "__main__":
    main()
