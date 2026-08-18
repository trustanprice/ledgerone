"""
generate_credit_risk_data.py

Purpose:
- Generate a synthetic consumer credit/loan portfolio (originations + monthly
  performance) for the credit-risk practice module, the same way
  generate_data.py generates the e-commerce ledger: deterministic, seeded,
  written to Parquet under data/raw/credit_risk/.
- This is a separate domain from the e-commerce ledger (users/accounts/
  events) and does not touch it.

Usage:
    python src/generate_credit_risk_data.py

Output:
    data/raw/credit_risk/loan_originations.parquet
    data/raw/credit_risk/loan_performance.parquet

Modeling notes (see dbt/models/credit_risk/README.md for the full writeup):
- Delinquency bucket evolves as a Markov chain: each month, an account's
  bucket transitions based only on its current bucket (mostly stays put,
  small probability of rolling forward, smaller probability of curing back).
  Origination credit score band scales the Current -> 30-59 DPD "first
  delinquency" probability (worse bands enter delinquency more often); cure/
  roll probabilities within delinquency are held constant across bands to
  keep the generator readable — a production model would vary the full
  matrix by segment.
- 120+/Charged-Off is absorbing: once an account charges off, no further
  monthly performance rows are generated for it (it exits the active book,
  same as it would in a real servicing system).
- statement_balance and payment_amount are descriptive (revolve/pay down
  when current, grow with fees/interest and go largely unpaid when
  delinquent) — they do NOT drive the bucket transition; the Markov matrix
  is the sole transition driver, kept intentionally separate so the roll-
  rate structure in the data is clean and easy to query.
"""

from __future__ import annotations

import random
import uuid
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd

# -----------------------------
# Config
# -----------------------------
SEED = 42
NUM_ORIGINATION_QUARTERS = 12  # 2022 Q1 -> 2024 Q4
ACCOUNTS_PER_QUARTER_MEAN = 333
ACCOUNTS_PER_QUARTER_STD = 40
OBSERVATION_END = date(2025, 6, 30)
FORECAST_HORIZON_MONTHS_NOTE = 12  # matches dbt var cecl_forecast_horizon_months

random.seed(SEED)
np.random.seed(SEED)

RAW_DIR = Path("data/raw/credit_risk")
RAW_DIR.mkdir(parents=True, exist_ok=True)

DELINQUENCY_BUCKETS = [
    "Current",
    "30-59 DPD",
    "60-89 DPD",
    "90-119 DPD",
    "120+/Charged-Off",
]

# Base monthly transition probabilities: mostly stays current, small
# probability of rolling forward one severity level, smaller probability of
# curing (30-59 can cure straight to Current; deeper buckets cure back one
# level at a time, reflecting gradual repayment). 120+/Charged-Off is
# terminal and is never a `from` state here (no rows are generated after
# charge-off) — see the absorbing-state note in dbt/models/credit_risk/README.md.
BASE_TRANSITIONS = {
    "Current":     {"Current": 0.994, "30-59 DPD": 0.006},
    "30-59 DPD":   {"Current": 0.40, "30-59 DPD": 0.20, "60-89 DPD": 0.40},
    "60-89 DPD":   {"30-59 DPD": 0.30, "60-89 DPD": 0.25, "90-119 DPD": 0.45},
    "90-119 DPD":  {"60-89 DPD": 0.20, "90-119 DPD": 0.20, "120+/Charged-Off": 0.60},
}

# Worse origination score bands enter first delinquency more often. Only the
# Current -> 30-59 DPD probability is scaled (capped) to keep the generator
# simple; cure/roll rates within delinquency don't vary by band.
SCORE_BANDS = [
    {"band": "Subprime",    "weight": 0.20, "limit_range": (500, 1500),   "apr_range": (26.0, 29.99), "risk_multiplier": 2.5},
    {"band": "Near Prime",  "weight": 0.30, "limit_range": (1500, 3000),  "apr_range": (24.0, 27.0),  "risk_multiplier": 1.6},
    {"band": "Prime",       "weight": 0.35, "limit_range": (3000, 7000),  "apr_range": (20.0, 24.0),  "risk_multiplier": 1.0},
    {"band": "Super Prime", "weight": 0.15, "limit_range": (7000, 15000), "apr_range": (15.0, 20.0),  "risk_multiplier": 0.6},
]
SCORE_BAND_NAMES = [b["band"] for b in SCORE_BANDS]
SCORE_BAND_WEIGHTS = [b["weight"] for b in SCORE_BANDS]
SCORE_BAND_BY_NAME = {b["band"]: b for b in SCORE_BANDS}

# Synchrony's actual book is private-label-heavy, so weight accordingly.
PRODUCT_TYPES = ["Private Label Card", "Dual Card", "General Purpose Card"]
PRODUCT_WEIGHTS = [0.60, 0.25, 0.15]

MAX_ROLL_PROB_CAP = 0.05


def uuid_str() -> str:
    return str(uuid.uuid4())


def add_months(d: date, n: int) -> date:
    month_index = d.month - 1 + n
    year = d.year + month_index // 12
    month = month_index % 12 + 1
    return date(year, month, 1)


def quarter_start_months(quarter_index: int) -> list[date]:
    """3 calendar months belonging to origination quarter `quarter_index` (0-based from 2022-Q1)."""
    first_month = add_months(date(2022, 1, 1), quarter_index * 3)
    return [add_months(first_month, i) for i in range(3)]


def random_origination_date(quarter_index: int) -> date:
    month = random.choice(quarter_start_months(quarter_index))
    day = random.randint(1, 28)
    return date(month.year, month.month, day)


# -----------------------------
# Originations
# -----------------------------
originations = []

for q in range(NUM_ORIGINATION_QUARTERS):
    n_accounts = max(1, int(np.random.normal(ACCOUNTS_PER_QUARTER_MEAN, ACCOUNTS_PER_QUARTER_STD)))
    for _ in range(n_accounts):
        band = np.random.choice(SCORE_BAND_NAMES, p=SCORE_BAND_WEIGHTS)
        band_cfg = SCORE_BAND_BY_NAME[band]
        limit_lo, limit_hi = band_cfg["limit_range"]
        apr_lo, apr_hi = band_cfg["apr_range"]

        originations.append({
            "account_id": uuid_str(),
            "origination_date": random_origination_date(q),
            "origination_score_band": band,
            "credit_limit": round(random.uniform(limit_lo, limit_hi), 2),
            "apr": round(random.uniform(apr_lo, apr_hi), 2),
            "product_type": np.random.choice(PRODUCT_TYPES, p=PRODUCT_WEIGHTS),
        })

originations_df = pd.DataFrame(originations)


def get_transition_probs(bucket: str, risk_multiplier: float) -> dict[str, float]:
    base = BASE_TRANSITIONS[bucket]
    if bucket == "Current":
        p_delinquent = min(base["30-59 DPD"] * risk_multiplier, MAX_ROLL_PROB_CAP)
        return {"Current": 1 - p_delinquent, "30-59 DPD": p_delinquent}
    return base


def sample_next_bucket(bucket: str, risk_multiplier: float) -> str:
    probs = get_transition_probs(bucket, risk_multiplier)
    outcomes = list(probs.keys())
    weights = list(probs.values())
    return np.random.choice(outcomes, p=weights)


def update_balance(bucket: str, balance: float, credit_limit: float) -> float:
    if bucket == "Current":
        target = credit_limit * 0.4
        balance = balance + 0.15 * (target - balance) + np.random.normal(0, credit_limit * 0.03)
        balance = max(balance, 0.0)
    else:
        balance = balance * (1 + random.uniform(0.01, 0.03))
    return round(balance, 2)


def compute_payment(bucket: str, balance: float) -> float:
    if bucket == "Current":
        return round(balance * random.uniform(0.05, 0.30), 2)
    if bucket == "120+/Charged-Off":
        return 0.0
    return round(balance * random.uniform(0.0, 0.05), 2)


# -----------------------------
# Monthly performance (Markov roll forward per account)
# -----------------------------
performance_rows = []

for row in originations_df.itertuples(index=False):
    band_cfg = SCORE_BAND_BY_NAME[row.origination_score_band]
    risk_multiplier = band_cfg["risk_multiplier"]

    bucket = "Current"
    mob = 0
    month = date(row.origination_date.year, row.origination_date.month, 1)
    balance = round(row.credit_limit * random.uniform(0.1, 0.6), 2)

    while True:
        balance = update_balance(bucket, balance, row.credit_limit)
        payment = compute_payment(bucket, balance)
        charge_off_flag = bucket == "120+/Charged-Off"

        performance_rows.append({
            "account_id": row.account_id,
            "performance_month": month,
            "months_on_book": mob,
            "statement_balance": balance,
            "payment_amount": payment,
            "delinquency_bucket": bucket,
            "charge_off_flag": charge_off_flag,
        })

        if charge_off_flag:
            break
        if add_months(month, 1) > OBSERVATION_END:
            break

        bucket = sample_next_bucket(bucket, risk_multiplier)
        mob += 1
        month = add_months(month, 1)

performance_df = pd.DataFrame(performance_rows)

# -----------------------------
# Write to disk
# -----------------------------
originations_df.to_parquet(RAW_DIR / "loan_originations.parquet", index=False)
performance_df.to_parquet(RAW_DIR / "loan_performance.parquet", index=False)

print("Synthetic credit risk data generation complete.")
print(f"Accounts (originations): {len(originations_df)}")
print(f"Performance rows: {len(performance_df)}")
print(f"Charge-offs: {int(performance_df['charge_off_flag'].sum())}")
print(
    "Lifetime charge-off rate (of originated accounts): "
    f"{performance_df['charge_off_flag'].sum() / len(originations_df):.2%}"
)
