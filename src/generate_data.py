import uuid
import random
from datetime import datetime, timedelta

import pandas as pd
import numpy as np
from pathlib import Path

# -----------------------------
# Config
# -----------------------------
NUM_USERS = 100
START_DATE = datetime(2024, 1, 1)
END_DATE = datetime(2024, 3, 31)
CURRENCY = "USD"
SEED = 42

random.seed(SEED)
np.random.seed(SEED)

RAW_DATA_PATH = Path("data/raw")
RAW_DATA_PATH.mkdir(parents=True, exist_ok=True)

# -----------------------------
# Helper functions
# -----------------------------
def random_date(start, end):
    return start + timedelta(
        seconds=random.randint(0, int((end - start).total_seconds()))
    )

def uuid_str():
    return str(uuid.uuid4())

# -----------------------------
# Generate users
# -----------------------------
users = []

for _ in range(NUM_USERS):
    users.append({
        "user_id": uuid_str(),
        "signup_date": random_date(START_DATE, START_DATE + timedelta(days=30)),
        "user_type": random.choice(["spender", "saver", "investor"])
    })

users_df = pd.DataFrame(users)

# -----------------------------
# Generate accounts (1 cash account per user)
# -----------------------------
accounts = []

for _, user in users_df.iterrows():
    accounts.append({
        "account_id": uuid_str(),
        "user_id": user["user_id"],
        "account_type": "CASH",
        "currency": CURRENCY
    })

accounts_df = pd.DataFrame(accounts)

# -----------------------------
# Generate financial events
# -----------------------------
events = []

for _, account in accounts_df.iterrows():
    user_id = account["user_id"]
    account_id = account["account_id"]

    # Initial deposit
    deposit_amount = random.choice([500, 1000, 1500, 2000])
    deposit_event_id = uuid_str()

    events.append({
        "event_id": deposit_event_id,
        "event_ts": random_date(START_DATE, END_DATE),
        "user_id": user_id,
        "account_id": account_id,
        "event_type": "DEPOSIT",
        "direction": "CREDIT",
        "amount": deposit_amount,
        "currency": CURRENCY,
        "reference_id": None
    })

    # Purchases
    num_purchases = random.randint(1, 5)

    for _ in range(num_purchases):
        purchase_amount = random.choice([10, 15, 20, 25])
        purchase_event_id = uuid_str()
        purchase_ts = random_date(START_DATE, END_DATE)

        # Purchase
        events.append({
            "event_id": purchase_event_id,
            "event_ts": purchase_ts,
            "user_id": user_id,
            "account_id": account_id,
            "event_type": "PURCHASE",
            "direction": "DEBIT",
            "amount": purchase_amount,
            "currency": CURRENCY,
            "reference_id": None
        })

        # Platform fee (linked to purchase)
        fee_amount = round(purchase_amount * 0.05, 2)
        events.append({
            "event_id": uuid_str(),
            "event_ts": purchase_ts,
            "user_id": user_id,
            "account_id": account_id,
            "event_type": "FEE",
            "direction": "DEBIT",
            "amount": fee_amount,
            "currency": CURRENCY,
            "reference_id": purchase_event_id
        })

        # Occasionally issue a refund
        if random.random() < 0.1:
            events.append({
                "event_id": uuid_str(),
                "event_ts": purchase_ts + timedelta(days=1),
                "user_id": user_id,
                "account_id": account_id,
                "event_type": "REFUND",
                "direction": "CREDIT",
                "amount": purchase_amount,
                "currency": CURRENCY,
                "reference_id": purchase_event_id
            })

events_df = pd.DataFrame(events)

# -----------------------------
# Write to disk
# -----------------------------
users_df.to_parquet(RAW_DATA_PATH / "users.parquet", index=False)
accounts_df.to_parquet(RAW_DATA_PATH / "accounts.parquet", index=False)
events_df.to_parquet(RAW_DATA_PATH / "events.parquet", index=False)

print("Synthetic data generation complete.")
print(f"Users: {len(users_df)}")
print(f"Accounts: {len(accounts_df)}")
print(f"Events: {len(events_df)}")
