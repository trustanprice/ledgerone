"""
ingest_freddie_mac_validation_data.py

Purpose:
- Ingest the Freddie Mac Single-Family Loan-Level Dataset (SFLLD) 2021
  vintage sample (50,000 loans) and map it into the same grain as
  credit_risk__stg_loan_performance, for the credit-risk validation /
  backtest module (notebooks/03_credit_risk_validation_backtest.ipynb).
- Read-only against the raw Freddie Mac files in data/external/freddie_mac/
  (gitignored — not redistributed, see that folder's AGENTS.md). Writes
  two derived Parquet files the validation notebook reads.

Usage:
    python src/ingest_freddie_mac_validation_data.py

Column layout — verified, not assumed
--------------------------------------
Freddie Mac's published layout has changed several times since 2018.
Before writing this script, the *current* (January 2026) General User
Guide was fetched directly from freddiemac.com and cross-checked against
the actual sample files by value-pattern, not just position — e.g.
column 20 of the origination file was confirmed as Loan Sequence Number
because every value is a unique PYYQnXXXXXX-shaped string, not because
the doc says "column 20." Two things fell out of that verification:

1. The origination file has 31 delimited fields, one short of the
   documented 32 — it's missing the newest field (MI Cancellation
   Indicator, column 32), consistent with this sample predating that
   specific field's addition. Not a parsing bug.
2. The performance file has 35 delimited fields, three MORE than the
   documented 32 — columns 33-35 aren't in the January 2026 layout at
   all. This script does not use or guess at those three columns.

This script only reads the handful of columns it actually needs, and
every one of them was independently confirmed by inspecting its value
distribution across all 50,000 loans / ~2.8M performance records before
being trusted (see dbt/models/credit_risk/README.md's Validation section
for the specifics) — not by reading the position off the PDF alone:

  Origination file (0-indexed, DuckDB's default headerless CSV naming —
  zero-padded to 2 digits since the file has >9 columns):
    column01 -> First Payment Date (YYYYMM)      -> origination_quarter
    column19 -> Loan Sequence Number              -> account_id

  Performance file (0-indexed, same zero-padded naming):
    column00 -> Loan Sequence Number                -> account_id
    column01 -> Monthly Reporting Period (YYYYMM)    -> performance_month
    column02 -> Current Actual UPB                    -> statement_balance
    column03 -> Current Loan Delinquency Status         -> delinquency_bucket (mapped, see below)
    column04 -> Loan Age                                 -> months_on_book
    column08 -> Zero Balance Code                         -> is_loss_event (mapped, see below)

Delinquency bucket mapping — a modeling decision, not a mechanical translation
-------------------------------------------------------------------------------
Freddie Mac's Current Loan Delinquency Status is a 2-digit MBA-method
code: '00' = current, '01' = 30-59 days delinquent, '02' = 60-89, '03' =
90-119, and so on upward in whole months delinquent; 'RA' = REO
Acquisition (post-foreclosure). Mapped onto this project's existing
5-bucket scheme (the same one credit_risk__stg_loan_performance uses):

    '00'            -> Current
    '01'            -> 30-59 DPD
    '02'            -> 60-89 DPD
    '03'            -> 90-119 DPD
    '04' and higher,
    or 'RA'          -> 120+/Charged-Off

Mortgages don't "charge off" the way credit cards do — there's no single
event that zeroes the balance the way a card issuer's charge-off does.
120+/Charged-Off here is used as a SERIOUS DELINQUENCY / FORECLOSURE-
ADJACENT proxy for loss, consistent with how the validation notebook
frames it. is_loss_event flags the specific zero-balance codes that
represent an actual realized credit loss (Third Party Sale, Short Sale/
Charge Off, REO Disposition — codes 02, 03, 09) as opposed to a benign
termination (voluntary payoff, whole/note loan sale — 01, 15, 16) or a
still-active loan (blank).
"""

from __future__ import annotations

from pathlib import Path

import duckdb

PROJECT_ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = PROJECT_ROOT / "data" / "external" / "freddie_mac"
ORIG_FILE = RAW_DIR / "sample_orig_2021.txt"
PERF_FILE = RAW_DIR / "sample_perf_2021.txt"
OUT_DIR = RAW_DIR  # derived parquet lives alongside the raw txt, still gitignored

LOSS_EVENT_ZERO_BALANCE_CODES = ("02", "03", "09")


def main() -> None:
    if not ORIG_FILE.exists() or not PERF_FILE.exists():
        raise FileNotFoundError(
            f"Expected Freddie Mac sample files at {RAW_DIR} "
            "(sample_orig_2021.txt, sample_perf_2021.txt) — not found."
        )

    con = duckdb.connect()

    print("Ingesting originations...")
    originations = con.execute(f"""
        select
            column19 as account_id,
            date_trunc('quarter', strptime(column01, '%Y%m')::date)::date as origination_quarter
        from read_csv(
            '{ORIG_FILE.as_posix()}',
            header = false,
            delim = '|',
            all_varchar = true
        )
    """).df()
    originations.to_parquet(OUT_DIR / "loan_originations.parquet", index=False)
    print(f"  {len(originations):,} accounts, {originations['origination_quarter'].nunique()} cohorts")

    print("Ingesting monthly performance (this is the ~270MB file, may take a minute)...")
    performance = con.execute(f"""
        select
            column00 as account_id,
            strptime(column01, '%Y%m')::date as performance_month,
            cast(column02 as double) as statement_balance,
            column03 as delinquency_status_code,
            cast(column04 as integer) as months_on_book,
            case
                when column03 = '00' then 'Current'
                when column03 = '01' then '30-59 DPD'
                when column03 = '02' then '60-89 DPD'
                when column03 = '03' then '90-119 DPD'
                else '120+/Charged-Off'  -- '04'+ (numeric) or 'RA'
            end as delinquency_bucket,
            column08 in {LOSS_EVENT_ZERO_BALANCE_CODES} as is_loss_event
        from read_csv(
            '{PERF_FILE.as_posix()}',
            header = false,
            delim = '|',
            all_varchar = true
        )
    """).df()
    performance.to_parquet(OUT_DIR / "loan_performance.parquet", index=False)
    print(f"  {len(performance):,} performance rows")
    print(f"  Delinquency bucket distribution:\n{performance['delinquency_bucket'].value_counts()}")
    print(f"  Loss events: {int(performance['is_loss_event'].sum())}")

    con.close()
    print(f"\nWrote {OUT_DIR / 'loan_originations.parquet'} and {OUT_DIR / 'loan_performance.parquet'}")


if __name__ == "__main__":
    main()
