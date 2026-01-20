"""
validate_ledger.py

Purpose:
- Run sanity checks and financial integrity validations on the LedgerOne ledger.
- This script is read-only (it does NOT modify ledger tables).
- It prints results and exits with a non-zero code if any "hard" validations fail.

Usage:
    python src/validate_ledger.py

Notes:
- Assumes the DuckDB database is at: data/processed/ledgerone.duckdb
- Assumes ledger table exists: ledger_entries
"""

from __future__ import annotations

import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import duckdb
import pandas as pd


# -----------------------------
# Config
# -----------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[1]  # .../ledgerone
DB_PATH = PROJECT_ROOT / "data" / "processed" / "ledgerone.duckdb"

LEDGER_TABLE = "ledger_entries"

# What should be true in a correct MVP ledger:
EXPECTED_EVENT_POSTING = {
    "DEPOSIT": "CREDIT",
    "PURCHASE": "DEBIT",
    "FEE": "DEBIT",
    "REFUND": "CREDIT",
    # "INVEST": depends on how you model it later
}

REQUIRED_COLUMNS = [
    "ledger_entry_id",
    "event_id",
    "event_ts",
    "user_id",
    "account_id",
    "event_type",
    "posting_type",
    "amount",
    "currency",
    "reference_id",
]


@dataclass
class CheckResult:
    name: str
    passed: bool
    details: str = ""
    preview: Optional[pd.DataFrame] = None


def _print_header(title: str) -> None:
    line = "=" * len(title)
    print(f"\n{title}\n{line}")


def _print_result(res: CheckResult) -> None:
    status = "PASS ✅" if res.passed else "FAIL ❌"
    print(f"[{status}] {res.name}")
    if res.details:
        print(f"  - {res.details}")
    if res.preview is not None and not res.preview.empty:
        print("  - Preview:")
        # Keep prints compact
        with pd.option_context("display.max_rows", 12, "display.max_columns", 20):
            print(res.preview)


def connect(db_path: Path) -> duckdb.DuckDBPyConnection:
    if not db_path.exists():
        raise FileNotFoundError(
            f"DuckDB file not found at: {db_path}\n"
            "Run your pipeline first (e.g., python src/run_pipeline.py) to create the DB and ledger table."
        )
    return duckdb.connect(str(db_path), read_only=True)


# -----------------------------
# Checks
# -----------------------------
def check_table_exists(con: duckdb.DuckDBPyConnection) -> CheckResult:
    df = con.execute("SHOW TABLES").df()
    exists = LEDGER_TABLE in set(df["name"].tolist()) if "name" in df.columns else False
    details = f"Found tables: {df['name'].tolist()}" if "name" in df.columns else "Could not list tables."
    return CheckResult(
        name=f"Ledger table '{LEDGER_TABLE}' exists",
        passed=exists,
        details=details,
    )


def check_required_columns(con: duckdb.DuckDBPyConnection) -> CheckResult:
    df = con.execute(f"DESCRIBE {LEDGER_TABLE}").df()
    cols = df["column_name"].tolist()
    missing = [c for c in REQUIRED_COLUMNS if c not in cols]
    return CheckResult(
        name="Ledger schema has required columns",
        passed=(len(missing) == 0),
        details=("Missing columns: " + ", ".join(missing)) if missing else "All required columns present.",
    )


def check_row_count(con: duckdb.DuckDBPyConnection) -> CheckResult:
    n = con.execute(f"SELECT COUNT(*) FROM {LEDGER_TABLE}").fetchone()[0]
    return CheckResult(
        name="Ledger has at least 1 row",
        passed=(n > 0),
        details=f"Row count = {n}",
    )


def check_non_positive_amounts(con: duckdb.DuckDBPyConnection) -> CheckResult:
    df = con.execute(f"""
        SELECT COUNT(*) AS non_positive_amounts
        FROM {LEDGER_TABLE}
        WHERE amount <= 0
    """).df()
    bad = int(df.loc[0, "non_positive_amounts"])
    return CheckResult(
        name="All amounts are positive (amount > 0)",
        passed=(bad == 0),
        details=f"Non-positive amounts = {bad}",
    )


def check_posting_type_values(con: duckdb.DuckDBPyConnection) -> CheckResult:
    df = con.execute(f"""
        SELECT DISTINCT posting_type
        FROM {LEDGER_TABLE}
        ORDER BY posting_type
    """).df()
    values = set(df["posting_type"].tolist())
    allowed = {"CREDIT", "DEBIT"}
    bad = sorted(list(values - allowed))
    return CheckResult(
        name="posting_type only contains CREDIT/DEBIT",
        passed=(len(bad) == 0),
        details=f"Distinct posting_type values = {sorted(values)}" if not bad else f"Invalid values: {bad}",
    )


def check_event_posting_consistency(con: duckdb.DuckDBPyConnection) -> CheckResult:
    # Any event_type with posting_type not matching our expected mapping
    df = con.execute(f"""
        SELECT event_type, posting_type, COUNT(*) AS cnt
        FROM {LEDGER_TABLE}
        GROUP BY 1,2
        ORDER BY 1,2
    """).df()

    mismatches = []
    for event_type, expected_posting in EXPECTED_EVENT_POSTING.items():
        subset = df[df["event_type"] == event_type]
        for _, row in subset.iterrows():
            if row["posting_type"] != expected_posting:
                mismatches.append(row)

    mismatch_df = pd.DataFrame(mismatches) if mismatches else pd.DataFrame()
    return CheckResult(
        name="Event types map to expected posting_type (DEPOSIT=CREDIT, PURCHASE=DEBIT, FEE=DEBIT, REFUND=CREDIT)",
        passed=mismatch_df.empty,
        details="Distribution looks consistent." if mismatch_df.empty else "Found event_type/posting_type mismatches.",
        preview=mismatch_df.head(12) if not mismatch_df.empty else None,
    )


def check_fee_reference_present(con: duckdb.DuckDBPyConnection) -> CheckResult:
    df = con.execute(f"""
        SELECT COUNT(*) AS fees_missing_reference
        FROM {LEDGER_TABLE}
        WHERE event_type = 'FEE'
          AND reference_id IS NULL
    """).df()
    bad = int(df.loc[0, "fees_missing_reference"])
    return CheckResult(
        name="FEE entries have reference_id (linked to a purchase)",
        passed=(bad == 0),
        details=f"Fees missing reference_id = {bad}",
    )


def check_refund_reference_present(con: duckdb.DuckDBPyConnection) -> CheckResult:
    df = con.execute(f"""
        SELECT COUNT(*) AS refunds_missing_reference
        FROM {LEDGER_TABLE}
        WHERE event_type = 'REFUND'
          AND reference_id IS NULL
    """).df()
    bad = int(df.loc[0, "refunds_missing_reference"])
    return CheckResult(
        name="REFUND entries have reference_id (linked to a purchase)",
        passed=(bad == 0),
        details=f"Refunds missing reference_id = {bad}",
    )


def check_refund_links_to_purchase(con: duckdb.DuckDBPyConnection) -> CheckResult:
    df = con.execute(f"""
        SELECT COUNT(*) AS refunds_missing_purchase
        FROM {LEDGER_TABLE} r
        LEFT JOIN {LEDGER_TABLE} p
          ON r.reference_id = p.event_id
         AND p.event_type = 'PURCHASE'
        WHERE r.event_type = 'REFUND'
          AND p.event_id IS NULL
    """).df()
    bad = int(df.loc[0, "refunds_missing_purchase"])
    return CheckResult(
        name="REFUND reference_id points to an existing PURCHASE event_id",
        passed=(bad == 0),
        details=f"Refunds missing a matching purchase = {bad}",
    )


def check_refund_amount_matches_purchase(con: duckdb.DuckDBPyConnection) -> CheckResult:
    mismatch_df = con.execute(f"""
        SELECT
          r.event_id AS refund_event_id,
          r.reference_id AS purchase_event_id,
          r.amount AS refund_amount,
          p.amount AS purchase_amount,
          r.user_id,
          r.account_id
        FROM {LEDGER_TABLE} r
        JOIN {LEDGER_TABLE} p
          ON r.reference_id = p.event_id
        WHERE r.event_type = 'REFUND'
          AND p.event_type = 'PURCHASE'
          AND r.amount <> p.amount
        LIMIT 50
    """).df()

    return CheckResult(
        name="REFUND amount equals PURCHASE amount for referenced purchase",
        passed=mismatch_df.empty,
        details="No mismatches found." if mismatch_df.empty else "Refund amount mismatches found.",
        preview=mismatch_df if not mismatch_df.empty else None,
    )


def report_balance_summary(con: duckdb.DuckDBPyConnection) -> None:
    _print_header("Balance Summary (Derived)")
    df = con.execute(f"""
        WITH balances AS (
          SELECT
            account_id,
            SUM(CASE WHEN posting_type = 'CREDIT' THEN amount ELSE -amount END) AS balance
          FROM {LEDGER_TABLE}
          GROUP BY 1
        )
        SELECT
          MIN(balance) AS min_balance,
          AVG(balance) AS avg_balance,
          MAX(balance) AS max_balance,
          COUNT(*) AS num_accounts
        FROM balances
    """).df()
    print(df)

    low10 = con.execute(f"""
        SELECT
          account_id,
          SUM(CASE WHEN posting_type = 'CREDIT' THEN amount ELSE -amount END) AS balance
        FROM {LEDGER_TABLE}
        GROUP BY 1
        ORDER BY balance ASC
        LIMIT 10
    """).df()
    print("\nLowest 10 balances:\n", low10)

    high10 = con.execute(f"""
        SELECT
          account_id,
          SUM(CASE WHEN posting_type = 'CREDIT' THEN amount ELSE -amount END) AS balance
        FROM {LEDGER_TABLE}
        GROUP BY 1
        ORDER BY balance DESC
        LIMIT 10
    """).df()
    print("\nHighest 10 balances:\n", high10)


def report_event_totals(con: duckdb.DuckDBPyConnection) -> None:
    _print_header("Event Totals (By event_type)")
    df = con.execute(f"""
        SELECT
          event_type,
          posting_type,
          COUNT(*) AS cnt,
          SUM(amount) AS total_amount
        FROM {LEDGER_TABLE}
        GROUP BY 1,2
        ORDER BY 1,2
    """).df()
    print(df)


def main() -> None:
    _print_header("LedgerOne — Ledger Validation")

    print(f"Project root: {PROJECT_ROOT}")
    print(f"DuckDB path : {DB_PATH}")
    print(f"Ledger table: {LEDGER_TABLE}")

    try:
        con = connect(DB_PATH)
    except Exception as e:
        print("\nFAILED to connect to DuckDB:", str(e))
        sys.exit(2)

    checks = [
        check_table_exists,
        check_required_columns,
        check_row_count,
        check_posting_type_values,
        check_non_positive_amounts,
        check_event_posting_consistency,
        check_fee_reference_present,
        check_refund_reference_present,
        check_refund_links_to_purchase,
        check_refund_amount_matches_purchase,
    ]

    failures = 0
    _print_header("Validation Checks")

    for check_fn in checks:
        try:
            res = check_fn(con)
        except Exception as e:
            res = CheckResult(
                name=f"{check_fn.__name__} (exception)",
                passed=False,
                details=str(e),
            )
        _print_result(res)
        if not res.passed:
            failures += 1

    # Reporting (informational, not pass/fail)
    report_event_totals(con)
    report_balance_summary(con)

    con.close()

    _print_header("Result")
    if failures == 0:
        print("All validations passed ✅")
        sys.exit(0)
    else:
        print(f"Validations failed ❌  (failures = {failures})")
        sys.exit(1)


if __name__ == "__main__":
    main()
