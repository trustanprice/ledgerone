"""
run_pipeline.py

Purpose:
- Build / refresh the LedgerOne DuckDB warehouse by executing SQL files in order.
- This is the "one command" pipeline runner.

Usage:
    python src/run_pipeline.py
"""

from pathlib import Path
import duckdb


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DB_PATH = PROJECT_ROOT / "data" / "processed" / "ledgerone.duckdb"

SQL_FILES = [
    "sql/ledger.sql",
    "sql/fact_transactions.sql",
    "sql/daily_account_balances.sql",
    "sql/monthly_revenue.sql",
]


def main() -> None:
    # Ensure processed directory exists
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(str(DB_PATH))

    for rel_path in SQL_FILES:
        sql_path = PROJECT_ROOT / rel_path
        if not sql_path.exists():
            raise FileNotFoundError(f"Missing SQL file: {sql_path}")

        with open(sql_path, "r") as f:
            con.execute(f.read())

        print(f"✅ Executed: {rel_path}")

    # Quick confirmations
    print("\n--- Row counts ---")
    for tbl in ["ledger_entries", "fact_transactions", "daily_account_balances", "monthly_revenue"]:
        try:
            n = con.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
            print(f"{tbl}: {n}")
        except Exception as e:
            print(f"{tbl}: ERROR ({e})")

    con.close()
    print(f"\n✅ Pipeline complete. DuckDB at: {DB_PATH}")


if __name__ == "__main__":
    main()
