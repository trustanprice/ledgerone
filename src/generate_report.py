"""
generate_report.py

Purpose:
- Query the dbt marts (main_marts schema) and produce a small set of
  business-readable outputs: a revenue/refund trend chart, an average
  transaction value chart, and a CSV export. This is the "so what" step —
  the pipeline output a business stakeholder could actually look at.
- Read-only. Does not modify any tables.

Usage:
    python src/generate_report.py

Assumes:
- The dbt project has already been built (dbt build), so
  main_marts.monthly_revenue_and_refund_rate and
  main_marts.avg_transaction_value_trend exist in the DuckDB file at
  data/processed/ledgerone.duckdb.
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import matplotlib.pyplot as plt

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DB_PATH = PROJECT_ROOT / "data" / "processed" / "ledgerone.duckdb"
REPORT_DIR = PROJECT_ROOT / "reports"


def main() -> None:
    REPORT_DIR.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(str(DB_PATH), read_only=True)

    revenue = con.execute("""
        SELECT month_start, gross_revenue, refunds, net_revenue, refund_rate
        FROM main_marts.monthly_revenue_and_refund_rate
        ORDER BY month_start
    """).df()

    avg_txn = con.execute("""
        SELECT month_start, purchase_count, avg_purchase_amount
        FROM main_marts.avg_transaction_value_trend
        ORDER BY month_start
    """).df()

    con.close()

    revenue["month_label"] = revenue["month_start"].astype(str).str.slice(0, 7)
    avg_txn["month_label"] = avg_txn["month_start"].astype(str).str.slice(0, 7)

    # --- CSV export ---
    csv_path = REPORT_DIR / "monthly_revenue_and_refund_rate.csv"
    revenue.to_csv(csv_path, index=False)
    print(f"Wrote {csv_path}")

    # --- Chart 1: gross vs net revenue trend ---
    fig, ax = plt.subplots(figsize=(8, 4.5))
    ax.plot(revenue["month_label"], revenue["gross_revenue"], marker="o", label="Gross revenue")
    ax.plot(revenue["month_label"], revenue["net_revenue"], marker="o", label="Net revenue")
    ax.set_title("Monthly gross vs. net revenue")
    ax.set_xlabel("Month")
    ax.set_ylabel("USD")
    ax.legend()
    fig.tight_layout()
    revenue_chart_path = REPORT_DIR / "monthly_revenue_trend.png"
    fig.savefig(revenue_chart_path, dpi=150)
    plt.close(fig)
    print(f"Wrote {revenue_chart_path}")

    # --- Chart 2: refund rate trend ---
    fig, ax = plt.subplots(figsize=(8, 4.5))
    ax.bar(revenue["month_label"], revenue["refund_rate"] * 100)
    ax.set_title("Monthly refund rate")
    ax.set_xlabel("Month")
    ax.set_ylabel("Refund rate (%)")
    fig.tight_layout()
    refund_chart_path = REPORT_DIR / "refund_rate_trend.png"
    fig.savefig(refund_chart_path, dpi=150)
    plt.close(fig)
    print(f"Wrote {refund_chart_path}")

    # --- Chart 3: average transaction value trend ---
    fig, ax = plt.subplots(figsize=(8, 4.5))
    ax.plot(avg_txn["month_label"], avg_txn["avg_purchase_amount"], marker="o", color="darkorange")
    ax.set_title("Average purchase value trend")
    ax.set_xlabel("Month")
    ax.set_ylabel("Avg purchase amount (USD)")
    fig.tight_layout()
    avg_txn_chart_path = REPORT_DIR / "avg_transaction_value_trend.png"
    fig.savefig(avg_txn_chart_path, dpi=150)
    plt.close(fig)
    print(f"Wrote {avg_txn_chart_path}")

    print("\nReport generation complete.")


if __name__ == "__main__":
    main()
