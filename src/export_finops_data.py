"""
export_finops_data.py

Purpose:
- Static data export for the walkthrough site's Infrastructure &
  Productionization tab: a real cost snapshot of the AWS account
  infra/ledgerone-stack.yml deploys into, via the Cost Explorer API.
- Read-only, and deliberately NOT part of `make all` or any other export
  script here — this queries a live AWS account, not this repo's own
  data, and costs run up independent of anything this repo controls.
  Export it occasionally by hand, not on every build.

Usage:
    python src/export_finops_data.py

Requires AWS credentials with Cost Explorer read access (ce:GetCostAndUsage)
for the account infra/ledgerone-stack.yml is deployed into, and Cost
Explorer itself enabled for that account (Billing console -> Cost
Explorer -> Enable -- a one-time manual toggle, not something this script
or any IAM policy can turn on; AWS also takes up to 24h after enabling
before data appears). If either isn't true yet, this script prints why
and exits without writing anything -- apps/walkthrough's Infrastructure
tab renders an explicit "not available yet" state rather than fabricate
a number, same as every other honest-gap call this project makes.

Output: apps/walkthrough/public/data/finops_snapshot.json (only written
on success -- if it's missing, that's the "not available yet" signal the
site's FinOps section checks for).
"""

from __future__ import annotations

import json
import sys
from datetime import date, timedelta
from pathlib import Path

import boto3
from botocore.exceptions import ClientError

PROJECT_ROOT = Path(__file__).resolve().parents[1]
OUT_DIR = PROJECT_ROOT / "apps" / "walkthrough" / "public" / "data"
OUT_FILE = OUT_DIR / "finops_snapshot.json"

# Cost Explorer's granularity floor is DAILY; look back 30 days so a
# still-mostly-empty account (this one, as of this writing) at least has
# a real window to report against instead of just "today."
LOOKBACK_DAYS = 30


def main() -> None:
    client = boto3.client("ce", region_name="us-east-1")  # Cost Explorer is a global API, always us-east-1
    end = date.today()
    start = end - timedelta(days=LOOKBACK_DAYS)

    try:
        response = client.get_cost_and_usage(
            TimePeriod={"Start": start.isoformat(), "End": end.isoformat()},
            Granularity="DAILY",
            Metrics=["UnblendedCost"],
            GroupBy=[{"Type": "DIMENSION", "Key": "SERVICE"}],
        )
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code", "")
        if code == "AccessDeniedException" or "not enabled for cost explorer" in str(exc).lower():
            print(
                "Skipping finops_snapshot.json -- Cost Explorer isn't enabled on this "
                "AWS account yet. Enable it once in the Billing console (Billing -> "
                "Cost Explorer -> Enable) -- a one-time manual step, not something "
                "this script or an IAM policy can do."
            )
            return
        if code == "DataUnavailableException":
            print(
                "Skipping finops_snapshot.json -- Cost Explorer is enabled, but AWS "
                "hasn't finished ingesting usage data yet (can lag up to 24h after "
                "first enabling, or after very recent activity). Not an error on this "
                "script's end -- just re-run later once data actually exists."
            )
            return
        raise

    by_service: dict[str, float] = {}
    daily_total: list[dict] = []
    for day in response["ResultsByTime"]:
        day_total = 0.0
        for group in day["Groups"]:
            service = group["Keys"][0]
            amount = float(group["Metrics"]["UnblendedCost"]["Amount"])
            by_service[service] = by_service.get(service, 0.0) + amount
            day_total += amount
        daily_total.append({"date": day["TimePeriod"]["Start"], "amount": round(day_total, 4)})

    total = sum(by_service.values())
    if total == 0.0:
        print(
            "Cost Explorer responded, but every line item is $0.00 over the last "
            f"{LOOKBACK_DAYS} days -- plausible for an account this new (this "
            "project's own AWS footprint is estimated at well under $1/month, see "
            "infra/AGENTS.md), or AWS just hasn't finished processing usage into "
            "Cost Explorer yet (can lag by up to 24h). Writing the snapshot anyway "
            "-- $0.00 is a real, honest answer, not a reason to skip."
        )

    services = sorted(
        ({"service": s, "amount": round(a, 4)} for s, a in by_service.items() if a > 0),
        key=lambda r: r["amount"],
        reverse=True,
    )

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    with open(OUT_FILE, "w") as f:
        json.dump(
            {
                "as_of": end.isoformat(),
                "window_days": LOOKBACK_DAYS,
                "total_unblended_cost": round(total, 4),
                "by_service": services,
                "daily_total": daily_total,
            },
            f,
            indent=2,
        )
    print(f"Wrote {OUT_FILE}  (total over last {LOOKBACK_DAYS}d: ${total:.4f})")


if __name__ == "__main__":
    sys.exit(main())
