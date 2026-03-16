"""
Fetch Razorpay Settlement Recon data for the past month and export to Excel.

Uses LIVE Razorpay API keys.
Run: python fetch_settlement_recon.py
"""

import base64
import json
import httpx
import pandas as pd
from datetime import date, timedelta, datetime
from typing import Any, Dict, List, Optional

# ── Live Razorpay Keys ────────────────────────────────────────────────
RAZORPAY_KEY_ID = "rzp_test_Re3HukCxYQVmOh"
RAZORPAY_KEY_SECRET = "suMlQgeEz6OU35m6UBW9ag8z"
RAZORPAY_BASE = "https://api.razorpay.com/v1"


def _auth_headers() -> Dict[str, str]:
    auth = f"{RAZORPAY_KEY_ID}:{RAZORPAY_KEY_SECRET}"
    encoded = base64.b64encode(auth.encode()).decode()
    return {
        "Authorization": f"Basic {encoded}",
        "Content-Type": "application/json",
    }


def fetch_settlement_recon(year: int, month: int, day: Optional[int] = None, count: int = 100, skip: int = 0) -> Dict[str, Any]:
    """GET /v1/settlements/recon/combined"""
    params: Dict[str, Any] = {"year": year, "month": month, "count": count, "skip": skip}
    if day:
        params["day"] = day

    resp = httpx.get(
        f"{RAZORPAY_BASE}/settlements/recon/combined",
        headers=_auth_headers(),
        params=params,
        timeout=30.0,
    )
    resp.raise_for_status()
    return resp.json()


def fetch_all_recon_items(year: int, month: int, day: Optional[int] = None) -> List[Dict[str, Any]]:
    """Paginate through all recon items for a given date."""
    all_items = []
    skip = 0
    page_size = 100

    while True:
        resp = fetch_settlement_recon(year=year, month=month, day=day, count=page_size, skip=skip)
        items = resp.get("items", [])
        all_items.extend(items)
        print(f"  Fetched {len(items)} items (skip={skip}, total so far={len(all_items)})")
        if len(items) < page_size:
            break
        skip += page_size

    return all_items


def fetch_settlements_list(from_ts: Optional[int] = None, to_ts: Optional[int] = None) -> List[Dict[str, Any]]:
    """GET /v1/settlements — fetch list of settlements."""
    all_settlements = []
    skip = 0
    page_size = 100

    while True:
        params: Dict[str, Any] = {"count": page_size, "skip": skip}
        if from_ts:
            params["from"] = from_ts
        if to_ts:
            params["to"] = to_ts

        resp = httpx.get(
            f"{RAZORPAY_BASE}/settlements",
            headers=_auth_headers(),
            params=params,
            timeout=30.0,
        )
        resp.raise_for_status()
        data = resp.json()
        items = data.get("items", [])
        all_settlements.extend(items)
        print(f"  Fetched {len(items)} settlements (skip={skip}, total so far={len(all_settlements)})")
        if len(items) < page_size:
            break
        skip += page_size

    return all_settlements


def ts_to_ist(ts: Optional[int]) -> str:
    """Convert Unix timestamp to IST datetime string."""
    if not ts:
        return ""
    from datetime import timezone
    ist = timezone(timedelta(hours=5, minutes=30))
    dt = datetime.fromtimestamp(ts, tz=ist)
    return dt.strftime("%Y-%m-%d %H:%M:%S IST")


def paise_to_rupees(paise: Optional[int]) -> float:
    if paise is None:
        return 0.0
    return round(paise / 100, 2)


def main():
    today = date.today()
    one_month_ago = today - timedelta(days=30)

    print("=" * 60)
    print(f"Fetching Razorpay Settlement Recon: {one_month_ago} to {today}")
    print("=" * 60)

    # ── Sheet 1: Settlement Recon Items (day by day for past month) ───
    print("\n--- Fetching Settlement Recon Items ---")
    all_recon_items = []

    current = one_month_ago
    while current <= today:
        print(f"\nDate: {current}")
        items = fetch_all_recon_items(year=current.year, month=current.month, day=current.day)
        for item in items:
            item["_fetch_date"] = str(current)
        all_recon_items.extend(items)
        current += timedelta(days=1)

    print(f"\nTotal recon items fetched: {len(all_recon_items)}")

    # ── Sheet 2: Settlements List ─────────────────────────────────────
    print("\n--- Fetching Settlements List ---")
    from_ts = int(datetime.combine(one_month_ago, datetime.min.time()).timestamp())
    to_ts = int(datetime.combine(today, datetime.max.time()).timestamp())
    settlements = fetch_settlements_list(from_ts=from_ts, to_ts=to_ts)
    print(f"Total settlements fetched: {len(settlements)}")

    # ── Build Excel ───────────────────────────────────────────────────
    output_file = f"razorpay_settlement_recon_{one_month_ago}_{today}.xlsx"

    # Process recon items into flat rows
    recon_rows = []
    for item in all_recon_items:
        recon_rows.append({
            "Fetch Date": item.get("_fetch_date", ""),
            "Type": item.get("type", ""),
            "Entity ID (Payment/Refund ID)": item.get("entity_id", ""),
            "Amount (₹)": paise_to_rupees(item.get("amount")),
            "Fee (₹)": paise_to_rupees(item.get("fee")),
            "Tax (₹)": paise_to_rupees(item.get("tax")),
            "Debit (₹)": paise_to_rupees(item.get("debit")),
            "Credit (₹)": paise_to_rupees(item.get("credit")),
            "Currency": item.get("currency", ""),
            "Settlement ID": item.get("settlement_id", ""),
            "Order ID": item.get("order_id", ""),
            "Method": item.get("method", ""),
            "Description": item.get("description", ""),
            "Card Network": item.get("card_network", ""),
            "Card Issuer": item.get("card_issuer", ""),
            "Card Type": item.get("card_type", ""),
            "Dispute ID": item.get("dispute_id", ""),
            "Settled At": ts_to_ist(item.get("settled_at")),
            "Created At": ts_to_ist(item.get("created_at")),
            "Notes": json.dumps(item.get("notes", {})) if item.get("notes") else "",
        })

    # Process settlements into flat rows
    settlement_rows = []
    for stl in settlements:
        settlement_rows.append({
            "Settlement ID": stl.get("id", ""),
            "Amount (₹)": paise_to_rupees(stl.get("amount")),
            "Fees (₹)": paise_to_rupees(stl.get("fees")),
            "Tax (₹)": paise_to_rupees(stl.get("tax")),
            "UTR": stl.get("utr", ""),
            "Status": stl.get("status", ""),
            "Created At": ts_to_ist(stl.get("created_at")),
            "Settled At": ts_to_ist(stl.get("settled_at")),
        })

    # Write to Excel with multiple sheets
    with pd.ExcelWriter(output_file, engine="openpyxl") as writer:
        if recon_rows:
            df_recon = pd.DataFrame(recon_rows)
            df_recon.to_excel(writer, sheet_name="Recon Items", index=False)

            # Summary by type
            summary = df_recon.groupby("Type").agg(
                Count=("Type", "count"),
                Total_Amount=("Amount (₹)", "sum"),
                Total_Fee=("Fee (₹)", "sum"),
                Total_Credit=("Credit (₹)", "sum"),
                Total_Debit=("Debit (₹)", "sum"),
            ).reset_index()
            summary.to_excel(writer, sheet_name="Summary by Type", index=False)

            # Summary by method
            method_summary = df_recon[df_recon["Type"] == "payment"].groupby("Method").agg(
                Count=("Method", "count"),
                Total_Amount=("Amount (₹)", "sum"),
                Total_Fee=("Fee (₹)", "sum"),
            ).reset_index()
            method_summary.to_excel(writer, sheet_name="Payments by Method", index=False)

            # Daily summary
            daily_summary = df_recon.groupby("Fetch Date").agg(
                Total_Items=("Fetch Date", "count"),
                Total_Amount=("Amount (₹)", "sum"),
                Total_Credit=("Credit (₹)", "sum"),
                Total_Debit=("Debit (₹)", "sum"),
            ).reset_index()
            daily_summary.to_excel(writer, sheet_name="Daily Summary", index=False)
        else:
            pd.DataFrame({"Info": ["No recon items found for this period"]}).to_excel(
                writer, sheet_name="Recon Items", index=False
            )

        if settlement_rows:
            df_stl = pd.DataFrame(settlement_rows)
            df_stl.to_excel(writer, sheet_name="Settlements", index=False)
        else:
            pd.DataFrame({"Info": ["No settlements found for this period"]}).to_excel(
                writer, sheet_name="Settlements", index=False
            )

        # Raw JSON dump for reference
        raw_data = []
        for i, item in enumerate(all_recon_items[:500]):  # limit to 500 for Excel
            raw_data.append({
                "Index": i + 1,
                "Raw JSON": json.dumps(item, indent=None),
            })
        if raw_data:
            pd.DataFrame(raw_data).to_excel(writer, sheet_name="Raw JSON (first 500)", index=False)

    print(f"\n{'=' * 60}")
    print(f"Excel saved: {output_file}")
    print(f"{'=' * 60}")
    print(f"\nSheets:")
    print(f"  1. Recon Items       — All recon items with readable columns")
    print(f"  2. Summary by Type   — payment/refund/adjustment counts & totals")
    print(f"  3. Payments by Method — card/upi/emi/netbanking breakdown")
    print(f"  4. Daily Summary     — Day-wise totals")
    print(f"  5. Settlements       — Settlement records")
    print(f"  6. Raw JSON          — Raw Razorpay response for reference")


if __name__ == "__main__":
    main()
