"""
Simulate the full reconciliation flow:
1. Fetch settlement recon items from Razorpay (test mode)
2. Match each payment against FittbotPayment records in local MySQL
3. Fetch payment details from Razorpay to detect EMI / no-cost EMI
4. Calculate deductions (2% PG + 2% TDS for regular, 5% flat for no-cost EMI)
5. Create Payout records in DB
6. Export full results to Excel

Run: python simulate_reconciliation.py
"""

import base64
import json
import pymysql
import httpx
import pandas as pd
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Dict, List, Optional, Tuple

# ── Config ────────────────────────────────────────────────────────────
RAZORPAY_KEY_ID = "rzp_test_Re3HukCxYQVmOh"
RAZORPAY_KEY_SECRET = "suMlQgeEz6OU35m6UBW9ag8z"
RAZORPAY_BASE = "https://api.razorpay.com/v1"

DB_HOST = "localhost"
DB_USER = "root"
DB_NAME = "fittbot_payments"

IST = timezone(timedelta(hours=5, minutes=30))

# Deduction rates
PG_FEE_RATE = Decimal("0.02")
TDS_RATE = Decimal("0.02")
NO_COST_EMI_RATE = Decimal("0.05")
TWO_PLACES = Decimal("0.01")

# Schedule rules
MONDAY_BULK_TYPES = {"daily_pass", "yoga", "zumba", "personal_training", "hiit",
                     "crossfit", "pilates", "dance", "boxing", "aerobic",
                     "strength", "swimming", "functional", "personal_training_session"}
NEXT_DAY_TYPES = {"gym_membership"}


# ── Razorpay API ──────────────────────────────────────────────────────

def _auth_headers() -> Dict[str, str]:
    auth = f"{RAZORPAY_KEY_ID}:{RAZORPAY_KEY_SECRET}"
    encoded = base64.b64encode(auth.encode()).decode()
    return {"Authorization": f"Basic {encoded}", "Content-Type": "application/json"}


def rp_get(url: str, params: Optional[Dict] = None) -> Dict[str, Any]:
    resp = httpx.get(url, headers=_auth_headers(), params=params, timeout=30.0)
    resp.raise_for_status()
    return resp.json()


def fetch_all_recon_items(year: int, month: int, day: Optional[int] = None) -> List[Dict]:
    all_items = []
    skip = 0
    while True:
        params = {"year": year, "month": month, "count": 100, "skip": skip}
        if day:
            params["day"] = day
        data = rp_get(f"{RAZORPAY_BASE}/settlements/recon/combined", params)
        items = data.get("items", [])
        all_items.extend(items)
        if len(items) < 100:
            break
        skip += 100
    return all_items


def fetch_payment_detail(payment_id: str) -> Dict[str, Any]:
    """GET /v1/payments/:id — to check method, offer_id for EMI detection."""
    return rp_get(f"{RAZORPAY_BASE}/payments/{payment_id}")


# ── Deduction Calculator ─────────────────────────────────────────────

def _round(amount: Decimal) -> Decimal:
    return amount.quantize(TWO_PLACES, rounding=ROUND_HALF_UP)


def calculate_deductions(owner_amount: Decimal, is_no_cost_emi: bool = False) -> Dict[str, Any]:
    owner_amount = Decimal(str(owner_amount))
    if is_no_cost_emi:
        emi_ded = _round(owner_amount * NO_COST_EMI_RATE)
        return {
            "owner_amount": owner_amount,
            "pg_fee": Decimal("0.00"),
            "tds": Decimal("0.00"),
            "emi_deduction": emi_ded,
            "net_to_owner": _round(owner_amount - emi_ded),
            "is_no_cost_emi": True,
            "deduction_type": "No-cost EMI (5% flat)",
        }
    pg = _round(owner_amount * PG_FEE_RATE)
    tds = _round(owner_amount * TDS_RATE)
    return {
        "owner_amount": owner_amount,
        "pg_fee": pg,
        "tds": tds,
        "emi_deduction": Decimal("0.00"),
        "net_to_owner": _round(owner_amount - pg - tds),
        "is_no_cost_emi": False,
        "deduction_type": "Regular (2% PG + 2% TDS)",
    }


def determine_schedule(source_type: str) -> Tuple[str, date]:
    today = datetime.now(IST).date()
    if source_type in NEXT_DAY_TYPES:
        return "immediate (next day)", today + timedelta(days=1)
    days_ahead = 7 - today.weekday()
    if today.weekday() == 0:
        days_ahead = 7
    return "bulk_monday", today + timedelta(days=days_ahead)


def ts_to_ist(ts: Optional[int]) -> str:
    if not ts:
        return ""
    return datetime.fromtimestamp(ts, tz=IST).strftime("%Y-%m-%d %H:%M:%S IST")


def paise_to_rupees(paise) -> Decimal:
    if paise is None:
        return Decimal("0.00")
    return Decimal(str(paise)) / 100


# ── Main Simulation ──────────────────────────────────────────────────

def main():
    print("=" * 70)
    print("  RECONCILIATION SIMULATION")
    print("=" * 70)

    # Connect to DB
    conn = pymysql.connect(host=DB_HOST, user=DB_USER, database=DB_NAME)
    cur = conn.cursor(pymysql.cursors.DictCursor)

    # Load all payments from DB indexed by gateway_payment_id
    cur.execute("""
        SELECT id, source_type, source_id, gym_id, client_id,
               amount_gross, amount_net, gateway_payment_id, gateway_order_id,
               status, paid_at, created_at
        FROM payments
        WHERE gateway_payment_id IS NOT NULL AND gateway_payment_id != ''
    """)
    db_payments = {row["gateway_payment_id"]: row for row in cur.fetchall()}
    print(f"\nLoaded {len(db_payments)} payments from DB")

    # Fetch recon items from Razorpay (past 2 months to cover all)
    print("\nFetching settlement recon from Razorpay...")
    today = date.today()
    all_recon = []
    # Fetch current month
    items = fetch_all_recon_items(year=today.year, month=today.month)
    print(f"  {today.year}-{today.month:02d}: {len(items)} items")
    all_recon.extend(items)
    # Fetch previous month
    prev = today.replace(day=1) - timedelta(days=1)
    items = fetch_all_recon_items(year=prev.year, month=prev.month)
    print(f"  {prev.year}-{prev.month:02d}: {len(items)} items")
    all_recon.extend(items)

    # Deduplicate by entity_id + settlement_id
    seen = set()
    unique_recon = []
    for item in all_recon:
        key = (item.get("entity_id", ""), item.get("settlement_id", ""))
        if key not in seen:
            seen.add(key)
            unique_recon.append(item)
    print(f"\nTotal unique recon items: {len(unique_recon)}")

    # Filter to payment-type items only
    payment_recon = [i for i in unique_recon if i.get("type") == "payment"]
    print(f"Payment-type items: {len(payment_recon)}")

    # ── Process each recon item ──────────────────────────────────────
    results = []
    matched = 0
    not_found = 0
    already_settled = 0
    payouts_to_create = []

    for idx, item in enumerate(payment_recon, 1):
        rp_payment_id = item.get("entity_id", "")
        rp_amount_paise = item.get("amount", 0)
        rp_fee_paise = item.get("fee", 0)
        rp_tax_paise = item.get("tax", 0)
        settlement_id = item.get("settlement_id", "")
        rp_method = item.get("method", "")

        rp_amount_rupees = paise_to_rupees(rp_amount_paise)
        rp_fee_rupees = paise_to_rupees(rp_fee_paise)

        row = {
            "Recon #": idx,
            "Razorpay Payment ID": rp_payment_id,
            "Settlement ID": settlement_id,
            "RP Amount (₹)": float(rp_amount_rupees),
            "RP Fee (₹)": float(rp_fee_rupees),
            "RP Method": rp_method,
            "Settled At": ts_to_ist(item.get("settled_at")),
        }

        # Match with our DB
        db_payment = db_payments.get(rp_payment_id)

        if not db_payment:
            row.update({
                "Match Status": "NOT FOUND",
                "DB Payment ID": "",
                "Source Type": "",
                "Gym ID": "",
                "DB Gross (₹)": "",
                "DB Net (Owner ₹)": "",
                "Amount Match": "",
                "EMI Check": "",
                "Is No-Cost EMI": "",
                "Deduction Type": "",
                "PG Fee (₹)": "",
                "TDS (₹)": "",
                "EMI Deduction (₹)": "",
                "Net to Owner (₹)": "",
                "Payout Schedule": "",
                "Payout Date": "",
            })
            not_found += 1
            results.append(row)
            continue

        matched += 1
        source_type = db_payment["source_type"] or ""
        owner_amount = Decimal(str(db_payment["amount_net"]))
        gross_amount = Decimal(str(db_payment["amount_gross"]))

        # Amount match check
        amount_match = "YES" if rp_amount_rupees == gross_amount else f"MISMATCH (RP={rp_amount_rupees}, DB={gross_amount})"

        # Fetch full payment detail from Razorpay to check EMI
        is_no_cost_emi = False
        emi_check_result = "N/A"
        try:
            rp_detail = fetch_payment_detail(rp_payment_id)
            actual_method = rp_detail.get("method", "")
            offer_id = rp_detail.get("offer_id")
            emi_check_result = f"method={actual_method}, offer_id={offer_id}"

            if actual_method == "emi" and offer_id:
                is_no_cost_emi = True

            # Update payment_method in DB
            cur.execute(
                "UPDATE payments SET payment_method = %s, is_no_cost_emi = %s WHERE id = %s",
                (actual_method, 1 if is_no_cost_emi else 0, db_payment["id"])
            )
        except Exception as e:
            emi_check_result = f"API error: {e}"

        # Calculate deductions
        deductions = calculate_deductions(owner_amount, is_no_cost_emi)

        # Determine payout schedule
        payout_type, payout_date = determine_schedule(source_type)

        row.update({
            "Match Status": "MATCHED",
            "DB Payment ID": db_payment["id"],
            "Source Type": source_type,
            "Gym ID": db_payment["gym_id"],
            "DB Gross (₹)": float(gross_amount),
            "DB Net (Owner ₹)": float(owner_amount),
            "Amount Match": amount_match,
            "EMI Check": emi_check_result,
            "Is No-Cost EMI": "YES" if is_no_cost_emi else "NO",
            "Deduction Type": deductions["deduction_type"],
            "PG Fee (₹)": float(deductions["pg_fee"]),
            "TDS (₹)": float(deductions["tds"]),
            "EMI Deduction (₹)": float(deductions["emi_deduction"]),
            "Net to Owner (₹)": float(deductions["net_to_owner"]),
            "Payout Schedule": payout_type,
            "Payout Date": str(payout_date),
        })

        payouts_to_create.append({
            "payment_id": db_payment["id"],
            "gym_id": db_payment["gym_id"],
            "amount_gross": owner_amount,
            "pg_fee": deductions["pg_fee"],
            "tds": deductions["tds"],
            "commission": deductions["emi_deduction"],
            "commission_rate": Decimal("5.00") if is_no_cost_emi else Decimal("0"),
            "amount_net": deductions["net_to_owner"],
            "payout_type": payout_type.replace(" (next day)", ""),
            "scheduled_for": payout_date,
            "status": "scheduled",
            "source_type": source_type,
            "is_no_cost_emi": is_no_cost_emi,
        })

        results.append(row)

        if idx % 20 == 0:
            print(f"  Processed {idx}/{len(payment_recon)}...")

    conn.commit()

    # ── Summary ──────────────────────────────────────────────────────
    print(f"\n{'=' * 70}")
    print(f"  RESULTS")
    print(f"{'=' * 70}")
    print(f"  Total recon payment items: {len(payment_recon)}")
    print(f"  Matched with DB:           {matched}")
    print(f"  Not found in DB:           {not_found}")

    if payouts_to_create:
        total_gross = sum(p["amount_gross"] for p in payouts_to_create)
        total_pg = sum(p["pg_fee"] for p in payouts_to_create)
        total_tds = sum(p["tds"] for p in payouts_to_create)
        total_emi = sum(p["commission"] for p in payouts_to_create)
        total_net = sum(p["amount_net"] for p in payouts_to_create)
        emi_count = sum(1 for p in payouts_to_create if p["is_no_cost_emi"])

        print(f"\n  Payouts to create:         {len(payouts_to_create)}")
        print(f"  No-cost EMI payments:      {emi_count}")
        print(f"  ─────────────────────────────────────")
        print(f"  Total owner amount:        ₹{total_gross:,.2f}")
        print(f"  Total PG fees (2%):        ₹{total_pg:,.2f}")
        print(f"  Total TDS (2%):            ₹{total_tds:,.2f}")
        print(f"  Total EMI deductions (5%): ₹{total_emi:,.2f}")
        print(f"  Total net to owners:       ₹{total_net:,.2f}")

        # By source type
        print(f"\n  By Source Type:")
        type_summary = {}
        for p in payouts_to_create:
            st = p["source_type"] or "(unknown)"
            if st not in type_summary:
                type_summary[st] = {"count": 0, "gross": Decimal("0"), "net": Decimal("0")}
            type_summary[st]["count"] += 1
            type_summary[st]["gross"] += p["amount_gross"]
            type_summary[st]["net"] += p["amount_net"]
        for st, info in sorted(type_summary.items()):
            schedule = "Monday bulk" if st not in NEXT_DAY_TYPES else "Next day"
            print(f"    {st:>25}: {info['count']:>3} payments, ₹{info['gross']:>10,.2f} → ₹{info['net']:>10,.2f} net  [{schedule}]")

        # By gym
        print(f"\n  By Gym ID:")
        gym_summary = {}
        for p in payouts_to_create:
            gid = p["gym_id"]
            if gid not in gym_summary:
                gym_summary[gid] = {"count": 0, "gross": Decimal("0"), "net": Decimal("0")}
            gym_summary[gid]["count"] += 1
            gym_summary[gid]["gross"] += p["amount_gross"]
            gym_summary[gid]["net"] += p["amount_net"]
        for gid, info in sorted(gym_summary.items()):
            print(f"    Gym {gid:>5}: {info['count']:>3} payments, ₹{info['gross']:>10,.2f} → ₹{info['net']:>10,.2f} net")

    # ── Insert Payouts into DB ───────────────────────────────────────
    print(f"\n--- Inserting Payout records into DB ---")
    inserted = 0
    for p in payouts_to_create:
        try:
            cur.execute("""
                INSERT INTO payouts
                (payment_id, gym_id, amount_gross, pg_fee, tds, commission, commission_rate,
                 amount_net, payout_type, scheduled_for, status, scheduled_at, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
            """, (
                p["payment_id"], p["gym_id"],
                float(p["amount_gross"]), float(p["pg_fee"]), float(p["tds"]),
                float(p["commission"]), float(p["commission_rate"]),
                float(p["amount_net"]), p["payout_type"], p["scheduled_for"],
                p["status"], datetime.now(IST),
            ))
            inserted += 1
        except Exception as e:
            print(f"  Error inserting payout for payment {p['payment_id']}: {e}")

    # Update payment statuses to 'settled'
    for p in payouts_to_create:
        cur.execute(
            "UPDATE payments SET status = 'settled', settled_at = %s WHERE id = %s",
            (datetime.now(IST), p["payment_id"])
        )

    conn.commit()
    print(f"  Inserted {inserted} payout records")
    print(f"  Updated {len(payouts_to_create)} payments to 'settled'")

    # ── Export to Excel ──────────────────────────────────────────────
    output_file = "reconciliation_simulation_results.xlsx"
    df_results = pd.DataFrame(results)

    with pd.ExcelWriter(output_file, engine="openpyxl") as writer:
        # Sheet 1: Full results
        df_results.to_excel(writer, sheet_name="Reconciliation Results", index=False)

        # Sheet 2: Matched only
        df_matched = df_results[df_results["Match Status"] == "MATCHED"]
        if not df_matched.empty:
            df_matched.to_excel(writer, sheet_name="Matched Payments", index=False)

        # Sheet 3: Not found
        df_notfound = df_results[df_results["Match Status"] == "NOT FOUND"]
        if not df_notfound.empty:
            df_notfound.to_excel(writer, sheet_name="Not Found", index=False)

        # Sheet 4: Payout summary by gym
        if payouts_to_create:
            gym_rows = []
            for gid, info in sorted(gym_summary.items()):
                gym_rows.append({
                    "Gym ID": gid,
                    "Total Payments": info["count"],
                    "Total Owner Amount (₹)": float(info["gross"]),
                    "Total Net Payout (₹)": float(info["net"]),
                    "Total Deductions (₹)": float(info["gross"] - info["net"]),
                })
            pd.DataFrame(gym_rows).to_excel(writer, sheet_name="Payout by Gym", index=False)

        # Sheet 5: Payout summary by source type
        if payouts_to_create:
            type_rows = []
            for st, info in sorted(type_summary.items()):
                schedule = "Monday bulk" if st not in NEXT_DAY_TYPES else "Next day"
                type_rows.append({
                    "Source Type": st,
                    "Schedule": schedule,
                    "Payment Count": info["count"],
                    "Total Owner Amount (₹)": float(info["gross"]),
                    "Total Net Payout (₹)": float(info["net"]),
                    "Total Deductions (₹)": float(info["gross"] - info["net"]),
                })
            pd.DataFrame(type_rows).to_excel(writer, sheet_name="Payout by Source Type", index=False)

        # Sheet 6: Deductions breakdown
        if payouts_to_create:
            ded_rows = []
            for p in payouts_to_create:
                ded_rows.append({
                    "Payment ID": p["payment_id"],
                    "Gym ID": p["gym_id"],
                    "Source Type": p["source_type"],
                    "Owner Amount (₹)": float(p["amount_gross"]),
                    "PG Fee 2% (₹)": float(p["pg_fee"]),
                    "TDS 2% (₹)": float(p["tds"]),
                    "EMI 5% (₹)": float(p["commission"]),
                    "Net to Owner (₹)": float(p["amount_net"]),
                    "Is No-Cost EMI": "YES" if p["is_no_cost_emi"] else "NO",
                    "Payout Type": p["payout_type"],
                    "Payout Date": str(p["scheduled_for"]),
                })
            pd.DataFrame(ded_rows).to_excel(writer, sheet_name="Deductions Breakdown", index=False)

    print(f"\n{'=' * 70}")
    print(f"  Excel saved: {output_file}")
    print(f"{'=' * 70}")
    print(f"\n  Sheets:")
    print(f"    1. Reconciliation Results  — Full matching results (all items)")
    print(f"    2. Matched Payments        — Only matched payments with deductions")
    print(f"    3. Not Found               — Recon items not found in our DB")
    print(f"    4. Payout by Gym           — Net payout per gym owner")
    print(f"    5. Payout by Source Type    — Breakdown by payment type")
    print(f"    6. Deductions Breakdown    — Per-payment deduction details")

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
