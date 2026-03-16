

import os
import sys
import csv
import time
import json
import argparse
import datetime as dt
from typing import Dict, Any, List, Tuple
import requests
from datetime import date

API_BASE = "https://api.razorpay.com/v1"
COUNT_PER_PAGE = 100
TIMEZONE = "Asia/Kolkata"

# ---------- Time helpers ----------
def ist_zone():
    try:
        from zoneinfo import ZoneInfo
        return ZoneInfo(TIMEZONE)
    except Exception:
        print("This script needs Python 3.9+ (zoneinfo).", file=sys.stderr)
        sys.exit(1)

def ist_midnight_range(target_date: dt.date) -> Tuple[int, int]:
    z = ist_zone()
    start = dt.datetime(target_date.year, target_date.month, target_date.day, 0, 0, 0, tzinfo=z)
    end = start + dt.timedelta(days=1)
    return int(start.timestamp()), int(end.timestamp())

def daterange_inclusive(d1: dt.date, d2: dt.date):
    cur = d1
    while cur <= d2:
        yield cur
        cur = cur + dt.timedelta(days=1)

def env(varname: str) -> str:
    v = os.getenv(varname)
    if not v:
        print(f"Missing environment variable: {varname}", file=sys.stderr)
        sys.exit(1)
    return v

# ---------- HTTP ----------
def api_get(path: str, params: Dict[str, Any], auth) -> Dict[str, Any]:
    url = f"{API_BASE}{path}"
    r = requests.get(url, params=params, auth=auth, timeout=60)
    if r.status_code >= 400:
        try:
            err = r.json()
        except Exception:
            err = {"error": r.text}
        print(f"HTTP {r.status_code} GET {url}\nParams: {params}\n{json.dumps(err, indent=2)}", file=sys.stderr)
        sys.exit(1)
    return r.json()

def fetch_paginated(path: str, base_params: Dict[str, Any], auth, item_key="items") -> List[Dict[str, Any]]:
    out = []
    skip = 0
    while True:
        params = dict(base_params)
        params["count"] = base_params.get("count", COUNT_PER_PAGE)
        params["skip"] = skip
        data = api_get(path, params, auth)
        items = data.get(item_key, [])
        out.extend(items)
        if len(items) < params["count"]:
            break
        skip += params["count"]
        time.sleep(0.2)  # polite
    return out

# ---------- Razorpay fetchers ----------
def fetch_settlements_range_ist(d_from: dt.date, d_to: dt.date, auth) -> List[Dict[str, Any]]:
    ts_from, _ = ist_midnight_range(d_from)
    _, ts_to_end = ist_midnight_range(d_to)
    params = {"from": ts_from, "to": ts_to_end, "count": COUNT_PER_PAGE}
    return fetch_paginated("/settlements", params, auth)

def fetch_recon_combined_for_day(day: dt.date, auth) -> List[Dict[str, Any]]:
    params = {"year": day.year, "month": day.month, "day": day.day, "count": COUNT_PER_PAGE}
    return fetch_paginated("/settlements/recon/combined", params, auth)

def fetch_recon_combined_range(d_from: dt.date, d_to: dt.date, auth) -> List[Dict[str, Any]]:
    all_items: List[Dict[str, Any]] = []
    for day in daterange_inclusive(d_from, d_to):
        items = fetch_recon_combined_for_day(day, auth)
        all_items.extend(items)
    return all_items

# ---------- Math helpers ----------
def paise(x): return int(x or 0)
def rs(p): return f"₹{p/100:.2f}"

def rollup_by_settlement(recon_items: List[Dict[str, Any]]) -> Dict[str, Dict[str, int]]:
    agg: Dict[str, Dict[str, int]] = {}
    for it in recon_items:
        # consider only settled lines
        if not it.get("settled"):
            continue
        sid = it.get("settlement_id") or "NO_SETTLEMENT_ID"
        a = agg.setdefault(sid, {"credit":0,"debit":0,"fee":0,"tax":0,"count":0})
        a["credit"] += paise(it.get("credit"))
        a["debit"]  += paise(it.get("debit"))
        a["fee"]    += paise(it.get("fee"))
        a["tax"]    += paise(it.get("tax"))
        a["count"]  += 1
    return agg

def expected_net(roll):  # to bank
    return roll["credit"] - roll["debit"] - roll["fee"] - roll["tax"]

# ---------- CSV ----------
def write_csv(filename: str, rows: List[Dict[str, Any]], fieldnames: List[str]) -> None:
    if not rows:
        return
    with open(filename, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k) for k in fieldnames})

# ---------- Main ----------
def parse_args():
    p = argparse.ArgumentParser(description="Razorpay settlement reconciliation for a date or range (IST).")
    g = p.add_mutually_exclusive_group()
    g.add_argument("--date", help="YYYY-MM-DD (IST) single date")
    p.add_argument("--from", dest="date_from", help="YYYY-MM-DD (IST) start date (inclusive)")
    p.add_argument("--to", dest="date_to", help="YYYY-MM-DD (IST) end date (inclusive)")
    return p.parse_args()

def resolve_dates(args) -> Tuple[dt.date, dt.date]:
    if args.date:
        d = dt.date.fromisoformat(args.date)
        return d, d
    if args.date_from and args.date_to:
        d1 = dt.date.fromisoformat(args.date_from)
        d2 = dt.date.fromisoformat(args.date_to)
        if d2 < d1:
            print("--to must be on/after --from", file=sys.stderr); sys.exit(1)
        return d1, d2
    # default: yesterday IST
    z = ist_zone()
    now_ist = dt.datetime.now(z).date()
    return now_ist - dt.timedelta(days=1), now_ist - dt.timedelta(days=1)

def main():
    args = parse_args()
    #d_from, d_to = resolve_dates(args)

    
    key = env("RAZORPAY_KEY_ID")
    secret = env("RAZORPAY_KEY_SECRET")
    auth = (key, secret)

    d_from_str = "2025-09-19"
    d_to_str   = "2025-09-11"

    # Parse to date objects
    d_from = date.fromisoformat(d_from_str)
    d_to   = date.fromisoformat(d_to_str)


    if d_to < d_from:
        print(f"⚠️  'to' ({d_to}) is before 'from' ({d_from}). Swapping to keep an inclusive range.")
        d_from, d_to = d_to, d_from


    print(f"\nReconciling Razorpay settlements for range {d_from} to {d_to} (IST)…\n")

    settlements = fetch_settlements_range_ist(d_from, d_to, auth)
    recon_items = fetch_recon_combined_range(d_from, d_to, auth)

    # Save raw CSVs (range-wide)
    write_csv(
        f"settlements_{d_from}_to_{d_to}.csv",
        settlements,
        fieldnames=[
            "id","amount","fees","tax","status","utr",
            "created_at","processed_at","initiated_at","reversed_at",
            "bank_account_id","batch_id"
        ],
    )

    # Normalize recon lines for easier viewing
    norm = []
    for it in recon_items:
        norm.append({
            "settlement_id": it.get("settlement_id"),
            "settlement_utr": it.get("settlement_utr"),
            "type": it.get("type"),
            "payment_id": it.get("payment_id"),
            "order_id": it.get("order_id"),
            "amount": it.get("amount"),
            "credit": it.get("credit"),
            "debit": it.get("debit"),
            "fee": it.get("fee"),
            "tax": it.get("tax"),
            "currency": it.get("currency"),
            "settled": it.get("settled"),
            "settled_at": it.get("settled_at"),
            "on_hold": it.get("on_hold"),
        })
    write_csv(
        f"recon_{d_from}_to_{d_to}.csv",
        norm,
        fieldnames=list(norm[0].keys()) if norm else []
    )

    # Roll up and compare
    roll = rollup_by_settlement(recon_items)
    settlements_by_id = {s["id"]: s for s in settlements}

    # Print report
    def line(): print("-" * 100)

    if not settlements and not recon_items:
        print("No settlements or recon line items found in this range.")
        return

    line()
    print(f"SUMMARY (Range: {d_from} → {d_to})")
    line()

    # union of ids from both sources
    all_ids = sorted(set(list(roll.keys()) + list(settlements_by_id.keys())), key=lambda x: (x=="NO_SETTLEMENT_ID", x))
    g_expected = 0
    g_reported = 0

    for sid in all_ids:
        r = roll.get(sid, {"credit":0,"debit":0,"fee":0,"tax":0,"count":0})
        exp_net = expected_net(r)
        s = settlements_by_id.get(sid)
        rep_net = paise(s.get("amount")) if s else 0
        utr = (s.get("utr") if s else "") or ""

        print(f"Settlement: {sid}")
        print(f"  UTR: {utr or '(n/a)'}")
        print(f"  Lines: {r['count']} | Credits: {rs(r['credit'])}  Debits: {rs(r['debit'])}")
        print(f"  Fees: {rs(r['fee'])}  Tax: {rs(r['tax'])}")
        print(f"  Expected Net (credits - debits - fees - tax): {rs(exp_net)}")
        if s:
            print(f"  Razorpay Reported Net: {rs(rep_net)} | Status: {s.get('status')}")
            diff = rep_net - exp_net
            sign = "+" if diff >= 0 else ""
            print(f"  Diff (Reported - Expected): {sign}{rs(diff)}")
        else:
            print("  (No matching settlement found in /settlements for this id in the given range)")
        line()

        g_expected += exp_net
        g_reported += rep_net

    print("GRAND TOTALS (range)")
    print(f"  Expected Net Sum: {rs(g_expected)}")
    print(f"  Razorpay Reported Net Sum: {rs(g_reported)}")
    diff_total = g_reported - g_expected
    sign = "+" if diff_total >= 0 else ""
    print(f"  Diff Total: {sign}{rs(diff_total)}")
    line()

    print("Files generated:")
    if settlements: print(f"  - settlements_{d_from}_to_{d_to}.csv")
    if recon_items: print(f"  - recon_{d_from}_to_{d_to}.csv")
    print("\nMatch UTRs above with your bank statement entries for final verification.")

if __name__ == "__main__":
    main()
