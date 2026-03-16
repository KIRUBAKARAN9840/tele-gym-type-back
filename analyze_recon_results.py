"""Analyze reconciliation simulation results for correctness."""

import pymysql
from decimal import Decimal

conn = pymysql.connect(host="localhost", user="root", database="fittbot_payments")
cur = conn.cursor(pymysql.cursors.DictCursor)

print("=" * 70)
print("  ANALYSIS: Checking payout correctness")
print("=" * 70)

# 1. Payout schedule check
print("\n1. PAYOUT SCHEDULE CHECK")
print("   (gym_membership → next day, everything else → Monday bulk)")
cur.execute("""
    SELECT p.source_type, po.payout_type, COUNT(*) as cnt,
           SUM(po.amount_net) as total_net
    FROM payouts po
    JOIN payments p ON po.payment_id = p.id
    WHERE po.payout_type IS NOT NULL
    GROUP BY p.source_type, po.payout_type
    ORDER BY p.source_type
""")
schedule_ok = 0
schedule_wrong = 0
for r in cur.fetchall():
    st = r["source_type"] or "(empty)"
    pt = r["payout_type"] or "(null)"
    if st == "gym_membership":
        ok = "immediate" in pt
    else:
        ok = "bulk" in pt or "monday" in pt
    if ok:
        schedule_ok += 1
    else:
        schedule_wrong += 1
    status = "OK" if ok else "WRONG!"
    print(f"   {st:>25} -> {pt:<25}  ({r['cnt']:>3} payments, Rs.{r['total_net']:>10})  {status}")
print(f"\n   Schedule check: {schedule_ok} correct, {schedule_wrong} wrong")

# Old payouts (before simulation)
cur.execute("SELECT COUNT(*) as cnt FROM payouts WHERE payout_type IS NULL")
null_type = cur.fetchone()["cnt"]
if null_type > 0:
    print(f"   Note: {null_type} pre-existing payouts with NULL payout_type (not from this simulation)")

# 2. Deduction math check
print("\n2. DEDUCTION MATH CHECK (all simulation payouts)")
cur.execute("""
    SELECT po.payment_id, p.source_type, p.amount_net as owner_amt,
           po.pg_fee, po.tds, po.commission, po.amount_net as payout_net
    FROM payouts po
    JOIN payments p ON po.payment_id = p.id
    WHERE po.payout_type IS NOT NULL
""")
total = 0
math_errors = 0
rate_errors = 0
for r in cur.fetchall():
    total += 1
    owner = Decimal(str(r["owner_amt"]))
    pg = Decimal(str(r["pg_fee"]))
    tds_val = Decimal(str(r["tds"]))
    comm = Decimal(str(r["commission"]))
    net = Decimal(str(r["payout_net"]))

    # Check net = owner - pg - tds - commission
    expected_net = owner - pg - tds_val - comm
    if abs(net - expected_net) > Decimal("0.02"):
        math_errors += 1
        print(f"   MATH ERROR: Payment {r['payment_id']} | {owner} - {pg} - {tds_val} - {comm} = {expected_net}, got {net}")

    # Check PG = 2% of owner
    expected_pg = (owner * Decimal("0.02")).quantize(Decimal("0.01"))
    if pg != expected_pg:
        rate_errors += 1
        print(f"   PG RATE ERROR: Payment {r['payment_id']} | owner={owner}, pg={pg}, expected_pg={expected_pg}")

    # Check TDS = 2% of owner
    expected_tds = (owner * Decimal("0.02")).quantize(Decimal("0.01"))
    if tds_val != expected_tds:
        rate_errors += 1
        print(f"   TDS RATE ERROR: Payment {r['payment_id']} | owner={owner}, tds={tds_val}, expected_tds={expected_tds}")

print(f"   Checked {total} payouts: {math_errors} math errors, {rate_errors} rate errors")

# 3. Anomaly: net > gross
print("\n3. AMOUNT ANOMALY CHECK (owner_amount vs client_amount)")
cur.execute("""
    SELECT p.id, p.source_type, p.amount_gross, p.amount_net, p.gateway_payment_id
    FROM payments p
    WHERE p.status = 'settled' AND p.amount_net > p.amount_gross
""")
anomalies = cur.fetchall()
print(f"   Payments where owner_amount (amount_net) > client_paid (amount_gross): {len(anomalies)}")
if anomalies:
    print("   PROBLEM: Owner's base price higher than what client paid!")
    print("   This means markup was negative — should investigate these:")
    for r in anomalies:
        diff = Decimal(str(r["amount_net"])) - Decimal(str(r["amount_gross"]))
        print(f"     ID={r['id']} | {str(r['source_type']):>15} | client_paid=Rs.{r['amount_gross']} | owner_base=Rs.{r['amount_net']} | diff=Rs.{diff} | {r['gateway_payment_id']}")

# 4. gym_id = 0
print("\n4. GYM_ID = 0 CHECK")
cur.execute("""
    SELECT COUNT(*) as cnt, SUM(po.amount_net) as total_net
    FROM payouts po
    JOIN payments p ON po.payment_id = p.id
    WHERE p.gym_id = 0 AND po.payout_type IS NOT NULL
""")
r = cur.fetchone()
cnt = r["cnt"]
total_net = r["total_net"] or 0
print(f"   Payouts for gym_id=0: {cnt} payments, total_net=Rs.{total_net}")
if cnt > 0:
    print("   WARNING: gym_id=0 means no gym assigned - CANNOT transfer to any gym owner!")
    cur.execute("""
        SELECT p.id, p.source_type, po.amount_net, p.gateway_payment_id
        FROM payments p
        JOIN payouts po ON po.payment_id = p.id
        WHERE p.gym_id = 0 AND po.payout_type IS NOT NULL
        LIMIT 5
    """)
    for r2 in cur.fetchall():
        print(f"     ID={r2['id']} | {str(r2['source_type']):>15} | payout=Rs.{r2['amount_net']} | {r2['gateway_payment_id']}")

# 5. Payment method from Razorpay
print("\n5. PAYMENT METHODS (backfilled from Razorpay API)")
cur.execute("""
    SELECT payment_method, is_no_cost_emi, COUNT(*) as cnt
    FROM payments
    WHERE status = 'settled'
    GROUP BY payment_method, is_no_cost_emi
    ORDER BY cnt DESC
""")
for r in cur.fetchall():
    method = r["payment_method"] or "(null)"
    emi = "YES" if r["is_no_cost_emi"] else "NO"
    print(f"   method={method:>12}, no_cost_emi={emi}: {r['cnt']} payments")

# 6. Empty source_type
print("\n6. MISSING SOURCE TYPE")
cur.execute("""
    SELECT COUNT(*) as cnt FROM payments p
    JOIN payouts po ON po.payment_id = p.id
    WHERE (p.source_type IS NULL OR p.source_type = '') AND po.payout_type IS NOT NULL
""")
no_source = cur.fetchone()["cnt"]
print(f"   Payments with empty source_type: {no_source}")
if no_source > 0:
    print("   WARNING: Without source_type, schedule defaults to Monday bulk")

# 7. Summary verdict
print("\n" + "=" * 70)
print("  VERDICT")
print("=" * 70)
issues = []
if schedule_wrong > 0:
    issues.append(f"Schedule wrong for {schedule_wrong} types")
if math_errors > 0:
    issues.append(f"{math_errors} deduction math errors")
if rate_errors > 0:
    issues.append(f"{rate_errors} rate calculation errors")
if len(anomalies) > 0:
    issues.append(f"{len(anomalies)} payments where owner_amount > client_amount")
if cnt > 0:
    issues.append(f"{cnt} payouts for gym_id=0 (no gym assigned)")

if not issues:
    print("  ALL CHECKS PASSED! Reconciliation logic is working correctly.")
else:
    print("  ISSUES FOUND:")
    for i, issue in enumerate(issues, 1):
        print(f"    {i}. {issue}")

cur.close()
conn.close()
