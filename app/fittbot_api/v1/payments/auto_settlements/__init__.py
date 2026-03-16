"""
Auto Settlements & Payouts - New reconciliation and auto-payout system.

Business Logic:
- No scan dependency: gym owners get paid regardless of client check-in
- Razorpay settlement confirmation triggers payout scheduling
- Daily pass & Sessions: Bulk transfer every Monday
- Gym membership: Next day transfer after settlement
- Deductions:
    Regular: 2% PG charges + 2% TDS from owner's amount
    No-cost EMI (gym membership only): 5% flat deduction
"""
