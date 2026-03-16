from datetime import datetime

from sqlalchemy import (
    Boolean,
    Column,
    Date,
    DateTime,
    Enum,
    ForeignKey,
    Integer,
    Numeric,
    String,
    Text,
)
from sqlalchemy.dialects.postgresql import JSON

from app.models.database import Base

PAYMENTS_SCHEMA = "fittbot_payments"


class Settlement(Base):
    """
    Tracks when Razorpay settles payments to Fittbot's bank account.
    Razorpay typically settles T+1 or T+2 days after payment capture.
    """
    __tablename__ = "settlements"
    __table_args__ = {"schema": PAYMENTS_SCHEMA}

    id = Column(Integer, primary_key=True, autoincrement=True)
    razorpay_settlement_id = Column(String(100), unique=True, nullable=False)
    settlement_date = Column(Date, nullable=False, index=True)

    # Amounts
    gross_amount = Column(Numeric(14, 2), nullable=False)  # Total before Razorpay fee
    pg_fee = Column(Numeric(14, 2), nullable=False)  # Razorpay's fee (typically 2%)
    net_amount = Column(Numeric(14, 2), nullable=False)  # Amount received by Fittbot

    # Bank details
    utr = Column(String(100), nullable=True)  # Bank UTR number
    bank_account = Column(String(50), nullable=True)  # Last 4 digits

    # Status tracking
    status = Column(
        Enum("pending", "processed", "failed", name="fittbot_settlement_status"),
        default="pending",
        nullable=False,
    )
    payments_count = Column(Integer, default=0)  # Number of payments in this settlement
    processed_at = Column(DateTime, nullable=True)

    # Metadata
    raw_data = Column(JSON, nullable=True)  # Store full Razorpay response
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)


class Payment(Base):

    __tablename__ = "payments"
    __table_args__ = {"schema": PAYMENTS_SCHEMA}

    id = Column(Integer, primary_key=True, autoincrement=True)

    # Source identification
    source_type = Column(String(50), nullable=False, index=True)  # yoga, zumba, daily_pass, gym_membership, personal_training
    source_id = Column(String(100), nullable=True)  # session_id or purchase_id
    booking_day_id = Column(Integer, nullable=True)
    purchase_id = Column(Integer, nullable=True)
    entitlement_id = Column(String(100), nullable=True, index=True)  # checkin_token for sessions

    # Parties
    gym_id = Column(Integer, nullable=False, index=True)
    client_id = Column(Integer, nullable=False, index=True)
    session_id = Column(Integer, nullable=True, index=True)

    # Amounts
    amount_gross = Column(Numeric(14, 2), nullable=False)  # What client paid
    amount_net = Column(Numeric(14, 2), nullable=False)  # After initial calculations
    currency = Column(String(10), nullable=False, default="INR")

    # Gateway details
    gateway = Column(String(30), nullable=True)  # razorpay, phonepe, etc.
    gateway_payment_id = Column(String(100), nullable=True, index=True)  # Razorpay payment_id
    gateway_order_id = Column(String(100), nullable=True)  # Razorpay order_id
    payment_method = Column(String(30), nullable=True)  # Actual method: card, upi, emi, netbanking, wallet

    # No-cost EMI tracking (set during verify when gym has no_cost_emi enabled and method=emi)
    is_no_cost_emi = Column(Boolean, default=False, nullable=False)

    # Payment status
    status = Column(
        Enum("created", "paid", "settled", "failed", "refunded", name="fittbot_payment_status"),
        nullable=False,
        default="paid",
    )
    paid_at = Column(DateTime, nullable=True)

    # Settlement tracking (updated by reconciliation job)
    settlement_id = Column(Integer, ForeignKey(f"{PAYMENTS_SCHEMA}.settlements.id", ondelete="SET NULL"), nullable=True)
    settled_at = Column(DateTime, nullable=True)

    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)


# ═══════════════════════════════════════════════════════════════════════════════
# PAYMENT BREAKDOWN - Detailed breakdown of deductions
# ═══════════════════════════════════════════════════════════════════════════════
class PaymentBreakdown(Base):
    """
    Stores detailed breakdown of each payment's deductions.
    Components: base, pg_fee, commission, gst, tds, net_to_gym
    """
    __tablename__ = "payment_breakdowns"
    __table_args__ = {"schema": PAYMENTS_SCHEMA}

    id = Column(Integer, primary_key=True, autoincrement=True)
    payment_id = Column(Integer, ForeignKey(f"{PAYMENTS_SCHEMA}.payments.id", ondelete="CASCADE"), nullable=False, index=True)

    component = Column(String(40), nullable=False)  # base/pg_fee/commission/gst_on_commission/tds/net_to_gym
    amount = Column(Numeric(14, 2), nullable=False)
    rate_pct = Column(Numeric(6, 3), nullable=True)  # e.g., 2.000 for 2%
    description = Column(String(255), nullable=True)

    created_at = Column(DateTime, default=datetime.now)


# ═══════════════════════════════════════════════════════════════════════════════
# BULK TRANSFER - Groups multiple payouts into single bank transfer
# ═══════════════════════════════════════════════════════════════════════════════
class BulkTransfer(Base):
    """
    Groups multiple payouts for the same gym into a single bank transfer.
    - daily_pass/session → Bulk transfer every Monday
    - gym_membership/personal_training → Next day transfer
    """
    __tablename__ = "bulk_transfers"
    __table_args__ = {"schema": PAYMENTS_SCHEMA}

    id = Column(Integer, primary_key=True, autoincrement=True)

    # Reference
    transfer_ref = Column(String(50), unique=True, nullable=False)  # FBT_YYYYMMDD_GYMID_001

    # Recipient
    gym_id = Column(Integer, nullable=False, index=True)
    gym_owner_id = Column(Integer, nullable=True, index=True)

    # Transfer details
    transfer_type = Column(String(20), nullable=False)  # bulk_monday, immediate
    transfer_date = Column(Date, nullable=False, index=True)
    payout_count = Column(Integer, default=0)  # Number of payouts in this transfer

    # Amounts (aggregated from all payouts)
    total_gross = Column(Numeric(14, 2), nullable=False)  # Sum of all gross amounts
    total_pg_fee = Column(Numeric(14, 2), nullable=False)
    total_commission = Column(Numeric(14, 2), nullable=False)
    total_gst = Column(Numeric(14, 2), nullable=False)  # GST on commission
    total_tds = Column(Numeric(14, 2), nullable=False)
    total_net = Column(Numeric(14, 2), nullable=False)  # Final amount to gym owner

    # Bank transfer details
    bank_account_id = Column(String(100), nullable=True)  # Gym owner's bank account reference
    bank_account_number = Column(String(20), nullable=True)  # Last 4 digits for display
    bank_ifsc = Column(String(20), nullable=True)

    # Gateway payout details (Razorpay Payouts / Bank transfer)
    razorpay_payout_id = Column(String(100), nullable=True)
    razorpay_fund_account_id = Column(String(100), nullable=True)
    utr = Column(String(100), nullable=True)  # Bank UTR after successful transfer

    # Status
    status = Column(
        Enum("pending", "initiated", "processing", "credited", "failed", "reversed", name="fittbot_bulk_transfer_status"),
        default="pending",
        nullable=False,
    )
    failure_reason = Column(String(255), nullable=True)

    # Timestamps
    initiated_at = Column(DateTime, nullable=True)
    credited_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    # Metadata for tracking
    source_types = Column(JSON, nullable=True)  # ["yoga", "zumba", "daily_pass"]
    payment_ids = Column(JSON, nullable=True)  # [1, 2, 3, 4] for reference


# ═══════════════════════════════════════════════════════════════════════════════
# PAYOUT - Individual payout record linked to Payment
# ═══════════════════════════════════════════════════════════════════════════════
class Payout(Base):
    """
    Tracks payout to gym owner for each scanned payment.
    Created when owner scans (scan.py).
    Updated by reconciliation job with deductions and schedule.
    """
    __tablename__ = "payouts"
    __table_args__ = {"schema": PAYMENTS_SCHEMA}

    id = Column(Integer, primary_key=True, autoincrement=True)

    # Links
    payment_id = Column(Integer, ForeignKey(f"{PAYMENTS_SCHEMA}.payments.id", ondelete="CASCADE"), nullable=False, index=True)
    bulk_transfer_id = Column(Integer, ForeignKey(f"{PAYMENTS_SCHEMA}.bulk_transfers.id", ondelete="SET NULL"), nullable=True, index=True)

    # Recipient
    gym_id = Column(Integer, nullable=False, index=True)
    gym_owner_id = Column(Integer, nullable=True, index=True)

    # Amounts - Detailed breakdown
    amount_gross = Column(Numeric(14, 2), nullable=False)  # Original payment amount
    pg_fee = Column(Numeric(14, 2), default=0)  # Razorpay fee (~2%)
    commission = Column(Numeric(14, 2), default=0)  # Fittbot commission
    commission_rate = Column(Numeric(5, 2), default=0)  # Commission % (e.g., 10.00)
    gst = Column(Numeric(14, 2), default=0)  # GST on commission (18%)
    tds = Column(Numeric(14, 2), default=0)  # TDS (2% on commission)
    amount_net = Column(Numeric(14, 2), nullable=False)  # Final amount to gym owner

    # Scheduling
    payout_type = Column(String(20), nullable=True)  # bulk_monday, immediate
    scheduled_for = Column(Date, nullable=True, index=True)  # When transfer is scheduled

    # Status flow: ready_for_transfer → scheduled → initiated → processing → credited
    status = Column(
        Enum(
            "ready_for_transfer",  # Scanned, waiting for settlement
            "scheduled",  # Settlement confirmed, scheduled for transfer
            "initiated",  # Transfer initiated
            "processing",  # Transfer in progress
            "credited",  # Successfully transferred to gym
            "failed",  # Transfer failed
            "on_hold",  # Manually held
            name="fittbot_payout_status",
        ),
        default="ready_for_transfer",
        nullable=False,
    )

    # Transfer reference (when part of bulk transfer)
    transfer_ref = Column(String(100), nullable=True)

    # Hold management
    hold_reason = Column(String(255), nullable=True)
    held_at = Column(DateTime, nullable=True)
    held_by = Column(String(50), nullable=True)

    # Timestamps
    scheduled_at = Column(DateTime, nullable=True)  # When scheduled_for was set
    initiated_at = Column(DateTime, nullable=True)
    credited_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)


# ═══════════════════════════════════════════════════════════════════════════════
# PAYOUT EVENT - Audit trail for payout status changes
# ═══════════════════════════════════════════════════════════════════════════════
class PayoutEvent(Base):
    """Audit trail for payout status changes."""
    __tablename__ = "payout_events"
    __table_args__ = {"schema": PAYMENTS_SCHEMA}

    id = Column(Integer, primary_key=True, autoincrement=True)
    payout_id = Column(Integer, ForeignKey(f"{PAYMENTS_SCHEMA}.payouts.id", ondelete="CASCADE"), nullable=False, index=True)

    from_status = Column(String(40), nullable=True)
    to_status = Column(String(40), nullable=False)
    actor = Column(String(40), nullable=True)  # system/manual/webhook/reconciliation
    notes = Column(Text, nullable=True)
    event_data = Column(JSON, nullable=True)  # Additional context (renamed from metadata)

    created_at = Column(DateTime, default=datetime.now)


# ═══════════════════════════════════════════════════════════════════════════════
# RECONCILIATION - Tracks reconciliation job runs
# ═══════════════════════════════════════════════════════════════════════════════
class Reconciliation(Base):
    """
    Tracks each reconciliation job run.
    Runs daily at 11 AM to check Razorpay settlements.
    """
    __tablename__ = "reconciliations"
    __table_args__ = {"schema": PAYMENTS_SCHEMA}

    id = Column(Integer, primary_key=True, autoincrement=True)

    # Job details
    job_date = Column(Date, nullable=False, index=True)
    job_type = Column(String(50), nullable=False)  # daily_settlement, manual, retry

    # Settlement being reconciled
    settlement_id = Column(Integer, ForeignKey(f"{PAYMENTS_SCHEMA}.settlements.id", ondelete="SET NULL"), nullable=True)

    # Results
    payments_found = Column(Integer, default=0)
    payments_matched = Column(Integer, default=0)
    payments_mismatched = Column(Integer, default=0)
    payouts_scheduled = Column(Integer, default=0)

    # Status
    status = Column(
        Enum("running", "completed", "failed", "partial", name="fittbot_recon_job_status"),
        default="running",
        nullable=False,
    )
    error_message = Column(Text, nullable=True)

    # Timing
    started_at = Column(DateTime, default=datetime.now)
    completed_at = Column(DateTime, nullable=True)

    # Metadata
    raw_response = Column(JSON, nullable=True)  # Store Razorpay API response
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)


# ═══════════════════════════════════════════════════════════════════════════════
# RECONCILIATION ITEM - Individual payment reconciliation record
# ═══════════════════════════════════════════════════════════════════════════════
class ReconciliationItem(Base):
    """
    Tracks individual payment reconciliation within a job.
    Links Razorpay settlement item to our Payment record.
    """
    __tablename__ = "reconciliation_items"
    __table_args__ = {"schema": PAYMENTS_SCHEMA}

    id = Column(Integer, primary_key=True, autoincrement=True)

    reconciliation_id = Column(Integer, ForeignKey(f"{PAYMENTS_SCHEMA}.reconciliations.id", ondelete="CASCADE"), nullable=False, index=True)
    payment_id = Column(Integer, ForeignKey(f"{PAYMENTS_SCHEMA}.payments.id", ondelete="SET NULL"), nullable=True, index=True)
    payout_id = Column(Integer, ForeignKey(f"{PAYMENTS_SCHEMA}.payouts.id", ondelete="SET NULL"), nullable=True)

    # Razorpay data
    razorpay_payment_id = Column(String(100), nullable=True)
    razorpay_order_id = Column(String(100), nullable=True)
    razorpay_amount = Column(Numeric(14, 2), nullable=True)
    razorpay_fee = Column(Numeric(14, 2), nullable=True)
    razorpay_tax = Column(Numeric(14, 2), nullable=True)  # GST on Razorpay fee

    # Our data
    our_amount = Column(Numeric(14, 2), nullable=True)

    # Match status
    status = Column(
        Enum("matched", "mismatched", "not_found", "duplicate", name="fittbot_recon_item_status"),
        default="matched",
        nullable=False,
    )
    delta_amount = Column(Numeric(14, 2), nullable=True)  # Difference if mismatched
    notes = Column(String(255), nullable=True)

    created_at = Column(DateTime, default=datetime.now)


# ═══════════════════════════════════════════════════════════════════════════════
# INVOICE - Invoice for gym owner
# ═══════════════════════════════════════════════════════════════════════════════
class Invoice(Base):
    """
    Invoice generated for gym owner showing commission deductions.
    Generated when bulk transfer is completed.
    """
    __tablename__ = "invoices"
    __table_args__ = {"schema": PAYMENTS_SCHEMA}

    id = Column(Integer, primary_key=True, autoincrement=True)

    # References
    invoice_number = Column(String(50), unique=True, nullable=False)  # FBT/2024-25/001
    bulk_transfer_id = Column(Integer, ForeignKey(f"{PAYMENTS_SCHEMA}.bulk_transfers.id", ondelete="SET NULL"), nullable=True)
    gym_id = Column(Integer, nullable=False, index=True)
    gym_owner_id = Column(Integer, nullable=True)

    # Invoice details
    invoice_date = Column(Date, nullable=False)
    billing_period_start = Column(Date, nullable=True)
    billing_period_end = Column(Date, nullable=True)

    # Amounts
    gross_amount = Column(Numeric(14, 2), nullable=False)  # Total payments
    pg_fee = Column(Numeric(14, 2), nullable=True)
    commission_amount = Column(Numeric(14, 2), nullable=True)  # Fittbot commission
    gst_amount = Column(Numeric(14, 2), nullable=True)  # GST on commission
    tds_amount = Column(Numeric(14, 2), nullable=True)  # TDS deducted
    net_amount = Column(Numeric(14, 2), nullable=False)  # Amount transferred

    # Tax details
    hsn_code = Column(String(20), default="998599")  # IT services HSN
    cgst_rate = Column(Numeric(5, 2), default=9.00)
    sgst_rate = Column(Numeric(5, 2), default=9.00)
    igst_rate = Column(Numeric(5, 2), default=0.00)

    # Status
    status = Column(
        Enum("draft", "generated", "sent", "paid", name="fittbot_invoice_status"),
        default="generated",
        nullable=False,
    )
    pdf_url = Column(String(255), nullable=True)
    sent_at = Column(DateTime, nullable=True)

    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)


# ═══════════════════════════════════════════════════════════════════════════════
# COMMISSION CONFIG - Configurable commission rates per gym/source_type
# ═══════════════════════════════════════════════════════════════════════════════
class CommissionConfig(Base):
    """
    Configurable commission rates.
    Can be set globally, per gym, or per source_type.
    """
    __tablename__ = "commission_configs"
    __table_args__ = {"schema": PAYMENTS_SCHEMA}

    id = Column(Integer, primary_key=True, autoincrement=True)

    # Scope (NULL means global/default)
    gym_id = Column(Integer, nullable=True, index=True)
    source_type = Column(String(50), nullable=True)  # yoga, daily_pass, gym_membership, etc.

    # Rates (in percentage)
    commission_rate = Column(Numeric(5, 2), nullable=False, default=10.00)  # Fittbot commission %
    pg_fee_rate = Column(Numeric(5, 2), nullable=False, default=2.00)  # Payment gateway fee %
    gst_rate = Column(Numeric(5, 2), nullable=False, default=18.00)  # GST on commission %
    tds_rate = Column(Numeric(5, 2), nullable=False, default=2.00)  # TDS on commission %

    # Effective dates
    effective_from = Column(Date, nullable=False)
    effective_to = Column(Date, nullable=True)  # NULL means currently active

    is_active = Column(Boolean, default=True)
    created_by = Column(String(50), nullable=True)

    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)
