"""
New models for auto-settlement & payout system.
Extends the fittbot_payments schema with gym bank account management.
"""

from datetime import datetime

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Integer,
    String,
    Text,
)
from sqlalchemy.dialects.postgresql import JSON

from app.models.database import Base

PAYMENTS_SCHEMA = "fittbot_payments"


class GymBankAccount(Base):
    """
    Stores gym owner's bank account / UPI details for RazorpayX payouts.
    Each gym can have one active bank account at a time.
    Stores RazorpayX contact_id and fund_account_id for payout execution.
    """
    __tablename__ = "gym_bank_accounts"
    __table_args__ = {"schema": PAYMENTS_SCHEMA}

    id = Column(Integer, primary_key=True, autoincrement=True)
    gym_id = Column(Integer, nullable=False, index=True)
    owner_id = Column(Integer, nullable=True, index=True)

    # Bank account details
    account_type = Column(String(20), nullable=False, default="bank")  # bank | upi
    account_holder_name = Column(String(200), nullable=False)
    account_number = Column(String(50), nullable=True)  # encrypted/masked in responses
    ifsc_code = Column(String(20), nullable=True)
    bank_name = Column(String(100), nullable=True)
    upi_id = Column(String(100), nullable=True)

    # RazorpayX references (created during onboarding)
    razorpayx_contact_id = Column(String(100), nullable=True, index=True)
    razorpayx_fund_account_id = Column(String(100), nullable=True, index=True)

    # Verification
    is_verified = Column(Boolean, default=False)
    is_active = Column(Boolean, default=True)
    verification_status = Column(String(30), nullable=True)  # pending | verified | failed

    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)


class SettlementSyncLog(Base):
    """
    Tracks each settlement sync run from Razorpay.
    Prevents re-processing the same settlements.
    """
    __tablename__ = "settlement_sync_logs"
    __table_args__ = {"schema": PAYMENTS_SCHEMA}

    id = Column(Integer, primary_key=True, autoincrement=True)

    razorpay_settlement_id = Column(String(100), unique=True, nullable=False, index=True)
    settlement_amount_paise = Column(Integer, nullable=False)
    settlement_date = Column(DateTime, nullable=True)
    utr = Column(String(100), nullable=True)

    payments_found = Column(Integer, default=0)
    payments_matched = Column(Integer, default=0)
    payouts_created = Column(Integer, default=0)

    status = Column(String(30), default="pending")  # pending | processing | completed | failed
    error_message = Column(Text, nullable=True)
    raw_data = Column(JSON, nullable=True)

    created_at = Column(DateTime, default=datetime.now)
    completed_at = Column(DateTime, nullable=True)
