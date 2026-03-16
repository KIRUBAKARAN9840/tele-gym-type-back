"""Payout models for gym and trainer payments"""

from typing import Optional
from datetime import date, datetime
from sqlalchemy import String, ForeignKey, BigInteger, Numeric, Date, Index
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .base import TimestampMixin
from app.models.database import Base
from .enums import StatusPayoutLine, PayoutBatchStatus


class PayoutBatch(Base, TimestampMixin):
    """Batch of payouts processed together"""
    
    __tablename__ = "payout_batches"
    __table_args__ = (
        Index('idx_payout_batches_gym_date', 'gym_id', 'batch_date'),
        Index('idx_payout_batches_status', 'status'),
        Index('idx_payout_batches_provider_ref', 'provider_ref'),
        {"schema": "payments"}
    )
    
    id: Mapped[str] = mapped_column(String(100), primary_key=True)
    batch_date: Mapped[date] = mapped_column(Date, nullable=False)
    gym_id: Mapped[str] = mapped_column(String(100), nullable=False)
    total_net_amount_minor: Mapped[int] = mapped_column(BigInteger, nullable=False)
    payout_mode: Mapped[str] = mapped_column(String(10), nullable=False)
    status: Mapped[str] = mapped_column(String(20), nullable=False)
    provider_ref: Mapped[Optional[str]] = mapped_column(String(100))
    fee_actual_minor: Mapped[int] = mapped_column(BigInteger, default=0, nullable=False)
    tax_on_fee_minor: Mapped[int] = mapped_column(BigInteger, default=0, nullable=False)
    
    def __repr__(self) -> str:
        return f"<PayoutBatch(id={self.id}, gym_id={self.gym_id}, status={self.status})>"
    
    @property
    def total_net_amount_rupees(self) -> float:
        """Convert minor units to rupees"""
        return self.total_net_amount_minor / 100.0
    
    @property
    def total_fees_minor(self) -> int:
        """Total fees including tax"""
        return self.fee_actual_minor + self.tax_on_fee_minor
    
    @property
    def net_amount_after_fees_minor(self) -> int:
        """Amount after deducting payout fees"""
        return self.total_net_amount_minor - self.total_fees_minor
    
    @property
    def is_successful(self) -> bool:
        """Check if payout was successful"""
        return self.status == PayoutBatchStatus.paid


class PayoutEvent(Base):
    """Events related to payout processing"""
    
    __tablename__ = "payout_events"
    __table_args__ = (
        Index('idx_payout_events_batch_id', 'payout_batch_id'),
        Index('idx_payout_events_event_time', 'event_time'),
        Index('idx_payout_events_provider_ref', 'provider_ref'),
        {"schema": "payments"}
    )
    
    id: Mapped[str] = mapped_column(String(100), primary_key=True)
    payout_batch_id: Mapped[str] = mapped_column(
        ForeignKey("payments.payout_batches.id", ondelete="CASCADE"), 
        nullable=False
    )
    provider: Mapped[str] = mapped_column(String(50), default="razorpayx", nullable=False)
    event_type: Mapped[str] = mapped_column(String(50), nullable=False)   # created|processed|failed|utr
    provider_ref: Mapped[Optional[str]] = mapped_column(String(100))
    event_time: Mapped[datetime] = mapped_column("event_time", nullable=False)
    
    def __repr__(self) -> str:
        return f"<PayoutEvent(batch_id={self.payout_batch_id}, type={self.event_type})>"


class PayoutLine(Base, TimestampMixin):
    """Individual payout lines for each entitlement"""
    
    __tablename__ = "payout_lines"
    __table_args__ = (
        Index('idx_payout_lines_entitlement_id', 'entitlement_id'),
        Index('idx_payout_lines_gym_scheduled', 'gym_id', 'scheduled_for'),
        Index('idx_payout_lines_status', 'status'),
        Index('idx_payout_lines_batch_id', 'batch_id'),
        {"schema": "payments"}
    )
    
    id: Mapped[str] = mapped_column(String(100), primary_key=True)
    entitlement_id: Mapped[str] = mapped_column(
        ForeignKey("payments.entitlements.id", ondelete="RESTRICT"), 
        nullable=False, 
        unique=True
    )
    gym_id: Mapped[str] = mapped_column(String(100), nullable=False)
    gross_amount_minor: Mapped[int] = mapped_column(BigInteger, nullable=False)
    commission_amount_minor: Mapped[int] = mapped_column(BigInteger, nullable=False)
    net_amount_minor: Mapped[int] = mapped_column(BigInteger, nullable=False)
    applied_commission_pct: Mapped[float] = mapped_column(Numeric(5,2), default=0)
    applied_commission_fixed_minor: Mapped[int] = mapped_column(BigInteger, default=0)
    payout_fee_allocated_minor: Mapped[int] = mapped_column(BigInteger, default=0)
    status: Mapped[str] = mapped_column(
        String(20), 
        default=StatusPayoutLine.pending, 
        nullable=False
    )
    scheduled_for: Mapped[date] = mapped_column(Date, nullable=False)
    batch_id: Mapped[Optional[str]] = mapped_column(
        ForeignKey("payments.payout_batches.id", ondelete="SET NULL")
    )
    provider_ref: Mapped[Optional[str]] = mapped_column(String(100))
    
    # Relationships
    entitlement: Mapped["Entitlement"] = relationship(back_populates="payout_line")
    
    def __repr__(self) -> str:
        return f"<PayoutLine(id={self.id}, gym_id={self.gym_id}, status={self.status})>"
    
    @property
    def gross_amount_rupees(self) -> float:
        """Convert minor units to rupees"""
        return self.gross_amount_minor / 100.0
    
    @property
    def commission_amount_rupees(self) -> float:
        """Commission amount in rupees"""
        return self.commission_amount_minor / 100.0
    
    @property
    def net_amount_rupees(self) -> float:
        """Net amount in rupees"""
        return self.net_amount_minor / 100.0
    
    @property
    def final_payout_minor(self) -> int:
        """Final payout amount after all deductions"""
        return self.net_amount_minor - self.payout_fee_allocated_minor
    
    @property
    def effective_commission_rate(self) -> float:
        """Effective commission rate including fixed component"""
        if self.gross_amount_minor > 0:
            return (self.commission_amount_minor / self.gross_amount_minor) * 100.0
        return 0.0


class Beneficiary(Base, TimestampMixin):
    """Beneficiary bank account details for payouts"""
    
    __tablename__ = "beneficiaries"
    __table_args__ = (
        Index('idx_beneficiaries_gym_id', 'gym_id'),
        Index('idx_beneficiaries_contact_id', 'contact_id'),
        Index('idx_beneficiaries_fund_account_id', 'fund_account_id'),
        {"schema": "payments"}
    )
    
    id: Mapped[str] = mapped_column(String(100), primary_key=True)
    gym_id: Mapped[str] = mapped_column(String(100), nullable=False)
    contact_id: Mapped[str] = mapped_column(String(100), nullable=False)
    fund_account_id: Mapped[str] = mapped_column(String(100), nullable=False)
    account_type: Mapped[str] = mapped_column(String(20), nullable=False)  # bank|upi
    masked_account: Mapped[Optional[str]] = mapped_column(String(50))
    ifsc: Mapped[Optional[str]] = mapped_column(String(20))
    upi: Mapped[Optional[str]] = mapped_column(String(100))
    kyc_status: Mapped[Optional[str]] = mapped_column(String(20))
    
    def __repr__(self) -> str:
        return f"<Beneficiary(id={self.id}, gym_id={self.gym_id}, type={self.account_type})>"
    
    @property
    def is_bank_account(self) -> bool:
        """Check if beneficiary uses bank account"""
        return self.account_type == "bank"
    
    @property
    def is_upi_account(self) -> bool:
        """Check if beneficiary uses UPI"""
        return self.account_type == "upi"
    
    @property
    def display_account(self) -> str:
        """Get displayable account information"""
        if self.is_upi_account and self.upi:
            return self.upi
        elif self.is_bank_account and self.masked_account:
            return f"{self.masked_account} ({self.ifsc})"
        else:
            return "Account details not available"