"""Fee and commission models"""

from typing import Optional
from datetime import date, datetime
from sqlalchemy import String, ForeignKey, BigInteger, Numeric, Text, Date, Index
from sqlalchemy.orm import Mapped, mapped_column

from .base import TimestampMixin
from app.models.database import Base


class FeesActuals(Base):
    """Actual fees charged by payment providers"""
    
    __tablename__ = "fees_actuals"
    __table_args__ = (
        Index('idx_fees_actuals_order_id', 'order_id'),
        Index('idx_fees_actuals_payment_id', 'payment_id'),
        Index('idx_fees_actuals_recorded_at', 'recorded_at'),
        {"schema": "payments"}
    )
    
    id: Mapped[str] = mapped_column(String(100), primary_key=True)
    order_id: Mapped[Optional[str]] = mapped_column(
        ForeignKey("payments.orders.id", ondelete="SET NULL")
    )
    payment_id: Mapped[Optional[str]] = mapped_column(
        ForeignKey("payments.payments.id", ondelete="SET NULL")
    )
    gateway_fee_minor: Mapped[int] = mapped_column(BigInteger, default=0, nullable=False)
    payout_fee_minor: Mapped[int] = mapped_column(BigInteger, default=0, nullable=False)
    tax_on_fees_minor: Mapped[int] = mapped_column(BigInteger, default=0, nullable=False)
    notes: Mapped[Optional[str]] = mapped_column(Text)
    recorded_at: Mapped[datetime] = mapped_column(
        "recorded_at",
        nullable=False,
        default=lambda: datetime.now()
    )
    
    def __repr__(self) -> str:
        return f"<FeesActuals(id={self.id}, gateway_fee={self.gateway_fee_minor})>"
    
    @property
    def total_fees_minor(self) -> int:
        """Total fees including tax"""
        return self.gateway_fee_minor + self.payout_fee_minor + self.tax_on_fees_minor
    
    @property
    def gateway_fee_rupees(self) -> float:
        """Gateway fee in rupees"""
        return self.gateway_fee_minor / 100.0
    
    @property
    def payout_fee_rupees(self) -> float:
        """Payout fee in rupees"""
        return self.payout_fee_minor / 100.0


class CommissionSchedule(Base, TimestampMixin):
    """Commission rate schedules for different scopes"""
    
    __tablename__ = "commission_schedules"
    __table_args__ = (
        Index('idx_commission_schedules_scope', 'scope', 'scope_id'),
        Index('idx_commission_schedules_effective', 'effective_from', 'effective_to'),
        Index('idx_commission_schedules_active', 'effective_from', 'effective_to', 'scope'),
        {"schema": "payments"}
    )
    
    id: Mapped[str] = mapped_column(String(100), primary_key=True)
    scope: Mapped[str] = mapped_column(String(20), nullable=False)  # global|gym|product
    scope_id: Mapped[Optional[str]] = mapped_column(String(100))      # gym_id or sku
    commission_pct: Mapped[float] = mapped_column(Numeric(5, 2), default=0)
    commission_fixed_minor: Mapped[int] = mapped_column(BigInteger, default=0)
    effective_from: Mapped[date] = mapped_column(Date, nullable=False)
    effective_to: Mapped[Optional[date]] = mapped_column(Date)
    
    def __repr__(self) -> str:
        return f"<CommissionSchedule(scope={self.scope}, pct={self.commission_pct})>"
    
    @property
    def commission_fixed_rupees(self) -> float:
        """Fixed commission in rupees"""
        return self.commission_fixed_minor / 100.0
    
    @property
    def is_active(self, as_of: Optional[date] = None) -> bool:
        """Check if commission schedule is active on given date"""
        check_date = as_of or date.today()
        
        if check_date < self.effective_from:
            return False
        
        if self.effective_to and check_date > self.effective_to:
            return False
        
        return True
    
    def calculate_commission(self, base_amount_minor: int) -> tuple[int, float, int]:
        """Calculate commission for given base amount
        
        Returns:
            tuple: (commission_amount_minor, applied_pct, applied_fixed_minor)
        """
        percentage_commission = round(base_amount_minor * float(self.commission_pct) / 100.0)
        fixed_commission = int(self.commission_fixed_minor)
        total_commission = percentage_commission + fixed_commission
        
        return total_commission, float(self.commission_pct), fixed_commission