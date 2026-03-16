"""Payment models"""

from typing import Optional, Dict, Any
from datetime import datetime
from sqlalchemy import String, ForeignKey, BigInteger, DateTime, JSON, Index
from sqlalchemy.orm import Mapped, mapped_column

from .base import TimestampMixin
from app.models.database import Base
from .enums import StatusPayment


class Payment(Base, TimestampMixin):
    """Payment records from various providers"""
    
    __tablename__ = "payments"
    __table_args__ = (
        Index('idx_payments_order_id', 'order_id'),
        Index('idx_payments_customer_status', 'customer_id', 'status'),
        Index('idx_payments_provider_payment_id', 'provider_payment_id'),
        Index('idx_payments_provider_status', 'provider', 'status'),
        Index('idx_payments_captured_at', 'captured_at'),
        {"schema": "payments"}
    )
    
    id: Mapped[str] = mapped_column(String(100), primary_key=True)
    order_id: Mapped[str] = mapped_column(
        ForeignKey("payments.orders.id", ondelete="CASCADE"), 
        nullable=False
    )
    customer_id: Mapped[str] = mapped_column(String(100), nullable=False)
    amount_minor: Mapped[int] = mapped_column(BigInteger, nullable=False)
    currency: Mapped[str] = mapped_column(String(3), default="INR", nullable=False)
    provider: Mapped[str] = mapped_column(String(50), nullable=False)
    provider_payment_id: Mapped[Optional[str]] = mapped_column(String(100))
    status: Mapped[str] = mapped_column(String(20), nullable=False)
    authorized_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    captured_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    failed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    payment_metadata: Mapped[Optional[Dict[str, Any]]] = mapped_column(JSON)
    
    def __repr__(self) -> str:
        return f"<Payment(id={self.id}, amount_minor={self.amount_minor}, status={self.status})>"
    
    @property
    def amount_rupees(self) -> float:
        """Convert minor units (paise) to rupees"""
        return self.amount_minor / 100.0
    
    @property
    def is_successful(self) -> bool:
        """Check if payment was successful"""
        return self.status == StatusPayment.captured
    
    @property
    def processing_time_seconds(self) -> Optional[int]:
        """Calculate time between authorization and capture"""
        if self.authorized_at and self.captured_at:
            delta = self.captured_at - self.authorized_at
            return int(delta.total_seconds())
        return None