"""Refund models"""

from typing import Optional, Dict, Any
from datetime import datetime
from sqlalchemy import String, ForeignKey, BigInteger, Text, DateTime, JSON, Index
from sqlalchemy.orm import Mapped, mapped_column

from .base import TimestampMixin
from app.models.database import Base
from .enums import RefundStatus


class Refund(Base, TimestampMixin):
    """Refund records for payments"""
    
    __tablename__ = "refunds"
    __table_args__ = (
        Index('idx_refunds_payment_id', 'payment_id'),
        Index('idx_refunds_entitlement_id', 'entitlement_id'),
        Index('idx_refunds_status', 'status'),
        Index('idx_refunds_provider', 'provider'),
        Index('idx_refunds_processed_at', 'processed_at'),
        {"schema": "payments"}
    )
    
    id: Mapped[str] = mapped_column(String(100), primary_key=True)
    payment_id: Mapped[str] = mapped_column(
        ForeignKey("payments.payments.id", ondelete="CASCADE"), 
        nullable=False
    )
    entitlement_id: Mapped[Optional[str]] = mapped_column(
        ForeignKey("payments.entitlements.id", ondelete="SET NULL")
    )
    amount_minor: Mapped[int] = mapped_column(BigInteger, nullable=False)
    currency: Mapped[str] = mapped_column(String(3), default="INR", nullable=False)
    provider: Mapped[str] = mapped_column(String(50), nullable=False)
    status: Mapped[str] = mapped_column(String(20), nullable=False)  # initiated|processed|failed
    provider_ref: Mapped[Optional[str]] = mapped_column(String(100))
    reason: Mapped[Optional[str]] = mapped_column(Text)
    processed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    refund_metadata: Mapped[Optional[Dict[str, Any]]] = mapped_column(JSON)
    
    def __repr__(self) -> str:
        return f"<Refund(id={self.id}, amount_minor={self.amount_minor}, status={self.status})>"
    
    @property
    def amount_rupees(self) -> float:
        """Convert minor units to rupees"""
        return self.amount_minor / 100.0
    
    @property
    def is_successful(self) -> bool:
        """Check if refund was successful"""
        return self.status == RefundStatus.processed
    
    @property
    def processing_time_hours(self) -> Optional[float]:
        """Calculate processing time in hours"""
        if not self.processed_at:
            return None
        
        delta = self.processed_at - self.created_at
        return delta.total_seconds() / 3600.0
    
    @property
    def is_partial_refund(self) -> bool:
        """Check if this is a partial refund (requires payment comparison)"""
        # This would need to be determined by comparing with the original payment amount
        # For now, we return False as we'd need to join with payment table
        return False