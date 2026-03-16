"""Dispute models"""

from typing import Optional, Dict, Any
from datetime import datetime
from sqlalchemy import String, ForeignKey, BigInteger, Text, DateTime, JSON, Index
from sqlalchemy.orm import Mapped, mapped_column

from .base import TimestampMixin
from app.models.database import Base
from .enums import DisputeStatus


class Dispute(Base, TimestampMixin):
    """Payment dispute records"""
    
    __tablename__ = "disputes"
    __table_args__ = (
        Index('idx_disputes_payment_id', 'payment_id'),
        Index('idx_disputes_status', 'status'),
        Index('idx_disputes_provider', 'provider'),
        Index('idx_disputes_opened_at', 'opened_at'),
        Index('idx_disputes_closed_at', 'closed_at'),
        {"schema": "payments"}
    )
    
    id: Mapped[str] = mapped_column(String(100), primary_key=True)
    payment_id: Mapped[str] = mapped_column(
        ForeignKey("payments.payments.id", ondelete="CASCADE"), 
        nullable=False
    )
    provider: Mapped[str] = mapped_column(String(50), nullable=False)
    amount_minor: Mapped[int] = mapped_column(BigInteger, nullable=False)
    reason: Mapped[Optional[str]] = mapped_column(Text)
    status: Mapped[str] = mapped_column(String(20), nullable=False)  # open|won|lost|canceled
    opened_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    closed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    payload_json: Mapped[Optional[Dict[str, Any]]] = mapped_column(JSON)
    
    def __repr__(self) -> str:
        return f"<Dispute(id={self.id}, amount_minor={self.amount_minor}, status={self.status})>"
    
    @property
    def amount_rupees(self) -> float:
        """Convert minor units to rupees"""
        return self.amount_minor / 100.0
    
    @property
    def is_open(self) -> bool:
        """Check if dispute is still open"""
        return self.status == DisputeStatus.open
    
    @property
    def is_resolved(self) -> bool:
        """Check if dispute has been resolved"""
        return self.status in [DisputeStatus.won, DisputeStatus.lost, DisputeStatus.canceled]
    
    @property
    def resolution_time_hours(self) -> Optional[float]:
        """Calculate time to resolution in hours"""
        if not self.closed_at:
            return None
        
        delta = self.closed_at - self.opened_at
        return delta.total_seconds() / 3600.0
    
    @property
    def is_won(self) -> bool:
        """Check if dispute was won (merchant favor)"""
        return self.status == DisputeStatus.won
    
    @property
    def is_lost(self) -> bool:
        """Check if dispute was lost (customer favor)"""
        return self.status == DisputeStatus.lost