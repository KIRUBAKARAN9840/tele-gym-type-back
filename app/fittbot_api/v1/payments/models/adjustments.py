"""Adjustment models for manual corrections"""

from typing import Optional
from sqlalchemy import String, BigInteger, Text, Index
from sqlalchemy.orm import Mapped, mapped_column

from .base import TimestampMixin
from app.models.database import Base


class Adjustment(Base, TimestampMixin):
    """Manual adjustments for accounting corrections"""
    
    __tablename__ = "adjustments"
    __table_args__ = (
        Index('idx_adjustments_scope', 'scope', 'scope_id'),
        Index('idx_adjustments_created_by', 'created_by'),
        Index('idx_adjustments_created_at', 'created_at'),
        {"schema": "payments"}
    )
    
    id: Mapped[str] = mapped_column(String(100), primary_key=True)
    scope: Mapped[str] = mapped_column(
        String(20), 
        nullable=False
    )  # order|payment|payout_batch|general
    scope_id: Mapped[Optional[str]] = mapped_column(String(100))
    amount_minor: Mapped[int] = mapped_column(
        BigInteger, 
        nullable=False
    )  # +/- values
    reason: Mapped[str] = mapped_column(Text, nullable=False)
    created_by: Mapped[str] = mapped_column(String(100), nullable=False)
    
    def __repr__(self) -> str:
        return f"<Adjustment(id={self.id}, scope={self.scope}, amount_minor={self.amount_minor})>"
    
    @property
    def amount_rupees(self) -> float:
        """Convert minor units to rupees"""
        return self.amount_minor / 100.0
    
    @property
    def is_credit(self) -> bool:
        """Check if adjustment is a credit (positive)"""
        return self.amount_minor > 0
    
    @property
    def is_debit(self) -> bool:
        """Check if adjustment is a debit (negative)"""
        return self.amount_minor < 0
    
    @property
    def adjustment_type(self) -> str:
        """Get human-readable adjustment type"""
        return "Credit" if self.is_credit else "Debit"
    
    @property
    def absolute_amount_minor(self) -> int:
        """Get absolute amount in minor units"""
        return abs(self.amount_minor)
    
    @property
    def absolute_amount_rupees(self) -> float:
        """Get absolute amount in rupees"""
        return self.absolute_amount_minor / 100.0