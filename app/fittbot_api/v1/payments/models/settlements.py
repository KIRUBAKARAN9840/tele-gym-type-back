"""Settlement and reconciliation models"""

from typing import Optional, Dict, Any
from datetime import date
from sqlalchemy import String, ForeignKey, BigInteger, Date, JSON, Index
from sqlalchemy.orm import Mapped, mapped_column

from .base import TimestampMixin
from app.models.database import Base


class Settlement(Base, TimestampMixin):
    """Settlement batches from payment providers"""
    
    __tablename__ = "settlements"
    __table_args__ = (
        Index('idx_settlements_provider_date', 'provider', 'settlement_date'),
        Index('idx_settlements_provider_ref', 'provider_ref'),
        {"schema": "payments"}
    )
    
    id: Mapped[str] = mapped_column(String(100), primary_key=True)
    provider: Mapped[str] = mapped_column(String(50), nullable=False)
    settlement_date: Mapped[date] = mapped_column(Date, nullable=False)
    provider_ref: Mapped[Optional[str]] = mapped_column(String(100))
    gross_captured_minor: Mapped[int] = mapped_column(BigInteger, nullable=False)
    mdr_amount_minor: Mapped[int] = mapped_column(BigInteger, nullable=False)
    tax_on_mdr_minor: Mapped[int] = mapped_column(BigInteger, nullable=False)
    net_settled_minor: Mapped[int] = mapped_column(BigInteger, nullable=False)
    payload_json: Mapped[Optional[Dict[str, Any]]] = mapped_column(JSON)
    
    def __repr__(self) -> str:
        return f"<Settlement(id={self.id}, provider={self.provider}, date={self.settlement_date})>"
    
    @property
    def gross_captured_rupees(self) -> float:
        """Convert minor units to rupees"""
        return self.gross_captured_minor / 100.0
    
    @property
    def mdr_amount_rupees(self) -> float:
        """MDR amount in rupees"""
        return self.mdr_amount_minor / 100.0
    
    @property
    def net_settled_rupees(self) -> float:
        """Net settled amount in rupees"""
        return self.net_settled_minor / 100.0
    
    @property
    def total_fees_minor(self) -> int:
        """Total fees including tax"""
        return self.mdr_amount_minor + self.tax_on_mdr_minor
    
    @property
    def effective_mdr_rate(self) -> float:
        """Calculate effective MDR rate as percentage"""
        if self.gross_captured_minor > 0:
            return (self.total_fees_minor / self.gross_captured_minor) * 100.0
        return 0.0


class SettlementItem(Base):
    """Individual payment items within a settlement"""
    
    __tablename__ = "settlement_items"
    __table_args__ = (
        Index('idx_settlement_items_settlement_id', 'settlement_id'),
        Index('idx_settlement_items_provider_payment_id', 'provider_payment_id'),
        Index('idx_settlement_items_payment_id', 'payment_id'),
        Index('idx_settlement_items_settled_on', 'settled_on'),
        {"schema": "payments"}
    )
    
    id: Mapped[str] = mapped_column(String(100), primary_key=True)
    settlement_id: Mapped[str] = mapped_column(
        ForeignKey("payments.settlements.id", ondelete="CASCADE"), 
        nullable=False
    )
    provider_payment_id: Mapped[str] = mapped_column(
        String(100), 
        nullable=False, 
        unique=True
    )
    payment_id: Mapped[Optional[str]] = mapped_column(
        ForeignKey("payments.payments.id", ondelete="SET NULL")
    )
    gross_captured_minor: Mapped[int] = mapped_column(BigInteger, nullable=False)
    mdr_amount_minor: Mapped[int] = mapped_column(BigInteger, nullable=False)
    tax_on_mdr_minor: Mapped[int] = mapped_column(BigInteger, nullable=False)
    net_settled_minor: Mapped[int] = mapped_column(BigInteger, nullable=False)
    settled_on: Mapped[date] = mapped_column(Date, nullable=False)
    
    def __repr__(self) -> str:
        return f"<SettlementItem(id={self.id}, provider_payment_id={self.provider_payment_id})>"
    
    @property
    def gross_captured_rupees(self) -> float:
        """Convert minor units to rupees"""
        return self.gross_captured_minor / 100.0
    
    @property
    def net_settled_rupees(self) -> float:
        """Net amount in rupees"""
        return self.net_settled_minor / 100.0
    
    @property
    def total_fees_minor(self) -> int:
        """Total fees for this item"""
        return self.mdr_amount_minor + self.tax_on_mdr_minor
    
    @property
    def mdr_rate(self) -> float:
        """MDR rate for this specific payment"""
        if self.gross_captured_minor > 0:
            return (self.total_fees_minor / self.gross_captured_minor) * 100.0
        return 0.0