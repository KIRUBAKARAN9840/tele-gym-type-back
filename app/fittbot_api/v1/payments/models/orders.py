"""Order and order item models"""

from typing import Optional, List, Dict, Any
from sqlalchemy import String, BigInteger, Integer, ForeignKey, JSON, Index
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .base import TimestampMixin
from app.models.database import Base
from .enums import StatusOrder


class Order(Base, TimestampMixin):
    """Main order entity"""
    
    __tablename__ = "orders"
    __table_args__ = (
        Index('idx_orders_customer_created', 'customer_id', 'created_at'),
        Index('idx_orders_provider_status', 'provider', 'status'),
        {"schema": "payments"}
    )
    
    id: Mapped[str] = mapped_column(String(100), primary_key=True)
    customer_id: Mapped[str] = mapped_column(String(100), nullable=False, index=True)
    currency: Mapped[str] = mapped_column(String(3), default="INR", nullable=False)
    provider: Mapped[str] = mapped_column(String(50), nullable=False)
    provider_order_id: Mapped[Optional[str]] = mapped_column(String(100), index=True)
    gross_amount_minor: Mapped[int] = mapped_column(BigInteger, nullable=False)
    status: Mapped[str] = mapped_column(String(20), default=StatusOrder.pending, nullable=False)
    order_metadata: Mapped[Optional[Dict[str, Any]]] = mapped_column(JSON)
    
    # Relationships
    items: Mapped[List["OrderItem"]] = relationship(
        back_populates="order", 
        cascade="all, delete-orphan",
        lazy="select"
    )
    
    def __repr__(self) -> str:
        return f"<Order(id={self.id}, customer_id={self.customer_id}, status={self.status})>"
    
    @property
    def gross_amount_rupees(self) -> float:
        """Convert minor units (paise) to rupees"""
        return self.gross_amount_minor / 100.0


class OrderItem(Base, TimestampMixin):
    """Individual items within an order"""
    
    __tablename__ = "order_items"
    __table_args__ = (
        Index('idx_order_items_order_id', 'order_id'),
        Index('idx_order_items_sku', 'sku'),
        Index('idx_order_items_gym_trainer', 'gym_id', 'trainer_id'),
        {"schema": "payments"}
    )
    
    id: Mapped[str] = mapped_column(String(100), primary_key=True)
    order_id: Mapped[str] = mapped_column(
        ForeignKey("payments.orders.id", ondelete="CASCADE"), 
        nullable=False
    )
    item_type: Mapped[str] = mapped_column(String(50), nullable=False)
    sku: Mapped[Optional[str]] = mapped_column(
        ForeignKey("payments.catalog_products.sku", ondelete="SET NULL")
    )
    gym_id: Mapped[Optional[str]] = mapped_column(String(100))
    trainer_id: Mapped[Optional[str]] = mapped_column(String(100))
    title: Mapped[Optional[str]] = mapped_column(String(200))
    unit_price_minor: Mapped[int] = mapped_column(BigInteger, nullable=False)
    qty: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    item_metadata: Mapped[Optional[Dict[str, Any]]] = mapped_column("metadata", JSON)
    
    # Relationships
    order: Mapped[Order] = relationship(back_populates="items")
    entitlements: Mapped[List["Entitlement"]] = relationship(
        back_populates="order_item", 
        cascade="all, delete-orphan"
    )
    
    def __repr__(self) -> str:
        return f"<OrderItem(id={self.id}, item_type={self.item_type}, qty={self.qty})>"
    
    @property
    def unit_price_rupees(self) -> float:
        """Convert minor units (paise) to rupees"""
        return self.unit_price_minor / 100.0
    
    @property
    def total_amount_minor(self) -> int:
        """Total amount for this item"""
        return self.unit_price_minor * self.qty
    
    @property
    def total_amount_rupees(self) -> float:
        """Total amount in rupees"""
        return self.total_amount_minor / 100.0