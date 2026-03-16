"""Product catalog models"""

from typing import Optional
from sqlalchemy import String, BigInteger, Text, Boolean
from sqlalchemy.orm import Mapped, mapped_column

from .base import TimestampMixin
from app.models.database import Base


class CatalogProduct(Base, TimestampMixin):
    """Product catalog for pricing and metadata"""

    __tablename__ = "catalog_products"
    __table_args__ = {"schema": "payments"}

    sku: Mapped[str] = mapped_column(String(100), primary_key=True)
    item_type: Mapped[str] = mapped_column(String(50), nullable=False, index=True)
    title: Mapped[str] = mapped_column(String(200), nullable=False)
    base_amount_minor: Mapped[int] = mapped_column(BigInteger, default=0, nullable=False)
    description: Mapped[Optional[str]] = mapped_column(Text)
    active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    razorpay_plan_id: Mapped[Optional[str]] = mapped_column(String(100), nullable=True, index=True)
    
    def __repr__(self) -> str:
        return f"<CatalogProduct(sku={self.sku}, title={self.title})>"
    
    @property
    def base_amount_rupees(self) -> float:
        """Convert minor units (paise) to rupees"""
        return self.base_amount_minor / 100.0