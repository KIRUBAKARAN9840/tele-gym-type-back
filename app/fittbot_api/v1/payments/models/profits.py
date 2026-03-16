"""Platform earnings (profits) accounting for Daily Pass etc."""

from typing import Optional, Dict, Any
from datetime import datetime, timezone
from sqlalchemy import String, BigInteger, DateTime, JSON, UniqueConstraint, Index
from sqlalchemy.orm import Mapped, mapped_column

from .base import TimestampMixin
from app.models.database import Base


class PlatformEarning(Base, TimestampMixin):
    """Per-event platform earnings lines.
    Example earnings_type: commission, breakage
    """

    __tablename__ = "platform_earnings"
    __table_args__ = (
        UniqueConstraint("source", "earning_type", "pass_day_id", name="uq_pe_src_type_day"),
        Index("idx_pe_gym_time", "gym_id", "recognized_on"),
        {"schema": "payments"},
    )

    id: Mapped[str] = mapped_column(String(100), primary_key=True)
    source: Mapped[str] = mapped_column(String(50), default="daily_pass", nullable=False)
    earning_type: Mapped[str] = mapped_column(String(30), nullable=False)  # commission | breakage
    gym_id: Mapped[str] = mapped_column(String(100), nullable=False)
    order_id: Mapped[Optional[str]] = mapped_column(String(100))
    payment_id: Mapped[Optional[str]] = mapped_column(String(100))
    pass_day_id: Mapped[Optional[str]] = mapped_column(String(100))
    amount_minor: Mapped[int] = mapped_column(BigInteger, nullable=False)
    recognized_on: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    meta: Mapped[Optional[Dict[str, Any]]] = mapped_column(JSON)

    @property
    def amount_rupees(self) -> float:
        return self.amount_minor / 100.0

