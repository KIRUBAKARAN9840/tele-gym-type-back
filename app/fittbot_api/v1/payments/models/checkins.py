"""Check-in models"""

from typing import Optional
from datetime import datetime
from sqlalchemy import String, ForeignKey, DateTime, Index
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .base import TimestampMixin
from app.models.database import Base
from .enums import StatusCheckin


class Checkin(Base, TimestampMixin):
    """Check-in records for gym visits"""
    
    __tablename__ = "checkins"
    __table_args__ = (
        Index('idx_checkins_gym_scanned', 'gym_id', 'scanned_at'),
        Index('idx_checkins_customer_scanned', 'customer_id', 'scanned_at'),
        Index('idx_checkins_entitlement', 'entitlement_id'),
        {"schema": "payments"}
    )
    
    id: Mapped[str] = mapped_column(String(100), primary_key=True)
    entitlement_id: Mapped[str] = mapped_column(
        ForeignKey("payments.entitlements.id", ondelete="RESTRICT"), 
        nullable=False, 
        unique=True
    )
    gym_id: Mapped[str] = mapped_column(String(100), nullable=False)
    customer_id: Mapped[str] = mapped_column(String(100), nullable=False)
    scanned_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    in_time: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    out_time: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    status: Mapped[str] = mapped_column(
        String(20), 
        default=StatusCheckin.ok, 
        nullable=False
    )
    
    # Relationships
    entitlement: Mapped["Entitlement"] = relationship(back_populates="checkin")
    
    def __repr__(self) -> str:
        return f"<Checkin(id={self.id}, gym_id={self.gym_id}, status={self.status})>"
    
    @property
    def duration_minutes(self) -> Optional[int]:
        """Calculate workout duration in minutes"""
        if self.in_time and self.out_time:
            delta = self.out_time - self.in_time
            return int(delta.total_seconds() / 60)
        return None
    
    @property
    def is_active_session(self) -> bool:
        """Check if user is currently checked in"""
        return self.in_time is not None and self.out_time is None