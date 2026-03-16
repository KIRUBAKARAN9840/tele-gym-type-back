"""Entitlement models"""

from typing import Optional
from datetime import datetime, date, timezone, timedelta
from sqlalchemy import String, ForeignKey, Date, DateTime, Index
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .base import TimestampMixin
from app.models.database import Base
from .enums import StatusEnt

# Indian Standard Time (IST) timezone
IST = timezone(timedelta(hours=5, minutes=30))

def now_ist():
    """Get current time in IST"""
    return datetime.now(IST)

def ensure_timezone_aware(dt: Optional[datetime]) -> Optional[datetime]:
    """Ensure datetime is timezone-aware (IST)"""
    if dt is None:
        return None
    if dt.tzinfo is None:
        # Assume naive datetimes are in IST
        return dt.replace(tzinfo=IST)
    return dt


class Entitlement(Base, TimestampMixin):
    """Customer entitlements from purchases"""
    
    __tablename__ = "entitlements"
    __table_args__ = (
        Index('idx_entitlements_customer_status', 'customer_id', 'status'),
        Index('idx_entitlements_gym_scheduled', 'gym_id', 'scheduled_for'),
        Index('idx_entitlements_trainer_scheduled', 'trainer_id', 'scheduled_for'),
        Index('idx_entitlements_type_status', 'entitlement_type', 'status'),
        {"schema": "payments"}
    )
    
    id: Mapped[str] = mapped_column(String(100), primary_key=True)
    order_item_id: Mapped[str] = mapped_column(
        ForeignKey("payments.order_items.id", ondelete="CASCADE"), 
        nullable=False
    )
    customer_id: Mapped[str] = mapped_column(String(100), nullable=False, index=True)
    gym_id: Mapped[Optional[str]] = mapped_column(String(100))
    trainer_id: Mapped[Optional[str]] = mapped_column(String(100))
    entitlement_type: Mapped[str] = mapped_column(String(50), nullable=False)
    scheduled_for: Mapped[Optional[date]] = mapped_column(Date)
    status: Mapped[str] = mapped_column(
        String(20), 
        nullable=False, 
        default=StatusEnt.pending
    )
    active_from: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    active_until: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    
    # Relationships
    order_item: Mapped["OrderItem"] = relationship(back_populates="entitlements")
    checkin: Mapped[Optional["Checkin"]] = relationship(
        back_populates="entitlement", 
        uselist=False,
        cascade="all, delete-orphan"
    )
    payout_line: Mapped[Optional["PayoutLine"]] = relationship(
        back_populates="entitlement", 
        uselist=False
    )
    
    def __repr__(self) -> str:
        return f"<Entitlement(id={self.id}, type={self.entitlement_type}, status={self.status})>"
    
    @property
    def is_active(self) -> bool:
        """Check if entitlement is currently active"""
        now = now_ist()
        active_from = ensure_timezone_aware(self.active_from)
        active_until = ensure_timezone_aware(self.active_until)
        
        if active_from and now < active_from:
            return False
        if active_until and now > active_until:
            return False
        return self.status in [StatusEnt.pending, StatusEnt.used]
    
    @property
    def is_expired(self) -> bool:
        """Check if entitlement has expired"""
        if self.active_until:
            active_until = ensure_timezone_aware(self.active_until)
            return now_ist() > active_until
        return False