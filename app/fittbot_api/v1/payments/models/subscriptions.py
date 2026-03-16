"""Subscription models for app subscriptions"""

from typing import Optional
from datetime import datetime, timezone, timedelta
from sqlalchemy import String, Boolean, Text, DateTime, Index, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from .base import TimestampMixin
from app.models.database import Base
from .enums import SubscriptionStatus

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


class Subscription(Base, TimestampMixin):
    """App subscription management"""
    
    __tablename__ = "subscriptions"
    __table_args__ = (
        Index('idx_subscriptions_customer_status', 'customer_id', 'status'),
        Index('idx_subscriptions_provider_product', 'provider', 'product_id'),
        Index('idx_subscriptions_active_period', 'active_from', 'active_until'),
        Index('idx_subscriptions_rc_txn', 'rc_original_txn_id'),
        {"schema": "payments"}
    )
    
    id: Mapped[str] = mapped_column(String(100), primary_key=True)
    customer_id: Mapped[str] = mapped_column(String(100), nullable=False)
    provider: Mapped[str] = mapped_column(String(50), nullable=False)
    product_id: Mapped[str] = mapped_column(String(100), nullable=False)
    status: Mapped[str] = mapped_column(String(20), nullable=False)
    rc_original_txn_id: Mapped[Optional[str]] = mapped_column(String(100))
    latest_txn_id: Mapped[Optional[str]] = mapped_column(String(100))
    active_from: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    active_until: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    trial_start: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    trial_end: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    auto_renew: Mapped[Optional[bool]] = mapped_column(Boolean)
    cancel_reason: Mapped[Optional[str]] = mapped_column(Text)
    
    def __repr__(self) -> str:
        return f"<Subscription(id={self.id}, customer_id={self.customer_id}, status={self.status})>"
    
    @property
    def is_active(self) -> bool:
        """Check if subscription is currently active"""
        now = now_ist()
        active_from = ensure_timezone_aware(self.active_from)
        active_until = ensure_timezone_aware(self.active_until)
        
        if self.status not in [SubscriptionStatus.active, SubscriptionStatus.renewed]:
            return False
        
        if active_from and now < active_from:
            return False
        
        if active_until and now > active_until:
            return False
        
        return True
    
    @property
    def is_in_trial(self) -> bool:
        """Check if subscription is in trial period"""
        if not self.trial_start or not self.trial_end:
            return False
        
        now = now_ist()
        trial_start = ensure_timezone_aware(self.trial_start)
        trial_end = ensure_timezone_aware(self.trial_end)
        return trial_start <= now <= trial_end
    
    @property
    def days_until_expiry(self) -> Optional[int]:
        """Get days until subscription expires"""
        if not self.active_until:
            return None
        
        now = now_ist()
        active_until = ensure_timezone_aware(self.active_until)
        if now >= active_until:
            return 0
        
        delta = active_until - now
        return delta.days
    
    @property
    def is_expired(self) -> bool:
        """Check if subscription has expired"""
        if not self.active_until:
            return False
        
        active_until = ensure_timezone_aware(self.active_until)
        return now_ist() > active_until
    
    @property
    def subscription_duration_days(self) -> Optional[int]:
        """Get total subscription duration in days"""
        if not self.active_from or not self.active_until:
            return None
        
        active_from = ensure_timezone_aware(self.active_from)
        active_until = ensure_timezone_aware(self.active_until)
        delta = active_until - active_from
        return delta.days
