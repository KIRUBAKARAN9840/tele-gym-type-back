"""Base database models and configuration"""

from datetime import datetime, timezone, timedelta
from sqlalchemy import DateTime, func
from sqlalchemy.orm import Mapped, mapped_column
from app.models.database import Base  # Use main app's Base class

# Indian Standard Time (IST) timezone
IST = timezone(timedelta(hours=5, minutes=30))

def now_ist():
    """Get current time in IST"""
    return datetime.now(IST)


class TimestampMixin:
    """Mixin for automatic timestamp management with IST timezone"""
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=now_ist,  # Use IST instead of func.now()
        nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=now_ist,  # Use IST instead of func.now()
        onupdate=now_ist,  # Use IST for updates too
        nullable=False
    )