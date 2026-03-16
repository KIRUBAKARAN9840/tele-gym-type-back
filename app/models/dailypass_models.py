"""Daily Pass Models for Gym Daily Pass System"""

from __future__ import annotations

import os
import uuid
from datetime import date, datetime, timezone, timedelta
from typing import List, Optional, Dict, Any

from sqlalchemy import (
    Column, String, Integer, Date, DateTime, ForeignKey, Boolean, JSON, UniqueConstraint,
    Index, CheckConstraint, func, create_engine, Float, select
)
from sqlalchemy.dialects.mysql import JSON as MYSQL_JSON
from sqlalchemy.engine.url import make_url
from sqlalchemy.orm import relationship, Session, sessionmaker, synonym as _synonym
from sqlalchemy.exc import ProgrammingError, OperationalError
from sqlalchemy.ext.asyncio import AsyncSession
from app.models.async_database import get_async_sessionmaker
from app.config.pricing import get_markup_multiplier

# Use main app's Base instead of separate Base
from app.models.database import Base as DailyPassBase

UTC = timezone.utc

try:
    from app.config.settings import settings
    _HAS_SETTINGS = True
except Exception:
    _HAS_SETTINGS = False


def new_id(prefix: str = "dp") -> str:
    """Generate a new ID with prefix"""
    return f"{prefix}_{str(uuid.uuid4()).replace('-', '')[:8]}"


class DailyPass(DailyPassBase):
    __tablename__ = "daily_passes"
    __table_args__ = {"schema": "dailypass"}

    id = Column(String(40), primary_key=True, default=lambda: new_id("dps"))
    # Unified expects client_id; keep backward-compatible user_id
    user_id = Column(String(64), nullable=False, index=True)
    client_id = Column(String(64), nullable=True, index=True)
    gym_id = Column(String(64), nullable=False, index=True)  # Will reference gyms.id from main DB
    order_id = Column(String(40), nullable=True)  # Will reference orders.id from payments DB
    payment_id = Column(String(64), nullable=False, index=True)  # Razorpay payment_id
    days_total = Column(Integer, nullable=False)
    days_used = Column(Integer, nullable=False, default=0)
    valid_from = Column(Date, nullable=True)
    valid_until = Column(Date, nullable=True)
    # Unified additional fields
    amount_paid = Column(Integer, nullable=True)
    selected_time = Column(String(64), nullable=True)
    status = Column(String(24), nullable=False, default="active")   # active|completed|canceled|expired
    # policy = {"reschedule_limit":1,"expiry_days":180,"commission_bp":3000,"reschedule_cutoff_hours":2}
    policy = Column(MYSQL_JSON, nullable=True)
    partial_schedule = Column(Boolean, nullable=False, default=False)  # True when pass has been partially rescheduled
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(UTC))
    updated_at = Column(DateTime(timezone=True), default=lambda: datetime.now(UTC), onupdate=lambda: datetime.now(UTC))

    # Relationships
    days = relationship("DailyPassDay", back_populates="daily_pass")

    # Attribute synonyms for unified routes compatibility
    # start_date/end_date map to valid_from/valid_until
    start_date = _synonym("valid_from")
    end_date = _synonym("valid_until")
    # purchase_timestamp maps to created_at
    purchase_timestamp = _synonym("created_at")
    # client_id maps to user_id if not explicitly set; we provide a simple fallback at runtime via property

    def __setattr__(self, key, value):
        # Keep client_id and user_id in sync if one is set
        if key == "client_id" and value and not getattr(self, "user_id", None):
            super().__setattr__("user_id", value)
        if key == "user_id" and value and not getattr(self, "client_id", None):
            super().__setattr__("client_id", value)
        super().__setattr__(key, value)


class DailyPassDay(DailyPassBase):
    __tablename__ = "daily_pass_days"

    id = Column(String(40), primary_key=True, default=lambda: new_id("dpd"))
    pass_id = Column(String(40), ForeignKey("dailypass.daily_passes.id"), nullable=False, index=True)
    scheduled_date = Column(Date, nullable=False)
    dailypass_price=Column(Integer, nullable=False)
    status = Column(String(16), nullable=False, default="scheduled")  # scheduled|attended|missed|rescheduled|canceled
    reschedule_count = Column(Integer, nullable=False, default=0)
    checkin_at = Column(DateTime(timezone=True), nullable=True)
    meta = Column(MYSQL_JSON, nullable=True)  # {"rescheduled_from":"YYYY-MM-DD", ...}
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(UTC))
    updated_at = Column(DateTime(timezone=True), default=lambda: datetime.now(UTC), onupdate=lambda: datetime.now(UTC))

    # Relationships
    daily_pass = relationship("DailyPass", back_populates="days")

    # Additional columns expected by unified routes
    gym_id = Column(String(64), nullable=True, index=True)
    client_id = Column(String(64), nullable=True, index=True)

    # Attribute synonyms for compatibility with unified routes
    daily_pass_id = _synonym("pass_id")
    date = _synonym("scheduled_date")

    __table_args__ = (
        UniqueConstraint("pass_id", "scheduled_date", name="uq_pass_day_unique_date"),
        Index("ix_dpd_pass_sched", "pass_id", "scheduled_date"),
        {"schema": "dailypass"}
    )


class DailyPassAudit(DailyPassBase):
    """
    Append-only history of pass/day changes.
    """
    __tablename__ = "daily_pass_audit"
    __table_args__ = {"schema": "dailypass"}

    id = Column(String(40), primary_key=True, default=lambda: new_id("dpa"))
    pass_id = Column(String(40), index=True, nullable=False)
    pass_day_id = Column(String(40), nullable=True)
    action = Column(String(32), nullable=False)   # create|reschedule|checkin|missed|expire|cancel|admin_adjust
    actor = Column(String(64), nullable=False, default="system")    # system|user:<id>|gym:<id>|admin:<id>
    before = Column(MYSQL_JSON, nullable=True)
    after = Column(MYSQL_JSON, nullable=True)
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(UTC))

    # Unified additions
    details = Column(String(512), nullable=True)
    client_id = Column(String(64), nullable=True, index=True)

    # Synonyms
    daily_pass_id = _synonym("pass_id")
    timestamp = _synonym("created_at")


class DailyPassPricing(DailyPassBase):
    """
    Daily pass pricing per gym
    """
    __tablename__ = "dailypass_pricing"

    id = Column(String(40), primary_key=True, default=lambda: new_id("dpp"))
    gym_id = Column(String(64), nullable=False, index=True)
    price = Column(Integer, nullable=False)  # price in minor units (paisa)
    discount_price = Column(Integer, nullable=True)
    discount_percentage=Column(Float, nullable=True)

    __table_args__ = (
        Index("ix_dpp_gym_id", "gym_id"),
        {"schema": "dailypass"}
    )

def get_price_for_gym(*args, **kwargs) -> int:
    """Get daily pass price for a gym. Can be called with Session or without."""
    session_provided: Optional[Session] = None
    gym_id: Optional[str] = None

    # Parse arguments
    if len(args) == 2 and isinstance(args[0], Session):
        session_provided, gym_id = args[0], str(args[1])
    elif len(args) == 1 and not isinstance(args[0], Session):
        gym_id = str(args[0])
    else:
        session_provided = kwargs.get("dbs") or kwargs.get("session")
        gym_id = kwargs.get("gym_id") or kwargs.get("gymId")

    if not gym_id:
        raise ValueError("gym_id is required")

    # Use provided session or create a new one
    should_close = False
    if session_provided:
        session = session_provided
    else:
        from app.models.database import get_db
        session = next(get_db())
        should_close = True

    try:
        rec = session.query(DailyPassPricing).filter(DailyPassPricing.gym_id == str(gym_id)).first()
        if not rec:
            raise ValueError("daily pass price not configured for gym")
        return round(int(rec.discount_price) * get_markup_multiplier())
    finally:
        if should_close:
            try:
                session.close()
            except Exception:
                pass

def get_actual_price_for_gym(*args, **kwargs) -> int:
    """Get daily pass price for a gym. Can be called with Session or without."""
    session_provided: Optional[Session] = None
    gym_id: Optional[str] = None

    # Parse arguments
    if len(args) == 2 and isinstance(args[0], Session):
        session_provided, gym_id = args[0], str(args[1])
    elif len(args) == 1 and not isinstance(args[0], Session):
        gym_id = str(args[0])
    else:
        session_provided = kwargs.get("dbs") or kwargs.get("session")
        gym_id = kwargs.get("gym_id") or kwargs.get("gymId")

    if not gym_id:
        raise ValueError("gym_id is required")

    # Use provided session or create a new one
    should_close = False
    if session_provided:
        session = session_provided
    else:
        from app.models.database import get_db
        session = next(get_db())
        should_close = True

    try:
        rec = session.query(DailyPassPricing).filter(DailyPassPricing.gym_id == str(gym_id)).first()
        if not rec:
            raise ValueError("daily pass price not configured for gym")
        return int(rec.discount_price)
    finally:
        if should_close:
            try:
                session.close()
            except Exception:
                pass


class LedgerAllocation(DailyPassBase):
    """
    Per-day money slice of one Razorpay payment (Daily Pass split).
    """
    __tablename__ = "ledger_allocations"

    id = Column(String(40), primary_key=True, default=lambda: new_id("lal"))
    gym_id = Column(String(64), nullable=False, index=True)  # Will reference gyms.id from main DB
    client_id = Column(String(64), nullable=True, index=True)
    payment_id = Column(String(64), nullable=False, index=True)  # Razorpay payment_id
    order_id = Column(String(40), nullable=True)  # Will reference orders.id from payments DB
    daily_pass_id = Column(String(40), ForeignKey("dailypass.daily_passes.id"), nullable=False, index=True)
    pass_day_id = Column(String(40), ForeignKey("dailypass.daily_pass_days.id"), nullable=False, unique=True)

    # money in minor units (paisa)
    amount_gross_minor = Column(Integer, nullable=False)    # what user paid per day
    commission_minor = Column(Integer, nullable=False, default=0)  # your 30% (attended days only)
    pg_fee_minor = Column(Integer, nullable=False, default=0)      # acquiring fees per-day share (for reporting)
    tax_minor = Column(Integer, nullable=False, default=0)         # GST portion if you track
    payout_fee_minor = Column(Integer, nullable=False, default=0)  # RazorpayX fee recorded at payout time
    amount_net_minor = Column(Integer, nullable=False)             # gym share BEFORE payout fee = gross - commission

    state = Column(String(32), nullable=False, default="held_pending_settlement")
    allocation_date = Column(Date, nullable=True)
    # held_pending_settlement -> held_settled -> released_eligible_for_payout -> in_payout -> paid_out
    # or -> expired/refunded/hold

    settlement_event_id = Column(String(40), nullable=True)  # FK optional to a SettlementEvent table
    payout_id = Column(String(40), nullable=True)            # FK optional to a Payout table

    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(UTC))
    updated_at = Column(DateTime(timezone=True), default=lambda: datetime.now(UTC), onupdate=lambda: datetime.now(UTC))

    __table_args__ = (
        Index("ix_alloc_state", "state"),
        CheckConstraint("amount_net_minor >= 0", name="ck_alloc_net_nonneg"),
        {"schema": "dailypass"}
    )

    # Synonyms for unified routes compatibility
    amount = _synonym("amount_gross_minor")
    status = _synonym("state")


# Database connection setup for dailypass schema - now uses main DB
class DailyPassDatabase:
    """Daily Pass database manager - uses main app database connection"""

    def __init__(self):
        # No need for separate database setup, use main app database
        pass

    def create_tables(self):
        """Create all daily pass tables - handled by main app"""
        # Tables are created through main app's alembic migrations
        pass

    def drop_tables(self):
        """Drop all daily pass tables (use with caution!)"""
        # Not implemented - use alembic for schema management
        pass

    def get_session(self) -> Session:
        """Get a database session from main app"""
        from app.models.database import get_db
        return next(get_db())


class _SessionWrapper:
    """Wrapper to make Session usable both directly and with next(get_dailypass_session())."""
    def __init__(self, sess: Session):
        self._sess = sess
        self._consumed = False

    def __getattr__(self, name):
        return getattr(self._sess, name)

    # Iterator protocol: next(get_dailypass_session())
    def __iter__(self):
        return self

    def __next__(self):
        if self._consumed:
            raise StopIteration
        self._consumed = True
        return self._sess

    # Optional context manager support
    def __enter__(self):
        return self._sess

    def __exit__(self, exc_type, exc, tb):
        try:
            self._sess.close()
        except Exception:
            pass


# Global database instance
_dailypass_db: DailyPassDatabase = None


def get_dailypass_db() -> DailyPassDatabase:
    """Get daily pass database singleton"""
    global _dailypass_db
    if _dailypass_db is None:
        _dailypass_db = DailyPassDatabase()
    return _dailypass_db


def get_dailypass_session() -> Session:
    """Get a daily pass database session"""
    db = get_dailypass_db()
    return _SessionWrapper(db.get_session())


def create_dailypass_tables():
    """Create all daily pass tables"""
    db = get_dailypass_db()
    db.create_tables()
    print("Daily pass tables created successfully!")


# Async accessors mirroring main async DB helpers
def get_dailypass_async_sessionmaker():
    """Return shared async sessionmaker for dailypass schema (uses main DB engine)."""
    return get_async_sessionmaker()


async def get_dailypass_async_session():
    """FastAPI dependency yielding an AsyncSession for dailypass operations."""
    SessionLocal = get_dailypass_async_sessionmaker()
    async with SessionLocal() as session:
        try:
            yield session
        finally:
            await session.close()


async def get_price_for_gym_async(db: AsyncSession, gym_id: str) -> int:
    """Async version of get_price_for_gym with 30% markup applied."""
    rec = (
        await db.execute(
            select(DailyPassPricing).where(DailyPassPricing.gym_id == str(gym_id))
        )
    ).scalars().first()
    if not rec:
        raise ValueError("daily pass price not configured for gym")
    return round(int(rec.discount_price) * get_markup_multiplier())


async def get_actual_price_for_gym_async(db: AsyncSession, gym_id: str) -> int:
    """Async version of get_actual_price_for_gym returning discount_price."""
    rec = (
        await db.execute(
            select(DailyPassPricing).where(DailyPassPricing.gym_id == str(gym_id))
        )
    ).scalars().first()
    if not rec:
        raise ValueError("daily pass price not configured for gym")
    return int(rec.discount_price)
