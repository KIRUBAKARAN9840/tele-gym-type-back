"""
Nutrition Consultation Models.

This module contains models for the nutrition consultation feature:
- Nutritionist management
- Schedule/slot management
- Client eligibility tracking
- Booking management
"""

from datetime import datetime

from sqlalchemy import (
    Boolean,
    Column,
    Date,
    DateTime,
    Enum,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    Time,
)
from sqlalchemy.dialects.postgresql import JSON

from app.models.database import Base

NUTRITION_SCHEMA = "nutrition"


# ═══════════════════════════════════════════════════════════════════════════════
# NUTRITIONIST - Manages nutritionist profiles
# ═══════════════════════════════════════════════════════════════════════════════
class Nutritionist(Base):
    """
    Nutritionists who provide consultation services.
    """
    __tablename__ = "nutritionists"
    __table_args__ = {"schema": NUTRITION_SCHEMA}

    id = Column(Integer, primary_key=True, autoincrement=True)
    full_name = Column(String(100), nullable=False)
    contact = Column(String(15), unique=True, nullable=False)
    email = Column(String(100), nullable=True)
    profile_image = Column(String(255), nullable=True)
    specializations = Column(JSON, nullable=True)
    experience = Column(Float, nullable=True)
    certifications = Column(Text, nullable=True)
    is_active = Column(Boolean, default=True, nullable=False)
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)


# ═══════════════════════════════════════════════════════════════════════════════
# NUTRITION_SCHEDULE - Available time slots for consultations
# ═══════════════════════════════════════════════════════════════════════════════
class NutritionSchedule(Base):
    """
    Available time slots for nutrition consultations.
    Each slot can only be booked by one client per date.
    """
    __tablename__ = "nutrition_schedules"
    __table_args__ = (
        Index("ix_nutrition_schedule_nutritionist_weekday", "nutritionist_id", "weekday"),
        {"schema": NUTRITION_SCHEMA},
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    nutritionist_id = Column(
        Integer,
        ForeignKey(f"{NUTRITION_SCHEMA}.nutritionists.id", ondelete="CASCADE"),
        nullable=False
    )
    weekday = Column(Integer, nullable=False)  # 0=Monday, 6=Sunday
    start_time = Column(Time, nullable=False)
    end_time = Column(Time, nullable=False)
    is_active = Column(Boolean, default=True, nullable=False)
    start_date = Column(Date, nullable=True)  # Schedule validity start
    end_date = Column(Date, nullable=True)  # Schedule validity end
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)


# ═══════════════════════════════════════════════════════════════════════════════
# NUTRITION_ELIGIBILITY - Tracks client eligibility for free sessions
# ═══════════════════════════════════════════════════════════════════════════════
class NutritionEligibility(Base):
    """
    Tracks clients eligible for free nutrition consultation sessions.

    Eligibility is granted based on:
    - Fittbot subscriptions (Diamond/Platinum plans, 6+ months)
    - Gym memberships purchased online (3+ months)
    - Personal training purchased online (3+ months equivalent)

    Session allocation rules:
    - Fittbot Platinum 6M: 1 session | 12M: 2 sessions
    - Fittbot Diamond 6M: 2 sessions | 12M: 3 sessions
    - Gym Membership 3-5M: 1 session | 6-11M: 2 sessions | 12M+: 3 sessions
    """
    __tablename__ = "nutrition_eligibility"
    __table_args__ = (
        Index("ix_nutrition_eligibility_client_source", "client_id", "source_type"),
        Index("ix_nutrition_eligibility_remaining", "client_id", "remaining_sessions"),
        {"schema": NUTRITION_SCHEMA},
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    # Note: Removed ForeignKey constraints to allow cross-database usage
    # The payments module uses a different DB session that can't resolve public.clients/gyms
    client_id = Column(
        Integer,
        nullable=False,
        index=True
    )
    gym_id = Column(
        Integer,
        nullable=True
    )  # Null for Fittbot subscriptions

    # Source of eligibility
    source_type = Column(
        Enum(
            "fittbot_subscription",
            "gym_membership",
            "personal_training",
            name="nutrition_eligibility_source",
            schema=NUTRITION_SCHEMA
        ),
        nullable=False
    )
    source_id = Column(String(100), nullable=True)  # Reference ID (subscription_id, membership_id, etc.)

    # Plan details
    plan_name = Column(String(100), nullable=True)  # e.g., "Platinum 6M", "Diamond 12M", "Gym 6 months"
    plan_duration_months = Column(Integer, nullable=True)  # Duration in months

    # Session tracking
    total_sessions = Column(Integer, nullable=False, default=1)
    used_sessions = Column(Integer, nullable=False, default=0)
    remaining_sessions = Column(Integer, nullable=False, default=1)

    # Validity
    granted_at = Column(DateTime, default=datetime.now)
    expires_at = Column(DateTime, nullable=True)  # Sessions expire if not used

    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)


# ═══════════════════════════════════════════════════════════════════════════════
# NUTRITION_BOOKING - Client bookings for consultation sessions
# ═══════════════════════════════════════════════════════════════════════════════
class NutritionBooking(Base):

    __tablename__ = "nutrition_bookings"
    __table_args__ = (
        Index("ix_nutrition_booking_date_status", "booking_date", "status"),
        Index("ix_nutrition_booking_nutritionist_date", "nutritionist_id", "booking_date"),
        {"schema": NUTRITION_SCHEMA},
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    client_id = Column(
        Integer,
        nullable=False,
        index=True
    )
    eligibility_id = Column(
        Integer,
        nullable=False
    )
    nutritionist_id = Column(
        Integer,
        nullable=False
    )
    schedule_id = Column(
        Integer,
        nullable=True
    )

    # Booking details
    booking_date = Column(Date, nullable=False)
    start_time = Column(Time, nullable=False)
    end_time = Column(Time, nullable=False)

    # Status tracking
    status = Column(
        Enum(
            "pending",
            "booked",
            "attended",
            "rescheduled",
            "cancelled",
            "no_show",
            name="nutrition_booking_status",
            schema=NUTRITION_SCHEMA
        ),
        default="booked",
        nullable=False
    )

    # Reschedule tracking
    rescheduled_from_id = Column(
        Integer,
        nullable=True
    )
    reschedule_reason = Column(String(255), nullable=True)
    reschedule_requested_by = Column(
        Enum(
            "client",
            "nutritionist",
            name="nutrition_reschedule_actor",
            schema=NUTRITION_SCHEMA
        ),
        nullable=True
    )

    # Session notes
    notes = Column(Text, nullable=True)
    meeting_link = Column(String(255),nullable=True)
    consultation_summary = Column(Text, nullable=True)  # Summary after session

    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)
