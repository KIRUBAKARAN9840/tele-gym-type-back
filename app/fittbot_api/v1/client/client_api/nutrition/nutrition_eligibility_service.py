"""
Nutrition Eligibility Service.

Utility functions to grant nutrition consultation eligibility
based on Fittbot subscriptions and gym memberships.
"""

import logging
from datetime import datetime, timedelta
from typing import Optional, Tuple

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Session

from app.models.nutrition_models import NutritionEligibility

logger = logging.getLogger("nutrition.eligibility")


def calculate_nutrition_sessions_from_gym_duration(duration_months: int) -> int:


    if duration_months >= 4:
        return 2
    elif duration_months >= 1:
        return 1
    else:
        return 0


def calculate_nutrition_sessions_from_fittbot_plan(
    plan_name: str,
    duration_months: int,
) -> int:
    """
    Calculate number of free nutrition sessions based on Fittbot subscription.

    Rules:
    - Platinum 6 months: 1 session
    - Platinum 12 months: 2 sessions
    - Diamond 6 months: 2 sessions
    - Diamond 12 months: 3 sessions

    For plans without explicit "diamond"/"platinum" in name:
    - 12 month plans: treated as Diamond (2 sessions for 6M, 3 for 12M)
    - 6 month plans: treated as Platinum (1 session for 6M, 2 for 12M)
    """
    plan_lower = plan_name.lower() if plan_name else ""



    # Explicit Diamond plans
    if "diamond" in plan_lower:
        if duration_months >= 12:
            return 3
        elif duration_months >= 6:
            return 2
        else:
            return 0
    # Explicit Platinum plans
    elif "platinum" in plan_lower:
        if duration_months >= 12:
            return 2
        elif duration_months >= 6:
            return 1
        else:
            return 0
    # For plans without explicit tier (like "twelve_month_plan")
    # 12-month plans → Diamond tier, 6-month plans → Platinum tier
    elif duration_months >= 12:
        # Treat 12-month plans as Diamond tier
        return 2  # Diamond 12M = 3, but without explicit tier, give 2
    elif duration_months >= 6:
        # Treat 6-month plans as Platinum tier
        return 1  # Platinum 6M = 1
    else:
        return 0


async def grant_nutrition_eligibility_async(
    db: AsyncSession,
    client_id: int,
    source_type: str,  # "fittbot_subscription", "gym_membership", "personal_training"
    source_id: str,
    plan_name: str,
    duration_months: int,
    gym_id: Optional[int] = None,
) -> Tuple[bool, Optional[int]]:

    try:
        # Calculate sessions based on source type
        if source_type == "fittbot_subscription":
            sessions = calculate_nutrition_sessions_from_fittbot_plan(plan_name, duration_months)
        else:
            sessions = calculate_nutrition_sessions_from_gym_duration(duration_months)

        print("##########sessssssionssss are",sessions)

        if sessions <= 0:

            return False, None

        # Check if this exact source_id was already processed (idempotency)
        existing_source_stmt = (
            select(NutritionEligibility)
            .where(
                NutritionEligibility.client_id == client_id,
                NutritionEligibility.source_id == source_id,
            )
        )

        existing_source = (await db.execute(existing_source_stmt)).scalars().first()

        if existing_source:

            return False, existing_source.id

        # Always create new record for each purchase
        expires_at = datetime.now() + timedelta(days=180)

        eligibility = NutritionEligibility(
            client_id=client_id,
            gym_id=gym_id,
            source_type=source_type,
            source_id=source_id,
            plan_name=plan_name,
            plan_duration_months=duration_months,
            total_sessions=sessions,
            used_sessions=0,
            remaining_sessions=sessions,
            granted_at=datetime.now(),
            expires_at=expires_at,
        )
        db.add(eligibility)
        await db.flush()


        return True, eligibility.id

    except Exception as exc:
        logger.error(
            f"[NUTRITION_ELIGIBILITY_ERROR] Failed to grant eligibility: {exc}",
            extra={"client_id": client_id, "source_type": source_type},
        )
        return False, None


def grant_nutrition_eligibility_sync(
    db: Session,
    client_id: int,
    source_type: str,
    source_id: str,
    plan_name: str,
    duration_months: int,
    gym_id: Optional[int] = None,
) -> Tuple[bool, Optional[int]]:

    try:
        # Calculate sessions based on source type
        if source_type == "fittbot_subscription":
            sessions = calculate_nutrition_sessions_from_fittbot_plan(plan_name, duration_months)
        else:
            sessions = calculate_nutrition_sessions_from_gym_duration(duration_months)

        if sessions <= 0:

            return False, None

        # Check if this exact source_id was already processed (idempotency)
        existing_source = (
            db.query(NutritionEligibility)
            .filter(
                NutritionEligibility.client_id == client_id,
                NutritionEligibility.source_id == source_id,
            )
            .first()
        )

        if existing_source:

            return False, existing_source.id

        # Always create new record for each purchase
        expires_at = datetime.now() + timedelta(days=180)

        eligibility = NutritionEligibility(
            client_id=client_id,
            gym_id=gym_id,
            source_type=source_type,
            source_id=source_id,
            plan_name=plan_name,
            plan_duration_months=duration_months,
            total_sessions=sessions,
            used_sessions=0,
            remaining_sessions=sessions,
            granted_at=datetime.now(),
            expires_at=expires_at,
        )
        db.add(eligibility)
        db.flush()



        return True, eligibility.id

    except Exception as exc:
        logger.error(
            f"[NUTRITION_ELIGIBILITY_ERROR] Failed to grant eligibility: {exc}",
            extra={"client_id": client_id, "source_type": source_type},
        )
        return False, None


async def mark_session_attended_async(
    db: AsyncSession,
    booking_id: int,
) -> bool:
    """
    Mark a nutrition session as attended and decrement remaining sessions.
    Called by nutritionist when session is completed.
    """
    from app.models.nutrition_models import NutritionBooking

    try:
        booking_stmt = (
            select(NutritionBooking)
            .where(
                NutritionBooking.id == booking_id,
                NutritionBooking.status == "booked",
            )
        )
        booking = (await db.execute(booking_stmt)).scalars().first()

        if not booking:
            logger.warning(f"[NUTRITION_ATTENDANCE] Booking {booking_id} not found or not in 'booked' status")
            return False

        # Update booking status
        booking.status = "attended"
        db.add(booking)

        # Decrement remaining sessions
        eligibility_stmt = (
            select(NutritionEligibility)
            .where(NutritionEligibility.id == booking.eligibility_id)
        )
        eligibility = (await db.execute(eligibility_stmt)).scalars().first()

        if eligibility and eligibility.remaining_sessions > 0:
            eligibility.used_sessions += 1
            eligibility.remaining_sessions -= 1
            db.add(eligibility)

        await db.flush()



        return True

    except Exception as exc:
        logger.error(f"[NUTRITION_ATTENDANCE_ERROR] {exc}", extra={"booking_id": booking_id})
        return False
