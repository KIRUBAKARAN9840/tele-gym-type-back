"""
Offer Eligibility Service
Checks if a user is eligible for promotional offers (₹49 daily pass or ₹99 session)
"""
import logging
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func
from typing import Literal, TypedDict

from app.models.dailypass_models import DailyPass, DailyPassDay
from app.models.fittbot_models import (
    SessionSetting,
    SessionPurchase,
    SessionBookingDay,
    NewOffer,
    Gym,
)

logger = logging.getLogger("offer_eligibility")

OfferMode = Literal["dailypass", "session"]


class EligibilityResult(TypedDict, total=False):
    is_eligible: bool
    client_eligible: bool
    gym_eligible: bool
    available_count: int


async def check_dailypass_offer_eligibility(db: AsyncSession, client_id: int, gym_id: int) -> bool:

    try:
        # 1. Check user eligibility: User must have < 3 daily pass days total (any price)
        user_dp_count_stmt = (
            select(func.count())
            .select_from(DailyPassDay)
            .join(DailyPass, DailyPass.id == DailyPassDay.pass_id)
            .where(
                DailyPass.client_id == str(client_id),
                DailyPass.status != "canceled",
            )
        )
        user_dp_result = await db.execute(user_dp_count_stmt)
        user_dp_count = user_dp_result.scalar() or 0

        if user_dp_count >= 3:

            return False

        # 2. Check gym has dailypass feature enabled
        gym_stmt = select(Gym).where(Gym.gym_id == gym_id)
        gym_result = await db.execute(gym_stmt)
        gym = gym_result.scalars().first()

        if not gym or not gym.dailypass:

            return False


        offer_stmt = select(NewOffer).where(NewOffer.gym_id == gym_id)
        offer_result = await db.execute(offer_stmt)
        offer_entry = offer_result.scalars().first()

        if not offer_entry or not offer_entry.dailypass:
 
            return False

      
        gym_promo_count_stmt = (
            select(func.count(func.distinct(DailyPass.client_id)))
            .select_from(DailyPass)
            .join(DailyPassDay, DailyPassDay.pass_id == DailyPass.id)
            .where(
                DailyPass.gym_id == str(gym_id),
                DailyPass.status != "canceled",
                DailyPassDay.dailypass_price == 49,  # ₹49 in rupees
            )
        )
        gym_promo_result = await db.execute(gym_promo_count_stmt)
        gym_promo_count = gym_promo_result.scalar() or 0

        if gym_promo_count >= 50:

            return False

        # User is eligible if they have < 3 total bookings and gym has eligibility
        # Note: For dailypass, users CAN book ₹49 at the same gym multiple times
        return True

    except Exception as e:
        logger.error(
            "DAILYPASS_OFFER_CHECK_ERROR",
            extra={
                "client_id": client_id,
                "gym_id": gym_id,
                "error": repr(e)
            }
        )
        # On error, default to not eligible (safer)
        return False


async def check_session_offer_eligibility(db: AsyncSession, client_id: int, gym_id: int) -> bool:
    """
    Check if user is eligible for ₹99 session offer at a specific gym.

    Returns True if ALL conditions are met:
    1. User has < 3 total session bookings (any price, from PAID purchases only)
    2. Gym has opted into the offer (NewOffer.session = True)
    3. Gym has < 50 unique users who booked at ₹99
    4. Gym has sessions available (SessionSetting.is_enabled = True)
    5. User hasn't already booked ₹99 session at this gym
    """
    try:
        # 1. Check user eligibility: User must have < 3 session booking days total (any price, PAID only)
        user_session_count_stmt = (
            select(func.count())
            .select_from(SessionBookingDay)
            .join(SessionPurchase, SessionPurchase.id == SessionBookingDay.purchase_id)
            .where(
                SessionBookingDay.client_id == client_id,
                SessionBookingDay.status.in_(["booked", "attended", "no_show"]),
                SessionPurchase.status == "paid",  # Only count paid purchases
            )
        )
        user_session_result = await db.execute(user_session_count_stmt)
        user_session_count = user_session_result.scalar() or 0

        if user_session_count >= 3:

            return False

        # 2. Check gym offer flags: Gym must have opted into the offer
        offer_stmt = select(NewOffer).where(NewOffer.gym_id == gym_id)
        offer_result = await db.execute(offer_stmt)
        offer_entry = offer_result.scalars().first()

        if not offer_entry or not offer_entry.session:

            return False

        # 3. Check gym cap: Gym must have < 50 unique users who booked at ₹99
        distinct_clients_subquery = (
            select(SessionPurchase.gym_id, SessionPurchase.client_id)
            .select_from(SessionPurchase)
            .join(SessionBookingDay, SessionBookingDay.purchase_id == SessionPurchase.id)
            .where(
                SessionPurchase.gym_id == gym_id,
                SessionPurchase.status == "paid",
                SessionBookingDay.status.in_(["booked", "attended", "no_show"]),
                SessionPurchase.price_per_session == 99,
            )
            .distinct()
        ).subquery()

        gym_promo_count_stmt = (
            select(func.count(distinct_clients_subquery.c.client_id))
        )
        gym_promo_result = await db.execute(gym_promo_count_stmt)
        gym_promo_count = gym_promo_result.scalar() or 0

        if gym_promo_count >= 50:

            return False

        # 4. Check gym has sessions available (SessionSetting.is_enabled = True)
        session_settings_stmt = (
            select(SessionSetting)
            .where(
                SessionSetting.gym_id == gym_id,
                SessionSetting.is_enabled.is_(True),
                SessionSetting.final_price.isnot(None)
            )
            .limit(1)
        )
        session_settings_result = await db.execute(session_settings_stmt)
        has_sessions = session_settings_result.scalars().first() is not None

        if not has_sessions:

            return False

        # 5. Check if user already booked ₹99 at this gym
        user_gym_promo_stmt = (
            select(SessionPurchase.id)
            .select_from(SessionPurchase)
            .join(SessionBookingDay, SessionBookingDay.purchase_id == SessionPurchase.id)
            .where(
                SessionPurchase.client_id == client_id,
                SessionPurchase.gym_id == gym_id,
                SessionPurchase.status == "paid",
                SessionBookingDay.status.in_(["booked", "attended", "no_show"]),
                SessionPurchase.price_per_session == 99,
            )
            .limit(1)
        )
        user_gym_promo_result = await db.execute(user_gym_promo_stmt)
        user_already_used_promo = user_gym_promo_result.scalars().first() is not None

        if user_already_used_promo:

            return False


        return True

    except Exception as e:
        logger.error(
            "SESSION_OFFER_CHECK_ERROR",
            extra={
                "client_id": client_id,
                "gym_id": gym_id,
                "error": repr(e)
            }
        )
        # On error, default to not eligible (safer)
        return False


async def check_offer_eligibility(
    db: AsyncSession,
    client_id: int,
    gym_id: int,
    mode: OfferMode
) -> bool:
    """
    Check offer eligibility based on mode (dailypass or session).

    Args:
        db: AsyncSession database connection
        client_id: Client ID to check eligibility for
        gym_id: Gym ID to check eligibility at
        mode: Either "dailypass" or "session"

    Returns:
        bool: True if eligible, False otherwise
    """
    if mode == "dailypass":
        return await check_dailypass_offer_eligibility(db, client_id, gym_id)
    elif mode == "session":
        return await check_session_offer_eligibility(db, client_id, gym_id)
    else:
        logger.error(f"Invalid mode: {mode}")
        return False


async def check_dailypass_offer_eligibility_detailed(db: AsyncSession, client_id: int, gym_id: int) -> EligibilityResult:
    """
    Detailed eligibility check for dailypass that returns both client and gym eligibility separately.
    """
    result: EligibilityResult = {
        "is_eligible": False,
        "client_eligible": False,
        "gym_eligible": False,
        "available_count": 0
    }

    try:
        # 1. Check CLIENT eligibility: User must have < 3 daily pass days total
        user_dp_count_stmt = (
            select(func.count())
            .select_from(DailyPassDay)
            .join(DailyPass, DailyPass.id == DailyPassDay.pass_id)
            .where(
                DailyPass.client_id == str(client_id),
                DailyPass.status != "canceled",
            )
        )
        user_dp_result = await db.execute(user_dp_count_stmt)
        user_dp_count = user_dp_result.scalar() or 0

        result["available_count"] = max(3 - user_dp_count, 0)
        # Client is eligible if they have < 3 bookings
        result["client_eligible"] = user_dp_count < 3

        if not result["client_eligible"]:

            return result

        # 2. Check GYM eligibility
        # 2a. Gym must have dailypass feature enabled
        gym_stmt = select(Gym).where(Gym.gym_id == gym_id)
        gym_result = await db.execute(gym_stmt)
        gym = gym_result.scalars().first()

        if not gym or not gym.dailypass:

            return result

        # 2b. Gym must have opted into the offer
        offer_stmt = select(NewOffer).where(NewOffer.gym_id == gym_id)
        offer_result = await db.execute(offer_stmt)
        offer_entry = offer_result.scalars().first()

        if not offer_entry or not offer_entry.dailypass:

            return result

        # 2c. Gym must have < 50 promo users
        gym_promo_count_stmt = (
            select(func.count(func.distinct(DailyPass.client_id)))
            .select_from(DailyPass)
            .join(DailyPassDay, DailyPassDay.pass_id == DailyPass.id)
            .where(
                DailyPass.gym_id == str(gym_id),
                DailyPass.status != "canceled",
                DailyPassDay.dailypass_price == 49,
            )
        )
        gym_promo_result = await db.execute(gym_promo_count_stmt)
        gym_promo_count = gym_promo_result.scalar() or 0

        if gym_promo_count >= 50:

            return result

        # All gym conditions met
        # Note: For dailypass, users CAN book ₹49 at the same gym multiple times
        result["gym_eligible"] = True
        result["is_eligible"] = True

        return result

    except Exception as e:
        logger.error(
            "DAILYPASS_DETAILED_CHECK_ERROR",
            extra={"client_id": client_id, "gym_id": gym_id, "error": repr(e)}
        )
        return result


async def check_session_offer_eligibility_detailed(db: AsyncSession, client_id: int, gym_id: int) -> EligibilityResult:
    """
    Detailed eligibility check for session that returns both client and gym eligibility separately.
    """
    result: EligibilityResult = {
        "is_eligible": False,
        "client_eligible": False,
        "gym_eligible": False,
        "available_count": 0
    }

    try:
        # 1. Check CLIENT eligibility: User must have < 3 session bookings
        user_session_count_stmt = (
            select(func.count())
            .select_from(SessionBookingDay)
            .join(SessionPurchase, SessionPurchase.id == SessionBookingDay.purchase_id)
            .where(
                SessionBookingDay.client_id == client_id,
                SessionBookingDay.status.in_(["booked", "attended", "no_show"]),
                SessionPurchase.status == "paid",
            )
        )
        user_session_result = await db.execute(user_session_count_stmt)
        user_session_count = user_session_result.scalar() or 0

        result["available_count"] = max(3 - user_session_count, 0)
        # Client is eligible if they have < 3 bookings
        result["client_eligible"] = user_session_count < 3

        if not result["client_eligible"]:

            return result

        # 2. Check GYM eligibility
        # 2a. Gym must have opted into the offer
        offer_stmt = select(NewOffer).where(NewOffer.gym_id == gym_id)
        offer_result = await db.execute(offer_stmt)
        offer_entry = offer_result.scalars().first()

        if not offer_entry or not offer_entry.session:

            return result

        # 2b. Gym must have < 50 promo users
        distinct_clients_subquery = (
            select(SessionPurchase.gym_id, SessionPurchase.client_id)
            .select_from(SessionPurchase)
            .join(SessionBookingDay, SessionBookingDay.purchase_id == SessionPurchase.id)
            .where(
                SessionPurchase.gym_id == gym_id,
                SessionPurchase.status == "paid",
                SessionBookingDay.status.in_(["booked", "attended", "no_show"]),
                SessionPurchase.price_per_session == 99,
            )
            .distinct()
        ).subquery()

        gym_promo_count_stmt = select(func.count(distinct_clients_subquery.c.client_id))
        gym_promo_result = await db.execute(gym_promo_count_stmt)
        gym_promo_count = gym_promo_result.scalar() or 0

        if gym_promo_count >= 50:

            return result

        # 2c. Gym must have sessions available
        session_settings_stmt = (
            select(SessionSetting)
            .where(
                SessionSetting.gym_id == gym_id,
                SessionSetting.is_enabled.is_(True),
                SessionSetting.final_price.isnot(None)
            )
            .limit(1)
        )
        session_settings_result = await db.execute(session_settings_stmt)
        has_sessions = session_settings_result.scalars().first() is not None

        if not has_sessions:

            return result

        # 2d. User hasn't already used ₹99 at this gym
        user_gym_promo_stmt = (
            select(SessionPurchase.id)
            .select_from(SessionPurchase)
            .join(SessionBookingDay, SessionBookingDay.purchase_id == SessionPurchase.id)
            .where(
                SessionPurchase.client_id == client_id,
                SessionPurchase.gym_id == gym_id,
                SessionPurchase.status == "paid",
                SessionBookingDay.status.in_(["booked", "attended", "no_show"]),
                SessionPurchase.price_per_session == 99,
            )
            .limit(1)
        )
        user_gym_promo_result = await db.execute(user_gym_promo_stmt)
        user_already_used_promo = user_gym_promo_result.scalars().first() is not None

        if user_already_used_promo:

            return result

        # All gym conditions met
        result["gym_eligible"] = True
        result["is_eligible"] = True


        return result

    except Exception as e:
        logger.error(
            "SESSION_DETAILED_CHECK_ERROR",
            extra={"client_id": client_id, "gym_id": gym_id, "error": repr(e)}
        )
        return result


async def check_offer_eligibility_detailed(
    db: AsyncSession,
    client_id: int,
    gym_id: int,
    mode: OfferMode
) -> EligibilityResult:
    """
    Detailed eligibility check that returns both client and gym eligibility.

    Returns:
        EligibilityResult with is_eligible, client_eligible, gym_eligible
    """
    if mode == "dailypass":
        return await check_dailypass_offer_eligibility_detailed(db, client_id, gym_id)
    elif mode == "session":
        return await check_session_offer_eligibility_detailed(db, client_id, gym_id)
    else:
        logger.error(f"Invalid mode: {mode}")
        return {"is_eligible": False, "client_eligible": False, "gym_eligible": False}
