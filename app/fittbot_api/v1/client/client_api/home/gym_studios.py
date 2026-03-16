from fastapi import APIRouter, Depends, Request, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text, func, literal, select, or_, String, cast
from typing import Optional, Dict, Any, List, Tuple, Set
from pydantic import BaseModel
from app.models.async_database import get_async_db, get_async_sessionmaker
from app.models.fittbot_models import (
    Gym,
    GymPlans,
    GymPhoto,
    TrainerProfile,
    GymOwner,
    ReferralFittbotCash,
    GymStudiosPic,
    GymLocation,
    NoCostEmi,
    SessionSetting,
    ClassSession,
    GymStudiosRequest,
    NewOffer,
    SessionPurchase,
    SessionBookingDay,
    Client,
)
from app.models.client_activity_models import ClientActivitySummary
from app.fittbot_api.v1.payments.models.orders import Order, OrderItem
import math
from app.models.fittbot_plans_model import FittbotPlan
from app.models.dailypass_models import DailyPassPricing, DailyPass, DailyPassDay
import json
from app.utils.redis_config import get_redis
from redis.asyncio import Redis
from app.utils.logging_utils import FittbotHTTPException
import asyncio
from app.config.pricing import get_markup_multiplier


router = APIRouter(prefix="/gym_studios", tags=["GymStudios"])


GEO_KEY = "geo:gyms:verified"
GEO_REFRESH_KEY = "geo:gyms:verified:last_refresh"
VERIFIED_SET_KEY = "set:verified_gyms"
GEO_TTL_SECONDS = 3 * 60 * 60  # 3 hours

# Dailypass pricing cache keys
DAILYPASS_HASH_KEY = "hash:dailypass:pricing"  
DAILYPASS_LOW_SET_KEY = "set:dailypass:low49"  
DAILYPASS_ENABLED_SET_KEY = "set:dailypass:enabled" 
DAILYPASS_REFRESH_KEY = "dailypass:last_refresh"

# Session settings cache keys
SESSION_LOW_SET_KEY = "set:session:low99"
SESSION_REFRESH_KEY = "session:last_refresh"

# Session IDs hidden from client-facing responses
HIDDEN_SESSION_IDS = {7, 8, 10, 11, 14}

# User offer ineligibility cache keys (set when user exhausts offer)
USER_DP_INELIGIBLE_KEY = "user:{client_id}:dp_ineligible"
USER_SESSION_INELIGIBLE_KEY = "user:{client_id}:session_ineligible"

# Promo plan IDs cache key (per gym) - stores set of all promo plan IDs
PROMO_PLANS_KEY = "promo_plans:{gym_id}"
PROMO_PLANS_TTL_SECONDS = 3 * 60 * 60

# Views and frequently_booked cache keys (per gym)
GYM_VIEWS_KEY = "gym_views:{gym_id}"
GYM_VIEWS_TTL = 10 * 60  # 10 minutes
GYM_FREQ_BOOKED_KEY = "gym_freq_booked:{gym_id}"
GYM_FREQ_BOOKED_TTL = 24 * 60 * 60  # 24 hours


def calculate_nutritional_plan(duration: int) -> Dict[str, Any]:
    """Calculate nutritional plan based on duration in months"""
    if duration >= 4:
        return {"consultations": 2, "amount": 2400}
    elif duration >= 1:
        return {"consultations": 1, "amount": 1200}
    return None


def calculate_fittbot_plan_offer(gym_plan_duration: int) -> Dict[str, Any]:

    BASE_ONE_MONTH_AMOUNT = 398
    fittbot_price = gym_plan_duration * BASE_ONE_MONTH_AMOUNT

    return {
        "fittbot_plan": {
            "duration": gym_plan_duration,
            "price_rupees": fittbot_price,
        },
        "can_offer_fittbot_plan": True
    }


def smart_round_price(price: float) -> int:

    price_int = int(round(price))
    last_two_digits = price_int % 100

    if last_two_digits == 0:
        # Ends in 00 -> make it xx99
        return price_int - 1
    elif last_two_digits <= 50:
        # Ends in 01-50 -> make it xx49
        return (price_int // 100) * 100 + 49
    else:
        # Ends in 51-99 -> make it xx99
        return ((price_int // 100) + 1) * 100 - 1


def round_per_month_price(price: float) -> int:
    """
    Round per-month price for lowest_plan display.
    Rounds to the nearest ceiling number ending in 9.
    e.g., 700 -> 709, 710 -> 719, 721 -> 729, 751 -> 759
    """
    price_int = int(round(price))
    # Round to nearest ceiling 9: (price // 10) * 10 + 9
    return (price_int // 10) * 10 + 9


async def get_promo_plan_ids_from_redis(redis: Redis, gym_id: int) -> set:
    """Get all promo plan IDs for a gym from Redis cache."""
    try:
        key = PROMO_PLANS_KEY.format(gym_id=gym_id)
        members = await redis.smembers(key)
        if members:
            return {int(m.decode() if isinstance(m, bytes) else m) for m in members}
        return set()
    except Exception as e:
        print(f"Error getting promo plans from Redis: {e}")
        return set()


async def set_promo_plan_ids_in_redis(redis: Redis, gym_id: int, plan_ids: set) -> bool:
    """Set all promo plan IDs for a gym in Redis cache."""
    try:
        key = PROMO_PLANS_KEY.format(gym_id=gym_id)
        await redis.delete(key)
        if plan_ids:
            await redis.sadd(key, *[str(pid) for pid in plan_ids])
            await redis.expire(key, PROMO_PLANS_TTL_SECONDS)
        return True
    except Exception as e:
        print(f"Error setting promo plans in Redis: {e}")
        return False


def calculate_haversine_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    R = 6371
    lat1_rad = math.radians(lat1)
    lat2_rad = math.radians(lat2)
    delta_lat = math.radians(lat2 - lat1)
    delta_lon = math.radians(lon2 - lon1)
    a = math.sin(delta_lat / 2) ** 2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(delta_lon / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c


def get_bounding_box(lat: float, lng: float, radius_km: float) -> Tuple[float, float, float, float]:
    lat_delta = radius_km / 111.0
    lng_delta = radius_km / (111.0 * math.cos(math.radians(lat)))
    return (
        lat - lat_delta,  # min_lat
        lat + lat_delta,  # max_lat
        lng - lng_delta,  # min_lng
        lng + lng_delta   # max_lng
    )


async def get_user_offer_eligibility(db: AsyncSession, client_id: Optional[int], redis: Optional[Redis] = None) -> Dict[str, Any]:

    if not client_id:
        return {
            "dailypass_count": 0,
            "session_count": 0,
            "dailypass_offer_eligible": False,
            "session_offer_eligible": False,
            "client_name": None,
        }

    dp_ineligible_key = USER_DP_INELIGIBLE_KEY.format(client_id=client_id)
    session_ineligible_key = USER_SESSION_INELIGIBLE_KEY.format(client_id=client_id)

    # Check Redis first for cached ineligibility
    dp_ineligible_cached = False
    session_ineligible_cached = False

    if redis:
        try:
            dp_ineligible_cached, session_ineligible_cached = await asyncio.gather(
                redis.exists(dp_ineligible_key),
                redis.exists(session_ineligible_key)
            )
        except Exception as e:
            print(f"Redis check error for user eligibility: {e}")

    # Get client name (always needed)
    client_stmt = select(Client.name).where(Client.client_id == client_id)
    client_result = await db.execute(client_stmt)
    client_name = client_result.scalar()

    # Daily pass: Skip DB query if cached as ineligible
    if dp_ineligible_cached:
        dp_count = 3  # We know it's >= 3
    else:
        dp_stmt = (
            select(func.count())
            .select_from(DailyPassDay)
            .join(DailyPass, DailyPass.id == DailyPassDay.pass_id)
            .where(
                DailyPass.client_id == str(client_id),
                DailyPass.status != "canceled",
            )
        )
        dp_result = await db.execute(dp_stmt)
        dp_count = dp_result.scalar() or 0

        # If user became ineligible, cache it in Redis (no TTL - permanent until manually cleared)
        if dp_count >= 3 and redis:
            try:
                await redis.set(dp_ineligible_key, "1")
            except Exception as e:
                print(f"Redis set error for dp ineligibility: {e}")

    # Session: Skip DB query if cached as ineligible
    if session_ineligible_cached:
        session_count = 3  # We know it's >= 3
    else:
        session_stmt = (
            select(func.count())
            .select_from(SessionBookingDay)
            .join(SessionPurchase, SessionPurchase.id == SessionBookingDay.purchase_id)
            .where(
                SessionBookingDay.client_id == client_id,
                SessionBookingDay.status.in_(["booked", "attended", "no_show"]),
                SessionPurchase.status == "paid",
            )
        )
        session_result = await db.execute(session_stmt)
        session_count = session_result.scalar() or 0

        # If user became ineligible, cache it in Redis (no TTL - permanent until manually cleared)
        if session_count >= 3 and redis:
            try:
                await redis.set(session_ineligible_key, "1")
            except Exception as e:
                print(f"Redis set error for session ineligibility: {e}")

    return {
        "dailypass_count": dp_count,
        "session_count": session_count,
        "dailypass_offer_eligible": dp_count < 3,
        "session_offer_eligible": session_count < 3,
        "client_name": client_name,
    }


async def get_gym_offer_flags(db: AsyncSession, gym_ids: List[int]) -> Dict[int, NewOffer]:
  
    if not gym_ids:
        return {}
    stmt = select(NewOffer).where(NewOffer.gym_id.in_(gym_ids))
    result = await db.execute(stmt)
    return {row.gym_id: row for row in result.scalars().all()}


async def get_gym_promo_unique_counts(
    db: AsyncSession, gym_ids: List[int]
) -> Tuple[Dict[int, int], Dict[int, int]]:

    if not gym_ids:
        return {}, {}

    dp_stmt = (
        select(DailyPass.gym_id, func.count(func.distinct(DailyPass.client_id)))
        .select_from(DailyPass)
        .join(DailyPassDay, DailyPassDay.pass_id == DailyPass.id)
        .where(
            DailyPass.gym_id.in_([str(gid) for gid in gym_ids]),
            DailyPass.status != "canceled",
            DailyPassDay.dailypass_price == 49,
        )
        .group_by(DailyPass.gym_id)
    )

    dp_result = await db.execute(dp_stmt)
    dp_map = {int(row[0]): int(row[1]) for row in dp_result.all()}

    # Count unique users who booked at ₹99 promo price (using price_per_session column)
    # Use subquery to get distinct client_ids first, then count them to match dailypass logic
    # This ensures same user booking multiple times counts as 1
    distinct_clients_subquery = (
        select(SessionPurchase.gym_id, SessionPurchase.client_id)
        .select_from(SessionPurchase)
        .join(SessionBookingDay, SessionBookingDay.purchase_id == SessionPurchase.id)
        .where(
            SessionPurchase.gym_id.in_(gym_ids),
            SessionPurchase.status == "paid",
            SessionBookingDay.status.in_(["booked", "attended", "no_show"]),
            SessionPurchase.price_per_session == 99,
        )
        .distinct()  # DISTINCT on (gym_id, client_id) ensures each user counted once per gym
    ).subquery()

    session_stmt = (
        select(
            distinct_clients_subquery.c.gym_id,
            func.count(distinct_clients_subquery.c.client_id)
        )
        .group_by(distinct_clients_subquery.c.gym_id)
    )

    session_result = await db.execute(session_stmt)
    session_map = {int(row[0]): int(row[1]) for row in session_result.all()}

    # Debug logging
    for gym_id, count in session_map.items():
        print(f"[DEBUG Session Count] Gym ID: {gym_id}, Unique Users: {count}")

    return dp_map, session_map


async def check_user_booked_promo_at_gym(
    db: AsyncSession, client_id: Optional[int], gym_ids: List[int]
) -> Tuple[Set[int], Set[int]]:
    """Check if user has already booked ₹49 dailypass or ₹99 session at specific gyms.

    Returns:
        Tuple of (gym_ids with ₹49 dailypass booking, gym_ids with ₹99 session booking)
    """
    if not client_id or not gym_ids:
        return set(), set()

    # Check if user booked ₹49 dailypass at any of these gyms
    dp_stmt = (
        select(DailyPass.gym_id)
        .select_from(DailyPass)
        .join(DailyPassDay, DailyPassDay.pass_id == DailyPass.id)
        .where(
            DailyPass.client_id == str(client_id),
            DailyPass.gym_id.in_([str(gid) for gid in gym_ids]),
            DailyPass.status != "canceled",
            DailyPassDay.dailypass_price == 49,
        )
        .distinct()
    )
    dp_result = await db.execute(dp_stmt)
    dp_booked_gyms = {int(row[0]) for row in dp_result.all()}

    # Check if user booked ₹99 session at any of these gyms
    session_stmt = (
        select(SessionPurchase.gym_id)
        .select_from(SessionPurchase)
        .join(SessionBookingDay, SessionBookingDay.purchase_id == SessionPurchase.id)
        .where(
            SessionPurchase.client_id == client_id,
            SessionPurchase.gym_id.in_(gym_ids),
            SessionPurchase.status == "paid",
            SessionBookingDay.status.in_(["booked", "attended", "no_show"]),
            SessionPurchase.price_per_session == 99,
        )
        .distinct()
    )
    session_result = await db.execute(session_stmt)
    session_booked_gyms = {int(row[0]) for row in session_result.all()}

    return dp_booked_gyms, session_booked_gyms


async def hydrate_verified_gyms_geo(db: AsyncSession, redis: Redis) -> bool:

    lock_key = f"{GEO_REFRESH_KEY}:lock"
    acquired = await redis.set(lock_key, "1", nx=True, ex=30)  # 30 sec lock

    if not acquired:
        # Another request is already refreshing, check if data exists
        exists = await redis.exists(GEO_REFRESH_KEY)
        if exists:
            return False
        # Wait briefly for other request to complete
        await asyncio.sleep(0.1)
        exists = await redis.exists(GEO_REFRESH_KEY)
        return not exists

    try:
        # Check if refresh is needed
        exists = await redis.exists(GEO_REFRESH_KEY)
        if exists:
            await redis.delete(lock_key)
            return False

        # Fetch all verified gyms with location in single query
        location_stmt = (
            select(
                Gym.gym_id,
                GymLocation.latitude,
                GymLocation.longitude,
            )
            .join(GymLocation, GymLocation.gym_id == Gym.gym_id)
            .where(
                Gym.fittbot_verified.is_(True),
                GymLocation.latitude.isnot(None),
                GymLocation.longitude.isnot(None),
            )
        )
        result = await db.execute(location_stmt)
        rows = result.all()

        if not rows:
            await redis.setex(GEO_REFRESH_KEY, GEO_TTL_SECONDS, "empty")
            await redis.delete(lock_key)
            return True

        # Clear old geo data and rebuild
        pipe = redis.pipeline()
        pipe.delete(GEO_KEY)
        pipe.delete(VERIFIED_SET_KEY)

        # Add all gyms to GEO index in batches
        # geoadd expects flat args: longitude, latitude, member, longitude, latitude, member, ...
        geo_args = []
        verified_ids = []
        for row in rows:
            geo_args.extend([float(row.longitude), float(row.latitude), str(row.gym_id)])
            verified_ids.append(str(row.gym_id))

        if geo_args:
            pipe.execute_command("GEOADD", GEO_KEY, *geo_args)
            pipe.sadd(VERIFIED_SET_KEY, *verified_ids)

        # Set refresh marker with TTL
        pipe.setex(GEO_REFRESH_KEY, GEO_TTL_SECONDS, str(len(rows)))
        pipe.delete(lock_key)
        await pipe.execute()

        return True

    except Exception as e:
        print(f"Error hydrating geo cache: {e}")
        await redis.delete(lock_key)
        return False


async def get_verified_gym_ids_from_redis(redis: Redis) -> Set[int]:
    """Get set of verified gym IDs from Redis."""
    try:
        members = await redis.smembers(VERIFIED_SET_KEY)
        return {int(m.decode() if isinstance(m, bytes) else m) for m in members}
    except Exception:
        return set()


async def get_nearby_gyms_redis(
    redis: Redis,
    lat: float,
    lng: float,
    radius_km: float,
    count: int = 500
) -> List[Tuple[int, float]]:
    """
    Get nearby gyms using Redis GEOSEARCH.
    Returns list of (gym_id, distance_km) sorted by distance.
    """
    try:
        results = await redis.geosearch(
            GEO_KEY,
            longitude=lng,
            latitude=lat,
            radius=radius_km,
            unit="km",
            withdist=True,
            count=count,
            sort="ASC"
        )
        return [
            (int(gid.decode() if isinstance(gid, bytes) else gid), float(dist))
            for gid, dist in results
        ]
    except Exception as e:
        print(f"Redis GEOSEARCH error: {e}")
        return []


async def hydrate_dailypass_cache(db: AsyncSession, redis: Redis) -> bool:

    lock_key = f"{DAILYPASS_REFRESH_KEY}:lock"
    acquired = await redis.set(lock_key, "1", nx=True, ex=30)

    if not acquired:
        exists = await redis.exists(DAILYPASS_REFRESH_KEY)
        if exists:
            return False
        await asyncio.sleep(0.1)
        return not await redis.exists(DAILYPASS_REFRESH_KEY)

    try:
        exists = await redis.exists(DAILYPASS_REFRESH_KEY)
        if exists:
            await redis.delete(lock_key)
            return False

        # Fetch all dailypass pricing with gym verification status
        pricing_stmt = (
            select(
                DailyPassPricing.gym_id,
                DailyPassPricing.discount_price,
                Gym.dailypass,
                Gym.fittbot_verified
            )
            .join(Gym, func.cast(Gym.gym_id, String) == DailyPassPricing.gym_id)
            .where(Gym.fittbot_verified.is_(True))
        )
        result = await db.execute(pricing_stmt)
        rows = result.all()

        pipe = redis.pipeline()
        pipe.delete(DAILYPASS_HASH_KEY)
        pipe.delete(DAILYPASS_LOW_SET_KEY)
        pipe.delete(DAILYPASS_ENABLED_SET_KEY)

        pricing_data = {}
        low_49_ids = []
        enabled_ids = []

        for row in rows:
            gym_id = str(row.gym_id)
            pricing_data[gym_id] = str(row.discount_price or 0)

            if row.dailypass:
                enabled_ids.append(gym_id)
                if row.discount_price == 4900:  # ₹49 in paisa
                    low_49_ids.append(gym_id)

        if pricing_data:
            pipe.hset(DAILYPASS_HASH_KEY, mapping=pricing_data)
        if low_49_ids:
            pipe.sadd(DAILYPASS_LOW_SET_KEY, *low_49_ids)
        if enabled_ids:
            pipe.sadd(DAILYPASS_ENABLED_SET_KEY, *enabled_ids)

        pipe.setex(DAILYPASS_REFRESH_KEY, GEO_TTL_SECONDS, str(len(rows)))
        pipe.delete(lock_key)
        await pipe.execute()
        return True

    except Exception as e:
        print(f"Error hydrating dailypass cache: {e}")
        await redis.delete(lock_key)
        return False


async def hydrate_session_cache(db: AsyncSession, redis: Redis) -> bool:
   
    lock_key = f"{SESSION_REFRESH_KEY}:lock"
    acquired = await redis.set(lock_key, "1", nx=True, ex=30)

    if not acquired:
        exists = await redis.exists(SESSION_REFRESH_KEY)
        if exists:
            return False
        await asyncio.sleep(0.1)
        return not await redis.exists(SESSION_REFRESH_KEY)

    try:
        exists = await redis.exists(SESSION_REFRESH_KEY)
        if exists:
            await redis.delete(lock_key)
            return False

        # Fetch gym_ids with ₹99 session price (verified gyms only)
        session_stmt = (
            select(SessionSetting.gym_id)
            .join(Gym, Gym.gym_id == SessionSetting.gym_id)
            .where(
                Gym.fittbot_verified.is_(True),
                SessionSetting.is_enabled.is_(True),
                SessionSetting.final_price == 99
            )
            .distinct()
        )
        result = await db.execute(session_stmt)
        rows = result.all()

        pipe = redis.pipeline()
        pipe.delete(SESSION_LOW_SET_KEY)

        low_99_ids = [str(row.gym_id) for row in rows]
        if low_99_ids:
            pipe.sadd(SESSION_LOW_SET_KEY, *low_99_ids)

        pipe.setex(SESSION_REFRESH_KEY, GEO_TTL_SECONDS, str(len(rows)))
        pipe.delete(lock_key)
        await pipe.execute()
        return True

    except Exception as e:
        print(f"Error hydrating session cache: {e}")
        await redis.delete(lock_key)
        return False


async def get_dailypass_low_gym_ids(redis: Redis) -> Set[int]:
    """Get gym IDs with ₹49 dailypass from Redis."""
    try:
        members = await redis.smembers(DAILYPASS_LOW_SET_KEY)
        return {int(m.decode() if isinstance(m, bytes) else m) for m in members}
    except Exception:
        return set()


async def get_dailypass_enabled_gym_ids(redis: Redis) -> Set[int]:
    """Get gym IDs with dailypass enabled from Redis."""
    try:
        members = await redis.smembers(DAILYPASS_ENABLED_SET_KEY)
        return {int(m.decode() if isinstance(m, bytes) else m) for m in members}
    except Exception:
        return set()


async def get_dailypass_pricing(redis: Redis, gym_id: int) -> Optional[int]:
    """Get discount_price for a gym from Redis hash."""
    try:
        price = await redis.hget(DAILYPASS_HASH_KEY, str(gym_id))
        if price:
            return int(price.decode() if isinstance(price, bytes) else price)
        return None
    except Exception:
        return None


async def get_gyms_with_higher_dailypass(redis: Redis, threshold_price: int) -> Set[int]:
    """Get gym IDs with dailypass price higher than threshold."""
    try:
        all_pricing = await redis.hgetall(DAILYPASS_HASH_KEY)
        enabled = await get_dailypass_enabled_gym_ids(redis)
        result = set()
        for gym_id, price in all_pricing.items():
            gid = int(gym_id.decode() if isinstance(gym_id, bytes) else gym_id)
            p = int(price.decode() if isinstance(price, bytes) else price)
            if p > threshold_price and gid in enabled:
                result.add(gid)
        return result
    except Exception:
        return set()


async def get_session_low_gym_ids(redis: Redis) -> Set[int]:
    """Get gym IDs with ₹99 session from Redis."""
    try:
        members = await redis.smembers(SESSION_LOW_SET_KEY)
        return {int(m.decode() if isinstance(m, bytes) else m) for m in members}
    except Exception:
        return set()


async def get_gyms_with_distance_db_fallback(
    db: AsyncSession,
    gym_ids: List[int],
    client_lat: float,
    client_lng: float,
    max_distance_km: Optional[float] = None
) -> List[Tuple[int, float]]:
    """
    Fallback: Calculate distances at DB level. Used only when Redis GEO fails.
    """
    if not gym_ids:
        return []

    try:
        gym_ids_str = ",".join(str(gid) for gid in gym_ids)

        bbox_filter = ""
        if max_distance_km:
            min_lat, max_lat, min_lng, max_lng = get_bounding_box(client_lat, client_lng, max_distance_km)
            bbox_filter = f"""
                AND gl.latitude BETWEEN {min_lat} AND {max_lat}
                AND gl.longitude BETWEEN {min_lng} AND {max_lng}
            """

        distance_filter = ""
        if max_distance_km:
            distance_filter = f"""
                AND (
                    6371 * ACOS(
                        LEAST(1, GREATEST(-1,
                            COS(RADIANS(:client_lat)) * COS(RADIANS(gl.latitude)) *
                            COS(RADIANS(gl.longitude) - RADIANS(:client_lng)) +
                            SIN(RADIANS(:client_lat)) * SIN(RADIANS(gl.latitude))
                        ))
                    )
                ) <= {max_distance_km}
            """

        sql = text(f"""
            SELECT
                gl.gym_id,
                (
                    6371 * ACOS(
                        LEAST(1, GREATEST(-1,
                            COS(RADIANS(:client_lat)) * COS(RADIANS(gl.latitude)) *
                            COS(RADIANS(gl.longitude) - RADIANS(:client_lng)) +
                            SIN(RADIANS(:client_lat)) * SIN(RADIANS(gl.latitude))
                        ))
                    )
                ) AS distance_km
            FROM gym_location gl
            WHERE gl.gym_id IN ({gym_ids_str})
                AND gl.latitude IS NOT NULL
                AND gl.longitude IS NOT NULL
                {bbox_filter}
                {distance_filter}
            ORDER BY distance_km ASC
        """)

        params = {"client_lat": client_lat, "client_lng": client_lng}
        result = await db.execute(sql, params)
        rows = result.fetchall()
        return [(row[0], round(row[1], 2)) for row in rows]

    except Exception as e:
        print(f"Error in get_gyms_with_distance_db_fallback: {e}")
        return []

async def _cache_client_location(redis: Redis, key: str, lat: float, lng: float):
    """Fire-and-forget client location caching."""
    try:
        await redis.hset(key, mapping={"lat": str(lat), "lng": str(lng)})
        await redis.expire(key, 60 * 60 * 24 * 30)  # 30 days
    except Exception:
        pass


async def _build_price_sort_map(
    redis: Redis, db: AsyncSession, gym_ids: list,
    use_dailypass: bool = False, use_session: bool = False,
    use_membership: bool = False,
    dailypass_low: bool = False, session_low: bool = False,
    user_dp_eligible: bool = False, user_sess_eligible: bool = False,
    offer_map: Dict = None, dp_unique_map: Dict = None,
    session_unique_map: Dict = None, session_booked_gyms: Set = None,
) -> Dict[int, float]:

    price_for_gym: Dict[int, float] = {}
    ids_to_price = list(gym_ids)
    offer_map = offer_map or {}
    dp_unique_map = dp_unique_map or {}
    session_unique_map = session_unique_map or {}
    session_booked_gyms = session_booked_gyms or set()

    if not use_dailypass and not use_session and not use_membership:
        use_dailypass = True
        use_session = True
        use_membership = True

    dp_display_prices: Dict[int, float] = {}
    sess_display_prices: Dict[int, float] = {}

    # --- Dailypass displayed price ---
    if use_dailypass:
        pipe = redis.pipeline(transaction=False)
        for gid in ids_to_price:
            pipe.hget(DAILYPASS_HASH_KEY, str(gid))
        raw_results = await pipe.execute()

        for gid, raw in zip(ids_to_price, raw_results):
            if raw is None:
                continue
            discount_price_paisa = int(raw)

            # Actual price with markup (same as response builder)
            if discount_price_paisa == 4900:
                base_actual_price = 49
            else:
                base_actual_price = round((discount_price_paisa / 100) * get_markup_multiplier())

            # Offer logic: same as daily_pass_offer_active in response builder
            offer_entry = offer_map.get(gid)
            dp_offer_enabled = bool(offer_entry and offer_entry.dailypass)
            dp_under_50 = dp_unique_map.get(gid, 0) < 50

            dp_offer_active = (
                dailypass_low
                or (dp_offer_enabled and user_dp_eligible and dp_under_50)
            )

            if dp_offer_active:
                dp_display_prices[gid] = 49  # Promo price
            else:
                dp_display_prices[gid] = base_actual_price

        print(f"[sort] dp_display_prices sample: {dict(list(dp_display_prices.items())[:5])}")

    # --- Session displayed price ---
    if use_session and ids_to_price:
        sess_stmt = (
            select(
                SessionSetting.gym_id,
                func.min(SessionSetting.final_price).label("min_price"),
            )
            .where(
                SessionSetting.gym_id.in_(ids_to_price),
                SessionSetting.is_enabled.is_(True),
                SessionSetting.final_price.isnot(None),
                SessionSetting.session_id.notin_(HIDDEN_SESSION_IDS),
            )
            .group_by(SessionSetting.gym_id)
        )
        sess_result = await db.execute(sess_stmt)

        for row in sess_result.all():
            gid = row.gym_id
            actual_session_price = round(row.min_price * get_markup_multiplier())

            # Offer logic: same as session_offer_active in response builder
            offer_entry = offer_map.get(gid)
            sess_offer_enabled = bool(offer_entry and offer_entry.session)
            sess_under_50 = session_unique_map.get(gid, 0) < 50
            user_already_booked = gid in session_booked_gyms

            sess_offer_active = (
                session_low
                or (
                    sess_offer_enabled
                    and user_sess_eligible
                    and sess_under_50
                    and not user_already_booked
                )
            )

            if sess_offer_active:
                sess_display_prices[gid] = 99  # Promo price
            else:
                sess_display_prices[gid] = actual_session_price

        print(f"[sort] sess_display_prices sample: {dict(list(sess_display_prices.items())[:5])}")

    # --- Membership displayed price (lowest per-month) ---
    membership_display_prices: Dict[int, float] = {}
    if use_membership and ids_to_price:
        mem_stmt = (
            select(
                GymPlans.gym_id,
                func.min(GymPlans.amount / GymPlans.duration).label("min_per_month"),
            )
            .where(
                GymPlans.gym_id.in_(ids_to_price),
                GymPlans.duration > 1,
            )
            .group_by(GymPlans.gym_id)
        )
        mem_result = await db.execute(mem_stmt)
        for row in mem_result.all():
            gid = row.gym_id
            raw_per_month = float(row.min_per_month) * get_markup_multiplier()
            membership_display_prices[gid] = round_per_month_price(raw_per_month)

        print(f"[sort] membership_display_prices sample: {dict(list(membership_display_prices.items())[:5])}")

    # Pick lowest displayed price across active filters
    for gid in ids_to_price:
        candidates = []
        if gid in dp_display_prices:
            candidates.append(dp_display_prices[gid])
        if gid in sess_display_prices:
            candidates.append(sess_display_prices[gid])
        if gid in membership_display_prices:
            candidates.append(membership_display_prices[gid])
        price_for_gym[gid] = min(candidates) if candidates else float("inf")

    return price_for_gym


async def _list_gyms_handler(
    request: Request,
    db: AsyncSession = Depends(get_async_db),
    redis: Redis = Depends(get_redis),
    search: Optional[str] = None,
    city: Optional[str] = None,
    area: Optional[str] = None,
    pincode: Optional[str] = None,
    state: Optional[str] = None,
    daily_pass: Optional[int] = None,
    dailypass_low: Optional[bool] = None,
    session_low: Optional[bool] = None,
    client_lat: Optional[float] = None,
    client_lng: Optional[float] = None,
    page: int = 1,
    limit: int = 10,
    strict_filters: bool = False,
    has_dailypass: Optional[bool] = None,
    membership_types: Optional[List[str]] = None,
    no_cost_emi: Optional[bool] = None,
    session_ids: Optional[List[int]] = None,
    fitness_types: Optional[List[str]] = None,
    sort_price: Optional[bool] = False,
    sort_type: Optional[str] = "ascending",
):
    try:
        filters_present = bool(city or area or pincode or state or search or dailypass_low or session_low or has_dailypass or membership_types or no_cost_emi or (session_ids and len(session_ids) > 0) or fitness_types)
        
        client_id = getattr(request.state, 'user', None)
        location_cache_key = f"client_location:{client_id}" if client_id else None

        if client_lat is not None and client_lng is not None and location_cache_key:
            asyncio.create_task(_cache_client_location(redis, location_cache_key, client_lat, client_lng))
        
        elif location_cache_key:
            cached = await redis.hgetall(location_cache_key)
            if cached:
                cached_lat = cached.get("lat") or cached.get(b"lat")
                cached_lng = cached.get("lng") or cached.get(b"lng")
                if cached_lat and cached_lng:
                    client_lat = float(cached_lat.decode() if isinstance(cached_lat, bytes) else cached_lat)
                    client_lng = float(cached_lng.decode() if isinstance(cached_lng, bytes) else cached_lng)


        AsyncSessionLocal = get_async_sessionmaker()

        async def _hydrate_with_own_session(coro_fn, redis):
            async with AsyncSessionLocal() as session:
                return await coro_fn(session, redis)

        await asyncio.gather(
            _hydrate_with_own_session(hydrate_verified_gyms_geo, redis),
            _hydrate_with_own_session(hydrate_dailypass_cache, redis),
            _hydrate_with_own_session(hydrate_session_cache, redis),
        )

       
        verified_gym_ids = await get_verified_gym_ids_from_redis(redis)
        
        if not verified_gym_ids:
            v_result = await db.execute(
                select(Gym.gym_id).where(Gym.fittbot_verified.is_(True))
            )
            verified_gym_ids = {r[0] for r in v_result.all()}

        # STEP 3: Apply special filters using REDIS (no DB queries!)
        special_filter_ids: Optional[Set[int]] = None

        if daily_pass is not None:
            try:
                # Get source gym's price from Redis
                source_price = await get_dailypass_pricing(redis, daily_pass)
                if source_price is None:
                    special_filter_ids = set()
                else:
                    # Get gyms with higher price from Redis
                    special_filter_ids = await get_gyms_with_higher_dailypass(redis, source_price)
                    special_filter_ids = special_filter_ids & verified_gym_ids
            except Exception as e:
                print(f"Daily pass filter error: {e}")

        if dailypass_low:
            try:
                # ONLY show gyms with new_offer.dailypass=true AND has dailypass pricing AND < 50 users
                # Get all dailypass-enabled gyms (has pricing)
                enabled_dp_ids = await get_dailypass_enabled_gym_ids(redis)
                enabled_dp_ids = enabled_dp_ids & verified_gym_ids

                dp_ids = set()
                if enabled_dp_ids:
                    # Get offer flags and promo counts
                    offer_map_temp = await get_gym_offer_flags(db, list(enabled_dp_ids))
                    dp_unique_map_temp, _ = await get_gym_promo_unique_counts(db, list(enabled_dp_ids))

                    # Bulk fetch gym names (avoid N+1 query)
                    gym_names_stmt = select(Gym.gym_id, Gym.name).where(Gym.gym_id.in_(enabled_dp_ids))
                    gym_names_result = await db.execute(gym_names_stmt)
                    gym_names_map = {row.gym_id: row.name for row in gym_names_result.all()}

                    # Filter gyms that have BOTH: new_offer.dailypass=true AND dailypass pricing AND < 50 users
                    for gym_id in enabled_dp_ids:
                        if offer_map_temp.get(gym_id) and offer_map_temp.get(gym_id).dailypass:
                            current_count = dp_unique_map_temp.get(gym_id, 0)
                            if current_count < 50:
                                remaining = 50 - current_count
                                gym_name = gym_names_map.get(gym_id, "Unknown")
                                print(f"[Dailypass Low Filter] Gym: {gym_name}, Current Count: {current_count}, Remaining: {remaining}")
                                dp_ids.add(gym_id)

                special_filter_ids = dp_ids if special_filter_ids is None else special_filter_ids & dp_ids
            except Exception as e:
                print(f"Daily pass low filter error: {e}")

        if session_low:
            try:
                
                session_stmt_temp = select(SessionSetting.gym_id).where(
                    SessionSetting.gym_id.in_(verified_gym_ids),
                    SessionSetting.is_enabled.is_(True)
                ).distinct()

                session_result_temp = await db.execute(session_stmt_temp)
                gyms_with_sessions = {row[0] for row in session_result_temp.all()}

                sess_ids = set()

                if gyms_with_sessions:
               
                    offer_map_temp = await get_gym_offer_flags(db, list(gyms_with_sessions))
                    _, session_unique_map_temp = await get_gym_promo_unique_counts(db, list(gyms_with_sessions))

                    # Bulk fetch gym names (avoid N+1 query)
                    gym_names_stmt = select(Gym.gym_id, Gym.name).where(Gym.gym_id.in_(gyms_with_sessions))
                    gym_names_result = await db.execute(gym_names_stmt)
                    gym_names_map = {row.gym_id: row.name for row in gym_names_result.all()}

                    # Filter gyms that have BOTH: new_offer.session=true AND session_settings AND < 50 users
                    for gym_id in gyms_with_sessions:
                        if offer_map_temp.get(gym_id) and offer_map_temp.get(gym_id).session:
                            current_count = session_unique_map_temp.get(gym_id, 0)
                            if current_count < 50:
                                remaining = 50 - current_count
                                gym_name = gym_names_map.get(gym_id, "Unknown")
                                print(f"[Session Low Filter] Gym: {gym_name}, Current Count: {current_count}, Remaining: {remaining}")
                                sess_ids.add(gym_id)

                # Exclude gyms where user has already booked ₹99 session (prevent duplicate offer usage)
                if sess_ids and client_id:
                    _, user_session_booked_gyms = await check_user_booked_promo_at_gym(db, client_id, list(sess_ids))
                    # Remove gyms where user already booked
                    sess_ids = sess_ids - user_session_booked_gyms
                    #print(f"[Session Low Filter] Excluded {len(user_session_booked_gyms)} gyms where user already booked ₹99 sessions")

                special_filter_ids = sess_ids if special_filter_ids is None else special_filter_ids & sess_ids
            except Exception as e:
                print(f"Session low filter error: {e}")

        # has_dailypass filter: only gyms with dailypass enabled + pricing exists
        if has_dailypass:
            try:
                enabled_dp_ids = await get_dailypass_enabled_gym_ids(redis)
                dp_filter = enabled_dp_ids & verified_gym_ids
                #print(f"[has_dailypass] enabled_dp_ids={len(enabled_dp_ids)}, after verified filter={len(dp_filter)}")
                special_filter_ids = dp_filter if special_filter_ids is None else special_filter_ids & dp_filter
                #print(f"[has_dailypass] special_filter_ids count={len(special_filter_ids) if special_filter_ids else 0}")
            except Exception as e:
                print(f"has_dailypass filter error: {e}")

        # membership_types filter: only gyms with plans matching requested categories
        # Valid values: membership, pt, couple_membership, couple_pt, buddy, buddy_pt
        if membership_types:
            try:
                mt_conditions = []
                for mt in membership_types:
                    if mt == "membership":
                        mt_conditions.append(
                            (GymPlans.personal_training.is_(False)) &
                            (or_(GymPlans.plan_for.is_(None), GymPlans.plan_for.notin_(["couple", "buddy"])))
                        )
                    elif mt == "pt":
                        mt_conditions.append(
                            (GymPlans.personal_training.is_(True)) &
                            (or_(GymPlans.plan_for.is_(None), GymPlans.plan_for.notin_(["couple", "buddy"])))
                        )
                    elif mt == "couple_membership":
                        mt_conditions.append(
                            (GymPlans.personal_training.is_(False)) & (GymPlans.plan_for == "couple")
                        )
                    elif mt == "couple_pt":
                        mt_conditions.append(
                            (GymPlans.personal_training.is_(True)) & (GymPlans.plan_for == "couple")
                        )
                    elif mt == "buddy":
                        mt_conditions.append(
                            (GymPlans.personal_training.is_(False)) & (GymPlans.plan_for == "buddy")
                        )
                    elif mt == "buddy_pt":
                        mt_conditions.append(
                            (GymPlans.personal_training.is_(True)) & (GymPlans.plan_for == "buddy")
                        )

                if mt_conditions:
                    membership_stmt = select(GymPlans.gym_id).where(
                        GymPlans.gym_id.in_(verified_gym_ids),
                        or_(*mt_conditions),
                    ).distinct()
                    membership_result = await db.execute(membership_stmt)
                    membership_filter_ids = {row[0] for row in membership_result.all()}
                    special_filter_ids = membership_filter_ids if special_filter_ids is None else special_filter_ids & membership_filter_ids
            except Exception as e:
                print(f"membership_types filter error: {e}")

        # no_cost_emi filter: gyms that have opted for no-cost EMI AND have at least one plan >= ₹4000
        if no_cost_emi:
            try:
                emi_stmt = (
                    select(NoCostEmi.gym_id)
                    .where(
                        NoCostEmi.gym_id.in_(verified_gym_ids),
                        NoCostEmi.no_cost_emi.is_(True),
                    )
                )
                emi_result = await db.execute(emi_stmt)
                emi_enabled_ids = {row[0] for row in emi_result.all()}

                if emi_enabled_ids:
                    plans_stmt = select(GymPlans.gym_id).where(
                        GymPlans.gym_id.in_(emi_enabled_ids),
                        GymPlans.amount >= 4000,
                    ).distinct()
                    plans_result = await db.execute(plans_stmt)
                    emi_filter_ids = {row[0] for row in plans_result.all()}
                else:
                    emi_filter_ids = set()

                special_filter_ids = emi_filter_ids if special_filter_ids is None else special_filter_ids & emi_filter_ids
            except Exception as e:
                print(f"no_cost_emi filter error: {e}")

        # session_ids filter: only gyms offering specific session types
        if session_ids:
            try:
                #print(f"[session_ids] filtering by session_ids={session_ids}")
                session_filter_stmt = select(SessionSetting.gym_id).where(
                    SessionSetting.session_id.in_(session_ids),
                    SessionSetting.is_enabled.is_(True),
                    SessionSetting.gym_id.in_(verified_gym_ids)
                ).distinct()
                session_filter_result = await db.execute(session_filter_stmt)
                session_filter_ids = {row[0] for row in session_filter_result.all()}
                #print(f"[session_ids] matched gyms={len(session_filter_ids)}, ids={session_filter_ids}")
                special_filter_ids = session_filter_ids if special_filter_ids is None else special_filter_ids & session_filter_ids
                #print(f"[session_ids] special_filter_ids count={len(special_filter_ids) if special_filter_ids else 0}")
            except Exception as e:
                print(f"Session IDs filter error: {e}")

        # fitness_types filter: gyms whose fitness_type JSON contains any requested type
        # OR gyms with an enabled session matching the requested type (via ClassSession.internal)
        if fitness_types:
            try:

                ft_conditions = [Gym.fitness_type.like(f'%"{ft}"%') for ft in fitness_types]
                ft_stmt = select(Gym.gym_id).where(
                    or_(*ft_conditions),
                    Gym.fittbot_verified.is_(True),
                    Gym.gym_id.in_(verified_gym_ids)
                )

                # Query 2: Check session_settings + all_sessions for enabled sessions
                session_ft_stmt = select(SessionSetting.gym_id).join(
                    ClassSession, SessionSetting.session_id == ClassSession.id
                ).where(
                    ClassSession.internal.in_(fitness_types),
                    SessionSetting.is_enabled.is_(True),
                    SessionSetting.gym_id.in_(verified_gym_ids),
                    SessionSetting.session_id.notin_(HIDDEN_SESSION_IDS),
                ).distinct()

                # Run sequentially (async sessions can't run concurrent queries)
                ft_result = await db.execute(ft_stmt)
                ft_ids_from_json = {row[0] for row in ft_result.all()}

                session_ft_result = await db.execute(session_ft_stmt)
                ft_ids_from_sessions = {row[0] for row in session_ft_result.all()}
                ft_filter_ids = ft_ids_from_json | ft_ids_from_sessions

                #print(f"[fitness_types] from fitness_type col={len(ft_ids_from_json)}, from sessions={len(ft_ids_from_sessions)}, total={len(ft_filter_ids)}")
                special_filter_ids = ft_filter_ids if special_filter_ids is None else special_filter_ids & ft_filter_ids
                #print(f"[fitness_types] special_filter_ids count={len(special_filter_ids) if special_filter_ids else 0}")
            except Exception as e:
                print(f"fitness_types filter error: {e}")

        # STEP 4: Apply text filters (search, city, area, state, pincode)
        text_filter_ids: Optional[Set[int]] = None
        pincode_fallback_used = False
        pincode_gym_ids: Set[int] = set()
        include_nearby_5km = False

        if search or city or area or state or pincode:
            query_filters = [Gym.fittbot_verified.is_(True)]

            if search:
                term = f"%{search}%"
                query_filters.append(or_(
                    Gym.name.ilike(term), Gym.location.ilike(term),
                    Gym.area.ilike(term), Gym.city.ilike(term),
                    Gym.state.ilike(term), Gym.pincode.ilike(term)
                ))
            if city:
                query_filters.append(Gym.city.ilike(f"%{city}%"))
            if area:
                query_filters.append(Gym.area.ilike(f"%{area}%"))
            if state:
                query_filters.append(Gym.state.ilike(f"%{state}%"))

            if pincode:
                if strict_filters:
                    query_filters.append(Gym.pincode == pincode)
                else:
                    # Check pincode matches
                    p_filters = [*query_filters, Gym.pincode == pincode]
                    p_result = await db.execute(select(Gym.gym_id).where(*p_filters))
                    pincode_gym_ids = {r[0] for r in p_result.all()}

                    if pincode_gym_ids:
                        if client_lat is not None and client_lng is not None:
                            include_nearby_5km = True
                        else:
                            query_filters.append(Gym.pincode == pincode)
                    elif client_lat is not None and client_lng is not None:
                        pincode_fallback_used = True
                    else:
                        query_filters.append(Gym.pincode == pincode)

            # Get filtered gym IDs
            t_result = await db.execute(select(Gym.gym_id).where(*query_filters))
            text_filter_ids = {r[0] for r in t_result.all()}

        # STEP 5: Combine all filter sets
        candidate_ids = verified_gym_ids.copy()
        if special_filter_ids is not None:
            candidate_ids = candidate_ids & special_filter_ids
        if text_filter_ids is not None:
            candidate_ids = candidate_ids & text_filter_ids

        # Pre-fetch offer context for price sorting (needed before pagination)
        client_id_int = client_id if isinstance(client_id, int) else int(client_id) if client_id else None
        sort_offer_map = {}
        sort_dp_unique_map = {}
        sort_session_unique_map = {}
        sort_session_booked_gyms = set()
        sort_user_dp_eligible = False
        sort_user_sess_eligible = False

        if sort_price and candidate_ids:
            user_offer_pre = await get_user_offer_eligibility(db, client_id_int, redis)
            sort_user_dp_eligible = user_offer_pre.get("dailypass_offer_eligible", False)
            sort_user_sess_eligible = user_offer_pre.get("session_offer_eligible", False)
            sort_offer_map = await get_gym_offer_flags(db, list(candidate_ids))
            sort_dp_unique_map, sort_session_unique_map = await get_gym_promo_unique_counts(db, list(candidate_ids))
            _, sort_session_booked_gyms = await check_user_booked_promo_at_gym(db, client_id_int, list(candidate_ids))
            #print(f"[sort_price] pre-fetched offer data for {len(candidate_ids)} candidates, user_dp_eligible={sort_user_dp_eligible}, user_sess_eligible={sort_user_sess_eligible}")

        # STEP 6: Location-based filtering using REDIS GEO (no DB for distance)
        gym_distances: Dict[int, float] = {}
        include_distance_km = 10.0

        if client_lat is not None and client_lng is not None:
            max_radius = include_distance_km if (pincode_fallback_used or not filters_present) else 200.0

            nearby = await get_nearby_gyms_redis(redis, client_lat, client_lng, max_radius, count=1000)

            if nearby:
                gym_distances = {gid: dist for gid, dist in nearby}
                ordered_ids = [gid for gid, _ in nearby if gid in candidate_ids]
            else:
               
                db_results = await get_gyms_with_distance_db_fallback(
                    db, list(candidate_ids), client_lat, client_lng, max_distance_km=max_radius
                )
                gym_distances = {gid: dist for gid, dist in db_results}
                ordered_ids = [gid for gid, _ in db_results]

            # Gyms without location go at end
            with_loc = set(gym_distances.keys())
            without_loc = [gid for gid in candidate_ids if gid not in with_loc]

            if pincode_fallback_used:
                final_ordered_ids = ordered_ids
            elif include_nearby_5km:
                combined = []
                for gid in ordered_ids:
                    dist = gym_distances.get(gid)
                    if gid in pincode_gym_ids or (dist is not None and dist <= include_distance_km):
                        combined.append(gid)
                pincode_no_loc = [g for g in without_loc if g in pincode_gym_ids]
                final_ordered_ids = combined + pincode_no_loc
            elif not filters_present:
                final_ordered_ids = [
                    gid for gid in ordered_ids
                    if gym_distances.get(gid) is not None and gym_distances[gid] <= include_distance_km
                ]
            else:
                final_ordered_ids = ordered_ids + without_loc

            # Price-based re-sorting (distance cap already applied above)
            if sort_price and final_ordered_ids:
                #print(f"[sort_price] sorting {len(final_ordered_ids)} gyms by price, sort_type={sort_type}, has_dailypass={has_dailypass}, session_ids={session_ids}")
                price_for_gym = await _build_price_sort_map(
                    redis, db, final_ordered_ids,
                    use_dailypass=bool(has_dailypass), use_session=bool(session_ids),
                    use_membership=bool(membership_types),
                    dailypass_low=bool(dailypass_low), session_low=bool(session_low),
                    user_dp_eligible=sort_user_dp_eligible, user_sess_eligible=sort_user_sess_eligible,
                    offer_map=sort_offer_map, dp_unique_map=sort_dp_unique_map,
                    session_unique_map=sort_session_unique_map, session_booked_gyms=sort_session_booked_gyms,
                )
                #print(f"[sort_price] price map sample: {dict(list(price_for_gym.items())[:5])}")
                reverse = (sort_type == "descending")
                final_ordered_ids = sorted(
                    final_ordered_ids,
                    key=lambda gid: price_for_gym.get(gid, float('inf')),
                    reverse=reverse,
                )
                #print(f"[sort_price] sorted order (first 10): {final_ordered_ids[:10]}")

            total_count = len(final_ordered_ids)
            offset = (page - 1) * limit
            paginated_ids = final_ordered_ids[offset:offset + limit]

            if paginated_ids:
                gyms_stmt = select(Gym).where(Gym.gym_id.in_(paginated_ids))
                gyms_result = await db.execute(gyms_stmt)
                gyms_dict = {g.gym_id: g for g in gyms_result.scalars().all()}
                # Preserve Redis distance ordering
                gyms = [gyms_dict[gid] for gid in paginated_ids if gid in gyms_dict]

                # Log if any gyms are missing (helps debug Redis-DB sync issues)
                missing = set(paginated_ids) - set(gyms_dict.keys())
                if missing:
                    print(f"[WARNING] Missing {len(missing)} gyms from DB that were in Redis: {missing}")
            else:
                gyms = []
        else:
            if not filters_present:
                total_count = 0
                gyms = []
            else:
                # Sort candidate_ids: by price if requested, otherwise by gym_id for consistency
                if sort_price and candidate_ids:
                    #print(f"[sort_price no-loc] sorting {len(candidate_ids)} gyms by price, sort_type={sort_type}, has_dailypass={has_dailypass}, session_ids={session_ids}")
                    price_for_gym = await _build_price_sort_map(
                        redis, db, list(candidate_ids),
                        use_dailypass=bool(has_dailypass), use_session=bool(session_ids),
                        use_membership=bool(membership_types),
                        dailypass_low=bool(dailypass_low), session_low=bool(session_low),
                        user_dp_eligible=sort_user_dp_eligible, user_sess_eligible=sort_user_sess_eligible,
                        offer_map=sort_offer_map, dp_unique_map=sort_dp_unique_map,
                        session_unique_map=sort_session_unique_map, session_booked_gyms=sort_session_booked_gyms,
                    )
                    #print(f"[sort_price no-loc] price map sample: {dict(list(price_for_gym.items())[:5])}")
                    reverse = (sort_type == "descending")
                    final_ids = sorted(
                        list(candidate_ids),
                        key=lambda gid: price_for_gym.get(gid, float('inf')),
                        reverse=reverse,
                    )
                    #print(f"[sort_price no-loc] sorted order (first 10): {final_ids[:10]}")
                else:
                    final_ids = sorted(list(candidate_ids))
                total_count = len(final_ids)
                offset = (page - 1) * limit
                paginated_ids = final_ids[offset:offset + limit]

                if paginated_ids:
                    gyms_stmt = select(Gym).where(Gym.gym_id.in_(paginated_ids))
                    gyms_result = await db.execute(gyms_stmt)
                    gyms_dict = {g.gym_id: g for g in gyms_result.scalars().all()}
                    # Preserve order from paginated_ids
                    gyms = [gyms_dict[gid] for gid in paginated_ids if gid in gyms_dict]

                    # Log if any gyms are missing
                    missing = set(paginated_ids) - set(gyms_dict.keys())
                    if missing:
                        print(f"[WARNING] Missing {len(missing)} gyms from DB in non-location query: {missing}")
                else:
                    gyms = []

        gym_list = []

        # Offer eligibility (user-level caps)
        client_id_int = client_id if isinstance(client_id, int) else int(client_id) if client_id else None
        user_offer = await get_user_offer_eligibility(db, client_id_int, redis)

        # BULK FETCH: Eliminate N+1 queries
        gym_ids_to_fetch = [gym.gym_id for gym in gyms]

        # Gym-level offer flags and promo user counts (50 unique-user caps)
        offer_map = await get_gym_offer_flags(db, gym_ids_to_fetch)
        dp_unique_map, session_unique_map = await get_gym_promo_unique_counts(db, gym_ids_to_fetch)

        # Check which gyms the user has already booked promo prices at
        dp_booked_gyms, session_booked_gyms = await check_user_booked_promo_at_gym(db, client_id_int, gym_ids_to_fetch)


        cover_pics_map = {}
        pricing_map = {}
        session_map = {}
        plans_map = {}
        emi_map = {}
        location_map = {}
        views_map = {}
        frequently_booked_set = set()

        if gym_ids_to_fetch:
            
            cover_pics_stmt = select(GymStudiosPic).where(
                GymStudiosPic.gym_id.in_(gym_ids_to_fetch),
                GymStudiosPic.type == 'cover_pic'
            )
            cover_pics_result = await db.execute(cover_pics_stmt)
            cover_pics_map = {cp.gym_id: cp.image_url for cp in cover_pics_result.scalars().all()}

            # Bulk fetch daily pass pricing
            pricing_stmt = select(DailyPassPricing).where(
                DailyPassPricing.gym_id.in_([str(gid) for gid in gym_ids_to_fetch])
            )
            pricing_result = await db.execute(pricing_stmt)
            pricing_map = {int(p.gym_id): p for p in pricing_result.scalars().all()}

            # Bulk fetch session settings
            session_stmt = select(SessionSetting).where(
                SessionSetting.gym_id.in_(gym_ids_to_fetch),
                SessionSetting.is_enabled.is_(True),
                SessionSetting.final_price.isnot(None),
                SessionSetting.session_id.notin_(HIDDEN_SESSION_IDS),
            )
            session_result = await db.execute(session_stmt)
            for session in session_result.scalars().all():
                if session.gym_id not in session_map:
                    session_map[session.gym_id] = []
                session_map[session.gym_id].append(session)

            # Bulk fetch gym plans
            plans_stmt = select(GymPlans).where(GymPlans.gym_id.in_(gym_ids_to_fetch))
            plans_result = await db.execute(plans_stmt)
            for plan in plans_result.scalars().all():
                if plan.gym_id not in plans_map:
                    plans_map[plan.gym_id] = []
                plans_map[plan.gym_id].append(plan)

            # Bulk fetch no cost EMI
            emi_stmt = select(NoCostEmi).where(NoCostEmi.gym_id.in_(gym_ids_to_fetch))
            emi_result = await db.execute(emi_stmt)
            emi_map = {emi.gym_id: emi.no_cost_emi for emi in emi_result.scalars().all()}

            # Bulk fetch gym locations
            location_stmt = select(GymLocation).where(GymLocation.gym_id.in_(gym_ids_to_fetch))
            location_result = await db.execute(location_stmt)
            location_map = {loc.gym_id: loc for loc in location_result.scalars().all()}

            # Bulk fetch views — Redis first, DB fallback for misses
            views_miss_ids = []
            try:
                views_pipe = redis.pipeline(transaction=False)
                for gid in gym_ids_to_fetch:
                    views_pipe.get(GYM_VIEWS_KEY.format(gym_id=gid))
                views_raw = await views_pipe.execute()
                for gid, raw in zip(gym_ids_to_fetch, views_raw):
                    if raw is not None:
                        views_map[gid] = int(raw)
                    else:
                        views_miss_ids.append(gid)
            except Exception:
                views_miss_ids = gym_ids_to_fetch

            if views_miss_ids:
                views_stmt = (
                    select(
                        ClientActivitySummary.gym_id,
                        func.sum(ClientActivitySummary.total_views).label("views")
                    )
                    .where(ClientActivitySummary.gym_id.in_(views_miss_ids))
                    .group_by(ClientActivitySummary.gym_id)
                )
                views_result = await db.execute(views_stmt)
                db_views = {row.gym_id: int(row.views) for row in views_result.all()}
                views_map.update(db_views)
                # Cache back to Redis (fire-and-forget)
                try:
                    cache_pipe = redis.pipeline(transaction=False)
                    for gid in views_miss_ids:
                        cache_pipe.setex(
                            GYM_VIEWS_KEY.format(gym_id=gid),
                            GYM_VIEWS_TTL,
                            str(db_views.get(gid, 0))
                        )
                    await cache_pipe.execute()
                except Exception:
                    pass

            # Bulk fetch frequently_booked — Redis first, DB fallback for misses
            freq_miss_ids = []
            try:
                freq_pipe = redis.pipeline(transaction=False)
                for gid in gym_ids_to_fetch:
                    freq_pipe.get(GYM_FREQ_BOOKED_KEY.format(gym_id=gid))
                freq_raw = await freq_pipe.execute()
                for gid, raw in zip(gym_ids_to_fetch, freq_raw):
                    if raw is not None:
                        if raw == b"1" or raw == "1":
                            frequently_booked_set.add(gid)
                    else:
                        freq_miss_ids.append(gid)
            except Exception:
                freq_miss_ids = gym_ids_to_fetch

            if freq_miss_ids:
                freq_stmt = (
                    select(OrderItem.gym_id)
                    .join(Order, Order.id == OrderItem.order_id)
                    .where(
                        OrderItem.gym_id.in_([str(gid) for gid in freq_miss_ids]),
                        Order.status == "paid"
                    )
                    .distinct()
                )
                freq_result = await db.execute(freq_stmt)
                db_freq = {int(row[0]) for row in freq_result.all()}
                frequently_booked_set.update(db_freq)
                # Cache back to Redis (fire-and-forget)
                try:
                    cache_pipe = redis.pipeline(transaction=False)
                    for gid in freq_miss_ids:
                        cache_pipe.setex(
                            GYM_FREQ_BOOKED_KEY.format(gym_id=gid),
                            GYM_FREQ_BOOKED_TTL,
                            "1" if gid in db_freq else "0"
                        )
                    await cache_pipe.execute()
                except Exception:
                    pass

        for gym in gyms:
            cover_pic_url = cover_pics_map.get(gym.gym_id, "")
            distance_km = gym_distances.get(gym.gym_id)

            gym_data = {
                "gym_id": gym.gym_id,
                "gym_name": gym.name.upper() if gym.name else None,
                "logo": gym.logo,
                "cover_pic": cover_pic_url,
                "address": {
                    "door_no":gym.door_no,
                    "building":gym.building,
                    "street": gym.street,
                    "area": gym.area,
                    "city": gym.city,
                    "state": gym.state,
                    "pincode": gym.pincode,
                },
                "contact_number": gym.contact_number,
                "services": gym.services,
                "operating_hours": gym.operating_hours,
                "gym_timings": gym.gym_timings,
                "dailypass": gym.dailypass,
                "distance_km": round(distance_km, 2) if distance_km is not None else None,
                "views": views_map.get(gym.gym_id, 0),
                "frequently_booked": gym.gym_id in frequently_booked_set,
            }

            offer_entry = offer_map.get(gym.gym_id)
            dailypass_offer_enabled = bool(offer_entry and offer_entry.dailypass)
            session_offer_enabled = bool(offer_entry and offer_entry.session)

            # Daily pass offer: gym opted in + user < 3 bookings + gym < 50 promo users
            # OVERRIDE: If dailypass_low filter is active, force promo price for ALL returned gyms
            dp_current_count = dp_unique_map.get(gym.gym_id, 0)
            dp_under_50 = dp_current_count < 50

            if gym.dailypass and dailypass_offer_enabled and dp_under_50:
                dp_remaining = 50 - dp_current_count
                #print(f"[Dailypass Offer Check] Gym: {gym.name}, Current Count: {dp_current_count}, Remaining: {dp_remaining}")

            daily_pass_offer_active = (
                dailypass_low  # If filter is active, ALL gyms get promo price
                or (
                    gym.dailypass
                    and dailypass_offer_enabled
                    and user_offer.get("dailypass_offer_eligible", False)
                    and dp_under_50
                )
            )

            if gym.dailypass:
                try:
                    pricing_record = pricing_map.get(gym.gym_id)
                    if pricing_record:
                        # Actual price from gym owner (with 30% markup)
                        # Use discount_price (not price) to match checkout processor logic
                        base_actual_price = round(((pricing_record.discount_price or 0) / 100) * get_markup_multiplier()) if pricing_record.discount_price else None

                        # Special case: If discount_price is exactly ₹49, no markup (matches processor logic)
                        if pricing_record.discount_price == 4900:
                            base_actual_price = 49

                        # If dailypass_low filter is active OR offer is active → show FIXED ₹49 promo price
                        # Otherwise → show actual price set by gym owner
                        if dailypass_low or daily_pass_offer_active:
                            gym_data["daily_pass_discount_price"] = 49  # Fixed promo price
                        else:
                            gym_data["daily_pass_discount_price"] = base_actual_price
                        gym_data["daily_pass_actual_price"] = base_actual_price
                        gym_data["daily_pass_discount"] = pricing_record.discount_percentage if pricing_record.discount_percentage else None
                    else:
                        gym_data["dailypass"] = False
                        gym_data["daily_pass_discount_price"] = None
                        gym_data["daily_pass_actual_price"] = None
                        gym_data["daily_pass_discount"] = None

                except Exception as e:
                    print(f"Error fetching daily pass price for gym {gym.gym_id}: {e}")

            else:
                gym_data["daily_pass_discount_price"] = None
                gym_data["daily_pass_actual_price"] = None
                gym_data["daily_pass_discount"] = None

            # Get lowest session price with offer gating
            session_settings = session_map.get(gym.gym_id, [])
            actual_session_price = None
            if session_settings:
                lowest_session_record = min(session_settings, key=lambda x: x.final_price)
                # Actual price with 30% markup
                actual_session_price = round((lowest_session_record.final_price) * get_markup_multiplier())

            # Session offer: gym opted in + user < 3 bookings + gym < 50 promo users + has sessions
            # OVERRIDE: If session_low filter is active, force promo price for ALL returned gyms
            # NEW: If user already booked ₹99 session at THIS gym, they can't get the offer again
            # REQUIRES BOTH: new_offer.session=true AND at least one session_setting
            session_current_count = session_unique_map.get(gym.gym_id, 0)
            session_under_50 = session_current_count < 50
            user_already_booked_here = gym.gym_id in session_booked_gyms

            if session_offer_enabled and session_settings and session_under_50:
                session_remaining = 50 - session_current_count
                #print(f"[Session Offer Check] Gym: {gym.name}, Current Count: {session_current_count}, Remaining: {session_remaining}, User Already Booked: {user_already_booked_here}")

            session_offer_active = (
                session_low  # If filter is active, ALL gyms get promo price (already excluded if user booked)
                or (
                    session_offer_enabled  # new_offer.session=true
                    and session_settings  # AND has at least one session_setting
                    and user_offer.get("session_offer_eligible", False)
                    and session_under_50
                    and not user_already_booked_here  # User hasn't booked ₹99 session at this gym before
                )
            )

            # If session_low filter is active OR offer is active → show FIXED ₹99 promo price; otherwise show actual price
            gym_data["lowest_session"] = 99 if (session_low or session_offer_active) else actual_session_price
            gym_data["dailypass_offer_active"] = daily_pass_offer_active
            gym_data["session_offer_active"] = session_offer_active

            # Get lowest per-month plan (same calculation as promo_plans)
            lowest_plan = None
            gym_plans = plans_map.get(gym.gym_id, [])

            if gym_plans:

                plans_with_per_month = []
                for plan in gym_plans:
                    if plan.duration <= 1:
                        continue

                    # Calculate raw per_month: raw × markup ÷ duration (no bonus in divisor)
                    raw_increased = plan.amount * get_markup_multiplier()
                    raw_per_month = raw_increased / plan.duration

                    # Apply ceiling 9 rounding to per_month
                    rounded_per_month = round_per_month_price(raw_per_month)

                    # Calculate final amount = rounded_per_month × duration
                    final_amount = rounded_per_month * plan.duration

                    plans_with_per_month.append((plan, rounded_per_month, final_amount))

                if plans_with_per_month:

                    lowest_plan_record, per_month_value, final_amount = min(plans_with_per_month, key=lambda x: x[1])

                    # Calculate original_amount using same logic (per_month × duration, no bonus)
                    original_amount_final = None
                    if lowest_plan_record.original_amount and lowest_plan_record.original_amount > 0:
                        raw_original_per_month = (lowest_plan_record.original_amount * get_markup_multiplier()) / lowest_plan_record.duration
                        rounded_original_per_month = round_per_month_price(raw_original_per_month)
                        original_amount_final = rounded_original_per_month * lowest_plan_record.duration

                    discount_percentage = None
                    if lowest_plan_record.original_amount and lowest_plan_record.original_amount > 0:
                        discount_percentage = round(((lowest_plan_record.original_amount - lowest_plan_record.amount) / lowest_plan_record.original_amount) * 100)

                    has_no_cost_emi = emi_map.get(gym.gym_id, False)
                    fittbot_offer = calculate_fittbot_plan_offer(gym_plan_duration=lowest_plan_record.duration)
                    nutritional_plan = calculate_nutritional_plan(lowest_plan_record.duration)

                    category_label = "Membership Plans"
                    if lowest_plan_record.personal_training:
                        if lowest_plan_record.plan_for == "couple":
                            category_label = "Couple PT"
                        elif lowest_plan_record.plan_for == "buddy":
                            category_label = "Buddy PT"
                        else:
                            category_label = "Personal Training"
                    else:
                        if lowest_plan_record.plan_for == "couple":
                            category_label = "Couple Membership"
                        elif lowest_plan_record.plan_for == "buddy":
                            category_label = "Buddy"

                    lowest_plan = {
                        "plan_id": lowest_plan_record.id,
                        "amount": final_amount,
                        "original_amount": original_amount_final,
                        "duration": lowest_plan_record.duration,
                        "per_month": per_month_value,
                        "discount_percentage": discount_percentage,
                        "no_cost_emi": has_no_cost_emi,
                        "bonus": lowest_plan_record.bonus,
                        "bonus_type": lowest_plan_record.bonus_type,
                        "plan_for": lowest_plan_record.plan_for,
                        "pause": lowest_plan_record.pause,
                        "services": lowest_plan_record.services,
                        "fittbot_plan_offer": fittbot_offer,
                        "nutritional_plan": nutritional_plan,
                        "category_label": category_label,
                        "sessions_count": lowest_plan_record.sessions_count
                    }


                    asyncio.create_task(set_promo_plan_ids_in_redis(redis, gym.gym_id, {lowest_plan_record.id}))

            gym_data["lowest_plan"] = lowest_plan

            location_record = location_map.get(gym.gym_id)
            if location_record:
                gym_data["exact_location"] = {
                    "latitude": location_record.latitude,
                    "longitude": location_record.longitude
                }
            else:
                gym_data["exact_location"] = None

            gym_list.append(gym_data)


        total_pages = (total_count + limit - 1) // limit
        has_next = page < total_pages
        has_prev = page > 1

        # If no gyms found, get the lowest daily pass price available in Fittbot (only from verified gyms)
        lowest_dailypass_price = None

        if not gym_list:
            lowest_pricing_stmt = (
                select(DailyPassPricing)
                .join(Gym, func.cast(Gym.gym_id, String) == DailyPassPricing.gym_id)
                .where(Gym.fittbot_verified == True, Gym.dailypass == True)
                .order_by(DailyPassPricing.discount_price.asc())
                .limit(1)
            )
            lowest_pricing_result = await db.execute(lowest_pricing_stmt)
            lowest_pricing = lowest_pricing_result.scalars().first()

            if lowest_pricing:
                lowest_dailypass_price = {
                    "gym_id": lowest_pricing.gym_id,
                    "price": lowest_pricing.price * 0.01 if lowest_pricing.price else None,
                    "discount_price": lowest_pricing.discount_price * 0.01 if lowest_pricing.discount_price else None,
                    "discount_percentage": lowest_pricing.discount_percentage
                }

       
        return {
            "status": 200,
            "data": gym_list,
            "lowest_dailypass_price": lowest_dailypass_price,
            "dailypass_offer_eligible": user_offer.get("dailypass_offer_eligible", False),
            "session_offer_eligible": user_offer.get("session_offer_eligible", False),
            "dailypass_count": user_offer.get("dailypass_count", 0),
            "session_count": user_offer.get("session_count", 0),
            "client_name": user_offer.get("client_name"),
            "pagination": {
                "current_page": page,
                "total_pages": total_pages,
                "total_count": total_count,
                "has_next": has_next,
                "has_prev": has_prev,
                "limit": limit
            }
        }
 
    
    except FittbotHTTPException:
        raise
    
    except Exception as e:
        await db.rollback()
        raise FittbotHTTPException(
            status_code=500,
            detail="Unable to retrieve gyms",
            error_code="GYM_LIST_FETCH_ERROR",
            log_data={"exc": repr(e)},
        )


@router.get("/list_gyms")
async def list_gyms_v2(
    request: Request,
    db: AsyncSession = Depends(get_async_db),
    redis: Redis = Depends(get_redis),
    search: Optional[str] = None,
    city: Optional[str] = None,
    area: Optional[str] = None,
    pincode: Optional[str] = None,
    state: Optional[str] = None,
    daily_pass: Optional[int] = None,
    dailypass_low: Optional[bool] = None,
    session_low: Optional[bool] = None,
    client_lat: Optional[float] = None,
    client_lng: Optional[float] = None,
    page: int = 1,
    limit: int = 10,
    has_daily_pass: Optional[bool] = None,
    membership_types: Optional[List[str]] = Query(default=None),
    no_cost_emi: Optional[bool] = None,
    selected_sessions: Optional[List[int]] = Query(default=None),
    fitness_types: Optional[List[str]] = Query(default=None),
    sort_price: Optional[bool] = False,
    sort_type: Optional[str] = "ascending",
):

    filters_present = bool(city or area or pincode or state or search or dailypass_low or session_low or has_daily_pass or membership_types or no_cost_emi or selected_sessions or fitness_types)


    return await _list_gyms_handler(
        request=request,
        db=db,
        redis=redis,
        search=search,
        city=city,
        area=area,
        pincode=pincode,
        state=state,
        daily_pass=daily_pass,
        dailypass_low=dailypass_low,
        session_low=session_low,
        client_lat=client_lat,
        client_lng=client_lng,
        page=page,
        limit=limit,
        strict_filters=filters_present,
        has_dailypass=has_daily_pass,
        membership_types=membership_types,
        no_cost_emi=no_cost_emi,
        session_ids=selected_sessions,
        fitness_types=fitness_types,
        sort_price=sort_price,
        sort_type=sort_type,
    )
 

@router.get("/gym")
async def get_gym_details(request: Request, gym_id: int, db: AsyncSession = Depends(get_async_db), redis: Redis = Depends(get_redis)):
    try:
        gym_stmt = select(Gym).where(Gym.gym_id == gym_id, Gym.fittbot_verified.is_(True)).limit(1)
        gym_result = await db.execute(gym_stmt)
        gym = gym_result.scalars().first()
        if not gym:
            raise FittbotHTTPException(
                status_code=404,
                detail="Gym not found",
                error_code="GYM_NOT_FOUND"
            )

        client_id = getattr(request.state, 'user', None)
        client_id_int = int(client_id) if client_id else None
 
        gym_photos_result = await db.execute(select(GymStudiosPic).where(GymStudiosPic.gym_id == gym_id))
        gym_photos = gym_photos_result.scalars().all()
        
        photos = [
            {
                "photo_id": photo.photo_id,
                "type": photo.type,
                "image_url": photo.image_url
            } for photo in gym_photos
        ]
 
        trainers_result = await db.execute(select(TrainerProfile).where(TrainerProfile.gym_id == gym_id))
        trainers = trainers_result.scalars().all()
        trainer_details = [
            {
                "profile_id": trainer.profile_id,
                "trainer_id": trainer.trainer_id,
                "full_name": trainer.full_name,
                "email": trainer.email,
                "specializations": trainer.specializations,
                "experience": trainer.experience,
                "certifications": trainer.certifications,
                "work_timings": trainer.work_timings,
                "profile_image": trainer.profile_image,
                "personal_trainer": trainer.personal_trainer
            } for trainer in trainers
        ]
 
        
        no_cost_emi_result = await db.execute(select(NoCostEmi).where(NoCostEmi.gym_id == gym_id))
        no_cost_emi_record = no_cost_emi_result.scalars().first()
        gym_no_cost_emi_enabled = no_cost_emi_record.no_cost_emi if no_cost_emi_record else False

        gym_plans_result = await db.execute(select(GymPlans).where(GymPlans.gym_id == gym_id))
        gym_plans = gym_plans_result.scalars().all()
        plans = []

        duration_count = {}
        for plan in gym_plans:
            key = (plan.duration, plan.personal_training, plan.plan_for)
            duration_count[key] = duration_count.get(key, 0) + 1


        for plan in gym_plans:

            fittbot_offer = calculate_fittbot_plan_offer(gym_plan_duration=plan.duration)
            nutritional_plan = calculate_nutritional_plan(plan.duration)
            plan_no_cost_emi = gym_no_cost_emi_enabled and plan.amount >= 4000

            increased_amount = smart_round_price(plan.amount * get_markup_multiplier())
            increased_original = smart_round_price(plan.original_amount * get_markup_multiplier()) if plan.original_amount else None

            # per_month = 49/99 rounded amount ÷ duration (no bonus in divisor)
            per_month = round(increased_amount / plan.duration) if plan.duration > 0 else increased_amount

            # Calculate user_saving_price: nutrition session value + fymble subscription value
            nutrition_saving = (nutritional_plan["consultations"] * 1000) if nutritional_plan else 0
            fymble_saving = 398 * plan.duration
            user_saving_price = nutrition_saving + fymble_saving

            plan_dict = {
                "plan_id": plan.id,
                "plan_name": plan.plans,
                "amount": increased_amount,
                "duration": plan.duration,
                "description": plan.description,
                "services": plan.services,
                "personal_training": plan.personal_training,
                "original": increased_original,
                "bonus": plan.bonus,
                "bonus_type": plan.bonus_type,
                "pause": plan.pause,
                "pause_type": plan.pause_type,
                "fittbot_plan_offer": fittbot_offer,
                "is_couple": True if plan.plan_for=="couple" else False,
                "plan_for": plan.plan_for,
                "buddy_count": plan.buddy_count,
                "nutritional_plan": nutritional_plan,
                "no_cost_emi": plan_no_cost_emi,
                "per_month": per_month,
                "user_saving_price": user_saving_price,
                "raw_amount": plan.amount,  # Store raw amount for lowest_plan calculation
                "raw_original_amount": plan.original_amount,  # Store raw original for lowest_plan calculation
                "duplicate": duration_count[(plan.duration, plan.personal_training, plan.plan_for)] > 1,
                "sessions_count": plan.sessions_count
            }
            plans.append(plan_dict)

        
        
        plans.sort(key=lambda x: (x["duration"], x["amount"]))


        def get_lowest_per_month(category_plans, category_label):

            eligible = [p for p in category_plans if p["duration"] > 1]
            if not eligible:
                return None

            lowest = min(eligible, key=lambda x: x["per_month"])

            return {
                "plan_id": lowest["plan_id"],
                "plan_name": lowest["plan_name"],
                "amount": lowest["amount"],
                "original_amount": lowest["original"],
                "duration": lowest["duration"],
                "per_month": lowest["per_month"],
                "bonus": lowest["bonus"],
                "bonus_type": lowest["bonus_type"],
                "category_label": category_label,
                "raw_amount": lowest["raw_amount"],
                "raw_original_amount": lowest["raw_original_amount"],
                "sessions_count": lowest["sessions_count"]
            }

        membership_plans = [p for p in plans if not p["personal_training"] and (p["plan_for"] is None or p["plan_for"] not in ["couple", "buddy"])]
        pt_plans = [p for p in plans if p["personal_training"] and (p["plan_for"] is None or p["plan_for"] not in ["couple", "buddy"])]
        couple_membership_plans = [p for p in plans if not p["personal_training"] and p["plan_for"] == "couple"]
        couple_pt_plans = [p for p in plans if p["personal_training"] and p["plan_for"] == "couple"]
        buddy_plans = [p for p in plans if not p["personal_training"] and p["plan_for"] == "buddy"]
        buddy_pt_plans = [p for p in plans if p["personal_training"] and p["plan_for"] == "buddy"]

        promo_plans = []

        lowest_membership = get_lowest_per_month(membership_plans, "Membership Plans")
        if lowest_membership:
            promo_plans.append(lowest_membership)

        lowest_pt = get_lowest_per_month(pt_plans, "Personal Training")
        if lowest_pt:
            promo_plans.append(lowest_pt)

        lowest_couple = get_lowest_per_month(couple_membership_plans, "Couple Membership")
        if lowest_couple:
            promo_plans.append(lowest_couple)

        lowest_couple_pt = get_lowest_per_month(couple_pt_plans, "Couple PT")
        if lowest_couple_pt:
            promo_plans.append(lowest_couple_pt)

        lowest_buddy = get_lowest_per_month(buddy_plans, "Buddy")
        if lowest_buddy:
            promo_plans.append(lowest_buddy)

        lowest_buddy_pt = get_lowest_per_month(buddy_pt_plans, "Buddy PT")
        if lowest_buddy_pt:
            promo_plans.append(lowest_buddy_pt)

        # Recalculate ALL promo plans with promo logic: raw × markup ÷ duration → ceil 9 → × duration
        # Collect promo plan_ids to override same plans in plans[] list
        promo_plan_ids = set()
        for promo in promo_plans:
            promo_plan_ids.add(promo["plan_id"])

            # Promo pricing: raw × markup ÷ duration (no bonus, no smart_round)
            raw_per_month = (promo["raw_amount"] * get_markup_multiplier()) / promo["duration"]
            rounded_per_month = round_per_month_price(raw_per_month)
            final_amount = rounded_per_month * promo["duration"]

            original_amount_final = None
            if promo["raw_original_amount"] and promo["raw_original_amount"] > 0:
                raw_original_per_month = (promo["raw_original_amount"] * get_markup_multiplier()) / promo["duration"]
                rounded_original_per_month = round_per_month_price(raw_original_per_month)
                original_amount_final = rounded_original_per_month * promo["duration"]

            promo["amount"] = final_amount
            promo["original_amount"] = original_amount_final
            promo["per_month"] = rounded_per_month

        # Cache all promo plan IDs in Redis for checkout consistency
        if promo_plans:
            asyncio.create_task(set_promo_plan_ids_in_redis(redis, gym_id, promo_plan_ids))

        # Override plans[] entries that match promo plan IDs with promo pricing
        for i, p in enumerate(plans):
            if p["plan_id"] in promo_plan_ids:
                # Find matching promo plan
                matching_promo = next(pr for pr in promo_plans if pr["plan_id"] == p["plan_id"])
                plans[i]["amount"] = matching_promo["amount"]
                plans[i]["original"] = matching_promo["original_amount"]
                plans[i]["per_month"] = matching_promo["per_month"]

        # Remove raw_amount and raw_original_amount from promo_plans (not needed in response)
        for p in promo_plans:
            p.pop("raw_amount", None)
            p.pop("raw_original_amount", None)

        # Remove raw_amount and raw_original_amount from plans[] (not needed in response)
        for p in plans:
            p.pop("raw_amount", None)
            p.pop("raw_original_amount", None)

        # Cache user_saving_price per gym in Redis (keyed by plan_id)
        savings_map = {str(p["plan_id"]): p.get("user_saving_price", 0) for p in plans}
        asyncio.create_task(redis.set(f"gym:{gym_id}:user_savings", json.dumps(savings_map), ex=86400))

        owner = None
        
        
        if gym.owner_id:
            owner_stmt = select(GymOwner).where(GymOwner.owner_id == gym.owner_id).limit(1)
            owner_result = await db.execute(owner_stmt)
            owner = owner_result.scalars().first()
        
        owner_details = None
        
        
        if owner:
            owner_details = {
                "owner_id": owner.owner_id,
                "name": owner.name,
                "email": owner.email,
                "contact_number": owner.contact_number,
                "profile": owner.profile
            }

        cover_pic_stmt = select(GymStudiosPic).where(
            GymStudiosPic.gym_id == gym_id,
            GymStudiosPic.type == 'cover_pic'
        ).limit(1)
        cover_pic_result = await db.execute(cover_pic_stmt)
        cover_pic_record = cover_pic_result.scalars().first()
        cover_pic_url = cover_pic_record.image_url if cover_pic_record else gym.cover_pic

        # Get user offer eligibility and gym offer flags
        user_offer = await get_user_offer_eligibility(db, client_id_int, redis)
        offer_map = await get_gym_offer_flags(db, [gym_id])
        dp_unique_map, session_unique_map = await get_gym_promo_unique_counts(db, [gym_id])

        # Check if user already booked promo prices at this gym
        dp_booked_gyms, session_booked_gyms = await check_user_booked_promo_at_gym(db, client_id_int, [gym_id])

        offer_entry = offer_map.get(gym_id)
        dailypass_offer_enabled = bool(offer_entry and offer_entry.dailypass)
        session_offer_enabled = bool(offer_entry and offer_entry.session)

        # Check if gym has any session settings and get lowest session price
        session_result = await db.execute(
            select(SessionSetting).where(
                SessionSetting.gym_id == gym_id,
                SessionSetting.is_enabled.is_(True),
                SessionSetting.final_price.isnot(None),
                SessionSetting.session_id.notin_(HIDDEN_SESSION_IDS),
            )
        )
        session_settings = session_result.scalars().all()
        has_session = len(session_settings) > 0

        # Calculate actual session price (with 30% markup)
        actual_session_price = None
        if session_settings:
            lowest_session_record = min(session_settings, key=lambda x: x.final_price)
            actual_session_price = round((lowest_session_record.final_price) * get_markup_multiplier())

        # Session offer: gym opted in + user < 3 bookings + gym < 50 promo users + has sessions
        # REQUIRES BOTH: new_offer.session=true AND at least one session_setting
        # NEW: If user already booked ₹99 session at THIS gym, they can't get the offer again
        session_current_count_detail = session_unique_map.get(gym_id, 0)
        session_under_50_detail = session_current_count_detail < 50
        user_already_booked_session_here = gym_id in session_booked_gyms

        if session_offer_enabled and has_session and session_under_50_detail:
            session_remaining_detail = 50 - session_current_count_detail
            print(f"[Gym Details - Session Offer] Gym: {gym.name}, Current Count: {session_current_count_detail}, Remaining: {session_remaining_detail}, User Already Booked: {user_already_booked_session_here}")

        session_offer_active = (
            session_offer_enabled  # new_offer.session=true
            and has_session  # AND has at least one session_setting
            and user_offer.get("session_offer_eligible", False)
            and session_under_50_detail
            and not user_already_booked_session_here  # User hasn't booked ₹99 session at this gym before
        )
        # If offer active → show FIXED ₹99 promo price; otherwise show actual price
        lowest_session_price = 99 if session_offer_active else actual_session_price

        # Fetch daily pass pricing
        daily_pass_discount_price = None
        daily_pass_actual_price = None
        daily_pass_discount = None
        dailypass_offer_active = False

        if gym.dailypass:
            pricing_result = await db.execute(
                select(DailyPassPricing).where(DailyPassPricing.gym_id == str(gym_id)).limit(1)
            )
            pricing_record = pricing_result.scalars().first()
            if pricing_record:
                # Actual price from gym owner (with 30% markup)
                # Use discount_price (not price) to match checkout processor logic
                daily_pass_actual_price = round(((pricing_record.discount_price or 0) / 100) * get_markup_multiplier()) if pricing_record.discount_price else None

                # Special case: If discount_price is exactly ₹49, no markup (matches processor logic)
                if pricing_record.discount_price == 4900:
                    daily_pass_actual_price = 49
                daily_pass_discount = pricing_record.discount_percentage if pricing_record.discount_percentage else None

                # Daily pass offer: gym opted in + user < 3 bookings + gym < 50 promo users
                dp_current_count_detail = dp_unique_map.get(gym_id, 0)
                dp_under_50_detail = dp_current_count_detail < 50

                if dailypass_offer_enabled and dp_under_50_detail:
                    dp_remaining_detail = 50 - dp_current_count_detail
                    print(f"[Gym Details - Dailypass Offer] Gym: {gym.name}, Current Count: {dp_current_count_detail}, Remaining: {dp_remaining_detail}")

                dailypass_offer_active = (
                    dailypass_offer_enabled
                    and user_offer.get("dailypass_offer_eligible", False)
                    and dp_under_50_detail
                )

                # If offer active → show FIXED ₹49 promo price; otherwise show actual price
                if dailypass_offer_active:
                    daily_pass_discount_price = 49  # Fixed promo price
                else:
                    daily_pass_discount_price = daily_pass_actual_price

        # Fetch gym location (latitude and longitude)
        location_result = await db.execute(
            select(GymLocation).where(GymLocation.gym_id == gym_id).limit(1)
        )
        location_record = location_result.scalars().first()
        exact_location = None
        if location_record:
            exact_location = {
                "latitude": location_record.latitude,
                "longitude": location_record.longitude
            }

        gym_details = {
            "gym_id": gym.gym_id,
            "gym_name": gym.name.upper() if gym.name else None,
            "logo": gym.logo,
            "cover_pic": cover_pic_url,
            "location": gym.location,
            "max_clients": gym.max_clients,
            "address": {
                "door_no":gym.door_no,
                "building":gym.building,
                "street": gym.street,
                "area": gym.area,
                "city": gym.city,
                "state": gym.state,
                "pincode": gym.pincode,
            },
            "contact_number": gym.contact_number,
            "services": gym.services,
            "operating_hours": gym.operating_hours,
            "gym_timings": gym.gym_timings,
            "dailypass": gym.dailypass,
            "fittbot_verified": gym.fittbot_verified,
            "subscription_start_date": gym.subscription_start_date.isoformat() if gym.subscription_start_date else None,
            "subscription_end_date": gym.subscription_end_date.isoformat() if gym.subscription_end_date else None,
            "photos": photos,
            "trainers": trainer_details,
            "plans": plans,
            "promo_plans": promo_plans,
            "owner": owner_details,
            "daily_pass_discount_price": daily_pass_discount_price,
            "daily_pass_actual_price": daily_pass_actual_price,
            "daily_pass_discount": daily_pass_discount,
            "dailypass_offer_active": dailypass_offer_active,
            "session": has_session,
            "lowest_session_price": lowest_session_price,
            "session_offer_active": session_offer_active,
            "exact_location": exact_location
        }

        # Track gym page view (non-blocking, fire-and-forget)
        # Only tracks "gym_viewed" — product interest is determined by checkout events
        if client_id_int:
            from app.services.activity_tracker import track_event
            await track_event(client_id_int, "gym_viewed", gym_id=gym_id, source="gym_studios")

        return {
            "status": 200,
            "data": gym_details,
            "dailypass_offer_eligible": user_offer.get("dailypass_offer_eligible", False),
            "session_offer_eligible": user_offer.get("session_offer_eligible", False),
            "dailypass_count": user_offer.get("dailypass_count", 0),
            "session_count": user_offer.get("session_count", 0),
        }

    except FittbotHTTPException:
        raise
    except Exception as e:
        await db.rollback()
        raise FittbotHTTPException(
            status_code=500,
            detail="Unable to retrieve gym details",
            error_code="GYM_DETAILS_FETCH_ERROR",
            log_data={"exc": repr(e)},
        )


class TrackProductViewRequest(BaseModel):
    gym_id: int
    product: str  # "dailypass" or "session"


@router.post("/track_view")
async def track_product_view(request: Request, body: TrackProductViewRequest):
    """Track when user taps on dailypass or session tab on a gym page."""
    client_id = getattr(request.state, "user", None)
    if not client_id:
        return {"status": 200, "message": "ok"}

    event_map = {
        "dailypass": "dailypass_viewed",
        "session": "session_viewed",
        "membership": "membership_viewed",
    }
    event_type = event_map.get(body.product)
    if not event_type:
        return {"status": 400, "message": "product must be dailypass, session, or membership"}

    from app.services.activity_tracker import track_event
    await track_event(
        int(client_id),
        event_type,
        gym_id=body.gym_id,
        source="gym_studios",
    )

    return {"status": 200, "message": "ok"}


class RewardCalculationRequest(BaseModel):
    client_id: int
    amount: float
    gym_id: Optional[int] = None  # Optional gym_id for potential future use in reward logic
    plan_id: Optional[int] = None  # Optional plan_id for potential future use in reward logic

@router.post("/calculate_rewards")
async def calculate_reward(request: RewardCalculationRequest, db: AsyncSession = Depends(get_async_db)):

    try:
        client_id = request.client_id
        amount = request.amount
        ten_percent = amount * 0.10
        capped_reward = min(ten_percent, 100)


        fittbot_cash_stmt = select(ReferralFittbotCash).where(ReferralFittbotCash.client_id == client_id)
        fittbot_cash_result = await db.execute(fittbot_cash_stmt)
        fittbot_cash_entry = fittbot_cash_result.scalars().first()

        available_fittbot_cash = fittbot_cash_entry.fittbot_cash if fittbot_cash_entry else 0

        if available_fittbot_cash >= capped_reward:
            
            reward_amount = round(capped_reward)
        else:
           
            reward_amount = round(available_fittbot_cash)

        

        # Track membership view if gym_id and plan_id are present
        if request.gym_id and request.plan_id:
            from app.services.activity_tracker import track_event
            await track_event(
                client_id, "membership_viewed",
                gym_id=request.gym_id,
                product_type="membership",
                source="calculate_rewards",
            )

        return {
            "status": 200,
             "rewards": reward_amount
            }


    except FittbotHTTPException:
        raise
    except Exception as e:
        await db.rollback()
        raise FittbotHTTPException(
            status_code=500,
            detail="Unable to calculate reward",
            error_code="REWARD_CALCULATION_ERROR",
            log_data={"exc": repr(e), "client_id": request.client_id, "amount": request.amount},
        )

class GymStudiosRequestPayload(BaseModel):
    lat: Optional[float] = None
    lng: Optional[float] = None
    area: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    pincode: Optional[str] = None

@router.post("/save_request")
async def save_gym_studios_request(
    request: Request,
    payload: GymStudiosRequestPayload,
    db: AsyncSession = Depends(get_async_db)
):
    try:
        client_id = getattr(request.state, 'user', None)
        if not client_id:
            raise FittbotHTTPException(
                status_code=401,
                detail="Unauthorized",
                error_code="UNAUTHORIZED",
            )

        new_record = GymStudiosRequest(
            client_id=client_id,
            lat=payload.lat,
            lng=payload.lng,
            area=payload.area,
            city=payload.city,
            state=payload.state,
            pincode=payload.pincode
        )
        db.add(new_record)
        await db.commit()

        return {
            "status": 200,
            "message": "Request saved successfully"
        }

    except FittbotHTTPException:
        raise
    except Exception as e:
        await db.rollback()
        raise FittbotHTTPException(
            status_code=500,
            detail="Unable to save request",
            error_code="GYM_STUDIOS_REQUEST_SAVE_ERROR",
            log_data={"exc": repr(e)},
        )


@router.get("/most_common")
async def get_most_common_locations(
    filter: str,
    db: AsyncSession = Depends(get_async_db),
    state: Optional[str] = None,
    city: Optional[str] = None,
    area: Optional[str] = None,
    pincode: Optional[str] = None,
    search: Optional[str] = None,
):

    try:
        filter_field = (filter or "").strip().lower()
        column_map = {
            "state": Gym.state,
            "city": Gym.city,
            "area": Gym.area,
            "pincode": Gym.pincode,
        }

        if filter_field not in column_map:
            raise FittbotHTTPException(
                status_code=400,
                detail="Invalid filter. Use one of: state, city, area, pincode.",
                error_code="INVALID_LOCATION_FILTER",
            )

        target_column = column_map[filter_field]

        # Build filters; allow narrowing with any combination of location fields.
        conditions = [Gym.fittbot_verified.is_(True), target_column.isnot(None)]
        provided_filters = {
            "state": state,
            "city": city,
            "area": area,
            "pincode": pincode,
        }

        def normalize_column(col, key):
            # Trim whitespace; lowercase for text fields to avoid duplicates like "Bangalore " vs "bangalore".
            if key == "pincode":
                return func.trim(col)
            return func.lower(func.trim(col))

        normalized_target = normalize_column(target_column, filter_field)

        for key, value in provided_filters.items():
            if value is None:
                continue
            column = normalize_column(column_map[key], key)
            trimmed = value.strip()
            conditions.append(column == (trimmed.lower() if key != "pincode" else trimmed))

        if search:
            search_term = f"%{search.strip().lower()}%"
            conditions.append(normalized_target.like(search_term))

        count_column = func.count(Gym.gym_id).label("count")
        stmt = (
            select(normalized_target.label("value"), count_column)
            .where(*conditions)
            .group_by(normalized_target)
            .order_by(count_column.desc())
            .limit(10)
        )

        result = await db.execute(stmt)
        rows = result.all()
        # Only return the values; counts are used solely for ordering. Dedup happens via group by normalized values.
        data = []
        for row in rows:
            value = row.value
            if isinstance(value, str):
                value = value.title()
            data.append(value)

        return {
            "status": 200,
            "data": data,
        }

    except FittbotHTTPException:
        raise
    except Exception as e:
        await db.rollback()
        raise FittbotHTTPException(
            status_code=500,
            detail="Unable to fetch most common locations",
            error_code="MOST_COMMON_LOCATION_ERROR",
            log_data={"exc": repr(e)},
        )
