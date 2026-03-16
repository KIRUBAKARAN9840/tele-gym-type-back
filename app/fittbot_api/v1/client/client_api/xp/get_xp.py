import json
import random
from fastapi import APIRouter, Depends
from sqlalchemy import asc, desc, select
from sqlalchemy.ext.asyncio import AsyncSession
from redis.asyncio import Redis
from app.models.async_database import get_async_db
from app.utils.redis_config import get_redis
from app.utils.logging_utils import FittbotHTTPException
from app.models.fittbot_models import Client, LeaderboardOverall, RewardBadge, Gym, ClientWeightSelection, FittbotGymMembership

router = APIRouter(prefix="/xp", tags=["Client XP & Badges"])

TAG_LINES: list[str] = [
    "Push past limits—your future self will thank you!",
    "Every rep fuels progress—stay relentless and finish strong!",
    "Sweat now, shine later—victory loves disciplined hearts!",
    "Consistency crushes excuses—show up and conquer goals today!",
    "Your grind inspires others—keep pushing and lead greatness!",
    "Transform sweat into strength—momentum builds champions!",
    "No shortcuts to power—earn progress rep by rep!",
    "Commit to today's workout—tomorrow's victories depend on it!",
    "Rise early, train hard—success waits for persistent warriors!",
    "Fuel determination with sweat—each session sculpts confidence!",
    "Beat yesterday's limits—progress is built on daily commitment!",
    "Strength grows silently—show up and let actions roar!",
]


async def get_client_registration_steps_async(db: AsyncSession, client_id: int) -> dict:

    stmt = select(Client).where(Client.client_id == client_id)
    result = await db.execute(stmt)
    client = result.scalars().first()

    if not client:
        return {
            "dob": False,
            "goal": False,
            "height": False,
            "weight": False,
            "body_shape": False,
            "lifestyle": False,
            "registration_complete": False,
        }

    # Check each step - handle None values safely
    dob_completed = client.dob is not None
    goal_completed = bool(client.goals and str(client.goals).strip())
    height_completed = client.height is not None
    weight_completed = client.weight is not None and client.bmi is not None

    # Check body shape using ClientWeightSelection
    stmt = select(ClientWeightSelection).where(ClientWeightSelection.client_id == str(client_id))
    result = await db.execute(stmt)
    weight_selection = result.scalars().first()
    body_shape_completed = weight_selection is not None

    lifestyle_completed = bool(client.lifestyle and str(client.lifestyle).strip())

    return {
        "dob": dob_completed,
        "goal": goal_completed,
        "height": height_completed,
        "weight": weight_completed,
        "body_shape": body_shape_completed,
        "lifestyle": lifestyle_completed,
        "registration_complete": not client.incomplete if client.incomplete is not None else False,
    }


async def get_client_tier_async(db: AsyncSession, client_id: int) -> dict:

    from app.fittbot_api.v1.payments.models.subscriptions import Subscription
    from datetime import datetime, date

    current_time = datetime.now()
    today = date.today()

    # Auto-expire subscriptions that have passed their active_until date but still show as active
    expired_stmt = select(Subscription).where(
        Subscription.customer_id == str(client_id),
        Subscription.active_until < current_time,
        Subscription.status == "active"
    )
    expired_result = await db.execute(expired_stmt)
    expired_subscriptions = expired_result.scalars().all()
    for sub in expired_subscriptions:
        sub.status = "expired"
    if expired_subscriptions:
        await db.commit()

    has_subscription = False
    has_gym_membership = False
    tier = "freemium"

    stmt = select(Subscription).where(
        Subscription.customer_id == str(client_id),
        Subscription.active_until > current_time
    ).order_by(Subscription.active_until.desc())

    result = await db.execute(stmt)
    subscription = result.scalars().first()

    if subscription:
        has_subscription = True
        tier = "premium"

    stmt = select(FittbotGymMembership).where(
        FittbotGymMembership.client_id == str(client_id),
        FittbotGymMembership.status == "active",
        FittbotGymMembership.expires_at > today
    ).order_by(desc(FittbotGymMembership.id))

    result = await db.execute(stmt)
    gym_membership = result.scalars().first()

    if gym_membership:
        has_gym_membership = True

    if has_subscription and has_gym_membership:
        tier = "premium_gym"

    elif has_subscription:
        tier = "premium"

    else:
        stmt = select(Client).where(Client.client_id == client_id)
        result = await db.execute(stmt)
        client = result.scalars().first()

        if client and client.gym_id and has_gym_membership:
            tier = "freemium_gym"

    return tier

@router.get("/get")
async def get_client_xp(
    client_id: int,
    db: AsyncSession = Depends(get_async_db),
    redis: Redis = Depends(get_redis),
):
    try:
        # Get client profile
        stmt = select(Client).where(Client.client_id == client_id)
        result = await db.execute(stmt)
        profile = result.scalars().first()

        # Get XP row
        stmt = select(LeaderboardOverall).where(LeaderboardOverall.client_id == client_id)
        result = await db.execute(stmt)
        xp_row = result.scalars().first()

        #tag_line = random.choice(TAG_LINES)
        tier = await get_client_tier_async(db, client_id)


        rediskey = f"logo:{profile.gym_id}:profileData"
        cached_gym = await redis.get(rediskey)
        gym_data = {"name": "", "logo": ""}

        if cached_gym:
            gym_data = json.loads(cached_gym)
        else:
            stmt = select(Gym).where(Gym.gym_id == profile.gym_id)
            result = await db.execute(stmt)
            gym = result.scalars().first()
            gym_data["logo"] = gym.logo if getattr(gym, "logo", None) else ""
            gym_data["name"] = gym.name if getattr(gym, "name", None) else ""

            await redis.set(rediskey, json.dumps(gym_data), ex=8600)

        # Get registration steps and usertype - check Redis first
        registration_key = f"registration_steps:{client_id}"
        cached_registration = await redis.get(registration_key)

        if cached_registration:
            registration_steps = json.loads(cached_registration)
            usertype = "full_user"
         
        else:
          
            registration_steps = await get_client_registration_steps_async(db, client_id)
            all_steps_completed = all([
                registration_steps["dob"],
                registration_steps["goal"],
                registration_steps["height"],
                registration_steps["weight"],
                registration_steps["body_shape"],
                registration_steps["lifestyle"],
            ])

            usertype = "full_user" if all_steps_completed else "guest"

            if usertype == "full_user":
                await redis.set(registration_key, json.dumps(registration_steps))

        if not xp_row:
            return {
                "status": 200,
                "data": 0,
                "profile": profile.profile,
                "name": profile.name,
                "gym": gym_data,
                "badge": "https://fittbot-uploads.s3.ap-south-2.amazonaws.com/Badges/BEGINNER.png",
                "progress": 0,
                "mobile_number": profile.contact,
                "tier": tier,
                "registration_steps": registration_steps,
                "usertype": usertype
            }

        # overall_xp = xp_row.xp


        # stmt = select(RewardBadge).where(
        #     RewardBadge.min_points <= overall_xp,
        #     RewardBadge.max_points >= overall_xp,
        # ).order_by(asc(RewardBadge.min_points))
        # result = await db.execute(stmt)
        # current_row = result.scalars().first()

        # if not current_row:
        #     return {
        #         "status": 200,
        #         "data": {"client_id": client_id, "gym_id": profile.gym_id, "xp": overall_xp},
        #         "profile": profile.profile,
        #         "name": profile.name,
        #         "badge": None,
        #         "gym": gym_data,
        #         "progress": 0,
        #         "mobile_number": profile.contact,
        #         "registration_steps": registration_steps,
        #         "usertype": usertype
        #     }

        # stmt = select(RewardBadge).where(
        #     RewardBadge.badge == current_row.badge
        # ).order_by(asc(RewardBadge.min_points))
        # result = await db.execute(stmt)
        # badge_rows = result.scalars().all()

        # start_xp = badge_rows[0].min_points
        # end_xp = badge_rows[-1].max_points

        # if end_xp > start_xp:
        #     progress = (overall_xp - start_xp) / (end_xp - start_xp)
        #     progress = max(0.0, min(progress, 1.0))
        # else:
        #     progress = 1.0

        return {
            "status": 200,
            # "data": {"client_id": client_id, "gym_id": profile.gym_id, "xp": overall_xp},
            "data": {"client_id": client_id, "gym_id": profile.gym_id},
            "profile": profile.profile,
            "name": profile.name,
            "mobile_number": profile.contact,
            # "badge": current_row.image_url,
            "gym": gym_data,
            # "progress": round(progress, 4),
            # "start_xp": start_xp,
            # "end_xp": end_xp,
            "tier": tier,
            "registration_steps": registration_steps,
            "usertype": usertype}

    except FittbotHTTPException:
        raise

    except Exception as e:
        raise FittbotHTTPException(
            status_code=500,
            detail=f"An unexpected error occurred {e}",
            error_code="GET_XP_ERROR",
            log_data={"client_id": client_id, "error": str(e)},
        )
