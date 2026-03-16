# app/routers/my_progress_router.py
 
from fastapi import APIRouter, Depends, Request
from sqlalchemy import and_, desc, or_, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql import extract, func
from typing import Optional, List
from pydantic import BaseModel
from datetime import datetime, timedelta, date
import json
from app.models.async_database import get_async_db
from app.models.fittbot_models import (
    CharactersCombination,
    CharactersCombinationOld,
    ClientCharacter,
    ClientWeightSelection,
    LeaderboardOverall,
    LeaderboardMonthly,
    HomePoster,
    ManualPoster,
    WeightJourney,
    ClientWeightData,
    ClientGeneralAnalysis,
    RewardGym,
    ActualWorkout,
    Client,
    Attendance,
    ClientTarget,
    ClientActual,
    ActualDiet,
    FreeTrial,
    RewardInterest,
    FittbotGymMembership,
)
from app.models.nutrition_models import NutritionEligibility, NutritionBooking
from app.fittbot_api.v1.payments.models.subscriptions import Subscription
from app.utils.redis_config import get_redis
from redis.asyncio import Redis
from app.utils.logging_utils import FittbotHTTPException
 
router = APIRouter(prefix="/my_progress", tags=["my_progress"])



async def get_client_tier_async(db: AsyncSession, client_id: int) -> str:

    current_time = datetime.now()
    today = date.today()

    # Auto-expire subscriptions that have passed their active_until date but still show as active
    expired_result = await db.execute(
        select(Subscription)
        .where(
            Subscription.customer_id == str(client_id),
            Subscription.active_until < current_time,
            Subscription.status == "active"
        )
    )
    expired_subscriptions = expired_result.scalars().all()
    for sub in expired_subscriptions:
        sub.status = "expired"
    if expired_subscriptions:
        await db.commit()

    has_subscription = False
    has_gym_membership = False
    tier = "freemium"

    subscription_result = await db.execute(
        select(Subscription)
        .where(
            Subscription.customer_id == str(client_id),
            Subscription.active_until > current_time
        )
        .order_by(Subscription.active_until.desc())
    )
    subscription = subscription_result.scalars().first()

    if subscription:
        has_subscription = True
        tier = "premium"

    gym_membership_result = await db.execute(
        select(FittbotGymMembership)
        .where(
            FittbotGymMembership.client_id == str(client_id),
            FittbotGymMembership.status == "active",
            FittbotGymMembership.expires_at > today
        )
        .order_by(desc(FittbotGymMembership.id))
    )
    gym_membership = gym_membership_result.scalars().first()

    if gym_membership:
        has_gym_membership = True

    if has_subscription and has_gym_membership:
        tier = "premium_gym"

    elif has_subscription:
        tier = "premium"

    else:
        client_result = await db.execute(
            select(Client).where(Client.client_id == client_id)
        )
        client = client_result.scalars().first()

        if client and client.gym_id and has_gym_membership:
            tier = "freemium_gym"

    return tier


async def get_leaderboard_with_top(session: AsyncSession, gym_id: int, client_id: int, top_n: int = 3):
    today = date.today()

    # Fetch all clients for the gym using gym_id from Client table
    result = await session.execute(
        select(Client).where(Client.gym_id == gym_id)
    )
    clients = result.scalars().all() or []

    # Filter out None clients and get valid client_ids
    valid_clients = [c for c in clients if c is not None and hasattr(c, 'client_id') and c.client_id is not None]
    client_ids = [client.client_id for client in valid_clients]

    # If no clients found, return empty data
    if not client_ids:
        return {"total": 0, "rank": 0, "top_performers": None}

    # Get total count using client_ids
    total = await session.scalar(
        select(func.count(LeaderboardMonthly.id)).where(
            LeaderboardMonthly.client_id.in_(client_ids),
            extract("year", LeaderboardMonthly.month) == today.year,
            extract("month", LeaderboardMonthly.month) == today.month,
        )
    )

    # Get current client's XP using client_id
    client_xp = await session.scalar(
        select(LeaderboardMonthly.xp).where(
            LeaderboardMonthly.client_id == client_id,
            extract("year", LeaderboardMonthly.month) == today.year,
            extract("month", LeaderboardMonthly.month) == today.month,
        )
    )

    if client_xp is None:
        rank = 0
    else:
        # Calculate rank among gym clients
        higher_count = await session.scalar(
            select(func.count(LeaderboardMonthly.id)).where(
                LeaderboardMonthly.client_id.in_(client_ids),
                extract("year", LeaderboardMonthly.month) == today.year,
                extract("month", LeaderboardMonthly.month) == today.month,
                LeaderboardMonthly.xp > client_xp,
            )
        )
        rank = higher_count + 1

    # Get top performers using client_ids
    rows_result = await session.execute(
        select(
            Client.client_id,
            Client.name,
            Client.profile.label("dp_url"),
            LeaderboardMonthly.xp,
        )
        .join(LeaderboardMonthly, LeaderboardMonthly.client_id == Client.client_id)
        .where(
            LeaderboardMonthly.client_id.in_(client_ids),
            extract("year", LeaderboardMonthly.month) == today.year,
            extract("month", LeaderboardMonthly.month) == today.month,
        )
        .order_by(desc(LeaderboardMonthly.xp))
        .limit(top_n)
    )
    rows = rows_result.all()

    if rows:
        top_performers = [
            {"name": name, "dp_url": dp_url, "position": idx, "points": xp}
            for idx, (_, name, dp_url, xp) in enumerate(rows, start=1)
        ]
    else:
        top_performers = None

    return {"total": total, "rank": rank, "top_performers": top_performers}
 
 
async def get_next_reward_info(db: AsyncSession, client_id: int, gym_id: int):
    overall_result = await db.execute(
        select(LeaderboardOverall).where(LeaderboardOverall.client_id == client_id)
    )
    overall = overall_result.scalars().first()
    if not overall:
        return {
            "client_id": client_id,
            "error": f"No overall leaderboard record for client {client_id}",
        }
 
    client_xp = overall.xp
 
    tiers_result = await db.execute(
        select(RewardGym).where(RewardGym.gym_id == gym_id).order_by(RewardGym.xp)
    )
    tiers = tiers_result.scalars().all()
 
    if not tiers:
        return {
            "client_xp": client_xp,
            "next_reward": None,
            "xp_to_next": None,
            "message": "No rewards configured for this gym.",
        }
 
    for tier in tiers:
        if tier.xp > client_xp:
            xp_to_next = tier.xp - client_xp
            return {
                "client_xp": client_xp,
                "next_reward": tier.xp,
                "xp_to_next": xp_to_next,
                "message": f"{xp_to_next} XP to next reward.",
            }
 
    highest = tiers[-1]
    return {
        "client_xp": client_xp,
        "next_reward": None,
        "xp_to_next": 0,
        "message": "Congratulations! You have unlocked all rewards.",
    }
 
 
async def get_manual_posters_info(db: AsyncSession) -> dict:
    """
    Check if manual posters are enabled and return the list.
    When use_manual_posters is True, frontend should show these instead of conditional posters.

    The urls column is a JSON array of URL strings: ["https://...", "https://...", ...]
    """
    active_result = await db.execute(
        select(ManualPoster).where(ManualPoster.show.is_(True))
    )
    active_record = active_result.scalars().first()

    if active_record and active_record.urls:
        posters = active_record.urls if isinstance(active_record.urls, list) else []
        if posters:
            return {
                "use_manual_posters": True,
                "manual_posters": posters
            }

    return {
        "use_manual_posters": False,
        "manual_posters": []
    }


async def get_client_registration_steps(db: AsyncSession, client_id: int) -> dict:
    """
    Get registration steps completion status for a client.
    Similar to owner registration steps in all.py.
    """
    client_result = await db.execute(
        select(Client).where(Client.client_id == client_id)
    )
    client = client_result.scalars().first()
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
    weight_selection_result = await db.execute(
        select(ClientWeightSelection).where(ClientWeightSelection.client_id == str(client_id))
    )
    weight_selection = weight_selection_result.scalars().first()
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


async def get_nutrition_eligibility_info(db: AsyncSession, client_id: int) -> dict:
    """
    Check if client has available nutrition consultation sessions.
    Returns info for showing nutrition poster on home screen.
    """
    today = date.today()

    # Get active eligibility with remaining sessions
    eligibility_result = await db.execute(
        select(NutritionEligibility)
        .where(
            NutritionEligibility.client_id == client_id,
            NutritionEligibility.remaining_sessions > 0,
        )
        .order_by(NutritionEligibility.created_at.asc())
    )
    eligibility = eligibility_result.scalars().first()

    if not eligibility:
        return {
            "eligibility_id":None,
            "show_nutrition_poster": False,
            "nutrition_sessions_available": 0,
            "nutrition_booking_status": None,
            "nutrition_booking_date": None,
            "nutrition_needs_reschedule": False,
        }

    # Check for pending/booked sessions
    pending_booking_result = await db.execute(
        select(NutritionBooking)
        .where(
            NutritionBooking.client_id == client_id,
            NutritionBooking.eligibility_id == eligibility.id,
            NutritionBooking.status.in_(["booked", "pending"]),
            NutritionBooking.booking_date >= today,
        )
        .order_by(NutritionBooking.booking_date.asc())
    )
    pending_booking = pending_booking_result.scalars().first()

    # Check if nutritionist requested reschedule
    reschedule_result = await db.execute(
        select(NutritionBooking)
        .where(
            NutritionBooking.client_id == client_id,
            NutritionBooking.eligibility_id == eligibility.id,
            NutritionBooking.status == "rescheduled",
            NutritionBooking.reschedule_requested_by == "nutritionist",
        )
    )
    reschedule_booking = reschedule_result.scalars().first()

    needs_reschedule = reschedule_booking is not None

    # Show poster if: has remaining sessions (show poster even when booking exists)
    # If client has booked, they can still see the poster with booking status
    show_poster = eligibility.remaining_sessions > 0

    # Format slot time if booking exists
    slot_time = None
    if pending_booking:
        start_str = pending_booking.start_time.strftime("%I:%M %p") if pending_booking.start_time else None
        end_str = pending_booking.end_time.strftime("%I:%M %p") if pending_booking.end_time else None
        if start_str and end_str:
            slot_time = f"{start_str} - {end_str}"

    return {
        "eligibility_id": eligibility.id,
        "show_nutrition_poster": show_poster,
        "nutrition_sessions_available": eligibility.remaining_sessions,
        "nutrition_booking_id": pending_booking.id if pending_booking else None,  # Booking ID for /join API
        "nutrition_booking_status": pending_booking.status if pending_booking else None,
        "nutrition_booking_date": pending_booking.booking_date.isoformat() if pending_booking else None,
        "nutrition_slot_time": slot_time,  # e.g., "10:00 AM - 10:30 AM"
        "nutrition_needs_reschedule": needs_reschedule,
        "nutrition_booked": pending_booking is None,  # True if NOT booked (can book), False if already booked
    }


async def get_workout_data(db: AsyncSession, client_id: int, target_date: Optional[date] = None):
    reference_date = target_date or date.today()
    records_result = await db.execute(
        select(ActualWorkout).where(
            ActualWorkout.client_id == client_id, ActualWorkout.date == reference_date
        )
    )
    records = records_result.scalars().all()

    total_volume = 0.0
    total_calories = 0.0
 
    if records:
        for rec in records:
            details = rec.workout_details or []
            for group in details:
                for exercises in group.values():  
                    for ex in exercises:
                        for s in ex.get("sets", []):
                            reps = s.get("reps", 0) or 0
                            weight = s.get("weight", 0) or 0
                            cals = s.get("calories", 0) or 0
                            total_volume += reps * weight
                            total_calories += cals
 
    attendance_result = await db.execute(
        select(Attendance).where(Attendance.client_id == client_id, Attendance.date == reference_date)
    )
    attendance = attendance_result.scalars().first()

    attendance_dict = {
        "in_time": None,
        "out_time": None,
        "in_time_2": None,
        "out_time_2": None,
        "in_time_3": None,
        "out_time_3": None,
    }
    total_minutes = 0
 
    if attendance:
        for suf in ["", "_2", "_3"]:
            in_attr = getattr(attendance, f"in_time{suf}")
            out_attr = getattr(attendance, f"out_time{suf}")
            in_key = f"in_time{suf}" if suf else "in_time"
            out_key = f"out_time{suf}" if suf else "out_time"
 
            attendance_dict[in_key] = in_attr.strftime("%I:%M %p") if in_attr else None
            attendance_dict[out_key] = out_attr.strftime("%I:%M %p") if out_attr else None

            if in_attr and out_attr:
                diff = datetime.combine(reference_date, out_attr) - datetime.combine(
                    reference_date, in_attr
                )
                total_minutes += diff.total_seconds() // 60

    if total_minutes:
        if total_minutes < 60:
            time_spent = f"{int(total_minutes)} mins"
        else:
            hrs, mins = divmod(int(total_minutes), 60)
            time_spent = f"{hrs}h {mins}mins"
    else:
        time_spent = None

    window = [reference_date - timedelta(days=i) for i in range(7)]
    present_dates_result = await db.execute(
        select(Attendance.date)
        .where(Attendance.client_id == client_id, Attendance.date.in_(window))
        .distinct()
    )
    present_dates = {rec_date for (rec_date,) in present_dates_result.all()}

    attendance = []
    for d in sorted(window):
        if d == reference_date:
            status = "green" if d in present_dates else "grey"
        else:
            status = "green" if d in present_dates else "red"
 
        attendance.append({"date": d, "day_initial": d.strftime("%A")[0], "status": status})
 
    return {
        "total_volume": round(total_volume, 2),
        "total_calories": round(total_calories, 2),
        "total_time": time_spent,
        "attendance": attendance,
    }
 
 
def get_difference(height, goals, actual_weight):
    if not height or not goals or actual_weight is None:
        return 0
 
    try:
        height = float(height)
        actual_weight = float(actual_weight)
        ideal_weight_kg = 23 * (height / 100) ** 2
 
        if goals.lower() == "weight_gain":
            difference = ideal_weight_kg - actual_weight
        elif goals.lower() == "weight_loss":
            difference = actual_weight - ideal_weight_kg
        else:
            difference = 0
        return difference
    except (ValueError, TypeError, ZeroDivisionError):
        return 0
 
 
def get_progress(goals, actual_weight, target_weight, start_weight):
    try:
        if actual_weight is not None and target_weight is not None and start_weight is not None:
            actual_weight = float(actual_weight)
            target_weight = float(target_weight)
            start_weight = float(start_weight)
 
            if not goals:
                return 0
 
            if goals.lower() == "weight_gain":
                if actual_weight < start_weight:
                    progress = 0
                else:
                    progress = (
                        ((actual_weight - start_weight) / (target_weight - start_weight)) * 100
                        if (target_weight - start_weight) > 0
                        else 0
                    )
            elif goals.lower() == "weight_loss":
                if actual_weight > start_weight:
                    progress = 0
                else:
                    progress = (
                        ((start_weight - actual_weight) / (start_weight - target_weight)) * 100
                        if (start_weight - target_weight) > 0
                        else 0
                    )
            else:
                progress = 0
        else:
            progress = 0
        return progress
    except (ValueError, TypeError, ZeroDivisionError):
        return 0
 
 
 
@router.get("/data")
async def get_clients_home(
    request: Request,
    client_id: int,
    gym_id: Optional[int] = None,
    db: AsyncSession = Depends(get_async_db),
    redis: Redis = Depends(get_redis),
):
    try:


        tier = await get_client_tier_async(db, client_id)

        client_result = await db.execute(
            select(Client).where(Client.client_id == client_id)
        )
        client = client_result.scalars().first()
        if not client:
            return {"status": 404, "message": "Client not found"}

        # Check if client is eligible for free trial
        # Eligible (True) only if: no entry in free_trial table AND no entry in subscriptions table
        free_trial_result = await db.execute(
            select(FreeTrial).where(FreeTrial.client_id == client_id)
        )
        free_trial_entry = free_trial_result.scalars().first()
        subscription_result = await db.execute(
            select(Subscription).where(Subscription.customer_id == str(client_id))
        )
        subscription_entry = subscription_result.scalars().first()

        if subscription_entry:
            remaining_days = None
            free_trial_active = False
        
        if subscription_entry and subscription_entry.provider=="free_trial":
            subscription_entry=None
            


        
        # True only if both tables have no entry
        has_free_trial = (free_trial_entry is None) and (subscription_entry is None)
        #has_free_trial = free_trial_entry is None
        remaining_days = None

        # Check if free trial is currently active
        free_trial_active = False

        # If free trial entry exists with active status, check expiry
        if free_trial_entry and free_trial_entry.status == "active":
            # Find subscription with provider = "free_trial"
            subscription_result = await db.execute(
                select(Subscription).where(
                    Subscription.customer_id == str(client_id),
                    Subscription.provider == "free_trial"
                )
            )
            subscription = subscription_result.scalars().first()

            if subscription and subscription.active_until:
                current_datetime = datetime.now(subscription.active_until.tzinfo) if subscription.active_until.tzinfo else datetime.now()

                # Check if expired
                if current_datetime > subscription.active_until:
                    # Update free_trial status to expired
                    free_trial_entry.status = "expired"
                    # Update subscription status to expired
                    subscription.status = "expired"
                    await db.commit()
                    has_free_trial = False
                    remaining_days = None
                    free_trial_active = False
                else:
          
                    time_diff = subscription.active_until - current_datetime
                    remaining_days = time_diff.days
                    remaining_days=remaining_days+1
                    has_free_trial = False
                    free_trial_active = True


        food_scan_available = None
        if tier in ["freemium", "freemium_gym"]:
            
            today_date = date.today().strftime("%Y-%m-%d")
            food_scan_key = f"{client_id}:food_scan:{today_date}"
            # Check if key exists - if not, scan is available
            scan_used = await redis.get(food_scan_key)
            food_scan_available = scan_used is None  # True if key doesn't exist, False if it does

           

        gym_key = str(gym_id) if gym_id is not None else "global"

        difference = None
        client_status_key = f"{client_id}:{gym_key}:status"
 
        actual_weight = await redis.hget(client_status_key, "actual_weight")
        target_weight = await redis.hget(client_status_key, "target_weight")
        start_weight = await redis.hget(client_status_key, "start_weight")
        client_status = await redis.hget(client_status_key, "status")
        gender = await redis.hget(client_status_key, "gender")
        bmi = await redis.hget(client_status_key, "bmi")
        difference = await redis.hget(client_status_key, "difference")
        goals = await redis.hget(client_status_key, "goals")
        height = await redis.hget(client_status_key, "height")
        age = await redis.hget(client_status_key, "age")
        progress = await redis.hget(client_status_key, "progress")
        lifestyle = await redis.hget(client_status_key, "lifestyle")
        client_name = await redis.hget(client_status_key, "name") or ""

        if client_status is None:
            target_result = await db.execute(
                select(ClientTarget).where(ClientTarget.client_id == client_id)
            )
            target_data = target_result.scalars().first()

            client_status = client.status
            bmi = client.bmi if client.bmi is not None else ""
            goals = client.goals if client.goals is not None else ""
            actual_weight = client.weight if client.weight is not None else None
            target_weight = target_data.weight if target_data and target_data.weight is not None else 0
            start_weight = target_data.start_weight if target_data and target_data.start_weight is not None else 0
            gender = client.gender if client.gender else None
            bmi = client.bmi if client.bmi else None
            height = client.height if client.height else None
 
            progress = get_progress(goals, actual_weight, target_weight, start_weight)
            progress = min(progress, 100)
 
            client_name = client.name if client.name is not None else ""
 
            difference = get_difference(height, goals, actual_weight)
 
            age = client.age if client.age is not None else ""
            lifestyle = client.lifestyle if client.lifestyle is not None else ""
        
        else:
         
            try:
                actual_weight = float(actual_weight) if actual_weight and actual_weight != "None" else None
                target_weight = float(target_weight) if target_weight and target_weight != "None" else 0
                start_weight = float(start_weight) if start_weight and start_weight != "None" else 0
                height = float(height) if height and height != "None" else None
                age = int(age) if age and age != "None" and str(age).isdigit() else ""
                difference = float(difference) if difference and difference != "None" else 0
                progress = float(progress) if progress and progress != "None" else 0
            
            except (ValueError, TypeError):
       
                target_result = await db.execute(
                    select(ClientTarget).where(ClientTarget.client_id == client_id)
                )
                target_data = target_result.scalars().first()

                actual_weight = client.weight if client.weight is not None else None
                target_weight = target_data.weight if target_data and target_data.weight is not None else 0
                start_weight = target_data.start_weight if target_data and target_data.start_weight is not None else 0
                height = client.height if client.height else None
                age = client.age if client.age is not None else ""
                difference = get_difference(height, goals, actual_weight)
                progress = get_progress(goals, actual_weight, target_weight, start_weight)
                progress = min(progress, 100)
 
            await redis.hset(
                client_status_key,
                mapping={
                    "status": client_status,
                    "gender": gender,
                    "progress": progress,
                    "bmi": bmi,
                    "goals": goals,
                    "height": height,
                    "actual_weight": actual_weight,
                    "target_weight": target_weight,
                    "name": client_name,
                    "start_weight": start_weight,
                    "difference": difference if difference else 0,
                    "age": age,
                    "lifestyle": lifestyle,
                },
            )
            await redis.expire(client_status_key, 86400)
 
        target_actual_key = f"{client_id}:{gym_key}:target_actual"
        target_actual_data = await redis.get(target_actual_key)

 
        if target_actual_data:
            target_actual = json.loads(target_actual_data)
        else:
            target_result = await db.execute(
                select(ClientTarget).where(ClientTarget.client_id == client_id)
            )
            target_data = target_result.scalars().first()

            # Fetch ActualDiet to calculate macros from diet_data (same logic as actual_diet.py)
            actual_diet_result = await db.execute(
                select(ActualDiet).where(
                    ActualDiet.client_id == client_id, ActualDiet.date == date.today()
                )
            )
            actual_diet_record = actual_diet_result.scalars().first()

            def _get_nutrient_value(food_item: dict, *keys: str) -> float:
                """Return the first matching nutrient value, handling legacy naming/casing."""
                for key in keys:
                    value = food_item.get(key)
                    if value not in (None, ""):
                        return value or 0
                return 0

            # Calculate total macros from ActualDiet.diet_data
            total_macros = {
                "calories": 0,
                "protein": 0,
                "carbs": 0,
                "fat": 0,
                "fiber": 0,
                "sugar": 0
            }

            total_micro = {
                "calcium": 0,
                "magnesium": 0,
                "sodium": 0,
                "potassium": 0,
                "iron": 0,
                "iodine": 0,
            }

            if actual_diet_record and actual_diet_record.diet_data:
                diet_data = actual_diet_record.diet_data
                if isinstance(diet_data, list):
                    for meal in diet_data:
                        if isinstance(meal, dict) and "foodList" in meal:
                            food_list = meal.get("foodList", [])
                            for food_item in food_list:
                                if isinstance(food_item, dict):
                                    total_macros["calories"] += food_item.get("calories", 0) or 0
                                    total_macros["protein"] += food_item.get("protein", 0) or 0
                                    total_macros["carbs"] += food_item.get("carbs", 0) or 0
                                    total_macros["fat"] += food_item.get("fat", 0) or 0
                                    total_macros["fiber"] += food_item.get("fiber", 0) or 0
                                    total_macros["sugar"] += food_item.get("sugar", 0) or 0
                                    total_micro["calcium"] += _get_nutrient_value(food_item, "calcium")
                                    total_micro["magnesium"] += _get_nutrient_value(food_item, "magnesium")
                                    total_micro["sodium"] += _get_nutrient_value(food_item, "sodium")
                                    total_micro["potassium"] += _get_nutrient_value(food_item, "potassium")
                                    total_micro["iron"] += _get_nutrient_value(food_item, "iron")

            # Fetch ClientActual for other metrics (burnt_calories, water_intake, weight)
            actual_data_result = await db.execute(
                select(ClientActual).where(
                    ClientActual.client_id == client_id, ClientActual.date == date.today()
                )
            )
            actual_data = actual_data_result.scalars().first()
 
            target_actual = {
                "calories": {
                    "target": target_data.calories if target_data else None,
                    "actual": total_macros["calories"],
                },
                "protein": {
                    "target": target_data.protein if target_data else None,
                    "actual": total_macros["protein"],
                },
                "carbs": {
                    "target": target_data.carbs if target_data else None,
                    "actual": total_macros["carbs"],
                },
                "fat": {
                    "target": target_data.fat if target_data else None,
                    "actual": total_macros["fat"],
                },
                "sugar": {
                    "target": target_data.sugar if target_data else None,
                    "actual": total_macros["sugar"],
                },
                "fiber": {
                    "target": target_data.fiber if target_data else None,
                    "actual": total_macros["fiber"],
                },
                "calories_burnt": {
                    "target": target_data.calories_to_burn if target_data else None,
                    "actual": actual_data.burnt_calories if actual_data else 0,
                },
                "water_intake": {
                    "target": target_data.water_intake if target_data else None,
                    "actual": actual_data.water_intake if actual_data else 0,
                },
                "weight": {
                    "target": target_data.weight if target_data else None,
                    "actual": actual_data.weight if actual_data else 0,
                },
                "calcium": {"actual": total_micro["calcium"]},
                "magnesium": {"actual": total_micro["magnesium"]},
                "sodium": {"actual": total_micro["sodium"]},
                "potassium": {"actual": total_micro["potassium"]},
                "iron": {"actual": total_micro["iron"]}
            }

        
 
            await redis.set(target_actual_key, json.dumps(target_actual))
            await redis.expire(target_actual_key, 86400)
 
        chart_key = f"{client_id}:{gym_key}:chart"
        chart_data = await redis.get(chart_key)
 
        if chart_data:
            chart = json.loads(chart_data)
        else:
            
            today = datetime.now().date()
            first_day_of_month = today.replace(day=1)
            actual_data_result = await db.execute(
                select(ClientActual)
                .where(
                    ClientActual.client_id == client_id,
                    ClientActual.date >= first_day_of_month,
                    ClientActual.date <= today,
                )
                .order_by(ClientActual.date)
            )
            actual_data = actual_data_result.scalars().all()
 

 
            if not actual_data:
                chart = {
                    "weight": [],
                    "calories": [],
                    "calories_burnt": [],
                    "protein": [],
                    "fat": [],
                    "sugar": [],
                    "fiber": [],
                    "carbs": [],
                    "water_intake": [],
                }
            else:
                # Filter out None entries and ensure all records have valid dates
                valid_data = [entry for entry in actual_data if entry and entry.date]
                dates = [entry.date for entry in valid_data]
 
                if len(dates) <= 7:
                    selected_records = valid_data
                else:
                    selected_records = []
                    if valid_data:  # Only proceed if we have valid data
                        selected_records.append(valid_data[0])
                        selected_records.append(valid_data[-1])
                        step = len(dates) // 6
                        for i in range(1, 6):
                            index = i * step
                            if index < len(valid_data) - 1:
                                selected_records.append(valid_data[index])
 
                chart = {
                    "weight": [],
                    "calories": [],
                    "calories_burnt": [],
                    "protein": [],
                    "fat": [],
                    "sugar": [],
                    "fiber": [],
                    "carbs": [],
                    "water_intake": [],
                }
 
                for record in selected_records:
                    if record and record.date:  # Add null check
                        formatted_date = record.date.isoformat()
                        #formatted_date = record.date
                        chart["weight"].append({"date": formatted_date, "weight": record.weight or 0})
                        chart["calories"].append({"date": formatted_date, "calories": record.calories or 0})
                        chart["calories_burnt"].append({"date": formatted_date, "calories_burnt": record.burnt_calories or 0})
                        chart["protein"].append({"date": formatted_date, "protein": record.protein or 0})
                        chart["fat"].append({"date": formatted_date, "fat": record.fats or 0})
                        chart["sugar"].append({"date": formatted_date, "sugar": record.sugar or 0})
                        chart["fiber"].append({"date": formatted_date, "fiber": record.fiber or 0})
                        chart["carbs"].append({"date": formatted_date, "carbs": record.carbs or 0})
                        chart["water_intake"].append({"date": formatted_date, "water_intake": record.water_intake or 0})

            value_keys = {
            "weight": "weight",
            "calories": "calories",
            "calories_burnt": "calories_burnt",
            "protein": "protein",
            "fat": "fat",
            "sugar": "sugar",
            "fiber": "fiber",
            "carbs": "carbs",
            "water_intake": "water_intake",
            }
            for metric, entries in chart.items():
                value_key = value_keys.get(metric)
                if not value_key:
                    continue
                chart[metric] = [
                    entry for entry in entries if entry.get(value_key, 0)
                ]
            
            
            await redis.set(chart_key, json.dumps(chart))
            await redis.expire(chart_key, 86400)

            
        chart_data = chart

        if gym_id is not None:
            leaderboard = await get_leaderboard_with_top(db, gym_id, client_id)
            reward_info = await get_next_reward_info(db, client_id, gym_id)
        else:
            leaderboard = None
            reward_info = None
        workout_data = await get_workout_data(db, client_id)
 
        weight_progress = {
            "actual_weight": actual_weight,
            "target_weight": target_weight,
            "start_weight": start_weight,
            "progress": progress,
        }
 
        general_data = {
            "height": height,
            "lifestyle": lifestyle,
            "age": age,
            "actual_weight": actual_weight,
            "goals": goals,
        }
 
        if gym_id is not None:
            cache_key = f"gym:{gym_id}:active_clients_count"
            cached = await redis.get(cache_key)
            if cached is not None:
                count = int(cached)
            else:
                count = (
                    await db.scalar(
                        select(func.count(Client.client_id)).where(
                            Client.gym_id == gym_id, Client.status == "active"
                        )
                    )
                )
                if count:
                    await redis.set(cache_key, count, ex=300)
                else:
                    count = 0
        else:
            count = 0
 
        cache_key = "home_posters:all"
        posters = []
        cached = await redis.get(cache_key)
        if cached:
            posters = json.loads(cached)
        else:
            rows_result = await db.execute(select(HomePoster))
            rows = rows_result.scalars().all()
            posters = [{"id": r.id, "description": r.description, "url": r.url} for r in rows]
            await redis.set(cache_key, json.dumps(posters), ex=300)

        client_character_result = await db.execute(
            select(ClientCharacter).where(ClientCharacter.client_id == client_id)
        )
        client_character = client_character_result.scalars().first()
        if client_character:
            # Use CharactersCombinationOld for client_id <= 469, else use CharactersCombination
            if client_id <= 469:
                url_db_result = await db.execute(
                    select(CharactersCombinationOld).where(CharactersCombinationOld.id == client_character.character_id)
                )
            else:
                url_db_result = await db.execute(
                    select(CharactersCombination).where(CharactersCombination.id == client_character.character_id)
                )
            url_db = url_db_result.scalars().first()
            url=url_db.characters_url if url_db else None
        else:
            url=None


        client_record = locals().get("client")
        if not client_record:
            client_record_result = await db.execute(
                select(Client).where(Client.client_id == client_id)
            )
            client_record = client_record_result.scalars().first()
        modal_shown_value = client_record.modal_shown if client_record and hasattr(client_record, "modal_shown") else False

        # Calculate reward_interest_modal
        reward_interest_result = await db.execute(
            select(RewardInterest).where(RewardInterest.client_id == client_id)
        )
        reward_interest_data = reward_interest_result.scalars().first()
        if reward_interest_data and reward_interest_data.interested:
         
            reward_interest_modal = False
        elif reward_interest_data and reward_interest_data.next_reminder:
            if reward_interest_data.next_reminder < datetime.now():
                reward_interest_modal = True
            else:
                reward_interest_modal = False
        else:
            reward_interest_modal = True


        if not modal_shown_value:
            reward_interest_modal = False

  
        if reward_interest_modal:
            next_reminder_time = datetime.now() + timedelta(days=1)
            if reward_interest_data:
                reward_interest_data.next_reminder = next_reminder_time
            else:
                new_entry = RewardInterest(
                    client_id=client_id,
                    interested=False,
                    next_reminder=next_reminder_time
                )
                db.add(new_entry)
            await db.commit()

        
        def _blank_chart():
            return {
                "weight": [],
                "calories": [],
                "calories_burnt": [],
                "protein": [],
                "fat": [],
                "sugar": [],
                "fiber": [],
                "carbs": [],
                "water_intake": [],
            }

        def _diet_targets_only(data):
            result = {}
            for key, value in data.items():
                result[key] = {
                    "target": (value.get("target") if isinstance(value, dict) else None),
                    "actual": 0,
                }
            return result

        if tier in {"freemium", "freemium_gym"}:
            general_payload = dict(general_data)
            response_data = {
                "gender": gender if tier == "freemium_gym" else gender,
                "difference": difference if difference else 0,
                "goals": goals or "",
                "weight_progress": {
                    "actual_weight": weight_progress["actual_weight"],
                    "target_weight": weight_progress["target_weight"],
                    "start_weight": weight_progress["start_weight"],
                    "progress": weight_progress.get("progress", 0) if tier == "freemium_gym" else 0,
                },
                "leaderboard": None,
                "bmi": bmi,
                "diet_progress": _diet_targets_only(target_actual),
                "health_dashboard": _blank_chart(),
                "workout_data": workout_data,
                "reward_info": None,
                "general_data": general_payload,
                "gym_count": count if tier == "freemium_gym" else 0,
                "posters": posters,
                "modal_shown": modal_shown_value,
                "url": url,
                "free_trial": has_free_trial,
                "remaining_days": remaining_days,
                "free_trial_active": free_trial_active,
                "reward_interest_modal": reward_interest_modal,
            }
        elif tier == "premium":
            response_data = {
                "gender": gender,
                "difference": difference if difference else 0,
                "goals": goals,
                "weight_progress": weight_progress,
                "leaderboard": None,
                "bmi": bmi,
                "diet_progress": target_actual,
                "health_dashboard": chart_data,
                "workout_data": workout_data,
                "reward_info": None,
                "general_data": general_data,
                "gym_count": 0,
                "posters": posters,
                "modal_shown": modal_shown_value,
                "url": url,
                "free_trial": has_free_trial,
                "remaining_days": remaining_days,
                "free_trial_active": free_trial_active,
                "reward_interest_modal": reward_interest_modal,
            }
        else:
            response_data = {
                "gender": gender,
                "difference": difference if difference else 0,
                "goals": goals,
                "weight_progress": weight_progress,
                "leaderboard": leaderboard,
                "bmi": bmi,
                "diet_progress": target_actual,
                "health_dashboard": chart_data,
                "workout_data": workout_data,
                "reward_info": reward_info,
                "general_data": general_data,
                "gym_count": count,
                "posters": posters,
                "modal_shown": modal_shown_value,
                "url":url,
                "free_trial": has_free_trial,
                "remaining_days": remaining_days,
                "free_trial_active": free_trial_active,
                "reward_interest_modal":reward_interest_modal
            }

            # Add food_scan key only for freemium users
        
        if food_scan_available is not None:
                response_data["food_scan"] = food_scan_available

        # Add nutrition consultation info
        nutrition_info = await get_nutrition_eligibility_info(db, client_id)
        response_data.update(nutrition_info)

        # Add manual posters info (when use_manual_posters is True, frontend shows these instead of conditional posters)
        manual_posters_info = await get_manual_posters_info(db)
        response_data.update(manual_posters_info)

        # Add client registration steps
        registration_steps = await get_client_registration_steps(db, client_id)
        response_data["registration_steps"] = registration_steps

        # Add usertype based on registration steps completion
        all_steps_completed = all([
            registration_steps["dob"],
            registration_steps["goal"],
            registration_steps["height"],
            registration_steps["weight"],
            registration_steps["body_shape"],
            registration_steps["lifestyle"],
        ])
        response_data["usertype"] = "full_user" if all_steps_completed else "guest"
        print("reward interest modal is",reward_interest_modal)

        return {"status": 200, "data": response_data}
 
    except FittbotHTTPException:
        raise
    except Exception as e:
        raise FittbotHTTPException(
            status_code=500,
            detail="Unable to retrieve progress information",
            error_code="MY_PROGRESS_FETCH_ERROR",
            log_data={"exc": repr(e), "client_id": client_id, "gym_id": gym_id},
    )



async def _recalculate_monthly_water_intake(db_session: AsyncSession, client_id: int, reference_date: date) -> None:
    month_start = date(reference_date.year, reference_date.month, 1)
    if month_start.month == 12:
        next_month = date(month_start.year + 1, 1, 1)
    else:
        next_month = date(month_start.year, month_start.month + 1, 1)

    water_rows_result = await db_session.execute(
        select(ClientActual.water_intake).where(
            ClientActual.client_id == client_id,
            ClientActual.date >= month_start,
            ClientActual.date < next_month,
            ClientActual.water_intake.isnot(None),
        )
    )
    water_rows = water_rows_result.all()

    water_values = [row[0] for row in water_rows if row[0] is not None]
    average_water = round(sum(water_values) / len(water_values), 2) if water_values else 0.0

    analysis_result = await db_session.execute(
        select(ClientGeneralAnalysis).where(
            ClientGeneralAnalysis.client_id == client_id,
            ClientGeneralAnalysis.date == month_start,
        )
    )
    analysis_record = analysis_result.scalars().first()

    if analysis_record:
        analysis_record.water_taken = average_water
    elif water_values:
        db_session.add(
            ClientGeneralAnalysis(
                client_id=client_id,
                date=month_start,
                water_taken=average_water,
            )
        )

class InputData(BaseModel):
    type: str
    client_id: int
    target_weight: Optional[float] = None
    actual_weight: Optional[float] = None
    start_weight: Optional[float] = None
    actual_water: Optional[float] = None
    target_water: Optional[float] = None
    calories: Optional[int] = None
    protein: Optional[int] = None
    carbs: Optional[int] = None
    fat: Optional[int] = None
    sugar: Optional[int] = None
    fiber: Optional[int] = None
 
 
@router.post("/add_inputs")
async def add_input(
    http_request: Request,
    request: InputData,
    db: AsyncSession = Depends(get_async_db),
    redis: Redis = Depends(get_redis),
):
    try:
        # Check if client is eligible for free trial
        # Eligible (True) only if: no entry in free_trial table AND no entry in subscriptions table
        free_trial_result = await db.execute(
            select(FreeTrial).where(FreeTrial.client_id == request.client_id)
        )
        free_trial_entry = free_trial_result.scalars().first()
        subscription_result = await db.execute(
            select(Subscription).where(Subscription.customer_id == str(request.client_id))
        )
        subscription_entry = subscription_result.scalars().first()

        # True only if both tables have no entry
        has_free_trial = (free_trial_entry is None) and (subscription_entry is None)

        input_type = request.type.lower()
 
        if input_type == "weight":
            actual_weight = request.actual_weight
            target_weight = request.target_weight
            start_weight = request.start_weight

            print("actual_weight", actual_weight, "type:", type(actual_weight))
            print("target_weight", target_weight, "type:", type(target_weight))
            print("start_weight", start_weight, "type:", type(start_weight))
 
            if request.client_id is None:
                raise FittbotHTTPException(
                    status_code=400,
                    detail="client_id and weight are required for weight update",
                    error_code="INPUT_VALIDATION_ERROR",
                    log_data={"type": "weight"},
                )
 
            record_date = date.today()

            if actual_weight:
                existing_record_result = await db.execute(
                    select(ClientActual).where(
                        ClientActual.client_id == request.client_id, ClientActual.date == record_date
                    )
                )
                existing_record = existing_record_result.scalars().first()
                if existing_record:
                    
                    existing_record.weight = request.actual_weight
                else:
                    db.add(
                        ClientActual(
                            client_id=request.client_id,
                            date=record_date,
                            weight=actual_weight,
                        )
                    )
                await db.commit()

                weight_result = await db.execute(
                    select(Client).where(Client.client_id == request.client_id)
                )
                weight = weight_result.scalars().first()
                if weight:
                    height = (weight.height or 0) / 100 if weight.height else 0
                    if height > 0:
                        bmi = round(actual_weight / (height ** 2), 2)
                    else:
                        bmi = None
                    weight.bmi = bmi
                    weight.weight = request.actual_weight
                    await db.commit()

                month_start_date = date(record_date.year, record_date.month, 1)
                
                analysis_result = await db.execute(
                    select(ClientGeneralAnalysis).where(
                        ClientGeneralAnalysis.client_id == request.client_id,
                        ClientGeneralAnalysis.date == month_start_date,
                    )
                )
                analysis_record = analysis_result.scalars().first()

 
                if analysis_record:
                    if analysis_record.weight is not None:
                        analysis_record.weight = (analysis_record.weight + actual_weight) / 2
                    else:
                        analysis_record.weight = actual_weight
                else:
                    db.add(
                        ClientGeneralAnalysis(
                            client_id=request.client_id,
                            date=month_start_date,
                            weight=actual_weight,
                        )
                    )
                await db.commit()
 
                last_record_result = await db.execute(
                    select(ClientWeightData)
                    .where(ClientWeightData.client_id == request.client_id)
                    .order_by(ClientWeightData.id.desc())
                )
                last_record = last_record_result.scalars().first()
                # Ensure proper type comparison
                if not last_record:
                    status = True
                else:
                    try:
                        last_weight_float = float(last_record.weight) if last_record.weight else 0
                        actual_weight_float = float(actual_weight)
                        status = actual_weight_float > last_weight_float
                        print("Status comparison:", actual_weight_float, ">", last_weight_float, "=", status)
                    except (ValueError, TypeError) as e:
                        print("Error comparing weights for status:", e)
                        status = True
 
                existing_weight_result = await db.execute(
                    select(ClientWeightData)
                    .where(ClientWeightData.client_id == request.client_id)
                    .order_by(desc(ClientWeightData.id))
                )
                existing_weight = existing_weight_result.scalars().first()
                if existing_weight:
                    if existing_weight.weight != actual_weight:
                        new_record = ClientWeightData(
                            client_id=request.client_id,
                            weight=actual_weight,
                            status=status,
                            date=date.today(),
                        )
                        db.add(new_record)
                        await db.commit()
                        await db.refresh(new_record)
                else:
                    new_record = ClientWeightData(
                        client_id=request.client_id,
                        weight=actual_weight,
                        status=status,
                        date=date.today(),
                    )
                    db.add(new_record)
                    await db.commit()
                    await db.refresh(new_record)

                existing_target_result = await db.execute(
                    select(ClientTarget).where(ClientTarget.client_id == request.client_id)
                )
                existing_target = existing_target_result.scalars().first()
 
                journey_completion = None
                if existing_target:
                    existing_target_weight = existing_target.weight
                    print("existing_target_weight", existing_target_weight, "type:", type(existing_target_weight))
                    if existing_target_weight and actual_weight:
                        # Ensure both values are floats for comparison
                        try:
                            existing_target_weight_float = float(existing_target_weight)
                            actual_weight_float = float(actual_weight)
                            print("Comparing:", actual_weight_float, ">", existing_target_weight_float)
                            if actual_weight_float > existing_target_weight_float:
                                journey_completion = True
                        except (ValueError, TypeError) as e:
                            print("Error converting weights for comparison:", e)
                            journey_completion = False
 
                if not journey_completion:
                    journey_completion = False
 
                last_journey_result = await db.execute(
                    select(WeightJourney)
                    .where(WeightJourney.client_id == request.client_id, WeightJourney.end_date.is_(None))
                    .order_by(desc(WeightJourney.start_date), desc(WeightJourney.id))
                )
                last_journey = last_journey_result.scalars().first()
                if last_journey:
                    last_journey.actual_weight = actual_weight
                    await db.commit()

            if target_weight:
                existing_target_result = await db.execute(
                    select(ClientTarget).where(ClientTarget.client_id == request.client_id)
                )
                existing_target = existing_target_result.scalars().first()
                if existing_target:
                    existing_target.weight = target_weight
                    await db.commit()
                else:
                    db.add(ClientTarget(client_id=request.client_id, weight=target_weight))
                    await db.commit()

                client_result = await db.execute(
                    select(Client).where(Client.client_id == request.client_id)
                )
                client = client_result.scalars().first()
                if not client:
                    raise FittbotHTTPException(
                        status_code=404,
                        detail="Client not found",
                        error_code="CLIENT_NOT_FOUND",
                        log_data={"client_id": request.client_id},
                    )
                actual_weight_now = client.weight
 
                last_journey_result = await db.execute(
                    select(WeightJourney)
                    .where(WeightJourney.client_id == request.client_id, WeightJourney.end_date.is_(None))
                    .order_by(desc(WeightJourney.start_date), desc(WeightJourney.id))
                )
                last_journey = last_journey_result.scalars().first()
                if last_journey:
                    if last_journey.target_weight != target_weight:
                        last_journey.end_date = date.today()
                        last_journey.actual_weight = actual_weight_now
                        db.add(last_journey)
                        await db.commit()
 
                        new_journey = WeightJourney(
                            client_id=request.client_id,
                            start_date=date.today(),
                            start_weight=start_weight,
                            actual_weight=actual_weight_now,
                            target_weight=target_weight,
                        )
                        db.add(new_journey)
                        await db.commit()
                else:
                    db.add(
                        WeightJourney(
                            client_id=request.client_id,
                            start_date=date.today(),
                            start_weight=actual_weight_now,
                            actual_weight=actual_weight_now,
                            target_weight=target_weight,
                        )
                    )
                    await db.commit()

            if start_weight:
                existing_target_result = await db.execute(
                    select(ClientTarget).where(ClientTarget.client_id == request.client_id)
                )
                existing_target = existing_target_result.scalars().first()
                if existing_target:
                    existing_target.start_weight = start_weight
                    await db.commit()
                else:
                    db.add(ClientTarget(client_id=request.client_id, start_weight=request.start_weight))
                    await db.commit()
 
            target_actual_key = f"client{request.client_id}:initial_target_actual"
            if await redis.exists(target_actual_key):
                await redis.delete(target_actual_key)
 
            client_status_key = f"client{request.client_id}:initialstatus"
            if await redis.exists(client_status_key):
                await redis.delete(client_status_key)
 
            client_status_key_pattern = "*:status"
            client_status_keys = await redis.keys(client_status_key_pattern)
            if client_status_keys:
                await redis.delete(*client_status_keys)
 
            analytics_key_pattern = "*:analytics"
            analytics_keys = await redis.keys(analytics_key_pattern)
            if analytics_keys:
                await redis.delete(*analytics_keys)
 
            pattern = "*:target_actual"
            keys = await redis.keys(pattern)
            if keys:
                await redis.delete(*keys)
 
            chart_key_pattern = "*:chart"
            chart_keys = await redis.keys(chart_key_pattern)
            if chart_keys:
                await redis.delete(*chart_keys)
 
            client_data_pattern = await redis.keys("gym:*:clientdata")
            if client_data_pattern:
                await redis.delete(*client_data_pattern)
 
            return {"status": 200, "message": "weight added successfully", "journey_completion": journey_completion, "free_trial": has_free_trial}
 
        elif input_type == "calories":
            if request.client_id is None:
                raise FittbotHTTPException(
                    status_code=400,
                    detail="client_id is required for calorie target update",
                    error_code="INPUT_VALIDATION_ERROR",
                    log_data={"type": "calories"},
                )
 
            existing_target_result = await db.execute(
                select(ClientTarget).where(ClientTarget.client_id == request.client_id)
            )
            existing_target = existing_target_result.scalars().first()
            if existing_target:
                existing_target.calories = request.calories
                existing_target.protein = request.protein
                existing_target.carbs = request.carbs
                existing_target.fat = request.fat
                existing_target.sugar = request.sugar
                existing_target.fiber = request.fiber
                await db.commit()
            else:
                db.add(
                    ClientTarget(
                        client_id=request.client_id,
                        calories=request.calories,
                        protein=request.protein,
                        carbs=request.carbs,
                        fat=request.fat,
                        sugar=request.sugar,
                        fiber=request.fiber,
                    )
                )
                await db.commit()
 
            target_actual_key = f"client{request.client_id}:initial_target_actual"
            if await redis.exists(target_actual_key):
                await redis.delete(target_actual_key)
 
            client_status_key = f"client{request.client_id}:initialstatus"
            if await redis.exists(client_status_key):
                await redis.delete(client_status_key)
 
            client_status_key_pattern = "*:status"
            client_status_keys = await redis.keys(client_status_key_pattern)
            if client_status_keys:
                await redis.delete(*client_status_keys)
 
            analytics_key_pattern = "*:analytics"
            analytics_keys = await redis.keys(analytics_key_pattern)
            if analytics_keys:
                await redis.delete(*analytics_keys)
 
            pattern = "*:target_actual"
            keys = await redis.keys(pattern)
            if keys:
                await redis.delete(*keys)
 
            chart_key_pattern = "*:chart"
            chart_keys = await redis.keys(chart_key_pattern)
            if chart_keys:
                await redis.delete(*chart_keys)
 
            return {"status": 200, "message": "Calories added successfully", "free_trial": has_free_trial}
 
        elif input_type == "water":
            if request.client_id is None:
                raise FittbotHTTPException(
                    status_code=400,
                    detail="client_id is required for water update",
                    error_code="INPUT_VALIDATION_ERROR",
                    log_data={"type": "water"},
                )

            actual_water = request.actual_water
            target_water = request.target_water

            actual_water_float = None
            if actual_water is not None:
                try:
                    actual_water_float = float(actual_water)
                    print("actual_water_float", actual_water_float, "type:", type(actual_water_float))
                except (ValueError, TypeError) as e:
                    print("Error converting actual_water to float:", e)
                    raise FittbotHTTPException(
                        status_code=400,
                        detail="Invalid water intake value",
                        error_code="INPUT_VALIDATION_ERROR",
                        log_data={"type": "water", "actual_water": actual_water},
                    )

            target_water_float = None
            if target_water is not None:
                try:
                    target_water_float = float(target_water)
                    print("target_water_float", target_water_float, "type:", type(target_water_float))
                except (ValueError, TypeError) as e:
                    print("Error converting target_water to float:", e)
                    raise FittbotHTTPException(
                        status_code=400,
                        detail="Invalid target water intake value",
                        error_code="INPUT_VALIDATION_ERROR",
                        log_data={"type": "water", "target_water": target_water},
                    )

            if actual_water_float is None and target_water_float is None:
                raise FittbotHTTPException(
                    status_code=400,
                    detail="Provide actual_water or target_water for update",
                    error_code="INPUT_VALIDATION_ERROR",
                    log_data={"type": "water"},
                )

            record_date = date.today()
            water_updated = False

            if actual_water_float is not None and actual_water_float > -1:
                existing_actual_result = await db.execute(
                    select(ClientActual).where(
                        ClientActual.client_id == request.client_id, ClientActual.date == record_date
                    )
                )
                existing_actual = existing_actual_result.scalars().first()
                if existing_actual:
                    existing_actual.water_intake = actual_water_float
                else:
                    db.add(
                        ClientActual(
                            client_id=request.client_id,
                            date=record_date,
                            water_intake=actual_water_float,
                        )
                    )
                water_updated = True
                await db.flush()

            if target_water_float is not None:
                existing_target_result = await db.execute(
                    select(ClientTarget).where(ClientTarget.client_id == request.client_id)
                )
                existing_target = existing_target_result.scalars().first()
                if existing_target:
                    existing_target.water_intake = target_water_float
                else:
                    db.add(ClientTarget(client_id=request.client_id, water_intake=target_water_float))

            if water_updated:
                await _recalculate_monthly_water_intake(db, request.client_id, record_date)

            await db.commit()

            if water_updated or target_water_float is not None:
                target_actual_key = f"client{request.client_id}:initial_target_actual"
                if await redis.exists(target_actual_key):
                    await redis.delete(target_actual_key)

                client_status_key = f"client{request.client_id}:initialstatus"
                if await redis.exists(client_status_key):
                    await redis.delete(client_status_key)

                client_status_key_pattern = "*:status"
                client_status_keys = await redis.keys(client_status_key_pattern)
                if client_status_keys:
                    await redis.delete(*client_status_keys)

                analytics_key_pattern = "*:analytics"
                analytics_keys = await redis.keys(analytics_key_pattern)
                if analytics_keys:
                    await redis.delete(*analytics_keys)

                pattern = "*:target_actual"
                keys = await redis.keys(pattern)
                if keys:
                    await redis.delete(*keys)

                chart_key_pattern = "*:chart"
                chart_keys = await redis.keys(chart_key_pattern)
                if chart_keys:
                    await redis.delete(*chart_keys)

            return {"status": 200, "message": "Water qty added successfully", "free_trial": has_free_trial}
 
        else:
            raise FittbotHTTPException(
                status_code=400,
                detail="Invalid input type",
                error_code="INPUT_VALIDATION_ERROR",
                log_data={"type": request.type},
            )
 
    except FittbotHTTPException:
        raise
    except Exception as e:
        raise FittbotHTTPException(
            status_code=500,
            detail="Unable to save input data",
            error_code="MY_PROGRESS_INPUT_SAVE_ERROR",
            log_data={"exc": repr(e), "client_id": getattr(request, "client_id", None)},
        )
 
 
 
class JourneyResponse(BaseModel):
    id: int
    client_id: int
    start_date: Optional[date]
    end_date: Optional[date]
    start_weight: float
    actual_weight: float
    target_weight: float
    days_diff: int
 
    class Config:
        from_attributes = True
 
 
class WeightDataResponse(BaseModel):
    id: int
    client_id: int
    weight: float
    status: bool
    date: date
 
    class Config:
        from_attributes = True
 
 
class WeightReport(BaseModel):
    journeys: List[JourneyResponse]
    records: List[WeightDataResponse]
    weight: List
 
 
@router.get("/weight_journey")
async def read_weight_report(
    request: Request,
    client_id: int,
    db: AsyncSession = Depends(get_async_db),
):
    try:
        client_result = await db.execute(
            select(Client).where(Client.client_id == client_id)
        )
        client = client_result.scalars().first()
        if not client:
            raise FittbotHTTPException(
                status_code=404,
                detail="Client not found",
                error_code="CLIENT_NOT_FOUND",
                log_data={"client_id": client_id},
            )

        # Check if client is eligible for free trial (no entry means eligible)
        free_trial_result = await db.execute(
            select(FreeTrial).where(FreeTrial.client_id == client_id)
        )
        free_trial_entry = free_trial_result.scalars().first()
        has_free_trial = free_trial_entry is None  # True if no entry (eligible), False if entry exists (already used)
 
        journeys_result = await db.execute(
            select(WeightJourney).where(WeightJourney.client_id == client_id)
        )
        journeys = journeys_result.scalars().all()
 
        journey_list = []
        for idx,j in enumerate(journeys,start=1):
            if j.start_date and j.end_date:
                days_diff = (j.end_date - j.start_date).days
            elif j.end_date is None:
                days_diff = (date.today() - j.start_date).days
            else:
                days_diff = 0
 
            journey_list.append(
                JourneyResponse(
                    id=idx,
                    client_id=j.client_id,
                    start_date=j.start_date,
                    end_date=j.end_date,
                    start_weight=j.start_weight,
                    actual_weight=j.actual_weight,
                    target_weight=j.target_weight,
                    days_diff=days_diff,
                )
            )
 
        record_list = []
        records_result = await db.execute(
            select(ClientWeightData)
            .where(ClientWeightData.client_id == client_id)
            .order_by(ClientWeightData.id.desc())
        )
        records = records_result.scalars().all()
        if records:
            record_list = [
                {
                    "id": r.id,
                    "client_id": r.client_id,
                    "weight": r.weight,
                    "status": r.status,
                    "date": r.date,
                }
                for r in records
            ]
 
        data_result = await db.execute(
            select(ClientGeneralAnalysis)
            .where(ClientGeneralAnalysis.client_id == client_id)
            .order_by(ClientGeneralAnalysis.date.asc())
        )
        data = data_result.scalars().all()
 
        weight = []
        if data:
            for record in data:
                month_name = record.date
                weight.append(
                    {
                        "label": month_name,
                        "value": record.weight if record.weight else 0,
                    }
                )
 
        journey_data = {"weight": weight, "journey_list": journey_list, "record_list": record_list, "free_trial": has_free_trial}
 
        response = {"status": 200, "data": journey_data}

        print("responseeee is",response)
        return response
 
    except FittbotHTTPException:
        raise
    except Exception as e:
        raise FittbotHTTPException(
            status_code=500,
            detail="Unable to retrieve weight journey data",
            error_code="WEIGHT_JOURNEY_FETCH_ERROR",
            log_data={"exc": repr(e), "client_id": client_id},
        )
 
 
