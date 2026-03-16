import uuid
from datetime import date, timedelta, datetime, time
from typing import List, Optional, Dict, Any

from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy import select, and_
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.async_database import get_async_db
from sqlalchemy import func

from app.models.fittbot_models import (
    SessionSetting,
    SessionSchedule,
    ClassSession,
    ReferralFittbotCash,
    SessionBookingDay,
    Trainer,
    TrainerProfile,
)
from app.utils.logging_utils import FittbotHTTPException
from app.config.pricing import get_markup_multiplier

router = APIRouter(prefix="/client/sessions", tags=["Client Sessions"])

# Default capacity limits by session type
DEFAULT_CAPACITY_PERSONAL_TRAINER = 5
DEFAULT_CAPACITY_OTHER = 25
PERSONAL_TRAINER_SESSION_ID = 2  # session_id=2 is Personal Training
HIDDEN_SESSION_IDS = {7, 8, 10, 11, 14}


def _as_time_str(t: time) -> str:
    return datetime.strptime(t.strftime("%H:%M"), "%H:%M").strftime("%I:%M %p")


class PriceResponse(BaseModel):
    base_price: Optional[int]
    discount_percentage: Optional[float]
    final_price: Optional[int]
    total_1: Optional[int]
    total_5: Optional[int]
    total_10: Optional[int]


class RewardCalculationRequest(BaseModel):
    client_id: int
    amount: float


@router.get("/list")
async def list_sessions(
    gym_id: int,
    db: AsyncSession = Depends(get_async_db),
):
    try:

        stmt = (
            select(SessionSetting, ClassSession)
            .join(ClassSession, ClassSession.id == SessionSetting.session_id)
            .where(
                SessionSetting.gym_id == gym_id,
                SessionSetting.is_enabled.is_(True),
                SessionSetting.base_price.isnot(None),
                SessionSetting.final_price.isnot(None),
                SessionSetting.session_id.notin_(HIDDEN_SESSION_IDS),
            )
        )

        rows = (await db.execute(stmt)).all()
        data = []

        trainer_added=False
        
        for setting, session_meta in rows:
            if setting.trainer_id is not None and not trainer_added:
                trainer_added=True
            elif setting.trainer_id:
                continue

            data.append(
                {
                    "id": setting.session_id,
                    "trainer_id": setting.trainer_id,
                    "name": session_meta.name if session_meta else None,
                    "image": session_meta.image if session_meta else None,
                    "description": session_meta.description if session_meta else None,
                    "timing": session_meta.timing if session_meta else None,
                    "price": setting.base_price,
                    "discount_percentage": setting.discount_percent,
                    "final_price": setting.final_price,
                }
            )

        return {"status": 200, "data": data}
    except Exception as exc:
        raise FittbotHTTPException(
            status_code=500,
            detail="Failed to fetch sessions",
            error_code="CLIENT_SESSIONS_FETCH_ERROR",
            log_data={"error": repr(exc), "gym_id": gym_id},
        )


@router.get("/get_trainers")
async def get_session_trainers(
    gym_id: int,
    db: AsyncSession = Depends(get_async_db),
):
    try:
        # Get trainer_ids that have properly configured sessions
        trainer_ids_stmt = (
            select(SessionSetting.trainer_id)
            .where(
                SessionSetting.gym_id == gym_id,
                SessionSetting.trainer_id.isnot(None),
                SessionSetting.is_enabled.is_(True),
                SessionSetting.base_price.isnot(None),
                SessionSetting.final_price.isnot(None),
            )
            .distinct()
        )

        trainer_ids_result = await db.execute(trainer_ids_stmt)
        trainer_ids = [row[0] for row in trainer_ids_result.all()]

        if not trainer_ids:
            return {"status": 200, "data":  []}


        trainers_stmt = (
            select(Trainer, TrainerProfile)
            .join(TrainerProfile, Trainer.trainer_id == TrainerProfile.trainer_id)
            .where(
                TrainerProfile.gym_id == gym_id,
                Trainer.trainer_id.in_(trainer_ids),
            )
        )

        trainers_result = await db.execute(trainers_stmt)
        trainers_data = []

        for trainer, profile in trainers_result.all():
            trainers_data.append({
                "trainer_id": trainer.trainer_id,
                "full_name": profile.full_name or trainer.full_name,
                "profile_image": profile.profile_image or trainer.profile_image,
                "specializations": profile.specializations or trainer.specializations,
                "experience": profile.experience or trainer.experience,
            })

        return {"status": 200, "data": trainers_data}

    except Exception as exc:
        raise FittbotHTTPException(
            status_code=500,
            detail="Failed to fetch trainers",
            error_code="SESSION_TRAINERS_FETCH_ERROR",
            log_data={"error": repr(exc), "gym_id": gym_id},
        )


@router.get("/price")
async def get_session_price(
    gym_id: int,
    session_id: int,
    trainer_id: Optional[int] = None,
    is_offer_eligible: bool = False,
    db: AsyncSession = Depends(get_async_db),
):
    try:
        
        import logging
        logger = logging.getLogger(__name__)


        stmt = select(SessionSetting).where(
            SessionSetting.gym_id == gym_id,
            SessionSetting.session_id == session_id,
            SessionSetting.is_enabled.is_(True),
            SessionSetting.base_price.isnot(None),
            SessionSetting.final_price.isnot(None),
        )

        if trainer_id is not None:
            stmt = stmt.where(SessionSetting.trainer_id == trainer_id)
        else:
            stmt = stmt.where(SessionSetting.trainer_id.is_(None))

        setting = (await db.execute(stmt)).scalars().first()
        if not setting:
            raise FittbotHTTPException(
                status_code=404,
                detail="Session not available",
                error_code="SESSION_NOT_AVAILABLE",
            )

        base_price = setting.base_price or 0
        discount_percent = setting.discount_percent or 0
        final_price = setting.final_price or 0

        #logger.info(f"SESSION_PRICE_BEFORE: base={base_price}, final={final_price}, discount%={discount_percent}")

        if is_offer_eligible:
            base_price_with_markup = 99
            final_price_with_markup = 99

        elif final_price == 99:
            base_price_with_markup = base_price
            final_price_with_markup = 99

        else:
      
            base_price_with_markup = round(base_price * get_markup_multiplier())
            final_price_with_markup = round(final_price * get_markup_multiplier())

        payload = {
            "single": {
                "base_price": base_price_with_markup,
                "discount_percentage": discount_percent,
                "final_price": final_price_with_markup,
            },
            "bulk_5": {
                "base_price": base_price_with_markup * 5,
                "discount_percentage": discount_percent,
                "final_price": final_price_with_markup * 5,
            },
        }


        return {"status": 200, "data": payload}
    
    except FittbotHTTPException:
        raise
    except Exception as exc:
        raise FittbotHTTPException(
            status_code=500,
            detail="Failed to fetch session price",
            error_code="SESSION_PRICE_ERROR",
            log_data={"error": repr(exc), "gym_id": gym_id, "session_id": session_id},
        )


@router.get("/dates")
async def get_available_dates(
    gym_id: int,
    session_id: int,
    trainer_id: Optional[int] = None,
    db: AsyncSession = Depends(get_async_db),
):
    try:
        today = date.today()
        end_range = today + timedelta(days=30)

        stmt = select(SessionSchedule).where(
            SessionSchedule.gym_id == gym_id,
            SessionSchedule.session_id == session_id,
            SessionSchedule.is_active.is_(True),
        )
        if trainer_id is not None:
            stmt = stmt.where(SessionSchedule.trainer_id == trainer_id)
        else:
            stmt = stmt.where(SessionSchedule.trainer_id.is_(None))

        schedules = (await db.execute(stmt)).scalars().all()
        available_dates = set()
        for sch in schedules:
            # respect date bounds
            start_bound = sch.start_date or today
            end_bound = sch.end_date or end_range
            range_start = max(today, start_bound)
            range_end = min(end_range, end_bound)
            if range_end < range_start:
                continue

            if sch.recurrence == "weekly":
                d = range_start
                while d <= range_end:
                    if sch.weekday is None or d.weekday() == sch.weekday:
                        available_dates.add(d)
                    d += timedelta(days=1)
            else:  # one_off
                d = range_start
                while d <= range_end:
                    if (sch.weekday is None) or (d.weekday() == sch.weekday):
                        available_dates.add(d)
                    d += timedelta(days=1)

        sorted_dates = sorted(list(available_dates))
        formatted = [d.isoformat() for d in sorted_dates]
        return {"status": 200, "data": {"dates": formatted}}
    except Exception as exc:
        raise FittbotHTTPException(
            status_code=500,
            detail="Failed to fetch available dates",
            error_code="SESSION_DATES_ERROR",
            log_data={"error": repr(exc), "gym_id": gym_id, "session_id": session_id},
        )


class SlotsCheckRequest(BaseModel):
    gym_id: int
    session_id: int
    dates: List[date] 
    trainer_id: Optional[int] = None


@router.post("/slots")
async def get_slots_for_dates(
    payload: SlotsCheckRequest,
    db: AsyncSession = Depends(get_async_db),
):

    try:
        gym_id = payload.gym_id
        session_id = payload.session_id
        trainer_id = payload.trainer_id
        selected_dates = payload.dates

        now = datetime.now()
        today = now.date()
        current_time = now.time()

        if not selected_dates:
            raise FittbotHTTPException(
                status_code=400,
                detail="At least one date must be provided",
                error_code="NO_DATES_PROVIDED",
            )


        is_personal_trainer = session_id == PERSONAL_TRAINER_SESSION_ID
        default_capacity = DEFAULT_CAPACITY_PERSONAL_TRAINER if is_personal_trainer else DEFAULT_CAPACITY_OTHER

        # 2. Get session setting for custom capacity (if configured)
        setting_stmt = select(SessionSetting).where(
            SessionSetting.gym_id == gym_id,
            SessionSetting.session_id == session_id,
            SessionSetting.is_enabled.is_(True),
        )
        if trainer_id is not None:
            setting_stmt = setting_stmt.where(SessionSetting.trainer_id == trainer_id)
        else:
            setting_stmt = setting_stmt.where(SessionSetting.trainer_id.is_(None))

        setting = (await db.execute(setting_stmt)).scalars().first()
        max_capacity = setting.capacity if (setting and setting.capacity) else default_capacity

        # 3. Get all schedules for this session
        stmt = select(SessionSchedule).where(
            SessionSchedule.gym_id == gym_id,
            SessionSchedule.session_id == session_id,
            SessionSchedule.is_active.is_(True),
        )
        if trainer_id is not None:
            stmt = stmt.where(SessionSchedule.trainer_id == trainer_id)
        else:
            stmt = stmt.where(SessionSchedule.trainer_id.is_(None))

        schedules = (await db.execute(stmt)).scalars().all()


        timing_to_schedules: Dict[tuple, Dict[date, int]] = {} 

        for target_date in selected_dates:
            for sch in schedules:
                # bounds check
                if sch.start_date and target_date < sch.start_date:
                    continue
                if sch.end_date and target_date > sch.end_date:
                    continue

                if target_date == today:

                    if sch.start_time is None:
                        continue


                    if isinstance(sch.start_time, time):
                        session_start = sch.start_time
                    elif isinstance(sch.start_time, str):
                        session_start = datetime.strptime(sch.start_time, "%H:%M:%S").time()
                    else:
                        continue


                    if current_time >= session_start:
                        continue

                if sch.recurrence == "weekly":
                    if sch.weekday is not None and target_date.weekday() != sch.weekday:
                        continue
                if sch.recurrence == "one_off" and sch.weekday is not None:
                    if target_date.weekday() != sch.weekday:
                        continue

                timing_key = (_as_time_str(sch.start_time), _as_time_str(sch.end_time))
                if timing_key not in timing_to_schedules:
                    timing_to_schedules[timing_key] = {}
                timing_to_schedules[timing_key][target_date] = sch.id


        all_schedule_ids = set()
        for date_schedule_map in timing_to_schedules.values():
            all_schedule_ids.update(date_schedule_map.values())


        booking_counts: Dict[tuple, int] = {}
        if all_schedule_ids:
            booking_stmt = (
                select(
                    SessionBookingDay.schedule_id,
                    SessionBookingDay.booking_date,
                    func.count(SessionBookingDay.id).label("count")
                )
                .where(
                    SessionBookingDay.schedule_id.in_(list(all_schedule_ids)),
                    SessionBookingDay.booking_date.in_(selected_dates),
                    SessionBookingDay.status.in_(["booked", "attended"]),
                )
                .group_by(SessionBookingDay.schedule_id, SessionBookingDay.booking_date)
            )
            booking_rows = (await db.execute(booking_stmt)).all()
            for row in booking_rows:
                booking_counts[(row.schedule_id, row.booking_date)] = row.count

       
        date_to_slots: Dict[str, List[Dict[str, Any]]] = {d.isoformat(): [] for d in selected_dates}
        merged_slots = []
        for timing_key, date_schedule_map in timing_to_schedules.items():
            start_time_str, end_time_str = timing_key

      
            full_dates = []
            any_date_full = False
            date_details = []

            for target_date, schedule_id in date_schedule_map.items():
                booked_count = booking_counts.get((schedule_id, target_date), 0)
                available_spots = max(0, max_capacity - booked_count)
                is_date_full = available_spots == 0

                if is_date_full:
                    any_date_full = True
                    full_dates.append(target_date.isoformat())

                date_details.append(
                    {
                        "date": target_date.isoformat(),
                        "schedule_id": schedule_id,
                        "booked_count": booked_count,
                        "available_spots": available_spots,
                        "is_full": is_date_full,
                    }
                )

                date_to_slots[target_date.isoformat()].append(
                    {
                        "start_time": start_time_str,
                        "end_time": end_time_str,
                        "max_capacity": max_capacity,
                        "booked_count": booked_count,
                        "available_spots": available_spots,
                        "is_full": is_date_full,
                        "schedule_id": schedule_id,
                    }
                )

            merged_slots.append(
                {
                    "start_time": start_time_str,
                    "end_time": end_time_str,
                    "max_capacity": max_capacity,
                    "is_full": any_date_full,  # TRUE if ANY date is full
                    "full_dates": full_dates,  # List of dates that are full
                    "date_details": date_details,  # Per-date breakdown
                }
            )


        slots = []
        for target_date in sorted(selected_dates):
            date_key = target_date.isoformat()
            day_slots = sorted(
                date_to_slots.get(date_key, []),
                key=lambda x: datetime.strptime(x["start_time"], "%I:%M %p"),
            )
            slots.append({"date": date_key, "slots": day_slots})


        num_selected_dates = len(selected_dates)
        default_slots = []
        has_any_slots = len(merged_slots) > 0

        for slot in merged_slots:
            
            if len(slot["date_details"]) == num_selected_dates:
                min_available = min((d["available_spots"] for d in slot["date_details"]), default=0)
                default_slots.append(
                    {
                        "start_time": slot["start_time"],
                        "end_time": slot["end_time"],
                        "max_capacity": slot["max_capacity"],
                        "is_full": slot["is_full"],
                        "full_dates": slot["full_dates"],
                        "min_available_spots": min_available,
                    }
                )

        show_custom = has_any_slots and len(default_slots) == 0

        return {
            "status": 200,
            "data": {
                "slots": slots,
                "default_slots": default_slots,
                "show_custom": show_custom,
                "selected_dates": [d.isoformat() for d in selected_dates],
                "session_type": "personal_trainer" if is_personal_trainer else "group",
                "default_capacity": default_capacity,
            }
        }
    
    except FittbotHTTPException:
        raise
    
    except Exception as exc:
        raise FittbotHTTPException(
            status_code=500,
            detail="Failed to fetch slots",
            error_code="SESSION_SLOTS_ERROR",
            log_data={"error": repr(exc), "gym_id": payload.gym_id, "session_id": payload.session_id},
        )


class ReviewRequest(BaseModel):
    gym_id: int
    session_id: int
    trainer_id: Optional[int] = None
    sessions_count: int
    client_id: Optional[int] = None
    is_offer_eligible: bool = False


@router.post("/review")
async def review_session_booking(
    payload: ReviewRequest,
    db: AsyncSession = Depends(get_async_db),
):
    try:
        stmt = select(SessionSetting).where(
            SessionSetting.gym_id == payload.gym_id,
            SessionSetting.session_id == payload.session_id,
            SessionSetting.is_enabled.is_(True),
            SessionSetting.final_price.isnot(None),
        )
        if payload.trainer_id is not None:
            stmt = stmt.where(SessionSetting.trainer_id == payload.trainer_id)
        else:
            stmt = stmt.where(SessionSetting.trainer_id.is_(None))

        setting = (await db.execute(stmt)).scalars().first()
        if not setting or not setting.final_price:
            raise FittbotHTTPException(
                status_code=404,
                detail="Session pricing not available",
                error_code="SESSION_NOT_AVAILABLE",
            )

        base_price = setting.base_price or 0
        discount_percent = setting.discount_percent or 0
        final_price = setting.final_price

        # If offer is eligible, force 99 rupees promo price
        if payload.is_offer_eligible:
            base_price_with_markup = base_price
            final_price_with_markup = 99
        # Skip 30% markup if final_price is exactly 99 rupees
        elif final_price == 99:
            base_price_with_markup = base_price
            final_price_with_markup = 99
        else:
            base_price_with_markup = round(base_price * get_markup_multiplier())
            final_price_with_markup = round(final_price * get_markup_multiplier())

        # Calculate total
        total = final_price_with_markup * payload.sessions_count

        reward_amount = 0

        if payload.client_id:
            # reward logic: min(10% of total, no cap, available cash) - matches session_processor
            # Use integer math: 10% in paise = total_rupees * 10
            ten_percent_minor = total * 10
            cash_row = (
                await db.execute(
                    select(ReferralFittbotCash).where(
                        ReferralFittbotCash.client_id == payload.client_id
                    )
                )
            ).scalars().first()
            available_cash = cash_row.fittbot_cash if cash_row else 0
            available_cash_minor = int(available_cash * 100)
            reward_amount_minor = min(ten_percent_minor, available_cash_minor)
            # Round to nearest rupee to avoid decimal in final payment
            reward_amount = int(round(reward_amount_minor / 100))


        return {
            "status": 200,
            "data": {
                "base_price": base_price_with_markup,
                "discount_percentage": discount_percent,
                "final_price": final_price_with_markup,
                "sessions_count": payload.sessions_count,
                "total": total,
                "is_rewards": reward_amount > 0,
                "rewards": reward_amount if reward_amount > 0 else None,
            },
        }
    except FittbotHTTPException:
        raise
    except Exception as exc:
        raise FittbotHTTPException(
            status_code=500,
            detail="Failed to compute review",
            error_code="SESSION_REVIEW_ERROR",
            log_data={"error": repr(exc), "payload": payload.dict()},
        )


@router.post("/rewards")
async def calculate_rewards(
    request: RewardCalculationRequest, db: AsyncSession = Depends(get_async_db)
):
    try:
        ten_percent = request.amount * 0.10
        capped_reward = min(ten_percent, 100)

        cash_row = (
            await db.execute(
                select(ReferralFittbotCash).where(ReferralFittbotCash.client_id == request.client_id)
            )
        ).scalars().first()
        available_cash = cash_row.fittbot_cash if cash_row else 0
        reward_amount = min(capped_reward, available_cash)

        return {"status": 200, "data": {"is_rewards": reward_amount > 0, "rewards": reward_amount}}
    except Exception as exc:
        raise FittbotHTTPException(
            status_code=500,
            detail="Unable to calculate reward",
            error_code="SESSION_REWARD_ERROR",
            log_data={"error": repr(exc), "client_id": request.client_id, "amount": request.amount},
        )
