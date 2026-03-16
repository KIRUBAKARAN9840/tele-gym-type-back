# Common session settings and bookings (owner-side setup, async)
import uuid
from datetime import date, time, datetime, timedelta
from typing import List, Optional, Literal, Dict, Any, Tuple

from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy import delete, select, func
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.async_database import get_async_db
from app.models.fittbot_models import (
    SessionSetting,
    SessionSchedule,
    SessionBooking,
    ClassSession,
)
from app.utils.logging_utils import FittbotHTTPException
from app.utils.redis_config import get_redis

# Session cache keys (must match gym_studios.py)
SESSION_LOW_SET_KEY = "set:session:low99"
SESSION_REFRESH_KEY = "session:last_refresh"


async def _invalidate_session_cache():
    """Clear all session cache keys to force re-hydration on next client request."""
    try:
        redis = await get_redis()
        await redis.delete(SESSION_LOW_SET_KEY, SESSION_REFRESH_KEY)
    except Exception:
        # Log but don't fail - cache will eventually expire via TTL
        pass


router = APIRouter(prefix="/owner/home", tags=["Gymowner Sessions"])

DAY_MAP: Dict[str, int] = {
    "Mon": 0,
    "Tue": 1,
    "Wed": 2,
    "Thu": 3,
    "Fri": 4,
    "Sat": 5,
    "Sun": 6,
}

REV_DAY_MAP: Dict[int, str] = {v: k for k, v in DAY_MAP.items()}


def _parse_time(slot: str) -> time:
    # Expect "06:00 AM" style
    try:
        return datetime.strptime(slot.strip(), "%I:%M %p").time()
    except Exception:
        raise FittbotHTTPException(
            status_code=400,
            detail=f"Invalid time slot format: {slot}",
            error_code="INVALID_TIME_SLOT",
        )


def _one_hour_after(t: time) -> time:
    dt = datetime.combine(date.today(), t) + timedelta(hours=1)
    return dt.time()


def _build_schedule_rows(payload: "SessionSettingsRequest") -> Tuple[List[SessionSchedule], bool]:
    """
    Build schedule rows from the payload.
    If duration_type=this_week but today is Sat/Sun, shift to next week's Monday and
    flag that shift so frontend can message accordingly.
    """
    today = date.today()
    shifted_to_next_week = False
    if payload.duration_type == "this_week":
        if today.weekday() >= 5:  # Sat (5) or Sun (6) -> shift to next week
            shifted_to_next_week = True
            start_date = today + timedelta(days=(7 - today.weekday()))
            end_date = start_date + timedelta(days=6)
        else:
            days_until_sunday = (6 - today.weekday()) % 7
            start_date = today
            end_date = today + timedelta(days=days_until_sunday)
    else:
        start_date = None
        end_date = None

    rows: List[SessionSchedule] = []
    recurrence_value = "one_off" if payload.duration_type == "this_week" else "weekly"

    def add_rows(day_codes: List[str], slots: List[str]):
        for day_code in day_codes:
            if day_code not in DAY_MAP:
                raise FittbotHTTPException(
                    status_code=400,
                    detail=f"Invalid day: {day_code}",
                    error_code="INVALID_DAY",
                )
            weekday = DAY_MAP[day_code]
            for slot in slots:
                start_t = _parse_time(slot)
                end_t = _one_hour_after(start_t)
                rows.append(
                    SessionSchedule(
                        gym_id=payload.gym_id,
                        session_id=payload.session_id,
                        trainer_id=payload.trainer_id,
                        recurrence=recurrence_value,
                        weekday=weekday,
                        start_time=start_t,
                        end_time=end_t,
                        start_date=start_date,
                        end_date=end_date,
                    )
                )

    if payload.schedule_type == "default":
        day_list = payload.selected_days or []
        slots = payload.time_slots or []
        if not day_list or not slots:
            raise FittbotHTTPException(
                status_code=400,
                detail="selected_days and time_slots are required for default schedule",
                error_code="SCHEDULE_MISSING",
            )
        add_rows(day_list, slots)
    else:
        custom = payload.custom_schedule or {}
        if not custom:
            raise FittbotHTTPException(
                status_code=400,
                detail="custom_schedule is required for custom schedule_type",
                error_code="SCHEDULE_MISSING",
            )
        for day_code, slots in custom.items():
            if not isinstance(slots, list) or not slots:
                raise FittbotHTTPException(
                    status_code=400,
                    detail=f"No time slots provided for {day_code}",
                    error_code="SCHEDULE_MISSING",
                )
            add_rows([day_code], slots)

    return rows, shifted_to_next_week


class SessionSettingsRequest(BaseModel):
    gym_id: int
    session_id: int
    session_name: Optional[str] = None
    session_type: Optional[str] = None  # e.g., "zumba", "personal_training"
    trainer_id: Optional[int] = None  # only for personal training
    enabled: bool = True
    price: Optional[int] = None
    discount_percentage: Optional[float] = 0
    discount_price: Optional[int] = None
    capacity: Optional[int] = None
    booking_lead_minutes: Optional[int] = None
    cancellation_cutoff_minutes: Optional[int] = None
    duration_type: Literal["every_week", "this_week"] = "every_week"
    schedule_type: Literal["default", "custom"] = "default"
    selected_days: Optional[List[str]] = None  # e.g., ["Sun", "Mon"]
    time_slots: Optional[List[str]] = None  # for default schedule
    custom_schedule: Optional[Dict[str, List[str]]] = None  # {"Sun": ["06:00 AM"], ...}


class BookingRequest(BaseModel):
    client_id: int
    gym_id: int
    session_id: int
    booking_date: date
    schedule_id: int
    trainer_id: Optional[int] = None  


@router.post("/set_session")
async def upsert_session_settings(
    payload: SessionSettingsRequest, db: AsyncSession = Depends(get_async_db)
):
    try:
        # compute final price
        final_price = None
        if payload.discount_price is not None:
            final_price = payload.discount_price
        elif payload.price is not None:
            discount = (payload.discount_percentage or 0) / 100
            final_price = int(payload.price * (1 - discount))

        # Upsert SessionSetting
        stmt = select(SessionSetting).where(
            SessionSetting.gym_id == payload.gym_id,
            SessionSetting.session_id == payload.session_id,
            SessionSetting.trainer_id == payload.trainer_id,
        )
        result = await db.execute(stmt)
        setting = result.scalars().first()

        if setting:
            setting.is_enabled = payload.enabled
            setting.base_price = payload.price
            setting.discount_percent = payload.discount_percentage
            setting.final_price = final_price
            setting.capacity = payload.capacity
            setting.booking_lead_minutes = payload.booking_lead_minutes
            setting.cancellation_cutoff_minutes = payload.cancellation_cutoff_minutes
        else:
            setting = SessionSetting(
                gym_id=payload.gym_id,
                session_id=payload.session_id,
                trainer_id=payload.trainer_id,
                is_enabled=payload.enabled,
                base_price=payload.price,
                discount_percent=payload.discount_percentage,
                final_price=final_price,
                capacity=payload.capacity,
                booking_lead_minutes=payload.booking_lead_minutes,
                cancellation_cutoff_minutes=payload.cancellation_cutoff_minutes,
            )
            db.add(setting)

        await db.execute(
            delete(SessionSchedule).where(
                SessionSchedule.gym_id == payload.gym_id,
                SessionSchedule.session_id == payload.session_id,
                SessionSchedule.trainer_id == payload.trainer_id,
            )
        )

        schedules_to_add, shifted_to_next_week = _build_schedule_rows(payload)
        if schedules_to_add:
            db.add_all(schedules_to_add)

        await db.commit()

        await _invalidate_session_cache()

        msg = "Session settings saved"
        if shifted_to_next_week:
            msg = (
                "Session settings saved; start shifted to next week because today is end of week"
            )

        return {
            "status": 200,
            "message": msg,
            "data": {
                "gym_id": payload.gym_id,
                "session_id": payload.session_id,
                "trainer_id": payload.trainer_id,
                "final_price": final_price,
                "schedules_saved": len(schedules_to_add),
                "shifted_to_next_week": shifted_to_next_week,
            },
        }

    except FittbotHTTPException:
        raise
    
    except Exception as exc:  # pragma: no cover - defensive
        await db.rollback()
        raise FittbotHTTPException(
            status_code=500,
            detail="Failed to save session settings",
            error_code="SESSION_SETTINGS_SAVE_ERROR",
            log_data={"error": repr(exc), "gym_id": payload.gym_id, "session_id": payload.session_id},
        )


@router.post("/session-bookings")
async def create_session_booking(
    payload: BookingRequest, db: AsyncSession = Depends(get_async_db)
):
    try:
        # Validate schedule exists and belongs to this gym/session/trainer
        schedule_stmt = select(SessionSchedule).where(
            SessionSchedule.id == payload.schedule_id,
            SessionSchedule.gym_id == payload.gym_id,
            SessionSchedule.session_id == payload.session_id,
            SessionSchedule.trainer_id == payload.trainer_id,
            SessionSchedule.is_active.is_(True),
        )
        schedule_row = (await db.execute(schedule_stmt)).scalars().first()
        if not schedule_row:
            raise FittbotHTTPException(
                status_code=404,
                detail="Schedule not found or inactive",
                error_code="SCHEDULE_NOT_FOUND",
                log_data={
                    "schedule_id": payload.schedule_id,
                    "gym_id": payload.gym_id,
                    "session_id": payload.session_id,
                    "trainer_id": payload.trainer_id,
                },
        )

        # Enforce weekday match and date bounds
        if schedule_row.weekday is not None:
            if payload.booking_date.weekday() != schedule_row.weekday:
                raise FittbotHTTPException(
                    status_code=400,
                    detail="Booking date does not match schedule weekday",
                    error_code="INVALID_BOOKING_DAY",
                )
        if schedule_row.start_date and payload.booking_date < schedule_row.start_date:
            raise FittbotHTTPException(
                status_code=400,
                detail="Booking date before schedule start",
                error_code="INVALID_BOOKING_DATE",
            )
        if schedule_row.end_date and payload.booking_date > schedule_row.end_date:
            raise FittbotHTTPException(
                status_code=400,
                detail="Booking date after schedule end",
                error_code="INVALID_BOOKING_DATE",
            )

        # Capacity resolution: schedule slot_quota > setting capacity > fallback 0 (blocked)
        setting_stmt = select(SessionSetting).where(
            SessionSetting.gym_id == payload.gym_id,
            SessionSetting.session_id == payload.session_id,
            SessionSetting.trainer_id == payload.trainer_id,
        )
        setting_row = (await db.execute(setting_stmt)).scalars().first()

        capacity = schedule_row.slot_quota or (setting_row.capacity if setting_row else None)
        if not capacity or capacity <= 0:
            raise FittbotHTTPException(
                status_code=400,
                detail="No capacity configured for this session",
                error_code="NO_CAPACITY",
            )

        count_stmt = select(func.count(SessionBooking.id)).where(
            SessionBooking.schedule_id == payload.schedule_id,
            SessionBooking.booking_date == payload.booking_date,
            SessionBooking.status.in_(("booked", "attended")),
        )
        booked_count = (await db.execute(count_stmt)).scalar_one()
        if booked_count >= capacity:
            raise FittbotHTTPException(
                status_code=400,
                detail="Slot is full",
                error_code="SLOT_FULL",
            )

        price_paid = setting_row.final_price if setting_row else None
        discount_applied = setting_row.discount_percent if setting_row else None

        booking = SessionBooking(
            client_id=payload.client_id,
            gym_id=payload.gym_id,
            session_id=payload.session_id,
            trainer_id=payload.trainer_id,
            schedule_id=payload.schedule_id,
            booking_date=payload.booking_date,
            status="booked",
            price_paid=price_paid,
            discount_applied=discount_applied,
            checkin_token=uuid.uuid4().hex,
        )
        db.add(booking)
        await db.commit()
        await db.refresh(booking)

        return {
            "status": 200,
            "message": "Booking created",
            "data": {
                "booking_id": booking.id,
                "checkin_token": booking.checkin_token,
                "price_paid": booking.price_paid,
                "discount_applied": booking.discount_applied,
            },
        }

    except FittbotHTTPException:
        raise
    except Exception as exc:  # pragma: no cover - defensive
        await db.rollback()
        raise FittbotHTTPException(
            status_code=500,
            detail="Failed to create booking",
            error_code="SESSION_BOOKING_ERROR",
            log_data={
                "error": repr(exc),
                "gym_id": payload.gym_id,
                "session_id": payload.session_id,
                "schedule_id": payload.schedule_id,
            },
        )


@router.get("/get_sessions")
async def get_session_settings(
    gym_id: int,
    session_id: int,
    trainer_id: Optional[int] = None,
    session_type: Optional[str] = None,
    db: AsyncSession = Depends(get_async_db),
):
    try:
        today = date.today()

        setting_stmt = (
            select(SessionSetting)
            .where(
                SessionSetting.gym_id == gym_id,
                SessionSetting.session_id == session_id,
                SessionSetting.trainer_id == trainer_id,
            )
            .order_by(SessionSetting.id.desc())
        )
        setting_row = (await db.execute(setting_stmt)).scalars().first()

        schedules_stmt = (
            select(SessionSchedule)
            .where(
                SessionSchedule.gym_id == gym_id,
                SessionSchedule.session_id == session_id,
                SessionSchedule.trainer_id == trainer_id,
                SessionSchedule.is_active.is_(True),
                # exclude expired schedules
                ((SessionSchedule.end_date.is_(None)) | (SessionSchedule.end_date >= today)),
            )
            .order_by(SessionSchedule.weekday, SessionSchedule.start_time)
        )
        schedules = (await db.execute(schedules_stmt)).scalars().all()

        session_meta = (
            await db.execute(
                select(ClassSession).where(ClassSession.id == session_id)
            )
        ).scalars().first()

        session_disabled = setting_row is None or not setting_row.is_enabled
        computed_discount_price = (
            setting_row.final_price
            if setting_row and setting_row.final_price is not None
            else None
        )

        schedules_expired = bool(setting_row) and len(schedules) == 0
        day_slots: Dict[int, List[str]] = {}
        
        for sch in schedules:
            if sch.weekday is None:
                continue
            day_slots.setdefault(sch.weekday, []).append(
                datetime.strptime(sch.start_time.strftime("%H:%M"), "%H:%M").strftime("%I:%M %p")
            )
        
        for k, v in day_slots.items():
            day_slots[k] = sorted(list(dict.fromkeys(v)))

        schedule_type = "default"
        selected_days: List[str] = []
        time_slots: List[str] = []
        custom_schedule: Dict[str, List[str]] = {}

        if day_slots:
            slot_sets = list(day_slots.values())
            if len(day_slots) == 1:
               
                schedule_type = "custom"
                for d, slots in day_slots.items():
                    if d in REV_DAY_MAP:
                        custom_schedule[REV_DAY_MAP[d]] = slots
                selected_days = list(custom_schedule.keys())
            else:
                all_same = all(slots == slot_sets[0] for slots in slot_sets)
                if all_same:
                    schedule_type = "default"
                    selected_days = [REV_DAY_MAP[d] for d in day_slots.keys() if d in REV_DAY_MAP]
                    time_slots = slot_sets[0]
                else:
                    schedule_type = "custom"
                    for d, slots in day_slots.items():
                        if d in REV_DAY_MAP:
                            custom_schedule[REV_DAY_MAP[d]] = slots
                    selected_days = list(custom_schedule.keys())

        duration_type = "every_week"
        if schedules and any(sch.start_date is not None or sch.end_date is not None for sch in schedules):
            duration_type = "this_week"

        data = {
            "gym_id": gym_id,
            "session_id": session_id,
            "trainer_id": trainer_id,
            "session_type": session_type,
            "enabled": False if setting_row is None else bool(setting_row.is_enabled),
            "session_disabled": session_disabled,
            "schedules_expired": schedules_expired,
            "price": setting_row.base_price if setting_row else None,
            "discount_percentage": setting_row.discount_percent if setting_row else None,
            "discount_price": computed_discount_price,
            "final_price": setting_row.final_price if setting_row else None,
            "capacity": setting_row.capacity if setting_row else None,
            "duration_type": duration_type,
            "schedule_type": schedule_type,
            "selected_days": selected_days,
            "time_slots": time_slots,
            "custom_schedule": custom_schedule if custom_schedule else None,
            "session_meta": {
                "name": session_meta.name if session_meta else None,
                "image": session_meta.image if session_meta else None,
                "description": session_meta.description if session_meta else None,
                "timing": session_meta.timing if session_meta else None,
            }
            if session_meta
            else None,
        }

        return {"status": 200, "data": data}

    except Exception as exc:  # pragma: no cover - defensive
        raise FittbotHTTPException(
            status_code=500,
            detail="Failed to fetch session settings",
            error_code="SESSION_SETTINGS_FETCH_ERROR",
            log_data={
                "error": repr(exc),
                "gym_id": gym_id,
                "session_id": session_id,
                "trainer_id": trainer_id,
            },
        )


