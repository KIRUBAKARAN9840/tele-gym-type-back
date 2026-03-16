"""
Nutritionist Admin APIs.

This module provides APIs for nutritionists to:
- View their scheduled bookings
- Mark sessions as attended
- Request reschedule for sessions
- Manage their availability schedules
"""

from datetime import date, datetime, time, timedelta
from typing import List, Optional, Dict, Any

from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy import select, and_, func, or_
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.async_database import get_async_db
from app.models.fittbot_models import Client
from app.models.nutrition_models import (
    Nutritionist,
    NutritionSchedule,
    NutritionEligibility,
    NutritionBooking,
)
from app.utils.logging_utils import FittbotHTTPException

router = APIRouter(prefix="/nutritionist", tags=["Nutritionist Admin"])


def _as_time_str(t: time) -> str:
    """Convert time to 12-hour format string."""
    return datetime.strptime(t.strftime("%H:%M"), "%H:%M").strftime("%I:%M %p")



class NutritionistLogin(BaseModel):
    contact: str


class ScheduleCreate(BaseModel):
    weekday: int  # 0=Monday, 6=Sunday
    start_time: str  # HH:MM format
    end_time: str  # HH:MM format
    start_date: Optional[date] = None
    end_date: Optional[date] = None


class ScheduleUpdate(BaseModel):
    schedule_id: int
    is_active: Optional[bool] = None
    start_time: Optional[str] = None
    end_time: Optional[str] = None
    start_date: Optional[date] = None
    end_date: Optional[date] = None


class RescheduleRequest(BaseModel):
    booking_id: int
    reason: str


class AttendanceRequest(BaseModel):
    booking_id: int
    summary: Optional[str] = None


@router.post("/login")
async def nutritionist_login(
    payload: NutritionistLogin,
    db: AsyncSession = Depends(get_async_db),
) -> Dict[str, Any]:

    try:
        stmt = (
            select(Nutritionist)
            .where(
                Nutritionist.contact == payload.contact,
                Nutritionist.is_active.is_(True),
            )
        )
        nutritionist = (await db.execute(stmt)).scalars().first()

        if not nutritionist:
            raise FittbotHTTPException(
                status_code=404,
                detail="Nutritionist not found or inactive",
                error_code="NUTRITIONIST_NOT_FOUND",
            )

        return {
            "status": 200,
            "data": {
                "id": nutritionist.id,
                "full_name": nutritionist.full_name,
                "contact": nutritionist.contact,
                "email": nutritionist.email,
                "profile_image": nutritionist.profile_image,
                "specializations": nutritionist.specializations,
            }
        }

    except FittbotHTTPException:
        raise
    except Exception as exc:
        raise FittbotHTTPException(
            status_code=500,
            detail="Login failed",
            error_code="NUTRITIONIST_LOGIN_ERROR",
            log_data={"error": repr(exc)},
        )


@router.get("/bookings")
async def get_bookings(
    nutritionist_id: int,
    status_filter: Optional[str] = None,  # "upcoming", "past", "all"
    db: AsyncSession = Depends(get_async_db),
) -> Dict[str, Any]:
    """
    Get all bookings for a nutritionist.
    """
    try:
        today = date.today()

        # Base query
        stmt = (
            select(NutritionBooking, Client)
            .join(Client, NutritionBooking.client_id == Client.client_id)
            .where(NutritionBooking.nutritionist_id == nutritionist_id)
        )

        # Apply status filter
        if status_filter == "upcoming":
            stmt = stmt.where(
                NutritionBooking.booking_date >= today,
                NutritionBooking.status.in_(["booked", "pending"]),
            )
        elif status_filter == "past":
            stmt = stmt.where(
                or_(
                    NutritionBooking.booking_date < today,
                    NutritionBooking.status.in_(["attended", "no_show", "cancelled"]),
                )
            )

        stmt = stmt.order_by(NutritionBooking.booking_date.desc(), NutritionBooking.start_time.asc())

        rows = (await db.execute(stmt)).all()

        bookings = []
        for booking, client in rows:
            bookings.append({
                "id": booking.id,
                "client_id": client.client_id,
                "client_name": client.name,
                "client_contact": client.contact,
                "client_profile": client.profile,
                "booking_date": booking.booking_date.isoformat(),
                "start_time": _as_time_str(booking.start_time),
                "end_time": _as_time_str(booking.end_time),
                "status": booking.status,
                "notes": booking.notes,
                "consultation_summary": booking.consultation_summary,
                "is_upcoming": booking.booking_date >= today and booking.status in ["booked", "pending"],
            })

        return {"status": 200, "data": {"bookings": bookings}}

    except Exception as exc:
        raise FittbotHTTPException(
            status_code=500,
            detail="Failed to fetch bookings",
            error_code="NUTRITIONIST_BOOKINGS_ERROR",
            log_data={"error": repr(exc), "nutritionist_id": nutritionist_id},
        )



@router.get("/today")
async def get_today_bookings(
    nutritionist_id: int,
    db: AsyncSession = Depends(get_async_db),
) -> Dict[str, Any]:
    """
    Get today's bookings for a nutritionist.
    """
    try:
        today = date.today()

        stmt = (
            select(NutritionBooking, Client)
            .join(Client, NutritionBooking.client_id == Client.client_id)
            .where(
                NutritionBooking.nutritionist_id == nutritionist_id,
                NutritionBooking.booking_date == today,
                NutritionBooking.status.in_(["booked", "pending"]),
            )
            .order_by(NutritionBooking.start_time.asc())
        )

        rows = (await db.execute(stmt)).all()

        bookings = []
        for booking, client in rows:
            bookings.append({
                "id": booking.id,
                "client_id": client.client_id,
                "client_name": client.name,
                "client_contact": client.contact,
                "client_profile": client.profile,
                "start_time": _as_time_str(booking.start_time),
                "end_time": _as_time_str(booking.end_time),
                "status": booking.status,
            })

        return {
            "status": 200,
            "data": {
                "date": today.isoformat(),
                "bookings_count": len(bookings),
                "bookings": bookings
            }
        }

    except Exception as exc:
        raise FittbotHTTPException(
            status_code=500,
            detail="Failed to fetch today's bookings",
            error_code="NUTRITIONIST_TODAY_ERROR",
            log_data={"error": repr(exc), "nutritionist_id": nutritionist_id},
        )


# ═══════════════════════════════════════════════════════════════════════════════
# MARK SESSION AS ATTENDED
# ═══════════════════════════════════════════════════════════════════════════════

@router.post("/mark-attended")
async def mark_session_attended(
    nutritionist_id: int,
    payload: AttendanceRequest,
    db: AsyncSession = Depends(get_async_db),
) -> Dict[str, Any]:
    """
    Mark a nutrition session as attended.
    Decrements the client's remaining sessions.
    """
    try:
        # Get booking
        booking_stmt = (
            select(NutritionBooking)
            .where(
                NutritionBooking.id == payload.booking_id,
                NutritionBooking.nutritionist_id == nutritionist_id,
                NutritionBooking.status == "booked",
            )
        )
        booking = (await db.execute(booking_stmt)).scalars().first()

        if not booking:
            raise FittbotHTTPException(
                status_code=404,
                detail="Booking not found or cannot be marked as attended",
                error_code="BOOKING_NOT_FOUND",
            )

        # Update booking status
        booking.status = "attended"
        booking.consultation_summary = payload.summary
        db.add(booking)

        # Decrement remaining sessions in eligibility
        eligibility_stmt = (
            select(NutritionEligibility)
            .where(NutritionEligibility.id == booking.eligibility_id)
        )
        eligibility = (await db.execute(eligibility_stmt)).scalars().first()

        if eligibility and eligibility.remaining_sessions > 0:
            eligibility.used_sessions += 1
            eligibility.remaining_sessions -= 1
            db.add(eligibility)

        await db.commit()

        return {
            "status": 200,
            "message": "Session marked as attended",
            "data": {
                "remaining_sessions": eligibility.remaining_sessions if eligibility else 0,
            }
        }

    except FittbotHTTPException:
        raise
    except Exception as exc:
        await db.rollback()
        raise FittbotHTTPException(
            status_code=500,
            detail="Failed to mark session as attended",
            error_code="NUTRITIONIST_ATTENDANCE_ERROR",
            log_data={"error": repr(exc), "booking_id": payload.booking_id},
        )


# ═══════════════════════════════════════════════════════════════════════════════
# REQUEST RESCHEDULE
# ═══════════════════════════════════════════════════════════════════════════════

@router.post("/request-reschedule")
async def request_reschedule(
    nutritionist_id: int,
    payload: RescheduleRequest,
    db: AsyncSession = Depends(get_async_db),
) -> Dict[str, Any]:
    """
    Nutritionist requests reschedule for a booking.
    The client will see a reschedule prompt on their app.
    """
    try:
        # Get booking
        booking_stmt = (
            select(NutritionBooking)
            .where(
                NutritionBooking.id == payload.booking_id,
                NutritionBooking.nutritionist_id == nutritionist_id,
                NutritionBooking.status == "booked",
            )
        )
        booking = (await db.execute(booking_stmt)).scalars().first()

        if not booking:
            raise FittbotHTTPException(
                status_code=404,
                detail="Booking not found or cannot be rescheduled",
                error_code="BOOKING_NOT_FOUND",
            )

        # Mark as rescheduled by nutritionist
        booking.status = "rescheduled"
        booking.reschedule_reason = payload.reason
        booking.reschedule_requested_by = "nutritionist"
        db.add(booking)

        await db.commit()

        return {
            "status": 200,
            "message": "Reschedule request sent to client",
        }

    except FittbotHTTPException:
        raise
    except Exception as exc:
        await db.rollback()
        raise FittbotHTTPException(
            status_code=500,
            detail="Failed to request reschedule",
            error_code="NUTRITIONIST_RESCHEDULE_ERROR",
            log_data={"error": repr(exc), "booking_id": payload.booking_id},
        )


# ═══════════════════════════════════════════════════════════════════════════════
# MARK AS NO SHOW
# ═══════════════════════════════════════════════════════════════════════════════

@router.post("/mark-no-show/{booking_id}")
async def mark_no_show(
    booking_id: int,
    nutritionist_id: int,
    db: AsyncSession = Depends(get_async_db),
) -> Dict[str, Any]:
    """
    Mark a session as no-show when client doesn't attend.
    The session is counted as used.
    """
    try:
        # Get booking
        booking_stmt = (
            select(NutritionBooking)
            .where(
                NutritionBooking.id == booking_id,
                NutritionBooking.nutritionist_id == nutritionist_id,
                NutritionBooking.status == "booked",
            )
        )
        booking = (await db.execute(booking_stmt)).scalars().first()

        if not booking:
            raise FittbotHTTPException(
                status_code=404,
                detail="Booking not found",
                error_code="BOOKING_NOT_FOUND",
            )

        # Mark as no-show
        booking.status = "no_show"
        db.add(booking)

        # Decrement remaining sessions (no-show counts as used)
        eligibility_stmt = (
            select(NutritionEligibility)
            .where(NutritionEligibility.id == booking.eligibility_id)
        )
        eligibility = (await db.execute(eligibility_stmt)).scalars().first()

        if eligibility and eligibility.remaining_sessions > 0:
            eligibility.used_sessions += 1
            eligibility.remaining_sessions -= 1
            db.add(eligibility)

        await db.commit()

        return {"status": 200, "message": "Marked as no-show"}

    except FittbotHTTPException:
        raise
    except Exception as exc:
        await db.rollback()
        raise FittbotHTTPException(
            status_code=500,
            detail="Failed to mark as no-show",
            error_code="NUTRITIONIST_NO_SHOW_ERROR",
            log_data={"error": repr(exc), "booking_id": booking_id},
        )


# ═══════════════════════════════════════════════════════════════════════════════
# MANAGE SCHEDULES
# ═══════════════════════════════════════════════════════════════════════════════

@router.get("/schedules")
async def get_schedules(
    nutritionist_id: int,
    db: AsyncSession = Depends(get_async_db),
) -> Dict[str, Any]:
    """
    Get all schedules for a nutritionist.
    """
    try:
        stmt = (
            select(NutritionSchedule)
            .where(NutritionSchedule.nutritionist_id == nutritionist_id)
            .order_by(NutritionSchedule.weekday, NutritionSchedule.start_time)
        )
        schedules = (await db.execute(stmt)).scalars().all()

        weekday_names = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]

        data = [
            {
                "id": s.id,
                "weekday": s.weekday,
                "weekday_name": weekday_names[s.weekday],
                "start_time": _as_time_str(s.start_time),
                "end_time": _as_time_str(s.end_time),
                "is_active": s.is_active,
                "start_date": s.start_date.isoformat() if s.start_date else None,
                "end_date": s.end_date.isoformat() if s.end_date else None,
            }
            for s in schedules
        ]

        return {"status": 200, "data": {"schedules": data}}

    except Exception as exc:
        raise FittbotHTTPException(
            status_code=500,
            detail="Failed to fetch schedules",
            error_code="NUTRITIONIST_SCHEDULES_ERROR",
            log_data={"error": repr(exc), "nutritionist_id": nutritionist_id},
        )


@router.post("/schedules")
async def create_schedule(
    nutritionist_id: int,
    payload: ScheduleCreate,
    db: AsyncSession = Depends(get_async_db),
) -> Dict[str, Any]:
    """
    Create a new schedule slot for a nutritionist.
    """
    try:
        # Parse times
        start_time = datetime.strptime(payload.start_time, "%H:%M").time()
        end_time = datetime.strptime(payload.end_time, "%H:%M").time()

        schedule = NutritionSchedule(
            nutritionist_id=nutritionist_id,
            weekday=payload.weekday,
            start_time=start_time,
            end_time=end_time,
            is_active=True,
            start_date=payload.start_date,
            end_date=payload.end_date,
        )
        db.add(schedule)
        await db.commit()
        await db.refresh(schedule)

        return {
            "status": 200,
            "message": "Schedule created successfully",
            "data": {"schedule_id": schedule.id}
        }

    except Exception as exc:
        await db.rollback()
        raise FittbotHTTPException(
            status_code=500,
            detail="Failed to create schedule",
            error_code="NUTRITIONIST_SCHEDULE_CREATE_ERROR",
            log_data={"error": repr(exc), "nutritionist_id": nutritionist_id},
        )


@router.put("/schedules")
async def update_schedule(
    nutritionist_id: int,
    payload: ScheduleUpdate,
    db: AsyncSession = Depends(get_async_db),
) -> Dict[str, Any]:
    """
    Update an existing schedule slot.
    """
    try:
        stmt = (
            select(NutritionSchedule)
            .where(
                NutritionSchedule.id == payload.schedule_id,
                NutritionSchedule.nutritionist_id == nutritionist_id,
            )
        )
        schedule = (await db.execute(stmt)).scalars().first()

        if not schedule:
            raise FittbotHTTPException(
                status_code=404,
                detail="Schedule not found",
                error_code="SCHEDULE_NOT_FOUND",
            )

        if payload.is_active is not None:
            schedule.is_active = payload.is_active
        if payload.start_time:
            schedule.start_time = datetime.strptime(payload.start_time, "%H:%M").time()
        if payload.end_time:
            schedule.end_time = datetime.strptime(payload.end_time, "%H:%M").time()
        if payload.start_date is not None:
            schedule.start_date = payload.start_date
        if payload.end_date is not None:
            schedule.end_date = payload.end_date

        db.add(schedule)
        await db.commit()

        return {"status": 200, "message": "Schedule updated successfully"}

    except FittbotHTTPException:
        raise
    except Exception as exc:
        await db.rollback()
        raise FittbotHTTPException(
            status_code=500,
            detail="Failed to update schedule",
            error_code="NUTRITIONIST_SCHEDULE_UPDATE_ERROR",
            log_data={"error": repr(exc), "schedule_id": payload.schedule_id},
        )


@router.delete("/schedules/{schedule_id}")
async def delete_schedule(
    schedule_id: int,
    nutritionist_id: int,
    db: AsyncSession = Depends(get_async_db),
) -> Dict[str, Any]:
    """
    Delete a schedule slot (soft delete by setting is_active=False).
    """
    try:
        stmt = (
            select(NutritionSchedule)
            .where(
                NutritionSchedule.id == schedule_id,
                NutritionSchedule.nutritionist_id == nutritionist_id,
            )
        )
        schedule = (await db.execute(stmt)).scalars().first()

        if not schedule:
            raise FittbotHTTPException(
                status_code=404,
                detail="Schedule not found",
                error_code="SCHEDULE_NOT_FOUND",
            )

        schedule.is_active = False
        db.add(schedule)
        await db.commit()

        return {"status": 200, "message": "Schedule deactivated successfully"}

    except FittbotHTTPException:
        raise
    except Exception as exc:
        await db.rollback()
        raise FittbotHTTPException(
            status_code=500,
            detail="Failed to delete schedule",
            error_code="NUTRITIONIST_SCHEDULE_DELETE_ERROR",
            log_data={"error": repr(exc), "schedule_id": schedule_id},
        )


