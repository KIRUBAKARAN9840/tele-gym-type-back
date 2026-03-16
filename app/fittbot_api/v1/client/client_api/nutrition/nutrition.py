

from datetime import date, datetime, time, timedelta
from typing import List, Optional, Dict, Any

from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy import select, and_, func
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

router = APIRouter(prefix="/client/nutrition", tags=["Client Nutrition"])


def _as_time_str(t: time) -> str:
    """Convert time to 12-hour format string."""
    return datetime.strptime(t.strftime("%H:%M"), "%H:%M").strftime("%I:%M %p")


# ═══════════════════════════════════════════════════════════════════════════════
# RESPONSE MODELS
# ═══════════════════════════════════════════════════════════════════════════════

class EligibilityResponse(BaseModel):
    is_eligible: bool
    total_sessions: int
    used_sessions: int
    remaining_sessions: int
    source_type: Optional[str] = None
    plan_name: Optional[str] = None
    expires_at: Optional[datetime] = None
    has_pending_booking: bool = False
    pending_booking_date: Optional[date] = None
    needs_reschedule: bool = False


class NutritionistInfo(BaseModel):
    id: int
    full_name: str
    profile_image: Optional[str] = None
    specializations: Optional[List[str]] = None
    experience: Optional[float] = None


class SlotInfo(BaseModel):
    schedule_id: int
    start_time: str
    end_time: str
    is_booked: bool


class DateSlotsResponse(BaseModel):
    date: str
    slots: List[SlotInfo]


class BookingRequest(BaseModel):
    client_id:int
    schedule_id: int
    booking_date: date
    eligibility_id:int


class BookingResponse(BaseModel):
    id: int
    booking_date: date
    start_time: str
    end_time: str
    status: str
    nutritionist_name: str
    nutritionist_image: Optional[str] = None


class RescheduleRequest(BaseModel):
    booking_id: int
    new_schedule_id: int
    new_booking_date: date
    reason: Optional[str] = None


# ═══════════════════════════════════════════════════════════════════════════════
# ELIGIBILITY CHECK
# ═══════════════════════════════════════════════════════════════════════════════

@router.get("/eligibility")
async def check_eligibility(
    client_id: int,
    db: AsyncSession = Depends(get_async_db),
) -> Dict[str, Any]:
    """
    Check if a client is eligible for free nutrition consultation sessions.

    Returns eligibility status, remaining sessions, and any pending bookings.
    Also checks if a reschedule is pending (nutritionist requested reschedule).
    """
    try:
        # Get active eligibility with remaining sessions
        eligibility_stmt = (
            select(NutritionEligibility)
            .where(
                NutritionEligibility.client_id == client_id,
                NutritionEligibility.remaining_sessions > 0,
                (NutritionEligibility.expires_at.is_(None) | (NutritionEligibility.expires_at > datetime.now())),
            )
            .order_by(NutritionEligibility.created_at.desc())
        )
        eligibility = (await db.execute(eligibility_stmt)).scalars().first()

        if not eligibility:
            return {
                "status": 200,
                "data": EligibilityResponse(
                    is_eligible=False,
                    total_sessions=0,
                    used_sessions=0,
                    remaining_sessions=0,
                ).dict()
            }

        # Check for pending bookings
        pending_booking_stmt = (
            select(NutritionBooking)
            .where(
                NutritionBooking.client_id == client_id,
                NutritionBooking.eligibility_id == eligibility.id,
                NutritionBooking.status.in_(["booked", "pending"]),
                NutritionBooking.booking_date >= date.today(),
            )
            .order_by(NutritionBooking.booking_date.asc())
        )
        pending_booking = (await db.execute(pending_booking_stmt)).scalars().first()

        # Check if reschedule is needed (nutritionist requested)
        reschedule_needed_stmt = (
            select(NutritionBooking)
            .where(
                NutritionBooking.client_id == client_id,
                NutritionBooking.eligibility_id == eligibility.id,
                NutritionBooking.status == "rescheduled",
                NutritionBooking.reschedule_requested_by == "nutritionist",
                NutritionBooking.booking_date >= date.today(),
            )
        )
        needs_reschedule = (await db.execute(reschedule_needed_stmt)).scalars().first() is not None

        response = EligibilityResponse(
            is_eligible=True,
            total_sessions=eligibility.total_sessions,
            used_sessions=eligibility.used_sessions,
            remaining_sessions=eligibility.remaining_sessions,
            source_type=eligibility.source_type,
            plan_name=eligibility.plan_name,
            expires_at=eligibility.expires_at,
            has_pending_booking=pending_booking is not None,
            pending_booking_date=pending_booking.booking_date if pending_booking else None,
            needs_reschedule=needs_reschedule,
        )

        return {"status": 200, "data": response.dict()}

    except Exception as exc:
        raise FittbotHTTPException(
            status_code=500,
            detail="Failed to check nutrition eligibility",
            error_code="NUTRITION_ELIGIBILITY_ERROR",
            log_data={"error": repr(exc), "client_id": client_id},
        )


# ═══════════════════════════════════════════════════════════════════════════════
# GET NUTRITIONISTS
# ═══════════════════════════════════════════════════════════════════════════════

@router.get("/nutritionists")
async def get_nutritionists(
    db: AsyncSession = Depends(get_async_db),
) -> Dict[str, Any]:
    """
    Get list of active nutritionists.
    For now, returns all active nutritionists (starting with 1 nutritionist).
    """
    try:
        stmt = (
            select(Nutritionist)
            .where(Nutritionist.is_active.is_(True))
            .order_by(Nutritionist.full_name)
        )
        nutritionists = (await db.execute(stmt)).scalars().all()

        data = [
            NutritionistInfo(
                id=n.id,
                full_name=n.full_name,
                profile_image=n.profile_image,
                specializations=n.specializations,
                experience=n.experience,
            ).dict()
            for n in nutritionists
        ]

        return {"status": 200, "data": data}

    except Exception as exc:
        raise FittbotHTTPException(
            status_code=500,
            detail="Failed to fetch nutritionists",
            error_code="NUTRITION_NUTRITIONISTS_ERROR",
            log_data={"error": repr(exc)},
        )


# ═══════════════════════════════════════════════════════════════════════════════
# GET AVAILABLE DATES
# ═══════════════════════════════════════════════════════════════════════════════

@router.get("/dates")
async def get_available_dates(
    db: AsyncSession = Depends(get_async_db),
) -> Dict[str, Any]:

    try:
        today = date.today()
        end_range = today + timedelta(days=30)
        nutritionist_id=1

        # Get all active schedules for this nutritionist
        stmt = (
            select(NutritionSchedule)
            .where(
                NutritionSchedule.nutritionist_id == nutritionist_id,
                NutritionSchedule.is_active.is_(True),
            )
        )
        schedules = (await db.execute(stmt)).scalars().all()

        available_dates = set()
        for sch in schedules:
            # Respect date bounds
            start_bound = sch.start_date or today
            end_bound = sch.end_date or end_range
            range_start = max(today, start_bound)
            range_end = min(end_range, end_bound)

            if range_end < range_start:
                continue

            # Find dates matching weekday
            d = range_start
            while d <= range_end:
                if d.weekday() == sch.weekday:
                    available_dates.add(d)
                d += timedelta(days=1)

        sorted_dates = sorted(list(available_dates))
        formatted = [d.isoformat() for d in sorted_dates]

        print("formatted data is",formatted)

        return {"status": 200, "data":  formatted}

    except Exception as exc:
        raise FittbotHTTPException(
            status_code=500,
            detail="Failed to fetch available dates",
            error_code="NUTRITION_DATES_ERROR",
            log_data={"error": repr(exc), "nutritionist_id": nutritionist_id},
        )





@router.get("/slots")
async def get_slots_for_dates(
    date: date,
    db: AsyncSession = Depends(get_async_db),
):

    try:
        nutritionist_id = 1
        selected_dates = date

        if not selected_dates:
            raise FittbotHTTPException(
                status_code=400,
                detail="At least one date must be provided",
                error_code="NO_DATES_PROVIDED",
            )

        # Get all schedules for the nutritionist
        schedules_stmt = (
            select(NutritionSchedule)
            .where(
                NutritionSchedule.nutritionist_id == nutritionist_id,
                NutritionSchedule.is_active.is_(True),
            )
        )
        schedules = (await db.execute(schedules_stmt)).scalars().all()

        # Get all existing bookings for these dates
        booking_stmt = (
            select(NutritionBooking.schedule_id, NutritionBooking.booking_date)
            .where(
                NutritionBooking.nutritionist_id == nutritionist_id,
                NutritionBooking.booking_date==selected_dates,
                NutritionBooking.status.in_(["booked", "pending", "attended"]),
            )
        )
        booking_rows = (await db.execute(booking_stmt)).all()
        booked_slots = {(row.schedule_id, row.booking_date) for row in booking_rows}

        # Build response by date

        day_slots = []
        for sch in schedules:
            # Check if schedule is valid for this date
            if selected_dates== date.today():
                continue

            if sch.start_date and selected_dates < sch.start_date:
                continue
            if sch.end_date and selected_dates > sch.end_date:
                continue
            if selected_dates.weekday() != sch.weekday:
                continue

            is_booked = (sch.id, selected_dates) in booked_slots

            day_slots.append(
                SlotInfo(
                    schedule_id=sch.id,
                    start_time=_as_time_str(sch.start_time),
                    end_time=_as_time_str(sch.end_time),
                    is_booked=is_booked,
                ).dict()
            )

            # Sort slots by time
            day_slots.sort(key=lambda x: datetime.strptime(x["start_time"], "%I:%M %p"))


        print("result is",day_slots)

        return {"status": 200, "data":  day_slots}

    except FittbotHTTPException:
        raise
    except Exception as exc:
        raise FittbotHTTPException(
            status_code=500,
            detail="Failed to fetch slots",
            error_code="NUTRITION_SLOTS_ERROR",
            log_data={"error": repr(exc), "nutritionist_id": nutritionist_id},
        )


@router.post("/book")
async def book_session(
    payload: BookingRequest,
    db: AsyncSession = Depends(get_async_db),
) -> Dict[str, Any]:

    try:
        nutritionist_id=1
        client_id=payload.client_id

        eligibility_stmt = (
            select(NutritionEligibility)
            .where(
                NutritionEligibility.id == payload.eligibility_id,
                NutritionEligibility.client_id == client_id,
                NutritionEligibility.remaining_sessions > 0,
            )
        )
        eligibility = (await db.execute(eligibility_stmt)).scalars().first()

        if not eligibility:
            raise FittbotHTTPException(
                status_code=400,
                detail="No eligible sessions available",
                error_code="NO_ELIGIBILITY",
            )

        # Verify schedule exists and is active
        schedule_stmt = (
            select(NutritionSchedule)
            .where(
                NutritionSchedule.id == payload.schedule_id,
                NutritionSchedule.nutritionist_id == nutritionist_id,
                NutritionSchedule.is_active.is_(True),
            )
        )
        schedule = (await db.execute(schedule_stmt)).scalars().first()

        if not schedule:
            raise FittbotHTTPException(
                status_code=404,
                detail="Schedule not found or not active",
                error_code="SCHEDULE_NOT_FOUND",
            )

        # Check if slot is already booked
        existing_booking_stmt = (
            select(NutritionBooking)
            .where(
                NutritionBooking.schedule_id == payload.schedule_id,
                NutritionBooking.booking_date == payload.booking_date,
                NutritionBooking.status.in_(["booked", "pending", "attended"]),
            )
        )
        existing_booking = (await db.execute(existing_booking_stmt)).scalars().first()

        if existing_booking:
            raise FittbotHTTPException(
                status_code=409,
                detail="This slot is already booked",
                error_code="SLOT_ALREADY_BOOKED",
            )

        # Check if client already has a booking for this eligibility
        client_booking_stmt = (
            select(NutritionBooking)
            .where(
                NutritionBooking.client_id == client_id,
                NutritionBooking.eligibility_id == payload.eligibility_id,
                NutritionBooking.status.in_(["booked", "pending"]),
                NutritionBooking.booking_date >= date.today(),
            )
        )
        client_existing = (await db.execute(client_booking_stmt)).scalars().first()

        if client_existing:
            raise FittbotHTTPException(
                status_code=409,
                detail="You already have a pending booking. Please attend or reschedule it first.",
                error_code="ALREADY_HAS_BOOKING",
            )

        # Create booking
        booking = NutritionBooking(
            client_id=client_id,
            eligibility_id=payload.eligibility_id,
            nutritionist_id=nutritionist_id,
            schedule_id=payload.schedule_id,
            booking_date=payload.booking_date,
            start_time=schedule.start_time,
            end_time=schedule.end_time,
            status="booked",
        )
        db.add(booking)

        # Note: We don't decrement remaining_sessions until the session is attended

        await db.commit()
        await db.refresh(booking)

        # Get nutritionist info for response
        nutritionist = (await db.execute(
            select(Nutritionist).where(Nutritionist.id == nutritionist_id)
        )).scalars().first()

        response = BookingResponse(
            id=booking.id,
            booking_date=booking.booking_date,
            start_time=_as_time_str(booking.start_time),
            end_time=_as_time_str(booking.end_time),
            status=booking.status,
            nutritionist_name=nutritionist.full_name if nutritionist else "Unknown",
            nutritionist_image=nutritionist.profile_image if nutritionist else None,
        )

        return {"status": 200, "message": "Session booked successfully", "data": response.dict()}

    except FittbotHTTPException:
        raise
    except Exception as exc:
        await db.rollback()
        raise FittbotHTTPException(
            status_code=500,
            detail="Failed to book session",
            error_code="NUTRITION_BOOKING_ERROR",
            log_data={"error": repr(exc), "client_id": client_id},
        )


@router.get("/my-bookings")
async def get_my_bookings(
    client_id: int,
    db: AsyncSession = Depends(get_async_db),
) -> Dict[str, Any]:
    """
    Get all bookings for a client.

    Returns both upcoming and past bookings with nutritionist details.
    """
    try:
        # Get all bookings with nutritionist info
        stmt = (
            select(NutritionBooking, Nutritionist)
            .join(Nutritionist, NutritionBooking.nutritionist_id == Nutritionist.id)
            .where(NutritionBooking.client_id == client_id)
            .order_by(NutritionBooking.booking_date.desc())
        )
        rows = (await db.execute(stmt)).all()

        bookings = []
        for booking, nutritionist in rows:
            bookings.append({
                "id": booking.id,
                "booking_date": booking.booking_date.isoformat(),
                "start_time": _as_time_str(booking.start_time),
                "end_time": _as_time_str(booking.end_time),
                "status": booking.status,
                "nutritionist_name": nutritionist.full_name,
                "nutritionist_image": nutritionist.profile_image,
                "needs_reschedule": booking.status == "rescheduled" and booking.reschedule_requested_by == "nutritionist",
                "reschedule_reason": booking.reschedule_reason,
                "is_upcoming": booking.booking_date >= date.today(),
            })

        return {"status": 200, "data": {"bookings": bookings}}

    except Exception as exc:
        raise FittbotHTTPException(
            status_code=500,
            detail="Failed to fetch bookings",
            error_code="NUTRITION_MY_BOOKINGS_ERROR",
            log_data={"error": repr(exc), "client_id": client_id},
        )


@router.post("/reschedule")
async def reschedule_session(
    client_id: int,
    payload: RescheduleRequest,
    db: AsyncSession = Depends(get_async_db),
) -> Dict[str, Any]:
    """
    Reschedule an existing booking to a new date/time.

    The old booking is marked as 'rescheduled' and a new booking is created.
    """
    try:
        # Get existing booking
        booking_stmt = (
            select(NutritionBooking)
            .where(
                NutritionBooking.id == payload.booking_id,
                NutritionBooking.client_id == client_id,
                NutritionBooking.status.in_(["booked", "pending", "rescheduled"]),
            )
        )
        old_booking = (await db.execute(booking_stmt)).scalars().first()

        if not old_booking:
            raise FittbotHTTPException(
                status_code=404,
                detail="Booking not found or cannot be rescheduled",
                error_code="BOOKING_NOT_FOUND",
            )

        # Verify new schedule exists and is active
        schedule_stmt = (
            select(NutritionSchedule)
            .where(
                NutritionSchedule.id == payload.new_schedule_id,
                NutritionSchedule.nutritionist_id == old_booking.nutritionist_id,
                NutritionSchedule.is_active.is_(True),
            )
        )
        schedule = (await db.execute(schedule_stmt)).scalars().first()

        if not schedule:
            raise FittbotHTTPException(
                status_code=404,
                detail="New schedule not found or not active",
                error_code="NEW_SCHEDULE_NOT_FOUND",
            )

        # Check if new slot is already booked
        existing_booking_stmt = (
            select(NutritionBooking)
            .where(
                NutritionBooking.schedule_id == payload.new_schedule_id,
                NutritionBooking.booking_date == payload.new_booking_date,
                NutritionBooking.status.in_(["booked", "pending", "attended"]),
            )
        )
        existing_booking = (await db.execute(existing_booking_stmt)).scalars().first()

        if existing_booking:
            raise FittbotHTTPException(
                status_code=409,
                detail="New slot is already booked",
                error_code="NEW_SLOT_ALREADY_BOOKED",
            )

        # Mark old booking as rescheduled
        old_booking.status = "rescheduled"
        old_booking.reschedule_reason = payload.reason
        old_booking.reschedule_requested_by = "client"
        db.add(old_booking)

        # Create new booking
        new_booking = NutritionBooking(
            client_id=client_id,
            eligibility_id=old_booking.eligibility_id,
            nutritionist_id=old_booking.nutritionist_id,
            schedule_id=payload.new_schedule_id,
            booking_date=payload.new_booking_date,
            start_time=schedule.start_time,
            end_time=schedule.end_time,
            status="booked",
            rescheduled_from_id=old_booking.id,
        )
        db.add(new_booking)

        await db.commit()
        await db.refresh(new_booking)

        # Get nutritionist info for response
        nutritionist = (await db.execute(
            select(Nutritionist).where(Nutritionist.id == old_booking.nutritionist_id)
        )).scalars().first()

        response = BookingResponse(
            id=new_booking.id,
            booking_date=new_booking.booking_date,
            start_time=_as_time_str(new_booking.start_time),
            end_time=_as_time_str(new_booking.end_time),
            status=new_booking.status,
            nutritionist_name=nutritionist.full_name if nutritionist else "Unknown",
            nutritionist_image=nutritionist.profile_image if nutritionist else None,
        )

        return {"status": 200, "message": "Session rescheduled successfully", "data": response.dict()}

    except FittbotHTTPException:
        raise
    except Exception as exc:
        await db.rollback()
        raise FittbotHTTPException(
            status_code=500,
            detail="Failed to reschedule session",
            error_code="NUTRITION_RESCHEDULE_ERROR",
            log_data={"error": repr(exc), "client_id": client_id, "booking_id": payload.booking_id},
        )


@router.post("/cancel/{booking_id}")
async def cancel_session(
    booking_id: int,
    client_id: int,
    db: AsyncSession = Depends(get_async_db),
) -> Dict[str, Any]:
    """
    Cancel a booked session.

    The session can be rebooked later if eligibility still has remaining sessions.
    """
    try:
        # Get existing booking
        booking_stmt = (
            select(NutritionBooking)
            .where(
                NutritionBooking.id == booking_id,
                NutritionBooking.client_id == client_id,
                NutritionBooking.status.in_(["booked", "pending"]),
            )
        )
        booking = (await db.execute(booking_stmt)).scalars().first()

        if not booking:
            raise FittbotHTTPException(
                status_code=404,
                detail="Booking not found or cannot be cancelled",
                error_code="BOOKING_NOT_FOUND",
            )

        # Cancel booking
        booking.status = "cancelled"
        db.add(booking)

        await db.commit()

        return {"status": 200, "message": "Session cancelled successfully"}

    except FittbotHTTPException:
        raise
    except Exception as exc:
        await db.rollback()
        raise FittbotHTTPException(
            status_code=500,
            detail="Failed to cancel session",
            error_code="NUTRITION_CANCEL_ERROR",
            log_data={"error": repr(exc), "client_id": client_id, "booking_id": booking_id},
        )


@router.get("/join")
async def join_session(
    booking_id: int,
    client_id: int,
    db: AsyncSession = Depends(get_async_db),
) -> Dict[str, Any]:
    """
    Check if client can join a nutrition consultation session.

    join_time logic:
    - True: Only if current time is BETWEEN start_time and end_time on booking_date
    - False: If session hasn't started yet OR session time has passed
    """
    try:
        # Get booking
        booking_stmt = (
            select(NutritionBooking)
            .where(
                NutritionBooking.id == booking_id,
                NutritionBooking.client_id == client_id,
                NutritionBooking.status.in_(["booked", "pending"]),
            )
        )
        booking = (await db.execute(booking_stmt)).scalars().first()

        if not booking:
            raise FittbotHTTPException(
                status_code=404,
                detail="Booking not found or not accessible",
                error_code="BOOKING_NOT_FOUND",
            )

        # Check current date and time
        today = date.today()
        now = datetime.now().time()

        # Check if meeting link exists
        has_meeting_link = booking.meeting_link is not None and booking.meeting_link.strip() != ""

        # Determine join_time status
        is_booking_date = booking.booking_date == today
        session_expired = False
        session_not_started = False
        can_join = False

        if today > booking.booking_date:
            # Date has passed - session expired
            session_expired = True
        elif today < booking.booking_date:
            # Date not yet reached - session not started
            session_not_started = True
        else:
            # Today is the booking date - check time window
            if now < booking.start_time:
                # Before start time
                session_not_started = True
            elif now > booking.end_time:
                # After end time - session expired
                session_expired = True
            else:
                # Within time window (start_time <= now <= end_time)
                can_join = True

        # Build response
        if session_expired:
            return {
                "status": 200,
                "data": {
                    "join_time": False,
                    "meeting_link": has_meeting_link,
                    "link": None,
                    "session_expired": True,
                    "message": "Session time has passed.",
                    "booking_date": booking.booking_date.isoformat(),
                    "start_time": booking.start_time.strftime("%I:%M %p"),
                    "end_time": booking.end_time.strftime("%I:%M %p"),
                }
            }
        elif session_not_started:
            return {
                "status": 200,
                "data": {
                    "join_time": False,
                    "meeting_link": has_meeting_link,
                    "link": None,
                    "session_expired": False,
                    "message": "Session has not started yet. Please join at the scheduled time.",
                    "booking_date": booking.booking_date.isoformat(),
                    "start_time": booking.start_time.strftime("%I:%M %p"),
                    "end_time": booking.end_time.strftime("%I:%M %p"),
                }
            }
        elif can_join and has_meeting_link:
            # Can join - within time window and link available
            return {
                "status": 200,
                "data": {
                    "join_time": True,
                    "meeting_link": True,
                    "link": booking.meeting_link,
                    "session_expired": False,
                    "booking_date": booking.booking_date.isoformat(),
                    "start_time": booking.start_time.strftime("%I:%M %p"),
                    "end_time": booking.end_time.strftime("%I:%M %p"),
                }
            }
        else:
            # Within time window but no link yet
            return {
                "status": 200,
                "data": {
                    "join_time": True,
                    "meeting_link": False,
                    "link": None,
                    "session_expired": False,
                    "message": "Meeting link not yet available. Please wait for the nutritionist to share the link.",
                    "booking_date": booking.booking_date.isoformat(),
                    "start_time": booking.start_time.strftime("%I:%M %p"),
                    "end_time": booking.end_time.strftime("%I:%M %p"),
                }
            }

    except FittbotHTTPException:
        raise
    except Exception as exc:
        raise FittbotHTTPException(
            status_code=500,
            detail="Failed to check join status",
            error_code="NUTRITION_JOIN_ERROR",
            log_data={"error": repr(exc), "client_id": client_id, "booking_id": booking_id},
        )
