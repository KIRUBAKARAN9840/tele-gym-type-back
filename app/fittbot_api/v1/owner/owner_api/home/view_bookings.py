# View bookings for owner home - daily pass, PT sessions, other sessions
from datetime import date, datetime, timedelta
from typing import Optional, Dict, Any, List
from collections import defaultdict

from fastapi import APIRouter, Depends, Query
from sqlalchemy import select, func, Integer, cast
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.async_database import get_async_db
from app.models.dailypass_models import DailyPassDay, DailyPass
from app.models.fittbot_models import (
    SessionBookingDay,
    ClassSession,
    TrainerProfile,
    Client,
    NewOffer,
    SessionPurchase,
)
from app.models.fittbot_payments_models import Payment
from app.utils.logging_utils import FittbotHTTPException

router = APIRouter(prefix="/owner/home", tags=["Gymowner"])



DAILY_PASS_SESSION_ID = 1
PT_SESSION_ID = 2


async def _get_offer_status(db: AsyncSession, gym_id: int) -> Dict[str, Any]:
    """Get offer enabled status from new_offer table and calculate remaining counts."""

    # Get offer flags from NewOffer table
    offer_stmt = select(NewOffer).where(NewOffer.gym_id == gym_id)
    offer_result = await db.execute(offer_stmt)
    offer_entry = offer_result.scalars().first()

    dailypass_enabled = bool(offer_entry and offer_entry.dailypass)
    session_enabled = bool(offer_entry and offer_entry.session)

    # Calculate dailypass remaining count (unique users who booked at ₹49)
    dp_remaining = 50  # Default
    if dailypass_enabled:
        dp_stmt = (
            select(func.count(func.distinct(DailyPass.client_id)))
            .select_from(DailyPass)
            .join(DailyPassDay, DailyPassDay.pass_id == DailyPass.id)
            .where(
                DailyPass.gym_id == str(gym_id),
                DailyPass.status != "canceled",
                DailyPassDay.dailypass_price == 49,
            )
        )
        dp_result = await db.execute(dp_stmt)
        dp_count = dp_result.scalar() or 0
        dp_remaining = max(0, 50 - dp_count)

    # Calculate session remaining count (unique users who booked at ₹99)
    session_remaining = 50  # Default
    if session_enabled:
        # Use subquery to get distinct client_ids first
        distinct_clients_subquery = (
            select(SessionPurchase.client_id)
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

        session_stmt = select(func.count(distinct_clients_subquery.c.client_id))
        session_result = await db.execute(session_stmt)
        session_count = session_result.scalar() or 0
        session_remaining = max(0, 50 - session_count)

    return {
        "dailypass_offer": {
            "is_enabled": dailypass_enabled,
            "remaining_count": dp_remaining
        },
        "session_offer": {
            "is_enabled": session_enabled,
            "remaining_count": session_remaining
        }
    }


def _format_time_slot(start_time) -> str:
    """Format start time into slot string like '10:00 - 11:00 AM' (adds 1hr for end time)"""
    if not start_time:
        return ""

    if hasattr(start_time, 'strftime'):
        start_hour = start_time.hour
        start_minute = start_time.minute
    else:
        start_hour = start_time // 3600
        start_minute = (start_time % 3600) // 60

    # Calculate end time (add 1 hour)
    end_hour = (start_hour + 1) % 24
    end_minute = start_minute

    # Determine AM/PM for start and end
    start_period = "AM" if start_hour < 12 else "PM"
    end_period = "AM" if end_hour < 12 else "PM"

    # Convert to 12-hour format
    start_display = start_hour % 12
    if start_display == 0:
        start_display = 12

    end_display = end_hour % 12
    if end_display == 0:
        end_display = 12

    # Format: "10:00 - 11:00 AM" or "11:00 AM - 12:00 PM" if periods differ
    if start_period == end_period:
        return f"{start_display:02d}:{start_minute:02d} - {end_display:02d}:{end_minute:02d} {end_period}"
    else:
        return f"{start_display:02d}:{start_minute:02d} {start_period} - {end_display:02d}:{end_minute:02d} {end_period}"


async def _get_dailypass_bookings(
    db: AsyncSession,
    gym_id: int,
    target_date: date
) -> Dict[str, Any]:
    """Get daily pass booking count + details for a specific date."""
    query = (
        select(
            DailyPassDay.id,
            DailyPassDay.client_id,
            DailyPassDay.created_at,
            Client.name,
            Client.profile,
            Payment.amount_net.label("price"),
            Payment.paid_at.label("paid_at"),
        )
        .select_from(DailyPassDay)
        .outerjoin(
            Payment,
            func.binary(Payment.entitlement_id) == func.binary(DailyPassDay.id)
        )
        .outerjoin(Client, Client.client_id == cast(DailyPassDay.client_id, Integer))
        .where(
            DailyPassDay.gym_id == gym_id,
            DailyPassDay.scheduled_date == target_date,
            DailyPassDay.status.in_(["scheduled", "attended", "available"])
        )
    )

    result = await db.execute(query)
    rows = result.all()

    bookings = []
    for row in rows:
        created_ts = row.paid_at or row.created_at
        price_val = float(row.price) if row.price is not None else None
        bookings.append({
            "id": row.id,
            "client_id": row.client_id,
            "name": row.name,
            "dp": row.profile,
            "created_at": created_ts.isoformat() if created_ts else None,
            "price": price_val,
        })

    return {
        "expectedCount": len(rows),
        "bookings": bookings
    }


async def _get_pt_session_bookings(
    db: AsyncSession,
    gym_id: int,
    target_date: date
) -> List[Dict[str, Any]]:
    """Get PT session bookings grouped by trainer with time slots and details."""
    query = (
        select(
            SessionBookingDay.id,
            SessionBookingDay.trainer_id,
            SessionBookingDay.start_time,
            SessionBookingDay.created_at,
            SessionBookingDay.client_id,
            Client.name,
            Client.profile,
            Payment.amount_net.label("price"),
            Payment.paid_at.label("paid_at")
        )
        .where(
            SessionBookingDay.gym_id == gym_id,
            SessionBookingDay.session_id == PT_SESSION_ID,
            SessionBookingDay.booking_date == target_date,
            SessionBookingDay.status.in_(["booked", "attended"])
        )
        .outerjoin(Client, Client.client_id == SessionBookingDay.client_id)
        .outerjoin(Payment, Payment.booking_day_id == SessionBookingDay.id)
    )
    result = await db.execute(query)
    rows = result.all()

    if not rows:
        return []

    # Get unique trainer IDs
    trainer_ids = list({r.trainer_id for r in rows if r.trainer_id})

    # Fetch trainer names
    trainer_map = {}
    if trainer_ids:
        trainer_query = (
            select(TrainerProfile.trainer_id, TrainerProfile.full_name)
            .where(
                TrainerProfile.gym_id == gym_id,
                TrainerProfile.trainer_id.in_(trainer_ids)
            )
        )
        trainer_result = await db.execute(trainer_query)
        trainer_map = {r.trainer_id: r.full_name for r in trainer_result.all()}

    # Group bookings by trainer
    trainer_bookings: Dict[int, Dict[str, Any]] = defaultdict(lambda: {
        "slot_counts": defaultdict(int),
        "expectedCount": 0,
        "bookings": []
    })

    for row in rows:
        trainer_id = row.trainer_id or 0
        time_slot = _format_time_slot(row.start_time)
        created_ts = row.paid_at or row.created_at
        price_val = float(row.price) if row.price is not None else None

        trainer_bookings[trainer_id]["slot_counts"][time_slot] += 1
        trainer_bookings[trainer_id]["expectedCount"] += 1
        trainer_bookings[trainer_id]["bookings"].append({
            "booking_id": row.id,
            "client_id": row.client_id,
            "name": row.name,
            "dp": row.profile,
            "created_at": created_ts.isoformat() if created_ts else None,
            "price": price_val,
            "time": time_slot
        })

    # Build response
    pt_sessions = []
    for trainer_id, data in trainer_bookings.items():
        slots = [{"time": t, "expectedCount": c} for t, c in sorted(data["slot_counts"].items(), key=lambda x: x[0])]
        pt_sessions.append({
            "id": trainer_id,
            "name": trainer_map.get(trainer_id, "Unknown Trainer"),
            "expectedCount": data["expectedCount"],
            "slots": slots,
            "bookings": data["bookings"]
        })

    return pt_sessions


async def _get_other_session_bookings(
    db: AsyncSession,
    gym_id: int,
    target_date: date
) -> List[Dict[str, Any]]:
    
    query = (
        select(
            SessionBookingDay.id,
            SessionBookingDay.session_id,
            SessionBookingDay.start_time,
            SessionBookingDay.created_at,
            SessionBookingDay.client_id,
            Client.name,
            Client.profile,
            Payment.amount_net.label("price"),
            Payment.paid_at.label("paid_at")
        )
        .where(
            SessionBookingDay.gym_id == gym_id,
            SessionBookingDay.session_id > PT_SESSION_ID,
            SessionBookingDay.booking_date == target_date,
            SessionBookingDay.status.in_(["booked", "attended"])
        )
        .outerjoin(Client, Client.client_id == SessionBookingDay.client_id)
        .outerjoin(Payment, Payment.booking_day_id == SessionBookingDay.id)
    )
    result = await db.execute(query)
    rows = result.all()

    if not rows:
        return []


    session_ids = list({r.session_id for r in rows})


    session_query = (
        select(ClassSession.id, ClassSession.name, ClassSession.image)
        .where(ClassSession.id.in_(session_ids))
    )
    session_result = await db.execute(session_query)
    session_map = {r.id: {"name": r.name, "image": r.image} for r in session_result.all()}


    session_bookings: Dict[int, Dict[str, Any]] = defaultdict(lambda: {
        "slot_counts": defaultdict(int),
        "expectedCount": 0,
        "bookings": []
    })

    for row in rows:
        session_id = row.session_id
        time_slot = _format_time_slot(row.start_time)
        created_ts = row.paid_at or row.created_at
        price_val = float(row.price) if row.price is not None else None

        session_bookings[session_id]["slot_counts"][time_slot] += 1
        session_bookings[session_id]["expectedCount"] += 1
        session_bookings[session_id]["bookings"].append({
            "booking_id": row.id,
            "client_id": row.client_id,
            "name": row.name,
            "dp": row.profile,
            "created_at": created_ts.isoformat() if created_ts else None,
            "price": price_val,
            "time": time_slot
        })

    other_sessions = []
    for session_id, data in session_bookings.items():
        session_info = session_map.get(session_id, {"name": "Unknown Session", "image": None})
        slots = [{"time": t, "expectedCount": c} for t, c in sorted(data["slot_counts"].items(), key=lambda x: x[0])]
        other_sessions.append({
            "id": session_id,
            "name": session_info["name"],
            "icon": session_info["image"],
            "expectedCount": data["expectedCount"],
            "slots": slots,
            "bookings": data["bookings"]
        })

    return other_sessions


async def _get_dailypass_counts_by_date(
    db: AsyncSession,
    gym_id: int,
    start_date: date,
    end_date: date
) -> Dict[date, int]:

    query = (
        select(
            DailyPassDay.scheduled_date,
            func.count(DailyPassDay.id).label("count")
        )
        .where(
            DailyPassDay.gym_id == gym_id,
            DailyPassDay.scheduled_date >= start_date,
            DailyPassDay.scheduled_date <= end_date,
            DailyPassDay.status.in_(["scheduled", "attended", "available"])
        )
        .group_by(DailyPassDay.scheduled_date)
    )
    result = await db.execute(query)
    return {row.scheduled_date: row.count for row in result.all()}


async def _get_pt_counts_by_date(
    db: AsyncSession,
    gym_id: int,
    start_date: date,
    end_date: date
) -> Dict[date, int]:
    
    query = (
        select(
            SessionBookingDay.booking_date,
            func.count(SessionBookingDay.id).label("count")
        )
        .where(
            SessionBookingDay.gym_id == gym_id,
            SessionBookingDay.session_id == PT_SESSION_ID,
            SessionBookingDay.booking_date >= start_date,
            SessionBookingDay.booking_date <= end_date,
            SessionBookingDay.status.in_(["booked", "attended"])
        )
        .group_by(SessionBookingDay.booking_date)
    )
    result = await db.execute(query)
    return {row.booking_date: row.count for row in result.all()}


async def _get_other_session_counts_by_date(
    db: AsyncSession,
    gym_id: int,
    start_date: date,
    end_date: date
) -> Dict[date, int]:
   
    query = (
        select(
            SessionBookingDay.booking_date,
            func.count(SessionBookingDay.id).label("count")
        )
        .where(
            SessionBookingDay.gym_id == gym_id,
            SessionBookingDay.session_id > PT_SESSION_ID,
            SessionBookingDay.booking_date >= start_date,
            SessionBookingDay.booking_date <= end_date,
            SessionBookingDay.status.in_(["booked", "attended"])
        )
        .group_by(SessionBookingDay.booking_date)
    )
    result = await db.execute(query)
    return {row.booking_date: row.count for row in result.all()}


@router.get("/view_bookings_summary")
async def get_view_bookings_summary(
    gym_id: int,
    db: AsyncSession = Depends(get_async_db),
):

    try:
        start_date = date.today()
        end_date = start_date + timedelta(days=30)


        dailypass_counts = await _get_dailypass_counts_by_date(db, gym_id, start_date, end_date)
        pt_counts = await _get_pt_counts_by_date(db, gym_id, start_date, end_date)
        other_counts = await _get_other_session_counts_by_date(db, gym_id, start_date, end_date)


        all_dates = set(dailypass_counts.keys()) | set(pt_counts.keys()) | set(other_counts.keys())


        bookings_by_date = []
        for d in sorted(all_dates):
            total = (
                dailypass_counts.get(d, 0) +
                pt_counts.get(d, 0) +
                other_counts.get(d, 0)
            )
            if total > 0:
                bookings_by_date.append({
                    "date": d.isoformat(),
                    "total_expected_count": total
                })

        return {
            "status": 200,
            "data": bookings_by_date
        }

    except Exception as exc:
        raise FittbotHTTPException(
            status_code=500,
            detail="Failed to fetch bookings summary",
            error_code="BOOKINGS_SUMMARY_ERROR",
            log_data={"gym_id": gym_id, "error": repr(exc)},
        )


@router.get("/view_bookings")
async def get_view_bookings(
    gym_id: int,
    booking_date: Optional[str] = Query(None, description="Date in YYYY-MM-DD format"),
    db: AsyncSession = Depends(get_async_db),
):

    try:

        if booking_date:
            try:
                target_date = datetime.strptime(booking_date, "%Y-%m-%d").date()
            except ValueError:
                raise FittbotHTTPException(
                    status_code=400,
                    detail="Invalid date format. Use YYYY-MM-DD",
                    error_code="INVALID_DATE_FORMAT",
                    log_data={"booking_date": booking_date},
                )
        else:
            target_date = date.today()

    
        dailypass_data = await _get_dailypass_bookings(db, gym_id, target_date)
        pt_sessions = await _get_pt_session_bookings(db, gym_id, target_date)
        other_sessions = await _get_other_session_bookings(db, gym_id, target_date)

        # Get offer status from new_offer table
        offer_status = await _get_offer_status(db, gym_id)


        total_expected_count = dailypass_data.get("expectedCount", 0)
        total_expected_count += sum(pt.get("expectedCount", 0) for pt in pt_sessions)
        total_expected_count += sum(s.get("expectedCount", 0) for s in other_sessions)


        start_date = date.today()
        end_date = start_date + timedelta(days=30)
        
        dailypass_counts = await _get_dailypass_counts_by_date(db, gym_id, start_date, end_date)
        pt_counts = await _get_pt_counts_by_date(db, gym_id, start_date, end_date)
        other_counts = await _get_other_session_counts_by_date(db, gym_id, start_date, end_date)

        all_dates = set(dailypass_counts.keys()) | set(pt_counts.keys()) | set(other_counts.keys())
        next_30_days = []
        for d in sorted(all_dates):
            dp_count = dailypass_counts.get(d, 0)
            pt_count = pt_counts.get(d, 0)
            other_count = other_counts.get(d, 0)
            total = dp_count + pt_count + other_count
            if total > 0:
                next_30_days.append({
                    "date": d.isoformat(),
                    "total_expected_count": total,
                    "dailypass_count": dp_count,
                    "pt_count": pt_count,
                    "other_count": other_count
                })

        return {
            "status": 200,
            "data": {
                "date": target_date.isoformat(),
                "total_expected_count": total_expected_count,
                "dailypass": dailypass_data,
                "pt_sessions": pt_sessions,
                "all_sessions": other_sessions,
                "next_30_days": next_30_days,
                "dailypass_offer": offer_status["dailypass_offer"],
                "session_offer": offer_status["session_offer"]
            }
        }

    except FittbotHTTPException:
        raise

    except Exception as exc:
        raise FittbotHTTPException(
            status_code=500,
            detail="Failed to fetch bookings",
            error_code="BOOKINGS_FETCH_ERROR",
            log_data={"gym_id": gym_id, "booking_date": booking_date, "error": repr(exc)},
        )
