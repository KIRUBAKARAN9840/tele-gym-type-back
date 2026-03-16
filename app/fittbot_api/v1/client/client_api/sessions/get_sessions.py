from datetime import date, datetime
from typing import List, Optional, Dict, Any

from fastapi import APIRouter, Depends
from sqlalchemy import select, and_, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.async_database import get_async_db
from app.models.fittbot_models import (
    SessionBookingDay,
    SessionPurchase,
    ClassSession,
    Gym,
)
from app.utils.logging_utils import FittbotHTTPException

router = APIRouter(prefix="/client/sessions", tags=["Client Sessions"])


def _format_time(t) -> Optional[str]:
    """Format time object to string like '05:00 PM'."""
    if t is None:
        return None
    if hasattr(t, 'strftime'):
        return t.strftime("%I:%M %p")
    return str(t)


async def _load_gym_info_async(db: AsyncSession, gym_id: int) -> Dict[str, Any]:
    """Async version of _load_gym_info - fetches gym info including address, lat/long, owner mobile"""
    # Get gym basic info + address fields + owner_id
    result = await db.execute(
        text(
            """SELECT g.name, g.location, g.city, g.owner_id,
                      g.door_no, g.building, g.street, g.area, g.state, g.pincode
               FROM gyms g WHERE g.gym_id = :gid"""
        ),
        {"gid": gym_id},
    )
    row = result.one_or_none()

    if not row:
        return {
            "name": f"Gym {gym_id}", "location": None, "city": None,
            "address": None, "latitude": None, "longitude": None, "mobile_number": None
        }

    # Build address dict
    address = {
        "door_no": row[4],
        "building": row[5],
        "street": row[6],
        "area": row[7],
        "city": row[2],
        "state": row[8],
        "pincode": row[9],
    }

    # Get latitude/longitude from gym_location
    loc_result = await db.execute(
        text("SELECT latitude, longitude FROM gym_location WHERE gym_id = :gid"),
        {"gid": gym_id},
    )
    loc_row = loc_result.one_or_none()

    latitude = float(loc_row[0]) if loc_row and loc_row[0] else None
    longitude = float(loc_row[1]) if loc_row and loc_row[1] else None

    # Get owner mobile from gym_owners using owner_id
    mobile_number = None
    if row[3]:  # owner_id
        owner_result = await db.execute(
            text("SELECT contact_number FROM gym_owners WHERE owner_id = :oid"),
            {"oid": row[3]},
        )
        owner_row = owner_result.one_or_none()
        if owner_row:
            mobile_number = owner_row[0]

    return {
        "name": row[0],
        "location": row[1],
        "city": row[2],
        "address": address,
        "latitude": latitude,
        "longitude": longitude,
        "mobile_number": mobile_number,
    }


@router.get("/get_upcoming")
async def get_upcoming_sessions(
    client_id: int,
    db: AsyncSession = Depends(get_async_db),
) -> Dict[str, Any]:

    try:
        today = date.today()

        booking_stmt = (
            select(SessionBookingDay, SessionPurchase)
            .join(SessionPurchase, SessionPurchase.id == SessionBookingDay.purchase_id)
            .where(
                SessionBookingDay.client_id == client_id,
                SessionBookingDay.booking_date >= today,
                SessionBookingDay.status == "booked",
                SessionPurchase.status == "paid",
            )
            .order_by(SessionPurchase.created_at.desc(), SessionBookingDay.booking_date.asc())
        )

        booking_result = await db.execute(booking_stmt)
        bookings = booking_result.all()

        if not bookings:
            return {"status": 200, "data": []}

        # Get unique session_ids to fetch session names
        session_ids = list({b.SessionBookingDay.session_id for b in bookings})
        session_stmt = select(ClassSession).where(ClassSession.id.in_(session_ids))
        session_result = await db.execute(session_stmt)
        sessions_map = {s.id: s for s in session_result.scalars().all()}

        # Group bookings by purchase_id
        purchases_map: Dict[int, Dict[str, Any]] = {}

        for row in bookings:
            booking = row.SessionBookingDay
            purchase = row.SessionPurchase
            purchase_id = booking.purchase_id

            if purchase_id not in purchases_map:
                session_meta = sessions_map.get(booking.session_id)
                session_name = session_meta.internal if session_meta and session_meta.internal else (
                    session_meta.name if session_meta else "Session"
                )

                # Use _load_gym_info_async to get full gym details including address, lat/long, mobile
                gym_info = await _load_gym_info_async(db, booking.gym_id) if booking.gym_id else {}
                gym_name = gym_info.get("name", "Unknown Gym")

                purchases_map[purchase_id] = {
                    "purchase_id": purchase_id,
                    "session_id": booking.session_id,
                    "session_name": "personal_training" if session_name=="personal_training_session" else session_name,
                    "trainer_id": booking.trainer_id,
                    "gym_id": booking.gym_id,
                    "gym_name": gym_name,
                    "address": gym_info.get("address"),
                    "latitude": gym_info.get("latitude"),
                    "longitude": gym_info.get("longitude"),
                    "owner_mobile": gym_info.get("mobile_number"),
                    "purchased_at": purchase.created_at.isoformat() if purchase.created_at else None,
                    "sessions": [],
                }

            # Add this booking to the purchase
            purchases_map[purchase_id]["sessions"].append({
                "booking_id": booking.id,
                "date": booking.booking_date.isoformat(),
                "start_time": _format_time(booking.start_time),
                "end_time": _format_time(booking.end_time),
                "status": booking.status,
                "checkin_token": booking.checkin_token,


            })

        result = list(purchases_map.values())


        return {"status": 200, "data": result}

    except Exception as exc:
        raise FittbotHTTPException(
            status_code=500,
            detail="Failed to fetch upcoming sessions",
            error_code="UPCOMING_SESSIONS_FETCH_ERROR",
            log_data={"error": repr(exc), "client_id": client_id},
        )


@router.get("/get_all_bookings")
async def get_all_session_bookings(
    client_id: int,
    db: AsyncSession = Depends(get_async_db),
) -> Dict[str, Any]:

    try:
        # Get all booking days for this client
        booking_stmt = (
            select(SessionBookingDay, SessionPurchase)
            .join(SessionPurchase, SessionPurchase.id == SessionBookingDay.purchase_id)
            .where(
                SessionBookingDay.client_id == client_id,
                SessionPurchase.status == "paid",
            )
            .order_by(SessionPurchase.created_at.desc(), SessionBookingDay.booking_date.asc())
        )

        booking_result = await db.execute(booking_stmt)
        bookings = booking_result.all()

        if not bookings:
            return {"status": 200, "data": []}

        # Get unique session_ids to fetch session names
        session_ids = list({b.SessionBookingDay.session_id for b in bookings})
        session_stmt = select(ClassSession).where(ClassSession.id.in_(session_ids))
        session_result = await db.execute(session_stmt)
        sessions_map = {s.id: s for s in session_result.scalars().all()}

        # Get unique gym_ids to fetch gym names
        gym_ids = list({b.SessionBookingDay.gym_id for b in bookings if b.SessionBookingDay.gym_id})
        gyms_map = {}
        if gym_ids:
            gym_stmt = select(Gym).where(Gym.gym_id.in_(gym_ids))
            gym_result = await db.execute(gym_stmt)
            gyms_map = {g.gym_id: g for g in gym_result.scalars().all()}

        # Group bookings by purchase_id
        purchases_map: Dict[int, Dict[str, Any]] = {}

        for row in bookings:
            booking = row.SessionBookingDay
            purchase = row.SessionPurchase
            purchase_id = booking.purchase_id

            if purchase_id not in purchases_map:
                session_meta = sessions_map.get(booking.session_id)
                session_name = session_meta.internal if session_meta and session_meta.internal else (
                    session_meta.name if session_meta else "Session"
                )

                gym = gyms_map.get(booking.gym_id)
                gym_name = gym.name if gym else "Unknown Gym"

                purchases_map[purchase_id] = {
                    "purchase_id": purchase_id,
                    "session_id": booking.session_id,
                    "session_name": session_name,
                    "trainer_id": booking.trainer_id,
                    "gym_id": booking.gym_id,
                    "gym_name": gym_name,
                    "purchased_at": purchase.created_at.isoformat() if purchase.created_at else None,
                    "sessions": [],
                }

            # Add this booking to the purchase
            purchases_map[purchase_id]["sessions"].append({
                "booking_id": booking.id,
                "date": booking.booking_date.isoformat(),
                "start_time": _format_time(booking.start_time),
                "end_time": _format_time(booking.end_time),
                "status": booking.status,
                "checkin_token": booking.checkin_token,
            })

        # Convert to list
        result = list(purchases_map.values())

        return {"status": 200, "data": result}

    except Exception as exc:
        raise FittbotHTTPException(
            status_code=500,
            detail="Failed to fetch session bookings",
            error_code="SESSION_BOOKINGS_FETCH_ERROR",
            log_data={"error": repr(exc), "client_id": client_id},
        )
