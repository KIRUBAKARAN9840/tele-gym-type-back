from __future__ import annotations

import base64
import hashlib
import hmac
import logging
import secrets
import time
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from fastapi import APIRouter, Depends, HTTPException, Query, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel, Field
from sqlalchemy import text, select, func
from sqlalchemy.orm import Session
from sqlalchemy.ext.asyncio import AsyncSession

from app.utils.logging_utils import FittbotHTTPException
from app.config.pricing import get_markup_multiplier
from app.models.database import get_db
from app.models.async_database import get_async_db
from app.fittbot_api.v1.payments.config.settings import get_payment_settings
from app.fittbot_api.v1.payments.models.orders import Order
from app.fittbot_api.v1.payments.models.orders import OrderItem
from app.fittbot_api.v1.payments.models.enums import StatusOrder, StatusPayment, ItemType
from app.fittbot_api.v1.payments.models.payments import Payment
from app.fittbot_api.v1.payments.Fittbot_Subscriptions_concurrent.config import get_high_concurrency_config
from app.fittbot_api.v1.payments.Fittbot_Subscriptions_concurrent.stores.command_store import CommandStore
from app.fittbot_api.v1.payments.Fittbot_Subscriptions_concurrent.schemas import CommandStatus
from app.utils.redis_config import get_redis_sync

# daily-pass DB (separate schema)
from app.models.dailypass_models import (
    get_dailypass_session,
    DailyPass,
    DailyPassDay,
    DailyPassAudit,
    LedgerAllocation,
    get_price_for_gym,
    get_actual_price_for_gym,
    get_price_for_gym_async,
)

# FittbotPayment for updating entitlement_id on reschedule
from app.models.fittbot_payments_models import Payment as FittbotPayment

# RP helpers you already use elsewhere
from app.fittbot_api.v1.payments.dailypass.rp_client import create_order as rzp_create_order, get_payment as rzp_get_payment, get_order as rzp_get_order

logger = logging.getLogger("api.dailypass.user_passes")

router = APIRouter(prefix="/get_dailypass", tags=["Daily Pass - User"])

security = HTTPBearer(auto_error=False)

UTC = timezone.utc
IST = timezone(timedelta(hours=5, minutes=30))

# ------------------------------- helpers ------------------------------------


def _now_ist() -> datetime:
    return datetime.now(IST)


def _new_id(prefix: str) -> str:
    ts = int(time.time() * 1000)
    return f"{prefix}{ts}_{secrets.token_hex(3)}"


def _mask(s: Optional[str]) -> str:
    if not s:
        return ""
    return f"{s[:4]}...{s[-4:]}" if len(s) > 8 else "***"




def _load_gym_info(db: Session, gym_id: int) -> Dict[str, Any]:
    # Get gym basic info + address fields + owner_id
    row = db.execute(
        text(
            """SELECT g.name, g.location, g.city, g.owner_id,
                      g.door_no, g.building, g.street, g.area, g.state, g.pincode
               FROM gyms g WHERE g.gym_id = :gid"""
        ),
        {"gid": gym_id},
    ).one_or_none()

    if not row:
        return {
            "name": f"Gym {gym_id}", "location": None, "city": None,
            "address": None, "latitude": None, "longitude": None, "owner_mobile": None
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
    loc_row = db.execute(
        text("SELECT latitude, longitude FROM gym_location WHERE gym_id = :gid"),
        {"gid": gym_id},
    ).one_or_none()

    latitude = float(loc_row[0]) if loc_row and loc_row[0] else None
    longitude = float(loc_row[1]) if loc_row and loc_row[1] else None

    # Get owner mobile from gym_owners using owner_id
    owner_mobile = None
    if row[3]:  # owner_id
        owner_row = db.execute(
            text("SELECT contact_number FROM gym_owners WHERE owner_id = :oid"),
            {"oid": row[3]},
        ).one_or_none()
        if owner_row:
            owner_mobile = owner_row[0]

    return {
        "name": row[0],
        "location": row[1],
        "city": row[2],
        "address": address,
        "latitude": latitude,
        "longitude": longitude,
        "owner_mobile": owner_mobile,
    }


async def _load_gym_info_async(db: AsyncSession, gym_id: int) -> Dict[str, Any]:
    """Async version of _load_gym_info"""
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
            "address": None, "latitude": None, "longitude": None, "owner_mobile": None
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
    owner_mobile = None
    if row[3]:  # owner_id
        owner_result = await db.execute(
            text("SELECT contact_number FROM gym_owners WHERE owner_id = :oid"),
            {"oid": row[3]},
        )
        owner_row = owner_result.one_or_none()
        if owner_row:
            owner_mobile = owner_row[0]

    return {
        "name": row[0],
        "location": row[1],
        "city": row[2],
        "address": address,
        "latitude": latitude,
        "longitude": longitude,
        "owner_mobile": owner_mobile,
    }


def _once_used(db: Session, pass_id: str, action: str) -> bool:
    return (
        db.query(DailyPassAudit.id)
        .filter(DailyPassAudit.pass_id == pass_id, DailyPassAudit.action == action)
        .limit(1)
        .first()
        is not None
    )


async def _once_used_async(db: AsyncSession, pass_id: str, action: str) -> bool:
    """Async version of _once_used"""
    result = await db.execute(
        select(DailyPassAudit.id)
        .where(DailyPassAudit.pass_id == pass_id, DailyPassAudit.action == action)
        .limit(1)
    )
    return result.scalars().first() is not None


def _hmac_qr(secret: str, payload: str) -> str:
    return hmac.new(secret.encode("utf-8"), payload.encode("utf-8"), hashlib.sha256).hexdigest()


def _today_ist() -> date:
    return _now_ist().date()


# ------------------------------- schemas ------------------------------------


class GymAddress(BaseModel):
    door_no: Optional[str] = None
    building: Optional[str] = None
    street: Optional[str] = None
    area: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    pincode: Optional[str] = None


class PassSummary(BaseModel):
    pass_id: str
    gym_id: int
    amount: float
    gym_name: Optional[str]
    locality: Optional[str] = None
    city: Optional[str] = None
    valid_from: str
    valid_until: str
    days_total: int
    selected_time: Optional[str] = None
    remaining_days: int
    next_dates: List[str]
    can_reschedule: bool
    can_upgrade: bool
    is_edited: bool = False
    actual_days: Optional[List[str]] = None
    rescheduled_days: Optional[List[str]] = None
    is_upgraded: bool = False
    old_gym_id: Optional[int] = None
    old_gym_name: Optional[str] = None
    # New fields
    address: Optional[GymAddress] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    owner_mobile: Optional[str] = None


class ListActiveResponse(BaseModel):
    client_id: str
    passes: List[PassSummary]


class RescheduleRequest(BaseModel):
    pass_id:str
    client_id:int
    new_start_date: str = Field(..., description="YYYY-MM-DD (consecutive range will be applied)")
    topup_payment_id: Optional[str] = None
    topup_command_id: Optional[str] = None
    paid: Optional[bool] = False



class RescheduleResponse(BaseModel):
    pass_id: str
    old_range: Tuple[str, str]
    new_range: Tuple[str, str]
    days_moved: int
    message: str
    actual_paid: int
    new_paid: int
    selected_time: str


class UpgradePreviewRequest(BaseModel):
    new_gym_id: int
    pass_id: str
    client_id: int


class UpgradePreviewResponse(BaseModel):

    pass_id: str
    old_gym_id: int
    new_gym_id: int
    old_gym_name: str
    new_gym_name: str
    original_dates: dict  # {"from": "2024-09-20", "to": "2024-09-28"}
    upgradeable_dates: dict  # {"from": "2024-09-25", "to": "2024-09-28"}
    total_days: int  # 9 (original total)
    upgradeable_days: int  # 4 (future days)
    old_price_per_day: float  # 50.0
    new_price_per_day: float  # 300.0
    price_difference_per_day: float  # 250.0
    total_upgrade_cost: float  # 1000.0 (250 * 4)
    currency: str = "INR"
    can_upgrade: bool
    new_paid:int
    actual_paid:int
    selected_time:str
    reason: Optional[str] = None  # If can't upgrade, reason why





class UpgradeInitiateRequest(BaseModel):
    new_gym_id: int
    pass_id: str
    client_id: int
    remaining_days_count: int
    delta_minor: int


class UpgradeInitiateResponse(BaseModel):
    pass_id: str
    old_gym_id: int
    new_gym_id: int
    remaining_days: int
    amount: int
    razorpay_order_id: str
    razorpay_key_id: str
    currency: str = "INR"
    description: str = "Proceed to pay delta"
    actual_paid: int
    new_paid: int
    selected_time: str



class UpgradeVerifyRequest(BaseModel):
    razorpay_order_id: str
    razorpay_payment_id: str
    razorpay_signature: str
    pass_id: str


class UpgradeVerifyResponse(BaseModel):
    pass_id: str
    old_gym_id: int
    new_gym_id: int
    remaining_days: int
    delta_minor: int
    payment_id: str
    updated: bool
    message: str
    actual_paid: int
    new_paid: int
    selected_time: str


class QrPayloadResponse(BaseModel):
    pass_id: str
    pass_day_id: str
    gym_id: int
    date: str
    client_id: str
    sig: str


# ------------------------- GET /dailypass/my-active --------------------------


@router.get("/all", response_model=ListActiveResponse)
async def list_active_passes(
    client_id: int,
    db: AsyncSession = Depends(get_async_db)
):

    today = _today_ist()

    try:
        # Get all passes for the client
        all_rows_result = await db.execute(
            select(DailyPass)
            .where(
                DailyPass.client_id == client_id,
                today <= DailyPass.valid_until
            )
            .order_by(DailyPass.created_at.desc())
        )
        all_rows = all_rows_result.scalars().all()

        # Filter logic: HIDE original passes that have been upgraded
        # Only show upgraded passes (which now include today's pass with old gym + future pass with new gym)
        upgraded_passes = [p for p in all_rows if p.status == "upgraded"]
        original_pass_ids = {p.order_id for p in upgraded_passes if p.order_id}

        # Simply exclude original passes - upgraded passes handle everything now
        rows = [p for p in all_rows if p.id not in original_pass_ids]

        resp: List[PassSummary] = []
        for p in rows:
            # Count remaining days based on DailyPassDay records
            remaining_result = await db.execute(
                select(func.count(DailyPassDay.id))
                .where(
                    DailyPassDay.pass_id == p.id,
                    DailyPassDay.scheduled_date >= today,  # Include today
                    DailyPassDay.status.in_(["scheduled", "available", "rescheduled"]),
                )
            )
            remaining = remaining_result.scalar() or 0

            next_days_result = await db.execute(
                select(DailyPassDay.scheduled_date)
                .where(
                    DailyPassDay.pass_id == p.id,
                    DailyPassDay.scheduled_date >= today,  # Include today
                    DailyPassDay.status.in_(["scheduled", "available", "rescheduled"]),
                )
                .order_by(DailyPassDay.scheduled_date.asc())
                .limit(5)
            )
            next_days = next_days_result.scalars().all()
            next_dates = [d.isoformat() for d in next_days]
            actual_valid_from = p.valid_from or p.start_date
            actual_valid_until = p.valid_until or p.end_date

            can_res = not await _once_used_async(db, p.id, "reschedule")
            can_upg = not await _once_used_async(db, p.id, "upgrade")

            # Enhanced can_reschedule logic for various edge cases
            original_start_date = p.valid_from or p.start_date
            original_end_date = p.valid_until or p.end_date

            # Case 1: Pass has ended
            if today >= original_end_date:
                can_res = False

            # Case 2: Check if there are any future days that can be rescheduled
            future_eligible_result = await db.execute(
                select(func.count(DailyPassDay.id))
                .where(
                    DailyPassDay.pass_id == p.id,
                    DailyPassDay.scheduled_date >= (today + timedelta(days=1)),  # Tomorrow and beyond
                    DailyPassDay.status.in_(["scheduled", "available", "rescheduled"]),
                )
            )
            future_reschedule_eligible_days = future_eligible_result.scalar() or 0

            if future_reschedule_eligible_days == 0:
                can_res = False  # No future days to reschedule

            # Case 3: Check if all remaining days are in the past or today
            all_remaining_result = await db.execute(
                select(DailyPassDay)
                .where(
                    DailyPassDay.pass_id == p.id,
                    DailyPassDay.status.in_(["scheduled", "available", "rescheduled"]),
                )
            )
            all_remaining_days = all_remaining_result.scalars().all()

            if all_remaining_days:
                latest_remaining_date = max(day.scheduled_date for day in all_remaining_days)
                if latest_remaining_date <= today:
                    can_res = False  # All remaining days are today or in the past

            # Case 4: If pass is completed (all days attended)
            total_pass_days_result = await db.execute(
                select(func.count(DailyPassDay.id))
                .where(DailyPassDay.pass_id == p.id)
            )
            total_pass_days = total_pass_days_result.scalar() or 0

            attended_days_result = await db.execute(
                select(func.count(DailyPassDay.id))
                .where(
                    DailyPassDay.pass_id == p.id,
                    DailyPassDay.status == "attended"
                )
            )
            attended_days = attended_days_result.scalar() or 0

            if total_pass_days > 0 and attended_days == total_pass_days:
                can_res = False  # All days are attended

            gym = await _load_gym_info_async(db, int(p.gym_id))

            # Handle upgraded passes
            is_upgraded = p.status == "upgraded"
            old_gym_id = None
            old_gym_name = None

            if is_upgraded and p.order_id:
                # Find the original pass using the order_id (which now contains the original pass ID)
                original_pass = next((op for op in all_rows if op.id == p.order_id), None)
                if original_pass:
                    old_gym_id = int(original_pass.gym_id)
                    old_gym_info = await _load_gym_info_async(db, old_gym_id)
                    old_gym_name = old_gym_info.get("name")

            # Handle partial schedule logic
            actual_days = None
            rescheduled_days = None

            # Check if pass has been edited by looking at audit records OR partial_schedule flag
            # For upgraded passes, check the original pass (referenced by order_id)
            pass_id_to_check = p.order_id if (is_upgraded and p.order_id) else p.id
            has_reschedule_audit = await _once_used_async(db, pass_id_to_check, "reschedule")
            is_edited = has_reschedule_audit or bool(p.partial_schedule)

            if is_edited:
                # Get all days for this pass
                all_pass_days_result = await db.execute(
                    select(DailyPassDay)
                    .where(DailyPassDay.pass_id == p.id)
                    .order_by(DailyPassDay.scheduled_date.asc())
                )
                all_pass_days = all_pass_days_result.scalars().all()

                # Find first continuous block (actual days) and rescheduled days
                actual_days_list = []
                rescheduled_days_list = []

                # Group days into continuous blocks
                for day in all_pass_days:
                    if day.reschedule_count and day.reschedule_count > 0:
                        rescheduled_days_list.append(day.scheduled_date.isoformat())
                    else:
                        actual_days_list.append(day.scheduled_date.isoformat())

                actual_days = actual_days_list
                rescheduled_days = rescheduled_days_list


            try:
                actual_price_minor = await get_price_for_gym_async(db, int(p.gym_id))
                actual_amount = actual_price_minor / 100  # Convert minor units to rupees
            except Exception:
                actual_amount = (p.amount_paid or 0) / 100  # Fallback to paid amount


            # Use days_total from pass record
            display_days_total = int(p.days_total or 0)

            resp.append(
                PassSummary(
                    pass_id=p.id,
                    amount=actual_amount,
                    gym_id=int(p.gym_id),
                    gym_name=gym.get("name"),
                    locality=gym.get("location"),
                    city=gym.get("city"),
                    valid_from=actual_valid_from.isoformat(),
                    valid_until=actual_valid_until.isoformat(),
                    days_total=display_days_total,
                    selected_time=p.selected_time,
                    remaining_days=remaining,
                    next_dates=next_dates,
                    can_reschedule=can_res,
                    can_upgrade=can_upg,
                    is_edited=is_edited,
                    actual_days=actual_days,
                    rescheduled_days=rescheduled_days,
                    is_upgraded=is_upgraded,
                    old_gym_id=old_gym_id,
                    old_gym_name=old_gym_name,
                    # New fields
                    address=GymAddress(**gym.get("address")) if gym.get("address") else None,
                    latitude=gym.get("latitude"),
                    longitude=gym.get("longitude"),
                    owner_mobile=gym.get("owner_mobile"),
                )
            )


        return ListActiveResponse(client_id=str(client_id), passes=resp)
    except Exception:
        raise


# --------------------- POST /dailypass/{pass_id}/reschedule ------------------


class EditQuoteRequest(BaseModel):
    pass_id: str
    client_id: int
    new_start_date: str


class EditQuoteResponse(BaseModel):
    status: int
    requires_topup: bool
    delta_minor: int
    new_total_minor: int
    days_count: int
    start_date: str
    end_date: str
    gym_id: int


class EditCheckoutRequest(BaseModel):
    pass_id: str
    client_id: int
    new_start_date: str


class EditCheckoutResponse(BaseModel):
    success: bool
    orderId: str
    razorpayOrderId: str
    razorpayKeyId: str
    amount: int
    currency: str
    message: str


class EditVerifyRequest(BaseModel):
    pass_id: str
    razorpay_order_id: str
    razorpay_payment_id: str
    razorpay_signature: str
    client_id: int


def _get_original_per_day_minor(db: Session, p: DailyPass) -> Optional[int]:
    """
    Get the original BASE price per day (discount_price) that was set by the gym owner
    when this pass was purchased.

    Business rule:
    - 49 Rs (4900 paisa) = special price, NO markup applied
    - All other prices = 30% markup applied

    So to get original base:
    - If user paid 4900, they paid base price directly (no markup)
    - If user paid != 4900, they paid with markup, so reverse it: base = paid / get_markup_multiplier()
    """
    print(f"[DEBUG _get_original_per_day_minor] pass_id={p.id}, payment_id={p.payment_id}, amount_paid={p.amount_paid}, days_total={p.days_total}")

    unit_price = None
    try:
        pay = (
            db.query(Payment)
            .filter(Payment.provider_payment_id == p.payment_id)
            .first()
        )
        print(f"[DEBUG _get_original_per_day_minor] Payment found: {pay is not None}")
        if pay:
            print(f"[DEBUG _get_original_per_day_minor] Payment order_id={pay.order_id}")
            oi = (
                db.query(OrderItem)
                .filter(OrderItem.order_id == pay.order_id, OrderItem.item_type == ItemType.daily_pass)
                .first()
            )
            print(f"[DEBUG _get_original_per_day_minor] OrderItem found: {oi is not None}")
            if oi and oi.unit_price_minor:
                unit_price = int(oi.unit_price_minor)
                print(f"[DEBUG _get_original_per_day_minor] unit_price_minor={unit_price}")
    except Exception as e:
        print(f"[DEBUG _get_original_per_day_minor] Exception in Payment/OrderItem lookup: {e}")

    # Fallback to amount_paid / days
    if unit_price is None:
        try:
            total = int(p.amount_paid or 0)
            days = max(1, int(p.days_total or 0))
            unit_price = total // days
            print(f"[DEBUG _get_original_per_day_minor] Fallback: amount_paid={total}, days={days}, unit_price={unit_price}")
        except Exception as e:
            print(f"[DEBUG _get_original_per_day_minor] Exception in fallback: {e}")
            return None

    # Business rule: 49 Rs (4900 paisa) has NO markup, all others have 30% markup
    if unit_price == 4900:
        # User paid 49 Rs which is the base price (no markup was applied)
        base_price = 4900
        print(f"[DEBUG _get_original_per_day_minor] 49 Rs special price - no markup, base_price={base_price}")
    else:
        # User paid with 30% markup, reverse it to get base price
        base_price = int(round(unit_price / get_markup_multiplier()))
        print(f"[DEBUG _get_original_per_day_minor] Non-49 Rs price - reversing markup: {unit_price} / get_markup_multiplier() = {base_price}")

    return base_price


def _calc_reschedule_pricing(
    dps: Session,
    p: DailyPass,
    new_start: date,
    eligible_days: List[DailyPassDay],
    original_per_day_minor: Optional[int],
) -> Dict[str, Any]:
    count = len(eligible_days)
    new_dates = [new_start + timedelta(days=i) for i in range(count)]
    original_per_day = original_per_day_minor or 0

    # Use get_actual_price_for_gym (without 30% markup) for fair comparison
    # since original_per_day is what user actually paid (discount_price, not marked up)
    current_per_day_minor = int(round(get_actual_price_for_gym(dps, int(p.gym_id)) or 0))

    print(f"[DEBUG _calc_reschedule_pricing] gym_id={p.gym_id}")
    print(f"[DEBUG _calc_reschedule_pricing] original_per_day_minor (input)={original_per_day_minor}")
    print(f"[DEBUG _calc_reschedule_pricing] original_per_day (used)={original_per_day}")
    print(f"[DEBUG _calc_reschedule_pricing] current_per_day_minor (from get_actual_price_for_gym)={current_per_day_minor}")
    print(f"[DEBUG _calc_reschedule_pricing] eligible_days count={count}")

    # Delta only when price increased (comparing base prices)
    delta_per_day_base = max(0, current_per_day_minor - original_per_day)
    delta_base = delta_per_day_base * count

    # Business rule: 49 Rs (4900 paisa) has NO markup, all others have 30% markup
    if current_per_day_minor == 4900:
        # Current price is 49 Rs - no markup on delta
        delta_minor = delta_base
        print(f"[DEBUG _calc_reschedule_pricing] 49 Rs price - no markup on delta: delta_minor={delta_minor}")
    else:
        # Apply 30% Fittbot markup to the delta
        delta_minor = int(round(delta_base * get_markup_multiplier()))
        print(f"[DEBUG _calc_reschedule_pricing] Non-49 Rs price - markup applied: delta_base={delta_base}, delta_minor={delta_minor}")

    # New total = current base price * days (with markup if not 49 Rs)
    base_total = current_per_day_minor * count
    if current_per_day_minor == 4900:
        new_total_minor = base_total
    else:
        new_total_minor = int(round(base_total * get_markup_multiplier()))


    return {
        "new_total_minor": new_total_minor,
        "delta_minor": delta_minor,
        "new_dates": new_dates,
        "start": new_dates[0].isoformat(),
        "end": new_dates[-1].isoformat(),
        "count": count,
    }


@router.post("/edit/quote", response_model=EditQuoteResponse)
async def reschedule_quote(payload: EditQuoteRequest, db: Session = Depends(get_db)):
    dps = next(get_dailypass_session())
    try:
        p: DailyPass = dps.query(DailyPass).filter(DailyPass.id == payload.pass_id).first()
        if not p or p.status != "active":
            raise FittbotHTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Pass not found or inactive",
                error_code="PASS_NOT_FOUND"
            )
        today = _today_ist()

        eligible_days = (
            dps.query(DailyPassDay)
            .filter(
                DailyPassDay.pass_id == payload.pass_id,
                DailyPassDay.scheduled_date >= (today + timedelta(days=1)),
                DailyPassDay.status.in_(["scheduled", "available", "rescheduled"]),
            )
            .order_by(DailyPassDay.scheduled_date.asc())
            .all()
        )

        if not eligible_days:
            raise FittbotHTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="No future days eligible to reschedule (D-1 rule)",
                error_code="NO_ELIGIBLE_DAYS"
            )
        
        try:
            new_start = date.fromisoformat(payload.new_start_date)
            
        except ValueError:
            raise FittbotHTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="new_start_date must be YYYY-MM-DD format",
                error_code="INVALID_DATE_FORMAT"
            )
        
        original_per_day_minor = _get_original_per_day_minor(db, p)
        pricing = _calc_reschedule_pricing(dps, p, new_start, eligible_days, original_per_day_minor)

        

        return EditQuoteResponse(
            status=200,
            requires_topup=pricing["delta_minor"] > 0,
            delta_minor=(int(pricing["delta_minor"]))/100,
            new_total_minor=int(pricing["new_total_minor"]),
            days_count=pricing["count"],
            start_date=pricing["start"],
            end_date=pricing["end"],
            gym_id=int(p.gym_id),
        )
    
    finally:

        try:
            dps.close()
        except Exception:
            pass


@router.post("/edit/checkout", response_model=EditCheckoutResponse)
async def edit_topup_checkout(
    payload: EditCheckoutRequest,
    db: Session = Depends(get_db),
):
    dps = next(get_dailypass_session())
    settings = get_payment_settings()
    try:
        p: DailyPass = dps.query(DailyPass).filter(DailyPass.id == payload.pass_id).first()
        if not p or p.status != "active":
            raise HTTPException(status.HTTP_404_NOT_FOUND, "Pass not found or inactive")
        today = _today_ist()
        eligible_days = (
            dps.query(DailyPassDay)
            .filter(
                DailyPassDay.pass_id == payload.pass_id,
                DailyPassDay.scheduled_date >= (today + timedelta(days=1)),
                DailyPassDay.status.in_(["scheduled", "available", "rescheduled"]),
            )
            .order_by(DailyPassDay.scheduled_date.asc())
            .all()
        )
        if not eligible_days:
            raise HTTPException(status.HTTP_409_CONFLICT, "No future days eligible to reschedule (D-1 rule)")
        try:
            new_start = date.fromisoformat(payload.new_start_date)
        except ValueError:
            raise HTTPException(status.HTTP_400_BAD_REQUEST, "new_start_date must be YYYY-MM-DD format")

        original_per_day_minor = _get_original_per_day_minor(db, p)
        pricing = _calc_reschedule_pricing(dps, p, new_start, eligible_days, original_per_day_minor)
        delta_minor = pricing["delta_minor"]
        if delta_minor <= 0:
            raise HTTPException(status.HTTP_409_CONFLICT, "No top-up required for this edit")

        order = Order(
            id=_new_id("ord_"),
            customer_id=p.client_id,
            provider="razorpay_pg",
            currency="INR",
            gross_amount_minor=delta_minor,
            status=StatusOrder.pending,
            order_metadata={
                "flow": "dailypass_edit_topup",
                "pass_id": payload.pass_id,
                "new_start_date": payload.new_start_date,
                "gym_id": int(p.gym_id),
                "days_count": pricing["count"],
            },
        )
        db.add(order)
        db.flush()

        notes = {
            "flow": "dailypass_edit_topup",
            "pass_id": payload.pass_id,
            "gym_id": int(p.gym_id),
            "days_count": pricing["count"],
        }
        rzp_order = rzp_create_order(
            amount_minor=delta_minor,
            currency="INR",
            receipt=order.id,
            notes=notes,
            settings=settings,
        )
        order.provider_order_id = rzp_order["id"]
        db.add(order)
        db.commit()

        return EditCheckoutResponse(
            success=True,
            orderId=order.id,
            razorpayOrderId=rzp_order["id"],
            razorpayKeyId=settings.razorpay_key_id,
            amount=delta_minor,
            currency="INR",
            message="Top-up order created",
        )
    except HTTPException:
        db.rollback()
        raise
    except Exception as e:
        db.rollback()
        logger.exception("Edit top-up checkout failed")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, f"Edit top-up checkout failed: {e}")
    finally:
        try:
            dps.close()
        except Exception:
            pass


@router.post("/edit/verify")
async def edit_topup_verify(
    body: EditVerifyRequest,
    db: Session = Depends(get_db),
):
    settings = get_payment_settings()
    order = db.query(Order).filter(Order.provider_order_id == body.razorpay_order_id).first()
    if not order:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Top-up order not found")

    payment_data = rzp_get_payment(body.razorpay_payment_id, settings)
    if payment_data.get("status") != "captured":
        raise HTTPException(status.HTTP_400_BAD_REQUEST, f"Payment not captured (status={payment_data.get('status')})")
    paid_amount = int(payment_data.get("amount", 0))
    if paid_amount != order.gross_amount_minor:
        raise HTTPException(status.HTTP_409_CONFLICT, "Payment amount mismatch")

    existing_payment = (
        db.query(Payment)
        .filter(Payment.provider_payment_id == body.razorpay_payment_id, Payment.status == "captured")
        .first()
    )
    if not existing_payment:
        pay = Payment(
            id=_new_id("pay_"),
            order_id=order.id,
            customer_id=order.customer_id,
            provider="razorpay_pg",
            provider_payment_id=body.razorpay_payment_id,
            amount_minor=paid_amount,
            currency=payment_data.get("currency", "INR"),
            status="captured",
            captured_at=datetime.now(UTC),
            payment_metadata={"method": payment_data.get("method"), "source": "dailypass_edit_topup"},
        )
        db.add(pay)
        order.status = StatusOrder.paid
        db.add(order)
        db.commit()

    return {
        "success": True,
        "payment_captured": True,
        "order_id": order.id,
        "payment_id": body.razorpay_payment_id,
        "message": "Top-up payment verified. Call /get_dailypass/edit with topup_payment_id to apply changes.",
    }


@router.post("/edit", response_model=RescheduleResponse)
async def reschedule_pass(
    payload: RescheduleRequest,
    db: Session = Depends(get_db)
    ):
    pass_id = payload.pass_id
    dps = next(get_dailypass_session())
    try:

        p: DailyPass = dps.query(DailyPass).filter(DailyPass.id == pass_id).first()
        if not p or p.status != "active":
            raise FittbotHTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Pass not found or inactive",
                error_code="PASS_NOT_FOUND"
            )

        today = _today_ist()

        # Gate: one-time only
        if _once_used(dps, pass_id, "reschedule"):
            raise FittbotHTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="Reschedule already used for this pass",
                error_code="RESCHEDULE_ALREADY_USED"
            )

   
        eligible_days = (
            dps.query(DailyPassDay)
            .filter(
                DailyPassDay.pass_id == pass_id,
                DailyPassDay.scheduled_date >= (today + timedelta(days=1)),
                DailyPassDay.status.in_(["scheduled", "available", "rescheduled"]),
            )
            .order_by(DailyPassDay.scheduled_date.asc())
            .all()
        )

        if not eligible_days:
            raise FittbotHTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="No future days eligible to reschedule (D-1 rule)",
                error_code="NO_ELIGIBLE_DAYS"
            )


        try:
            new_start = date.fromisoformat(payload.new_start_date)
        except ValueError:
            raise FittbotHTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="new_start_date must be YYYY-MM-DD format",
                error_code="INVALID_DATE_FORMAT"
            )

        count = len(eligible_days)
        new_dates = [new_start + timedelta(days=i) for i in range(count)]

        old_range = (eligible_days[0].scheduled_date.isoformat(), eligible_days[-1].scheduled_date.isoformat())
        new_range = (new_dates[0].isoformat(), new_dates[-1].isoformat())

        original_start_date = p.valid_from or p.start_date
        has_pass_started = today >= original_start_date

        gym_id = int(p.gym_id)
        original_per_day_minor = _get_original_per_day_minor(db, p)
        pricing = _calc_reschedule_pricing(dps, p, new_start, eligible_days, original_per_day_minor)
        per_day_minor = pricing["new_total_minor"] // max(1, pricing["count"])
        dailypass_price_rupees = per_day_minor // 100
        delta_minor = 0 if payload.paid else pricing["delta_minor"]

        if delta_minor > 0 and not payload.paid:
            if payload.paid:
                # Caller asserts payment already handled; skip top-up validation and proceed
                topup_reference = None
            else:
                topup_payment_id = payload.topup_payment_id
                # If not provided, try to auto-resolve the latest captured edit-topup payment for this pass/client
                if not topup_payment_id and payload.paid:
                    try:
                        latest = (
                            db.query(Payment.provider_payment_id)
                            .join(Order, Payment.order_id == Order.id)
                            .filter(
                                Payment.customer_id == p.client_id,
                                Payment.status == StatusPayment.captured,
                                Order.order_metadata["flow"].astext == "dailypass_edit_topup",
                            )
                            .order_by(Payment.captured_at.desc().nullslast())
                            .first()
                        )
                        if not latest:
                            # Fallback: any captured payment with metadata source edit_topup for this client
                            latest = (
                                db.query(Payment.provider_payment_id)
                                .filter(
                                    Payment.customer_id == p.client_id,
                                    Payment.status == StatusPayment.captured,
                                    Payment.payment_metadata["source"].astext == "dailypass_edit_topup",
                                )
                                .order_by(Payment.captured_at.desc().nullslast())
                                .first()
                            )
                        if latest:
                            topup_payment_id = latest[0]
                    except Exception:
                        topup_payment_id = None
                # Optional: fall back to command lookup if provided
                if not topup_payment_id and payload.topup_command_id:
                    try:
                        cfg = get_high_concurrency_config()
                        store = CommandStore(
                            get_redis_sync(),
                            cfg,
                            redis_prefix=cfg.dailypass_redis_prefix,
                            command_id_prefix="dp_cmd",
                        )
                        cmd = await store.get(payload.topup_command_id, owner_id=str(payload.client_id))
                        if cmd and cmd.status == CommandStatus.completed and cmd.result:
                            topup_payment_id = cmd.result.get("payment_id")
                    except Exception:
                        pass
                if not topup_payment_id:
                    raise FittbotHTTPException(
                        status_code=status.HTTP_402_PAYMENT_REQUIRED,
                        detail="Top-up required for new schedule",
                        error_code="TOPUP_REQUIRED",
                        log_data={"delta_minor": delta_minor},
                    )
                existing_payment = (
                    db.query(Payment)
                    .filter(Payment.provider_payment_id == topup_payment_id, Payment.status == "captured")
                    .first()
                )
                if not existing_payment:
                    raise FittbotHTTPException(
                        status_code=status.HTTP_402_PAYMENT_REQUIRED,
                        detail="Top-up payment not captured yet",
                        error_code="TOPUP_NOT_CAPTURED"
                    )
                p.amount_paid = int(p.amount_paid or 0) + delta_minor
                topup_reference = topup_payment_id
        else:
            topup_reference = None

        # Store old day metadata before deletion
        old_day_ids = [dpd.id for dpd in eligible_days]
        old_dates = [dpd.scheduled_date for dpd in eligible_days]

        print(f"DEBUG: Deleting {len(eligible_days)} old day records: {old_day_ids}")

        # Delete ledger allocations FIRST (foreign key constraint)
        dps.query(LedgerAllocation).filter(
            LedgerAllocation.pass_day_id.in_(old_day_ids)
        ).delete(synchronize_session=False)
        print(f"DEBUG: Deleted ledger allocations for old day records")

        # Flush ledger deletions
        dps.flush()

        # Now delete old day records to avoid unique constraint violation
        for dpd in eligible_days:
            dps.delete(dpd)

        # Flush the day record deletions
        dps.flush()

        new_day_records = []
        for i, new_date in enumerate(new_dates):
            new_day = DailyPassDay(
                pass_id=pass_id,
                scheduled_date=new_date,
                status="rescheduled",
                reschedule_count=1,
                gym_id=str(gym_id),
                client_id=p.client_id,
                dailypass_price=dailypass_price_rupees,
                meta={"rescheduled_from": old_dates[i].isoformat()},
            )
            dps.add(new_day)
            new_day_records.append(new_day)

        # FLUSH changes to database so we can query updated dates
        dps.flush()

        # Create new ledger allocations for the new day records
        # Calculate amount per day
        total_minor = int(p.amount_paid or 0)
        n = max(1, len(new_day_records))
        base, rem = divmod(total_minor, int(p.days_total or n))

        for i, dr in enumerate(new_day_records):
            # Only allocate to the rescheduled days, proportionally
            amt = base + (1 if i < rem else 0)
            ledger = LedgerAllocation(
                daily_pass_id=pass_id,
                pass_day_id=dr.id,
                gym_id=gym_id,
                client_id=p.client_id,
                payment_id=p.payment_id,
                order_id=p.order_id or pass_id,
                amount=amt,
                amount_net_minor=amt,
                allocation_date=datetime.now(IST).date(),
                status="allocated",
            )
            dps.add(ledger)

        dps.flush()


        for i, old_day_id in enumerate(old_day_ids):
            if i < len(new_day_records):
                new_day_id = new_day_records[i].id
    
                db.query(FittbotPayment).filter(
                    FittbotPayment.entitlement_id == str(old_day_id)
                ).update(
                    {"entitlement_id": str(new_day_id)},
                    synchronize_session=False
                )
                #logger.info(f"[RESCHEDULE_PAYMENT_UPDATE] Updated FittbotPayment entitlement_id: {old_day_id} -> {new_day_id}")

        db.flush()


        if has_pass_started:
            p.partial_schedule = True
        else:

            all_days = (
                dps.query(DailyPassDay.scheduled_date)
                .filter(DailyPassDay.pass_id == pass_id)
                .order_by(DailyPassDay.scheduled_date.asc())
                .all()
            )
            old_valid_from = p.valid_from
            old_valid_until = p.valid_until
            p.valid_from = all_days[0][0]
            p.valid_until = all_days[-1][0]



        dps.add(p)

        audit_record = DailyPassAudit(
            pass_id=pass_id,
            action="reschedule",
            actor="user",
            details=f"{count} days {old_range} -> {new_range}",
            before={"old_range": old_range},
            after={"new_range": new_range, "topup_payment_id": topup_reference} if topup_reference else {"new_range": new_range},
            client_id=p.client_id,
        )
        dps.add(audit_record)
  
        try:
            dps.commit()
            db.commit()  # Commit FittbotPayment entitlement_id updates

            # Verify what's actually in the database after commit
            fresh_pass = dps.query(DailyPass).filter(DailyPass.id == pass_id).first()

            fresh_days = dps.query(DailyPassDay).filter(DailyPassDay.pass_id == pass_id).order_by(DailyPassDay.scheduled_date.asc()).all()

        except Exception as e:
            print(f"DEBUG: COMMIT FAILED: {e}")
            db.rollback()
            raise

        # Calculate actual_paid and new_paid for the response
        selected_time = p.selected_time or ""
        try:
            gym_id = int(p.gym_id)
            ppd = int(get_price_for_gym(dps, gym_id))  # minor units (paisa)
            # For reschedule, actual_paid and new_paid are the same since it's same gym
            days_total = int(p.days_total or 0)
            actual_paid = (days_total * ppd) // 100  # Convert to rupees
            new_paid = actual_paid  # Same since it's the same gym
        except Exception:
            actual_paid = 0
            new_paid = 0

        return RescheduleResponse(
            pass_id=pass_id,
            old_range=old_range,
            new_range=new_range,
            days_moved=count,
            message="Rescheduled successfully",
            actual_paid=actual_paid,
            new_paid=new_paid,
            selected_time=selected_time,
        )
    except FittbotHTTPException:
        dps.rollback()
        db.rollback()
        raise
    except Exception as e:
        dps.rollback()
        db.rollback()
        logger.exception("Reschedule failed")
        raise FittbotHTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Reschedule operation failed",
            error_code="RESCHEDULE_FAILED",
            log_data={"error": str(e)}
        )
    finally:
        try:
            dps.close()
        except Exception:
            pass


# ----------------- POST /dailypass/{pass_id}/upgrade/preview ----------------


@router.get("/upgrade/preview")
async def upgrade_preview(
    pass_id,new_gym_id,client_id,
    db: Session = Depends(get_db),
):
    
    dps = next(get_dailypass_session())

    try:
        p: DailyPass = dps.query(DailyPass).filter(DailyPass.id == pass_id).first()
        if not p or p.status != "active":
            raise FittbotHTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Pass not found or inactive",
                error_code="PASS_NOT_FOUND"
            )

        if _once_used(dps, pass_id, "upgrade"):
            raise FittbotHTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="Upgrade already used for this pass",
                error_code="UPGRADE_ALREADY_USED"
            )

        today = _today_ist()
        tomorrow = today + timedelta(days=1)  # Upgrade only applies from tomorrow
        old_gym_id = int(p.gym_id)
        old_paid_price= int((p.amount_paid*0.01) or 0)
        selected_time= p.selected_time or ""
        days_total = int(p.days_total or 0)
        

        if new_gym_id == old_gym_id:
            return UpgradePreviewResponse(
                pass_id=pass_id,
                old_gym_id=old_gym_id,
                new_gym_id=new_gym_id,
                old_gym_name="",
                new_gym_name="",
                original_dates={},
                upgradeable_dates={},
                total_days=0,
                upgradeable_days=0,
                old_price_per_day=0,
                new_price_per_day=0,
                price_difference_per_day=0,
                total_upgrade_cost=0,
                can_upgrade=False,
                reason="New gym must be different from current gym",
                new_paid=0,
                actual_paid=0,
                selected_time=selected_time
            )

        # Get gym information
        old_gym_info = _load_gym_info(db, old_gym_id)
        new_gym_info = _load_gym_info(db, new_gym_id)

        # Get pricing
        try:
            old_ppd = int(get_price_for_gym(dps, old_gym_id))  # minor units (paisa)
            new_ppd = int(get_price_for_gym(dps, new_gym_id))  # minor units (paisa)
        except Exception as e:
            return UpgradePreviewResponse(
                pass_id=pass_id,
                old_gym_id=old_gym_id,
                new_gym_id=new_gym_id,
                old_gym_name=old_gym_info.get("name", ""),
                new_gym_name=new_gym_info.get("name", ""),
                original_dates={},
                upgradeable_dates={},
                total_days=0,
                upgradeable_days=0,
                old_price_per_day=0,
                new_price_per_day=0,
                price_difference_per_day=0,
                total_upgrade_cost=0,
                can_upgrade=False,
                reason="Gym pricing not configured",
                new_paid=0,
                actual_paid=0,
                selected_time=selected_time
            )

        if new_ppd <= old_ppd:
            return UpgradePreviewResponse(
                pass_id=pass_id,
                old_gym_id=old_gym_id,
                new_gym_id=new_gym_id,
                old_gym_name=old_gym_info.get("name", ""),
                new_gym_name=new_gym_info.get("name", ""),
                original_dates={},
                upgradeable_dates={},
                total_days=0,
                upgradeable_days=0,
                old_price_per_day=old_ppd / 100,
                new_price_per_day=new_ppd / 100,
                price_difference_per_day=0,
                total_upgrade_cost=0,
                can_upgrade=False,
                reason="New gym price must be higher (upgrade only)",
                new_paid=0,
                actual_paid=0,
                selected_time=selected_time
            )

        # Calculate date ranges and upgradeable days
        original_from = p.valid_from or p.start_date
        original_to = p.valid_until or p.end_date

        # Find upgradeable days (tomorrow onwards - today stays with old gym)
        upgradeable_days = (
            dps.query(DailyPassDay)
            .filter(
                DailyPassDay.pass_id == pass_id,
                DailyPassDay.scheduled_date >= tomorrow,  # Tomorrow onwards only
                DailyPassDay.status.in_(["scheduled", "available", "rescheduled"]),
            )
            .order_by(DailyPassDay.scheduled_date.asc())
            .all()
        )

        

        if not upgradeable_days:
            return UpgradePreviewResponse(
                pass_id=pass_id,
                old_gym_id=old_gym_id,
                new_gym_id=new_gym_id,
                old_gym_name=old_gym_info.get("name", ""),
                new_gym_name=new_gym_info.get("name", ""),
                original_dates={"from": original_from.isoformat(), "to": original_to.isoformat()},
                upgradeable_dates={},
                total_days=p.days_total,
                upgradeable_days=0,
                old_price_per_day=old_ppd / 100,
                new_price_per_day=new_ppd / 100,
                price_difference_per_day=(new_ppd - old_ppd) / 100,
                total_upgrade_cost=0,
                can_upgrade=False,
                reason="No remaining days available for upgrade",
                new_paid=0,
                actual_paid=0,
                selected_time=selected_time
            )

        upgradeable_from = upgradeable_days[0].scheduled_date
        upgradeable_to = upgradeable_days[-1].scheduled_date
        upgradeable_count = len(upgradeable_days)
        actual_days=days_total-upgradeable_count
        actual_paid= (upgradeable_count*old_ppd) // 100
        new_paid=(upgradeable_count*new_ppd) // 100

        # Calculate costs
        price_diff_per_day_minor = new_ppd - old_ppd
        total_upgrade_cost_minor = price_diff_per_day_minor * upgradeable_count

        
        data=UpgradePreviewResponse(
            pass_id=pass_id,
            old_gym_id=old_gym_id,
            new_gym_id=new_gym_id,
            old_gym_name=old_gym_info.get("name", ""),
            new_gym_name=new_gym_info.get("name", ""),
            original_dates={"from": original_from.isoformat(), "to": original_to.isoformat()},
            upgradeable_dates={"from": upgradeable_from.isoformat(), "to": upgradeable_to.isoformat()},
            total_days=p.days_total,
            upgradeable_days=upgradeable_count,
            old_price_per_day=old_ppd / 100,
            new_price_per_day=new_ppd / 100,
            price_difference_per_day=price_diff_per_day_minor / 100,
            total_upgrade_cost=total_upgrade_cost_minor / 100,
            can_upgrade=True,
            reason=None,
            actual_paid=actual_paid,
            new_paid=new_paid,
            selected_time=selected_time,
        )



        return {
            "status":200,
            "data":data
        }

    except FittbotHTTPException:
        raise
    except Exception as e:
        logger.exception("Upgrade preview failed")
        raise FittbotHTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Upgrade preview failed",
            error_code="UPGRADE_PREVIEW_FAILED",
            log_data={"error": str(e)}
        )
    finally:
        try:
            dps.close()
        except Exception:
            pass


# ----------------- POST /dailypass/{pass_id}/upgrade/initiate ----------------


@router.post("/upgrade")
async def upgrade_initiate(
    payload: UpgradeInitiateRequest,
    db: Session = Depends(get_db),
):

    pass_id= payload.pass_id
    dps = next(get_dailypass_session())
    settings = get_payment_settings()

    try:
        p: DailyPass = dps.query(DailyPass).filter(DailyPass.id == pass_id).first()
        if not p or p.status != "active":
            raise FittbotHTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Pass not found or inactive",
                error_code="PASS_NOT_FOUND"
            )

        if _once_used(dps, pass_id, "upgrade"):
            raise FittbotHTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="Upgrade already used for this pass",
                error_code="UPGRADE_ALREADY_USED"
            )

        # Use values from client request (already calculated in preview)
        old_gym_id = int(p.gym_id)
        new_gym_id = int(payload.new_gym_id)
        n = payload.remaining_days_count
        delta_minor = (payload.delta_minor)*100

        # Calculate actual_paid and new_paid for the response
        selected_time = p.selected_time or ""
        try:
            old_ppd = int(get_price_for_gym(dps, old_gym_id))  # minor units (paisa)
            new_ppd = int(get_price_for_gym(dps, new_gym_id))  # minor units (paisa)
            actual_paid = (n * old_ppd) // 100  # Convert to rupees
            new_paid = (n * new_ppd) // 100  # Convert to rupees
        except Exception:
            actual_paid = 0
            new_paid = 0

        # Basic validation - still need to check if gyms are different
        if new_gym_id == old_gym_id:
            raise FittbotHTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="New gym must be different from current gym",
                error_code="SAME_GYM_SELECTED"
            )

        # Get actual remaining days (tomorrow onwards - today stays with old gym)
        today = _today_ist()
        tomorrow = today + timedelta(days=1)
        remaining_days_query = (
            dps.query(DailyPassDay)
            .filter(
                DailyPassDay.pass_id == pass_id,
                DailyPassDay.scheduled_date >= tomorrow,  # Tomorrow onwards only
                DailyPassDay.status.in_(["scheduled", "available", "rescheduled"]),
            )
            .order_by(DailyPassDay.scheduled_date.asc())
            .all()
        )
        if not remaining_days_query:
            raise FittbotHTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="No remaining days to upgrade",
                error_code="NO_REMAINING_DAYS"
            )

        print("delta minor",delta_minor)
        print("n",n)
        
        # Create an internal "upgrade order" in payments DB (for traceability)
        order = Order(
            id=_new_id("ord_"),
            customer_id=p.client_id,
            provider="razorpay_pg",
            currency="INR",
            gross_amount_minor=delta_minor,
            status=StatusOrder.pending,
        )
        db.add(order)
        db.flush()

        notes = {
            "flow": "dailypass_upgrade",
            "pass_id": pass_id,
            "old_gym_id": old_gym_id,
            "new_gym_id": new_gym_id,
            "remaining_days": n,
        }
        rzp_order = rzp_create_order(
            amount_minor=delta_minor,
            currency="INR",
            receipt=order.id,
            notes=notes,
            settings=settings,
        )
        order.provider_order_id = rzp_order["id"]
        db.add(order)
        db.commit()

        # Calculate date ranges for the upgrade
        upgrade_start_date = remaining_days_query[0].scheduled_date.isoformat()
        upgrade_end_date = remaining_days_query[-1].scheduled_date.isoformat()

        return {
            "success": True,
            "orderId": order.id,
            "razorpayOrderId": rzp_order["id"],
            "razorpayKeyId": settings.razorpay_key_id,
            "amount": delta_minor,
            "currency": "INR",
            "dailyPassAmount": 0,  # No daily pass amount for upgrade
            "subscriptionAmount": 0,  # No subscription amount for upgrade
            "finalAmount": delta_minor,
            "gymId": new_gym_id,
            "daysTotal": n,
            "startDate": upgrade_start_date,  # First day of upgrade period
            "endDate": upgrade_end_date,  # Last day of upgrade period
            "includesSubscription": False,
            "displayTitle": f"Upgrade to Gym {new_gym_id} ({n} days)",
            "description": f"Daily pass upgrade from Gym {old_gym_id} to Gym {new_gym_id}",
            # Additional upgrade-specific fields
            "pass_id": pass_id,
            "old_gym_id": old_gym_id,
            "new_gym_id": new_gym_id,
            "remaining_days": n,
            "actual_paid": actual_paid,
            "new_paid": new_paid,
            "selected_time": selected_time,
            "upgrade_date_range": {
                "from": upgrade_start_date,
                "to": upgrade_end_date
            }
        }
    except HTTPException:
        db.rollback()
        raise
    except Exception as e:
        db.rollback()
        logger.exception("Upgrade initiate failed")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, f"Upgrade initiate failed: {e}")
    finally:
        try:
            dps.close()
        except Exception:
            pass


# ------------------ POST /dailypass/{pass_id}/upgrade/verify -----------------


@router.post("/upgrade/verify")
async def upgrade_verify(
    body: UpgradeVerifyRequest,
    db: Session = Depends(get_db),
):
    pass_id = body.pass_id

    settings = get_payment_settings()
    dps = next(get_dailypass_session())
    try:
        # 1) Find the order by provider_order_id
        order = db.query(Order).filter(Order.provider_order_id == body.razorpay_order_id).first()
        if not order:
            raise HTTPException(status.HTTP_404_NOT_FOUND, "Upgrade order not found")

        # 2) Verify payment with Razorpay
        payment_data = rzp_get_payment(body.razorpay_payment_id, settings)
        if payment_data.get("status") != "captured":
            raise HTTPException(status.HTTP_400_BAD_REQUEST, f"Payment not captured (status={payment_data.get('status')})")
        paid_amount = int(payment_data.get("amount", 0))
        if paid_amount != order.gross_amount_minor:
            raise HTTPException(status.HTTP_409_CONFLICT, "Payment amount mismatch")

        # 3) Idempotency: if a captured payment exists for this provider_payment_id, we're done
        existing_payment = (
            db.query(Payment)
            .filter(Payment.provider_payment_id == body.razorpay_payment_id, Payment.status == "captured")
            .first()
        )
        if existing_payment:
            # Fetch context to respond correctly
            p = dps.query(DailyPass).filter(DailyPass.id == pass_id).first()
            if not p:
                raise HTTPException(status.HTTP_404_NOT_FOUND, "Pass not found after payment")
            old_gym_id = int(p.gym_id)  # already updated
            # best effort: read new_gym_id from notes stored on order
            new_gym_id = old_gym_id
            remaining_days_count = (
                dps.query(DailyPassDay)
                .filter(DailyPassDay.pass_id == pass_id, DailyPassDay.status.in_(["scheduled", "available", "rescheduled"]))
                .count()
            )

            # Calculate actual_paid and new_paid for the response
            selected_time = p.selected_time or ""
            try:
                old_ppd = int(get_price_for_gym(dps, old_gym_id))  # minor units (paisa)
                new_ppd = int(get_price_for_gym(dps, new_gym_id))  # minor units (paisa)
                actual_paid = (remaining_days_count * old_ppd) // 100  # Convert to rupees
                new_paid = (remaining_days_count * new_ppd) // 100  # Convert to rupees
            except Exception:
                actual_paid = 0
                new_paid = 0

            return {
                "success": True,
                "payment_captured": True,
                "order_id": order.id,
                "payment_id": existing_payment.provider_payment_id,
                "daily_pass_activated": True,
                "daily_pass_details": {
                    "pass_id": pass_id,
                    "gym_id": old_gym_id,
                    "remaining_days": remaining_days_count,
                    "status": "active",
                    "actual_paid": actual_paid,
                    "new_paid": new_paid,
                    "selected_time": selected_time,
                    "rescheduled_from": {
                        "original_gym_id": old_gym_id,
                        "original_pass_id": pass_id,
                        "upgrade_date": _now_ist().date().isoformat(),
                        "is_partial_schedule": p.partial_schedule
                    }
                },
                "subscription_activated": False,
                "subscription_details": None,
                "total_amount": order.gross_amount_minor,
                "currency": "INR",
                "message": "Upgrade payment already processed",
                "actual_paid": actual_paid,
                "new_paid": new_paid,
                "selected_time": selected_time,
            }

        # 4) Write payment + mark order paid
        pay = Payment(
            id=_new_id("pay_"),
            order_id=order.id,
            customer_id=order.customer_id,
            provider="razorpay_pg",
            provider_payment_id=body.razorpay_payment_id,
            amount_minor=paid_amount,
            currency=payment_data.get("currency", "INR"),
            status="captured",
            captured_at=datetime.now(UTC),
            payment_metadata={"method": payment_data.get("method"), "source": "dailypass_upgrade"},
        )
        db.add(pay)
        order.status = StatusOrder.paid
        db.add(order)
        db.commit()

        # 5) Apply the upgrade
        p: DailyPass = dps.query(DailyPass).filter(DailyPass.id == pass_id).first()
        if not p or p.status != "active":
            raise HTTPException(status.HTTP_404_NOT_FOUND, "Pass not found / inactive")

        if _once_used(dps, pass_id, "upgrade"):
            # If client retries verify after we've already upgraded, just respond OK.
            #logger.info("Upgrade already applied (audit-gate)")
            remaining_count = (
                dps.query(DailyPassDay)
                .filter(DailyPassDay.pass_id == pass_id, DailyPassDay.status.in_(["scheduled", "available", "rescheduled"]))
                .count()
            )

            # Calculate actual_paid and new_paid for the response
            selected_time = p.selected_time or ""
            try:
                old_ppd = int(get_price_for_gym(dps, int(p.gym_id)))  # minor units (paisa)
                new_ppd = int(get_price_for_gym(dps, int(p.gym_id)))  # Same gym since already upgraded
                actual_paid = (remaining_count * old_ppd) // 100  # Convert to rupees
                new_paid = (remaining_count * new_ppd) // 100  # Convert to rupees
            except Exception:
                actual_paid = 0
                new_paid = 0

            return {
                "success": True,
                "payment_captured": True,
                "order_id": order.id,
                "payment_id": body.razorpay_payment_id,
                "daily_pass_activated": True,
                "daily_pass_details": {
                    "pass_id": pass_id,
                    "gym_id": int(p.gym_id),
                    "remaining_days": remaining_count,
                    "status": p.status,
                    "actual_paid": actual_paid,
                    "new_paid": new_paid,
                    "selected_time": selected_time,
                    "rescheduled_from": {
                        "original_gym_id": int(p.gym_id),
                        "original_pass_id": pass_id,
                        "upgrade_date": _now_ist().date().isoformat(),
                        "is_partial_schedule": p.partial_schedule
                    }
                },
                "subscription_activated": False,
                "subscription_details": None,
                "total_amount": paid_amount,
                "currency": "INR",
                "message": "Upgrade already applied",
                "actual_paid": actual_paid,
                "new_paid": new_paid,
                "selected_time": selected_time,
            }

        # 6) Get days for upgrade - separate today and future days
        today = _today_ist()
        tomorrow = today + timedelta(days=1)
        old_gym_id = int(p.gym_id)
        old_order_id = p.id  # Original pass ID for linking

        # Get today's day (if exists) - stays with OLD gym
        todays_day = (
            dps.query(DailyPassDay)
            .filter(
                DailyPassDay.pass_id == pass_id,
                DailyPassDay.scheduled_date == today,
                DailyPassDay.status.in_(["scheduled", "available", "rescheduled"]),
            )
            .first()
        )

        # Get future days (tomorrow onwards) - goes to NEW gym
        future_days = (
            dps.query(DailyPassDay)
            .filter(
                DailyPassDay.pass_id == pass_id,
                DailyPassDay.scheduled_date >= tomorrow,
                DailyPassDay.status.in_(["scheduled", "available", "rescheduled"]),
            )
            .order_by(DailyPassDay.scheduled_date.asc())
            .all()
        )

        if not future_days:
            raise HTTPException(status.HTTP_409_CONFLICT, "No future days to upgrade")

        # Extract new_gym_id from order notes
        new_gym_id = None
        try:
            if hasattr(order, 'provider_order_id'):
                rzp_order = rzp_get_order(order.provider_order_id, settings)
                if 'notes' in rzp_order:
                    new_gym_id = rzp_order['notes'].get('new_gym_id')
            if not new_gym_id:
                payment_full = rzp_get_payment(body.razorpay_payment_id, settings)
                if 'notes' in payment_full:
                    new_gym_id = payment_full['notes'].get('new_gym_id')
            if new_gym_id:
                new_gym_id = int(new_gym_id)
            else:
                raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, "Could not determine new gym ID from order notes")
        except Exception as e:
            logger.error(f"Could not determine new_gym_id: {e}")
            raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, f"Could not determine new gym ID for upgrade: {str(e)}")

        # 7) Create upgraded passes - TWO if today exists, ONE if not
        today_pass_id = None
        future_pass_id = None

        # 7a) If today's day exists, create pass for today with OLD gym
        if todays_day:
            today_pass_id = _new_id("dps")
            today_pass = DailyPass(
                id=today_pass_id,
                user_id=p.user_id,
                client_id=p.client_id,
                gym_id=str(old_gym_id),  # OLD gym for today
                order_id=old_order_id,
                payment_id=body.razorpay_payment_id,
                days_total=1,
                days_used=0,
                valid_from=today,
                valid_until=today,
                amount_paid=0,  # No extra payment for today
                selected_time=p.selected_time,
                status="upgraded",
                policy=p.policy,
                partial_schedule=p.partial_schedule,
            )
            dps.add(today_pass)
            dps.flush()

            # Update today's day record
            todays_day.pass_id = today_pass_id
            todays_day.gym_id = str(old_gym_id)  # Keep old gym
            dps.add(todays_day)

        # 7b) Create pass for future days with NEW gym
        future_pass_id = _new_id("dps")
        future_pass = DailyPass(
            id=future_pass_id,
            user_id=p.user_id,
            client_id=p.client_id,
            gym_id=str(new_gym_id),  # NEW gym for future
            order_id=old_order_id,
            payment_id=body.razorpay_payment_id,
            days_total=len(future_days),
            days_used=0,
            valid_from=future_days[0].scheduled_date,
            valid_until=future_days[-1].scheduled_date,
            amount_paid=paid_amount,
            selected_time=p.selected_time,
            status="upgraded",
            policy=p.policy,
            partial_schedule=p.partial_schedule,
        )
        dps.add(future_pass)
        dps.flush()

        # Update future day records
        for day in future_days:
            day.pass_id = future_pass_id
            day.gym_id = str(new_gym_id)
            day.client_id = p.client_id
            dps.add(day)

        # 8) Add audit record
        audit_record = DailyPassAudit(
            pass_id=pass_id,
            action="upgrade",
            actor="user",
            details=f"Upgraded from gym {old_gym_id} to gym {new_gym_id}, today_pass={today_pass_id}, future_pass={future_pass_id}",
            before={"gym_id": old_gym_id, "status": p.status},
            after={"gym_id": new_gym_id, "status": "upgraded", "today_pass_id": today_pass_id, "future_pass_id": future_pass_id},
            client_id=p.client_id,
        )
        dps.add(audit_record)

        # 9) Calculate actual_paid and new_paid for response
        selected_time = p.selected_time or ""
        try:
            old_ppd = int(get_price_for_gym(dps, old_gym_id))
            new_ppd = int(get_price_for_gym(dps, new_gym_id))
            actual_paid = (len(future_days) * old_ppd) // 100
            new_paid = (len(future_days) * new_ppd) // 100
        except Exception:
            actual_paid = 0
            new_paid = 0

        # 10) Commit all changes
        dps.commit()

        return {
            "success": True,
            "payment_captured": True,
            "order_id": order.id,
            "payment_id": body.razorpay_payment_id,
            "daily_pass_activated": True,
            "daily_pass_details": {
                "today_pass_id": today_pass_id,
                "today_gym_id": old_gym_id if today_pass_id else None,
                "future_pass_id": future_pass_id,
                "future_gym_id": new_gym_id,
                "old_pass_id": pass_id,
                "future_days_count": len(future_days),
                "today_valid": today_pass_id is not None,
                "valid_from": future_days[0].scheduled_date.isoformat(),
                "valid_until": future_days[-1].scheduled_date.isoformat(),
                "status": "upgraded",
                "scheduled_dates": [day.scheduled_date.isoformat() for day in future_days],
                "actual_paid": actual_paid,
                "new_paid": new_paid,
                "selected_time": selected_time,
                "upgrade_info": {
                    "original_gym_id": old_gym_id,
                    "original_pass_id": pass_id,
                    "upgrade_date": tomorrow.isoformat(),
                }
            },
            "subscription_activated": False,
            "subscription_details": None,
            "total_amount": paid_amount,
            "currency": "INR",
            "message": "Upgrade completed successfully",
            "actual_paid": actual_paid,
            "new_paid": new_paid,
            "selected_time": selected_time,
        }

    except FittbotHTTPException:
        dps.rollback()
        raise
    except HTTPException:
        dps.rollback()
        raise
    except Exception as e:
        dps.rollback()
        logger.exception("Upgrade verify failed")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, f"Upgrade verify failed: {e}")
    finally:
        try:
            dps.close()
        except Exception:
            pass
