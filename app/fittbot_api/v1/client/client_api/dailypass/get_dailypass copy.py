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
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.utils.logging_utils import FittbotHTTPException
from app.models.database import get_db
from app.fittbot_api.v1.payments.config.settings import get_payment_settings
from app.fittbot_api.v1.payments.models.orders import Order
from app.fittbot_api.v1.payments.models.enums import StatusOrder
from app.fittbot_api.v1.payments.models.payments import Payment

# daily-pass DB (separate schema)
from app.models.dailypass_models import (
    get_dailypass_session,
    DailyPass,
    DailyPassDay,
    DailyPassAudit,
    LedgerAllocation,
    get_price_for_gym,
)

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

    row = db.execute(
        text(
            "SELECT name, location, city FROM gyms WHERE gym_id = :gid"
        ),
        {"gid": gym_id},
    ).one_or_none()
    if not row:
        return {"name": f"Gym {gym_id}", "location": None, "city": None}
    return {"name": row[0], "location": row[1], "city": row[2]}


def _once_used(db: Session, pass_id: str, action: str) -> bool:
    return (
        db.query(DailyPassAudit.id)
        .filter(DailyPassAudit.pass_id == pass_id, DailyPassAudit.action == action)
        .limit(1)
        .first()
        is not None
    )


def _hmac_qr(secret: str, payload: str) -> str:
    return hmac.new(secret.encode("utf-8"), payload.encode("utf-8"), hashlib.sha256).hexdigest()


def _today_ist() -> date:
    return _now_ist().date()


# ------------------------------- schemas ------------------------------------


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


class ListActiveResponse(BaseModel):
    client_id: str
    passes: List[PassSummary]


class RescheduleRequest(BaseModel):
    pass_id:str
    client_id:int
    new_start_date: str = Field(..., description="YYYY-MM-DD (consecutive range will be applied)")



class RescheduleResponse(BaseModel):
    pass_id: str
    old_range: Tuple[str, str]
    new_range: Tuple[str, str]
    days_moved: int
    message: str


class UpgradeInitiateRequest(BaseModel):
    new_gym_id: int
    pass_id:str
    client_id:int


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
    db: Session = Depends(get_db)
):

    today = _today_ist()
    # Use dailypass database session instead of main database
    dps = next(get_dailypass_session())

    try:
        # Get all passes for the client
        all_rows = (
            dps.query(DailyPass)
            .filter(
                DailyPass.client_id == client_id,
                today < DailyPass.valid_until
            )
            .order_by(DailyPass.created_at.desc())
            .all()
        )

        # Filter logic: exclude original passes that have been upgraded
        # Find passes with "upgraded" status and their corresponding original passes
        upgraded_passes = [p for p in all_rows if p.status == "upgraded"]
        original_pass_ids = {p.order_id for p in upgraded_passes if p.order_id}

        # Filter out original passes that have been upgraded, keep upgraded passes
        # If a pass ID appears in original_pass_ids, exclude it (it's the original that was upgraded)
        rows = [p for p in all_rows if p.id not in original_pass_ids]

        resp: List[PassSummary] = []
        for p in rows:
            # Count remaining days = days with scheduled_date >= today and status in sched/available
            remaining = (
                dps.query(DailyPassDay)
                .filter(
                    DailyPassDay.pass_id == p.id,
                    DailyPassDay.scheduled_date >= today,
                    DailyPassDay.status.in_(["scheduled", "available", "rescheduled"]),
                )
                .count()
            )
            next_days = (
                dps.query(DailyPassDay.scheduled_date)
                .filter(
                    DailyPassDay.pass_id == p.id,
                    DailyPassDay.scheduled_date >= today,
                    DailyPassDay.status.in_(["scheduled", "available", "rescheduled"]),
                )
                .order_by(DailyPassDay.scheduled_date.asc())
                .limit(5)
                .all()
            )
            next_dates = [d[0].isoformat() for d in next_days]

            can_res = not _once_used(dps, p.id, "reschedule")
            can_upg = not _once_used(dps, p.id, "upgrade")

            # Enhanced can_reschedule logic for various edge cases
            original_start_date = p.valid_from or p.start_date
            original_end_date = p.valid_until or p.end_date

            # Case 1: Pass has ended
            if today >= original_end_date:
                can_res = False

            # Case 2: Check if there are any future days that can be rescheduled
            future_reschedule_eligible_days = (
                dps.query(DailyPassDay)
                .filter(
                    DailyPassDay.pass_id == p.id,
                    DailyPassDay.scheduled_date >= (today + timedelta(days=1)),  # Tomorrow and beyond
                    DailyPassDay.status.in_(["scheduled", "available", "rescheduled"]),
                )
                .count()
            )

            if future_reschedule_eligible_days == 0:
                can_res = False  # No future days to reschedule

            # Case 3: Check if all remaining days are in the past or today
            all_remaining_days = (
                dps.query(DailyPassDay)
                .filter(
                    DailyPassDay.pass_id == p.id,
                    DailyPassDay.status.in_(["scheduled", "available", "rescheduled"]),
                )
                .all()
            )

            if all_remaining_days:
                latest_remaining_date = max(day.scheduled_date for day in all_remaining_days)
                if latest_remaining_date <= today:
                    can_res = False  # All remaining days are today or in the past

            # Case 4: If pass is completed (all days attended)
            total_pass_days = (
                dps.query(DailyPassDay)
                .filter(DailyPassDay.pass_id == p.id)
                .count()
            )

            attended_days = (
                dps.query(DailyPassDay)
                .filter(
                    DailyPassDay.pass_id == p.id,
                    DailyPassDay.status == "attended"
                )
                .count()
            )

            if total_pass_days > 0 and attended_days == total_pass_days:
                can_res = False  # All days are attended

            gym = _load_gym_info(db, int(p.gym_id))

            # Handle upgraded passes
            is_upgraded = p.status == "upgraded"
            old_gym_id = None
            old_gym_name = None

            if is_upgraded and p.order_id:
                # Find the original pass using the order_id (which now contains the original pass ID)
                original_pass = next((op for op in all_rows if op.id == p.order_id), None)
                if original_pass:
                    old_gym_id = int(original_pass.gym_id)
                    old_gym_info = _load_gym_info(db, old_gym_id)
                    old_gym_name = old_gym_info.get("name")

            # Handle partial schedule logic
            actual_days = None
            rescheduled_days = None
            is_edited = bool(p.partial_schedule)

            if p.partial_schedule:
                # Get all days for this pass
                all_pass_days = (
                    dps.query(DailyPassDay)
                    .filter(DailyPassDay.pass_id == p.id)
                    .order_by(DailyPassDay.scheduled_date.asc())
                    .all()
                )

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



            # Get actual daily pass price from dailypass_pricing table
            try:
                actual_price_minor = get_price_for_gym(dps, int(p.gym_id))
                actual_amount = actual_price_minor / 100  # Convert minor units to rupees
            except Exception:
                actual_amount = (p.amount_paid or 0) / 100  # Fallback to paid amount

            resp.append(
                PassSummary(
                    pass_id=p.id,
                    amount=actual_amount,
                    gym_id=int(p.gym_id),
                    gym_name=gym.get("name"),
                    locality=gym.get("location"),
                    city=gym.get("city"),
                    valid_from=(p.valid_from or p.start_date).isoformat(),
                    valid_until=(p.valid_until or p.end_date).isoformat(),
                    days_total=int(p.days_total or 0),
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
                )
            )

        print("response",resp)

        return ListActiveResponse(client_id=str(client_id), passes=resp)
    except Exception as e:
        raise
    finally:
        try:
            dps.close()
        except Exception:
            pass


# --------------------- POST /dailypass/{pass_id}/reschedule ------------------


@router.post("/edit", response_model=RescheduleResponse)
def reschedule_pass(
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

        # find days eligible to move (tomorrow and beyond, not attended/canceled)
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

        # new consecutive range with same count
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

        # Check if this is a partial reschedule (pass has already started)
        original_start_date = p.valid_from or p.start_date
        has_pass_started = today >= original_start_date

        print(f"DEBUG: today={today}")
        print(f"DEBUG: original_start_date={original_start_date}")
        print(f"DEBUG: original_end_date={p.valid_until or p.end_date}")
        print(f"DEBUG: has_pass_started={has_pass_started}")
        print(f"DEBUG: Before update - valid_from={p.valid_from}, valid_until={p.valid_until}")

        # Apply: keep same rows, only change scheduled_date (+ bump reschedule_count)
        for i, dpd in enumerate(eligible_days):
            old_date = dpd.scheduled_date
            dpd.scheduled_date = new_dates[i]
            dpd.reschedule_count = int(dpd.reschedule_count or 0) + 1
            print(f"DEBUG: Updated day {dpd.id}: {old_date} -> {new_dates[i]}, reschedule_count={dpd.reschedule_count}")
            dps.add(dpd)

        # FLUSH changes to database so we can query updated dates
        dps.flush()
        print("DEBUG: Flushed changes to database")

        # Update pass boundaries and partial_schedule flag AFTER updating individual days
        if has_pass_started:
            # Partial reschedule - pass has started, don't change from/to dates
            print("DEBUG: PARTIAL RESCHEDULE - Pass has started, setting partial_schedule=True")
            p.partial_schedule = True
            print(f"DEBUG: Keeping valid_from={p.valid_from}, valid_until={p.valid_until} unchanged")
        else:
            # Full reschedule - pass hasn't started yet, update from/to dates
            print("DEBUG: FULL RESCHEDULE - Pass hasn't started, updating valid_from/valid_until")
            # Get all days AFTER updating the eligible ones
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
            print(f"DEBUG: Updated valid_from: {old_valid_from} -> {p.valid_from}")
            print(f"DEBUG: Updated valid_until: {old_valid_until} -> {p.valid_until}")
            print(f"DEBUG: All days after update: {[d[0] for d in all_days]}")

        print(f"DEBUG: Final state - partial_schedule={p.partial_schedule}")
        print(f"DEBUG: Final state - valid_from={p.valid_from}, valid_until={p.valid_until}")

        dps.add(p)
        print("DEBUG: Added pass to session")

        # Audit
        audit_record = DailyPassAudit(
            pass_id=pass_id,
            action="reschedule",
            actor="user",
            details=f"{count} days {old_range} -> {new_range}",
            before={"old_range": old_range},
            after={"new_range": new_range},
            client_id=p.client_id,
        )
        dps.add(audit_record)
        print("DEBUG: Added audit record to session")

        print("DEBUG: About to commit transaction...")
        try:
            dps.commit()
            print("DEBUG: Transaction committed successfully!")

            # Verify what's actually in the database after commit
            fresh_pass = dps.query(DailyPass).filter(DailyPass.id == pass_id).first()
            print(f"DEBUG: Fresh query after commit - valid_from={fresh_pass.valid_from}, valid_until={fresh_pass.valid_until}, partial_schedule={fresh_pass.partial_schedule}")

            fresh_days = dps.query(DailyPassDay).filter(DailyPassDay.pass_id == pass_id).order_by(DailyPassDay.scheduled_date.asc()).all()
            print(f"DEBUG: Fresh days after commit: {[(d.id, d.scheduled_date, d.reschedule_count) for d in fresh_days]}")

        except Exception as e:
            print(f"DEBUG: COMMIT FAILED: {e}")
            raise
        return RescheduleResponse(
            pass_id=pass_id,
            old_range=old_range,
            new_range=new_range,
            days_moved=count,
            message="Rescheduled successfully",
        )
    except FittbotHTTPException:
        dps.rollback()
        raise
    except Exception as e:
        dps.rollback()
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


# ----------------- POST /dailypass/{pass_id}/upgrade/initiate ----------------


@router.post("/upgrade")
def upgrade_initiate(
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

        today = _today_ist()

        # remaining days = today or future (if today not attended), and not attended/canceled
        remaining_days = (
            dps.query(DailyPassDay)
            .filter(
                DailyPassDay.pass_id == pass_id,
                DailyPassDay.scheduled_date >= today,
                DailyPassDay.status.in_(["scheduled", "available", "rescheduled"]),
            )
            .order_by(DailyPassDay.scheduled_date.asc())
            .all()
        )
        if not remaining_days:
            raise FittbotHTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="No remaining days to upgrade",
                error_code="NO_REMAINING_DAYS"
            )

        old_gym_id = int(p.gym_id)
        new_gym_id = int(payload.new_gym_id)

        if new_gym_id == old_gym_id:
            raise FittbotHTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="New gym must be different from current gym",
                error_code="SAME_GYM_SELECTED"
            )

        try:
            old_ppd = int(get_price_for_gym(dps, old_gym_id))  # minor
            new_ppd = int(get_price_for_gym(dps, new_gym_id))  # minor
        except Exception as e:
            raise FittbotHTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Gym price not configured",
                error_code="PRICE_NOT_CONFIGURED",
                log_data={"error": str(e)}
            )

        if new_ppd <= old_ppd:
            raise FittbotHTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="New gym price must be higher (upgrade only)",
                error_code="INVALID_UPGRADE_PRICE"
            )

        n = len(remaining_days)
        delta_minor = (new_ppd - old_ppd) * n

        print("delta minor",delta_minor)
        print("new_ppd",new_ppd)
        print("old_ppd minor",old_ppd)
        print("n",n)
        print("remaining_days",remaining_days)

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
            "startDate": None,  # Not applicable for upgrade
            "includesSubscription": False,
            "displayTitle": f"Upgrade to Gym {new_gym_id} ({n} days)",
            "description": f"Daily pass upgrade from Gym {old_gym_id} to Gym {new_gym_id}",
            # Additional upgrade-specific fields
            "pass_id": pass_id,
            "old_gym_id": old_gym_id,
            "new_gym_id": new_gym_id,
            "remaining_days": n
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
def upgrade_verify(
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
                    "status": "active"
                },
                "subscription_activated": False,
                "subscription_details": None,
                "total_amount": order.gross_amount_minor,
                "currency": "INR",
                "message": "Upgrade payment already processed",
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
            logger.info("Upgrade already applied (audit-gate)")
            remaining_count = (
                dps.query(DailyPassDay)
                .filter(DailyPassDay.pass_id == pass_id, DailyPassDay.status.in_(["scheduled", "available", "rescheduled"]))
                .count()
            )
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
                    "status": p.status
                },
                "subscription_activated": False,
                "subscription_details": None,
                "total_amount": paid_amount,
                "currency": "INR",
                "message": "Upgrade already applied",
            }

        # 6) Get remaining days for upgrade
        today = _today_ist()
        remaining_days = (
            dps.query(DailyPassDay)
            .filter(
                DailyPassDay.pass_id == pass_id,
                DailyPassDay.scheduled_date >= today,
                DailyPassDay.status.in_(["scheduled", "available", "rescheduled"]),
            )
            .order_by(DailyPassDay.scheduled_date.asc())
            .all()
        )

        if not remaining_days:
            raise HTTPException(status.HTTP_409_CONFLICT, "No remaining days to upgrade")

        old_gym_id = int(p.gym_id)

        # Extract new_gym_id from order notes created during /upgrade initiate
        new_gym_id = None
        try:
            # First try to get from the original Razorpay order notes
            if hasattr(order, 'provider_order_id'):
                rzp_order = rzp_get_order(order.provider_order_id, settings)
                if 'notes' in rzp_order:
                    new_gym_id = rzp_order['notes'].get('new_gym_id')

            # If not in order notes, try payment details
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

        # 7) Create new daily_pass record with "upgraded" status
        new_pass_id = _new_id("dps")

        # Use original pass ID as the order_id reference for linking
        # Since original pass order_id might be None, we use the original pass ID itself
        old_order_id = p.id  # Use original pass ID instead of order_id
        print(f"DEBUG: Original pass ID (using as order_id): {old_order_id}")
        print(f"DEBUG: Original pass order_id: {p.order_id}")
        print(f"DEBUG: Original pass details - id: {p.id}, gym_id: {p.gym_id}, client_id: {p.client_id}")

        new_daily_pass = DailyPass(
            id=new_pass_id,
            user_id=p.user_id,
            client_id=p.client_id,
            gym_id=str(new_gym_id),
            order_id=old_order_id,  # Use original pass ID as reference
            payment_id=body.razorpay_payment_id,
            days_total=len(remaining_days),
            days_used=0,
            valid_from=remaining_days[0].scheduled_date,  # First remaining date
            valid_until=remaining_days[-1].scheduled_date,  # Last remaining date
            amount_paid=paid_amount,
            selected_time=p.selected_time,
            status="upgraded",  # Set status as "upgraded"
            policy=p.policy,
            partial_schedule=p.partial_schedule,
        )

        print(f"DEBUG: New pass created with order_id: {new_daily_pass.order_id}")
        dps.add(new_daily_pass)
        dps.flush()

        # 8) Update daily_pass_days records with new gym_id
        for day in remaining_days:
            day.gym_id = str(new_gym_id)
            day.client_id = p.client_id
            dps.add(day)

        # 9) Add audit record for upgrade
        audit_record = DailyPassAudit(
            pass_id=pass_id,
            action="upgrade",
            actor="user",
            details=f"Upgraded from gym {old_gym_id} to gym {new_gym_id}, {len(remaining_days)} days",
            before={"gym_id": old_gym_id, "status": p.status},
            after={"gym_id": new_gym_id, "status": "upgraded", "new_pass_id": new_pass_id},
            client_id=p.client_id,
        )
        dps.add(audit_record)

        # 10) Commit all changes
        dps.commit()

        return {
            "success": True,
            "payment_captured": True,
            "order_id": order.id,
            "payment_id": body.razorpay_payment_id,
            "daily_pass_activated": True,
            "daily_pass_details": {
                "pass_id": new_pass_id,
                "old_pass_id": pass_id,
                "gym_id": new_gym_id,
                "old_gym_id": old_gym_id,
                "remaining_days": len(remaining_days),
                "valid_from": remaining_days[0].scheduled_date.isoformat(),
                "valid_until": remaining_days[-1].scheduled_date.isoformat(),
                "status": "upgraded",
                "days_total": len(remaining_days)
            },
            "subscription_activated": False,
            "subscription_details": None,
            "total_amount": paid_amount,
            "currency": "INR",
            "message": "Upgrade completed successfully",
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
