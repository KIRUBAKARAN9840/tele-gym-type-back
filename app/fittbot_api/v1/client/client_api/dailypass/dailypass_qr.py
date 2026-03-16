from __future__ import annotations

import base64
import hashlib
import hmac
import json
import logging
import secrets
import time
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, Optional

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, Field
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.fittbot_api.v1.payments.config.settings import get_payment_settings
from app.models.dailypass_models import (
    DailyPass,
    DailyPassDay,
    DailyPassAudit,
    LedgerAllocation,
)
from app.models.fittbot_models import GymLocation, Attendance
from app.models.fittbot_payments_models import Payment as FittbotPayment, Payout
from app.models.async_database import get_async_db

logger = logging.getLogger("dailypass.qr")
router = APIRouter(prefix="/dailypass_qr", tags=["Daily Pass - QR & Scan"])

UTC = timezone.utc
IST = timezone(timedelta(hours=5, minutes=30))

# -----------------------------------------------------------------------------
# helpers
# -----------------------------------------------------------------------------

def _now_ist() -> datetime:
    return datetime.now(IST)


def _today_ist() -> date:
    return _now_ist().date()


def _new_id(prefix: str) -> str:
    return f"{prefix}{int(time.time()*1000)}_{secrets.token_hex(4)}"


def _sign(secret: str, payload_str: str) -> str:
    # HMAC-SHA256 signature (hex)
    return hmac.new(secret.encode("utf-8"), payload_str.encode("utf-8"), hashlib.sha256).hexdigest()


# -----------------------------------------------------------------------------
# Schemas
# -----------------------------------------------------------------------------

class QrPayloadResponse(BaseModel):
    status:int
    pid: str = Field(..., description="daily_pass_id")
    day_id: str
    gym_id: int
    date: str
    nonce: str
    exp: int
    sig: str
    b64: str
    in_punch: bool
    out_punch: bool
    session_id:int
    gym_location: dict
    in_time: Optional[str] = None
    out_time: Optional[str] = None


class ScanVerifyRequest(BaseModel):

    day_id: Optional[str] = None
    gym_id: Optional[int] = None


class ScanVerifyResponse(BaseModel):
    status: int
    already_attended: bool
    session_id:int

    message: str


@router.get("/get")
async def get_today_qr(daily_pass_id: str, db: AsyncSession = Depends(get_async_db)):
    try:
        # Get the daily pass - async query
        p_result = await db.execute(
            select(DailyPass).where(DailyPass.id == daily_pass_id)
        )
        p: Optional[DailyPass] = p_result.scalars().first()

        if not p or p.status not in ("active", "upgraded"):
            raise HTTPException(status.HTTP_404_NOT_FOUND, "Pass not found / inactive")

        today = _today_ist()

        # Get all daily_pass_days for this pass_id - async query
        all_days_result = await db.execute(
            select(DailyPassDay).where(DailyPassDay.daily_pass_id == p.id)
        )
        all_days = all_days_result.scalars().all()

        # Find today's day entry from all days
        today_day = None
        for day in all_days:
            if day.date == today:
                today_day = day
                break

        if not today_day:
            raise HTTPException(status.HTTP_404_NOT_FOUND, "No day entry for today")

        # Check if today's pass is valid and scannable
        if today_day.status == "attended":
            return{
                "status":200,
                "day_id": today_day.id,
                "message":"Today's pass already attended"
            }
        if today_day.status not in ("available", "scheduled", "rescheduled"):
            raise HTTPException(status.HTTP_409_CONFLICT, f"Day not scannable (status={today_day.status})")

        # Check attendance table for today's attendance - async query
        attendance_result = await db.execute(
            select(Attendance).where(
                Attendance.client_id == p.client_id,
                Attendance.gym_id == today_day.gym_id,
                Attendance.date == today
            )
        )
        attendance_record = attendance_result.scalars().first()

        # Determine punch status
        in_punch = True
        out_punch = True
        in_time_str = None
        out_time_str = None

        if attendance_record:
            if attendance_record.in_time:
                in_punch = False
                print("in time is",attendance_record.in_time)
                in_time_str = attendance_record.in_time.strftime("%H:%M:%S")
            if attendance_record.out_time:
                out_punch = False
                out_time_str = attendance_record.out_time.strftime("%H:%M:%S")

        # build payload - return only today's day_id
        payload = {
            "pid": p.id,
            "day_id": today_day.id,
            "gym_id": int(today_day.gym_id),
            "date": today.isoformat(),
            "nonce": secrets.token_hex(8),
            "exp": int(time.time()) + 15 * 60,
        }
        payload_str = json.dumps(payload, separators=(",", ":"))

        # Get gym location - async query
        location_result = await db.execute(
            select(GymLocation).where(GymLocation.gym_id == today_day.gym_id)
        )
        location_row = location_result.scalars().first()

        if location_row and location_row.latitude is not None and location_row.longitude is not None:
            gym_location = {
                "latitude": float(location_row.latitude),
                "longitude": float(location_row.longitude),
            }
        else:
            gym_location = {"latitude": None, "longitude": None}


        return QrPayloadResponse(
            status=200,
            pid=p.id,
            day_id=str(today_day.id),
            gym_id=int(today_day.gym_id),
            date=today.isoformat(),
            nonce=payload["nonce"],
            exp=payload["exp"],
            sig="sig",
            b64="b64",
            session_id=1,
            in_punch=in_punch,
            out_punch=out_punch,
            in_time=in_time_str,
            out_time=out_time_str,
            gym_location=gym_location
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Get QR failed: %s", e)
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, f"Get QR failed: {e}")


@router.post("/scan/verify")
async def scan_verify(body: ScanVerifyRequest, db: AsyncSession = Depends(get_async_db)):

    try:
        id = body.day_id

        data_result = await db.execute(
            select(DailyPassDay)
            .where(DailyPassDay.id == id)
            .with_for_update(nowait=False)
        )
        data: Optional[DailyPassDay] = data_result.scalars().first()

        if not data:
            raise HTTPException(status.HTTP_404_NOT_FOUND, "Invalid or expired check-in token")

        if data.status == "attended":
            return ScanVerifyResponse(
                status=200,
                session_id=1,
                already_attended=True,
                message="Already checked-in. Repeat scan ignored.",
            )

        # Check if different gym - return dict with status 403 (like scan.py)
        if data.gym_id != str(body.gym_id):
            return {
                "status": 403,
                "message": "Pass-day belongs to a different gym"
            }

        # Check if booking date is not today - return dict with status 409 (like scan.py)
        if data.scheduled_date != date.today():
            return {
                "status": 409,
                "message": "Check-in date mismatch. Can only check-in on the booked date."
            }

        # All checks passed - process the check-in
        data.status = "attended"
        data.checkin_at = datetime.now()
        db.add(data)

        # mark allocation ready for payout (if exists) - async query
        alloc_result = await db.execute(
            select(LedgerAllocation).where(LedgerAllocation.pass_day_id == data.id)
        )
        alloc: Optional[LedgerAllocation] = alloc_result.scalars().first()

        if alloc:
            alloc.status = "ready_for_payout"
            alloc.marked_ready_at = _now_ist()
            db.add(alloc)

        # audit
        db.add(
            DailyPassAudit(
                daily_pass_id=body.day_id,
                action="checkin",
                details=f"Checked-in at gym",
                timestamp=_now_ist(),
                client_id=data.client_id,
                actor="gym_scanner"
            )
        )

        await db.commit()

        # ═══════════════════════════════════════════════════════════════════
        # Find Payment by entitlement_id (DailyPassDay.id) and create Payout
        # - entitlement_id was set during payment verification in dailypass_processor
        # ═══════════════════════════════════════════════════════════════════
        payment_result = await db.execute(
            select(FittbotPayment).where(FittbotPayment.entitlement_id == str(data.id))
        )
        payment_row: Optional[FittbotPayment] = payment_result.scalars().first()

        if payment_row:
            # Lock the payment row to prevent duplicate payout creation
            locked_payment = await db.execute(
                select(FittbotPayment)
                .where(FittbotPayment.id == payment_row.id)
                .with_for_update(nowait=False)
            )
            locked_payment.scalars().first()

            # Check if Payout already exists for this payment to avoid duplicates
            existing_payout = await db.execute(
                select(Payout).where(Payout.payment_id == payment_row.id)
            )
            if existing_payout.scalars().first():
                logger.info(f"[DAILYPASS_PAYOUT_EXISTS] payment_id={payment_row.id}, day_id={data.id} - skipping duplicate")
            else:
                # Create Payout for this scan
                payout = Payout(
                    payment_id=payment_row.id,
                    gym_id=int(data.gym_id),
                    gym_owner_id=None,
                    amount_gross=payment_row.amount_net,  # Original amount before deductions
                    amount_net=payment_row.amount_net,  # Will be recalculated after settlement
                    status="ready_for_transfer",
                )
                db.add(payout)
                await db.commit()
                logger.info(f"[DAILYPASS_PAYOUT_CREATED] payment_id={payment_row.id}, day_id={data.id}")
        else:
            logger.warning(f"[DAILYPASS_PAYMENT_NOT_FOUND] entitlement_id={data.id}")

        return ScanVerifyResponse(
            status=200,
            session_id=1,
            already_attended=False,
            message="Check-in recorded & payout flagged",
        )

    except HTTPException:
        await db.rollback()
        raise
    except Exception as e:
        await db.rollback()
        logger.exception("Scan verify failed: %s", e)
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, f"Scan verify failed: {e}")

