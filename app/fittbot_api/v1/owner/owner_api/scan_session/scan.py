from __future__ import annotations

import logging
from datetime import date, datetime, timedelta, timezone
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, Field
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.async_database import get_async_db
from app.models.fittbot_models import (
    ClassSession,
    FittbotGymMembership,
    SessionBookingAudit,
    SessionBookingDay,
    SessionPurchase,
)
from app.models.fittbot_payments_models import Payment, Payout

logger = logging.getLogger("owner.scan_session")

router = APIRouter(prefix="/owner/scan_session", tags=["Gymowner"])

IST = timezone(timedelta(hours=5, minutes=30))


def _now_ist() -> datetime:
    return datetime.now(IST)


def _today_ist() -> date:
    return _now_ist().date()


class ScanSessionRequest(BaseModel):
    checkin_token: str = Field(..., min_length=4)
    gym_id: int = Field(..., gt=0)


class ScanSessionResponse(BaseModel):
    status: int
    message: str
    already_attended: bool = False
    session_id: Optional[int] = None


@router.post("/verify", response_model=ScanSessionResponse)
async def verify_session_scan(payload: ScanSessionRequest, db: AsyncSession = Depends(get_async_db)):

    today = _today_ist()
    token = payload.checkin_token.strip()
    if not token:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Missing checkin_token")

    try:

        booking_result = await db.execute(
            select(SessionBookingDay)
            .where(SessionBookingDay.checkin_token == token)
            .with_for_update(nowait=False)
        )

        booking: Optional[SessionBookingDay] = booking_result.scalars().first()
        if not booking:
            raise HTTPException(status.HTTP_404_NOT_FOUND, "Invalid or expired check-in token")

        if booking.gym_id != payload.gym_id:
            return{
                "status":403,
                "message":"Check-in date mismatch. Can only check-in on the booked date."}

        if booking.status == "attended":
            return ScanSessionResponse(status=200, message="Already checked-in. Repeat scan ignored.", already_attended=True)

        if booking.booking_date != today:
            return{
                "status":409,
                "message":"Check-in date mismatch. Can only check-in on the booked date."}

        purchase_result = await db.execute(
            select(SessionPurchase).where(SessionPurchase.id == booking.purchase_id)
        )

        purchase: Optional[SessionPurchase] = purchase_result.scalars().first()

        if not purchase:
            raise HTTPException(status.HTTP_404_NOT_FOUND, "Purchase not found for booking")
        if purchase.status != "paid":
            raise HTTPException(status.HTTP_409_CONFLICT, "Payment not completed for this booking")
        if not purchase.sessions_count or purchase.sessions_count <= 0:
            raise HTTPException(status.HTTP_409_CONFLICT, "Invalid session count on purchase")

        session_result = await db.execute(
            select(ClassSession).where(ClassSession.id == booking.session_id)
        )
        session_row: Optional[ClassSession] = session_result.scalars().first()
        if not session_row:
            raise HTTPException(status.HTTP_404_NOT_FOUND, "Session not found")


        payment_result = await db.execute(
            select(Payment).where(Payment.entitlement_id == token)
        )
        payment_row: Optional[Payment] = payment_result.scalars().first()

        if not payment_row:
            logger.error(f"Payment not found for entitlement_id: {token}")
            raise HTTPException(status.HTTP_404_NOT_FOUND, "Payment record not found for this booking")

        booking.status = "attended"
        booking.scanned_at = _now_ist()
        db.add(booking)

        db.add(
            SessionBookingAudit(
                purchase_id=booking.purchase_id,
                booking_day_id=booking.id,
                event="checkin",
                actor_role="gym_scanner",
                actor_id=payload.gym_id,
                notes={"token": token},
            )
        )

        locked_payment = await db.execute(
            select(Payment)
            .where(Payment.id == payment_row.id)
            .with_for_update(nowait=False)
        )
        locked_payment.scalars().first()

        existing_payout = await db.execute(
            select(Payout).where(Payout.payment_id == payment_row.id)
        )
        if existing_payout.scalars().first():
            logger.info(f"[SESSION_PAYOUT_EXISTS] payment_id={payment_row.id}, token={token} - skipping duplicate")
        else:
            db.add(
                Payout(
                    payment_id=payment_row.id,
                    gym_id=booking.gym_id,
                    gym_owner_id=None,
                    amount_gross=payment_row.amount_net,  
                    amount_net=payment_row.amount_net,  
                    status="ready_for_transfer",
                )
            )
       

        await db.commit()

        return ScanSessionResponse(status=200, message="Check-in recorded & payment captured", already_attended=False,session_id=booking.session_id)

    except HTTPException:
        await db.rollback()
        raise

    except Exception as exc:  
        await db.rollback()
        logger.exception("Session scan failed: %s", exc)
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, "Failed to verify session scan")
