# app.py
from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import List, Optional
from zoneinfo import ZoneInfo

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel, field_serializer
from sqlalchemy import String, and_, distinct, func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import aliased

from app.models.async_database import get_async_db
from app.models.fittbot_models import Client, ClassSession
from app.models.fittbot_payments_models import Payment, Payout


class PaymentRow(BaseModel):
    id: int
    client_id: str
    client_name: Optional[str] = None
    date: date
    amount: Decimal
    status: str  # Owner-friendly status: Scheduled, Initiated, Deposited, Failed, On Hold
    payout_status: str  # Raw payout status: ready_for_transfer, initiated, processing, credited, failed, on_hold
    mode: str
    entitlement_id: Optional[str] = None
    payment_id: Optional[str] = None  # gateway_payment_id
    order_id: Optional[str] = None  # gateway_payment_id (razorpay order)
    session_name: Optional[str] = None  # Session/class name for session_booking
    scheduled_for: Optional[date] = None  # When payout is scheduled

    @field_serializer("amount")
    def _amount_to_float(self, v: Decimal, _info):
        return float(v)


class RevenueResponse(BaseModel):
    status: int = 200
    gym_id: str
    mode: str
    month: int
    year: int
    month_label: str
    revenue: float
    total_users: int
    rows: List[PaymentRow]


router = APIRouter(prefix="/fittbot_gym", tags=["gym status"])


def month_window(year: int, month: int) -> tuple[date, date]:
    start = date(year, month, 1)
    end = date(year + (month // 12), (month % 12) + 1, 1)
    return start, end


def _map_payout_status_for_owner(payout_status: str) -> str:

    status_map = {
        "ready_for_transfer": "Awaiting Settlement",  # Waiting for Razorpay to settle
        "scheduled": "Scheduled",  # Settlement confirmed, transfer scheduled
        "initiated": "Transfer Initiated",  # Bank transfer started
        "processing": "Processing",  # Bank processing
        "credited": "Deposited",  # Successfully deposited
        "failed": "Failed",
        "on_hold": "On Hold",
    }
    return status_map.get(payout_status, payout_status.replace("_", " ").title())


@router.get("/api/gym-revenue", response_model=RevenueResponse, status_code=200)
async def get_gym_revenue(
    gym_id: str = Query(..., description="Gym ID"),
    mode: str = Query(..., description="Payment mode filter (daily_pass, session, membership, pt_subscription)"),
    month: Optional[int] = Query(None, ge=1, le=12),
    year: Optional[int] = Query(None, ge=1970, le=2100),
    db: AsyncSession = Depends(get_async_db),
):
    ist = ZoneInfo("Asia/Kolkata")
    now_ist = datetime.now(ist)
    y = year or now_ist.year 
    m = month or now_ist.month

    start_d, end_d = month_window(y, m)

    source_type_map = {
        "membership": "gym_membership",
        "pt_subscription": "personal_training",
    }
    source_type = source_type_map.get(mode, mode)  # Pass through if not in map

    try:
        gym_id_int = int(gym_id)
    except ValueError:
        gym_id_int = 0

    # Build base filters for Payment table
    base_filters    = [
        Payment.gym_id == gym_id_int,
        Payment.source_type == source_type,
        func.date(Payment.paid_at) >= start_d,
        func.date(Payment.paid_at) < end_d,
        Payment.status == "paid",
    ]

    # For aggregates, only count payments that have been scanned (have Payout)
    PayoutForAgg = aliased(Payout)
    agg_stmt = (
        select(
            func.coalesce(func.sum(Payment.amount_net), 0),
            func.count(distinct(Payment.client_id)),
        )
        .select_from(Payment)
        .join(PayoutForAgg, PayoutForAgg.payment_id == Payment.id)  # INNER JOIN - only scanned
        .where(and_(*base_filters))
    )

    agg_result = await db.execute(agg_stmt)
    revenue_sum, user_count = agg_result.one()
    revenue_sum = float(revenue_sum or 0.0)

    # Query payments with payout status and client name
    PayoutAlias = aliased(Payout)
    ClientAlias = aliased(Client)
    SessionAlias = aliased(ClassSession)

    rows_stmt = (
        select(
            Payment.id,
            Payment.client_id,
            ClientAlias.name.label("client_name"),
            func.date(Payment.paid_at).label("date"),
            Payment.amount_net.label("amount"),  # Gym owner's base price
            Payment.source_type.label("mode"),
            Payment.entitlement_id,
            Payment.gateway_payment_id.label("payment_id"),
            Payment.gateway_payment_id.label("order_id"),
            Payment.session_id,
            SessionAlias.name.label("session_name"),
            PayoutAlias.status.label("payout_status"),
            PayoutAlias.scheduled_for,
        )
        .select_from(Payment)
        .join(PayoutAlias, PayoutAlias.payment_id == Payment.id)  # INNER JOIN - only show scanned payments
        .outerjoin(ClientAlias, ClientAlias.client_id == func.cast(Payment.client_id, String))
        .outerjoin(SessionAlias, SessionAlias.id == Payment.session_id)
        .where(and_(*base_filters))
        .order_by(Payment.paid_at.desc(), Payment.id.desc())
    )

    result = await db.execute(rows_stmt)
    rows = result.all()

    payload_rows = []

    for r in rows:
        owner_status = _map_payout_status_for_owner(r.payout_status)

        # amount_net contains gym owner's base price (stored at payment time)
        # For new payments: base price without markup
        # For old payments (fallback): we'll need to divide by markup
        final_amount = int(r.amount)  # r.amount is Payment.amount_net

        payload_rows.append(
            PaymentRow(
                id=r.id,
                client_id=str(r.client_id),
                client_name=r.client_name,
                date=r.date,
                amount=final_amount,  # Gym owner's base price
                status=owner_status,
                payout_status=r.payout_status,
                mode=r.mode,
                entitlement_id=r.entitlement_id,
                payment_id=r.payment_id,
                order_id=r.order_id,
                session_name=r.session_name,
                scheduled_for=r.scheduled_for,
            )
        )

    month_label = date(y, m, 1).strftime("%B %Y")

    check=RevenueResponse(
        status=200,
        gym_id=gym_id,
        mode=mode,
        month=m,
        year=y,
        month_label=month_label,
        revenue=revenue_sum,
        total_users=int(user_count or 0),
        rows=payload_rows,
    )

    print("Revenue Response Check:", check)

    return RevenueResponse(
        status=200,
        gym_id=gym_id,
        mode=mode,
        month=m,
        year=y,
        month_label=month_label,
        revenue=revenue_sum,
        total_users=int(user_count or 0),
        rows=payload_rows,
    )
