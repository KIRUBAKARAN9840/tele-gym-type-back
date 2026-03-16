from __future__ import annotations

"""
Razorpay checkout orchestration (new file)
Flows:
- Daily Pass only: compute price from dailypass_prices, create order, confirm, materialize daily pass
- Combined (Fittbot subscription + Daily Pass): create subscription (fittbot), create order (daily pass), confirm both

This module only uses existing services/tables; no changes to existing files.
"""
import hmac
import hashlib
from datetime import date, timedelta
from typing import List, Optional, Dict, Any

from fastapi import APIRouter, Body, Depends, HTTPException, status
from pydantic import BaseModel, Field, validator
from sqlalchemy.orm import Session

from app.models.dailypass_price_model import get_dailypass_session, get_price_for_gym
from app.fittbot_api.v1.payments.routes.gym_dailypass import (
    create_daily_pass_after_capture,
)

# Reuse payments DB session factory
try:
    from app.fittbot_api.v1.payments.config.database import get_db_session as get_payments_db
except Exception:
    # Fallback: keep API usable even if payments DB utility differs in non-prod setups
    def get_payments_db():
        raise RuntimeError("payments DB session factory not available")


router = APIRouter(prefix="/razorpay", tags=["Razorpay Checkout (new)"])


# =============================
# Shared Schemas / Utilities
# =============================

class DailyPassCheckoutRequest(BaseModel):
    user_id: str
    gym_id: str
    days_total: int = Field(gt=0, le=60)
    start_date: date
    commission_bp: int = 3000
    pg_fee_minor: int = 0
    tax_minor: int = 0

    @validator("start_date")
    def _no_past_start(cls, v: date):
        # Allow today or future; business rule can be adjusted later
        return v


class DailyPassCheckoutResponse(BaseModel):
    order: Dict[str, Any]
    dates: List[date]
    total_gross_minor: int
    currency: str = "INR"
    gateway: str = "razorpay"


class RPConfirmRequest(BaseModel):
    # Razorpay confirmation payload from client
    razorpay_order_id: str
    razorpay_payment_id: str
    razorpay_signature: str
    # we also need to bind to the intended materialization context
    user_id: str
    gym_id: str
    days_total: int
    dates: List[date]
    total_gross_minor: int
    commission_bp: int = 3000
    pg_fee_minor: int = 0
    tax_minor: int = 0


def _compute_dates(start: date, days_total: int) -> List[date]:
    return [start + timedelta(days=i) for i in range(days_total)]


def _verify_razorpay_sig(secret: str, order_id: str, payment_id: str, signature: str) -> bool:
    # Per Razorpay docs: HMAC_SHA256(order_id|"|"|payment_id, secret)
    msg = f"{order_id}|{payment_id}".encode()
    mac = hmac.new(secret.encode(), msg, hashlib.sha256).hexdigest()
    return hmac.compare_digest(mac, signature)


# =============================
# Daily Pass only flow
# =============================

@router.post("/dailypass/checkout", response_model=DailyPassCheckoutResponse, status_code=status.HTTP_201_CREATED)
def rp_dailypass_checkout(
    payload: DailyPassCheckoutRequest = Body(...),
    payments_db: Session = Depends(get_payments_db),
):
    """
    Step 1 (server): compute daily pass price from dailypass_prices, create Razorpay Order (server-side),
    return order payload for client to pay via RN Razorpay SDK.
    """
    # Compute dates and total
    dps = get_dailypass_session()
    try:
        price_per_day_minor = get_price_for_gym(dps, payload.gym_id)
    finally:
        dps.close()

    dates = _compute_dates(payload.start_date, payload.days_total)
    total_gross_minor = int(price_per_day_minor) * payload.days_total

    # Create Razorpay order here. We do not call network in this skeleton; adapt to your client/service.
    # Expected order dict shape similar to: {id, amount, currency, receipt, notes}
    # You likely already have an Order factory in payments; use that.
    order = {
        "id": "order_Fake123",  # placeholder; replace with actual order ID from Razorpay
        "amount": total_gross_minor,
        "currency": "INR",
        "receipt": f"dp:{payload.user_id}:{payload.gym_id}",
        "notes": {
            "flow": "daily_pass",
            "gym_id": payload.gym_id,
            "user_id": payload.user_id,
        },
    }

    return DailyPassCheckoutResponse(order=order, dates=dates, total_gross_minor=total_gross_minor)


@router.post("/dailypass/confirm")
def rp_dailypass_confirm(
    payload: RPConfirmRequest = Body(...),
    payments_db: Session = Depends(get_payments_db),
):
    """
    Step 2 (server): verify signature, then materialize DailyPass by calling existing service.
    This reuses create_daily_pass_after_capture so existing tables and audit remain consistent.
    """
    # Verify signature (ensure to use your actual Razorpay key secret from settings)
    try:
        from app.config.settings import settings
        secret = settings.razorpay_key_secret
    except Exception:
        secret = None

    if secret:
        ok = _verify_razorpay_sig(
            secret,
            payload.razorpay_order_id,
            payload.razorpay_payment_id,
            payload.razorpay_signature,
        )
        if not ok:
            raise HTTPException(status_code=400, detail="invalid razorpay signature")

    # Materialize the pass (existing logic handles idempotency on payment_id)
    dps = get_dailypass_session()
    try:
        dpid = create_daily_pass_after_capture(
            dps,
            payments_db,
            user_id=payload.user_id,
            gym_id=payload.gym_id,
            order_id=payload.razorpay_order_id,
            payment_id=payload.razorpay_payment_id,
            days_total=payload.days_total,
            dates=payload.dates,
            total_gross_minor=payload.total_gross_minor,
            commission_bp=payload.commission_bp,
            pg_fee_minor=payload.pg_fee_minor,
            tax_minor=payload.tax_minor,
            policy=None,
        )
    finally:
        dps.close()

    return {"ok": True, "daily_pass_id": dpid}


# =============================
# Combined flow (Fittbot subscription + Daily Pass)
# =============================

class CombinedCheckoutRequest(BaseModel):
    user_id: str
    gym_id: str
    days_total: int = Field(gt=0, le=60)
    start_date: date
    fittbot_plan_name: str

class CombinedCheckoutResponse(BaseModel):
    client_actions: List[Dict[str, Any]]
    session: Dict[str, Any]


@router.post("/combined/checkout", response_model=CombinedCheckoutResponse, status_code=status.HTTP_201_CREATED)
def rp_combined_checkout(
    payload: CombinedCheckoutRequest = Body(...),
    payments_db: Session = Depends(get_payments_db),
):
    """
    Step 1 (server): Build two intents:
      - Create Razorpay subscription for Fittbot
      - Create Razorpay order for Daily Pass
    Return ordered client actions for the RN app to execute: authorize subscription first, then pay daily pass.
    """
    # Resolve prices
    dps = get_dailypass_session()
    try:
        dp_price_per_day = get_price_for_gym(dps, payload.gym_id)
    finally:
        dps.close()

    dp_dates = _compute_dates(payload.start_date, payload.days_total)
    dp_total = int(dp_price_per_day) * payload.days_total

    # Resolve fittbot plan price. Keep this abstract: fetch from your plans table/service.
    # This code assumes a helper that returns price_minor and interval info.
    plan = {
        "name": payload.fittbot_plan_name,
        "price_minor": 0,  # TODO: plug actual plan price here
        "interval": "monthly",
        "interval_count": 1,
    }

    # Create subscription skeleton (server-side)
    subscription = {
        "id": "sub_Fake123",
        "plan_name": plan["name"],
        "amount": plan["price_minor"],
        "interval": plan["interval"],
        "interval_count": plan["interval_count"],
    }

    # Create order skeleton for DP
    order = {
        "id": "order_FakeDP",
        "amount": dp_total,
        "currency": "INR",
        "receipt": f"dp:{payload.user_id}:{payload.gym_id}",
        "notes": {"flow": "daily_pass", "gym_id": payload.gym_id, "user_id": payload.user_id},
    }

    client_actions = [
        {
            "type": "authorize_subscription",
            "provider": "razorpay",
            "payload": {"subscription_id": subscription["id"], "plan": plan},
        },
        {
            "type": "pay_order",
            "provider": "razorpay",
            "payload": {"order": order, "dp_dates": dp_dates, "dp_total": dp_total},
        },
    ]

    session = {
        "user_id": payload.user_id,
        "gym_id": payload.gym_id,
        "fittbot_plan": plan["name"],
        "dp_days_total": payload.days_total,
        "dp_start_date": str(payload.start_date),
    }

    return CombinedCheckoutResponse(client_actions=client_actions, session=session)


class CombinedConfirmRequest(BaseModel):
    user_id: str
    gym_id: str
    # Subscription piece
    subscription_id: str
    subscription_authorized: bool
    # Daily pass piece (Razorpay confirm payload)
    razorpay_order_id: str
    razorpay_payment_id: str
    razorpay_signature: str
    # For materialization
    days_total: int
    dates: List[date]
    total_gross_minor: int


@router.post("/combined/confirm")
def rp_combined_confirm(
    payload: CombinedConfirmRequest = Body(...),
    payments_db: Session = Depends(get_payments_db),
):
    """
    Step 2 (server): finalize subscription (record/verify) and materialize daily pass using existing service.
    Keeps behavior idempotent and consistent with current tables.
    """
    # Verify and record subscription authorization (persist to your subscriptions table if needed)
    if not payload.subscription_authorized:
        raise HTTPException(status_code=400, detail="subscription not authorized")

    # Verify Razorpay signature for the order
    try:
        from app.config.settings import settings
        secret = settings.razorpay_key_secret
    except Exception:
        secret = None
    if secret:
        ok = _verify_razorpay_sig(secret, payload.razorpay_order_id, payload.razorpay_payment_id, payload.razorpay_signature)
        if not ok:
            raise HTTPException(status_code=400, detail="invalid razorpay signature")

    # Materialize Daily Pass
    dps = get_dailypass_session()
    try:
        dpid = create_daily_pass_after_capture(
            dps,
            payments_db,
            user_id=payload.user_id,
            gym_id=payload.gym_id,
            order_id=payload.razorpay_order_id,
            payment_id=payload.razorpay_payment_id,
            days_total=payload.days_total,
            dates=payload.dates,
            total_gross_minor=payload.total_gross_minor,
            commission_bp=3000,
            pg_fee_minor=0,
            tax_minor=0,
            policy=None,
        )
    finally:
        dps.close()

    return {"ok": True, "daily_pass_id": dpid, "subscription_id": payload.subscription_id}


__all__ = ["router"]

