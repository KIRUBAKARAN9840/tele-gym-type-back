from __future__ import annotations



import hmac
import hashlib
import secrets
import time
import logging
from datetime import date, timedelta, datetime, timezone
from typing import List, Optional, Dict, Any

from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel, Field, validator
from sqlalchemy.orm import Session
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from ..config.database import get_db_session
from ..config.settings import get_payment_settings
from ..models.enums import ItemType, EntType, StatusOrder, StatusPayment, SubscriptionStatus, StatusEnt
from ..models.orders import Order, OrderItem
from ..models.payments import Payment
from ..models.subscriptions import Subscription, now_ist
from ..models.entitlements import Entitlement
from ..services.entitlement_service import EntitlementService

from app.models.dailypass_models import (
    DailyPass,
    DailyPassDay,
    DailyPassAudit,
    LedgerAllocation,
    get_price_for_gym,
    get_actual_price_for_gym
)
from app.models.fittbot_plans_model import get_plan_by_id,get_plan_by_duration
from app.models.fittbot_models import ReferralFittbotCash, RewardProgramOptIn
from app.models.async_database import get_async_db

from .rp_client import create_order as rzp_create_order, get_payment as rzp_get_payment

logger = logging.getLogger("payments.unified_dailypass")
router = APIRouter(prefix="/pay", tags=["Unified Daily Pass Payments"])
security = HTTPBearer(auto_error=False)

UTC = timezone.utc
IST = timezone(timedelta(hours=5, minutes=30))
MAX_DAILY_PASS_DAYS = 365

# -----------------------------------------------------------------------------
# Utils
# -----------------------------------------------------------------------------

def _new_id(prefix: str) -> str:
    ts = int(time.time() * 1000)
    return f"{prefix}{ts}_{secrets.token_hex(4)}"


def _mask(value: Optional[str], left: int = 4, right: int = 4) -> str:
    if not value:
        return "***"
    if len(value) <= left + right:
        return "***"
    return f"{value[:left]}...{value[-right:]}"


def _verify_checkout_signature(key_secret: str, order_id: str, payment_id: str, signature: str) -> bool:
    try:
        data = f"{order_id}|{payment_id}".encode("utf-8")
        expected = hmac.new(key_secret.encode("utf-8"), data, hashlib.sha256).hexdigest()
        return hmac.compare_digest(expected, signature or "")
    except Exception:
        return False


def _validate_date_range(start_date: date, days_total: int) -> List[date]:
    if days_total <= 0 or days_total > MAX_DAILY_PASS_DAYS:
        raise HTTPException(status_code=400, detail=f"daysTotal must be 1..{MAX_DAILY_PASS_DAYS}")
    return [start_date + timedelta(days=i) for i in range(days_total)]


def _get_customer_id(creds: Optional[HTTPAuthorizationCredentials]) -> str:
    customer_id = "guest_user"
    if creds and creds.scheme.lower() == "bearer" and creds.credentials:
        try:
            from app.utils.security import SECRET_KEY, ALGORITHM
            from jose import jwt
            payload_jwt = jwt.decode(creds.credentials, SECRET_KEY, algorithms=[ALGORITHM])
            customer_id = payload_jwt.get("sub") or customer_id
        except Exception:
            pass
    return customer_id

# -----------------------------------------------------------------------------
# Daily Pass activation (unchanged business rules)
# -----------------------------------------------------------------------------

def _process_daily_pass_activation(
    db: Session,
    payments_db: Session,
    order_item: OrderItem,
    customer_id: str,
    payment_id: str,
) -> Dict[str, Any]:
    # Note: payments_db and db now point to same connection pool, use db for consistency
    logger.info(f"[DAILYPASS_ACTIVATION_START] order_item_id={order_item.id}, customer_id={customer_id}, payment_id={payment_id}")

    metadata = order_item.item_metadata or {}
    gym_id = int(order_item.gym_id)
    dates = [datetime.fromisoformat(d).date() for d in metadata.get("dates", [])]
    selected_time = metadata.get("selected_time")

    logger.info(f"[DAILYPASS_METADATA] gym_id={gym_id}, dates_count={len(dates)}, selected_time={selected_time}")

    if not dates:
        logger.error(f"[DAILYPASS_ERROR] Missing dates in metadata: {metadata}")
        raise HTTPException(500, "Daily pass metadata missing dates")

    # Get actual_price from item_metadata and convert to rupees (divide by 100)
    actual_price = metadata.get("actual_price", order_item.unit_price_minor)
    dailypass_price_rupees = actual_price // 100

    # Calculate amount_paid: start with subtotal, then subtract rewards if applied
    pricing_breakdown = metadata.get("pricing_breakdown", {})
    reward_details = metadata.get("reward_details", {})

    # Use subtotal from pricing breakdown (after multi-day discount)
    amount_before_rewards = pricing_breakdown.get("subtotal_minor", order_item.unit_price_minor * order_item.qty)

    # Subtract reward amount if applied
    reward_amount = reward_details.get("reward_amount_minor", 0) if reward_details else 0
    actual_amount_paid = amount_before_rewards - reward_amount

    logger.info(f"Daily pass amount calculation: subtotal={amount_before_rewards}, reward={reward_amount}, final={actual_amount_paid}")

    daily_pass = DailyPass(
        client_id=customer_id,
        gym_id=gym_id,
        start_date=dates[0],
        end_date=dates[-1],
        days_total=len(dates),
        amount_paid=actual_amount_paid,  # This is the ACTUAL amount paid after all discounts and rewards
        payment_id=payment_id,
        status="active",
        selected_time=selected_time,
        purchase_timestamp=datetime.now(IST),
    )

    logger.info(f"[DAILYPASS_CREATING] Creating DailyPass: client_id={customer_id}, gym_id={gym_id}, days={len(dates)}, amount={actual_amount_paid}")
    db.add(daily_pass)
    logger.info(f"[DAILYPASS_ADDED] DailyPass added to session, flushing...")
    db.flush()
    logger.info(f"[DAILYPASS_FLUSHED] DailyPass ID: {daily_pass.id}")

    logger.info(f"[DAILYPASS_DAYS] Creating {len(dates)} DailyPassDay records for pass_id={daily_pass.id}")
    day_records = []
    for i, d in enumerate(dates):
        rec = DailyPassDay(
            daily_pass_id=daily_pass.id,
            date=d,
            status="available",
            gym_id=gym_id,
            client_id=customer_id,
            dailypass_price=dailypass_price_rupees,
        )
        db.add(rec)
        db.flush()
        day_records.append(rec)
        if i == 0 or i == len(dates) - 1:
            logger.info(f"[DAILYPASS_DAY] Created day {i+1}/{len(dates)}: id={rec.id}, date={d}")
    logger.info(f"[DAILYPASS_DAYS_COMPLETE] Created {len(day_records)} day records")

    db.add(
        DailyPassAudit(
            daily_pass_id=daily_pass.id,
            action="purchase",
            details=f"Daily pass purchased for {len(dates)} days",
            timestamp=datetime.now(IST),
            client_id=customer_id,
            actor="system",
        )
    )

    total_minor = int(daily_pass.amount_paid)
    n = max(1, len(day_records))
    base, rem = divmod(total_minor, n)
    for i, dr in enumerate(day_records):
        amt = base + (1 if i < rem else 0)
        db.add(
            LedgerAllocation(
                daily_pass_id=daily_pass.id,
                pass_day_id=dr.id,
                gym_id=gym_id,
                client_id=customer_id,
                payment_id=payment_id,
                order_id=order_item.order_id,
                amount=amt,
                amount_net_minor=amt,
                allocation_date=datetime.now(IST).date(),
                status="allocated",
            )
        )

    # Don't commit here - let the caller handle the transaction
    logger.info(f"[DAILYPASS_ACTIVATION_COMPLETE] Successfully created pass {daily_pass.id} with {len(day_records)} days, NOT committing yet (caller will commit)")
    return {
        "daily_pass_id": daily_pass.id,
        "start_date": dates[0].isoformat(),
        "end_date": dates[-1].isoformat(),
        "days_total": len(dates),
        "status": "active",
    }

# -----------------------------------------------------------------------------
# Local subscription activation (no provider subscription)
# -----------------------------------------------------------------------------

def _process_local_subscription_activation(
    db: Session,
    order_item: OrderItem,
    customer_id: str,
    payment_id: str,
) -> Dict[str, Any]:
    meta = order_item.item_metadata or {}
    plan_id = meta.get("plan_id")
    duration_months = int(meta.get("duration_months") or 1)
    product_id = f"fittbot_plan_{plan_id}" if plan_id is not None else "fittbot_plan"

    now_ist = datetime.now(IST)
    sub = Subscription(
        id=_new_id("sub_"),
        customer_id=customer_id,
        provider="internal_manual",
        product_id=str(product_id),
        status=SubscriptionStatus.active,
        rc_original_txn_id=None,
        latest_txn_id=payment_id,
        active_from=now_ist,
        active_until=(now_ist + timedelta(days=30*duration_months)),  # coarse months; business can switch to relativedelta if needed
        auto_renew=False,
    )
    db.add(sub)
    logger.debug(f"Created subscription {sub.id} for customer {customer_id}, plan {plan_id}, duration {duration_months} months")

    order = db.query(Order).filter(Order.id == order_item.order_id).first()
    if order:
        ents = (
            db.query(Entitlement)
            .join(OrderItem, Entitlement.order_item_id == OrderItem.id)
            .filter(OrderItem.order_id == order.id)
            .all()
        )
        if not ents:
            EntitlementService(db).create_entitlements_from_order(order)
            logger.debug(f"Created entitlements for order {order.id}")
            ents = (
                db.query(Entitlement)
                .join(OrderItem, Entitlement.order_item_id == OrderItem.id)
                .filter(OrderItem.order_id == order.id)
                .all()
            )
        for e in ents:
            if e.order_item_id == order_item.id:
                e.entitlement_type = EntType.app
                e.active_from = sub.active_from
                e.active_until = sub.active_until
                e.status = StatusEnt.active
                db.add(e)
                logger.debug(f"Updated entitlement {e.id} for subscription {sub.id}")

    # Don't commit here - let the caller handle the transaction
    return {
        "subscription_id": sub.id,
        "plan_id": plan_id,
        "active_from": sub.active_from.isoformat(),
        "active_until": sub.active_until.isoformat(),
        "status": "active",
        "provider": "internal_manual",
    }

# -----------------------------------------------------------------------------
# Schemas
# -----------------------------------------------------------------------------

class UnifiedCheckoutRequest(BaseModel):
    gymId: int = Field(...)
    clientId: str = Field(...)
    startDate: str = Field(..., description="YYYY-MM-DD")
    daysTotal: int = Field(..., ge=1, le=MAX_DAILY_PASS_DAYS)
    selectedTime: Optional[str] = Field(None)

    includeSubscription: bool = Field(False)
    selectedPlan: Optional[int] = Field(None, description="fittbot_plans.id")
    reward: bool = Field(False)
    # REMOVED: is_offer_eligible - now calculated server-side for security

    # Legacy/ignored client numbers (server becomes source of truth)
    dailyPassAmount: Optional[int] = Field(None)
    subscriptionAmount: Optional[int] = Field(None)
    rewardDiscount: Optional[int] = Field(0)
    finalAmount: Optional[int] = Field(None)

    @validator("startDate")
    def _v_date(cls, v):
        try:
            date.fromisoformat(v)
            return v
        except ValueError:
            raise ValueError("startDate must be YYYY-MM-DD")

class UnifiedCheckoutResponse(BaseModel):
    success: bool
    orderId: str
    razorpayOrderId: str
    razorpayKeyId: str
    amount: int
    currency: str
    # breakdown
    dailyPassAmount: int
    subscriptionAmount: int
    finalAmount: int
    # service
    gymId: int
    daysTotal: int
    startDate: str
    includesSubscription: bool
    displayTitle: str
    description: str
    reward_applied: Optional[int]

class UnifiedVerificationRequest(BaseModel):
    razorpay_order_id: str
    razorpay_payment_id: str
    razorpay_signature: str
    reward: Optional[bool] = False
    reward_applied: Optional[int] = 0

class UnifiedVerificationResponse(BaseModel):
    success: bool
    payment_captured: bool
    order_id: str
    payment_id: str
    daily_pass_activated: bool
    daily_pass_details: Optional[Dict[str, Any]] = None
    subscription_activated: bool
    subscription_details: Optional[Dict[str, Any]] = None
    total_amount: int
    currency: str
    message: str

# -----------------------------------------------------------------------------
# Calculate Reward
# -----------------------------------------------------------------------------

class DailyPassRewardRequest(BaseModel):
    client_id: int
    amount:int


@router.post("/calculate_reward")
async def calculate_dailypass_reward(request: DailyPassRewardRequest, db: AsyncSession = Depends(get_async_db)):

    try:
        client_id = request.client_id
       
        # Check if client has opted into the reward program
        opt_in_result = await db.execute(
            select(RewardProgramOptIn).where(
                RewardProgramOptIn.client_id == client_id
            )
        )
        opt_in = opt_in_result.scalars().first()

        opted_in = bool(opt_in and opt_in.status == "active")

        amount = (request.amount) * 100
        ten_percent_minor = int(amount * 0.10)
        capped_reward_minor = ten_percent_minor

        fittbot_cash_result = await db.execute(
            select(ReferralFittbotCash).where(
                ReferralFittbotCash.client_id == client_id
            )
        )
        fittbot_cash_entry = fittbot_cash_result.scalars().first()

        available_fittbot_cash_rupees = fittbot_cash_entry.fittbot_cash if fittbot_cash_entry else 0
        available_fittbot_cash_minor = available_fittbot_cash_rupees * 100


        if available_fittbot_cash_minor >= capped_reward_minor:
            reward_amount = capped_reward_minor
        else:
            reward_amount = available_fittbot_cash_minor

        reward_amount = round(reward_amount * 0.01)


        return {
            "status": 200,
            "reward_amount": reward_amount,
            "opted_in": opted_in
        }

    except HTTPException:
        raise
    except Exception as e:
        await db.rollback()
        logger.error(f"Error calculating daily pass reward: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Unable to calculate reward: {str(e)}"
        )


# -----------------------------------------------------------------------------
# Checkout: single RP order, server-authoritative pricing
# -----------------------------------------------------------------------------

@router.post("/dailypass/checkout", response_model=UnifiedCheckoutResponse)
async def unified_checkout(
    payload: UnifiedCheckoutRequest,
    db: Session = Depends(get_db_session),
):
    cid = payload.clientId
    settings = get_payment_settings()
    logger.debug(f"Checkout request - Client: {cid}, Gym: {payload.gymId}, Days: {payload.daysTotal}, Include subscription: {payload.includeSubscription}")

    try:
        start_date = date.fromisoformat(payload.startDate)
        dp_dates = _validate_date_range(start_date, payload.daysTotal)
        logger.debug(f"Validated date range: {payload.startDate} to {dp_dates[-1].isoformat()} ({len(dp_dates)} days)")
    
    except ValueError as e:
        logger.error(f"Invalid date in checkout request: {e}")
        raise HTTPException(400, f"Invalid date: {e}")

    # 2) Server-authoritative DP pricing (with >=4d 10% discount)
    try:
        per_day_minor = get_price_for_gym(db, payload.gymId)
        actual_price = get_actual_price_for_gym(db, payload.gymId)
        dp_gross = per_day_minor * payload.daysTotal
        if payload.daysTotal >= 5:
            dp_discount = (dp_gross * 10) // 100
            logger.debug(f"Daily pass discount applied: {dp_discount} paisa (10% for {payload.daysTotal} days)")
            dp_total = dp_gross - dp_discount
            logger.debug(f"Daily pass total after discount: {dp_total} paisa")
        else:
            dp_discount = 0
            dp_total = dp_gross
        logger.debug(f"Daily pass pricing - Gym: {payload.gymId}, Days: {payload.daysTotal}, Per day: {per_day_minor}, Gross: {dp_gross}, Discount: {dp_discount}, Total: {dp_total}")
    except Exception as e:
        logger.error(f"DP pricing failed: {e}")
        raise HTTPException(500, "Unable to load gym pricing")

    # 3) Subscription pricing from fittbot_plans (if included)
    sub_total = 0
    plan_duration_months = None
    print("######## SUBSCRIPTION CHECK ########")
    print(f"payload.includeSubscription: {payload.includeSubscription}")

    if payload.includeSubscription:
        print(f"Selected Plan ID: {payload.selectedPlan}")
        if not payload.selectedPlan:
            raise HTTPException(400, "selectedPlan is required when includeSubscription=true")

        plan = get_plan_by_duration(db, payload.selectedPlan)
        print(f"Plan fetched from DB: {plan}")
        print(f"Plan ID: {plan.id if plan else 'None'}")
        print(f"Plan attributes: {dir(plan) if plan else 'None'}")

        logger.debug(f"Fetched plan: {plan.id if plan else 'None'}")
        if not plan:
            raise HTTPException(404, "Plan not found")

        # Flexible accessors depending on your model
        sub_total = int(getattr(plan, "price", None))
        print(f"Plan price (sub_total): {sub_total} paisa")
        logger.debug(f"Subscription total from plan: {sub_total} paisa")

        if sub_total and sub_total < 100:  # if amount is in rupees accidentally
            sub_total *= 100
            print(f"Converted subscription amount to paisa: {sub_total}")
            logger.debug(f"Converted subscription amount to paisa: {sub_total}")
        plan_duration_months = int(getattr(plan, "duration", 1))
        print(f"Plan duration: {plan_duration_months} months")
        logger.debug(f"Plan duration: {plan_duration_months} months")
        if sub_total <= 0 or plan_duration_months <= 0:
            raise HTTPException(409, "Invalid plan configuration")

    print(f"######## FINAL SUBSCRIPTION VALUES ########")
    print(f"sub_total: {sub_total} paisa ({sub_total/100} rs)")
    print(f"plan_duration_months: {plan_duration_months}")

    logger.debug(f"Order totals - Daily pass: {dp_total}, Subscription: {sub_total}")
    gross_total_before_rewards = dp_total + sub_total
    logger.debug(f"Gross total before rewards: {gross_total_before_rewards} paisa")

    # 4) Calculate rewards BEFORE creating order
    print("#########dailypass total is",dp_total)
    reward_amount = 0
    reward_calculation_details = {}

    if payload.reward:
        print("dp total is",dp_total)
        amount = dp_total
        ten_percent_minor = int(amount * 0.10)
        capped_reward_minor = ten_percent_minor

        fittbot_cash_entry = db.query(ReferralFittbotCash).filter(
            ReferralFittbotCash.client_id == payload.clientId
        ).first()

        available_fittbot_cash_rupees = fittbot_cash_entry.fittbot_cash if fittbot_cash_entry else 0
        available_fittbot_cash_minor = available_fittbot_cash_rupees * 100

        logger.info(f"Available fittbot_cash for client {payload.clientId}: {available_fittbot_cash_rupees}₹ ({available_fittbot_cash_minor} paisa)")

        if available_fittbot_cash_minor >= capped_reward_minor:
            reward_amount = capped_reward_minor
        else:
            reward_amount = available_fittbot_cash_minor

        print("gross total is", gross_total_before_rewards)
        print("reward amount", reward_amount)

        # Store reward calculation details for order metadata
        reward_calculation_details = {
            "reward_applied": True,
            "reward_amount_minor": reward_amount,
            "reward_amount_rupees": reward_amount / 100,
            "ten_percent_cap_minor": ten_percent_minor,
            "available_fittbot_cash_minor": available_fittbot_cash_minor,
            "available_fittbot_cash_rupees": available_fittbot_cash_rupees,
            "calculation_base": "daily_pass_total"
        }

    # Calculate final grand total after rewards
    grand_total = gross_total_before_rewards - reward_amount
    logger.debug(f"Grand total after rewards: {grand_total} paisa (reward: {reward_amount} paisa)")

    # 5) Create internal order with COMPLETE transaction details - Enterprise-level tracking
    order_metadata = {
        # ============ ORDER IDENTIFICATION ============
        "order_info": {
            "order_type": "unified_dailypass_local_sub" if payload.includeSubscription else "dailypass_only",
            "customer_id": cid,
            "created_at": datetime.now(IST).isoformat(),
            "currency": "INR",
            "flow": "unified_dailypass_local_sub" if payload.includeSubscription else "dailypass_only",
        },

        # ============ WHAT'S IN THIS ORDER? ============
        "order_composition": {
            "includes_daily_pass": True,
            "includes_subscription": payload.includeSubscription,
            "items_count": 2 if payload.includeSubscription else 1,
        },

        # ============ COMPLETE PAYMENT BREAKDOWN ============
        "payment_summary": {
            # Step 1: Calculate base amounts
            "step_1_base_amounts": {
                "daily_pass_base_minor": dp_gross,
                "daily_pass_base_rupees": dp_gross / 100,
                "subscription_base_minor": sub_total,
                "subscription_base_rupees": sub_total / 100,
                "total_base_minor": dp_gross + sub_total,
                "total_base_rupees": (dp_gross + sub_total) / 100,
            },

            # Step 2: Apply multi-day discount (if applicable)
            "step_2_multi_day_discount": {
                "discount_applicable": dp_discount > 0,
                "discount_percentage": 10 if payload.daysTotal >= 5 else 0,
                "discount_reason": f"{payload.daysTotal} days >= 4 days" if payload.daysTotal >= 5 else "No discount",
                "discount_amount_minor": dp_discount,
                "discount_amount_rupees": dp_discount / 100,
                "subtotal_after_discount_minor": gross_total_before_rewards,
                "subtotal_after_discount_rupees": gross_total_before_rewards / 100,
            },

            # Step 3: Apply fittbot cash reward (if applicable)
            "step_3_reward_deduction": {
                "reward_requested": payload.reward,
                "reward_applied": reward_amount > 0,
                "reward_amount_minor": reward_amount,
                "reward_amount_rupees": reward_amount / 100,
                "reward_source": "fittbot_cash",
                "available_fittbot_cash_minor": reward_calculation_details.get("available_fittbot_cash_minor", 0) if payload.reward else 0,
                "available_fittbot_cash_rupees": reward_calculation_details.get("available_fittbot_cash_rupees", 0) if payload.reward else 0,
                "ten_percent_cap_minor": reward_calculation_details.get("ten_percent_cap_minor", 0) if payload.reward else 0,
                "reward_calculation": f"min(10% of {dp_total/100}rs, available cash {reward_calculation_details.get('available_fittbot_cash_rupees', 0)}rs) = {reward_amount/100}rs" if payload.reward else "No reward applied",
            },

            # Step 4: Final amount
            "step_4_final_amount": {
                "final_amount_minor": grand_total,
                "final_amount_rupees": grand_total / 100,
                "amount_saved_minor": (dp_gross + sub_total) - grand_total,
                "amount_saved_rupees": ((dp_gross + sub_total) - grand_total) / 100,
                "savings_percentage": round(((dp_gross + sub_total - grand_total) / (dp_gross + sub_total)) * 100, 2) if (dp_gross + sub_total) > 0 else 0,
            },

            # Human-readable summary
            "calculation_formula": f"({(dp_gross + sub_total)/100}rs base - {dp_discount/100}rs discount - {reward_amount/100}rs reward) = {grand_total/100}rs paid",
            "one_line_summary": f"Paid {grand_total/100}rs (saved {((dp_gross + sub_total) - grand_total)/100}rs from {(dp_gross + sub_total)/100}rs)",
        },

        # ============ DAILY PASS DETAILS ============
        "daily_pass": {
            "included": True,
            "gym_id": payload.gymId,
            "days_purchased": payload.daysTotal,
            "date_range": {
                "start_date": payload.startDate,
                "end_date": dp_dates[-1].isoformat(),
                "dates": [d.isoformat() for d in dp_dates],
            },
            "timing": {
                "selected_time": payload.selectedTime,
            },
            "pricing": {
                "per_day_price_minor": per_day_minor,
                "per_day_price_rupees": per_day_minor / 100,
                "actual_gym_price_minor": actual_price,
                "actual_gym_price_rupees": actual_price / 100,
                "gross_amount_minor": dp_gross,
                "gross_amount_rupees": dp_gross / 100,
                "discount_applied_minor": dp_discount,
                "discount_applied_rupees": dp_discount / 100,
                "final_daily_pass_cost_minor": dp_total - reward_amount,  # Daily pass portion after all deductions
                "final_daily_pass_cost_rupees": (dp_total - reward_amount) / 100,
            },
            "summary": f"{payload.daysTotal} days at {per_day_minor/100}rs/day = {dp_gross/100}rs, after discount = {dp_total/100}rs, after reward = {(dp_total - reward_amount)/100}rs",
        },

        # ============ SUBSCRIPTION DETAILS ============
        "subscription": {
            "included": payload.includeSubscription,
            "plan_id": payload.selectedPlan if payload.includeSubscription else None,
            "duration_months": plan_duration_months if payload.includeSubscription else None,
            "pricing": {
                "amount_minor": sub_total if payload.includeSubscription else 0,
                "amount_rupees": sub_total / 100 if payload.includeSubscription else 0,
            },
            "summary": f"{plan_duration_months} months subscription for {sub_total/100}rs" if payload.includeSubscription else "No subscription included",
        } if payload.includeSubscription else {
            "included": False,
            "summary": "No subscription included",
        },

        # ============ REWARD DETAILS ============
        "reward": {
            "reward_used": payload.reward and reward_amount > 0,
            "reward_details": reward_calculation_details if payload.reward else None,
            "summary": f"Fittbot cash {reward_amount/100}rs applied" if (payload.reward and reward_amount > 0) else "No reward used",
        },

        # ============ AMOUNTS SUMMARY (Quick Reference) ============
        "amounts": {
            "base_amount": (dp_gross + sub_total) / 100,
            "total_discounts": (dp_discount + reward_amount) / 100,
            "final_paid": grand_total / 100,
            "currency": "INR",
        },

        # ============ AUDIT INFO ============
        "audit": {
            "created_by": "system",
            "source": "dailypass_checkout_api",
            "api_version": "v1",
            "client_payload": {
                "reward_requested": payload.reward,
                "subscription_requested": payload.includeSubscription,
            }
        }
    }

    order = Order(
        id=_new_id("ord_"),
        customer_id=cid,
        provider="razorpay_pg",
        currency="INR",
        gross_amount_minor=grand_total,  # This is the ACTUAL amount to be paid
        status=StatusOrder.pending,
        order_metadata=order_metadata,  # Complete transaction summary
    )
    db.add(order)
    db.flush()

    # Create order items with complete transaction tracking
    dp_item_metadata = {
        "dates": [d.isoformat() for d in dp_dates],
        "selected_time": payload.selectedTime,
        "start_date": payload.startDate,
        "end_date": dp_dates[-1].isoformat(),
        "gym_id": payload.gymId,
        "discount_minor": dp_discount,
        "actual_price": actual_price,
        # Transaction details for enterprise tracking
        "pricing_breakdown": {
            "per_day_price_minor": per_day_minor,
            "per_day_price_rupees": per_day_minor / 100,
            "days_count": payload.daysTotal,
            "gross_amount_minor": dp_gross,
            "gross_amount_rupees": dp_gross / 100,
            "discount_applied": dp_discount > 0,
            "discount_minor": dp_discount,
            "discount_rupees": dp_discount / 100,
            "discount_percentage": 10 if payload.daysTotal >= 5 else 0,
            "subtotal_after_discount_minor": dp_total,
            "subtotal_after_discount_rupees": dp_total / 100,
            "reward_deducted_minor": reward_amount,
            "reward_deducted_rupees": reward_amount / 100,
            "final_amount_paid_minor": dp_total - reward_amount,
            "final_amount_paid_rupees": (dp_total - reward_amount) / 100,
            # Calculation formula for clarity
            "calculation": f"({dp_gross/100} - {dp_discount/100} - {reward_amount/100}) = {(dp_total - reward_amount)/100} rs"
        },
        "reward_details": reward_calculation_details if payload.reward else None
    }

    db.add(
        OrderItem(
            id=_new_id("itm_"),
            order_id=order.id,
            item_type=ItemType.daily_pass,
            gym_id=str(payload.gymId),
            unit_price_minor=per_day_minor,
            qty=payload.daysTotal,
            item_metadata=dp_item_metadata,
        )
    )

    # Local subscription item (no provider subscription will be created)
    if payload.includeSubscription:
        sub_item_metadata = {
            "plan_id": payload.selectedPlan,
            "duration_months": plan_duration_months,
            "provider": "internal_manual",
            # Transaction details
            "pricing_breakdown": {
                "plan_price_minor": sub_total,
                "plan_price_rupees": sub_total / 100,
                "duration_months": plan_duration_months
            }
        }

        db.add(
            OrderItem(
                id=_new_id("itm_"),
                order_id=order.id,
                item_type=ItemType.app_subscription,
                unit_price_minor=sub_total,
                qty=1,
                item_metadata=sub_item_metadata,
            )
        )

    db.flush()

    # 6) Create Razorpay order with complete transaction tracking
    logger.debug(f"Grand total before rounding: {grand_total} paisa")
    grand_total = round(grand_total / 100)
    grand_total = grand_total * 100
    logger.debug(f"Grand total after rounding: {grand_total} paisa ({grand_total / 100} rupees)")

    rzp_order = rzp_create_order(
        amount_minor=grand_total,
        currency="INR",
        receipt=order.id,
        notes={
            # Order identification
            "order_id": order.id,
            "customer_id": cid,
            "gym_id": payload.gymId,
            "flow": "unified_dailypass_local_sub" if payload.includeSubscription else "dailypass_only",

            # Transaction breakdown (for enterprise tracking)
            "gross_before_rewards": str(gross_total_before_rewards),
            "daily_pass_subtotal": str(dp_total),
            "subscription_subtotal": str(sub_total),
            "reward_applied": str(reward_amount),
            "final_amount": str(grand_total),

            # Additional flags
            "includes_subscription": str(payload.includeSubscription),
            "reward_used": str(payload.reward and reward_amount > 0),
        },
        settings=settings,
    )

    order.provider_order_id = rzp_order["id"]
    db.add(order)
    db.commit()
    logger.debug(f"Order {order.id} created successfully with Razorpay order ID {rzp_order['id']}, grand total: {grand_total} paisa")

    response=UnifiedCheckoutResponse(
        success=True,
        orderId=order.id,
        razorpayOrderId=rzp_order["id"],
        razorpayKeyId=settings.razorpay_key_id,
        amount=grand_total,
        currency="INR",
        dailyPassAmount=dp_total,
        subscriptionAmount=sub_total,
        finalAmount=grand_total,
        gymId=payload.gymId,
        daysTotal=payload.daysTotal,
        startDate=payload.startDate,
        includesSubscription=payload.includeSubscription,
        displayTitle=f"{payload.daysTotal} Day Pass" + (" + Membership" if payload.includeSubscription else ""),
        description="Daily pass" + (" with app membership" if payload.includeSubscription else ""),
        reward_applied=reward_amount
    )

    
    return response


@router.post("/dailypass/verify", response_model=UnifiedVerificationResponse)
async def unified_verify_payment(
    request: UnifiedVerificationRequest,
    db: Session = Depends(get_db_session),
):
    settings = get_payment_settings()
    logger.debug(f"Payment verification request - Order: {request.razorpay_order_id}, Payment: {_mask(request.razorpay_payment_id)}")

    if not _verify_checkout_signature(
        settings.razorpay_key_secret,
        request.razorpay_order_id,
        request.razorpay_payment_id,
        request.razorpay_signature,
    ):
        logger.error(f"Invalid payment signature for order {request.razorpay_order_id}")
        raise HTTPException(status.HTTP_403_FORBIDDEN, "Invalid payment signature")

    logger.debug(f"Payment signature verified for order {request.razorpay_order_id}")

    # 2) Locate order
    order = db.query(Order).filter(Order.provider_order_id == request.razorpay_order_id).first()
    if not order:
        logger.error(f"Order not found for Razorpay order ID {request.razorpay_order_id}")
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Order not found")

    logger.debug(f"Order {order.id} found for customer {order.customer_id}, amount: {order.gross_amount_minor} paisa")

    # 3) Fetch payment and validate amount/status
    payment_data = rzp_get_payment(request.razorpay_payment_id, settings)
    logger.debug(f"Payment data fetched - Status: {payment_data.get('status')}, Amount: {payment_data.get('amount')}")

    if payment_data.get("status") != "captured":
        logger.error(f"Payment not captured - Status: {payment_data.get('status')}")
        raise HTTPException(status.HTTP_400_BAD_REQUEST, f"Payment not captured (status={payment_data.get('status')})")

    existing_payment = (
        db.query(Payment)
        .filter(Payment.provider_payment_id == request.razorpay_payment_id, Payment.status == StatusPayment.captured)
        .first()
    )
    if existing_payment:
        logger.debug(f"Payment {request.razorpay_payment_id} already processed, returning cached response")
        return UnifiedVerificationResponse(
            success=True,
            payment_captured=True,
            order_id=order.id,
            payment_id=request.razorpay_payment_id,
            daily_pass_activated=True,
            daily_pass_details=None,
            subscription_activated=True,
            subscription_details=None,
            total_amount=order.gross_amount_minor,
            currency="INR",
            message="Payment already processed",
        )

    # 5) Record payment + mark order paid
    try:
        pay = Payment(
            id=_new_id("pay_"),
            order_id=order.id,
            customer_id=order.customer_id,
            provider="razorpay_pg",
            provider_payment_id=request.razorpay_payment_id,
            amount_minor=order.gross_amount_minor,
            currency=payment_data.get("currency", "INR"),
            status=StatusPayment.captured,
            captured_at=datetime.now(IST),
            payment_metadata={
                "method": payment_data.get("method"),
                "source": "unified_verify",
                "razorpay_order_id": request.razorpay_order_id,
            },
        )
        db.add(pay)
        order.status = StatusOrder.paid
        db.add(order)
        logger.debug(f"Payment {pay.id} recorded for order {order.id}, amount: {order.gross_amount_minor} paisa")

        # Deduct reward amount from fittbot_cash if reward is enabled
        print("request.reward",request.reward)
        print("request.reward_applied",request.reward_applied)
        if request.reward and request.reward_applied:
            client_id = int(order.customer_id)
            fittbot_cash_entry = db.query(ReferralFittbotCash).filter(
                ReferralFittbotCash.client_id == client_id
            ).first()

            print("fittbot_cash_entry is",fittbot_cash_entry)

            if fittbot_cash_entry:
               
                # Convert reward_amount from paisa to rupees for deduction
                reward_rupees = request.reward_applied / 100
                fittbot_cash_entry.fittbot_cash -= reward_rupees
                db.add(fittbot_cash_entry)
                db.flush()
                logger.info(f"Deducted {reward_rupees}₹ from fittbot_cash for client {client_id}. New balance: {fittbot_cash_entry.fittbot_cash}₹")

        # 6) Fulfil order items
        # Note: dailypass now uses same DB connection, no need for separate session
        dp_details: Optional[Dict[str, Any]] = None
        sub_details: Optional[Dict[str, Any]] = None
        dp_ok = False
        sub_ok = False

        items = db.query(OrderItem).filter(OrderItem.order_id == order.id).all()
        logger.debug(f"Processing {len(items)} order items for order {order.id}")

        for it in items:
            if it.item_type == ItemType.daily_pass:
                logger.debug(f"Activating daily pass for order item {it.id}")
                dp_details = _process_daily_pass_activation(
                    db=db,
                    payments_db=db,  # Both use same session now
                    order_item=it,
                    customer_id=order.customer_id,
                    payment_id=request.razorpay_payment_id,
                )
                dp_ok = True
                logger.debug(f"Daily pass activated: {dp_details}")
            elif it.item_type == ItemType.app_subscription:
                logger.debug(f"Activating subscription for order item {it.id}")
                sub_details = _process_local_subscription_activation(
                    db=db,
                    order_item=it,
                    customer_id=order.customer_id,
                    payment_id=request.razorpay_payment_id,
                )
                sub_ok = True
                logger.debug(f"Subscription activated: {sub_details}")

        db.commit()
        logger.debug(f"Transaction committed successfully for order {order.id}")

        return UnifiedVerificationResponse(
            success=True,
            payment_captured=True,
            order_id=order.id,
            payment_id=request.razorpay_payment_id,
            daily_pass_activated=dp_ok,
            daily_pass_details=dp_details,
            subscription_activated=sub_ok,
            subscription_details=sub_details,
            total_amount=order.gross_amount_minor,
            currency="INR",
            message="Payment verified and services activated",
        )
    except Exception as e:
        db.rollback()
        logger.error(f"Payment verification failed for order {order.id if order else 'unknown'}: {str(e)}", exc_info=True)
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, f"Failed to process payment: {str(e)}")

# -----------------------------------------------------------------------------
# Health
# -----------------------------------------------------------------------------

@router.get("/dailypass/health")
async def health_check():
    return {
        "status": "healthy",
        "service": "unified_dailypass_local_sub",
        "version": "2.0.0",
        "timestamp": datetime.now(UTC).isoformat(),
    }