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
from app.models.fittbot_plans_model import get_plan_by_id
from app.models.fittbot_models import ReferralFittbotCash

from .rp_client import create_order as rzp_create_order, get_payment as rzp_get_payment

logger = logging.getLogger("payments.unified_dailypass")
router = APIRouter(prefix="/pay_copy", tags=["Unified Daily Pass Payments"])
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
    metadata = order_item.item_metadata or {}
    gym_id = int(order_item.gym_id)
    dates = [datetime.fromisoformat(d).date() for d in metadata.get("dates", [])]
    selected_time = metadata.get("selected_time")
    if not dates:
        raise HTTPException(500, "Daily pass metadata missing dates")

    # Get actual_price from item_metadata and convert to rupees (divide by 100)
    actual_price = metadata.get("actual_price", order_item.unit_price_minor)
    dailypass_price_rupees = actual_price // 100

    daily_pass = DailyPass(
        client_id=customer_id,
        gym_id=gym_id,
        start_date=dates[0],
        end_date=dates[-1],
        days_total=len(dates),
        amount_paid=order_item.unit_price_minor * order_item.qty,
        payment_id=payment_id,
        status="active",
        selected_time=selected_time,
        purchase_timestamp=datetime.now(IST),
    )
    db.add(daily_pass)
    db.flush()

    day_records = []
    for d in dates:
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
async def calculate_dailypass_reward(request: DailyPassRewardRequest, db: Session = Depends(get_db_session)):

    try:
        client_id = request.client_id

        amount=(request.amount)*100
        ten_percent_minor = int(amount * 0.10)
        capped_reward_minor = ten_percent_minor

        fittbot_cash_entry = db.query(ReferralFittbotCash).filter(
            ReferralFittbotCash.client_id == client_id
        ).first()

        available_fittbot_cash_rupees = fittbot_cash_entry.fittbot_cash if fittbot_cash_entry else 0

        available_fittbot_cash_minor = available_fittbot_cash_rupees * 100

        logger.info(f"Available fittbot_cash for client {client_id}: {available_fittbot_cash_rupees}₹ ({available_fittbot_cash_minor} paisa)")

        if available_fittbot_cash_minor >= capped_reward_minor:
            reward_amount = capped_reward_minor
        else:
            reward_amount = available_fittbot_cash_minor

        reward_amount=round(reward_amount*0.01)

        logger.info(f"Reward amount to send: {reward_amount}₹")

        return {
            "status": 200,
            "reward_amount": reward_amount
            }
        

    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
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
        if payload.daysTotal >= 4:
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
    if payload.includeSubscription:
        if not payload.selectedPlan:
            raise HTTPException(400, "selectedPlan is required when includeSubscription=true")
        plan = get_plan_by_id(db, payload.selectedPlan)
        logger.debug(f"Fetched plan: {plan.id if plan else 'None'}")
        if not plan:
            raise HTTPException(404, "Plan not found")
        # Flexible accessors depending on your model
        sub_total = int(getattr(plan, "price", None))
        logger.debug(f"Subscription total from plan: {sub_total} paisa")

        if sub_total and sub_total < 100:  # if amount is in rupees accidentally
            sub_total *= 100
            logger.debug(f"Converted subscription amount to paisa: {sub_total}")
        plan_duration_months = int(getattr(plan, "duration", 1))
        logger.debug(f"Plan duration: {plan_duration_months} months")
        if sub_total <= 0 or plan_duration_months <= 0:
            raise HTTPException(409, "Invalid plan configuration")

    logger.debug(f"Order totals - Daily pass: {dp_total}, Subscription: {sub_total}")
    grand_total = dp_total + sub_total
    logger.debug(f"Grand total before rounding: {grand_total} paisa")

    # 4) Create internal order + items
    order = Order(
        id=_new_id("ord_"),
        customer_id=cid,
        provider="razorpay_pg",
        currency="INR",
        gross_amount_minor=grand_total,
        status=StatusOrder.pending,
    )
    db.add(order)
    db.flush()

    db.add(
        OrderItem(
            id=_new_id("itm_"),
            order_id=order.id,
            item_type=ItemType.daily_pass,
            gym_id=str(payload.gymId),
            unit_price_minor=per_day_minor,
            qty=payload.daysTotal,
            item_metadata={
                "dates": [d.isoformat() for d in dp_dates],
                "selected_time": payload.selectedTime,
                "start_date": payload.startDate,
                "end_date": dp_dates[-1].isoformat(),
                "gym_id": payload.gymId,
                "discount_minor": dp_discount,
                "actual_price":actual_price
            },
        )
    )

    # Local subscription item (no provider subscription will be created)
    if payload.includeSubscription:
        db.add(
            OrderItem(
                id=_new_id("itm_"),
                order_id=order.id,
                item_type=ItemType.app_subscription,
                unit_price_minor=sub_total,
                qty=1,
                item_metadata={
                    "plan_id": payload.selectedPlan,
                    "duration_months": plan_duration_months,
                    "provider": "internal_manual",
                },
            )
        )

    db.flush()
    
    print("#########dailypass total is",dp_total)
    reward_amount = 0
    if payload.reward:

        print("dp total is",dp_total)
        amount=dp_total
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

        # reward_amount=round(reward_amount*0.01)

        print("grand total is",grand_total)
        print("rewatd amount",reward_amount)

        grand_total=grand_total-reward_amount

    # 5) Create Razorpay order
    logger.debug(f"Grand total before rounding: {grand_total} paisa")
    grand_total = round(grand_total / 100)
    grand_total = grand_total * 100
    logger.debug(f"Grand total after rounding: {grand_total} paisa ({grand_total / 100} rupees)")
    
    rzp_order = rzp_create_order(
        amount_minor=grand_total,
        currency="INR",
        receipt=order.id,
        notes={
            "order_id": order.id,
            "customer_id": cid,
            "gym_id": payload.gymId,
            "flow": "unified_dailypass_local_sub" if payload.includeSubscription else "dailypass_only",
            "dp_total": dp_total,
            "sub_total": sub_total,
            "includes_subscription": payload.includeSubscription,
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
    print("response is",response)
    
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
    # paid_amount = int(payment_data.get("amount", 0))
    # if paid_amount != order.gross_amount_minor:
    #     paid_amount=paid_amount/100
    #     paid_amount=round(paid_amount)
    #     paid_amount=paid_amount*100
    #     raise HTTPException(status.HTTP_409_CONFLICT, "Payment amount mismatch")

    # 4) Idempotency: skip if this payment already processed
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