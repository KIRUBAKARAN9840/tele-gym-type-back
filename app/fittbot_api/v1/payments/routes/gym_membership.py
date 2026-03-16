
from __future__ import annotations
import asyncio
import base64
import hashlib
import hmac
import json
import logging
import secrets
import time
from datetime import datetime, timedelta, timezone, date
from typing import Any, Dict, Optional, Tuple, List
from dateutil.relativedelta import relativedelta
from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi import status as http_status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from sqlalchemy import text
from sqlalchemy.orm import Session

# Your system imports
from ..config.database import get_db_session
from ..config.settings import get_payment_settings
from ..models.orders import Order, OrderItem
from ..models.payments import Payment
from ..models.entitlements import Entitlement
from ..models.subscriptions import Subscription, now_ist
from ..models.enums import ItemType, EntType, StatusOrder, StatusEnt, StatusPayoutLine, SubscriptionStatus
from ..models.payouts import PayoutLine
from app.models.fittbot_plans_model import get_plan_by_id
from app.fittbot_api.v1.payments.razorpay_async_gateway import (
    create_order as rzp_create_order_async,
    get_payment as rzp_get_payment_async,
)

# Import models
from app.models.fittbot_models import GymBusinessPayment, FittbotGymMembership, ReferralFittbotCash

logger = logging.getLogger("payments.gym_membership")
security = HTTPBearer(auto_error=False)

UTC = timezone.utc
RZP_API = "https://api.razorpay.com/v1"

router = APIRouter(prefix="/gym_membership_rg", tags=["Gym Membership Orders"])


def _new_id(prefix: str) -> str:
    """Generate unique ID with prefix"""
    return f"{prefix}{int(time.time()*1000)}_{secrets.token_hex(3)}"

def _mask(s: Optional[str]) -> str:
    """Mask sensitive strings for logging"""
    if not s:
        return ""
    return f"{s[:4]}...{s[-4:]}" if len(s) > 8 else "***"

def _require_user_id(creds: Optional[HTTPAuthorizationCredentials]) -> str:
    """
    Extract user ID from JWT token using your existing JWT verification
    """
    if not creds or creds.scheme.lower() != "bearer" or not creds.credentials:
        raise HTTPException(http_status.HTTP_401_UNAUTHORIZED, "Missing/invalid auth")

    from app.utils.security import SECRET_KEY, ALGORITHM
    from jose import jwt, JWTError
    from jose.exceptions import ExpiredSignatureError

    token = creds.credentials
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        user_id = payload.get("sub")
        role = payload.get("role")

        if not user_id:
            raise HTTPException(http_status.HTTP_401_UNAUTHORIZED, "Token missing subject")

        if role not in ["client", "owner"]:
            raise HTTPException(http_status.HTTP_403_FORBIDDEN, "Invalid role for gym membership")

        return user_id

    except ExpiredSignatureError:
        raise HTTPException(http_status.HTTP_401_UNAUTHORIZED, "Session expired, Please Login again")
    except JWTError:
        raise HTTPException(http_status.HTTP_401_UNAUTHORIZED, "Invalid token")

# -----------------------------------------------------------------------------
# Razorpay (PG) helpers
# -----------------------------------------------------------------------------

def _rzp_auth_headers(settings) -> Dict[str, str]:
    """Generate Razorpay API auth headers"""
    auth_string = f"{settings.razorpay_key_id}:{settings.razorpay_key_secret}"
    encoded = base64.b64encode(auth_string.encode("utf-8")).decode("utf-8")
    return {"Authorization": f"Basic {encoded}", "Content-Type": "application/json"}

def _run_async(coro):
    try:
        loop = asyncio.get_running_loop()
        if loop.is_running():
            return asyncio.run_coroutine_threadsafe(coro, loop).result()
    except RuntimeError:
        return asyncio.run(coro)
    return loop.run_until_complete(coro)


def _rzp_create_order(
    amount_minor: int,
    currency: str,
    receipt: str,
    notes: Dict[str, Any],
    settings,
    offers: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """Create Razorpay order using shared async gateway."""
    try:
        return _run_async(
            rzp_create_order_async(
                amount_minor=amount_minor,
                currency=currency,
                receipt=receipt,
                notes=notes,
                offers=offers,
            )
        )
    except Exception as e:
        logger.error("RZP order create failed", extra={"receipt": receipt, "error": repr(e)})
        raise HTTPException(http_status.HTTP_502_BAD_GATEWAY, "Failed to create order with provider")

def _rzp_get_payment(payment_id: str, settings) -> Dict[str, Any]:
    """Get Razorpay payment details"""
    try:
        return _run_async(rzp_get_payment_async(payment_id))
    except Exception as e:
        logger.error("RZP get payment failed", extra={"pid": _mask(payment_id), "error": repr(e)})
        raise HTTPException(http_status.HTTP_502_BAD_GATEWAY, "Failed to verify payment with provider")

def _verify_checkout_sig(key_secret: str, rzp_order_id: str, rzp_payment_id: str, rzp_signature: str) -> bool:
    """Verify Razorpay checkout signature"""
    data = f"{rzp_order_id}|{rzp_payment_id}".encode("utf-8")
    expected = hmac.new(key_secret.encode("utf-8"), data, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, rzp_signature or "")

def _find_active_subscription(db: Session, customer_id: str) -> Optional[Dict[str, Any]]:
    """Find active subscription for customer"""
    try:
        active_sub = (
            db.query(Subscription)
            .filter(
                Subscription.customer_id == customer_id,
                Subscription.status == SubscriptionStatus.active,
                Subscription.active_until > datetime.now(UTC)
            )
            .first()
        )
        if active_sub:
            return {
                "id": active_sub.id,
                "provider": active_sub.provider,
                "product_id": active_sub.product_id,
                "active_until": active_sub.active_until,
                "rc_original_txn_id": active_sub.rc_original_txn_id
            }
    except Exception as e:
        logger.error(f"Error finding active subscription: {e}")
    return None

def _pause_razorpay_subscription(subscription_id: str, settings) -> bool:
    """Pause Razorpay subscription"""
    try:
        resp = requests.post(
            f"{RZP_API}/subscriptions/{subscription_id}/pause",
            headers=_rzp_auth_headers(settings),
            data=json.dumps({
                "pause_at": "now"
            }),
            timeout=(5, 15)
        )
        resp.raise_for_status()
        logger.info(f"Successfully paused Razorpay subscription: {subscription_id}")
        return True
    except requests.RequestException as e:
        logger.error(f"Failed to pause Razorpay subscription {subscription_id}: {e}")
        return False

def _extend_subscription_validity(db: Session, subscription_id: str, plan_id: int) -> bool:
    """Extend subscription validity by plan duration months"""
    try:
        subscription = db.query(Subscription).filter(Subscription.id == subscription_id).first()
        if subscription:
            # Get the plan duration (already in months)
            plan = get_plan_by_id(db, plan_id)
            if not plan:
                logger.error(f"Plan {plan_id} not found for subscription extension")
                return False

            # Plan duration is already in months, use directly
            months_to_add = int(plan.duration)

            # Extend from the current active_until date (not from now)
            current_end = subscription.active_until or datetime.now(UTC)
            new_end = current_end + relativedelta(months=months_to_add)

            subscription.active_until = new_end
            db.add(subscription)
            logger.info(f"Extended subscription {subscription_id} by {months_to_add} months from {current_end} to {new_end}")
            return True
    except Exception as e:
        logger.error(f"Error extending subscription validity: {e}")
    return False

# -----------------------------------------------------------------------------
# Domain helpers (price, commission, legacy mirror)
# -----------------------------------------------------------------------------

def _load_gym_plan(db: Session, gym_id: int, plan_id: int) -> Tuple[int, int]:

    row = db.execute(
        text("SELECT amount, duration FROM gym_plans WHERE id=:pid AND gym_id=:gid"),
        {"pid": plan_id, "gid": gym_id},
    ).one_or_none()
    if not row:
        raise HTTPException(http_status.HTTP_404_NOT_FOUND, "Plan not found for gym")
    amount = int(row.amount)
    duration = int(row.duration or 1)
    if amount <= 0 or duration <= 0:
        raise HTTPException(http_status.HTTP_409_CONFLICT, "Invalid plan config")
    return amount * 100, duration

def _get_plan_details(db: Session, gym_id: int, plan_id: int) -> Tuple[int, int, bool]:
    """Get plan details including whether it's a personal training service"""
    row = db.execute(
        text("SELECT amount, duration, personal_training FROM gym_plans WHERE id=:pid AND gym_id=:gid"),
        {"pid": plan_id, "gid": gym_id},
    ).one_or_none()
    if not row:
        raise HTTPException(http_status.HTTP_404_NOT_FOUND, "Plan not found for gym")

    amount = int(row.amount)
    duration = int(row.duration or 1)
    is_personal_training = bool(row.personal_training)

    if amount <= 0 or duration <= 0:
        raise HTTPException(http_status.HTTP_409_CONFLICT, "Invalid plan config")

    return amount * 100, duration, is_personal_training

def _load_personal_training_plan(db: Session, gym_id: int, plan_id: int) -> Tuple[int, int]:
    """Load personal training pricing and duration using same gym_plans table"""
    # Use the same gym_plans table structure for personal training
    row = db.execute(
        text("SELECT amount, duration FROM gym_plans WHERE id=:pid AND gym_id=:gid"),
        {"pid": plan_id, "gid": gym_id},
    ).one_or_none()

    if not row:
        raise HTTPException(http_status.HTTP_404_NOT_FOUND, "Personal training plan not found for gym")

    amount = int(row.amount)
    duration = int(row.duration or 1)  # For PT, this could be number of sessions

    if amount <= 0 or duration <= 0:
        raise HTTPException(http_status.HTTP_409_CONFLICT, "Invalid personal training plan config")

    return amount * 100, duration

# Commission logic removed as per request

def _upsert_gym_fees(db: Session, client_id: str, start_date: date, end_date: date):
    """
    Mirrors to gym_fees table. Assumes a UNIQUE KEY exists on (client_id, start_date)
    so ON DUPLICATE KEY works. If you don't have it, add:
      ALTER TABLE gym_fees ADD UNIQUE KEY uq_client_start (client_id, start_date);
    """
    db.execute(
        text("""
        INSERT INTO gym_fees (client_id, start_date, end_date)
        VALUES (:cid, :sd, :ed)
        ON DUPLICATE KEY UPDATE end_date = GREATEST(end_date, VALUES(end_date))
        """),
        {"cid": client_id, "sd": start_date, "ed": end_date},
    )

def _process_app_subscription_activation(
    db: Session,
    order_item: OrderItem,
    customer_id: str,
    payment_id: str,
) -> Dict[str, Any]:
    """Process app subscription activation for unified flow"""
    from ..config.settings import get_payment_settings

    meta = order_item.item_metadata or {}
    plan_id = meta.get("plan_id")
    duration_months = int(meta.get("duration_days", 1))  # This is actually months from fittbot plan
    product_id = f"fittbot_plan_{plan_id}" if plan_id else "fittbot_plan"
    is_existing = meta.get("is_existing", False)
    existing_subscription_id = meta.get("existing_subscription_id")
    existing_subscription_provider = meta.get("existing_subscription_provider")
    existing_active_until = meta.get("existing_subscription_active_until")

    nowu = datetime.now(UTC)
    paused_existing = False
    extended_existing = False

    # Handle existing subscription logic
    if is_existing and existing_subscription_id:
        logger.info(f"Processing existing subscription logic for {existing_subscription_id}")

        # Try to pause existing subscription if it's from Razorpay
        if existing_subscription_provider == "razorpay":
            settings = get_payment_settings()
            paused_existing = _pause_razorpay_subscription(existing_subscription_id, settings)

        # Extend existing subscription validity
        if paused_existing or existing_subscription_provider != "razorpay":
            extended_existing = _extend_subscription_validity(db, existing_subscription_id, plan_id)
            if extended_existing:
                # Return info about the extended subscription instead of creating new one
                existing_sub = db.query(Subscription).filter(Subscription.id == existing_subscription_id).first()
                if existing_sub:
                    return {
                        "subscription_id": existing_sub.id,
                        "plan_id": plan_id,
                        "active_from": existing_sub.active_from.isoformat(),
                        "active_until": existing_sub.active_until.isoformat(),
                        "status": "extended",
                        "provider": existing_sub.provider,
                        "was_paused": paused_existing,
                        "was_extended": True,
                        "extension_months": duration_months
                    }

    # Create new subscription (default behavior or if extension failed)
    # duration_months is from fittbot plan, use directly
    months_to_add = duration_months

    sub = Subscription(
        id=_new_id("sub_"),
        customer_id=customer_id,
        provider="internal_manual",
        product_id=str(product_id),
        status=SubscriptionStatus.active,
        rc_original_txn_id=None,
        latest_txn_id=payment_id,
        active_from=nowu,
        active_until=(nowu + relativedelta(months=months_to_add)),
        auto_renew=False,
    )
    db.add(sub)
    db.flush()  # Ensure subscription ID is available

    # Create entitlements for subscription
    order = db.query(Order).filter(Order.id == order_item.order_id).first()
    if order:
        ents = (
            db.query(Entitlement)
            .join(OrderItem, Entitlement.order_item_id == OrderItem.id)
            .filter(OrderItem.order_id == order.id)
            .all()
        )
        if not ents:
            from ..services.entitlement_service import EntitlementService
            EntitlementService(db).create_entitlements_from_order(order)
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

    return {
        "subscription_id": sub.id,
        "plan_id": plan_id,
        "active_from": sub.active_from.isoformat(),
        "active_until": sub.active_until.isoformat(),
        "status": "active",
        "provider": "internal_manual",
        "existing_subscription_handled": is_existing and existing_subscription_id is not None,
        "existing_subscription_paused": paused_existing,
    }

def _assert_amount_currency(order: Order, payment: Dict[str, Any]):
    """Verify payment amount and currency match order"""
    paid = int(payment.get("amount") or 0)
    cur = payment.get("currency") or ""
    if paid != order.gross_amount_minor or cur != "INR":
        logger.warning("Amount/currency mismatch",
                       extra={"order_id": order.id, "paid": paid, "expected": order.gross_amount_minor, "cur": cur})
        raise HTTPException(http_status.HTTP_409_CONFLICT, "Amount/currency mismatch")

# -----------------------------------------------------------------------------
# Public endpoints
# -----------------------------------------------------------------------------

from pydantic import BaseModel
class UnifiedMembershipRequest(BaseModel):
    gym_id: int
    plan_id: int  # gym_plans.id - same for both gym membership and personal training
    client_id: str
    includeSubscription: bool = False  # Legacy - no longer used, all memberships get free Fittbot
    selectedFittbotPlan: Optional[int] = None  # Legacy - no longer required
    fittbotDuration: Optional[int] = None  # Duration in months for free Fittbot subscription (defaults to gym plan duration)
    reward: Optional[bool] = None
    is_existing: bool = False  # If True and is_paid=False, pause existing subscription and extend validity

class Credentials(BaseModel):
    gym_id: int
    plan_id: int
    client_id: int

@router.post("/checkout/unified-create-order")
async def unified_create_order(
    body: UnifiedMembershipRequest,
    db: Session = Depends(get_db_session),
):
    user_id = body.client_id
    settings = get_payment_settings()
    gym_id = body.gym_id
    plan_id = body.plan_id

    if not isinstance(gym_id, int) or not isinstance(plan_id, int):
        raise HTTPException(http_status.HTTP_400_BAD_REQUEST, "gym_id and plan_id are required integers")

    # 1) Get plan details and determine service type from database
    service_amount_minor, duration_value, is_personal_training = _get_plan_details(db, gym_id, plan_id)

    if is_personal_training:
        service_label = "Personal Training"
        service_type = "personal_training"
    else:
        service_label = "Gym Membership"
        service_type = "gym_membership"

    # 2) Get subscription pricing if included
    sub_total = 0
    sub_duration_days = None
    existing_subscription = None

    if body.includeSubscription:
        if not body.selectedFittbotPlan:
            raise HTTPException(http_status.HTTP_400_BAD_REQUEST, "selectedFittbotPlan is required when includeSubscription=true")

        plan = get_plan_by_id(db, body.selectedFittbotPlan)
        if not plan:
            raise HTTPException(http_status.HTTP_404_NOT_FOUND, "Fittbot plan not found")

        sub_duration_days = int(plan.duration)

        sub_total = 0  
        if body.is_existing:

        #if True:
            existing_subscription = _find_active_subscription(db, user_id)
            if existing_subscription:
                logger.info(f"Found existing subscription for user {user_id}: {existing_subscription['id']}")

        if sub_duration_days <= 0:
            raise HTTPException(http_status.HTTP_409_CONFLICT, "Invalid subscription plan configuration")

    grand_total = service_amount_minor + sub_total
    print("grand total is",grand_total)

    # Calculate reward if enabled
    reward_amount = 0
    print("body.reward is",body.reward)
    if body.reward:

        # Calculate 10% of service_amount_minor (which is already in minor units - paisa)
        ten_percent_minor = int(service_amount_minor * 0.10)

        # Cap at 100 rupees = 10000 paisa
        capped_reward_minor = min(ten_percent_minor, 10000)

        logger.info(f"Reward calculation: service_amount={service_amount_minor/100}₹, 10%={ten_percent_minor/100}₹, capped={capped_reward_minor/100}₹")

        # Get available fittbot_cash for the client (stored in rupees in database)
        fittbot_cash_entry = db.query(ReferralFittbotCash).filter(
            ReferralFittbotCash.client_id == int(user_id)
        ).first()

        available_fittbot_cash_rupees = fittbot_cash_entry.fittbot_cash if fittbot_cash_entry else 0
        # Convert to paisa (minor units) for comparison
        available_fittbot_cash_minor = available_fittbot_cash_rupees * 100

        logger.info(f"Available fittbot_cash for client {user_id}: {available_fittbot_cash_rupees}₹ ({available_fittbot_cash_minor} paisa)")

        # Determine the reward amount to apply (in paisa)
        # If available cash >= capped reward, apply capped reward
        # If available cash < capped reward, apply available cash amount
        if available_fittbot_cash_minor >= capped_reward_minor:
            reward_amount = capped_reward_minor
        else:
            reward_amount = available_fittbot_cash_minor

        # Reduce grand_total by reward_amount
        grand_total = grand_total - reward_amount

        logger.info(f"Reward applied: {reward_amount/100}₹ ({reward_amount} paisa), New grand_total: {grand_total/100}₹")

    # 3) Create comprehensive order metadata - Enterprise-level tracking
    gross_before_rewards = service_amount_minor + sub_total

    # Build reward calculation details
    reward_calculation_details = {}
    if body.reward and reward_amount > 0:
        reward_calculation_details = {
            "reward_applied": True,
            "reward_amount_minor": reward_amount,
            "reward_amount_rupees": reward_amount / 100,
            "ten_percent_cap_minor": min(int(service_amount_minor * 0.10), 10000),
            "available_fittbot_cash_minor": available_fittbot_cash_minor if body.reward else 0,
            "available_fittbot_cash_rupees": available_fittbot_cash_rupees if body.reward else 0,
            "calculation_base": "service_amount",
            "max_reward_cap": 100  # 100 rupees max
        }

    order_metadata = {
        # ============ ORDER IDENTIFICATION ============
        "order_info": {
            "order_type": f"unified_{service_type}_with_sub" if body.includeSubscription else f"{service_type}_only",
            "customer_id": user_id,
            "created_at": datetime.now(UTC).isoformat(),
            "currency": "INR",
            "flow": f"unified_{service_type}_with_sub" if body.includeSubscription else f"{service_type}_only",
        },

        # ============ WHAT'S IN THIS ORDER? ============
        "order_composition": {
            "includes_gym_service": True,
            "service_type": service_type,  # "gym_membership" or "personal_training"
            "service_label": service_label,  # "Gym Membership" or "Personal Training"
            "includes_subscription": body.includeSubscription,
            "items_count": 2 if body.includeSubscription else 1,
        },

        # ============ COMPLETE PAYMENT BREAKDOWN ============
        "payment_summary": {
            # Step 1: Base amounts
            "step_1_base_amounts": {
                "service_base_minor": service_amount_minor,
                "service_base_rupees": service_amount_minor / 100,
                "subscription_base_minor": sub_total,
                "subscription_base_rupees": sub_total / 100,
                "total_base_minor": gross_before_rewards,
                "total_base_rupees": gross_before_rewards / 100,
            },

            # Step 2: Apply fittbot cash reward (if applicable)
            "step_2_reward_deduction": {
                "reward_requested": body.reward,
                "reward_applied": reward_amount > 0,
                "reward_amount_minor": reward_amount,
                "reward_amount_rupees": reward_amount / 100,
                "reward_source": "fittbot_cash",
                "available_fittbot_cash_minor": reward_calculation_details.get("available_fittbot_cash_minor", 0),
                "available_fittbot_cash_rupees": reward_calculation_details.get("available_fittbot_cash_rupees", 0),
                "ten_percent_cap_minor": reward_calculation_details.get("ten_percent_cap_minor", 0),
                "max_reward_cap_rupees": 100,
                "reward_calculation": f"min(10% of {service_amount_minor/100}rs, 100rs cap, available cash {reward_calculation_details.get('available_fittbot_cash_rupees', 0)}rs) = {reward_amount/100}rs" if body.reward else "No reward applied",
            },

            # Step 3: Final amount
            "step_3_final_amount": {
                "final_amount_minor": grand_total,
                "final_amount_rupees": grand_total / 100,
                "amount_saved_minor": gross_before_rewards - grand_total,
                "amount_saved_rupees": (gross_before_rewards - grand_total) / 100,
                "savings_percentage": round(((gross_before_rewards - grand_total) / gross_before_rewards) * 100, 2) if gross_before_rewards > 0 else 0,
            },

            # Human-readable summary
            "calculation_formula": f"({gross_before_rewards/100}rs base - {reward_amount/100}rs reward) = {grand_total/100}rs paid",
            "one_line_summary": f"Paid {grand_total/100}rs (saved {(gross_before_rewards - grand_total)/100}rs from {gross_before_rewards/100}rs)",
        },

        # ============ GYM SERVICE DETAILS ============
        "gym_service": {
            "included": True,
            "service_type": service_type,
            "service_label": service_label,
            "gym_id": gym_id,
            "plan_id": plan_id,
            "duration_value": duration_value,
            "duration_type": "months" if not is_personal_training else "sessions",
            "is_personal_training": is_personal_training,
            "pricing": {
                "service_amount_minor": service_amount_minor,
                "service_amount_rupees": service_amount_minor / 100,
                "final_service_cost_minor": service_amount_minor - reward_amount,  # Service portion after reward
                "final_service_cost_rupees": (service_amount_minor - reward_amount) / 100,
            },
            "summary": f"{duration_value} {('months' if not is_personal_training else 'sessions')} of {service_label} for {service_amount_minor/100}rs, after reward = {(service_amount_minor - reward_amount)/100}rs",
        },

        # ============ SUBSCRIPTION DETAILS ============
        "subscription": {
            "included": body.includeSubscription,
            "plan_id": body.selectedFittbotPlan if body.includeSubscription else None,
            "duration_months": sub_duration_days if body.includeSubscription else None,
            "is_existing_extension": body.is_existing if body.includeSubscription else False,
            "existing_subscription_id": existing_subscription["id"] if (body.includeSubscription and existing_subscription) else None,
            "pricing": {
                "amount_minor": sub_total if body.includeSubscription else 0,
                "amount_rupees": sub_total / 100 if body.includeSubscription else 0,
            },
            "summary": f"{sub_duration_days} months subscription {'(extension)' if body.is_existing else '(new)'} for {sub_total/100}rs" if body.includeSubscription else "No subscription included",
        } if body.includeSubscription else {
            "included": False,
            "summary": "No subscription included",
        },

        # ============ REWARD DETAILS ============
        "reward": {
            "reward_used": body.reward and reward_amount > 0,
            "reward_details": reward_calculation_details if (body.reward and reward_amount > 0) else None,
            "summary": f"Fittbot cash {reward_amount/100}rs applied" if (body.reward and reward_amount > 0) else "No reward used",
        },

        # ============ AMOUNTS SUMMARY (Quick Reference) ============
        "amounts": {
            "base_amount": gross_before_rewards / 100,
            "total_discounts": reward_amount / 100,
            "final_paid": grand_total / 100,
            "currency": "INR",
        },

        # ============ AUDIT INFO ============
        "audit": {
            "created_by": "system",
            "source": "gym_membership_checkout_api",
            "api_version": "v1",
            "client_payload": {
                "reward_requested": body.reward,
                "subscription_requested": body.includeSubscription,
                "is_existing_subscription": body.is_existing if body.includeSubscription else False,
            }
        }
    }

    # 4) Create internal order + items with metadata
    order = Order(
        id=_new_id("ord_"),
        customer_id=user_id,
        provider="razorpay_pg",
        currency="INR",
        gross_amount_minor=grand_total,
        status=StatusOrder.pending,
        order_metadata=order_metadata  # Complete transaction summary
    )
    db.add(order)
    db.flush()

    # Main service item (gym membership or personal training)
    if is_personal_training:
        item_type = ItemType.pt_session
        metadata = {
            "plan_id": plan_id,
            "sessions": duration_value,
            "service_type": "personal_training",
            "amount": service_amount_minor / 100
        }
    else:  # GYM_MEMBERSHIP
        item_type = ItemType.gym_membership
        metadata = {
            "plan_id": plan_id,
            "duration_months": duration_value,
            "service_type": "gym_membership",
            "amount": service_amount_minor / 100  # store major units for reporting tables
        }

    logger.debug(
        "Creating service order item",
        extra={
            "order_id": order.id,
            "item_type": item_type.name if hasattr(item_type, "name") else str(item_type),
            "unit_price_minor": service_amount_minor,
            "metadata": metadata,
        },
    )

    service_item = OrderItem(
        id=_new_id("itm_"),
        order_id=order.id,
        item_type=item_type,
        gym_id=str(gym_id),
        unit_price_minor=service_amount_minor,
        qty=1,
        item_metadata=metadata
    )
    db.add(service_item)

    # App subscription item (if included)
    if body.includeSubscription:
        sub_item = OrderItem(
            id=_new_id("itm_"),
            order_id=order.id,
            item_type=ItemType.app_subscription,
            unit_price_minor=sub_total,
            qty=1,
            item_metadata={
                "plan_id": body.selectedFittbotPlan,
                "duration_days": sub_duration_days,
                "provider": "internal_manual",
                "is_existing": body.is_existing,
                "existing_subscription_id": existing_subscription["id"] if existing_subscription else None,
                "existing_subscription_provider": existing_subscription["provider"] if existing_subscription else None,
                "existing_subscription_active_until": existing_subscription["active_until"].isoformat() if existing_subscription else None
            }
        )
        db.add(sub_item)

    db.flush()

    # 4) Create Razorpay order
    rzp_order = _rzp_create_order(
        amount_minor=grand_total,
        currency="INR",
        receipt=order.id,
        notes={
            "order_id": order.id,
            "amount_minor": grand_total,
            "user_id": user_id,
            "gym_id": gym_id,
            "plan_id": plan_id,
            "flow": f"unified_{service_type}_with_sub" if body.includeSubscription else f"{service_type}_only",
            "service_total": service_amount_minor,
            "service_type": service_type,
            "sub_total": sub_total,
            "includes_subscription": body.includeSubscription
        },
        settings=settings,
    )
    order.provider_order_id = rzp_order["id"]
    db.add(order)
    db.commit()

    logger.info("Unified gym order created", extra={
        "order_id": order.id,
        "rzp_order_id": _mask(rzp_order['id']),
        "is_existing": body.is_existing,
        "has_existing_subscription": existing_subscription is not None
    })

    response_data = {
        "razorpay_order_id": rzp_order["id"],
        "razorpay_key_id": settings.razorpay_key_id,
        "order_id": order.id,
        "amount_minor": grand_total,
        "currency": "INR",
        "service_amount": service_amount_minor,
        "service_type": service_type,
        "subscription_amount": sub_total,
        "total_amount": grand_total,
        "reward_applied": reward_amount,
        "reward_enabled": body.reward,
        "includes_subscription": body.includeSubscription,
        "subscription_is_existing": body.is_existing,
        "display_title": f"{service_label}" + "Free App Subscription"
    }

    # Add existing subscription info if applicable
    if existing_subscription:
        response_data["existing_subscription"] = {
            "id": existing_subscription["id"],
            "provider": existing_subscription["provider"],
            "active_until": existing_subscription["active_until"].isoformat(),
            "will_be_paused": True
        }

    print("##########response data@@@@@@@@",response_data)

    return response_data


@router.post("/checkout/unified-verify")
async def unified_verify_checkout(
    body: Dict[str, Any],
    db: Session = Depends(get_db_session),
):

    settings = get_payment_settings()
    reward=body.get("reward")
    if reward:
        reward_amount=body.get("reward_applied")

    pid = body.get("razorpay_payment_id")
    oid = body.get("razorpay_order_id")
    sig = body.get("razorpay_signature")
    if not all([pid, oid, sig]):
        raise HTTPException(http_status.HTTP_400_BAD_REQUEST, "Missing required fields")

    if not _verify_checkout_sig(settings.razorpay_key_secret, oid, pid, sig):
        logger.warning("Invalid checkout signature", extra={"pid": _mask(pid), "oid": _mask(oid)})
        raise HTTPException(http_status.HTTP_403_FORBIDDEN, "Invalid signature")

    order = db.query(Order).filter(Order.provider_order_id == oid).first()
    if not order:
        raise HTTPException(http_status.HTTP_404_NOT_FOUND, "Order not found")

    try:
        payment = _rzp_get_payment(pid, settings)
    except HTTPException:
        return {"verified": True, "captured": False, "retryAfterMs": 4000, "message": "Verifying payment status"}

    logger.debug(
        "Unified verify checkout retrieved payment",
        extra={
            "order_id": order.id,
            "payment_id": _mask(pid),
            "status": payment.get("status"),
            "amount_minor": payment.get("amount"),
            "currency": payment.get("currency"),
            "captured": payment.get("captured"),
        },
    )

    status = payment.get("status")
    if status == "captured":
        # Deduct reward amount from fittbot_cash if reward is enabled
        if reward and reward_amount:
            print("reward amount is",reward_amount)
            client_id = int(order.customer_id)
            print("client id id",client_id)
            fittbot_cash_entry = db.query(ReferralFittbotCash).filter(
                ReferralFittbotCash.client_id == client_id
            ).first()

            if fittbot_cash_entry:
                # Convert reward_amount from paisa to rupees for deduction
                reward_rupees = reward_amount / 100
                fittbot_cash_entry.fittbot_cash -= reward_rupees
                db.add(fittbot_cash_entry)
                db.flush()
                logger.info(f"Deducted {reward_rupees}₹ from fittbot_cash for client {client_id}. New balance: {fittbot_cash_entry.fittbot_cash}₹")

        return _finalize_unified_captured_payment(db, order, payment)

    if status == "authorized":
        return {"verified": True, "captured": False, "retryAfterMs": 3000, "message": "Payment authorized, finalizing..."}

    if status in ("failed", "refunded"):
        return {"verified": False, "captured": False, "status": status, "message": f"Payment {status}"}

    return {"verified": True, "captured": False, "retryAfterMs": 3000, "message": "Payment verification in progress"}



def _finalize_unified_captured_payment(
    db: Session,
    order: Order,
    payment_data: Dict[str, Any],
) -> Dict[str, Any]:
    """Process both gym membership and app subscription from a single unified payment"""
    _assert_amount_currency(order, payment_data)

    rzp_amount_minor = payment_data.get("amount")
    if rzp_amount_minor is None:
        logger.warning(
            "Payment payload missing amount",
            extra={
                "order_id": order.id,
                "payment_id": _mask(payment_data.get("id")),
                "payload_keys": list(payment_data.keys()),
            },
        )
    else:
        logger.debug(
            "Finalizing captured payment with amount",
            extra={
                "order_id": order.id,
                "payment_id": _mask(payment_data.get("id")),
                "rzp_amount_minor": rzp_amount_minor,
                "order_amount_minor": order.gross_amount_minor,
            },
        )

    # Check for idempotency
    existing = (
        db.query(Payment)
        .filter(
            Payment.order_id == order.id,
            Payment.provider_payment_id == payment_data.get("id"),
            Payment.status == "captured"
        )
        .first()
    )
    if existing:
        # Return existing result
        return {
            "verified": True,
            "captured": True,
            "order_id": order.id,
            "payment_id": existing.provider_payment_id,
            "gym_membership_activated": True,
            "subscription_activated": True,
            "message": "Payment already processed",
            "purchased_at":""

        }

    # 1) Mark order as paid and record payment
    order.status = StatusOrder.paid
    db.add(order)

    

    pay = Payment(
        id=_new_id("pay_"),
        order_id=order.id,
        customer_id=order.customer_id,
        amount_minor=order.gross_amount_minor,
        currency=order.currency,
        provider=order.provider,
        provider_payment_id=payment_data.get("id"),
        status="captured",
        captured_at=datetime.now(UTC),
        payment_metadata={
            "method": payment_data.get("method"),
            "source": "unified_gym_checkout"
        },
    )
    logger.debug(
        "Creating captured payment record",
        extra={
            "payment_id": pay.id,
            "order_id": order.id,
            "provider_payment_id": _mask(pay.provider_payment_id or ""),
            "amount_minor": pay.amount_minor,
            "currency": pay.currency,
        },
    )
    db.add(pay)
    db.flush()

    # 2) Process each order item
    gym_details = None
    sub_details = None
    gym_activated = False
    sub_activated = False

    items = db.query(OrderItem).filter(OrderItem.order_id == order.id).all()
    for item in items:
        if item.item_type == ItemType.gym_membership:
            gym_details = _process_gym_membership_item(db, item, order, pay)
            gym_activated = True
        elif item.item_type == ItemType.pt_session:
            gym_details = _process_personal_training_item(db, item, order, pay)
            gym_activated = True
        elif item.item_type == ItemType.app_subscription:
            sub_details = _process_app_subscription_activation(
                db, item, order.customer_id, pay.provider_payment_id
            )
            sub_activated = True

    db.commit()


    return {
        "verified": True,
        "captured": True,
        "order_id": order.id,
        "payment_id": pay.provider_payment_id,
        "service_activated": gym_activated,
        "service_details": gym_details,
        "subscription_activated": sub_activated,
        "subscription_details": sub_details,
        "total_amount": order.gross_amount_minor,
        "currency": "INR",
        "message": "Payment verified and services activated",
        "purchased_at":datetime.now()
    }

def _process_gym_membership_item(
    db: Session,
    item: OrderItem,
    order: Order,
    payment: Payment
) -> Dict[str, Any]:
    """Process gym membership item activation"""
    meta = item.item_metadata or {}
    duration = int(meta.get("duration_months", 1))
    start_on = meta.get("start_on")
    amount_major = meta.get("amount")

    if amount_major is None:
        logger.warning(
            "Gym membership metadata missing amount",
            extra={
                "order_item_id": item.id,
                "order_id": order.id,
                "metadata_keys": list(meta.keys()),
                "unit_price_minor": item.unit_price_minor,
            },
        )
    else:
        logger.debug(
            "Resolved gym membership amount",
            extra={
                "order_item_id": item.id,
                "order_id": order.id,
                "amount_major": amount_major,
                "unit_price_minor": item.unit_price_minor,
            },
        )

    # Determine start date
    if start_on:
        start_dt = datetime.fromisoformat(start_on).replace(tzinfo=UTC)
    else:
        start_dt = datetime.now(UTC)

    end_dt = start_dt + relativedelta(months=duration)

    # Create entitlement
    ent = Entitlement(
        id=_new_id("ent_"),
        order_item_id=item.id,
        customer_id=order.customer_id,
        gym_id=item.gym_id,
        entitlement_type=EntType.membership,
        active_from=start_dt,
        active_until=end_dt,
        status=StatusEnt.active,
    )
    db.add(ent)
    db.flush()

    # Create payout line
    gross = item.unit_price_minor * item.qty
    pl = PayoutLine(
        id=_new_id("pl_"),
        entitlement_id=ent.id,
        gym_id=item.gym_id,
        gross_amount_minor=gross,
        commission_amount_minor=0,
        net_amount_minor=gross,
        applied_commission_pct=0.0,
        applied_commission_fixed_minor=0,
        scheduled_for=date.today() + timedelta(days=7),
        status=StatusPayoutLine.pending,
    )
    db.add(pl)

    # Settlement tracking
    try:
        from ..services.gym_membership_settlements import ensure_gym_exists, LedgerEarning
        gym_entity = ensure_gym_exists(db, str(item.gym_id), f"Gym {item.gym_id}")
        ledger_earning = LedgerEarning(
            gym_id=gym_entity.id,
            payment_id=payment.provider_payment_id,
            order_id=order.id,
            amount_gross_minor=payment.amount_minor,
            fees_minor=0,
            tax_minor=0,
            amount_net_minor=payment.amount_minor,
            state="pending_settlement",
        )
        db.add(ledger_earning)
    except (ImportError, Exception) as e:
        logger.warning(f"Settlement tracking failed: {e}")

    # Legacy table mirror
    _upsert_gym_fees(
        db,
        client_id=order.customer_id,
        start_date=start_dt.date(),
        end_date=end_dt.date()
    )

    # Insert into gym_business_payment table
    # try:
    #     gym_business_payment = GymBusinessPayment(
    #         client_id=order.customer_id,
    #         gym_id=item.gym_id,
    #         date=date.today(),
    #         amount=item.unit_price_minor / 100,  # Convert from minor units to major units
    #         status="not_initiated",
    #         mode="gym_membership",
    #         entitlement_id=ent.id,
    #         payment_id=payment.provider_payment_id,
    #         order_id=order.id
    #     )
    #     db.add(gym_business_payment)
    #     logger.info(f"Created gym_business_payment record for gym_membership: {ent.id}")
    # except Exception as e:
    #     logger.warning(f"Failed to create gym_business_payment record: {e}")

    # Insert into fittbot_gym_membership table
    try:
        fittbot_membership = FittbotGymMembership(
            gym_id=str(item.gym_id),
            client_id=order.customer_id,
            plan_id=item.item_metadata.get("plan_id") if item.item_metadata else None,
            amount=amount_major,
            type="gym_membership",
            entitlement_id=ent.id,
            purchased_at=datetime.now(),
            status="upcoming"
        )
        db.add(fittbot_membership)
        logger.info(
            "Created fittbot_gym_membership record for gym_membership",
            extra={
                "entitlement_id": ent.id,
                "gym_id": fittbot_membership.gym_id,
                "client_id": fittbot_membership.client_id,
                "plan_id": fittbot_membership.plan_id,
                "amount_major": fittbot_membership.amount,
            },
        )
    except Exception as e:
        logger.warning(f"Failed to create fittbot_gym_membership record: {e}")

    return {
        "entitlement_id": ent.id,
        "gym_id": item.gym_id,
        "active_from": start_dt.isoformat(),
        "active_until": end_dt.isoformat(),
        "status": "active"
    }

def _process_personal_training_item(
    db: Session,
    item: OrderItem,
    order: Order,
    payment: Payment
) -> Dict[str, Any]:
    """Process personal training item activation"""
    meta = item.item_metadata or {}
    sessions = int(meta.get("sessions", 1))
    start_on = meta.get("start_on")
    amount_major = meta.get("amount")

    if amount_major is None:
        logger.warning(
            "Personal training metadata missing amount",
            extra={
                "order_item_id": item.id,
                "order_id": order.id,
                "metadata_keys": list(meta.keys()),
                "unit_price_minor": item.unit_price_minor,
            },
        )
    else:
        logger.debug(
            "Resolved personal training amount",
            extra={
                "order_item_id": item.id,
                "order_id": order.id,
                "amount_major": amount_major,
                "unit_price_minor": item.unit_price_minor,
            },
        )

    # Determine start date
    if start_on:
        start_dt = datetime.fromisoformat(start_on).replace(tzinfo=UTC)
    else:
        start_dt = datetime.now(UTC)

    # For PT, we don't set end date - sessions are used as needed
    # But we can set an expiry date (e.g., sessions valid for 6 months)
    end_dt = start_dt + timedelta(days=180)  # 6 months validity

    # Create entitlement for PT sessions
    ent = Entitlement(
        id=_new_id("ent_"),
        order_item_id=item.id,
        customer_id=order.customer_id,
        gym_id=item.gym_id,
        entitlement_type=EntType.session,  # Use session type for PT
        active_from=start_dt,
        active_until=end_dt,
        status=StatusEnt.active,
    )
    db.add(ent)
    db.flush()

    # Create payout line
    gross = item.unit_price_minor * item.qty
    pl = PayoutLine(
        id=_new_id("pl_"),
        entitlement_id=ent.id,
        gym_id=item.gym_id,
        gross_amount_minor=gross,
        commission_amount_minor=0,
        net_amount_minor=gross,
        applied_commission_pct=0.0,
        applied_commission_fixed_minor=0,
        scheduled_for=date.today() + timedelta(days=7),
        status=StatusPayoutLine.pending,
    )
    db.add(pl)

    # Settlement tracking
    try:
        from ..services.gym_membership_settlements import ensure_gym_exists, LedgerEarning
        gym_entity = ensure_gym_exists(db, str(item.gym_id), f"Gym {item.gym_id}")
        ledger_earning = LedgerEarning(
            gym_id=gym_entity.id,
            payment_id=payment.provider_payment_id,
            order_id=order.id,
            amount_gross_minor=payment.amount_minor,
            fees_minor=0,
            tax_minor=0,
            amount_net_minor=payment.amount_minor,
            state="pending_settlement",
        )
        db.add(ledger_earning)
    except (ImportError, Exception) as e:
        logger.warning(f"Settlement tracking failed: {e}")

 
    try:
        fittbot_membership = FittbotGymMembership(
            gym_id=str(item.gym_id),
            client_id=order.customer_id,
            plan_id=item.item_metadata.get("plan_id") if item.item_metadata else None,
            type="personal_training",
            amount=amount_major,
            entitlement_id=ent.id,
            purchased_at=datetime.now(UTC),
            status="upcoming"
        )
        db.add(fittbot_membership)
        logger.info(
            "Created fittbot_gym_membership record for personal_training",
            extra={
                "entitlement_id": ent.id,
                "gym_id": fittbot_membership.gym_id,
                "client_id": fittbot_membership.client_id,
                "plan_id": fittbot_membership.plan_id,
                "amount_major": fittbot_membership.amount,
            },
        )
    except Exception as e:
        logger.warning(f"Failed to create fittbot_gym_membership record: {e}")

    return {
        "entitlement_id": ent.id,
        "gym_id": item.gym_id,
        "sessions": sessions,
        "active_from": start_dt.isoformat(),
        "active_until": end_dt.isoformat(),
        "status": "active",
        "service_type": "personal_training"
    }

def _finalize_captured_membership(
    db: Session,
    order: Order,
    payment_data: Dict[str, Any],
    create_commission_line: bool = True,
):

    _assert_amount_currency(order, payment_data)
    item = order.items[0]

    # Idempotency: if captured payment already exists for this order+provider_payment_id, return ok
    existing = (
        db.query(Payment)
        .filter(Payment.order_id == order.id,
                Payment.provider_payment_id == payment_data.get("id"),
                Payment.status == "captured")
        .first()
    )
    if existing:
        ent = (db.query(Entitlement)
                 .filter(Entitlement.order_item_id == item.id,
                         Entitlement.entitlement_type == EntType.membership)
                 .order_by(Entitlement.created_at.desc())
                 .first())
        return {
            "verified": True,
            "captured": True,
            "order_id": order.id,
            "payment_id": existing.provider_payment_id,
            "entitlement_id": ent.id if ent else None,
        }

    # 1) mark order paid + write payment
    order.status = StatusOrder.paid
    db.add(order)

    pay = Payment(
        id=_new_id("pay_"),
        order_id=order.id,
        customer_id=order.customer_id,
        amount_minor=order.gross_amount_minor,
        currency=order.currency,
        provider=order.provider,  # "razorpay_pg" or "offline"
        provider_payment_id=payment_data.get("id"),
        status="captured",
        captured_at=datetime.now(UTC),
        payment_metadata={
            "method": payment_data.get("method"),
            "source": "gym_checkout"
        },
    )
    db.add(pay)
    db.flush()

    # 2) Entitlement window - membership starts from payment time + duration
    meta = item.item_metadata or {}
    duration = int(meta.get("duration_months", 1))
    start_on = meta.get("start_on")
    amount_major = meta.get("amount")

    if amount_major is None:
        derived_amount = (item.unit_price_minor * item.qty) / 100
        amount_major = derived_amount
        logger.warning(
            "Legacy membership metadata missing amount; deriving from unit price",
            extra={
                "order_item_id": item.id,
                "order_id": order.id,
                "unit_price_minor": item.unit_price_minor,
                "qty": item.qty,
                "derived_amount_major": derived_amount,
                "metadata_keys": list(meta.keys()),
            },
        )
    else:
        logger.debug(
            "Legacy membership metadata amount resolved",
            extra={
                "order_item_id": item.id,
                "order_id": order.id,
                "amount_major": amount_major,
                "unit_price_minor": item.unit_price_minor,
                "qty": item.qty,
            },
        )

    # If start_on is provided, use it; otherwise membership starts from payment capture time
    if start_on:
        start_dt = datetime.fromisoformat(start_on).replace(tzinfo=UTC)
    else:
        # Membership starts from when payment was captured (not created)
        start_dt = datetime.now(UTC)

    # Membership is active until start_date + duration months
    end_dt = start_dt + relativedelta(months=duration)

    ent = Entitlement(
        id=_new_id("ent_"),
        order_item_id=item.id,
        customer_id=order.customer_id,
        gym_id=item.gym_id,
        entitlement_type=EntType.membership,
        active_from=start_dt,
        active_until=end_dt,
        status=StatusEnt.active,
    )
    db.add(ent)
    db.flush()

    # 3) Create basic payout line (commission calculation removed)
    if create_commission_line:
        gross = item.unit_price_minor * item.qty
        pl = PayoutLine(
            id=_new_id("pl_"),
            entitlement_id=ent.id,
            gym_id=item.gym_id,
            gross_amount_minor=gross,
            commission_amount_minor=0,  # No commission calculation
            net_amount_minor=gross,      # Full amount to gym
            applied_commission_pct=0.0,
            applied_commission_fixed_minor=0,
            scheduled_for=date.today() + timedelta(days=7),  # cooling-off window
            status=StatusPayoutLine.pending,
        )
        db.add(pl)

    # Settlement tracking integration
    try:
        from ..services.gym_membership_settlements import ensure_gym_exists, LedgerEarning

        # Ensure gym exists in settlements system
        gym_entity = ensure_gym_exists(db, str(item.gym_id), f"Gym {item.gym_id}")

        # Create ledger earning entry (pending settlement)
        ledger_earning = LedgerEarning(
            gym_id=gym_entity.id,
            payment_id=pay.provider_payment_id,
            order_id=order.id,
            amount_gross_minor=pay.amount_minor,
            fees_minor=0,  # Will be updated when settlement arrives
            tax_minor=0,   # Will be updated when settlement arrives
            amount_net_minor=pay.amount_minor,  # Will be adjusted with actual fees
            state="pending_settlement",
        )
        db.add(ledger_earning)
        logger.info(f"Created settlement tracking for gym {item.gym_id}, payment {pay.provider_payment_id}")
    except ImportError:
        logger.warning("Gym settlement tracking not available")
    except Exception as e:
        logger.error(f"Failed to create settlement tracking: {e}")

    # 4) Mirror to legacy table
    _upsert_gym_fees(
        db,
        client_id=order.customer_id,
        start_date=start_dt.date(),
        end_date=end_dt.date()
    )

    # 5) Insert into gym_business_payment table
    # try:
    #     gym_business_payment = GymBusinessPayment(
    #         client_id=order.customer_id,
    #         gym_id=item.gym_id,
    #         date=date.today(),
    #         amount=item.unit_price_minor / 100,  # Convert from minor units to major units
    #         status="not_initiated",
    #         mode="gym_membership",  # Legacy flow is always gym membership
    #         entitlement_id=ent.id,
    #         payment_id=pay.provider_payment_id,
    #         order_id=order.id
    #     )
    #     db.add(gym_business_payment)
    #     logger.info(f"Created gym_business_payment record for legacy gym_membership: {ent.id}")
    # except Exception as e:
    #     logger.warning(f"Failed to create gym_business_payment record in legacy flow: {e}")

    # 6) Insert into fittbot_gym_membership table for legacy flow
    try:
        fittbot_membership = FittbotGymMembership(
            gym_id=str(item.gym_id),
            client_id=order.customer_id,
            plan_id=meta.get("plan_id"),
            amount=amount_major,
            type=meta.get("service_type", "gym_membership"),
            entitlement_id=ent.id,
            purchased_at=datetime.now(),
            status="upcoming",
            joined_at=start_dt.date() if start_dt else None,
            expires_at=end_dt.date() if end_dt else None,
        )
        db.add(fittbot_membership)
        logger.info(
            "Created fittbot_gym_membership record for legacy membership",
            extra={
                "entitlement_id": ent.id,
                "gym_id": fittbot_membership.gym_id,
                "client_id": fittbot_membership.client_id,
                "plan_id": fittbot_membership.plan_id,
                "amount_major": fittbot_membership.amount,
            },
        )
    except Exception as e:
        logger.warning(f"Failed to create legacy fittbot_gym_membership record: {e}")

    db.commit()

    logger.info(
        "Membership activated",
        extra={
            "order_id": order.id,
            "payment_id": _mask(pay.provider_payment_id),
            "client_id": _mask(order.customer_id),
            "gym_id": item.gym_id,
            "active_from": start_dt.isoformat(),
            "active_until": end_dt.isoformat(),
        },
    )

    return {
        "verified": True,
        "captured": True,
        "order_id": order.id,
        "payment_id": pay.provider_payment_id,
        "entitlement_id": ent.id,
        "active_from": start_dt.isoformat(),
        "active_until": end_dt.isoformat(),
    }


@router.post("/webhook/razorpay")
async def handle_razorpay_webhook(
    request: Request,
    db: Session = Depends(get_db_session),
):
    """
    Handle Razorpay webhook for gym membership payments
    This will process payment.captured events and activate memberships
    """
    import json
    from ..utils.webhook_verifier import verify_razorpay_signature

    settings = get_payment_settings()

    # Get raw body and signature
    body = await request.body()
    signature = request.headers.get("X-Razorpay-Signature", "")

    # Verify signature
    if not verify_razorpay_signature(body.decode(), signature, settings.razorpay_webhook_secret):
        logger.warning("Invalid webhook signature")
        raise HTTPException(http_status.HTTP_401_UNAUTHORIZED, "Invalid signature")

    try:
        # Parse payload
        payload = json.loads(body.decode())
        event_type = payload.get("event")
        payment_data = payload.get("payload", {}).get("payment", {}).get("entity", {})

        logger.info(f"Received webhook: {event_type}")

        # Only handle payment.captured events for gym memberships
        if event_type != "payment.captured":
            return {"status": "ignored", "event": event_type}

        # Get payment details
        payment_id = payment_data.get("id")
        order_id = payment_data.get("order_id")

        if not payment_id or not order_id:
            logger.warning("Missing payment_id or order_id in webhook")
            return {"status": "error", "message": "Missing required fields"}

        # Find the order
        order = db.query(Order).filter(Order.provider_order_id == order_id).first()
        if not order:
            logger.warning(f"Order not found for provider_order_id: {order_id}")
            return {"status": "ignored", "message": "Order not found"}

        # Check if this is a gym membership order
        if not order.items or order.items[0].item_type != ItemType.gym_membership:
            logger.info(f"Not a gym membership order: {order.id}")
            return {"status": "ignored", "message": "Not a gym membership order"}

        # Process the captured payment using our idempotent function
        try:
            result = _finalize_captured_membership(db, order, payment_data)
            logger.info(f"Webhook processed gym membership: {order.id}")

            return {
                "status": "processed",
                "order_id": order.id,
                "entitlement_id": result.get("entitlement_id"),
                "active_from": result.get("active_from"),
                "active_until": result.get("active_until"),
            }

        except Exception as e:
            logger.error(f"Failed to process webhook for order {order.id}: {str(e)}")
            db.rollback()
            raise HTTPException(http_status.HTTP_500_INTERNAL_SERVER_ERROR, "Processing failed")

    except json.JSONDecodeError:
        logger.error("Invalid JSON in webhook payload")
        raise HTTPException(http_status.HTTP_400_BAD_REQUEST, "Invalid JSON")
    except Exception as e:
        logger.error(f"Webhook processing error: {str(e)}")
        raise HTTPException(http_status.HTTP_500_INTERNAL_SERVER_ERROR, "Internal server error")
