
from __future__ import annotations
import base64
import hashlib
import hmac
import json
import logging
import secrets
import time
from datetime import datetime, timedelta, timezone, date
from typing import Any, Dict, Optional, Tuple

import requests
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

def _rzp_create_order(
    amount_minor: int,
    currency: str,
    receipt: str,
    notes: Dict[str, Any],
    settings
) -> Dict[str, Any]:
    """Create Razorpay order"""
    try:
        resp = requests.post(
            f"{RZP_API}/orders",
            headers=_rzp_auth_headers(settings),
            data=json.dumps({
                "amount": amount_minor,
                "currency": currency,
                "receipt": receipt,
                "payment_capture": 1,
                "notes": notes
            }),
            timeout=(5, 15),
        )
        resp.raise_for_status()
        return resp.json()
    except requests.RequestException as e:
        logger.error("RZP order create failed", extra={"receipt": receipt, "error": str(e)})
        raise HTTPException(http_status.HTTP_502_BAD_GATEWAY, "Failed to create order with provider")

def _rzp_get_payment(payment_id: str, settings) -> Dict[str, Any]:
    """Get Razorpay payment details"""
    try:
        resp = requests.get(
            f"{RZP_API}/payments/{payment_id}",
            headers=_rzp_auth_headers(settings),
            timeout=(5, 15)
        )
        resp.raise_for_status()
        return resp.json()
    except requests.RequestException as e:
        logger.error("RZP get payment failed", extra={"pid": _mask(payment_id), "error": str(e)})
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

def _extend_subscription_validity(db: Session, subscription_id: str, additional_days: int) -> bool:
    """Extend subscription validity by additional days"""
    try:
        subscription = db.query(Subscription).filter(Subscription.id == subscription_id).first()
        if subscription:
            # Extend the validity
            subscription.active_until = subscription.active_until + timedelta(days=additional_days)
            db.add(subscription)
            logger.info(f"Extended subscription {subscription_id} by {additional_days} days")
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
    duration_days = int(meta.get("duration_days", 30))
    product_id = f"fittbot_plan_{plan_id}" if plan_id else "fittbot_plan"
    is_paid = meta.get("is_paid", True)
    is_existing = meta.get("is_existing", False)
    existing_subscription_id = meta.get("existing_subscription_id")
    existing_subscription_provider = meta.get("existing_subscription_provider")
    existing_active_until = meta.get("existing_subscription_active_until")

    nowu = datetime.now(UTC)
    paused_existing = False
    extended_existing = False

    # Handle existing subscription logic
    if not is_paid and is_existing and existing_subscription_id:
        logger.info(f"Processing existing subscription logic for {existing_subscription_id}")

        # Try to pause existing subscription if it's from Razorpay
        if existing_subscription_provider == "razorpay":
            settings = get_payment_settings()
            paused_existing = _pause_razorpay_subscription(existing_subscription_id, settings)

        # Extend existing subscription validity
        if paused_existing or existing_subscription_provider != "razorpay":
            extended_existing = _extend_subscription_validity(db, existing_subscription_id, duration_days)
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
                        "extension_days": duration_days
                    }

    # Create new subscription (default behavior or if extension failed)
    sub = Subscription(
        id=_new_id("sub_"),
        customer_id=customer_id,
        provider="internal_manual",
        product_id=str(product_id),
        status=SubscriptionStatus.active,
        rc_original_txn_id=None,
        latest_txn_id=payment_id,
        active_from=nowu,
        active_until=(nowu + timedelta(days=duration_days)),
        auto_renew=False,
    )
    db.add(sub)

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
        "is_paid": is_paid,
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
    plan_id: int
    client_id: str
    includeSubscription: bool = False
    selectedFittbotPlan: Optional[str] = None
    is_paid: bool = True  # If True, add subscription cost to total; if False, subscription is free
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

    # 1) Get gym membership pricing
    gym_amount_minor, duration_months = _load_gym_plan(db, gym_id, plan_id)

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

        # Handle subscription pricing based on is_paid flag
        if body.is_paid:
            sub_total = int(plan.price)  # Add subscription cost to total
        else:
            sub_total = 0  # Free subscription, don't add to total

            # Check for existing subscription if is_existing is True
            if body.is_existing:
                existing_subscription = _find_active_subscription(db, user_id)
                if existing_subscription:
                    logger.info(f"Found existing subscription for user {user_id}: {existing_subscription['id']}")

        if sub_duration_days <= 0:
            raise HTTPException(http_status.HTTP_409_CONFLICT, "Invalid subscription plan configuration")

    grand_total = gym_amount_minor + sub_total

    # 3) Create internal order + items
    order = Order(
        id=_new_id("ord_"),
        customer_id=user_id,
        provider="razorpay_pg",
        currency="INR",
        gross_amount_minor=grand_total,
        status=StatusOrder.pending
    )
    db.add(order)
    db.flush()

    # Gym membership item
    gym_item = OrderItem(
        id=_new_id("itm_"),
        order_id=order.id,
        item_type=ItemType.gym_membership,
        gym_id=str(gym_id),
        unit_price_minor=gym_amount_minor,
        qty=1,
        item_metadata={
            "plan_id": plan_id,
            "duration_months": duration_months
        }
    )
    db.add(gym_item)

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
                "is_paid": body.is_paid,
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
            "user_id": user_id,
            "gym_id": gym_id,
            "plan_id": plan_id,
            "flow": "unified_gym_membership_with_sub" if body.includeSubscription else "gym_membership_only",
            "gym_total": gym_amount_minor,
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
        "is_paid": body.is_paid,
        "is_existing": body.is_existing,
        "has_existing_subscription": existing_subscription is not None
    })

    response_data = {
        "razorpay_order_id": rzp_order["id"],
        "razorpay_key_id": settings.razorpay_key_id,
        "order_id": order.id,
        "amount_minor": grand_total,
        "currency": "INR",
        "gym_amount": gym_amount_minor,
        "subscription_amount": sub_total,
        "total_amount": grand_total,
        "includes_subscription": body.includeSubscription,
        "subscription_is_paid": body.is_paid,
        "subscription_is_existing": body.is_existing,
        "display_title": f"Gym Membership" + (
            " + App Subscription" if body.includeSubscription and body.is_paid
            else " + Free App Subscription" if body.includeSubscription and not body.is_paid
            else ""
        ),
    }

    # Add existing subscription info if applicable
    if existing_subscription:
        response_data["existing_subscription"] = {
            "id": existing_subscription["id"],
            "provider": existing_subscription["provider"],
            "active_until": existing_subscription["active_until"].isoformat(),
            "will_be_paused": True
        }

    return response_data

@router.post("/checkout/create-order")
async def create_order(
    body: Credentials,

    db: Session = Depends(get_db_session),
):

    user_id = body.client_id
    settings = get_payment_settings()

    gym_id = body.gym_id
    plan_id = body.plan_id
 # optional ISO date

    if not isinstance(gym_id, int) or not isinstance(plan_id, int):
        raise HTTPException(http_status.HTTP_400_BAD_REQUEST, "gym_id and plan_id are required integers")

    # 1) authoritative price & duration
    amount_minor, duration_months = _load_gym_plan(db, gym_id, plan_id)

    # 2) create internal order + item
    order = Order(
        id=_new_id("ord_"),
        customer_id=user_id,
        provider="razorpay_pg",
        currency="INR",
        gross_amount_minor=amount_minor,
        status=StatusOrder.pending
    )
    db.add(order)
    db.flush()

    item = OrderItem(
        id=_new_id("itm_"),
        order_id=order.id,
        item_type=ItemType.gym_membership,
        gym_id=str(gym_id),
        unit_price_minor=amount_minor,
        qty=1,
        item_metadata={
            "plan_id": plan_id,
            "duration_months": duration_months
        }
    )
    db.add(item)
    db.flush()

    # 3) provider order
    rzp_order = _rzp_create_order(
        amount_minor=amount_minor,
        currency="INR",
        receipt=order.id,
        notes={
            "order_id": order.id,
            "user_id": user_id,
            "gym_id": gym_id,
            "plan_id": plan_id
        },
        settings=settings,
    )
    order.provider_order_id = rzp_order["id"]
    db.add(order)
    db.commit()

    logger.info("Gym order created", extra={"order_id": order.id, "rzp_order_id": _mask(rzp_order['id'])})
    return {
        "razorpay_order_id": rzp_order["id"],
        "razorpay_key_id": settings.razorpay_key_id,
        "order_id": order.id,
        "amount_minor": amount_minor,
        "currency": "INR",
        "display_title": f"Gym membership (plan {plan_id})",
    }

@router.post("/checkout/unified-verify")
async def unified_verify_checkout(
    body: Dict[str, Any],
    creds: Optional[HTTPAuthorizationCredentials] = Depends(security),
    db: Session = Depends(get_db_session),
):
    """Unified verification that handles both gym membership and app subscription"""
    _ = _require_user_id(creds)
    settings = get_payment_settings()

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

    status = payment.get("status")
    if status == "captured":
        return _finalize_unified_captured_payment(db, order, payment)

    if status == "authorized":
        return {"verified": True, "captured": False, "retryAfterMs": 3000, "message": "Payment authorized, finalizing..."}

    if status in ("failed", "refunded"):
        return {"verified": False, "captured": False, "status": status, "message": f"Payment {status}"}

    return {"verified": True, "captured": False, "retryAfterMs": 3000, "message": "Payment verification in progress"}

@router.post("/checkout/verify")
async def verify_checkout(
    body: Dict[str, Any],
    creds: Optional[HTTPAuthorizationCredentials] = Depends(security),
    db: Session = Depends(get_db_session),
):

    _ = _require_user_id(creds)
    settings = get_payment_settings()

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
        # soft failure for client retry; webhook will finish
        return {"verified": True, "captured": False, "retryAfterMs": 4000, "message": "Verifying payment status"}

    status = payment.get("status")
    if status == "captured":
        return _finalize_captured_membership(db, order, payment)

    if status == "authorized":
        return {"verified": True, "captured": False, "retryAfterMs": 3000, "message": "Payment authorized, finalizing..."}

    if status in ("failed", "refunded"):
        return {"verified": False, "captured": False, "status": status, "message": f"Payment {status}"}

    return {"verified": True, "captured": False, "retryAfterMs": 3000, "message": "Payment verification in progress"}

@router.post("/membership/offline-record")
async def record_offline_membership(
    body: Dict[str, Any],
    creds: Optional[HTTPAuthorizationCredentials] = Depends(security),
    db: Session = Depends(get_db_session),
):

    _ = _require_user_id(creds)  # TODO: enforce admin/owner role

    client_id = body.get("client_id")
    gym_id = body.get("gym_id")
    plan_id = body.get("plan_id")
    amount = body.get("amount")
    start_on = body.get("start_on")

    if not all([client_id, isinstance(gym_id, int), isinstance(plan_id, int), isinstance(amount, (int, float))]):
        raise HTTPException(http_status.HTTP_400_BAD_REQUEST, "Invalid payload")

    expected_minor, duration_months = _load_gym_plan(db, gym_id, plan_id)
    paid_minor = int(amount) * 100
    if paid_minor <= 0:
        raise HTTPException(http_status.HTTP_409_CONFLICT, "Invalid amount")

    # Order + item
    order = Order(
        id=_new_id("ord_"),
        customer_id=client_id,
        provider="offline",
        currency="INR",
        gross_amount_minor=paid_minor,
        status=StatusOrder.pending
    )
    db.add(order)
    db.flush()

    item = OrderItem(
        id=_new_id("itm_"),
        order_id=order.id,
        item_type=ItemType.gym_membership,
        gym_id=str(gym_id),
        unit_price_minor=paid_minor,
        qty=1,
        item_metadata={
            "plan_id": plan_id,
            "duration_months": duration_months,
            "start_on": start_on,
            "source": "offline"
        }
    )
    db.add(item)
    db.flush()

    offline_payment = {
        "id": f"offline_{order.id}",
        "amount": paid_minor,
        "currency": "INR",
        "method": "offline",
        "status": "captured",
    }
    result = _finalize_captured_membership(db, order, offline_payment, create_commission_line=False)
    return {"recorded": True, **result}


def _finalize_unified_captured_payment(
    db: Session,
    order: Order,
    payment_data: Dict[str, Any],
) -> Dict[str, Any]:
    """Process both gym membership and app subscription from a single unified payment"""
    _assert_amount_currency(order, payment_data)

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
            "message": "Payment already processed"
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
        "gym_membership_activated": gym_activated,
        "gym_membership_details": gym_details,
        "subscription_activated": sub_activated,
        "subscription_details": sub_details,
        "total_amount": order.gross_amount_minor,
        "currency": "INR",
        "message": "Payment verified and services activated"
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

    return {
        "entitlement_id": ent.id,
        "gym_id": item.gym_id,
        "active_from": start_dt.isoformat(),
        "active_until": end_dt.isoformat(),
        "status": "active"
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