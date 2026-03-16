"""
Enterprise-Level Unified Payment Verification
=============================================

This module handles payment verification for unified daily pass and subscription
payments, following the same patterns as razorpay routes and gym_membership
with comprehensive error handling and business logic separation.
"""

from __future__ import annotations

import hmac
import hashlib
import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, Optional, Tuple, List

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi import status as http_status
from pydantic import BaseModel
from sqlalchemy.orm import Session
from sqlalchemy import text

from ..config.database import get_db_session
from ..config.settings import get_payment_settings
from ..models.enums import ItemType, EntType, StatusOrder, StatusPayment, SubscriptionStatus, StatusEnt
from ..models.orders import Order, OrderItem
from ..models.payments import Payment
from ..models.subscriptions import Subscription, now_ist
from ..models.entitlements import Entitlement
from ..services.entitlement_service import EntitlementService

from app.models.dailypass_models import (
    get_dailypass_session,
    DailyPass,
    DailyPassDay,
    DailyPassAudit,
    LedgerAllocation,
)
from app.models.database import get_db as get_main_db

from .rp_client import get_payment as rzp_get_payment

# Configure logging
logger = logging.getLogger("payments.dailypass.verification")
router = APIRouter(prefix="/pay", tags=["Unified Payment Verification"])

UTC = timezone.utc
IST = timezone(timedelta(hours=5, minutes=30))


def _mask_sensitive(value: str, show_start: int = 4, show_end: int = 4) -> str:
    """Mask sensitive values for logging"""
    if not value or len(value) <= (show_start + show_end):
        return "***"
    return f"{value[:show_start]}...{value[-show_end:]}"


def _verify_checkout_signature(key_secret: str, order_id: str, payment_id: str, signature: str) -> bool:
    """Verify Razorpay checkout signature - same as gym_membership pattern"""
    try:
        data = f"{order_id}|{payment_id}".encode("utf-8")
        expected = hmac.new(key_secret.encode("utf-8"), data, hashlib.sha256).hexdigest()
        return hmac.compare_digest(expected, signature or "")
    except Exception as e:
        logger.error(f"Signature verification failed: {e}")
        return False


def _assert_payment_amount(order: Order, payment_data: Dict[str, Any]) -> None:
    """Verify payment amount matches order - same as gym_membership pattern"""
    paid_amount = int(payment_data.get("amount") or 0)
    expected_amount = order.gross_amount_minor
    currency = payment_data.get("currency") or ""

    if paid_amount != expected_amount or currency != "INR":
        logger.error(
            f"Payment amount mismatch: paid={paid_amount}, expected={expected_amount}, currency={currency}",
            extra={"order_id": order.id, "payment_id": _mask_sensitive(payment_data.get("id", ""))}
        )
        raise HTTPException(
            status_code=http_status.HTTP_409_CONFLICT,
            detail="Payment amount or currency mismatch"
        )


def _create_payment_record(
    db: Session,
    order: Order,
    customer_id: str,
    amount_minor: int,
    currency: str,
    provider_payment_id: str,
    metadata: Dict[str, Any],
    status: str = "captured"
) -> Payment:
    """Create payment record - following gym_membership pattern"""
    payment = Payment(
        id=f"pay_{int(datetime.now().timestamp() * 1000)}_{provider_payment_id[-6:]}",
        order_id=order.id,
        customer_id=customer_id,
        provider="razorpay_pg",
        provider_payment_id=provider_payment_id,
        amount_minor=amount_minor,
        currency=currency,
        status=status,
        payment_metadata=metadata,
        captured_at=datetime.now(UTC) if status == "captured" else None
    )
    db.add(payment)
    return payment


def _process_daily_pass_activation(
    db: Session,
    payments_db: Session,
    order_item: OrderItem,
    customer_id: str,
    payment_id: str
) -> Dict[str, Any]:
    """Process daily pass activation with comprehensive business logic"""
    try:
        metadata = order_item.item_metadata or {}
        gym_id = int(order_item.gym_id)
        dates = [datetime.fromisoformat(d).date() for d in metadata.get("dates", [])]
        selected_time = metadata.get("selected_time")

        if not dates:
            raise ValueError("No dates found in order item metadata")

        # Create DailyPass record
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
        payments_db.add(daily_pass)
        payments_db.flush()

        # Create individual day records
        for single_date in dates:
            day_record = DailyPassDay(
                daily_pass_id=daily_pass.id,
                date=single_date,
                status="available",
                gym_id=gym_id,
                client_id=customer_id,
            )
            payments_db.add(day_record)

        # Create audit record
        audit_record = DailyPassAudit(
            daily_pass_id=daily_pass.id,
            action="purchase",
            details=f"Daily pass purchased for {len(dates)} days",
            timestamp=datetime.now(IST),
            client_id=customer_id,
        )
        payments_db.add(audit_record)

        # Create ledger allocation
        ledger_allocation = LedgerAllocation(
            daily_pass_id=daily_pass.id,
            gym_id=gym_id,
            client_id=customer_id,
            amount=daily_pass.amount_paid,
            allocation_date=datetime.now(IST).date(),
            status="allocated",
        )
        payments_db.add(ledger_allocation)

        payments_db.commit()

        logger.info(
            "Daily pass activated successfully",
            extra={
                "daily_pass_id": daily_pass.id,
                "customer_id": _mask_sensitive(customer_id),
                "gym_id": gym_id,
                "days_count": len(dates),
                "amount": daily_pass.amount_paid
            }
        )

        return {
            "daily_pass_id": daily_pass.id,
            "start_date": dates[0].isoformat(),
            "end_date": dates[-1].isoformat(),
            "days_total": len(dates),
            "status": "active"
        }

    except Exception as e:
        payments_db.rollback()
        logger.error(f"Daily pass activation failed: {e}", exc_info=True)
        raise


def _process_subscription_activation(
    db: Session,
    order_item: OrderItem,
    customer_id: str,
    payment_id: str
) -> Dict[str, Any]:
    """Process subscription activation following razorpay routes pattern"""
    try:
        metadata = order_item.item_metadata or {}
        plan_sku = metadata.get("plan_sku", "platinum_plan_yearly")

        # Find existing subscription record
        subscription = (
            db.query(Subscription)
            .filter(
                Subscription.customer_id == customer_id,
                Subscription.product_id == plan_sku,
                Subscription.provider == "razorpay_pg"
            )
            .order_by(Subscription.created_at.desc())
            .first()
        )

        if not subscription:
            logger.warning(f"Subscription record not found for customer {_mask_sensitive(customer_id)}")
            return {"status": "subscription_not_found"}

        # Update subscription status
        subscription.status = SubscriptionStatus.active
        subscription.latest_txn_id = payment_id

        # Set subscription period (1 year for platinum plan)
        now = datetime.now(UTC)
        subscription.active_from = now
        subscription.active_until = now + timedelta(days=365)  # 1 year

        db.add(subscription)

        # Create/update entitlements
        order = db.query(Order).filter(Order.id == order_item.order_id).first()
        if order:
            # Check if entitlements already exist
            existing_entitlements = (
                db.query(Entitlement)
                .join(OrderItem, Entitlement.order_item_id == OrderItem.id)
                .filter(OrderItem.order_id == order.id)
                .all()
            )

            if not existing_entitlements:
                # Create new entitlements
                EntitlementService(db).create_entitlements_from_order(order)
                existing_entitlements = (
                    db.query(Entitlement)
                    .join(OrderItem, Entitlement.order_item_id == OrderItem.id)
                    .filter(OrderItem.order_id == order.id)
                    .all()
                )

            # Update entitlement periods
            for entitlement in existing_entitlements:
                entitlement.entitlement_type = EntType.app
                entitlement.active_from = subscription.active_from
                entitlement.active_until = subscription.active_until
                entitlement.status = StatusEnt.active
                db.add(entitlement)

        db.commit()

        logger.info(
            "Subscription activated successfully",
            extra={
                "subscription_id": subscription.id,
                "customer_id": _mask_sensitive(customer_id),
                "plan_sku": plan_sku,
                "active_until": subscription.active_until.isoformat() if subscription.active_until else None
            }
        )

        return {
            "subscription_id": subscription.id,
            "plan_sku": plan_sku,
            "active_from": subscription.active_from.isoformat() if subscription.active_from else None,
            "active_until": subscription.active_until.isoformat() if subscription.active_until else None,
            "status": "active"
        }

    except Exception as e:
        db.rollback()
        logger.error(f"Subscription activation failed: {e}", exc_info=True)
        raise


class UnifiedVerificationRequest(BaseModel):
    """Request model for unified payment verification"""
    razorpay_order_id: str
    razorpay_payment_id: str
    razorpay_signature: str


class UnifiedVerificationResponse(BaseModel):
    """Response model for unified payment verification"""
    success: bool
    payment_captured: bool
    order_id: str
    payment_id: str

    # Service activations
    daily_pass_activated: bool
    daily_pass_details: Optional[Dict[str, Any]] = None

    subscription_activated: bool
    subscription_details: Optional[Dict[str, Any]] = None

    # Summary
    total_amount: int
    currency: str
    message: str


@router.post("/dailypass/unified-verify", response_model=UnifiedVerificationResponse)
async def unified_verify_payment(
    request: UnifiedVerificationRequest,
    db: Session = Depends(get_db_session)
):
    """
    Enterprise-Level Unified Payment Verification

    Following the same patterns as razorpay routes and gym_membership:
    1. Verify signature
    2. Fetch and validate payment
    3. Process each order item separately
    4. Comprehensive error handling and logging
    """
    settings = get_payment_settings()

    # 1. SIGNATURE VERIFICATION (same as gym_membership)
    if not _verify_checkout_signature(
        settings.razorpay_key_secret,
        request.razorpay_order_id,
        request.razorpay_payment_id,
        request.razorpay_signature
    ):
        logger.warning(
            "Invalid payment signature",
            extra={
                "order_id": _mask_sensitive(request.razorpay_order_id),
                "payment_id": _mask_sensitive(request.razorpay_payment_id)
            }
        )
        raise HTTPException(
            status_code=http_status.HTTP_403_FORBIDDEN,
            detail="Invalid payment signature"
        )

    # 2. FETCH ORDER
    order = (
        db.query(Order)
        .filter(Order.provider_order_id == request.razorpay_order_id)
        .first()
    )
    if not order:
        logger.error(f"Order not found for Razorpay order ID: {_mask_sensitive(request.razorpay_order_id)}")
        raise HTTPException(
            status_code=http_status.HTTP_404_NOT_FOUND,
            detail="Order not found"
        )

    # 3. FETCH AND VALIDATE PAYMENT (same as gym_membership)
    try:
        payment_data = rzp_get_payment(request.razorpay_payment_id, settings)
        payment_status = payment_data.get("status")

        if payment_status != "captured":
            logger.warning(
                f"Payment not captured: status={payment_status}",
                extra={"payment_id": _mask_sensitive(request.razorpay_payment_id)}
            )
            raise HTTPException(
                status_code=http_status.HTTP_400_BAD_REQUEST,
                detail=f"Payment not captured. Status: {payment_status}"
            )

        _assert_payment_amount(order, payment_data)

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Payment validation failed: {e}")
        raise HTTPException(
            status_code=http_status.HTTP_502_BAD_GATEWAY,
            detail="Unable to verify payment with provider"
        )

    # 4. CHECK FOR DUPLICATE PROCESSING
    existing_payment = (
        db.query(Payment)
        .filter(
            Payment.provider_payment_id == request.razorpay_payment_id,
            Payment.status == "captured"
        )
        .first()
    )
    if existing_payment:
        logger.info(f"Payment already processed: {_mask_sensitive(request.razorpay_payment_id)}")
        return UnifiedVerificationResponse(
            success=True,
            payment_captured=True,
            order_id=order.id,
            payment_id=request.razorpay_payment_id,
            daily_pass_activated=True,
            subscription_activated=True,
            total_amount=order.gross_amount_minor,
            currency="INR",
            message="Payment already processed successfully"
        )

    # 5. PROCESS PAYMENT AND SERVICES
    try:
        # Create payment record
        payment_record = _create_payment_record(
            db=db,
            order=order,
            customer_id=order.customer_id,
            amount_minor=int(payment_data.get("amount", 0)),
            currency=payment_data.get("currency", "INR"),
            provider_payment_id=request.razorpay_payment_id,
            metadata={
                "method": payment_data.get("method"),
                "source": "unified_verification",
                "razorpay_order_id": request.razorpay_order_id
            }
        )

        # Update order status
        order.status = StatusOrder.paid
        db.add(order)

        # Get order items
        order_items = db.query(OrderItem).filter(OrderItem.order_id == order.id).all()

        daily_pass_details = None
        subscription_details = None
        daily_pass_activated = False
        subscription_activated = False

        # Get dailypass database session
        dailypass_db = next(get_dailypass_session())

        try:
            # Process each order item separately
            for item in order_items:
                if item.item_type == ItemType.daily_pass:
                    # Process daily pass
                    daily_pass_details = _process_daily_pass_activation(
                        db=db,
                        payments_db=dailypass_db,
                        order_item=item,
                        customer_id=order.customer_id,
                        payment_id=request.razorpay_payment_id
                    )
                    daily_pass_activated = True

                elif item.item_type == ItemType.app_subscription:
                    # Process subscription
                    subscription_details = _process_subscription_activation(
                        db=db,
                        order_item=item,
                        customer_id=order.customer_id,
                        payment_id=request.razorpay_payment_id
                    )
                    subscription_activated = True

        finally:
            try:
                dailypass_db.close()
            except Exception:
                pass

        db.commit()

        # 6. FINAL LOGGING
        logger.info(
            "Unified payment verification completed",
            extra={
                "order_id": order.id,
                "payment_id": _mask_sensitive(request.razorpay_payment_id),
                "customer_id": _mask_sensitive(order.customer_id),
                "amount": order.gross_amount_minor,
                "daily_pass_activated": daily_pass_activated,
                "subscription_activated": subscription_activated
            }
        )

        return UnifiedVerificationResponse(
            success=True,
            payment_captured=True,
            order_id=order.id,
            payment_id=request.razorpay_payment_id,
            daily_pass_activated=daily_pass_activated,
            daily_pass_details=daily_pass_details,
            subscription_activated=subscription_activated,
            subscription_details=subscription_details,
            total_amount=order.gross_amount_minor,
            currency="INR",
            message="Payment verified and services activated successfully"
        )

    except Exception as e:
        db.rollback()
        logger.error(f"Unified verification failed: {e}", exc_info=True)
        raise HTTPException(
            status_code=http_status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal server error during payment processing"
        )