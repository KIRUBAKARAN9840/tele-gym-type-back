"""
Enterprise Razorpay Payment System
Exact mirror of Google Play billing with proper provider handling, idempotency, and webhook flow
Uses SubscriptionSyncService for enterprise-level synchronization
"""

import json
import logging
import hmac
import hashlib
import razorpay
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any
from fastapi import APIRouter, Request, HTTPException, Depends, BackgroundTasks
from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session
from pydantic import BaseModel, Field

from .config.database import get_db_session
# from .config.settings import get_payment_settings
from .models.subscriptions import Subscription
from .models.entitlements import Entitlement
from .models.orders import Order, OrderItem
from .models.payments import Payment
from .models.catalog import CatalogProduct
from .models.webhook_logs import WebhookProcessingLog
from .models.enums import (
    Provider, StatusOrder, StatusPayment, SubscriptionStatus,
    StatusEnt, ItemType, EntType, WebhookProvider
)
from .services.subscription_sync_service import SubscriptionSyncService
from .utils import generate_unique_id
from app.config.settings import settings

# IST timezone
IST = timezone(timedelta(hours=5, minutes=30))

def now_ist():
    """Get current time in IST"""
    return datetime.now(IST)

logger = logging.getLogger("payments.razorpay_enterprise")
router = APIRouter(prefix="/razorpay", tags=["Razorpay Enterprise Payment System"])

# Initialize Razorpay client
razorpay_client = razorpay.Client(auth=(settings.razorpay_key_id, settings.razorpay_key_secret))

# Pydantic models for requests
class CreateSubscriptionRequest(BaseModel):
    customer_id: str = Field(..., min_length=1, max_length=100)
    product_id: str = Field(..., min_length=1, description="Product SKU from catalog")
    customer_email: Optional[str] = None
    customer_name: Optional[str] = None
    customer_contact: Optional[str] = None

class VerifyPaymentRequest(BaseModel):
    razorpay_payment_id: str
    razorpay_subscription_id: str
    razorpay_signature: str

class OneTimePaymentRequest(BaseModel):
    customer_id: str = Field(..., min_length=1, max_length=100)
    amount: float = Field(..., gt=0)
    description: str
    customer_email: Optional[str] = None
    customer_name: Optional[str] = None
    customer_contact: Optional[str] = None

class VerifyOrderRequest(BaseModel):
    razorpay_payment_id: str
    razorpay_order_id: str
    razorpay_signature: str

def get_razorpay_plan_details(plan_id: str) -> Dict[str, Any]:
    """
    Fetch plan details from Razorpay API (NOT hardcoded)
    This gets actual active_from, active_until, and interval from Razorpay
    """
    try:
        plan = razorpay_client.plan.fetch(plan_id)
        return {
            "id": plan["id"],
            "amount": plan["item"]["amount"],
            "currency": plan["item"]["currency"],
            "interval": plan["period"],  # 'monthly', 'yearly', etc.
            "interval_count": plan["interval"],  # 1, 3, 6, 12 etc.
            "description": plan["item"]["description"],
            "created_at": plan["created_at"]
        }
    except Exception as e:
        logger.error(f"Failed to fetch Razorpay plan {plan_id}: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to fetch plan details")

def get_razorpay_subscription_details(subscription_id: str) -> Dict[str, Any]:
    """
    Fetch subscription details from Razorpay API
    This gets actual start_at, end_at dates from Razorpay
    """
    try:
        subscription = razorpay_client.subscription.fetch(subscription_id)
        return {
            "id": subscription["id"],
            "status": subscription["status"],
            "current_start": subscription.get("current_start"),
            "current_end": subscription.get("current_end"),
            "start_at": subscription.get("start_at"),
            "end_at": subscription.get("end_at"),
            "plan_id": subscription["plan_id"],
            "customer_id": subscription.get("customer_id"),
            "created_at": subscription["created_at"]
        }
    except Exception as e:
        logger.error(f"Failed to fetch Razorpay subscription {subscription_id}: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to fetch subscription details")

def get_product_from_catalog(db: Session, product_id: str) -> CatalogProduct:
    """Get product from catalog with proper error handling"""
    product = db.query(CatalogProduct).filter(
        CatalogProduct.sku == product_id,
        CatalogProduct.active == True
    ).first()

    if not product:
        raise HTTPException(status_code=404, detail=f"Product {product_id} not found in catalog")

    if not product.provider_product_id:
        raise HTTPException(status_code=400, detail=f"Product {product_id} not configured for Razorpay (missing provider_product_id)")

    return product

def create_razorpay_subscription(customer_id: str, plan_id: str, notes: Dict = None) -> Dict:
    """Create subscription in Razorpay with proper error handling"""
    subscription_data = {
        "plan_id": plan_id,
        "customer_notify": 1,
        "notes": notes or {}
    }

    try:
        subscription = razorpay_client.subscription.create(subscription_data)
        return subscription
    except Exception as e:
        logger.error(f"Razorpay subscription creation failed: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to create subscription: {str(e)}")

def validate_signature(payment_id: str, subscription_id: str, signature: str) -> bool:
    """Validate Razorpay signature for subscription"""
    try:
        generated_signature = hmac.new(
            settings.razorpay_key_secret.encode('utf-8'),
            f"{payment_id}|{subscription_id}".encode('utf-8'),
            hashlib.sha256
        ).hexdigest()
        return hmac.compare_digest(signature, generated_signature)
    except Exception:
        return False

def validate_order_signature(order_id: str, payment_id: str, signature: str) -> bool:
    """Validate Razorpay signature for order"""
    try:
        generated_signature = hmac.new(
            settings.razorpay_key_secret.encode('utf-8'),
            f"{order_id}|{payment_id}".encode('utf-8'),
            hashlib.sha256
        ).hexdigest()
        return hmac.compare_digest(signature, generated_signature)
    except Exception:
        return False

def generate_event_id_razorpay(event: dict) -> str:
    """Generate unique event ID for Razorpay webhook idempotency (like RevenueCat)"""
    event_type = event.get("event", "unknown")

    if event_type == "subscription.activated":
        subscription_entity = event.get("payload", {}).get("subscription", {}).get("entity", {})
        subscription_id = subscription_entity.get("id", "")
        start_at = subscription_entity.get("start_at", "")
        return f"SUB_ACTIVATED_{subscription_id}_{start_at}"

    elif event_type == "subscription.charged":
        payment_entity = event.get("payload", {}).get("payment", {}).get("entity", {})
        payment_id = payment_entity.get("id", "")
        subscription_id = payment_entity.get("subscription_id", "")
        created_at = payment_entity.get("created_at", "")
        return f"SUB_CHARGED_{subscription_id}_{payment_id}_{created_at}"

    elif event_type == "subscription.cancelled":
        subscription_entity = event.get("payload", {}).get("subscription", {}).get("entity", {})
        subscription_id = subscription_entity.get("id", "")
        cancelled_at = subscription_entity.get("cancelled_at", int(datetime.now().timestamp()))
        return f"SUB_CANCELLED_{subscription_id}_{cancelled_at}"

    elif event_type == "payment.captured":
        payment_entity = event.get("payload", {}).get("payment", {}).get("entity", {})
        payment_id = payment_entity.get("id", "")
        order_id = payment_entity.get("order_id", "")
        created_at = payment_entity.get("created_at", "")
        return f"PAYMENT_CAPTURED_{order_id}_{payment_id}_{created_at}"

    else:
        timestamp = int(datetime.now().timestamp())
        return f"{event_type.upper()}_{timestamp}"

# SUBSCRIPTION ENDPOINTS

@router.post("/subscriptions/create")
async def create_subscription(
    request: CreateSubscriptionRequest,
    db: Session = Depends(get_db_session)
):
    """
    Step 1: Create Razorpay subscription (Enterprise flow like Google Play)
    """
    try:
        logger.info(f"Creating subscription for customer: {request.customer_id}, product: {request.product_id}")

        # Get product from catalog (same as Google Play)
        product = get_product_from_catalog(db, request.product_id)
        razorpay_plan_id = product.provider_product_id

        # Get actual plan details from Razorpay API (NOT hardcoded)
        plan_details = get_razorpay_plan_details(razorpay_plan_id)

        # Create subscription in Razorpay
        rp_subscription = create_razorpay_subscription(
            customer_id=request.customer_id,
            plan_id=razorpay_plan_id,
            notes={
                "customer_id": request.customer_id,
                "product_id": request.product_id,
                "source": "fittbot_mobile",
                "internal_product_sku": product.sku
            }
        )

        # Create subscription record using SubscriptionSyncService pattern
        sync_service = SubscriptionSyncService(db)
        subscription_id = sync_service.generate_id("sub")

        subscription = Subscription(
            id=subscription_id,
            customer_id=request.customer_id,
            provider=Provider.razorpay_pg.value,  # Use proper enum
            product_id=request.product_id,
            status=SubscriptionStatus.pending.value,
            rc_original_txn_id=rp_subscription["id"],  # Store Razorpay subscription ID
            created_at=now_ist(),
            updated_at=now_ist()
        )
        db.add(subscription)

        # Create order record (enterprise pattern)
        order_id = sync_service.generate_id("order")
        order = Order(
            id=order_id,
            customer_id=request.customer_id,
            provider=Provider.razorpay_pg.value,
            provider_order_id=rp_subscription["id"],
            gross_amount_minor=plan_details["amount"],
            currency=plan_details["currency"],
            status=StatusOrder.pending.value,
            created_at=now_ist()
        )
        db.add(order)

        # Create order item (enterprise pattern)
        order_item = OrderItem(
            id=sync_service.generate_id("item"),
            order_id=order_id,
            catalog_product_id=product.id,
            quantity=1,
            unit_amount_minor=plan_details["amount"]
        )
        db.add(order_item)

        db.commit()

        logger.info(f"✅ Created subscription: {rp_subscription['id']}")

        # Return response for frontend
        return {
            "subscription_id": rp_subscription["id"],
            "razorpay_key_id": settings.razorpay_key_id,
            "product_title": product.title,
            "amount": plan_details["amount"],
            "currency": plan_details["currency"],
            "prefill": {
                "email": request.customer_email,
                "contact": request.customer_contact,
                "name": request.customer_name
            }
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error creating subscription: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to create subscription")

@router.post("/subscriptions/verify")
async def verify_subscription(
    request: VerifyPaymentRequest,
    db: Session = Depends(get_db_session)
):
    """
    Step 3: Verify subscription payment (Enterprise pattern)
    """
    try:
        logger.info(f"Verifying subscription payment: {request.razorpay_payment_id}")

        # Validate signature (critical security step)
        if not validate_signature(
            request.razorpay_payment_id,
            request.razorpay_subscription_id,
            request.razorpay_signature
        ):
            logger.error(f"Invalid signature for payment: {request.razorpay_payment_id}")
            raise HTTPException(status_code=400, detail="Invalid payment signature")

        # Update order status (enterprise pattern)
        order = db.query(Order).filter(
            Order.provider_order_id == request.razorpay_subscription_id
        ).first()

        if order:
            order.status = StatusOrder.paid.value
            order.provider_payment_id = request.razorpay_payment_id
            order.updated_at = now_ist()

        # Update subscription status
        subscription = db.query(Subscription).filter(
            Subscription.rc_original_txn_id == request.razorpay_subscription_id
        ).first()

        if subscription:
            subscription.latest_txn_id = request.razorpay_payment_id
            subscription.updated_at = now_ist()

        db.commit()

        logger.info(f"✅ Payment verified: {request.razorpay_payment_id}")

        return {
            "verified": True,
            "subscription_id": request.razorpay_subscription_id,
            "payment_id": request.razorpay_payment_id,
            "message": "Payment verified. Subscription will be activated via webhook."
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error verifying subscription: {str(e)}")
        raise HTTPException(status_code=500, detail="Payment verification failed")

# ONE-TIME PAYMENT ENDPOINTS

@router.post("/orders/create")
async def create_order(
    request: OneTimePaymentRequest,
    db: Session = Depends(get_db_session)
):
    """Create one-time payment order (Enterprise pattern)"""
    try:
        logger.info(f"Creating order for customer: {request.customer_id}, amount: ₹{request.amount}")

        amount_minor = int(request.amount * 100)  # Convert to paise
        sync_service = SubscriptionSyncService(db)

        # Create order in Razorpay
        order_data = {
            "amount": amount_minor,
            "currency": "INR",
            "receipt": sync_service.generate_id("receipt"),
            "notes": {
                "customer_id": request.customer_id,
                "description": request.description
            }
        }

        rp_order = razorpay_client.order.create(order_data)

        # Create order record (enterprise pattern)
        order_id = sync_service.generate_id("order")
        order = Order(
            id=order_id,
            customer_id=request.customer_id,
            provider=Provider.razorpay_pg.value,
            provider_order_id=rp_order["id"],
            gross_amount_minor=amount_minor,
            currency="INR",
            status=StatusOrder.pending.value,
            created_at=now_ist()
        )
        db.add(order)
        db.commit()

        logger.info(f"✅ Created order: {rp_order['id']}")

        return {
            "order_id": rp_order["id"],
            "razorpay_key_id": settings.razorpay_key_id,
            "amount": amount_minor,
            "currency": "INR",
            "name": "Fittbot",
            "description": request.description,
            "prefill": {
                "email": request.customer_email,
                "contact": request.customer_contact,
                "name": request.customer_name
            }
        }

    except Exception as e:
        logger.error(f"Error creating order: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to create order")

# STATUS ENDPOINTS (Same as Google Play)

@router.get("/customers/{customer_id}/premium-status")
async def check_premium_status(
    customer_id: str,
    db: Session = Depends(get_db_session)
):
    """Check premium status using same logic as Google Play"""
    try:
        sync_service = SubscriptionSyncService(db)
        now = now_ist()

        # Check active entitlements (SAME TABLE AS GOOGLE PLAY)
        entitlement = db.query(Entitlement).filter(
            Entitlement.customer_id == customer_id,
            Entitlement.status == StatusEnt.pending.value,  # 'pending' means active in your system
            Entitlement.expires_at > now
        ).first()

        if entitlement:
            subscription = db.query(Subscription).filter(
                Subscription.id == entitlement.subscription_id
            ).first()

            return {
                "has_premium": True,
                "expires_at": entitlement.expires_at.isoformat(),
                "product_id": subscription.product_id if subscription else None,
                "provider": Provider.razorpay_pg.value
            }

        return {"has_premium": False}

    except Exception as e:
        logger.error(f"Error checking premium status: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to check premium status")

# WEBHOOK ENDPOINT (Enterprise level with idempotency)

@router.post("/webhook")
async def razorpay_webhook(
    request: Request,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db_session)
):
    """
    Razorpay webhook handler with enterprise idempotency (like Google Play/RevenueCat)
    """
    try:
        body = await request.body()
        signature = request.headers.get("x-razorpay-signature")

        if not signature:
            raise HTTPException(status_code=400, detail="Missing signature")

        # Verify webhook signature
        expected_signature = hmac.new(
            settings.razorpay_webhook_secret.encode('utf-8'),
            body,
            hashlib.sha256
        ).hexdigest()

        if not hmac.compare_digest(signature, expected_signature):
            raise HTTPException(status_code=400, detail="Invalid signature")

        # Parse webhook data
        webhook_data = json.loads(body.decode('utf-8'))
        event = webhook_data.get("event")

        logger.info(f"Received Razorpay webhook: {event}")

        # Generate unique event ID for idempotency (SAME AS REVENUECAT)
        event_id = generate_event_id_razorpay(webhook_data)

        # Check idempotency using SubscriptionSyncService (ENTERPRISE PATTERN)
        sync_service = SubscriptionSyncService(db)
        should_process, existing_log = sync_service.check_idempotency(
            event_id, event, allow_retry_on_failure=True
        )

        if not should_process:
            return {"status": "ignored", "reason": "already_processed", "event_id": event_id}

        # Create or update processing log
        if not existing_log:
            processing_log = WebhookProcessingLog(
                event_id=event_id,
                event_type=event,
                provider=WebhookProvider.razorpay_pg.value,
                raw_payload=webhook_data,
                status="processing",
                started_at=now_ist(),
                retry_count=0
            )
            db.add(processing_log)
        else:
            processing_log = existing_log

        db.commit()

        # Process webhook in background (ENTERPRISE PATTERN)
        background_tasks.add_task(process_razorpay_webhook, webhook_data, processing_log.event_id, db)

        return {"status": "accepted", "event_id": event_id}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Webhook error: {str(e)}")
        raise HTTPException(status_code=500, detail="Webhook processing failed")

async def process_razorpay_webhook(webhook_data: dict, event_id: str, db: Session):
    """
    Process Razorpay webhook events using SubscriptionSyncService (ENTERPRISE PATTERN)
    Exact same flow as Google Play/RevenueCat webhook processing
    """
    sync_service = SubscriptionSyncService(db)

    try:
        event = webhook_data.get("event")
        payload = webhook_data.get("payload", {})

        processing_log = db.query(WebhookProcessingLog).filter(
            WebhookProcessingLog.event_id == event_id
        ).first()

        if event == "subscription.activated":
            result = await handle_subscription_activation(payload, sync_service, db)
        elif event == "subscription.charged":
            result = await handle_subscription_renewal(payload, sync_service, db)
        elif event == "subscription.cancelled":
            result = await handle_subscription_cancellation(payload, sync_service, db)
        elif event == "payment.captured":
            result = await handle_payment_captured(payload, sync_service, db)
        else:
            result = {"status": "ignored", "reason": "unhandled_event_type"}

        # Update processing log
        if processing_log:
            processing_log.status = "completed"
            processing_log.completed_at = now_ist()
            processing_log.result_data = result
            db.commit()

        logger.info(f"✅ Processed webhook {event}: {result}")

    except Exception as e:
        logger.error(f"Error processing webhook {event}: {str(e)}")

        # Update processing log
        if processing_log:
            processing_log.status = "failed"
            processing_log.completed_at = now_ist()
            processing_log.error_message = str(e)
            db.commit()

async def handle_subscription_activation(payload: dict, sync_service: SubscriptionSyncService, db: Session):
    """
    Handle subscription activation using SubscriptionSyncService
    SAME FLOW AS GOOGLE PLAY INITIAL_PURCHASE
    """
    try:
        subscription_entity = payload.get("subscription", {}).get("entity", {})
        razorpay_subscription_id = subscription_entity.get("id")

        if not razorpay_subscription_id:
            return {"status": "error", "reason": "missing_subscription_id"}

        # Get actual subscription details from Razorpay API (NOT hardcoded dates)
        rp_subscription_details = get_razorpay_subscription_details(razorpay_subscription_id)

        # Find our subscription record
        subscription = db.query(Subscription).filter(
            Subscription.rc_original_txn_id == razorpay_subscription_id
        ).first()

        if not subscription:
            return {"status": "error", "reason": "subscription_not_found"}

        # Get product details
        product = get_product_from_catalog(db, subscription.product_id)

        # Calculate dates from Razorpay API response (NOT hardcoded)
        now = now_ist()
        if rp_subscription_details.get("current_start"):
            active_from = datetime.fromtimestamp(rp_subscription_details["current_start"], tz=IST)
        else:
            active_from = now

        if rp_subscription_details.get("current_end"):
            active_until = datetime.fromtimestamp(rp_subscription_details["current_end"], tz=IST)
        else:
            # Fallback: calculate based on plan interval if Razorpay doesn't provide end date
            plan_details = get_razorpay_plan_details(rp_subscription_details["plan_id"])
            if plan_details["interval"] == "monthly":
                active_until = active_from + timedelta(days=30 * plan_details["interval_count"])
            elif plan_details["interval"] == "yearly":
                active_until = active_from + timedelta(days=365 * plan_details["interval_count"])
            else:
                active_until = active_from + timedelta(days=30)  # Fallback

        # Update subscription (SAME AS GOOGLE PLAY)
        subscription.status = SubscriptionStatus.active.value
        subscription.active_from = active_from
        subscription.active_until = active_until
        subscription.updated_at = now

        # Create entitlement (SAME TABLE AS GOOGLE PLAY)
        entitlement_id = sync_service.generate_id("ent")
        entitlement = Entitlement(
            id=entitlement_id,
            customer_id=subscription.customer_id,
            subscription_id=subscription.id,
            ent_type=EntType.app.value,
            status=StatusEnt.pending.value,  # 'pending' means active in your system
            granted_at=now,
            expires_at=active_until,
            created_at=now,
            updated_at=now
        )
        db.add(entitlement)
        db.commit()

        logger.info(f"✅ Activated subscription and granted entitlement: {razorpay_subscription_id}")

        return {
            "status": "success",
            "subscription_id": subscription.id,
            "entitlement_id": entitlement_id,
            "active_from": active_from.isoformat(),
            "active_until": active_until.isoformat()
        }

    except Exception as e:
        logger.error(f"Error handling subscription activation: {str(e)}")
        return {"status": "error", "reason": str(e)}

async def handle_subscription_renewal(payload: dict, sync_service: SubscriptionSyncService, db: Session):
    """Handle subscription renewal (SAME AS GOOGLE PLAY RENEWAL)"""
    try:
        payment_entity = payload.get("payment", {}).get("entity", {})
        payment_id = payment_entity.get("id")
        razorpay_subscription_id = payment_entity.get("subscription_id")

        if not payment_id or not razorpay_subscription_id:
            return {"status": "error", "reason": "missing_payment_or_subscription_id"}

        # Get actual subscription details from Razorpay API
        rp_subscription_details = get_razorpay_subscription_details(razorpay_subscription_id)

        subscription = db.query(Subscription).filter(
            Subscription.rc_original_txn_id == razorpay_subscription_id
        ).first()

        if subscription:
            now = now_ist()

            # Calculate new expiry from Razorpay API (NOT hardcoded)
            if rp_subscription_details.get("current_end"):
                new_expires_at = datetime.fromtimestamp(rp_subscription_details["current_end"], tz=IST)
            else:
                # Fallback: extend from current expiry
                current_expiry = subscription.active_until or now
                new_expires_at = current_expiry + timedelta(days=30)  # Adjust based on plan

            # Extend subscription
            subscription.active_until = new_expires_at
            subscription.latest_txn_id = payment_id
            subscription.updated_at = now

            # Extend entitlement (SAME AS GOOGLE PLAY)
            entitlement = db.query(Entitlement).filter(
                Entitlement.subscription_id == subscription.id,
                Entitlement.status == StatusEnt.pending.value
            ).first()

            if entitlement:
                entitlement.expires_at = new_expires_at
                entitlement.updated_at = now

            # Create payment record
            payment_record = Payment(
                id=sync_service.generate_id("pay"),
                order_id=subscription.id,  # Link to subscription
                customer_id=subscription.customer_id,
                provider=Provider.razorpay_pg.value,
                provider_payment_id=payment_id,
                gross_amount_minor=payment_entity.get("amount", 0),
                currency="INR",
                status=StatusPayment.captured.value,
                created_at=now
            )
            db.add(payment_record)
            db.commit()

            logger.info(f"✅ Renewed subscription: {razorpay_subscription_id}")

            return {
                "status": "success",
                "subscription_id": subscription.id,
                "new_expires_at": new_expires_at.isoformat(),
                "payment_id": payment_id
            }

    except Exception as e:
        logger.error(f"Error handling subscription renewal: {str(e)}")
        return {"status": "error", "reason": str(e)}

async def handle_subscription_cancellation(payload: dict, sync_service: SubscriptionSyncService, db: Session):
    """Handle subscription cancellation (SAME AS GOOGLE PLAY CANCELLATION)"""
    try:
        subscription_entity = payload.get("subscription", {}).get("entity", {})
        razorpay_subscription_id = subscription_entity.get("id")

        if not razorpay_subscription_id:
            return {"status": "error", "reason": "missing_subscription_id"}

        subscription = db.query(Subscription).filter(
            Subscription.rc_original_txn_id == razorpay_subscription_id
        ).first()

        if subscription:
            now = now_ist()
            subscription.status = SubscriptionStatus.cancelled.value
            subscription.updated_at = now

            # Deactivate entitlements (SAME AS GOOGLE PLAY)
            entitlements = db.query(Entitlement).filter(
                Entitlement.subscription_id == subscription.id,
                Entitlement.status == StatusEnt.pending.value
            ).all()

            for entitlement in entitlements:
                entitlement.status = StatusEnt.revoked.value
                entitlement.updated_at = now

            db.commit()
            logger.info(f"✅ Cancelled subscription: {razorpay_subscription_id}")

            return {
                "status": "success",
                "subscription_id": subscription.id,
                "cancelled_entitlements": len(entitlements)
            }

    except Exception as e:
        logger.error(f"Error handling subscription cancellation: {str(e)}")
        return {"status": "error", "reason": str(e)}

async def handle_payment_captured(payload: dict, sync_service: SubscriptionSyncService, db: Session):
    """Handle payment capture for one-time payments"""
    try:
        payment_entity = payload.get("payment", {}).get("entity", {})
        payment_id = payment_entity.get("id")
        order_id = payment_entity.get("order_id")

        if payment_id and order_id:
            order = db.query(Order).filter(
                Order.provider_order_id == order_id
            ).first()

            if order:
                order.status = StatusOrder.paid.value
                order.provider_payment_id = payment_id
                order.updated_at = now_ist()
                db.commit()
                logger.info(f"✅ Payment captured: {payment_id}")

                return {
                    "status": "success",
                    "order_id": order.id,
                    "payment_id": payment_id
                }

    except Exception as e:
        logger.error(f"Error handling payment capture: {str(e)}")
        return {"status": "error", "reason": str(e)}

@router.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "service": "razorpay_enterprise_payment_system",
        "timestamp": now_ist().isoformat(),
        "features": [
            "enterprise_idempotency",
            "subscription_sync_service",
            "razorpay_api_integration",
            "webhook_processing",
            "google_play_compatible"
        ]
    }