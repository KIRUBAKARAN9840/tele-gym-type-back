"""
RevenueCat Integration - Order Creation and Webhook Handler
Handles RevenueCat subscription purchases and webhook events
"""

import json
import logging
from datetime import datetime, timezone, timedelta
from typing import Any, Callable, Dict, Optional, TypeVar
from fastapi import APIRouter, Request, Depends, HTTPException, Path, BackgroundTasks
from sqlalchemy.orm import Session
from sqlalchemy.exc import InvalidRequestError
from pydantic import BaseModel

# Indian Standard Time (IST) timezone
IST = timezone(timedelta(hours=5, minutes=30))

def now_ist():
    """Get current time in IST"""
    return datetime.now(IST)


def lock_query(query):
    """Attempt row-level lock; fall back silently if unsupported"""
    try:
        return query.with_for_update()
    except (InvalidRequestError, AttributeError):
        return query
    except Exception as lock_err:
        logger.debug("Lock not applied: %s", lock_err)
        return query

# Import database and settings
from ..config.database import get_db_session
from ..config.settings import get_payment_settings

# Import models
from ..models.orders import Order
from ..models.catalog import CatalogProduct
from ..models.webhook_logs import WebhookProcessingLog
from ..models.subscriptions import Subscription
from ..models.enums import Provider, SubscriptionStatus
from ..models.payments import Payment
from app.models.fittbot_models import FreeTrial

from ..services.subscription_sync_service import SubscriptionSyncService
from ..revenuecat.client import verify_purchase as rc_verify_purchase, RevenueCatAPIError
from ..utils import run_sync_db_operation
from app.utils.request_auth import resolve_authenticated_user_id

# Setup logging
logger = logging.getLogger("payments.revenuecat")

# Create router
router = APIRouter(prefix="/revenuecat", tags=["RevenueCat"])

# Helper functions for secure logging
def _mask(value: Optional[str], left=4, right=4) -> str:
    """Mask sensitive data for logging"""
    if not value:
        return ""
    if len(value) <= left + right:
        return "*" * len(value)
    return f"{value[:left]}...{value[-right:]}"


def log_security_event(event_type: str, data: dict):
    """Log security-related events"""
    logger.warning(
        "SECURITY_EVENT",
        extra={"event": event_type, "timestamp": now_ist().isoformat(), **(data or {})},
    )

T = TypeVar("T")


async def _run_in_db_thread(func: Callable[[], T]) -> T:
    """Execute blocking database work inside the shared executor."""
    return await run_sync_db_operation(func)


# ============================================================================
# REQUEST/RESPONSE MODELS
# ============================================================================

class CreateOrderRequest(BaseModel):
    client_id: Optional[str] = None
    product_sku: str
    currency: str = "INR"


class VerifyPurchaseRequest(BaseModel):
    client_id: Optional[str] = None  # The app_user_id (customer_id)


# ============================================================================
# ORDER CREATION ENDPOINT
# ============================================================================

@router.post("/subscriptions/create")
async def create_pending_order(
    payload: CreateOrderRequest,
    http_request: Request,
    db: Session = Depends(get_db_session)
) -> Dict[str, Any]:
    """Create pending order before RevenueCat purchase"""

    settings = get_payment_settings()
    client_id = resolve_authenticated_user_id(http_request, payload.client_id)

    def _work() -> Dict[str, Any]:
        try:
            product = (
                db.query(CatalogProduct)
                .filter(
                    CatalogProduct.sku == payload.product_sku,
                    CatalogProduct.active == True
                )
                .first()
            )

            if not product:
                raise HTTPException(status_code=404, detail="Product not found")

            ist_now = now_ist()
            order_id = f"ord_{ist_now.strftime('%Y%m%d')}_{client_id}_{int(ist_now.timestamp())}"

            order = Order(
                id=order_id,
                customer_id=client_id,
                currency=payload.currency,
                provider="google_play",  # Fixed: Use consistent provider
                gross_amount_minor=product.base_amount_minor,
                status="pending"
            )

            db.add(order)
            db.commit()
            db.refresh(order)

            logger.info(
                "Created pending order %s for client %s",
                order_id,
                client_id,
                extra={
                    "order_id": order_id,
                    "customer_id": _mask(client_id),
                    "product_sku": payload.product_sku,
                    "amount_minor": product.base_amount_minor,
                    "provider": "google_play"
                }
            )

            return {
                "order_id": order.id,
                "client_id": client_id,
                "product_sku": payload.product_sku,
                "amount": product.base_amount_minor,
                "currency": payload.currency,
                "status": "pending",
                "api_key": settings.revenuecat_api_key,
                "expires_at": (now_ist() + timedelta(minutes=15)).isoformat(),
                "created_at": order.created_at.isoformat()
            }
        except HTTPException:
            raise
        except Exception as exc:
            logger.error("Error creating order: %s", exc, exc_info=True)
            db.rollback()
            raise

    try:
        return await _run_in_db_thread(_work)
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Error creating order: {exc}") from exc


# ============================================================================
# VERIFY PURCHASE ENDPOINT (Fast-path activation)
# ============================================================================

@router.post("/subscriptions/verify")
async def verify_purchase(
    payload: VerifyPurchaseRequest,
    http_request: Request,
    db: Session = Depends(get_db_session)
) -> Dict[str, Any]:

    settings = get_payment_settings()
    customer_id = resolve_authenticated_user_id(http_request, payload.client_id)

    def _work() -> Dict[str, Any]:
        sync_service = SubscriptionSyncService(db)
        try:
            logger.info("🔍 VERIFY - Checking subscription for customer: %s", customer_id)

            latest_order = (
                db.query(Order)
                .filter(
                    Order.customer_id == customer_id,
                    Order.provider == Provider.google_play.value
                )
                .order_by(Order.created_at.desc())
                .first()
            )

            logger.info("🔐 Calling RevenueCat API...")

            has_active, subscription_data, error_msg = rc_verify_purchase(
                app_user_id=customer_id,
                api_key=settings.revenuecat_api_key
            )

            if not has_active:
                logger.warning(
                    "❌ No active subscription: %s",
                    error_msg,
                    extra={
                        "customer_id": _mask(customer_id),
                        "error": error_msg,
                        "has_order": bool(latest_order)
                    }
                )
                friendly_message = error_msg or "No active Google Play subscription found. Please retry in a few seconds."
                return {
                    "verified": False,
                    "captured": False,
                    "subscription_active": False,
                    "has_premium": False,
                    "message": friendly_message,
                    "order_id": latest_order.id if latest_order else None,
                    "order_status": latest_order.status if latest_order else None,
                    "order_created_at": latest_order.created_at.isoformat() if latest_order and latest_order.created_at else None
                }

            logger.info("✅ Active subscription verified with RevenueCat")

            price_info = subscription_data.get("price") or {}
            price_amount = price_info.get("amount")
            price_currency = price_info.get("currency") or "INR"
            price_minor = None
            if price_amount is not None:
                try:
                    price_minor = int(round(float(price_amount) * 100))
                except (TypeError, ValueError):
                    price_minor = None
            base_product_id = subscription_data.get("product_identifier", "unknown")
            plan_identifier = (
                subscription_data.get("product_plan_identifier")
                or subscription_data.get("base_plan_identifier")
                or subscription_data.get("base_plan_id")
            )
            if plan_identifier and ":" not in base_product_id:
                product_id = f"{base_product_id}:{plan_identifier}"
            else:
                product_id = base_product_id
            rc_purchased_date = subscription_data.get("original_purchase_date")
            rc_expires_date = subscription_data.get("expires_date")
            txn_candidates = [
                subscription_data.get("original_transaction_id"),
                subscription_data.get("original_transaction_identifier"),
                subscription_data.get("original_store_transaction_id"),
                subscription_data.get("original_external_purchase_id"),
                subscription_data.get("transaction_id"),
                subscription_data.get("store_transaction_id"),
            ]
            rc_original_txn_id = next((val for val in txn_candidates if val), None)
            store_transaction_id = subscription_data.get(
                "store_transaction_id",
                f"rc_{customer_id}_{int(now_ist().timestamp())}"
            )
            rc_original_txn_id = rc_original_txn_id or store_transaction_id

            if rc_purchased_date:
                purchased_date = datetime.fromisoformat(rc_purchased_date.replace('Z', '+00:00')).astimezone(IST)
            else:
                purchased_date = now_ist()

            if rc_expires_date:
                expires_date = datetime.fromisoformat(rc_expires_date.replace('Z', '+00:00')).astimezone(IST)
            else:
                expires_date = now_ist() + timedelta(days=30)

            logger.info("   📝 Subscription details from RevenueCat:")
            logger.info("      - product_id: %s", product_id)
            logger.info("      - purchased_date: %s", purchased_date.isoformat())
            logger.info("      - expires_date: %s", expires_date.isoformat())
            logger.info("      - transaction_id: %s", store_transaction_id)
            logger.info("      - rc_original_txn_id: %s", rc_original_txn_id)

            existing_subscription: Optional[Subscription] = None
            if store_transaction_id:
                existing_subscription = lock_query(
                    db.query(Subscription).filter(
                        Subscription.provider == Provider.google_play.value,
                        Subscription.latest_txn_id == store_transaction_id
                    )
                ).first()

            if not existing_subscription and rc_original_txn_id:
                existing_subscription = lock_query(
                    db.query(Subscription).filter(
                        Subscription.provider == Provider.google_play.value,
                        Subscription.rc_original_txn_id == rc_original_txn_id
                    )
                ).first()

            if not existing_subscription:
                possible_products = [product_id]
                if base_product_id and base_product_id != product_id:
                    possible_products.append(base_product_id)
                existing_subscription = lock_query(
                    db.query(Subscription)
                    .filter(
                        Subscription.customer_id == customer_id,
                        Subscription.product_id.in_(possible_products),
                        Subscription.provider == Provider.google_play.value,
                        Subscription.status.in_([
                            SubscriptionStatus.active.value,
                            SubscriptionStatus.renewed.value
                        ])
                    )
                    .order_by(Subscription.created_at.desc())
                ).first()

            already_active = (
                existing_subscription is not None
                and existing_subscription.status in ['active', 'renewed']
            )

            pending_order = lock_query(
                db.query(Order).filter(
                    Order.customer_id == customer_id,
                    Order.status == "pending",
                    Order.provider == Provider.google_play.value
                ).order_by(Order.created_at.desc())
            ).first()

            if pending_order:
                latest_order_local = pending_order
            else:
                latest_order_local = latest_order

            amount_minor = pending_order.gross_amount_minor if pending_order else None
            if amount_minor in (None, 0) and price_minor is not None:
                amount_minor = price_minor

            if pending_order:
                pending_order.status = "paid"
                pending_order.provider_order_id = store_transaction_id
                if price_minor is not None and pending_order.gross_amount_minor in (None, 0):
                    pending_order.gross_amount_minor = price_minor
                if price_currency:
                    pending_order.currency = price_currency
                db.add(pending_order)
                logger.info("✅ Updated order %s to paid", pending_order.id)
            else:
                log_security_event(
                    "ORDER_NOT_FOUND_ON_VERIFY",
                    {
                        "customer_id": _mask(customer_id),
                        "store_transaction_id": _mask(store_transaction_id) if store_transaction_id else None,
                        "has_latest_order": bool(latest_order_local)
                    }
                )
                logger.warning(
                    "⚠️ Pending order not found during RevenueCat verify | customer_id=%s",
                    customer_id,
                )
                amount_minor = 0

            if existing_subscription:
                subscription = existing_subscription
                subscription.product_id = product_id
                subscription.status = "active"
                subscription.rc_original_txn_id = rc_original_txn_id
                subscription.latest_txn_id = store_transaction_id
                subscription.active_from = purchased_date
                subscription.active_until = expires_date
                subscription.auto_renew = True
                db.add(subscription)
                logger.info("✅ Updated existing subscription %s", subscription.id)
            else:
                subscription_id = sync_service.generate_id("sub")
                subscription = Subscription(
                    id=subscription_id,
                    customer_id=customer_id,
                    product_id=product_id,
                    provider=Provider.google_play.value,
                    status="active",
                    rc_original_txn_id=rc_original_txn_id,
                    latest_txn_id=store_transaction_id,
                    active_from=purchased_date,
                    active_until=expires_date,
                    auto_renew=True
                )
                db.add(subscription)
                db.flush()
                logger.info("✅ Created new subscription %s", subscription_id)

            payment_id: Optional[str] = None
            if pending_order:
                payment_id = sync_service.generate_id("pay")
                payment = Payment(
                    id=payment_id,
                    order_id=pending_order.id,
                    customer_id=customer_id,
                    provider=Provider.google_play.value,
                    provider_payment_id=store_transaction_id,
                    amount_minor=amount_minor or 0,
                    currency=price_currency,
                    status="captured",
                    payment_metadata={"source": "verify_endpoint", "verified_at": now_ist().isoformat()}
                )
                db.add(payment)
                logger.info("✅ Created payment %s", payment_id)

            try:
                free_trial = db.query(FreeTrial).filter(FreeTrial.client_id == int(customer_id)).first()
                if free_trial and free_trial.status != "expired":
                    free_trial.status = "expired"
                    db.add(free_trial)
                    logger.info("✅ Free trial expired for client_id: %s", customer_id)
            except Exception as ft_error:
                logger.warning("⚠️ Failed to update free_trial status: %s", ft_error)

            db.commit()

            logger.info(
                "✅ VERIFY - Successfully verified and activated premium for %s",
                customer_id,
                extra={
                    "customer_id": _mask(customer_id),
                    "subscription_id": subscription.id,
                    "payment_id": payment_id,
                    "product_id": product_id,
                    "amount_minor": amount_minor,
                    "active_until": subscription.active_until.isoformat() if subscription.active_until else None,
                    "was_already_active": already_active
                }
            )

            response_message = "Purchase verified - Premium activated"
            if already_active:
                response_message = "Purchase already verified"

            return {
                "verified": True,
                "captured": True,
                "subscription_active": True,
                "has_premium": True,
                "message": response_message,
                "subscription_id": subscription.id,
                "payment_id": payment_id,
                "order_id": pending_order.id if pending_order else (latest_order_local.id if latest_order_local else None),
                "active_from": subscription.active_from.isoformat(),
                "active_until": subscription.active_until.isoformat(),
                "auto_renew": True
            }
        except HTTPException:
            raise
        except RevenueCatAPIError as rc_error:
            logger.error("RevenueCat API error: %s", rc_error, exc_info=True)
            db.rollback()
            raise HTTPException(status_code=502, detail=str(rc_error))
        except Exception as exc:
            logger.error("Error verifying purchase: %s", exc, exc_info=True)
            db.rollback()
            raise

    try:
        return await _run_in_db_thread(_work)
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Error verifying purchase: {exc}") from exc




def generate_event_id(event: dict) -> str:
    """Generate unique event ID for idempotency"""
    customer_id = event.get("app_user_id", "unknown")
    event_type = event.get("type", "unknown")

    # Use event ID if provided
    if event.get("id"):
        return event["id"]

    # Generate based on event type and details
    if event_type == "INITIAL_PURCHASE":
        transaction_id = event.get("transaction_id", "")
        return f"{event_type}_{customer_id}_{transaction_id}"
    elif event_type == "RENEWAL":
        transaction_id = event.get("transaction_id", "")
        purchased_at = event.get("purchased_at_ms", 0)
        return f"{event_type}_{customer_id}_{transaction_id}_{purchased_at}"
    elif event_type == "CANCELLATION":
        product_id = event.get("product_id", "")
        cancelled_at = event.get("cancelled_at_ms", datetime.now().timestamp() * 1000)
        return f"{event_type}_{customer_id}_{product_id}_{int(cancelled_at)}"
    elif event_type == "EXPIRATION":
        product_id = event.get("product_id", "")
        expiration_at = event.get("expiration_at_ms", 0)
        return f"{event_type}_{customer_id}_{product_id}_{expiration_at}"
    else:
        timestamp = int(datetime.now().timestamp() * 1000)
        return f"{event_type}_{customer_id}_{timestamp}"


@router.post("/webhooks")
async def webhook_revenuecat(
    request: Request,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db_session)
):
    """Handle RevenueCat subscription webhooks with enterprise-level sync"""
    settings = get_payment_settings()

    try:
        body = await request.body()
        signature = request.headers.get("Authorization", "").replace("Bearer ", "")

        logger.info("revenuecat.webhook.received | signature_prefix=%s", _mask(signature, left=8, right=0))

        if signature != settings.revenuecat_webhook_secret:
            log_security_event(
                "INVALID_WEBHOOK_SIGNATURE",
                {"signature_prefix": _mask(signature, left=8, right=0), "source": "revenuecat"}
            )
            logger.warning("Invalid webhook authorization")
            raise HTTPException(status_code=401, detail="Invalid authorization")

        payload = json.loads(body.decode())
        event = payload.get("event", {})

        logger.debug("revenuecat.webhook.payload | event=%s", json.dumps(event))

        def _work() -> Dict[str, Any]:
            sync_service = SubscriptionSyncService(db)
            try:
                customer_id = event.get("app_user_id")
                event_type = event.get("type")

                logger.info(
                    "revenuecat.webhook.event | customer_id=%s event_type=%s",
                    customer_id,
                    event_type,
                )

                purchased_at_ms = event.get("purchased_at_ms")
                expiration_at_ms = event.get("expiration_at_ms")

                if purchased_at_ms:
                    purchased_dt = datetime.fromtimestamp(purchased_at_ms / 1000, tz=IST)
                else:
                    purchased_dt = None
                    logger.warning(
                        "revenuecat.webhook.missing_purchased_at | customer_id=%s event_id=%s",
                        customer_id,
                        event.get("id"),
                    )

                if expiration_at_ms:
                    expiration_dt = datetime.fromtimestamp(expiration_at_ms / 1000, tz=IST)
                else:
                    expiration_dt = None
                    logger.warning(
                        "revenuecat.webhook.missing_expiration | customer_id=%s event_id=%s",
                        customer_id,
                        event.get("id"),
                    )

                if not customer_id:
                    log_security_event(
                        "WEBHOOK_MISSING_CUSTOMER_ID",
                        {
                            "event_type": event_type,
                            "event_id": event.get("id"),
                            "has_product_id": bool(event.get("product_id"))
                        }
                    )
                    logger.warning("❌ Missing customer_id in webhook event")
                    return {"status": "ignored", "reason": "missing_customer_id"}

                event_id = generate_event_id(event)

                should_process, existing_log = sync_service.check_idempotency(
                    event_id, event_type, allow_retry_on_failure=True
                )

                if not should_process:
                    logger.info("Event %s already processed or still processing", event_id)
                    return {
                        "status": "already_processed",
                        "event_id": event_id,
                        "processing_status": existing_log.status if existing_log else None
                    }

                if existing_log:
                    processing_log = existing_log
                else:
                    processing_log = WebhookProcessingLog(
                        id=sync_service.generate_id("whl"),
                        event_id=event_id,
                        event_type=event_type,
                        customer_id=customer_id,
                        status="processing",
                        started_at=now_ist(),
                        raw_event_data=json.dumps(event),
                        is_recovery_event=event.get("_is_recovery", False)
                    )
                    db.add(processing_log)
                    db.flush()

                result: Dict[str, Any] = {}

                logger.info("🚀 About to call sync_service.process_%s with event data", (event_type or "").lower())

                if event_type == "INITIAL_PURCHASE":
                    logger.info("📝 Processing INITIAL_PURCHASE...")
                    result = sync_service.process_initial_purchase(event, processing_log)
                    logger.info("📊 INITIAL_PURCHASE result: %s", json.dumps(result, default=str, indent=2))
                elif event_type == "RENEWAL":
                    logger.info("🔄 Processing RENEWAL...")
                    result = sync_service.process_renewal(event, processing_log)
                    logger.info("📊 RENEWAL result: %s", json.dumps(result, default=str, indent=2))
                elif event_type == "CANCELLATION":
                    logger.info("❌ Processing CANCELLATION...")
                    result = sync_service.process_cancellation(event, processing_log)
                    logger.info("📊 CANCELLATION result: %s", json.dumps(result, default=str, indent=2))
                elif event_type == "EXPIRATION":
                    logger.info("⏰ Processing EXPIRATION...")
                    result = sync_service.process_expiration(event, processing_log)
                    logger.info("📊 EXPIRATION result: %s", json.dumps(result, default=str, indent=2))
                elif event_type == "BILLING_ISSUES":
                    logger.info("💳 Processing BILLING_ISSUES...")
                    result = handle_billing_issues(event, db, processing_log)
                    logger.info("📊 BILLING_ISSUES result: %s", json.dumps(result, default=str, indent=2))
                else:
                    logger.warning("⚠️ Unhandled event type: %s", event_type)
                    processing_log.status = "ignored"
                    processing_log.completed_at = now_ist()
                    processing_log.result_summary = f"Unhandled event type: {event_type}"
                    db.commit()
                    return {"status": "ignored", "reason": f"unhandled_event_type: {event_type}"}

                if result.get("success") and result.get("subscription_id"):
                    sub = (
                        db.query(Subscription)
                        .filter(Subscription.id == result["subscription_id"])
                        .first()
                    )
                    if sub:
                        logger.info("🗄️ DATABASE CHECK - Subscription after processing:")
                        logger.info("   - Subscription ID: %s", sub.id)
                        logger.info("   - active_from: %s (type: %s)", sub.active_from, type(sub.active_from))
                        logger.info("   - active_until: %s (type: %s)", sub.active_until, type(sub.active_until))
                        logger.info("   - status: %s", sub.status)
                        logger.info("   - product_id: %s", sub.product_id)

                if result.get("success"):
                    logger.info(
                        "✅ Successfully processed %s for %s",
                        event_type,
                        customer_id,
                        extra={
                            "event_type": event_type,
                            "event_id": event_id,
                            "customer_id": _mask(customer_id),
                            "subscription_id": result.get("subscription_id")
                        }
                    )
                    return {
                        "status": "processed",
                        "event_type": event_type,
                        "event_id": event_id,
                        "result": result
                    }

                log_security_event(
                    "WEBHOOK_PROCESSING_FAILED",
                    {
                        "event_type": event_type,
                        "event_id": event_id,
                        "customer_id": _mask(customer_id),
                        "error": result.get("error")
                    }
                )
                logger.error("❌ Failed to process %s: %s", event_type, result.get("error"))
                raise HTTPException(
                    status_code=500,
                    detail=f"Processing failed: {result.get('error')}"
                )
            except HTTPException:
                raise
            except Exception as exc:
                logger.error("Webhook processing error: %s", exc, exc_info=True)
                db.rollback()
                raise

        return await _run_in_db_thread(_work)

    except json.JSONDecodeError as json_err:
        log_security_event(
            "WEBHOOK_INVALID_JSON",
            {"error": str(json_err), "source": "revenuecat"}
        )
        logger.error("Invalid JSON in webhook payload: %s", json_err)
        raise HTTPException(status_code=400, detail="Invalid JSON")
    except HTTPException:
        raise
    except Exception as exc:
        log_security_event(
            "WEBHOOK_PROCESSING_EXCEPTION",
            {"error": str(exc), "error_type": type(exc).__name__}
        )
        logger.error("Webhook processing error: %s", exc, exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error") from exc


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def handle_billing_issues(event: dict, db: Session, processing_log: WebhookProcessingLog) -> dict:
    """Handle billing issues event"""
    customer_id = event.get("app_user_id")
    product_id = event.get("product_id")

    if not product_id:
        error_msg = "No product_id found in billing issues event"
        logger.error(error_msg)
        processing_log.status = "failed"
        processing_log.error_message = error_msg
        processing_log.completed_at = now_ist()
        db.commit()
        return {"success": False, "error": error_msg}

    logger.warning(f"Billing issues for user {customer_id}, product {product_id}")

    try:
        # Find subscription and mark as having billing issues
        subscription = db.query(Subscription).filter(
            Subscription.customer_id == customer_id,
            Subscription.product_id == product_id,
            Subscription.provider == Provider.google_play.value
        ).first()

        if subscription:
            # Keep subscription active but flag billing issue in metadata
            if not subscription.metadata:
                subscription.metadata = {}
            subscription.metadata["billing_issue"] = True
            subscription.metadata["billing_issue_date"] = now_ist().isoformat()

            processing_log.status = "completed"
            processing_log.completed_at = now_ist()
            processing_log.result_summary = f"Marked billing issue for subscription {subscription.id}"

            db.commit()
            logger.info(f"Marked billing issue for subscription {subscription.id}")

            return {"success": True, "subscription_id": subscription.id}
        else:
            processing_log.status = "completed"
            processing_log.completed_at = now_ist()
            processing_log.result_summary = "No subscription found for billing issue"

            db.commit()
            return {"success": True, "message": "No subscription found"}

    except Exception as e:
        logger.error(f"Error handling billing issues: {str(e)}")
        processing_log.status = "failed"
        processing_log.error_message = str(e)
        processing_log.completed_at = now_ist()
        db.commit()
        return {"success": False, "error": str(e)}
