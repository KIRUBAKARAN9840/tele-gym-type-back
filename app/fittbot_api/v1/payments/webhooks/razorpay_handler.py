"""Razorpay webhook handlers for both payouts and subscriptions"""

import json
import logging
from datetime import datetime, timezone, timedelta
from fastapi import APIRouter, Request, Depends, HTTPException, BackgroundTasks
from sqlalchemy.orm import Session

from ..config.database import get_db_session
from ..config.settings import get_payment_settings
from ..models import PayoutBatch, PayoutEvent, PayoutLine
from ..models.webhook_logs import WebhookProcessingLog
from ..models.enums import StatusPayoutLine
from ..schemas.webhooks import RazorpayXWebhook
from ..utils.webhook_verifier import verify_razorpay_signature
from ..services.razorpay_subscription_sync_service import RazorpaySubscriptionSyncService

logger = logging.getLogger("payments.razorpay")
router = APIRouter(prefix="/webhooks", tags=["Webhooks"])

# Indian Standard Time (IST) timezone
IST = timezone(timedelta(hours=5, minutes=30))

def now_ist():
    """Get current time in IST"""
    return datetime.now(IST)


def generate_razorpay_event_id(event: dict) -> str:
    """Generate unique event ID for Razorpay webhook idempotency"""
    event_type = event.get("event", "unknown")
    
    # Use webhook ID if provided
    if event.get("id"):
        return event["id"]
    
    # Use subscription or payment entity ID if available
    payload = event.get("payload", {})
    entity_data = None
    
    if payload.get("subscription"):
        entity_data = payload["subscription"].get("entity", {})
        entity_id = entity_data.get("id", "")
        return f"{event_type}_{entity_id}"
    elif payload.get("payment"):
        entity_data = payload["payment"].get("entity", {})
        entity_id = entity_data.get("id", "")
        return f"{event_type}_{entity_id}"
    
    # Fallback to timestamp-based ID
    timestamp = int(datetime.now().timestamp() * 1000)
    return f"{event_type}_{timestamp}"


@router.post("/razorpay")
async def webhook_razorpay(
    request: Request,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db_session)
):
    """Handle Razorpay subscription webhooks with enterprise-level sync"""
    settings = get_payment_settings()
    sync_service = RazorpaySubscriptionSyncService(db)
    
    try:
        # Get raw body and signature
        body = await request.body()
        signature = request.headers.get("X-Razorpay-Signature", "")
        
        # Log for debugging
        logger.info(f"Razorpay webhook received with signature: {signature[:10]}...")
        
        # Verify signature
        if not verify_razorpay_signature(body.decode(), signature, settings.razorpay_webhook_secret):
            logger.warning(f"Invalid Razorpay webhook signature")
            raise HTTPException(status_code=401, detail="Invalid signature")
        
        # Parse payload
        payload = json.loads(body.decode())
        event_type = payload.get("event")
        
        # Allow both subscription and payment events, but ignore system events
        if event_type.startswith("payment.downtime") or event_type in ["payment.dispute", "payment.dispute.created"]:
            logger.info(f"Ignoring system event: {event_type}")
            return {"status": "ignored", "reason": "system_event", "event_type": event_type}

        # Extract customer_id based on event type
        def _safe(fn, default=None):
            try:
                return fn()
            except Exception:
                return default

        customer_id = None

        # For subscription events
        if event_type.startswith("subscription."):
            customer_id = _safe(lambda: payload["payload"]["subscription"]["entity"]["notes"].get("customer_id"))

        # For payment events - these don't have customer_id in the payload directly
        # We need to handle them but they won't have customer_id available
        elif event_type.startswith("payment."):
            # Try to get customer_id from subscription context if it exists in the same payload
            customer_id = _safe(lambda: payload["payload"]["subscription"]["entity"]["notes"].get("customer_id"))

            # If no subscription context, check payment notes
            if not customer_id:
                customer_id = _safe(lambda: payload["payload"]["payment"]["entity"]["notes"].get("customer_id"))

            # For payment events without customer_id, we'll let them proceed but mark as unknown
            if not customer_id:
                logger.info(f"Payment event {event_type} has no customer_id, processing anyway")
                customer_id = "UNKNOWN_PAYMENT"

        logger.info(f"Processing Razorpay {event_type} for customer {customer_id}")

        # Skip processing only if truly missing customer_id, not for payment events with UNKNOWN_PAYMENT
        if not customer_id:
            # Log this as a processed event to prevent infinite retries
            event_id = generate_razorpay_event_id(payload)

            # Create a log entry to mark this as processed (ignored)
            processing_log = WebhookProcessingLog(
                id=sync_service.generate_id("whl"),
                event_id=event_id,
                event_type=event_type,
                customer_id="MISSING",  # Use placeholder instead of None
                status="ignored",
                started_at=now_ist(),
                completed_at=now_ist(),
                raw_event_data=json.dumps(payload),
                error_message="Missing customer_id in webhook payload",
                result_summary="Ignored due to missing customer_id"
            )
            db.add(processing_log)
            db.commit()

            logger.warning(f"Ignored webhook {event_id} due to missing customer_id")
            return {"status": "ignored", "reason": "missing_customer_id", "event_id": event_id}

        # Add customer_id to payload root level so service methods can access it
        payload["customer_id"] = customer_id
        payload["app_user_id"] = customer_id  # For compatibility

        # Generate unique event ID for idempotency
        event_id = generate_razorpay_event_id(payload)

        # Check idempotency with retry logic
        should_process, existing_log = sync_service.check_idempotency(
            event_id, event_type, allow_retry_on_failure=True
        )

        if not should_process:
            logger.info(f"Event {event_id} already processed or still processing")
            return {
                "status": "already_processed",
                "event_id": event_id,
                "processing_status": existing_log.status if existing_log else None
            }

        # Create or update processing log
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
                raw_event_data=json.dumps(payload),
                is_recovery_event=payload.get("_is_recovery", False)
            )
            db.add(processing_log)
            db.flush()

        # Process event based on type using sync service
        result = {}

        if event_type == "subscription.activated":
            # Map to initial purchase
            result = sync_service.process_initial_purchase(payload, processing_log)
        elif event_type == "subscription.charged":
            # Map to renewal
            result = sync_service.process_renewal(payload, processing_log)
        elif event_type == "subscription.cancelled":
            # Map to cancellation
            result = sync_service.process_cancellation(payload, processing_log)
        elif event_type == "subscription.completed":
            # Map to expiration
            result = sync_service.process_expiration(payload, processing_log)
        elif event_type == "subscription.paused":
            # Handle subscription pause (similar to cancellation but temporary)
            result = sync_service.process_cancellation(payload, processing_log)
        elif event_type == "subscription.resumed":
            # Handle subscription resume (similar to renewal)
            result = sync_service.process_renewal(payload, processing_log)
        elif event_type == "subscription.authenticated":
            # Mark as processed but don't do anything - just authentication confirmation
            processing_log.status = "completed"
            processing_log.completed_at = now_ist()
            processing_log.result_summary = "Subscription authenticated - no action needed"
            db.commit()
            return {"status": "processed", "event_type": event_type, "message": "subscription authenticated"}
        elif event_type == "payment.authorized":
            # Payment authorized - just log it as processed since verify endpoint handles the actual processing
            processing_log.status = "completed"
            processing_log.completed_at = now_ist()
            processing_log.result_summary = "Payment authorized - processed by verify endpoint"
            db.commit()
            return {"status": "processed", "event_type": event_type, "message": "Payment authorized"}
        elif event_type == "payment.captured":
            # Payment captured - just log it as processed since verify endpoint handles the actual processing
            processing_log.status = "completed"
            processing_log.completed_at = now_ist()
            processing_log.result_summary = "Payment captured - processed by verify endpoint"
            db.commit()
            return {"status": "processed", "event_type": event_type, "message": "Payment captured"}
        else:
            logger.warning(f"Unhandled Razorpay event type: {event_type}")
            processing_log.status = "ignored"
            processing_log.completed_at = now_ist()
            processing_log.result_summary = f"Unhandled event type: {event_type}"
            db.commit()
            return {"status": "ignored", "reason": f"unhandled_event_type: {event_type}"}
        
        # Check result and update processing log
        if result.get("success"):
            logger.info(f"✅ Successfully processed {event_type} for {customer_id}")
            return {
                "status": "processed",
                "event_type": event_type,
                "event_id": event_id,
                "result": result
            }
        else:
            logger.error(f"❌ Failed to process {event_type}: {result.get('error')}")
            raise HTTPException(
                status_code=500,
                detail=f"Processing failed: {result.get('error')}"
            )
    
    except json.JSONDecodeError:
        logger.error("Invalid JSON in Razorpay webhook payload")
        raise HTTPException(status_code=400, detail="Invalid JSON")
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Razorpay webhook processing error: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/razorpayx")
async def webhook_razorpayx(
    request: Request,
    db: Session = Depends(get_db_session)
):
    """Handle RazorpayX payout webhooks"""
    settings = get_payment_settings()
    
    # Get raw body and signature
    body = await request.body()
    signature = request.headers.get("X-Razorpay-Signature", "")
    
    # Verify signature
    if not verify_razorpay_signature(body.decode(), signature, settings.razorpayx_webhook_secret):
        raise HTTPException(status_code=401, detail="Invalid signature")
    
    try:
        # Parse payload
        payload = json.loads(body.decode())
        event = payload.get("event")
        data = payload.get("payload", {})
        
        # Process based on event type
        if event == "payout.processed":
            await _handle_payout_processed(db, data)
        elif event == "payout.failed":
            await _handle_payout_failed(db, data)
        else:
            # Log unknown event type but don't fail
            pass
        
        return {"status": "processed"}
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Webhook processing failed: {str(e)}")


async def _handle_payout_processed(db: Session, data: dict):
    """Handle payout processed event"""
    batch_id = data.get("batch_id")
    if not batch_id:
        return

    batch = db.get(PayoutBatch, batch_id)
    if not batch:
        return
    
    # Update batch status
    batch.status = "paid"
    batch.provider_ref = data.get("provider_ref")
    batch.fee_actual_minor = int(data.get("fee_minor", 0))
    batch.tax_on_fee_minor = int(data.get("tax_minor", 0))
    
    # Create payout event
    event = PayoutEvent(
        id=f"pev_{batch_id}",
        payout_batch_id=batch_id,
        provider="razorpayx",
        event_type="processed",
        provider_ref=batch.provider_ref,
        event_time=data.get("processed_at")
    )
    db.add(event)
    
    # Update payout lines and allocate fees
    lines = db.query(PayoutLine).filter(PayoutLine.batch_id == batch_id).all()
    total_net = sum(line.net_amount_minor for line in lines) or 1
    total_fees = batch.fee_actual_minor + batch.tax_on_fee_minor
    
    for line in lines:
        # Allocate fees proportionally
        allocated_fee = round(total_fees * (line.net_amount_minor / total_net))
        line.payout_fee_allocated_minor = allocated_fee
        line.status = StatusPayoutLine.paid
    
    db.commit()


async def _handle_payout_failed(db: Session, data: dict):
    """Handle payout failed event"""
    batch_id = data.get("batch_id")
    if not batch_id:
        return

    batch = db.get(PayoutBatch, batch_id)
    if not batch:
        return
    
    # Update batch status
    batch.status = "failed"
    
    # Revert payout lines to pending
    lines = db.query(PayoutLine).filter(PayoutLine.batch_id == batch_id).all()
    for line in lines:
        line.status = StatusPayoutLine.pending
        line.batch_id = None

    db.commit()