"""
RevenueCat webhook handler with enterprise-level idempotency and sync
"""

import json
import logging
from datetime import datetime, timezone, timedelta
from fastapi import APIRouter, Request, Depends, HTTPException, BackgroundTasks
from sqlalchemy.orm import Session

# Indian Standard Time (IST) timezone
IST = timezone(timedelta(hours=5, minutes=30))

def now_ist():
    """Get current time in IST"""
    return datetime.now(IST)

from ..config.database import get_db_session
from ..config.settings import get_payment_settings
from ..models.webhook_logs import WebhookProcessingLog
from ..services.subscription_sync_service import SubscriptionSyncService

logger = logging.getLogger("payments.revenuecat")
router = APIRouter(prefix="/payments/webhooks", tags=["RevenueCat Webhooks"])


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


@router.post("/revenuecat")
async def webhook_revenuecat(
    request: Request,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db_session)
):
    """Handle RevenueCat subscription webhooks with enterprise-level sync"""
    settings = get_payment_settings()
    sync_service = SubscriptionSyncService(db)
    
    try:
        # Get raw body and signature
        body = await request.body()
        signature = request.headers.get("Authorization", "").replace("Bearer ", "")
        
        # Log for debugging
        logger.info(f"Webhook received with signature: {signature[:10]}...")
        
        # Verify authorization token
        if signature != settings.revenuecat_webhook_secret:
            logger.warning(f"Invalid webhook authorization")
            raise HTTPException(status_code=401, detail="Invalid authorization")
        
        # Parse payload
        payload = json.loads(body.decode())
        event = payload.get("event", {})
        
        # Extract event details
        customer_id = event.get("app_user_id")
        event_type = event.get("type")
        
        logger.info(f"Processing {event_type} for customer {customer_id}")
        
        if not customer_id:
            return {"status": "ignored", "reason": "missing_customer_id"}
        
        # Generate unique event ID for idempotency
        event_id = generate_event_id(event)
        
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
                raw_event_data=json.dumps(event),
                is_recovery_event=event.get("_is_recovery", False)
            )
            db.add(processing_log)
            db.flush()
        
        # Process event based on type using sync service
        result = {}
        
        if event_type == "INITIAL_PURCHASE":
            result = sync_service.process_initial_purchase(event, processing_log)
        elif event_type == "RENEWAL":
            result = sync_service.process_renewal(event, processing_log)
        elif event_type == "CANCELLATION":
            result = sync_service.process_cancellation(event, processing_log)
        elif event_type == "EXPIRATION":
            result = sync_service.process_expiration(event, processing_log)
        elif event_type == "BILLING_ISSUES":
            # Handle billing issues separately
            result = await handle_billing_issues(event, db, processing_log)
        else:
            logger.warning(f"Unhandled event type: {event_type}")
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
        logger.error("Invalid JSON in webhook payload")
        raise HTTPException(status_code=400, detail="Invalid JSON")
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Webhook processing error: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


async def handle_billing_issues(event: dict, db: Session, processing_log: WebhookProcessingLog) -> dict:
    """Handle billing issues event"""
    from ..models.subscriptions import Subscription
    from ..models.enums import Provider
    
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