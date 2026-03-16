"""
Migration script to fix idempotency issues and sync existing data
"""

import json
import logging
from datetime import datetime, timezone, timedelta
from sqlalchemy.orm import Session
from sqlalchemy import and_, or_

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("fix_idempotency")

# Indian Standard Time (IST) timezone
IST = timezone(timedelta(hours=5, minutes=30))

def now_ist():
    return datetime.now(IST)


def fix_webhook_processing_logs(db: Session):
    """
    Fix webhook processing logs that might be causing issues
    """
    logger.info("Starting webhook processing logs fix...")
    
    from ..models.webhook_logs import WebhookProcessingLog
    
    # Find logs that are stuck in processing state for more than 10 minutes
    cutoff_time = now_ist() - timedelta(minutes=10)
    stuck_logs = db.query(WebhookProcessingLog).filter(
        WebhookProcessingLog.status == "processing",
        WebhookProcessingLog.started_at <= cutoff_time
    ).all()
    
    logger.info(f"Found {len(stuck_logs)} stuck webhook processing logs")
    
    for log in stuck_logs:
        # Reset to allow reprocessing
        log.status = "failed"
        log.completed_at = now_ist()
        log.error_message = "Timeout - reset for reprocessing"
        logger.info(f"Reset stuck log: {log.event_id}")
    
    # Find duplicate completed logs (same event_id and event_type)
    from sqlalchemy import func
    duplicate_groups = db.query(
        WebhookProcessingLog.event_id,
        WebhookProcessingLog.event_type,
        func.count(WebhookProcessingLog.id).label('count')
    ).filter(
        WebhookProcessingLog.status == "completed"
    ).group_by(
        WebhookProcessingLog.event_id,
        WebhookProcessingLog.event_type
    ).having(func.count(WebhookProcessingLog.id) > 1).all()
    
    logger.info(f"Found {len(duplicate_groups)} duplicate event groups")
    
    for event_id, event_type, count in duplicate_groups:
        # Keep the latest one, mark others as duplicate
        logs = db.query(WebhookProcessingLog).filter(
            WebhookProcessingLog.event_id == event_id,
            WebhookProcessingLog.event_type == event_type,
            WebhookProcessingLog.status == "completed"
        ).order_by(WebhookProcessingLog.completed_at.desc()).all()
        
        # Mark all but the first as duplicates
        for log in logs[1:]:
            log.status = "duplicate"
            log.error_message = "Duplicate processing - marked for cleanup"
            logger.info(f"Marked duplicate log: {log.event_id}")
    
    db.commit()
    logger.info("Webhook processing logs fix completed")


def sync_entitlements_with_subscriptions(db: Session):
    """
    Ensure all active subscriptions have corresponding active entitlements
    """
    logger.info("Starting entitlement sync...")
    
    from ..models.subscriptions import Subscription
    from ..models.entitlements import Entitlement
    from ..models.enums import SubscriptionStatus, StatusEnt, EntType
    
    # Get all active subscriptions
    active_subscriptions = db.query(Subscription).filter(
        Subscription.status == SubscriptionStatus.active.value,
        or_(
            Subscription.active_until.is_(None),
            Subscription.active_until > now_ist()
        )
    ).all()
    
    logger.info(f"Found {len(active_subscriptions)} active subscriptions")
    
    created_entitlements = 0
    updated_entitlements = 0
    
    for subscription in active_subscriptions:
        # Check if entitlement exists
        entitlement = db.query(Entitlement).filter(
            Entitlement.customer_id == subscription.customer_id,
            Entitlement.entitlement_type == EntType.app.value
        ).first()
        
        if not entitlement:
            # Create missing entitlement
            entitlement = Entitlement(
                id=f"ent_{int(now_ist().timestamp())}_{subscription.customer_id}",
                customer_id=subscription.customer_id,
                entitlement_type=EntType.app.value,
                status=StatusEnt.pending.value,  # Use pending as active equivalent
                active_from=subscription.active_from or now_ist(),
                active_until=subscription.active_until,
                metadata={
                    "source": "migration_sync",
                    "subscription_id": subscription.id,
                    "created_at": now_ist().isoformat()
                }
            )
            db.add(entitlement)
            created_entitlements += 1
            logger.info(f"Created entitlement for customer {subscription.customer_id}")
            
        else:
            # Update existing entitlement if needed
            needs_update = False
            
            if entitlement.status != StatusEnt.pending.value:
                entitlement.status = StatusEnt.pending.value
                needs_update = True
            
            if entitlement.active_until != subscription.active_until:
                entitlement.active_until = subscription.active_until
                needs_update = True
            
            if needs_update:
                updated_entitlements += 1
                logger.info(f"Updated entitlement for customer {subscription.customer_id}")
    
    db.commit()
    logger.info(f"Entitlement sync completed: {created_entitlements} created, {updated_entitlements} updated")


def fix_expired_subscriptions_and_entitlements(db: Session):
    """
    Properly mark expired subscriptions and entitlements
    """
    logger.info("Starting expired subscription/entitlement fix...")
    
    from ..models.subscriptions import Subscription
    from ..models.entitlements import Entitlement
    from ..models.enums import SubscriptionStatus, StatusEnt, EntType
    
    current_time = now_ist()
    
    # Find subscriptions that should be expired
    expired_subscriptions = db.query(Subscription).filter(
        Subscription.status == SubscriptionStatus.active.value,
        Subscription.active_until <= current_time
    ).all()
    
    logger.info(f"Found {len(expired_subscriptions)} subscriptions to expire")
    
    expired_count = 0
    
    for subscription in expired_subscriptions:
        # Update subscription status
        subscription.status = SubscriptionStatus.expired.value
        subscription.auto_renew = False
        
        # Update corresponding entitlement
        entitlement = db.query(Entitlement).filter(
            Entitlement.customer_id == subscription.customer_id,
            Entitlement.entitlement_type == EntType.app.value
        ).first()
        
        if entitlement and entitlement.status != StatusEnt.expired.value:
            entitlement.status = StatusEnt.expired.value
        
        expired_count += 1
        logger.info(f"Expired subscription and entitlement for customer {subscription.customer_id}")
    
    db.commit()
    logger.info(f"Expired subscription fix completed: {expired_count} subscriptions expired")


def cleanup_invalid_webhook_logs(db: Session):
    """
    Cleanup webhook logs with invalid or missing data
    """
    logger.info("Starting webhook log cleanup...")
    
    from ..models.webhook_logs import WebhookProcessingLog
    
    # Find logs with missing customer_id or event_type
    invalid_logs = db.query(WebhookProcessingLog).filter(
        or_(
            WebhookProcessingLog.customer_id.is_(None),
            WebhookProcessingLog.customer_id == "",
            WebhookProcessingLog.event_type.is_(None),
            WebhookProcessingLog.event_type == ""
        )
    ).all()
    
    logger.info(f"Found {len(invalid_logs)} invalid webhook logs")
    
    for log in invalid_logs:
        logger.info(f"Deleting invalid log: {log.id}")
        db.delete(log)
    
    # Find very old failed logs (older than 7 days)
    old_failed_cutoff = now_ist() - timedelta(days=7)
    old_failed_logs = db.query(WebhookProcessingLog).filter(
        WebhookProcessingLog.status == "failed",
        WebhookProcessingLog.started_at <= old_failed_cutoff
    ).all()
    
    logger.info(f"Found {len(old_failed_logs)} old failed webhook logs to delete")
    
    for log in old_failed_logs:
        db.delete(log)
    
    db.commit()
    logger.info("Webhook log cleanup completed")


def run_migration(db: Session):
    """
    Run all migration steps
    """
    logger.info("="*50)
    logger.info("Starting idempotency fix migration")
    logger.info("="*50)
    
    try:
        # Step 1: Fix webhook processing logs
        fix_webhook_processing_logs(db)
        
        # Step 2: Sync entitlements with subscriptions
        sync_entitlements_with_subscriptions(db)
        
        # Step 3: Fix expired subscriptions and entitlements
        fix_expired_subscriptions_and_entitlements(db)
        
        # Step 4: Cleanup invalid webhook logs
        cleanup_invalid_webhook_logs(db)
        
        logger.info("="*50)
        logger.info("Migration completed successfully!")
        logger.info("="*50)
        
        return {
            "success": True,
            "message": "Migration completed successfully",
            "timestamp": now_ist().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Migration failed: {str(e)}")
        db.rollback()
        raise


if __name__ == "__main__":
    # This would be run manually or via a management command
    from ..config.database import get_db_session
    
    db = next(get_db_session())
    try:
        result = run_migration(db)
        print(json.dumps(result, indent=2))
    finally:
        db.close()