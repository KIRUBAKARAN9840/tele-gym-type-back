"""
Webhook Recovery Service - Enterprise-level recovery for stuck/failed webhooks
"""

import json
import logging
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any, Optional
from sqlalchemy.orm import Session
from sqlalchemy import and_, or_

from ..models.webhook_logs import WebhookProcessingLog
from ..models.entitlements import Entitlement
from ..models.subscriptions import Subscription
from ..models.orders import Order
from ..models.enums import StatusEnt, SubscriptionStatus, Provider
from .subscription_sync_service import SubscriptionSyncService

logger = logging.getLogger("payments.webhook_recovery")

# Indian Standard Time (IST) timezone
IST = timezone(timedelta(hours=5, minutes=30))

def now_ist():
    """Get current time in IST"""
    return datetime.now(IST)


class WebhookRecoveryService:
    """
    Service to recover from webhook processing failures and sync issues
    """
    
    def __init__(self, db: Session):
        self.db = db
        self.sync_service = SubscriptionSyncService(db)
        self.logger = logger
    
    def recover_stuck_webhooks(self, max_age_minutes: int = 10) -> Dict[str, Any]:
        """
        Recover webhooks that have been processing for too long
        """
        cutoff_time = now_ist() - timedelta(minutes=max_age_minutes)
        
        stuck_webhooks = self.db.query(WebhookProcessingLog).filter(
            WebhookProcessingLog.status == "processing",
            WebhookProcessingLog.started_at <= cutoff_time
        ).all()
        
        recovery_results = {
            "stuck_webhooks_found": len(stuck_webhooks),
            "recovered": 0,
            "failed_recovery": 0,
            "details": []
        }
        
        for webhook in stuck_webhooks:
            try:
                self.logger.info(f"Attempting to recover stuck webhook {webhook.event_id}")
                
                # Parse the original event data
                event_data = json.loads(webhook.raw_event_data)
                
                # Reset the webhook status to allow reprocessing
                webhook.status = "processing"
                webhook.started_at = now_ist()
                webhook.retry_count += 1
                
                # Re-process using sync service
                result = self.reprocess_webhook(webhook, event_data)
                
                if result.get("success"):
                    recovery_results["recovered"] += 1
                    recovery_results["details"].append({
                        "webhook_id": webhook.id,
                        "event_id": webhook.event_id,
                        "event_type": webhook.event_type,
                        "status": "recovered",
                        "customer_id": webhook.customer_id
                    })
                else:
                    recovery_results["failed_recovery"] += 1
                    recovery_results["details"].append({
                        "webhook_id": webhook.id,
                        "event_id": webhook.event_id,
                        "event_type": webhook.event_type,
                        "status": "failed_recovery",
                        "error": result.get("error"),
                        "customer_id": webhook.customer_id
                    })
                
            except Exception as e:
                self.logger.error(f"Error recovering webhook {webhook.event_id}: {str(e)}")
                recovery_results["failed_recovery"] += 1
                recovery_results["details"].append({
                    "webhook_id": webhook.id,
                    "event_id": webhook.event_id,
                    "event_type": webhook.event_type,
                    "status": "recovery_error",
                    "error": str(e),
                    "customer_id": webhook.customer_id
                })
        
        self.logger.info(f"Webhook recovery completed: {recovery_results}")
        return recovery_results
    
    def reprocess_webhook(self, webhook: WebhookProcessingLog, event_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Reprocess a single webhook using the sync service
        """
        try:
            event_type = webhook.event_type
            
            if event_type == "INITIAL_PURCHASE":
                return self.sync_service.process_initial_purchase(event_data, webhook)
            elif event_type == "RENEWAL":
                return self.sync_service.process_renewal(event_data, webhook)
            elif event_type == "CANCELLATION":
                return self.sync_service.process_cancellation(event_data, webhook)
            elif event_type == "EXPIRATION":
                return self.sync_service.process_expiration(event_data, webhook)
            else:
                error_msg = f"Unsupported event type for recovery: {event_type}"
                webhook.status = "failed"
                webhook.error_message = error_msg
                webhook.completed_at = now_ist()
                self.db.commit()
                return {"success": False, "error": error_msg}
                
        except Exception as e:
            error_msg = f"Error reprocessing webhook: {str(e)}"
            webhook.status = "failed"
            webhook.error_message = error_msg
            webhook.completed_at = now_ist()
            self.db.commit()
            return {"success": False, "error": error_msg}
    
    def recover_missing_entitlements(self) -> Dict[str, Any]:
        """
        Recover missing entitlements for customers with active subscriptions
        """
        # Find customers with active subscriptions but no active entitlements
        active_subscriptions = self.db.query(Subscription).filter(
            Subscription.status.in_([
                SubscriptionStatus.active.value,
                SubscriptionStatus.trial.value
            ]),
            Subscription.active_until > now_ist()
        ).all()
        
        recovery_results = {
            "subscriptions_checked": len(active_subscriptions),
            "missing_entitlements": 0,
            "entitlements_created": 0,
            "details": []
        }
        
        for subscription in active_subscriptions:
            try:
                # Check if customer has active entitlement
                entitlement = self.db.query(Entitlement).filter(
                    Entitlement.customer_id == subscription.customer_id,
                    Entitlement.entitlement_type == "app",
                    Entitlement.status == StatusEnt.active.value
                ).first()
                
                if not entitlement:
                    recovery_results["missing_entitlements"] += 1
                    
                    # Create missing entitlement
                    new_entitlement = self.sync_service.ensure_active_entitlement(
                        subscription.customer_id,
                        subscription.active_until
                    )
                    
                    recovery_results["entitlements_created"] += 1
                    recovery_results["details"].append({
                        "customer_id": subscription.customer_id,
                        "subscription_id": subscription.id,
                        "entitlement_id": new_entitlement.id,
                        "action": "created_missing_entitlement"
                    })
                    
                    self.logger.info(f"Created missing entitlement for customer {subscription.customer_id}")
                
            except Exception as e:
                self.logger.error(f"Error creating entitlement for {subscription.customer_id}: {str(e)}")
                recovery_results["details"].append({
                    "customer_id": subscription.customer_id,
                    "subscription_id": subscription.id,
                    "action": "failed_to_create_entitlement",
                    "error": str(e)
                })
        
        if recovery_results["entitlements_created"] > 0:
            self.db.commit()
        
        self.logger.info(f"Entitlement recovery completed: {recovery_results}")
        return recovery_results
    
    def sync_subscription_entitlements(self) -> Dict[str, Any]:
        """
        Sync subscription status with entitlement status
        """
        sync_results = {
            "subscriptions_processed": 0,
            "entitlements_updated": 0,
            "issues_found": 0,
            "details": []
        }
        
        # Get all active subscriptions
        active_subscriptions = self.db.query(Subscription).filter(
            Subscription.status.in_([
                SubscriptionStatus.active.value,
                SubscriptionStatus.trial.value
            ])
        ).all()
        
        for subscription in active_subscriptions:
            try:
                sync_results["subscriptions_processed"] += 1
                
                # Get customer's entitlement
                entitlement = self.db.query(Entitlement).filter(
                    Entitlement.customer_id == subscription.customer_id,
                    Entitlement.entitlement_type == "app"
                ).first()
                
                current_time = now_ist()
                subscription_expired = (
                    subscription.active_until and 
                    subscription.active_until <= current_time
                )
                
                if subscription_expired:
                    # Subscription expired, update both subscription and entitlement
                    subscription.status = SubscriptionStatus.expired.value
                    
                    if entitlement and entitlement.status != StatusEnt.expired.value:
                        entitlement.status = StatusEnt.expired.value
                        sync_results["entitlements_updated"] += 1
                        
                    sync_results["details"].append({
                        "customer_id": subscription.customer_id,
                        "action": "expired_subscription_and_entitlement",
                        "subscription_id": subscription.id,
                        "entitlement_id": entitlement.id if entitlement else None
                    })
                    
                elif entitlement:
                    # Subscription active, ensure entitlement is also active
                    if entitlement.status != StatusEnt.active.value:
                        entitlement.status = StatusEnt.active.value
                        entitlement.active_until = subscription.active_until
                        sync_results["entitlements_updated"] += 1
                        
                        sync_results["details"].append({
                            "customer_id": subscription.customer_id,
                            "action": "activated_entitlement",
                            "subscription_id": subscription.id,
                            "entitlement_id": entitlement.id
                        })
                else:
                    # No entitlement found for active subscription
                    new_entitlement = self.sync_service.ensure_active_entitlement(
                        subscription.customer_id,
                        subscription.active_until
                    )
                    
                    sync_results["entitlements_updated"] += 1
                    sync_results["issues_found"] += 1
                    sync_results["details"].append({
                        "customer_id": subscription.customer_id,
                        "action": "created_missing_entitlement",
                        "subscription_id": subscription.id,
                        "entitlement_id": new_entitlement.id,
                        "issue": "missing_entitlement_for_active_subscription"
                    })
                
            except Exception as e:
                self.logger.error(f"Error syncing subscription {subscription.id}: {str(e)}")
                sync_results["issues_found"] += 1
                sync_results["details"].append({
                    "customer_id": subscription.customer_id,
                    "subscription_id": subscription.id,
                    "action": "sync_error",
                    "error": str(e)
                })
        
        if sync_results["entitlements_updated"] > 0:
            self.db.commit()
        
        self.logger.info(f"Subscription-Entitlement sync completed: {sync_results}")
        return sync_results
    
    def cleanup_old_webhook_logs(self, days_old: int = 30) -> Dict[str, Any]:
        """
        Cleanup old webhook logs to prevent database bloat
        """
        cutoff_date = now_ist() - timedelta(days=days_old)
        
        # Count logs to be deleted
        old_logs = self.db.query(WebhookProcessingLog).filter(
            WebhookProcessingLog.started_at <= cutoff_date,
            WebhookProcessingLog.status == "completed"  # Only delete completed logs
        )
        
        count_to_delete = old_logs.count()
        
        if count_to_delete > 0:
            # Delete old logs
            old_logs.delete()
            self.db.commit()
            
            self.logger.info(f"Cleaned up {count_to_delete} old webhook logs older than {days_old} days")
        
        return {
            "logs_deleted": count_to_delete,
            "cutoff_date": cutoff_date.isoformat(),
            "days_old": days_old
        }
    
    def get_webhook_stats(self, hours: int = 24) -> Dict[str, Any]:
        """
        Get webhook processing statistics for monitoring
        """
        cutoff_time = now_ist() - timedelta(hours=hours)
        
        webhooks = self.db.query(WebhookProcessingLog).filter(
            WebhookProcessingLog.started_at >= cutoff_time
        ).all()
        
        stats = {
            "total_webhooks": len(webhooks),
            "by_status": {},
            "by_event_type": {},
            "by_customer": {},
            "failed_webhooks": [],
            "average_processing_time_ms": 0,
            "time_period_hours": hours
        }
        
        total_processing_time = 0
        completed_count = 0
        
        for webhook in webhooks:
            # Status stats
            status = webhook.status
            stats["by_status"][status] = stats["by_status"].get(status, 0) + 1
            
            # Event type stats
            event_type = webhook.event_type
            stats["by_event_type"][event_type] = stats["by_event_type"].get(event_type, 0) + 1
            
            # Customer stats
            customer_id = webhook.customer_id
            stats["by_customer"][customer_id] = stats["by_customer"].get(customer_id, 0) + 1
            
            # Failed webhooks
            if status == "failed":
                stats["failed_webhooks"].append({
                    "event_id": webhook.event_id,
                    "event_type": webhook.event_type,
                    "customer_id": webhook.customer_id,
                    "error_message": webhook.error_message,
                    "started_at": webhook.started_at.isoformat(),
                    "retry_count": webhook.retry_count
                })
            
            # Processing time
            if webhook.processing_duration_ms:
                total_processing_time += webhook.processing_duration_ms
                completed_count += 1
        
        if completed_count > 0:
            stats["average_processing_time_ms"] = total_processing_time / completed_count
        
        return stats