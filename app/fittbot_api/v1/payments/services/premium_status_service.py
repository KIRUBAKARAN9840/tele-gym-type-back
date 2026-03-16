"""
Premium Status Service - Enterprise-level status checking with proper sync
"""

import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, Optional
from sqlalchemy.orm import Session
from sqlalchemy import and_, or_

from ..models.entitlements import Entitlement
from ..models.subscriptions import Subscription
from ..models.orders import Order
from ..models.payments import Payment
from ..models.webhook_logs import WebhookProcessingLog
from ..models.enums import StatusEnt, SubscriptionStatus, StatusOrder, StatusPayment, Provider

logger = logging.getLogger("payments.premium_status")

# Indian Standard Time (IST) timezone
IST = timezone(timedelta(hours=5, minutes=30))

def now_ist():
    """Get current time in IST"""
    return datetime.now(IST)


class PremiumStatusService:

    
    def __init__(self, db: Session):
        self.db = db
        self.logger = logger
    
    def ensure_timezone_aware(self, dt: Optional[datetime]) -> Optional[datetime]:
        """Ensure datetime is timezone-aware (IST)"""
        if dt is None:
            return None
        if dt.tzinfo is None:
            # Assume naive datetimes are in IST
            return dt.replace(tzinfo=IST)
        return dt
    
    def get_premium_status(self, customer_id: str) -> Dict[str, Any]:
        """
        Get comprehensive premium status for a customer
        This is the main method used by frontend verification
        """
        try:
            # # 1. Check for active entitlement (primary source of truth)
            # entitlement_status = self.check_entitlement_status(customer_id)
            
            # # 2. Check subscription status
            subscription_status = self.check_subscription_status(customer_id)
            
            # # 3. Check recent webhook processing (for debugging)
            # webhook_status = self.check_recent_webhook_activity(customer_id)
            
            # # 4. Check payment status
            # payment_status = self.check_payment_status(customer_id)
            
            # Determine overall premium status
            # has_premium = self.determine_premium_status(
            #     entitlement_status, subscription_status, payment_status
            # )

            has_premium = self.determine_premium_status(
                 subscription_status
            )
            
            return {
                "customer_id": customer_id,
                "has_premium": has_premium,
                # "status_details": {
                #     "entitlement": entitlement_status,
                #     "subscription": subscription_status,
                #     "payment": payment_status,
                #     "webhook": webhook_status
                # },
                # "checked_at": now_ist().isoformat(),
                # "debug_info": {
                #     "entitlement_active": entitlement_status.get("is_active", False),
                #     "subscription_active": subscription_status.get("is_active", False),
                #     "payment_successful": payment_status.get("has_successful_payment", False),
                #     "webhook_processed": webhook_status.get("recent_success", False)
                # }
            }
            
        except Exception as e:
            self.logger.error(f"Error checking premium status for {customer_id}: {str(e)}")
            return {
                "customer_id": customer_id,
                "has_premium": False,
                "error": str(e),
                "checked_at": now_ist().isoformat()
            }
    
    def check_entitlement_status(self, customer_id: str) -> Dict[str, Any]:
        """Check entitlement status (primary source of truth)"""
        entitlement = self.db.query(Entitlement).filter(
            Entitlement.customer_id == customer_id,
            Entitlement.entitlement_type == "app"  # EntType.app.value
        ).first()
        
        if not entitlement:
            return {
                "exists": False,
                "is_active": False,
                "reason": "no_entitlement_found"
            }
        
        current_time = now_ist()
        active_until = self.ensure_timezone_aware(entitlement.active_until)
        
        # Check if entitlement is active (use pending as active status)
        is_active = (
            entitlement.status == StatusEnt.pending.value and
            (not active_until or active_until > current_time)
        )
        
        return {
            "exists": True,
            "is_active": is_active,
            "status": entitlement.status,
            "active_since": entitlement.active_from.isoformat() if entitlement.active_from else None,
            "active_until": entitlement.active_until.isoformat() if entitlement.active_until else None,
            "days_remaining": self.calculate_days_remaining(entitlement.active_until) if entitlement.active_until else None,
            "entitlement_id": entitlement.id
        }
    
    def check_subscription_status(self, customer_id: str) -> Dict[str, Any]:
        """Check subscription status"""
        # Get the most recent active subscription
        subscription = self.db.query(Subscription).filter(
            Subscription.customer_id == customer_id,
            Subscription.provider == Provider.google_play.value
        ).order_by(Subscription.created_at.desc()).first()
        
        if not subscription:
            return {
                "exists": False,
                "is_active": False,
                "reason": "no_subscription_found"
            }
        
        current_time = now_ist()
        active_until = self.ensure_timezone_aware(subscription.active_until)
        
        # Check if subscription is active
        is_active = (
            subscription.status == SubscriptionStatus.active.value and
            (not active_until or active_until > current_time)
        )
        
        return {
            "exists": True,
            "is_active": is_active,
            "status": subscription.status,
            "product_id": subscription.product_id,
            "auto_renew": subscription.auto_renew,
            "active_since": subscription.active_from.isoformat() if subscription.active_from else None,
            "active_until": subscription.active_until.isoformat() if subscription.active_until else None,
            "days_remaining": self.calculate_days_remaining(subscription.active_until) if subscription.active_until else None,
            "latest_transaction_id": subscription.latest_txn_id,
            "subscription_id": subscription.id
        }
    
    def check_payment_status(self, customer_id: str) -> Dict[str, Any]:
        """Check payment status"""
        # Get most recent successful payment
        successful_payment = self.db.query(Payment).filter(
            Payment.customer_id == customer_id,
            Payment.status == StatusPayment.captured.value,
            Payment.provider == Provider.google_play.value
        ).order_by(Payment.captured_at.desc()).first()
        
        if not successful_payment:
            return {
                "has_successful_payment": False,
                "reason": "no_successful_payment_found"
            }
        
        # Get associated order
        order = self.db.query(Order).filter(Order.id == successful_payment.order_id).first()
        
        return {
            "has_successful_payment": True,
            "payment_id": successful_payment.id,
            "order_id": successful_payment.order_id,
            "amount": successful_payment.amount_minor / 100,  # Convert to rupees
            "currency": successful_payment.currency,
            "captured_at": successful_payment.captured_at.isoformat(),
            "provider_payment_id": successful_payment.provider_payment_id,
            "order_status": order.status if order else "unknown"
        }
    
    def check_recent_webhook_activity(self, customer_id: str, hours: int = 1) -> Dict[str, Any]:
        """Check recent webhook processing activity for debugging"""
        cutoff_time = now_ist() - timedelta(hours=hours)
        # Convert to naive datetime for comparison with database stored times
        cutoff_time_naive = cutoff_time.replace(tzinfo=None)
        
        recent_webhooks = self.db.query(WebhookProcessingLog).filter(
            WebhookProcessingLog.customer_id == customer_id,
            WebhookProcessingLog.started_at >= cutoff_time_naive
        ).order_by(WebhookProcessingLog.started_at.desc()).limit(5).all()
        
        if not recent_webhooks:
            return {
                "recent_activity": False,
                "reason": f"no_webhook_activity_in_last_{hours}_hours"
            }
        
        # Count statuses
        status_counts = {}
        latest_webhook = recent_webhooks[0]
        
        for webhook in recent_webhooks:
            status = webhook.status
            status_counts[status] = status_counts.get(status, 0) + 1
        
        return {
            "recent_activity": True,
            "webhook_count": len(recent_webhooks),
            "status_counts": status_counts,
            "latest_webhook": {
                "event_type": latest_webhook.event_type,
                "status": latest_webhook.status,
                "started_at": latest_webhook.started_at.isoformat(),
                "completed_at": latest_webhook.completed_at.isoformat() if latest_webhook.completed_at else None,
                "result_summary": latest_webhook.result_summary
            },
            "recent_success": any(w.status == "completed" for w in recent_webhooks)
        }
    
    def determine_premium_status(
        self, 
        entitlement_status: Dict[str, Any],
        subscription_status: Dict[str, Any],
        payment_status: Dict[str, Any]
    ) -> bool:
        """
        Determine overall premium status based on all checks
        Priority: Entitlement > Subscription > Payment
        """
        # Primary check: Active entitlement
        if entitlement_status.get("is_active"):
            return True
        
        # Secondary check: Active subscription (in case entitlement hasn't been updated yet)
        if subscription_status.get("is_active"):
            # If subscription is active but entitlement isn't, this might be a sync issue
            self.logger.warning(
                f"Subscription active but entitlement not active - possible sync issue"
            )
            return True
        
        # Fallback: Recent successful payment with pending processing
        if payment_status.get("has_successful_payment"):
            # Check if payment is very recent (within last 10 minutes)
            try:
                captured_at_str = payment_status.get("captured_at", "")
                if captured_at_str:
                    captured_at = datetime.fromisoformat(captured_at_str.replace("Z", "+00:00"))
                    captured_at = self.ensure_timezone_aware(captured_at)
                    time_diff = now_ist() - captured_at
                    
                    # If payment is very recent, give benefit of doubt
                    if time_diff.total_seconds() < 600:  # 10 minutes
                        self.logger.info("Recent payment found, giving benefit of doubt for premium status")
                        return True
            except Exception as e:
                self.logger.warning(f"Error parsing payment timestamp: {e}")
        
        return False
    
    def calculate_days_remaining(self, active_until: datetime) -> int:
        """Calculate days remaining until expiration"""
        if not active_until:
            return 0
        
        active_until = self.ensure_timezone_aware(active_until)
        current_time = now_ist()
        if active_until <= current_time:
            return 0
        
        time_diff = active_until - current_time
        return time_diff.days
    
    def get_order_status(self, order_id: str) -> Dict[str, Any]:
        """Get detailed order status for frontend polling"""
        try:
            order = self.db.query(Order).filter(Order.id == order_id).first()
            
            if not order:
                return {
                    "order_exists": False,
                    "error": "Order not found"
                }
            
            # Get associated payment
            payment = self.db.query(Payment).filter(
                Payment.order_id == order_id
            ).first()
            
            # Get associated subscription
            subscription = self.db.query(Subscription).filter(
                Subscription.customer_id == order.customer_id
            ).order_by(Subscription.created_at.desc()).first()
            
            # Check if webhook processing completed for this customer
            recent_webhook = self.db.query(WebhookProcessingLog).filter(
                WebhookProcessingLog.customer_id == order.customer_id,
                WebhookProcessingLog.status == "completed"
            ).order_by(WebhookProcessingLog.completed_at.desc()).first()
            
            return {
                "order_exists": True,
                "order_id": order.id,
                "status": order.status,
                "customer_id": order.customer_id,
                "payment": {
                    "exists": payment is not None,
                    "status": payment.status if payment else None,
                    "provider_payment_id": payment.provider_payment_id if payment else None
                },
                "subscription": {
                    "exists": subscription is not None,
                    "status": subscription.status if subscription else None,
                    "subscription_created": subscription is not None
                },
                "webhook_processed": recent_webhook is not None,
                "webhook_completed_at": recent_webhook.completed_at.isoformat() if recent_webhook else None
            }
            
        except Exception as e:
            self.logger.error(f"Error getting order status for {order_id}: {str(e)}")
            return {
                "order_exists": False,
                "error": str(e)
            }