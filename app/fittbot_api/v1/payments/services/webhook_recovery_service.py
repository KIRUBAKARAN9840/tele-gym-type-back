"""
Webhook Recovery Service - Handle missed/failed webhooks
Enterprise-grade system to ensure no payments are lost due to webhook issues
"""

import logging
from datetime import datetime, timedelta
from typing import List, Dict
from sqlalchemy.orm import Session
from sqlalchemy import and_, or_

from ..models.orders import Order
from ..models.payments import Payment
from ..models.subscriptions import Subscription
from ..models.enums import StatusOrder, StatusPayment
from ..webhooks.revenuecat_handler import handle_initial_purchase, now_ist
from ..config.database import get_db_session

logger = logging.getLogger("webhook_recovery")

class WebhookRecoveryService:
    """Enterprise webhook failure recovery system"""
    
    def __init__(self):
        self.revenuecat_api = RevenueCatAPI()
    
    async def check_for_missed_webhooks(self, db: Session):
        """Check for orders that should have webhooks but don't"""
        
        # Find orders that are stuck in pending for > 10 minutes
        cutoff_time = now_ist() - timedelta(minutes=10)
        
        stuck_orders = db.query(Order).filter(
            Order.status == StatusOrder.pending.value,
            Order.provider == "google_play",
            Order.created_at < cutoff_time
        ).all()
        
        logger.info(f"Found {len(stuck_orders)} potentially stuck orders")
        
        for order in stuck_orders:
            try:
                await self.recover_order_status(order, db)
            except Exception as e:
                logger.error(f"Failed to recover order {order.id}: {e}")
    
    async def recover_order_status(self, order: Order, db: Session):
        """Recover single order by checking RevenueCat directly"""
        
        customer_id = order.customer_id
        
        # Check RevenueCat API directly for customer's subscription status
        customer_info = await self.revenuecat_api.get_customer_info(customer_id)
        
        if not customer_info:
            logger.warning(f"No customer info found in RevenueCat for {customer_id}")
            return
        
        # Check if customer has active subscriptions
        active_subscriptions = customer_info.get('subscriber', {}).get('subscriptions', {})
        
        for product_id, subscription_data in active_subscriptions.items():
            if subscription_data.get('unsubscribe_detected_at') is None:  # Still active
                
                # Find the transaction for this timeframe
                purchase_date = subscription_data.get('purchase_date')
                if self.is_transaction_for_this_order(order, purchase_date):
                    
                    logger.info(f"🔄 RECOVERY: Found active subscription for order {order.id}")
                    
                    # Simulate the webhook that we missed
                    await self.simulate_missed_webhook(order, subscription_data, db)
                    
                    return
        
        # Check if order was actually cancelled/failed in RevenueCat
        await self.check_if_order_failed(order, customer_info, db)
    
    def is_transaction_for_this_order(self, order: Order, purchase_date: str) -> bool:
        """Check if this RevenueCat transaction matches our order timeline"""
        
        if not purchase_date:
            return False
        
        try:
            rc_purchase_time = datetime.fromisoformat(purchase_date.replace('Z', '+00:00'))
            order_time = order.created_at
            
            # Transaction should be within 15 minutes of order creation
            time_diff = abs((rc_purchase_time - order_time).total_seconds())
            return time_diff <= 900  # 15 minutes tolerance
            
        except Exception as e:
            logger.error(f"Error parsing purchase date {purchase_date}: {e}")
            return False
    
    async def simulate_missed_webhook(self, order: Order, subscription_data: dict, db: Session):
        """Simulate the webhook that was missed"""
        
        # Create mock webhook event with unique recovery ID
        recovery_event_id = f"RECOVERY_{order.customer_id}_{order.id}_{int(datetime.now().timestamp())}"
        
        mock_event = {
            "id": recovery_event_id,  # Unique ID for idempotency
            "app_user_id": order.customer_id,
            "product_id": subscription_data.get('product_identifier', 'unknown'),
            "transaction_id": subscription_data.get('store_transaction_id'),
            "purchased_at_ms": int(datetime.fromisoformat(
                subscription_data.get('purchase_date', '').replace('Z', '+00:00')
            ).timestamp() * 1000) if subscription_data.get('purchase_date') else None,
            "expiration_at_ms": int(datetime.fromisoformat(
                subscription_data.get('expires_date', '').replace('Z', '+00:00')  
            ).timestamp() * 1000) if subscription_data.get('expires_date') else None,
            "price_in_purchased_currency": subscription_data.get('price_in_purchased_currency', 0),
            "currency": "INR",
            "type": "RECOVERY_INITIAL_PURCHASE",
            "_is_recovery": True  # Mark as recovery event
        }
        
        logger.info(f"🔄 Simulating missed webhook for order {order.id}")
        logger.info(f"Mock event: {mock_event}")
        
        # Process through normal webhook handler
        await handle_initial_purchase(mock_event, db)
        
        # Log recovery action
        recovery_log = WebhookRecoveryLog(
            order_id=order.id,
            customer_id=order.customer_id,
            recovery_reason="missed_webhook",
            original_webhook_type="INITIAL_PURCHASE",
            recovery_action="simulated_webhook",
            recovered_at=now_ist()
        )
        db.add(recovery_log)
        db.commit()
        
        logger.info(f"✅ Successfully recovered order {order.id}")
    
    async def check_if_order_failed(self, order: Order, customer_info: dict, db: Session):
        """Check if order actually failed and should be marked as failed"""
        
        # If no active subscriptions found and order is old
        if now_ist() - order.created_at > timedelta(hours=1):
            
            logger.warning(f"⚠️ Order {order.id} appears to have failed - no subscription found")
            
            # Mark order as failed
            order.status = StatusOrder.failed.value
            order.updated_at = now_ist()
            
            # Create failure log
            failure_log = WebhookRecoveryLog(
                order_id=order.id,
                customer_id=order.customer_id,
                recovery_reason="payment_failed",
                recovery_action="marked_as_failed",
                recovered_at=now_ist()
            )
            db.add(failure_log)
            
            # Notify customer support
            await self.notify_failed_payment(order)
            
            db.commit()


class RevenueCatAPI:
    """RevenueCat API client for direct data fetching"""
    
    def __init__(self):
        self.api_key = "your_revenuecat_api_key"
        self.base_url = "https://api.revenuecat.com/v1"
    
    async def get_customer_info(self, customer_id: str) -> dict:
        """Get customer subscription info directly from RevenueCat API"""
        
        import aiohttp
        
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }
        
        try:
            async with aiohttp.ClientSession() as session:
                url = f"{self.base_url}/subscribers/{customer_id}"
                async with session.get(url, headers=headers) as response:
                    if response.status == 200:
                        return await response.json()
                    else:
                        logger.error(f"RevenueCat API error: {response.status}")
                        return None
        except Exception as e:
            logger.error(f"Error calling RevenueCat API: {e}")
            return None


# Database model for recovery logging
from sqlalchemy import Column, String, DateTime, Text
from ..models.base import Base

class WebhookRecoveryLog(Base):
    """Log all webhook recovery actions"""
    __tablename__ = "webhook_recovery_logs"
    
    id = Column(String(255), primary_key=True)
    order_id = Column(String(255), index=True)
    customer_id = Column(String(255), index=True)
    recovery_reason = Column(String(100))  # missed_webhook, payment_failed, etc.
    original_webhook_type = Column(String(50))  # INITIAL_PURCHASE, RENEWAL, etc.
    recovery_action = Column(String(100))  # simulated_webhook, marked_as_failed, etc.
    recovered_at = Column(DateTime)
    recovery_data = Column(Text)  # JSON data


# Scheduled job to run recovery checks
async def scheduled_webhook_recovery():
    """Run this every 15 minutes via cron job or Celery"""
    
    with get_db_session() as db:
        recovery_service = WebhookRecoveryService()
        await recovery_service.check_for_missed_webhooks(db)
        
        logger.info("✅ Webhook recovery check completed")