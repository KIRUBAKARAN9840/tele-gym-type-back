"""
Enterprise-level Razorpay Subscription Sync Service
Handles synchronization between normal flow and idempotent webhook processing
Mirrors RevenueCat functionality for Razorpay subscriptions
"""

import json
import logging
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any, Tuple
from sqlalchemy.orm import Session
from sqlalchemy import and_, or_

from ..models.orders import Order, OrderItem
from ..models.payments import Payment
from ..models.subscriptions import Subscription
from ..models.entitlements import Entitlement
from ..models.webhook_logs import WebhookProcessingLog
from ..models.catalog import CatalogProduct
from ..models.enums import (
    StatusOrder, StatusPayment, SubscriptionStatus, 
    StatusEnt, Provider, ItemType, EntType
)

logger = logging.getLogger("payments.razorpay_sync_service")

# Indian Standard Time (IST) timezone
IST = timezone(timedelta(hours=5, minutes=30))

def now_ist():
    """Get current time in IST"""
    return datetime.now(IST)

def timestamp_to_ist(timestamp: int) -> datetime:
    """Convert timestamp seconds to IST datetime"""
    return datetime.fromtimestamp(timestamp, tz=IST)


class RazorpaySubscriptionSyncService:
    """
    Enterprise-level service to handle Razorpay subscription synchronization
    with proper idempotency and error recovery
    Mirrors the functionality of SubscriptionSyncService but for Razorpay
    """
    
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
    
    def generate_id(self, prefix: str) -> str:
        """Generate unique ID with timestamp"""
        timestamp = int(datetime.now().timestamp())
        import random
        random_suffix = random.randint(1000, 9999)
        return f"{prefix}_{timestamp}_{random_suffix}"
    
    def check_idempotency(
        self, 
        event_id: str, 
        event_type: str,
        allow_retry_on_failure: bool = True
    ) -> Tuple[bool, Optional[WebhookProcessingLog]]:
        """
        Check if an event has already been processed
        Returns: (should_process, existing_log)
        """
        existing_log = self.db.query(WebhookProcessingLog).filter(
            WebhookProcessingLog.event_id == event_id,
            WebhookProcessingLog.event_type == event_type
        ).first()
        
        if not existing_log:
            return True, None
        
        # If completed successfully, don't reprocess
        if existing_log.status == "completed":
            self.logger.info(f"Event {event_id} already processed successfully")
            return False, existing_log
        
        # If failed and retry is allowed, allow reprocessing
        if existing_log.status == "failed" and allow_retry_on_failure:
            self.logger.info(f"Retrying failed event {event_id}")
            existing_log.retry_count += 1
            existing_log.status = "processing"
            existing_log.started_at = now_ist()
            return True, existing_log
        
        # If still processing, check timeout (5 minutes)
        if existing_log.status == "processing":
            time_elapsed = (now_ist() - existing_log.started_at).total_seconds()
            if time_elapsed > 300:  # 5 minutes timeout
                self.logger.warning(f"Event {event_id} processing timeout, allowing retry")
                existing_log.retry_count += 1
                existing_log.started_at = now_ist()
                return True, existing_log
            else:
                self.logger.info(f"Event {event_id} still processing, skipping")
                return False, existing_log
        
        return False, existing_log
    
    def create_or_update_catalog_product(
        self,
        product_id: str,
        amount_minor: int,
        title: Optional[str] = None
    ) -> CatalogProduct:
        """Create or update catalog product"""
        existing_product = self.db.query(CatalogProduct).filter(
            CatalogProduct.sku == product_id
        ).first()
        
        if existing_product:
            # Update price if different
            if existing_product.base_amount_minor != amount_minor:
                existing_product.base_amount_minor = amount_minor
                self.logger.info(f"Updated catalog product {product_id} price to {amount_minor}")
            return existing_product
        
        # Create new product
        catalog_product = CatalogProduct(
            sku=product_id,
            item_type=ItemType.app_subscription.value,
            title=title or f"Premium Subscription ({product_id})",
            base_amount_minor=amount_minor,
            description=f"Auto-created from Razorpay webhook",
            active=True
        )
        self.db.add(catalog_product)
        self.db.flush()
        self.logger.info(f"Created catalog product: {product_id}")
        return catalog_product
    
    def process_initial_purchase(
        self,
        event: Dict[str, Any],
        processing_log: Optional[WebhookProcessingLog] = None
    ) -> Dict[str, Any]:
        """
        Process initial Razorpay subscription purchase with proper error handling and recovery
        """
        customer_id = event.get("customer_id") or event.get("app_user_id")
        
        # Extract product and transaction details from Razorpay event
        product_id, subscription_id, amount_minor, purchase_date, expires_date = self.extract_razorpay_purchase_details(event)
        
        if not product_id or not subscription_id:
            error_msg = f"Missing required fields: product_id={product_id}, subscription_id={subscription_id}"
            self.logger.error(error_msg)
            if processing_log:
                processing_log.status = "failed"
                processing_log.error_message = error_msg
                processing_log.completed_at = now_ist()
            return {"success": False, "error": error_msg}
        
        try:
            # Ensure catalog product exists
            self.create_or_update_catalog_product(product_id, amount_minor)
            
            # Check for existing subscription (in case of duplicate events)
            existing_subscription = self.db.query(Subscription).filter(
                Subscription.customer_id == customer_id,
                Subscription.product_id == product_id,
                Subscription.status == SubscriptionStatus.active.value
            ).first()
            
            if existing_subscription:
                self.logger.info(f"Customer {customer_id} already has active subscription for {product_id}")
                # Update subscription ID if newer
                existing_subscription.latest_txn_id = subscription_id
                existing_subscription.active_until = expires_date
                
                # Ensure entitlement is active
                self.ensure_active_entitlement(customer_id, expires_date)
                
                if processing_log:
                    processing_log.status = "completed"
                    processing_log.completed_at = now_ist()
                    processing_log.result_summary = f"Updated existing subscription {existing_subscription.id}"
                
                self.db.commit()
                return {"success": True, "subscription_id": existing_subscription.id}
            
            # Create or update order
            order = self.create_or_update_order(
                customer_id, subscription_id, amount_minor, "initial_purchase"
            )
            
            # Create order item
            order_item = self.create_order_item(
                order.id, product_id, amount_minor, "initial_purchase"
            )
            
            # Create or update payment
            payment = self.create_or_update_payment(
                order.id, customer_id, subscription_id, amount_minor
            )
            
            # Create subscription
            subscription = self.create_subscription(
                customer_id, product_id, subscription_id, purchase_date, expires_date
            )
            
            # Create or update entitlement
            entitlement = self.ensure_active_entitlement(customer_id, expires_date, order_item.id)
            
            # Mark processing as completed
            if processing_log:
                processing_log.status = "completed"
                processing_log.completed_at = now_ist()
                processing_log.result_summary = (
                    f"Created: Order={order.id}, Payment={payment.id}, "
                    f"Subscription={subscription.id}, Entitlement={entitlement.id}"
                )
                processing_log.processing_duration_ms = int(
                    (processing_log.completed_at - processing_log.started_at).total_seconds() * 1000
                )
            
            self.db.commit()
            self.logger.info(f"✅ Successfully processed initial purchase for {customer_id}")
            
            return {
                "success": True,
                "order_id": order.id,
                "subscription_id": subscription.id,
                "entitlement_id": entitlement.id
            }
            
        except Exception as e:
            self.logger.error(f"Error processing initial purchase: {str(e)}")
            self.db.rollback()
            
            if processing_log:
                processing_log.status = "failed"
                processing_log.error_message = str(e)
                processing_log.completed_at = now_ist()
                self.db.commit()  # Commit the error log
            
            return {"success": False, "error": str(e)}
    
    def extract_razorpay_purchase_details(self, event: Dict[str, Any]) -> Tuple:
        """Extract purchase details from Razorpay subscription event formats"""
        product_id = None
        subscription_id = None
        amount_minor = 0
        purchase_date = now_ist()
        expires_date = now_ist() + timedelta(days=30)
        
        # Handle different Razorpay subscription event structures
        subscription_data = event.get("payload", {}).get("subscription", {}).get("entity", {})
        payment_data = event.get("payload", {}).get("payment", {}).get("entity", {})
        
        # Extract subscription ID
        subscription_id = subscription_data.get("id") or event.get("subscription_id")
        
        # Extract product/plan ID
        product_id = (subscription_data.get("plan_id") or 
                     event.get("plan_id") or 
                     event.get("product_id"))
        
        # Extract amount
        if subscription_data.get("amount"):
            amount_minor = int(subscription_data["amount"])  # Razorpay already in minor units
        elif payment_data.get("amount"):
            amount_minor = int(payment_data["amount"])
        
        # Extract dates
        if subscription_data.get("created_at"):
            purchase_date = timestamp_to_ist(subscription_data["created_at"])
        if subscription_data.get("end_at"):
            expires_date = timestamp_to_ist(subscription_data["end_at"])
        elif subscription_data.get("current_end"):
            expires_date = timestamp_to_ist(subscription_data["current_end"])
        
        # Handle plan-based subscriptions
        if not expires_date or expires_date <= purchase_date:
            # Default to monthly subscription if no end date
            expires_date = purchase_date + timedelta(days=30)
        
        return product_id, subscription_id, amount_minor, purchase_date, expires_date
    
    def create_or_update_order(
        self,
        customer_id: str,
        subscription_id: str,
        amount_minor: int,
        order_type: str = "initial"
    ) -> Order:
        """Create or update order for Razorpay"""
        # Check for existing pending order
        existing_order = self.db.query(Order).filter(
            Order.customer_id == customer_id,
            Order.status == StatusOrder.pending.value,
            Order.provider == Provider.razorpay_pg.value
        ).order_by(Order.created_at.desc()).first()
        
        if existing_order:
            # Update existing order
            existing_order.status = StatusOrder.paid.value
            existing_order.provider_order_id = subscription_id
            existing_order.gross_amount_minor = amount_minor
            self.logger.info(f"Updated existing order {existing_order.id}")
            return existing_order
        
        # Create new order
        order_id = self.generate_id(f"ord_{order_type[:2].upper()}")
        order = Order(
            id=order_id,
            customer_id=customer_id,
            currency="INR",
            provider=Provider.razorpay_pg.value,
            provider_order_id=subscription_id,
            gross_amount_minor=amount_minor,
            status=StatusOrder.paid.value
        )
        self.db.add(order)
        self.db.flush()
        self.logger.info(f"Created new order {order_id}")
        return order
    
    def create_order_item(
        self,
        order_id: str,
        product_id: str,
        amount_minor: int,
        item_type: str = "subscription"
    ) -> OrderItem:
        """Create order item"""
        order_item_id = self.generate_id("oi")
        order_item = OrderItem(
            id=order_item_id,
            order_id=order_id,
            item_type=ItemType.app_subscription.value,
            sku=product_id,
            title=f"Subscription: {product_id}",
            unit_price_minor=amount_minor,
            qty=1,
            item_metadata={
                "product_id": product_id,
                "subscription_period": "monthly" if "monthly" in product_id.lower() else "yearly",
                "item_type": item_type,
                "provider": "razorpay"
            }
        )
        self.db.add(order_item)
        self.db.flush()
        return order_item
    
    def create_or_update_payment(
        self,
        order_id: str,
        customer_id: str,
        subscription_id: str,
        amount_minor: int
    ) -> Payment:
        """Create or update payment for Razorpay"""
        # Check for existing payment with same subscription ID
        existing_payment = self.db.query(Payment).filter(
            Payment.provider_payment_id == subscription_id,
            Payment.customer_id == customer_id
        ).first()
        
        if existing_payment:
            # Update status if needed
            if existing_payment.status != StatusPayment.captured.value:
                existing_payment.status = StatusPayment.captured.value
                existing_payment.captured_at = now_ist()
            self.logger.info(f"Updated existing payment {existing_payment.id}")
            return existing_payment
        
        # Create new payment
        payment_id = self.generate_id("pay")
        payment = Payment(
            id=payment_id,
            order_id=order_id,
            customer_id=customer_id,
            amount_minor=amount_minor,
            currency="INR",
            provider=Provider.razorpay_pg.value,
            provider_payment_id=subscription_id,
            status=StatusPayment.captured.value,
            authorized_at=now_ist(),
            captured_at=now_ist(),
            payment_metadata={
                "subscription_id": subscription_id,
                "provider": "razorpay"
            }
        )
        self.db.add(payment)
        self.db.flush()
        self.logger.info(f"Created new payment {payment_id}")
        return payment
    
    def create_subscription(
        self,
        customer_id: str,
        product_id: str,
        subscription_id: str,
        purchase_date: datetime,
        expires_date: datetime
    ) -> Subscription:
        """Create subscription for Razorpay"""
        subscription_db_id = self.generate_id("sub")
        subscription = Subscription(
            id=subscription_db_id,
            customer_id=customer_id,
            provider=Provider.razorpay_pg.value,
            product_id=product_id,
            status=SubscriptionStatus.active.value,
            auto_renew=True,
            active_from=purchase_date,
            active_until=expires_date,
            rc_original_txn_id=subscription_id,  # Using same field for consistency
            latest_txn_id=subscription_id
        )
        self.db.add(subscription)
        self.db.flush()
        self.logger.info(f"Created subscription {subscription_db_id}")
        return subscription
    
    def ensure_active_entitlement(
        self,
        customer_id: str,
        expires_date: datetime,
        order_item_id: str = None
    ) -> Entitlement:
        """Ensure customer has active entitlement"""
        # Check for existing entitlement
        existing_entitlement = self.db.query(Entitlement).filter(
            Entitlement.customer_id == customer_id,
            Entitlement.entitlement_type == EntType.app.value
        ).first()
        
        if existing_entitlement:
            # Update to active status
            existing_entitlement.status = StatusEnt.pending.value  # Use pending as active
            existing_entitlement.active_until = expires_date
            self.logger.info(f"Updated entitlement {existing_entitlement.id} to active")
            return existing_entitlement
        
        # Create new entitlement
        entitlement_id = self.generate_id("ent")
        if not order_item_id:
            # Create a placeholder order item if none provided
            order = self.create_or_update_order(customer_id, f"temp_{int(now_ist().timestamp())}", 0, "subscription")
            order_item = self.create_order_item(order.id, "app_subscription", 0, "subscription")
            order_item_id = order_item.id
        
        entitlement = Entitlement(
            id=entitlement_id,
            order_item_id=order_item_id,
            customer_id=customer_id,
            entitlement_type=EntType.app.value,
            status=StatusEnt.pending.value,  # Use pending as active equivalent
            active_from=now_ist(),
            active_until=expires_date
        )
        self.db.add(entitlement)
        self.db.flush()
        self.logger.info(f"Created entitlement {entitlement_id}")
        return entitlement
    
    def process_renewal(
        self,
        event: Dict[str, Any],
        processing_log: Optional[WebhookProcessingLog] = None
    ) -> Dict[str, Any]:
        """Process Razorpay subscription renewal"""
        customer_id = event.get("customer_id") or event.get("app_user_id")
        
        # Extract details from Razorpay event
        product_id, subscription_id, amount_minor, purchase_date, expires_date = self.extract_razorpay_purchase_details(event)
        
        if not product_id or not subscription_id:
            error_msg = f"Missing required fields for renewal"
            self.logger.error(error_msg)
            if processing_log:
                processing_log.status = "failed"
                processing_log.error_message = error_msg
                processing_log.completed_at = now_ist()
            return {"success": False, "error": error_msg}
        
        try:
            # Find existing subscription
            subscription = self.db.query(Subscription).filter(
                Subscription.customer_id == customer_id,
                Subscription.product_id == product_id,
                Subscription.provider == Provider.razorpay_pg.value
            ).first()
            
            if subscription:
                # Update subscription
                subscription.status = SubscriptionStatus.active.value
                subscription.latest_txn_id = subscription_id
                subscription.active_until = expires_date
                subscription.auto_renew = True
                
                # Create renewal order
                order = self.create_or_update_order(
                    customer_id, subscription_id, amount_minor, "renewal"
                )
                
                # Create order item
                self.create_order_item(
                    order.id, product_id, amount_minor, "renewal"
                )
                
                # Create payment
                payment = self.create_or_update_payment(
                    order.id, customer_id, subscription_id, amount_minor
                )
                
                # Update entitlement
                entitlement = self.ensure_active_entitlement(customer_id, expires_date)
                
                if processing_log:
                    processing_log.status = "completed"
                    processing_log.completed_at = now_ist()
                    processing_log.result_summary = f"Renewed subscription {subscription.id}"
                
                self.db.commit()
                self.logger.info(f"✅ Successfully processed renewal for {customer_id}")
                
                return {
                    "success": True,
                    "subscription_id": subscription.id,
                    "order_id": order.id
                }
            else:
                # No existing subscription, treat as initial purchase
                self.logger.warning(f"No subscription found for renewal, treating as initial purchase")
                return self.process_initial_purchase(event, processing_log)
                
        except Exception as e:
            self.logger.error(f"Error processing renewal: {str(e)}")
            self.db.rollback()
            
            if processing_log:
                processing_log.status = "failed"
                processing_log.error_message = str(e)
                processing_log.completed_at = now_ist()
                self.db.commit()
            
            return {"success": False, "error": str(e)}
    
    def process_cancellation(
        self,
        event: Dict[str, Any],
        processing_log: Optional[WebhookProcessingLog] = None
    ) -> Dict[str, Any]:
        """Process Razorpay subscription cancellation"""
        customer_id = event.get("customer_id") or event.get("app_user_id")
        subscription_data = event.get("payload", {}).get("subscription", {}).get("entity", {})
        product_id = subscription_data.get("plan_id") or event.get("plan_id") or event.get("product_id")
        
        try:
            # Find subscription
            subscription = self.db.query(Subscription).filter(
                Subscription.customer_id == customer_id,
                Subscription.product_id == product_id,
                Subscription.provider == Provider.razorpay_pg.value
            ).first()
            
            if subscription:
                # Update subscription
                subscription.status = SubscriptionStatus.canceled.value
                subscription.auto_renew = False
                
                # Don't immediately expire entitlement - let it run until active_until
                entitlement = self.db.query(Entitlement).filter(
                    Entitlement.customer_id == customer_id,
                    Entitlement.entitlement_type == EntType.app.value
                ).first()
                
                if entitlement:
                    # Keep active but set to not renew
                    active_until = self.ensure_timezone_aware(entitlement.active_until)
                    current_time = now_ist()
                    if active_until and active_until > current_time:
                        # Still has time left, keep active
                        entitlement.status = StatusEnt.pending.value  # Use pending as active
                    else:
                        # Already expired
                        entitlement.status = StatusEnt.expired.value
                
                if processing_log:
                    processing_log.status = "completed"
                    processing_log.completed_at = now_ist()
                    processing_log.result_summary = f"Cancelled subscription {subscription.id}"
                
                self.db.commit()
                self.logger.info(f"✅ Successfully processed cancellation for {customer_id}")
                
                return {"success": True, "subscription_id": subscription.id}
            else:
                if processing_log:
                    processing_log.status = "completed"
                    processing_log.completed_at = now_ist()
                    processing_log.result_summary = "No subscription found to cancel"
                
                self.db.commit()
                return {"success": True, "message": "No subscription found"}
                
        except Exception as e:
            self.logger.error(f"Error processing cancellation: {str(e)}")
            self.db.rollback()
            
            if processing_log:
                processing_log.status = "failed"
                processing_log.error_message = str(e)
                processing_log.completed_at = now_ist()
                self.db.commit()
            
            return {"success": False, "error": str(e)}
    
    def process_expiration(
        self,
        event: Dict[str, Any],
        processing_log: Optional[WebhookProcessingLog] = None
    ) -> Dict[str, Any]:
        """Process Razorpay subscription expiration"""
        customer_id = event.get("customer_id") or event.get("app_user_id")
        subscription_data = event.get("payload", {}).get("subscription", {}).get("entity", {})
        product_id = subscription_data.get("plan_id") or event.get("plan_id") or event.get("product_id")

        try:
            expires_date = now_ist()
            if subscription_data.get("end_at"):
                expires_date = timestamp_to_ist(subscription_data["end_at"])
            elif subscription_data.get("current_end"):
                expires_date = timestamp_to_ist(subscription_data["current_end"])

            # Find subscription
            subscription = self.db.query(Subscription).filter(
                Subscription.customer_id == customer_id,
                Subscription.product_id == product_id,
                Subscription.provider == Provider.razorpay_pg.value
            ).first()

            if subscription:
                # Update subscription
                subscription.status = SubscriptionStatus.expired.value
                subscription.auto_renew = False
                subscription.active_until = expires_date

                # Update entitlement
                entitlement = self.db.query(Entitlement).filter(
                    Entitlement.customer_id == customer_id,
                    Entitlement.entitlement_type == EntType.app.value
                ).first()

                if entitlement:
                    entitlement.status = StatusEnt.expired.value
                    entitlement.active_until = expires_date

                if processing_log:
                    processing_log.status = "completed"
                    processing_log.completed_at = now_ist()
                    processing_log.result_summary = f"Expired subscription {subscription.id}"

                self.db.commit()
                self.logger.info(f"✅ Successfully processed expiration for {customer_id}")

                return {"success": True, "subscription_id": subscription.id}
            else:
                if processing_log:
                    processing_log.status = "completed"
                    processing_log.completed_at = now_ist()
                    processing_log.result_summary = "No subscription found to expire"

                self.db.commit()
                return {"success": True, "message": "No subscription found"}

        except Exception as e:
            self.logger.error(f"Error processing expiration: {str(e)}")
            self.db.rollback()

            if processing_log:
                processing_log.status = "failed"
                processing_log.error_message = str(e)
                processing_log.completed_at = now_ist()
                self.db.commit()

            return {"success": False, "error": str(e)}

    def process_gym_payment_captured(
        self,
        event: Dict[str, Any],
        processing_log: Optional[WebhookProcessingLog] = None
    ) -> Dict[str, Any]:
        """Process gym membership payment captured from Razorpay webhook"""
        try:
            # Extract payment data
            payment_data = event.get("payload", {}).get("payment", {}).get("entity", {})
            payment_id = payment_data.get("id")
            order_id = payment_data.get("order_id")
            amount_minor = int(payment_data.get("amount", 0))

            if not payment_id or not order_id:
                error_msg = f"Missing payment_id or order_id in webhook"
                self.logger.error(error_msg)
                if processing_log:
                    processing_log.status = "failed"
                    processing_log.error_message = error_msg
                    processing_log.completed_at = now_ist()
                return {"success": False, "error": error_msg}

            # Find the order in our system
            from ..models.orders import Order
            order = self.db.query(Order).filter(
                Order.provider_order_id == order_id
            ).first()

            if not order:
                error_msg = f"Order {order_id} not found in our system"
                self.logger.error(error_msg)
                if processing_log:
                    processing_log.status = "failed"
                    processing_log.error_message = error_msg
                    processing_log.completed_at = now_ist()
                return {"success": False, "error": error_msg}

            # Check if payment already processed
            from ..models.payments import Payment
            existing_payment = self.db.query(Payment).filter(
                Payment.provider_payment_id == payment_id
            ).first()

            if existing_payment and existing_payment.status == StatusPayment.captured.value:
                self.logger.info(f"Payment {payment_id} already processed")
                if processing_log:
                    processing_log.status = "completed"
                    processing_log.completed_at = now_ist()
                    processing_log.result_summary = f"Payment already processed: {payment_id}"
                return {"success": True, "payment_id": existing_payment.id}

            # Use the gym membership activation logic from routes
            from ..routes.gym_membership import _finalize_captured_membership

            # Call the finalization function
            result = _finalize_captured_membership(
                self.db,
                order_id,
                payment_id,
                amount_minor,
                captured_at=timestamp_to_ist(payment_data.get("created_at", int(now_ist().timestamp())))
            )

            if result.get("success"):
                if processing_log:
                    processing_log.status = "completed"
                    processing_log.completed_at = now_ist()
                    processing_log.result_summary = f"Gym membership activated: {result.get('entitlement_id')}"

                self.db.commit()
                self.logger.info(f"✅ Gym membership activated via webhook for order {order_id}")
                return result
            else:
                error_msg = result.get("error", "Unknown error during membership activation")
                self.logger.error(f"Failed to activate membership: {error_msg}")
                if processing_log:
                    processing_log.status = "failed"
                    processing_log.error_message = error_msg
                    processing_log.completed_at = now_ist()
                self.db.rollback()
                return {"success": False, "error": error_msg}

        except Exception as e:
            error_msg = f"Error processing gym payment captured: {str(e)}"
            self.logger.error(error_msg)
            self.db.rollback()

            if processing_log:
                processing_log.status = "failed"
                processing_log.error_message = error_msg
                processing_log.completed_at = now_ist()
                self.db.commit()

            return {"success": False, "error": error_msg}