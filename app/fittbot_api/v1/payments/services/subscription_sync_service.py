"""
Enterprise-level Subscription Sync Service
Handles synchronization between normal flow and idempotent webhook processing
"""

import json
import logging
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any, Tuple
from sqlalchemy.orm import Session
from sqlalchemy import and_, or_
from sqlalchemy.exc import InvalidRequestError

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

logger = logging.getLogger("payments.sync_service")

# Indian Standard Time (IST) timezone
IST = timezone(timedelta(hours=5, minutes=30))

def now_ist():
    """Get current time in IST"""
    return datetime.now(IST)

def timestamp_to_ist(timestamp_ms: int) -> datetime:
    """Convert timestamp milliseconds to IST datetime"""
    return datetime.fromtimestamp(timestamp_ms / 1000, tz=IST)


class SubscriptionSyncService:
    """
    Enterprise-level service to handle subscription synchronization
    with proper idempotency and error recovery
    """
    
    def __init__(self, db: Session):
        self.db = db
        self.logger = logger

    def lock_for_update(self, query):
        """Attempt to acquire row-level lock to prevent concurrent writes"""
        try:
            return query.with_for_update()
        except (InvalidRequestError, AttributeError):
            return query
        except Exception as lock_err:
            self.logger.debug("Lock not applied: %s", lock_err)
            return query
    
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
            description=f"Auto-created from RevenueCat webhook",
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
        Process initial purchase with proper error handling and recovery
        """
        customer_id = event.get("app_user_id")
        
        # Extract product and transaction details
        product_id, transaction_id, price, purchase_date, expires_date = self.extract_purchase_details(event)
        
        if not product_id or not transaction_id:
            error_msg = f"Missing required fields: product_id={product_id}, transaction_id={transaction_id}"
            self.logger.error(error_msg)
            if processing_log:
                processing_log.status = "failed"
                processing_log.error_message = error_msg
                processing_log.completed_at = now_ist()
            return {"success": False, "error": error_msg}
        
        try:
            # Convert price to minor units
            amount_minor = int(float(price) * 100) if price else 0
            
            # Ensure catalog product exists
            self.create_or_update_catalog_product(product_id, amount_minor)

            # Check for existing subscription (fast-path verify may have created it)
            existing_subscription = self.lock_for_update(
                self.db.query(Subscription).filter(
                    Subscription.provider == Provider.google_play.value,
                    or_(
                        Subscription.rc_original_txn_id == transaction_id,
                        Subscription.latest_txn_id == transaction_id
                    )
                )
            ).first()

            if not existing_subscription:
                existing_subscription = self.lock_for_update(
                    self.db.query(Subscription)
                    .filter(
                        Subscription.provider == Provider.google_play.value,
                        Subscription.customer_id == customer_id,
                        Subscription.product_id == product_id,
                        Subscription.status.in_([
                            SubscriptionStatus.active.value,
                            SubscriptionStatus.renewed.value
                        ])
                    )
                    .order_by(Subscription.created_at.desc())
                ).first()

            if existing_subscription:
                self.logger.info(f"Customer {customer_id} already has active subscription for {product_id}")
                # Update subscription details from authoritative webhook payload
                existing_subscription.product_id = product_id or existing_subscription.product_id
                existing_subscription.latest_txn_id = transaction_id
                if not existing_subscription.rc_original_txn_id:
                    existing_subscription.rc_original_txn_id = transaction_id
                existing_subscription.status = SubscriptionStatus.active.value
                existing_subscription.auto_renew = True
                existing_subscription.active_from = purchase_date
                existing_subscription.active_until = expires_date
                self.db.add(existing_subscription)
                
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
                customer_id, transaction_id, amount_minor, "initial_purchase"
            )
            
            # Create order item
            order_item = self.create_order_item(
                order.id, product_id, amount_minor, "initial_purchase"
            )
            
            # Create or update payment
            payment = self.create_or_update_payment(
                order.id, customer_id, transaction_id, amount_minor
            )
            
            # Create subscription
            subscription = self.create_subscription(
                customer_id, product_id, transaction_id, purchase_date, expires_date
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
    
    def extract_purchase_details(self, event: Dict[str, Any]) -> Tuple:
        """Extract purchase details from various event formats"""
        product_id = None
        transaction_id = None
        price = 0
        purchase_date = now_ist()
        expires_date = now_ist() + timedelta(days=30)
        
        # Direct fields
        product_id = event.get("product_id")
        transaction_id = event.get("transaction_id")
        price = event.get("price_in_purchased_currency", 0)
        
        # Timestamps
        if event.get("purchased_at_ms"):
            purchase_date = timestamp_to_ist(event["purchased_at_ms"])
        if event.get("expiration_at_ms"):
            expires_date = timestamp_to_ist(event["expiration_at_ms"])
        
        # Check in subscriptions object
        if not product_id or not transaction_id:
            subscriptions = event.get("subscriptions", {})
            for sub_key, sub_data in subscriptions.items():
                if sub_data:
                    product_id = product_id or sub_key
                    transaction_id = transaction_id or sub_data.get("store_transaction_id")
                    price = price or sub_data.get("price_in_purchased_currency", 0)
                    if sub_data.get("purchased_at_ms"):
                        purchase_date = timestamp_to_ist(sub_data["purchased_at_ms"])
                    if sub_data.get("expiration_at_ms"):
                        expires_date = timestamp_to_ist(sub_data["expiration_at_ms"])
                    break
        
        # Check in entitlements object
        if not product_id:
            entitlements = event.get("entitlements", {})
            for ent_key, ent_data in entitlements.items():
                if ent_data and "product_identifier" in ent_data:
                    product_id = ent_data["product_identifier"]
                    break
        
        return product_id, transaction_id, price, purchase_date, expires_date
    
    def create_or_update_order(
        self,
        customer_id: str,
        transaction_id: str,
        amount_minor: int,
        order_type: str = "initial",
        currency: str = "INR"
    ) -> Order:
        """Create or update order"""
        existing_order = self.lock_for_update(
            self.db.query(Order).filter(
                Order.customer_id == customer_id,
                Order.status == StatusOrder.pending.value,
                Order.provider == Provider.google_play.value
            ).order_by(Order.created_at.desc())
        ).first()
        
        if existing_order:
            existing_order.status = StatusOrder.paid.value
            existing_order.provider_order_id = transaction_id
            if amount_minor and amount_minor > 0:
                existing_order.gross_amount_minor = amount_minor
            if currency:
                existing_order.currency = currency
            self.db.add(existing_order)
            self.logger.info(f"Updated existing order {existing_order.id}")
            return existing_order
        
        order_id = self.generate_id(f"ord_{order_type[:2].upper()}")
        order = Order(
            id=order_id,
            customer_id=customer_id,
            currency=currency or "INR",
            provider=Provider.google_play.value,
            provider_order_id=transaction_id,
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
                "item_type": item_type
            }
        )
        self.db.add(order_item)
        self.db.flush()
        return order_item
    
    def create_or_update_payment(
        self,
        order_id: str,
        customer_id: str,
        transaction_id: str,
        amount_minor: int,
        currency: str = "INR"
    ) -> Payment:
        """Create or update payment"""
        existing_payment = self.lock_for_update(
            self.db.query(Payment).filter(
                Payment.provider_payment_id == transaction_id,
                Payment.customer_id == customer_id
            )
        ).first()
        
        if existing_payment:
            updated = False
            if existing_payment.status != StatusPayment.captured.value:
                existing_payment.status = StatusPayment.captured.value
                existing_payment.captured_at = now_ist()
                updated = True
            if amount_minor and amount_minor > 0 and existing_payment.amount_minor != amount_minor:
                existing_payment.amount_minor = amount_minor
                updated = True
            if currency and existing_payment.currency != currency:
                existing_payment.currency = currency
                updated = True
            if order_id and existing_payment.order_id != order_id:
                existing_payment.order_id = order_id
                updated = True
            if updated:
                self.db.add(existing_payment)
            self.logger.info(f"Updated existing payment {existing_payment.id}")
            return existing_payment
        
        payment_id = self.generate_id("pay")
        payment = Payment(
            id=payment_id,
            order_id=order_id,
            customer_id=customer_id,
            amount_minor=amount_minor,
            currency=currency or "INR",
            provider=Provider.google_play.value,
            provider_payment_id=transaction_id,
            status=StatusPayment.captured.value,
            authorized_at=now_ist(),
            captured_at=now_ist(),
            payment_metadata={
                "transaction_id": transaction_id,
                "provider": "google_play"
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
        transaction_id: str,
        purchase_date: datetime,
        expires_date: datetime
    ) -> Subscription:
        """Create subscription"""
        subscription_id = self.generate_id("sub")
        subscription = Subscription(
            id=subscription_id,
            customer_id=customer_id,
            provider=Provider.google_play.value,
            product_id=product_id,
            status=SubscriptionStatus.active.value,
            auto_renew=True,
            active_from=purchase_date,
            active_until=expires_date,
            rc_original_txn_id=transaction_id,
            latest_txn_id=transaction_id
        )
        self.db.add(subscription)
        self.db.flush()
        self.logger.info(f"Created subscription {subscription_id}")
        return subscription
    
    def ensure_active_entitlement(
        self,
        customer_id: str,
        expires_date: datetime,
        order_item_id: str = None
    ) -> Entitlement:
        """Ensure customer has active entitlement"""
        # Check for existing entitlement
        existing_entitlement = self.lock_for_update(
            self.db.query(Entitlement).filter(
                Entitlement.customer_id == customer_id,
                Entitlement.entitlement_type == EntType.app.value
            )
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
        """Process subscription renewal"""
        customer_id = event.get("app_user_id")
        product_id = event.get("product_id")
        transaction_id = event.get("transaction_id")
        expiration_at_ms = event.get("expiration_at_ms", 0)
        purchased_at_ms = event.get("purchased_at_ms", 0)
        price = event.get("price_in_purchased_currency", 0)

        # 🔍 DEBUG: Log renewal timestamp conversion
        self.logger.info(f"🔄 RENEWAL - Converting timestamps:")
        self.logger.info(f"   - purchased_at_ms: {purchased_at_ms}")
        self.logger.info(f"   - expiration_at_ms: {expiration_at_ms}")

        if not product_id or not transaction_id:
            error_msg = f"Missing required fields for renewal"
            self.logger.error(error_msg)
            if processing_log:
                processing_log.status = "failed"
                processing_log.error_message = error_msg
                processing_log.completed_at = now_ist()
            return {"success": False, "error": error_msg}

        try:
            amount_minor = int(float(price) * 100) if price else 0
            purchased_date = timestamp_to_ist(purchased_at_ms) if purchased_at_ms else now_ist()
            expires_date = timestamp_to_ist(expiration_at_ms) if expiration_at_ms else now_ist() + timedelta(days=30)

            # 🔍 DEBUG: Log converted timestamps
            self.logger.info(f"   ✅ Converted purchased_date (IST): {purchased_date.isoformat()}")
            self.logger.info(f"   ✅ Converted expires_date (IST): {expires_date.isoformat()}")
            
            # Find existing subscription
            subscription = self.lock_for_update(
                self.db.query(Subscription).filter(
                    Subscription.customer_id == customer_id,
                    Subscription.product_id == product_id,
                    Subscription.provider == Provider.google_play.value,
                    Subscription.status.in_([
                        SubscriptionStatus.active.value,
                        SubscriptionStatus.renewed.value
                    ])
                ).order_by(Subscription.created_at.desc())
            ).first()
            
            if subscription:
                # Update subscription
                subscription.status = SubscriptionStatus.active.value
                subscription.latest_txn_id = transaction_id
                subscription.active_until = expires_date
                subscription.auto_renew = True

                # 🔍 DEBUG: Verify subscription update
                self.logger.info(f"🗄️ Updated subscription {subscription.id}:")
                self.logger.info(f"   - active_until set to: {subscription.active_until.isoformat()}")

                # Create renewal order
                order = self.create_or_update_order(
                    customer_id, transaction_id, amount_minor, "renewal"
                )

                # 🔍 DEBUG: Check order timestamps
                self.logger.info(f"📝 Order {order.id} timestamps:")
                self.logger.info(f"   - created_at: {order.created_at}")
                self.logger.info(f"   - updated_at: {order.updated_at}")
                
                # Create order item
                self.create_order_item(
                    order.id, product_id, amount_minor, "renewal"
                )
                
                # Create payment
                payment = self.create_or_update_payment(
                    order.id, customer_id, transaction_id, amount_minor
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
        """Process subscription cancellation"""
        customer_id = event.get("app_user_id")
        product_id = event.get("product_id")
        cancelled_at_ms = event.get("cancelled_at_ms")

        # 🔍 DEBUG: Log cancellation
        self.logger.info(f"❌ CANCELLATION for customer {customer_id}, product {product_id}")
        if cancelled_at_ms:
            cancelled_date = timestamp_to_ist(cancelled_at_ms)
            self.logger.info(f"   - Cancelled at (IST): {cancelled_date.isoformat()}")

        try:
            # Find subscription
            subscription = self.db.query(Subscription).filter(
                Subscription.customer_id == customer_id,
                Subscription.product_id == product_id,
                Subscription.provider == Provider.google_play.value,
                Subscription.status.in_([
                    SubscriptionStatus.active.value,
                    SubscriptionStatus.renewed.value,
                    SubscriptionStatus.canceled.value
                ])
            ).order_by(Subscription.created_at.desc()).first()
            
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
        """Process subscription expiration"""
        customer_id = event.get("app_user_id")
        product_id = event.get("product_id")
        expiration_at_ms = event.get("expiration_at_ms", 0)
        expiration_reason = event.get("expiration_reason", "Unknown")
        transaction_id = (
            event.get("transaction_id")
            or event.get("store_transaction_id")
            or event.get("original_transaction_id")
        )

        # 🔍 DEBUG: Log expiration
        self.logger.info(f"⏰ EXPIRATION for customer {customer_id}, product {product_id}")
        self.logger.info(f"   - Expiration reason: {expiration_reason}")
        self.logger.info(f"   - expiration_at_ms: {expiration_at_ms}")

        try:
            expires_date = timestamp_to_ist(expiration_at_ms) if expiration_at_ms else now_ist()
            expires_date = self.ensure_timezone_aware(expires_date)

            # 🔍 DEBUG: Log converted timestamp
            self.logger.info(f"   ✅ Converted expires_date (IST): {expires_date.isoformat()}")
            
            # Find subscription
            subscription = self.lock_for_update(
                self.db.query(Subscription).filter(
                    Subscription.customer_id == customer_id,
                    Subscription.product_id == product_id,
                    Subscription.provider == Provider.google_play.value,
                    Subscription.status.in_([
                        SubscriptionStatus.active.value,
                        SubscriptionStatus.renewed.value,
                        SubscriptionStatus.canceled.value,
                        SubscriptionStatus.expired.value
                    ])
                ).order_by(Subscription.created_at.desc())
            ).first()

            if subscription:
                subscription.active_until = self.ensure_timezone_aware(subscription.active_until)
                subscription.active_from = self.ensure_timezone_aware(subscription.active_from)

            if subscription:
                if subscription.latest_txn_id and transaction_id and subscription.latest_txn_id != transaction_id:
                    if subscription.active_until and subscription.active_until > expires_date:
                        self.logger.info(
                            "Skipping expiration event for %s: txn %s is not latest (%s) and active_until=%s",
                            subscription.id,
                            transaction_id,
                            subscription.latest_txn_id,
                            subscription.active_until.isoformat()
                        )
                        if processing_log:
                            processing_log.status = "completed"
                            processing_log.completed_at = now_ist()
                            processing_log.result_summary = (
                                f"Ignored stale expiration for subscription {subscription.id}"
                            )
                        self.db.commit()
                        return {"success": True, "ignored": "stale_expiration"}
                if subscription.active_until and expires_date < subscription.active_until and not transaction_id:
                    self.logger.info(
                        "Skipping expiration without txn id for %s: expires_date %s older than active_until %s",
                        subscription.id,
                        expires_date.isoformat(),
                        subscription.active_until.isoformat()
                    )
                    if processing_log:
                        processing_log.status = "completed"
                        processing_log.completed_at = now_ist()
                        processing_log.result_summary = (
                            f"Ignored stale expiration for subscription {subscription.id}"
                        )
                    self.db.commit()
                    return {"success": True, "ignored": "stale_expiration"}
            
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
                    processing_log.result_summary = f"Expired subscription {subscription.id}, reason: {expiration_reason}"
                
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
