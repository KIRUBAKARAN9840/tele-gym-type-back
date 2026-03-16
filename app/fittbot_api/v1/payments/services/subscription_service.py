"""Subscription service for app subscription management"""

from typing import List, Optional
from datetime import datetime, timezone
from sqlalchemy.orm import Session
from sqlalchemy import select

from .base_service import BaseService
from ..models import Subscription
from ..models.enums import SubscriptionStatus


class SubscriptionService(BaseService[Subscription]):
    """Service for subscription management"""
    
    def __init__(self, db_session: Optional[Session] = None):
        super().__init__(Subscription, db_session)
    
    def get_customer_subscriptions(
        self,
        customer_id: str,
        status: Optional[SubscriptionStatus] = None
    ) -> List[Subscription]:
        """Get subscriptions for a customer"""
        query = select(Subscription).where(Subscription.customer_id == customer_id)
        
        if status:
            query = query.where(Subscription.status == status)
        
        query = query.order_by(Subscription.created_at.desc())
        
        return list(self.db.execute(query).scalars().all())
    
    def get_active_subscription(self, customer_id: str, product_id: str) -> Optional[Subscription]:
        """Get active subscription for customer and product"""
        now = datetime.now(timezone.utc)
        
        query = (
            select(Subscription)
            .where(Subscription.customer_id == customer_id)
            .where(Subscription.product_id == product_id)
            .where(Subscription.status.in_([SubscriptionStatus.active, SubscriptionStatus.renewed]))
            .where(
                (Subscription.active_from.is_(None)) | (Subscription.active_from <= now)
            )
            .where(
                (Subscription.active_until.is_(None)) | (Subscription.active_until > now)
            )
            .order_by(Subscription.created_at.desc())
        )
        
        return self.db.execute(query).scalars().first()
    
    def update_subscription_from_webhook(
        self,
        customer_id: str,
        product_id: str,
        webhook_data: dict
    ) -> Subscription:
        """Update subscription from webhook data"""
        sub_id = f"sub_{customer_id}_{product_id}"
        
        # Get or create subscription
        subscription = self.get_by_id(sub_id)
        if not subscription:
            subscription = Subscription(
                id=sub_id,
                customer_id=customer_id,
                provider="revenuecat",
                product_id=product_id,
                status=SubscriptionStatus.active,
                created_at=datetime.now(timezone.utc)
            )
            self.db.add(subscription)
        
        # Update from webhook data
        subscription.status = webhook_data.get("status", subscription.status)
        subscription.rc_original_txn_id = webhook_data.get("original_transaction_id")
        subscription.latest_txn_id = webhook_data.get("transaction_id")
        
        if webhook_data.get("period_start"):
            subscription.active_from = datetime.fromisoformat(webhook_data["period_start"])
        
        if webhook_data.get("period_end"):
            subscription.active_until = datetime.fromisoformat(webhook_data["period_end"])
        
        if webhook_data.get("trial_start"):
            subscription.trial_start = datetime.fromisoformat(webhook_data["trial_start"])
        
        if webhook_data.get("trial_end"):
            subscription.trial_end = datetime.fromisoformat(webhook_data["trial_end"])
        
        subscription.auto_renew = webhook_data.get("auto_renew", subscription.auto_renew)
        subscription.updated_at = datetime.now(timezone.utc)
        
        if subscription.status in [SubscriptionStatus.expired, SubscriptionStatus.canceled, SubscriptionStatus.revoked]:
            subscription.cancel_reason = webhook_data.get("cancel_reason")
        
        self.db.commit()
        self.db.refresh(subscription)
        return subscription