"""Payment service for payment processing"""

from typing import List, Optional
from datetime import datetime, timezone
from sqlalchemy.orm import Session
from sqlalchemy import select

from .base_service import BaseService
from .entitlement_service import EntitlementService
from .order_service import OrderService
from ..models import Payment, Order
from ..models.enums import StatusPayment, StatusOrder
from ..schemas.payments import VerifyPaymentRequest


class PaymentService(BaseService[Payment]):
    """Service for payment processing"""
    
    def __init__(self, db_session: Optional[Session] = None):
        super().__init__(Payment, db_session)
        self.order_service = OrderService(db_session)
        self.entitlement_service = EntitlementService(db_session)
    
    def verify_payment(self, request: VerifyPaymentRequest) -> Payment:
        """Verify and capture a payment"""
        # Get the order
        order = self.order_service.get_by_id(request.order_id)
        if not order:
            raise ValueError("Order not found")
        
        # Check if payment already exists
        existing_payment = self.get_payment_by_id(request.payment_id)
        if existing_payment:
            raise ValueError("Payment already exists")
        
        # Create payment record
        now = request.captured_at or datetime.now(timezone.utc)
        payment = Payment(
            id=request.payment_id,
            order_id=request.order_id,
            customer_id=request.customer_id,
            amount_minor=request.amount_minor,
            currency=order.currency,
            provider=request.provider,
            provider_payment_id=request.provider_payment_id or request.payment_id,
            status=StatusPayment.captured,
            authorized_at=now,
            captured_at=now,
            created_at=now,
            updated_at=now
        )
        
        self.db.add(payment)
        
        # Update order status
        self.order_service.update_order_status(request.order_id, StatusOrder.paid)
        
        # Create entitlements
        entitlements = self.entitlement_service.create_entitlements_from_order(order)
        
        self.db.commit()
        self.db.refresh(payment)
        
        return payment
    
    def get_payment_by_id(self, payment_id: str) -> Optional[Payment]:
        """Get payment by ID"""
        return self.get_by_id(payment_id)
    
    def get_payments_by_order(self, order_id: str) -> List[Payment]:
        """Get all payments for an order"""
        return list(self.db.execute(
            select(Payment).where(Payment.order_id == order_id)
        ).scalars().all())
    
    def get_payments_by_customer(
        self, 
        customer_id: str, 
        limit: Optional[int] = None
    ) -> List[Payment]:
        """Get payments for a customer"""
        query = select(Payment).where(Payment.customer_id == customer_id)
        query = query.order_by(Payment.created_at.desc())
        
        if limit:
            query = query.limit(limit)
        
        return list(self.db.execute(query).scalars().all())
    
    def mark_payment_failed(
        self, 
        payment_id: str, 
        reason: Optional[str] = None
    ) -> Optional[Payment]:
        """Mark a payment as failed"""
        payment = self.get_by_id(payment_id)
        if not payment:
            return None
        
        payment.status = StatusPayment.failed
        payment.failed_at = datetime.now(timezone.utc)
        payment.updated_at = datetime.now(timezone.utc)
        
        if reason:
            if not payment.payment_metadata:
                payment.payment_metadata = {}
            payment.payment_metadata['failure_reason'] = reason
        
        self.db.commit()
        self.db.refresh(payment)
        return payment