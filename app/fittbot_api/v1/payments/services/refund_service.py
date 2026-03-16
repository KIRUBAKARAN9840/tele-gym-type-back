"""Refund service for payment refund processing"""

from typing import Optional
from datetime import datetime, timezone
from sqlalchemy.orm import Session
from sqlalchemy import select

from .base_service import BaseService
from ..models import Refund, Payment, Entitlement, PayoutLine
from ..models.enums import StatusEnt, StatusPayoutLine, RefundStatus
from ..schemas.refunds import RefundCreateRequest


class RefundService(BaseService[Refund]):
    """Service for refund processing"""
    
    def __init__(self, db_session: Optional[Session] = None):
        super().__init__(Refund, db_session)
    
    def create_refund(self, request: RefundCreateRequest) -> Refund:
        """Create a new refund and handle related entities"""
        # Validate payment exists
        payment = self.db.get(Payment, request.payment_id)
        if not payment:
            raise ValueError("Payment not found")
        
        # Create refund record
        refund = Refund(
            id=request.refund_id,
            payment_id=request.payment_id,
            entitlement_id=request.entitlement_id,
            amount_minor=request.amount_minor,
            currency="INR",
            provider=request.provider.value,
            status=RefundStatus.processed,
            reason=request.reason,
            processed_at=request.processed_at or datetime.now(timezone.utc),
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc)
        )
        
        self.db.add(refund)
        
        # Handle entitlement refund
        if request.entitlement_id:
            self._handle_entitlement_refund(request.entitlement_id)
        
        self.db.commit()
        self.db.refresh(refund)
        return refund
    
    def _handle_entitlement_refund(self, entitlement_id: str):
        """Handle refund of entitlement and related payout"""
        entitlement = self.db.get(Entitlement, entitlement_id)
        if not entitlement:
            return
        
        # Update entitlement status based on current state
        if entitlement.status == StatusEnt.pending:
            entitlement.status = StatusEnt.refunded
        elif entitlement.status == StatusEnt.used:
            # Handle payout line reversal
            payout_line = self.db.execute(
                select(PayoutLine).where(PayoutLine.entitlement_id == entitlement_id)
            ).scalar_one_or_none()
            
            if payout_line:
                if payout_line.status in [StatusPayoutLine.pending, StatusPayoutLine.batched, StatusPayoutLine.failed]:
                    # Delete pending/batched payout line
                    self.db.delete(payout_line)
                elif payout_line.status == StatusPayoutLine.paid:
                    # Create negative payout line to reverse the payment
                    negative_line = PayoutLine(
                        id=f"pl_neg_{entitlement_id}",
                        entitlement_id=entitlement_id,
                        gym_id=payout_line.gym_id,
                        gross_amount_minor=-payout_line.gross_amount_minor,
                        commission_amount_minor=-payout_line.commission_amount_minor,
                        net_amount_minor=-payout_line.net_amount_minor,
                        applied_commission_pct=payout_line.applied_commission_pct,
                        applied_commission_fixed_minor=-payout_line.applied_commission_fixed_minor,
                        payout_fee_allocated_minor=0,
                        status=StatusPayoutLine.pending,
                        scheduled_for=datetime.now(timezone.utc).date()
                    )
                    self.db.add(negative_line)
        
        entitlement.updated_at = datetime.now(timezone.utc)