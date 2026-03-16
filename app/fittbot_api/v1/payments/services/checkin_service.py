"""Check-in service for gym visit processing"""

from typing import List, Optional, Dict, Any
from datetime import datetime, timezone
from sqlalchemy.orm import Session
from sqlalchemy import select

from .base_service import BaseService
from .commission_service import CommissionService
from ..models import Checkin, Entitlement, OrderItem, PayoutLine
from ..models.enums import StatusEnt, StatusCheckin, StatusPayoutLine
from ..schemas.checkins import ScanRequest
from ..config.settings import get_payment_settings


class CheckinService(BaseService[Checkin]):
    """Service for check-in processing"""
    
    def __init__(self, db_session: Optional[Session] = None):
        super().__init__(Checkin, db_session)
        self.commission_service = CommissionService(db_session)
        self.settings = get_payment_settings()
    
    def process_scan(self, request: ScanRequest) -> Dict[str, Any]:
        """Process gym check-in scan and create payout line"""
        # Get entitlement
        entitlement = self.db.get(Entitlement, request.entitlement_id)
        if not entitlement:
            raise ValueError("Entitlement not found")
        
        # Validate entitlement
        if entitlement.status != StatusEnt.pending:
            raise ValueError(f"Entitlement not pending (current status: {entitlement.status})")
        
        if entitlement.gym_id != request.gym_id:
            raise ValueError("Entitlement not valid for this gym")
        
        # Check for duplicate check-in
        existing_checkin = self.db.execute(
            select(Checkin).where(Checkin.entitlement_id == request.entitlement_id)
        ).scalar_one_or_none()
        
        if existing_checkin:
            raise ValueError("Entitlement already used")
        
        # Create check-in record
        now = request.scan_at.astimezone(timezone.utc)
        checkin = Checkin(
            id=f"chk_{entitlement.id}",
            entitlement_id=entitlement.id,
            gym_id=request.gym_id,
            customer_id=entitlement.customer_id,
            scanned_at=now,
            status=StatusCheckin.ok,
            created_at=now
        )
        
        # Update entitlement status
        entitlement.status = StatusEnt.used
        entitlement.updated_at = now
        
        # Create payout line
        payout_line = self._create_payout_line(entitlement, now)
        
        # Save all changes
        self.db.add(checkin)
        self.db.add(payout_line)
        self.db.commit()
        
        self.db.refresh(checkin)
        self.db.refresh(payout_line)
        
        return {
            "checkin_id": checkin.id,
            "payout_line_id": payout_line.id
        }
    
    def _create_payout_line(self, entitlement: Entitlement, scan_time: datetime) -> PayoutLine:
        """Create payout line for the entitlement"""
        # Get order item for pricing
        order_item = self.db.get(OrderItem, entitlement.order_item_id)
        if not order_item:
            raise ValueError("Order item not found")
        
        # Calculate base amount (what gym should receive)
        base_amount_minor = order_item.unit_price_minor
        
        # Calculate commission
        commission_minor, applied_pct, applied_fixed = self.commission_service.compute_commission_for_unit(
            gym_id=entitlement.gym_id,
            sku=order_item.sku,
            base_amount_minor=base_amount_minor,
            calculation_date=scan_time.astimezone(self.settings.ist_timezone).date()
        )
        
        # Net amount to gym (full amount in this case)
        net_amount_minor = base_amount_minor
        
        payout_line = PayoutLine(
            id=f"pl_{entitlement.id}",
            entitlement_id=entitlement.id,
            gym_id=entitlement.gym_id or "",
            gross_amount_minor=base_amount_minor,
            commission_amount_minor=commission_minor,
            net_amount_minor=net_amount_minor,
            applied_commission_pct=applied_pct,
            applied_commission_fixed_minor=applied_fixed,
            payout_fee_allocated_minor=0,
            status=StatusPayoutLine.pending,
            scheduled_for=scan_time.astimezone(self.settings.ist_timezone).date()
        )
        
        return payout_line
    
    def get_gym_checkins(self, gym_id: str, limit: int = 50) -> List[Checkin]:
        """Get recent check-ins for a gym"""
        query = (
            select(Checkin)
            .where(Checkin.gym_id == gym_id)
            .order_by(Checkin.scanned_at.desc())
            .limit(limit)
        )
        
        return list(self.db.execute(query).scalars().all())
    
    def get_customer_checkins(self, customer_id: str, limit: int = 50) -> List[Checkin]:
        """Get check-in history for a customer"""
        query = (
            select(Checkin)
            .where(Checkin.customer_id == customer_id)
            .order_by(Checkin.scanned_at.desc())
            .limit(limit)
        )
        
        return list(self.db.execute(query).scalars().all())