"""Payout service for gym payment processing"""

from typing import List, Optional, Dict
from datetime import date, datetime, timezone
from sqlalchemy.orm import Session
from sqlalchemy import select

from .base_service import BaseService
from ..models import PayoutBatch, PayoutLine, PayoutEvent
from ..models.enums import StatusPayoutLine, PayoutMode, PayoutBatchStatus
from ..config.settings import get_payment_settings


class PayoutService(BaseService[PayoutBatch]):
    """Service for payout processing"""
    
    def __init__(self, db_session: Optional[Session] = None):
        super().__init__(PayoutBatch, db_session)
        self.settings = get_payment_settings()
    
    def process_payouts(self, payout_date: date, payout_mode: PayoutMode) -> List[PayoutBatch]:
        """Process payouts for a specific date"""
        # Get pending payout lines for the date
        pending_lines = self.db.execute(
            select(PayoutLine)
            .where(PayoutLine.status == StatusPayoutLine.pending)
            .where(PayoutLine.scheduled_for == payout_date)
        ).scalars().all()
        
        if not pending_lines:
            return []
        
        # Group by gym
        gym_lines: Dict[str, List[PayoutLine]] = {}
        for line in pending_lines:
            gym_lines.setdefault(line.gym_id, []).append(line)
        
        # Create batches
        batches = []
        for gym_id, lines in gym_lines.items():
            total_net = sum(line.net_amount_minor for line in lines)
            
            # Skip if below threshold
            if total_net < self.settings.payout_batch_threshold_minor:
                continue
            
            batch_id = f"pb_{gym_id}_{payout_date}"
            batch = PayoutBatch(
                id=batch_id,
                batch_date=payout_date,
                gym_id=gym_id,
                total_net_amount_minor=total_net,
                payout_mode=payout_mode.value,
                status=PayoutBatchStatus.processing,
                created_at=datetime.now(timezone.utc),
                updated_at=datetime.now(timezone.utc)
            )
            
            self.db.add(batch)
            
            # Update lines
            for line in lines:
                line.status = StatusPayoutLine.batched
                line.batch_id = batch_id
                line.updated_at = datetime.now(timezone.utc)
            
            batches.append(batch)
        
        self.db.commit()
        
        # Refresh batches
        for batch in batches:
            self.db.refresh(batch)
        
        return batches
    
    def get_batch_by_id(self, batch_id: str) -> Optional[PayoutBatch]:
        """Get payout batch by ID"""
        return self.get_by_id(batch_id)
    
    def get_gym_payouts(self, gym_id: str, limit: int = 20) -> List[PayoutBatch]:
        """Get payout batches for a gym"""
        query = (
            select(PayoutBatch)
            .where(PayoutBatch.gym_id == gym_id)
            .order_by(PayoutBatch.batch_date.desc())
            .limit(limit)
        )
        
        return list(self.db.execute(query).scalars().all())
    
    def update_batch_status(
        self,
        batch_id: str,
        status: PayoutBatchStatus,
        provider_ref: Optional[str] = None,
        fee_actual_minor: int = 0,
        tax_on_fee_minor: int = 0
    ) -> Optional[PayoutBatch]:
        """Update payout batch status"""
        batch = self.get_by_id(batch_id)
        if not batch:
            return None
        
        batch.status = status
        batch.updated_at = datetime.now(timezone.utc)
        
        if provider_ref:
            batch.provider_ref = provider_ref
        
        if fee_actual_minor > 0:
            batch.fee_actual_minor = fee_actual_minor
        
        if tax_on_fee_minor > 0:
            batch.tax_on_fee_minor = tax_on_fee_minor
        
        self.db.commit()
        self.db.refresh(batch)
        return batch