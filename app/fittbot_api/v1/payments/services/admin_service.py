"""Admin service for dashboard analytics"""

from typing import Optional, Dict, Any
from datetime import date
from sqlalchemy.orm import Session
from sqlalchemy import select, func

from .base_service import BaseService
from ..models import (
    Payment, PayoutLine, PayoutBatch, FeesActuals, 
    SettlementItem, Order, Entitlement
)
from ..models.enums import StatusPayment, PayoutBatchStatus


class AdminService:
    """Service for admin dashboard analytics"""
    
    def __init__(self, db_session: Session):
        self.db = db_session
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        pass
    
    def get_dashboard_summary(self, from_date: date, to_date: date) -> Dict[str, Any]:
        """Get comprehensive dashboard summary"""
        # GMV (Gross Merchandise Value)
        gmv = self.db.execute(
            select(func.coalesce(func.sum(Payment.amount_minor), 0))
            .where(Payment.status == StatusPayment.captured)
            .where(func.date(Payment.captured_at) >= from_date)
            .where(func.date(Payment.captured_at) <= to_date)
        ).scalar_one()
        
        # Platform commission earned
        commission = self.db.execute(
            select(func.coalesce(func.sum(PayoutLine.commission_amount_minor), 0))
            .where(func.date(PayoutLine.created_at) >= from_date)
            .where(func.date(PayoutLine.created_at) <= to_date)
        ).scalar_one()
        
        # Gateway fees paid
        gateway_fees = self.db.execute(
            select(func.coalesce(func.sum(FeesActuals.gateway_fee_minor + FeesActuals.tax_on_fees_minor), 0))
            .where(func.date(FeesActuals.recorded_at) >= from_date)
            .where(func.date(FeesActuals.recorded_at) <= to_date)
        ).scalar_one()
        
        # Payout fees paid
        payout_fees = self.db.execute(
            select(func.coalesce(func.sum(PayoutBatch.fee_actual_minor + PayoutBatch.tax_on_fee_minor), 0))
            .where(func.date(PayoutBatch.batch_date) >= from_date)
            .where(func.date(PayoutBatch.batch_date) <= to_date)
            .where(PayoutBatch.status == PayoutBatchStatus.paid)
        ).scalar_one()
        
        # Cash inflow (settlements)
        cash_in = self.db.execute(
            select(func.coalesce(func.sum(SettlementItem.net_settled_minor), 0))
            .where(SettlementItem.settled_on >= from_date)
            .where(SettlementItem.settled_on <= to_date)
        ).scalar_one()
        
        # Cash outflow (payouts)
        cash_out = self.db.execute(
            select(func.coalesce(func.sum(PayoutBatch.total_net_amount_minor), 0))
            .where(PayoutBatch.status == PayoutBatchStatus.paid)
            .where(PayoutBatch.batch_date >= from_date)
            .where(PayoutBatch.batch_date <= to_date)
        ).scalar_one()
        
        # Net operational profit
        net_operational = int(commission) - int(gateway_fees) - int(payout_fees)
        
        return {
            "period": {"from": str(from_date), "to": str(to_date)},
            "kpis": {
                "gmv_minor": int(gmv),
                "gmv_rupees": int(gmv) / 100.0,
                "platform_commission_minor": int(commission),
                "platform_commission_rupees": int(commission) / 100.0,
                "gateway_fees_minor": int(gateway_fees),
                "gateway_fees_rupees": int(gateway_fees) / 100.0,
                "payout_fees_minor": int(payout_fees),
                "payout_fees_rupees": int(payout_fees) / 100.0,
                "net_operational_profit_minor": net_operational,
                "net_operational_profit_rupees": net_operational / 100.0,
                "cash_in_minor": int(cash_in),
                "cash_in_rupees": int(cash_in) / 100.0,
                "cash_out_minor": int(cash_out),
                "cash_out_rupees": int(cash_out) / 100.0,
            }
        }
    
    def get_gmv_analytics(self, from_date: date, to_date: date) -> Dict[str, Any]:
        """Get detailed GMV analytics"""
        # Total GMV
        total_gmv = self.db.execute(
            select(func.coalesce(func.sum(Payment.amount_minor), 0))
            .where(Payment.status == StatusPayment.captured)
            .where(func.date(Payment.captured_at) >= from_date)
            .where(func.date(Payment.captured_at) <= to_date)
        ).scalar_one()
        
        # Transaction count
        transaction_count = self.db.execute(
            select(func.count(Payment.id))
            .where(Payment.status == StatusPayment.captured)
            .where(func.date(Payment.captured_at) >= from_date)
            .where(func.date(Payment.captured_at) <= to_date)
        ).scalar_one()
        
        # Average transaction value
        avg_transaction = int(total_gmv) / max(transaction_count, 1)
        
        return {
            "period": {"from": str(from_date), "to": str(to_date)},
            "gmv": {
                "total_minor": int(total_gmv),
                "total_rupees": int(total_gmv) / 100.0,
                "transaction_count": transaction_count,
                "average_transaction_minor": int(avg_transaction),
                "average_transaction_rupees": avg_transaction / 100.0
            }
        }
    
    def get_commission_analytics(
        self, 
        from_date: date, 
        to_date: date, 
        gym_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """Get commission analytics"""
        query = select(func.coalesce(func.sum(PayoutLine.commission_amount_minor), 0))
        query = query.where(func.date(PayoutLine.created_at) >= from_date)
        query = query.where(func.date(PayoutLine.created_at) <= to_date)
        
        if gym_id:
            query = query.where(PayoutLine.gym_id == gym_id)
        
        total_commission = self.db.execute(query).scalar_one()
        
        return {
            "period": {"from": str(from_date), "to": str(to_date)},
            "gym_id": gym_id,
            "commission": {
                "total_minor": int(total_commission),
                "total_rupees": int(total_commission) / 100.0
            }
        }
    
    def get_payout_analytics(
        self,
        from_date: date,
        to_date: date,
        gym_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """Get payout analytics"""
        query = select(func.coalesce(func.sum(PayoutBatch.total_net_amount_minor), 0))
        query = query.where(PayoutBatch.batch_date >= from_date)
        query = query.where(PayoutBatch.batch_date <= to_date)
        query = query.where(PayoutBatch.status == PayoutBatchStatus.paid)
        
        if gym_id:
            query = query.where(PayoutBatch.gym_id == gym_id)
        
        total_payouts = self.db.execute(query).scalar_one()
        
        # Count of payout batches
        count_query = select(func.count(PayoutBatch.id))
        count_query = count_query.where(PayoutBatch.batch_date >= from_date)
        count_query = count_query.where(PayoutBatch.batch_date <= to_date)
        count_query = count_query.where(PayoutBatch.status == PayoutBatchStatus.paid)
        
        if gym_id:
            count_query = count_query.where(PayoutBatch.gym_id == gym_id)
        
        batch_count = self.db.execute(count_query).scalar_one()
        
        return {
            "period": {"from": str(from_date), "to": str(to_date)},
            "gym_id": gym_id,
            "payouts": {
                "total_minor": int(total_payouts),
                "total_rupees": int(total_payouts) / 100.0,
                "batch_count": batch_count,
                "average_batch_minor": int(total_payouts) / max(batch_count, 1),
                "average_batch_rupees": (int(total_payouts) / max(batch_count, 1)) / 100.0
            }
        }