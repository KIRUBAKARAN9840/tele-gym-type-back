"""Settlement reconciliation service"""

from typing import Optional
from datetime import datetime, timezone, date
from sqlalchemy.orm import Session
from sqlalchemy import select

from .base_service import BaseService
from ..models import Settlement, SettlementItem, Payment, FeesActuals
from ..schemas.settlements import ReconImportRequest


class SettlementService(BaseService[Settlement]):
    """Service for settlement reconciliation"""
    
    def __init__(self, db_session: Optional[Session] = None):
        super().__init__(Settlement, db_session)
    
    def import_settlement(self, request: ReconImportRequest) -> Settlement:
        """Import settlement reconciliation data"""
        # Calculate totals
        gross_total = sum(int(item["gross"]) for item in request.items)
        fee_total = sum(int(item["fee"]) for item in request.items)
        tax_total = sum(int(item["tax"]) for item in request.items)
        net_total = sum(int(item["net"]) for item in request.items)
        
        # Create or update settlement
        settlement_id = f"stl_{request.provider}_{request.settlement_date}"
        settlement = self.get_by_id(settlement_id)
        
        if not settlement:
            settlement = Settlement(
                id=settlement_id,
                provider=request.provider.value,
                settlement_date=request.settlement_date,
                gross_captured_minor=gross_total,
                mdr_amount_minor=fee_total,
                tax_on_mdr_minor=tax_total,
                net_settled_minor=net_total,
                payload_json={"count": len(request.items)},
                created_at=datetime.now(timezone.utc),
                updated_at=datetime.now(timezone.utc)
            )
            self.db.add(settlement)
        else:
            # Update existing settlement
            settlement.gross_captured_minor = gross_total
            settlement.mdr_amount_minor = fee_total
            settlement.tax_on_mdr_minor = tax_total
            settlement.net_settled_minor = net_total
            settlement.updated_at = datetime.now(timezone.utc)
        
        # Process settlement items
        for item_data in request.items:
            self._process_settlement_item(settlement.id, item_data)
        
        self.db.commit()
        self.db.refresh(settlement)
        return settlement
    
    def _process_settlement_item(self, settlement_id: str, item_data: dict):
        """Process individual settlement item"""
        provider_payment_id = item_data["provider_payment_id"]
        item_id = f"sti_{provider_payment_id}"
        
        # Create or update settlement item
        settlement_item = self.db.get(SettlementItem, item_id)
        
        if not settlement_item:
            settlement_item = SettlementItem(
                id=item_id,
                settlement_id=settlement_id,
                provider_payment_id=provider_payment_id,
                gross_captured_minor=int(item_data["gross"]),
                mdr_amount_minor=int(item_data["fee"]),
                tax_on_mdr_minor=int(item_data["tax"]),
                net_settled_minor=int(item_data["net"]),
                settled_on=date.fromisoformat(item_data["settled_on"])
            )
            self.db.add(settlement_item)
        
        # Link to payment if exists
        payment = self.db.execute(
            select(Payment).where(Payment.provider_payment_id == provider_payment_id)
        ).scalar_one_or_none()
        
        if payment:
            settlement_item.payment_id = payment.id
            
            # Record gateway fees
            fees_actual = FeesActuals(
                id=f"fa_{provider_payment_id}",
                order_id=payment.order_id,
                payment_id=payment.id,
                gateway_fee_minor=settlement_item.mdr_amount_minor,
                payout_fee_minor=0,
                tax_on_fees_minor=settlement_item.tax_on_mdr_minor,
                notes="Gateway MDR+GST from settlement reconciliation",
                recorded_at=datetime.now(timezone.utc)
            )
            
            # Check if fees record already exists
            existing_fees = self.db.get(FeesActuals, fees_actual.id)
            if not existing_fees:
                self.db.add(fees_actual)