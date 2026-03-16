"""Settlement reconciliation routes"""

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from ..config.database import get_db_session
from ..services.settlement_service import SettlementService
from ..schemas.settlements import ReconImportRequest, SettlementResponse

router = APIRouter(prefix="/settlements", tags=["Settlements"])


@router.post("/import", response_model=SettlementResponse)
async def import_settlement(
    payload: ReconImportRequest,
    db: Session = Depends(get_db_session)
):
    """Import settlement reconciliation data"""
    try:
        with SettlementService(db) as settlement_service:
            settlement = settlement_service.import_settlement(payload)
            
            return SettlementResponse(
                settlement_id=settlement.id,
                items=len(payload.items)
            )
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to import settlement: {str(e)}")


@router.get("/{settlement_id}")
async def get_settlement(
    settlement_id: str,
    db: Session = Depends(get_db_session)
):
    """Get settlement details"""
    with SettlementService(db) as settlement_service:
        settlement = settlement_service.get_by_id(settlement_id)
        
        if not settlement:
            raise HTTPException(status_code=404, detail="Settlement not found")
        
        return {
            "id": settlement.id,
            "provider": settlement.provider,
            "settlement_date": settlement.settlement_date,
            "gross_captured_minor": settlement.gross_captured_minor,
            "mdr_amount_minor": settlement.mdr_amount_minor,
            "tax_on_mdr_minor": settlement.tax_on_mdr_minor,
            "net_settled_minor": settlement.net_settled_minor
        }