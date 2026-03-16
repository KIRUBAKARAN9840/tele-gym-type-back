"""Refund management routes"""

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from ..config.database import get_db_session
from ..services.refund_service import RefundService
from ..schemas.refunds import RefundCreateRequest, RefundResponse

router = APIRouter(prefix="/refunds", tags=["Refunds"])


@router.post("/create", response_model=RefundResponse)
async def create_refund(
    payload: RefundCreateRequest,
    db: Session = Depends(get_db_session)
):
    """Create a new refund"""
    try:
        with RefundService(db) as refund_service:
            refund = refund_service.create_refund(payload)
            
            return RefundResponse(refund_id=refund.id)
    
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to create refund: {str(e)}")


@router.get("/{refund_id}")
async def get_refund(
    refund_id: str,
    db: Session = Depends(get_db_session)
):
    """Get refund details"""
    with RefundService(db) as refund_service:
        refund = refund_service.get_by_id(refund_id)
        
        if not refund:
            raise HTTPException(status_code=404, detail="Refund not found")
        
        return {
            "id": refund.id,
            "payment_id": refund.payment_id,
            "amount_minor": refund.amount_minor,
            "status": refund.status,
            "reason": refund.reason,
            "processed_at": refund.processed_at
        }