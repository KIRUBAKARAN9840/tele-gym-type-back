"""Payout management routes"""

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from ..config.database import get_db_session
from ..services.payout_service import PayoutService
from ..schemas.payouts import PayoutRunRequest, PayoutBatchResponse

router = APIRouter(prefix="/payouts", tags=["Payouts"])


@router.post("/run", response_model=PayoutBatchResponse)
async def run_payouts(
    payload: PayoutRunRequest,
    db: Session = Depends(get_db_session)
):
    """Run payout processing for a specific date"""
    try:
        with PayoutService(db) as payout_service:
            batches = payout_service.process_payouts(payload.date, payload.payout_mode)
            
            return PayoutBatchResponse(
                batches=[batch.id for batch in batches],
                status="queued_for_provider"
            )
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to run payouts: {str(e)}")


@router.get("/batch/{batch_id}")
async def get_payout_batch(
    batch_id: str,
    db: Session = Depends(get_db_session)
):
    """Get payout batch details"""
    with PayoutService(db) as payout_service:
        batch = payout_service.get_batch_by_id(batch_id)
        
        if not batch:
            raise HTTPException(status_code=404, detail="Payout batch not found")
        
        return {
            "id": batch.id,
            "gym_id": batch.gym_id,
            "batch_date": batch.batch_date,
            "total_net_amount_minor": batch.total_net_amount_minor,
            "status": batch.status,
            "payout_mode": batch.payout_mode
        }


@router.get("/gym/{gym_id}")
async def get_gym_payouts(
    gym_id: str,
    limit: int = 20,
    db: Session = Depends(get_db_session)
):
    """Get payout batches for a gym"""
    with PayoutService(db) as payout_service:
        batches = payout_service.get_gym_payouts(gym_id, limit=limit)
        
        return [
            {
                "id": batch.id,
                "batch_date": batch.batch_date,
                "total_net_amount_minor": batch.total_net_amount_minor,
                "status": batch.status,
                "payout_mode": batch.payout_mode
            }
            for batch in batches
        ]