"""Check-in management routes"""

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from ..config.database import get_db_session
from ..services.checkin_service import CheckinService
from ..schemas.checkins import ScanRequest, CheckinResponse

router = APIRouter(prefix="/checkins", tags=["Check-ins"])


@router.post("/scan", response_model=dict)
async def checkin_scan(
    payload: ScanRequest,
    db: Session = Depends(get_db_session)
):
    """Process gym check-in scan"""
    try:
        with CheckinService(db) as checkin_service:
            result = checkin_service.process_scan(payload)
            
            return {
                "checkin_id": result["checkin_id"],
                "payout_line_id": result.get("payout_line_id")
            }
    
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail="Failed to process check-in")


@router.get("/gym/{gym_id}")
async def get_gym_checkins(
    gym_id: str,
    limit: int = 50,
    db: Session = Depends(get_db_session)
):
    """Get recent check-ins for a gym"""
    with CheckinService(db) as checkin_service:
        checkins = checkin_service.get_gym_checkins(gym_id, limit=limit)
        
        return [CheckinResponse.from_orm(checkin) for checkin in checkins]


@router.get("/customer/{customer_id}")
async def get_customer_checkins(
    customer_id: str,
    limit: int = 50,
    db: Session = Depends(get_db_session)
):
    """Get check-in history for a customer"""
    with CheckinService(db) as checkin_service:
        checkins = checkin_service.get_customer_checkins(customer_id, limit=limit)
        
        return [CheckinResponse.from_orm(checkin) for checkin in checkins]