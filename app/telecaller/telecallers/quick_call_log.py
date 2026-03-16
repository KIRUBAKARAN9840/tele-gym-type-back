from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from sqlalchemy import and_
from app.models.database import get_db
from app.models.telecaller_models import GymAssignment, GymCallLogs, Telecaller,GymDatabase
from app.telecaller.dependencies import get_current_telecaller
from datetime import datetime, timedelta
from pydantic import BaseModel
from typing import Optional
import pytz

router = APIRouter()

class QuickCallLogRequest(BaseModel):
    action: str  # 'interested', 'not_interested', 'follow_up', 'rejected', 'converted', 'no_response'
    notes: Optional[str] = None
    follow_up_date: Optional[datetime] = None

@router.post("/gyms/{gym_id}/call", status_code=status.HTTP_201_CREATED)
async def log_quick_call(
    gym_id: int,
    log_data: QuickCallLogRequest,
    db: Session = Depends(get_db),
    current_telecaller: Telecaller = Depends(get_current_telecaller)
):
    """Log a quick call for a gym"""
    try:
        # Verify gym exists
        gym = db.query(GymDatabase).filter(GymDatabase.id == gym_id).first()
        if not gym:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Gym not found"
            )

        # Verify gym is assigned to this telecaller
        assignment = db.query(GymAssignment).filter(
            and_(
                GymAssignment.gym_id == gym_id,
                GymAssignment.telecaller_id == current_telecaller.id,
                GymAssignment.status == "active"
            )
        ).first()

        if not assignment:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Gym is not assigned to this telecaller"
            )

        # Map frontend action to backend status
        status_mapping = {
            'interested': 'interested',
            'not_interested': 'not_interested',
            'follow_up': 'follow_up',
            'rejected': 'rejected',
            'converted': 'converted',
            'no_response': 'no_response'
        }

        call_status = status_mapping.get(log_data.action)
        if not call_status:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid action: {log_data.action}. Must be one of: {list(status_mapping.keys())}"
            )

        # Validation based on requirements
        if log_data.action in ['converted', 'rejected'] and not log_data.notes:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Notes are required for converted and rejected statuses"
            )

        if log_data.action in ['follow_up', 'no_response']:
            if not log_data.follow_up_date:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Follow-up date is required for follow-up and no_response statuses"
                )

        # Create call log entry with IST time
        ist_tz = pytz.timezone('Asia/Kolkata')
        ist_now = datetime.now(ist_tz)
        # Store as UTC in database but the actual time is IST
        utc_time = ist_now.astimezone(pytz.UTC).replace(tzinfo=None)

        call_log = GymCallLogs(
            gym_id=gym_id,
            telecaller_id=current_telecaller.id,
            manager_id=current_telecaller.manager_id,
            call_status=call_status,
            remarks=log_data.notes,
            follow_up_date=log_data.follow_up_date if log_data.action in ['follow_up', 'no_response'] else None,
            created_at=utc_time
        )

        db.add(call_log)
        db.commit()
        db.refresh(call_log)

        return {
            "message": "Call logged successfully",
            "call_log": {
                "id": call_log.id,
                "gym_id": call_log.gym_id,
                "call_status": call_log.call_status,
                "remarks": call_log.remarks,
                "follow_up_date": call_log.follow_up_date,
                "created_at": call_log.created_at
            }
        }

    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to log call: {str(e)}"
        )