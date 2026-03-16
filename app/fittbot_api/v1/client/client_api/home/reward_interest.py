# app/fittbot_api/v1/client/client_api/home/reward_interest.py

from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy.orm import Session
from datetime import datetime, timedelta

from app.models.database import get_db
from app.models.fittbot_models import RewardInterest
from app.utils.logging_utils import FittbotHTTPException

router = APIRouter(prefix="/reward_interest", tags=["Reward Interest"])


@router.get("/get_interest")
async def get_reward_interest(client_id: int, db: Session = Depends(get_db)):
    try:
        data = db.query(RewardInterest).filter(RewardInterest.client_id == client_id).first()
        interest = data.interested if data else False

        return {
            "status": 200,
            "message": "Data retrieved successfully",
            "data": interest,
        }
    except FittbotHTTPException:
        raise
    except Exception as e:
        raise FittbotHTTPException(
            status_code=500,
            detail=f"An error occurred, {str(e)}",
            error_code="REWARD_INTEREST_GET_ERROR",
            log_data={"client_id": client_id, "error": str(e)},
        )


class InterestRequest(BaseModel):
    client_id: int


@router.post("/show_interest")
async def add_interest(request: InterestRequest, db: Session = Depends(get_db)):
    try:
        client_id = request.client_id
        interest = True

        existing = (
            db.query(RewardInterest).filter(RewardInterest.client_id == client_id).first()
        )
        if existing:
            existing.interested = True
        else:
            data = RewardInterest(client_id=client_id, interested=interest)
            db.add(data)

        db.commit()

        return {"status": 200, "message": "Thank you for showing the interest"}
    except FittbotHTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise FittbotHTTPException(
            status_code=500,
            detail=f"An error occurred, {str(e)}",
            error_code="REWARD_INTEREST_POST_ERROR",
            log_data={
                "client_id": request.client_id,
                "interest": request.interest,
                "error": str(e),
            },
        )


class CancelRequest(BaseModel):
    client_id: int


@router.post("/cancel")
async def cancel_reward_modal(request: CancelRequest, db: Session = Depends(get_db)):
    """When user clicks cancel - reminder is already set when modal was shown"""
    try:
        # Reminder is already set in my_progress API when modal is shown
        # This API just acknowledges the cancel action
        return {"status": 200, "message": "Cancelled successfully"}
    except FittbotHTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise FittbotHTTPException(
            status_code=500,
            detail=f"An error occurred, {str(e)}",
            error_code="REWARD_INTEREST_CANCEL_ERROR",
            log_data={
                "client_id": request.client_id,
                "error": str(e),
            },
        )
