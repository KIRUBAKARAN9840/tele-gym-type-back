# app/routers/feedback_router.py

from typing import Optional
from fastapi import APIRouter, Depends, Request
from sqlalchemy.orm import Session
from pydantic import BaseModel

from app.models.database import get_db
from app.models.fittbot_models import Gym_Feedback
from app.utils.logging_utils import FittbotHTTPException

router = APIRouter(prefix="/feedback", tags=["Client Tokens"])


class FeedbackCreate(BaseModel):
    client_id: int
    gym_id: int
    tag: str
    ratings: int
    feedback: Optional[str] = None


@router.post("/create_feedback")
async def create_feedback(
    request: Request,
    feedback_data: FeedbackCreate,
    db: Session = Depends(get_db),
):
    try:
        new_feedback = Gym_Feedback(
            gym_id=feedback_data.gym_id,
            client_id=feedback_data.client_id,
            tag=feedback_data.tag,
            ratings=feedback_data.ratings,
            feedback=feedback_data.feedback,
        )

        db.add(new_feedback)
        db.commit()
        db.refresh(new_feedback)

        return {
            "status": 200,
            "message": "Feedback submitted successfully",
            "feedback_id": new_feedback.id,
        }

    except FittbotHTTPException:
        # Already logged through FittbotHTTPException
        raise
    except Exception as e:
        db.rollback()
        raise FittbotHTTPException(
            status_code=500,
            detail="Unable to submit feedback",
            error_code="FEEDBACK_CREATE_ERROR",
            log_data={
                "exc": repr(e),
                "client_id": getattr(feedback_data, "client_id", None),
                "gym_id": getattr(feedback_data, "gym_id", None),
                "tag": getattr(feedback_data, "tag", None),
                "ratings": getattr(feedback_data, "ratings", None),
            },
        )
