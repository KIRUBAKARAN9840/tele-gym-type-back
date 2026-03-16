from fastapi import APIRouter, Depends, Request
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session
from typing import Optional
from datetime import datetime, date, timedelta
from app.models.database import get_db
from app.models.fittbot_models import FittbotRatings, ClientFeedback, FreeTrial
from app.utils.logging_utils import FittbotHTTPException
from app.utils.check_subscriptions import get_client_tier


router = APIRouter(prefix="/ratings", tags=["Fittbot Ratings"])


def check_feedback_status(db: Session, client_id: int) -> bool:

    feedback_record = (
        db.query(ClientFeedback)
        .filter(ClientFeedback.client_id == client_id)
        .order_by(ClientFeedback.updated_at.desc())
        .first()
    )

    if not feedback_record:
        return True
    elif feedback_record.status == "submitted":
        return False
    elif feedback_record.status == "canceled":
        if feedback_record.next_feedback_date:
            today = date.today()
            return today >= feedback_record.next_feedback_date
        else:
            return True
    elif feedback_record.status == "pending":
        return True

    return False


class RatingCreate(BaseModel):
    client_id: int 
    status: str 
    star: Optional[int] 
    feedback: Optional[str] 


@router.get("/check_rating")
async def check_rating(
    request: Request,
    client_id: int,
    db: Session = Depends(get_db),
):
    """API endpoint that calls the internal function"""
    try:
        show_rating = check_feedback_status(db, client_id)
        return {
            "status": 200,
            "data": {
                "show_rating": show_rating
            }
        }
    except FittbotHTTPException:
        raise
    except Exception as e:
        raise FittbotHTTPException(
            status_code=500,
            detail="Unable to check rating eligibility",
            error_code="RATING_CHECK_ERROR",
            log_data={"exc": repr(e), "client_id": client_id},
        )


@router.post("/create")
async def create_rating(request: Request, rating_data: RatingCreate, db: Session = Depends(get_db)):

    try:
        # Validate status
        if rating_data.status not in ["ok", "cancel"]:
            raise FittbotHTTPException(
                status_code=400,
                detail="Status must be 'ok' or 'cancel'",
                error_code="INVALID_STATUS",
                log_data={"status": rating_data.status},
            )

   

        # Check if user is in free trial with active status
        free_trial_entry = db.query(FreeTrial).filter(
            FreeTrial.client_id == rating_data.client_id
        ).first()
        is_free_trial_active = (
            free_trial_entry is not None and free_trial_entry.status == "active"
        )

        today = date.today()

        # Handle CANCEL status
        if rating_data.status == "cancel":
            # Calculate next_feedback_date based on tier
            if is_free_trial_active:
                # Free trial users: next day
                next_feedback_date = today + timedelta(days=1)
            else:
                # Premium users: 3 days later
                next_feedback_date = today + timedelta(days=3)

            # Get or create feedback record
            feedback_record = (
                db.query(ClientFeedback)
                .filter(ClientFeedback.client_id == rating_data.client_id)
                .order_by(ClientFeedback.updated_at.desc())
                .first()
            )

            if feedback_record:
                feedback_record.status = "canceled"
                feedback_record.next_feedback_date = next_feedback_date
                feedback_record.updated_at = datetime.now()
            else:
                feedback_record = ClientFeedback(
                    client_id=rating_data.client_id,
                    status="canceled",
                    next_feedback_date=next_feedback_date
                )
                db.add(feedback_record)

            db.commit()

            return {
                "status": 200,
                "message": "Rating canceled. We'll ask again later.",
                "data": {
                    "next_feedback_date": next_feedback_date.isoformat(),
                    "days_until_next": (next_feedback_date - today).days
                }
            }

        # Handle OK status
        if rating_data.status == "ok":
            # Validate star rating
            if not rating_data.star or rating_data.star < 1 or rating_data.star > 5:
                raise FittbotHTTPException(
                    status_code=400,
                    detail="Star rating must be between 1 and 5 when status is 'ok'",
                    error_code="INVALID_STAR_RATING",
                    log_data={"star": rating_data.star},
                )

            # Create rating record in FittbotRatings
            new_rating = FittbotRatings(
                client_id=rating_data.client_id,
                star=rating_data.star,
                feedback=rating_data.feedback
            )
            db.add(new_rating)

            # Update or create ClientFeedback with submitted status
            feedback_record = (
                db.query(ClientFeedback)
                .filter(ClientFeedback.client_id == rating_data.client_id)
                .order_by(ClientFeedback.updated_at.desc())
                .first()
            )

            if feedback_record:
                feedback_record.status = "submitted"
                feedback_record.feedback_text = rating_data.feedback
                feedback_record.rating = rating_data.star
                feedback_record.next_feedback_date = None  # Clear next feedback date
                feedback_record.updated_at = datetime.now()
            else:
                feedback_record = ClientFeedback(
                    client_id=rating_data.client_id,
                    status="submitted",
                    feedback_text=rating_data.feedback,
                    rating=rating_data.star,
                    next_feedback_date=None
                )
                db.add(feedback_record)

            db.commit()
            db.refresh(new_rating)

            return {
                "status": 200,
                "message": "Thank you for your rating!",
                "data": {
                    "id": new_rating.id,
                    "client_id": new_rating.client_id,
                    "star": new_rating.star,
                    "feedback": new_rating.feedback,
                    "created_at": new_rating.created_at.isoformat() if new_rating.created_at else None
                }
            }

    except FittbotHTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise FittbotHTTPException(
            status_code=500,
            detail="Error processing rating",
            error_code="CREATE_RATING_ERROR",
            log_data={
                "client_id": rating_data.client_id,
                "status": rating_data.status,
                "error": repr(e)
            },
        )
