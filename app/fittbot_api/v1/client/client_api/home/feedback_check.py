# app/fittbot_api/v1/client/client_api/home/feedback_check.py

from fastapi import APIRouter, Depends, Request
from sqlalchemy.orm import Session
from pydantic import BaseModel
from datetime import datetime, date, timedelta
from typing import Optional

from app.models.database import get_db
from app.models.fittbot_models import ClientFeedback, FreeTrial, Client
from app.fittbot_api.v1.payments.models.subscriptions import Subscription
from app.utils.logging_utils import FittbotHTTPException
from app.utils.check_subscriptions import get_client_tier

router = APIRouter(prefix="/feedback_check", tags=["Feedback"])


@router.get("/should_show_feedback")
async def should_show_feedback(
    request: Request,
    client_id: int,
    db: Session = Depends(get_db),
):
    """
    Check if feedback prompt should be shown to the client.

    Logic:
    - If client has already submitted feedback (status='submitted'), return False
    - For freemium users with free_trial:
      - Ask once during the trial period
      - If canceled, ask again next day
    - For non-freemium users (premium/premium_gym):
      - Ask every 3 days if canceled
      - Don't ask if already submitted
    """
    try:
        # Get client tier
        tier = get_client_tier(db, client_id)

        # Check if client exists
        client = db.query(Client).filter(Client.client_id == client_id).first()
        if not client:
            raise FittbotHTTPException(
                status_code=404,
                detail="Client not found",
                error_code="CLIENT_NOT_FOUND",
                log_data={"client_id": client_id},
            )

        # Check feedback record
        feedback_record = (
            db.query(ClientFeedback)
            .filter(ClientFeedback.client_id == client_id)
            .order_by(ClientFeedback.updated_at.desc())
            .first()
        )

        # If feedback already submitted, never ask again
        if feedback_record and feedback_record.status == "submitted":
            return {
                "status": 200,
                "data": {
                    "show_feedback": False,
                    "reason": "already_submitted"
                }
            }

        # Check if user is in free trial
        free_trial_entry = db.query(FreeTrial).filter(FreeTrial.client_id == client_id).first()
        is_free_trial = free_trial_entry is not None and free_trial_entry.status == "active"

        # Check if user has subscription
        subscription_entry = db.query(Subscription).filter(
            Subscription.customer_id == str(client_id)
        ).first()

        # Determine if freemium user
        is_freemium = tier in ["freemium", "freemium_gym"]

        today = date.today()

        # If no feedback record exists, show feedback for the first time
        if not feedback_record:
            # Create initial record
            new_feedback = ClientFeedback(
                client_id=client_id,
                status="pending",
                next_feedback_date=today
            )
            db.add(new_feedback)
            db.commit()

            return {
                "status": 200,
                "data": {
                    "show_feedback": True,
                    "reason": "first_time"
                }
            }

        # If status is canceled, check next_feedback_date
        if feedback_record.status == "canceled":
            if feedback_record.next_feedback_date is None:
                # No next_feedback_date set, show immediately
                return {
                    "status": 200,
                    "data": {
                        "show_feedback": True,
                        "reason": "canceled_no_date"
                    }
                }

            # Check if today is >= next_feedback_date
            if today >= feedback_record.next_feedback_date:
                # Time to ask again
                return {
                    "status": 200,
                    "data": {
                        "show_feedback": True,
                        "reason": "time_to_ask_again"
                    }
                }
            else:
                # Not yet time to ask
                days_remaining = (feedback_record.next_feedback_date - today).days
                return {
                    "status": 200,
                    "data": {
                        "show_feedback": False,
                        "reason": "waiting_for_next_date",
                        "days_remaining": days_remaining
                    }
                }

        # If status is pending, show feedback
        if feedback_record.status == "pending":
            return {
                "status": 200,
                "data": {
                    "show_feedback": True,
                    "reason": "pending"
                }
            }

        # Default: don't show
        return {
            "status": 200,
            "data": {
                "show_feedback": False,
                "reason": "default"
            }
        }

    except FittbotHTTPException:
        raise
    except Exception as e:
        raise FittbotHTTPException(
            status_code=500,
            detail="Unable to check feedback eligibility",
            error_code="FEEDBACK_CHECK_ERROR",
            log_data={"exc": repr(e), "client_id": client_id},
        )


class FeedbackCancelRequest(BaseModel):
    client_id: int


@router.post("/cancel_feedback")
async def cancel_feedback(
    request: Request,
    feedback_request: FeedbackCancelRequest,
    db: Session = Depends(get_db),
):
    """
    Mark feedback as canceled and set next_feedback_date based on user tier.

    Logic:
    - For freemium users with free_trial: next day
    - For premium users: 3 days later
    """
    try:
        client_id = feedback_request.client_id

        # Get client tier
        tier = get_client_tier(db, client_id)

        # Check if user is in free trial
        free_trial_entry = db.query(FreeTrial).filter(FreeTrial.client_id == client_id).first()
        is_free_trial = free_trial_entry is not None and free_trial_entry.status == "active"

        # Determine if freemium user
        is_freemium = tier in ["freemium", "freemium_gym"]

        today = date.today()

        # Calculate next_feedback_date
        if is_freemium and is_free_trial:
            # Ask again next day for free trial users
            next_feedback_date = today + timedelta(days=1)
        else:
            # Ask again in 3 days for premium users
            next_feedback_date = today + timedelta(days=3)

        # Get or create feedback record
        feedback_record = (
            db.query(ClientFeedback)
            .filter(ClientFeedback.client_id == client_id)
            .order_by(ClientFeedback.updated_at.desc())
            .first()
        )

        if feedback_record:
            feedback_record.status = "canceled"
            feedback_record.next_feedback_date = next_feedback_date
            feedback_record.updated_at = datetime.now()
        else:
            feedback_record = ClientFeedback(
                client_id=client_id,
                status="canceled",
                next_feedback_date=next_feedback_date
            )
            db.add(feedback_record)

        db.commit()

        return {
            "status": 200,
            "message": "Feedback canceled successfully",
            "data": {
                "next_feedback_date": next_feedback_date.isoformat(),
                "days_until_next": (next_feedback_date - today).days
            }
        }

    except FittbotHTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise FittbotHTTPException(
            status_code=500,
            detail="Unable to cancel feedback",
            error_code="FEEDBACK_CANCEL_ERROR",
            log_data={"exc": repr(e), "client_id": feedback_request.client_id},
        )


class FeedbackSubmitRequest(BaseModel):
    client_id: int
    feedback_text: Optional[str] = None
    rating: Optional[int] = None


@router.post("/submit_feedback")
async def submit_feedback(
    request: Request,
    feedback_request: FeedbackSubmitRequest,
    db: Session = Depends(get_db),
):
    """
    Submit user feedback and mark as submitted (won't ask again).
    """
    try:
        client_id = feedback_request.client_id

        # Validate rating if provided
        if feedback_request.rating is not None:
            if feedback_request.rating < 1 or feedback_request.rating > 5:
                raise FittbotHTTPException(
                    status_code=400,
                    detail="Rating must be between 1 and 5",
                    error_code="INVALID_RATING",
                    log_data={"rating": feedback_request.rating},
                )

        # Get or create feedback record
        feedback_record = (
            db.query(ClientFeedback)
            .filter(ClientFeedback.client_id == client_id)
            .order_by(ClientFeedback.updated_at.desc())
            .first()
        )

        if feedback_record:
            feedback_record.status = "submitted"
            feedback_record.feedback_text = feedback_request.feedback_text
            feedback_record.rating = feedback_request.rating
            feedback_record.next_feedback_date = None  # Clear next feedback date
            feedback_record.updated_at = datetime.now()
        else:
            feedback_record = ClientFeedback(
                client_id=client_id,
                status="submitted",
                feedback_text=feedback_request.feedback_text,
                rating=feedback_request.rating,
                next_feedback_date=None
            )
            db.add(feedback_record)

        db.commit()
        db.refresh(feedback_record)

        return {
            "status": 200,
            "message": "Thank you for your feedback!",
            "data": {
                "feedback_id": feedback_record.id
            }
        }

    except FittbotHTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise FittbotHTTPException(
            status_code=500,
            detail="Unable to submit feedback",
            error_code="FEEDBACK_SUBMIT_ERROR",
            log_data={
                "exc": repr(e),
                "client_id": feedback_request.client_id,
            },
        )
