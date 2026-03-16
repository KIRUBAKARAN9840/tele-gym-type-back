# app/routers/free_trial_router.py

from fastapi import APIRouter, Depends, Request
from sqlalchemy.orm import Session
from datetime import datetime, timedelta, timezone
from pydantic import BaseModel

from app.models.database import get_db
from app.models.fittbot_models import Client, FreeTrial
from app.utils.logging_utils import FittbotHTTPException
from app.fittbot_api.v1.payments.models.subscriptions import Subscription

router = APIRouter(prefix="/free_trial", tags=["Free_Trial"])

# Indian Standard Time (IST) timezone
IST = timezone(timedelta(hours=5, minutes=30))

def now_ist():
    """Get current time in IST"""
    return datetime.now(IST)


def check_and_expire_trial(db: Session, trial: FreeTrial) -> bool:
    """
    Check if a free trial should be expired (older than 7 days).
    Returns True if trial was expired, False otherwise.
    """
    if not trial or trial.status != "active":
        return False

    now = now_ist()
    created_at = trial.created_at

    # Make created_at timezone-aware if it isn't
    if created_at.tzinfo is None:
        created_at = created_at.replace(tzinfo=IST)

    # Check if trial is older than 7 days
    trial_age = now - created_at
    if trial_age > timedelta(days=7):
        # Expire the free trial
        trial.status = "expired"

        # Also expire the associated subscription
        subscription = db.query(Subscription).filter(
            Subscription.customer_id == str(trial.client_id),
            Subscription.provider == "free_trial",
            Subscription.status == "active"
        ).first()

        if subscription:
            subscription.status = "expired"

        db.flush()
        return True

    return False


class FreeTrialRequest(BaseModel):
    client_id: int


@router.post("/activate")
async def activate_free_trial(
    request: FreeTrialRequest,
    db: Session = Depends(get_db)
):

    try:
        client_id = request.client_id

        # Check if client exists
        client = db.query(Client).filter(Client.client_id == client_id).first()
        if not client:
            raise FittbotHTTPException(
                status_code=404,
                detail=f"Client with id {client_id} not found",
                error_code="CLIENT_NOT_FOUND",
                log_data={"client_id": client_id},
            )

        

        # Create free trial entry
        new_trial = FreeTrial(
            client_id=client_id,
            status="active"
        )
        db.add(new_trial)
        db.flush()

        # Calculate dates for subscription
        active_from = now_ist()
        active_until = active_from + timedelta(days=7)

        subscription_id = f"free_trial_{client_id}_{int(active_from.timestamp())}"

        new_subscription = Subscription(
            id=subscription_id,
            customer_id=str(client_id),
            provider="free_trial",
            product_id="free_trial_7days",
            status="active",
            active_from=active_from,
            active_until=active_until,
            auto_renew=False
        )
        db.add(new_subscription)

        db.commit()

        return {
            "status": 200,
            "message": "Free trial activated successfully",
            "data": {
                "client_id": client_id,
                "trial_id": new_trial.id,
                "subscription_id": subscription_id,
                "active_from": active_from.isoformat(),
                "active_until": active_until.isoformat(),
                "duration_days": 7
            }
        }

    except FittbotHTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise FittbotHTTPException(
            status_code=500,
            detail="Unable to activate free trial",
            error_code="FREE_TRIAL_ACTIVATION_ERROR",
            log_data={"exc": repr(e), "client_id": request.client_id},
        )
