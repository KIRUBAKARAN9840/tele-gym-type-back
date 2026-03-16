# app/routers/free_trial_router.py

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from datetime import datetime, date
from pydantic import BaseModel

from app.models.database import get_db
from app.models.fittbot_models import ClientFittbotAccess, Client, Gym

from app.utils.logging_utils import FittbotHTTPException

router = APIRouter(
    prefix="/free_trial",
    tags=["Fittbot Access"],
)

class FreeTrialRequest(BaseModel):
    client_id: int


@router.post("/avail_free_trial")
async def avail_free_trial(Request: FreeTrialRequest, db: Session = Depends(get_db)):
    try:
        today = date.today()
        now = datetime.now()


        client = db.query(Client).filter(Client.client_id == Request.client_id).first()
        if client is None:
            raise FittbotHTTPException(
                status_code=404,
                detail=f"Client with id {Request.client_id} not found",
                error_code="CLIENT_NOT_FOUND",
                log_data={"client_id": Request.client_id},
            )

        # Fetch gym (original code assumed it exists; we enforce a clean 404 if not)
        gym = db.query(Gym).filter(Gym.gym_id == client.gym_id).first() if client.gym_id else None
        if gym is None:
            raise FittbotHTTPException(
                status_code=404,
                detail="Gym not found for the client",
                error_code="GYM_NOT_FOUND",
                log_data={"client_id": Request.client_id, "gym_id": client.gym_id},
            )

        plan = client.training_id
        payload = {
            "gender": client.gender,
            "gym_id": client.gym_id,
            "gym_name": gym.name,
        }

        # Upsert access row
        access_row = (
            db.query(ClientFittbotAccess)
            .filter(ClientFittbotAccess.client_id == Request.client_id)
            .first()
        )

        if access_row:
            access_row.access_status = "active"
            access_row.free_trial = "started"
            access_row.start_date = today
            access_row.paid_date = now
            access_row.days_left = 30
        else:
            access_row = ClientFittbotAccess(
                client_id=Request.client_id,
                paid_date=now,
                plan=plan,
                access_status="active",
                free_trial="started",
                start_date=today,
                days_left=30,
            )
            db.add(access_row)

        db.commit()
        db.refresh(access_row)

        return {
            "status": 200,
            "message": "Free trial activated successfully",
            "data": payload,
        }

    except FittbotHTTPException:
        raise

    except Exception as exc:
        db.rollback()
        raise FittbotHTTPException(
            status_code=500,
            detail="Internal server error occurred during free trial activation",
            error_code="FREE_TRIAL_ACTIVATION_ERROR",
            log_data={
                "error": repr(exc),
                "client_id": Request.client_id if Request and hasattr(Request, "client_id") else None,
            },
        )
