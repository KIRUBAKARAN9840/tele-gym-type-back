# app/routers/gym_subscription_router.py

from fastapi import APIRouter, Depends, Request
from sqlalchemy.orm import Session

from app.models.database import get_db
from app.models.fittbot_models import GymPlans, Client
from app.utils.logging_utils import FittbotHTTPException

router = APIRouter(prefix="/gym_subscription", tags=["Client Tokens"])


@router.get("/get_plans")
async def get_plans_for_client(
    request: Request,
    client_id: int,
    db: Session = Depends(get_db),
):
    try:
        plans = (
            db.query(GymPlans)
            .join(Client, GymPlans.id == Client.training_id)
            .filter(Client.client_id == client_id)
            .all()
        )

        if not plans:
            raise FittbotHTTPException(
                status_code=404,
                detail="No training plan found for this client",
                error_code="GYM_PLANS_NOT_FOUND",
                log_data={"client_id": client_id},
            )

        return {"status": 200, "data": plans}

    except FittbotHTTPException:
        raise
    except Exception as e:
        raise FittbotHTTPException(
            status_code=500,
            detail="Unable to retrieve gym plans",
            error_code="GYM_PLANS_FETCH_ERROR",
            log_data={"exc": repr(e), "client_id": client_id},
        )
