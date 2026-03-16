# app/api/v1/client/unpaid_home.py

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from app.models.database import get_db
from app.models.fittbot_models import Client, Gym
from app.utils.logging_utils import FittbotHTTPException

router = APIRouter(prefix="/unpaid", tags=["Unpaid/Home"])


@router.get("/home")
async def get_unpaid_home(client_id: int, db: Session = Depends(get_db)):
    """
    Return minimal info for clients who haven't completed payment/join flow.
    (Logic preserved exactly as provided.)
    """
    try:
        client = db.query(Client).filter(Client.client_id == client_id).one()
        gym_name = None
        gym_id = None
        joined = False
        gym_location = None

        if client.gym_id:
            gym = db.query(Gym).filter(Gym.gym_id == client.gym_id).one()
            gym_name = gym.name
            gym_id = gym.gym_id
            gym_location = gym.location
            joined = True

        return {
            "status": 200,
            "message": "Data retrived successfully",
            "data": {
                "client_name": client.name,
                "gym_id": gym_id,
                "gym_name": gym_name,
                "gym_location": gym_location,
                "joined": joined,
            },
        }

    except FittbotHTTPException:
        # Pass through structured errors unchanged
        raise
    except Exception as e:
        # Normalize any unexpected error to FittbotHTTPException without changing logic
        raise FittbotHTTPException(
            status_code=500,
            detail="An error occurred while fetching unpaid home data",
            error_code="UNPAID_HOME_FETCH_ERROR",
            log_data={"exc": repr(e), "client_id": client_id},
        )
