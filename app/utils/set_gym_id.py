

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from app.models.database import get_db
from app.models.fittbot_models import Client
from app.utils.logging_utils import FittbotHTTPException

router = APIRouter(prefix="/get_gym_id", tags=["Utils"])


@router.get("/reset")
async def get_client_gym_id(client_id: int, db: Session = Depends(get_db)):

    try:
        client = db.query(Client).filter(Client.client_id == client_id).first()
        if not client:
            raise FittbotHTTPException(
                status_code=404,
                detail="Client not found",
                error_code="CLIENT_NOT_FOUND",
                log_data={"client_id": client_id},
            )

        return {
            "status": 200,
            "gym_id": client.gym_id,
        }

    except FittbotHTTPException:
        raise
    except Exception as e:
        raise FittbotHTTPException(
            status_code=500,
            detail=f"An error occurred: {str(e)}",
            error_code="GET_CLIENT_GYM_ID_ERROR",
            log_data={"client_id": client_id, "error": str(e)},
        )


