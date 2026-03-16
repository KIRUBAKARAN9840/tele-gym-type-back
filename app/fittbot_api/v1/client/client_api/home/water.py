from datetime import datetime
from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from redis.asyncio import Redis
from app.models.database import get_db
from app.utils.redis_config import get_redis
from app.utils.logging_utils import FittbotHTTPException
from app.models.fittbot_models import ClientTarget, ClientActual
from typing import Optional

router = APIRouter(prefix="/water",tags=["Water Tracker"])


@router.get("/get")
async def get_clients_water(
    
    client_id: int,
    gym_id: Optional[int]=None,
    db: Session = Depends(get_db),
    redis: Redis = Depends(get_redis),
):
    try:
        today = datetime.now().date()

        target_data = (
            db.query(ClientTarget)
            .filter(ClientTarget.client_id == client_id)
            .first()
        )
        actual_data = (
            db.query(ClientActual)
            .filter(ClientActual.client_id == client_id, ClientActual.date == today)
            .first()
        )

        target_actual = {
            "water_intake": {
                "target": target_data.water_intake if target_data else 0,
                "actual": actual_data.water_intake if actual_data else 0,
            },
        }

        # Invalidate cache key (keep original logic)

        if gym_id is not None:
            target_actual_key = f"{client_id}:{gym_id}:target_actual"
            if await redis.exists(target_actual_key):
                await redis.delete(target_actual_key)

        return {
            "status": 200,
            "message": "Data fetched successfully",
            "data": {"target_actual": target_actual},
        }

    except FittbotHTTPException:
        raise
    except Exception as e:
        # Normalize to your error handling pattern without changing logic
        raise FittbotHTTPException(
            status_code=500,
            detail=f"An error occurred: {str(e)}",
            error_code="WATER_TRACKER_FETCH_ERROR",
            log_data={ "client_id": client_id, "error": str(e)},
        )
