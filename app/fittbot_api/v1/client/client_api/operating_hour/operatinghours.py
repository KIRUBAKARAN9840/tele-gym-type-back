from fastapi import APIRouter, Depends
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.async_database import get_async_db
from app.models.fittbot_models import Gym
from app.utils.logging_utils import FittbotHTTPException

router = APIRouter(prefix="/operating_hours", tags=["Client Operating Hours"])


@router.get("/get")
async def get_operating_hours(
    gym_id: int,
    db: AsyncSession = Depends(get_async_db),
):
    try:
        result = await db.execute(
            select(Gym.operating_hours).where(Gym.gym_id == gym_id)
        )
        operating_hours = result.scalar_one_or_none()

        return {
            "status": 200,
            "data":operating_hours if operating_hours else []
        }
    except FittbotHTTPException:
        raise
    except Exception as exc:
        raise FittbotHTTPException(
            status_code=500,
            detail="Failed to fetch operating hours",
            error_code="OPERATING_HOURS_FETCH_ERROR",
            log_data={"error": repr(exc), "gym_id": gym_id},
        )
