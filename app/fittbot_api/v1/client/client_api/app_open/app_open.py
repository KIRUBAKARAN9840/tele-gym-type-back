from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from pydantic import BaseModel
from typing import Optional
from datetime import datetime, timezone, timedelta

from app.models.async_database import get_async_db
from app.models.fittbot_models import AppOpen
from app.utils.logging_utils import FittbotHTTPException

router = APIRouter(prefix="/app_open", tags=["AppOpen"])


class AppOpenRequest(BaseModel):
    device_id: Optional[str]=None
    platform: Optional[str] = None
    device_data: Optional[dict] = None


@router.post("/track")
async def track_app_open(payload: AppOpenRequest, db: AsyncSession = Depends(get_async_db)):
    try:


        IST = timezone(timedelta(hours=5, minutes=30))
        new_record = AppOpen(
            platform=payload.platform,
            open_time=datetime.now(IST),
        )
        db.add(new_record)
        await db.commit()

        return {
            "status": 200,
            "message": "App open tracked successfully",
        }

    except FittbotHTTPException:
        raise
    except Exception as e:
        await db.rollback()
        raise FittbotHTTPException(
            status_code=500,
            detail="Unable to track app open",
            error_code="APP_OPEN_TRACK_ERROR",
            log_data={"exc": repr(e)},
        )
