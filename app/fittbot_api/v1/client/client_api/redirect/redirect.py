# app/fittbot_api/v1/client/client_api/redirect/redirect.py

from fastapi import APIRouter, Depends
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.async_database import get_async_db
from app.models.fittbot_models import AppRedirect
from app.utils.logging_utils import FittbotHTTPException

router = APIRouter(prefix="/redirect", tags=["App Redirect"])


@router.get("/check")
async def check_redirect(app: str, db: AsyncSession = Depends(get_async_db)):
    try:
        # Get all redirect records for this app where show=True
        stmt = select(AppRedirect).where(
            AppRedirect.app == app,
            AppRedirect.show == True
        )
        result = await db.execute(stmt)
        redirect_records = result.scalars().all()

        if not redirect_records:
            return {
                "status": 200,
                "show_modal": False
            }

        maintenance_record = None
        redirect_record = None

        for record in redirect_records:
            if record.type == "maintenance":
                maintenance_record = record
            elif record.type == "redirect":
                redirect_record = record

        # Prioritize maintenance over redirect
        active_record = maintenance_record if maintenance_record else redirect_record

        if active_record:
            return {
                "status": 200,
                "show_modal": True,
                "type": active_record.type,
                "message": active_record.message,
                "play_store_url": active_record.play_store_url,
                "app_store_url": active_record.app_store_url
            }

        return {
            "status": 200,
            "show_modal": False
        }

    except FittbotHTTPException:
        raise
    except Exception as e:
        raise FittbotHTTPException(
            status_code=500,
            detail="Failed to check redirect status",
            error_code="REDIRECT_CHECK_ERROR",
            log_data={"app": app, "error": str(e)}
        )
