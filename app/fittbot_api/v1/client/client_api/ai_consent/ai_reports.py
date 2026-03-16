from fastapi import APIRouter, Depends
from pydantic import BaseModel
from typing import Optional
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.async_database import get_async_db
from app.models.fittbot_models import AIReports
from app.utils.logging_utils import FittbotHTTPException

router = APIRouter(prefix="/client/ai_reports", tags=["Client AI Reports"])


class AIReportsRequest(BaseModel):
    client_id: int
    message: Optional[str] = None
    template: Optional[str] = None


@router.post("/create")
async def create_ai_report(
    payload: AIReportsRequest,
    db: AsyncSession = Depends(get_async_db),
):
    try:
        new_report = AIReports(
            client_id=payload.client_id,
            content=payload.message,
            template=payload.template,
        )
        db.add(new_report)
        await db.commit()
        await db.refresh(new_report)
        return {
            "status": 200
        }
    except Exception as exc:
        await db.rollback()
        raise FittbotHTTPException(
            status_code=500,
            detail="Failed to save AI report",
            error_code="AI_REPORTS_SAVE_ERROR",
            log_data={"error": repr(exc), "client_id": payload.client_id},
        )


@router.get("/{client_id}")
async def get_ai_reports(
    client_id: int,
    db: AsyncSession = Depends(get_async_db),
):
    try:
        stmt = select(AIReports).where(AIReports.client_id == client_id).order_by(AIReports.created_at.desc())
        reports = (await db.execute(stmt)).scalars().all()

        return {
            "status": 200,
            "data": [
                {
                    "id": report.id,
                    "client_id": report.client_id,
                    "content": report.content,
                    "template": report.template,
                    "created_at": report.created_at.isoformat() if report.created_at else None,
                }
                for report in reports
            ],
        }
    except Exception as exc:
        raise FittbotHTTPException(
            status_code=500,
            detail="Failed to fetch AI reports",
            error_code="AI_REPORTS_FETCH_ERROR",
            log_data={"error": repr(exc), "client_id": client_id},
        )
