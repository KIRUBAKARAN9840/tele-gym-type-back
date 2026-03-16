from fastapi import APIRouter, Depends
from pydantic import BaseModel
from typing import Optional
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.async_database import get_async_db
from app.models.fittbot_models import DeleteRequest
from app.utils.logging_utils import FittbotHTTPException

router = APIRouter(prefix="/client/delete_account", tags=["Client Delete Account"])


class DeleteRequestPayload(BaseModel):
    client_id: int
    feedback: Optional[str] = None


@router.post("/create")
async def create_delete_request(
    payload: DeleteRequestPayload,
    db: AsyncSession = Depends(get_async_db),
):
    try:
        stmt = select(DeleteRequest).where(DeleteRequest.client_id == payload.client_id)
        existing = (await db.execute(stmt)).scalars().first()

        if existing:
            return {
                "status": 200,
                "message": "Delete request already exists",
            }

        new_request = DeleteRequest(
            client_id=payload.client_id,
            feedback=payload.feedback,
        )
        db.add(new_request)
        await db.commit()
        await db.refresh(new_request)
        return {
            "status": 200,
            "message": "Delete request created successfully",
        }
    except Exception as exc:
        await db.rollback()
        raise FittbotHTTPException(
            status_code=500,
            detail="Failed to save delete request",
            error_code="DELETE_REQUEST_SAVE_ERROR",
            log_data={"error": repr(exc), "client_id": payload.client_id},
        )


@router.get("/get")
async def get_delete_request(
    client_id: int,
    db: AsyncSession = Depends(get_async_db),
):
    try:
        stmt = select(DeleteRequest).where(DeleteRequest.client_id == client_id)
        request = (await db.execute(stmt)).scalars().first()

        if not request:
            return {
                "status": 200,
                "exists": False,
            }

        return {
            "status": 200,
            "exists": True,
            "created_at": request.created_at.isoformat() if request.created_at else None,
        }
    except Exception as exc:
        raise FittbotHTTPException(
            status_code=500,
            detail="Failed to fetch delete request",
            error_code="DELETE_REQUEST_FETCH_ERROR",
            log_data={"error": repr(exc), "client_id": client_id},
        )
