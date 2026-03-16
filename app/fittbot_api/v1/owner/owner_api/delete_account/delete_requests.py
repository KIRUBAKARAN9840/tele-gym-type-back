from fastapi import APIRouter, Depends
from pydantic import BaseModel
from typing import Optional
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.async_database import get_async_db
from app.models.fittbot_models import OwnerDeleteRequest
from app.utils.logging_utils import FittbotHTTPException

router = APIRouter(prefix="/owner/delete_account", tags=["Owner Delete Account"])

class OwnerDeleteRequestPayload(BaseModel):
    owner_id: int
    feedback: Optional[str] = None

@router.post("/create")
async def create_owner_delete_request(
    payload: OwnerDeleteRequestPayload,
    db: AsyncSession = Depends(get_async_db),
):
    try:

        stmt = select(OwnerDeleteRequest).where(OwnerDeleteRequest.owner_id == payload.owner_id)
        existing = (await db.execute(stmt)).scalars().first()

        if existing:
            return {
                "status": 200,
                "message": "Delete request already exists",
            }

        # Create new delete request
        new_request = OwnerDeleteRequest(
            owner_id=payload.owner_id,
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
            error_code="OWNER_DELETE_REQUEST_SAVE_ERROR",
            log_data={"error": repr(exc), "owner_id": payload.owner_id},
        )

@router.get("/get")
async def get_owner_delete_request(
    owner_id: int,
    db: AsyncSession = Depends(get_async_db),
):
    try:
        stmt = select(OwnerDeleteRequest).where(OwnerDeleteRequest.owner_id == owner_id)
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
            error_code="OWNER_DELETE_REQUEST_FETCH_ERROR",
            log_data={"error": repr(exc), "owner_id": owner_id},
        )


