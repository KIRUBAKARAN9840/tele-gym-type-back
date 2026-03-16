from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.async_database import get_async_db
from app.models.fittbot_models import AIConsent
from app.utils.logging_utils import FittbotHTTPException

router = APIRouter(prefix="/client/ai_consent", tags=["Client AI Consent"])


class AIConsentRequest(BaseModel):
    client_id: int



class AIConsentResponse(BaseModel):
    id: int
    client_id: int
    consent: bool


@router.post("/create")
async def create_or_update_ai_consent(
    payload: AIConsentRequest,
    db: AsyncSession = Depends(get_async_db),
):
    try:
        # Check if consent already exists for this client
        stmt = select(AIConsent).where(AIConsent.client_id == payload.client_id)
        existing = (await db.execute(stmt)).scalars().first()

        if existing:
            # Update existing consent
            existing.consent = True
            await db.commit()
            await db.refresh(existing)
            return {
                "status": 200,
                "message": "AI consent updated successfully",
                "data": {
                    "id": existing.id,
                    "client_id": existing.client_id,
                    "consent": existing.consent,
                },
            }
        else:
            # Create new consent
            new_consent = AIConsent(
                client_id=payload.client_id,
                consent=True,
            )
            db.add(new_consent)
            await db.commit()
            await db.refresh(new_consent)
            return {
                "status": 200,
                "message": "AI consent created successfully",
                "data": {
                    "id": new_consent.id,
                    "client_id": new_consent.client_id,
                    "consent": new_consent.consent,
                },
            }
    except Exception as exc:
        await db.rollback()
        raise FittbotHTTPException(
            status_code=500,
            detail="Failed to save AI consent",
            error_code="AI_CONSENT_SAVE_ERROR",
            log_data={"error": repr(exc), "client_id": payload.client_id},
        )


@router.get("/get")
async def get_ai_consent(
    client_id: int,
    db: AsyncSession = Depends(get_async_db),
):
    try:
        stmt = select(AIConsent).where(AIConsent.client_id == client_id)
        consent = (await db.execute(stmt)).scalars().first()

        if not consent:
            return {
                "status": 200,
                "consent": False           
            }

        return {
            "status": 200,
            "consent": consent.consent           
        }
    except Exception as exc:
        raise FittbotHTTPException(
            status_code=500,
            detail="Failed to fetch AI consent",
            error_code="AI_CONSENT_FETCH_ERROR",
            log_data={"error": repr(exc), "client_id": client_id},
        )
