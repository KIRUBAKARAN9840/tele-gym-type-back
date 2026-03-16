from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.async_database import get_async_db
from app.models.fittbot_models import StepConsent
from app.utils.logging_utils import FittbotHTTPException

router = APIRouter(prefix="/client/step_consent", tags=["Client Step Consent"])


class StepConsentRequest(BaseModel):
    client_id: int


class StepConsentResponse(BaseModel):
    id: int
    client_id: int


@router.post("/create")
async def create_or_update_step_consent(
    payload: StepConsentRequest,
    db: AsyncSession = Depends(get_async_db),
):
    try:
        # Check if consent already exists for this client
        stmt = select(StepConsent).where(StepConsent.client_id == payload.client_id)
        existing = (await db.execute(stmt)).scalars().first()

        if existing:
            # Update existing consent
            existing.consent = True
            await db.commit()
            await db.refresh(existing)
            return {
                "status": 200
            }
        else:
            # Create new consent
            new_consent = StepConsent(
                client_id=payload.client_id,
                consent=True,
            )
            db.add(new_consent)
            await db.commit()
            await db.refresh(new_consent)
            return {
                "status": 200
            }
    except Exception as exc:
        await db.rollback()
        raise FittbotHTTPException(
            status_code=500,
            detail="Failed to save Step consent",
            error_code="STEP_CONSENT_SAVE_ERROR",
            log_data={"error": repr(exc), "client_id": payload.client_id},
        )


@router.get("/get")
async def get_step_consent(
    client_id: int,
    db: AsyncSession = Depends(get_async_db),
):
    try:
        stmt = select(StepConsent).where(StepConsent.client_id == client_id)
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
            detail="Failed to fetch Step consent",
            error_code="STEP_CONSENT_FETCH_ERROR",
            log_data={"error": repr(exc), "client_id": client_id},
        )
