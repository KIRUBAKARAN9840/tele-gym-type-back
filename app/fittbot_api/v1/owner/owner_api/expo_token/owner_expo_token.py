# app/routers/owner_expo.py

from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy.orm import Session

from app.models.database import get_db
from app.models.fittbot_models import GymOwner
from app.utils.logging_utils import FittbotHTTPException

router = APIRouter(prefix="/owner_expo", tags=["Profile"])


class ExpoTokenPayload(BaseModel):
    owner_id: int
    expo_token: str


@router.post("/update_expo_token")
def update_expo_token(payload: ExpoTokenPayload, db: Session = Depends(get_db)):
    """
    Append an Expo push token for a gym owner if it's not already present.
    Keeps prior API shape/behavior but uses FittbotHTTPException for error handling.
    """
    try:
        # Basic validation (keeps original contract intact)
        token = (payload.expo_token or "").strip()
        if not token:
            raise FittbotHTTPException(
                status_code=400,
                detail="expo_token is required",
                error_code="MISSING_EXPO_TOKEN",
                log_data={"owner_id": payload.owner_id},
            )

        owner = db.query(GymOwner).filter(GymOwner.owner_id == payload.owner_id).first()
        if not owner:
            raise FittbotHTTPException(
                status_code=404,
                detail="Owner not found",
                error_code="OWNER_NOT_FOUND",
                log_data={"owner_id": payload.owner_id},
            )

        current_tokens = owner.expo_token if owner.expo_token else []
        if not isinstance(current_tokens, list):
            current_tokens = [current_tokens]

        if token in current_tokens:
            return {"status": 200, "message": "Expo token already exists"}

        current_tokens.append(token)
        owner.expo_token = current_tokens
        db.commit()
        db.refresh(owner)

        return {"status": 200, "message": "Expo token added successfully"}

    except FittbotHTTPException:
        # Already structured & logged upstream; just re-raise
        raise
    except Exception as e:
        db.rollback()
        raise FittbotHTTPException(
            status_code=500,
            detail="Failed to update Expo token",
            error_code="EXPO_TOKEN_UPDATE_ERROR",
            log_data={"owner_id": payload.owner_id, "error": repr(e)},
        ) from e
