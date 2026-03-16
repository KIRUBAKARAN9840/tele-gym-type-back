# app/api/v1/notifications/expo_tokens.py

from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy.orm import Session
from typing import Optional
from app.models.database import get_db
from app.models.fittbot_models import Client
from app.utils.logging_utils import FittbotHTTPException

router = APIRouter(prefix="/expo_token",tags=["Expo Tokens"])


class ExpoTokenPayload(BaseModel):
    client_id: int
    expo_token: str
    device_token:Optional[str]=None

@router.post("/update")
async def update_expo_token(payload: ExpoTokenPayload, db: Session = Depends(get_db)):
    try:
        client = db.query(Client).filter(Client.client_id == payload.client_id).first()
        if not client:
            raise FittbotHTTPException(
                status_code=400,
                detail="Client not found",
                error_code="CLIENT_NOT_FOUND",
                log_data={"client_id": payload.client_id},
            )

        current_tokens = client.expo_token if client.expo_token else []

        if not isinstance(current_tokens, list):
            current_tokens = [current_tokens]

        current_device_tokens = client.device_token if client.device_token else []

        if not isinstance(current_device_tokens, list):
            current_device_tokens = [current_device_tokens]

 
        expo_exists = payload.expo_token in current_tokens
        device_exists = payload.device_token in current_device_tokens

        if expo_exists and device_exists:
            return {"status": 200, "message": "Both tokens already exist"}

 
        if not expo_exists:
            current_tokens.append(payload.expo_token)
            client.expo_token = current_tokens

  
        if not device_exists:
            current_device_tokens.append(payload.device_token)
            client.device_token = current_device_tokens

        db.commit()


        if not expo_exists and not device_exists:
            message = "Both tokens added successfully"
        elif not expo_exists:
            message = "Expo token added successfully"
        else:
            message = "Device token added successfully"

        return {"status": 200, "message": message}

    except FittbotHTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise FittbotHTTPException(
            status_code=500,
            detail="Failed to update Expo token",
            error_code="EXPO_TOKEN_UPDATE_ERROR",
            log_data={"client_id": payload.client_id, "error": str(e)},
        )
