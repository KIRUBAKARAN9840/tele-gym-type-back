
import logging
import os
from typing import Any, Dict, List, Optional

import firebase_admin
from firebase_admin import credentials, messaging
from fastapi import APIRouter, Depends, Header, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from app.models.database import get_db
from app.models.fittbot_models import Client
from app.config.settings import settings

logger = logging.getLogger("notifications.rich")

router = APIRouter(prefix="/notifications", tags=["Rich Notifications"])


def _init_firebase():
    """Initialise the Firebase Admin SDK if not already initialised."""
    if firebase_admin._apps:
        return
    sa_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "..", "..", "..", "..", "firebase",
        "fittbot-c72eb-firebase-adminsdk-fbsvc-bfc6a7f7e9.json",
    )
    sa_path = os.path.normpath(sa_path)
    if not os.path.exists(sa_path):
        logger.warning("Firebase service account JSON not found at %s", sa_path)
        return
    cred = credentials.Certificate(sa_path)
    firebase_admin.initialize_app(cred)

_init_firebase()


def verify_notification_key(x_notification_key: str = Header(...)):
    """Validate the static API key sent in the X-Notification-Key header."""
    expected = getattr(settings, "notification_api_key", None)
    if not expected:
        raise HTTPException(status_code=500, detail="NOTIFICATION_API_KEY is not configured on the server")
    if x_notification_key != expected:
        raise HTTPException(status_code=403, detail="Invalid notification API key")
    return True


# ── Request / Response schemas ─────────────────────────────────────────────────

class RichNotificationRequest(BaseModel):
    client_ids: List[int] = Field(
        default=[],
        description="List of client IDs to notify. Empty list = broadcast to ALL clients with device tokens.",
    )
    title: str = Field(..., description="Notification title. Use {{name}} for client name.")
    body: str = Field(..., description="Notification body. Use {{name}} for client name.")
    image_url: Optional[str] = Field(
        default=None,
        description="Public URL of the image to show in the rich notification.",
    )
    data: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Extra key-value data payload sent to the app (for deep-linking etc).",
    )


class ClientResult(BaseModel):
    client_id: int
    name: str
    tokens_count: int
    sent: int
    failed: int
    errors: List[str] = []


class RichNotificationResponse(BaseModel):
    total_clients: int
    total_sent: int
    total_failed: int
    results: List[ClientResult]



def _send_fcm_batch(tokens: List[str], title: str, body: str,
                     image_url: Optional[str], data: Optional[Dict[str, str]]):

    notification = messaging.Notification(
        title=title,
        body=body,
        image=image_url,  # FCM natively renders this image in the notification tray
    )

    android_config = messaging.AndroidConfig(
        priority="high",
        notification=messaging.AndroidNotification(
            channel_id="rich_notifications",
            image=image_url,
            sound="default",
        ),
    )

    apns_config = messaging.APNSConfig(
        payload=messaging.APNSPayload(
            aps=messaging.Aps(
                alert=messaging.ApsAlert(title=title, body=body),
                mutable_content=True,
                sound="default",
            ),
        ),
        fcm_options=messaging.APNSFCMOptions(image=image_url) if image_url else None,
    )

    # FCM data values must all be strings
    str_data = {k: str(v) for k, v in data.items()} if data else {}

    message = messaging.MulticastMessage(
        tokens=tokens,
        notification=notification,
        android=android_config,
        apns=apns_config,
        data=str_data,
    )

    response = messaging.send_each_for_multicast(message)

    errors = []
    invalid_tokens = []
    for i, send_resp in enumerate(response.responses):
        if send_resp.exception:
            err_msg = str(send_resp.exception)
            errors.append(err_msg)
            # Token is invalid / unregistered — mark for cleanup
            if "UNREGISTERED" in err_msg.upper() or "NOT_FOUND" in err_msg.upper():
                invalid_tokens.append(tokens[i])

    return response.success_count, response.failure_count, errors, invalid_tokens


# ── Endpoint ───────────────────────────────────────────────────────────────────

@router.post("/send-rich", response_model=RichNotificationResponse)
def send_rich_notification(
    payload: RichNotificationRequest,
    db: Session = Depends(get_db),
    _auth: bool = Depends(verify_notification_key),
):

    if not firebase_admin._apps:
        raise HTTPException(status_code=500, detail="Firebase is not initialised. Check FIREBASE_SERVICE_ACCOUNT_PATH in .env")

    # ── 1. Fetch target clients ────────────────────────────────────────────
    if payload.client_ids:
        clients = (
            db.query(Client)
            .filter(Client.client_id.in_(payload.client_ids))
            .all()
        )
    else:
        # Broadcast — every client that has at least one device token
        clients = (
            db.query(Client)
            .filter(Client.device_token.isnot(None))
            .all()
        )

    if not clients:
        raise HTTPException(
            status_code=404,
            detail="No clients found with the given IDs (or no clients have device tokens)",
        )


    results: List[ClientResult] = []
    total_sent = 0
    total_failed = 0

    for client in clients:
        tokens = client.device_token
        if not tokens:
            continue
        if not isinstance(tokens, list):
            tokens = [tokens]
        tokens = [t for t in tokens if t]
        if not tokens:
            continue

        # Personalise title & body
        client_name = client.name or "there"
        title = payload.title.replace("{{name}}", client_name)
        body = payload.body.replace("{{name}}", client_name)

        # Build data payload (all values must be strings for FCM)
        data = {k: str(v) for k, v in payload.data.items()} if payload.data else {}
        data["type"] = "rich_notification"
        data["client_id"] = str(client.client_id)

        # Send via FCM
        sent, failed, errors, invalid_tokens = _send_fcm_batch(
            tokens=tokens,
            title=title,
            body=body,
            image_url=payload.image_url,
            data=data,
        )

        # Cleanup invalid tokens from DB
        if invalid_tokens:
            current = client.device_token if isinstance(client.device_token, list) else [client.device_token]
            updated = [t for t in current if t and t not in invalid_tokens]
            client.device_token = updated if updated else None
            try:
                db.commit()
            except Exception:
                db.rollback()

        total_sent += sent
        total_failed += failed
        results.append(
            ClientResult(
                client_id=client.client_id,
                name=client_name,
                tokens_count=len(tokens),
                sent=sent,
                failed=failed,
                errors=errors[:3],  # Cap error messages to avoid huge responses
            )
        )

    logger.info(
        "RICH_NOTIFICATION_BATCH_SENT",
        extra={
            "total_clients": len(results),
            "total_sent": total_sent,
            "total_failed": total_failed,
            "image_url": payload.image_url,
        },
    )

    return RichNotificationResponse(
        total_clients=len(results),
        total_sent=total_sent,
        total_failed=total_failed,
        results=results,
    )
