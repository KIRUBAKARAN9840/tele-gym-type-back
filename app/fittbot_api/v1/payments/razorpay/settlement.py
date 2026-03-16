from __future__ import annotations

"""
Razorpay settlement/webhook glue (new file)
- Verifies Razorpay webhook signatures
- On relevant events, marks Daily Pass allocations settled using existing service
"""
import hmac
import hashlib
from typing import Dict, Any

from fastapi import APIRouter, Header, HTTPException, Request

from app.fittbot_api.v1.payments.routes.gym_dailypass import (
    mark_daily_pass_allocations_on_settlement,
)
from app.models.dailypass_models import get_dailypass_session


router = APIRouter(prefix="/razorpay/settlements", tags=["Razorpay Settlements (new)"])


def _verify_webhook(secret: str, body: bytes, signature: str) -> bool:
    mac = hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(mac, signature)


@router.post("/webhook")
async def rp_webhook(
    request: Request,
    x_razorpay_signature: str = Header(None),
):
    try:
        from app.config.settings import settings
        secret = settings.razorpay_webhook_secret
    except Exception:
        secret = None

    body = await request.body()
    if secret:
        if not x_razorpay_signature:
            raise HTTPException(status_code=400, detail="missing webhook signature")
        if not _verify_webhook(secret, body, x_razorpay_signature):
            raise HTTPException(status_code=400, detail="invalid webhook signature")

    event = await request.json()
    etype = event.get("event") or event.get("type")
    payload: Dict[str, Any] = event.get("payload") or {}

    # Handle a few relevant events. Adjust as per Razorpay's actual payloads in your setup.
    # payment.captured -> you may record Payment row elsewhere
    # settlement.processed / transfer.processed -> mark allocations settled for the payment
    if etype in ("settlement.processed", "transfer.processed"):
        payment_id = None
        # try to find provider payment id from payload
        for key in ("payment", "entity", "source"):
            obj = payload.get(key) or {}
            if isinstance(obj, dict):
                rid = (obj.get("entity") or {}).get("id") if "entity" in obj else obj.get("id")
                if rid and rid.startswith("pay_"):
                    payment_id = rid
                    break
        if payment_id:
            dps = get_dailypass_session()
            try:
                mark_daily_pass_allocations_on_settlement(dps, payment_id=payment_id)
            finally:
                dps.close()

    return {"ok": True}


__all__ = ["router"]

