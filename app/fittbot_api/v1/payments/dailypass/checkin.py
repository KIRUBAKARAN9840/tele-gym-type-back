from __future__ import annotations

import hmac
import hashlib
import time
from datetime import datetime, timezone
from typing import Optional, Dict, Any

from fastapi import APIRouter, Body, Depends, HTTPException, status
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from ..config.database import get_db_session
from ..config.settings import get_payment_settings
from ..models.entitlements import Entitlement
from ..models.enums import EntType
from ..models.orders import OrderItem

from app.models.dailypass_models import get_dailypass_session, DailyPassDay, DailyPass, LedgerAllocation


router = APIRouter(prefix="/pay/dailypass", tags=["Daily Pass Check-in (new)"])

UTC = timezone.utc


class GenerateQRRequest(BaseModel):
    pass_day_id: str
    ttl_seconds: int = Field(300, gt=0, le=3600)


class GenerateQRResponse(BaseModel):
    token: str
    expires_at: int


class CheckinRequest(BaseModel):
    token: Optional[str] = None
    pass_day_id: Optional[str] = None
    gym_id: str
    device_id: Optional[str] = None
    geo: Optional[Dict[str, Any]] = None


def _sign(secret: str, msg: str) -> str:
    return hmac.new(secret.encode("utf-8"), msg.encode("utf-8"), hashlib.sha256).hexdigest()


@router.post("/qr-token", response_model=GenerateQRResponse, status_code=status.HTTP_201_CREATED)
def generate_qr_token(payload: GenerateQRRequest = Body(...)):
    settings = get_payment_settings()
    dps = get_dailypass_session()
    try:
        dpd = dps.query(DailyPassDay).get(payload.pass_day_id)
        if not dpd:
            raise HTTPException(status_code=404, detail="pass_day not found")
        dpass = dpd.daily_pass
        exp = int(time.time()) + payload.ttl_seconds
        msg = f"dpd:{dpd.id}:{dpass.gym_id}:{exp}"
        sig = _sign(settings.razorpay_key_secret, msg)
        token = f"{msg}:{sig}"
        # Store transiently in meta for audit/debug
        dpd.meta = {**(dpd.meta or {}), "qr_exp": exp}
        dps.add(dpd)
        dps.commit()
        return GenerateQRResponse(token=token, expires_at=exp)
    finally:
        dps.close()


@router.post("/checkin")
def checkin(payload: CheckinRequest = Body(...), payments_db: Session = Depends(get_db_session)):
    settings = get_payment_settings()
    if not payload.token and not payload.pass_day_id:
        raise HTTPException(status_code=400, detail="token or pass_day_id required")

    dps = get_dailypass_session()
    try:
        dpd = None
        if payload.token:
            try:
                prefix, dpd_id, gym_id, exp, sig = payload.token.split(":")
                if prefix != "dpd":
                    raise ValueError("bad prefix")
                if gym_id != payload.gym_id:
                    raise ValueError("gym mismatch")
                if int(exp) < int(time.time()):
                    raise ValueError("expired")
                msg = f"dpd:{dpd_id}:{gym_id}:{exp}"
                if not hmac.compare_digest(_sign(settings.razorpay_key_secret, msg), sig):
                    raise ValueError("bad signature")
                dpd = dps.query(DailyPassDay).get(dpd_id)
            except Exception:
                raise HTTPException(status_code=403, detail="invalid token")
        else:
            dpd = dps.query(DailyPassDay).get(payload.pass_day_id)

        if not dpd:
            raise HTTPException(status_code=404, detail="pass_day not found")
        dpass = dpd.daily_pass
        if str(dpass.gym_id) != str(payload.gym_id):
            raise HTTPException(status_code=403, detail="gym mismatch for pass")
        if dpd.status == "attended":
            # idempotent
            alloc = dps.query(LedgerAllocation).filter(LedgerAllocation.pass_day_id == dpd.id).first()
            return {"ok": True, "pass_day_id": dpd.id, "alloc_state": alloc.state if alloc else "unknown"}
        if dpd.status != "scheduled":
            raise HTTPException(status_code=409, detail=f"cannot checkin in status={dpd.status}")

        # Mark attended
        dpd.status = "attended"
        dpd.checkin_at = datetime.now(UTC)
        dps.add(dpd)

        dpass.days_used = (dpass.days_used or 0) + 1
        dps.add(dpass)

        # Flip allocation state if already settled
        alloc = dps.query(LedgerAllocation).filter(LedgerAllocation.pass_day_id == dpd.id).first()
        if alloc and alloc.state == "held_settled":
            alloc.state = "released_eligible_for_payout"
            dps.add(alloc)
        dps.commit()

        # Mirror entitlement to used
        # Find DP order item for this order and scheduled date
        item = payments_db.query(OrderItem).filter(
            OrderItem.order_id == dpass.order_id,
            OrderItem.item_type == "daily_pass",
        ).first()
        if item:
            ent = payments_db.query(Entitlement).filter(
                Entitlement.order_item_id == item.id,
                Entitlement.entitlement_type == EntType.visit,
                Entitlement.scheduled_for == dpd.scheduled_date,
            ).first()
            if ent:
                ent.status = "used"
                ent.active_from = dpd.checkin_at
                payments_db.add(ent)
                payments_db.commit()

        return {"ok": True, "pass_day_id": dpd.id, "alloc_state": alloc.state if alloc else "unknown"}
    finally:
        dps.close()

