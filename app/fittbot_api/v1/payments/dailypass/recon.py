from __future__ import annotations

from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List

from fastapi import APIRouter, Body, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from ..config.database import get_db_session
from ..config.settings import get_payment_settings
from ..models.payments import Payment

from app.models.dailypass_models import get_dailypass_session, LedgerAllocation, DailyPassDay

from .rp_client import get_settlement_recon


router = APIRouter(prefix="/pay/dailypass", tags=["Daily Pass Settlement Recon (new)"])


class ReconRunRequest(BaseModel):
    year: Optional[int] = None
    month: Optional[int] = None
    day: Optional[int] = None
    days_back: int = Field(default=1, ge=1, le=7, description="If date not provided, run for N days back from today")


@router.post("/recon/run")
def run_recon(body: ReconRunRequest = Body(default_factory=ReconRunRequest), payments_db: Session = Depends(get_db_session)):
    settings = get_payment_settings()

    dates: List[datetime] = []
    if body.year and body.month and body.day:
        try:
            dates = [datetime(body.year, body.month, body.day)]
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid date supplied")
    else:
        today = datetime.utcnow().date()
        for i in range(1, body.days_back + 1):
            dt = today - timedelta(days=i)
            dates.append(datetime(dt.year, dt.month, dt.day))

    totals = {"payments": 0, "allocs_marked": 0}
    per_day = []

    for dt in dates:
        yy, mm, dd = dt.year, dt.month, dt.day
        data = get_settlement_recon(yy, mm, dd, settings)
        txns = data if isinstance(data, list) else data.get("items") or data.get("items_list") or []

        # dailypass DB session for allocation updates
        dps = get_dailypass_session()
        try:
            day_allocs = 0
            matched = 0
            for t in txns:
                # Filter payment transactions
                etype = (t.get("type") or t.get("entity_type") or "").lower()
                if etype != "payment":
                    continue
                pay_id = t.get("entity_id") or t.get("payment_id")
                if not pay_id or not str(pay_id).startswith("pay_"):
                    continue

                fee_minor = int(t.get("fee") or 0)
                tax_minor = int(t.get("tax") or 0)
                settlement_id = t.get("settlement_id")
                utr = t.get("settlement_utr") or t.get("utr")
                settled_at = t.get("settled_at") or t.get("settled_on")

                # Find our payment
                p: Optional[Payment] = payments_db.query(Payment).filter(Payment.provider_payment_id == pay_id).first()
                if not p:
                    continue
                matched += 1

                # Update payment metadata with settlement info
                meta = dict(p.payment_metadata or {})
                meta.update({
                    "settlement_id": settlement_id,
                    "settlement_utr": utr,
                    "settled_at": settled_at,
                    "rp_fee_minor": fee_minor,
                    "rp_tax_minor": tax_minor,
                })
                p.payment_metadata = meta
                payments_db.add(p)

                # Mark all allocations for this payment as settled
                allocs: List[LedgerAllocation] = dps.query(LedgerAllocation).filter(LedgerAllocation.payment_id == pay_id).all()
                n = len(allocs)
                fee_share = fee_minor // n if n else 0
                tax_share = tax_minor // n if n else 0
                for a in allocs:
                    if a.state == "held_pending_settlement":
                        a.state = "held_settled"
                    # move to released if already attended
                    dpd = dps.query(DailyPassDay).get(a.pass_day_id)
                    if dpd and dpd.status == "attended":
                        a.state = "released_eligible_for_payout"
                    # spread fees/tax
                    a.pg_fee_minor = fee_share
                    a.tax_minor = tax_share
                    dps.add(a)
                    day_allocs += 1

            if matched:
                payments_db.commit()
            if day_allocs:
                dps.commit()

            totals["payments"] += matched
            totals["allocs_marked"] += day_allocs
            per_day.append({"date": f"{yy:04d}-{mm:02d}-{dd:02d}", "payments": matched, "allocs_marked": day_allocs})
        finally:
            dps.close()

    return {"ok": True, "totals": totals, "per_day": per_day}

