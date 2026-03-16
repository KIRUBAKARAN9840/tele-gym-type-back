from __future__ import annotations

import hmac
import hashlib
from collections import defaultdict
from datetime import date, datetime, timezone
from typing import Dict, Any, List, Optional, Tuple

from fastapi import APIRouter, Depends, Header, HTTPException, Request, status
from sqlalchemy.orm import Session

from ..config.database import get_db_session
from ..config.settings import get_payment_settings
from ..models.enums import PayoutBatchStatus
from ..models.payouts import PayoutBatch, PayoutEvent, PayoutLine
from ..models.profits import PlatformEarning
from ..models.entitlements import Entitlement
from ..models.orders import OrderItem

from app.models.dailypass_models import get_dailypass_session, DailyPassDay, LedgerAllocation


router = APIRouter(prefix="/pay/dailypass", tags=["Daily Pass Settlement & Payout (new)"])

UTC = timezone.utc


def _verify_webhook(secret: str, body: bytes, signature: str) -> bool:
    mac = hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(mac, signature)


@router.post("/webhook/razorpay")
async def razorpay_webhook(request: Request):
    settings = get_payment_settings()
    body = await request.body()
    sig = request.headers.get("X-Razorpay-Signature") or request.headers.get("x-razorpay-signature")
    if settings.razorpay_webhook_secret and not _verify_webhook(settings.razorpay_webhook_secret, body, sig or ""):
        raise HTTPException(status_code=403, detail="invalid webhook signature")

    payload = await request.json()
    etype = payload.get("event")
    # Only care about settlement/transfer style events
    if etype not in ("settlement.processed", "transfer.processed", "payment.settled"):
        return {"ok": True, "ignored": etype}

    # Try to pull payment_id
    p = payload.get("payload", {})
    payment_id = (
        ((p.get("payment") or {}).get("entity") or {}).get("id")
        or ((p.get("entity") or {}).get("id"))
    )
    if not payment_id:
        return {"ok": True, "ignored": "no_payment"}

    # Mark allocations as settled, and release eligible if already attended
    dps = get_dailypass_session()
    try:
        allocs: List[LedgerAllocation] = (
            dps.query(LedgerAllocation).filter(LedgerAllocation.payment_id == payment_id).all()
        )
        changed = 0
        for a in allocs:
            if a.state == "held_pending_settlement":
                a.state = "held_settled"
                # If attended, release
                dpd = dps.query(DailyPassDay).get(a.pass_day_id)
                if dpd and dpd.status == "attended":
                    a.state = "released_eligible_for_payout"
                dps.add(a)
                changed += 1
        if changed:
            dps.commit()
    finally:
        dps.close()

    return {"ok": True, "marked": True}


def _collect_eligible(payments_db: Session, dps: Session, gym_filter: Optional[str] = None) -> List[Tuple[LedgerAllocation, DailyPassDay, Entitlement]]:
    """Collect allocations that are eligible for payout: attended + settled (released_eligible_for_payout)
    and map them to entitlements for payout line creation. Idempotent via unique entitlement_id in PayoutLine.
    """
    q = dps.query(LedgerAllocation).filter(LedgerAllocation.state == "released_eligible_for_payout")
    if gym_filter:
        q = q.filter(LedgerAllocation.gym_id == gym_filter)
    allocs = q.all()
    results = []
    for a in allocs:
        dpd = dps.query(DailyPassDay).get(a.pass_day_id)
        if not dpd:
            continue
        # Find matching entitlement in payments DB (by order item and scheduled date)
        item = payments_db.query(OrderItem).filter(
            OrderItem.order_id == a.order_id,
            OrderItem.item_type == "daily_pass",
        ).first()
        if not item:
            continue
        ent = payments_db.query(Entitlement).filter(
            Entitlement.order_item_id == item.id,
            Entitlement.scheduled_for == dpd.scheduled_date,
        ).first()
        if not ent:
            continue
        # Only pay if entitlement was used (check-in complete)
        if (ent.status or "").lower() not in ("used", "active"):
            continue
        results.append((a, dpd, ent))
    return results


@router.post("/payouts/run")
def run_payouts(
    gym_id: Optional[str] = None,
    only_on_schedule: bool = True,
    payments_db: Session = Depends(get_db_session)
):
    """Create payout batches for eligible daily pass days. One batch per gym.
    Uses PayoutLine and PayoutBatch tables and marks allocations as in_payout.
    """
    # Optional schedule guard: Saturday 20:00 local time
    if only_on_schedule:
        now = datetime.now(UTC)
        # Convert to naive weekday/hour using UTC or adjust to IST via payment settings if desired
        # Here we assume IST offset +5:30 for simplicity
        ist = now.astimezone(get_payment_settings().ist_timezone)
        if not (ist.weekday() == 5 and ist.hour >= 20):  # Saturday (5) at/after 20:00 IST
            raise HTTPException(status_code=403, detail="Payout run allowed only on Saturdays after 20:00 IST or set only_on_schedule=false")
    dps = get_dailypass_session()
    try:
        elig = _collect_eligible(payments_db, dps, gym_filter=gym_id)
        by_gym: Dict[str, List[Tuple[LedgerAllocation, DailyPassDay, Entitlement]]] = defaultdict(list)
        for row in elig:
            by_gym[row[0].gym_id].append(row)

        batches = []
        for gym, rows in by_gym.items():
            total_net = sum(a.amount_net_minor for a, _, _ in rows)
            batch = PayoutBatch(
                id=f"pb_{int(datetime.now(UTC).timestamp())}_{gym}",
                batch_date=date.today(),
                gym_id=gym,
                total_net_amount_minor=total_net,
                payout_mode="NEFT",
                status=PayoutBatchStatus.queued,
            )
            payments_db.add(batch)
            payments_db.flush()

            for a, dpd, ent in rows:
                # Create payout line idempotently (unique entitlement_id)
                exists = payments_db.query(PayoutLine).filter(PayoutLine.entitlement_id == ent.id).first()
                if exists:
                    continue
                pl = PayoutLine(
                    id=f"pl_{ent.id}",
                    entitlement_id=ent.id,
                    gym_id=gym,
                    gross_amount_minor=a.amount_gross_minor,
                    commission_amount_minor=a.commission_minor,
                    net_amount_minor=a.amount_net_minor,
                    applied_commission_pct=round((a.commission_minor / a.amount_gross_minor) * 100.0, 2) if a.amount_gross_minor else 0.0,
                    applied_commission_fixed_minor=0,
                    scheduled_for=date.today(),
                    status="pending",
                    batch_id=batch.id,
                )
                payments_db.add(pl)

                # Ensure commission earning is recognized (idempotent via unique constraint)
                pe = PlatformEarning(
                    id=f"pe_comm_{a.pass_day_id}",
                    source="daily_pass",
                    earning_type="commission",
                    gym_id=gym,
                    order_id=a.order_id,
                    payment_id=a.payment_id,
                    pass_day_id=a.pass_day_id,
                    amount_minor=a.commission_minor,
                    recognized_on=datetime.now(UTC),
                    meta={"reason": "commission_on_attended_settlement"},
                )
                try:
                    payments_db.add(pe)
                    payments_db.flush()
                except Exception:
                    payments_db.rollback()
                    payments_db.begin()

                # Mark allocation in-payout to prevent double inclusion
                a.state = "in_payout"
                dps.add(a)

            payments_db.add(batch)
            payments_db.commit()
            dps.commit()
            batches.append({"gym_id": gym, "batch_id": batch.id, "total_net_minor": total_net, "count": len(rows)})

        return {"ok": True, "batches": batches}
    finally:
        dps.close()


@router.post("/profits/accrue-breakage")
def accrue_breakage(payments_db: Session = Depends(get_db_session)):
    """Recognize platform breakage revenue for settled but unattended days after expiry.
    Policy: if DailyPassDay not attended and allocation.state == held_settled, and the day is past its date,
    recognize amount_net_minor as 'breakage' and mark allocation as 'expired_no_show'.
    """
    dps = get_dailypass_session()
    try:
        today = date.today()
        rows: List[Tuple[LedgerAllocation, DailyPassDay]] = []
        allocs = dps.query(LedgerAllocation).filter(LedgerAllocation.state == "held_settled").all()
        for a in allocs:
            dpd = dps.query(DailyPassDay).get(a.pass_day_id)
            if not dpd:
                continue
            if dpd.scheduled_date < today and dpd.status != "attended":
                rows.append((a, dpd))

        count = 0
        for a, dpd in rows:
            # Commission (if not already recorded for this day)
            pe_comm = PlatformEarning(
                id=f"pe_comm_{a.pass_day_id}",
                source="daily_pass",
                earning_type="commission",
                gym_id=a.gym_id,
                order_id=a.order_id,
                payment_id=a.payment_id,
                pass_day_id=a.pass_day_id,
                amount_minor=a.commission_minor,
                recognized_on=datetime.now(UTC),
                meta={"reason": "commission_on_no_show_settlement"},
            )
            try:
                payments_db.add(pe_comm)
                payments_db.flush()
            except Exception:
                payments_db.rollback(); payments_db.begin()

            # Breakage (net to gym retained)
            pe = PlatformEarning(
                id=f"pe_break_{a.pass_day_id}",
                source="daily_pass",
                earning_type="breakage",
                gym_id=a.gym_id,
                order_id=a.order_id,
                payment_id=a.payment_id,
                pass_day_id=a.pass_day_id,
                amount_minor=a.amount_net_minor,
                recognized_on=datetime.now(UTC),
                meta={"reason": "no_show_after_expiry"},
            )
            try:
                payments_db.add(pe)
                payments_db.flush()
                # Mark allocation expired_no_show
                a.state = "expired_no_show"
                dps.add(a)
                count += 1
            except Exception:
                payments_db.rollback()
                payments_db.begin()
        if count:
            dps.commit()
            payments_db.commit()
        return {"ok": True, "recognized": count}
    finally:
        dps.close()
