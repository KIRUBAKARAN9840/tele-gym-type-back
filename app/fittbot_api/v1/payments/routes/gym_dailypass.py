
from __future__ import annotations

from datetime import date, datetime, timezone, timedelta
from typing import List, Optional, Dict, Any, Tuple

from fastapi import APIRouter, Depends, HTTPException, Body, Path, Query, status
from pydantic import BaseModel, Field, validator
from sqlalchemy.orm import Session

# Import models from the dailypass_models module
from app.models.dailypass_models import (
    DailyPass, DailyPassDay, DailyPassAudit, LedgerAllocation,
    get_dailypass_session, new_id
)

# Import payment system models for validation
from ..models.orders import Order
from ..models.payments import Payment
from ..models.subscriptions import Subscription  # not required here, kept for parity
from app.models.fittbot_models import Gym
from ..config.database import get_db_session  # For payments DB access

try:
    from ..models.settlements import SettlementEvent  # provider settlements (optional foreign key)
except Exception:
    SettlementEvent = None  # soft dep

try:
    from ..models.payouts import Payout  # provider payouts (optional foreign key)
except Exception:
    Payout = None  # soft dep

UTC = timezone.utc

# =============================================================================
#                         Pydantic Schemas (API)
# =============================================================================

class DPCreateRequest(BaseModel):
    user_id: str
    gym_id: str
    order_id: Optional[str] = None
    payment_id: str  # Razorpay payment_id (captured)
    days_total: int = Field(gt=0, le=60)
    dates: List[date] = Field(..., description="Length must equal days_total")
    total_gross_minor: int = Field(..., gt=0, description="Total the user paid in minor units")
    commission_bp: int = Field(3000, description="30% default (basis points)")
    pg_fee_minor: int = Field(0, description="Optional: acquiring fee total in minor units (across all days)")
    tax_minor: int = Field(0, description="Optional: tax on PG etc., total across all days")
    policy: Optional[Dict[str, Any]] = Field(default_factory=lambda: {
        "reschedule_limit": 1,
        "expiry_days": 180,
        "reschedule_cutoff_hours": 2,
        "commission_bp": 3000
    })

    @validator("dates")
    def _len_matches_days_total(cls, v, values):
        if "days_total" in values and len(v) != values["days_total"]:
            raise ValueError("dates length must equal days_total")
        return v


class DPCreateResponse(BaseModel):
    daily_pass_id: str
    days_created: int
    valid_from: Optional[date]
    valid_until: Optional[date]


class DPCheckinRequest(BaseModel):
    device_id: Optional[str] = None
    geo: Optional[Dict[str, Any]] = None
    actor: str = Field("gym:<id>", description="for audit; fill gym:<gym_id> or system")


class DPCheckinResponse(BaseModel):
    ok: bool
    pass_day_id: str
    new_state: str


class DPRescheduleRequest(BaseModel):
    new_date: date
    actor: str = Field("user:<id>", description="for audit; fill user:<user_id> or admin:<id>")


class DPRescheduleResponse(BaseModel):
    ok: bool
    old_day_id: str
    new_day_id: str


class DPHistoricalDay(BaseModel):
    id: str
    scheduled_date: date
    status: str
    reschedule_count: int
    checkin_at: Optional[datetime]
    money: Dict[str, int]


class DPHistoricalAudit(BaseModel):
    id: str
    action: str
    actor: str
    created_at: datetime
    before: Optional[Dict[str, Any]]
    after: Optional[Dict[str, Any]]


class DPHistoricalResponse(BaseModel):
    pass_id: str
    user_id: str
    gym_id: str
    status: str
    valid_from: Optional[date]
    valid_until: Optional[date]
    days_total: int
    days_used: int
    days: List[DPHistoricalDay]
    audit: List[DPHistoricalAudit]

# =============================================================================
#                              Utilities
# =============================================================================

def _bp_to_minor(part_minor: int, bp: int) -> int:
    """bp = basis points; 3000bp = 30%."""
    # commission = round(part_minor * bp / 10000.0)
    # banker's rounding not needed; use standard round
    return int(round(part_minor * (bp / 10000.0)))

def _split_evenly(total: int, n: int) -> List[int]:
    """Split `total` into `n` ints that sum to total (distribute remainders to earliest)."""
    base = total // n
    rem = total % n
    parts = [base] * n
    for i in range(rem):
        parts[i] += 1
    return parts

def _now_utc() -> datetime:
    return datetime.now(UTC)

def _add_audit(dailypass_db: Session, *, pass_id: str, action: str, actor: str, pass_day_id: Optional[str] = None,
               before: Optional[Dict[str, Any]] = None, after: Optional[Dict[str, Any]] = None) -> None:
    dailypass_db.add(DailyPassAudit(
        pass_id=pass_id, pass_day_id=pass_day_id, action=action, actor=actor,
        before=before, after=after
    ))

# =============================================================================
#                              Services
# =============================================================================

def create_daily_pass_after_capture(
    dailypass_db: Session,
    payments_db: Session, *,
    user_id: str,
    gym_id: str,
    order_id: Optional[str],
    payment_id: str,
    days_total: int,
    dates: List[date],
    total_gross_minor: int,
    commission_bp: int = 3000,
    pg_fee_minor: int = 0,
    tax_minor: int = 0,
    policy: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Create DailyPass + N DailyPassDay + N LedgerAllocation after a successful capture.
    Idempotent on (payment_id, days_total, dates) – if pass exists for payment_id+user+gym, reuse.
    """
    if days_total <= 0 or len(dates) != days_total:
        raise HTTPException(status_code=400, detail="days_total and dates mismatch")

    # Try to reuse an existing pass for this payment_id + user + gym if it exists
    existing = (
        dailypass_db.query(DailyPass)
        .filter(
            DailyPass.payment_id == payment_id,
            DailyPass.user_id == user_id,
            DailyPass.gym_id == gym_id,
        )
        .order_by(DailyPass.created_at.desc())
        .first()
    )
    if existing:
        return existing.id  # idempotent return

    # Create the pass
    dpass = DailyPass(
        user_id=user_id,
        gym_id=gym_id,
        order_id=order_id,
        payment_id=payment_id,
        days_total=days_total,
        valid_from=min(dates) if dates else None,
        valid_until=max(dates) if dates else None,
        policy=policy or {"reschedule_limit": 1, "expiry_days": 180, "reschedule_cutoff_hours": 2, "commission_bp": commission_bp},
    )
    dailypass_db.add(dpass)
    dailypass_db.flush()

    # Split money into N parts
    per_day_gross = _split_evenly(total_gross_minor, days_total)
    per_day_pg = _split_evenly(pg_fee_minor, days_total) if pg_fee_minor else [0] * days_total
    per_day_tax = _split_evenly(tax_minor, days_total) if tax_minor else [0] * days_total

    # Build days + allocations
    for i, dt in enumerate(dates):
        dpd = DailyPassDay(pass_id=dpass.id, scheduled_date=dt)
        dailypass_db.add(dpd)
        dailypass_db.flush()

        gross_i = per_day_gross[i]
        commission_i = _bp_to_minor(gross_i, commission_bp)
        net_before_payout_fee = max(0, gross_i - commission_i)

        alloc = LedgerAllocation(
            gym_id=gym_id,
            payment_id=payment_id,
            order_id=order_id,
            daily_pass_id=dpass.id,
            pass_day_id=dpd.id,
            amount_gross_minor=gross_i,
            commission_minor=commission_i,
            pg_fee_minor=per_day_pg[i],
            tax_minor=per_day_tax[i],
            amount_net_minor=net_before_payout_fee,
            state="held_pending_settlement",
        )
        dailypass_db.add(alloc)

    _add_audit(dailypass_db, pass_id=dpass.id, action="create", actor="system", before=None, after={
        "days_total": days_total, "dates": [str(d) for d in dates], "payment_id": payment_id
    })

    dailypass_db.commit()
    return dpass.id


def reschedule_pass_day(
    dailypass_db: Session, *, pass_day_id: str, new_date: date, actor: str = "user:<id>"
) -> Tuple[str, str]:
    dpd = dailypass_db.query(DailyPassDay).get(pass_day_id)
    if not dpd:
        raise HTTPException(status_code=404, detail="pass_day not found")
    if dpd.status != "scheduled":
        raise HTTPException(status_code=400, detail="only scheduled days can be rescheduled")

    dpass = dpd.daily_pass
    policy = (dpass.policy or {})
    limit = int(policy.get("reschedule_limit", 1))
    cutoff_h = int(policy.get("reschedule_cutoff_hours", 2))

    if dpd.reschedule_count >= limit:
        raise HTTPException(status_code=400, detail="reschedule limit reached")

    # enforce cutoff: can't reschedule too close to the original scheduled day (start of day used as reference)
    now = _now_utc()
    cutoff_dt = datetime.combine(dpd.scheduled_date, datetime.min.time(), tzinfo=UTC) - timedelta(hours=cutoff_h)
    if now > cutoff_dt:
        raise HTTPException(status_code=400, detail="reschedule cutoff passed")

    # ensure new date isn't already used by this pass
    dup = (
        dailypass_db.query(DailyPassDay)
        .filter(DailyPassDay.pass_id == dpass.id, DailyPassDay.scheduled_date == new_date)
        .first()
    )
    if dup:
        raise HTTPException(status_code=409, detail="date already scheduled for this pass")

    before = {"scheduled_date": str(dpd.scheduled_date), "reschedule_count": dpd.reschedule_count}
    dpd.status = "rescheduled"
    dpd.reschedule_count += 1
    dpd.meta = {"rescheduled_to": str(new_date), **(dpd.meta or {})}
    dailypass_db.add(dpd)

    # create new pass day
    new_dpd = DailyPassDay(pass_id=dpass.id, scheduled_date=new_date, status="scheduled", meta={"rescheduled_from": str(before["scheduled_date"])})
    dailypass_db.add(new_dpd)
    dailypass_db.flush()

    # move allocation if still held (not eligible or paid)
    alloc = dailypass_db.query(LedgerAllocation).filter(LedgerAllocation.pass_day_id == dpd.id).first()
    if alloc and alloc.state in ("held_pending_settlement", "held_settled"):
        alloc.pass_day_id = new_dpd.id
        dailypass_db.add(alloc)

    _add_audit(dailypass_db, pass_id=dpass.id, pass_day_id=dpd.id, action="reschedule", actor=actor,
               before=before, after={"scheduled_date": str(new_date), "reschedule_count": dpd.reschedule_count})

    dailypass_db.commit()
    return dpd.id, new_dpd.id


def checkin_pass_day(
    dailypass_db: Session, *, pass_day_id: str, device_id: Optional[str], geo: Optional[Dict[str, Any]], actor: str = "gym:<id>"
) -> Tuple[bool, str, str]:
    dpd = dailypass_db.query(DailyPassDay).get(pass_day_id)
    if not dpd:
        raise HTTPException(status_code=404, detail="pass_day not found")
    if dpd.status != "scheduled":
        # idempotent-ish: if already attended, return ok
        if dpd.status == "attended":
            alloc = dailypass_db.query(LedgerAllocation).filter(LedgerAllocation.pass_day_id == pass_day_id).first()
            return True, pass_day_id, (alloc.state if alloc else "unknown")
        raise HTTPException(status_code=400, detail=f"cannot checkin a day in status={dpd.status}")

    # TODO: add anti-fraud validations here (geo fence, device binding, QR token, time window)
    dpass = dpd.daily_pass

    before = {"status": dpd.status}
    dpd.status = "attended"
    dpd.checkin_at = _now_utc()
    dailypass_db.add(dpd)

    # increment days_used
    dpass.days_used = (dpass.days_used or 0) + 1
    dailypass_db.add(dpass)

    alloc = dailypass_db.query(LedgerAllocation).filter(LedgerAllocation.pass_day_id == pass_day_id).first()
    if alloc:
        if alloc.state == "held_settled":
            alloc.state = "released_eligible_for_payout"
            dailypass_db.add(alloc)
        # else: if still held_pending_settlement, it will flip upon settlement ingestion

    _add_audit(dailypass_db, pass_id=dpass.id, pass_day_id=dpd.id, action="checkin", actor=actor,
               before=before, after={"status": dpd.status, "checkin_at": dpd.checkin_at.isoformat()})

    dailypass_db.commit()
    return True, dpd.id, alloc.state if alloc else "unknown"


def mark_daily_pass_allocations_on_settlement(dailypass_db: Session, *, payment_id: str, settlement_event_id: Optional[str] = None) -> int:
    """
    Called by your settlement poller after inserting a SettlementEvent for payment_id.
    For all allocations tied to this payment:
      - held_pending_settlement -> held_settled
      - if pass-day already attended -> released_eligible_for_payout
    Returns count of allocations touched.
    """
    allocs: List[LedgerAllocation] = (
        dailypass_db.query(LedgerAllocation)
        .filter(LedgerAllocation.payment_id == payment_id)
        .all()
    )
    changed = 0
    for a in allocs:
        if a.state == "held_pending_settlement":
            a.state = "held_settled"
            if settlement_event_id:
                a.settlement_event_id = settlement_event_id
            # If the user already attended this day, release now
            dpd = dailypass_db.query(DailyPassDay).get(a.pass_day_id)
            if dpd and dpd.status == "attended":
                a.state = "released_eligible_for_payout"
            dailypass_db.add(a)
            changed += 1
    if changed:
        dailypass_db.commit()
    return changed

# =============================================================================
#                              API ROUTER
# =============================================================================

router = APIRouter(prefix="/daily-pass", tags=["Daily Pass"])

@router.post("/create", response_model=DPCreateResponse, status_code=status.HTTP_201_CREATED)
def api_create_daily_pass(
    payload: DPCreateRequest = Body(...),
    payments_db: Session = Depends(get_db_session),
):
    """
    INTERNAL: call this right after you confirm payment captured for a Daily Pass SKU.
    Idempotent on (user_id, gym_id, payment_id).
    """
    # Get dailypass database session
    dailypass_db = get_dailypass_session()

    try:
        # sanity: check gym and (optionally) order/payment existence using payments DB
        from app.models.database import get_db
        main_db = next(get_db())
        try:
            gym = main_db.query(Gym).filter(Gym.id == payload.gym_id).first()
            if not gym:
                raise HTTPException(status_code=404, detail="gym not found")
        finally:
            main_db.close()

        # Optional: ensure Payment exists
        pay = payments_db.query(Payment).filter(Payment.provider_payment_id == payload.payment_id).first()
        if not pay:
            # Not fatal; you may choose to require Payment row first
            pass

        dpid = create_daily_pass_after_capture(
            dailypass_db,
            payments_db,
            user_id=payload.user_id,
            gym_id=payload.gym_id,
            order_id=payload.order_id,
            payment_id=payload.payment_id,
            days_total=payload.days_total,
            dates=payload.dates,
            total_gross_minor=payload.total_gross_minor,
            commission_bp=payload.commission_bp,
            pg_fee_minor=payload.pg_fee_minor,
            tax_minor=payload.tax_minor,
            policy=payload.policy,
        )
        dpass = dailypass_db.query(DailyPass).get(dpid)
    finally:
        dailypass_db.close()
    return DPCreateResponse(
        daily_pass_id=dpid,
        days_created=dpass.days_total,
        valid_from=dpass.valid_from,
        valid_until=dpass.valid_until,
    )


@router.post("/{pass_day_id}/checkin", response_model=DPCheckinResponse)
def api_checkin_pass_day(
    pass_day_id: str = Path(...),
    payload: DPCheckinRequest = Body(...),
):
    dailypass_db = get_dailypass_session()
    try:
        ok, pdid, state = checkin_pass_day(
            dailypass_db,
            pass_day_id=pass_day_id,
            device_id=payload.device_id,
            geo=payload.geo,
            actor=payload.actor or "gym:<id>",
        )
        return DPCheckinResponse(ok=ok, pass_day_id=pdid, new_state=state)
    finally:
        dailypass_db.close()


@router.post("/{pass_day_id}/reschedule", response_model=DPRescheduleResponse)
def api_reschedule_pass_day(
    pass_day_id: str = Path(...),
    payload: DPRescheduleRequest = Body(...),
):
    dailypass_db = get_dailypass_session()
    try:
        old_id, new_id_ = reschedule_pass_day(
            dailypass_db,
            pass_day_id=pass_day_id,
            new_date=payload.new_date,
            actor=payload.actor or "user:<id>",
        )
        return DPRescheduleResponse(ok=True, old_day_id=old_id, new_day_id=new_id_)
    finally:
        dailypass_db.close()


@router.get("/{pass_id}/history", response_model=DPHistoricalResponse)
def api_daily_pass_history(
    pass_id: str = Path(...),
):
    dailypass_db = get_dailypass_session()
    try:
        dpass = dailypass_db.query(DailyPass).get(pass_id)
        if not dpass:
            raise HTTPException(status_code=404, detail="daily pass not found")

        # assemble days + money
        rows: List[DPHistoricalDay] = []
        for d in (
            dailypass_db.query(DailyPassDay)
            .filter(DailyPassDay.pass_id == pass_id)
            .order_by(DailyPassDay.scheduled_date.asc())
            .all()
        ):
            alloc = dailypass_db.query(LedgerAllocation).filter(LedgerAllocation.pass_day_id == d.id).first()
            rows.append(DPHistoricalDay(
                id=d.id,
                scheduled_date=d.scheduled_date,
                status=d.status,
                reschedule_count=d.reschedule_count,
                checkin_at=d.checkin_at,
                money={
                    "gross": alloc.amount_gross_minor if alloc else 0,
                    "commission": alloc.commission_minor if alloc else 0,
                    "net_before_payout_fee": alloc.amount_net_minor if alloc else 0,
                    "payout_fee": alloc.payout_fee_minor if alloc else 0,
                }
            ))

        audits = (
            dailypass_db.query(DailyPassAudit)
            .filter(DailyPassAudit.pass_id == pass_id)
            .order_by(DailyPassAudit.created_at.asc())
            .all()
        )
        audit_rows = [
            DPHistoricalAudit(
                id=a.id, action=a.action, actor=a.actor, created_at=a.created_at,
                before=a.before, after=a.after
            ) for a in audits
        ]

        return DPHistoricalResponse(
            pass_id=dpass.id,
            user_id=dpass.user_id,
            gym_id=dpass.gym_id,
            status=dpass.status,
            valid_from=dpass.valid_from,
            valid_until=dpass.valid_until,
            days_total=dpass.days_total,
            days_used=dpass.days_used,
            days=rows,
            audit=audit_rows,
        )
    finally:
        dailypass_db.close()
