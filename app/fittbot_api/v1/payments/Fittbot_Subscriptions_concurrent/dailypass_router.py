from datetime import timedelta

from fastapi import APIRouter, Depends, HTTPException, Request, status
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.fittbot_api.v1.payments.dailypass import routes as dailypass_routes
from app.fittbot_api.v1.payments.dailypass.routes import (
    UnifiedCheckoutRequest,
    UnifiedVerificationRequest,
)
from app.fittbot_api.v1.client.client_api.dailypass.get_dailypass import (
    EditCheckoutRequest,
    EditVerifyRequest,
)
from app.fittbot_api.v1.client.client_api.dailypass.get_dailypass import PassSummary, ListActiveResponse
from app.models.dailypass_models import (
    DailyPass,
    DailyPassDay,
    DailyPassAudit,
    DailyPassPricing,
    get_dailypass_session,
    get_price_for_gym,
)
from app.utils.request_auth import resolve_authenticated_user_id
from app.config.pricing import get_markup_multiplier

from .dependencies import (
    get_dailypass_command_dispatcher,
    get_dailypass_command_store,
)
from .schemas import CommandStatusResponse, DailyPassCommandAccepted
from .services.dailypass_dispatcher import DailyPassCommandDispatcher
from .stores.command_store import CommandStore
from ..config.database import get_payment_db

router = APIRouter(prefix="/pay/dailypass_v2", tags=["Daily Pass Payments v2"])


@router.post(
    "/checkout",
    response_model=DailyPassCommandAccepted,
    status_code=status.HTTP_202_ACCEPTED,
)
async def enqueue_dailypass_checkout(
    request: Request,
    body: UnifiedCheckoutRequest,
    dispatcher: DailyPassCommandDispatcher = Depends(get_dailypass_command_dispatcher),
):
    client_id = resolve_authenticated_user_id(request, getattr(body, "client_id", None))
    status_record = await dispatcher.enqueue_checkout(body, owner_id=client_id)
    status_url = request.url_for("get_dailypass_command_status", command_id=status_record.request_id)

    # Track checkout initiation (non-blocking)
    from app.services.activity_tracker import track_event
    await track_event(
        int(client_id), "checkout_initiated",
        gym_id=getattr(body, "gymId", getattr(body, "gym_id", None)),
        product_type="dailypass",
        product_details={"days": getattr(body, "daysTotal", 1)},
        source="payment_dailypass",
        command_id=status_record.request_id,
    )

    return DailyPassCommandAccepted(
        request_id=status_record.request_id,
        status=status_record.status,
        status_url=str(status_url),
    )


@router.post(
    "/verify",
    response_model=DailyPassCommandAccepted,
    status_code=status.HTTP_202_ACCEPTED,
)
async def enqueue_dailypass_verify(
    request: Request,
    body: UnifiedVerificationRequest,
    dispatcher: DailyPassCommandDispatcher = Depends(get_dailypass_command_dispatcher),
):
    client_id = resolve_authenticated_user_id(request)
    status_record = await dispatcher.enqueue_verify(body, owner_id=client_id)
    status_url = request.url_for("get_dailypass_command_status", command_id=status_record.request_id)

    # Track payment verification (non-blocking)
    from app.services.activity_tracker import track_event
    await track_event(
        int(client_id), "checkout_completed",
        product_type="dailypass",
        source="payment_dailypass",
        command_id=status_record.request_id,
    )

    return DailyPassCommandAccepted(
        request_id=status_record.request_id,
        status=status_record.status,
        status_url=str(status_url),
    )


@router.get(
    "/commands/{command_id}",
    response_model=CommandStatusResponse,
    name="get_dailypass_command_status",
)
async def get_dailypass_command_status(
    command_id: str,
    request: Request,
    store: CommandStore = Depends(get_dailypass_command_store),
):
    client_id = resolve_authenticated_user_id(request)
    record = await store.get(command_id, owner_id=client_id)
    if not record:
        raise HTTPException(status_code=404, detail="command_not_found")
    return record.to_response()


# Upgrade endpoints
from pydantic import BaseModel


class UpgradeCheckoutRequest(BaseModel):
    new_gym_id: int
    client_id: str
    pass_id: str
    remaining_days_count: int
    delta_minor: float


class UpgradeVerifyRequest(BaseModel):
    razorpay_order_id: str
    razorpay_payment_id: str
    razorpay_signature: str
    pass_id: str


class EditTopupCheckoutRequest(BaseModel):
    pass_id: str
    client_id: str
    new_start_date: str
    delta_minor: float


class EditTopupVerifyRequest(BaseModel):
    razorpay_order_id: str
    razorpay_payment_id: str
    razorpay_signature: str
    pass_id: str


@router.post(
    "/upgrade/checkout",
    response_model=DailyPassCommandAccepted,
    status_code=status.HTTP_202_ACCEPTED,
)
async def enqueue_dailypass_upgrade_checkout(
    request: Request,
    body: UpgradeCheckoutRequest,
    dispatcher: DailyPassCommandDispatcher = Depends(get_dailypass_command_dispatcher),
):
    client_id = resolve_authenticated_user_id(request, body.client_id)
    status_record = await dispatcher.enqueue_upgrade_checkout(body, owner_id=client_id)
    status_url = request.url_for("get_dailypass_command_status", command_id=status_record.request_id)
    return DailyPassCommandAccepted(
        request_id=status_record.request_id,
        status=status_record.status,
        status_url=str(status_url),
    )


@router.post(
    "/upgrade/verify",
    response_model=DailyPassCommandAccepted,
    status_code=status.HTTP_202_ACCEPTED,
)
async def enqueue_dailypass_upgrade_verify(
    request: Request,
    body: UpgradeVerifyRequest,
    dispatcher: DailyPassCommandDispatcher = Depends(get_dailypass_command_dispatcher),
):
    client_id = resolve_authenticated_user_id(request)
    status_record = await dispatcher.enqueue_upgrade_verify(body, owner_id=client_id)
    status_url = request.url_for("get_dailypass_command_status", command_id=status_record.request_id)
    return DailyPassCommandAccepted(
        request_id=status_record.request_id,
        status=status_record.status,
        status_url=str(status_url),
    )


@router.post(
    "/edit_topup/checkout",
    response_model=DailyPassCommandAccepted,
    status_code=status.HTTP_202_ACCEPTED,
)
async def enqueue_dailypass_edit_topup_checkout(
    request: Request,
    body: EditTopupCheckoutRequest,
    dispatcher: DailyPassCommandDispatcher = Depends(get_dailypass_command_dispatcher),
):
    client_id = resolve_authenticated_user_id(request, body.client_id)
    status_record = await dispatcher.enqueue_edit_topup_checkout(body, owner_id=client_id)
    status_url = request.url_for("get_dailypass_command_status", command_id=status_record.request_id)
    return DailyPassCommandAccepted(
        request_id=status_record.request_id,
        status=status_record.status,
        status_url=str(status_url),
    )


@router.post(
    "/edit_topup/verify",
    response_model=DailyPassCommandAccepted,
    status_code=status.HTTP_202_ACCEPTED,
)
async def enqueue_dailypass_edit_topup_verify(
    request: Request,
    body: EditTopupVerifyRequest,
    dispatcher: DailyPassCommandDispatcher = Depends(get_dailypass_command_dispatcher),
):
    client_id = resolve_authenticated_user_id(request)
    status_record = await dispatcher.enqueue_edit_topup_verify(body, owner_id=client_id)
    status_url = request.url_for("get_dailypass_command_status", command_id=status_record.request_id)
    return DailyPassCommandAccepted(
        request_id=status_record.request_id,
        status=status_record.status,
        status_url=str(status_url),
    )


@router.get("/passes", response_model=ListActiveResponse)
async def get_active_dailypass(client_id: int):
    today = _today_ist()
    dps = next(get_dailypass_session())
    payments_db = get_payment_db()
    response: ListActiveResponse
    with payments_db.get_session() as session:
        try:
            all_rows = (
                dps.query(DailyPass)
                .filter(
                    DailyPass.client_id == str(client_id),
                    today <= (DailyPass.valid_until or DailyPass.end_date),
                )
                .order_by(DailyPass.created_at.desc())
                .all()
            )

            upgraded_passes = [p for p in all_rows if p.status == "upgraded"]
            original_pass_ids = {p.order_id for p in upgraded_passes if p.order_id}
            rows = [p for p in all_rows if p.id not in original_pass_ids]

            # === BATCH LOAD ALL DATA UPFRONT (N+1 FIX) ===
            all_pass_ids = [p.id for p in rows]
            all_pass_ids_for_audit = list(all_pass_ids)
            # Include order_ids for audit checks on upgraded passes
            for p in rows:
                if p.status == "upgraded" and p.order_id:
                    all_pass_ids_for_audit.append(p.order_id)

            # BATCH 1: Load all DailyPassDay records for all passes at once
            all_days_map = {}
            if all_pass_ids:
                all_days = (
                    dps.query(DailyPassDay)
                    .filter(DailyPassDay.pass_id.in_(all_pass_ids))
                    .order_by(DailyPassDay.scheduled_date.asc())
                    .all()
                )
                for day in all_days:
                    if day.pass_id not in all_days_map:
                        all_days_map[day.pass_id] = []
                    all_days_map[day.pass_id].append(day)

            # BATCH 2: Load all DailyPassAudit records for all passes at once
            all_audits_map = {}
            if all_pass_ids_for_audit:
                all_audits = (
                    dps.query(DailyPassAudit)
                    .filter(DailyPassAudit.pass_id.in_(all_pass_ids_for_audit))
                    .all()
                )
                for audit in all_audits:
                    if audit.pass_id not in all_audits_map:
                        all_audits_map[audit.pass_id] = []
                    all_audits_map[audit.pass_id].append(audit)

            # BATCH 3: Load all gym info at once
            all_gym_ids = list({int(p.gym_id) for p in rows})
            # Also include old gym ids for upgraded passes
            for p in rows:
                if p.status == "upgraded" and p.order_id:
                    original_pass = next((op for op in all_rows if op.id == p.order_id), None)
                    if original_pass:
                        all_gym_ids.append(int(original_pass.gym_id))
            all_gym_ids = list(set(all_gym_ids))

            gym_map = {}
            if all_gym_ids:
                gym_rows = session.execute(
                    text("SELECT gym_id, name, location, city FROM gyms WHERE gym_id IN :ids"),
                    {"ids": tuple(all_gym_ids)},
                ).fetchall()
                for row in gym_rows:
                    gym_map[row[0]] = {"name": row[1], "location": row[2], "city": row[3]}

            # BATCH 4: Load all prices at once
            price_map = _batch_load_prices(dps, all_gym_ids)

            # === HELPER FUNCTIONS FOR IN-MEMORY LOOKUPS ===
            def _check_audit_used(pass_id: str, action: str) -> bool:
                audits = all_audits_map.get(pass_id, [])
                return any(a.action == action for a in audits)

            def _get_gym_info(gym_id: int) -> dict:
                return gym_map.get(gym_id, {"name": f"Gym {gym_id}", "location": None, "city": None})

            # === PROCESS PASSES IN MEMORY (NO MORE QUERIES IN LOOP) ===
            resp = []
            for p in rows:
                pass_days = all_days_map.get(p.id, [])
                active_statuses = ["scheduled", "available", "rescheduled"]

                # remaining count (was query at lines 177-185)
                remaining = sum(
                    1 for d in pass_days
                    if d.scheduled_date > today and d.status in active_statuses
                )

                # next_dates (was query at lines 186-196)
                future_active_days = sorted(
                    [d for d in pass_days if d.scheduled_date > today and d.status in active_statuses],
                    key=lambda d: d.scheduled_date
                )[:5]
                next_dates = [d.scheduled_date.isoformat() for d in future_active_days]

                # can_res and can_upg (was _once_used queries at lines 199-200)
                can_res = not _check_audit_used(p.id, "reschedule")
                can_upg = not _check_audit_used(p.id, "upgrade")

                original_start_date = p.valid_from or p.start_date
                original_end_date = p.valid_until or p.end_date

                if today >= original_end_date:
                    can_res = False

                # future_resched_count (was query at lines 208-216)
                future_resched_count = sum(
                    1 for d in pass_days
                    if d.scheduled_date >= (today + timedelta(days=1)) and d.status in active_statuses
                )
                if future_resched_count == 0:
                    can_res = False

                # all_remaining logic (was query at lines 220-227)
                all_remaining = [d for d in pass_days if d.status in active_statuses]
                if all_remaining:
                    latest_remaining_date = max(day.scheduled_date for day in all_remaining)
                    if latest_remaining_date <= today:
                        can_res = False

                # total_days and attended_days (was queries at lines 233-242)
                total_days = len(pass_days)
                attended_days = sum(1 for d in pass_days if d.status == "attended")
                if total_days > 0 and attended_days == total_days:
                    can_res = False

                # gym info (was _load_gym_info at line 246)
                gym = _get_gym_info(int(p.gym_id))

                is_upgraded = p.status == "upgraded"
                old_gym_id = None
                old_gym_name = None
                if is_upgraded and p.order_id:
                    original_pass = next((op for op in all_rows if op.id == p.order_id), None)
                    if original_pass:
                        old_gym_id = int(original_pass.gym_id)
                        old_gym_info = _get_gym_info(old_gym_id)
                        old_gym_name = old_gym_info.get("name")

                actual_days = None
                rescheduled_days = None
                pass_id_to_check = p.order_id if (is_upgraded and p.order_id) else p.id
                has_reschedule_audit = _check_audit_used(pass_id_to_check, "reschedule")
                is_edited = has_reschedule_audit or bool(p.partial_schedule)
                if is_edited:
                    # all_pass_days already available from pass_days (was query at lines 264-269)
                    actual_days = [
                        day.scheduled_date.isoformat()
                        for day in pass_days
                        if not (day.reschedule_count and day.reschedule_count > 0)
                    ]
                    rescheduled_days = [
                        day.scheduled_date.isoformat()
                        for day in pass_days
                        if day.reschedule_count and day.reschedule_count > 0
                    ]

                # price lookup (was get_price_for_gym at line 282)
                try:
                    actual_price_minor = price_map.get(int(p.gym_id))
                    if actual_price_minor is not None:
                        actual_amount = actual_price_minor / 100
                    else:
                        actual_amount = (p.amount_paid or 0) / 100
                except Exception:
                    actual_amount = (p.amount_paid or 0) / 100

                resp.append(
                    PassSummary(
                        pass_id=p.id,
                        amount=actual_amount,
                        gym_id=int(p.gym_id),
                        gym_name=gym.get("name"),
                        locality=gym.get("location"),
                        city=gym.get("city"),
                        valid_from=(p.valid_from or p.start_date).isoformat(),
                        valid_until=(p.valid_until or p.end_date).isoformat(),
                        days_total=int(p.days_total or 0),
                        selected_time=p.selected_time,
                        remaining_days=remaining,
                        next_dates=next_dates,
                        can_reschedule=can_res,
                        can_upgrade=can_upg,
                        is_edited=is_edited,
                        actual_days=actual_days,
                        rescheduled_days=rescheduled_days,
                        is_upgraded=is_upgraded,
                        old_gym_id=old_gym_id,
                        old_gym_name=old_gym_name,
                    )
                )

            response = ListActiveResponse(client_id=str(client_id), passes=resp)
        finally:
            try:
                dps.close()
            except Exception:
                pass
    return response


def _today_ist():
    return dailypass_routes.now_ist().date()


def _once_used(db_session, pass_id: str, action: str) -> bool:
    return (
        db_session.query(DailyPassAudit.id)
        .filter(DailyPassAudit.pass_id == pass_id, DailyPassAudit.action == action)
        .limit(1)
        .first()
        is not None
    )


def _load_gym_info(db: Session, gym_id: int):
    row = db.execute(
        text("SELECT name, location, city FROM gyms WHERE gym_id = :gid"),
        {"gid": gym_id},
    ).one_or_none()
    if not row:
        return {"name": f"Gym {gym_id}", "location": None, "city": None}
    return {"name": row[0], "location": row[1], "city": row[2]}


def _batch_load_prices(dps, gym_ids: list) -> dict:
    """Batch load prices for all gyms at once. Returns {gym_id: price_minor}."""
    if not gym_ids:
        return {}
    price_map = {}
    pricing_records = (
        dps.query(DailyPassPricing)
        .filter(DailyPassPricing.gym_id.in_([str(gid) for gid in gym_ids]))
        .all()
    )
    for rec in pricing_records:
        # Same logic as get_price_for_gym: round(int(rec.discount_price) * markup)
        price_map[int(rec.gym_id)] = round(int(rec.discount_price) * get_markup_multiplier())
    return price_map
