# fittbot_api/v1/payments/razorpay/routes.py
# Enterprise-hardened Razorpay subscriptions flow with resilient webhook handling,
# strict idempotency, consistent premium gating, and safe logging.
import hashlib
import json
import logging
import secrets
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, Optional, Tuple, TypeVar

import httpx
from sqlalchemy.exc import InvalidRequestError, IntegrityError
from fastapi import APIRouter, Depends, HTTPException, Path, Request
from fastapi import status as http_status
from fastapi.responses import JSONResponse
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from sqlalchemy import or_
from sqlalchemy.orm import Session
from starlette.requests import ClientDisconnect  # for webhook client aborts

from ..config.database import get_db_session
from ..config.settings import get_payment_settings
from ..models.catalog import CatalogProduct
from ..models.entitlements import Entitlement
from ..models.enums import EntType, StatusEnt
from ..models.orders import OrderItem
from ..models.payments import Payment
from ..models.subscriptions import Subscription
from ..models.webhook_idempotency import WebhookMonitoringStats
from ..models.webhook_logs import WebhookProcessingLog
from ..models.idempotency import IdempotencyKey
from ..services.entitlement_service import EntitlementService
from app.models.fittbot_models import FreeTrial
from app.fittbot_api.v1.client.client_api.nutrition.nutrition_eligibility_service import (
    grant_nutrition_eligibility_sync,
    calculate_nutrition_sessions_from_fittbot_plan,
)

from ..razorpay.client import (
    cancel_subscription_async as rzp_cancel_subscription,
    create_subscription_async as rzp_create_subscription,
    get_payment_async as rzp_get_payment,
    get_plan_async as rzp_get_plan,
    get_subscription_async as rzp_get_subscription,
)
from ..razorpay.crypto import verify_checkout_subscription_sig, verify_webhook_sig
from ..razorpay.db_helpers import (
    PROVIDER,
    create_or_update_subscription_pending,
    create_payment,
    create_pending_order,
    cycle_window_from_sub_entity,
    find_order_by_sub_id,
    get_subscription_by_provider_id,
    mark_order_paid,
    new_id,
)
from ..razorpay.receipt_service import send_subscription_receipt
from ..utils import run_sync_db_operation
from ..utils.http_client import close_async_http_clients
from app.utils.request_auth import resolve_authenticated_user_id

logger = logging.getLogger("payments.razorpay")
router = APIRouter(prefix="/razorpay_payments", tags=["Razorpay Subscriptions"])
subscription_routes_router = router  # Alias for backward compatibility
security = HTTPBearer(auto_error=False)

IST = timezone(timedelta(hours=5, minutes=30))
T = TypeVar("T")


async def _db_call(db: Session, fn: Callable[[Session], T]) -> T:
    """Execute blocking DB work inside the shared executor."""
    return await run_sync_db_operation(lambda: fn(db))


async def _db_add(db: Session, *objects: Any) -> None:
    """Add ORM objects within the executor to avoid blocking the loop."""
    def _add(session: Session) -> None:
        for obj in objects:
            session.add(obj)
    await _db_call(db, _add)


async def _db_commit(db: Session) -> None:
    await run_sync_db_operation(db.commit)


async def _db_flush(db: Session) -> None:
    await run_sync_db_operation(db.flush)


async def _db_rollback(db: Session) -> None:
    await run_sync_db_operation(db.rollback)


async def _increment_webhook_stat(db: Session, field: str, *, flush: bool = False) -> None:
    """Atomically bump a webhook monitoring counter."""
    def _op(session: Session) -> None:
        stats = WebhookMonitoringStats.get_current_hour_stats(session)
        current = getattr(stats, field) or 0
        setattr(stats, field, current + 1)
        session.add(stats)
        if flush:
            session.flush()

    await _db_call(db, _op)


def now_ist() -> datetime:
    return datetime.now(IST)


def ensure_timezone_aware(dt: Optional[datetime]) -> Optional[datetime]:
    """Ensure datetime is timezone-aware (IST)"""
    if dt is None:
        return None
    if dt.tzinfo is None:
        # Assume naive datetimes are in IST
        return dt.replace(tzinfo=IST)
    return dt


def _safe(fn, default=None):
    try:
        return fn()
    except Exception:
        return default


def _lock_query(query):
    """Apply row-level lock to prevent race conditions; fall back silently if unsupported."""
    try:
        return query.with_for_update()
    except (InvalidRequestError, AttributeError):
        return query
    except Exception as lock_err:
        logger.debug("Lock not applied: %s", lock_err)
        return query


def _new_webhook_log_id() -> str:
    ms = int(time.time() * 1000)
    rand = secrets.token_hex(3)
    return f"whl_{ms}_{rand}"


def _summarize_webhook(payload: Dict[str, Any]) -> Dict[str, Any]:
    evt = payload.get("event", "")
    p = payload.get("payload", {})
    sub = _safe(lambda: p["subscription"]["entity"], {}) or {}
    pay = _safe(lambda: p["payment"]["entity"], {}) or {}
    order = _safe(lambda: p["order"]["entity"], {}) or {}
    return {
        "event": evt,
        "subscription_id": sub.get("id"),
        "customer_id": _safe(lambda: sub.get("notes", {}).get("customer_id"))
        or _safe(lambda: pay.get("notes", {}).get("customer_id"))
        or _safe(lambda: order.get("notes", {}).get("customer_id")),
        "payment_id": pay.get("id"),
        "order_id": pay.get("order_id") or order.get("id"),
        "amount": pay.get("amount"),
        "currency": pay.get("currency"),
        "created_at": pay.get("created_at") or sub.get("start_at"),
        "method": pay.get("method"),
    }


async def log_verification_event(event_type: str, payment_id: str, sub_id: str, extra_data=None):
    logger.info(
        "VERIFY_EVENT",
        extra={
            "event": event_type,
            "payment_id": payment_id,
            "subscription_id": sub_id,
            "timestamp": now_ist().isoformat(),  # ✅ Changed to IST
            **(extra_data or {}),
        },
    )


async def log_security_event(event_type: str, data: dict):
    logger.warning(
        "SECURITY_EVENT",
        extra={"event": event_type, "timestamp": now_ist().isoformat(), **(data or {})},  # ✅ Changed to IST
    )


def _rzp_event_id(payload: Dict[str, Any]) -> str:
    # Prefer explicit id if provided by Razorpay
    if "id" in payload:
        return payload["id"]

    event_type = payload.get("event", "unknown")
    p = payload.get("payload", {})

    if event_type == "subscription.activated":
        sub_e = p.get("subscription", {}).get("entity", {}) or {}
        return f"SUB_ACTIVATED|{sub_e.get('id','')}|{sub_e.get('start_at') or sub_e.get('current_start') or ''}"

    if event_type == "subscription.charged":
        pay_e = p.get("payment", {}).get("entity", {}) or {}
        return f"SUB_CHARGED|{pay_e.get('subscription_id','')}|{pay_e.get('id','')}"

    if event_type == "subscription.cancelled":
        sub_e = p.get("subscription", {}).get("entity", {}) or {}
        return f"SUB_CANCELLED|{sub_e.get('id','')}|{sub_e.get('cancelled_at') or ''}"

    if event_type == "subscription.expired":
        sub_e = p.get("subscription", {}).get("entity", {}) or {}
        return f"SUB_EXPIRED|{sub_e.get('id','')}|{sub_e.get('end_at') or sub_e.get('current_end') or ''}"

    # IMPORTANT: payment.captured -> use payment_id ONLY for strict idempotency
    if event_type == "payment.captured":
        pay_e = p.get("payment", {}).get("entity", {}) or {}
        return f"PAYMENT_CAPTURED|{pay_e.get('id','')}"

    # Fallback deterministic hash
    try:
        to_hash = json.dumps(payload, sort_keys=True).encode("utf-8")
        h = hashlib.sha1(to_hash).hexdigest()
        return f"{event_type}|{h}"
    except Exception:
        return event_type


def _mask(value: Optional[str], left=4, right=4) -> str:
    if not value:
        return ""
    if len(value) <= left + right:
        return "*" * len(value)
    return f"{value[:left]}...{value[-right:]}"


# ---------------------------------------------------------------------------
# Receipts (idempotent)
# ---------------------------------------------------------------------------

async def _send_receipt_if_needed(db: Session, sub_id: Optional[str], payment_id: Optional[str]) -> None:
    try:
        if not sub_id or not payment_id:
            return
        sub = await _db_call(db, lambda session: get_subscription_by_provider_id(session, sub_id))
        if not sub:
            return
        pay = await _db_call(
            db,
            lambda session: session.query(Payment)
            .filter(Payment.provider == PROVIDER, Payment.provider_payment_id == payment_id)
            .first()
        )
        if not pay:
            return
        meta = pay.payment_metadata or {}
        if meta.get("receipt_sent"):
            return

        # TODO: plug in real invoice numbering + customer email
        pdf_path = send_subscription_receipt(
            db,
            customer_id=sub.customer_id,
            subscription=sub,
            payment=pay,
            invoice_no=None,  # let service assign
            to_email=None,  # let service determine destination
        )
        if pdf_path:
            meta["receipt_sent"] = True
            pay.payment_metadata = meta
            await _db_add(db, pay)
            logger.info("Receipt sent", extra={"payment_id": _mask(payment_id)})

    except Exception as e:
        # Never break the payment flow for mailing failures
        logger.warning("Receipt send failure", extra={"error": str(e), "payment_id": _mask(payment_id) if payment_id else None})


# ---------------------------------------------------------------------------
# Premium gating helpers
# ---------------------------------------------------------------------------

async def _find_current_subscription(db: Session, client_id: str, lock: bool = False) -> Optional[Subscription]:
    now = now_ist()  # ✅ Changed to IST
    return await _db_call(
        db,
        lambda session: _lock_query(
            session.query(Subscription)
            .filter(
                Subscription.customer_id == client_id,
                Subscription.provider == PROVIDER,
                Subscription.status.in_(["active", "renewed", "canceled"]),
                Subscription.active_from <= now,
                or_(Subscription.active_until == None, Subscription.active_until >= now),
            )
            .order_by(Subscription.created_at.desc())
        ).first() if lock else session.query(Subscription)
        .filter(
            Subscription.customer_id == client_id,
            Subscription.provider == PROVIDER,
            Subscription.status.in_(["active", "renewed", "canceled"]),
            Subscription.active_from <= now,
            or_(Subscription.active_until == None, Subscription.active_until >= now),
        )
        .order_by(Subscription.created_at.desc())
        .first()
    )


async def _has_premium_now(db: Session, client_id: str) -> Tuple[bool, Optional[Subscription], Optional[str]]:
    sub = await _find_current_subscription(db, client_id)
    if not sub:
        return False, None, "no_subscription"

    captured_payment = await _db_call(
        db,
        lambda session: session.query(Payment)
        .filter(
            Payment.provider == PROVIDER,
            Payment.provider_payment_id == sub.latest_txn_id,
            Payment.status == "captured",
        )
        .first()
    )
    captured = captured_payment is not None
    return captured, sub, None if captured else "no_captured_payment_for_cycle"



@router.post("/subscriptions/create")
async def create_subscription(request: Request, db: Session = Depends(get_db_session)):
 
    settings = get_payment_settings()
    body = await request.json()
    plan_sku = body.get("plan_sku")
    if not plan_sku:
        raise HTTPException(status_code=400, detail="plan_sku is required")

    user_id = resolve_authenticated_user_id(request, body.get("user_id"))

    catalog: CatalogProduct = await _db_call(
        db,
        lambda session: session.query(CatalogProduct)
        .filter(CatalogProduct.sku == plan_sku, CatalogProduct.active == True)
        .first()
    )
    if not catalog or not catalog.razorpay_plan_id:
        raise HTTPException(status_code=404, detail="Invalid or inactive SKU / missing razorpay_plan_id")

    # Optional: cross-check plan with Razorpay
    rp = None
    try:
        rp = await rzp_get_plan(catalog.razorpay_plan_id)
        if rp.get("item", {}).get("amount") != catalog.base_amount_minor or rp.get("item", {}).get("currency") != "INR":
            raise HTTPException(status_code=409, detail="Plan mismatch between DB and Razorpay")
    except (httpx.RequestError, httpx.HTTPStatusError) as exc:
        logger.warning("Plan validation error", extra={"error": str(exc)})

    # Determine total_count based on plan period (default 12 for monthly subscriptions)
    total_count = 12  # Default for monthly plans (1 year = 12 months)
    try:
        period = (rp or {}).get("period")
        interval = (rp or {}).get("interval")
        if period in ("year", "yearly"):
            total_count = 1  # Annual subscription
        elif period == "monthly" and interval and int(interval) > 1:
            total_count = 1  # Multi-month subscription (e.g., 3-month, 6-month)
        elif period == "monthly":
            total_count = 12  # Standard monthly subscription (1 year)
    except Exception as e:
        logger.debug("Using default total_count: %s", e)
        total_count = 12  # Fallback to monthly default

    try:
        sub = await rzp_create_subscription(
            catalog.razorpay_plan_id,
            notes={"plan_sku": plan_sku, "customer_id": user_id},
            total_count=total_count,
        )
    except (httpx.RequestError, httpx.HTTPStatusError) as exc:
        resp_text = None
        if isinstance(exc, httpx.HTTPStatusError) and exc.response is not None:
            try:
                resp_text = exc.response.text
            except Exception:  # pragma: no cover - logging safeguard
                resp_text = None
        logger.error(
            "Razorpay subscription.create failed",
            extra={"error": str(exc), "body": resp_text},
        )
        raise HTTPException(status_code=502, detail="Failed to create subscription with Razorpay")

    sub_id = sub["id"]

    # Create internal order + pending subscription record
    order = await _db_call(
        db,
        lambda session: create_pending_order(
            session,
            user_id=user_id,
            amount_minor=catalog.base_amount_minor,
            sub_id=sub_id,
            sku=catalog.sku,
            title=catalog.title,
        )
    )
    await _db_call(
        db,
        lambda session: create_or_update_subscription_pending(
            session, user_id=user_id, plan_sku=plan_sku, provider_subscription_id=sub_id
        )
    )
    await _db_commit(db)
    return {
        "subscription_id": sub_id,
        "razorpay_key_id": settings.razorpay_key_id,
        "order_id": order.id,
        "display_title": catalog.title,
    }


@router.post("/subscriptions/verify")
async def verify_subscription(request: Request, db: Session = Depends(get_db_session)):

    settings = get_payment_settings()
    body = await request.json()

    # Ensure caller is authenticated (and optional legacy body id matches)
    resolve_authenticated_user_id(request, body.get("user_id", None))

    pid = body.get("razorpay_payment_id")
    sid = body.get("razorpay_subscription_id")
    sig = body.get("razorpay_signature")
    if not all([pid, sid, sig]):
        raise HTTPException(status_code=400, detail="Missing required fields")

    if not verify_checkout_subscription_sig(settings.razorpay_key_secret, pid, sid, sig):
        await log_security_event("INVALID_SIGNATURE", {"payment_id": _mask(pid), "sub_id": _mask(sid)})
        raise HTTPException(status_code=403, detail="Invalid signature")
    
    

    # Fetch payment securely
    try:
        payment_data = await rzp_get_payment(pid)
        payment_status = payment_data.get("status")
    except httpx.TimeoutException:
        await log_verification_event("API_TIMEOUT", pid, sid)
        return JSONResponse(
            status_code=200,
            content={
                "verified": True,
                "captured": False,
                "retryAfterMs": 3000,
                "message": "Payment verification in progress",
            },
            headers={"Retry-After": "3"},
        )
    except httpx.HTTPStatusError as exc:
        status_code = exc.response.status_code if exc.response is not None else None
        if status_code == 429:
            await log_security_event("RATE_LIMITED", {"payment_id": _mask(pid)})
            return JSONResponse(
                status_code=429,
                content={
                    "verified": False,
                    "captured": False,
                    "error": "rate_limited",
                    "message": "Please retry soon",
                },
                headers={"Retry-After": "2"},
            )
        await log_verification_event("API_ERROR", pid, sid, {"error": str(exc)})
        return JSONResponse(
            status_code=200,
            content={
                "verified": True,
                "captured": False,
                "retryAfterMs": 5000,
                "message": "Verifying payment status",
            },
            headers={"Retry-After": "5"},
        )
    except httpx.RequestError as exc:
        await log_security_event("UNKNOWN_API_ERROR", {"payment_id": _mask(pid), "error": str(exc)})
        return JSONResponse(
            status_code=200,
            content={
                "verified": True,
                "captured": False,
                "retryAfterMs": 5000,
                "message": "Payment verification in progress",
            },
            headers={"Retry-After": "5"},
        )

    if payment_status == "captured":
        return await handle_captured_payment_secure(db, pid, sid, payment_data)

    if payment_status == "authorized":
        await log_verification_event("AUTHORIZED", pid, sid)
        return JSONResponse(
            status_code=200,
            content={
                "verified": True,
                "captured": False,
                "retryAfterMs": 2000,
                "message": "Payment authorized, finalizing...",
            },
            headers={"Retry-After": "2"},
        )

    if payment_status in ["failed", "refunded"]:
        await log_verification_event("FAILED_PAYMENT", pid, sid, {"status": payment_status})
        return {"verified": False, "captured": False, "status": payment_status, "message": f"Payment {payment_status}"}

    await log_security_event("UNKNOWN_STATUS", {"payment_id": _mask(pid), "status": payment_status})
    return JSONResponse(
        status_code=200,
        content={"verified": True, "captured": False, "retryAfterMs": 3000, "message": "Payment verification in progress"},
        headers={"Retry-After": "3"},
    )


async def handle_captured_payment_secure(db: Session, pid: str, sid: str, payment_data: dict):
    """Finalize captured payments quickly; webhook remains canonical (idempotent)."""
    subscription = await _db_call(
        db,
        lambda session: _lock_query(
            session.query(Subscription).filter(
                Subscription.provider == PROVIDER,
                Subscription.id == sid
            )
        ).first() or get_subscription_by_provider_id(session, sid)
    )
    if not subscription:
        await log_security_event("SUBSCRIPTION_NOT_FOUND", {"sub_id": _mask(sid), "payment_id": _mask(pid)})
        return {"verified": False, "captured": False, "error": "subscription_not_found"}

    order = await _db_call(db, lambda session: find_order_by_sub_id(session, sid))
    existing_payment = await _db_call(
        db,
        lambda session: session.query(Payment)
        .filter(Payment.provider == PROVIDER, Payment.provider_payment_id == pid, Payment.status == "captured")
        .first()
    )

    amount = int(payment_data.get("amount") or 0)
    currency = payment_data.get("currency") or "INR"
    method = payment_data.get("method")
    if amount <= 0:
        await log_security_event("INVALID_AMOUNT", {"payment_id": _mask(pid), "amount": amount})
        return {"verified": False, "captured": False, "error": "invalid_amount"}

    now = now_ist()  # ✅ Changed to IST
    start_dt = subscription.active_from or now
    end_dt = subscription.active_until

    try:
        sub_entity = await rzp_get_subscription(sid)
        if isinstance(sub_entity, dict):
            s_start, s_end = cycle_window_from_sub_entity(sub_entity)
            start_dt = s_start or start_dt
            end_dt = s_end or end_dt
    except (httpx.RequestError, httpx.HTTPStatusError) as fetch_err:
        await log_verification_event("SUB_LOOKUP_FAILED", pid, sid, {"error": str(fetch_err)})

    if order is None:
        await log_security_event(
            "ORDER_NOT_FOUND_FOR_SUBSCRIPTION",
            {"sub_id": _mask(sid), "payment_id": _mask(pid), "customer_id": subscription.customer_id},
        )

    fallback_end = end_dt or subscription.active_until or ((start_dt or now) + timedelta(days=30))
    already_processed = existing_payment is not None

    subscription.status = "active"
    subscription.latest_txn_id = pid
    subscription.active_from = start_dt or now
    subscription.active_until = end_dt or fallback_end
    try:
        sub_full = await rzp_get_subscription(sid)
        subscription.auto_renew = bool(sub_full.get("auto_renew", True))
    except (httpx.RequestError, httpx.HTTPStatusError):
        pass

    async def _persist_updates() -> None:
        def _op(session: Session) -> None:
            try:
                if order:
                    mark_order_paid(session, order)

                if not already_processed and order:
                    create_payment(
                        session,
                        order=order,
                        user_id=subscription.customer_id,
                        amount_minor=amount,
                        currency=currency,
                        provider_payment_id=pid,
                        meta={"method": method, "source": "verify_endpoint"},
                    )

                session.add(subscription)

                if order:
                    entitlements = (
                        session.query(Entitlement)
                        .join(OrderItem, Entitlement.order_item_id == OrderItem.id)
                        .filter(OrderItem.order_id == order.id)
                        .all()
                    )
                    if not entitlements:
                        EntitlementService(session).create_entitlements_from_order(order)
                        entitlements = (
                            session.query(Entitlement)
                            .join(OrderItem, Entitlement.order_item_id == OrderItem.id)
                            .filter(OrderItem.order_id == order.id)
                            .all()
                        )
                    for ent in entitlements:
                        ent.entitlement_type = EntType.app
                        ent.active_from = subscription.active_from
                        ent.active_until = subscription.active_until
                        ent.status = StatusEnt.active
                        session.add(ent)

                try:
                    free_trial = (
                        session.query(FreeTrial)
                        .filter(FreeTrial.client_id == int(subscription.customer_id))
                        .first()
                    )
                    if free_trial and free_trial.status != "expired":
                        free_trial.status = "expired"
                        session.add(free_trial)
                        logger.info(f"Free trial expired for client_id: {subscription.customer_id}")
                except Exception as ft_error:
                    logger.warning(f"Failed to update free_trial status: {str(ft_error)}")
                session.commit()
            except Exception:
                session.rollback()
                raise

        await _db_call(db, _op)

    try:
        await _persist_updates()
    except Exception as e:
        await log_security_event(
            "ACTIVATION_FAILED",
            {
                "payment_id": _mask(pid),
                "sub_id": _mask(sid),
                "error": str(e),
                "order_found": bool(order),
                "subscription_exists": True,
            },
        )
        return JSONResponse(
            status_code=200,
            content={
                "verified": True,
                "captured": True,
                "subscription_active": False,
                "message": "Payment captured, activation pending webhook",
                "retryAfterMs": 5000,
            },
            headers={"Retry-After": "5"},
        )

    await log_verification_event(
        "PAYMENT_ALREADY_ACTIVATED" if already_processed else "PAYMENT_ACTIVATED",
        pid,
        sid,
        {"amount": amount, "customer_id": subscription.customer_id, "active_until": subscription.active_until.isoformat() if subscription.active_until else None},
    )

    if order:
        await _send_receipt_if_needed(db, sid, pid)

    # Grant nutrition eligibility for Fittbot subscription (Diamond/Platinum plans)
    # Run regardless of payment idempotency; service is idempotent by source_id.
    try:
        # Get plan info from subscription product_id
        plan_name = None
        duration_months = 0

        if subscription.product_id:
            plan_name = subscription.product_id
            product_lower = subscription.product_id.lower()
            if "12" in product_lower:
                duration_months = 12
            elif "6" in product_lower:
                duration_months = 6

        # Calculate if eligible for nutrition sessions
        if plan_name and duration_months >= 6:
            sessions = calculate_nutrition_sessions_from_fittbot_plan(plan_name, duration_months)
            if sessions > 0:
                def _grant_nutrition(session: Session) -> None:
                    grant_nutrition_eligibility_sync(
                        db=session,
                        client_id=int(subscription.customer_id),
                        source_type="fittbot_subscription",
                        source_id=sid,
                        plan_name=plan_name,
                        duration_months=duration_months,
                        gym_id=None,  # Fittbot subscriptions are not gym-specific
                    )
                await _db_call(db, _grant_nutrition)
                logger.info(
                    f"[NUTRITION_ELIGIBILITY_GRANTED] Fittbot subscription: "
                    f"client={subscription.customer_id}, plan={plan_name}, sessions={sessions}, "
                    f"already_processed={already_processed}"
                )
    except Exception as nutr_exc:
        logger.warning(f"[NUTRITION_ELIGIBILITY_ERROR] Failed to grant nutrition eligibility: {nutr_exc}")

    return {
        "verified": True,
        "captured": True,
        "subscription_active": True,
        "has_premium": True,
        "message": "Payment already processed" if already_processed else "Payment successful - Premium activated",
        "active_from": subscription.active_from.isoformat() if subscription.active_from else None,
        "active_until": subscription.active_until.isoformat() if subscription.active_until else None,
        "auto_renew": bool(subscription.auto_renew),
        "payment_id": pid,
        "subscription_id": sid,
        "order_id": order.id if order else None,
    }



async def process_razorpay_webhook_payload(raw: bytes, signature: str, db: Session) -> Dict[str, Any]:
    settings = get_payment_settings()

    logger.info("=" * 80)
    logger.info("🔔 INCOMING RAZORPAY WEBHOOK")
    logger.info("=" * 80)
    logger.info(f"📦 Raw payload (first 500 chars): {raw.decode('utf-8')[:500]}")
    logger.info(f"🔐 Signature: {signature[:30]}...")

    if not verify_webhook_sig(settings.razorpay_webhook_secret, raw, signature):
        logger.error("❌ SIGNATURE VERIFICATION FAILED!")
        raise HTTPException(status_code=401, detail="Invalid signature")

    logger.info("✅ Signature verified")

    payload = json.loads(raw.decode("utf-8"))
    event = payload.get("event", "")
    event_id = _rzp_event_id(payload)

    logger.info(f"📋 Event Type: {event}")
    logger.info(f"🆔 Event ID: {event_id}")

    summary = _summarize_webhook(payload)
    logger.info("📊 Webhook Summary:")
    logger.info(f"   Event: {summary.get('event')}")
    logger.info(f"   Customer ID: {summary.get('customer_id')}")
    logger.info(f"   Subscription ID: {summary.get('subscription_id')}")
    logger.info(f"   Payment ID: {summary.get('payment_id')}")
    logger.info(f"   Order ID: {summary.get('order_id')}")
    logger.info(f"   Amount: {summary.get('amount')}")
    logger.info(f"   Currency: {summary.get('currency')}")

    logger.info("🗂️  Full Payload Structure:")
    logger.info(f"   Contains: {payload.get('contains', [])}")
    if 'payload' in payload:
        logger.info(f"   Payload keys: {list(payload['payload'].keys())}")
        if 'subscription' in payload['payload']:
            sub_entity = payload['payload']['subscription'].get('entity', {})
            logger.info(f"   Subscription Entity Keys: {list(sub_entity.keys())}")
            logger.info(f"   Subscription ID: {sub_entity.get('id')}")
            logger.info(f"   Subscription Status: {sub_entity.get('status')}")
            logger.info(f"   Subscription Notes: {sub_entity.get('notes', {})}")
        if 'payment' in payload['payload']:
            pay_entity = payload['payload']['payment'].get('entity', {})
            logger.info(f"   Payment Entity Keys: {list(pay_entity.keys())}")
            logger.info(f"   Payment ID: {pay_entity.get('id')}")
            logger.info(f"   Payment Status: {pay_entity.get('status')}")

    logger.info("-" * 80)

    try:
        await _increment_webhook_stat(db, "total_webhooks_received", flush=True)
    except Exception:
        pass

    existing = await _db_call(
        db,
        lambda session: session.query(WebhookProcessingLog)
        .filter(WebhookProcessingLog.event_id == event_id)
        .first()
    )
    if existing and existing.status == "completed":
        try:
            def _mark_completed_duplicate(session: Session) -> None:
                stats = WebhookMonitoringStats.get_current_hour_stats(session)
                stats.duplicate_webhooks_blocked = (stats.duplicate_webhooks_blocked or 0) + 1
                session.add(stats)
                existing.retry_count = (existing.retry_count or 0) + 1
                session.add(existing)
                session.commit()

            await _db_call(db, _mark_completed_duplicate)
        except Exception:
            pass
        return {"status": "already_processed"}

    if existing and existing.status == "processing":
        try:
            def _mark_processing_duplicate(session: Session) -> None:
                stats = WebhookMonitoringStats.get_current_hour_stats(session)
                stats.duplicate_webhooks_blocked = (stats.duplicate_webhooks_blocked or 0) + 1
                session.add(stats)
                existing.retry_count = (existing.retry_count or 0) + 1
                session.add(existing)
                session.commit()

            await _db_call(db, _mark_processing_duplicate)
        except Exception:
            pass
        return {"status": "processing"}

    resolved_customer_id = (
        _safe(lambda: payload["payload"]["subscription"]["entity"]["notes"].get("customer_id"))
        or _safe(lambda: payload["payload"]["payment"]["entity"]["notes"].get("customer_id"))
        or _safe(lambda: payload["payload"]["order"]["entity"]["notes"].get("customer_id"))
    )

    def _log_entry(status: str = "processing") -> WebhookProcessingLog:
        log = WebhookProcessingLog(
            id=_new_webhook_log_id(),
            event_id=event_id,
            event_type=event,
            customer_id=resolved_customer_id or "unknown",
            status=status,
            started_at=now_ist(),
            raw_event_data=json.dumps(payload),
            result_summary=None,
            error_message=None,
            retry_count=0,
            webhook_source="razorpay_pg",
        )
        return log

    log = _log_entry("processing")
    await _db_add(db, log)

    try:
        if event == "payment.captured":
            await _on_payment_captured(db, payload, log)
        elif event == "subscription.activated":
            await _on_subscription_activated(db, payload, log)
        elif event == "subscription.charged":
            await _on_subscription_charged(db, payload, log)
        elif event == "subscription.cancelled":
            await _on_subscription_cancelled(db, payload, log)
        elif event == "subscription.completed":
            await _on_subscription_completed(db, payload, log)
        elif event == "subscription.halted":
            await _on_subscription_halted(db, payload, log)
        elif event == "subscription.renewed":
            await _on_subscription_renewed(db, payload, log)
        else:
            log.status = "ignored"
            log.result_summary = f"Unhandled event type {event}"
            await _db_add(db, log)
            try:
                await _db_commit(db)
            except IntegrityError as e:
                if "Duplicate entry" in str(e) and "event_id" in str(e):
                    await _db_rollback(db)
                    logger.info(f"Duplicate webhook detected via DB constraint (ignored event): {event_id}")
                    return {"status": "already_processed", "event": event}
                await _db_rollback(db)
                raise
            return {"status": "ignored", "event": event}

        log.status = "completed"
        log.completed_at = now_ist()
        log.result_summary = log.result_summary or f"Processed {event}"
        await _db_add(db, log)
        try:
            await _increment_webhook_stat(db, "webhooks_processed_successfully")
        except Exception:
            pass

        # Commit all changes (subscription, payment, entitlements, nutrition eligibility)
        try:
            await _db_commit(db)
        except IntegrityError as e:
            if "Duplicate entry" in str(e) and "event_id" in str(e):
                await _db_rollback(db)
                logger.info(f"Duplicate webhook detected via DB constraint: {event_id}")
                return {"status": "already_processed", "event": event}
            await _db_rollback(db)
            raise

        return {"status": "processed", "event": event}

    except Exception as exc:
        # Rollback first to clear any broken session state (e.g., after IntegrityError)
        try:
            await _db_rollback(db)
        except Exception:
            pass

        log.status = "failed"
        log.error_message = str(exc)
        await _db_add(db, log)
        try:
            await _increment_webhook_stat(db, "webhooks_failed")
        except Exception:
            pass
        # Try to commit at least the log entry and stats
        try:
            await _db_commit(db)
        except Exception:
            pass
        raise


@router.post("/webhooks/razorpay")
async def webhook_razorpay(request: Request, db: Session = Depends(get_db_session)):
    settings = get_payment_settings()

    # Handle client disconnect gracefully
    try:
        raw = await request.body()
    except ClientDisconnect:
        logger.warning("⚠️  Client disconnected before body could be read - webhook likely succeeded on retry")
        return {"status": "client_disconnected", "message": "Client disconnected, webhook will be retried"}

    sig = request.headers.get("X-Razorpay-Signature", "")
    return await process_razorpay_webhook_payload(raw, sig, db)

    # Idempotency gate
    existing = await _db_call(
        db,
        lambda session: session.query(WebhookProcessingLog)
        .filter(WebhookProcessingLog.event_id == event_id)
        .first()
    )
    if existing and existing.status == "completed":
        try:
            def _mark_completed_duplicate(session: Session) -> None:
                stats = WebhookMonitoringStats.get_current_hour_stats(session)
                stats.duplicate_webhooks_blocked = (stats.duplicate_webhooks_blocked or 0) + 1
                session.add(stats)
                existing.retry_count = (existing.retry_count or 0) + 1
                session.add(existing)
                session.commit()

            await _db_call(db, _mark_completed_duplicate)
        except Exception:
            pass
        return {"status": "already_processed"}

    if existing and existing.status == "processing":
        try:
            def _mark_processing_duplicate(session: Session) -> None:
                stats = WebhookMonitoringStats.get_current_hour_stats(session)
                stats.duplicate_webhooks_blocked = (stats.duplicate_webhooks_blocked or 0) + 1
                session.add(stats)
                existing.retry_count = (existing.retry_count or 0) + 1
                session.add(existing)
                session.commit()

            await _db_call(db, _mark_processing_duplicate)
        except Exception:
            pass
        return {"status": "processing"}

    # Resolve customer id best-effort
    resolved_customer_id = (
        _safe(lambda: payload["payload"]["subscription"]["entity"]["notes"].get("customer_id"))
        or _safe(lambda: payload["payload"]["payment"]["entity"]["notes"].get("customer_id"))
        or _safe(lambda: payload["payload"]["order"]["entity"]["notes"].get("customer_id"))
        or "unknown"
    )

    log = existing or WebhookProcessingLog(
        id=_new_webhook_log_id(),
        event_id=event_id,
        event_type=event,
        customer_id=resolved_customer_id,
        status="processing",
        started_at=now_ist(),
        raw_event_data=json.dumps(payload),  # ensure at-rest protections at DB level
        is_recovery_event=False,
        webhook_source="razorpay_pg",
    )
    if not existing:
        def _insert_log(session: Session) -> None:
            session.add(log)
            session.flush()
            # Record request hash in idempotency table (optional response caching)
            try:
                request_hash = hashlib.sha256(raw).hexdigest()
                ttl = getattr(settings, "idempotency_ttl_delta", None)
                expires_at = now_ist() + ttl if ttl else None  # ✅ Changed to IST
                key_name = f"wh:razorpay:{event_id}"
                idem = (
                    session.query(IdempotencyKey)
                    .filter(IdempotencyKey.key == key_name)
                    .first()
                )
                if not idem:
                    idem = IdempotencyKey(key=key_name, request_hash=request_hash, expires_at=expires_at)
                    session.add(idem)
                    session.flush()
            except Exception:
                pass

        await _db_call(db, _insert_log)

    try:
        logger.info(f"🎯 Routing to handler for event: {event}")

        if event == "subscription.activated":
            logger.info("➡️  Calling _on_subscription_activated")
            await _on_subscription_activated(db, payload, log)
        elif event == "payment.captured":
            logger.info("➡️  Calling _on_payment_captured")
            await _on_payment_captured(db, payload, log)
        elif event == "payment.authorized":
            logger.info("➡️  Handling payment.authorized (observation only)")
            # NOTE: do NOT send receipts on authorization; only observe.
            log.result_summary = (log.result_summary or "") + " | authorized_seen"
        elif event == "subscription.charged":
            logger.info("➡️  Calling _on_subscription_charged")
            await _on_subscription_charged(db, payload, log)
        elif event == "subscription.completed":
            logger.info("➡️  Calling _on_subscription_completed")
            await _on_subscription_completed(db, payload, log)
        elif event == "subscription.authenticated":
            logger.info("➡️  Handling subscription.authenticated (observation only)")
            # Optional: could trigger receipt if you require; we skip to avoid duplicates.
            log.result_summary = (log.result_summary or "") + " | authenticated_seen"
        elif event == "subscription.cancelled":
            logger.info("➡️  Calling _on_subscription_cancelled")
            await _on_subscription_cancelled(db, payload, log)
        elif event == "subscription.expired":
            logger.info("➡️  Calling _on_subscription_expired")
            await _on_subscription_expired(db, payload, log)
        elif event == "payment.failed":
            logger.info("➡️  Calling _on_payment_failed")
            await _on_payment_failed(db, payload, log)
        else:
            logger.warning(f"⚠️  Unhandled event type: {event}")
            log.status = "ignored"
            log.completed_at = now_ist()
            log.result_summary = f"Unhandled: {event}"
            await _db_commit(db)
            return {"status": "ignored", "event": event}

        logger.info(f"✅ Handler completed for {event}")

        log.status = "completed"
        log.completed_at = now_ist()
        if log.started_at and log.completed_at:
            delta = log.completed_at - log.started_at
            log.processing_duration_ms = int(delta.total_seconds() * 1000)

        try:
            await _increment_webhook_stat(db, "webhooks_processed_successfully")
        except Exception:
            pass

        # Cache success in idempotency keys (optional)
        try:
            def _cache_success(session: Session) -> None:
                key_name = f"wh:razorpay:{event_id}"
                idem = (
                    session.query(IdempotencyKey)
                    .filter(IdempotencyKey.key == key_name)
                    .first()
                )
                if idem:
                    idem.set_response(200, json.dumps({"status": "processed", "event": event}).encode("utf-8"))
                    session.add(idem)

            await _db_call(db, _cache_success)
        except Exception:
            pass

        await _db_commit(db)
        return {"status": "processed", "event": event}

    except Exception as e:
        logger.exception("Webhook error")
        log.status = "failed"
        log.error_message = str(e)
        log.completed_at = now_ist()
        try:
            await _increment_webhook_stat(db, "webhooks_failed")
        except Exception:
            pass
        await _db_commit(db)
        raise HTTPException(status_code=500, detail="Internal error")


def _activate(sub: Subscription, start: datetime, end: Optional[datetime], latest_txn_id: str) -> None:
    sub.status = "active"
    sub.active_from = start
    if end:
        sub.active_until = end
    sub.latest_txn_id = latest_txn_id


async def _on_subscription_activated(db: Session, payload: Dict[str, Any], log: WebhookProcessingLog) -> None:
    sub_e = payload["payload"]["subscription"]["entity"]
    pay_e = payload["payload"]["payment"]["entity"]
    sub_id = sub_e["id"]
    user_id = sub_e["notes"].get("customer_id")
    plan_sku = sub_e["notes"].get("plan_sku")

    start, end = cycle_window_from_sub_entity(sub_e)
    if not end:
        full = await rzp_get_subscription(sub_id)
        start = datetime.fromtimestamp(full.get("current_start"), tz=IST) if full.get("current_start") else start  # ✅ Changed to IST
        end = datetime.fromtimestamp(full.get("current_end"), tz=IST) if full.get("current_end") else end  # ✅ Changed to IST

    # Pre-calculate nutrition eligibility params
    plan_sku_local = plan_sku or ""
    plan_name = plan_sku_local.lower() if plan_sku_local else ""
    duration_months = 0
    if "12" in plan_name or "twelve" in plan_name:
        duration_months = 12
    elif "6" in plan_name or "six" in plan_name:
        duration_months = 6

    async def _process_activation() -> bool:
        def _op(session: Session) -> bool:
            # Use row-level lock to prevent race conditions
            sub_local = _lock_query(
                session.query(Subscription).filter(
                    Subscription.provider == PROVIDER,
                    Subscription.id == sub_id
                )
            ).first() or get_subscription_by_provider_id(session, sub_id) or create_or_update_subscription_pending(
                session, user_id=user_id, plan_sku=plan_sku, provider_subscription_id=sub_id
            )
            _activate(sub_local, start, end, pay_e["id"])
            session.add(sub_local)

            order_local = find_order_by_sub_id(session, sub_id)
            if not order_local:
                # Still grant nutrition eligibility even without order
                if duration_months >= 6:
                    sessions_count = calculate_nutrition_sessions_from_fittbot_plan(plan_name, duration_months)
                    if sessions_count > 0:
                        grant_nutrition_eligibility_sync(
                            db=session,
                            client_id=int(user_id),
                            source_type="fittbot_subscription",
                            source_id=sub_id,
                            plan_name=plan_sku_local,
                            duration_months=duration_months,
                            gym_id=None,
                        )
                        logger.info(
                            f"[NUTRITION_ELIGIBILITY_GRANTED] client_id={user_id}, sessions={sessions_count}, "
                            f"source=fittbot_subscription, plan={plan_sku_local}, duration={duration_months}m"
                        )
                # Commit subscription and nutrition eligibility even without order
                session.commit()
                return False

            mark_order_paid(session, order_local)
            create_payment(
                session,
                order=order_local,
                user_id=user_id,
                amount_minor=pay_e["amount"],
                currency=pay_e["currency"],
                provider_payment_id=pay_e["id"],
                meta={"method": pay_e.get("method"), "type": "activation"},
            )

            ent = (
                session.query(Entitlement)
                .join(OrderItem, Entitlement.order_item_id == OrderItem.id)
                .filter(OrderItem.order_id == order_local.id)
                .first()
            )
            if not ent:
                EntitlementService(session).create_entitlements_from_order(order_local)

            entitlements = (
                session.query(Entitlement)
                .join(OrderItem, Entitlement.order_item_id == OrderItem.id)
                .filter(OrderItem.order_id == order_local.id)
                .all()
            )
            for ent_obj in entitlements:
                ent_obj.entitlement_type = EntType.app
                ent_obj.active_from = start
                ent_obj.active_until = end
                ent_obj.status = StatusEnt.active
                session.add(ent_obj)

            # Grant nutrition eligibility INSIDE the same transaction (like RevenueCat does)
            if duration_months >= 6:
                sessions_count = calculate_nutrition_sessions_from_fittbot_plan(plan_name, duration_months)
                if sessions_count > 0:
                    grant_nutrition_eligibility_sync(
                        db=session,
                        client_id=int(user_id),
                        source_type="fittbot_subscription",
                        source_id=sub_id,
                        plan_name=plan_sku_local,
                        duration_months=duration_months,
                        gym_id=None,
                    )
                    logger.info(
                        f"[NUTRITION_ELIGIBILITY_GRANTED] client_id={user_id}, sessions={sessions_count}, "
                        f"source=fittbot_subscription, plan={plan_sku_local}, duration={duration_months}m"
                    )

            # Commit all changes including nutrition eligibility (like RevenueCat does)
            session.commit()
            return True

        return await _db_call(db, _op)

    order_exists = await _process_activation()
    if order_exists:
        await _send_receipt_if_needed(db, sub_id, pay_e["id"])

    log.result_summary = f"activated {user_id} {sub_id} {start}->{end}"


async def _on_payment_captured(db: Session, payload: Dict[str, Any], log: WebhookProcessingLog) -> None:
    pay_e = payload["payload"]["payment"]["entity"]
    sub_id = pay_e.get("subscription_id")
    amount = pay_e.get("amount")
    currency = pay_e.get("currency")
    payment_id = pay_e.get("id")
    user_id = _safe(lambda: pay_e["notes"].get("customer_id"))
    plan_sku = _safe(lambda: pay_e["notes"].get("plan_sku"))

    # Resolve missing subscription id
    order_id = pay_e.get("order_id")
    if not sub_id and payment_id:
        try:
            pfull = await rzp_get_payment(payment_id)
            sub_id = pfull.get("subscription_id") or sub_id
            user_id = user_id or _safe(lambda: pfull.get("notes", {}).get("customer_id"))
            plan_sku = plan_sku or _safe(lambda: pfull.get("notes", {}).get("plan_sku"))
            amount = pfull.get("amount") or amount
            currency = pfull.get("currency") or currency
        except Exception:
            pass

    # If no subscription_id found, this is likely a one-time payment (gym membership, dailypass)
    # Don't try to create a subscription for non-subscription payments
    if not sub_id:
        log.result_summary = f"payment_captured_no_subscription order_id={order_id} payment_id={payment_id}"
        return

    # Skip if already handled
    existing_payment = await _db_call(
        db,
        lambda session: session.query(Payment)
        .filter(Payment.provider == PROVIDER, Payment.provider_payment_id == payment_id, Payment.status == "captured")
        .first()
    )
    if existing_payment:
        log.result_summary = f"payment already processed by verify endpoint {payment_id}"
        return

    # Derive window and get plan_sku from subscription notes
    start, end = None, None
    try:
        if sub_id:
            full = await rzp_get_subscription(sub_id)
            start = datetime.fromtimestamp(full.get("current_start"), tz=IST) if full.get("current_start") else None  # ✅ Changed to IST
            end = datetime.fromtimestamp(full.get("current_end"), tz=IST) if full.get("current_end") else None  # ✅ Changed to IST
            user_id = user_id or _safe(lambda: full.get("notes", {}).get("customer_id"))
            # Get actual plan_sku from subscription notes
            plan_sku = plan_sku or _safe(lambda: full.get("notes", {}).get("plan_sku"))
    except Exception:
        pass

    # Use actual plan_sku or fallback to "app_subscription" only as last resort
    resolved_plan_sku = plan_sku or "app_subscription"

    # Update/create subscription with row-level lock
    if sub_id:
        sub = await _db_call(
            db,
            lambda session: _lock_query(
                session.query(Subscription).filter(
                    Subscription.provider == PROVIDER,
                    Subscription.id == sub_id
                )
            ).first() or get_subscription_by_provider_id(session, sub_id)
        )
        if not sub and user_id:
            sub = await _db_call(
                db,
                lambda session: create_or_update_subscription_pending(
                    session, user_id=user_id, plan_sku=resolved_plan_sku, provider_subscription_id=sub_id
                )
            )
        if sub:
            if not start:
                start = now_ist()  # ✅ Changed to IST
            sub.active_from = start
            if end:
                sub.active_until = end
            sub.latest_txn_id = payment_id
            sub.status = "active"
            await _db_add(db, sub)

    # Order + Payment + Entitlements
    if sub_id:
        order = await _db_call(db, lambda session: find_order_by_sub_id(session, sub_id))
        if order:
            created_at = getattr(order, "created_at", None)
            if created_at:
                created_aware = created_at if created_at.tzinfo else created_at.replace(tzinfo=timezone.utc)
                webhook_seen_at = log.started_at or now_ist()
                try:
                    webhook_seen_at = webhook_seen_at if webhook_seen_at.tzinfo else webhook_seen_at.replace(tzinfo=timezone.utc)
                except AttributeError:
                    webhook_seen_at = now_ist()
                delta_seconds = max(0.0, (webhook_seen_at - created_aware).total_seconds())
                logger.info(
                    "RAZORPAY_WEBHOOK_ORDER_DELAY",
                    extra={
                        "order_id": order.id,
                        "subscription_id": _mask(sub_id),
                        "payment_id": _mask(payment_id),
                        "seconds_since_order": round(delta_seconds, 3),
                        "order_created_at": created_aware.isoformat(),
                        "webhook_started_at": webhook_seen_at.isoformat(),
                    },
                )
            if not existing_payment:
                await _db_call(db, lambda session: mark_order_paid(session, order))
                await _db_call(
                    db,
                    lambda session: create_payment(
                        session,
                        order=order,
                        user_id=user_id or order.customer_id,
                        amount_minor=amount or 0,
                        currency=currency or "INR",
                        provider_payment_id=payment_id,
                        meta={"method": pay_e.get("method"), "type": "webhook_capture"},
                    )
                )

            ent = await _db_call(
                db,
                lambda session: session.query(Entitlement)
                .join(OrderItem, Entitlement.order_item_id == OrderItem.id)
                .filter(OrderItem.order_id == order.id)
                .first()
            )
            if not ent:
                await _db_call(db, lambda session: EntitlementService(session).create_entitlements_from_order(order))

            ents = await _db_call(
                db,
                lambda session: session.query(Entitlement)
                .join(OrderItem, Entitlement.order_item_id == OrderItem.id)
                .filter(OrderItem.order_id == order.id)
                .all()
            )
            for e in ents:
                e.entitlement_type = EntType.app
                if start:
                    e.active_from = start
                if end:
                    e.active_until = end
                e.status = StatusEnt.active
            if ents:
                await _db_add(db, *ents)

            await _send_receipt_if_needed(db, sub_id, payment_id)

    # Grant nutrition eligibility for Fittbot subscription (6+ months plans)
    if sub_id and user_id:
        try:
            plan_sku_local = resolved_plan_sku or ""
            plan_name = plan_sku_local.lower() if plan_sku_local else ""
            duration_months = 0

            # Check for duration in plan_sku
            if "12" in plan_name or "twelve" in plan_name:
                duration_months = 12
            elif "6" in plan_name or "six" in plan_name:
                duration_months = 6

            logger.info(
                f"[NUTRITION_ELIGIBILITY_DEBUG_PAYMENT_CAPTURED] plan_sku={plan_sku_local}, "
                f"plan_name={plan_name}, duration_months={duration_months}"
            )

            # Check if eligible for nutrition sessions (6+ months plans)
            if duration_months >= 6:
                sessions = calculate_nutrition_sessions_from_fittbot_plan(plan_name, duration_months)
                logger.info(f"[NUTRITION_ELIGIBILITY_DEBUG_PAYMENT_CAPTURED] calculated sessions={sessions}")
                if sessions > 0:
                    def _grant_nutrition_captured(session: Session) -> None:
                        grant_nutrition_eligibility_sync(
                            db=session,
                            client_id=int(user_id),
                            source_type="fittbot_subscription",
                            source_id=sub_id,
                            plan_name=plan_sku_local,
                            duration_months=duration_months,
                            gym_id=None,
                        )
                    await _db_call(db, _grant_nutrition_captured)
                    logger.info(
                        f"[NUTRITION_ELIGIBILITY_GRANTED] Razorpay webhook payment.captured: "
                        f"client={user_id}, plan={plan_sku_local}, sessions={sessions}"
                    )
                else:
                    logger.info(f"[NUTRITION_ELIGIBILITY_SKIPPED_PAYMENT_CAPTURED] sessions=0 for plan={plan_sku_local}")
            else:
                logger.info(
                    f"[NUTRITION_ELIGIBILITY_SKIPPED_PAYMENT_CAPTURED] duration < 6 months: "
                    f"duration_months={duration_months}"
                )
        except Exception as nutr_exc:
            logger.warning(f"[NUTRITION_ELIGIBILITY_ERROR] Webhook payment.captured: {nutr_exc}")

    log.result_summary = f"captured and activated {user_id} {sub_id} {start}->{end}"


async def _on_subscription_charged(db: Session, payload: Dict[str, Any], log: WebhookProcessingLog) -> None:
    sub_e = payload["payload"]["subscription"]["entity"]
    pay_e = payload["payload"]["payment"]["entity"]
    sub_id = sub_e["id"]
    user_id = sub_e["notes"].get("customer_id")

    logger.info(
        f"🔄 RENEWAL EVENT | Razorpay Sub: {sub_id} | Customer: {user_id} | Payment: {pay_e['id']}",
        extra={
            "event": "subscription.charged",
            "razorpay_subscription_id": sub_id,
            "customer_id": user_id,
            "payment_id": pay_e["id"],
            "amount": pay_e.get("amount"),
            "currency": pay_e.get("currency"),
            "paid_count": sub_e.get("paid_count"),
            "total_count": sub_e.get("total_count")
        }
    )

    start, end = cycle_window_from_sub_entity(sub_e)
    logger.info(f"📅 Cycle window from entity: {start} → {end}")

    if not end:
        logger.info(f"🔍 Fetching full subscription details for accurate dates")
        full = await rzp_get_subscription(sub_id)
        start = datetime.fromtimestamp(full.get("current_start"), tz=IST) if full.get("current_start") else start  # ✅ Changed to IST
        end = datetime.fromtimestamp(full.get("current_end"), tz=IST) if full.get("current_end") else end  # ✅ Changed to IST
        logger.info(f"📅 Updated cycle window: {start} → {end}")

    # ✅ FIX: Search by customer_id like RevenueCat does (with row-level lock)
    sub = await _db_call(
        db,
        lambda session: _lock_query(
            session.query(Subscription)
            .filter(
                Subscription.customer_id == user_id,
                Subscription.provider == PROVIDER
            )
            .order_by(Subscription.created_at.desc())
        ).first()
    )

    if sub:
        logger.info(
            f"📦 Found subscription | DB ID: {sub.id} | Old Status: {sub.status} | Was until: {sub.active_until}",
            extra={
                "subscription_id": sub.id,
                "old_status": sub.status,
                "old_active_until": sub.active_until.isoformat() if sub.active_until else None,
                "new_active_until": end.isoformat() if end else None
            }
        )

        sub.active_from = start
        if end:
            sub.active_until = end
        sub.latest_txn_id = pay_e["id"]
        sub.status = "renewed"
        await _db_add(db, sub)
        logger.info(f"✅ Renewed subscription for user {user_id}, active until {end}")

    order = await _db_call(db, lambda session: find_order_by_sub_id(session, sub_id))
    if order:
        await _db_call(db, lambda session: mark_order_paid(session, order))
        await _db_call(
            db,
            lambda session: create_payment(
                session,
                order=order,
                user_id=user_id,
                amount_minor=pay_e["amount"],
                currency=pay_e["currency"],
                provider_payment_id=pay_e["id"],
                meta={"method": pay_e.get("method"), "type": "renewal"},
            )
        )
        await _send_receipt_if_needed(db, sub_id, pay_e["id"])

        ents = await _db_call(
            db,
            lambda session: session.query(Entitlement)
            .join(OrderItem, Entitlement.order_item_id == OrderItem.id)
            .filter(OrderItem.order_id == order.id)
            .all()
        )
        for e in ents:
            e.entitlement_type = EntType.app
            e.active_from = start
            e.active_until = end
            e.status = StatusEnt.active
        if ents:
            await _db_add(db, *ents)

    log.result_summary = f"charged {user_id} {sub_id} {start}->{end}"


async def _on_subscription_completed(db: Session, payload: Dict[str, Any], log: WebhookProcessingLog) -> None:
    sub_e = payload["payload"]["subscription"]["entity"]
    pay_e = payload["payload"].get("payment", {}).get("entity", {})
    sub_id = sub_e["id"]
    user_id = sub_e["notes"].get("customer_id")

    # ✅ FIX: Search by customer_id like RevenueCat does (with row-level lock)
    sub = await _db_call(
        db,
        lambda session: _lock_query(
            session.query(Subscription)
            .filter(
                Subscription.customer_id == user_id,
                Subscription.provider == PROVIDER
            )
            .order_by(Subscription.created_at.desc())
        ).first()
    )

    if sub:
        if sub_e.get("status") == "completed" and not sub_e.get("auto_renew", True):
            sub.status = "completed"
        else:
            sub.status = "active"

        start, end = cycle_window_from_sub_entity(sub_e)
        if start:
            sub.active_from = start
        if end:
            sub.active_until = end
        if pay_e and pay_e.get("id"):
            sub.latest_txn_id = pay_e["id"]
        await _db_add(db, sub)
        log.result_summary = f"completed {user_id} {sub_id} status={sub.status}"
    else:
        log.result_summary = f"completed-not-found {sub_id}"


async def _on_subscription_cancelled(db: Session, payload: Dict[str, Any], log: WebhookProcessingLog) -> None:
    sub_e = payload["payload"]["subscription"]["entity"]
    sub_id = sub_e["id"]
    user_id = sub_e.get("notes", {}).get("customer_id")
    plan_sku = sub_e.get("notes", {}).get("plan_sku")

    logger.info(
        f"🔔 CANCELLATION EVENT | Razorpay Sub: {sub_id} | Customer: {user_id} | Plan: {plan_sku}",
        extra={
            "event": "subscription.cancelled",
            "razorpay_subscription_id": sub_id,
            "customer_id": user_id,
            "plan_sku": plan_sku,
            "cancelled_at": sub_e.get("cancelled_at"),
            "current_end": sub_e.get("current_end"),
            "auto_renew": sub_e.get("auto_renew")
        }
    )

    # ✅ FIX: Search by customer_id like RevenueCat does (not by provider_subscription_id) with row-level lock
    sub = await _db_call(
        db,
        lambda session: _lock_query(
            session.query(Subscription)
            .filter(
                Subscription.customer_id == user_id,
                Subscription.provider == PROVIDER,
                Subscription.status.in_(["active", "renewed"])
            )
            .order_by(Subscription.created_at.desc())
        ).first()
    )

    if sub:
        logger.info(
            f"📦 Found subscription | DB ID: {sub.id} | Status: {sub.status} | Active until: {sub.active_until}",
            extra={
                "subscription_id": sub.id,
                "old_status": sub.status,
                "active_until": sub.active_until.isoformat() if sub.active_until else None
            }
        )

        sub.status = "canceled"
        sub.auto_renew = False
        sub.cancel_reason = "razorpay_webhook"
        await _db_add(db, sub)

        # Also update entitlements to keep access until active_until
        order = await _db_call(db, lambda session: find_order_by_sub_id(session, sub_id))
        if order:
            logger.info(f"📝 Found order {order.id} for subscription")
            ents = await _db_call(
                db,
                lambda session: session.query(Entitlement)
                .join(OrderItem, Entitlement.order_item_id == OrderItem.id)
                .filter(OrderItem.order_id == order.id)
                .all()
            )
            logger.info(f"🎫 Found {len(ents)} entitlements to update")
            for ent in ents:
                old_status = ent.status
                # Keep active but mark for no renewal
                if ent.active_until and ensure_timezone_aware(ent.active_until) > now_ist():
                    ent.status = StatusEnt.active  # Keep active until expiry
                else:
                    ent.status = StatusEnt.expired
                logger.info(
                    f"  → Entitlement {ent.id}: {old_status} → {ent.status} (expires: {ent.active_until})"
                )
            if ents:
                await _db_add(db, *ents)
        else:
            logger.warning(f"⚠️ No order found for subscription {sub_id}")

        log.result_summary = f"cancelled {user_id} {sub_id} - access until {sub.active_until}"
        logger.info(f"✅ Cancelled subscription for user {user_id}, access until {sub.active_until}")
    else:
        log.result_summary = f"cancel-not-found {sub_id} for customer {user_id}"
        logger.warning(
            f"⚠️ No active subscription found for user {user_id} to cancel",
            extra={
                "razorpay_subscription_id": sub_id,
                "customer_id": user_id,
                "searched_statuses": ["active", "renewed"]
            }
        )


async def _on_subscription_expired(db: Session, payload: Dict[str, Any], log: WebhookProcessingLog) -> None:
    sub_e = payload["payload"]["subscription"]["entity"]
    sub_id = sub_e["id"]
    user_id = sub_e.get("notes", {}).get("customer_id")

    logger.info(
        f"⏰ EXPIRATION EVENT | Razorpay Sub: {sub_id} | Customer: {user_id}",
        extra={
            "event": "subscription.expired",
            "razorpay_subscription_id": sub_id,
            "customer_id": user_id,
            "ended_at": sub_e.get("ended_at"),
            "current_end": sub_e.get("current_end")
        }
    )

    # ✅ FIX: Search by customer_id like RevenueCat does (with row-level lock)
    sub = await _db_call(
        db,
        lambda session: _lock_query(
            session.query(Subscription)
            .filter(
                Subscription.customer_id == user_id,
                Subscription.provider == PROVIDER
            )
            .order_by(Subscription.created_at.desc())
        ).first()
    )

    if sub:
        logger.info(
            f"📦 Found subscription | DB ID: {sub.id} | Status: {sub.status} | Was active until: {sub.active_until}",
            extra={
                "subscription_id": sub.id,
                "old_status": sub.status,
                "was_active_until": sub.active_until.isoformat() if sub.active_until else None
            }
        )

        sub.status = "expired"
        sub.auto_renew = False
        await _db_add(db, sub)

        order = await _db_call(db, lambda session: find_order_by_sub_id(session, sub_id))
        if order:
            logger.info(f"📝 Found order {order.id} - expiring entitlements")
            ents = await _db_call(
                db,
                lambda session: session.query(Entitlement)
                .join(OrderItem, Entitlement.order_item_id == OrderItem.id)
                .filter(OrderItem.order_id == order.id)
                .all()
            )
            logger.info(f"🎫 Expiring {len(ents)} entitlements")
            for ent in ents:
                old_status = ent.status
                ent.status = StatusEnt.expired
                logger.info(f"  → Entitlement {ent.id}: {old_status} → expired")
            if ents:
                await _db_add(db, *ents)
        else:
            logger.warning(f"⚠️ No order found for subscription {sub_id}")

        log.result_summary = f"expired {user_id} {sub_id} - access revoked"
        logger.info(f"✅ Expired subscription for user {user_id}")
    else:
        log.result_summary = f"expire-not-found {sub_id} for customer {user_id}"
        logger.warning(
            f"⚠️ No subscription found for user {user_id} to expire",
            extra={
                "razorpay_subscription_id": sub_id,
                "customer_id": user_id
            }
        )


async def _on_payment_failed(db: Session, payload: Dict[str, Any], log: WebhookProcessingLog) -> None:
    pay_e = payload["payload"]["payment"]["entity"]
    payment_id = pay_e.get("id")
    order_id = pay_e.get("order_id")
    sub_id = pay_e.get("subscription_id")
    amount = pay_e.get("amount")
    error_code = pay_e.get("error_code")
    error_description = pay_e.get("error_description")
    method = pay_e.get("method")

    logger.error(
        f"❌ PAYMENT FAILED | Payment: {payment_id} | Subscription: {sub_id}",
        extra={
            "event": "payment.failed",
            "payment_id": payment_id,
            "subscription_id": sub_id,
            "order_id": order_id,
            "amount": amount,
            "error_code": error_code,
            "error_description": error_description,
            "method": method,
            "error_step": pay_e.get("error_step"),
            "error_source": pay_e.get("error_source"),
            "error_reason": pay_e.get("error_reason")
        }
    )

    user_id = _safe(lambda: pay_e["notes"].get("customer_id"))
    if sub_id and not user_id:
        try:
            full_sub = await rzp_get_subscription(sub_id)
            user_id = _safe(lambda: full_sub.get("notes", {}).get("customer_id"))
            logger.info(f"🔍 Resolved customer_id from subscription: {user_id}")
        except Exception as e:
            logger.warning(f"⚠️ Could not fetch subscription details: {e}")

    if sub_id:
        order = await _db_call(db, lambda session: find_order_by_sub_id(session, sub_id))
        if order:
            logger.info(f"📝 Found order {order.id} - marking as failed")
            order.status = "failed"
            order.failure_reason = f"{error_code}: {error_description}" if error_code else "Payment failed"
            await _db_add(db, order)
            user_id = user_id or order.customer_id
        else:
            logger.warning(f"⚠️ No order found for subscription {sub_id}")

    if sub_id:
        sub = await _db_call(db, lambda session: get_subscription_by_provider_id(session, sub_id))
        if sub:
            logger.info(f"📦 Found subscription {sub.id} - marking as payment_failed")
            sub.status = "payment_failed"
            await _db_add(db, sub)
        else:
            logger.warning(f"⚠️ No subscription found for {sub_id}")

    if order_id or sub_id:
        try:
            order = locals().get("order") or (await _db_call(db, lambda session: find_order_by_sub_id(session, sub_id)) if sub_id else None)
            if order:
                logger.info(f"💳 Creating failed payment record for order {order.id}")
                await _db_call(
                    db,
                    lambda session: create_payment(
                        session,
                        order=order,
                        user_id=user_id or order.customer_id,
                        amount_minor=amount or 0,
                        currency=pay_e.get("currency") or "INR",
                        provider_payment_id=payment_id,
                        meta={
                            "method": method,
                            "error_code": error_code,
                            "error_description": error_description,
                            "type": "failed",
                            "source": "webhook",
                        },
                        status="failed",
                    )
                )
                logger.info(f"✅ Failed payment record created")
        except Exception as e:
            logger.warning("Failed to create failed payment record", extra={"error": str(e)})

    log.result_summary = f"payment_failed {user_id} {payment_id} {error_code}"
    logger.info(f"📊 Payment failure logged for customer {user_id}")


def premium_required() -> Callable:
    async def _dep(
        creds: Optional[HTTPAuthorizationCredentials] = Depends(security),
        db: Session = Depends(get_db_session),
    ):
        client_id = None
        if creds and creds.scheme.lower() == "bearer":
            # TODO: decode JWT; for now, use token-as-client-id pattern
            client_id = creds.credentials
        if not client_id:
            raise HTTPException(status_code=http_status.HTTP_401_UNAUTHORIZED, detail="No client_id")

        captured, sub, _ = await _has_premium_now(db, client_id)
        if not (captured and sub):
            raise HTTPException(status_code=http_status.HTTP_402_PAYMENT_REQUIRED, detail="Premium required")
        return True

    return _dep


# ---------------------------------------------------------------------------
# Cancel subscription (truthful outcomes)
# ---------------------------------------------------------------------------

@router.post("/razorpay/subscriptions/cancel")
async def cancel_subscription(request: Request, db: Session = Depends(get_db_session)):
    body = await request.json()
    user_id = body.get("user_id")
    reason = body.get("reason", "user_requested")
    provider_sub_id = body.get("subscription_id") or body.get("provider_subscription_id")

    if not user_id:
        raise HTTPException(status_code=400, detail="user_id is required")

    # Use row-level lock to prevent concurrent cancellation conflicts
    sub = await _db_call(
        db,
        lambda session: _lock_query(
            session.query(Subscription)
            .filter(
                Subscription.customer_id == user_id,
                Subscription.provider == PROVIDER,
                Subscription.status.in_(["active", "renewed"]),
            )
            .order_by(Subscription.created_at.desc())
        ).first()
    )
    if not sub:
        raise HTTPException(status_code=404, detail="No active subscription found for user")

    if not provider_sub_id:
        # Use subscription.id as it stores the Razorpay subscription ID
        provider_sub_id = sub.id
    if not provider_sub_id:
        raise HTTPException(status_code=409, detail="Subscription missing provider reference")

    try:
        cancel_response = await rzp_cancel_subscription(provider_sub_id, cancel_at_cycle_end=True)
        cancel_response.raise_for_status()
        razorpay_data = cancel_response.json()

        sub.auto_renew = False
        sub.cancel_reason = f"provider_subscription_id:{provider_sub_id} reason:{reason}"
        await _db_add(db, sub)
        await _db_commit(db)

        return {
            "canceled": True,
            "provider_cancelled": True,
            "subscription_id": sub.id,
            "provider_subscription_id": provider_sub_id,
            "message": f"Auto-renewal canceled. Subscription remains active until {sub.active_until.strftime('%Y-%m-%d') if sub.active_until else 'cycle end'}",
            "auto_renew": False,
            "expires_at": sub.active_until.isoformat() if sub.active_until else None,
            "razorpay_status": razorpay_data.get("status", "canceled"),
        }

    except (httpx.RequestError, httpx.HTTPStatusError) as e:
        logger.error(
            "Razorpay cancel API error",
            extra={"error": str(e), "provider_subscription_id": _mask(provider_sub_id)},
        )
        # Local safeguard to stop auto-renew attempts in our system
        sub.auto_renew = False
        sub.cancel_reason = f"provider_subscription_id:{provider_sub_id} reason:{reason} (API_ERROR)"
        await _db_add(db, sub)
        await _db_commit(db)

        return {
            "canceled": False,  # truthful — provider not cancelled
            "provider_cancelled": False,
            "subscription_id": sub.id,
            "provider_subscription_id": provider_sub_id,
            "message": f"Auto-renew disabled locally. Provider cancel failed: {str(e)}",
            "auto_renew": False,
            "expires_at": sub.active_until.isoformat() if sub.active_until else None,
            "razorpay_status": "api_error",
        }


# ---------------------------------------------------------------------------
# Debug (masked)
# ---------------------------------------------------------------------------

@router.get("/razorpay/debug")
async def razorpay_debug(plan_sku: Optional[str] = None, db: Session = Depends(get_db_session)):
    settings = get_payment_settings()
    key_id = settings.razorpay_key_id
    masked_key = f"{key_id[:6]}...{key_id[-4:]}" if key_id and len(key_id) > 10 else "masked"

    info: Dict[str, Any] = {
        "router_loaded": True,
        "endpoints": [
            "/payments/razorpay/subscriptions/create",
            "/payments/razorpay/subscriptions/verify",
            "/payments/webhooks/razorpay",
            "/payments/user/{client_id}/premium-status",
        ],
        "razorpay_key_id_masked": masked_key,
        "env": getattr(settings, "environment", "unknown"),
    }

    if plan_sku:
        catalog: CatalogProduct = await _db_call(
            db,
            lambda session: session.query(CatalogProduct)
            .filter(CatalogProduct.sku == plan_sku, CatalogProduct.active == True)
            .first()
        )
        if not catalog:
            info["plan_check"] = {"sku": plan_sku, "exists": False}
        else:
            try:
                rp = await rzp_get_plan(catalog.razorpay_plan_id)
                info["plan_check"] = {
                    "sku": plan_sku,
                    "db_amount_minor": getattr(catalog, "base_amount_minor", None),
                    "db_plan_id": catalog.razorpay_plan_id,
                    "rzp_amount_minor": rp.get("item", {}).get("amount"),
                    "rzp_currency": rp.get("item", {}).get("currency"),
                    "ok": rp.get("item", {}).get("amount") == getattr(catalog, "base_amount_minor", None),
                }
            except (httpx.RequestError, httpx.HTTPStatusError) as exc:
                info["plan_check"] = {"sku": plan_sku, "error": str(exc)}

    return info


@router.on_event("shutdown")
async def _shutdown_async_clients() -> None:
    await close_async_http_clients()
