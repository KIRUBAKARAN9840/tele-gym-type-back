# fittbot_api/v1/payments/razorpay/routes.py
# Enterprise-hardened Razorpay subscriptions flow with resilient webhook handling,
# strict idempotency, consistent premium gating, and safe logging.

import base64
import hashlib
import json
import logging
import secrets
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, Optional, Tuple

import requests
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

from .client import (
    create_subscription as rzp_create_subscription,
    get_payment as rzp_get_payment,
    get_plan as rzp_get_plan,
    get_subscription as rzp_get_subscription,
)
from .crypto import verify_checkout_subscription_sig, verify_webhook_sig
from .db_helpers import (
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
from .receipt_service import send_subscription_receipt

logger = logging.getLogger("payments.razorpay")
router = APIRouter(prefix="/razorpay_payments", tags=["Razorpay Subscriptions"])
subscription_routes_router = router  # Alias for backward compatibility
security = HTTPBearer(auto_error=False)

IST = timezone(timedelta(hours=5, minutes=30))


def now_ist() -> datetime:
    return datetime.now(IST)


def _safe(fn, default=None):
    try:
        return fn()
    except Exception:
        return default


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
            "timestamp": datetime.now(timezone.utc).isoformat(),
            **(extra_data or {}),
        },
    )


async def log_security_event(event_type: str, data: dict):
    logger.warning(
        "SECURITY_EVENT",
        extra={"event": event_type, "timestamp": datetime.now(timezone.utc).isoformat(), **(data or {})},
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

def _send_receipt_if_needed(db: Session, sub_id: Optional[str], payment_id: Optional[str]) -> None:
    try:
        if not sub_id or not payment_id:
            return
        sub = get_subscription_by_provider_id(db, sub_id)
        if not sub:
            return
        pay = (
            db.query(Payment)
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
            db.add(pay)
            logger.info("Receipt sent", extra={"payment_id": _mask(payment_id)})

    except Exception as e:
        # Never break the payment flow for mailing failures
        print("Errrror is",e)
        logger.warning("Receipt send failure", extra={"error": str(e)})


# ---------------------------------------------------------------------------
# Premium gating helpers
# ---------------------------------------------------------------------------

def _find_current_subscription(db: Session, client_id: str) -> Optional[Subscription]:
    now = datetime.now(timezone.utc)
    return (
        db.query(Subscription)
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


def _has_premium_now(db: Session, client_id: str) -> Tuple[bool, Optional[Subscription], Optional[str]]:
    sub = _find_current_subscription(db, client_id)
    if not sub:
        return False, None, "no_subscription"

    captured = (
        db.query(Payment)
        .filter(
            Payment.provider == PROVIDER,
            Payment.provider_payment_id == sub.latest_txn_id,
            Payment.status == "captured",
        )
        .first()
        is not None
    )
    return captured, sub, None if captured else "no_captured_payment_for_cycle"


# ---------------------------------------------------------------------------
# Public endpoints
# ---------------------------------------------------------------------------

@router.post("/subscriptions/create")
async def create_subscription(request: Request, db: Session = Depends(get_db_session)):
    """
    Body: { "user_id": "<uid>", "plan_sku": "<sku>" }
    Returns: { subscription_id, razorpay_key_id, order_id, display_title }
    """
    settings = get_payment_settings()
    body = await request.json()
    user_id = body.get("user_id")
    plan_sku = body.get("plan_sku")
    if not user_id or not plan_sku:
        raise HTTPException(status_code=400, detail="user_id and plan_sku are required")

    catalog: CatalogProduct = (
        db.query(CatalogProduct)
        .filter(CatalogProduct.sku == plan_sku, CatalogProduct.active == True)
        .first()
    )
    if not catalog or not catalog.razorpay_plan_id:
        raise HTTPException(status_code=404, detail="Invalid or inactive SKU / missing razorpay_plan_id")

    # Optional: cross-check plan with Razorpay
    rp = None
    try:
        rp = rzp_get_plan(catalog.razorpay_plan_id)
        if rp.get("item", {}).get("amount") != catalog.base_amount_minor or rp.get("item", {}).get("currency") != "INR":
            raise HTTPException(status_code=409, detail="Plan mismatch between DB and Razorpay")
    except Exception as e:
        logger.warning("Plan validation error", extra={"error": str(e)})

    # Determine total_count safely
    total_count = 12
    try:
        period = (rp or {}).get("period")
        interval = (rp or {}).get("interval")
        if period in ("year", "yearly"):
            total_count = 1
        elif period == "monthly" and interval and int(interval) > 1:
            total_count = 1
    except Exception:
        pass

    try:
        sub = rzp_create_subscription(
            catalog.razorpay_plan_id,
            notes={"plan_sku": plan_sku, "customer_id": user_id},
            total_count=total_count,
        )
    except Exception as e:
        msg = getattr(getattr(e, "response", None), "text", None)
        logger.error(
            "razorpay.subscription.create_failed | user_id=%s plan_sku=%s error=%s body=%s",
            user_id,
            plan_sku,
            str(e),
            msg,
        )
        raise HTTPException(status_code=502, detail="Failed to create subscription with Razorpay")

    sub_id = sub["id"]

    # Create internal order + pending subscription record
    order = create_pending_order(
        db,
        user_id=user_id,
        amount_minor=catalog.base_amount_minor,
        sub_id=sub_id,
        sku=catalog.sku,
        title=catalog.title,
    )
    create_or_update_subscription_pending(db, user_id=user_id, plan_sku=plan_sku, provider_subscription_id=sub_id)
    db.commit()
    return {
        "subscription_id": sub_id,
        "razorpay_key_id": settings.razorpay_key_id,
        "order_id": order.id,
        "display_title": catalog.title,
    }


@router.post("/subscriptions/verify")
async def verify_subscription(request: Request, db: Session = Depends(get_db_session)):
    """
    Body: { razorpay_payment_id, razorpay_subscription_id, razorpay_signature }
    Security-first verification flow:
      - Verify HMAC
      - Fetch payment
      - Activate only on CAPTURED (webhook remains authoritative; this is a fast-path)
    """
    settings = get_payment_settings()
    body = await request.json()

    pid = body.get("razorpay_payment_id")
    sid = body.get("razorpay_subscription_id")
    sig = body.get("razorpay_signature")

    logger.info(
        "razorpay.verify.request | subscription_id=%s payment_id=%s",
        sid,
        pid,
    )
    if not all([pid, sid, sig]):
        raise HTTPException(status_code=400, detail="Missing required fields")

    if not verify_checkout_subscription_sig(settings.razorpay_key_secret, pid, sid, sig):
        await log_security_event("INVALID_SIGNATURE", {"payment_id": _mask(pid), "sub_id": _mask(sid)})
        raise HTTPException(status_code=403, detail="Invalid signature")

    # Fetch payment securely
    try:
        payment_data = rzp_get_payment(pid)
        payment_status = payment_data.get("status")
        logger.debug(
            "razorpay.verify.payment_fetched | subscription_id=%s payment_id=%s status=%s",
            sid,
            pid,
            payment_status,
        )
    except requests.exceptions.Timeout:
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
    except requests.exceptions.HTTPError as e:
        code = getattr(getattr(e, "response", None), "status_code", None)
        if code == 429:
            await log_security_event("RATE_LIMITED", {"payment_id": _mask(pid)})
            return JSONResponse(
                status_code=429,
                content={"verified": False, "captured": False, "error": "rate_limited", "message": "Please retry soon"},
                headers={"Retry-After": "2"},
            )
        await log_verification_event("API_ERROR", pid, sid, {"error": str(e)})
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
    except Exception as e:
        await log_security_event("UNKNOWN_API_ERROR", {"payment_id": _mask(pid), "error": str(e)})
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
        response = await handle_captured_payment_secure(db, pid, sid, payment_data)
        logger.info(
            "razorpay.verify.success | subscription_id=%s payment_id=%s",
            sid,
            pid,
        )
        return response

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
    subscription = get_subscription_by_provider_id(db, sid)
    if not subscription:
        await log_security_event("SUBSCRIPTION_NOT_FOUND", {"sub_id": _mask(sid), "payment_id": _mask(pid)})
        return {"verified": False, "captured": False, "error": "subscription_not_found"}

    order = find_order_by_sub_id(db, sid)
    existing_payment = (
        db.query(Payment)
        .filter(Payment.provider == PROVIDER, Payment.provider_payment_id == pid, Payment.status == "captured")
        .first()
    )

    amount = int(payment_data.get("amount") or 0)
    currency = payment_data.get("currency") or "INR"
    method = payment_data.get("method")
    if amount <= 0:
        await log_security_event("INVALID_AMOUNT", {"payment_id": _mask(pid), "amount": amount})
        return {"verified": False, "captured": False, "error": "invalid_amount"}

    now = datetime.now(timezone.utc)
    start_dt = subscription.active_from or now
    end_dt = subscription.active_until

    try:
        sub_entity = rzp_get_subscription(sid)
        if isinstance(sub_entity, dict):
            s_start, s_end = cycle_window_from_sub_entity(sub_entity)
            start_dt = s_start or start_dt
            end_dt = s_end or end_dt
    except Exception as fetch_err:
        await log_verification_event("SUB_LOOKUP_FAILED", pid, sid, {"error": str(fetch_err)})

    if order is None:
        await log_security_event(
            "ORDER_NOT_FOUND_FOR_SUBSCRIPTION",
            {"sub_id": _mask(sid), "payment_id": _mask(pid), "customer_id": subscription.customer_id},
        )

    fallback_end = end_dt or subscription.active_until or ((start_dt or now) + timedelta(days=30))
    already_processed = existing_payment is not None

    try:
        if order:
            mark_order_paid(db, order)

        if not already_processed and order:
            create_payment(
                db,
                order=order,
                user_id=subscription.customer_id,
                amount_minor=amount,
                currency=currency,
                provider_payment_id=pid,
                meta={"method": method, "source": "verify_endpoint"},
            )

        subscription.status = "active"
        subscription.latest_txn_id = pid
        subscription.active_from = start_dt or now
        subscription.active_until = end_dt or fallback_end
        # Derive auto_renew from subscription entity instead of payment
        try:
            sub_full = rzp_get_subscription(sid)
            subscription.auto_renew = bool(sub_full.get("auto_renew", True))
        except Exception:
            pass
        db.add(subscription)

        if order:
            # Ensure entitlements exist then set active window
            entitlements = (
                db.query(Entitlement)
                .join(OrderItem, Entitlement.order_item_id == OrderItem.id)
                .filter(OrderItem.order_id == order.id)
                .all()
            )
            if not entitlements:
                EntitlementService(db).create_entitlements_from_order(order)
                entitlements = (
                    db.query(Entitlement)
                    .join(OrderItem, Entitlement.order_item_id == OrderItem.id)
                    .filter(OrderItem.order_id == order.id)
                    .all()
                )
            for ent in entitlements:
                ent.entitlement_type = EntType.app
                ent.active_from = subscription.active_from
                ent.active_until = subscription.active_until
                ent.status = StatusEnt.active
                db.add(ent)

        db.commit()
    except Exception as e:
        db.rollback()
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
        _send_receipt_if_needed(db, sid, pid)

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



@router.post("/webhooks/razorpay")
async def webhook_razorpay(request: Request, db: Session = Depends(get_db_session)):
    settings = get_payment_settings()

    raw = await request.body()
    sig = request.headers.get("X-Razorpay-Signature", "")

    if not verify_webhook_sig(settings.razorpay_webhook_secret, raw, sig):
        raise HTTPException(status_code=
                            401, detail="Invalid signature")
    payload = json.loads(raw.decode("utf-8"))
    event = payload.get("event", "")
    event_id = _rzp_event_id(payload)

    # Debug (PII-safe)
    summary = _summarize_webhook(payload)
    logger.info("[RZP] Webhook summary", extra={"event_id": event_id, "summary": summary})

    # Metrics: inbound webhook
    try:
        stats = WebhookMonitoringStats.get_current_hour_stats(db)
        stats.total_webhooks_received = (stats.total_webhooks_received or 0) + 1
        db.add(stats)
        db.flush()
    except Exception:
        pass

    # Idempotency gate
    existing = db.query(WebhookProcessingLog).filter(WebhookProcessingLog.event_id == event_id).first()
    if existing and existing.status == "completed":
        try:
            stats = WebhookMonitoringStats.get_current_hour_stats(db)
            stats.duplicate_webhooks_blocked = (stats.duplicate_webhooks_blocked or 0) + 1
            db.add(stats)
            existing.retry_count = (existing.retry_count or 0) + 1
            db.add(existing)
            db.commit()
        except Exception:
            pass
        return {"status": "already_processed"}

    if existing and existing.status == "processing":
        try:
            stats = WebhookMonitoringStats.get_current_hour_stats(db)
            stats.duplicate_webhooks_blocked = (stats.duplicate_webhooks_blocked or 0) + 1
            db.add(stats)
            existing.retry_count = (existing.retry_count or 0) + 1
            db.add(existing)
            db.commit()
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
        db.add(log)
        db.flush()
        # Record request hash in idempotency table (optional response caching)
        try:
            request_hash = hashlib.sha256(raw).hexdigest()
            ttl = getattr(settings, "idempotency_ttl_delta", None)
            expires_at = datetime.now(timezone.utc) + ttl if ttl else None
            key_name = f"wh:razorpay:{event_id}"
            idem = db.query(IdempotencyKey).filter(IdempotencyKey.key == key_name).first()
            if not idem:
                idem = IdempotencyKey(key=key_name, request_hash=request_hash, expires_at=expires_at)
                db.add(idem)
                db.flush()
        except Exception:
            pass

    try:
        if event == "subscription.activated":
            _on_subscription_activated(db, payload, log)
        elif event == "payment.captured":
            _on_payment_captured(db, payload, log)
        elif event == "payment.authorized":
            # NOTE: do NOT send receipts on authorization; only observe.
            log.result_summary = (log.result_summary or "") + " | authorized_seen"
        elif event == "subscription.charged":
            _on_subscription_charged(db, payload, log)
        elif event == "subscription.completed":
            _on_subscription_completed(db, payload, log)
        elif event == "subscription.authenticated":
            # Optional: could trigger receipt if you require; we skip to avoid duplicates.
            log.result_summary = (log.result_summary or "") + " | authenticated_seen"
        elif event == "subscription.cancelled":
            _on_subscription_cancelled(db, payload, log)
        elif event == "subscription.expired":
            _on_subscription_expired(db, payload, log)
        elif event == "payment.failed":
            _on_payment_failed(db, payload, log)
        else:
            log.status = "ignored"
            log.completed_at = now_ist()
            log.result_summary = f"Unhandled: {event}"
            db.commit()
            return {"status": "ignored", "event": event}

        log.status = "completed"
        log.completed_at = now_ist()
        if log.started_at and log.completed_at:
            delta = log.completed_at - log.started_at
            log.processing_duration_ms = int(delta.total_seconds() * 1000)

        try:
            stats = WebhookMonitoringStats.get_current_hour_stats(db)
            stats.webhooks_processed_successfully = (stats.webhooks_processed_successfully or 0) + 1
            db.add(stats)
        except Exception:
            pass

        # Cache success in idempotency keys (optional)
        try:
            key_name = f"wh:razorpay:{event_id}"
            idem = db.query(IdempotencyKey).filter(IdempotencyKey.key == key_name).first()
            if idem:
                idem.set_response(200, json.dumps({"status": "processed", "event": event}).encode("utf-8"))
                db.add(idem)
        except Exception:
            pass

        db.commit()
        return {"status": "processed", "event": event}

    except Exception as e:
        logger.exception("Webhook error")
        log.status = "failed"
        log.error_message = str(e)
        log.completed_at = now_ist()
        try:
            stats = WebhookMonitoringStats.get_current_hour_stats(db)
            stats.webhooks_failed = (stats.webhooks_failed or 0) + 1
            db.add(stats)
        except Exception:
            pass
        db.commit()
        raise HTTPException(status_code=500, detail="Internal error")


def _activate(db: Session, sub: Subscription, start: datetime, end: Optional[datetime], latest_txn_id: str) -> None:
    sub.status = "active"
    sub.active_from = start
    if end:
        sub.active_until = end
    sub.latest_txn_id = latest_txn_id
    db.add(sub)


def _on_subscription_activated(db: Session, payload: Dict[str, Any], log: WebhookProcessingLog) -> None:
    sub_e = payload["payload"]["subscription"]["entity"]
    pay_e = payload["payload"]["payment"]["entity"]
    sub_id = sub_e["id"]
    user_id = sub_e["notes"].get("customer_id")
    plan_sku = sub_e["notes"].get("plan_sku")

    start, end = cycle_window_from_sub_entity(sub_e)
    if not end:
        full = rzp_get_subscription(sub_id)
        start = datetime.fromtimestamp(full.get("current_start"), tz=timezone.utc) if full.get("current_start") else start
        end = datetime.fromtimestamp(full.get("current_end"), tz=timezone.utc) if full.get("current_end") else end

    sub = get_subscription_by_provider_id(db, sub_id) or create_or_update_subscription_pending(
        db, user_id=user_id, plan_sku=plan_sku, provider_subscription_id=sub_id
    )
    _activate(db, sub, start, end, pay_e["id"])

    order = find_order_by_sub_id(db, sub_id)
    if order:
        mark_order_paid(db, order)
        create_payment(
            db,
            order=order,
            user_id=user_id,
            amount_minor=pay_e["amount"],
            currency=pay_e["currency"],
            provider_payment_id=pay_e["id"],
            meta={"method": pay_e.get("method"), "type": "activation"},
        )
        _send_receipt_if_needed(db, sub_id, pay_e["id"])

        ent = (
            db.query(Entitlement)
            .join(OrderItem, Entitlement.order_item_id == OrderItem.id)
            .filter(OrderItem.order_id == order.id)
            .first()
        )
        if not ent:
            EntitlementService(db).create_entitlements_from_order(order)

        ents = (
            db.query(Entitlement)
            .join(OrderItem, Entitlement.order_item_id == OrderItem.id)
            .filter(OrderItem.order_id == order.id)
            .all()
        )
        for e in ents:
            e.entitlement_type = EntType.app
            e.active_from = start
            e.active_until = end
            e.status = StatusEnt.active
            db.add(e)

    log.result_summary = f"activated {user_id} {sub_id} {start}->{end}"


def _on_payment_captured(db: Session, payload: Dict[str, Any], log: WebhookProcessingLog) -> None:
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
            pfull = rzp_get_payment(payment_id)
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
    existing_payment = (
        db.query(Payment)
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
            full = rzp_get_subscription(sub_id)
            start = datetime.fromtimestamp(full.get("current_start"), tz=timezone.utc) if full.get("current_start") else None
            end = datetime.fromtimestamp(full.get("current_end"), tz=timezone.utc) if full.get("current_end") else None
            user_id = user_id or _safe(lambda: full.get("notes", {}).get("customer_id"))
            # Get actual plan_sku from subscription notes
            plan_sku = plan_sku or _safe(lambda: full.get("notes", {}).get("plan_sku"))
    except Exception:
        pass

    # Use actual plan_sku or fallback to "app_subscription" only as last resort
    resolved_plan_sku = plan_sku or "app_subscription"

    # Update/create subscription
    if sub_id:
        sub = get_subscription_by_provider_id(db, sub_id)
        if not sub and user_id:
            sub = create_or_update_subscription_pending(db, user_id=user_id, plan_sku=resolved_plan_sku, provider_subscription_id=sub_id)
        if sub:
            if not start:
                start = datetime.now(timezone.utc)
            sub.active_from = start
            if end:
                sub.active_until = end
            sub.latest_txn_id = payment_id
            sub.status = "active"
            db.add(sub)

    # Order + Payment + Entitlements
    if sub_id:
        order = find_order_by_sub_id(db, sub_id)
        if order:
            if not existing_payment:
                mark_order_paid(db, order)
                create_payment(
                    db,
                    order=order,
                    user_id=user_id or order.customer_id,
                    amount_minor=amount or 0,
                    currency=currency or "INR",
                    provider_payment_id=payment_id,
                    meta={"method": pay_e.get("method"), "type": "webhook_capture"},
                )

            ent = (
                db.query(Entitlement)
                .join(OrderItem, Entitlement.order_item_id == OrderItem.id)
                .filter(OrderItem.order_id == order.id)
                .first()
            )
            if not ent:
                EntitlementService(db).create_entitlements_from_order(order)

            ents = (
                db.query(Entitlement)
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
                db.add(e)

            _send_receipt_if_needed(db, sub_id, payment_id)

    log.result_summary = f"captured and activated {user_id} {sub_id} {start}->{end}"


def _on_subscription_charged(db: Session, payload: Dict[str, Any], log: WebhookProcessingLog) -> None:
    sub_e = payload["payload"]["subscription"]["entity"]
    pay_e = payload["payload"]["payment"]["entity"]
    sub_id = sub_e["id"]
    user_id = sub_e["notes"].get("customer_id")

    start, end = cycle_window_from_sub_entity(sub_e)
    if not end:
        full = rzp_get_subscription(sub_id)
        start = datetime.fromtimestamp(full.get("current_start"), tz=timezone.utc) if full.get("current_start") else start
        end = datetime.fromtimestamp(full.get("current_end"), tz=timezone.utc) if full.get("current_end") else end

    sub = get_subscription_by_provider_id(db, sub_id)
    if sub:
        sub.active_from = start
        if end:
            sub.active_until = end
        sub.latest_txn_id = pay_e["id"]
        sub.status = "renewed"
        db.add(sub)

    order = find_order_by_sub_id(db, sub_id)
    if order:
        mark_order_paid(db, order)
        create_payment(
            db,
            order=order,
            user_id=user_id,
            amount_minor=pay_e["amount"],
            currency=pay_e["currency"],
            provider_payment_id=pay_e["id"],
            meta={"method": pay_e.get("method"), "type": "renewal"},
        )
        _send_receipt_if_needed(db, sub_id, pay_e["id"])

        ents = (
            db.query(Entitlement)
            .join(OrderItem, Entitlement.order_item_id == OrderItem.id)
            .filter(OrderItem.order_id == order.id)
            .all()
        )
        for e in ents:
            e.entitlement_type = EntType.app
            e.active_from = start
            e.active_until = end
            e.status = StatusEnt.active
            db.add(e)

    log.result_summary = f"charged {user_id} {sub_id} {start}->{end}"


def _on_subscription_completed(db: Session, payload: Dict[str, Any], log: WebhookProcessingLog) -> None:
    sub_e = payload["payload"]["subscription"]["entity"]
    pay_e = payload["payload"].get("payment", {}).get("entity", {})
    sub_id = sub_e["id"]
    user_id = sub_e["notes"].get("customer_id")

    sub = get_subscription_by_provider_id(db, sub_id)
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
        db.add(sub)
        log.result_summary = f"completed {user_id} {sub_id} status={sub.status}"
    else:
        log.result_summary = f"completed-not-found {sub_id}"


def _on_subscription_cancelled(db: Session, payload: Dict[str, Any], log: WebhookProcessingLog) -> None:
    sub_e = payload["payload"]["subscription"]["entity"]
    sub_id = sub_e["id"]
    user_id = sub_e.get("notes", {}).get("customer_id")

    sub = get_subscription_by_provider_id(db, sub_id)
    if sub:
        sub.status = "canceled"
        sub.auto_renew = False
        sub.cancel_reason = "razorpay_webhook"
        db.add(sub)
        log.result_summary = f"cancelled {user_id} {sub_id}"
    else:
        log.result_summary = f"cancel-not-found {sub_id}"


def _on_subscription_expired(db: Session, payload: Dict[str, Any], log: WebhookProcessingLog) -> None:
    sub_e = payload["payload"]["subscription"]["entity"]
    sub_id = sub_e["id"]
    user_id = sub_e.get("notes", {}).get("customer_id")

    sub = get_subscription_by_provider_id(db, sub_id)
    if sub:
        sub.status = "expired"
        db.add(sub)

        order = find_order_by_sub_id(db, sub_id)
        if order:
            ents = (
                db.query(Entitlement)
                .join(OrderItem, Entitlement.order_item_id == OrderItem.id)
                .filter(OrderItem.order_id == order.id)
                .all()
            )
            for ent in ents:
                ent.status = StatusEnt.expired
                db.add(ent)

        log.result_summary = f"expired {user_id} {sub_id} - access revoked"
    else:
        log.result_summary = f"expire-not-found {sub_id}"


def _on_payment_failed(db: Session, payload: Dict[str, Any], log: WebhookProcessingLog) -> None:
    pay_e = payload["payload"]["payment"]["entity"]
    payment_id = pay_e.get("id")
    order_id = pay_e.get("order_id")
    sub_id = pay_e.get("subscription_id")
    amount = pay_e.get("amount")
    error_code = pay_e.get("error_code")
    error_description = pay_e.get("error_description")
    method = pay_e.get("method")

    user_id = _safe(lambda: pay_e["notes"].get("customer_id"))
    if sub_id and not user_id:
        try:
            full_sub = rzp_get_subscription(sub_id)
            user_id = _safe(lambda: full_sub.get("notes", {}).get("customer_id"))
        except Exception:
            pass

    if sub_id:
        order = find_order_by_sub_id(db, sub_id)
        if order:
            order.status = "failed"
            order.failure_reason = f"{error_code}: {error_description}" if error_code else "Payment failed"
            db.add(order)
            user_id = user_id or order.customer_id

    if sub_id:
        sub = get_subscription_by_provider_id(db, sub_id)
        if sub:
            sub.status = "payment_failed"
            db.add(sub)

    if order_id or sub_id:
        try:
            order = locals().get("order") or (find_order_by_sub_id(db, sub_id) if sub_id else None)
            if order:
                create_payment(
                    db,
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
        except Exception as e:
            logger.warning("Failed to create failed payment record", extra={"error": str(e)})

    log.result_summary = f"payment_failed {user_id} {payment_id} {error_code}"


# ---------------------------------------------------------------------------
# Premium status & gate (internal helper)
# ---------------------------------------------------------------------------


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

        captured, sub, _ = _has_premium_now(db, client_id)
        if not (captured and sub):
            raise HTTPException(status_code=http_status.HTTP_402_PAYMENT_REQUIRED, detail="Premium required")
        return True

    return _dep


# ---------------------------------------------------------------------------
# Cancel subscription (truthful outcomes)
# ---------------------------------------------------------------------------

@router.post("/razorpay/subscriptions/cancel")
async def cancel_subscription(request: Request, db: Session = Depends(get_db_session)):
    settings = get_payment_settings()
    body = await request.json()
    user_id = body.get("user_id")
    reason = body.get("reason", "user_requested")
    provider_sub_id = body.get("subscription_id") or body.get("provider_subscription_id")

    if not user_id:
        raise HTTPException(status_code=400, detail="user_id is required")

    sub = (
        db.query(Subscription)
        .filter(
            Subscription.customer_id == user_id,
            Subscription.provider == PROVIDER,
            Subscription.status.in_(["active", "renewed"]),
        )
        .order_by(Subscription.created_at.desc())
        .first()
    )
    if not sub:
        raise HTTPException(status_code=404, detail="No active subscription found for user")

    if not provider_sub_id:
        # Extract from cancel_reason field (format: "provider_subscription_id:sub_XXXXX")
        if sub.cancel_reason and "provider_subscription_id:" in sub.cancel_reason:
            try:
                parts = sub.cancel_reason.split("provider_subscription_id:")
                if len(parts) > 1:
                    sub_id_part = parts[1].strip()
                    provider_sub_id = sub_id_part.split()[0] if ' ' in sub_id_part else sub_id_part
                    logger.info(f"[CANCEL] Extracted provider_sub_id from cancel_reason: {provider_sub_id}")
            except Exception as e:
                logger.warning(f"[CANCEL] Failed to parse cancel_reason: {e}")

        # Fallback to rc_original_txn_id
        if not provider_sub_id and sub.rc_original_txn_id:
            provider_sub_id = sub.rc_original_txn_id
            logger.info(f"[CANCEL] Using rc_original_txn_id: {provider_sub_id}")

    if not provider_sub_id:
        raise HTTPException(status_code=409, detail="Subscription missing provider reference (no Razorpay subscription ID found)")

    try:
        RZP_API = "https://api.razorpay.com/v1"
        cancel_url = f"{RZP_API}/subscriptions/{provider_sub_id}/cancel"
        auth_string = f"{settings.razorpay_key_id}:{settings.razorpay_key_secret}"
        auth_bytes = base64.b64encode(auth_string.encode("utf-8"))
        headers = {"Authorization": f"Basic {auth_bytes.decode('utf-8')}", "Content-Type": "application/json"}

        cancel_response = requests.post(
            cancel_url,
            headers=headers,
            json={"cancel_at_cycle_end": False},  # Immediate cancel - Razorpay ignores cancel_at_cycle_end=true
            timeout=15,
        )
        cancel_response.raise_for_status()
        razorpay_data = cancel_response.json()

        # Check if cancellation is scheduled for end of billing cycle
        # When cancel_at_cycle_end=1, Razorpay sets:
        # - has_scheduled_changes: true
        # - schedule_change_at: "cycle_end"
        # - status remains "active" until billing cycle ends
        has_scheduled_changes = razorpay_data.get("has_scheduled_changes", False)
        schedule_change_at = razorpay_data.get("schedule_change_at")
        razorpay_status = razorpay_data.get("status")
        current_end = razorpay_data.get("current_end")

        is_scheduled_cancellation = (
            has_scheduled_changes and
            schedule_change_at == "cycle_end" and
            razorpay_status == "active"
        )

        logger.info(
            f"[CANCEL] Razorpay response",
            extra={
                "provider_sub_id": provider_sub_id,
                "razorpay_status": razorpay_status,
                "has_scheduled_changes": has_scheduled_changes,
                "schedule_change_at": schedule_change_at,
                "is_scheduled_cancellation": is_scheduled_cancellation
            }
        )

        sub.auto_renew = False
        if is_scheduled_cancellation:
            sub.cancel_reason = f"scheduled_cancellation:provider_subscription_id:{provider_sub_id} reason:{reason}"
            # Update active_until from Razorpay's current_end
            if current_end:
                from datetime import datetime as dt
                sub.active_until = dt.fromtimestamp(current_end)
        else:
            sub.cancel_reason = f"provider_subscription_id:{provider_sub_id} reason:{reason}"
        db.add(sub)
        db.commit()

        return {
            "canceled": True,
            "provider_cancelled": True,
            "scheduled_cancellation": is_scheduled_cancellation,
            "subscription_id": sub.id,
            "provider_subscription_id": provider_sub_id,
            "message": f"{'Cancellation scheduled for cycle end' if is_scheduled_cancellation else 'Auto-renewal canceled'}. Subscription remains active until {sub.active_until.strftime('%Y-%m-%d') if sub.active_until else 'cycle end'}",
            "auto_renew": False,
            "expires_at": sub.active_until.isoformat() if sub.active_until else None,
            "razorpay_status": razorpay_status,
            "has_scheduled_changes": has_scheduled_changes,
            "schedule_change_at": schedule_change_at,
        }

    except requests.exceptions.RequestException as e:
        logger.error("Razorpay cancel API error", extra={"error": str(e), "provider_subscription_id": _mask(provider_sub_id)})
        # Local safeguard to stop auto-renew attempts in our system
        sub.auto_renew = False
        sub.cancel_reason = f"provider_subscription_id:{provider_sub_id} reason:{reason} (API_ERROR)"
        db.add(sub)
        db.commit()

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
        catalog: CatalogProduct = (
            db.query(CatalogProduct)
            .filter(CatalogProduct.sku == plan_sku, CatalogProduct.active == True)
            .first()
        )
        if not catalog:
            info["plan_check"] = {"sku": plan_sku, "exists": False}
        else:
            try:
                rp = rzp_get_plan(catalog.razorpay_plan_id)
                info["plan_check"] = {
                    "sku": plan_sku,
                    "db_amount_minor": getattr(catalog, "base_amount_minor", None),
                    "db_plan_id": catalog.razorpay_plan_id,
                    "rzp_amount_minor": rp.get("item", {}).get("amount"),
                    "rzp_currency": rp.get("item", {}).get("currency"),
                    "ok": rp.get("item", {}).get("amount") == getattr(catalog, "base_amount_minor", None),
                }
            except Exception as e:
                info["plan_check"] = {"sku": plan_sku, "error": str(e)}

    return info
