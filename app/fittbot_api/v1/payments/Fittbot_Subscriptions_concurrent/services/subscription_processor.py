import asyncio
import json
import logging
import random
import time
from contextlib import contextmanager
from typing import Any, Dict, Optional

import httpx
from sqlalchemy import or_
from sqlalchemy.orm import Session
from redis import Redis

from ..config import HighConcurrencyConfig
from ..schemas import SubscriptionCheckoutCommand, SubscriptionVerifyCommand
from ..stores.command_store import CommandStore
from ...config.database import PaymentDatabase
from ...config.settings import get_payment_settings
from ...models.catalog import CatalogProduct
from ...models.payments import Payment
from ...models.subscriptions import Subscription
from ...models.orders import Order, OrderItem
from ...razorpay.client import (
    create_subscription_async as rzp_create_subscription,
    get_plan_async as rzp_get_plan,
    get_payment_async as rzp_get_payment,
)
from ...razorpay.db_helpers import (
    create_or_update_subscription_pending,
    create_pending_order,
)
from ...utils import run_sync_db_operation
from app.fittbot_api.v1.payments.Fittbot_Subscriptions import razorpay as legacy_rzp

from app.fittbot_api.v1.client.client_api.nutrition.nutrition_eligibility_service import (
    grant_nutrition_eligibility_sync,
    calculate_nutrition_sessions_from_fittbot_plan,
)
from app.fittbot_api.v1.client.client_api.reward_program.reward_service import (
    add_subscription_entry,
)
from app.models.async_database import create_celery_async_sessionmaker
from .referral_cash_service import maybe_credit_referrer_for_yearly_subscription
from .payment_event_logger import PaymentEventLogger

logger = logging.getLogger("payments.razorpay.v2.processor")
pel = PaymentEventLogger("razorpay", "subscription")


class SubscriptionProcessor:

    def __init__(
        self,
        config: HighConcurrencyConfig,
        payment_db: PaymentDatabase,
        *,
        redis: Optional[Redis] = None,
    ):
        self.config = config
        self.payment_db = payment_db
        self.settings = get_payment_settings()
        self._provider_semaphore = asyncio.Semaphore(config.max_provider_concurrency)
        self.redis = redis

    async def process_checkout(self, command_id: str, store: CommandStore) -> None:
        record = await store.mark_processing(command_id)
        payload = SubscriptionCheckoutCommand(command_id=command_id, **record.payload)
        _start = time.perf_counter()
        pel.checkout_started(command_id=command_id, client_id=payload.user_id, plan_sku=payload.plan_sku)
        try:
            result = await self._execute_checkout(payload)
        except Exception as exc:
            pel.checkout_failed(command_id=command_id, client_id=payload.user_id,
                                error_code=type(exc).__name__, error_detail=str(exc),
                                duration_ms=int((time.perf_counter() - _start) * 1000),
                                plan_sku=payload.plan_sku)
            logger.exception("Checkout command failed", extra={"command_id": command_id})
            await store.mark_failed(command_id, str(exc))
            return
        pel.checkout_completed(command_id=command_id, client_id=payload.user_id,
                               duration_ms=int((time.perf_counter() - _start) * 1000),
                               plan_sku=payload.plan_sku)
        await store.mark_completed(command_id, result)

    async def process_verify(self, command_id: str, store: CommandStore) -> None:
        record = await store.mark_processing(command_id)
        payload = SubscriptionVerifyCommand(command_id=command_id, **record.payload)
        _start = time.perf_counter()
        pel.verify_started(command_id=command_id, client_id=payload.user_id,
                           razorpay_payment_id=payload.razorpay_payment_id,
                           razorpay_subscription_id=payload.razorpay_subscription_id)
        try:
            result = await self._execute_verify(payload)
        except Exception as exc:
            pel.verify_failed(command_id=command_id, client_id=payload.user_id,
                              error_code=type(exc).__name__, error_detail=str(exc),
                              duration_ms=int((time.perf_counter() - _start) * 1000))
            logger.exception("Verify command failed", extra={"command_id": command_id})
            await store.mark_failed(command_id, str(exc))
            return
        await store.mark_completed(command_id, result)

    async def _execute_checkout(self, command: SubscriptionCheckoutCommand) -> Dict[str, Any]:
        with self._session_scope() as session:
            catalog = await self._fetch_catalog(session, command.plan_sku)
            if not catalog or not catalog.razorpay_plan_id:
                raise ValueError("invalid_plan_sku")

            plan = await self._maybe_fetch_plan(catalog.razorpay_plan_id)
            total_count = self._resolve_total_count(plan)

            notes = {"plan_sku": command.plan_sku, "customer_id": command.user_id}
            notes.update({k: str(v) for k, v in (command.metadata or {}).items()})

            _prov_start = time.perf_counter()
            pel.provider_call_started(command_id=command.command_id, provider_endpoint="create_subscription")
            try:
                subscription = await self._provider_call(
                    rzp_create_subscription(
                        catalog.razorpay_plan_id,
                        notes=notes,
                        total_count=total_count,
                    )
                )
            except Exception as exc:
                pel.provider_call_failed(command_id=command.command_id, provider_endpoint="create_subscription",
                                         error_code=type(exc).__name__,
                                         duration_ms=int((time.perf_counter() - _prov_start) * 1000))
                raise
            pel.provider_call_completed(command_id=command.command_id, provider_endpoint="create_subscription",
                                        duration_ms=int((time.perf_counter() - _prov_start) * 1000))
            sub_id = subscription["id"]

            try:
                order = await run_sync_db_operation(
                    lambda: create_pending_order(
                        session,
                        user_id=command.user_id,
                        amount_minor=catalog.base_amount_minor,
                        sub_id=sub_id,
                        sku=catalog.sku,
                        title=catalog.title,
                    )
                )
                pel.order_created(command_id=command.command_id, client_id=command.user_id,
                                  razorpay_subscription_id=sub_id, plan_sku=catalog.sku)
                self._log_order_creation(command.user_id, sub_id, order)
                await run_sync_db_operation(
                    lambda: create_or_update_subscription_pending(
                        session,
                        user_id=command.user_id,
                        plan_sku=catalog.sku,
                        provider_subscription_id=sub_id,
                    )
                )
                await run_sync_db_operation(session.commit)
            except Exception:
                await run_sync_db_operation(session.rollback)
                raise

            return {
                "subscription_id": sub_id,
                "order_id": getattr(order, "id", None),
                "razorpay_key_id": self.settings.razorpay_key_id,
                "display_title": catalog.title,
            }

    async def _fetch_catalog(self, session: Session, plan_sku: str) -> CatalogProduct:
        def _op() -> CatalogProduct:
            return (
                session.query(CatalogProduct)
                .filter(CatalogProduct.sku == plan_sku, CatalogProduct.active.is_(True))
                .first()
            )

        return await run_sync_db_operation(_op)

    async def _maybe_fetch_plan(self, plan_id: str) -> Dict[str, Any]:
        try:
            return await self._provider_call(rzp_get_plan(plan_id))
        except (httpx.RequestError, httpx.HTTPStatusError) as exc:
            logger.warning("Plan validation skipped: %s", exc)
            return {}

    async def _provider_call(self, coro):
        async with self._provider_semaphore:
            return await asyncio.wait_for(coro, timeout=self.config.provider_timeout_seconds)

    def _resolve_total_count(self, plan_entity: Dict[str, Any]) -> int:
        period = (plan_entity or {}).get("period")
        interval = (plan_entity or {}).get("interval")
        if period in ("year", "yearly"):
            return 1
        if period == "monthly" and interval and int(interval) > 1:
            return 1
        return 12

    async def _execute_verify(self, command: SubscriptionVerifyCommand) -> Dict[str, Any]:
        pid = command.razorpay_payment_id
        sid = command.razorpay_subscription_id
        sig = command.razorpay_signature
        user_id = command.user_id

        _verify_start = time.perf_counter()
        if not legacy_rzp.verify_checkout_subscription_sig(
            self.settings.razorpay_key_secret, pid, sid, sig
        ):
            await legacy_rzp.log_security_event(
                "INVALID_SIGNATURE", {"payment_id": legacy_rzp._mask(pid), "sub_id": legacy_rzp._mask(sid)}
            )
            pel.verify_signature_invalid(command_id=command.command_id, client_id=user_id,
                                         razorpay_payment_id=pid, razorpay_subscription_id=sid,
                                         duration_ms=int((time.perf_counter() - _verify_start) * 1000))
            return {"verified": False, "captured": False, "error": "invalid_signature"}

        logger.info(
            "RAZORPAY_VERIFY_COMMAND_START",
            extra={
                "payment_id": legacy_rzp._mask(pid),
                "subscription_id": legacy_rzp._mask(sid),
            },
        )

        local_result = await self._poll_local_confirmation(command)
        if local_result:
            _dur = int((time.perf_counter() - _verify_start) * 1000)
            logger.info(
                "RAZORPAY_VERIFY_COMPLETED_VIA_WEBHOOK",
                extra={
                    "payment_id": legacy_rzp._mask(pid),
                    "subscription_id": legacy_rzp._mask(sid),
                },
            )
            pel.verify_completed(command_id=command.command_id, verify_path="cache_or_db_poll",
                                 client_id=user_id, duration_ms=_dur,
                                 razorpay_payment_id=pid, razorpay_subscription_id=sid)
            pel.payment_captured(command_id=command.command_id, client_id=user_id,
                                 razorpay_payment_id=pid, razorpay_subscription_id=sid)
            # Ensure nutrition eligibility is granted even when webhook path completed first
            try:
                with self._session_scope() as session:
                    await self._maybe_grant_nutrition_eligibility(session, sid, user_id)
                pel.side_effect_success(command_id=command.command_id, side_effect="nutrition", client_id=user_id)
            except Exception as exc:  # pragma: no cover - defensive
                pel.side_effect_failed(command_id=command.command_id, side_effect="nutrition",
                                       error_detail=str(exc), client_id=user_id)
                logger.warning(
                    "RAZORPAY_VERIFY_NUTRITION_ELIGIBILITY_FAILED",
                    extra={
                        "subscription_id": legacy_rzp._mask(sid),
                        "user_id": legacy_rzp._mask(user_id) if user_id else None,
                        "error": str(exc),
                    },
                )
            # Add reward entry for subscription (2 entries)
            try:
                await self._maybe_add_reward_entry(sid, user_id)
                pel.side_effect_success(command_id=command.command_id, side_effect="reward", client_id=user_id)
            except Exception as exc:  # pragma: no cover - defensive
                pel.side_effect_failed(command_id=command.command_id, side_effect="reward",
                                       error_detail=str(exc), client_id=user_id)
                logger.warning(
                    "RAZORPAY_VERIFY_REWARD_ENTRY_FAILED",
                    extra={
                        "subscription_id": legacy_rzp._mask(sid),
                        "user_id": legacy_rzp._mask(user_id) if user_id else None,
                        "error": str(exc),
                    },
                )
            # Credit referrer if referee purchased a 1-year plan
            try:
                plan_name = self._resolve_plan_name_for_subscription(sid, user_id)
                if plan_name:
                    await maybe_credit_referrer_for_yearly_subscription(user_id, sid, plan_name)
                    pel.side_effect_success(command_id=command.command_id, side_effect="referral", client_id=user_id)
                else:
                    pel.side_effect_skipped(command_id=command.command_id, side_effect="referral",
                                            reason="not_yearly_plan", client_id=user_id)
            except Exception as exc:  # pragma: no cover - defensive
                pel.side_effect_failed(command_id=command.command_id, side_effect="referral",
                                       error_detail=str(exc), client_id=user_id)
                logger.warning(
                    "RAZORPAY_VERIFY_REFERRAL_CREDIT_FAILED",
                    extra={
                        "subscription_id": legacy_rzp._mask(sid),
                        "user_id": legacy_rzp._mask(user_id) if user_id else None,
                        "error": str(exc),
                    },
                )
            return local_result

        payment_data = await self._fetch_payment_from_provider(pid)
        payment_status = payment_data.get("status")
        logger.warning(
            "RAZORPAY_PAYMENT_STATUS_FROM_PROVIDER",
            extra={
                "payment_id": legacy_rzp._mask(pid),
                "status": payment_status,
                "full_status_data": {k: v for k, v in payment_data.items() if k in ["status", "captured", "amount", "method"]},
            },
        )

        if payment_status == "captured":
            _dur = int((time.perf_counter() - _verify_start) * 1000)
            pel.verify_completed(command_id=command.command_id, verify_path="provider_fallback",
                                 client_id=user_id, duration_ms=_dur,
                                 razorpay_payment_id=pid, razorpay_subscription_id=sid)
            pel.payment_captured(command_id=command.command_id, client_id=user_id,
                                 razorpay_payment_id=pid, razorpay_subscription_id=sid)
            with self._session_scope() as session:
                result = await legacy_rzp.handle_captured_payment_secure(
                    session, pid, sid, payment_data
                )
                # Explicitly ensure nutrition eligibility mirrors legacy/webhook behavior
                try:
                    await self._maybe_grant_nutrition_eligibility(session, sid, user_id)
                    pel.side_effect_success(command_id=command.command_id, side_effect="nutrition", client_id=user_id)
                except Exception as exc:  # pragma: no cover - defensive logging
                    pel.side_effect_failed(command_id=command.command_id, side_effect="nutrition",
                                           error_detail=str(exc), client_id=user_id)
                    logger.warning(
                        "RAZORPAY_VERIFY_NUTRITION_ELIGIBILITY_FAILED",
                        extra={
                            "subscription_id": legacy_rzp._mask(sid),
                            "user_id": legacy_rzp._mask(user_id) if user_id else None,
                            "error": str(exc),
                        },
                    )
                # Add reward entry for subscription (2 entries)
                try:
                    await self._maybe_add_reward_entry(sid, user_id)
                    pel.side_effect_success(command_id=command.command_id, side_effect="reward", client_id=user_id)
                except Exception as exc:  # pragma: no cover - defensive logging
                    pel.side_effect_failed(command_id=command.command_id, side_effect="reward",
                                           error_detail=str(exc), client_id=user_id)
                    logger.warning(
                        "RAZORPAY_VERIFY_REWARD_ENTRY_FAILED",
                        extra={
                            "subscription_id": legacy_rzp._mask(sid),
                            "user_id": legacy_rzp._mask(user_id) if user_id else None,
                            "error": str(exc),
                        },
                    )
                # Credit referrer if referee purchased a 1-year plan
                try:
                    plan_name = self._resolve_plan_name_for_subscription(sid, user_id)
                    if plan_name:
                        await maybe_credit_referrer_for_yearly_subscription(user_id, sid, plan_name)
                        pel.side_effect_success(command_id=command.command_id, side_effect="referral", client_id=user_id)
                    else:
                        pel.side_effect_skipped(command_id=command.command_id, side_effect="referral",
                                                reason="not_yearly_plan", client_id=user_id)
                except Exception as exc:  # pragma: no cover - defensive
                    pel.side_effect_failed(command_id=command.command_id, side_effect="referral",
                                           error_detail=str(exc), client_id=user_id)
                    logger.warning(
                        "RAZORPAY_VERIFY_REFERRAL_CREDIT_FAILED",
                        extra={
                            "subscription_id": legacy_rzp._mask(sid),
                            "user_id": legacy_rzp._mask(user_id) if user_id else None,
                            "error": str(exc),
                        },
                    )
                return result

        if payment_status == "authorized":
            pel.payment_authorized(command_id=command.command_id, client_id=user_id,
                                   razorpay_payment_id=pid, razorpay_subscription_id=sid,
                                   duration_ms=int((time.perf_counter() - _verify_start) * 1000))
            await legacy_rzp.log_verification_event("AUTHORIZED", pid, sid)
            return {
                "verified": True,
                "captured": False,
                "retryAfterMs": 2000,
                "message": "Payment authorized, finalizing...",
            }

        if payment_status in ["failed", "refunded"]:
            pel.verify_failed(command_id=command.command_id, client_id=user_id,
                              error_code=payment_status,
                              razorpay_payment_id=pid, razorpay_subscription_id=sid,
                              duration_ms=int((time.perf_counter() - _verify_start) * 1000))
            await legacy_rzp.log_verification_event(
                "FAILED_PAYMENT", pid, sid, {"status": payment_status}
            )
            return {
                "verified": False,
                "captured": False,
                "status": payment_status,
                "message": f"Payment {payment_status}",
            }

        pel.verify_pending(command_id=command.command_id, client_id=user_id,
                           razorpay_payment_id=pid, razorpay_subscription_id=sid,
                           provider_status=payment_status,
                           duration_ms=int((time.perf_counter() - _verify_start) * 1000))
        await legacy_rzp.log_security_event(
            "UNKNOWN_STATUS", {"payment_id": legacy_rzp._mask(pid), "status": payment_status}
        )
        logger.info(
            "RAZORPAY_VERIFY_PENDING",
            extra={
                "payment_id": legacy_rzp._mask(pid),
                "subscription_id": legacy_rzp._mask(sid),
                "status": payment_status,
            },
        )
        return {
            "verified": True,
            "captured": False,
            "retryAfterMs": 3000,
            "message": "Payment verification in progress",
        }

    async def _poll_local_confirmation(self, command: SubscriptionVerifyCommand) -> Optional[Dict[str, Any]]:
        pid = command.razorpay_payment_id
        sid = command.razorpay_subscription_id
        user_id = command.user_id
        delay = max(0.2, self.config.verify_db_poll_base_delay_ms / 1000)
        max_delay = self.config.verify_db_poll_max_delay_ms / 1000
        deadline = time.monotonic() + max(1, self.config.verify_db_poll_total_timeout_seconds)

        attempt = 0
        max_attempts = max(1, self.config.verify_db_poll_attempts)
        while time.monotonic() < deadline and attempt < max_attempts:
            attempt += 1

            capture_snapshot = await self._capture_marker_snapshot(pid)
            if capture_snapshot:
                logger.info(
                    "RAZORPAY_VERIFY_CAPTURE_CACHE_HIT",
                    extra={
                        "payment_id": legacy_rzp._mask(pid),
                        "subscription_id": legacy_rzp._mask(sid),
                        "attempt": attempt,
                    },
                )
                with self._session_scope() as session:
                    return await legacy_rzp.handle_captured_payment_secure(
                        session, pid, sid, capture_snapshot
                    )

            with self._session_scope() as session:
                premium_snapshot = self._premium_confirmation_snapshot(session, user_id, sid, pid)
                if premium_snapshot:
                    logger.info(
                        "RAZORPAY_VERIFY_HAS_PREMIUM",
                        extra={
                            "payment_id": legacy_rzp._mask(pid),
                            "subscription_id": legacy_rzp._mask(sid),
                            "attempt": attempt,
                            "user_id": legacy_rzp._mask(user_id),
                        },
                    )
                    return await legacy_rzp.handle_captured_payment_secure(
                        session, pid, sid, premium_snapshot
                    )

                payment_data = self._payment_payload_from_db(session, pid)
                if payment_data:
                    logger.info(
                        "RAZORPAY_VERIFY_WEBHOOK_PAYMENT_FOUND",
                        extra={
                            "payment_id": legacy_rzp._mask(pid),
                            "subscription_id": legacy_rzp._mask(sid),
                            "attempt": attempt,
                        },
                    )
                    return await legacy_rzp.handle_captured_payment_secure(
                        session, pid, sid, payment_data
                    )

            if time.monotonic() >= deadline:
                break
            await asyncio.sleep(delay + random.uniform(0, 0.3))
            delay = min(delay * 1.5, max_delay)

        return None

    def _premium_confirmation_snapshot(
        self, session: Session, user_id: Optional[str], sid: Optional[str], pid: str
    ) -> Optional[Dict[str, Any]]:
        now = legacy_rzp.now_ist()
        subscription = None
        if user_id:
            subscription = (
                session.query(Subscription)
                .filter(
                    Subscription.customer_id == user_id,
                    Subscription.provider == legacy_rzp.PROVIDER,
                    Subscription.status.in_(["active", "renewed", "pending"]),
                    or_(Subscription.active_from == None, Subscription.active_from <= now),
                    or_(Subscription.active_until == None, Subscription.active_until >= now),
                )
                .order_by(Subscription.created_at.desc())
                .first()
            )
        if not subscription and sid:
            subscription = (
                session.query(Subscription)
                .filter(
                    Subscription.provider == legacy_rzp.PROVIDER,
                    Subscription.id == sid,
                )
                .order_by(Subscription.created_at.desc())
                .first()
            )
        if not subscription or subscription.latest_txn_id != pid:
            return None
        return self._payment_payload_from_db(session, pid)

    def _payment_payload_from_db(self, session: Session, pid: str) -> Optional[Dict[str, Any]]:
        payment = (
            session.query(Payment)
            .filter(
                Payment.provider == legacy_rzp.PROVIDER,
                Payment.provider_payment_id == pid,
                Payment.status == "captured",
            )
            .first()
        )
        if not payment:
            return None
        return {
            "amount": payment.amount_minor,
            "currency": payment.currency or "INR",
            "method": (payment.payment_metadata or {}).get("method") if payment.payment_metadata else None,
        }

    async def _capture_marker_snapshot(self, pid: str) -> Optional[Dict[str, Any]]:
        if not self.redis:
            return None
        key = f"{self.config.redis_prefix}:capture:{pid}"
        raw = await asyncio.to_thread(self.redis.get, key)
        if not raw:
            return None
        try:
            payload = json.loads(raw)
        except (TypeError, json.JSONDecodeError):
            return None
        amount = payload.get("amount")
        if amount is None:
            return None
        return {
            "amount": int(amount),
            "currency": payload.get("currency") or "INR",
            "method": payload.get("method"),
        }

    def _log_order_creation(self, user_id: str, sub_id: str, order) -> None:
        if not order:
            return
        created_at = getattr(order, "created_at", None)
        logger.info(
            "RAZORPAY_CHECKOUT_ORDER_CREATED",
            extra={
                "order_id": getattr(order, "id", None),
                "subscription_id": legacy_rzp._mask(sub_id),
                "user_id": legacy_rzp._mask(user_id),
                "order_created_at": created_at.isoformat() if created_at else None,
            },
        )

    async def _fetch_payment_from_provider(self, pid: str) -> Dict[str, Any]:
        attempts = max(1, self.config.verify_provider_max_attempts)
        last_exc: Optional[Exception] = None
        for attempt in range(1, attempts + 1):
            _call_start = time.perf_counter()
            try:
                logger.info(
                    "RAZORPAY_VERIFY_PROVIDER_CALL",
                    extra={
                        "payment_id": legacy_rzp._mask(pid),
                        "attempt": attempt,
                    },
                )
                pel.provider_call_started(command_id=pid, provider_endpoint="get_payment", attempt=attempt)
                result = await self._provider_call(rzp_get_payment(pid))
                pel.provider_call_completed(command_id=pid, provider_endpoint="get_payment",
                                            duration_ms=int((time.perf_counter() - _call_start) * 1000))
                return result
            except (httpx.RequestError, httpx.HTTPStatusError) as exc:
                pel.provider_call_failed(command_id=pid, provider_endpoint="get_payment",
                                         error_code=type(exc).__name__,
                                         duration_ms=int((time.perf_counter() - _call_start) * 1000),
                                         attempt=attempt)
                last_exc = exc
                await asyncio.sleep(min(attempt * 1.5, 5))
        if last_exc:
            logger.warning(
                "RAZORPAY_VERIFY_PROVIDER_CALL_FAILED",
                extra={
                    "payment_id": legacy_rzp._mask(pid),
                    "attempts": attempts,
                    "error": str(last_exc),
                },
            )
            raise last_exc
        return {}

    @contextmanager
    def _session_scope(self):
        with self.payment_db.get_session() as session:
            yield session

    async def _maybe_grant_nutrition_eligibility(
        self, session: Session, subscription_id: str, user_id: Optional[str]
    ) -> None:
        """
        Best-effort nutrition eligibility grant for Razorpay subscriptions in the concurrent flow.
        Uses idempotent source_id so safe to call after legacy handlers.
        """
        # Resolve subscription and plan
        sub = (
            session.query(Subscription)
            .filter(
                Subscription.provider == legacy_rzp.PROVIDER,
                Subscription.id == subscription_id,
            )
            .order_by(Subscription.created_at.desc())
            .first()
        )

        plan_name: Optional[str] = None
        duration_months = 0
        resolved_client_id: Optional[str] = user_id or (sub.customer_id if sub else None)

        if sub and sub.product_id:
            plan_name = sub.product_id
        else:
            # Fallback: derive plan from order item SKU
            order = (
                session.query(Order)
                .filter(
                    Order.provider == legacy_rzp.PROVIDER,
                    Order.provider_order_id == subscription_id,
                )
                .order_by(Order.created_at.desc())
                .first()
            )
            if order:
                resolved_client_id = resolved_client_id or order.customer_id
                order_item = (
                session.query(OrderItem)
                    .filter(OrderItem.order_id == order.id)
                    .order_by(OrderItem.id.desc())
                    .first()
                )
                if order_item and getattr(order_item, "sku", None):
                    plan_name = order_item.sku

        if plan_name:
            lower = plan_name.lower()
            if "12" in lower or "twelve" in lower:
                duration_months = 12
            elif "6" in lower or "six" in lower:
                duration_months = 6

        if not plan_name or duration_months < 6 or not resolved_client_id:
            return

        sessions = calculate_nutrition_sessions_from_fittbot_plan(plan_name, duration_months)
        if sessions <= 0:
            return

        grant_nutrition_eligibility_sync(
            db=session,
            client_id=int(resolved_client_id),
            source_type="fittbot_subscription",
            source_id=subscription_id,
            plan_name=plan_name,
            duration_months=duration_months,
            gym_id=None,
        )
        session.commit()
        logger.info(
            "[NUTRITION_ELIGIBILITY_CONCURRENT] client_id=%s, sub_id=%s, plan=%s, duration=%sm, sessions=%s",
            resolved_client_id,
            subscription_id,
            plan_name,
            duration_months,
            sessions,
        )

    async def _maybe_add_reward_entry(
        self, subscription_id: str, user_id: Optional[str]
    ) -> None:
        """
        Best-effort reward entry grant for Razorpay subscriptions.
        Gives 2 entries per subscription (max 8 total for subscription method).
        Uses idempotent source_id so safe to call multiple times.
        """
        if not user_id:
            logger.warning(
                "[REWARD_ENTRY_SKIPPED] No user_id for subscription",
                extra={"subscription_id": legacy_rzp._mask(subscription_id)},
            )
            return

        try:
            async_session_maker = create_celery_async_sessionmaker()
            async with async_session_maker() as async_db:
                reward_ok, entries_added, reward_msg = await add_subscription_entry(
                    async_db,
                    client_id=int(user_id),
                    source_id=subscription_id,
                )
                await async_db.commit()

                if reward_ok:
                    logger.info(
                        "[REWARD_ENTRY_ADDED] Razorpay subscription reward entry",
                        extra={
                            "client_id": user_id,
                            "subscription_id": legacy_rzp._mask(subscription_id),
                            "entries_added": entries_added,
                            "reward_msg": reward_msg,
                        },
                    )
                else:
                    logger.warning(
                        "[REWARD_ENTRY_SKIPPED] %s",
                        reward_msg,
                        extra={
                            "client_id": user_id,
                            "subscription_id": legacy_rzp._mask(subscription_id),
                        },
                    )
        except Exception as exc:
            logger.warning(
                "[REWARD_ENTRY_FAILED] Error adding reward entry for Razorpay subscription",
                extra={
                    "client_id": user_id,
                    "subscription_id": legacy_rzp._mask(subscription_id),
                    "error": str(exc),
                },
            )

    def _resolve_plan_name_for_subscription(
        self, subscription_id: str, user_id: Optional[str]
    ) -> Optional[str]:
        """
        Resolve plan name (product_id) for a subscription by querying the DB.
        Falls back to OrderItem SKU if Subscription.product_id is unavailable.
        """
        with self._session_scope() as session:
            sub = (
                session.query(Subscription)
                .filter(
                    Subscription.provider == legacy_rzp.PROVIDER,
                    Subscription.id == subscription_id,
                )
                .order_by(Subscription.created_at.desc())
                .first()
            )

            if sub and sub.product_id:
                return sub.product_id

            # Fallback: derive plan from order item SKU
            order = (
                session.query(Order)
                .filter(
                    Order.provider == legacy_rzp.PROVIDER,
                    Order.provider_order_id == subscription_id,
                )
                .order_by(Order.created_at.desc())
                .first()
            )
            if order:
                order_item = (
                    session.query(OrderItem)
                    .filter(OrderItem.order_id == order.id)
                    .order_by(OrderItem.id.desc())
                    .first()
                )
                if order_item and getattr(order_item, "sku", None):
                    return order_item.sku

            return None
