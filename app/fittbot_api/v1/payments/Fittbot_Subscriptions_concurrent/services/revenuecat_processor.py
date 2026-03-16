import asyncio
import json
import logging
import time
from contextlib import contextmanager
from datetime import datetime
from typing import Any, Dict, Optional

from sqlalchemy.orm import Session
from redis import Redis

from ..config import HighConcurrencyConfig
from ..schemas import (
    RevenueCatOrderCommand,
    RevenueCatVerifyCommand,
    RevenueCatWebhookCommand,
)
from ..stores.command_store import CommandStore
from ...config.database import PaymentDatabase
from ...config.settings import get_payment_settings
from ...models.catalog import CatalogProduct
from ...models.enums import Provider, SubscriptionStatus
from ...models.orders import Order
from ...models.payments import Payment
from ...models.subscriptions import Subscription
from ...models.webhook_logs import WebhookProcessingLog
from ...services.subscription_sync_service import SubscriptionSyncService
from ...utils import run_sync_db_operation
from ...revenuecat.client import RevenueCatAPIError, verify_purchase as rc_verify_purchase
from app.models.fittbot_models import FreeTrial

from app.fittbot_api.v1.payments.Fittbot_Subscriptions import revenue_cat as legacy_rc
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

logger = logging.getLogger("payments.revenuecat.v2.processor")
pel = PaymentEventLogger("revenuecat", "subscription")

mask_value = legacy_rc._mask  # pylint: disable=protected-access
now_ist = legacy_rc.now_ist
lock_query = legacy_rc.lock_query
generate_event_id = legacy_rc.generate_event_id
log_security_event = legacy_rc.log_security_event
handle_billing_issues = legacy_rc.handle_billing_issues


class RevenueCatProcessor:
    """Background worker that mirrors the legacy RevenueCat endpoints."""

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
        self.redis = redis

    async def process_order(self, command_id: str, store: CommandStore) -> None:
        record = await store.mark_processing(command_id)
        payload = RevenueCatOrderCommand(**record.payload)
        _start = time.perf_counter()
        pel.checkout_started(command_id=command_id, client_id=str(payload.client_id),
                             plan_sku=payload.product_sku)
        try:
            result = await self._create_pending_order(payload)
        except Exception as exc:  # pragma: no cover - logged for observability
            pel.checkout_failed(command_id=command_id, client_id=str(payload.client_id),
                                error_code=type(exc).__name__, error_detail=str(exc),
                                duration_ms=int((time.perf_counter() - _start) * 1000))
            logger.exception("RevenueCat order command failed: %s", exc)
            await store.mark_failed(command_id, str(exc))
            return
        pel.checkout_completed(command_id=command_id, client_id=str(payload.client_id),
                               duration_ms=int((time.perf_counter() - _start) * 1000),
                               plan_sku=payload.product_sku)
        pel.order_created(command_id=command_id, client_id=str(payload.client_id),
                          plan_sku=payload.product_sku)
        await store.mark_completed(command_id, result)

    async def process_verify(self, command_id: str, store: CommandStore) -> None:
        record = await store.mark_processing(command_id)
        payload = RevenueCatVerifyCommand(**record.payload)
        _start = time.perf_counter()
        pel.verify_started(command_id=command_id, client_id=str(payload.client_id))
        try:
            result = await self._verify_purchase(payload.client_id)
        except Exception as exc:
            pel.verify_failed(command_id=command_id, client_id=str(payload.client_id),
                              error_code=type(exc).__name__, error_detail=str(exc),
                              duration_ms=int((time.perf_counter() - _start) * 1000))
            logger.exception("RevenueCat verify command failed: %s", exc)
            await store.mark_failed(command_id, str(exc))
            return
        _dur = int((time.perf_counter() - _start) * 1000)
        if result.get("verified"):
            pel.verify_completed(command_id=command_id, client_id=str(payload.client_id),
                                 verify_path="revenuecat_api", duration_ms=_dur)
            pel.payment_captured(command_id=command_id, client_id=str(payload.client_id))
        else:
            pel.verify_failed(command_id=command_id, client_id=str(payload.client_id),
                              error_code="not_verified", duration_ms=_dur,
                              error_detail=result.get("message"))
        await store.mark_completed(command_id, result)

    async def process_webhook(self, command_id: str, store: CommandStore) -> None:
        record = await store.mark_processing(command_id)
        payload = RevenueCatWebhookCommand(**record.payload)
        _start = time.perf_counter()
        pel.webhook_received(command_id=command_id)
        try:
            result = await self._handle_webhook(payload.signature, payload.raw_body)
        except Exception as exc:
            pel.webhook_failed(command_id=command_id, error_code=type(exc).__name__,
                               duration_ms=int((time.perf_counter() - _start) * 1000),
                               error_detail=str(exc))
            logger.exception("RevenueCat webhook command failed: %s", exc)
            await store.mark_failed(command_id, str(exc))
            return
        pel.webhook_processed(command_id=command_id,
                              duration_ms=int((time.perf_counter() - _start) * 1000),
                              event_type=result.get("event_type"), status=result.get("status"))
        await store.mark_completed(command_id, result)

    async def _create_pending_order(self, payload: RevenueCatOrderCommand) -> Dict[str, Any]:
        with self._session_scope() as session:
            def _op() -> Dict[str, Any]:
                product = (
                    session.query(CatalogProduct)
                    .filter(
                        CatalogProduct.sku == payload.product_sku,
                        CatalogProduct.active.is_(True),
                    )
                    .first()
                )
                if not product:
                    raise ValueError("product_not_found")

                ist_now = now_ist()
                order_id = f"ord_{ist_now.strftime('%Y%m%d')}_{payload.client_id}_{int(ist_now.timestamp())}"
                order = Order(
                    id=order_id,
                    customer_id=payload.client_id,
                    currency=payload.currency,
                    provider=Provider.google_play.value,
                    gross_amount_minor=product.base_amount_minor,
                    status="pending",
                )
                session.add(order)
                session.commit()
                session.refresh(order)

                return {
                    "order_id": order.id,
                    "client_id": payload.client_id,
                    "product_sku": payload.product_sku,
                    "amount": product.base_amount_minor,
                    "currency": payload.currency,
                    "status": "pending",
                    "api_key": self.settings.revenuecat_api_key,
                    "expires_at": (now_ist() + legacy_rc.timedelta(minutes=15)).isoformat(),
                    "created_at": order.created_at.isoformat(),
                }

            return await run_sync_db_operation(_op)

    async def _verify_purchase(self, customer_id: str) -> Dict[str, Any]:
        local_result = await self._poll_local_confirmation(customer_id)
        if local_result:
            return local_result
        return await self._verify_purchase_via_revenuecat(customer_id)

    async def _verify_purchase_via_revenuecat(self, customer_id: str) -> Dict[str, Any]:
        with self._session_scope() as session:
            settings = self.settings

            def _op() -> Dict[str, Any]:
                sync_service = SubscriptionSyncService(session)
                try:
                    latest_order = (
                        session.query(Order)
                        .filter(
                            Order.customer_id == customer_id,
                            Order.provider == Provider.google_play.value,
                        )
                        .order_by(Order.created_at.desc())
                        .first()
                    )

                    pel.provider_call_started(command_id=f"rc_verify_{customer_id}",
                                              provider_endpoint="verify_purchase")
                    _prov_start = time.perf_counter()
                    try:
                        has_active, subscription_data, error_msg = rc_verify_purchase(
                            app_user_id=customer_id,
                            api_key=settings.revenuecat_api_key,
                        )
                        pel.provider_call_completed(command_id=f"rc_verify_{customer_id}",
                                                    provider_endpoint="verify_purchase",
                                                    duration_ms=int((time.perf_counter() - _prov_start) * 1000))
                    except Exception as prov_exc:
                        pel.provider_call_failed(command_id=f"rc_verify_{customer_id}",
                                                 provider_endpoint="verify_purchase",
                                                 error_code=type(prov_exc).__name__,
                                                 duration_ms=int((time.perf_counter() - _prov_start) * 1000))
                        raise

                    if not has_active:
                        friendly_message = (
                            error_msg
                            or "No active Google Play subscription found. Please retry in a few seconds."
                        )
                        pel.verify_failed(command_id=f"rc_verify_{customer_id}", client_id=customer_id,
                                          error_code="no_active_subscription",
                                          error_detail=friendly_message)
                        return {
                            "verified": False,
                            "captured": False,
                            "subscription_active": False,
                            "has_premium": False,
                            "message": friendly_message,
                            "order_id": latest_order.id if latest_order else None,
                            "order_status": latest_order.status if latest_order else None,
                            "order_created_at": latest_order.created_at.isoformat()
                            if latest_order and latest_order.created_at
                            else None,
                        }

                    price_info = subscription_data.get("price") or {}
                    price_amount = price_info.get("amount")
                    price_currency = price_info.get("currency") or "INR"
                    price_minor: Optional[int] = None
                    if price_amount is not None:
                        try:
                            price_minor = int(round(float(price_amount) * 100))
                        except (TypeError, ValueError):
                            price_minor = None

                    base_product_id = subscription_data.get("product_identifier", "unknown")
                    plan_identifier = (
                        subscription_data.get("product_plan_identifier")
                        or subscription_data.get("base_plan_identifier")
                        or subscription_data.get("base_plan_id")
                    )
                    if plan_identifier and ":" not in base_product_id:
                        product_id = f"{base_product_id}:{plan_identifier}"
                    else:
                        product_id = base_product_id

                    rc_purchased_date = subscription_data.get("original_purchase_date")
                    rc_expires_date = subscription_data.get("expires_date")

                    txn_candidates = [
                        subscription_data.get("original_transaction_id"),
                        subscription_data.get("original_transaction_identifier"),
                        subscription_data.get("original_store_transaction_id"),
                        subscription_data.get("original_external_purchase_id"),
                        subscription_data.get("transaction_id"),
                        subscription_data.get("store_transaction_id"),
                    ]
                    rc_original_txn_id = next((val for val in txn_candidates if val), None)
                    store_transaction_id = subscription_data.get(
                        "store_transaction_id",
                        f"rc_{customer_id}_{int(now_ist().timestamp())}",
                    )
                    rc_original_txn_id = rc_original_txn_id or store_transaction_id

                    if rc_purchased_date:
                        purchased_date = datetime.fromisoformat(
                            rc_purchased_date.replace("Z", "+00:00")
                        ).astimezone(legacy_rc.IST)
                    else:
                        purchased_date = now_ist()

                    if rc_expires_date:
                        expires_date = datetime.fromisoformat(
                            rc_expires_date.replace("Z", "+00:00")
                        ).astimezone(legacy_rc.IST)
                    else:
                        expires_date = now_ist() + legacy_rc.timedelta(days=30)

                    existing_subscription: Optional[Subscription] = None
                    if store_transaction_id:
                        existing_subscription = lock_query(
                            session.query(Subscription).filter(
                                Subscription.provider == Provider.google_play.value,
                                Subscription.latest_txn_id == store_transaction_id,
                            )
                        ).first()

                    if not existing_subscription and rc_original_txn_id:
                        existing_subscription = lock_query(
                            session.query(Subscription).filter(
                                Subscription.provider == Provider.google_play.value,
                                Subscription.rc_original_txn_id == rc_original_txn_id,
                            )
                        ).first()

                    if not existing_subscription:
                        possible_products = [product_id]
                        if base_product_id and base_product_id != product_id:
                            possible_products.append(base_product_id)
                        existing_subscription = lock_query(
                            session.query(Subscription)
                            .filter(
                                Subscription.customer_id == customer_id,
                                Subscription.product_id.in_(possible_products),
                                Subscription.provider == Provider.google_play.value,
                                Subscription.status.in_(
                                    [
                                        SubscriptionStatus.active.value,
                                        SubscriptionStatus.renewed.value,
                                    ]
                                ),
                            )
                            .order_by(Subscription.created_at.desc())
                        ).first()

                    already_active = (
                        existing_subscription is not None
                        and existing_subscription.status in ["active", "renewed"]
                    )

                    pending_order = lock_query(
                        session.query(Order)
                        .filter(
                            Order.customer_id == customer_id,
                            Order.status == "pending",
                            Order.provider == Provider.google_play.value,
                        )
                        .order_by(Order.created_at.desc())
                    ).first()

                    latest_order_local = pending_order or latest_order
                    amount_minor = pending_order.gross_amount_minor if pending_order else None
                    if amount_minor in (None, 0) and price_minor is not None:
                        amount_minor = price_minor

                    if pending_order:
                        pending_order.status = "paid"
                        pending_order.provider_order_id = store_transaction_id
                        if price_minor is not None and pending_order.gross_amount_minor in (None, 0):
                            pending_order.gross_amount_minor = price_minor
                        if price_currency:
                            pending_order.currency = price_currency
                        session.add(pending_order)
                    else:
                        log_security_event(
                            "ORDER_NOT_FOUND_ON_VERIFY",
                            {
                                "customer_id": mask_value(customer_id),
                                "store_transaction_id": mask_value(store_transaction_id)
                                if store_transaction_id
                                else None,
                                "has_latest_order": bool(latest_order_local),
                            },
                        )
                        amount_minor = amount_minor or 0

                    if existing_subscription:
                        subscription = existing_subscription
                        subscription.product_id = product_id
                        subscription.status = "active"
                        subscription.rc_original_txn_id = rc_original_txn_id
                        subscription.latest_txn_id = store_transaction_id
                        subscription.active_from = purchased_date
                        subscription.active_until = expires_date
                        subscription.auto_renew = True
                        session.add(subscription)
                    else:
                        subscription_id = sync_service.generate_id("sub")
                        subscription = Subscription(
                            id=subscription_id,
                            customer_id=customer_id,
                            product_id=product_id,
                            provider=Provider.google_play.value,
                            status="active",
                            rc_original_txn_id=rc_original_txn_id,
                            latest_txn_id=store_transaction_id,
                            active_from=purchased_date,
                            active_until=expires_date,
                            auto_renew=True,
                        )
                        session.add(subscription)
                        session.flush()

                    payment_id: Optional[str] = None
                    if pending_order:
                        payment_id = sync_service.generate_id("pay")
                        payment = Payment(
                            id=payment_id,
                            order_id=pending_order.id,
                            customer_id=customer_id,
                            provider=Provider.google_play.value,
                            provider_payment_id=store_transaction_id,
                            amount_minor=amount_minor or 0,
                            currency=price_currency,
                            status="captured",
                            payment_metadata={
                                "source": "verify_endpoint",
                                "verified_at": now_ist().isoformat(),
                            },
                        )
                        session.add(payment)

                    try:
                        free_trial = (
                            session.query(FreeTrial)
                            .filter(FreeTrial.client_id == int(customer_id))
                            .first()
                        )
                        if free_trial and free_trial.status != "expired":
                            free_trial.status = "expired"
                            session.add(free_trial)
                    except Exception as ft_error:  # pragma: no cover
                        logger.warning("Failed to update free_trial status: %s", ft_error)

                    # Grant nutrition eligibility for Fittbot subscription (Diamond/Platinum plans)
                    try:
                        plan_name = product_id.lower() if product_id else ""
                        duration_months = 0

                        if "half-yearly" in plan_name or "half_yearly" in plan_name:
                            duration_months = 6
                        elif "12" in plan_name or "twelve" in plan_name or "yearly" in plan_name:
                            duration_months = 12
                        elif "6" in plan_name or "six" in plan_name:
                            duration_months = 6

                        is_eligible_plan = (
                            "diamond" in plan_name or
                            "platinum" in plan_name or
                            duration_months >= 6
                        )

                        if is_eligible_plan and duration_months >= 6:
                            sessions = calculate_nutrition_sessions_from_fittbot_plan(plan_name, duration_months)
                            if sessions > 0:
                                grant_nutrition_eligibility_sync(
                                    db=session,
                                    client_id=int(customer_id),
                                    source_type="fittbot_subscription",
                                    source_id=subscription.id,
                                    plan_name=product_id,
                                    duration_months=duration_months,
                                    gym_id=None,
                                )
                                pel.side_effect_success(command_id=f"rc_verify_{customer_id}",
                                                        side_effect="nutrition", client_id=customer_id)
                            else:
                                pel.side_effect_skipped(command_id=f"rc_verify_{customer_id}",
                                                        side_effect="nutrition", reason="sessions_zero",
                                                        client_id=customer_id)
                        else:
                            pel.side_effect_skipped(command_id=f"rc_verify_{customer_id}",
                                                    side_effect="nutrition", reason="not_eligible_plan",
                                                    client_id=customer_id)
                    except Exception as nutr_exc:
                        pel.side_effect_failed(command_id=f"rc_verify_{customer_id}",
                                               side_effect="nutrition", error_detail=str(nutr_exc),
                                               client_id=customer_id)
                        logger.warning(f"[NUTRITION_ELIGIBILITY_ERROR] Failed to grant nutrition eligibility: {nutr_exc}")

                    session.commit()

                    response_message = (
                        "Purchase already verified" if already_active else "Purchase verified - Premium activated"
                    )
                    return {
                        "verified": True,
                        "captured": True,
                        "subscription_active": True,
                        "has_premium": True,
                        "message": response_message,
                        "subscription_id": subscription.id,
                        "payment_id": payment_id,
                        "order_id": pending_order.id
                        if pending_order
                        else (latest_order_local.id if latest_order_local else None),
                        "active_from": subscription.active_from.isoformat()
                        if subscription.active_from
                        else None,
                        "active_until": subscription.active_until.isoformat()
                        if subscription.active_until
                        else None,
                        "auto_renew": True,
                        "_is_new_subscription": not already_active,  # Flag for reward entry
                        "_customer_id": customer_id,
                        "_product_id": product_id,
                    }
                except RevenueCatAPIError as rc_error:
                    session.rollback()
                    raise rc_error
                except Exception:
                    session.rollback()
                    raise

            result = await run_sync_db_operation(_op)

            # Add reward program entries for new subscriptions (best-effort)
            if result.get("verified") and result.get("_is_new_subscription"):
                _rc_cid = result.get("_customer_id")
                try:
                    if _rc_cid:
                        SessionLocal = create_celery_async_sessionmaker()
                        async with SessionLocal() as async_db:
                            reward_ok, entries_added, reward_msg = await add_subscription_entry(
                                async_db,
                                client_id=int(_rc_cid),
                                source_id=result.get("subscription_id"),
                            )
                            await async_db.commit()
                            pel.side_effect_success(command_id=f"rc_verify_{_rc_cid}",
                                                    side_effect="reward", client_id=_rc_cid)
                            logger.info(
                                "REVENUECAT_SUBSCRIPTION_REWARD_ENTRY",
                                extra={
                                    "client_id": _rc_cid,
                                    "success": reward_ok,
                                    "entries_added": entries_added,
                                    "reward_msg": reward_msg,
                                    "subscription_id": result.get("subscription_id"),
                                },
                            )
                except Exception as reward_exc:
                    pel.side_effect_failed(command_id=f"rc_verify_{_rc_cid}",
                                           side_effect="reward", error_detail=str(reward_exc),
                                           client_id=_rc_cid)
                    logger.warning(
                        "REVENUECAT_REWARD_ENTRY_FAILED",
                        extra={
                            "client_id": _rc_cid,
                            "subscription_id": result.get("subscription_id"),
                            "error": repr(reward_exc),
                        },
                    )

            # Credit referrer if referee purchased a 1-year plan (verify path)
            if result.get("verified") and result.get("_is_new_subscription"):
                _rc_cid = result.get("_customer_id")
                try:
                    await maybe_credit_referrer_for_yearly_subscription(
                        referee_id=_rc_cid,
                        subscription_id=result.get("subscription_id"),
                        plan_name=result.get("_product_id", ""),
                    )
                    pel.side_effect_success(command_id=f"rc_verify_{_rc_cid}",
                                            side_effect="referral", client_id=_rc_cid)
                except Exception as exc:
                    pel.side_effect_failed(command_id=f"rc_verify_{_rc_cid}",
                                           side_effect="referral", error_detail=str(exc),
                                           client_id=_rc_cid)
                    logger.warning(
                        "REVENUECAT_VERIFY_REFERRAL_CREDIT_FAILED",
                        extra={
                            "client_id": _rc_cid,
                            "subscription_id": result.get("subscription_id"),
                            "error": repr(exc),
                        },
                    )

            # Remove internal flags before returning
            result.pop("_is_new_subscription", None)
            result.pop("_customer_id", None)
            result.pop("_product_id", None)
            return result

    async def _poll_local_confirmation(self, customer_id: str) -> Optional[Dict[str, Any]]:
        delay = max(0.2, self.config.revenuecat_verify_poll_base_delay_ms / 1000)
        max_delay = self.config.revenuecat_verify_poll_max_delay_ms / 1000
        deadline = time.monotonic() + max(1, self.config.revenuecat_verify_total_timeout_seconds)
        max_attempts = max(1, self.config.revenuecat_verify_poll_attempts)
        attempt = 0

        while time.monotonic() < deadline and attempt < max_attempts:
            attempt += 1

            capture_marker = await self._capture_marker_snapshot(customer_id)
            if capture_marker:
                logger.info(
                    "REVENUECAT_VERIFY_CAPTURE_CACHE_HIT",
                    extra={
                        "customer_id": mask_value(customer_id),
                        "attempt": attempt,
                        "event_type": capture_marker.get("event_type"),
                    },
                )
                local_snapshot = await self._fetch_local_verification_payload(customer_id)
                if local_snapshot:
                    return local_snapshot

            local_snapshot = await self._fetch_local_verification_payload(customer_id)
            if local_snapshot:
                logger.info(
                    "REVENUECAT_VERIFY_LOCAL_SUB_FOUND",
                    extra={
                        "customer_id": mask_value(customer_id),
                        "attempt": attempt,
                    },
                )
                return local_snapshot

            await asyncio.sleep(delay)
            delay = min(delay * 1.5, max_delay)

        return None

    async def _fetch_local_verification_payload(self, customer_id: str) -> Optional[Dict[str, Any]]:
        with self._session_scope() as session:
            def _op() -> Optional[Dict[str, Any]]:
                return self._local_verification_payload(session, customer_id)

            return await run_sync_db_operation(_op)

    def _local_verification_payload(self, session: Session, customer_id: str) -> Optional[Dict[str, Any]]:
        now = legacy_rc.now_ist()
        subscription = (
            session.query(Subscription)
            .filter(
                Subscription.customer_id == customer_id,
                Subscription.provider == Provider.google_play.value,
                Subscription.status.in_(
                    [
                        SubscriptionStatus.active.value,
                        SubscriptionStatus.renewed.value,
                    ]
                ),
            )
            .order_by(Subscription.created_at.desc())
            .first()
        )

        if not subscription:
            return None

        active_until = subscription.active_until
        if active_until:
            if active_until.tzinfo is None:
                active_until = active_until.replace(tzinfo=legacy_rc.timezone.utc)
            if active_until < now:
                return None

        payment: Optional[Payment] = None
        if subscription.latest_txn_id:
            payment = (
                session.query(Payment)
                .filter(
                    Payment.provider == Provider.google_play.value,
                    Payment.provider_payment_id == subscription.latest_txn_id,
                )
                .order_by(Payment.created_at.desc())
                .first()
            )

        order: Optional[Order] = None
        if payment and payment.order_id:
            order = session.query(Order).filter(Order.id == payment.order_id).first()
        if not order:
            order = (
                session.query(Order)
                .filter(
                    Order.customer_id == customer_id,
                    Order.provider == Provider.google_play.value,
                )
                .order_by(Order.created_at.desc())
                .first()
            )

        message = "Subscription verified via webhook"
        return {
            "verified": True,
            "captured": True,
            "subscription_active": True,
            "has_premium": True,
            "message": message,
            "subscription_id": subscription.id,
            "payment_id": payment.provider_payment_id if payment else subscription.latest_txn_id,
            "order_id": order.id if order else None,
            "active_from": subscription.active_from.isoformat() if subscription.active_from else None,
            "active_until": subscription.active_until.isoformat() if subscription.active_until else None,
            "auto_renew": bool(subscription.auto_renew),
        }

    async def _handle_webhook(self, signature: str, raw_body: str) -> Dict[str, Any]:
        if signature != self.settings.revenuecat_webhook_secret:
            pel.webhook_signature_invalid(command_id="rc_webhook")
            log_security_event(
                "INVALID_WEBHOOK_SIGNATURE",
                {"signature_prefix": mask_value(signature, left=8, right=0), "source": "revenuecat"},
            )
            raise ValueError("invalid_webhook_signature")

        try:
            payload = json.loads(raw_body)
        except json.JSONDecodeError as exc:
            log_security_event("WEBHOOK_INVALID_JSON", {"error": str(exc), "source": "revenuecat"})
            raise

        event = payload.get("event", {})

        with self._session_scope() as session:
            def _op() -> Dict[str, Any]:
                sync_service = SubscriptionSyncService(session)
                try:
                    customer_id = event.get("app_user_id")
                    event_type = event.get("type")

                    purchased_at_ms = event.get("purchased_at_ms")
                    expiration_at_ms = event.get("expiration_at_ms")

                    if purchased_at_ms:
                        datetime.fromtimestamp(purchased_at_ms / 1000, tz=legacy_rc.IST)
                    else:
                        logger.warning(
                            "revenuecat.webhook.missing_purchased_at | customer_id=%s event_id=%s",
                            customer_id,
                            event.get("id"),
                        )

                    if expiration_at_ms:
                        datetime.fromtimestamp(expiration_at_ms / 1000, tz=legacy_rc.IST)
                    else:
                        logger.warning(
                            "revenuecat.webhook.missing_expiration | customer_id=%s event_id=%s",
                            customer_id,
                            event.get("id"),
                        )

                    if not customer_id:
                        log_security_event(
                            "WEBHOOK_MISSING_CUSTOMER_ID",
                            {
                                "event_type": event_type,
                                "event_id": event.get("id"),
                                "has_product_id": bool(event.get("product_id")),
                            },
                        )
                        return {"status": "ignored", "reason": "missing_customer_id"}

                    event_id = generate_event_id(event)
                    should_process, existing_log = sync_service.check_idempotency(
                        event_id, event_type, allow_retry_on_failure=True
                    )

                    if not should_process:
                        return {
                            "status": "already_processed",
                            "event_id": event_id,
                            "processing_status": existing_log.status if existing_log else None,
                        }

                    if existing_log:
                        processing_log = existing_log
                    else:
                        processing_log = WebhookProcessingLog(
                            id=sync_service.generate_id("whl"),
                            event_id=event_id,
                            event_type=event_type,
                            customer_id=customer_id,
                            status="processing",
                            started_at=now_ist(),
                            raw_event_data=json.dumps(event),
                            is_recovery_event=event.get("_is_recovery", False),
                        )
                        session.add(processing_log)
                        session.flush()

                    if event_type == "INITIAL_PURCHASE":
                        result = sync_service.process_initial_purchase(event, processing_log)

                        # Grant nutrition eligibility for Fittbot subscription (Diamond/Platinum plans)
                        if result.get("success"):
                            try:
                                product_id = event.get("product_id", "")
                                plan_name = product_id.lower() if product_id else ""
                                duration_months = 0

                                if "half-yearly" in plan_name or "half_yearly" in plan_name:
                                    duration_months = 6
                                elif "12" in plan_name or "twelve" in plan_name or "yearly" in plan_name:
                                    duration_months = 12
                                elif "6" in plan_name or "six" in plan_name:
                                    duration_months = 6

                                if duration_months >= 6:
                                    sessions = calculate_nutrition_sessions_from_fittbot_plan(plan_name, duration_months)
                                    if sessions > 0:
                                        subscription_id = result.get("subscription_id", event.get("store_transaction_id"))
                                        grant_nutrition_eligibility_sync(
                                            db=session,
                                            client_id=int(customer_id),
                                            source_type="fittbot_subscription",
                                            source_id=subscription_id,
                                            plan_name=product_id,
                                            duration_months=duration_months,
                                            gym_id=None,
                                        )
                                        pel.side_effect_success(command_id=f"rc_webhook_{customer_id}",
                                                                side_effect="nutrition", client_id=customer_id)
                                    else:
                                        pel.side_effect_skipped(command_id=f"rc_webhook_{customer_id}",
                                                                side_effect="nutrition", reason="sessions_zero",
                                                                client_id=customer_id)
                                else:
                                    pel.side_effect_skipped(command_id=f"rc_webhook_{customer_id}",
                                                            side_effect="nutrition", reason="duration_lt_6m",
                                                            client_id=customer_id)
                            except Exception as nutr_exc:
                                pel.side_effect_failed(command_id=f"rc_webhook_{customer_id}",
                                                       side_effect="nutrition", error_detail=str(nutr_exc),
                                                       client_id=customer_id)
                                logger.warning(f"[NUTRITION_ELIGIBILITY_ERROR] Webhook: {nutr_exc}")

                    elif event_type == "RENEWAL":
                        result = sync_service.process_renewal(event, processing_log)
                    elif event_type == "CANCELLATION":
                        result = sync_service.process_cancellation(event, processing_log)
                    elif event_type == "EXPIRATION":
                        result = sync_service.process_expiration(event, processing_log)
                    elif event_type == "BILLING_ISSUES":
                        result = handle_billing_issues(event, session, processing_log)
                    else:
                        processing_log.status = "ignored"
                        processing_log.completed_at = now_ist()
                        processing_log.result_summary = f"Unhandled event type: {event_type}"
                        session.commit()
                        return {"status": "ignored", "reason": f"unhandled_event_type: {event_type}"}

                    if result.get("success"):
                        session.commit()
                        return {
                            "status": "processed",
                            "event_type": event_type,
                            "event_id": event_id,
                            "result": result,
                            "_customer_id": customer_id,  # For reward entry
                        }

                    log_security_event(
                        "WEBHOOK_PROCESSING_FAILED",
                        {
                            "event_type": event_type,
                            "event_id": event_id,
                            "customer_id": mask_value(customer_id),
                            "error": result.get("error"),
                        },
                    )
                    session.rollback()
                    raise ValueError(f"Processing failed: {result.get('error')}")

                except Exception:
                    session.rollback()
                    raise

            result = await run_sync_db_operation(_op)

        if result.get("status") == "processed":
            await self._record_capture_marker(event)

            # Add reward program entries for INITIAL_PURCHASE (best-effort)
            if result.get("event_type") == "INITIAL_PURCHASE":
                _wh_cid = result.get("_customer_id")
                try:
                    if _wh_cid:
                        SessionLocal = create_celery_async_sessionmaker()
                        async with SessionLocal() as async_db:
                            subscription_id = result.get("result", {}).get("subscription_id")
                            reward_ok, entries_added, reward_msg = await add_subscription_entry(
                                async_db,
                                client_id=int(_wh_cid),
                                source_id=subscription_id,
                            )
                            await async_db.commit()
                            pel.side_effect_success(command_id=f"rc_webhook_{_wh_cid}",
                                                    side_effect="reward", client_id=_wh_cid)
                            logger.info(
                                "REVENUECAT_WEBHOOK_SUBSCRIPTION_REWARD_ENTRY",
                                extra={
                                    "client_id": _wh_cid,
                                    "success": reward_ok,
                                    "entries_added": entries_added,
                                    "reward_msg": reward_msg,
                                    "subscription_id": subscription_id,
                                },
                            )
                except Exception as reward_exc:
                    pel.side_effect_failed(command_id=f"rc_webhook_{_wh_cid}",
                                           side_effect="reward", error_detail=str(reward_exc),
                                           client_id=_wh_cid)
                    logger.warning(
                        "REVENUECAT_WEBHOOK_REWARD_ENTRY_FAILED",
                        extra={
                            "client_id": _wh_cid,
                            "error": repr(reward_exc),
                        },
                    )

            # Credit referrer if referee purchased a 1-year plan (webhook path)
            if result.get("event_type") == "INITIAL_PURCHASE":
                _wh_cid = result.get("_customer_id")
                try:
                    await maybe_credit_referrer_for_yearly_subscription(
                        referee_id=_wh_cid,
                        subscription_id=result.get("result", {}).get("subscription_id"),
                        plan_name=event.get("product_id", ""),
                    )
                    pel.side_effect_success(command_id=f"rc_webhook_{_wh_cid}",
                                            side_effect="referral", client_id=_wh_cid)
                except Exception as exc:
                    pel.side_effect_failed(command_id=f"rc_webhook_{_wh_cid}",
                                           side_effect="referral", error_detail=str(exc),
                                           client_id=_wh_cid)
                    logger.warning(
                        "REVENUECAT_WEBHOOK_REFERRAL_CREDIT_FAILED",
                        extra={
                            "client_id": _wh_cid,
                            "subscription_id": result.get("result", {}).get("subscription_id"),
                            "error": repr(exc),
                        },
                    )

        # Remove internal flag before returning
        result.pop("_customer_id", None)
        return result

    @contextmanager
    def _session_scope(self):
        with self.payment_db.get_session() as session:
            yield session

    def _capture_cache_key(self, customer_id: str) -> str:
        prefix = self.config.revenuecat_redis_prefix or self.config.redis_prefix
        return f"{prefix}:capture:{customer_id}"

    async def _capture_marker_snapshot(self, customer_id: str) -> Optional[Dict[str, Any]]:
        if not self.redis or not customer_id:
            return None
        key = self._capture_cache_key(customer_id)
        raw = await asyncio.to_thread(self.redis.get, key)
        if not raw:
            return None
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            return None

    async def _record_capture_marker(self, event: Dict[str, Any]) -> None:
        if not self.redis:
            return
        customer_id = event.get("app_user_id")
        if not customer_id:
            return
        marker = {
            "event_type": event.get("type"),
            "product_id": event.get("product_id"),
            "store_transaction_id": event.get("store_transaction_id"),
            "purchased_at_ms": event.get("purchased_at_ms"),
            "expiration_at_ms": event.get("expiration_at_ms"),
        }
        key = self._capture_cache_key(customer_id)
        try:
            await asyncio.to_thread(
                self.redis.set,
                key,
                json.dumps(marker),
                ex=self.config.revenuecat_capture_cache_ttl_seconds,
            )
            logger.info(
                "REVENUECAT_CAPTURE_CACHE_SET",
                extra={
                    "customer_id": mask_value(customer_id),
                    "event_type": marker.get("event_type"),
                    "ttl_seconds": self.config.revenuecat_capture_cache_ttl_seconds,
                },
            )
        except Exception:
            logger.exception(
                "Failed to set RevenueCat capture cache",
                extra={"customer_id": mask_value(customer_id)},
            )
