import asyncio
import json
import logging
import time
from contextlib import contextmanager
from typing import Dict, Optional

from ...config.database import PaymentDatabase
from ...config.settings import get_payment_settings
from ..config import HighConcurrencyConfig
from ..stores.command_store import CommandStore
from ...Fittbot_Subscriptions.razorpay import process_razorpay_webhook_payload
from ...models.orders import Order
from redis import Redis
from .payment_event_logger import PaymentEventLogger


logger = logging.getLogger("payments.razorpay.v2.webhook")
pel = PaymentEventLogger("razorpay", "webhook")


class WebhookProcessor:
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

    async def process(self, command_id: str, store: CommandStore) -> None:
        record = await store.mark_processing(command_id)
        payload = record.payload
        _start = time.perf_counter()
        pel.webhook_received(command_id=command_id, event_type=payload.get("event"))
        try:
            await self._persist(payload)
        except Exception as exc:
            pel.webhook_failed(command_id=command_id, error_code=type(exc).__name__,
                               duration_ms=int((time.perf_counter() - _start) * 1000),
                               event_type=payload.get("event"), error_detail=str(exc))
            logger.exception("Webhook command failed", extra={"command_id": command_id})
            await store.mark_failed(command_id, str(exc))
            return

        pel.webhook_processed(command_id=command_id,
                              duration_ms=int((time.perf_counter() - _start) * 1000),
                              event_type=payload.get("event"))
        await store.mark_completed(
            command_id,
            {"event": payload.get("event"), "webhook_id": payload.get("webhook_id")},
        )

    async def _persist(self, body: Dict) -> None:
        raw = body.get("raw_body")
        signature = body.get("signature")
        if raw is None or signature is None:
            pel.webhook_signature_invalid(command_id="razorpay_webhook")
            raise ValueError("webhook_signature_missing")
        raw_bytes = raw if isinstance(raw, bytes) else raw.encode("utf-8")

        with self._session_scope() as session:
            await process_razorpay_webhook_payload(raw_bytes, signature, session)
        await self._record_capture_marker(body)

        # For payment.captured events, trigger fulfillment (dailypass, session, or gym membership)
        if body.get("event") == "payment.captured":
            await self._try_dailypass_fulfillment(body)
            await self._try_session_fulfillment(body)
            await self._try_gym_membership_fulfillment(body)

    @contextmanager
    def _session_scope(self):
        with self.payment_db.get_session() as session:
            yield session

    async def _try_dailypass_fulfillment(self, body: Dict) -> None:
        """If the captured payment belongs to a dailypass order, fulfill it now."""
        try:
            pay_entity = body.get("payload", {}).get("payment", {}).get("entity", {})
            razorpay_order_id = pay_entity.get("order_id")
            payment_id = pay_entity.get("id")
            if not razorpay_order_id or not payment_id:
                return

            # Check if this order is a dailypass order by looking at the notes
            notes = pay_entity.get("notes", {})
            flow = notes.get("flow", "")
            is_dailypass = flow in ("dailypass_only", "unified_dailypass_local_sub",
                                     "dailypass_upgrade", "dailypass_edit_topup")

            # If notes don't have flow, check DB
            if not is_dailypass:
                with self._session_scope() as session:
                    order = session.query(Order).filter(
                        Order.provider_order_id == razorpay_order_id
                    ).first()
                    if order and order.order_metadata:
                        # Try direct flow field first (upgrade/topup store it at top level)
                        order_flow = order.order_metadata.get("flow", "")
                        if not order_flow:
                            order_flow = order.order_metadata.get("order_info", {}).get("flow", "")
                        if "dailypass" in order_flow:
                            is_dailypass = True
                            flow = order_flow  # Use DB flow for routing

            if not is_dailypass:
                return

            logger.info(
                "DAILYPASS_WEBHOOK_FULFILLMENT_TRIGGERED",
                extra={
                    "razorpay_order_id": razorpay_order_id,
                    "payment_id": f"****{payment_id[-4:]}" if len(payment_id) > 4 else payment_id,
                    "flow": flow,
                },
            )

            payment_data = {
                "amount": pay_entity.get("amount"),
                "currency": pay_entity.get("currency"),
                "method": pay_entity.get("method"),
                "status": "captured",
            }

            from .dailypass_processor import DailyPassProcessor
            processor = DailyPassProcessor(
                config=self.config, payment_db=self.payment_db, redis=self.redis
            )

            # Route to correct fulfillment method based on flow
            if flow == "dailypass_upgrade":
                await processor.fulfill_upgrade_from_webhook(razorpay_order_id, payment_id, payment_data)
            elif flow == "dailypass_edit_topup":
                await processor.fulfill_edit_topup_from_webhook(razorpay_order_id, payment_id, payment_data)
            else:
                await processor.fulfill_from_webhook(razorpay_order_id, payment_id, payment_data)

        except Exception:
            # Webhook fulfillment is best-effort — don't fail the webhook processing
            logger.exception(
                "DAILYPASS_WEBHOOK_FULFILLMENT_ERROR",
                extra={"razorpay_order_id": body.get("payload", {}).get("payment", {}).get("entity", {}).get("order_id")},
            )

    async def _try_session_fulfillment(self, body: Dict) -> None:
        """If the captured payment belongs to a session order, fulfill it now."""
        try:
            pay_entity = body.get("payload", {}).get("payment", {}).get("entity", {})
            razorpay_order_id = pay_entity.get("order_id")
            payment_id = pay_entity.get("id")
            if not razorpay_order_id or not payment_id:
                return

            # Check if this is a session order by looking at notes
            notes = pay_entity.get("notes", {})
            order_type = notes.get("type", "")
            is_session = order_type == "session_booking"

            # If notes don't have type, check DB for order_metadata
            if not is_session:
                with self._session_scope() as session:
                    order = session.query(Order).filter(
                        Order.provider_order_id == razorpay_order_id
                    ).first()
                    if order and order.order_metadata:
                        is_session = order.order_metadata.get("type") == "session_booking"

            if not is_session:
                return

            logger.info(
                "SESSION_WEBHOOK_FULFILLMENT_TRIGGERED",
                extra={
                    "razorpay_order_id": razorpay_order_id,
                    "payment_id": f"****{payment_id[-4:]}" if len(payment_id) > 4 else payment_id,
                },
            )

            payment_data = {
                "amount": pay_entity.get("amount"),
                "currency": pay_entity.get("currency"),
                "method": pay_entity.get("method"),
                "status": "captured",
            }

            from .session_processor import SessionProcessor
            processor = SessionProcessor(
                config=self.config, payment_db=self.payment_db, redis=self.redis
            )
            await processor.fulfill_from_webhook(razorpay_order_id, payment_id, payment_data)

        except Exception:
            # Webhook fulfillment is best-effort — don't fail the webhook processing
            logger.exception(
                "SESSION_WEBHOOK_FULFILLMENT_ERROR",
                extra={"razorpay_order_id": body.get("payload", {}).get("payment", {}).get("entity", {}).get("order_id")},
            )

    async def _try_gym_membership_fulfillment(self, body: Dict) -> None:
        """If the captured payment belongs to a gym membership order, fulfill it now."""
        try:
            pay_entity = body.get("payload", {}).get("payment", {}).get("entity", {})
            razorpay_order_id = pay_entity.get("order_id")
            payment_id = pay_entity.get("id")
            if not razorpay_order_id or not payment_id:
                return

            # Check if this is a gym membership order by looking at notes
            notes = pay_entity.get("notes", {})
            flow = notes.get("flow", "")
            is_gym_membership = "gym_membership" in flow or "personal_training" in flow

            # If notes don't have flow, check DB
            if not is_gym_membership:
                with self._session_scope() as session:
                    order = session.query(Order).filter(
                        Order.provider_order_id == razorpay_order_id
                    ).first()
                    if order and order.order_metadata:
                        order_flow = order.order_metadata.get("order_info", {}).get("flow", "")
                        if "gym_membership" in order_flow or "personal_training" in order_flow:
                            is_gym_membership = True

            if not is_gym_membership:
                return

            logger.info(
                "GM_CENTRAL_WEBHOOK_FULFILLMENT_TRIGGERED",
                extra={
                    "razorpay_order_id": razorpay_order_id,
                    "payment_id": f"****{payment_id[-4:]}" if len(payment_id) > 4 else payment_id,
                    "flow": flow,
                },
            )

            payment_data = {
                "amount": pay_entity.get("amount"),
                "currency": pay_entity.get("currency"),
                "method": pay_entity.get("method"),
                "offer_id": pay_entity.get("offer_id"),
                "status": "captured",
            }

            from .gym_membership_processor import GymMembershipProcessor
            processor = GymMembershipProcessor(
                config=self.config, payment_db=self.payment_db, redis=self.redis
            )
            await processor.fulfill_from_webhook(razorpay_order_id, payment_id, payment_data)

        except Exception:
            # Webhook fulfillment is best-effort — don't fail the webhook processing
            logger.exception(
                "GM_CENTRAL_WEBHOOK_FULFILLMENT_ERROR",
                extra={"razorpay_order_id": body.get("payload", {}).get("payment", {}).get("entity", {}).get("order_id")},
            )

    async def _record_capture_marker(self, body: Dict) -> None:
        if not self.redis:
            return
        if body.get("event") != "payment.captured":
            return
        pay_entity = body.get("payload", {}).get("payment", {}).get("entity", {})
        payment_id = pay_entity.get("id")
        if not payment_id:
            return
        marker = {
            "subscription_id": pay_entity.get("subscription_id"),
            "amount": pay_entity.get("amount"),
            "currency": pay_entity.get("currency"),
            "method": pay_entity.get("method"),
            "customer_id": pay_entity.get("notes", {}).get("customer_id"),
            "order_id": pay_entity.get("order_id"),
            "captured_at": pay_entity.get("created_at") or int(time.time()),
        }
        key = f"{self.config.redis_prefix}:capture:{payment_id}"
        try:
            await asyncio.to_thread(
                self.redis.set,
                key,
                json.dumps(marker),
                ex=self.config.verify_capture_cache_ttl_seconds,
            )
            masked_payment = (
                f"****{payment_id[-4:]}" if isinstance(payment_id, str) and len(payment_id) > 4 else payment_id
            )
            logger.info(
                "RAZORPAY_CAPTURE_CACHE_SET",
                extra={
                    "payment_id": masked_payment,
                    "subscription_id": marker.get("subscription_id"),
                    "ttl_seconds": self.config.verify_capture_cache_ttl_seconds,
                },
            )
        except Exception:
            logger.exception("Failed to set capture cache marker", extra={"payment_id": payment_id})
