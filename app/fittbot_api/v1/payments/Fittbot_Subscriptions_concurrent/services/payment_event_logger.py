"""
Standardized payment event logger for CloudWatch observability.

Emits structured JSON log events with consistent field names across all
payment types so CloudWatch Log Insights queries work uniformly.

Usage:
    from .payment_event_logger import PaymentEventLogger

    pel = PaymentEventLogger("razorpay", "subscription")
    pel.checkout_started(command_id=cmd_id, client_id=uid, plan_sku="diamond_12m")
    pel.checkout_completed(command_id=cmd_id, client_id=uid, duration_ms=1234)
    pel.checkout_failed(command_id=cmd_id, client_id=uid, error_code="provider_timeout", error_detail="...")
"""

import logging
import time
from typing import Any, Optional

from .payment_notification_email import fire_payment_notification_email

logger = logging.getLogger("payments.events")


class PaymentEventLogger:
    """Emits structured payment events for CloudWatch Log Insights."""

    def __init__(self, provider: str, payment_type: str):
        self.provider = provider
        self.payment_type = payment_type

    def _emit(self, event: str, level: str = "info", **fields: Any) -> None:
        extra = {
            "event": event,
            "provider": self.provider,
            "payment_type": self.payment_type,
        }
        # Add all caller-supplied fields, skip None values
        for k, v in fields.items():
            if v is not None:
                extra[k] = v

        log_fn = getattr(logger, level, logger.info)
        log_fn(event, extra=extra)

    # ---- Checkout ----

    def checkout_started(self, command_id: str, **kw: Any) -> None:
        self._emit("payment.checkout.started", operation="checkout", command_id=command_id, **kw)

    def checkout_completed(self, command_id: str, **kw: Any) -> None:
        self._emit("payment.checkout.completed", operation="checkout", outcome="success", command_id=command_id, **kw)

    def checkout_failed(self, command_id: str, error_code: str, **kw: Any) -> None:
        self._emit("payment.checkout.failed", level="error", operation="checkout", outcome="failed",
                    command_id=command_id, error_code=error_code, **kw)

    # ---- Verify ----

    def verify_started(self, command_id: str, **kw: Any) -> None:
        self._emit("payment.verify.started", operation="verify", command_id=command_id, **kw)

    def verify_completed(self, command_id: str, verify_path: str, **kw: Any) -> None:
        self._emit("payment.verify.completed", operation="verify", outcome="success",
                    command_id=command_id, verify_path=verify_path, **kw)

    def verify_failed(self, command_id: str, error_code: str, **kw: Any) -> None:
        self._emit("payment.verify.failed", level="error", operation="verify", outcome="failed",
                    command_id=command_id, error_code=error_code, **kw)

    def verify_pending(self, command_id: str, **kw: Any) -> None:
        self._emit("payment.verify.pending", operation="verify", outcome="pending", command_id=command_id, **kw)

    def verify_signature_invalid(self, command_id: str, **kw: Any) -> None:
        self._emit("payment.verify.signature_invalid", level="warning", operation="verify",
                    outcome="failed", error_code="invalid_signature", command_id=command_id, **kw)

    # ---- Webhook ----

    def webhook_received(self, command_id: str, **kw: Any) -> None:
        self._emit("payment.webhook.received", operation="webhook", command_id=command_id, **kw)

    def webhook_processed(self, command_id: str, **kw: Any) -> None:
        self._emit("payment.webhook.processed", operation="webhook", outcome="success", command_id=command_id, **kw)

    def webhook_failed(self, command_id: str, error_code: str, **kw: Any) -> None:
        self._emit("payment.webhook.failed", level="error", operation="webhook", outcome="failed",
                    command_id=command_id, error_code=error_code, **kw)

    def webhook_signature_invalid(self, command_id: str, **kw: Any) -> None:
        self._emit("payment.webhook.signature_invalid", level="warning", operation="webhook",
                    outcome="failed", error_code="invalid_signature", command_id=command_id, **kw)

    # ---- Provider API calls ----

    def provider_call_started(self, command_id: str, provider_endpoint: str, **kw: Any) -> None:
        self._emit("payment.provider.call_started", command_id=command_id,
                    provider_endpoint=provider_endpoint, **kw)

    def provider_call_completed(self, command_id: str, provider_endpoint: str, duration_ms: int, **kw: Any) -> None:
        self._emit("payment.provider.call_completed", command_id=command_id,
                    provider_endpoint=provider_endpoint, duration_ms=duration_ms, **kw)

    def provider_call_failed(self, command_id: str, provider_endpoint: str, error_code: str, **kw: Any) -> None:
        self._emit("payment.provider.call_failed", level="error", command_id=command_id,
                    provider_endpoint=provider_endpoint, error_code=error_code, **kw)

    # ---- Side effects ----

    def side_effect_success(self, command_id: str, side_effect: str, **kw: Any) -> None:
        self._emit("payment.side_effect.success", command_id=command_id,
                    side_effect=side_effect, side_effect_status="success", **kw)

    def side_effect_failed(self, command_id: str, side_effect: str, error_detail: str, **kw: Any) -> None:
        self._emit("payment.side_effect.failed", level="warning", command_id=command_id,
                    side_effect=side_effect, side_effect_status="failed", error_detail=error_detail, **kw)

    def side_effect_skipped(self, command_id: str, side_effect: str, reason: str, **kw: Any) -> None:
        self._emit("payment.side_effect.skipped", command_id=command_id,
                    side_effect=side_effect, side_effect_status="skipped", skip_reason=reason, **kw)

    # ---- Funnel tracking ----

    def order_created(self, command_id: str, **kw: Any) -> None:
        self._emit("payment.order.created", operation="checkout", command_id=command_id, **kw)

    def payment_captured(self, command_id: str, **kw: Any) -> None:
        self._emit("payment.captured", operation="verify", outcome="captured", command_id=command_id, **kw)
        fire_payment_notification_email(
            provider=self.provider,
            payment_type=self.payment_type,
            client_id=str(kw.get("client_id", "")),
            command_id=command_id,
            razorpay_payment_id=kw.get("razorpay_payment_id"),
            razorpay_subscription_id=kw.get("razorpay_subscription_id"),
            plan_sku=kw.get("plan_sku"),
            amount=str(kw.get("amount", "")) if kw.get("amount") else None,
        )

    def payment_authorized(self, command_id: str, **kw: Any) -> None:
        self._emit("payment.authorized", operation="verify", outcome="authorized", command_id=command_id, **kw)
