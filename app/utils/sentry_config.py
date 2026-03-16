"""
Sentry Error Tracking Integration for Fittbot API

Provides:
- Automatic exception capture
- Performance monitoring (transactions)
- User context tracking
- Release tracking
- Environment separation

Dashboard: https://sentry.io
"""

import os
import logging
from typing import Optional, Dict, Any

logger = logging.getLogger("app.sentry")

_sentry_initialized = False


def init_sentry(
    dsn: Optional[str] = None,
    environment: str = "development",
    release: Optional[str] = None,
    traces_sample_rate: float = 0.1,
    profiles_sample_rate: float = 0.1,
    debug: bool = False,
) -> bool:
    """
    Initialize Sentry for error tracking and performance monitoring.

    Args:
        dsn: Sentry DSN (from environment SENTRY_DSN if not provided)
        environment: Environment name (development, staging, production)
        release: Release version (from APP_VERSION env if not provided)
        traces_sample_rate: Percentage of transactions to sample (0.0 to 1.0)
        profiles_sample_rate: Percentage of profiled transactions (0.0 to 1.0)
        debug: Enable Sentry debug mode

    Returns:
        True if Sentry was successfully initialized
    """
    global _sentry_initialized

    dsn = dsn or os.getenv("SENTRY_DSN")
    if not dsn:
        logger.info("Sentry DSN not configured, error tracking disabled")
        return False

    try:
        import sentry_sdk
        from sentry_sdk.integrations.fastapi import FastApiIntegration
        from sentry_sdk.integrations.starlette import StarletteIntegration
        from sentry_sdk.integrations.sqlalchemy import SqlalchemyIntegration
        from sentry_sdk.integrations.redis import RedisIntegration
        from sentry_sdk.integrations.celery import CeleryIntegration
        from sentry_sdk.integrations.httpx import HttpxIntegration
        from sentry_sdk.integrations.logging import LoggingIntegration

        # Configure logging integration
        logging_integration = LoggingIntegration(
            level=logging.INFO,  # Capture info and above as breadcrumbs
            event_level=logging.ERROR,  # Send errors as events
        )

        sentry_sdk.init(
            dsn=dsn,
            environment=environment,
            release=release or os.getenv("APP_VERSION", "1.0.0"),
            traces_sample_rate=traces_sample_rate,
            profiles_sample_rate=profiles_sample_rate,
            debug=debug,
            send_default_pii=False,  # Don't send PII by default
            attach_stacktrace=True,
            include_local_variables=environment != "production",

            # Integrations
            integrations=[
                FastApiIntegration(transaction_style="endpoint"),
                StarletteIntegration(transaction_style="endpoint"),
                SqlalchemyIntegration(),
                RedisIntegration(),
                CeleryIntegration(),
                HttpxIntegration(),
                logging_integration,
            ],

            # Filter out noisy/expected errors
            before_send=_before_send,
            before_send_transaction=_before_send_transaction,

            # Ignore certain paths
            traces_sampler=_traces_sampler,
        )

        _sentry_initialized = True
        logger.info(f"Sentry initialized for {environment} environment")
        return True

    except ImportError:
        logger.warning("Sentry SDK not installed. Install with: pip install sentry-sdk[fastapi]")
        return False
    except Exception as e:
        logger.error(f"Failed to initialize Sentry: {e}")
        return False


def _before_send(event: Dict[str, Any], hint: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Filter events before sending to Sentry.
    Return None to drop the event.
    """
    # Get exception info
    if "exc_info" in hint:
        exc_type, exc_value, _ = hint["exc_info"]

        # Ignore expected/handled exceptions
        ignored_exceptions = [
            "HTTPException",
            "RequestValidationError",
            "StarletteHTTPException",
        ]

        if exc_type.__name__ in ignored_exceptions:
            # Only send 5xx errors
            if hasattr(exc_value, "status_code") and exc_value.status_code < 500:
                return None

        # Ignore rate limit errors
        if exc_type.__name__ == "HTTPException":
            if hasattr(exc_value, "status_code") and exc_value.status_code == 429:
                return None

    # Remove sensitive data
    if "request" in event:
        request = event["request"]
        if "headers" in request:
            # Redact sensitive headers
            sensitive_headers = ["authorization", "cookie", "x-api-key"]
            for header in sensitive_headers:
                if header in request["headers"]:
                    request["headers"][header] = "[REDACTED]"

    return event


def _before_send_transaction(event: Dict[str, Any], hint: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Filter transactions before sending."""
    # Don't send transactions for health checks
    transaction_name = event.get("transaction", "")
    if transaction_name in ["/health", "/health/ready", "/metrics", "/"]:
        return None

    return event


def _traces_sampler(sampling_context: Dict[str, Any]) -> float:
    """
    Dynamic sampling based on transaction type.
    Returns sample rate (0.0 to 1.0).
    """
    transaction_name = sampling_context.get("transaction_context", {}).get("name", "")

    # Don't trace health checks
    if transaction_name in ["/health", "/health/ready", "/metrics"]:
        return 0.0

    # Higher sampling for payments
    if "/payment" in transaction_name or "/razorpay" in transaction_name:
        return 0.5

    # Higher sampling for AI endpoints
    if "/chatbot" in transaction_name or "/food_scanner" in transaction_name:
        return 0.3

    # Lower sampling for feed (high volume)
    if "/feed" in transaction_name:
        return 0.05

    # Default sampling
    return 0.1


def capture_exception(exception: Exception, extra: Optional[Dict[str, Any]] = None):
    """Manually capture an exception."""
    if not _sentry_initialized:
        return

    try:
        import sentry_sdk
        with sentry_sdk.push_scope() as scope:
            if extra:
                for key, value in extra.items():
                    scope.set_extra(key, value)
            sentry_sdk.capture_exception(exception)
    except Exception as e:
        logger.debug(f"Failed to capture exception in Sentry: {e}")


def capture_message(message: str, level: str = "info", extra: Optional[Dict[str, Any]] = None):
    """Manually capture a message."""
    if not _sentry_initialized:
        return

    try:
        import sentry_sdk
        with sentry_sdk.push_scope() as scope:
            if extra:
                for key, value in extra.items():
                    scope.set_extra(key, value)
            sentry_sdk.capture_message(message, level=level)
    except Exception as e:
        logger.debug(f"Failed to capture message in Sentry: {e}")


def set_user(user_id: str, email: Optional[str] = None, username: Optional[str] = None, role: Optional[str] = None):
    """Set user context for error tracking."""
    if not _sentry_initialized:
        return

    try:
        import sentry_sdk
        sentry_sdk.set_user({
            "id": user_id,
            "email": email,
            "username": username,
            "role": role,
        })
    except Exception as e:
        logger.debug(f"Failed to set user in Sentry: {e}")


def set_tag(key: str, value: str):
    """Set a tag for the current scope."""
    if not _sentry_initialized:
        return

    try:
        import sentry_sdk
        sentry_sdk.set_tag(key, value)
    except Exception:
        pass


def set_context(name: str, data: Dict[str, Any]):
    """Set additional context for errors."""
    if not _sentry_initialized:
        return

    try:
        import sentry_sdk
        sentry_sdk.set_context(name, data)
    except Exception:
        pass


def add_breadcrumb(
    message: str,
    category: str = "default",
    level: str = "info",
    data: Optional[Dict[str, Any]] = None,
):
    """Add a breadcrumb for debugging."""
    if not _sentry_initialized:
        return

    try:
        import sentry_sdk
        sentry_sdk.add_breadcrumb(
            message=message,
            category=category,
            level=level,
            data=data or {},
        )
    except Exception:
        pass


def start_transaction(name: str, op: str = "http.server"):
    """Start a new transaction for performance monitoring."""
    if not _sentry_initialized:
        return None

    try:
        import sentry_sdk
        return sentry_sdk.start_transaction(name=name, op=op)
    except Exception:
        return None


def is_sentry_enabled() -> bool:
    """Check if Sentry is initialized."""
    return _sentry_initialized
