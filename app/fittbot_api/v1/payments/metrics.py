"""
Payment Metrics Module for Fittbot

Provides comprehensive metrics for payment operations:
- Checkout/Verify/Webhook latency by provider and payment type
- Success/failure rates with detailed labels
- Amount tracking for revenue analysis
- Queue depth and processing metrics
"""

import time
import functools
import logging
from typing import Optional, Callable, Any
from contextlib import asynccontextmanager

from prometheus_client import Counter, Histogram, Gauge

logger = logging.getLogger("payments.metrics")


# =============================================================================
# PAYMENT OPERATION METRICS
# =============================================================================

# Payment operation latency (checkout, verify, webhook)
PAYMENT_OPERATION_LATENCY = Histogram(
    "payment_operation_duration_seconds",
    "Payment operation latency in seconds",
    ["provider", "operation", "payment_type", "status"],
    buckets=[0.1, 0.25, 0.5, 1.0, 2.0, 3.0, 5.0, 8.0, 10.0, 15.0, 30.0],
)

# Payment operation counts
PAYMENT_OPERATION_TOTAL = Counter(
    "payment_operations_total",
    "Total payment operations by provider, type, and status",
    ["provider", "operation", "payment_type", "status"],
)

# Payment amount processed (in paise/minor units)
PAYMENT_AMOUNT_PROCESSED = Counter(
    "payment_amount_processed_minor",
    "Total payment amount processed in minor units (paise)",
    ["provider", "payment_type", "status", "currency"],
)

# Active payment operations (in-progress)
PAYMENT_OPERATIONS_IN_PROGRESS = Gauge(
    "payment_operations_in_progress",
    "Number of payment operations currently being processed",
    ["provider", "operation", "payment_type"],
    multiprocess_mode="livesum",
)

# Payment verification outcomes
PAYMENT_VERIFICATION_OUTCOME = Counter(
    "payment_verification_outcome_total",
    "Payment verification outcomes",
    ["provider", "payment_type", "outcome"],  # outcome: captured, authorized, failed, pending, invalid_signature
)

# Webhook processing metrics
WEBHOOK_EVENTS_TOTAL = Counter(
    "payment_webhook_events_total",
    "Total webhook events received by provider and event type",
    ["provider", "event_type", "status"],  # status: processed, failed, duplicate
)

WEBHOOK_PROCESSING_LATENCY = Histogram(
    "payment_webhook_processing_seconds",
    "Webhook processing latency",
    ["provider", "event_type"],
    buckets=[0.05, 0.1, 0.25, 0.5, 1.0, 2.0, 5.0],
)

# Provider API call metrics
PROVIDER_API_LATENCY = Histogram(
    "payment_provider_api_duration_seconds",
    "External payment provider API call latency",
    ["provider", "endpoint", "status"],
    buckets=[0.1, 0.25, 0.5, 1.0, 2.0, 3.0, 5.0, 8.0],
)

PROVIDER_API_CALLS_TOTAL = Counter(
    "payment_provider_api_calls_total",
    "Total payment provider API calls",
    ["provider", "endpoint", "status"],
)

# Subscription metrics
SUBSCRIPTION_STATUS_CHANGES = Counter(
    "subscription_status_changes_total",
    "Subscription status changes",
    ["provider", "from_status", "to_status"],
)

ACTIVE_SUBSCRIPTIONS = Gauge(
    "active_subscriptions_count",
    "Current number of active subscriptions",
    ["provider", "plan_type"],
    multiprocess_mode="livemax",
)

# Payment task queue metrics
PAYMENT_QUEUE_SIZE = Gauge(
    "payment_queue_size",
    "Number of payment tasks in queue",
    ["queue_name"],  # payments, ai, celery
    multiprocess_mode="livemax",
)

PAYMENT_TASK_RETRY_COUNT = Counter(
    "payment_task_retries_total",
    "Payment task retry count",
    ["provider", "operation", "payment_type"],
)


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def record_payment_operation(
    provider: str,
    operation: str,
    payment_type: str,
    status: str,
    duration: float,
    amount_minor: int = 0,
    currency: str = "INR",
):
    """Record a completed payment operation with all metrics."""
    # Record latency
    PAYMENT_OPERATION_LATENCY.labels(
        provider=provider,
        operation=operation,
        payment_type=payment_type,
        status=status,
    ).observe(duration)

    # Record count
    PAYMENT_OPERATION_TOTAL.labels(
        provider=provider,
        operation=operation,
        payment_type=payment_type,
        status=status,
    ).inc()

    # Record amount if successful and amount provided
    if status == "success" and amount_minor > 0:
        PAYMENT_AMOUNT_PROCESSED.labels(
            provider=provider,
            payment_type=payment_type,
            status=status,
            currency=currency,
        ).inc(amount_minor)


def record_verification_outcome(
    provider: str,
    payment_type: str,
    outcome: str,
):
    """Record payment verification outcome."""
    PAYMENT_VERIFICATION_OUTCOME.labels(
        provider=provider,
        payment_type=payment_type,
        outcome=outcome,
    ).inc()


def record_webhook_event(
    provider: str,
    event_type: str,
    status: str,
    duration: float = 0,
):
    """Record webhook event processing."""
    WEBHOOK_EVENTS_TOTAL.labels(
        provider=provider,
        event_type=event_type,
        status=status,
    ).inc()

    if duration > 0:
        WEBHOOK_PROCESSING_LATENCY.labels(
            provider=provider,
            event_type=event_type,
        ).observe(duration)


def record_provider_api_call(
    provider: str,
    endpoint: str,
    status: str,
    duration: float,
):
    """Record external payment provider API call."""
    PROVIDER_API_LATENCY.labels(
        provider=provider,
        endpoint=endpoint,
        status=status,
    ).observe(duration)

    PROVIDER_API_CALLS_TOTAL.labels(
        provider=provider,
        endpoint=endpoint,
        status=status,
    ).inc()


def record_subscription_status_change(
    provider: str,
    from_status: str,
    to_status: str,
):
    """Record subscription status change."""
    SUBSCRIPTION_STATUS_CHANGES.labels(
        provider=provider,
        from_status=from_status,
        to_status=to_status,
    ).inc()


@asynccontextmanager
async def track_payment_operation(
    provider: str,
    operation: str,
    payment_type: str,
):
    """
    Async context manager to track payment operation metrics.

    Usage:
        async with track_payment_operation("razorpay", "checkout", "subscription"):
            result = await process_checkout(...)
    """
    start = time.perf_counter()
    status = "success"

    # Increment in-progress counter
    PAYMENT_OPERATIONS_IN_PROGRESS.labels(
        provider=provider,
        operation=operation,
        payment_type=payment_type,
    ).inc()

    try:
        yield
    except Exception as e:
        status = "failed"
        logger.warning(
            "Payment operation failed",
            extra={
                "provider": provider,
                "operation": operation,
                "payment_type": payment_type,
                "error": str(e),
            }
        )
        raise
    finally:
        duration = time.perf_counter() - start

        # Decrement in-progress counter
        PAYMENT_OPERATIONS_IN_PROGRESS.labels(
            provider=provider,
            operation=operation,
            payment_type=payment_type,
        ).dec()

        # Record metrics
        record_payment_operation(
            provider=provider,
            operation=operation,
            payment_type=payment_type,
            status=status,
            duration=duration,
        )


def payment_task_metrics(provider: str, operation: str, payment_type: str):
    """
    Decorator to track payment task metrics.

    Usage:
        @payment_task_metrics("razorpay", "checkout", "subscription")
        async def _checkout_worker(command_id: str):
            ...
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            start = time.perf_counter()
            status = "success"

            PAYMENT_OPERATIONS_IN_PROGRESS.labels(
                provider=provider,
                operation=operation,
                payment_type=payment_type,
            ).inc()

            try:
                result = await func(*args, **kwargs)
                return result
            except Exception as e:
                status = "failed"
                logger.error(
                    "Payment task failed",
                    extra={
                        "provider": provider,
                        "operation": operation,
                        "payment_type": payment_type,
                        "error": str(e),
                    }
                )
                raise
            finally:
                duration = time.perf_counter() - start

                PAYMENT_OPERATIONS_IN_PROGRESS.labels(
                    provider=provider,
                    operation=operation,
                    payment_type=payment_type,
                ).dec()

                record_payment_operation(
                    provider=provider,
                    operation=operation,
                    payment_type=payment_type,
                    status=status,
                    duration=duration,
                )

        return wrapper
    return decorator


def sync_payment_task_metrics(provider: str, operation: str, payment_type: str):
    """
    Decorator to track sync payment task metrics (for Celery tasks).

    Usage:
        @sync_payment_task_metrics("razorpay", "checkout", "subscription")
        def process_checkout_task(command_id: str):
            ...
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            start = time.perf_counter()
            status = "success"

            PAYMENT_OPERATIONS_IN_PROGRESS.labels(
                provider=provider,
                operation=operation,
                payment_type=payment_type,
            ).inc()

            try:
                result = func(*args, **kwargs)
                return result
            except Exception as e:
                status = "failed"
                PAYMENT_TASK_RETRY_COUNT.labels(
                    provider=provider,
                    operation=operation,
                    payment_type=payment_type,
                ).inc()
                raise
            finally:
                duration = time.perf_counter() - start

                PAYMENT_OPERATIONS_IN_PROGRESS.labels(
                    provider=provider,
                    operation=operation,
                    payment_type=payment_type,
                ).dec()

                record_payment_operation(
                    provider=provider,
                    operation=operation,
                    payment_type=payment_type,
                    status=status,
                    duration=duration,
                )

        return wrapper
    return decorator
