"""
Razorpay API Client with Circuit Breaker and Retry Logic

Features:
- Circuit breaker to prevent cascading failures during Razorpay outages
- Exponential backoff retry for transient errors
- Proper logging for payment monitoring
"""

import json
import logging
import time
from typing import Any, Dict, Optional

import asyncio
import requests
from requests.exceptions import RequestException, Timeout, ConnectionError

from ..config.settings import get_payment_settings
from .crypto import auth_header
from app.utils.circuit_breaker import CircuitBreaker, CircuitOpenError

logger = logging.getLogger("payments.razorpay")

RZP_API = "https://api.razorpay.com/v1"

# Circuit breaker for Razorpay API - payments are critical so lower threshold
razorpay_circuit_breaker = CircuitBreaker(
    name="razorpay",
    failure_threshold=3,      # Open after 3 consecutive failures (payments are critical)
    recovery_timeout=30.0,    # Wait 30s before testing recovery
    half_open_max_calls=2,    # Allow 2 test calls
    success_threshold=2,      # Need 2 successes to close
)


def _is_retryable_error(status_code: Optional[int] = None, exc: Optional[Exception] = None) -> bool:
    """Check if error is retryable."""
    if status_code and status_code in [429, 500, 502, 503, 504]:
        return True
    if isinstance(exc, (Timeout, ConnectionError)):
        return True
    return False


def _request_with_retry(
    method: str,
    url: str,
    headers: Dict[str, str],
    data: Optional[str] = None,
    json_data: Optional[Dict] = None,
    timeout: int = 15,
    max_retries: int = 3,
    base_delay: float = 1.0,
) -> requests.Response:
    """
    Make HTTP request with circuit breaker and retry logic.
    """
    # Check circuit breaker first
    try:
        razorpay_circuit_breaker._before_call()
    except CircuitOpenError as e:
        logger.warning(f"Razorpay circuit OPEN: {e.remaining_seconds:.1f}s until retry")
        raise RequestException(f"Razorpay service unavailable. Retry in {e.remaining_seconds:.1f}s")

    last_error = None

    for attempt in range(1, max_retries + 1):
        try:
            logger.debug(f"Razorpay API {method} {url} attempt {attempt}/{max_retries}")
            start_time = time.time()

            if method == "GET":
                r = requests.get(url, headers=headers, timeout=timeout)
            else:
                r = requests.post(url, headers=headers, data=data, json=json_data, timeout=timeout)

            duration_ms = (time.time() - start_time) * 1000

            # Success
            if r.status_code < 400:
                razorpay_circuit_breaker.record_success()
                if attempt > 1:
                    logger.info(f"Razorpay succeeded on attempt {attempt} ({duration_ms:.0f}ms)")
                return r

            # Retryable error
            if _is_retryable_error(status_code=r.status_code):
                last_error = f"HTTP {r.status_code}"
                logger.warning(f"Razorpay {r.status_code} error, attempt {attempt}/{max_retries}")
                if attempt < max_retries:
                    delay = base_delay * (2 ** (attempt - 1))
                    time.sleep(delay)
                    continue

            # Non-retryable client error - don't count as circuit failure
            r.raise_for_status()

        except (Timeout, ConnectionError) as e:
            last_error = str(e)
            logger.warning(f"Razorpay connection error, attempt {attempt}/{max_retries}: {e}")
            if attempt < max_retries:
                delay = base_delay * (2 ** (attempt - 1))
                time.sleep(delay)
                continue

        except requests.exceptions.HTTPError as e:
            # 4xx errors - don't retry, don't trip circuit
            raise

        except Exception as e:
            last_error = str(e)
            logger.error(f"Razorpay unexpected error: {e}")
            break

    # All retries exhausted
    razorpay_circuit_breaker.record_failure(Exception(last_error or "Unknown error"))
    logger.error(f"Razorpay API failed after {max_retries} attempts: {last_error}")
    raise RequestException(f"Razorpay failed after {max_retries} attempts: {last_error}")


def get_plan(plan_id: str) -> Dict[str, Any]:
    """Get plan details with circuit breaker and retry."""
    settings = get_payment_settings()
    r = _request_with_retry(
        method="GET",
        url=f"{RZP_API}/plans/{plan_id}",
        headers=auth_header(settings.razorpay_key_id, settings.razorpay_key_secret),
        timeout=15,
    )
    r.raise_for_status()
    return r.json()


def create_subscription(
    plan_id: str,
    notes: Dict[str, Any],
    *,
    total_count: Optional[int] = None,
    customer_notify: int = 1,
) -> Dict[str, Any]:
    """Create subscription with circuit breaker and retry."""
    settings = get_payment_settings()
    payload = {
        "plan_id": plan_id,
        "customer_notify": customer_notify,
        "notes": notes,
    }
    if total_count is None:
        total_count = 12
    payload["total_count"] = total_count

    r = _request_with_retry(
        method="POST",
        url=f"{RZP_API}/subscriptions",
        headers={
            "Content-Type": "application/json",
            **auth_header(settings.razorpay_key_id, settings.razorpay_key_secret),
        },
        data=json.dumps(payload),
        timeout=20,
    )
    r.raise_for_status()
    return r.json()


def get_subscription(sub_id: str) -> Dict[str, Any]:
    """Get subscription details with circuit breaker and retry."""
    settings = get_payment_settings()
    r = _request_with_retry(
        method="GET",
        url=f"{RZP_API}/subscriptions/{sub_id}",
        headers=auth_header(settings.razorpay_key_id, settings.razorpay_key_secret),
        timeout=15,
    )
    r.raise_for_status()
    return r.json()


def get_payment(payment_id: str) -> Dict[str, Any]:
    """Fetch a payment by id with circuit breaker and retry."""
    settings = get_payment_settings()
    r = _request_with_retry(
        method="GET",
        url=f"{RZP_API}/payments/{payment_id}",
        headers=auth_header(settings.razorpay_key_id, settings.razorpay_key_secret),
        timeout=15,
    )
    r.raise_for_status()
    return r.json()


async def get_plan_async(plan_id: str) -> Dict[str, Any]:
    return await asyncio.to_thread(get_plan, plan_id)


async def create_subscription_async(
    plan_id: str,
    notes: Dict[str, Any],
    *,
    total_count: Optional[int] = None,
    customer_notify: int = 1,
) -> Dict[str, Any]:
    return await asyncio.to_thread(
        create_subscription,
        plan_id,
        notes,
        total_count=total_count,
        customer_notify=customer_notify,
    )


async def get_subscription_async(sub_id: str) -> Dict[str, Any]:
    return await asyncio.to_thread(get_subscription, sub_id)


async def get_payment_async(payment_id: str) -> Dict[str, Any]:
    return await asyncio.to_thread(get_payment, payment_id)


async def cancel_subscription_async(
    provider_subscription_id: str,
    *,
    cancel_at_cycle_end: bool = True,
) -> requests.Response:
    """Cancel subscription with circuit breaker and retry."""
    def _cancel():
        settings = get_payment_settings()
        headers = {
            "Content-Type": "application/json",
            **auth_header(settings.razorpay_key_id, settings.razorpay_key_secret),
        }
        payload = {"cancel_at_cycle_end": 1 if cancel_at_cycle_end else 0}
        r = _request_with_retry(
            method="POST",
            url=f"{RZP_API}/subscriptions/{provider_subscription_id}/cancel",
            headers=headers,
            json_data=payload,
            timeout=20,
        )
        r.raise_for_status()
        return r

    return await asyncio.to_thread(_cancel)


def get_razorpay_circuit_status() -> Dict[str, Any]:
    """Get Razorpay circuit breaker status for monitoring."""
    return razorpay_circuit_breaker.get_status()
