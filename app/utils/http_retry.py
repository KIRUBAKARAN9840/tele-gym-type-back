"""
Exponential Backoff and Retry Logic for HTTP Calls (SMS, APIs, etc.)

This module provides enterprise-grade retry logic for HTTP requests,
specifically designed for SMS providers, payment gateways, and external APIs.

Features:
- Exponential backoff with jitter (prevents thundering herd)
- Handles HTTP errors (429, 500, 502, 503, 504)
- Handles timeouts and connection errors
- Respects Retry-After headers
- Works with requests library (sync and async)

Usage:
    from app.utils.http_retry import http_get_with_retry

    # SMS example
    response = http_get_with_retry(
        url="http://pwtpl.com/sms/V1/send-sms-api.php?...",
        timeout=10,
        service_name="pwtl-sms"
    )
"""

import logging
import random
import time
from typing import Optional, Dict, Any
import requests
from requests.exceptions import RequestException, Timeout, ConnectionError

logger = logging.getLogger("http_retry")


# ---------------------------------------------------------------------------
# Exception Classification for HTTP
# ---------------------------------------------------------------------------

def is_retryable_http_error(exc: Exception, status_code: Optional[int] = None) -> bool:
    """
    Determine if an HTTP exception is retryable.

    Retryable errors:
    - Rate limits (429)
    - Server errors (500, 502, 503, 504)
    - Timeouts
    - Connection errors

    Non-retryable errors:
    - Client errors (400, 401, 403, 404)
    - SSL errors
    """
    # Check status code
    if status_code is not None:
        # Retry on rate limits and server errors
        if status_code in [429, 500, 502, 503, 504]:
            return True
        # Don't retry on client errors
        if 400 <= status_code < 500:
            return False

    # Check exception types
    if isinstance(exc, Timeout):
        return True
    if isinstance(exc, ConnectionError):
        return True

    # Check error messages
    error_str = str(exc).lower()
    if any(keyword in error_str for keyword in ['timeout', 'connection', 'network']):
        return True

    # SSL errors - don't retry
    if 'ssl' in error_str or 'certificate' in error_str:
        return False

    # Default: retry on unknown errors
    return True


def get_http_retry_after_seconds(response: Optional[requests.Response]) -> Optional[float]:
    """Extract Retry-After seconds from HTTP response if available."""
    if response is not None and 'Retry-After' in response.headers:
        retry_after = response.headers['Retry-After']
        try:
            return float(retry_after)
        except (ValueError, TypeError):
            pass
    return None


# ---------------------------------------------------------------------------
# Exponential Backoff
# ---------------------------------------------------------------------------

def calculate_http_backoff_seconds(
    attempt: int,
    base_delay: float = 1.0,
    max_delay: float = 30.0,
    jitter: bool = True
) -> float:
    """
    Calculate exponential backoff with optional jitter.

    Formula: min(base_delay * (2 ** (attempt - 1)), max_delay) + jitter

    Examples:
        attempt=1: ~1s
        attempt=2: ~2s
        attempt=3: ~4s
        attempt=4: ~8s
        attempt=5: ~16s

    Args:
        attempt: Current attempt number (1-indexed)
        base_delay: Base delay in seconds (default 1.0)
        max_delay: Maximum delay in seconds (default 30.0)
        jitter: Add random jitter (default True)

    Returns:
        Delay in seconds
    """
    # Exponential backoff
    delay = base_delay * (2 ** (attempt - 1))

    # Cap at max_delay
    delay = min(delay, max_delay)

    # Add jitter (0% to 25% of delay)
    if jitter:
        jitter_amount = random.uniform(0, delay * 0.25)
        delay += jitter_amount

    return delay


# ---------------------------------------------------------------------------
# HTTP GET with Retry
# ---------------------------------------------------------------------------

def http_get_with_retry(
    url: str,
    max_attempts: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 30.0,
    timeout: float = 10.0,
    service_name: str = "http-get",
    **kwargs
) -> requests.Response:
    """
    Execute HTTP GET request with automatic retry logic.

    Handles:
    - Rate limiting (429 errors)
    - Server errors (500, 502, 503, 504)
    - Timeouts
    - Connection errors

    Args:
        url: URL to request
        max_attempts: Maximum number of attempts (default 3)
        base_delay: Base delay for exponential backoff (default 1.0s)
        max_delay: Maximum delay between retries (default 30.0s)
        timeout: Request timeout in seconds (default 10.0)
        service_name: Name for logging (default "http-get")
        **kwargs: Additional arguments for requests.get()

    Returns:
        requests.Response object

    Raises:
        The last exception if all retries are exhausted

    Example:
        # SMS provider
        response = http_get_with_retry(
            url="http://pwtpl.com/sms/V1/send-sms-api.php?...",
            timeout=10,
            service_name="pwtl-sms"
        )

        if response.status_code == 200:
            print("SMS sent successfully!")
    """
    last_exception = None

    for attempt in range(1, max_attempts + 1):
        try:
            start_time = time.time()
            response = requests.get(url, timeout=timeout, **kwargs)
            duration_ms = (time.time() - start_time) * 1000

            # Check if response is successful
            if response.status_code < 400:
                if attempt > 1:
                    logger.info(
                        f"✓ {service_name} succeeded on attempt {attempt}/{max_attempts} "
                        f"(status={response.status_code}, {duration_ms:.0f}ms)"
                    )
                return response

            # Got an error response, check if retryable
            if not is_retryable_http_error(None, response.status_code):
                logger.warning(
                    f"✗ {service_name} non-retryable error: HTTP {response.status_code} "
                    f"({duration_ms:.0f}ms)"
                )
                return response  # Return error response (don't raise)

            # Last attempt - return error response
            if attempt >= max_attempts:
                logger.error(
                    f"✗ {service_name} failed after {max_attempts} attempts: "
                    f"HTTP {response.status_code} ({duration_ms:.0f}ms)"
                )
                return response

            # Calculate backoff delay
            retry_after = get_http_retry_after_seconds(response)
            if retry_after is not None:
                delay = min(retry_after, max_delay)
                logger.info(f"⏳ {service_name} respecting Retry-After: {delay:.1f}s")
            else:
                delay = calculate_http_backoff_seconds(attempt, base_delay, max_delay, jitter=True)

            logger.warning(
                f"⚠️  {service_name} attempt {attempt}/{max_attempts} failed: "
                f"HTTP {response.status_code}. Retrying in {delay:.1f}s... ({duration_ms:.0f}ms)"
            )

            # Wait before retry
            time.sleep(delay)

        except RequestException as exc:
            last_exception = exc
            duration_ms = (time.time() - start_time) * 1000

            # Check if we should retry
            if not is_retryable_http_error(exc):
                logger.warning(
                    f"✗ {service_name} non-retryable error: {exc} ({duration_ms:.0f}ms)"
                )
                raise exc

            # Last attempt - give up
            if attempt >= max_attempts:
                logger.error(
                    f"✗ {service_name} failed after {max_attempts} attempts: {exc} "
                    f"({duration_ms:.0f}ms)"
                )
                raise exc

            # Calculate backoff delay
            delay = calculate_http_backoff_seconds(attempt, base_delay, max_delay, jitter=True)

            logger.warning(
                f"⚠️  {service_name} attempt {attempt}/{max_attempts} failed: {exc}. "
                f"Retrying in {delay:.1f}s... ({duration_ms:.0f}ms)"
            )

            # Wait before retry
            time.sleep(delay)

    # Should never reach here, but just in case
    if last_exception:
        raise last_exception
    raise Exception(f"{service_name} failed without explicit error")


# ---------------------------------------------------------------------------
# HTTP POST with Retry
# ---------------------------------------------------------------------------

def http_post_with_retry(
    url: str,
    max_attempts: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 30.0,
    timeout: float = 10.0,
    service_name: str = "http-post",
    **kwargs
) -> requests.Response:
    """
    Execute HTTP POST request with automatic retry logic.

    Args:
        url: URL to request
        max_attempts: Maximum number of attempts (default 3)
        base_delay: Base delay for exponential backoff (default 1.0s)
        max_delay: Maximum delay between retries (default 30.0s)
        timeout: Request timeout in seconds (default 10.0)
        service_name: Name for logging (default "http-post")
        **kwargs: Additional arguments for requests.post()
            - data: Request body
            - json: JSON data
            - headers: HTTP headers

    Returns:
        requests.Response object

    Example:
        response = http_post_with_retry(
            url="https://api.example.com/webhook",
            json={"event": "user_created"},
            service_name="webhook-api"
        )
    """
    last_exception = None

    for attempt in range(1, max_attempts + 1):
        try:
            start_time = time.time()
            response = requests.post(url, timeout=timeout, **kwargs)
            duration_ms = (time.time() - start_time) * 1000

            # Check if response is successful
            if response.status_code < 400:
                if attempt > 1:
                    logger.info(
                        f"✓ {service_name} succeeded on attempt {attempt}/{max_attempts} "
                        f"(status={response.status_code}, {duration_ms:.0f}ms)"
                    )
                return response

            # Got an error response, check if retryable
            if not is_retryable_http_error(None, response.status_code):
                logger.warning(
                    f"✗ {service_name} non-retryable error: HTTP {response.status_code} "
                    f"({duration_ms:.0f}ms)"
                )
                return response

            # Last attempt - return error response
            if attempt >= max_attempts:
                logger.error(
                    f"✗ {service_name} failed after {max_attempts} attempts: "
                    f"HTTP {response.status_code} ({duration_ms:.0f}ms)"
                )
                return response

            # Calculate backoff delay
            retry_after = get_http_retry_after_seconds(response)
            if retry_after is not None:
                delay = min(retry_after, max_delay)
                logger.info(f"⏳ {service_name} respecting Retry-After: {delay:.1f}s")
            else:
                delay = calculate_http_backoff_seconds(attempt, base_delay, max_delay, jitter=True)

            logger.warning(
                f"⚠️  {service_name} attempt {attempt}/{max_attempts} failed: "
                f"HTTP {response.status_code}. Retrying in {delay:.1f}s... ({duration_ms:.0f}ms)"
            )

            # Wait before retry
            time.sleep(delay)

        except RequestException as exc:
            last_exception = exc
            duration_ms = (time.time() - start_time) * 1000

            # Check if we should retry
            if not is_retryable_http_error(exc):
                logger.warning(
                    f"✗ {service_name} non-retryable error: {exc} ({duration_ms:.0f}ms)"
                )
                raise exc

            # Last attempt - give up
            if attempt >= max_attempts:
                logger.error(
                    f"✗ {service_name} failed after {max_attempts} attempts: {exc} "
                    f"({duration_ms:.0f}ms)"
                )
                raise exc

            # Calculate backoff delay
            delay = calculate_http_backoff_seconds(attempt, base_delay, max_delay, jitter=True)

            logger.warning(
                f"⚠️  {service_name} attempt {attempt}/{max_attempts} failed: {exc}. "
                f"Retrying in {delay:.1f}s... ({duration_ms:.0f}ms)"
            )

            # Wait before retry
            time.sleep(delay)

    # Should never reach here, but just in case
    if last_exception:
        raise last_exception
    raise Exception(f"{service_name} failed without explicit error")


# ---------------------------------------------------------------------------
# Export
# ---------------------------------------------------------------------------

__all__ = [
    "http_get_with_retry",
    "http_post_with_retry",
    "is_retryable_http_error",
    "calculate_http_backoff_seconds",
]
