"""
Exponential Backoff and Retry Logic for AI API Calls (OpenAI, Gemini, etc.)

This module provides enterprise-grade retry logic similar to the payment system's
EnterpriseHTTPClient, specifically designed for AI API calls.

Features:
- Exponential backoff with jitter (prevents thundering herd)
- Respects Retry-After headers from APIs
- Handles rate limits (429), timeouts, and transient errors
- Circuit breaker pattern for fail-fast behavior
- Async/await support for high concurrency
- Works with OpenAI, Gemini, and other AI APIs

Usage:
    from app.utils.ai_retry import with_ai_retry, ai_call_with_retry

    # Option 1: Decorator
    @with_ai_retry(max_attempts=3, service_name="openai-gpt4")
    async def my_ai_function():
        return await openai_client.chat.completions.create(...)

    # Option 2: Direct call wrapper
    result = await ai_call_with_retry(
        lambda: openai_client.chat.completions.create(...),
        max_attempts=3,
        service_name="openai-gpt4"
    )
"""

import asyncio
import logging
import random
import time
import threading
from typing import Callable, TypeVar, Optional, Any
from functools import wraps
from collections import defaultdict
from datetime import datetime

logger = logging.getLogger("ai_retry")

T = TypeVar('T')


# ---------------------------------------------------------------------------
# Circuit Breaker for AI Services
# ---------------------------------------------------------------------------

class AICircuitBreaker:
    """
    Circuit breaker pattern for AI API calls.

    States:
    - CLOSED: Normal operation (failures < threshold)
    - OPEN: Too many failures, reject requests immediately
    - HALF_OPEN: Testing if service recovered after timeout

    Used by Netflix, Amazon, Stripe for API resilience.
    """

    def __init__(
        self,
        failure_threshold: int = 10,
        recovery_timeout: int = 60,
        success_threshold: int = 2
    ):
        """
        Args:
            failure_threshold: Number of consecutive failures before opening circuit
            recovery_timeout: Seconds to wait before trying again (HALF_OPEN state)
            success_threshold: Number of successes in HALF_OPEN to close circuit
        """
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.success_threshold = success_threshold

        # Per-service tracking
        self.failure_count = defaultdict(int)
        self.last_failure_time = defaultdict(lambda: 0.0)
        self.state = defaultdict(lambda: "CLOSED")  # CLOSED, OPEN, HALF_OPEN
        self.success_count = defaultdict(int)
        # Use threading.Lock instead of asyncio.Lock for gevent compatibility
        self._lock = threading.Lock()

    def can_attempt(self, service_name: str) -> bool:
        """Check if we can attempt a request to this service."""
        with self._lock:
            state = self.state[service_name]

            if state == "CLOSED":
                return True

            if state == "OPEN":
                # Check if enough time has passed to try again
                elapsed = time.time() - self.last_failure_time[service_name]
                if elapsed >= self.recovery_timeout:
                    logger.info(f"Circuit breaker for {service_name} entering HALF_OPEN state")
                    self.state[service_name] = "HALF_OPEN"
                    self.success_count[service_name] = 0
                    return True
                else:
                    logger.warning(
                        f"Circuit breaker OPEN for {service_name} - "
                        f"rejecting request ({int(self.recovery_timeout - elapsed)}s until retry)"
                    )
                    return False

            # HALF_OPEN state
            return True

    def record_success(self, service_name: str) -> None:
        """Record a successful call."""
        with self._lock:
            state = self.state[service_name]

            if state == "HALF_OPEN":
                self.success_count[service_name] += 1
                if self.success_count[service_name] >= self.success_threshold:
                    logger.info(f"Circuit breaker for {service_name} CLOSED after recovery")
                    self.state[service_name] = "CLOSED"
                    self.failure_count[service_name] = 0
            elif state == "CLOSED":
                # Reset failure count on success
                self.failure_count[service_name] = 0

    def record_failure(self, service_name: str) -> None:
        """Record a failed call."""
        with self._lock:
            self.failure_count[service_name] += 1
            self.last_failure_time[service_name] = time.time()

            if self.state[service_name] == "HALF_OPEN":
                # Failed during recovery, go back to OPEN
                logger.error(f"Circuit breaker for {service_name} OPEN again (recovery failed)")
                self.state[service_name] = "OPEN"
            elif self.failure_count[service_name] >= self.failure_threshold:
                logger.error(
                    f"Circuit breaker OPEN for {service_name} - "
                    f"{self.failure_count[service_name]} consecutive failures"
                )
                self.state[service_name] = "OPEN"


# Global circuit breaker instance
circuit_breaker = AICircuitBreaker(failure_threshold=10, recovery_timeout=60)


# ---------------------------------------------------------------------------
# Exception Classification
# ---------------------------------------------------------------------------

def is_retryable_error(exc: Exception) -> bool:
    """
    Determine if an exception is retryable.

    Retryable errors:
    - Rate limits (429)
    - Timeouts
    - Connection errors
    - Server errors (500, 502, 503, 504)
    - Overloaded errors

    Non-retryable errors:
    - Authentication (401)
    - Invalid requests (400)
    - Not found (404)
    - Other 4xx client errors
    """
    error_str = str(exc).lower()
    error_type = type(exc).__name__

    # OpenAI specific errors
    if "ratelimiterror" in error_type.lower():
        return True
    if "apierror" in error_type.lower() and any(
        code in error_str for code in ["429", "500", "502", "503", "504", "timeout"]
    ):
        return True

    # HTTP errors
    if hasattr(exc, "status_code"):
        status = exc.status_code
        # Retry on rate limits and server errors
        if status in [429, 500, 502, 503, 504]:
            return True
        # Don't retry on client errors
        if 400 <= status < 500:
            return False

    # Timeout errors
    if "timeout" in error_type.lower() or "timeout" in error_str:
        return True

    # Connection errors
    if "connection" in error_type.lower() or "connection" in error_str:
        return True

    # Overloaded/capacity errors
    if any(keyword in error_str for keyword in ["overloaded", "capacity", "too many requests"]):
        return True

    # Default: retry on unknown errors (conservative approach)
    return True


def get_retry_after_seconds(exc: Exception) -> Optional[float]:
    """Extract Retry-After seconds from exception if available."""
    # Check for Retry-After header in response
    if hasattr(exc, "response") and exc.response is not None:
        retry_after = exc.response.headers.get("Retry-After")
        if retry_after:
            try:
                return float(retry_after)
            except (ValueError, TypeError):
                pass

    # Check in error message
    error_str = str(exc)
    if "retry after" in error_str.lower():
        import re
        match = re.search(r"retry after (\d+(?:\.\d+)?)\s*(?:second|sec|s)?", error_str, re.I)
        if match:
            try:
                return float(match.group(1))
            except (ValueError, TypeError):
                pass

    return None


# ---------------------------------------------------------------------------
# Exponential Backoff with Jitter
# ---------------------------------------------------------------------------

def calculate_backoff_seconds(
    attempt: int,
    base_delay: float = 1.0,
    max_delay: float = 30.0,
    jitter: bool = True
) -> float:
    """
    Calculate exponential backoff with optional jitter.

    Formula: min(base_delay * (2 ** (attempt - 1)), max_delay) + jitter

    Jitter prevents "thundering herd" problem where many clients retry simultaneously.

    Examples:
        attempt=1: ~1s
        attempt=2: ~2s
        attempt=3: ~4s
        attempt=4: ~8s
        attempt=5: ~16s
        attempt=6: ~30s (capped)

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
# Main Retry Logic
# ---------------------------------------------------------------------------

async def ai_call_with_retry(
    coro_factory: Callable[[], Any],
    max_attempts: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 30.0,
    service_name: str = "ai-api",
    use_circuit_breaker: bool = True,
) -> T:
    """
    Execute an AI API call with exponential backoff retry logic.

    This function handles:
    - Rate limiting (429 errors)
    - Timeouts
    - Transient server errors (500, 502, 503, 504)
    - Connection errors
    - Circuit breaker pattern
    - Retry-After header respect
    - Exponential backoff with jitter

    Args:
        coro_factory: A callable that returns the coroutine to execute
        max_attempts: Maximum number of attempts (default 3)
        base_delay: Base delay for exponential backoff (default 1.0s)
        max_delay: Maximum delay between retries (default 30.0s)
        service_name: Name of the service for circuit breaker tracking
        use_circuit_breaker: Enable circuit breaker (default True)

    Returns:
        The result of the successful API call

    Raises:
        The last exception if all retries are exhausted

    Example:
        result = await ai_call_with_retry(
            lambda: openai_client.chat.completions.create(
                model="gpt-4",
                messages=[{"role": "user", "content": "Hello"}]
            ),
            max_attempts=5,
            service_name="openai-gpt4"
        )
    """
    last_exception = None

    for attempt in range(1, max_attempts + 1):
        # Check circuit breaker (now sync - works with gevent)
        if use_circuit_breaker:
            if not circuit_breaker.can_attempt(service_name):
                raise Exception(f"Circuit breaker OPEN for {service_name} - service unavailable")

        try:
            # Execute the API call
            start_time = time.time()
            result = await coro_factory()
            duration_ms = (time.time() - start_time) * 1000

            # Success! Record it and return
            if use_circuit_breaker:
                circuit_breaker.record_success(service_name)

            if attempt > 1:
                logger.info(
                    f"✓ {service_name} succeeded on attempt {attempt}/{max_attempts} "
                    f"({duration_ms:.0f}ms)"
                )

            return result

        except Exception as exc:
            last_exception = exc
            duration_ms = (time.time() - start_time) * 1000

            # Check if we should retry
            if not is_retryable_error(exc):
                logger.warning(
                    f"✗ {service_name} non-retryable error: {exc} ({duration_ms:.0f}ms)"
                )
                if use_circuit_breaker:
                    circuit_breaker.record_failure(service_name)
                raise exc

            # Last attempt - give up
            if attempt >= max_attempts:
                logger.error(
                    f"✗ {service_name} failed after {max_attempts} attempts: {exc} "
                    f"({duration_ms:.0f}ms)"
                )
                if use_circuit_breaker:
                    circuit_breaker.record_failure(service_name)
                raise exc

            # Calculate backoff delay
            retry_after = get_retry_after_seconds(exc)
            if retry_after is not None:
                delay = min(retry_after, max_delay)
                logger.info(f"⏳ {service_name} respecting Retry-After: {delay:.1f}s")
            else:
                delay = calculate_backoff_seconds(attempt, base_delay, max_delay, jitter=True)

            logger.warning(
                f"⚠️  {service_name} attempt {attempt}/{max_attempts} failed: {exc}. "
                f"Retrying in {delay:.1f}s... ({duration_ms:.0f}ms)"
            )

            # Record failure (but don't open circuit yet - we're retrying)
            if use_circuit_breaker and attempt == max_attempts:
                circuit_breaker.record_failure(service_name)

            # Wait before retry
            await asyncio.sleep(delay)

    # Should never reach here, but just in case
    if last_exception:
        raise last_exception
    raise Exception(f"{service_name} failed without explicit error")


# ---------------------------------------------------------------------------
# Decorator Version
# ---------------------------------------------------------------------------

def with_ai_retry(
    max_attempts: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 30.0,
    service_name: Optional[str] = None,
    use_circuit_breaker: bool = True,
):
    """
    Decorator to add exponential backoff retry logic to async functions.

    Args:
        max_attempts: Maximum number of attempts (default 3)
        base_delay: Base delay for exponential backoff (default 1.0s)
        max_delay: Maximum delay between retries (default 30.0s)
        service_name: Name of the service for circuit breaker (default: function name)
        use_circuit_breaker: Enable circuit breaker (default True)

    Example:
        @with_ai_retry(max_attempts=5, service_name="openai-gpt4")
        async def call_openai():
            return await openai_client.chat.completions.create(
                model="gpt-4",
                messages=[{"role": "user", "content": "Hello"}]
            )

        # Usage
        result = await call_openai()
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        async def wrapper(*args, **kwargs):
            # Use function name as service name if not provided
            svc_name = service_name or f"{func.__module__}.{func.__name__}"

            # Create a factory that calls the original function
            async def coro_factory():
                return await func(*args, **kwargs)

            return await ai_call_with_retry(
                coro_factory=coro_factory,
                max_attempts=max_attempts,
                base_delay=base_delay,
                max_delay=max_delay,
                service_name=svc_name,
                use_circuit_breaker=use_circuit_breaker,
            )

        return wrapper
    return decorator


# ---------------------------------------------------------------------------
# Helper: Wrap existing OpenAI client with retry logic
# ---------------------------------------------------------------------------

class RetryingOpenAIClient:
    """
    Wrapper around AsyncOpenAI client that adds automatic retry logic.

    Usage:
        from openai import AsyncOpenAI
        from app.utils.ai_retry import RetryingOpenAIClient

        openai_client = AsyncOpenAI(api_key="...")
        retrying_client = RetryingOpenAIClient(openai_client, max_attempts=5)

        # Use like normal OpenAI client - retries are automatic
        response = await retrying_client.chat.completions.create(
            model="gpt-4",
            messages=[{"role": "user", "content": "Hello"}]
        )
    """

    def __init__(
        self,
        client,
        max_attempts: int = 5,
        base_delay: float = 1.0,
        max_delay: float = 30.0,
        service_name: str = "openai",
    ):
        self._client = client
        self.max_attempts = max_attempts
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.service_name = service_name

    def __getattr__(self, name):
        """Proxy attribute access to underlying client."""
        attr = getattr(self._client, name)

        # If it's a method/callable, wrap it with retry logic
        if callable(attr):
            async def retry_wrapper(*args, **kwargs):
                return await ai_call_with_retry(
                    lambda: attr(*args, **kwargs),
                    max_attempts=self.max_attempts,
                    base_delay=self.base_delay,
                    max_delay=self.max_delay,
                    service_name=f"{self.service_name}.{name}",
                )
            return retry_wrapper

        return attr


# ---------------------------------------------------------------------------
# Export
# ---------------------------------------------------------------------------

__all__ = [
    "ai_call_with_retry",
    "with_ai_retry",
    "AICircuitBreaker",
    "circuit_breaker",
    "RetryingOpenAIClient",
    "is_retryable_error",
    "calculate_backoff_seconds",
]
