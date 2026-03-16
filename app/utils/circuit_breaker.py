"""
Circuit Breaker Pattern for External API Resilience

This module implements the Circuit Breaker pattern to prevent cascading failures
when external services (Razorpay, WhatsApp, SMS, etc.) are unavailable.

States:
- CLOSED: Normal operation, requests flow through
- OPEN: Service is down, requests fail immediately (fast-fail)
- HALF_OPEN: Testing if service recovered, allowing limited requests

Usage:
    from app.utils.circuit_breaker import CircuitBreaker, circuit_breaker_registry

    # Create a breaker for a service
    razorpay_breaker = CircuitBreaker(
        name="razorpay",
        failure_threshold=5,      # Open after 5 failures
        recovery_timeout=60,      # Try again after 60 seconds
        half_open_max_calls=3     # Allow 3 test calls in half-open
    )

    # Use as decorator
    @razorpay_breaker
    def call_razorpay_api():
        ...

    # Or use directly
    with razorpay_breaker:
        response = requests.post(...)

    # Check status
    print(razorpay_breaker.state)  # "closed", "open", "half_open"

    # Get all breakers status
    print(circuit_breaker_registry.get_all_status())
"""

import asyncio
import logging
import threading
import time
from dataclasses import dataclass, field
from enum import Enum
from functools import wraps
from typing import Any, Callable, Dict, Optional, Type, Tuple
from contextlib import contextmanager, asynccontextmanager

logger = logging.getLogger("circuit_breaker")


# ---------------------------------------------------------------------------
# Circuit Breaker States
# ---------------------------------------------------------------------------

class CircuitState(Enum):
    CLOSED = "closed"       # Normal operation
    OPEN = "open"           # Failing fast
    HALF_OPEN = "half_open" # Testing recovery


# ---------------------------------------------------------------------------
# Circuit Breaker Exceptions
# ---------------------------------------------------------------------------

class CircuitBreakerError(Exception):
    """Base exception for circuit breaker errors."""
    pass


class CircuitOpenError(CircuitBreakerError):
    """Raised when circuit is open and request is rejected."""
    def __init__(self, breaker_name: str, remaining_seconds: float):
        self.breaker_name = breaker_name
        self.remaining_seconds = remaining_seconds
        super().__init__(
            f"Circuit breaker '{breaker_name}' is OPEN. "
            f"Service unavailable. Retry in {remaining_seconds:.1f}s"
        )


# ---------------------------------------------------------------------------
# Circuit Breaker Statistics
# ---------------------------------------------------------------------------

@dataclass
class CircuitBreakerStats:
    """Statistics for monitoring circuit breaker behavior."""
    total_calls: int = 0
    successful_calls: int = 0
    failed_calls: int = 0
    rejected_calls: int = 0  # Calls rejected due to open circuit
    last_failure_time: Optional[float] = None
    last_success_time: Optional[float] = None
    state_changes: int = 0
    consecutive_failures: int = 0
    consecutive_successes: int = 0

    def record_success(self):
        self.total_calls += 1
        self.successful_calls += 1
        self.last_success_time = time.time()
        self.consecutive_successes += 1
        self.consecutive_failures = 0

    def record_failure(self):
        self.total_calls += 1
        self.failed_calls += 1
        self.last_failure_time = time.time()
        self.consecutive_failures += 1
        self.consecutive_successes = 0

    def record_rejection(self):
        self.total_calls += 1
        self.rejected_calls += 1

    def record_state_change(self):
        self.state_changes += 1

    def to_dict(self) -> Dict[str, Any]:
        return {
            "total_calls": self.total_calls,
            "successful_calls": self.successful_calls,
            "failed_calls": self.failed_calls,
            "rejected_calls": self.rejected_calls,
            "success_rate": (
                f"{(self.successful_calls / self.total_calls * 100):.1f}%"
                if self.total_calls > 0 else "N/A"
            ),
            "last_failure": self.last_failure_time,
            "last_success": self.last_success_time,
            "state_changes": self.state_changes,
            "consecutive_failures": self.consecutive_failures,
            "consecutive_successes": self.consecutive_successes,
        }


# ---------------------------------------------------------------------------
# Circuit Breaker Implementation
# ---------------------------------------------------------------------------

class CircuitBreaker:
    """
    Circuit Breaker implementation for external service resilience.

    The circuit breaker monitors failures and prevents cascading failures
    by failing fast when a service is detected to be down.

    Args:
        name: Identifier for this circuit breaker (e.g., "razorpay", "whatsapp")
        failure_threshold: Number of failures before opening circuit (default: 5)
        recovery_timeout: Seconds to wait before testing recovery (default: 60)
        half_open_max_calls: Max calls allowed in half-open state (default: 3)
        success_threshold: Successes needed in half-open to close circuit (default: 2)
        excluded_exceptions: Exception types that don't count as failures

    Example:
        breaker = CircuitBreaker(name="payment-api", failure_threshold=5)

        @breaker
        def process_payment():
            return requests.post(payment_url, json=data)

        # Or async
        @breaker
        async def process_payment_async():
            async with httpx.AsyncClient() as client:
                return await client.post(payment_url, json=data)
    """

    def __init__(
        self,
        name: str,
        failure_threshold: int = 5,
        recovery_timeout: float = 60.0,
        half_open_max_calls: int = 3,
        success_threshold: int = 2,
        excluded_exceptions: Optional[Tuple[Type[Exception], ...]] = None,
    ):
        self.name = name
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.half_open_max_calls = half_open_max_calls
        self.success_threshold = success_threshold
        self.excluded_exceptions = excluded_exceptions or ()

        # State
        self._state = CircuitState.CLOSED
        self._last_failure_time: Optional[float] = None
        self._half_open_calls = 0

        # Thread safety
        self._lock = threading.RLock()

        # Statistics
        self.stats = CircuitBreakerStats()

        # Register in global registry
        circuit_breaker_registry.register(self)

        logger.info(
            f"Circuit breaker '{name}' initialized: "
            f"threshold={failure_threshold}, timeout={recovery_timeout}s"
        )

    @property
    def state(self) -> str:
        """Current state as string."""
        return self._state.value

    @property
    def is_closed(self) -> bool:
        return self._state == CircuitState.CLOSED

    @property
    def is_open(self) -> bool:
        return self._state == CircuitState.OPEN

    @property
    def is_half_open(self) -> bool:
        return self._state == CircuitState.HALF_OPEN

    def _should_attempt_reset(self) -> bool:
        """Check if enough time has passed to try recovery."""
        if self._last_failure_time is None:
            return True
        elapsed = time.time() - self._last_failure_time
        return elapsed >= self.recovery_timeout

    def _get_remaining_timeout(self) -> float:
        """Get remaining seconds until recovery attempt."""
        if self._last_failure_time is None:
            return 0
        elapsed = time.time() - self._last_failure_time
        return max(0, self.recovery_timeout - elapsed)

    def _transition_to(self, new_state: CircuitState):
        """Transition to a new state with logging."""
        if self._state != new_state:
            old_state = self._state
            self._state = new_state
            self.stats.record_state_change()

            if new_state == CircuitState.HALF_OPEN:
                self._half_open_calls = 0

            logger.warning(
                f"Circuit breaker '{self.name}': {old_state.value} -> {new_state.value}"
            )

    def _handle_success(self):
        """Handle a successful call."""
        with self._lock:
            self.stats.record_success()

            if self._state == CircuitState.HALF_OPEN:
                if self.stats.consecutive_successes >= self.success_threshold:
                    self._transition_to(CircuitState.CLOSED)
                    logger.info(
                        f"Circuit breaker '{self.name}': Service recovered, circuit CLOSED"
                    )

    def _handle_failure(self, exc: Exception):
        """Handle a failed call."""
        # Check if this exception type is excluded
        if isinstance(exc, self.excluded_exceptions):
            logger.debug(
                f"Circuit breaker '{self.name}': Excluded exception {type(exc).__name__}"
            )
            return

        with self._lock:
            self.stats.record_failure()
            self._last_failure_time = time.time()

            if self._state == CircuitState.CLOSED:
                if self.stats.consecutive_failures >= self.failure_threshold:
                    self._transition_to(CircuitState.OPEN)
                    logger.error(
                        f"Circuit breaker '{self.name}': OPENED after "
                        f"{self.failure_threshold} consecutive failures"
                    )

            elif self._state == CircuitState.HALF_OPEN:
                # Any failure in half-open immediately opens circuit
                self._transition_to(CircuitState.OPEN)
                logger.warning(
                    f"Circuit breaker '{self.name}': Failed in HALF_OPEN, reopening"
                )

    def _before_call(self):
        """Check circuit state before making a call."""
        with self._lock:
            if self._state == CircuitState.CLOSED:
                return True

            if self._state == CircuitState.OPEN:
                if self._should_attempt_reset():
                    self._transition_to(CircuitState.HALF_OPEN)
                    logger.info(
                        f"Circuit breaker '{self.name}': Testing recovery (HALF_OPEN)"
                    )
                else:
                    self.stats.record_rejection()
                    remaining = self._get_remaining_timeout()
                    raise CircuitOpenError(self.name, remaining)

            if self._state == CircuitState.HALF_OPEN:
                if self._half_open_calls >= self.half_open_max_calls:
                    self.stats.record_rejection()
                    raise CircuitOpenError(self.name, 0)
                self._half_open_calls += 1

            return True

    # ---------------------------------------------------------------------------
    # Decorator Support (Sync)
    # ---------------------------------------------------------------------------

    def __call__(self, func: Callable) -> Callable:
        """Use as decorator for sync or async functions."""
        if asyncio.iscoroutinefunction(func):
            return self._wrap_async(func)
        return self._wrap_sync(func)

    def _wrap_sync(self, func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            self._before_call()
            try:
                result = func(*args, **kwargs)
                self._handle_success()
                return result
            except Exception as exc:
                self._handle_failure(exc)
                raise
        return wrapper

    def _wrap_async(self, func: Callable) -> Callable:
        @wraps(func)
        async def wrapper(*args, **kwargs):
            self._before_call()
            try:
                result = await func(*args, **kwargs)
                self._handle_success()
                return result
            except Exception as exc:
                self._handle_failure(exc)
                raise
        return wrapper

    # ---------------------------------------------------------------------------
    # Context Manager Support
    # ---------------------------------------------------------------------------

    @contextmanager
    def __enter__(self):
        """Use as sync context manager."""
        self._before_call()
        try:
            yield self
            self._handle_success()
        except Exception as exc:
            self._handle_failure(exc)
            raise

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass  # Handled in __enter__

    @asynccontextmanager
    async def async_context(self):
        """Use as async context manager."""
        self._before_call()
        try:
            yield self
            self._handle_success()
        except Exception as exc:
            self._handle_failure(exc)
            raise

    # ---------------------------------------------------------------------------
    # Manual Control
    # ---------------------------------------------------------------------------

    def record_success(self):
        """Manually record a successful call."""
        self._handle_success()

    def record_failure(self, exc: Optional[Exception] = None):
        """Manually record a failed call."""
        self._handle_failure(exc or Exception("Manual failure"))

    def reset(self):
        """Force reset circuit to closed state."""
        with self._lock:
            self._transition_to(CircuitState.CLOSED)
            self.stats.consecutive_failures = 0
            self.stats.consecutive_successes = 0
            logger.info(f"Circuit breaker '{self.name}': Manually reset to CLOSED")

    def force_open(self):
        """Force circuit to open state (for maintenance, etc.)."""
        with self._lock:
            self._transition_to(CircuitState.OPEN)
            self._last_failure_time = time.time()
            logger.warning(f"Circuit breaker '{self.name}': Manually OPENED")

    def get_status(self) -> Dict[str, Any]:
        """Get current status and statistics."""
        with self._lock:
            return {
                "name": self.name,
                "state": self._state.value,
                "failure_threshold": self.failure_threshold,
                "recovery_timeout": self.recovery_timeout,
                "remaining_timeout": (
                    self._get_remaining_timeout() if self._state == CircuitState.OPEN else 0
                ),
                "stats": self.stats.to_dict(),
            }


# ---------------------------------------------------------------------------
# Circuit Breaker Registry (Global)
# ---------------------------------------------------------------------------

class CircuitBreakerRegistry:
    """
    Global registry for all circuit breakers.

    Allows monitoring and managing all breakers from a single place.

    Usage:
        from app.utils.circuit_breaker import circuit_breaker_registry

        # Get all breakers status
        status = circuit_breaker_registry.get_all_status()

        # Get specific breaker
        razorpay_breaker = circuit_breaker_registry.get("razorpay")

        # Reset all breakers
        circuit_breaker_registry.reset_all()
    """

    def __init__(self):
        self._breakers: Dict[str, CircuitBreaker] = {}
        self._lock = threading.Lock()

    def register(self, breaker: CircuitBreaker):
        """Register a circuit breaker."""
        with self._lock:
            self._breakers[breaker.name] = breaker

    def get(self, name: str) -> Optional[CircuitBreaker]:
        """Get a circuit breaker by name."""
        return self._breakers.get(name)

    def get_all(self) -> Dict[str, CircuitBreaker]:
        """Get all registered circuit breakers."""
        return dict(self._breakers)

    def get_all_status(self) -> Dict[str, Dict[str, Any]]:
        """Get status of all circuit breakers."""
        return {name: breaker.get_status() for name, breaker in self._breakers.items()}

    def reset_all(self):
        """Reset all circuit breakers to closed state."""
        for breaker in self._breakers.values():
            breaker.reset()
        logger.info("All circuit breakers reset to CLOSED")

    def get_open_breakers(self) -> Dict[str, CircuitBreaker]:
        """Get all breakers that are currently open."""
        return {
            name: breaker
            for name, breaker in self._breakers.items()
            if breaker.is_open
        }


# Global registry instance
circuit_breaker_registry = CircuitBreakerRegistry()


# ---------------------------------------------------------------------------
# Pre-configured Circuit Breakers for Common Services
# ---------------------------------------------------------------------------

def create_payment_breaker(name: str = "razorpay") -> CircuitBreaker:
    """
    Create circuit breaker optimized for payment services.

    - Lower threshold (3 failures) - payments are critical
    - Shorter recovery timeout (30s) - need quick recovery
    """
    return CircuitBreaker(
        name=name,
        failure_threshold=3,
        recovery_timeout=30.0,
        half_open_max_calls=2,
        success_threshold=2,
    )


def create_notification_breaker(name: str = "notification") -> CircuitBreaker:
    """
    Create circuit breaker optimized for notification services (SMS, WhatsApp).

    - Higher threshold (5 failures) - notifications less critical
    - Longer recovery timeout (60s) - SMS providers can be slow to recover
    """
    return CircuitBreaker(
        name=name,
        failure_threshold=5,
        recovery_timeout=60.0,
        half_open_max_calls=3,
        success_threshold=2,
    )


def create_ai_breaker(name: str = "openai") -> CircuitBreaker:
    """
    Create circuit breaker optimized for AI services (OpenAI, Groq).

    - Higher threshold (10 failures) - AI services have rate limits
    - Medium recovery timeout (45s) - rate limits usually clear quickly
    """
    return CircuitBreaker(
        name=name,
        failure_threshold=10,
        recovery_timeout=45.0,
        half_open_max_calls=5,
        success_threshold=3,
    )


# ---------------------------------------------------------------------------
# Export
# ---------------------------------------------------------------------------

__all__ = [
    "CircuitBreaker",
    "CircuitBreakerError",
    "CircuitOpenError",
    "CircuitState",
    "CircuitBreakerStats",
    "circuit_breaker_registry",
    "create_payment_breaker",
    "create_notification_breaker",
    "create_ai_breaker",
]
