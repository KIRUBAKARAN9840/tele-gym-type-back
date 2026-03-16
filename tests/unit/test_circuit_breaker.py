"""
Unit tests for the Circuit Breaker pattern implementation.

Tests cover state transitions, failure thresholds, recovery timeouts,
decorator/context-manager usage, statistics, registry, and factory functions.
"""

import time
import asyncio
import pytest

from app.utils.circuit_breaker import (
    CircuitBreaker,
    CircuitBreakerError,
    CircuitOpenError,
    CircuitState,
    CircuitBreakerStats,
    CircuitBreakerRegistry,
    create_payment_breaker,
    create_notification_breaker,
    create_ai_breaker,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _fail_breaker(breaker: CircuitBreaker, n: int):
    """Record *n* failures on the breaker."""
    for _ in range(n):
        breaker.record_failure(Exception("boom"))


# ---------------------------------------------------------------------------
# State machine tests
# ---------------------------------------------------------------------------

class TestCircuitBreakerStates:
    def test_initial_state_is_closed(self):
        cb = CircuitBreaker(name="test-init", failure_threshold=5, recovery_timeout=10)
        assert cb.state == "closed"
        assert cb.is_closed is True

    def test_success_keeps_closed(self):
        cb = CircuitBreaker(name="test-success", failure_threshold=5)
        for _ in range(10):
            cb.record_success()
        assert cb.is_closed

    def test_failures_below_threshold_stay_closed(self):
        cb = CircuitBreaker(name="test-below", failure_threshold=5)
        _fail_breaker(cb, 4)
        assert cb.is_closed

    def test_failures_at_threshold_opens(self):
        cb = CircuitBreaker(name="test-open", failure_threshold=3)
        _fail_breaker(cb, 3)
        assert cb.is_open

    def test_open_rejects_calls(self):
        cb = CircuitBreaker(name="test-reject", failure_threshold=2, recovery_timeout=60)
        _fail_breaker(cb, 2)
        with pytest.raises(CircuitOpenError) as exc_info:
            cb._before_call()
        assert "test-reject" in str(exc_info.value)

    def test_open_to_half_open_after_timeout(self):
        cb = CircuitBreaker(name="test-half", failure_threshold=2, recovery_timeout=0.1)
        _fail_breaker(cb, 2)
        assert cb.is_open
        time.sleep(0.15)
        cb._before_call()  # should transition
        assert cb.is_half_open

    def test_half_open_success_closes(self):
        cb = CircuitBreaker(
            name="test-recover",
            failure_threshold=2,
            recovery_timeout=0.1,
            success_threshold=2,
        )
        _fail_breaker(cb, 2)
        time.sleep(0.15)
        cb._before_call()  # move to half-open
        cb._handle_success()
        cb._handle_success()
        assert cb.is_closed

    def test_half_open_failure_reopens(self):
        cb = CircuitBreaker(name="test-reopen", failure_threshold=2, recovery_timeout=0.1)
        _fail_breaker(cb, 2)
        time.sleep(0.15)
        cb._before_call()  # move to half-open
        assert cb.is_half_open
        cb._handle_failure(Exception("fail"))
        assert cb.is_open

    def test_half_open_max_calls_enforced(self):
        cb = CircuitBreaker(
            name="test-maxcalls",
            failure_threshold=2,
            recovery_timeout=0.1,
            half_open_max_calls=2,
        )
        _fail_breaker(cb, 2)
        time.sleep(0.15)
        cb._before_call()  # transition + 1st call
        cb._before_call()  # 2nd call
        with pytest.raises(CircuitOpenError):
            cb._before_call()  # 3rd call should be rejected

    def test_excluded_exceptions_ignored(self):
        cb = CircuitBreaker(
            name="test-exclude",
            failure_threshold=2,
            excluded_exceptions=(ValueError,),
        )
        cb._handle_failure(ValueError("harmless"))
        cb._handle_failure(ValueError("harmless"))
        assert cb.is_closed  # ValueErrors are excluded, so still closed


# ---------------------------------------------------------------------------
# Decorator tests
# ---------------------------------------------------------------------------

class TestCircuitBreakerDecorator:
    def test_decorator_sync_success(self):
        cb = CircuitBreaker(name="test-dec-sync", failure_threshold=3)

        @cb
        def add(a, b):
            return a + b

        assert add(1, 2) == 3
        assert cb.stats.successful_calls == 1

    def test_decorator_sync_failure(self):
        cb = CircuitBreaker(name="test-dec-sync-fail", failure_threshold=3)

        @cb
        def boom():
            raise RuntimeError("kaboom")

        with pytest.raises(RuntimeError):
            boom()
        assert cb.stats.failed_calls == 1

    @pytest.mark.asyncio
    async def test_decorator_async_success(self):
        cb = CircuitBreaker(name="test-dec-async", failure_threshold=3)

        @cb
        async def async_add(a, b):
            return a + b

        result = await async_add(2, 3)
        assert result == 5
        assert cb.stats.successful_calls == 1

    @pytest.mark.asyncio
    async def test_decorator_async_failure(self):
        cb = CircuitBreaker(name="test-dec-async-fail", failure_threshold=3)

        @cb
        async def async_boom():
            raise RuntimeError("async kaboom")

        with pytest.raises(RuntimeError):
            await async_boom()
        assert cb.stats.failed_calls == 1


# ---------------------------------------------------------------------------
# Manual control tests
# ---------------------------------------------------------------------------

class TestCircuitBreakerManualControl:
    def test_manual_record_success(self):
        cb = CircuitBreaker(name="test-manual-s", failure_threshold=3)
        cb.record_success()
        assert cb.stats.successful_calls == 1
        assert cb.stats.consecutive_successes == 1

    def test_manual_record_failure(self):
        cb = CircuitBreaker(name="test-manual-f", failure_threshold=3)
        cb.record_failure()
        assert cb.stats.failed_calls == 1
        assert cb.stats.consecutive_failures == 1

    def test_reset(self):
        cb = CircuitBreaker(name="test-reset", failure_threshold=2)
        _fail_breaker(cb, 2)
        assert cb.is_open
        cb.reset()
        assert cb.is_closed
        assert cb.stats.consecutive_failures == 0

    def test_force_open(self):
        cb = CircuitBreaker(name="test-force-open", failure_threshold=10)
        assert cb.is_closed
        cb.force_open()
        assert cb.is_open


# ---------------------------------------------------------------------------
# Stats tests
# ---------------------------------------------------------------------------

class TestCircuitBreakerStats:
    def test_stats_tracking(self):
        cb = CircuitBreaker(name="test-stats", failure_threshold=5)
        cb.record_success()
        cb.record_success()
        cb.record_failure()

        status = cb.get_status()
        assert status["name"] == "test-stats"
        assert status["state"] == "closed"
        stats = status["stats"]
        assert stats["total_calls"] == 3
        assert stats["successful_calls"] == 2
        assert stats["failed_calls"] == 1

    def test_stats_success_rate(self):
        stats = CircuitBreakerStats()
        stats.record_success()
        stats.record_success()
        stats.record_failure()
        result = stats.to_dict()
        assert result["success_rate"] == "66.7%"

    def test_stats_no_calls(self):
        stats = CircuitBreakerStats()
        result = stats.to_dict()
        assert result["success_rate"] == "N/A"


# ---------------------------------------------------------------------------
# Registry tests
# ---------------------------------------------------------------------------

class TestCircuitBreakerRegistry:
    def test_register_and_get(self):
        registry = CircuitBreakerRegistry()
        cb = CircuitBreaker.__new__(CircuitBreaker)
        cb.name = "reg-test"
        cb._state = CircuitState.CLOSED
        cb._lock = __import__("threading").RLock()
        cb.stats = CircuitBreakerStats()
        registry.register(cb)
        assert registry.get("reg-test") is cb

    def test_get_nonexistent(self):
        registry = CircuitBreakerRegistry()
        assert registry.get("does-not-exist") is None

    def test_get_all_status(self):
        registry = CircuitBreakerRegistry()
        cb1 = CircuitBreaker(name="reg-s1", failure_threshold=3)
        cb2 = CircuitBreaker(name="reg-s2", failure_threshold=3)
        registry.register(cb1)
        registry.register(cb2)
        statuses = registry.get_all_status()
        assert "reg-s1" in statuses
        assert "reg-s2" in statuses

    def test_get_open_breakers(self):
        registry = CircuitBreakerRegistry()
        cb1 = CircuitBreaker(name="reg-open1", failure_threshold=2)
        cb2 = CircuitBreaker(name="reg-open2", failure_threshold=2)
        registry.register(cb1)
        registry.register(cb2)
        _fail_breaker(cb1, 2)  # open cb1
        opened = registry.get_open_breakers()
        assert "reg-open1" in opened
        assert "reg-open2" not in opened

    def test_reset_all(self):
        registry = CircuitBreakerRegistry()
        cb = CircuitBreaker(name="reg-reset", failure_threshold=2)
        registry.register(cb)
        _fail_breaker(cb, 2)
        assert cb.is_open
        registry.reset_all()
        assert cb.is_closed


# ---------------------------------------------------------------------------
# Factory function tests
# ---------------------------------------------------------------------------

class TestCircuitBreakerFactories:
    def test_create_payment_breaker(self):
        cb = create_payment_breaker("pay-test")
        assert cb.name == "pay-test"
        assert cb.failure_threshold == 3
        assert cb.recovery_timeout == 30.0

    def test_create_notification_breaker(self):
        cb = create_notification_breaker("notif-test")
        assert cb.name == "notif-test"
        assert cb.failure_threshold == 5
        assert cb.recovery_timeout == 60.0

    def test_create_ai_breaker(self):
        cb = create_ai_breaker("ai-test")
        assert cb.name == "ai-test"
        assert cb.failure_threshold == 10
        assert cb.recovery_timeout == 45.0
