"""
Unit tests for the AI retry module.

Tests error classification, backoff calculation, Retry-After parsing,
circuit breaker behavior, and the retry wrapper.
"""

import asyncio
import pytest
from unittest.mock import MagicMock, AsyncMock, patch

from app.utils.ai_retry import (
    is_retryable_error,
    get_retry_after_seconds,
    calculate_backoff_seconds,
    ai_call_with_retry,
    AICircuitBreaker,
)


# ---------------------------------------------------------------------------
# Error classification
# ---------------------------------------------------------------------------

class TestIsRetryableError:
    def test_rate_limit_429(self):
        exc = MagicMock()
        exc.status_code = 429
        assert is_retryable_error(exc) is True

    def test_server_error_500(self):
        exc = MagicMock()
        exc.status_code = 500
        assert is_retryable_error(exc) is True

    def test_server_error_502(self):
        exc = MagicMock()
        exc.status_code = 502
        assert is_retryable_error(exc) is True

    def test_server_error_503(self):
        exc = MagicMock()
        exc.status_code = 503
        assert is_retryable_error(exc) is True

    def test_client_error_400_not_retryable(self):
        exc = MagicMock()
        exc.status_code = 400
        assert is_retryable_error(exc) is False

    def test_client_error_401_not_retryable(self):
        exc = MagicMock()
        exc.status_code = 401
        assert is_retryable_error(exc) is False

    def test_client_error_404_not_retryable(self):
        exc = MagicMock()
        exc.status_code = 404
        assert is_retryable_error(exc) is False

    def test_timeout_error(self):
        exc = TimeoutError("connection timed out")
        assert is_retryable_error(exc) is True

    def test_connection_error(self):
        exc = ConnectionError("refused")
        assert is_retryable_error(exc) is True

    def test_overloaded_in_message(self):
        exc = Exception("Service overloaded, try again later")
        assert is_retryable_error(exc) is True


# ---------------------------------------------------------------------------
# Retry-After parsing
# ---------------------------------------------------------------------------

class TestGetRetryAfterSeconds:
    def test_from_response_header(self):
        exc = MagicMock()
        exc.response = MagicMock()
        exc.response.headers = {"Retry-After": "5.0"}
        assert get_retry_after_seconds(exc) == 5.0

    def test_from_error_message(self):
        exc = Exception("Rate limited. Retry after 10 seconds")
        assert get_retry_after_seconds(exc) == 10.0

    def test_none_when_no_info(self):
        exc = Exception("generic error")
        assert get_retry_after_seconds(exc) is None

    def test_none_when_no_response(self):
        exc = MagicMock(spec=Exception)
        exc.response = None
        result = get_retry_after_seconds(exc)
        assert result is None


# ---------------------------------------------------------------------------
# Backoff calculation
# ---------------------------------------------------------------------------

class TestCalculateBackoff:
    def test_first_attempt(self):
        delay = calculate_backoff_seconds(attempt=1, base_delay=1.0, max_delay=30.0, jitter=False)
        assert delay == 1.0

    def test_second_attempt(self):
        delay = calculate_backoff_seconds(attempt=2, base_delay=1.0, max_delay=30.0, jitter=False)
        assert delay == 2.0

    def test_third_attempt(self):
        delay = calculate_backoff_seconds(attempt=3, base_delay=1.0, max_delay=30.0, jitter=False)
        assert delay == 4.0

    def test_capped_at_max_delay(self):
        delay = calculate_backoff_seconds(attempt=10, base_delay=1.0, max_delay=30.0, jitter=False)
        assert delay == 30.0

    def test_jitter_adds_randomness(self):
        delays = set()
        for _ in range(20):
            d = calculate_backoff_seconds(attempt=3, base_delay=1.0, max_delay=30.0, jitter=True)
            delays.add(round(d, 2))
        # With jitter, we should see varying values
        assert len(delays) > 1

    def test_jitter_within_bounds(self):
        for _ in range(50):
            d = calculate_backoff_seconds(attempt=2, base_delay=1.0, max_delay=30.0, jitter=True)
            # base is 2.0, jitter adds 0-25% → max 2.5
            assert 2.0 <= d <= 2.5


# ---------------------------------------------------------------------------
# AI Circuit Breaker
# ---------------------------------------------------------------------------

class TestAICircuitBreaker:
    def test_initial_state_allows(self):
        cb = AICircuitBreaker(failure_threshold=3)
        assert cb.can_attempt("test-svc") is True

    def test_opens_after_threshold(self):
        cb = AICircuitBreaker(failure_threshold=3, recovery_timeout=60)
        for _ in range(3):
            cb.record_failure("test-svc")
        assert cb.can_attempt("test-svc") is False

    def test_success_resets_failures(self):
        cb = AICircuitBreaker(failure_threshold=3)
        cb.record_failure("test-svc")
        cb.record_failure("test-svc")
        cb.record_success("test-svc")
        assert cb.failure_count["test-svc"] == 0

    def test_half_open_recovery(self):
        cb = AICircuitBreaker(failure_threshold=2, recovery_timeout=0.1, success_threshold=1)
        cb.record_failure("test-svc")
        cb.record_failure("test-svc")
        assert cb.can_attempt("test-svc") is False
        import time
        time.sleep(0.15)
        assert cb.can_attempt("test-svc") is True  # transitions to HALF_OPEN
        cb.record_success("test-svc")
        assert cb.state["test-svc"] == "CLOSED"

    def test_half_open_failure_reopens(self):
        cb = AICircuitBreaker(failure_threshold=2, recovery_timeout=0.1)
        cb.record_failure("test-svc")
        cb.record_failure("test-svc")
        import time
        time.sleep(0.15)
        cb.can_attempt("test-svc")  # transitions to HALF_OPEN
        cb.record_failure("test-svc")
        assert cb.state["test-svc"] == "OPEN"

    def test_per_service_isolation(self):
        cb = AICircuitBreaker(failure_threshold=2)
        cb.record_failure("svc-a")
        cb.record_failure("svc-a")
        assert cb.can_attempt("svc-a") is False
        assert cb.can_attempt("svc-b") is True  # different service


# ---------------------------------------------------------------------------
# ai_call_with_retry
# ---------------------------------------------------------------------------

class TestAICallWithRetry:
    @pytest.mark.asyncio
    async def test_successful_call_no_retry(self):
        async def success():
            return "ok"

        result = await ai_call_with_retry(success, max_attempts=3, use_circuit_breaker=False)
        assert result == "ok"

    @pytest.mark.asyncio
    async def test_retry_then_succeed(self):
        call_count = 0

        async def flaky():
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise ConnectionError("transient")
            return "recovered"

        result = await ai_call_with_retry(
            flaky, max_attempts=3, base_delay=0.01, use_circuit_breaker=False
        )
        assert result == "recovered"
        assert call_count == 2

    @pytest.mark.asyncio
    async def test_max_attempts_exceeded(self):
        async def always_fail():
            raise ConnectionError("persistent failure")

        with pytest.raises(ConnectionError):
            await ai_call_with_retry(
                always_fail, max_attempts=2, base_delay=0.01, use_circuit_breaker=False
            )

    @pytest.mark.asyncio
    async def test_non_retryable_fails_immediately(self):
        call_count = 0

        async def bad_request():
            nonlocal call_count
            call_count += 1
            exc = Exception("Bad request")
            exc.status_code = 400
            raise exc

        with pytest.raises(Exception):
            await ai_call_with_retry(
                bad_request, max_attempts=3, base_delay=0.01, use_circuit_breaker=False
            )
        assert call_count == 1  # should not retry
