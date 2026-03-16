"""
Unit tests for the rate limiter middleware.

Tests IP extraction, window calculations, limit enforcement,
whitelist bypass, and endpoint-specific limits using fakeredis.
"""

import pytest
from unittest.mock import MagicMock

from app.middleware.rate_limit_middleware import (
    get_real_client_ip,
    _window_secs,
    _now,
    IPRateLimitMiddleware,
    EndpointSpecificRateLimit,
)


# ---------------------------------------------------------------------------
# IP extraction
# ---------------------------------------------------------------------------

class TestGetRealClientIP:
    def _req(self, xff=None, xri=None, host="127.0.0.1"):
        req = MagicMock()
        headers = {}
        if xff:
            headers["x-forwarded-for"] = xff
        if xri:
            headers["x-real-ip"] = xri
        req.headers = MagicMock()
        req.headers.get = lambda key, default=None: headers.get(key, default)
        req.client = MagicMock()
        req.client.host = host
        return req

    def test_xff_single_ip(self):
        ip = get_real_client_ip(self._req(xff="1.2.3.4"))
        assert ip == "1.2.3.4"

    def test_xff_multiple_ips_takes_last(self):
        """ALB appends client IP at the end."""
        ip = get_real_client_ip(self._req(xff="10.0.0.1, 192.168.1.1, 1.2.3.4"))
        assert ip == "1.2.3.4"

    def test_xri_header(self):
        ip = get_real_client_ip(self._req(xri="5.6.7.8"))
        assert ip == "5.6.7.8"

    def test_xff_takes_priority_over_xri(self):
        ip = get_real_client_ip(self._req(xff="1.2.3.4", xri="5.6.7.8"))
        assert ip == "1.2.3.4"

    def test_fallback_to_client_host(self):
        ip = get_real_client_ip(self._req(host="9.8.7.6"))
        assert ip == "9.8.7.6"

    def test_no_client(self):
        req = MagicMock()
        req.headers = MagicMock()
        req.headers.get = lambda key, default=None: None
        req.client = None
        ip = get_real_client_ip(req)
        assert ip == "unknown"


# ---------------------------------------------------------------------------
# Window helpers
# ---------------------------------------------------------------------------

class TestWindowHelpers:
    def test_window_secs_returns_positive(self):
        now = _now()
        secs = _window_secs(now)
        assert 0 < secs["min"] <= 60
        assert 0 < secs["hour"] <= 3600
        assert 0 < secs["day"] <= 86400


# ---------------------------------------------------------------------------
# IPRateLimitMiddleware
# ---------------------------------------------------------------------------

class TestIPRateLimitMiddleware:
    @pytest.mark.asyncio
    async def test_under_limit_allowed(self, fake_redis):
        limiter = IPRateLimitMiddleware(
            redis_client=fake_redis,
            requests_per_minute=10,
            requests_per_hour=100,
            requests_per_day=1000,
            burst_limit=5,
            burst_window=10,
        )
        limited, info = await limiter.is_subject_limited("1.2.3.4")
        assert limited is False
        assert info["minute_count"] == 1

    @pytest.mark.asyncio
    async def test_over_minute_limit_blocked(self, fake_redis):
        limiter = IPRateLimitMiddleware(
            redis_client=fake_redis,
            requests_per_minute=2,
            requests_per_hour=1000,
            requests_per_day=10000,
            burst_limit=100,
            burst_window=10,
        )
        await limiter.is_subject_limited("1.2.3.4")
        await limiter.is_subject_limited("1.2.3.4")
        limited, info = await limiter.is_subject_limited("1.2.3.4")
        assert limited is True
        assert "minute" in info["tripped"]

    @pytest.mark.asyncio
    async def test_burst_limit_blocked(self, fake_redis):
        limiter = IPRateLimitMiddleware(
            redis_client=fake_redis,
            requests_per_minute=100,
            requests_per_hour=1000,
            requests_per_day=10000,
            burst_limit=2,
            burst_window=60,
        )
        await limiter.is_subject_limited("1.2.3.4")
        await limiter.is_subject_limited("1.2.3.4")
        limited, info = await limiter.is_subject_limited("1.2.3.4")
        assert limited is True
        assert "burst" in info["tripped"]

    @pytest.mark.asyncio
    async def test_whitelist_bypass(self, fake_redis):
        limiter = IPRateLimitMiddleware(
            redis_client=fake_redis,
            requests_per_minute=1,
            requests_per_hour=1,
            requests_per_day=1,
            burst_limit=1,
            whitelist_subjects=["10.0.0.1"],
        )
        # Should not be limited even though limits are 1
        await limiter.is_subject_limited("10.0.0.1")
        limited, info = await limiter.is_subject_limited("10.0.0.1")
        assert limited is False
        assert info.get("whitelisted") is True

    @pytest.mark.asyncio
    async def test_retry_after_positive(self, fake_redis):
        limiter = IPRateLimitMiddleware(
            redis_client=fake_redis,
            requests_per_minute=1,
            requests_per_hour=1000,
            requests_per_day=10000,
            burst_limit=100,
        )
        await limiter.is_subject_limited("1.2.3.4")
        limited, info = await limiter.is_subject_limited("1.2.3.4")
        assert limited is True
        assert info["retry_after"] > 0

    @pytest.mark.asyncio
    async def test_different_subjects_isolated(self, fake_redis):
        limiter = IPRateLimitMiddleware(
            redis_client=fake_redis,
            requests_per_minute=2,
            requests_per_hour=1000,
            requests_per_day=10000,
            burst_limit=100,
        )
        await limiter.is_subject_limited("1.1.1.1")
        await limiter.is_subject_limited("1.1.1.1")
        limited_a, _ = await limiter.is_subject_limited("1.1.1.1")
        limited_b, _ = await limiter.is_subject_limited("2.2.2.2")
        assert limited_a is True
        assert limited_b is False


# ---------------------------------------------------------------------------
# EndpointSpecificRateLimit
# ---------------------------------------------------------------------------

class TestEndpointSpecificRateLimit:
    @pytest.mark.asyncio
    async def test_matching_endpoint_tracked(self, fake_redis):
        erl = EndpointSpecificRateLimit(redis_client=fake_redis)
        blocked, info = await erl.check("/auth/login", "1.2.3.4")
        assert blocked is False  # first request
        assert "pattern" in info

    @pytest.mark.asyncio
    async def test_unmatched_endpoint_not_tracked(self, fake_redis):
        erl = EndpointSpecificRateLimit(redis_client=fake_redis)
        blocked, info = await erl.check("/api/v1/some-endpoint", "1.2.3.4")
        assert blocked is False
        assert info == {}  # no pattern matched

    @pytest.mark.asyncio
    async def test_endpoint_limit_enforced(self, fake_redis):
        erl = EndpointSpecificRateLimit(redis_client=fake_redis)
        # /auth/login has per_minute: 15
        for _ in range(16):
            await erl.check("/auth/login", "1.2.3.4")
        blocked, info = await erl.check("/auth/login", "1.2.3.4")
        assert blocked is True
