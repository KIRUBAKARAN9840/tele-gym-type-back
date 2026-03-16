"""
Unit tests for the SMS service.

Tests SMS delivery, retry handling, Redis logging, and phone masking.
"""

import pytest
from unittest.mock import patch, AsyncMock

from app.services.sms_service import SMSService


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

@pytest.fixture
def sms_service():
    return SMSService()


# ---------------------------------------------------------------------------
# Mobile masking
# ---------------------------------------------------------------------------

class TestMaskMobile:
    def test_mask_standard(self, sms_service):
        assert sms_service._mask_mobile("9876543210") == "98******10"

    def test_mask_short(self, sms_service):
        assert sms_service._mask_mobile("123") == "***"

    def test_mask_four_digits(self, sms_service):
        assert sms_service._mask_mobile("1234") == "****"

    def test_mask_five_digits(self, sms_service):
        assert sms_service._mask_mobile("12345") == "12*45"


# ---------------------------------------------------------------------------
# send_otp_sms
# ---------------------------------------------------------------------------

class TestSendOTPSMS:
    @pytest.mark.asyncio
    async def test_send_success(self, sms_service, fake_redis):
        with patch("app.services.sms_service.async_send_verification_sms", new_callable=AsyncMock, return_value=True), \
             patch("app.services.sms_service.get_redis", return_value=fake_redis):
            result = await sms_service.send_otp_sms("9876543210", "123456")
        assert result is True

    @pytest.mark.asyncio
    async def test_send_failure(self, sms_service, fake_redis):
        with patch("app.services.sms_service.async_send_verification_sms", new_callable=AsyncMock, return_value=False), \
             patch("app.services.sms_service.get_redis", return_value=fake_redis):
            result = await sms_service.send_otp_sms("9876543210", "123456")
        assert result is False

    @pytest.mark.asyncio
    async def test_exception_returns_false(self, sms_service, fake_redis):
        with patch("app.services.sms_service.async_send_verification_sms", new_callable=AsyncMock, side_effect=Exception("boom")), \
             patch("app.services.sms_service.get_redis", return_value=fake_redis):
            result = await sms_service.send_otp_sms("9876543210", "123456")
        assert result is False


# ---------------------------------------------------------------------------
# Redis logging
# ---------------------------------------------------------------------------

class TestSMSLogging:
    @pytest.mark.asyncio
    async def test_log_attempt_increments_redis(self, sms_service, fake_redis):
        with patch("app.services.sms_service.get_redis", return_value=fake_redis):
            await sms_service._log_sms_attempt("9876543210", "attempt")

        # Key format: sms_log:{YYYYMMDD}:attempt
        keys = []
        async for key in fake_redis.scan_iter("sms_log:*:attempt"):
            keys.append(key)
        assert len(keys) == 1

    @pytest.mark.asyncio
    async def test_log_attempt_redis_error_caught(self, sms_service):
        """Redis failure in logging should not raise."""
        broken_redis = AsyncMock()
        broken_redis.incr = AsyncMock(side_effect=ConnectionError("Redis down"))
        with patch("app.services.sms_service.get_redis", return_value=broken_redis):
            # Should not raise
            await sms_service._log_sms_attempt("9876543210", "attempt")
