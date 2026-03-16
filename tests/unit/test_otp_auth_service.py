"""
Unit tests for the OTP authentication service.

Tests OTP generation, send/verify flows, token creation, rate limiting,
dev mode, and mobile number masking.
"""

import json
import pytest
from unittest.mock import patch, AsyncMock, MagicMock
from datetime import datetime

from app.services.otp_auth_service import OTPAuthService


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

@pytest.fixture
def otp_service():
    """Fresh OTPAuthService with mocked SMS."""
    svc = OTPAuthService()
    svc.sms_service = AsyncMock()
    svc.sms_service.send_otp_sms = AsyncMock(return_value=True)
    return svc


# ---------------------------------------------------------------------------
# Mobile masking
# ---------------------------------------------------------------------------

class TestMobileMasking:
    def test_mask_standard_number(self):
        svc = OTPAuthService()
        assert svc._mask_mobile_number("9876543210") == "98******10"

    def test_mask_short_number(self):
        svc = OTPAuthService()
        assert svc._mask_mobile_number("123") == "***"

    def test_mask_four_digit(self):
        svc = OTPAuthService()
        assert svc._mask_mobile_number("1234") == "****"

    def test_mask_five_digit(self):
        svc = OTPAuthService()
        result = svc._mask_mobile_number("12345")
        assert result == "12*45"


# ---------------------------------------------------------------------------
# Session token generation
# ---------------------------------------------------------------------------

class TestSessionToken:
    def test_generates_unique_tokens(self):
        svc = OTPAuthService()
        t1 = svc._generate_session_token()
        t2 = svc._generate_session_token()
        assert t1 != t2
        assert len(t1) > 20  # secrets.token_urlsafe(32) gives ~43 chars


# ---------------------------------------------------------------------------
# send_otp
# ---------------------------------------------------------------------------

class TestSendOTP:
    @pytest.mark.asyncio
    async def test_send_otp_success(self, otp_service, fake_redis):
        with patch("app.services.otp_auth_service.get_redis", return_value=fake_redis), \
             patch("app.services.otp_auth_service.generate_otp", return_value="123456"):
            result = await otp_service.send_otp("9876543210", user_type="manager")

        assert result["status"] == "success"
        assert result["delivery_method"] == "sms"
        assert "****" in result["mobile_masked"]
        otp_service.sms_service.send_otp_sms.assert_awaited_once_with("9876543210", "123456")

    @pytest.mark.asyncio
    async def test_send_otp_stores_in_redis(self, otp_service, fake_redis):
        with patch("app.services.otp_auth_service.get_redis", return_value=fake_redis), \
             patch("app.services.otp_auth_service.generate_otp", return_value="654321"):
            await otp_service.send_otp("9876543210", user_type="manager")

        stored = await fake_redis.get("telecaller:otp:9876543210")
        assert stored is not None
        data = json.loads(stored)
        assert data["otp"] == "654321"
        assert data["user_type"] == "manager"

    @pytest.mark.asyncio
    async def test_send_otp_sets_rate_limit(self, otp_service, fake_redis):
        with patch("app.services.otp_auth_service.get_redis", return_value=fake_redis), \
             patch("app.services.otp_auth_service.generate_otp", return_value="111111"):
            await otp_service.send_otp("9876543210")

        rate_key = await fake_redis.get("otp_rate_limit:9876543210")
        assert rate_key == "1"

    @pytest.mark.asyncio
    async def test_send_otp_sms_failure_still_succeeds(self, otp_service, fake_redis):
        """Even if SMS fails, OTP is stored and success returned."""
        otp_service.sms_service.send_otp_sms = AsyncMock(return_value=False)
        with patch("app.services.otp_auth_service.get_redis", return_value=fake_redis), \
             patch("app.services.otp_auth_service.generate_otp", return_value="999999"):
            result = await otp_service.send_otp("9876543210")

        assert result["status"] == "success"
        assert result["delivery_method"] == "failed"

    @pytest.mark.asyncio
    async def test_send_otp_dev_mode(self, fake_redis):
        svc = OTPAuthService()
        svc.SKIP_SMS_IN_DEV = True
        svc.DEV_TEST_OTP = "000000"
        svc.sms_service = AsyncMock()

        with patch("app.services.otp_auth_service.get_redis", return_value=fake_redis):
            result = await svc.send_otp("9876543210")

        assert result["delivery_method"] == "dev_test"
        svc.sms_service.send_otp_sms.assert_not_awaited()


# ---------------------------------------------------------------------------
# verify_otp
# ---------------------------------------------------------------------------

class TestVerifyOTP:
    @pytest.mark.asyncio
    async def test_verify_otp_success(self, otp_service, fake_redis):
        # Store OTP first
        otp_data = json.dumps({
            "otp": "123456",
            "mobile_number": "9876543210",
            "created_at": datetime.utcnow().isoformat(),
            "user_type": "manager",
            "sms_sent": True,
        })
        await fake_redis.setex("telecaller:otp:9876543210", 300, otp_data)

        with patch("app.services.otp_auth_service.get_redis", return_value=fake_redis):
            result = await otp_service.verify_otp("9876543210", "123456")

        assert result["status"] == "success"
        assert result["user"]["mobile_number"] == "9876543210"
        assert result["user"]["role"] == "manager"

    @pytest.mark.asyncio
    async def test_verify_otp_wrong_code(self, otp_service, fake_redis):
        otp_data = json.dumps({
            "otp": "123456",
            "mobile_number": "9876543210",
            "created_at": datetime.utcnow().isoformat(),
            "user_type": "manager",
            "sms_sent": True,
        })
        await fake_redis.setex("telecaller:otp:9876543210", 300, otp_data)

        from fastapi import HTTPException
        with patch("app.services.otp_auth_service.get_redis", return_value=fake_redis):
            with pytest.raises(HTTPException) as exc_info:
                await otp_service.verify_otp("9876543210", "000000")
        assert exc_info.value.status_code == 400
        assert "Invalid OTP" in exc_info.value.detail

    @pytest.mark.asyncio
    async def test_verify_otp_expired(self, otp_service, fake_redis):
        """No OTP in Redis simulates expiry."""
        from fastapi import HTTPException
        with patch("app.services.otp_auth_service.get_redis", return_value=fake_redis):
            with pytest.raises(HTTPException) as exc_info:
                await otp_service.verify_otp("9876543210", "123456")
        assert exc_info.value.status_code == 400
        assert "expired" in exc_info.value.detail.lower()

    @pytest.mark.asyncio
    async def test_verify_otp_deletes_key(self, otp_service, fake_redis):
        otp_data = json.dumps({
            "otp": "123456",
            "mobile_number": "9876543210",
            "created_at": datetime.utcnow().isoformat(),
            "user_type": "manager",
            "sms_sent": True,
        })
        await fake_redis.setex("telecaller:otp:9876543210", 300, otp_data)

        with patch("app.services.otp_auth_service.get_redis", return_value=fake_redis):
            await otp_service.verify_otp("9876543210", "123456")

        remaining = await fake_redis.get("telecaller:otp:9876543210")
        assert remaining is None


# ---------------------------------------------------------------------------
# create_session_tokens
# ---------------------------------------------------------------------------

class TestCreateSessionTokens:
    @pytest.mark.asyncio
    async def test_creates_access_and_refresh(self):
        svc = OTPAuthService()
        user = MagicMock()
        user.id = 42
        user.mobile_number = "9876543210"

        tokens = await svc.create_session_tokens(user, "manager")
        assert "access_token" in tokens
        assert "refresh_token" in tokens
        assert tokens["access_token"] != tokens["refresh_token"]

    @pytest.mark.asyncio
    async def test_telecaller_includes_manager_id(self):
        svc = OTPAuthService()
        user = MagicMock()
        user.id = 7
        user.mobile_number = "1234567890"
        user.manager_id = 99

        from jose import jwt
        from app.utils.security import SECRET_KEY, ALGORITHM

        tokens = await svc.create_session_tokens(user, "telecaller")
        payload = jwt.decode(tokens["access_token"], SECRET_KEY, algorithms=[ALGORITHM])
        assert payload["manager_id"] == 99
        assert payload["role"] == "telecaller"
