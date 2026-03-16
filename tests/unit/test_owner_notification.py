"""
Unit tests for the owner notification service.

Tests message building for different booking types,
push notification sending, and invalid token cleanup.
"""

import pytest
from unittest.mock import MagicMock, AsyncMock, patch
from datetime import date

from app.services.owner_notification_service import (
    OwnerNotificationService,
    BookingNotification,
    BookingType,
    NOTIFICATION_TEMPLATES,
)


# ---------------------------------------------------------------------------
# Message building
# ---------------------------------------------------------------------------

class TestBuildNotificationMessage:
    def setup_method(self):
        self.service = OwnerNotificationService.__new__(OwnerNotificationService)
        self.service.push_client = MagicMock()

    def test_gym_membership_message(self):
        notif = BookingNotification(
            booking_type=BookingType.GYM_MEMBERSHIP,
            gym_id=1, client_id=42, amount=2999.0, duration_months=3,
        )
        msg = self.service.build_notification_message(notif, "John Doe")
        assert msg["title"] == "New Membership! 🎉"
        assert "John Doe" in msg["body"]
        assert "3M" in msg["body"]

    def test_personal_training_message(self):
        notif = BookingNotification(
            booking_type=BookingType.PERSONAL_TRAINING,
            gym_id=1, client_id=42, amount=4999.0, duration_months=1,
        )
        msg = self.service.build_notification_message(notif, "Jane")
        assert "PT" in msg["title"]
        assert "Jane" in msg["body"]

    def test_session_message(self):
        notif = BookingNotification(
            booking_type=BookingType.SESSION,
            gym_id=1, client_id=42, amount=500.0,
            session_name="Yoga", sessions_count=5,
            starting_date=date(2026, 3, 15),
        )
        msg = self.service.build_notification_message(notif, "Alice")
        assert "Session" in msg["title"]
        assert "Alice" in msg["body"]
        assert "Yoga" in msg["body"]
        assert "15 Mar" in msg["body"]

    def test_daily_pass_message(self):
        notif = BookingNotification(
            booking_type=BookingType.DAILY_PASS,
            gym_id=1, client_id=42, amount=49.0, days_count=1,
            starting_date=date(2026, 2, 20),
        )
        msg = self.service.build_notification_message(notif, "Bob")
        assert "DailyPass" in msg["title"]
        assert "Bob" in msg["body"]

    def test_checkin_message(self):
        notif = BookingNotification(
            booking_type=BookingType.DAILYPASS_CHECKIN,
            gym_id=1, client_id=42, amount=0,
        )
        msg = self.service.build_notification_message(notif, "Charlie")
        assert "Check-in" in msg["title"]
        assert "Charlie" in msg["body"]

    def test_scan_alert_message(self):
        notif = BookingNotification(
            booking_type=BookingType.SCAN_ALERT,
            gym_id=1, client_id=42, amount=0,
            session_name="expired membership",
        )
        msg = self.service.build_notification_message(notif, "Dave")
        assert "Rejected" in msg["title"]
        assert "expired membership" in msg["body"]

    def test_unknown_booking_type_fallback(self):
        """Unknown type should return generic message."""
        notif = BookingNotification(
            booking_type=BookingType.SESSION_CHECKIN,
            gym_id=1, client_id=42, amount=0,
        )
        msg = self.service.build_notification_message(notif, "Eve")
        assert msg["title"] is not None
        assert "Eve" in msg["body"]


# ---------------------------------------------------------------------------
# Push notification sending
# ---------------------------------------------------------------------------

class TestSendPushNotifications:
    def setup_method(self):
        self.service = OwnerNotificationService.__new__(OwnerNotificationService)
        self.service.push_client = MagicMock()

    def test_send_success(self):
        mock_response = MagicMock()
        mock_response.status = "ok"
        self.service.push_client.publish_multiple = MagicMock(return_value=[mock_response])

        result = self.service.send_push_notifications(
            tokens=["ExponentPushToken[abc123]"],
            title="Test",
            body="Hello",
        )
        assert result["sent"] == 1
        assert result["failed"] == 0

    def test_send_empty_tokens(self):
        result = self.service.send_push_notifications(
            tokens=[], title="Test", body="Hello",
        )
        assert result["sent"] == 0

    def test_send_detects_invalid_token(self):
        ok_resp = MagicMock()
        ok_resp.status = "ok"

        bad_resp = MagicMock()
        bad_resp.status = "error"
        bad_resp.details = MagicMock()
        bad_resp.details.error = "DeviceNotRegistered"

        self.service.push_client.publish_multiple = MagicMock(return_value=[ok_resp, bad_resp])

        result = self.service.send_push_notifications(
            tokens=["token-good", "token-bad"],
            title="Test",
            body="Hello",
        )
        assert result["sent"] == 1
        assert result["failed"] == 1
        assert "token-bad" in result["invalid_tokens"]

    def test_push_server_error_handled(self):
        from exponent_server_sdk import PushServerError
        self.service.push_client.publish_multiple = MagicMock(
            side_effect=PushServerError("server error", MagicMock(), errors=[], response_data={})
        )

        result = self.service.send_push_notifications(
            tokens=["token-1"],
            title="Test",
            body="Hello",
        )
        assert result["sent"] == 0
        assert result["failed"] == 1


# ---------------------------------------------------------------------------
# get_client_name
# ---------------------------------------------------------------------------

class TestGetClientName:
    @pytest.mark.asyncio
    async def test_returns_name(self, mock_db):
        service = OwnerNotificationService.__new__(OwnerNotificationService)
        client = MagicMock()
        client.name = "Test User"
        mock_result = MagicMock()
        mock_result.scalars.return_value.first.return_value = client
        mock_db.execute = AsyncMock(return_value=mock_result)

        name = await service.get_client_name(mock_db, client_id=1)
        assert name == "Test User"

    @pytest.mark.asyncio
    async def test_returns_default_when_no_client(self, mock_db):
        service = OwnerNotificationService.__new__(OwnerNotificationService)
        mock_result = MagicMock()
        mock_result.scalars.return_value.first.return_value = None
        mock_db.execute = AsyncMock(return_value=mock_result)

        name = await service.get_client_name(mock_db, client_id=999)
        assert name == "A customer"
