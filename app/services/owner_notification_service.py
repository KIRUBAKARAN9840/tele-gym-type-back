# app/services/owner_notification_service.py


import logging
from dataclasses import dataclass
from datetime import date
from enum import Enum
from typing import Any, Dict, List, Optional

from exponent_server_sdk import (
    DeviceNotRegisteredError,
    PushClient,
    PushMessage,
    PushServerError,
)
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.fittbot_models import Gym, GymOwner, Client

logger = logging.getLogger("services.owner_notification")


class BookingType(Enum):
 
    GYM_MEMBERSHIP = "gym_membership"
    PERSONAL_TRAINING = "personal_training"
    SESSION = "session"
    DAILY_PASS = "daily_pass"
    DAILYPASS_CHECKIN = "dailypass_checkin"
    SESSION_CHECKIN = "session_checkin"
    SCAN_ALERT = "scan_alert"


@dataclass
class BookingNotification:
    """Data structure for booking notification details."""
    booking_type: BookingType
    gym_id: int
    client_id: int
    amount: float  # Amount in rupees

    # Optional fields based on booking type
    duration_months: Optional[int] = None  # For gym membership/PT
    session_name: Optional[str] = None  # For session bookings
    sessions_count: Optional[int] = None  # For session bookings (number of sessions)
    days_count: Optional[int] = None  # For daily passes
    starting_date: Optional[date] = None  # For session/daily pass bookings



NOTIFICATION_TEMPLATES = {
    BookingType.GYM_MEMBERSHIP: {
        "title": "New Membership! 🎉",
        "body": "{client_name} purchased a {duration}M gym membership",
    },
    BookingType.PERSONAL_TRAINING: {
        "title": "New PT Client! 💪",
        "body": "{client_name} purchased {duration}M PT sessions",
    },
    BookingType.SESSION: {
        "title": "New Session Booking! 🏋️",
        "body": "{client_name} booked {sessions_count} {session_name} session(s) starting {starting_date}",
    },
    BookingType.DAILY_PASS: {
        "title": "New DailyPass Booking! 🎫",
        "body": "{client_name} purchased a {days}-day pass starting {starting_date}",
    },
    BookingType.DAILYPASS_CHECKIN: {
        "title": "DailyPass Check-in ✅",
        "body": "{client_name} checked in with DailyPass",
    },
    BookingType.SESSION_CHECKIN: {
        "title": "Session Check-in ✅",
        "body": "{client_name} checked in for {session_name} session",
    },
    BookingType.SCAN_ALERT: {
        "title": "Scan Rejected ⚠️",
        "body": "{client_name} tried to check in but was denied — {reason}. Please verify with the client before allowing entry.",
    },
}


class OwnerNotificationService:
   
    def __init__(self):
        self.push_client = PushClient()

    async def get_owner_tokens(
        self, db: AsyncSession, gym_id: int
    ) -> tuple[List[str], int]:
        """
        Get all expo tokens for a gym's owner.

        Returns:
            Tuple of (list of expo tokens, owner_id)
        """
        # Get gym to find owner_id
        gym = (
            await db.execute(
                select(Gym).where(Gym.gym_id == gym_id)
            )
        ).scalars().first()

        if not gym or not gym.owner_id:
            logger.warning(
                "OWNER_NOTIFICATION_NO_GYM_OWNER",
                extra={"gym_id": gym_id, "reason": "Gym not found or no owner_id"}
            )
            return [], 0

        # Get owner's expo tokens
        owner = (
            await db.execute(
                select(GymOwner).where(GymOwner.owner_id == gym.owner_id)
            )
        ).scalars().first()

        if not owner:
            logger.warning(
                "OWNER_NOTIFICATION_OWNER_NOT_FOUND",
                extra={"gym_id": gym_id, "owner_id": gym.owner_id}
            )
            return [], gym.owner_id

        tokens = owner.expo_token
        if not tokens:
            logger.info(
                "OWNER_NOTIFICATION_NO_TOKENS",
                extra={
                    "gym_id": gym_id,
                    "owner_id": gym.owner_id,
                    "reason": "Owner has no expo tokens (may not have app installed)"
                }
            )
            return [], gym.owner_id

        # Ensure tokens is a list
        if not isinstance(tokens, list):
            tokens = [tokens]

        # Filter out None/empty tokens
        tokens = [t for t in tokens if t]

        return tokens, gym.owner_id

    async def get_client_name(self, db: AsyncSession, client_id: int) -> str:
        """Get client's name for notification message."""
        client = (
            await db.execute(
                select(Client).where(Client.client_id == client_id)
            )
        ).scalars().first()

        if client and client.name:
            return client.name
        return "A customer"

    def build_notification_message(
        self,
        notification: BookingNotification,
        client_name: str,
    ) -> Dict[str, str]:
        """Build notification title and body based on booking type."""
        template = NOTIFICATION_TEMPLATES.get(notification.booking_type)
        if not template:
            return {
                "title": "New Booking!",
                "body": f"{client_name} made a booking for ₹{int(notification.amount)}",
            }

        # Format amount as integer (no decimals)
        amount_str = str(int(notification.amount))

        # Build body based on booking type
        if notification.booking_type == BookingType.GYM_MEMBERSHIP:
            body = template["body"].format(
                client_name=client_name,
                duration=notification.duration_months or 1,
                amount=amount_str,
            )
        elif notification.booking_type == BookingType.PERSONAL_TRAINING:
            body = template["body"].format(
                client_name=client_name,
                duration=notification.duration_months or 1,
                amount=amount_str,
            )
        elif notification.booking_type == BookingType.SESSION:
            starting_date_str = (
                notification.starting_date.strftime("%d %b")
                if notification.starting_date
                else "soon"
            )
            body = template["body"].format(
                client_name=client_name,
                sessions_count=notification.sessions_count or 1,
                session_name=notification.session_name or "session",
                starting_date=starting_date_str,
                amount=amount_str,
            )
        elif notification.booking_type == BookingType.DAILY_PASS:
            starting_date_str = (
                notification.starting_date.strftime("%d %b")
                if notification.starting_date
                else "soon"
            )
            body = template["body"].format(
                client_name=client_name,
                days=notification.days_count or 1,
                starting_date=starting_date_str,
                amount=amount_str,
            )
        elif notification.booking_type == BookingType.DAILYPASS_CHECKIN:
            body = template["body"].format(client_name=client_name)
        elif notification.booking_type == BookingType.SESSION_CHECKIN:
            body = template["body"].format(
                client_name=client_name,
                session_name=notification.session_name or "session",
            )
        elif notification.booking_type == BookingType.SCAN_ALERT:
            body = template["body"].format(
                client_name=client_name,
                reason=notification.session_name or "unknown issue",
            )
        else:
            body = f"{client_name} made a booking for ₹{amount_str}"

        return {
            "title": template["title"],
            "body": body,
        }

    def send_push_notifications(
        self,
        tokens: List[str],
        title: str,
        body: str,
        data: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Send push notifications to multiple tokens.

        Returns dict with results and any invalid tokens found.
        """
        if not tokens:
            return {"sent": 0, "failed": 0, "invalid_tokens": []}

        messages = []
        for token in tokens:
            messages.append(
                PushMessage(
                    to=token,
                    title=title,
                    body=body,
                    sound="default",
                    priority="high",
                    channel_id="booking_notifications",
                    data=data or {},
                )
            )

        invalid_tokens = []
        sent_count = 0
        failed_count = 0

        try:
            responses = self.push_client.publish_multiple(messages)

            for token, response in zip(tokens, responses):
                if response.status == "ok":
                    sent_count += 1
                else:
                    failed_count += 1
                    # Check for DeviceNotRegistered
                    error_type = (
                        getattr(response.details, "error", None)
                        if response.details
                        else None
                    )
                    if error_type == "DeviceNotRegistered":
                        invalid_tokens.append(token)
                        logger.info(
                            "OWNER_TOKEN_INVALID",
                            extra={"token": token[:20] + "...", "error": error_type}
                        )

        except PushServerError as exc:
            logger.error(
                "OWNER_NOTIFICATION_PUSH_ERROR",
                extra={"error": repr(exc), "tokens_count": len(tokens)}
            )
            return {
                "sent": 0,
                "failed": len(tokens),
                "invalid_tokens": [],
                "error": str(exc),
            }

        return {
            "sent": sent_count,
            "failed": failed_count,
            "invalid_tokens": invalid_tokens,
        }

    async def cleanup_invalid_tokens(
        self, db: AsyncSession, owner_id: int, invalid_tokens: List[str]
    ) -> None:
        """Remove invalid tokens from owner's expo_token list."""
        if not invalid_tokens or not owner_id:
            return

        try:
            owner = (
                await db.execute(
                    select(GymOwner).where(GymOwner.owner_id == owner_id)
                )
            ).scalars().first()

            if not owner or not owner.expo_token:
                return

            current_tokens = (
                owner.expo_token
                if isinstance(owner.expo_token, list)
                else [owner.expo_token]
            )
            updated_tokens = [t for t in current_tokens if t and t not in invalid_tokens]

            await db.execute(
                update(GymOwner)
                .where(GymOwner.owner_id == owner_id)
                .values(expo_token=updated_tokens if updated_tokens else None)
            )
            await db.commit()

            logger.info(
                "OWNER_TOKENS_CLEANED",
                extra={
                    "owner_id": owner_id,
                    "removed_count": len(invalid_tokens),
                    "remaining_count": len(updated_tokens),
                }
            )

        except Exception as exc:
            logger.warning(
                "OWNER_TOKEN_CLEANUP_FAILED",
                extra={"owner_id": owner_id, "error": repr(exc)}
            )

    async def send_booking_notification(
        self,
        db: AsyncSession,
        notification: BookingNotification,
    ) -> Dict[str, Any]:
        """
        Send booking notification to gym owner.

        This is the main method to call from processors.
        """
        try:
            # Get owner's expo tokens
            tokens, owner_id = await self.get_owner_tokens(db, notification.gym_id)
            if not tokens:
                return {
                    "success": False,
                    "reason": "no_tokens",
                    "gym_id": notification.gym_id,
                    "owner_id": owner_id,
                }

            # Get client name
            client_name = await self.get_client_name(db, notification.client_id)

            # Build notification message
            message = self.build_notification_message(notification, client_name)

            # Prepare notification data
            data = {
                "type": "booking_notification",
                "booking_type": notification.booking_type.value,
                "gym_id": notification.gym_id,
                "client_id": notification.client_id,
                "amount": notification.amount,
            }

            # Send notifications
            result = self.send_push_notifications(
                tokens=tokens,
                title=message["title"],
                body=message["body"],
                data=data,
            )

            # Cleanup invalid tokens if any
            if result.get("invalid_tokens"):
                await self.cleanup_invalid_tokens(
                    db, owner_id, result["invalid_tokens"]
                )

            logger.info(
                "OWNER_BOOKING_NOTIFICATION_SENT",
                extra={
                    "gym_id": notification.gym_id,
                    "owner_id": owner_id,
                    "booking_type": notification.booking_type.value,
                    "client_id": notification.client_id,
                    "amount": notification.amount,
                    "sent": result.get("sent", 0),
                    "failed": result.get("failed", 0),
                }
            )

            return {
                "success": True,
                "gym_id": notification.gym_id,
                "owner_id": owner_id,
                "sent": result.get("sent", 0),
                "failed": result.get("failed", 0),
            }

        except Exception as exc:
            logger.error(
                "OWNER_BOOKING_NOTIFICATION_ERROR",
                extra={
                    "gym_id": notification.gym_id,
                    "client_id": notification.client_id,
                    "booking_type": notification.booking_type.value,
                    "error": repr(exc),
                }
            )
            return {
                "success": False,
                "reason": "exception",
                "error": str(exc),
            }
