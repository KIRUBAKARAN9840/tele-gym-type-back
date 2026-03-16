# app/tasks/notification_tasks.py

"""
Celery tasks for sending owner push notifications on bookings.

These tasks run asynchronously (fire-and-forget) so notification failures
never affect payment success.
"""

import logging
from datetime import date
from typing import Optional

from app.celery_app import celery_app
from app.models.async_database import create_celery_async_sessionmaker
from app.services.owner_notification_service import (
    BookingNotification,
    BookingType,
    OwnerNotificationService,
)
from app.utils.celery_asyncio import get_worker_loop

logger = logging.getLogger("tasks.notifications")


@celery_app.task(
    name="notifications.send_owner_booking",
    bind=True,
    max_retries=3,
    default_retry_delay=10,
    autoretry_for=(Exception,),
    retry_backoff=True,
    retry_backoff_max=60,
)
def send_owner_booking_notification_task(
    self,
    booking_type: str,
    gym_id: int,
    client_id: int,
    amount: float,
    duration_months: Optional[int] = None,
    session_name: Optional[str] = None,
    sessions_count: Optional[int] = None,
    days_count: Optional[int] = None,
    starting_date: Optional[str] = None,
):
    
    try:
        # Convert string to BookingType enum
        try:
            booking_type_enum = BookingType(booking_type)
        except ValueError:
            logger.error(
                "NOTIFICATION_TASK_INVALID_TYPE",
                extra={"booking_type": booking_type, "gym_id": gym_id}
            )
            return {"success": False, "reason": "invalid_booking_type"}

        # Parse starting_date if provided (passed as ISO string for Celery serialization)
        parsed_starting_date = None
        if starting_date:
            parsed_starting_date = date.fromisoformat(starting_date)

        # Create notification object
        notification = BookingNotification(
            booking_type=booking_type_enum,
            gym_id=gym_id,
            client_id=client_id,
            amount=amount,
            duration_months=duration_months,
            session_name=session_name,
            sessions_count=sessions_count,
            days_count=days_count,
            starting_date=parsed_starting_date,
        )

        # Run async notification sending
        loop = get_worker_loop()
        result = loop.run_until_complete(
            _send_notification_async(notification)
        )

        logger.info(
            "NOTIFICATION_TASK_COMPLETED",
            extra={
                "gym_id": gym_id,
                "client_id": client_id,
                "booking_type": booking_type,
                "result": result,
            }
        )

        return result

    except Exception as exc:
        logger.error(
            "NOTIFICATION_TASK_ERROR",
            extra={
                "gym_id": gym_id,
                "client_id": client_id,
                "booking_type": booking_type,
                "error": repr(exc),
                "retry_count": self.request.retries,
            }
        )
        # Let Celery handle retries
        raise


async def _send_notification_async(notification: BookingNotification):
    """Helper to run notification sending with async DB session."""
    SessionLocal = create_celery_async_sessionmaker()
    async with SessionLocal() as db:
        service = OwnerNotificationService()
        return await service.send_booking_notification(db, notification)


# ═══════════════════════════════════════════════════════════════════════════════
# CONVENIENCE FUNCTIONS - Call these from processors
# ═══════════════════════════════════════════════════════════════════════════════

def queue_membership_notification(
    gym_id: int,
    client_id: int,
    amount: float,
    duration_months: int,
    is_personal_training: bool = False,
) -> None:

    booking_type = (
        BookingType.PERSONAL_TRAINING.value
        if is_personal_training
        else BookingType.GYM_MEMBERSHIP.value
    )

    send_owner_booking_notification_task.delay(
        booking_type=booking_type,
        gym_id=gym_id,
        client_id=client_id,
        amount=amount,
        duration_months=duration_months,
    )

    logger.info(
        "NOTIFICATION_QUEUED_MEMBERSHIP",
        extra={
            "gym_id": gym_id,
            "client_id": client_id,
            "amount": amount,
            "duration_months": duration_months,
            "is_personal_training": is_personal_training,
        }
    )


def queue_session_notification(
    gym_id: int,
    client_id: int,
    amount: float,
    session_name: str,
    sessions_count: int = 1,
    starting_date: Optional[date] = None,
) -> None:

    send_owner_booking_notification_task.delay(
        booking_type=BookingType.SESSION.value,
        gym_id=gym_id,
        client_id=client_id,
        amount=amount,
        session_name=session_name,
        sessions_count=sessions_count,
        starting_date=starting_date.isoformat() if starting_date else None,
    )

    logger.info(
        "NOTIFICATION_QUEUED_SESSION",
        extra={
            "gym_id": gym_id,
            "client_id": client_id,
            "amount": amount,
            "session_name": session_name,
            "sessions_count": sessions_count,
            "starting_date": starting_date.isoformat() if starting_date else None,
        }
    )


def queue_dailypass_notification(
    gym_id: int,
    client_id: int,
    amount: float,
    days_count: int,
    starting_date: Optional[date] = None,
) -> None:

    send_owner_booking_notification_task.delay(
        booking_type=BookingType.DAILY_PASS.value,
        gym_id=gym_id,
        client_id=client_id,
        amount=amount,
        days_count=days_count,
        starting_date=starting_date.isoformat() if starting_date else None,
    )

    logger.info(
        "NOTIFICATION_QUEUED_DAILYPASS",
        extra={
            "gym_id": gym_id,
            "client_id": client_id,
            "amount": amount,
            "days_count": days_count,
            "starting_date": starting_date.isoformat() if starting_date else None,
        }
    )


def queue_dailypass_checkin_notification(
    gym_id: int,
    client_id: int,
) -> None:

    send_owner_booking_notification_task.delay(
        booking_type=BookingType.DAILYPASS_CHECKIN.value,
        gym_id=gym_id,
        client_id=client_id,
        amount=0,
    )

    logger.info(
        "NOTIFICATION_QUEUED_DAILYPASS_CHECKIN",
        extra={"gym_id": gym_id, "client_id": client_id}
    )


def queue_session_checkin_notification(
    gym_id: int,
    client_id: int,
    session_name: str,
) -> None:

    send_owner_booking_notification_task.delay(
        booking_type=BookingType.SESSION_CHECKIN.value,
        gym_id=gym_id,
        client_id=client_id,
        amount=0,
        session_name=session_name,
    )

    logger.info(
        "NOTIFICATION_QUEUED_SESSION_CHECKIN",
        extra={"gym_id": gym_id, "client_id": client_id, "session_name": session_name}
    )


def queue_scan_alert_notification(
    gym_id: int,
    client_id: int,
    reason: str,
) -> None:

    send_owner_booking_notification_task.delay(
        booking_type=BookingType.SCAN_ALERT.value,
        gym_id=gym_id,
        client_id=client_id,
        amount=0,
        session_name=reason,
    )

    logger.info(
        "NOTIFICATION_QUEUED_SCAN_ALERT",
        extra={"gym_id": gym_id, "client_id": client_id, "reason": reason}
    )
