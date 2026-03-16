"""
Celery tasks for auto-settlement & payout system.

Cron Schedule:
1. daily_reconciliation     → Every day at 11:00 AM IST
   Fetches Razorpay settlements, matches payments, creates payout records.

2. process_gym_membership_payouts → Every day at 10:00 AM IST
   Processes next-day gym membership payouts.

3. process_monday_bulk_payouts → Every Monday at 10:00 AM IST
   Processes weekly bulk payouts for daily_pass and sessions.

4. retry_failed_payouts → Every day at 3:00 PM IST
   Retries any failed RazorpayX payout transfers.
"""

from __future__ import annotations

import logging

from app.celery_app import celery_app
from app.models.async_database import create_celery_async_sessionmaker
from app.utils.celery_asyncio import run_in_worker_loop
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger("auto_settlements.tasks")


async def _run_reconciliation() -> dict:
    """Async reconciliation logic called from Celery task."""
    from .reconciliation_engine import ReconciliationEngine

    SessionLocal = create_celery_async_sessionmaker()
    async with SessionLocal() as db:
        try:
            engine = ReconciliationEngine(db)
            result = await engine.run_daily_reconciliation()
            logger.info("Daily reconciliation completed: %s", result)
            return result
        except Exception as exc:
            logger.exception("Daily reconciliation failed: %s", exc)
            return {"status": "error", "error": str(exc)}


async def _run_payouts(payout_type: str | None = None) -> dict:
    """Async payout processing logic called from Celery task."""
    from .auto_payout_engine import AutoPayoutEngine

    SessionLocal = create_celery_async_sessionmaker()
    async with SessionLocal() as db:
        try:
            engine = AutoPayoutEngine(db)
            result = await engine.process_scheduled_payouts(payout_type=payout_type)
            logger.info("Payout processing completed: %s", result)
            return result
        except Exception as exc:
            logger.exception("Payout processing failed: %s", exc)
            return {"status": "error", "error": str(exc)}


async def _retry_failed() -> dict:
    """Async retry logic called from Celery task."""
    from .auto_payout_engine import AutoPayoutEngine

    SessionLocal = create_celery_async_sessionmaker()
    async with SessionLocal() as db:
        try:
            engine = AutoPayoutEngine(db)
            result = await engine.retry_failed_transfers()
            logger.info("Retry failed transfers completed: %s", result)
            return result
        except Exception as exc:
            logger.exception("Retry failed transfers error: %s", exc)
            return {"status": "error", "error": str(exc)}


# ─── Celery Tasks ─────────────────────────────────────────────────────────────


@celery_app.task(
    name="settlements.daily_reconciliation",
    bind=True,
    max_retries=2,
    default_retry_delay=300,
)
def daily_reconciliation(self):
    """
    Daily reconciliation task.
    Fetches yesterday's Razorpay settlements, matches with our payments,
    calculates deductions, and creates scheduled payout records.

    Schedule: Every day at 11:00 AM IST
    """
    logger.info("Starting daily reconciliation task")
    try:
        result = run_in_worker_loop(_run_reconciliation())
        return result
    except Exception as exc:
        logger.exception("Daily reconciliation task failed: %s", exc)
        raise self.retry(exc=exc)


@celery_app.task(
    name="settlements.process_gym_membership_payouts",
    bind=True,
    max_retries=2,
    default_retry_delay=300,
)
def process_gym_membership_payouts(self):
    """
    Process next-day gym membership payouts.
    Picks up gym_membership payouts scheduled for today and initiates transfers.

    Schedule: Every day at 10:00 AM IST
    """
    logger.info("Starting gym membership payout processing")
    try:
        result = run_in_worker_loop(_run_payouts(payout_type="immediate"))
        return result
    except Exception as exc:
        logger.exception("Gym membership payout task failed: %s", exc)
        raise self.retry(exc=exc)


@celery_app.task(
    name="settlements.process_monday_bulk_payouts",
    bind=True,
    max_retries=2,
    default_retry_delay=300,
)
def process_monday_bulk_payouts(self):
    """
    Process Monday bulk payouts for daily_pass and sessions.
    Picks up bulk_monday payouts scheduled for today and initiates transfers.

    Schedule: Every Monday at 10:00 AM IST
    """
    logger.info("Starting Monday bulk payout processing")
    try:
        result = run_in_worker_loop(_run_payouts(payout_type="bulk_monday"))
        return result
    except Exception as exc:
        logger.exception("Monday bulk payout task failed: %s", exc)
        raise self.retry(exc=exc)


@celery_app.task(
    name="settlements.retry_failed_payouts",
    bind=True,
    max_retries=1,
    default_retry_delay=600,
)
def retry_failed_payouts(self):
    """
    Retry failed RazorpayX payout transfers.

    Schedule: Every day at 3:00 PM IST
    """
    logger.info("Starting failed payout retry")
    try:
        result = run_in_worker_loop(_retry_failed())
        return result
    except Exception as exc:
        logger.exception("Retry failed payouts task error: %s", exc)
        raise self.retry(exc=exc)


# ─── Celery Beat Schedule ────────────────────────────────────────────────────

SETTLEMENT_BEAT_SCHEDULE = {
    "daily-reconciliation": {
        "task": "settlements.daily_reconciliation",
        "schedule": {
            "__type__": "crontab",
            "minute": "0",
            "hour": "11",           # 11:00 AM IST
        },
        "options": {"queue": "payments"},
    },
    "gym-membership-daily-payout": {
        "task": "settlements.process_gym_membership_payouts",
        "schedule": {
            "__type__": "crontab",
            "minute": "0",
            "hour": "10",           # 10:00 AM IST
        },
        "options": {"queue": "payments"},
    },
    "monday-bulk-payout": {
        "task": "settlements.process_monday_bulk_payouts",
        "schedule": {
            "__type__": "crontab",
            "minute": "0",
            "hour": "10",           # 10:00 AM IST
            "day_of_week": "1",     # Monday
        },
        "options": {"queue": "payments"},
    },
    "retry-failed-payouts": {
        "task": "settlements.retry_failed_payouts",
        "schedule": {
            "__type__": "crontab",
            "minute": "0",
            "hour": "15",           # 3:00 PM IST
        },
        "options": {"queue": "payments"},
    },
}
