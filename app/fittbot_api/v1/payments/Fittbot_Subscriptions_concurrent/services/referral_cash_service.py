"""
Service for crediting referrer Fittbot Cash when a referee purchases a 1-year subscription.
Called from both Razorpay and RevenueCat subscription processors (best-effort, idempotent).
"""

import logging
from datetime import datetime

from sqlalchemy import select

from app.models.fittbot_models import (
    ReferralFittbotCash,
    ReferralFittbotCashLogs,
    ReferralMapping,
)
from app.models.async_database import create_celery_async_sessionmaker

logger = logging.getLogger("payments.referral_cash_service")

REFERRAL_YEARLY_CASH_AMOUNT = 100
REASON_PREFIX = "referral_yearly_sub"


def _is_yearly_plan(plan_name: str) -> bool:
    """Check if the plan is a 1-year plan based on product_id/SKU naming."""
    if not plan_name:
        return False
    lower = plan_name.lower()
    # Razorpay old SKUs: "diamond_12m:base", "twelve_month_plan:..."
    # RevenueCat new SKUs: "premium_monthly:plan-yearly"
    # Note: "half-yearly" contains "yearly", so exclude it first
    if "half-yearly" in lower or "half_yearly" in lower:
        return False
    return "12" in lower or "twelve" in lower or "yearly" in lower


async def maybe_credit_referrer_for_yearly_subscription(
    referee_id: str,
    subscription_id: str,
    plan_name: str,
) -> None:
    """
    Credit 100 Fittbot Cash to the referrer when their referee purchases a 1-year subscription.

    - Best-effort: logs warnings on failure, never raises.
    - Idempotent: checks ReferralFittbotCashLogs for existing entry with matching subscription_id.
    """
    if not referee_id or not subscription_id:
        return

    if not _is_yearly_plan(plan_name):
        return

    try:
        async_session_maker = create_celery_async_sessionmaker()
        async with async_session_maker() as db:
            # Find who referred this client
            stmt = select(ReferralMapping).where(
                ReferralMapping.referee_id == int(referee_id)
            )
            result = await db.execute(stmt)
            mapping = result.scalars().first()

            if not mapping:
                logger.info(
                    "[REFERRAL_YEARLY_SKIP] No referrer found for referee",
                    extra={"referee_id": referee_id, "subscription_id": subscription_id},
                )
                return

            referrer_id = mapping.referrer_id
            reason = f"{REASON_PREFIX}:{subscription_id}"

            # Idempotency check: already credited for this subscription?
            stmt = select(ReferralFittbotCashLogs).where(
                ReferralFittbotCashLogs.client_id == referrer_id,
                ReferralFittbotCashLogs.reason == reason,
            )
            result = await db.execute(stmt)
            existing_log = result.scalars().first()

            if existing_log:
                logger.info(
                    "[REFERRAL_YEARLY_SKIP] Already credited for this subscription",
                    extra={
                        "referrer_id": referrer_id,
                        "referee_id": referee_id,
                        "subscription_id": subscription_id,
                    },
                )
                return

            # Credit referrer: create-or-update ReferralFittbotCash
            stmt = select(ReferralFittbotCash).where(
                ReferralFittbotCash.client_id == referrer_id
            )
            result = await db.execute(stmt)
            referrer_cash = result.scalars().first()

            if referrer_cash:
                referrer_cash.fittbot_cash += REFERRAL_YEARLY_CASH_AMOUNT
                referrer_cash.updated_at = datetime.now()
            else:
                referrer_cash = ReferralFittbotCash(
                    client_id=referrer_id,
                    fittbot_cash=REFERRAL_YEARLY_CASH_AMOUNT,
                )
                db.add(referrer_cash)

            # Audit log
            log_entry = ReferralFittbotCashLogs(
                client_id=referrer_id,
                fittbot_cash=REFERRAL_YEARLY_CASH_AMOUNT,
                reason=reason,
                created_at=datetime.now(),
                updated_at=datetime.now(),
            )
            db.add(log_entry)

            await db.commit()

            logger.info(
                "[REFERRAL_YEARLY_CREDITED] Referrer credited for referee 1-year subscription",
                extra={
                    "referrer_id": referrer_id,
                    "referee_id": referee_id,
                    "subscription_id": subscription_id,
                    "amount": REFERRAL_YEARLY_CASH_AMOUNT,
                    "plan_name": plan_name,
                },
            )

    except Exception as exc:
        logger.warning(
            "[REFERRAL_YEARLY_FAILED] Error crediting referrer for yearly subscription",
            extra={
                "referee_id": referee_id,
                "subscription_id": subscription_id,
                "plan_name": plan_name,
                "error": str(exc),
            },
        )
