"""
RazorpayX Payout Webhook Handler.

Handles webhook events for payout status updates:
- payout.processed  → Mark transfer as credited, update payouts
- payout.failed     → Mark transfer as failed, revert payouts to scheduled
- payout.reversed   → Mark transfer as reversed, create reversal records

Separate from existing webhook handlers to avoid conflicts.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Request, HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.async_database import get_async_db
from app.models.fittbot_payments_models import (
    BulkTransfer,
    Payout,
    PayoutEvent,
)
from app.fittbot_api.v1.payments.utils.webhook_verifier import verify_razorpay_signature
from app.fittbot_api.v1.payments.config.settings import get_payment_settings

logger = logging.getLogger("auto_settlements.webhook")

router = APIRouter(
    prefix="/auto-settlements/webhooks",
    tags=["Auto Settlements Webhooks"],
)

IST = timezone(timedelta(hours=5, minutes=30))


def _now_ist() -> datetime:
    return datetime.now(IST)


@router.post("/razorpayx-payout")
async def handle_razorpayx_payout_webhook(request: Request):
    """
    Handle RazorpayX payout webhook events.
    Events: payout.processed, payout.failed, payout.reversed
    """
    settings = get_payment_settings()

    # Get raw body and signature
    body = await request.body()
    signature = request.headers.get("X-Razorpay-Signature", "")

    # Verify signature
    if settings.razorpayx_webhook_secret:
        if not verify_razorpay_signature(
            body.decode(), signature, settings.razorpayx_webhook_secret
        ):
            logger.warning("Invalid RazorpayX webhook signature")
            raise HTTPException(status_code=401, detail="Invalid signature")

    try:
        payload = json.loads(body.decode())
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="Invalid JSON")

    event_type = payload.get("event", "")
    payout_entity = (
        payload.get("payload", {}).get("payout", {}).get("entity", {})
    )

    if not payout_entity:
        logger.info("No payout entity in webhook, ignoring")
        return {"status": "ignored", "reason": "no_payout_entity"}

    rp_payout_id = payout_entity.get("id", "")
    reference_id = payout_entity.get("reference_id", "")
    utr = payout_entity.get("utr")
    failure_reason = payout_entity.get("failure_reason")

    logger.info(
        "RazorpayX webhook: event=%s, payout_id=%s, ref=%s",
        event_type, rp_payout_id, reference_id,
    )

    # Get async DB session
    from app.models.async_database import get_async_sessionmaker
    SessionLocal = get_async_sessionmaker()
    async with SessionLocal() as db:
        try:
            # Find the bulk transfer by razorpay_payout_id or transfer_ref
            transfer = None
            if rp_payout_id:
                result = await db.execute(
                    select(BulkTransfer).where(
                        BulkTransfer.razorpay_payout_id == rp_payout_id
                    )
                )
                transfer = result.scalars().first()

            if not transfer and reference_id:
                # Strip _retry suffix if present
                ref = reference_id.replace("_retry", "")
                result = await db.execute(
                    select(BulkTransfer).where(BulkTransfer.transfer_ref == ref)
                )
                transfer = result.scalars().first()

            if not transfer:
                logger.warning(
                    "No transfer found for payout_id=%s, ref=%s", rp_payout_id, reference_id
                )
                return {"status": "ignored", "reason": "transfer_not_found"}

            # Process based on event type
            if event_type == "payout.processed":
                await _handle_payout_processed(db, transfer, utr, payout_entity)
            elif event_type == "payout.failed":
                await _handle_payout_failed(db, transfer, failure_reason, payout_entity)
            elif event_type == "payout.reversed":
                await _handle_payout_reversed(db, transfer, failure_reason, payout_entity)
            else:
                logger.info("Unhandled payout event: %s", event_type)
                return {"status": "ignored", "reason": f"unhandled_event: {event_type}"}

            await db.commit()

            return {
                "status": "processed",
                "event_type": event_type,
                "transfer_ref": transfer.transfer_ref,
            }

        except Exception as exc:
            await db.rollback()
            logger.exception("Webhook processing failed: %s", exc)
            raise HTTPException(status_code=500, detail="Webhook processing failed")


async def _handle_payout_processed(
    db: AsyncSession,
    transfer: BulkTransfer,
    utr: str | None,
    payout_entity: dict,
):
    """Handle payout.processed: Mark transfer and payouts as credited."""
    transfer.status = "credited"
    transfer.utr = utr
    transfer.credited_at = _now_ist()

    # Update all linked payouts
    result = await db.execute(
        select(Payout).where(Payout.bulk_transfer_id == transfer.id)
    )
    for payout in result.scalars().all():
        old_status = payout.status
        payout.status = "credited"
        payout.credited_at = _now_ist()
        db.add(PayoutEvent(
            payout_id=payout.id,
            from_status=old_status,
            to_status="credited",
            actor="webhook",
            notes=f"RazorpayX payout processed. UTR={utr}",
            event_data={
                "razorpay_payout_id": payout_entity.get("id"),
                "utr": utr,
            },
        ))

    logger.info(
        "Transfer %s credited: UTR=%s, amount=₹%s",
        transfer.transfer_ref, utr, transfer.total_net,
    )


async def _handle_payout_failed(
    db: AsyncSession,
    transfer: BulkTransfer,
    failure_reason: str | None,
    payout_entity: dict,
):
    """Handle payout.failed: Mark transfer as failed, revert payouts to scheduled for retry."""
    transfer.status = "failed"
    transfer.failure_reason = failure_reason

    # Revert payouts to scheduled so they can be picked up again
    result = await db.execute(
        select(Payout).where(Payout.bulk_transfer_id == transfer.id)
    )
    for payout in result.scalars().all():
        old_status = payout.status
        payout.status = "scheduled"
        payout.bulk_transfer_id = None
        payout.transfer_ref = None
        payout.initiated_at = None
        db.add(PayoutEvent(
            payout_id=payout.id,
            from_status=old_status,
            to_status="scheduled",
            actor="webhook",
            notes=f"RazorpayX payout failed: {failure_reason}. Reverted for retry.",
            event_data={
                "razorpay_payout_id": payout_entity.get("id"),
                "failure_reason": failure_reason,
            },
        ))

    logger.warning(
        "Transfer %s failed: reason=%s", transfer.transfer_ref, failure_reason
    )


async def _handle_payout_reversed(
    db: AsyncSession,
    transfer: BulkTransfer,
    failure_reason: str | None,
    payout_entity: dict,
):
    """Handle payout.reversed: Mark as reversed (requires manual intervention)."""
    transfer.status = "reversed"
    transfer.failure_reason = f"REVERSED: {failure_reason or 'Unknown reason'}"

    result = await db.execute(
        select(Payout).where(Payout.bulk_transfer_id == transfer.id)
    )
    for payout in result.scalars().all():
        old_status = payout.status
        payout.status = "failed"
        db.add(PayoutEvent(
            payout_id=payout.id,
            from_status=old_status,
            to_status="failed",
            actor="webhook",
            notes=f"RazorpayX payout reversed: {failure_reason}. Manual review required.",
            event_data={
                "razorpay_payout_id": payout_entity.get("id"),
                "failure_reason": failure_reason,
            },
        ))

    logger.error(
        "Transfer %s REVERSED: reason=%s. Manual review required.",
        transfer.transfer_ref, failure_reason,
    )
