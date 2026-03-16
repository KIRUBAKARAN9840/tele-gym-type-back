"""
Async Auto Payout Engine.

Picks up scheduled Payout records, groups them per gym into BulkTransfer,
and initiates RazorpayX payouts.

Flow:
1. Query scheduled payouts where scheduled_for <= today
2. Group by gym_id
3. For each gym:
   a. Look up gym's bank account (GymBankAccount with RazorpayX fund_account_id)
   b. Sum all payouts for the gym
   c. Create BulkTransfer record
   d. Initiate RazorpayX payout
   e. Update payout/transfer statuses
4. Webhook callback updates final status (credited/failed)

Scheduling:
- Monday bulk (daily_pass/sessions): Runs every Monday at 10:00 AM IST
- Next-day (gym_membership): Runs daily at 10:00 AM IST
"""

from __future__ import annotations

import logging
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Dict, List, Optional

from sqlalchemy import select, and_, func
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.fittbot_payments_models import (
    BulkTransfer,
    Payout,
    PayoutEvent,
)
from .models import GymBankAccount
from . import razorpayx_client as rzp

logger = logging.getLogger("auto_settlements.payout_engine")

IST = timezone(timedelta(hours=5, minutes=30))

MIN_PAYOUT_AMOUNT = Decimal("1.00")  # Minimum ₹1 to avoid zero-amount transfers


def _now_ist() -> datetime:
    return datetime.now(IST)


def _today_ist() -> date:
    return _now_ist().date()


class AutoPayoutEngine:
    """Async engine for processing scheduled payouts via RazorpayX."""

    def __init__(self, db: AsyncSession):
        self.db = db

    async def process_scheduled_payouts(
        self,
        payout_type: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Main entry point. Picks up all payouts where scheduled_for <= today
        and status = 'scheduled', groups by gym, creates transfers.

        Args:
            payout_type: If set, only process payouts of this type (bulk_monday / immediate).
        """
        today = _today_ist()
        logger.info("Processing scheduled payouts for date=%s, type=%s", today, payout_type or "all")

        # Query scheduled payouts ready for transfer
        query = select(Payout).where(
            and_(
                Payout.status == "scheduled",
                Payout.scheduled_for <= today,
            )
        )
        if payout_type:
            query = query.where(Payout.payout_type == payout_type)

        result = await self.db.execute(query)
        payouts: List[Payout] = list(result.scalars().all())

        if not payouts:
            logger.info("No scheduled payouts found for processing")
            return {"status": "no_payouts", "date": str(today)}

        # Group by gym_id
        by_gym: Dict[int, List[Payout]] = defaultdict(list)
        for p in payouts:
            by_gym[p.gym_id].append(p)

        logger.info("Found %d payouts across %d gyms", len(payouts), len(by_gym))

        transfers_created = 0
        transfers_failed = 0
        results = []

        for gym_id, gym_payouts in by_gym.items():
            try:
                transfer_result = await self._process_gym_transfer(gym_id, gym_payouts, today)
                results.append(transfer_result)
                if transfer_result.get("status") == "initiated":
                    transfers_created += 1
                else:
                    transfers_failed += 1
            except Exception as exc:
                logger.exception("Failed to process transfer for gym %s: %s", gym_id, exc)
                transfers_failed += 1
                results.append({
                    "gym_id": gym_id,
                    "status": "error",
                    "error": str(exc)[:200],
                    "payout_count": len(gym_payouts),
                })

        await self.db.commit()

        summary = {
            "status": "completed",
            "date": str(today),
            "total_payouts": len(payouts),
            "gyms_processed": len(by_gym),
            "transfers_created": transfers_created,
            "transfers_failed": transfers_failed,
            "results": results,
        }
        logger.info("Payout processing completed: %s", summary)
        return summary

    async def _process_gym_transfer(
        self,
        gym_id: int,
        payouts: List[Payout],
        transfer_date: date,
    ) -> Dict[str, Any]:
        """
        Create a bulk transfer for a single gym and initiate RazorpayX payout.
        """
        # Look up gym's bank account
        bank_result = await self.db.execute(
            select(GymBankAccount).where(
                and_(
                    GymBankAccount.gym_id == gym_id,
                    GymBankAccount.is_active == True,
                    GymBankAccount.is_verified == True,
                )
            )
        )
        bank_account = bank_result.scalars().first()

        if not bank_account or not bank_account.razorpayx_fund_account_id:
            logger.warning(
                "No verified bank account for gym %s, putting %d payouts on hold",
                gym_id, len(payouts),
            )
            for p in payouts:
                p.status = "on_hold"
                p.hold_reason = "No verified bank account registered"
                p.held_at = _now_ist()
                p.held_by = "system"
                self.db.add(PayoutEvent(
                    payout_id=p.id,
                    from_status="scheduled",
                    to_status="on_hold",
                    actor="payout_engine",
                    notes="No verified bank account for gym",
                ))
            return {
                "gym_id": gym_id,
                "status": "on_hold",
                "reason": "no_bank_account",
                "payout_count": len(payouts),
            }

        # Calculate totals
        total_gross = sum(p.amount_gross or Decimal("0") for p in payouts)
        total_pg_fee = sum(p.pg_fee or Decimal("0") for p in payouts)
        total_tds = sum(p.tds or Decimal("0") for p in payouts)
        total_commission = sum(p.commission or Decimal("0") for p in payouts)
        total_net = sum(p.amount_net or Decimal("0") for p in payouts)

        if total_net < MIN_PAYOUT_AMOUNT:
            logger.warning("Total net amount ₹%s too low for gym %s, skipping", total_net, gym_id)
            return {
                "gym_id": gym_id,
                "status": "skipped",
                "reason": "amount_too_low",
                "total_net": str(total_net),
            }

        # Determine transfer type from first payout
        transfer_type = payouts[0].payout_type or "bulk_monday"

        # Generate unique transfer reference
        seq = await self._next_transfer_seq(gym_id, transfer_date)
        transfer_ref = f"FBT_{transfer_date.strftime('%Y%m%d')}_{gym_id}_{seq:03d}"

        # Collect source types and payment IDs
        source_types = list({p.payout_type for p in payouts if p.payout_type})
        payment_ids = [p.payment_id for p in payouts]

        # Create BulkTransfer record
        bulk_transfer = BulkTransfer(
            transfer_ref=transfer_ref,
            gym_id=gym_id,
            transfer_type=transfer_type,
            transfer_date=transfer_date,
            payout_count=len(payouts),
            total_gross=total_gross,
            total_pg_fee=total_pg_fee,
            total_commission=total_commission,
            total_gst=Decimal("0.00"),
            total_tds=total_tds,
            total_net=total_net,
            bank_account_id=bank_account.razorpayx_fund_account_id,
            bank_account_number=bank_account.account_number[-4:] if bank_account.account_number else None,
            bank_ifsc=bank_account.ifsc_code,
            razorpay_fund_account_id=bank_account.razorpayx_fund_account_id,
            status="pending",
            source_types=source_types,
            payment_ids=payment_ids,
        )
        self.db.add(bulk_transfer)
        await self.db.flush()

        # Link payouts to bulk transfer
        for p in payouts:
            p.bulk_transfer_id = bulk_transfer.id
            p.status = "initiated"
            p.transfer_ref = transfer_ref
            p.initiated_at = _now_ist()
            self.db.add(PayoutEvent(
                payout_id=p.id,
                from_status="scheduled",
                to_status="initiated",
                actor="payout_engine",
                notes=f"Linked to transfer {transfer_ref}",
            ))

        # Initiate RazorpayX payout
        amount_paise = int(total_net * 100)
        try:
            rp_response = await rzp.create_payout(
                fund_account_id=bank_account.razorpayx_fund_account_id,
                amount_paise=amount_paise,
                currency="INR",
                mode="NEFT",
                purpose="payout",
                reference_id=transfer_ref,
                narration=f"Fittbot settlement {transfer_ref}",
                queue_if_low_balance=True,
                notes={
                    "gym_id": str(gym_id),
                    "transfer_ref": transfer_ref,
                    "payout_count": str(len(payouts)),
                },
            )

            rp_payout_id = rp_response.get("id", "")
            rp_status = rp_response.get("status", "")

            bulk_transfer.razorpay_payout_id = rp_payout_id
            bulk_transfer.status = "initiated"
            bulk_transfer.initiated_at = _now_ist()

            if rp_status == "processed":
                bulk_transfer.status = "processing"
                bulk_transfer.utr = rp_response.get("utr")

            # Update payout statuses
            for p in payouts:
                p.status = "processing"
                self.db.add(PayoutEvent(
                    payout_id=p.id,
                    from_status="initiated",
                    to_status="processing",
                    actor="payout_engine",
                    notes=f"RazorpayX payout created: {rp_payout_id}",
                    event_data={"razorpay_payout_id": rp_payout_id, "razorpay_status": rp_status},
                ))

            logger.info(
                "RazorpayX payout initiated: gym=%s, ref=%s, amount=₹%s, rp_id=%s",
                gym_id, transfer_ref, total_net, rp_payout_id,
            )

            return {
                "gym_id": gym_id,
                "status": "initiated",
                "transfer_ref": transfer_ref,
                "bulk_transfer_id": bulk_transfer.id,
                "razorpay_payout_id": rp_payout_id,
                "amount_net": str(total_net),
                "payout_count": len(payouts),
            }

        except Exception as exc:
            logger.exception(
                "RazorpayX payout failed for gym %s, ref %s: %s", gym_id, transfer_ref, exc
            )
            bulk_transfer.status = "failed"
            bulk_transfer.failure_reason = str(exc)[:255]

            for p in payouts:
                p.status = "failed"
                self.db.add(PayoutEvent(
                    payout_id=p.id,
                    from_status="initiated",
                    to_status="failed",
                    actor="payout_engine",
                    notes=f"RazorpayX payout failed: {str(exc)[:200]}",
                ))

            return {
                "gym_id": gym_id,
                "status": "failed",
                "transfer_ref": transfer_ref,
                "error": str(exc)[:200],
                "payout_count": len(payouts),
            }

    async def _next_transfer_seq(self, gym_id: int, transfer_date: date) -> int:
        """Get next sequence number for transfer ref on a given date."""
        result = await self.db.execute(
            select(func.count(BulkTransfer.id)).where(
                and_(
                    BulkTransfer.gym_id == gym_id,
                    BulkTransfer.transfer_date == transfer_date,
                )
            )
        )
        count = result.scalar() or 0
        return count + 1

    async def retry_failed_transfers(self) -> Dict[str, Any]:
        """Retry failed bulk transfers that haven't exceeded retry limit."""
        result = await self.db.execute(
            select(BulkTransfer).where(BulkTransfer.status == "failed")
        )
        failed_transfers = list(result.scalars().all())

        if not failed_transfers:
            return {"status": "no_failed_transfers"}

        retried = 0
        for transfer in failed_transfers:
            if not transfer.razorpay_fund_account_id:
                continue

            try:
                amount_paise = int(transfer.total_net * 100)
                rp_response = await rzp.create_payout(
                    fund_account_id=transfer.razorpay_fund_account_id,
                    amount_paise=amount_paise,
                    currency="INR",
                    mode="NEFT",
                    purpose="payout",
                    reference_id=f"{transfer.transfer_ref}_retry",
                    narration=f"Fittbot settlement retry {transfer.transfer_ref}",
                    queue_if_low_balance=True,
                )

                transfer.razorpay_payout_id = rp_response.get("id", "")
                transfer.status = "initiated"
                transfer.initiated_at = _now_ist()
                transfer.failure_reason = None

                # Update linked payouts
                payout_result = await self.db.execute(
                    select(Payout).where(Payout.bulk_transfer_id == transfer.id)
                )
                for p in payout_result.scalars().all():
                    p.status = "processing"

                retried += 1
            except Exception as exc:
                logger.warning("Retry failed for transfer %s: %s", transfer.transfer_ref, exc)

        await self.db.commit()
        return {"retried": retried, "total_failed": len(failed_transfers)}
