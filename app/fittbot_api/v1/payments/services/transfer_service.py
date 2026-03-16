"""
Transfer Service
================
Handles bulk transfers to gym owners.

Schedule:
- Monday: Bulk transfer for daily_pass/session types
- Daily: Immediate transfer for gym_membership/personal_training

Flow:
1. Query payouts where scheduled_for = today and status = 'scheduled'
2. Group by gym_id
3. For each gym:
   a. Create BulkTransfer record
   b. Sum all amounts
   c. Initiate bank transfer via Razorpay Payouts API
   d. Update status to 'initiated'
4. On webhook/confirmation:
   a. Update BulkTransfer status to 'credited'
   b. Update all Payout status to 'credited'
   c. Generate invoice
"""

import logging
import uuid
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Dict, List, Optional
from zoneinfo import ZoneInfo

import razorpay
from sqlalchemy import and_, func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.fittbot_payments_models import (
    BulkTransfer,
    Invoice,
    Payment,
    Payout,
    PayoutEvent,
)

logger = logging.getLogger("payments.transfer")

IST = ZoneInfo("Asia/Kolkata")


def _now_ist() -> datetime:
    return datetime.now(IST)


def _today_ist() -> date:
    return _now_ist().date()


def _generate_transfer_ref(gym_id: int, transfer_date: date, seq: int = 1) -> str:
    """Generate unique transfer reference: FBT_YYYYMMDD_GYMID_SEQ"""
    return f"FBT_{transfer_date.strftime('%Y%m%d')}_{gym_id}_{seq:03d}"


def _generate_invoice_number(gym_id: int, invoice_date: date) -> str:
    """Generate invoice number: FBT/YYYY-YY/GYM/SEQ"""
    fiscal_year = invoice_date.year if invoice_date.month >= 4 else invoice_date.year - 1
    fy_str = f"{fiscal_year}-{str(fiscal_year + 1)[-2:]}"
    return f"FBT/{fy_str}/{gym_id}/{int(datetime.now().timestamp())}"


class TransferService:
    """
    Handles bulk transfers to gym owners.
    """

    def __init__(
        self,
        razorpay_key_id: str,
        razorpay_key_secret: str,
    ):
        self.razorpay_client = razorpay.Client(auth=(razorpay_key_id, razorpay_key_secret))

    async def run_scheduled_transfers(self, db: AsyncSession) -> Dict:
        """
        Main entry point for transfer job.
        Processes all payouts scheduled for today.
        """
        today = _today_ist()
        logger.info(f"Starting scheduled transfers for {today}")

        # Find all scheduled payouts for today
        result = await db.execute(
            select(Payout)
            .where(
                Payout.scheduled_for == today,
                Payout.status == "scheduled",
            )
        )
        payouts = result.scalars().all()

        if not payouts:
            logger.info("No payouts scheduled for today")
            return {"status": "completed", "message": "No transfers to process", "transfers": 0}

        # Group by gym_id
        gym_payouts: Dict[int, List[Payout]] = {}
        for payout in payouts:
            if payout.gym_id not in gym_payouts:
                gym_payouts[payout.gym_id] = []
            gym_payouts[payout.gym_id].append(payout)

        logger.info(f"Found {len(payouts)} payouts for {len(gym_payouts)} gyms")

        # Process each gym's payouts
        success_count = 0
        failed_count = 0

        for gym_id, payout_list in gym_payouts.items():
            try:
                result = await self._process_gym_transfer(db, gym_id, payout_list, today)
                if result.get("success"):
                    success_count += 1
                else:
                    failed_count += 1
            except Exception as e:
                logger.exception(f"Failed to process transfer for gym {gym_id}: {e}")
                failed_count += 1

        await db.commit()

        return {
            "status": "completed",
            "transfers_successful": success_count,
            "transfers_failed": failed_count,
            "total_gyms": len(gym_payouts),
        }

    async def _process_gym_transfer(
        self,
        db: AsyncSession,
        gym_id: int,
        payouts: List[Payout],
        transfer_date: date,
    ) -> Dict:
        """Process bulk transfer for a single gym."""

        # Calculate totals
        total_gross = sum(p.amount_gross for p in payouts)
        total_pg_fee = sum(p.pg_fee or Decimal("0") for p in payouts)
        total_commission = sum(p.commission or Decimal("0") for p in payouts)
        total_gst = sum(p.gst or Decimal("0") for p in payouts)
        total_tds = sum(p.tds or Decimal("0") for p in payouts)
        total_net = sum(p.amount_net for p in payouts)

        # Determine transfer type
        payout_types = set(p.payout_type for p in payouts if p.payout_type)
        transfer_type = "bulk_monday" if "bulk_monday" in payout_types else "immediate"

        # Collect source types for reference
        payment_ids = [p.payment_id for p in payouts]
        result = await db.execute(
            select(Payment.source_type).where(Payment.id.in_(payment_ids))
        )
        source_types = list(set(row[0] for row in result.all()))

        # Generate transfer reference
        transfer_ref = _generate_transfer_ref(gym_id, transfer_date)

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
            total_gst=total_gst,
            total_tds=total_tds,
            total_net=total_net,
            status="pending",
            source_types=source_types,
            payment_ids=payment_ids,
        )
        db.add(bulk_transfer)
        await db.flush()

        # Update all payouts with bulk_transfer_id
        for payout in payouts:
            payout.bulk_transfer_id = bulk_transfer.id
            payout.transfer_ref = transfer_ref

        # Get gym owner's bank details (placeholder - need to fetch from gym profile)
        bank_details = await self._get_gym_bank_details(db, gym_id)

        if not bank_details:
            logger.warning(f"No bank details found for gym {gym_id}, marking transfer on hold")
            bulk_transfer.status = "failed"
            bulk_transfer.failure_reason = "Bank details not configured"
            for payout in payouts:
                payout.status = "on_hold"
                payout.hold_reason = "Bank details not configured"
                payout.held_at = _now_ist()
                db.add(PayoutEvent(
                    payout_id=payout.id,
                    from_status="scheduled",
                    to_status="on_hold",
                    actor="transfer_service",
                    notes="Bank details not configured for gym",
                ))
            return {"success": False, "reason": "No bank details"}

        # Store bank details in bulk transfer
        bulk_transfer.bank_account_id = bank_details.get("account_id")
        bulk_transfer.bank_account_number = bank_details.get("account_number_masked")
        bulk_transfer.bank_ifsc = bank_details.get("ifsc")

        # Initiate transfer via Razorpay Payouts (or bank transfer API)
        try:
            payout_result = await self._initiate_razorpay_payout(
                amount=int(total_net * 100),  # Amount in paisa
                fund_account_id=bank_details.get("fund_account_id"),
                transfer_ref=transfer_ref,
                gym_id=gym_id,
            )

            if payout_result.get("success"):
                bulk_transfer.razorpay_payout_id = payout_result.get("payout_id")
                bulk_transfer.razorpay_fund_account_id = bank_details.get("fund_account_id")
                bulk_transfer.status = "initiated"
                bulk_transfer.initiated_at = _now_ist()

                # Update all payouts
                for payout in payouts:
                    payout.status = "initiated"
                    payout.initiated_at = _now_ist()
                    db.add(PayoutEvent(
                        payout_id=payout.id,
                        from_status="scheduled",
                        to_status="initiated",
                        actor="transfer_service",
                        notes=f"Bulk transfer initiated: {transfer_ref}",
                        event_data={"razorpay_payout_id": payout_result.get("payout_id")},
                    ))

                logger.info(
                    f"Transfer initiated for gym {gym_id}: "
                    f"ref={transfer_ref}, amount=₹{total_net}, payouts={len(payouts)}"
                )
                return {"success": True, "transfer_ref": transfer_ref}

            else:
                # Transfer initiation failed
                bulk_transfer.status = "failed"
                bulk_transfer.failure_reason = payout_result.get("error", "Unknown error")

                for payout in payouts:
                    payout.status = "failed"
                    db.add(PayoutEvent(
                        payout_id=payout.id,
                        from_status="scheduled",
                        to_status="failed",
                        actor="transfer_service",
                        notes=f"Transfer failed: {payout_result.get('error')}",
                    ))

                return {"success": False, "reason": payout_result.get("error")}

        except Exception as e:
            logger.exception(f"Transfer initiation failed for gym {gym_id}: {e}")
            bulk_transfer.status = "failed"
            bulk_transfer.failure_reason = str(e)
            return {"success": False, "reason": str(e)}

    async def _get_gym_bank_details(self, db: AsyncSession, gym_id: int) -> Optional[Dict]:
        """
        Get gym owner's bank account details.
        This should fetch from your gym/owner profile table.
        Returns Razorpay fund_account_id if available.
        """
        # TODO: Implement actual bank details fetch from gym profile
        # For now, return None to indicate bank details need to be configured

        # Example structure:
        # return {
        #     "account_id": "ba_xxx",
        #     "fund_account_id": "fa_xxx",  # Razorpay Fund Account ID
        #     "account_number_masked": "****1234",
        #     "ifsc": "HDFC0001234",
        #     "beneficiary_name": "Gym Owner Name",
        # }

        logger.warning(f"Bank details lookup not implemented for gym {gym_id}")
        return None

    async def _initiate_razorpay_payout(
        self,
        amount: int,
        fund_account_id: str,
        transfer_ref: str,
        gym_id: int,
    ) -> Dict:
        """
        Initiate payout via Razorpay Payouts API.
        """
        try:
            payout = self.razorpay_client.payout.create({
                "account_number": "your_razorpay_x_account",  # Your RazorpayX account
                "fund_account_id": fund_account_id,
                "amount": amount,
                "currency": "INR",
                "mode": "IMPS",  # or NEFT, RTGS based on amount
                "purpose": "payout",
                "queue_if_low_balance": True,
                "reference_id": transfer_ref,
                "narration": f"Fittbot payout for gym {gym_id}",
            })

            return {
                "success": True,
                "payout_id": payout.get("id"),
                "status": payout.get("status"),
            }

        except razorpay.errors.BadRequestError as e:
            logger.error(f"Razorpay payout failed: {e}")
            return {"success": False, "error": str(e)}
        except Exception as e:
            logger.exception(f"Razorpay payout exception: {e}")
            return {"success": False, "error": str(e)}

    async def handle_payout_webhook(
        self, db: AsyncSession, webhook_data: Dict
    ) -> Dict:
        """
        Handle Razorpay payout webhook.
        Called when payout status changes (processed, reversed, failed).
        """
        event = webhook_data.get("event")
        payload = webhook_data.get("payload", {})
        payout_entity = payload.get("payout", {}).get("entity", {})

        razorpay_payout_id = payout_entity.get("id")
        status = payout_entity.get("status")
        utr = payout_entity.get("utr")
        reference_id = payout_entity.get("reference_id")  # Our transfer_ref

        logger.info(f"Payout webhook received: event={event}, payout_id={razorpay_payout_id}, status={status}")

        # Find bulk transfer
        result = await db.execute(
            select(BulkTransfer).where(
                BulkTransfer.razorpay_payout_id == razorpay_payout_id
            )
        )
        bulk_transfer = result.scalars().first()

        if not bulk_transfer:
            logger.warning(f"BulkTransfer not found for payout {razorpay_payout_id}")
            return {"status": "not_found"}

        # Update based on status
        if status == "processed":
            await self._mark_transfer_credited(db, bulk_transfer, utr)
            return {"status": "credited"}

        elif status == "reversed":
            await self._mark_transfer_reversed(db, bulk_transfer, payout_entity.get("failure_reason"))
            return {"status": "reversed"}

        elif status == "failed":
            await self._mark_transfer_failed(db, bulk_transfer, payout_entity.get("failure_reason"))
            return {"status": "failed"}

        return {"status": "ignored"}

    async def _mark_transfer_credited(
        self, db: AsyncSession, bulk_transfer: BulkTransfer, utr: Optional[str]
    ):
        """Mark transfer and all payouts as credited."""
        bulk_transfer.status = "credited"
        bulk_transfer.utr = utr
        bulk_transfer.credited_at = _now_ist()

        # Update all payouts
        result = await db.execute(
            select(Payout).where(Payout.bulk_transfer_id == bulk_transfer.id)
        )
        payouts = result.scalars().all()

        for payout in payouts:
            old_status = payout.status
            payout.status = "credited"
            payout.credited_at = _now_ist()
            db.add(PayoutEvent(
                payout_id=payout.id,
                from_status=old_status,
                to_status="credited",
                actor="webhook",
                notes=f"Bank transfer completed. UTR: {utr}",
                event_data={"utr": utr},
            ))

        # Generate invoice
        await self._generate_invoice(db, bulk_transfer)

        await db.commit()
        logger.info(f"Transfer {bulk_transfer.transfer_ref} marked as credited. UTR: {utr}")

    async def _mark_transfer_failed(
        self, db: AsyncSession, bulk_transfer: BulkTransfer, reason: Optional[str]
    ):
        """Mark transfer and all payouts as failed."""
        bulk_transfer.status = "failed"
        bulk_transfer.failure_reason = reason

        # Update all payouts
        result = await db.execute(
            select(Payout).where(Payout.bulk_transfer_id == bulk_transfer.id)
        )
        payouts = result.scalars().all()

        for payout in payouts:
            old_status = payout.status
            payout.status = "failed"
            db.add(PayoutEvent(
                payout_id=payout.id,
                from_status=old_status,
                to_status="failed",
                actor="webhook",
                notes=f"Transfer failed: {reason}",
            ))

        await db.commit()
        logger.warning(f"Transfer {bulk_transfer.transfer_ref} failed: {reason}")

    async def _mark_transfer_reversed(
        self, db: AsyncSession, bulk_transfer: BulkTransfer, reason: Optional[str]
    ):
        """Mark transfer as reversed (money returned)."""
        bulk_transfer.status = "reversed"
        bulk_transfer.failure_reason = reason

        # Update all payouts - back to scheduled for retry
        result = await db.execute(
            select(Payout).where(Payout.bulk_transfer_id == bulk_transfer.id)
        )
        payouts = result.scalars().all()

        for payout in payouts:
            old_status = payout.status
            payout.status = "scheduled"  # Can be retried
            payout.bulk_transfer_id = None  # Detach from failed transfer
            db.add(PayoutEvent(
                payout_id=payout.id,
                from_status=old_status,
                to_status="scheduled",
                actor="webhook",
                notes=f"Transfer reversed: {reason}. Ready for retry.",
            ))

        await db.commit()
        logger.warning(f"Transfer {bulk_transfer.transfer_ref} reversed: {reason}")

    async def _generate_invoice(self, db: AsyncSession, bulk_transfer: BulkTransfer):
        """Generate invoice for completed transfer."""
        invoice_number = _generate_invoice_number(bulk_transfer.gym_id, _today_ist())

        invoice = Invoice(
            invoice_number=invoice_number,
            bulk_transfer_id=bulk_transfer.id,
            gym_id=bulk_transfer.gym_id,
            invoice_date=_today_ist(),
            gross_amount=bulk_transfer.total_gross,
            pg_fee=bulk_transfer.total_pg_fee,
            commission_amount=bulk_transfer.total_commission,
            gst_amount=bulk_transfer.total_gst,
            tds_amount=bulk_transfer.total_tds,
            net_amount=bulk_transfer.total_net,
            status="generated",
        )
        db.add(invoice)
        logger.info(f"Invoice {invoice_number} generated for transfer {bulk_transfer.transfer_ref}")


# ═══════════════════════════════════════════════════════════════════════════════
# Utility functions
# ═══════════════════════════════════════════════════════════════════════════════

async def retry_failed_transfer(
    db: AsyncSession,
    bulk_transfer_id: int,
    razorpay_key_id: str,
    razorpay_key_secret: str,
) -> Dict:
    """Retry a failed bulk transfer."""
    result = await db.execute(
        select(BulkTransfer).where(BulkTransfer.id == bulk_transfer_id)
    )
    bulk_transfer = result.scalars().first()

    if not bulk_transfer:
        return {"error": "BulkTransfer not found"}

    if bulk_transfer.status not in ("failed", "reversed"):
        return {"error": f"Cannot retry transfer in status: {bulk_transfer.status}"}

    # Get payouts
    result = await db.execute(
        select(Payout).where(Payout.bulk_transfer_id == bulk_transfer_id)
    )
    payouts = result.scalars().all()

    # Create new transfer
    service = TransferService(razorpay_key_id, razorpay_key_secret)
    return await service._process_gym_transfer(
        db, bulk_transfer.gym_id, payouts, _today_ist()
    )
