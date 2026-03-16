

from __future__ import annotations

import logging
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import select, and_
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.fittbot_payments_models import (
    Payment as FittbotPayment,
    Payout,
    PayoutEvent,
    PaymentBreakdown,
    Reconciliation,
    ReconciliationItem,
    Settlement,
)
from .models import SettlementSyncLog
from .deduction_calculator import calculate_deductions
from . import razorpayx_client as rzp

logger = logging.getLogger("auto_settlements.reconciliation")

IST = timezone(timedelta(hours=5, minutes=30))

# Source types that get Monday bulk transfer
MONDAY_BULK_TYPES = {"daily_pass", "yoga", "zumba", "personal_training", "hiit", "crossfit", "pilates", "dance"}
# Source types that get next-day transfer
NEXT_DAY_TYPES = {"gym_membership"}


def _now_ist() -> datetime:
    return datetime.now(IST)


def _today_ist() -> date:
    return _now_ist().date()


def _next_monday(from_date: date) -> date:
    """Get next Monday from the given date. If today is Monday, return next Monday."""
    days_ahead = 7 - from_date.weekday()  # Monday is 0
    if from_date.weekday() == 0:
        days_ahead = 7
    return from_date + timedelta(days=days_ahead)


def _next_day(from_date: date) -> date:
    return from_date + timedelta(days=1)


def _determine_schedule(source_type: str) -> Tuple[str, date]:
    """
    Determine payout schedule based on source type.
    Returns (payout_type, scheduled_for_date).
    """
    today = _today_ist()
    if source_type in NEXT_DAY_TYPES:
        return "immediate", _next_day(today)
    # All sessions and daily pass → Monday bulk
    return "bulk_monday", _next_monday(today)


class ReconciliationEngine:
    """Async reconciliation engine for matching Razorpay settlements with payments."""

    def __init__(self, db: AsyncSession):
        self.db = db

    async def run_daily_reconciliation(
        self,
        target_date: Optional[date] = None,
    ) -> Dict[str, Any]:
        """
        Main entry point for daily reconciliation.
        Fetches settlements, matches payments, creates payouts.

        Args:
            target_date: Date to reconcile. Defaults to yesterday (T-1 settlement).
        """
        if target_date is None:
            target_date = _today_ist() - timedelta(days=1)

        logger.info("Starting reconciliation for date: %s", target_date)

        # Create reconciliation job record
        recon_job = Reconciliation(
            job_date=target_date,
            job_type="daily_settlement",
            status="running",
            started_at=_now_ist(),
        )
        self.db.add(recon_job)
        await self.db.flush()

        try:
            # Step 1: Fetch settlement recon items from Razorpay
            recon_items = await rzp.fetch_all_settlement_recon_items(
                year=target_date.year,
                month=target_date.month,
                day=target_date.day,
            )

            if not recon_items:
                logger.info("No settlement recon items found for %s", target_date)
                recon_job.status = "completed"
                recon_job.completed_at = _now_ist()
                recon_job.payments_found = 0
                await self.db.commit()
                return {"status": "no_items", "date": str(target_date), "recon_id": recon_job.id}

            # Filter only payment-type items (not refunds/adjustments)
            payment_items = [
                item for item in recon_items
                if item.get("type") == "payment"
            ]

            logger.info(
                "Found %d recon items (%d payments) for %s",
                len(recon_items), len(payment_items), target_date,
            )
            recon_job.payments_found = len(payment_items)

            # Step 2: Process each settled payment
            matched = 0
            mismatched = 0
            payouts_created = 0

            for item in payment_items:
                result = await self._process_settlement_item(item, recon_job.id)
                if result == "matched":
                    matched += 1
                    payouts_created += 1
                elif result == "already_settled":
                    matched += 1
                elif result == "not_found":
                    mismatched += 1

            recon_job.payments_matched = matched
            recon_job.payments_mismatched = mismatched
            recon_job.payouts_scheduled = payouts_created
            recon_job.status = "completed"
            recon_job.completed_at = _now_ist()
            recon_job.raw_response = {"total_items": len(recon_items), "payment_items": len(payment_items)}

            await self.db.commit()

            result = {
                "status": "completed",
                "date": str(target_date),
                "recon_id": recon_job.id,
                "payments_found": len(payment_items),
                "matched": matched,
                "mismatched": mismatched,
                "payouts_created": payouts_created,
            }
            logger.info("Reconciliation completed: %s", result)
            return result

        except Exception as exc:
            logger.exception("Reconciliation failed for %s: %s", target_date, exc)
            recon_job.status = "failed"
            recon_job.error_message = str(exc)[:500]
            recon_job.completed_at = _now_ist()
            await self.db.commit()
            raise

    async def _process_settlement_item(
        self,
        item: Dict[str, Any],
        recon_job_id: int,
    ) -> str:
        """
        Process a single settlement recon item.
        Matches with our Payment record, calculates deductions, creates Payout.

        Returns: "matched", "already_settled", or "not_found"
        """
        rp_payment_id = item.get("entity_id", "")
        rp_amount_paise = item.get("amount", 0)
        rp_fee_paise = item.get("fee", 0)
        rp_tax_paise = item.get("tax", 0)
        settlement_id = item.get("settlement_id", "")

        if not rp_payment_id:
            return "not_found"

        # Find our Payment record by gateway_payment_id
        result = await self.db.execute(
            select(FittbotPayment).where(
                FittbotPayment.gateway_payment_id == rp_payment_id
            )
        )
        payment = result.scalars().first()

        # Create reconciliation item record
        recon_item = ReconciliationItem(
            reconciliation_id=recon_job_id,
            razorpay_payment_id=rp_payment_id,
            razorpay_amount=Decimal(str(rp_amount_paise / 100)),
            razorpay_fee=Decimal(str(rp_fee_paise / 100)),
            razorpay_tax=Decimal(str(rp_tax_paise / 100)),
        )

        if not payment:
            logger.warning("Payment not found for razorpay_id: %s", rp_payment_id)
            recon_item.status = "not_found"
            recon_item.notes = "No matching payment in our records"
            self.db.add(recon_item)
            return "not_found"

        recon_item.payment_id = payment.id
        recon_item.our_amount = payment.amount_gross
        recon_item.razorpay_order_id = payment.gateway_order_id

        # Check amount match
        rp_amount_rupees = Decimal(str(rp_amount_paise / 100))
        if rp_amount_rupees != payment.amount_gross:
            recon_item.status = "mismatched"
            recon_item.delta_amount = rp_amount_rupees - payment.amount_gross
            recon_item.notes = f"Amount mismatch: Razorpay={rp_amount_rupees}, Ours={payment.amount_gross}"
            logger.warning(
                "Amount mismatch for %s: RP=%s, Ours=%s",
                rp_payment_id, rp_amount_rupees, payment.amount_gross,
            )
        else:
            recon_item.status = "matched"

        self.db.add(recon_item)

        # Check if already settled (idempotency)
        if payment.status == "settled":
            logger.debug("Payment %s already settled, skipping", payment.id)
            return "already_settled"

        # Check if payout already exists for this payment
        existing_payout = await self.db.execute(
            select(Payout).where(Payout.payment_id == payment.id)
        )
        if existing_payout.scalars().first():
            logger.debug("Payout already exists for payment %s, skipping", payment.id)
            return "already_settled"

        # Mark payment as settled
        payment.status = "settled"
        payment.settled_at = _now_ist()
        if not payment.settlement_id:
            # Find or create settlement record
            stl = await self._get_or_create_settlement(settlement_id, item)
            payment.settlement_id = stl.id

        # Determine if this is a no-cost EMI payment.
        # Layer 1: Check is_no_cost_emi flag (set at verify time for new payments)
        # Layer 2: For old payments without the flag, check if payment_method or
        #          recon item method is "emi" — then call Razorpay GET /v1/payments/:id
        #          to check offer_id (the definitive no-cost EMI signal)
        is_no_cost_emi = getattr(payment, "is_no_cost_emi", False)

        if not is_no_cost_emi and payment.source_type == "gym_membership":
            pm = getattr(payment, "payment_method", None)
            recon_method = item.get("method", "")

            if pm == "emi" or recon_method == "emi":
                # Payment method is EMI but is_no_cost_emi not set (old record
                # or flag missed). Call Razorpay to check offer_id.
                try:
                    rp_payment_detail = await rzp.fetch_payment(rp_payment_id)
                    if rp_payment_detail.get("offer_id"):
                        is_no_cost_emi = True
                        payment.is_no_cost_emi = True
                        payment.payment_method = "emi"
                        logger.info(
                            "Backfilled is_no_cost_emi=True for payment %s (offer_id=%s)",
                            payment.id, rp_payment_detail.get("offer_id"),
                        )
                except Exception as exc:
                    logger.warning(
                        "Failed to fetch Razorpay payment %s for EMI check: %s",
                        rp_payment_id, exc,
                    )

        # Calculate deductions based on no-cost EMI detection
        deductions = calculate_deductions(
            owner_amount=payment.amount_net,
            source_type=payment.source_type or "",
            is_no_cost_emi=is_no_cost_emi,
        )

        # Determine payout schedule
        payout_type, scheduled_for = _determine_schedule(payment.source_type or "")

        # Create Payout record
        payout = Payout(
            payment_id=payment.id,
            gym_id=payment.gym_id,
            amount_gross=payment.amount_net,  # Owner's amount before deductions
            pg_fee=deductions.pg_fee,
            tds=deductions.tds,
            commission=deductions.emi_deduction,  # EMI deduction goes in commission field
            commission_rate=deductions.emi_rate if deductions.is_no_cost_emi else Decimal("0"),
            amount_net=deductions.net_to_owner,
            payout_type=payout_type,
            scheduled_for=scheduled_for,
            status="scheduled",
            scheduled_at=_now_ist(),
        )
        self.db.add(payout)
        await self.db.flush()

        recon_item.payout_id = payout.id

        # Create payment breakdown records
        breakdowns = [
            PaymentBreakdown(
                payment_id=payment.id,
                component="base",
                amount=payment.amount_net,
                description="Owner's base price",
            ),
        ]

        if deductions.is_no_cost_emi:
            breakdowns.append(PaymentBreakdown(
                payment_id=payment.id,
                component="emi_deduction",
                amount=deductions.emi_deduction,
                rate_pct=deductions.emi_rate,
                description="No-cost EMI processing fee (5%)",
            ))
        else:
            breakdowns.extend([
                PaymentBreakdown(
                    payment_id=payment.id,
                    component="pg_fee",
                    amount=deductions.pg_fee,
                    rate_pct=deductions.pg_rate,
                    description="Payment gateway charges (2%)",
                ),
                PaymentBreakdown(
                    payment_id=payment.id,
                    component="tds",
                    amount=deductions.tds,
                    rate_pct=deductions.tds_rate,
                    description="TDS deduction (2%)",
                ),
            ])

        breakdowns.append(PaymentBreakdown(
            payment_id=payment.id,
            component="net_to_gym",
            amount=deductions.net_to_owner,
            description="Final amount to gym owner",
        ))

        for bd in breakdowns:
            self.db.add(bd)

        # Create payout event
        self.db.add(PayoutEvent(
            payout_id=payout.id,
            from_status=None,
            to_status="scheduled",
            actor="reconciliation",
            notes=f"Auto-scheduled via reconciliation. Type={payout_type}, date={scheduled_for}",
            event_data={
                "settlement_id": settlement_id,
                "razorpay_payment_id": rp_payment_id,
                "deductions": {
                    "pg_fee": str(deductions.pg_fee),
                    "tds": str(deductions.tds),
                    "emi_deduction": str(deductions.emi_deduction),
                    "is_no_cost_emi": deductions.is_no_cost_emi,
                },
            },
        ))

        logger.info(
            "Created payout for payment=%s, gym=%s, net=%s, type=%s, scheduled=%s",
            payment.id, payment.gym_id, deductions.net_to_owner, payout_type, scheduled_for,
        )
        return "matched"

    async def _get_or_create_settlement(
        self, settlement_id: str, item: Dict[str, Any]
    ) -> Settlement:
        """Get existing or create new Settlement record."""
        result = await self.db.execute(
            select(Settlement).where(
                Settlement.razorpay_settlement_id == settlement_id
            )
        )
        existing = result.scalars().first()
        if existing:
            return existing

        settled_at_ts = item.get("settled_at")
        settlement_date = _today_ist()
        if settled_at_ts:
            settlement_date = datetime.fromtimestamp(settled_at_ts, tz=IST).date()

        stl = Settlement(
            razorpay_settlement_id=settlement_id,
            settlement_date=settlement_date,
            gross_amount=Decimal(str(item.get("amount", 0) / 100)),
            pg_fee=Decimal(str(item.get("fee", 0) / 100)),
            net_amount=Decimal(str((item.get("amount", 0) - item.get("fee", 0)) / 100)),
            status="processed",
            processed_at=_now_ist(),
        )
        self.db.add(stl)
        await self.db.flush()
        return stl

    async def run_manual_reconciliation(
        self,
        from_date: date,
        to_date: date,
    ) -> Dict[str, Any]:
        """
        Run reconciliation for a date range (manual/backfill).
        Iterates day by day.
        """
        results = []
        current = from_date
        while current <= to_date:
            result = await self.run_daily_reconciliation(target_date=current)
            results.append(result)
            current += timedelta(days=1)

        return {
            "status": "completed",
            "from_date": str(from_date),
            "to_date": str(to_date),
            "days_processed": len(results),
            "results": results,
        }
