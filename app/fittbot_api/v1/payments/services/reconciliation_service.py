

import logging
from datetime import date, datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP
from typing import Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

import razorpay
from sqlalchemy import and_, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.fittbot_payments_models import (
    BulkTransfer,
    CommissionConfig,
    Payment,
    PaymentBreakdown,
    Payout,
    PayoutEvent,
    Reconciliation,
    ReconciliationItem,
    Settlement,
)

logger = logging.getLogger("payments.reconciliation")

IST = ZoneInfo("Asia/Kolkata")

# Default rates (can be overridden by CommissionConfig)
DEFAULT_COMMISSION_RATE = Decimal("30.00")  # 10% Fittbot commission
DEFAULT_PG_FEE_RATE = Decimal("2.00")  # 2% Razorpay fee
DEFAULT_GST_RATE = Decimal("18.00")  # 18% GST on commission
DEFAULT_TDS_RATE = Decimal("2.00")  # 2% TDS on commission

# Source types that get Monday bulk payout
BULK_MONDAY_SOURCE_TYPES = {
    "daily_pass",
    "yoga", "zumba", "pilates", "crossfit", "hiit", "spinning", "aerobics",
    "strength_training", "cardio", "dance", "martial_arts", "boxing",
    # Add more session types as needed
}

# Source types that get immediate (next day) payout
IMMEDIATE_SOURCE_TYPES = {"gym_membership", "personal_training"}


def _now_ist() -> datetime:
    return datetime.now(IST)


def _today_ist() -> date:
    return _now_ist().date()


def _next_monday(from_date: date) -> date:
    """Get next Monday from given date. If today is Monday, return next Monday."""
    days_until_monday = (7 - from_date.weekday()) % 7
    if days_until_monday == 0:
        days_until_monday = 7  # If today is Monday, get next Monday
    return from_date + timedelta(days=days_until_monday)


def _next_business_day(from_date: date) -> date:
    """Get next business day (skip weekends)."""
    next_day = from_date + timedelta(days=1)
    while next_day.weekday() >= 5:  # Saturday=5, Sunday=6
        next_day += timedelta(days=1)
    return next_day


class DeductionCalculator:
    """Calculates deductions for a payment."""

    def __init__(
        self,
        commission_rate: Decimal = DEFAULT_COMMISSION_RATE,
        pg_fee_rate: Decimal = DEFAULT_PG_FEE_RATE,
        gst_rate: Decimal = DEFAULT_GST_RATE,
        tds_rate: Decimal = DEFAULT_TDS_RATE,
    ):
        self.commission_rate = commission_rate
        self.pg_fee_rate = pg_fee_rate
        self.gst_rate = gst_rate
        self.tds_rate = tds_rate

    def calculate(self, gross_amount: Decimal) -> Dict[str, Decimal]:
        """
        Calculate all deductions from gross amount.

        Formula:
        - PG Fee = gross * pg_fee_rate% (already deducted by Razorpay in settlement)
        - Commission = gross * commission_rate%
        - GST on Commission = commission * gst_rate%
        - TDS = commission * tds_rate%
        - Net to Gym = gross - pg_fee - commission - gst - tds

        Example for ₹1000:
        - PG Fee (2%) = ₹20
        - Commission (10%) = ₹100
        - GST on Commission (18%) = ₹18
        - TDS (2%) = ₹2
        - Net to Gym = 1000 - 20 - 100 - 18 - 2 = ₹860
        """
        gross = Decimal(str(gross_amount))

        pg_fee = (gross * self.pg_fee_rate / 100).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        commission = (gross * self.commission_rate / 100).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        gst = (commission * self.gst_rate / 100).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        tds = (commission * self.tds_rate / 100).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

        net_to_gym = gross - pg_fee - commission - gst - tds

        return {
            "gross": gross,
            "pg_fee": pg_fee,
            "pg_fee_rate": self.pg_fee_rate,
            "commission": commission,
            "commission_rate": self.commission_rate,
            "gst": gst,
            "gst_rate": self.gst_rate,
            "tds": tds,
            "tds_rate": self.tds_rate,
            "net_to_gym": net_to_gym,
        }


class ReconciliationService:
    
    def __init__(
        self,
        razorpay_key_id: str,
        razorpay_key_secret: str,
    ):
        self.razorpay_client = razorpay.Client(auth=(razorpay_key_id, razorpay_key_secret))

    async def run_daily_reconciliation(self, db: AsyncSession) -> Dict:
        """
        Main entry point for daily reconciliation job.
        Should be called at 11 AM IST daily.
        """
        job_date = _today_ist()
        logger.info(f"Starting daily reconciliation for {job_date}")

        # Create reconciliation job record
        recon_job = Reconciliation(
            job_date=job_date,
            job_type="daily_settlement",
            status="running",
            started_at=_now_ist(),
        )
        db.add(recon_job)
        await db.flush()

        try:
            # Step 1: Fetch settlements from Razorpay (for yesterday and day before)
            settlements = await self._fetch_razorpay_settlements(
                from_date=job_date - timedelta(days=3),
                to_date=job_date,
            )

            if not settlements:
                logger.info("No settlements found for reconciliation")
                recon_job.status = "completed"
                recon_job.completed_at = _now_ist()
                await db.commit()
                return {"status": "completed", "message": "No settlements to process"}

            # Step 2: Process each settlement
            total_matched = 0
            total_mismatched = 0
            total_scheduled = 0

            for settlement_data in settlements:
                result = await self._process_settlement(db, recon_job.id, settlement_data)
                total_matched += result.get("matched", 0)
                total_mismatched += result.get("mismatched", 0)
                total_scheduled += result.get("scheduled", 0)

            # Update job stats
            recon_job.payments_matched = total_matched
            recon_job.payments_mismatched = total_mismatched
            recon_job.payouts_scheduled = total_scheduled
            recon_job.status = "completed"
            recon_job.completed_at = _now_ist()

            await db.commit()

            logger.info(
                f"Reconciliation completed: matched={total_matched}, "
                f"mismatched={total_mismatched}, scheduled={total_scheduled}"
            )

            return {
                "status": "completed",
                "matched": total_matched,
                "mismatched": total_mismatched,
                "scheduled": total_scheduled,
            }

        except Exception as e:
            logger.exception(f"Reconciliation failed: {e}")
            recon_job.status = "failed"
            recon_job.error_message = str(e)
            recon_job.completed_at = _now_ist()
            await db.commit()
            raise

    async def _fetch_razorpay_settlements(
        self, from_date: date, to_date: date
    ) -> List[Dict]:
        """
        Fetch settlements from Razorpay API.
        Uses the Settlements API to get all settled payments.
        """
        try:
            # Convert dates to timestamps
            from_ts = int(datetime.combine(from_date, datetime.min.time()).timestamp())
            to_ts = int(datetime.combine(to_date, datetime.max.time()).timestamp())

            # Fetch settlements
            # Note: Razorpay API returns settlements with their payment items
            response = self.razorpay_client.settlement.all({
                "from": from_ts,
                "to": to_ts,
                "count": 100,
            })

            settlements = response.get("items", [])
            logger.info(f"Fetched {len(settlements)} settlements from Razorpay")
            return settlements

        except Exception as e:
            logger.error(f"Failed to fetch Razorpay settlements: {e}")
            # Return empty list to allow manual reconciliation
            return []

    async def _process_settlement(
        self, db: AsyncSession, recon_job_id: int, settlement_data: Dict
    ) -> Dict:
        """Process a single Razorpay settlement."""
        razorpay_settlement_id = settlement_data.get("id")
        settlement_date = datetime.fromtimestamp(
            settlement_data.get("created_at", 0)
        ).date()

        # Check if already processed
        existing = await db.execute(
            select(Settlement).where(
                Settlement.razorpay_settlement_id == razorpay_settlement_id
            )
        )
        if existing.scalars().first():
            logger.info(f"Settlement {razorpay_settlement_id} already processed, skipping")
            return {"matched": 0, "mismatched": 0, "scheduled": 0}

        # Create settlement record
        settlement = Settlement(
            razorpay_settlement_id=razorpay_settlement_id,
            settlement_date=settlement_date,
            gross_amount=Decimal(str(settlement_data.get("amount", 0))) / 100,
            pg_fee=Decimal(str(settlement_data.get("fees", 0))) / 100,
            net_amount=Decimal(str(settlement_data.get("amount", 0) - settlement_data.get("fees", 0))) / 100,
            utr=settlement_data.get("utr"),
            status="pending",
            raw_data=settlement_data,
        )
        db.add(settlement)
        await db.flush()

        # Fetch settlement details (individual payments)
        try:
            settlement_recon = self.razorpay_client.settlement.reports({
                "settlement_id": razorpay_settlement_id,
            })
            payment_items = settlement_recon.get("items", [])
        except Exception:
            # If we can't get individual items, try to match by date range
            payment_items = []

        matched = 0
        mismatched = 0
        scheduled = 0

        # If we have individual payment items
        for item in payment_items:
            razorpay_payment_id = item.get("payment_id")
            razorpay_amount = Decimal(str(item.get("amount", 0))) / 100
            razorpay_fee = Decimal(str(item.get("fee", 0))) / 100

            # Find our payment record
            payment_result = await db.execute(
                select(Payment).where(
                    Payment.gateway_payment_id == razorpay_payment_id
                )
            )
            payment = payment_result.scalars().first()

            if payment:
                # Match found - process it
                result = await self._process_matched_payment(
                    db, recon_job_id, settlement, payment,
                    razorpay_amount, razorpay_fee
                )
                if result:
                    matched += 1
                    scheduled += 1
            else:
                # No match found
                mismatched += 1
                await self._record_mismatch(
                    db, recon_job_id, razorpay_payment_id, razorpay_amount, razorpay_fee
                )

        # If no individual items, process all pending payouts from this settlement period
        if not payment_items:
            result = await self._process_pending_payouts_by_date(
                db, recon_job_id, settlement, settlement_date
            )
            matched = result.get("matched", 0)
            scheduled = result.get("scheduled", 0)

        # Update settlement status
        settlement.payments_count = matched
        settlement.status = "processed"
        settlement.processed_at = _now_ist()

        return {"matched": matched, "mismatched": mismatched, "scheduled": scheduled}

    async def _process_matched_payment(
        self,
        db: AsyncSession,
        recon_job_id: int,
        settlement: Settlement,
        payment: Payment,
        razorpay_amount: Decimal,
        razorpay_fee: Decimal,
    ) -> bool:
        """Process a matched payment - calculate deductions and schedule payout."""

        # Update payment with settlement info
        payment.settlement_id = settlement.id
        payment.settled_at = _now_ist()
        payment.status = "settled"

        # Find the payout for this payment
        payout_result = await db.execute(
            select(Payout).where(
                Payout.payment_id == payment.id,
                Payout.status == "ready_for_transfer",
            )
        )
        payout = payout_result.scalars().first()

        if not payout:
            logger.warning(f"No pending payout found for payment {payment.id}")
            return False

        # Get commission config for this gym/source_type
        config = await self._get_commission_config(db, payment.gym_id, payment.source_type)
        calculator = DeductionCalculator(
            commission_rate=config.get("commission_rate", DEFAULT_COMMISSION_RATE),
            pg_fee_rate=config.get("pg_fee_rate", DEFAULT_PG_FEE_RATE),
            gst_rate=config.get("gst_rate", DEFAULT_GST_RATE),
            tds_rate=config.get("tds_rate", DEFAULT_TDS_RATE),
        )

        # Calculate deductions
        deductions = calculator.calculate(payout.amount_gross)

        # Update payout with deductions
        payout.pg_fee = deductions["pg_fee"]
        payout.commission = deductions["commission"]
        payout.commission_rate = deductions["commission_rate"]
        payout.gst = deductions["gst"]
        payout.tds = deductions["tds"]
        payout.amount_net = deductions["net_to_gym"]

        # Determine payout type and schedule
        source_type = payment.source_type.lower()
        if source_type in IMMEDIATE_SOURCE_TYPES:
            payout.payout_type = "immediate"
            payout.scheduled_for = _next_business_day(_today_ist())
        else:
            payout.payout_type = "bulk_monday"
            payout.scheduled_for = _next_monday(_today_ist())

        payout.status = "scheduled"
        payout.scheduled_at = _now_ist()

        # Create payment breakdown records
        breakdown_components = [
            ("gross", deductions["gross"], None, "Original payment amount"),
            ("pg_fee", deductions["pg_fee"], deductions["pg_fee_rate"], "Payment gateway fee (Razorpay)"),
            ("commission", deductions["commission"], deductions["commission_rate"], "Fittbot platform commission"),
            ("gst_on_commission", deductions["gst"], deductions["gst_rate"], "GST on commission"),
            ("tds", deductions["tds"], deductions["tds_rate"], "TDS deducted"),
            ("net_to_gym", deductions["net_to_gym"], None, "Net amount payable to gym"),
        ]

        for component, amount, rate, description in breakdown_components:
            db.add(PaymentBreakdown(
                payment_id=payment.id,
                component=component,
                amount=amount,
                rate_pct=rate,
                description=description,
            ))

        # Create payout event
        db.add(PayoutEvent(
            payout_id=payout.id,
            from_status="ready_for_transfer",
            to_status="scheduled",
            actor="reconciliation",
            notes=f"Scheduled for {payout.scheduled_for}. Net: ₹{payout.amount_net}",
            event_data={
                "settlement_id": settlement.razorpay_settlement_id,
                "deductions": {k: str(v) for k, v in deductions.items()},
            },
        ))

        # Create reconciliation item
        db.add(ReconciliationItem(
            reconciliation_id=recon_job_id,
            payment_id=payment.id,
            payout_id=payout.id,
            razorpay_payment_id=payment.gateway_payment_id,
            razorpay_amount=razorpay_amount,
            razorpay_fee=razorpay_fee,
            our_amount=payment.amount_gross,
            status="matched",
        ))

        logger.info(
            f"Payment {payment.id} reconciled: gross={payout.amount_gross}, "
            f"net={payout.amount_net}, scheduled_for={payout.scheduled_for}"
        )

        return True

    async def _process_pending_payouts_by_date(
        self,
        db: AsyncSession,
        recon_job_id: int,
        settlement: Settlement,
        settlement_date: date,
    ) -> Dict:
        """
        Process pending payouts when we don't have individual settlement items.
        Matches payments by paid_at date within settlement window.
        """
        # Find payments from 2-3 days before settlement date (T+1/T+2 rule)
        payment_start = settlement_date - timedelta(days=3)
        payment_end = settlement_date - timedelta(days=1)

        # Find all pending payouts with unsettled payments in this date range
        result = await db.execute(
            select(Payout, Payment)
            .join(Payment, Payment.id == Payout.payment_id)
            .where(
                Payout.status == "ready_for_transfer",
                Payment.status == "paid",
                Payment.settled_at.is_(None),
                Payment.paid_at >= datetime.combine(payment_start, datetime.min.time()),
                Payment.paid_at <= datetime.combine(payment_end, datetime.max.time()),
            )
        )
        pending_rows = result.all()

        matched = 0
        scheduled = 0

        for payout, payment in pending_rows:
            # Process each one
            success = await self._process_matched_payment(
                db, recon_job_id, settlement, payment,
                razorpay_amount=payment.amount_gross,
                razorpay_fee=Decimal("0"),  # Will calculate our own fees
            )
            if success:
                matched += 1
                scheduled += 1

        return {"matched": matched, "scheduled": scheduled}

    async def _record_mismatch(
        self,
        db: AsyncSession,
        recon_job_id: int,
        razorpay_payment_id: str,
        razorpay_amount: Decimal,
        razorpay_fee: Decimal,
    ):
        """Record a payment that couldn't be matched."""
        db.add(ReconciliationItem(
            reconciliation_id=recon_job_id,
            razorpay_payment_id=razorpay_payment_id,
            razorpay_amount=razorpay_amount,
            razorpay_fee=razorpay_fee,
            status="not_found",
            notes="Payment not found in our records",
        ))
        logger.warning(f"Unmatched Razorpay payment: {razorpay_payment_id}")

    async def _get_commission_config(
        self, db: AsyncSession, gym_id: int, source_type: str
    ) -> Dict:
        """Get commission configuration for gym/source_type."""
        today = _today_ist()

        # Try to find specific config (gym_id + source_type)
        result = await db.execute(
            select(CommissionConfig)
            .where(
                CommissionConfig.gym_id == gym_id,
                CommissionConfig.source_type == source_type,
                CommissionConfig.is_active == True,
                CommissionConfig.effective_from <= today,
                (CommissionConfig.effective_to.is_(None) | (CommissionConfig.effective_to >= today)),
            )
            .order_by(CommissionConfig.effective_from.desc())
            .limit(1)
        )
        config = result.scalars().first()

        if config:
            return {
                "commission_rate": config.commission_rate,
                "pg_fee_rate": config.pg_fee_rate,
                "gst_rate": config.gst_rate,
                "tds_rate": config.tds_rate,
            }

        # Try gym-level config
        result = await db.execute(
            select(CommissionConfig)
            .where(
                CommissionConfig.gym_id == gym_id,
                CommissionConfig.source_type.is_(None),
                CommissionConfig.is_active == True,
                CommissionConfig.effective_from <= today,
                (CommissionConfig.effective_to.is_(None) | (CommissionConfig.effective_to >= today)),
            )
            .order_by(CommissionConfig.effective_from.desc())
            .limit(1)
        )
        config = result.scalars().first()

        if config:
            return {
                "commission_rate": config.commission_rate,
                "pg_fee_rate": config.pg_fee_rate,
                "gst_rate": config.gst_rate,
                "tds_rate": config.tds_rate,
            }

        # Return defaults
        return {
            "commission_rate": DEFAULT_COMMISSION_RATE,
            "pg_fee_rate": DEFAULT_PG_FEE_RATE,
            "gst_rate": DEFAULT_GST_RATE,
            "tds_rate": DEFAULT_TDS_RATE,
        }


# ═══════════════════════════════════════════════════════════════════════════════
# Utility function for manual reconciliation
# ═══════════════════════════════════════════════════════════════════════════════
async def manual_reconcile_payment(
    db: AsyncSession,
    payment_id: int,
    razorpay_key_id: str,
    razorpay_key_secret: str,
) -> Dict:

    service = ReconciliationService(razorpay_key_id, razorpay_key_secret)

    # Get payment
    result = await db.execute(
        select(Payment).where(Payment.id == payment_id)
    )
    payment = result.scalars().first()

    if not payment:
        return {"error": "Payment not found"}

    # Create a dummy settlement for manual reconciliation
    settlement = Settlement(
        razorpay_settlement_id=f"manual_{payment_id}_{int(_now_ist().timestamp())}",
        settlement_date=_today_ist(),
        gross_amount=payment.amount_gross,
        pg_fee=Decimal("0"),
        net_amount=payment.amount_gross,
        status="processed",
    )
    db.add(settlement)
    await db.flush()

    # Create reconciliation job
    recon_job = Reconciliation(
        job_date=_today_ist(),
        job_type="manual",
        status="running",
        started_at=_now_ist(),
    )
    db.add(recon_job)
    await db.flush()

    # Process the payment
    success = await service._process_matched_payment(
        db, recon_job.id, settlement, payment,
        razorpay_amount=payment.amount_gross,
        razorpay_fee=Decimal("0"),
    )

    recon_job.status = "completed" if success else "failed"
    recon_job.completed_at = _now_ist()

    await db.commit()

    return {"success": success, "payment_id": payment_id}
