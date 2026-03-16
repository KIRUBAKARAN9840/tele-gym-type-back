"""
Deduction calculator for gym owner payouts.

Business rules:
1. Regular payments (daily_pass, sessions, gym_membership without no-cost EMI):
   - 2% PG charges on owner's amount
   - 2% TDS on owner's amount
   - Net to owner = owner_amount - PG - TDS

2. No-cost EMI (gym_membership only):
   - 5% flat deduction from owner's amount
   - Net to owner = owner_amount - 5%

How no-cost EMI is detected:
- At verify time: Razorpay payment entity has method="emi" + offer_id != null
  → is_no_cost_emi=True is saved on the FittbotPayment record
- At reconciliation: We read payment.is_no_cost_emi flag directly
- Fallback: Call Razorpay GET /v1/payments/:id to check offer_id

The "owner's amount" is Payment.amount_net (the base price the owner set,
before Fittbot markup for the client).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from decimal import Decimal, ROUND_HALF_UP

logger = logging.getLogger("auto_settlements.deductions")

# Rates (as Decimal for precision)
PG_FEE_RATE = Decimal("0.02")       # 2% PG charges
TDS_RATE = Decimal("0.02")          # 2% TDS
NO_COST_EMI_RATE = Decimal("0.05")  # 5% flat deduction for no-cost EMI

TWO_PLACES = Decimal("0.01")


@dataclass
class DeductionBreakdown:
    """Result of deduction calculation."""
    owner_amount: Decimal       # Base amount (Payment.amount_net)
    pg_fee: Decimal             # PG charge deducted
    tds: Decimal                # TDS deducted
    emi_deduction: Decimal      # No-cost EMI deduction (0 for regular)
    net_to_owner: Decimal       # Final amount to transfer to gym owner
    is_no_cost_emi: bool        # Whether no-cost EMI was applied
    pg_rate: Decimal            # PG rate applied
    tds_rate: Decimal           # TDS rate applied
    emi_rate: Decimal           # EMI deduction rate applied

    @property
    def total_deduction(self) -> Decimal:
        return self.pg_fee + self.tds + self.emi_deduction


def _round(amount: Decimal) -> Decimal:
    return amount.quantize(TWO_PLACES, rounding=ROUND_HALF_UP)


def calculate_deductions(
    owner_amount: Decimal,
    source_type: str,
    is_no_cost_emi: bool = False,
) -> DeductionBreakdown:
    """
    Calculate deductions for a payment.

    Args:
        owner_amount: The gym owner's base price (Payment.amount_net in rupees)
        source_type: Payment source type (daily_pass, gym_membership, yoga, zumba, etc.)
        is_no_cost_emi: Whether this payment was made via no-cost EMI.
            Determined by: Razorpay offer_id != null + method == "emi"
            Stored on Payment.is_no_cost_emi (set at verify time)

    Returns:
        DeductionBreakdown with all calculated amounts.
    """
    owner_amount = Decimal(str(owner_amount))

    if is_no_cost_emi:
        # No-cost EMI: flat 5% deduction
        emi_deduction = _round(owner_amount * NO_COST_EMI_RATE)
        return DeductionBreakdown(
            owner_amount=owner_amount,
            pg_fee=Decimal("0.00"),
            tds=Decimal("0.00"),
            emi_deduction=emi_deduction,
            net_to_owner=_round(owner_amount - emi_deduction),
            is_no_cost_emi=True,
            pg_rate=Decimal("0.00"),
            tds_rate=Decimal("0.00"),
            emi_rate=NO_COST_EMI_RATE * 100,
        )

    # Regular payment: 2% PG + 2% TDS
    pg_fee = _round(owner_amount * PG_FEE_RATE)
    tds = _round(owner_amount * TDS_RATE)
    net_to_owner = _round(owner_amount - pg_fee - tds)

    return DeductionBreakdown(
        owner_amount=owner_amount,
        pg_fee=pg_fee,
        tds=tds,
        emi_deduction=Decimal("0.00"),
        net_to_owner=net_to_owner,
        is_no_cost_emi=False,
        pg_rate=PG_FEE_RATE * 100,
        tds_rate=TDS_RATE * 100,
        emi_rate=Decimal("0.00"),
    )
