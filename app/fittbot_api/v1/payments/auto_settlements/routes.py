"""
API routes for the auto-settlement & payout system.

Endpoints:
- Bank account management (gym owner onboarding)
- Manual reconciliation triggers
- Payout management and monitoring
- Dashboard analytics
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timedelta, timezone
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, Field
from sqlalchemy import select, and_, func, desc
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.async_database import get_async_db
from app.models.fittbot_payments_models import (
    BulkTransfer,
    Payout,
    Reconciliation,
    Settlement,
)
from .models import GymBankAccount
from .reconciliation_engine import ReconciliationEngine
from .auto_payout_engine import AutoPayoutEngine
from . import razorpayx_client as rzp

logger = logging.getLogger("auto_settlements.routes")

router = APIRouter(
    prefix="/auto-settlements",
    tags=["Auto Settlements & Payouts"],
)

IST = timezone(timedelta(hours=5, minutes=30))


# ─── Pydantic Schemas ────────────────────────────────────────────────────────


class RegisterBankAccountRequest(BaseModel):
    gym_id: int = Field(..., gt=0)
    owner_id: Optional[int] = None
    account_type: str = Field(default="bank", pattern="^(bank|upi)$")
    account_holder_name: str = Field(..., min_length=2, max_length=200)
    account_number: Optional[str] = Field(default=None, min_length=5, max_length=50)
    ifsc_code: Optional[str] = Field(default=None, min_length=11, max_length=11)
    bank_name: Optional[str] = None
    upi_id: Optional[str] = None


class RegisterBankAccountResponse(BaseModel):
    status: str
    gym_id: int
    bank_account_id: int
    razorpayx_contact_id: Optional[str] = None
    razorpayx_fund_account_id: Optional[str] = None


class RunReconRequest(BaseModel):
    target_date: Optional[date] = None
    from_date: Optional[date] = None
    to_date: Optional[date] = None


class TriggerPayoutsRequest(BaseModel):
    payout_type: Optional[str] = Field(default=None, pattern="^(bulk_monday|immediate)$")


# ─── Bank Account Management ─────────────────────────────────────────────────


@router.post("/bank-account/register", response_model=RegisterBankAccountResponse)
async def register_bank_account(
    payload: RegisterBankAccountRequest,
    db: AsyncSession = Depends(get_async_db),
):
    """
    Register gym owner's bank account for RazorpayX payouts.
    Creates RazorpayX contact and fund account.
    """
    if payload.account_type == "bank":
        if not payload.account_number or not payload.ifsc_code:
            raise HTTPException(
                status.HTTP_400_BAD_REQUEST,
                "account_number and ifsc_code are required for bank account",
            )
    elif payload.account_type == "upi":
        if not payload.upi_id:
            raise HTTPException(
                status.HTTP_400_BAD_REQUEST,
                "upi_id is required for UPI account",
            )

    # Deactivate any existing active account for this gym
    existing_result = await db.execute(
        select(GymBankAccount).where(
            and_(
                GymBankAccount.gym_id == payload.gym_id,
                GymBankAccount.is_active == True,
            )
        )
    )
    for existing in existing_result.scalars().all():
        existing.is_active = False

    # Create RazorpayX contact
    contact_resp = await rzp.create_contact(
        name=payload.account_holder_name,
        contact_type="vendor",
        reference_id=f"gym_{payload.gym_id}",
        notes={"gym_id": str(payload.gym_id)},
    )
    contact_id = contact_resp.get("id", "")

    # Create fund account
    fund_account_id = ""
    if payload.account_type == "bank":
        fa_resp = await rzp.create_fund_account_bank(
            contact_id=contact_id,
            account_holder_name=payload.account_holder_name,
            account_number=payload.account_number,
            ifsc=payload.ifsc_code,
        )
        fund_account_id = fa_resp.get("id", "")
    else:
        fa_resp = await rzp.create_fund_account_upi(
            contact_id=contact_id,
            upi_address=payload.upi_id,
        )
        fund_account_id = fa_resp.get("id", "")

    # Save to database
    bank_account = GymBankAccount(
        gym_id=payload.gym_id,
        owner_id=payload.owner_id,
        account_type=payload.account_type,
        account_holder_name=payload.account_holder_name,
        account_number=payload.account_number,
        ifsc_code=payload.ifsc_code,
        bank_name=payload.bank_name,
        upi_id=payload.upi_id,
        razorpayx_contact_id=contact_id,
        razorpayx_fund_account_id=fund_account_id,
        is_verified=True,
        is_active=True,
        verification_status="verified",
    )
    db.add(bank_account)
    await db.commit()
    await db.refresh(bank_account)

    return RegisterBankAccountResponse(
        status="registered",
        gym_id=payload.gym_id,
        bank_account_id=bank_account.id,
        razorpayx_contact_id=contact_id,
        razorpayx_fund_account_id=fund_account_id,
    )


@router.get("/bank-account/{gym_id}")
async def get_bank_account(
    gym_id: int,
    db: AsyncSession = Depends(get_async_db),
):
    """Get active bank account details for a gym."""
    result = await db.execute(
        select(GymBankAccount).where(
            and_(
                GymBankAccount.gym_id == gym_id,
                GymBankAccount.is_active == True,
            )
        )
    )
    account = result.scalars().first()
    if not account:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "No active bank account found")

    return {
        "gym_id": gym_id,
        "account_type": account.account_type,
        "account_holder_name": account.account_holder_name,
        "masked_account": f"****{account.account_number[-4:]}" if account.account_number else None,
        "ifsc_code": account.ifsc_code,
        "bank_name": account.bank_name,
        "upi_id": account.upi_id,
        "is_verified": account.is_verified,
        "razorpayx_fund_account_id": account.razorpayx_fund_account_id,
    }


# ─── Reconciliation ──────────────────────────────────────────────────────────


@router.post("/reconciliation/run")
async def run_reconciliation(
    payload: RunReconRequest,
    db: AsyncSession = Depends(get_async_db),
):
    """
    Trigger reconciliation manually.
    Either for a single date or a date range.
    """
    engine = ReconciliationEngine(db)

    if payload.from_date and payload.to_date:
        result = await engine.run_manual_reconciliation(
            from_date=payload.from_date,
            to_date=payload.to_date,
        )
    else:
        result = await engine.run_daily_reconciliation(
            target_date=payload.target_date,
        )

    return result


@router.get("/reconciliation/history")
async def get_reconciliation_history(
    limit: int = 20,
    offset: int = 0,
    db: AsyncSession = Depends(get_async_db),
):
    """Get reconciliation job history."""
    result = await db.execute(
        select(Reconciliation)
        .order_by(desc(Reconciliation.started_at))
        .limit(limit)
        .offset(offset)
    )
    jobs = result.scalars().all()

    return {
        "jobs": [
            {
                "id": j.id,
                "job_date": str(j.job_date),
                "job_type": j.job_type,
                "status": j.status,
                "payments_found": j.payments_found,
                "payments_matched": j.payments_matched,
                "payments_mismatched": j.payments_mismatched,
                "payouts_scheduled": j.payouts_scheduled,
                "started_at": str(j.started_at) if j.started_at else None,
                "completed_at": str(j.completed_at) if j.completed_at else None,
                "error_message": j.error_message,
            }
            for j in jobs
        ],
        "limit": limit,
        "offset": offset,
    }


# ─── Payouts ─────────────────────────────────────────────────────────────────


@router.post("/payouts/trigger")
async def trigger_payouts(
    payload: TriggerPayoutsRequest,
    db: AsyncSession = Depends(get_async_db),
):
    """
    Manually trigger payout processing.
    - bulk_monday: Process Monday bulk transfers (daily_pass/sessions)
    - immediate: Process next-day transfers (gym_membership)
    - None: Process all scheduled payouts
    """
    engine = AutoPayoutEngine(db)
    result = await engine.process_scheduled_payouts(payout_type=payload.payout_type)
    return result


@router.post("/payouts/retry-failed")
async def retry_failed_payouts(
    db: AsyncSession = Depends(get_async_db),
):
    """Retry all failed bulk transfers."""
    engine = AutoPayoutEngine(db)
    result = await engine.retry_failed_transfers()
    return result


@router.get("/payouts/pending")
async def get_pending_payouts(
    gym_id: Optional[int] = None,
    limit: int = 50,
    db: AsyncSession = Depends(get_async_db),
):
    """Get pending/scheduled payouts."""
    query = select(Payout).where(
        Payout.status.in_(["scheduled", "initiated", "processing"])
    )
    if gym_id:
        query = query.where(Payout.gym_id == gym_id)
    query = query.order_by(desc(Payout.scheduled_at)).limit(limit)

    result = await db.execute(query)
    payouts = result.scalars().all()

    return {
        "payouts": [
            {
                "id": p.id,
                "payment_id": p.payment_id,
                "gym_id": p.gym_id,
                "amount_gross": str(p.amount_gross),
                "pg_fee": str(p.pg_fee),
                "tds": str(p.tds),
                "commission": str(p.commission),
                "amount_net": str(p.amount_net),
                "payout_type": p.payout_type,
                "scheduled_for": str(p.scheduled_for) if p.scheduled_for else None,
                "status": p.status,
                "transfer_ref": p.transfer_ref,
            }
            for p in payouts
        ],
        "count": len(payouts),
    }


@router.get("/transfers/history")
async def get_transfer_history(
    gym_id: Optional[int] = None,
    status_filter: Optional[str] = None,
    limit: int = 20,
    offset: int = 0,
    db: AsyncSession = Depends(get_async_db),
):
    """Get bulk transfer history."""
    query = select(BulkTransfer).order_by(desc(BulkTransfer.created_at))

    if gym_id:
        query = query.where(BulkTransfer.gym_id == gym_id)
    if status_filter:
        query = query.where(BulkTransfer.status == status_filter)

    query = query.limit(limit).offset(offset)
    result = await db.execute(query)
    transfers = result.scalars().all()

    return {
        "transfers": [
            {
                "id": t.id,
                "transfer_ref": t.transfer_ref,
                "gym_id": t.gym_id,
                "transfer_type": t.transfer_type,
                "transfer_date": str(t.transfer_date),
                "payout_count": t.payout_count,
                "total_gross": str(t.total_gross),
                "total_net": str(t.total_net),
                "total_pg_fee": str(t.total_pg_fee),
                "total_tds": str(t.total_tds),
                "razorpay_payout_id": t.razorpay_payout_id,
                "utr": t.utr,
                "status": t.status,
                "failure_reason": t.failure_reason,
                "initiated_at": str(t.initiated_at) if t.initiated_at else None,
                "credited_at": str(t.credited_at) if t.credited_at else None,
            }
            for t in transfers
        ],
        "limit": limit,
        "offset": offset,
    }


# ─── Dashboard Analytics ─────────────────────────────────────────────────────


@router.get("/dashboard/summary")
async def get_dashboard_summary(
    gym_id: Optional[int] = None,
    db: AsyncSession = Depends(get_async_db),
):
    """
    Get settlement & payout summary stats for dashboard.
    """
    today = datetime.now(IST).date()
    week_ago = today - timedelta(days=7)

    # Base filters
    payout_filter = []
    transfer_filter = []
    if gym_id:
        payout_filter.append(Payout.gym_id == gym_id)
        transfer_filter.append(BulkTransfer.gym_id == gym_id)

    # Pending payouts
    pending_result = await db.execute(
        select(
            func.count(Payout.id),
            func.coalesce(func.sum(Payout.amount_net), 0),
        ).where(
            and_(
                Payout.status.in_(["scheduled", "initiated", "processing"]),
                *payout_filter,
            )
        )
    )
    pending_count, pending_amount = pending_result.one()

    # Credited this week
    credited_result = await db.execute(
        select(
            func.count(BulkTransfer.id),
            func.coalesce(func.sum(BulkTransfer.total_net), 0),
        ).where(
            and_(
                BulkTransfer.status == "credited",
                BulkTransfer.credited_at >= week_ago,
                *transfer_filter,
            )
        )
    )
    credited_count, credited_amount = credited_result.one()

    # Failed transfers
    failed_result = await db.execute(
        select(func.count(BulkTransfer.id)).where(
            and_(
                BulkTransfer.status == "failed",
                *transfer_filter,
            )
        )
    )
    failed_count = failed_result.scalar() or 0

    # On-hold payouts
    on_hold_result = await db.execute(
        select(
            func.count(Payout.id),
            func.coalesce(func.sum(Payout.amount_net), 0),
        ).where(
            and_(
                Payout.status == "on_hold",
                *payout_filter,
            )
        )
    )
    on_hold_count, on_hold_amount = on_hold_result.one()

    return {
        "pending_payouts": {"count": pending_count, "amount": str(pending_amount)},
        "credited_this_week": {"count": credited_count, "amount": str(credited_amount)},
        "failed_transfers": {"count": failed_count},
        "on_hold": {"count": on_hold_count, "amount": str(on_hold_amount)},
    }
