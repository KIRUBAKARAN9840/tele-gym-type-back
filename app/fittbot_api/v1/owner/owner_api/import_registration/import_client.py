"""
Import Client Fee Update API
For updating fees of imported clients (GymImportData)
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timedelta, timezone
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, status
from redis.asyncio import Redis
from pydantic import BaseModel, Field
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.async_database import get_async_db
from app.utils.redis_config import get_redis
from app.models.fittbot_models import (
    GymImportData,
    GymPlans,
    FittbotGymMembership,
    FeesReceipt,
    Gym,
    AccountDetails,
    GymMonthlyData,
)

logger = logging.getLogger("owner.import_registration")

router = APIRouter(prefix="/owner/gym/import", tags=["Import Client Registration"])

IST = timezone(timedelta(hours=5, minutes=30))


def _now_ist() -> datetime:
    return datetime.now(IST)


def _today_ist() -> date:
    return _now_ist().date()


class ImportUpdateFeeRequest(BaseModel):
    """Request model for updating fee status of import client"""
    manual_client_id: int = Field(..., gt=0)  # This is import_id from GymImportData
    gym_id: int = Field(..., gt=0)
    plan_id: int = Field(..., gt=0)
    fees: float = Field(..., ge=0)
    total_amount: float = Field(..., ge=0)
    discount_amount: Optional[float] = 0
    payment_method: Optional[str] = None
    joined_at: Optional[date] = None
    expires_at: Optional[date] = None
    batch_id: Optional[int] = None


@router.post("/update_fee_status")
async def update_import_client_fee_status(
    payload: ImportUpdateFeeRequest,
    db: AsyncSession = Depends(get_async_db),
    redis: Redis = Depends(get_redis),
):
    """
    Update fee status for an imported client.
    Checks GymImportData table and creates FittbotGymMembership with client_id = "import_{id}"
    """
    try:
        #(f"[import_update_fee_status] Received payload: {payload.dict()}")

        # Find the import client in GymImportData table
        result = await db.execute(
            select(GymImportData).where(GymImportData.import_id == payload.manual_client_id)
        )
        client = result.scalars().first()

        if not client:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Import client not found"
            )

        if client.gym_id != payload.gym_id:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Client does not belong to this gym"
            )

        #(f"[import_update_fee_status] Found client: {client.client_name} (id={client.import_id})")

        plan_result = await db.execute(
            select(GymPlans).where(GymPlans.id == payload.plan_id)
        )
        plan = plan_result.scalars().first()

        if not plan:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Plan not found"
            )

        joined_at = payload.joined_at or _today_ist()
        expires_at = payload.expires_at

        if not expires_at and plan.duration:
            from dateutil.relativedelta import relativedelta
            expires_at = joined_at + relativedelta(months=int(plan.duration))

        # Update GymImportData record
        client.joined_at = joined_at
        client.expires_at = expires_at
        client.status = "active"

        #logger.info(f"[import_update_fee_status] Updated client: joined_at={joined_at}, expires_at={expires_at}")

        # Update GymMonthlyData
        month_tag = datetime.now().strftime("%Y-%m")
        monthly_result = await db.execute(
            select(GymMonthlyData).where(
                GymMonthlyData.gym_id == payload.gym_id,
                GymMonthlyData.month_year.like(f"{month_tag}%")
            )
        )
        monthly_rec = monthly_result.scalars().first()

        if monthly_rec:
            monthly_rec.income = (monthly_rec.income or 0) + payload.fees
        else:
            new_monthly = GymMonthlyData(
                gym_id=payload.gym_id,
                month_year=datetime.now().strftime("%Y-%m-%d"),
                income=payload.fees,
                expenditure=0,
                new_entrants=0
            )
            db.add(new_monthly)

        # Get gym and account details
        gym_result = await db.execute(select(Gym).where(Gym.gym_id == payload.gym_id))
        gym = gym_result.scalars().first()

        account_result = await db.execute(
            select(AccountDetails).where(AccountDetails.gym_id == payload.gym_id)
        )
        account = account_result.scalars().first()
        
        import_client_id_str = f"import_{client.import_id}"

        membership_result = await db.execute(
            select(FittbotGymMembership).where(
                FittbotGymMembership.client_id == import_client_id_str,
                FittbotGymMembership.gym_id == str(payload.gym_id)
            ).order_by(FittbotGymMembership.id.desc())
        )
        existing_membership = membership_result.scalars().first()

        if existing_membership:
            existing_membership.status = "inactive"

        new_membership = FittbotGymMembership(
            gym_id=str(payload.gym_id),
            client_id=import_client_id_str,
            plan_id=payload.plan_id,
            type="normal",
            amount=payload.total_amount,
            purchased_at=_now_ist(),
            status="active",
            joined_at=joined_at,
            expires_at=expires_at,
        )
        db.add(new_membership)

        # Create FeesReceipt
        if gym and plan:
            new_receipt = FeesReceipt(
                client_id=None,
                manual_client_id=None,
                gym_id=payload.gym_id,
                client_name=client.client_name,
                gym_name=gym.name,
                gym_logo=gym.logo,
                gym_contact=gym.contact_number or "",
                gym_location=gym.location,
                plan_id=payload.plan_id,
                plan_description=plan.plans,
                fees=plan.amount,
                discount=payload.discount_amount or 0,
                discounted_fees=payload.total_amount,
                due_date=expires_at,
                invoice_number=None,
                client_contact=client.client_contact,
                bank_details=account.account_number if account else "",
                ifsc_code=account.account_ifsccode if account else "",
                account_holder_name=account.account_holdername if account else "",
                invoice_date=_today_ist(),
                payment_method=payload.payment_method,
                gst_number=account.gst_number if account else "",
                bank_name=account.bank_name if account else "",
                branch=account.account_branch if account else "",
                client_email=client.client_email,
                mail_status=False,
                payment_date=joined_at,
                payment_reference_number=None,
                created_at=_now_ist(),
                update_at=_now_ist(),
                gst_percentage=None,
                gst_type=None,
                total_amount=payload.total_amount,
                fees_type="Renewal"
            )
            db.add(new_receipt)
            await db.flush()

            receipt_count_result = await db.execute(
                select(func.count()).select_from(FeesReceipt).where(FeesReceipt.gym_id == payload.gym_id)
            )
            gym_receipt_count = receipt_count_result.scalar() or 0
            location_prefix = (gym.location[:3].upper() if gym.location else "GYM")
            new_receipt.invoice_number = f"{location_prefix}-{gym.gym_id}-{gym_receipt_count}"

        await db.commit()

        try:
            await redis.delete(f"gym:{payload.gym_id}:members")
        except Exception as cache_err:
            logger.warning(f"Failed to clear cache: {cache_err}")

        return {
            "status": 200,
            "message": "Fee status updated successfully",
            "client_data": {
                "import_id": client.import_id,
                "name": client.client_name,
                "joined_at": str(client.joined_at) if client.joined_at else None,
                "expires_at": str(client.expires_at) if client.expires_at else None,
                "status": client.status
            }
        }

    except HTTPException:
        raise
    except Exception as e:
        await db.rollback()
        logger.error(f"[import_update_fee_status] Error: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to update fee status: {str(e)}"
        )
