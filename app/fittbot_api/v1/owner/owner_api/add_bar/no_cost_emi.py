from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from typing import Optional
from app.models.async_database import get_async_db
from app.models.fittbot_models import NoCostEmi
from app.utils.logging_utils import FittbotHTTPException


router = APIRouter(prefix="/no_cost_emi", tags=["No Cost EMI"])


class NoCostEmiRequest(BaseModel):
    gym_id: int
    no_cost_emi: Optional[bool]=None 
    bnpl:  Optional[bool]=None


@router.post("/set")
async def set_no_cost_emi(payload: NoCostEmiRequest, db: AsyncSession = Depends(get_async_db)):

    try:
        result = await db.execute(select(NoCostEmi).where(NoCostEmi.gym_id == payload.gym_id))
        record = result.scalars().first()

        if not record:
            record = NoCostEmi(
                gym_id=payload.gym_id,
                no_cost_emi=bool(payload.no_cost_emi) if payload.no_cost_emi is not None else False,
                bnpl=bool(payload.bnpl) if payload.bnpl is not None else False,
            )
            db.add(record)
        else:
            if payload.no_cost_emi is not None:
                record.no_cost_emi = bool(payload.no_cost_emi)
            if payload.bnpl is not None:
                record.bnpl = bool(payload.bnpl)

        await db.commit()
        await db.refresh(record)

        return {
            "status": 200,
            "message": "No cost EMI settings updated",
            "data": {
                "gym_id": record.gym_id,
                "no_cost_emi": record.no_cost_emi,
                "bnpl": record.bnpl,
            },
        }
    except FittbotHTTPException:
        raise
    except Exception as exc:
        await db.rollback()
        raise FittbotHTTPException(
            status_code=500,
            detail="Failed to update no cost EMI settings",
            error_code="NO_COST_EMI_UPDATE_FAILED",
            log_data={"error": repr(exc), "gym_id": payload.gym_id},
        )


@router.get("/terms_and_conditions")
async def get_no_cost_emi_terms():
    """Get Terms & Conditions for No Cost EMI feature"""
    return {
        "status": 200,
        "data": {
            "title": "No Cost EMI Terms & Conditions",
            "deduction_percentage": 5,
            "emi_tenure_months": 3,
            "minimum_amount": 4000,
            "terms": [
                "A 5% processing fee will be deducted from the total payment amount received by the gym owner.",
                "Clients can only pay using 3-month EMI tenure option.",
                "No Cost EMI is available only for plans with amount ₹4,000 or above.",
                "The EMI interest is borne by the gym owner through the 5% deduction.",
                "Clients will see 0% interest on their EMI payments.",
                "The full payment (minus 5% deduction) will be settled to the gym owner within standard settlement timelines.",
                "This feature can be enabled or disabled at any time from the gym settings.",
                "Razorpay's standard EMI terms and conditions apply.",
            ],
            "summary": "By enabling No Cost EMI, you agree that 5% of the transaction amount will be deducted as processing fee. Your clients will be able to pay in 3-month interest-free EMI installments for plans ₹4,000 and above."
        }
    }


@router.get("/get")
async def get_no_cost_emi(gym_id: int, db: AsyncSession = Depends(get_async_db)):

    try:
        result = await db.execute(select(NoCostEmi).where(NoCostEmi.gym_id == gym_id))
        record = result.scalars().first()

        if not record:
            return {
                "status": 200,
                "data": {
                    "gym_id": gym_id,
                    "no_cost_emi": False,
                    "bnpl": False,
                },
                "message": "No cost EMI settings not found; defaulting to false",
            }

        return {
            "status": 200,
            "data": {
                "gym_id": record.gym_id,
                "no_cost_emi": record.no_cost_emi,
                "bnpl": record.bnpl,
            },
        }
    except FittbotHTTPException:
        raise
    except Exception as exc:
        await db.rollback()
        raise FittbotHTTPException(
            status_code=500,
            detail="Failed to fetch no cost EMI settings",
            error_code="NO_COST_EMI_FETCH_FAILED",
            log_data={"error": repr(exc), "gym_id": gym_id},
        )
