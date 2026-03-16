

from datetime import datetime, date
from typing import Optional

from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.async_database import get_async_db
from app.models.fittbot_models import RewardProgramOptIn, RewardProgramEntry
from app.utils.logging_utils import FittbotHTTPException

router = APIRouter(prefix="/reward_program", tags=["Client Reward Program"])

# Program dates
PROGRAM_START_DATE = date(2026, 1, 26)
PROGRAM_END_DATE = date(2026, 5, 31)

# Max entries per method
MAX_ENTRIES = {
    "dailypass": 100,
    "session": 100,
    "subscription": 8,
    "gym_membership": 15,
    "referral": 25,
}


class OptInRequest(BaseModel):
    client_id: int


class OptInResponse(BaseModel):
    opted_in: bool
    opted_in_at: Optional[datetime] = None
    status: Optional[str] = None


class DashboardEntry(BaseModel):
    entry_id: str
    method: str
    created_at: datetime


class DashboardResponse(BaseModel):
    opted_in: bool
    total_entries: int
    entries_by_method: dict
    entries: list


@router.get("/check_opted_in")
async def check_opted_in(
    client_id: int,
    db: AsyncSession = Depends(get_async_db),
):

    try:
        result = await db.execute(
            select(RewardProgramOptIn).where(
                RewardProgramOptIn.client_id == client_id
            )
        )
        opt_in = result.scalars().first()

        if opt_in:
            return {
                "status": 200,
                "opted_in": True
                }
            
        else:
            return {
                "status": 200,
                "opted_in": False
                }
            

    except Exception as exc:
        raise FittbotHTTPException(
            status_code=500,
            detail="Failed to check opt-in status",
            error_code="REWARD_CHECK_OPT_IN_ERROR",
            log_data={"error": repr(exc), "client_id": client_id},
        )


@router.post("/opt_in")
async def opt_in(
    request: OptInRequest,
    db: AsyncSession = Depends(get_async_db),
):
    client_id = request.client_id

    try:
        today = date.today()

        if today < PROGRAM_START_DATE:
            return {
                "status": 400,
                "message": f"Program has not started yet. It begins on {PROGRAM_START_DATE.strftime('%B %d, %Y')}.",
                "data": {"opted_in": False}
            }

        if today > PROGRAM_END_DATE:
            return {
                "status": 400,
                "message": f"Program has ended on {PROGRAM_END_DATE.strftime('%B %d, %Y')}.",
                "data": {"opted_in": False}
            }

        existing = await db.execute(
            select(RewardProgramOptIn).where(
                RewardProgramOptIn.client_id == client_id
            )
        )
        existing_opt_in = existing.scalars().first()

        if existing_opt_in:

            return {
                "status": 200,
                "message": "You are already enrolled in the Fymble Mega Fitness Rewards Program!",
                "data": {
                    "opted_in": True,
                    "opted_in_at": existing_opt_in.opted_in_at.isoformat() if existing_opt_in.opted_in_at else None,
                }
            }


        new_opt_in = RewardProgramOptIn(
            client_id=client_id,
            opted_in_at=datetime.now(),
            status="active",
        )
        db.add(new_opt_in)
        await db.commit()
        await db.refresh(new_opt_in)

        return {
            "status": 200,
            "message": "Successfully enrolled in the Fymble Mega Fitness Rewards Program!",
            "data": {
                "opted_in": True,
                "opted_in_at": new_opt_in.opted_in_at.isoformat() if new_opt_in.opted_in_at else None,
            }
        }

    except Exception as exc:
        await db.rollback()
        raise FittbotHTTPException(
            status_code=500,
            detail="Failed to opt into reward program",
            error_code="REWARD_OPT_IN_ERROR",
            log_data={"error": repr(exc), "client_id": client_id},
        )


@router.get("/dashboard")
async def reward_dashboard(
    client_id: int,
    db: AsyncSession = Depends(get_async_db),
):

    try:

        opt_in_result = await db.execute(
            select(RewardProgramOptIn).where(
                RewardProgramOptIn.client_id == client_id
            )
        )
        opt_in = opt_in_result.scalars().first()

        if not opt_in:
            return {
                "status": 200,
                "data": {
                    "opted_in": False,
                    "total_entries": 0,
                    "entries_by_method": {
                        "dailypass": {"count": 0, "max": MAX_ENTRIES["dailypass"]},
                        "session": {"count": 0, "max": MAX_ENTRIES["session"]},
                        "subscription": {"count": 0, "max": MAX_ENTRIES["subscription"]},
                        "gym_membership": {"count": 0, "max": MAX_ENTRIES["gym_membership"]},
                        "referral": {"count": 0, "max": MAX_ENTRIES["referral"]},
                    },
                    "entries": [],
                }
            }

        entries_result = await db.execute(
            select(RewardProgramEntry)
            .where(
                RewardProgramEntry.client_id == client_id,
                RewardProgramEntry.status == "valid",
            )
            .order_by(RewardProgramEntry.created_at.desc())
        )
        entries = entries_result.scalars().all()

        entries_by_method = {
            "dailypass": {"count": 0, "max": MAX_ENTRIES["dailypass"]},
            "session": {"count": 0, "max": MAX_ENTRIES["session"]},
            "subscription": {"count": 0, "max": MAX_ENTRIES["subscription"]},
            "gym_membership": {"count": 0, "max": MAX_ENTRIES["gym_membership"]},
            "referral": {"count": 0, "max": MAX_ENTRIES["referral"]},
        }

        entries_list = []
        for entry in entries:
            if entry.method in entries_by_method:
                entries_by_method[entry.method]["count"] += 1

            entries_list.append({
                "entry_id": entry.entry_id,
                "method": entry.method,
                "created_at": entry.created_at.isoformat() if entry.created_at else None,
            })

        total_entries = sum(m["count"] for m in entries_by_method.values())

        return {
            "status": 200,
            "data": {
                "opted_in": True,
                "opted_in_at": opt_in.opted_in_at.isoformat() if opt_in.opted_in_at else None,
                "total_entries": total_entries,
                "entries_by_method": entries_by_method,
                "entries": entries_list,
                "program_start": PROGRAM_START_DATE.isoformat(),
                "program_end": PROGRAM_END_DATE.isoformat(),
            }
        }

    except Exception as exc:
        raise FittbotHTTPException(
            status_code=500,
            detail="Failed to fetch reward dashboard",
            error_code="REWARD_DASHBOARD_ERROR",
            log_data={"error": repr(exc), "client_id": client_id},
        )
