"""
API endpoints to fetch data from owner's oldest gym for copying to new gyms.
Only returns data if owner has more than one gym.
"""

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from pydantic import BaseModel
from typing import Optional, List, Any
from datetime import datetime

from app.models.async_database import get_async_db
from app.models.fittbot_models import Gym, GymOwner, AccountDetails
from app.utils.logging_utils import auth_logger

router = APIRouter(prefix="/get_old_data", tags=["Get Old Data"])


# Response Models
class OldGymDataResponse(BaseModel):
    status: int
    message: str
    has_other_gyms: bool
    oldest_gym_name: Optional[str] = None
    data: Optional[Any] = None


class CheckMultipleGymsResponse(BaseModel):
    status: int
    has_multiple_gyms: bool
    oldest_gym_id: Optional[int] = None
    oldest_gym_name: Optional[str] = None


async def get_oldest_gym_for_owner(owner_id: int, current_gym_id: int, db: AsyncSession):
    """
    Helper function to get the oldest gym for an owner, excluding the current gym.
    Returns None if owner has only one gym.
    """
    # Get all gyms for this owner
    stmt = select(Gym).where(Gym.owner_id == owner_id).order_by(Gym.gym_id.asc())
    result = await db.execute(stmt)
    all_gyms = result.scalars().all()

    # If owner has only one gym, return None
    if len(all_gyms) <= 1:
        return None

    # Get the oldest gym (excluding current gym)
    for gym in all_gyms:
        if gym.gym_id != current_gym_id:
            return gym

    return None


@router.get("/check_multiple_gyms", response_model=CheckMultipleGymsResponse)
async def check_multiple_gyms(
    owner_id: int,
    current_gym_id: int,
    db: AsyncSession = Depends(get_async_db)
):
    """
    Check if owner has multiple gyms and return oldest gym info.
    """
    try:
        oldest_gym = await get_oldest_gym_for_owner(owner_id, current_gym_id, db)

        if oldest_gym:
            return CheckMultipleGymsResponse(
                status=200,
                has_multiple_gyms=True,
                oldest_gym_id=oldest_gym.gym_id,
                oldest_gym_name=oldest_gym.name
            )
        else:
            return CheckMultipleGymsResponse(
                status=200,
                has_multiple_gyms=False
            )
    except Exception as e:
        auth_logger.error(f"Error checking multiple gyms: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/services", response_model=OldGymDataResponse)
async def get_old_services_data(
    owner_id: int,
    current_gym_id: int,
    db: AsyncSession = Depends(get_async_db)
):
    """
    Get services data from owner's oldest gym.
    Returns services array if owner has multiple gyms and oldest gym has services.
    """
    try:
        oldest_gym = await get_oldest_gym_for_owner(owner_id, current_gym_id, db)

        if not oldest_gym:
            return OldGymDataResponse(
                status=200,
                message="Owner has only one gym",
                has_other_gyms=False
            )

        # Check if oldest gym has services data
        if not oldest_gym.services:
            return OldGymDataResponse(
                status=200,
                message="Oldest gym has no services data",
                has_other_gyms=True,
                oldest_gym_name=oldest_gym.name,
                data=None
            )

        return OldGymDataResponse(
            status=200,
            message="Services data found",
            has_other_gyms=True,
            oldest_gym_name=oldest_gym.name,
            data=oldest_gym.services
        )

    except Exception as e:
        auth_logger.error(f"Error fetching old services data: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/operating_hours", response_model=OldGymDataResponse)
async def get_old_operating_hours_data(
    owner_id: int,
    current_gym_id: int,
    db: AsyncSession = Depends(get_async_db)
):
    """
    Get operating hours data from owner's oldest gym.
    Returns operating_hours array if owner has multiple gyms and oldest gym has data.
    """
    try:
        oldest_gym = await get_oldest_gym_for_owner(owner_id, current_gym_id, db)

        if not oldest_gym:
            return OldGymDataResponse(
                status=200,
                message="Owner has only one gym",
                has_other_gyms=False
            )

        # Check if oldest gym has operating hours data
        if not oldest_gym.operating_hours:
            return OldGymDataResponse(
                status=200,
                message="Oldest gym has no operating hours data",
                has_other_gyms=True,
                oldest_gym_name=oldest_gym.name,
                data=None
            )

        return OldGymDataResponse(
            status=200,
            message="Operating hours data found",
            has_other_gyms=True,
            oldest_gym_name=oldest_gym.name,
            data=oldest_gym.operating_hours
        )

    except Exception as e:
        auth_logger.error(f"Error fetching old operating hours data: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/account_details", response_model=OldGymDataResponse)
async def get_old_account_details_data(
    owner_id: int,
    current_gym_id: int,
    db: AsyncSession = Depends(get_async_db)
):
    """
    Get account details from owner's oldest gym.
    Returns account details if owner has multiple gyms and oldest gym has data.
    """
    try:
        oldest_gym = await get_oldest_gym_for_owner(owner_id, current_gym_id, db)

        if not oldest_gym:
            return OldGymDataResponse(
                status=200,
                message="Owner has only one gym",
                has_other_gyms=False
            )

        # Fetch account details for oldest gym
        stmt = select(AccountDetails).where(AccountDetails.gym_id == oldest_gym.gym_id)
        result = await db.execute(stmt)
        account = result.scalar_one_or_none()

        if not account:
            return OldGymDataResponse(
                status=200,
                message="Oldest gym has no account details",
                has_other_gyms=True,
                oldest_gym_name=oldest_gym.name,
                data=None
            )

        # Return account details (excluding sensitive audit fields)
        account_data = {
            "account_number": account.account_number,
            "bank_name": account.bank_name,
            "account_ifsccode": account.account_ifsccode,
            "account_branch": account.account_branch,
            "account_holdername": account.account_holdername,
            "upi_id": account.upi_id,
            "gst_number": account.gst_number,
            "pan_number": account.pan_number,
            "gst_type": account.gst_type,
            "gst_percentage": account.gst_percentage
        }

        return OldGymDataResponse(
            status=200,
            message="Account details found",
            has_other_gyms=True,
            oldest_gym_name=oldest_gym.name,
            data=account_data
        )

    except Exception as e:
        auth_logger.error(f"Error fetching old account details: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
