# Backend Implementation for Reward Program Participants API
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import func, desc, asc, select, distinct
from typing import Optional
import math

from app.models.fittbot_models import (
    Client,
    ClientFittbotAccess,
    FittbotPlans,
    FittbotGymMembership,
    Gym,
    RewardProgramOptIn,
)
from app.models.async_database import get_async_db

router = APIRouter(prefix="/api/admin/reward-participants", tags=["RewardParticipants"])

class RewardParticipantResponse(BaseModel):
    client_id: int
    name: Optional[str] = None
    email: Optional[str] = None
    contact: Optional[str] = None
    gender: Optional[str] = None
    profile: Optional[str] = None
    gym_name: Optional[str] = None
    gym_location: Optional[str] = None
    fittbot_plan: Optional[str] = None
    fittbot_access_status: Optional[str] = None
    opt_in_date: Optional[str] = None
    created_at: Optional[str] = None


@router.get("")
async def get_reward_participants(
    page: int = Query(1, ge=1, description="Page number"),
    limit: int = Query(10, ge=1, le=100, description="Items per page"),
    search: Optional[str] = Query(None, description="Search by name, email, or mobile"),
    sort_order: str = Query("desc", description="Sort order for opt-in date"),
    db: AsyncSession = Depends(get_async_db)
):
    """
    Get list of reward program participants with pagination and search.
    Returns clients who have opted into the reward program.
    """
    try:
        # Build query to get reward program participants with client details
        # Using DISTINCT to avoid duplicates if a client opted in multiple times
        stmt = select(
            func.distinct(RewardProgramOptIn.client_id).label('client_id')
        ).select_from(
            RewardProgramOptIn
        )

        # Execute the subquery first to get participant client_ids
        result = await db.execute(stmt)
        participant_client_ids = [row[0] for row in result.all() if row[0] is not None]

        if not participant_client_ids:
            return {
                "success": True,
                "data": {
                    "participants": [],
                    "total": 0,
                    "page": page,
                    "limit": limit,
                    "totalPages": 0,
                    "hasNext": False,
                    "hasPrev": False,
                },
                "message": "No reward program participants found"
            }

        # Now build the main query to get full client details
        main_stmt = select(
            Client.client_id,
            Client.name,
            Client.email,
            Client.contact,
            Client.gender,
            Client.profile,
            Client.created_at,
            Gym.name.label('gym_name'),
            Gym.location.label('gym_location'),
            FittbotPlans.plan_name.label('fittbot_plan_name'),
            ClientFittbotAccess.access_status.label('fittbot_access_status'),
            func.max(RewardProgramOptIn.created_at).label('opt_in_date')
        ).outerjoin(
            Gym, Client.gym_id == Gym.gym_id
        ).outerjoin(
            ClientFittbotAccess, Client.client_id == ClientFittbotAccess.client_id
        ).outerjoin(
            FittbotPlans, ClientFittbotAccess.fittbot_plan == FittbotPlans.id
        ).join(
            RewardProgramOptIn, Client.client_id == RewardProgramOptIn.client_id
        ).where(
            Client.client_id.in_(participant_client_ids)
        ).group_by(
            Client.client_id,
            Client.name,
            Client.email,
            Client.contact,
            Client.gender,
            Client.profile,
            Client.created_at,
            Gym.name,
            Gym.location,
            FittbotPlans.plan_name,
            ClientFittbotAccess.access_status
        )

        # Apply search filter
        if search:
            search_term = f"%{search.lower()}%"
            main_stmt = main_stmt.where(
                func.lower(Client.name).like(search_term) |
                func.lower(Client.email).like(search_term) |
                Client.contact.like(search_term)
            )

        # Apply sorting by opt-in date
        if sort_order == "asc":
            main_stmt = main_stmt.order_by(asc(func.max(RewardProgramOptIn.created_at)))
        else:
            main_stmt = main_stmt.order_by(desc(func.max(RewardProgramOptIn.created_at)))

        # Get total count
        count_stmt = select(func.count()).select_from(main_stmt.subquery())
        count_result = await db.execute(count_stmt)
        total_count = count_result.scalar() or 0

        # Apply pagination
        offset = (page - 1) * limit
        main_stmt = main_stmt.offset(offset).limit(limit)

        # Execute query
        result = await db.execute(main_stmt)
        participants = result.all()

        # Format response
        participants_data = []
        for participant in participants:
            participant_data = {
                "client_id": participant.client_id,
                "name": participant.name or "-",
                "email": participant.email or "-",
                "contact": participant.contact or "-",
                "gender": participant.gender,
                "profile": participant.profile,
                "gym_name": participant.gym_name or "-",
                "gym_location": participant.gym_location or "-",
                "fittbot_plan": participant.fittbot_plan_name,
                "fittbot_access_status": participant.fittbot_access_status or "inactive",
                "opt_in_date": participant.opt_in_date.isoformat() if participant.opt_in_date else None,
                "created_at": participant.created_at.isoformat() if participant.created_at else None,
            }
            participants_data.append(participant_data)

        # Calculate pagination info
        total_pages = math.ceil(total_count / limit)
        has_next = page < total_pages
        has_prev = page > 1

        return {
            "success": True,
            "data": {
                "participants": participants_data,
                "total": total_count,
                "page": page,
                "limit": limit,
                "totalPages": total_pages,
                "hasNext": has_next,
                "hasPrev": has_prev,
            },
            "message": "Reward program participants fetched successfully"
        }

    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error fetching reward program participants: {str(e)}")


@router.get("/summary")
async def get_reward_participants_summary(db: AsyncSession = Depends(get_async_db)):
    """
    Get summary statistics for reward program participants.
    """
    try:
        # Get total distinct participants
        stmt = select(func.count(distinct(RewardProgramOptIn.client_id)))
        result = await db.execute(stmt)
        total_participants = result.scalar() or 0

        return {
            "success": True,
            "data": {
                "total": total_participants
            },
            "message": "Reward program participants summary fetched successfully"
        }

    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error fetching summary: {str(e)}")
