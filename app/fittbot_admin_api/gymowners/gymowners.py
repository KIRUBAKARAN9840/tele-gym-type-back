from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import or_, func, desc, asc, select
from datetime import datetime, timedelta
from typing import Optional, List
import math

from app.models.fittbot_models import GymOwner, Gym
from app.models.async_database import get_async_db

router = APIRouter(prefix="/api/admin/gym-owners", tags=["AdminGymOwners"])


@router.get("/list")
async def get_gym_owners(
    page: int = Query(1, ge=1, description="Page number"),
    limit: int = Query(10, ge=1, le=100, description="Items per page"),
    search: Optional[str] = Query(None, description="Search by owner name, email, or contact"),
    sort_order: str = Query("desc", description="Sort order for created_at"),
    db: AsyncSession = Depends(get_async_db)
):
    """
    Get list of gym owners with their associated gyms
    """
    try:
        # Build base query to get gym owners
        stmt = select(GymOwner)

        # Apply search filter
        if search:
            search_term = f"%{search.lower()}%"
            stmt = stmt.where(
                or_(
                    func.lower(GymOwner.name).like(search_term),
                    func.lower(GymOwner.email).like(search_term),
                    GymOwner.contact_number.like(search_term)
                )
            )

        # Apply sorting
        if sort_order == "asc":
            stmt = stmt.order_by(asc(GymOwner.created_at))
        else:
            stmt = stmt.order_by(desc(GymOwner.created_at))

        # Get total count before pagination
        count_stmt = select(func.count()).select_from(stmt.subquery())
        count_result = await db.execute(count_stmt)
        total_count = count_result.scalar() or 0

        # Apply pagination
        offset = (page - 1) * limit
        stmt = stmt.offset(offset).limit(limit)

        # Execute query
        result = await db.execute(stmt)
        owners = result.scalars().all()

        # Fetch gyms for each owner
        owners_with_gyms = []
        for owner in owners:
            # Get all gyms associated with this owner
            gym_stmt = select(Gym).where(Gym.owner_id == owner.owner_id)
            gym_result = await db.execute(gym_stmt)
            gyms = gym_result.scalars().all()

            gym_list = []
            for gym in gyms:
                gym_data = {
                    "gym_id": gym.gym_id,
                    "name": gym.name,
                    "location": gym.location or gym.city or "N/A",
                    "fittbot_verified": gym.fittbot_verified,
                    "created_at": gym.created_at.isoformat() if gym.created_at else None
                }
                gym_list.append(gym_data)

            owner_data = {
                "owner_id": owner.owner_id,
                "name": owner.name or "N/A",
                "email": owner.email or "N/A",
                "contact_number": owner.contact_number or "N/A",
                "profile": owner.profile,
                "created_at": owner.created_at.isoformat() if owner.created_at else None,
                "gyms": gym_list,
                "total_gyms": len(gym_list)
            }
            owners_with_gyms.append(owner_data)

        # Calculate pagination info
        total_pages = math.ceil(total_count / limit)
        has_next = page < total_pages
        has_prev = page > 1

        return {
            "success": True,
            "data": {
                "owners": owners_with_gyms,
                "total": total_count,
                "page": page,
                "limit": limit,
                "totalPages": total_pages,
                "hasNext": has_next,
                "hasPrev": has_prev
            },
            "message": "Gym owners fetched successfully"
        }

    except Exception as e:
        import traceback
        traceback.print_exc()
        return {
            "success": False,
            "message": f"Error fetching gym owners: {str(e)}"
        }


@router.get("/stats")
async def get_gym_owners_stats(
    db: AsyncSession = Depends(get_async_db)
):
    """
    Get overall gym owners statistics
    """
    try:
        today = datetime.now().date()

        # Total gym owners
        total_owners_stmt = select(func.count()).select_from(GymOwner)
        total_owners_result = await db.execute(total_owners_stmt)
        total_owners = total_owners_result.scalar() or 0

        # Owners today
        owners_today_stmt = select(func.count()).select_from(GymOwner).where(
            func.date(GymOwner.created_at) == today
        )
        owners_today_result = await db.execute(owners_today_stmt)
        owners_today = owners_today_result.scalar() or 0

        # Owners this week
        owners_week_stmt = select(func.count()).select_from(GymOwner).where(
            GymOwner.created_at >= today - timedelta(days=7),
            GymOwner.created_at < today + timedelta(days=1)
        )
        owners_week_result = await db.execute(owners_week_stmt)
        owners_week = owners_week_result.scalar() or 0

        # Owners this month
        owners_month_stmt = select(func.count()).select_from(GymOwner).where(
            GymOwner.created_at >= today - timedelta(days=30),
            GymOwner.created_at < today + timedelta(days=1)
        )
        owners_month_result = await db.execute(owners_month_stmt)
        owners_month = owners_month_result.scalar() or 0

        return {
            "success": True,
            "data": {
                "total": total_owners,
                "today": owners_today,
                "week": owners_week,
                "month": owners_month
            },
            "message": "Gym owners stats fetched successfully"
        }

    except Exception as e:
        import traceback
        traceback.print_exc()
        return {
            "success": False,
            "message": f"Error fetching gym owners stats: {str(e)}"
        }
