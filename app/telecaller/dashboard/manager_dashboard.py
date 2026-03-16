from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import func, and_, cast, Date, select, text
from datetime import datetime, timedelta
from app.models.async_database import get_async_db
from app.models.telecaller_models import Manager, Telecaller, GymAssignment, GymCallLogs,GymDatabase

from app.telecaller.dependencies import get_current_manager
from typing import Dict, Any, List
from pydantic import BaseModel
import logging

logger = logging.getLogger(__name__)

router = APIRouter()

@router.get("/stats")
async def get_manager_dashboard_stats(
    db: AsyncSession = Depends(get_async_db),
    current_manager: Manager = Depends(get_current_manager)
) -> Dict[str, Any]:
    """
    Get dashboard statistics for manager - async database operations
    """
    try:


        today = datetime.now().date()
        start_of_day = datetime.combine(today, datetime.min.time())
        end_of_day = datetime.combine(today, datetime.max.time())

        # Get total telecallers under this manager - async
        stmt_telecallers = select(func.count(Telecaller.id)).where(
            Telecaller.manager_id == current_manager.id
        )
        result_telecallers = await db.execute(stmt_telecallers)
        total_telecallers = result_telecallers.scalar()

      

        # Get total assigned gyms (only active assignments) - async
        stmt_gyms = select(func.count(func.distinct(GymAssignment.gym_id))).where(
            and_(
                GymAssignment.manager_id == current_manager.id,
                GymAssignment.status == 'active'
            )
        )
        result_gyms = await db.execute(stmt_gyms)
        total_gyms_assigned = result_gyms.scalar()

        # Get telecaller IDs for this manager - async
        stmt_telecaller_ids = select(Telecaller.id).where(
            Telecaller.manager_id == current_manager.id
        )
        result_telecaller_ids = await db.execute(stmt_telecaller_ids)
        telecaller_ids_list = [row[0] for row in result_telecaller_ids.fetchall()]
        

        # Get today's stats using a single query with grouping - async
        # Skip if no telecallers
        if not telecaller_ids_list:
            logger.warning(f"No telecallers found for manager {current_manager.id}")
            today_stats = {}
        else:
            # Count by status for today
            stmt_today_calls = select(
                GymCallLogs.call_status,
                func.count(GymCallLogs.id).label('count')
            ).where(
                and_(
                    GymCallLogs.telecaller_id.in_(telecaller_ids_list),
                    GymCallLogs.created_at >= start_of_day,
                    GymCallLogs.created_at <= end_of_day
                )
            ).group_by(GymCallLogs.call_status)

            result_today_calls = await db.execute(stmt_today_calls)
            today_stats = {row.call_status: row.count for row in result_today_calls.fetchall()}

        converted_today = today_stats.get('converted', 0)
        rejected_today = today_stats.get('rejected', 0)
        no_response_today = today_stats.get('no_response', 0)

        # Get today's call target - async
        stmt_call_target = select(func.count(func.distinct(GymAssignment.gym_id))).where(
            and_(
                GymAssignment.manager_id == current_manager.id,
                GymAssignment.status == 'active',
                cast(GymAssignment.target_date, Date) == today
            )
        )
        result_call_target = await db.execute(stmt_call_target)
        todays_call_target = result_call_target.scalar() or 0

        # Calculate unassigned gyms - async
        stmt_inactive = select(GymAssignment.gym_id).where(
            and_(
                GymAssignment.manager_id == current_manager.id,
                GymAssignment.status == 'inactive'
            )
        ).distinct()

        result_inactive = await db.execute(stmt_inactive)
        inactive_gym_ids = {row[0] for row in result_inactive.fetchall()}

        stmt_active = select(GymAssignment.gym_id).where(
            GymAssignment.status == 'active'
        ).distinct()

        result_active = await db.execute(stmt_active)
        active_gym_ids = {row[0] for row in result_active.fetchall()}

        unassigned_gyms = len([gym_id for gym_id in inactive_gym_ids if gym_id not in active_gym_ids])

        # Get follow-ups pending - use IN clause with tuple
        followups_query = text("""
            SELECT COUNT(DISTINCT l.gym_id)
            FROM telecaller.gym_call_logs l
            WHERE l.telecaller_id IN :telecaller_ids
            AND l.created_at = (
                SELECT MAX(l2.created_at)
                FROM telecaller.gym_call_logs l2
                WHERE l2.gym_id = l.gym_id
            )
            AND l.call_status = 'follow_up'
        """)

        result_followups = await db.execute(followups_query, {"telecaller_ids": tuple(telecaller_ids_list)})
        followups_pending = result_followups.scalar() or 0

        # Get today's follow-ups
        todays_followups_query = text("""
            SELECT COUNT(DISTINCT l.gym_id)
            FROM telecaller.gym_call_logs l
            WHERE l.telecaller_id IN :telecaller_ids
            AND l.created_at = (
                SELECT MAX(l2.created_at)
                FROM telecaller.gym_call_logs l2
                WHERE l2.gym_id = l.gym_id
            )
            AND l.call_status = 'follow_up'
            AND DATE(l.follow_up_date) = :today
        """)

        result_todays_followups = await db.execute(todays_followups_query, {"telecaller_ids": tuple(telecaller_ids_list), "today": today})
        todays_followups = result_todays_followups.scalar() or 0

        return {
            "total_telecallers": total_telecallers,
            "total_gyms_assigned": total_gyms_assigned,
            "unassigned_gyms": unassigned_gyms,
            "todays_call_target": todays_call_target,
            "todays_followups": todays_followups,
            "converted_today": converted_today,
            "followups_pending": followups_pending,
            "rejected_today": rejected_today,
            "no_response_today": no_response_today,
        }

    except Exception as e:
        logger.error(f"Error fetching dashboard stats for manager_id {current_manager.id}: "
                    f"{str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch dashboard stats: {str(e)}"
        )

@router.get("/telecallers/list")
async def get_telecallers_list(
    search: str = None,
    status: str = None,
    page: int = 1,
    limit: int = 10,
    db: AsyncSession = Depends(get_async_db),
    current_manager: Manager = Depends(get_current_manager)
) -> Dict[str, Any]:
    """
    Get list of telecallers with performance metrics - async database operations
    For Super Admin (is_super_admin = 1): Returns ALL telecallers across all managers
    For Normal Manager (is_super_admin = 0): Returns only telecallers under that manager
    """
    try:


        offset = (page - 1) * limit

        # Check if this is a Super Admin
        is_super_admin = getattr(current_manager, 'is_super_admin', 0)

        # Build base query - filter by manager_id only for normal managers
        if is_super_admin == 1:
            # Super Admin: No manager filter, get all telecallers
            stmt = select(Telecaller)
        else:
            # Normal Manager: Filter by manager_id
            stmt = select(Telecaller).where(
                Telecaller.manager_id == current_manager.id
            )

        # Apply filters
        if search:
            search_term = f"%{search}%"
            stmt = stmt.where(
                Telecaller.name.ilike(search_term) |
                Telecaller.mobile_number.ilike(search_term)
            )

        if status:
            stmt = stmt.where(Telecaller.status == status)

        # Get telecallers first - simpler approach
        stmt_paginated = stmt.offset(offset).limit(limit)
        result_telecallers = await db.execute(stmt_paginated)
        telecallers = result_telecallers.scalars().all()
 

        # Get total count - apply same super admin logic
        if is_super_admin == 1:
            # Super Admin: Count all telecallers
            stmt_count = select(func.count(Telecaller.id))
        else:
            # Normal Manager: Count only telecallers under this manager
            stmt_count = select(func.count(Telecaller.id)).where(
                Telecaller.manager_id == current_manager.id
            )

        if search:
            search_term = f"%{search}%"
            stmt_count = stmt_count.where(
                Telecaller.name.ilike(search_term) |
                Telecaller.mobile_number.ilike(search_term)
            )
        if status:
            stmt_count = stmt_count.where(Telecaller.status == status)

        result_count = await db.execute(stmt_count)
        total = result_count.scalar() or 0
      

        # Get performance metrics for each telecaller
        today = datetime.now().date()
        start_of_day = datetime.combine(today, datetime.min.time())
        end_of_day = datetime.combine(today, datetime.max.time())
        thirty_days_ago = datetime.now() - timedelta(days=30)

        result = []
        for telecaller in telecallers:
            # Today's calls - async
            stmt_today_calls = select(func.count(GymCallLogs.id)).where(
                and_(
                    GymCallLogs.telecaller_id == telecaller.id,
                    GymCallLogs.created_at >= start_of_day,
                    GymCallLogs.created_at <= end_of_day
                )
            )
            result_today_calls = await db.execute(stmt_today_calls)
            today_calls_count = result_today_calls.scalar() or 0

            # Assigned gyms count - async (GymAssignment has composite PK, use count(*))
            stmt_assigned = select(func.count()).select_from(
                select(GymAssignment).where(
                    and_(
                        GymAssignment.telecaller_id == telecaller.id,
                        GymAssignment.status == 'active'
                    )
                ).subquery()
            )
            result_assigned = await db.execute(stmt_assigned)
            assigned_gyms = result_assigned.scalar() or 0

            # Total calls and conversions (last 30 days) - async
            stmt_total_calls = select(func.count(GymCallLogs.id)).where(
                and_(
                    GymCallLogs.telecaller_id == telecaller.id,
                    GymCallLogs.created_at >= thirty_days_ago
                )
            )
            result_total_calls = await db.execute(stmt_total_calls)
            total_calls = result_total_calls.scalar() or 0

            stmt_conversions = select(func.count(GymCallLogs.id)).where(
                and_(
                    GymCallLogs.telecaller_id == telecaller.id,
                    GymCallLogs.call_status == 'converted',
                    GymCallLogs.created_at >= thirty_days_ago
                )
            )
            result_conversions = await db.execute(stmt_conversions)
            conversions = result_conversions.scalar() or 0

            conversion_rate = (conversions / total_calls * 100) if total_calls > 0 else 0

            result.append({
                "id": telecaller.id,
                "name": telecaller.name,
                "mobile_number": telecaller.mobile_number,
                "status": telecaller.status,
                "assigned_gyms": assigned_gyms,
                "calls_today": today_calls_count,
                "conversion_rate": round(conversion_rate, 2),
                "last_active": telecaller.last_login_at,
                "language_known": telecaller.language_known or [],
            })

        return {
            "telecallers": result,
            "total": total,
            "page": page,
            "limit": limit,
            "total_pages": (total + limit - 1) // limit
        }

    except Exception as e:
        logger.error(f"Error fetching telecallers list for manager_id {current_manager.id}: "
                    f"{str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch telecallers list: {str(e)}"
        )


# Pydantic models for request/response
class UpdateTelecallerLanguagesRequest(BaseModel):
    telecaller_id: int
    languages: List[str]


@router.put("/telecallers/update-languages")
async def update_telecaller_languages(
    request_data: UpdateTelecallerLanguagesRequest,
    db: AsyncSession = Depends(get_async_db),
    current_manager: Manager = Depends(get_current_manager)
) -> Dict[str, Any]:
    """
    Update languages known by a telecaller - async database operations
    """
    try:

        # Verify that the telecaller belongs to this manager
        stmt = select(Telecaller).where(
            and_(
                Telecaller.id == request_data.telecaller_id,
                Telecaller.manager_id == current_manager.id
            )
        )
        result = await db.execute(stmt)
        telecaller = result.scalar_one_or_none()

        if not telecaller:
            raise HTTPException(
                status_code=404,
                detail="Telecaller not found or does not belong to this manager"
            )

        # Update languages
        telecaller.language_known = request_data.languages
        await db.commit()


        return {
            "message": "Languages updated successfully",
            "telecaller_id": telecaller.id,
            "languages": telecaller.language_known
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error updating languages for telecaller_id {request_data.telecaller_id}: "
                    f"{str(e)}", exc_info=True)
        await db.rollback()
        raise HTTPException(
            status_code=500,
            detail=f"Failed to update languages: {str(e)}"
        )
