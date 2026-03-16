from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import func, and_, or_, select, desc, distinct
from app.models.async_database import get_async_db
from app.models.telecaller_models import (
    Telecaller, GymAssignment, GymAssignmentHistory, GymCallLogs,GymDatabase
)

from app.telecaller.dependencies import get_current_telecaller
from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime, timedelta

router = APIRouter()

class GymInfo(BaseModel):
    id: int
    gym_name: str
    owner_name: Optional[str]
    contact_number: Optional[str]
    assigned_at: datetime

class UpdateCallStatus(BaseModel):
    gym_id: int
    call_status: str
    remarks: Optional[str]
    follow_up_date: Optional[datetime]

@router.get("/assigned-gyms")
async def get_my_assigned_gyms(
    telecaller: Telecaller = Depends(get_current_telecaller),
    db: AsyncSession = Depends(get_async_db),
    skip: int = 0,
    limit: int = 100,
    search: Optional[str] = None
):
    """Get gyms assigned to the current telecaller"""
    # Build base query
    query = (
        select(GymAssignment, GymDatabase)
        .join(
            GymDatabase,
            GymAssignment.gym_id == GymDatabase.id
        )
        .where(
            GymAssignment.telecaller_id == telecaller.id,
            GymAssignment.status == "active"
        )
    )

    # Apply search filter if provided
    if search:
        search_term = f"%{search}%"
        # Build search conditions dynamically
        search_conditions = [GymDatabase.gym_name.ilike(search_term)]

        owner_name = getattr(GymDatabase, 'owner_name', None)
        if owner_name is not None:
            search_conditions.append(owner_name.ilike(search_term))

        contact_number = getattr(GymDatabase, 'contact_number', None)
        if contact_number is not None:
            search_conditions.append(contact_number.ilike(search_term))

        if search_conditions:
            query = query.where(or_(*search_conditions))

    # Get total count
    count_query = select(func.count()).select_from(query)
    count_result = await db.execute(count_query)
    total_count = count_result.scalar() or 0

    # Get paginated results
    query = query.order_by(desc(GymAssignment.assigned_at)).offset(skip).limit(limit)
    result = await db.execute(query)
    assignments = result.all()

    gyms = []
    for assignment, gym in assignments:
        gyms.append(GymInfo(
            id=gym.id,
            gym_name=gym.gym_name,
            owner_name=getattr(gym, 'owner_name', None),
            contact_number=getattr(gym, 'contact_number', None),
            assigned_at=assignment.assigned_at
        ))

    return {
        "assigned_gyms": gyms,
        "total_count": total_count,
        "page_info": {
            "skip": skip,
            "limit": limit
        }
    }

@router.get("/dashboard-stats")
async def get_dashboard_stats(
    telecaller: Telecaller = Depends(get_current_telecaller),
    db: AsyncSession = Depends(get_async_db)
):
    """Get dashboard statistics for the telecaller"""
    # Get total assigned gyms - use distinct because GymAssignment has composite PK
    total_assigned_query = select(func.count(distinct(GymAssignment.gym_id))).where(
        GymAssignment.telecaller_id == telecaller.id,
        GymAssignment.status == "active"
    )
    total_assigned_result = await db.execute(total_assigned_query)
    total_assigned = total_assigned_result.scalar() or 0

    # Get call statistics
    call_stats_query = select(
        func.count(GymCallLogs.id).label("total_calls"),
        func.sum(func.case([(GymCallLogs.call_status == "converted", 1)], else_=0)).label("converted"),
        func.sum(func.case([(GymCallLogs.call_status == "follow_up_required", 1)], else_=0)).label("follow_ups"),
        func.sum(func.case([(GymCallLogs.call_status == "not_interested", 1)], else_=0)).label("rejected"),
        func.sum(func.case([(GymCallLogs.call_status == "no_response", 1)], else_=0)).label("no_response"),
        func.sum(func.case([(GymCallLogs.call_status == "pending", 1)], else_=0)).label("pending")
    ).where(
        GymCallLogs.telecaller_id == telecaller.id
    )

    call_stats_result = await db.execute(call_stats_query)
    call_stats = call_stats_result.first()

    total_calls = call_stats.total_calls or 0
    total_converted = call_stats.converted or 0
    total_follow_ups = call_stats.follow_ups or 0
    total_rejected = call_stats.rejected or 0
    total_no_response = call_stats.no_response or 0

    # Calculate conversion rate
    conversion_rate = (total_converted / total_calls * 100) if total_calls > 0 else 0

    # Get follow-ups for today
    today = datetime.now().date()
    tomorrow = today + timedelta(days=1)

    today_follow_ups_query = select(func.count(GymCallLogs.id)).where(
        GymCallLogs.telecaller_id == telecaller.id,
        GymCallLogs.follow_up_date >= today,
        GymCallLogs.follow_up_date < tomorrow,
        GymCallLogs.call_status == "follow_up_required"
    )
    today_follow_ups_result = await db.execute(today_follow_ups_query)
    today_follow_ups = today_follow_ups_result.scalar() or 0

    return {
        "total_assigned_gyms": total_assigned,
        "calls_made": total_calls,
        "converted": total_converted,
        "follow_ups": total_follow_ups,
        "rejected": total_rejected,
        "no_response": total_no_response,
        "pending": call_stats.pending or 0,
        "conversion_rate": round(conversion_rate, 2),
        "follow_ups_today": today_follow_ups
    }

@router.get("/gym-details/{gym_id}")
async def get_gym_details(
    gym_id: int,
    telecaller: Telecaller = Depends(get_current_telecaller),
    db: AsyncSession = Depends(get_async_db)
):
    """Get detailed information about a specific gym"""
    # Verify gym is assigned to telecaller
    assignment_query = select(GymAssignment).where(
        and_(
            GymAssignment.gym_id == gym_id,
            GymAssignment.telecaller_id == telecaller.id,
            GymAssignment.status == "active"
        )
    )
    assignment_result = await db.execute(assignment_query)
    assignment = assignment_result.scalar_one_or_none()

    if not assignment:
        raise HTTPException(
            status_code=404,
            detail="Gym not found or not assigned to you"
        )

    # Get gym details
    gym_query = select(GymDatabase).where(GymDatabase.id == gym_id)
    gym_result = await db.execute(gym_query)
    gym = gym_result.scalar_one_or_none()

    if not gym:
        raise HTTPException(status_code=404, detail="Gym not found")

    # Get call history for this gym
    call_logs_query = select(GymCallLogs).where(
        and_(
            GymCallLogs.gym_id == gym_id,
            GymCallLogs.telecaller_id == telecaller.id
        )
    ).order_by(desc(GymCallLogs.created_at))

    call_logs_result = await db.execute(call_logs_query)
    call_logs = call_logs_result.scalars().all()

    gym_data = {
        "id": gym.id,
        "gym_name": gym.gym_name,
        "owner_name": getattr(gym, 'owner_name', None),
        "contact_number": getattr(gym, 'contact_number', None),
        "address": getattr(gym, 'address', None),
        "city": getattr(gym, 'city', None),
        "state": getattr(gym, 'state', None),
        "assigned_at": assignment.assigned_at,
        "call_history": [
            {
                "id": log.id,
                "call_status": log.call_status,
                "remarks": log.remarks,
                "follow_up_date": log.follow_up_date,
                "created_at": log.created_at
            }
            for log in call_logs
        ],
        "total_calls": len(call_logs)
    }

    # Add any other relevant fields from GymDatabase
    for column in gym.__table__.columns:
        if column.name not in gym_data:
            gym_data[column.name] = getattr(gym, column.name, None)

    return gym_data

@router.post("/update-call-status")
async def update_call_status(
    data: UpdateCallStatus,
    telecaller: Telecaller = Depends(get_current_telecaller),
    db: AsyncSession = Depends(get_async_db)
):
    """Update the call status for a gym"""
    # Verify gym is assigned to telecaller
    assignment_query = select(GymAssignment).where(
        and_(
            GymAssignment.gym_id == data.gym_id,
            GymAssignment.telecaller_id == telecaller.id,
            GymAssignment.status == "active"
        )
    )
    assignment_result = await db.execute(assignment_query)
    assignment = assignment_result.scalar_one_or_none()

    if not assignment:
        raise HTTPException(
            status_code=404,
            detail="Gym not found or not assigned to you"
        )

    # Validate call status
    valid_statuses = ["pending", "contacted", "interested", "not_interested", "follow_up_required", "closed"]
    if data.call_status not in valid_statuses:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid call status. Must be one of: {', '.join(valid_statuses)}"
        )

    # If follow_up_required, follow_up_date is mandatory
    if data.call_status == "follow_up_required" and not data.follow_up_date:
        raise HTTPException(
            status_code=400,
            detail="follow_up_date is required when call_status is follow_up_required"
        )

    # Create call log entry
    call_log = GymCallLogs(
        gym_id=data.gym_id,
        telecaller_id=telecaller.id,
        manager_id=telecaller.manager_id,
        call_status=data.call_status,
        remarks=data.remarks,
        follow_up_date=data.follow_up_date,
        created_at=datetime.utcnow()
    )
    db.add(call_log)
    await db.commit()

    return {"message": "Call status updated successfully", "log_id": call_log.id}
