from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from sqlalchemy import func, and_, case
from app.models.database import get_db
from app.models.telecaller_models import (
    Manager, Telecaller, GymAssignment, GymAssignmentHistory, GymCallLogs,GymDatabase
)
from app.telecaller.dependencies import get_current_manager
from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime, timedelta

router = APIRouter()

class TelecallerStats(BaseModel):
    telecaller_id: int
    name: str
    mobile_number: str
    total_assigned: int
    calls_made: int
    converted: int
    follow_ups: int
    rejected: int
    no_response: int
    pending: int
    conversion_rate: float

class ManagerOverview(BaseModel):
    total_telecallers: int
    total_assigned_gyms: int
    total_calls_made: int
    total_converted: int
    total_follow_ups: int
    total_rejected: int
    total_no_response: int
    overall_conversion_rate: float

class CallLogDetails(BaseModel):
    id: int
    gym_id: int
    gym_name: str
    call_status: str
    remarks: Optional[str]
    follow_up_date: Optional[datetime]
    created_at: datetime

@router.get("/overview")
async def get_manager_overview(
    manager: Manager = Depends(get_current_manager),
    db: Session = Depends(get_db)
):
    """Get overall performance overview for the manager"""
    # Get total telecallers
    total_telecallers = db.query(func.count(Telecaller.id)).filter(
        Telecaller.manager_id == manager.id
    ).scalar() or 0

    # Get total assigned gyms
    total_assigned = db.query(func.count(GymAssignment.gym_id)).filter(
        GymAssignment.manager_id == manager.id,
        GymAssignment.status == "active"
    ).scalar() or 0

    # Get call statistics
    call_stats = db.query(
        func.count(GymCallLogs.id).label("total_calls"),
        func.sum(case([(GymCallLogs.call_status == "converted", 1)], else_=0)).label("converted"),
        func.sum(case([(GymCallLogs.call_status == "follow_up", 1)], else_=0)).label("follow_ups"),
        func.sum(case([(GymCallLogs.call_status == "not_interested", 1)], else_=0)).label("rejected"),
        func.sum(case([(GymCallLogs.call_status == "no_response", 1)], else_=0)).label("no_response"),
        func.sum(case([(GymCallLogs.call_status == "pending", 1)], else_=0)).label("pending")
    ).filter(
        GymCallLogs.manager_id == manager.id
    ).first()

    total_calls = call_stats.total_calls or 0
    total_converted = call_stats.converted or 0
    total_follow_ups = call_stats.follow_ups or 0
    total_rejected = call_stats.rejected or 0
    total_no_response = call_stats.no_response or 0

    # Calculate conversion rate
    conversion_rate = (total_converted / total_calls * 100) if total_calls > 0 else 0

    return ManagerOverview(
        total_telecallers=total_telecallers,
        total_assigned_gyms=total_assigned,
        total_calls_made=total_calls,
        total_converted=total_converted,
        total_follow_ups=total_follow_ups,
        total_rejected=total_rejected,
        total_no_response=total_no_response,
        overall_conversion_rate=round(conversion_rate, 2)
    )

@router.get("/telecaller-stats")
async def get_telecaller_performance_stats(
    manager: Manager = Depends(get_current_manager),
    db: Session = Depends(get_db),
    skip: int = 0,
    limit: int = 100,
    date_from: Optional[str] = None
):
    """Get performance statistics for all telecallers under the manager"""

    # print(f"🔍 Getting telecaller stats for manager: {manager.id}")
    # print(f"🔍 Date filter: {date_from}")

    # Parse date filter
    filter_date = None
    if date_from:
        try:
            # Add timezone handling - make date_from start of day
            filter_date = datetime.strptime(date_from, "%Y-%m-%d")
            # print(f"🔍 Parsed filter_date: {filter_date}")
        except ValueError as e:
            # print(f"🔍 Invalid date format: {e}")
            pass  # Invalid date format, ignore filter

    # Get all telecallers for this manager
    telecallers = db.query(Telecaller).filter(
        Telecaller.manager_id == manager.id
    ).all()

    # print(f"🔍 Found {len(telecallers)} telecallers for manager {manager.id}")

    telecaller_stats = []
    for telecaller in telecallers:
        # Count calls directly
        total_calls = db.query(GymCallLogs).filter(
            GymCallLogs.telecaller_id == telecaller.id
        )

        # Apply date filter if provided
        if filter_date:
            total_calls = total_calls.filter(GymCallLogs.created_at >= filter_date)

        # Count conversions (reuse the same query with additional filter)
        conversions = total_calls.filter(
            GymCallLogs.call_status == "converted"
        )

        # Get counts
        total_calls_count = total_calls.count()
        conversions_count = conversions.count()

        # print(f"🔍 Telecaller {telecaller.name}: calls={total_calls_count}, conversions={conversions_count}")

        # Get assigned gyms count
        assigned_gyms = db.query(GymAssignment).filter(
            GymAssignment.telecaller_id == telecaller.id,
            GymAssignment.status == "active"
        ).count()

        telecaller_stats.append(TelecallerStats(
            telecaller_id=telecaller.id,
            name=telecaller.name,
            mobile_number=telecaller.mobile_number,
            total_assigned=assigned_gyms,
            calls_made=total_calls_count,
            converted=conversions_count,
            follow_ups=0,
            rejected=0,
            no_response=0,
            pending=0,
            conversion_rate=round((conversions_count / total_calls_count * 100) if total_calls_count > 0 else 0, 2)
        ))

    return {
        "telecaller_stats": telecaller_stats,
        "total_count": len(telecaller_stats)
    }

@router.get("/call-history")
async def get_call_history(
    manager: Manager = Depends(get_current_manager),
    db: Session = Depends(get_db),
    telecaller_id: Optional[int] = None,
    status_filter: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    skip: int = 0,
    limit: int = 100
):
    """Get call logs with optional filters"""
    # Build base query
    query = db.query(GymCallLogs).join(
        Telecaller,
        GymCallLogs.telecaller_id == Telecaller.id
    ).join(
        GymDatabase,
        GymCallLogs.gym_id == GymDatabase.id
    ).filter(
        GymCallLogs.manager_id == manager.id
    )

    # Apply filters
    if telecaller_id:
        # Verify telecaller belongs to manager
        telecaller = db.query(Telecaller).filter(
            Telecaller.id == telecaller_id,
            Telecaller.manager_id == manager.id
        ).first()
        if not telecaller:
            raise HTTPException(
                status_code=404,
                detail="Telecaller not found or not under your management"
            )
        query = query.filter(GymCallLogs.telecaller_id == telecaller_id)

    if status_filter:
        query = query.filter(GymCallLogs.call_status == status_filter)

    if date_from:
        try:
            date_from_dt = datetime.strptime(date_from, "%Y-%m-%d")
            query = query.filter(GymCallLogs.created_at >= date_from_dt)
        except ValueError:
            raise HTTPException(
                status_code=400,
                detail="Invalid date_from format. Use YYYY-MM-DD"
            )

    if date_to:
        try:
            date_to_dt = datetime.strptime(date_to, "%Y-%m-%d") + timedelta(days=1)
            query = query.filter(GymCallLogs.created_at < date_to_dt)
        except ValueError:
            raise HTTPException(
                status_code=400,
                detail="Invalid date_to format. Use YYYY-MM-DD"
            )

    # Order by created_at desc and paginate
    call_logs = query.order_by(GymCallLogs.created_at.desc()).offset(skip).limit(limit).all()

    call_history = []
    for log in call_logs:
        gym = db.query(GymDatabase).filter(GymDatabase.id == log.gym_id).first()
        call_history.append(CallLogDetails(
            id=log.id,
            gym_id=log.gym_id,
            gym_name=gym.gym_name if gym else "Unknown",
            call_status=log.call_status,
            remarks=log.remarks,
            follow_up_date=log.follow_up_date,
            created_at=log.created_at
        ))

    # Get total count
    total_count = query.count()

    return {
        "call_history": call_history,
        "total_count": total_count,
        "filters": {
            "telecaller_id": telecaller_id,
            "status_filter": status_filter,
            "date_from": date_from,
            "date_to": date_to
        }
    }

@router.get("/follow-ups-today")
async def get_today_follow_ups(
    manager: Manager = Depends(get_current_manager),
    db: Session = Depends(get_db)
):
    """Get all follow-ups scheduled for today"""
    today = datetime.now().date()
    tomorrow = today + timedelta(days=1)

    follow_ups = db.query(
        GymCallLogs,
        Telecaller,
        GymDatabase
    ).join(
        Telecaller,
        GymCallLogs.telecaller_id == Telecaller.id
    ).join(
        GymDatabase,
        GymCallLogs.gym_id == GymDatabase.id
    ).filter(
        GymCallLogs.manager_id == manager.id,
        GymCallLogs.follow_up_date >= today,
        GymCallLogs.follow_up_date < tomorrow,
        GymCallLogs.call_status == "follow_up"
    ).all()

    follow_up_list = []
    for log, telecaller, gym in follow_ups:
        follow_up_list.append({
            "log_id": log.id,
            "telecaller": {
                "id": telecaller.id,
                "name": telecaller.name,
                "mobile_number": telecaller.mobile_number
            },
            "gym": {
                "id": gym.id,
                "gym_name": gym.gym_name,
                "contact_number": getattr(gym, 'contact_number', None)
            },
            "follow_up_date": log.follow_up_date,
            "remarks": log.remarks
        })

    return {
        "follow_ups": follow_up_list,
        "total_count": len(follow_up_list)
    }
