from fastapi import APIRouter, Depends, HTTPException, status, Query
from sqlalchemy.orm import Session
from sqlalchemy import func, and_, or_, desc
from sqlalchemy.orm import aliased
from app.models.database import get_db
from app.models.telecaller_models import Telecaller, GymCallLogs, GymDatabase, ConvertedStatus
from app.telecaller.dependencies import get_current_telecaller
from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime, timedelta, date
import pytz

router = APIRouter()

class CallLogResponse(BaseModel):
    id: int
    gym_id: int
    gym_name: str
    call_status: str
    remarks: Optional[str]
    follow_up_date: Optional[str]
    created_at: str

class FollowUpInfo(BaseModel):
    id: int
    gym_id: int
    gym_name: str
    owner_name: Optional[str]
    contact_number: Optional[str]
    remarks: Optional[str]
    follow_up_date: str
    days_overdue: int

@router.get("/recent-calls")
async def get_recent_calls(
    telecaller: Telecaller = Depends(get_current_telecaller),
    db: Session = Depends(get_db),
    skip: int = 0,
    limit: int = 50
):
    """Get recent call logs for the telecaller"""
    # Define IST timezone
    ist_tz = pytz.timezone('Asia/Kolkata')
    call_logs = db.query(
        GymCallLogs,
        GymDatabase
    ).join(
        GymDatabase,
        GymCallLogs.gym_id == GymDatabase.id
    ).filter(
        GymCallLogs.telecaller_id == telecaller.id
    ).order_by(
        desc(GymCallLogs.created_at)
    ).offset(skip).limit(limit).all()

    calls = []
    for log, gym in call_logs:
        # Datetimes are now stored in IST, handle both naive and timezone-aware
        # If naive, assume it's IST
        if log.created_at:
            if log.created_at.tzinfo is None:
                created_at_ist = ist_tz.localize(log.created_at)
            else:
                created_at_ist = log.created_at.astimezone(ist_tz)
        else:
            created_at_ist = None

        if log.follow_up_date:
            if log.follow_up_date.tzinfo is None:
                follow_up_date_ist = ist_tz.localize(log.follow_up_date)
            else:
                follow_up_date_ist = log.follow_up_date.astimezone(ist_tz)
        else:
            follow_up_date_ist = None

        calls.append(CallLogResponse(
            id=log.id,
            gym_id=log.gym_id,
            gym_name=gym.gym_name,
            call_status=log.call_status,
            remarks=log.remarks,
            follow_up_date=follow_up_date_ist.isoformat() if follow_up_date_ist else None,
            created_at=created_at_ist.isoformat() if created_at_ist else None
        ))

    # Get total count
    total_count = db.query(func.count(GymCallLogs.id)).filter(
        GymCallLogs.telecaller_id == telecaller.id
    ).scalar()

    return {
        "recent_calls": calls,
        "total_count": total_count,
        "page_info": {
            "skip": skip,
            "limit": limit
        }
    }

@router.get("/follow-ups")
async def get_follow_ups(
    telecaller: Telecaller = Depends(get_current_telecaller),
    db: Session = Depends(get_db),
    status: Optional[str] = None,  # 'pending', 'overdue', 'all'
    skip: int = 0,
    limit: int = 50
):
    """Get follow-ups with optional status filter"""
    # Define IST timezone
    ist_tz = pytz.timezone('Asia/Kolkata')
    today = datetime.now().date()

    # Build base query
    query = db.query(
        GymCallLogs,
        GymDatabase
    ).join(
        GymDatabase,
        GymCallLogs.gym_id == GymDatabase.id
    ).filter(
        GymCallLogs.telecaller_id == telecaller.id,
        GymCallLogs.call_status.in_(["follow_up_required", "follow_up"]),
        GymCallLogs.follow_up_date.isnot(None)
    )

    # Apply status filter
    if status == "pending":
        query = query.filter(GymCallLogs.follow_up_date >= today)
    elif status == "overdue":
        query = query.filter(GymCallLogs.follow_up_date < today)
    # 'all' doesn't need any additional filter

    # Order by follow_up_date
    query = query.order_by(GymCallLogs.follow_up_date)

    # Get total count
    total_count = query.count()

    # Get paginated results
    follow_ups = query.offset(skip).limit(limit).all()

    follow_up_list = []
    for log, gym in follow_ups:
        days_overdue = (today - log.follow_up_date.date()).days if log.follow_up_date.date() < today else 0

        # Datetimes are now stored in IST, handle both naive and timezone-aware
        # If naive, assume it's IST
        if log.follow_up_date:
            if log.follow_up_date.tzinfo is None:
                follow_up_date_ist = ist_tz.localize(log.follow_up_date)
            else:
                follow_up_date_ist = log.follow_up_date.astimezone(ist_tz)
        else:
            follow_up_date_ist = None

        follow_up_list.append(FollowUpInfo(
            id=log.id,
            gym_id=log.gym_id,
            gym_name=gym.gym_name,
            owner_name=getattr(gym, 'contact_person', None),
            contact_number=getattr(gym, 'contact_phone', None),
            remarks=log.remarks,
            follow_up_date=follow_up_date_ist.isoformat() if follow_up_date_ist else None,
            days_overdue=days_overdue
        ))

    return {
        "follow_ups": follow_up_list,
        "total_count": total_count,
        "status_filter": status or "all",
        "page_info": {
            "skip": skip,
            "limit": limit
        }
    }

@router.get("/follow-ups/today")
async def get_todays_follow_ups(
    telecaller: Telecaller = Depends(get_current_telecaller),
    db: Session = Depends(get_db)
):
    """Get all follow-ups scheduled for today"""
    # Define IST timezone
    ist_tz = pytz.timezone('Asia/Kolkata')
    now_ist = datetime.now(ist_tz)
    today = now_ist.date()
    start_of_day = ist_tz.localize(datetime.combine(today, datetime.min.time()))
    end_of_day = ist_tz.localize(datetime.combine(today, datetime.max.time()))

    # Convert to UTC for comparison since DB stores UTC
    start_of_day_utc = start_of_day.astimezone(pytz.UTC).replace(tzinfo=None)
    end_of_day_utc = end_of_day.astimezone(pytz.UTC).replace(tzinfo=None)

    # Get only the latest follow-up for each gym today
    subquery = (
        db.query(
            GymCallLogs.gym_id,
            func.max(GymCallLogs.created_at).label('max_created')
        )
        .filter(
            and_(
                GymCallLogs.telecaller_id == telecaller.id,
                GymCallLogs.call_status.in_(['follow_up', 'follow_up_required']),
                GymCallLogs.follow_up_date >= start_of_day_utc,
                GymCallLogs.follow_up_date <= end_of_day_utc
            )
        )
        .group_by(GymCallLogs.gym_id)
        .subquery()
    )

    # Get follow-ups with gym details (only latest per gym)
    follow_ups = (
        db.query(GymCallLogs, GymDatabase)
        .join(GymDatabase, GymCallLogs.gym_id == GymDatabase.id)
        .join(
            subquery,
            and_(
                GymCallLogs.gym_id == subquery.c.gym_id,
                GymCallLogs.created_at == subquery.c.max_created
            )
        )
        .filter(
            and_(
                GymCallLogs.telecaller_id == telecaller.id,
                GymCallLogs.call_status.in_(['follow_up', 'follow_up_required']),
                GymCallLogs.follow_up_date >= start_of_day_utc,
                GymCallLogs.follow_up_date <= end_of_day_utc
            )
        )
        .order_by(GymCallLogs.follow_up_date)
        .all()
    )

    today_follow_ups = []
    for log, gym in follow_ups:
        # Datetimes are now stored in IST, handle both naive and timezone-aware
        # If naive, assume it's IST
        if log.created_at:
            if log.created_at.tzinfo is None:
                created_at_ist = ist_tz.localize(log.created_at)
            else:
                created_at_ist = log.created_at.astimezone(ist_tz)
        else:
            created_at_ist = None

        if log.follow_up_date:
            if log.follow_up_date.tzinfo is None:
                follow_up_date_ist = ist_tz.localize(log.follow_up_date)
            else:
                follow_up_date_ist = log.follow_up_date.astimezone(ist_tz)
        else:
            follow_up_date_ist = None

        today_follow_ups.append({
            "id": log.id,
            "gym_id": log.gym_id,
            "gym_name": gym.gym_name,
            "owner_name": getattr(gym, 'contact_person', None),
            "contact_number": getattr(gym, 'contact_phone', None),
            "remarks": log.remarks,
            "follow_up_date": follow_up_date_ist.isoformat() if follow_up_date_ist else None,
            "created_at": created_at_ist.isoformat() if created_at_ist else None
        })

    return {
        "today_follow_ups": today_follow_ups,
        "total_count": len(today_follow_ups),
        "date": today.isoformat()
    }

@router.get("/calls-by-status")
async def get_calls_by_status(
    telecaller: Telecaller = Depends(get_current_telecaller),
    db: Session = Depends(get_db),
    call_status: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    search: Optional[str] = None,
    skip: int = 0,
    limit: int = 50
):
    """Get call logs filtered by status and date range"""
    # Define IST timezone
    ist_tz = pytz.timezone('Asia/Kolkata')
    # Build base query
    query = db.query(
        GymCallLogs,
        GymDatabase
    ).join(
        GymDatabase,
        GymCallLogs.gym_id == GymDatabase.id
    ).filter(
        GymCallLogs.telecaller_id == telecaller.id
    )

    # Apply filters
    if call_status:
        # Map frontend status to database statuses
        if call_status == "converted":
            query = query.filter(GymCallLogs.call_status == "converted")
        elif call_status == "rejected":
            query = query.filter(GymCallLogs.call_status == "rejected")
        elif call_status == "no_response":
            query = query.filter(GymCallLogs.call_status == "no_response")
        elif call_status== "out_of_service":
            query = query.filter(GymCallLogs.call_status == "out_of_service")
        # Note: For "all", no filter is applied

    if search:
        search_term = f"%{search}%"
        query = query.filter(GymDatabase.gym_name.like(search_term))

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

    # Order by created_at desc
    query = query.order_by(desc(GymCallLogs.created_at))

    # Get total count
    total_count = query.count()

    # Get paginated results
    call_logs = query.offset(skip).limit(limit).all()

    calls = []
    for log, gym in call_logs:
        # Datetimes are now stored in IST, handle both naive and timezone-aware
        # If naive, assume it's IST
        if log.created_at:
            if log.created_at.tzinfo is None:
                created_at_ist = ist_tz.localize(log.created_at)
            else:
                created_at_ist = log.created_at.astimezone(ist_tz)
        else:
            created_at_ist = None

        if log.follow_up_date:
            if log.follow_up_date.tzinfo is None:
                follow_up_date_ist = ist_tz.localize(log.follow_up_date)
            else:
                follow_up_date_ist = log.follow_up_date.astimezone(ist_tz)
        else:
            follow_up_date_ist = None

        calls.append(CallLogResponse(
            id=log.id,
            gym_id=log.gym_id,
            gym_name=gym.gym_name,
            call_status=log.call_status,
            remarks=log.remarks,
            follow_up_date=follow_up_date_ist.isoformat() if follow_up_date_ist else None,
            created_at=created_at_ist.isoformat() if created_at_ist else None
        ))

    return {
        "calls": calls,
        "total_count": total_count,
        "filters": {
            "call_status": call_status,
            "date_from": date_from,
            "date_to": date_to
        },
        "page_info": {
            "skip": skip,
            "limit": limit
        }
    }

@router.get("/call-history/{gym_id}")
async def get_gym_call_history(
    gym_id: int,
    telecaller: Telecaller = Depends(get_current_telecaller),
    db: Session = Depends(get_db)
):
    """Get complete call history for a specific gym"""
    # Define IST timezone
    ist_tz = pytz.timezone('Asia/Kolkata')
    # Verify gym is assigned to telecaller OR delegated to telecaller
    from app.models.telecaller_models import GymAssignment
    assignment = db.query(GymAssignment).filter(
        GymAssignment.gym_id == gym_id,
        GymAssignment.telecaller_id == telecaller.id,
        GymAssignment.status == "active"
    ).first()

    # Also check if this gym was delegated to this telecaller
    delegated_to_me = db.query(GymCallLogs).filter(
        GymCallLogs.gym_id == gym_id,
        GymCallLogs.assigned_telecaller_id == telecaller.id,
        GymCallLogs.telecaller_id != telecaller.id
    ).first()

    if not assignment and not delegated_to_me:
        raise HTTPException(
            status_code=404,
            detail="Gym not found or not assigned to you"
        )

    # Get ALL call logs for this gym (complete history)
    # Once verified that the telecaller has access (assigned or delegated),
    # show the complete call history for the gym regardless of who created it
    call_logs = db.query(GymCallLogs).filter(
        GymCallLogs.gym_id == gym_id
    ).order_by(desc(GymCallLogs.created_at)).all()

    history = []
    for log in call_logs:
        # Datetimes are now stored in IST, handle both naive and timezone-aware
        # If naive, assume it's IST
        if log.created_at:
            if log.created_at.tzinfo is None:
                created_at_ist = ist_tz.localize(log.created_at)
            else:
                created_at_ist = log.created_at.astimezone(ist_tz)
        else:
            created_at_ist = None

        if log.follow_up_date:
            if log.follow_up_date.tzinfo is None:
                follow_up_date_ist = ist_tz.localize(log.follow_up_date)
            else:
                follow_up_date_ist = log.follow_up_date.astimezone(ist_tz)
        else:
            follow_up_date_ist = None

        # Get assigned telecaller info if exists
        assigned_telecaller_info = None
        if log.assigned_telecaller_id:
            assigned_to = db.query(Telecaller).filter(Telecaller.id == log.assigned_telecaller_id).first()
            if assigned_to:
                assigned_telecaller_info = {
                    "id": assigned_to.id,
                    "name": assigned_to.name
                }

        # Get creator telecaller info
        creator_telecaller_info = None
        if log.telecaller_id and log.telecaller_id != telecaller.id:
            creator = db.query(Telecaller).filter(Telecaller.id == log.telecaller_id).first()
            if creator:
                creator_telecaller_info = {
                    "id": creator.id,
                    "name": creator.name
                }

        history.append({
            "id": log.id,
            "call_status": log.call_status,
            "remarks": log.remarks,
            "follow_up_date": follow_up_date_ist.isoformat() if follow_up_date_ist else None,
            "created_at": created_at_ist.isoformat() if created_at_ist else None,
            "assigned_telecaller": assigned_telecaller_info,
            "creator_telecaller": creator_telecaller_info
        })

    return {
        "gym_id": gym_id,
        "call_history": history,
        "total_calls": len(history)
    }

@router.post("/follow-ups/{follow_up_id}/complete")
async def complete_follow_up(
    follow_up_id: int,
    telecaller: Telecaller = Depends(get_current_telecaller),
    db: Session = Depends(get_db)
):
    """Mark a follow-up as completed"""
    # Verify the follow-up belongs to the telecaller
    follow_up = db.query(GymCallLogs).filter(
        GymCallLogs.id == follow_up_id,
        GymCallLogs.telecaller_id == telecaller.id,
        GymCallLogs.call_status.in_(["follow_up_required", "follow_up"])
    ).first()

    if not follow_up:
        raise HTTPException(
            status_code=404,
            detail="Follow-up not found or not assigned to you"
        )

    # Update the follow-up status to contacted
    follow_up.call_status = "contacted"
    follow_up.follow_up_date = None
    db.commit()

    return {"message": "Follow-up marked as completed"}

@router.put("/follow-ups/{follow_up_id}/reschedule")
async def reschedule_follow_up(
    follow_up_id: int,
    new_date: str = Query(..., description="New follow-up date in ISO format"),
    telecaller: Telecaller = Depends(get_current_telecaller),
    db: Session = Depends(get_db)
):
    """Reschedule a follow-up to a new date"""
    # Verify the follow-up belongs to the telecaller
    follow_up = db.query(GymCallLogs).filter(
        GymCallLogs.id == follow_up_id,
        GymCallLogs.telecaller_id == telecaller.id,
        GymCallLogs.call_status.in_(["follow_up_required", "follow_up"])
    ).first()

    if not follow_up:
        raise HTTPException(
            status_code=404,
            detail="Follow-up not found or not assigned to you"
        )

    # Parse the date string
    from datetime import datetime
    try:
        parsed_date = datetime.fromisoformat(new_date.replace('Z', '+00:00'))
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail="Invalid date format. Use ISO format (YYYY-MM-DDTHH:MM:SS)"
        )

    # Update the follow-up date
    follow_up.follow_up_date = parsed_date
    db.commit()

    return {
        "message": "Follow-up rescheduled successfully",
        "new_date": parsed_date
    }


class LatestStatusLogResponse(BaseModel):
    id: int
    gym_id: int
    gym_name: str
    owner_name: Optional[str]
    contact_number: Optional[str]
    phone_number_source: Optional[str]  # 'call_log' or 'gym_database'
    address: Optional[str]
    area: Optional[str]
    city: Optional[str]
    call_status: str
    remarks: Optional[str]
    follow_up_date: Optional[str]
    created_at: str
    assigned_telecaller_id: Optional[int]
    assigned_telecaller_name: Optional[str] = None  # Name of the assigned telecaller
    # Converted status fields (for converted tab)
    converted_status: Optional[dict] = None  # Contains all verification checklist items


@router.get("/latest-by-status")
async def get_latest_logs_by_status(
    telecaller: Telecaller = Depends(get_current_telecaller),
    db: Session = Depends(get_db),
    call_status: str = Query(..., description="Status: delegated, converted, rejected, no_response, out_of_service"),
    search: Optional[str] = None,
    skip: int = 0,
    limit: int = 50,
    # Date filter parameters (based on created_at when status was set)
    converted_filter: Optional[str] = Query(None, description="Filter by converted date: today, this_week, this_month, custom"),
    converted_start_date: Optional[date] = Query(None, description="Converted start date for custom filter"),
    converted_end_date: Optional[date] = Query(None, description="Converted end date for custom filter"),
    rejected_filter: Optional[str] = Query(None, description="Filter by rejected date: today, this_week, this_month, custom"),
    rejected_start_date: Optional[date] = Query(None, description="Rejected start date for custom filter"),
    rejected_end_date: Optional[date] = Query(None, description="Rejected end date for custom filter"),
    no_response_filter: Optional[str] = Query(None, description="Filter by no_response date: today, this_week, this_month, custom"),
    no_response_start_date: Optional[date] = Query(None, description="No response start date for custom filter"),
    no_response_end_date: Optional[date] = Query(None, description="No response end date for custom filter"),
    out_of_service_filter: Optional[str] = Query(None, description="Filter by out_of_service date: today, this_week, this_month, custom"),
    out_of_service_start_date: Optional[date] = Query(None, description="Out of service start date for custom filter"),
    out_of_service_end_date: Optional[date] = Query(None, description="Out of service end date for custom filter"),
):
    """
    Get latest call logs by status for delegated, converted, rejected, no_response, out_of_service tabs.

    IMPORTANT: For each gym_id, only the LATEST entry (by created_at) is considered, regardless of who created it.
    - If another telecaller updates the gym after you delegated it, your 'delegated' entry is no longer the latest.
    - The gym will only appear if your entry is STILL the latest entry.

    For 'delegated': shows gyms where you created the latest entry with status='delegated'
    For other statuses: shows gyms where you created the latest entry with that status
    """

    # Validate status
    valid_statuses = ["delegated", "converted", "rejected", "no_response", "out_of_service"]
    if call_status not in valid_statuses:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid status. Must be one of: {', '.join(valid_statuses)}"
        )

    # Define IST timezone
    ist_tz = pytz.timezone('Asia/Kolkata')

    # Map frontend status to database call_status
    status_mapping = {
        "delegated": "delegated",
        "converted": "converted",
        "rejected": "rejected",
        "no_response": "no_response",
        "out_of_service": "out_of_service"
    }

    db_status = status_mapping[call_status]

    # For delegated status: check telecaller_id (the delegator/sender)
    if call_status == "delegated":
        # For delegated: records created by current telecaller (delegated to others)
        telecaller_filter = GymCallLogs.telecaller_id == telecaller.id
    else:
        # For other statuses: records created by current telecaller
        telecaller_filter = GymCallLogs.telecaller_id == telecaller.id

    # Subquery: Get the latest entry for each gym_id (regardless of who created it or status)
    # This ensures we get the TRUE latest entry for each gym
    latest_logs_subquery = (
        db.query(
            GymCallLogs.gym_id,
            func.max(GymCallLogs.created_at).label('max_created')
        )
        .group_by(GymCallLogs.gym_id)
        .subquery()
    )

    # Build main query - filter by telecaller and call_status
    # This ensures we only get gyms where:
    # 1. The latest entry was created by the current telecaller
    # 2. The latest entry has the specified status
    # Alias Telecaller for assigned_telecaller join
    AssignedTelecaller = aliased(Telecaller)

    query = (
        db.query(GymCallLogs, GymDatabase, ConvertedStatus, AssignedTelecaller)
        .join(GymDatabase, GymCallLogs.gym_id == GymDatabase.id)
        .outerjoin(ConvertedStatus, and_(
            ConvertedStatus.gym_id == GymCallLogs.gym_id,
            ConvertedStatus.telecaller_id == telecaller.id
        ))
        .outerjoin(AssignedTelecaller, GymCallLogs.assigned_telecaller_id == AssignedTelecaller.id)
        .join(
            latest_logs_subquery,
            and_(
                GymCallLogs.gym_id == latest_logs_subquery.c.gym_id,
                GymCallLogs.created_at == latest_logs_subquery.c.max_created
            )
        )
        .filter(
            and_(
                telecaller_filter,
                GymCallLogs.call_status == db_status
            )
        )
    )

    # Apply search filter if provided
    if search:
        search_term = f"%{search}%"
        query = query.filter(GymDatabase.gym_name.like(search_term))

    # Apply date filter based on created_at (when status was set)
    today = date.today()
    date_filter = None
    date_start = None
    date_end = None

    # Determine which date filter to use based on call_status
    if call_status == "converted":
        date_filter = converted_filter
        date_start = converted_start_date
        date_end = converted_end_date
    elif call_status == "rejected":
        date_filter = rejected_filter
        date_start = rejected_start_date
        date_end = rejected_end_date
    elif call_status == "no_response":
        date_filter = no_response_filter
        date_start = no_response_start_date
        date_end = no_response_end_date
    elif call_status == "out_of_service":
        date_filter = out_of_service_filter
        date_start = out_of_service_start_date
        date_end = out_of_service_end_date

    # Apply the date filter
    if date_filter and date_filter != "all":
        if date_filter == "today":
            query = query.where(func.date(GymCallLogs.created_at) == today)
        elif date_filter == "this_week":
            week_start = today - timedelta(days=today.weekday())
            week_end = week_start + timedelta(days=6)
            query = query.where(func.date(GymCallLogs.created_at).between(week_start, week_end))
        elif date_filter == "this_month":
            query = query.where(
                and_(
                    func.extract('year', GymCallLogs.created_at) == today.year,
                    func.extract('month', GymCallLogs.created_at) == today.month
                )
            )
        elif date_filter == "custom" and date_start and date_end:
            query = query.where(func.date(GymCallLogs.created_at).between(date_start, date_end))

    # Order by created_at desc
    query = query.order_by(desc(GymCallLogs.created_at))

    # Get total count before pagination
    total_count = query.count()

    # Get paginated results
    logs = query.offset(skip).limit(limit).all()

    result = []
    for log, gym, converted_status, assigned_telecaller in logs:
        # Handle created_at timezone
        if log.created_at:
            if log.created_at.tzinfo is None:
                created_at_ist = ist_tz.localize(log.created_at)
            else:
                created_at_ist = log.created_at.astimezone(ist_tz)
        else:
            created_at_ist = None

        # Handle follow_up_date timezone
        if log.follow_up_date:
            if log.follow_up_date.tzinfo is None:
                follow_up_date_ist = ist_tz.localize(log.follow_up_date)
            else:
                follow_up_date_ist = log.follow_up_date.astimezone(ist_tz)
        else:
            follow_up_date_ist = None

        # Phone number resolution logic:
        # Priority 1: new_contact_number from the latest call log (if not null/empty)
        # Priority 2: contact_phone from gym_database
        contact_number = None
        phone_number_source = None

        # Try to get new_contact_number from the call log
        new_contact = getattr(log, 'new_contact_number', None)
        # print(f"DEBUG: gym_id={log.gym_id}, new_contact_number={repr(new_contact)}")  # Debug log

        if new_contact and str(new_contact).strip():
            contact_number = str(new_contact).strip()
            phone_number_source = "call_log"
            # print(f"DEBUG: Using call_log number for gym {log.gym_id}: {contact_number}")
        else:
            contact_number = getattr(gym, 'contact_phone', None)
            phone_number_source = "gym_database"
            # print(f"DEBUG: Using gym_database number for gym {log.gym_id}: {contact_number}")

        # Build converted_status dict if available
        converted_status_data = None
        if converted_status:
            converted_status_data = {
                "document_uploaded": converted_status.document_uploaded,
                "membership_plan_created": converted_status.membership_plan_created,
                "session_created": converted_status.session_created,
                "daily_pass_created": converted_status.daily_pass_created,
                "gym_studio_images_uploaded": converted_status.gym_studio_images_uploaded,
                "agreement_signed": converted_status.agreement_signed,
                "biometric_required": converted_status.biometric_required,
                "registered_place": converted_status.registered_place,
            }

        result.append(LatestStatusLogResponse(
            id=log.id,
            gym_id=log.gym_id,
            gym_name=gym.gym_name,
            owner_name=getattr(gym, 'contact_person', None),
            contact_number=contact_number,
            phone_number_source=phone_number_source,
            address=getattr(gym, 'address', None),
            area=getattr(gym, 'area', None),
            city=getattr(gym, 'city', None),
            call_status=log.call_status,
            remarks=log.remarks,
            follow_up_date=follow_up_date_ist.isoformat() if follow_up_date_ist else None,
            created_at=created_at_ist.isoformat() if created_at_ist else None,
            assigned_telecaller_id=log.assigned_telecaller_id,
            assigned_telecaller_name=assigned_telecaller.name if assigned_telecaller else None,
            converted_status=converted_status_data
        ))

    return {
        "data": result,
        "total_count": total_count,
        "filters": {
            "call_status": call_status,
            "search": search
        },
        "page_info": {
            "skip": skip,
            "limit": limit
        }
    }