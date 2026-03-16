"""
Optimized Async API endpoints for Manager Tracker.

This module provides fully async, optimized endpoints for the manager tracker page:
1. Pending gyms with cursor-based pagination
2. Today's gyms (subset of pending)
3. Follow-up gyms with cursor-based pagination
4. Other status gyms (Converted, Rejected, No Response, Out of Service)

All endpoints use AsyncSession and avoid any blocking operations.
Optimized to eliminate N+1 queries and use database-level filtering.
"""

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import and_, or_, func, not_, desc, select
from typing import List, Optional
from datetime import datetime, date, timedelta
import pytz

from app.models.async_database import get_async_db
from app.models.telecaller_models import (
    Manager, Telecaller, GymAssignment, GymCallLogs, GymDatabase, ConvertedStatus
)
from app.telecaller.dependencies import get_current_manager
from pydantic import BaseModel

router = APIRouter()

# ============================================================================
# Pydantic Models for Request/Response
# ============================================================================

class ManagerGymListItem(BaseModel):
    gym_id: int
    gym_name: str
    contact_person: Optional[str] = None
    contact_number: Optional[str] = None
    city: Optional[str] = None
    address: Optional[str] = None
    area: Optional[str] = None
    zone: Optional[str] = None
    isprime: Optional[bool] = None
    location: Optional[str] = None
    target_date: Optional[str] = None
    assigned_at: Optional[str] = None
    days_since_assigned: Optional[int] = None
    telecaller_id: Optional[int] = None
    telecaller_name: Optional[str] = None
    telecaller_mobile: Optional[str] = None
    type: Optional[str] = None


class FollowUpGymListItem(BaseModel):
    gym_id: int
    gym_name: str
    contact_person: Optional[str] = None
    contact_number: Optional[str] = None
    city: Optional[str] = None
    address: Optional[str] = None
    area: Optional[str] = None
    zone: Optional[str] = None
    isprime: Optional[bool] = None
    location: Optional[str] = None
    follow_up_date: Optional[str] = None
    last_call_date: Optional[str] = None
    interest_level: Optional[str] = None
    remarks: Optional[str] = None
    telecaller_id: Optional[int] = None
    telecaller_name: Optional[str] = None
    telecaller_mobile: Optional[str] = None
    delegated_by_name: Optional[str] = None
    type: Optional[str] = None


class OtherStatusGymListItem(BaseModel):
    gym_id: int
    gym_name: str
    contact_person: Optional[str] = None
    contact_number: Optional[str] = None
    city: Optional[str] = None
    address: Optional[str] = None
    area: Optional[str] = None
    zone: Optional[str] = None
    isprime: Optional[bool] = None
    location: Optional[str] = None
    call_date: Optional[str] = None
    call_status: Optional[str] = None
    interest_level: Optional[str] = None
    remarks: Optional[str] = None
    telecaller_id: Optional[int] = None
    telecaller_name: Optional[str] = None
    telecaller_mobile: Optional[str] = None
    type: Optional[str] = None
    # Converted status fields
    document_collected: Optional[bool] = None
    membership_collected: Optional[bool] = None
    session_collected: Optional[bool] = None
    daily_pass_collected: Optional[bool] = None
    studio_images_collected: Optional[bool] = None
    agreement_collected: Optional[bool] = None


class ManagerCursorPaginatedResponse(BaseModel):
    gyms: List[ManagerGymListItem]
    next_cursor: Optional[str] = None
    has_more: bool = False
    page_size: int = 50
    total_count: Optional[int] = None


class FollowUpCursorPaginatedResponse(BaseModel):
    gyms: List[FollowUpGymListItem]
    next_cursor: Optional[str] = None
    has_more: bool = False
    page_size: int = 50
    total_count: Optional[int] = None


class OtherStatusCursorPaginatedResponse(BaseModel):
    gyms: List[OtherStatusGymListItem]
    next_cursor: Optional[str] = None
    has_more: bool = False
    page_size: int = 50
    total_count: Optional[int] = None


# ============================================================================
# Pending Gyms Endpoint
# ============================================================================

@router.get("/pending-gyms", response_model=ManagerCursorPaginatedResponse)
async def get_manager_pending_gyms(
    cursor: Optional[str] = Query(None, description="Encoded cursor for pagination"),
    page_size: int = Query(50, ge=1, le=100, description="Number of items per page"),
    include_total_count: bool = Query(False, description="Include total count"),
    target_date_filter: Optional[str] = Query(None, description="Filter by target date: today, this_week, this_month, custom, overdue"),
    target_start_date: Optional[date] = Query(None, description="Target start date for custom filter"),
    target_end_date: Optional[date] = Query(None, description="Target end date for custom filter"),
    search_query: Optional[str] = Query(None, description="Search by gym name or contact person"),
    telecaller_id: Optional[int] = Query(None, description="Filter by telecaller ID"),
    type: Optional[str] = Query(None, description="Filter by gym type"),
    sort_by: Optional[str] = Query("target_date", description="Sort by: target_date, assigned_at, gym_name"),
    sort_order: Optional[str] = Query("asc", description="Sort order: asc, desc"),
    manager: Manager = Depends(get_current_manager),
    db: AsyncSession = Depends(get_async_db)
):
    """
    Get pending gyms for manager with CURSOR-BASED pagination.

    A gym qualifies for Pending tab if:
    1. It exists in gym_assignments with telecaller's manager_id = current manager AND status = 'active'
    2. AND gym_id does NOT exist in gym_call_logs for that telecaller (or only has pending/contacted/interested statuses)
    3. AND target_date matches filter (if provided)

    Uses SINGLE optimized query with cursor-based pagination.
    """
    try:
        import base64
        import json
        from urllib.parse import unquote, quote

        ist_tz = pytz.timezone('Asia/Kolkata')
        today = date.today()

        # Decode cursor
        last_target_date = None
        last_assigned_at = None
        last_gym_id = None
        last_gym_name = None

        if cursor:
            try:
                cursor_data = json.loads(base64.b64decode(unquote(cursor)))
                last_target_date_str = cursor_data.get("target_date")
                last_assigned_at_str = cursor_data.get("assigned_at")
                last_gym_id = cursor_data.get("gym_id")
                last_gym_name = cursor_data.get("gym_name")

                if last_target_date_str:
                    last_target_date = date.fromisoformat(last_target_date_str)
                if last_assigned_at_str:
                    last_assigned_at = datetime.fromisoformat(last_assigned_at_str)
            except Exception:
                pass

        # Determine sort column
        sort_column = GymAssignment.target_date if sort_by == "target_date" else GymAssignment.assigned_at if sort_by == "assigned_at" else GymDatabase.gym_name

        # Build the base query with all filters
        base_query = select(
            GymDatabase.id,
            GymDatabase.gym_name,
            GymDatabase.contact_person,
            GymDatabase.contact_phone,
            GymDatabase.city,
            GymDatabase.address,
            GymDatabase.area,
            GymDatabase.zone,
            GymDatabase.isprime,
            GymDatabase.location,
            GymDatabase.type,
            GymAssignment.target_date,
            GymAssignment.assigned_at,
            Telecaller.id.label('telecaller_id'),
            Telecaller.name.label('telecaller_name'),
            Telecaller.mobile_number.label('telecaller_mobile')
        ).select_from(
            GymAssignment
        ).join(
            GymDatabase,
            GymAssignment.gym_id == GymDatabase.id
        ).join(
            Telecaller,
            GymAssignment.telecaller_id == Telecaller.id
        ).where(
            and_(
                Telecaller.manager_id == manager.id,
                GymAssignment.status == "active",
                ~GymAssignment.gym_id.in_(
                    select(GymCallLogs.gym_id).where(
                        and_(
                            GymCallLogs.telecaller_id == GymAssignment.telecaller_id,
                            GymCallLogs.call_status.in_(['follow_up', 'follow_up_required', 'converted', 'rejected', 'no_response', 'out_of_service', 'delegated'])
                        )
                    )
                )
            )
        )

        # Apply telecaller filter
        if telecaller_id:
            base_query = base_query.where(GymAssignment.telecaller_id == telecaller_id)

        # Apply type filter
        if type:
            base_query = base_query.where(GymDatabase.type == type)

        # Apply target date filter at database level
        if target_date_filter and target_date_filter != "all":
            if target_date_filter == "today":
                base_query = base_query.where(GymAssignment.target_date == today)
            elif target_date_filter == "this_week":
                week_start = today - timedelta(days=today.weekday())
                week_end = week_start + timedelta(days=6)
                base_query = base_query.where(GymAssignment.target_date.between(week_start, week_end))
            elif target_date_filter == "this_month":
                base_query = base_query.where(
                    and_(
                        func.extract('year', GymAssignment.target_date) == today.year,
                        func.extract('month', GymAssignment.target_date) == today.month
                    )
                )
            elif target_date_filter == "overdue":
                base_query = base_query.where(GymAssignment.target_date < today)
            elif target_date_filter == "custom" and target_start_date and target_end_date:
                base_query = base_query.where(GymAssignment.target_date.between(target_start_date, target_end_date))

        # Apply search filter at database level
        if search_query:
            search_pattern = f"%{search_query}%"
            base_query = base_query.where(
                or_(
                    GymDatabase.gym_name.ilike(search_pattern),
                    GymDatabase.contact_person.ilike(search_pattern)
                )
            )

        # CURSOR-BASED PAGINATION
        if cursor and (last_target_date is not None or last_assigned_at is not None or last_gym_name is not None):
            if sort_by == "target_date":
                if last_target_date is not None:
                    if sort_order == "desc":
                        base_query = base_query.where(
                            or_(
                                GymAssignment.target_date < last_target_date,
                                and_(
                                    GymAssignment.target_date == last_target_date,
                                    GymDatabase.id < last_gym_id if last_gym_id else True
                                )
                            )
                        )
                    else:
                        base_query = base_query.where(
                            or_(
                                GymAssignment.target_date > last_target_date,
                                and_(
                                    GymAssignment.target_date == last_target_date,
                                    GymDatabase.id > last_gym_id if last_gym_id else True
                                )
                            )
                        )
            elif sort_by == "assigned_at":
                if last_assigned_at is not None:
                    if sort_order == "desc":
                        base_query = base_query.where(
                            or_(
                                GymAssignment.assigned_at < last_assigned_at,
                                and_(
                                    GymAssignment.assigned_at == last_assigned_at,
                                    GymDatabase.id < last_gym_id if last_gym_id else True
                                )
                            )
                        )
                    else:
                        base_query = base_query.where(
                            or_(
                                GymAssignment.assigned_at > last_assigned_at,
                                and_(
                                    GymAssignment.assigned_at == last_assigned_at,
                                    GymDatabase.id > last_gym_id if last_gym_id else True
                                )
                            )
                        )
            elif sort_by == "gym_name":
                if last_gym_name is not None:
                    if sort_order == "desc":
                        base_query = base_query.where(GymDatabase.gym_name < last_gym_name)
                    else:
                        base_query = base_query.where(GymDatabase.gym_name > last_gym_name)

        # Apply sorting
        if sort_order == "desc":
            base_query = base_query.order_by(desc(sort_column), desc(GymDatabase.id))
        else:
            base_query = base_query.order_by(sort_column, GymDatabase.id)

        # Apply pagination
        paginated_query = base_query.limit(page_size + 1)

        # Execute query
        result = await db.execute(paginated_query)
        rows = result.all()

        # Check if there are more results
        has_more = len(rows) > page_size
        if has_more:
            rows = rows[:page_size]

        # Process results
        gyms = []
        next_cursor_data = None

        for row in rows:
            (gym_id, gym_name, contact_person, contact_phone, city, address,
             area, zone, isprime, location, gym_type, target_date, assigned_at,
             telecaller_id, telecaller_name, telecaller_mobile) = row

            # Calculate days since assigned
            days_since_assigned = None
            if assigned_at:
                if assigned_at.tzinfo is None:
                    assigned_at_ist = ist_tz.localize(assigned_at)
                else:
                    assigned_at_ist = assigned_at.astimezone(ist_tz)
                days_since_assigned = (ist_tz.localize(datetime.now()) - assigned_at_ist).days

            gym_item = ManagerGymListItem(
                gym_id=gym_id,
                gym_name=gym_name,
                contact_person=contact_person,
                contact_number=contact_phone,
                city=city,
                address=address,
                area=area,
                zone=zone,
                isprime=isprime,
                location=location,
                type=gym_type,
                target_date=target_date.isoformat() if target_date else None,
                assigned_at=assigned_at.isoformat() if assigned_at else None,
                days_since_assigned=days_since_assigned,
                telecaller_id=telecaller_id,
                telecaller_name=telecaller_name,
                telecaller_mobile=telecaller_mobile
            )
            gyms.append(gym_item)

            # Store cursor data
            next_cursor_data = {
                "target_date": target_date.isoformat() if target_date else None,
                "assigned_at": assigned_at.isoformat() if assigned_at else None,
                "gym_id": gym_id,
                "gym_name": gym_name
            }

        # Encode next cursor
        next_cursor = None
        if has_more and next_cursor_data:
            cursor_json = json.dumps(next_cursor_data)
            next_cursor = quote(base64.b64encode(cursor_json.encode()).decode())

        # Get total count only if requested
        total_count = None
        if include_total_count:
            count_query = select(func.count(GymDatabase.id)).select_from(
                GymAssignment
            ).join(
                GymDatabase,
                GymAssignment.gym_id == GymDatabase.id
            ).join(
                Telecaller,
                GymAssignment.telecaller_id == Telecaller.id
            ).where(
                and_(
                    Telecaller.manager_id == manager.id,
                    GymAssignment.status == "active",
                    ~GymAssignment.gym_id.in_(
                        select(GymCallLogs.gym_id).where(
                            and_(
                                GymCallLogs.telecaller_id == GymAssignment.telecaller_id,
                                GymCallLogs.call_status.in_(['follow_up', 'follow_up_required', 'converted', 'rejected', 'no_response', 'out_of_service', 'delegated'])
                            )
                        )
                    )
                )
            )

            # Apply same filters
            if telecaller_id:
                count_query = count_query.where(GymAssignment.telecaller_id == telecaller_id)
            if type:
                count_query = count_query.where(GymDatabase.type == type)
            if target_date_filter and target_date_filter != "all":
                if target_date_filter == "today":
                    count_query = count_query.where(GymAssignment.target_date == today)
                elif target_date_filter == "this_week":
                    week_start = today - timedelta(days=today.weekday())
                    week_end = week_start + timedelta(days=6)
                    count_query = count_query.where(GymAssignment.target_date.between(week_start, week_end))
                elif target_date_filter == "this_month":
                    count_query = count_query.where(
                        and_(
                            func.extract('year', GymAssignment.target_date) == today.year,
                            func.extract('month', GymAssignment.target_date) == today.month
                        )
                    )
                elif target_date_filter == "overdue":
                    count_query = count_query.where(GymAssignment.target_date < today)
                elif target_date_filter == "custom" and target_start_date and target_end_date:
                    count_query = count_query.where(GymAssignment.target_date.between(target_start_date, target_end_date))
            if search_query:
                search_pattern = f"%{search_query}%"
                count_query = count_query.where(
                    or_(
                        GymDatabase.gym_name.ilike(search_pattern),
                        GymDatabase.contact_person.ilike(search_pattern)
                    )
                )

            count_result = await db.execute(count_query)
            total_count = count_result.scalar() or 0

        return ManagerCursorPaginatedResponse(
            gyms=gyms,
            next_cursor=next_cursor,
            has_more=has_more,
            page_size=page_size,
            total_count=total_count
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


@router.get("/pending-count")
async def get_manager_pending_count(
    target_date_filter: Optional[str] = Query(None, description="Filter by target date"),
    target_start_date: Optional[date] = Query(None, description="Target start date for custom filter"),
    target_end_date: Optional[date] = Query(None, description="Target end date for custom filter"),
    manager: Manager = Depends(get_current_manager),
    db: AsyncSession = Depends(get_async_db)
):
    """Get count of pending gyms for statistics."""
    try:
        today = date.today()

        count_query = select(func.count(GymDatabase.id)).select_from(
            GymAssignment
        ).join(
            GymDatabase,
            GymAssignment.gym_id == GymDatabase.id
        ).join(
            Telecaller,
            GymAssignment.telecaller_id == Telecaller.id
        ).where(
            and_(
                Telecaller.manager_id == manager.id,
                GymAssignment.status == "active",
                ~GymAssignment.gym_id.in_(
                    select(GymCallLogs.gym_id).where(
                        and_(
                            GymCallLogs.telecaller_id == GymAssignment.telecaller_id,
                            GymCallLogs.call_status.in_(['follow_up', 'follow_up_required', 'converted', 'rejected', 'no_response', 'out_of_service', 'delegated'])
                        )
                    )
                )
            )
        )

        # Apply target date filter
        if target_date_filter and target_date_filter != "all":
            if target_date_filter == "today":
                count_query = count_query.where(GymAssignment.target_date == today)
            elif target_date_filter == "this_week":
                week_start = today - timedelta(days=today.weekday())
                week_end = week_start + timedelta(days=6)
                count_query = count_query.where(GymAssignment.target_date.between(week_start, week_end))
            elif target_date_filter == "this_month":
                count_query = count_query.where(
                    and_(
                        func.extract('year', GymAssignment.target_date) == today.year,
                        func.extract('month', GymAssignment.target_date) == today.month
                    )
                )
            elif target_date_filter == "overdue":
                count_query = count_query.where(GymAssignment.target_date < today)
            elif target_date_filter == "custom" and target_start_date and target_end_date:
                count_query = count_query.where(GymAssignment.target_date.between(target_start_date, target_end_date))

        result = await db.execute(count_query)
        count = result.scalar() or 0

        return {"count": count}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


# ============================================================================
# Follow-up Gyms Endpoint
# ============================================================================

@router.get("/follow-up-gyms", response_model=FollowUpCursorPaginatedResponse)
async def get_manager_follow_up_gyms(
    cursor: Optional[str] = Query(None, description="Encoded cursor for pagination"),
    page_size: int = Query(50, ge=1, le=100, description="Number of items per page"),
    include_total_count: bool = Query(False, description="Include total count"),
    follow_up_filter: Optional[str] = Query(None, description="Filter by follow-up date: today, this_week, this_month, overdue, custom"),
    follow_up_start_date: Optional[date] = Query(None, description="Follow-up start date for custom filter"),
    follow_up_end_date: Optional[date] = Query(None, description="Follow-up end date for custom filter"),
    search_query: Optional[str] = Query(None, description="Search by gym name or contact person"),
    telecaller_id: Optional[int] = Query(None, description="Filter by telecaller ID"),
    type: Optional[str] = Query(None, description="Filter by gym type"),
    sort_by: Optional[str] = Query("follow_up_date", description="Sort by: follow_up_date, last_call_date, gym_name"),
    sort_order: Optional[str] = Query("asc", description="Sort order: asc, desc"),
    manager: Manager = Depends(get_current_manager),
    db: AsyncSession = Depends(get_async_db)
):
    """
    Get follow-up gyms for manager with CURSOR-BASED pagination.

    A gym appears in Follow-up tab if:
    1. Latest gym_call_logs entry has call_status='follow_up' for telecaller under this manager
    2. OR latest entry has call_status='delegated' AND assigned_telecaller_id is under this manager

    Only considers LATEST entry per gym_id from gym_call_logs table.
    """
    try:
        import base64
        import json
        from urllib.parse import unquote, quote

        ist_tz = pytz.timezone('Asia/Kolkata')
        today = date.today()

        # Decode cursor
        last_follow_up_date = None
        last_created_at = None
        last_gym_id = None
        last_gym_name = None

        if cursor:
            try:
                cursor_data = json.loads(base64.b64decode(unquote(cursor)))
                last_follow_up_date_str = cursor_data.get("follow_up_date")
                last_created_at_str = cursor_data.get("created_at")
                last_gym_id = cursor_data.get("gym_id")
                last_gym_name = cursor_data.get("gym_name")

                if last_follow_up_date_str:
                    last_follow_up_date = datetime.fromisoformat(last_follow_up_date_str)
                if last_created_at_str:
                    last_created_at = datetime.fromisoformat(last_created_at_str)
            except Exception:
                pass

        # Determine sort column
        if sort_by == "follow_up_date":
            sort_column = GymCallLogs.follow_up_date
        elif sort_by == "last_call_date":
            sort_column = GymCallLogs.created_at
        else:
            sort_column = GymDatabase.gym_name

        # Get latest log for each gym
        latest_log_subq = select(
            GymCallLogs.gym_id,
            func.max(GymCallLogs.created_at).label('max_created')
        ).group_by(GymCallLogs.gym_id).subquery()

        # Build main query
        base_query = select(
            GymDatabase.id,
            GymDatabase.gym_name,
            GymDatabase.contact_person,
            GymDatabase.contact_phone,
            GymDatabase.city,
            GymDatabase.address,
            GymDatabase.area,
            GymDatabase.zone,
            GymDatabase.isprime,
            GymDatabase.location,
            GymDatabase.type,
            GymCallLogs.follow_up_date,
            GymCallLogs.created_at,
            GymCallLogs.interest_level,
            GymCallLogs.remarks,
            GymCallLogs.telecaller_id.label('log_telecaller_id'),
            GymCallLogs.assigned_telecaller_id,
            Telecaller.id.label('telecaller_id'),
            Telecaller.name.label('telecaller_name'),
            Telecaller.mobile_number.label('telecaller_mobile')
        ).select_from(
            GymDatabase
        ).join(
            GymCallLogs,
            GymDatabase.id == GymCallLogs.gym_id
        ).join(
            latest_log_subq,
            and_(
                GymCallLogs.gym_id == latest_log_subq.c.gym_id,
                GymCallLogs.created_at == latest_log_subq.c.max_created
            )
        ).join(
            Telecaller,
            GymCallLogs.telecaller_id == Telecaller.id
        ).where(
            or_(
                # Condition 1: My telecaller's follow-up
                and_(
                    Telecaller.manager_id == manager.id,
                    GymCallLogs.call_status == 'follow_up'
                ),
                # Condition 2: Delegated to my telecaller by someone else
                and_(
                    GymCallLogs.assigned_telecaller_id.in_(
                        select(Telecaller.id).where(Telecaller.manager_id == manager.id)
                    ),
                    GymCallLogs.call_status == 'delegated'
                )
            )
        )

        # Apply telecaller filter
        if telecaller_id:
            base_query = base_query.where(
                or_(
                    GymCallLogs.telecaller_id == telecaller_id,
                    GymCallLogs.assigned_telecaller_id == telecaller_id
                )
            )

        # Apply type filter
        if type:
            base_query = base_query.where(GymDatabase.type == type)

        # Apply follow-up date filter
        if follow_up_filter and follow_up_filter != "all":
            if follow_up_filter == "today":
                base_query = base_query.where(func.date(GymCallLogs.follow_up_date) == today)
            elif follow_up_filter == "this_week":
                week_start = today - timedelta(days=today.weekday())
                week_end = week_start + timedelta(days=6)
                base_query = base_query.where(func.date(GymCallLogs.follow_up_date).between(week_start, week_end))
            elif follow_up_filter == "this_month":
                base_query = base_query.where(
                    and_(
                        func.extract('year', GymCallLogs.follow_up_date) == today.year,
                        func.extract('month', GymCallLogs.follow_up_date) == today.month
                    )
                )
            elif follow_up_filter == "overdue":
                base_query = base_query.where(func.date(GymCallLogs.follow_up_date) < today)
            elif follow_up_filter == "custom" and follow_up_start_date and follow_up_end_date:
                base_query = base_query.where(func.date(GymCallLogs.follow_up_date).between(follow_up_start_date, follow_up_end_date))

        # Apply search filter
        if search_query:
            search_pattern = f"%{search_query}%"
            base_query = base_query.where(
                or_(
                    GymDatabase.gym_name.ilike(search_pattern),
                    GymDatabase.contact_person.ilike(search_pattern)
                )
            )

        # CURSOR-BASED PAGINATION
        if cursor and (last_follow_up_date is not None or last_created_at is not None or last_gym_name is not None):
            if sort_by == "follow_up_date":
                if last_follow_up_date is not None:
                    if sort_order == "desc":
                        base_query = base_query.where(
                            or_(
                                GymCallLogs.follow_up_date < last_follow_up_date,
                                and_(
                                    GymCallLogs.follow_up_date == last_follow_up_date,
                                    GymDatabase.id < last_gym_id if last_gym_id else True
                                )
                            )
                        )
                    else:
                        base_query = base_query.where(
                            or_(
                                GymCallLogs.follow_up_date > last_follow_up_date,
                                and_(
                                    GymCallLogs.follow_up_date == last_follow_up_date,
                                    GymDatabase.id > last_gym_id if last_gym_id else True
                                )
                            )
                        )
            elif sort_by == "last_call_date":
                if last_created_at is not None:
                    if sort_order == "desc":
                        base_query = base_query.where(
                            or_(
                                GymCallLogs.created_at < last_created_at,
                                and_(
                                    GymCallLogs.created_at == last_created_at,
                                    GymDatabase.id < last_gym_id if last_gym_id else True
                                )
                            )
                        )
                    else:
                        base_query = base_query.where(
                            or_(
                                GymCallLogs.created_at > last_created_at,
                                and_(
                                    GymCallLogs.created_at == last_created_at,
                                    GymDatabase.id > last_gym_id if last_gym_id else True
                                )
                            )
                        )
            elif sort_by == "gym_name":
                if last_gym_name is not None:
                    if sort_order == "desc":
                        base_query = base_query.where(GymDatabase.gym_name < last_gym_name)
                    else:
                        base_query = base_query.where(GymDatabase.gym_name > last_gym_name)

        # Apply sorting
        if sort_order == "desc":
            base_query = base_query.order_by(desc(sort_column), desc(GymDatabase.id))
        else:
            base_query = base_query.order_by(sort_column, GymDatabase.id)

        # Apply pagination
        paginated_query = base_query.limit(page_size + 1)

        # Execute query
        result = await db.execute(paginated_query)
        rows = result.all()

        # Check if there are more results
        has_more = len(rows) > page_size
        if has_more:
            rows = rows[:page_size]

        # Get delegated telecaller IDs for batch query
        delegated_telecaller_ids = list(set([
            row.log_telecaller_id for row in rows
            if row.log_telecaller_id != row.telecaller_id
        ]))

        # Batch query for delegated telecaller names
        delegated_by_names = {}
        if delegated_telecaller_ids:
            result_names = await db.execute(
                select(Telecaller.id, Telecaller.name).where(
                    Telecaller.id.in_(delegated_telecaller_ids)
                )
            )
            delegated_by_names = {row[0]: row[1] for row in result_names.all()}

        # Process results
        gyms = []
        next_cursor_data = None

        for row in rows:
            (gym_id, gym_name, contact_person, contact_phone, city, address,
             area, zone, isprime, location, gym_type, follow_up_date, created_at,
             interest_level, remarks, log_telecaller_id, assigned_telecaller_id,
             telecaller_id, telecaller_name, telecaller_mobile) = row

            # Format follow_up_date
            follow_up_date_iso = None
            if follow_up_date:
                if follow_up_date.tzinfo is None:
                    follow_up_date = ist_tz.localize(follow_up_date)
                else:
                    follow_up_date = follow_up_date.astimezone(ist_tz)
                follow_up_date_iso = follow_up_date.isoformat()

            # Format created_at
            created_at_iso = None
            if created_at:
                if created_at.tzinfo is None:
                    created_at = ist_tz.localize(created_at)
                else:
                    created_at = created_at.astimezone(ist_tz)
                created_at_iso = created_at.isoformat()

            gym_item = FollowUpGymListItem(
                gym_id=gym_id,
                gym_name=gym_name,
                contact_person=contact_person,
                contact_number=contact_phone,
                city=city,
                address=address,
                area=area,
                zone=zone,
                isprime=isprime,
                location=location,
                type=gym_type,
                follow_up_date=follow_up_date_iso,
                last_call_date=created_at_iso,
                interest_level=interest_level,
                remarks=remarks,
                telecaller_id=telecaller_id,
                telecaller_name=telecaller_name,
                telecaller_mobile=telecaller_mobile,
                delegated_by_name=delegated_by_names.get(log_telecaller_id) if log_telecaller_id != telecaller_id else None
            )
            gyms.append(gym_item)

            # Store cursor data
            next_cursor_data = {
                "follow_up_date": follow_up_date_iso,
                "created_at": created_at_iso,
                "gym_id": gym_id,
                "gym_name": gym_name
            }

        # Encode next cursor
        next_cursor = None
        if has_more and next_cursor_data:
            cursor_json = json.dumps(next_cursor_data)
            next_cursor = quote(base64.b64encode(cursor_json.encode()).decode())

        # Get total count only if requested
        total_count = None
        if include_total_count:
            count_query = select(func.count(GymDatabase.id)).select_from(
                GymDatabase
            ).join(
                GymCallLogs,
                GymDatabase.id == GymCallLogs.gym_id
            ).join(
                latest_log_subq,
                and_(
                    GymCallLogs.gym_id == latest_log_subq.c.gym_id,
                    GymCallLogs.created_at == latest_log_subq.c.max_created
                )
            ).join(
                Telecaller,
                GymCallLogs.telecaller_id == Telecaller.id
            ).where(
                or_(
                    and_(
                        Telecaller.manager_id == manager.id,
                        GymCallLogs.call_status == 'follow_up'
                    ),
                    and_(
                        GymCallLogs.assigned_telecaller_id.in_(
                            select(Telecaller.id).where(Telecaller.manager_id == manager.id)
                        ),
                        GymCallLogs.call_status == 'delegated'
                    )
                )
            )

            # Apply same filters
            if telecaller_id:
                count_query = count_query.where(
                    or_(
                        GymCallLogs.telecaller_id == telecaller_id,
                        GymCallLogs.assigned_telecaller_id == telecaller_id
                    )
                )
            if type:
                count_query = count_query.where(GymDatabase.type == type)
            if follow_up_filter and follow_up_filter != "all":
                if follow_up_filter == "today":
                    count_query = count_query.where(func.date(GymCallLogs.follow_up_date) == today)
                elif follow_up_filter == "this_week":
                    week_start = today - timedelta(days=today.weekday())
                    week_end = week_start + timedelta(days=6)
                    count_query = count_query.where(func.date(GymCallLogs.follow_up_date).between(week_start, week_end))
                elif follow_up_filter == "this_month":
                    count_query = count_query.where(
                        and_(
                            func.extract('year', GymCallLogs.follow_up_date) == today.year,
                            func.extract('month', GymCallLogs.follow_up_date) == today.month
                        )
                    )
                elif follow_up_filter == "overdue":
                    count_query = count_query.where(func.date(GymCallLogs.follow_up_date) < today)
                elif follow_up_filter == "custom" and follow_up_start_date and follow_up_end_date:
                    count_query = count_query.where(func.date(GymCallLogs.follow_up_date).between(follow_up_start_date, follow_up_end_date))
            if search_query:
                search_pattern = f"%{search_query}%"
                count_query = count_query.where(
                    or_(
                        GymDatabase.gym_name.ilike(search_pattern),
                        GymDatabase.contact_person.ilike(search_pattern)
                    )
                )

            count_result = await db.execute(count_query)
            total_count = count_result.scalar() or 0

        return FollowUpCursorPaginatedResponse(
            gyms=gyms,
            next_cursor=next_cursor,
            has_more=has_more,
            page_size=page_size,
            total_count=total_count
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


# ============================================================================
# Other Status Gyms Endpoint (Converted, Rejected, No Response, Out of Service)
# ============================================================================

@router.get("/other-status-gyms", response_model=OtherStatusCursorPaginatedResponse)
async def get_manager_other_status_gyms(
    call_status: str = Query(..., description="Status: converted, rejected, no_response, out_of_service"),
    cursor: Optional[str] = Query(None, description="Encoded cursor for pagination"),
    page_size: int = Query(50, ge=1, le=100, description="Number of items per page"),
    include_total_count: bool = Query(False, description="Include total count"),
    status_date_filter: Optional[str] = Query(None, description="Filter by status date: today, this_week, this_month, custom"),
    status_start_date: Optional[date] = Query(None, description="Status start date for custom filter"),
    status_end_date: Optional[date] = Query(None, description="Status end date for custom filter"),
    verification_complete: Optional[str] = Query(None, description="For converted: Filter by verification completion"),
    search_query: Optional[str] = Query(None, description="Search by gym name or contact person"),
    telecaller_id: Optional[int] = Query(None, description="Filter by telecaller ID"),
    type: Optional[str] = Query(None, description="Filter by gym type"),
    sort_by: Optional[str] = Query("call_date", description="Sort by: call_date, gym_name"),
    sort_order: Optional[str] = Query("desc", description="Sort order: asc, desc"),
    manager: Manager = Depends(get_current_manager),
    db: AsyncSession = Depends(get_async_db)
):
    """
    Get gyms for other status tabs with CURSOR-BASED pagination.

    Supports: Converted, Rejected, No Response, Out of Service tabs.
    Only considers LATEST entry per gym_id from gym_call_logs table.
    """
    try:
        import base64
        import json
        from urllib.parse import unquote, quote

        ist_tz = pytz.timezone('Asia/Kolkata')
        today = date.today()

        # Validate call_status
        valid_statuses = ['converted', 'rejected', 'no_response', 'out_of_service']
        if call_status not in valid_statuses:
            raise HTTPException(status_code=400, detail=f"Invalid call_status. Must be one of: {', '.join(valid_statuses)}")

        # Decode cursor
        last_created_at = None
        last_gym_id = None
        last_gym_name = None

        if cursor:
            try:
                cursor_data = json.loads(base64.b64decode(unquote(cursor)))
                last_created_at_str = cursor_data.get("created_at")
                last_gym_id = cursor_data.get("gym_id")
                last_gym_name = cursor_data.get("gym_name")

                if last_created_at_str:
                    last_created_at = datetime.fromisoformat(last_created_at_str)
            except Exception:
                pass

        # Determine sort column
        sort_column = GymCallLogs.created_at if sort_by == "call_date" else GymDatabase.gym_name

        # Get latest log for each gym
        latest_log_subq = select(
            GymCallLogs.gym_id,
            func.max(GymCallLogs.created_at).label('max_created')
        ).group_by(GymCallLogs.gym_id).subquery()

        # Build main query
        base_query = select(
            GymDatabase.id,
            GymDatabase.gym_name,
            GymDatabase.contact_person,
            GymDatabase.contact_phone,
            GymDatabase.city,
            GymDatabase.address,
            GymDatabase.area,
            GymDatabase.zone,
            GymDatabase.isprime,
            GymDatabase.location,
            GymDatabase.type,
            GymCallLogs.created_at,
            GymCallLogs.interest_level,
            GymCallLogs.remarks,
            Telecaller.id.label('telecaller_id'),
            Telecaller.name.label('telecaller_name'),
            Telecaller.mobile_number.label('telecaller_mobile')
        ).select_from(
            GymDatabase
        ).join(
            GymCallLogs,
            GymDatabase.id == GymCallLogs.gym_id
        ).join(
            latest_log_subq,
            and_(
                GymCallLogs.gym_id == latest_log_subq.c.gym_id,
                GymCallLogs.created_at == latest_log_subq.c.max_created
            )
        ).join(
            Telecaller,
            GymCallLogs.telecaller_id == Telecaller.id
        ).where(
            and_(
                Telecaller.manager_id == manager.id,
                GymCallLogs.call_status == call_status
            )
        )

        # Apply telecaller filter
        if telecaller_id:
            base_query = base_query.where(GymCallLogs.telecaller_id == telecaller_id)

        # Apply type filter
        if type:
            base_query = base_query.where(GymDatabase.type == type)

        # Apply status date filter
        if status_date_filter and status_date_filter != "all":
            if status_date_filter == "today":
                base_query = base_query.where(func.date(GymCallLogs.created_at) == today)
            elif status_date_filter == "this_week":
                week_start = today - timedelta(days=today.weekday())
                week_end = week_start + timedelta(days=6)
                base_query = base_query.where(func.date(GymCallLogs.created_at).between(week_start, week_end))
            elif status_date_filter == "this_month":
                base_query = base_query.where(
                    and_(
                        func.extract('year', GymCallLogs.created_at) == today.year,
                        func.extract('month', GymCallLogs.created_at) == today.month
                    )
                )
            elif status_date_filter == "custom" and status_start_date and status_end_date:
                base_query = base_query.where(func.date(GymCallLogs.created_at).between(status_start_date, status_end_date))

        # Apply verification filter for converted status
        if call_status == 'converted' and verification_complete:
            base_query = base_query.join(
                ConvertedStatus,
                and_(
                    ConvertedStatus.gym_id == GymDatabase.id,
                    ConvertedStatus.telecaller_id == Telecaller.id
                )
            )

            # Check if verification is complete
            if verification_complete.lower() == 'true':
                # All required fields must be True
                base_query = base_query.where(
                    and_(
                        ConvertedStatus.document_uploaded == True,
                        ConvertedStatus.membership_plan_created == True,
                        ConvertedStatus.session_created == True,
                        ConvertedStatus.daily_pass_created == True,
                        ConvertedStatus.gym_studio_images_uploaded == True,
                        ConvertedStatus.agreement_signed == True
                    )
                )
            elif verification_complete.lower() == 'false':
                # At least one required field is False
                base_query = base_query.where(
                    or_(
                        ConvertedStatus.document_uploaded == False,
                        ConvertedStatus.membership_plan_created == False,
                        ConvertedStatus.session_created == False,
                        ConvertedStatus.daily_pass_created == False,
                        ConvertedStatus.gym_studio_images_uploaded == False,
                        ConvertedStatus.agreement_signed == False
                    )
                )

        # Apply search filter
        if search_query:
            search_pattern = f"%{search_query}%"
            base_query = base_query.where(
                or_(
                    GymDatabase.gym_name.ilike(search_pattern),
                    GymDatabase.contact_person.ilike(search_pattern)
                )
            )

        # CURSOR-BASED PAGINATION
        if cursor and (last_created_at is not None or last_gym_name is not None):
            if sort_by == "call_date":
                if last_created_at is not None:
                    if sort_order == "desc":
                        base_query = base_query.where(
                            or_(
                                GymCallLogs.created_at < last_created_at,
                                and_(
                                    GymCallLogs.created_at == last_created_at,
                                    GymDatabase.id < last_gym_id if last_gym_id else True
                                )
                            )
                        )
                    else:
                        base_query = base_query.where(
                            or_(
                                GymCallLogs.created_at > last_created_at,
                                and_(
                                    GymCallLogs.created_at == last_created_at,
                                    GymDatabase.id > last_gym_id if last_gym_id else True
                                )
                            )
                        )
            elif sort_by == "gym_name":
                if last_gym_name is not None:
                    if sort_order == "desc":
                        base_query = base_query.where(GymDatabase.gym_name < last_gym_name)
                    else:
                        base_query = base_query.where(GymDatabase.gym_name > last_gym_name)

        # Apply sorting
        if sort_order == "desc":
            base_query = base_query.order_by(desc(sort_column), desc(GymDatabase.id))
        else:
            base_query = base_query.order_by(sort_column, GymDatabase.id)

        # Apply pagination
        paginated_query = base_query.limit(page_size + 1)

        # Execute query
        result = await db.execute(paginated_query)
        rows = result.all()

        # Check if there are more results
        has_more = len(rows) > page_size
        if has_more:
            rows = rows[:page_size]

        # For converted status, fetch converted_status data in batch
        gym_ids = [row[0] for row in rows]
        telecaller_ids = [row[14] for row in rows]  # telecaller_id

        converted_status_map = {}
        if call_status == 'converted' and gym_ids:
            result_cs = await db.execute(
                select(ConvertedStatus).where(
                    and_(
                        ConvertedStatus.gym_id.in_(gym_ids),
                        ConvertedStatus.telecaller_id.in_(telecaller_ids)
                    )
                )
            )
            for cs in result_cs.scalars():
                key = f"{cs.gym_id}_{cs.telecaller_id}"
                converted_status_map[key] = cs

        # Process results
        gyms = []
        next_cursor_data = None

        for row in rows:
            (gym_id, gym_name, contact_person, contact_phone, city, address,
             area, zone, isprime, location, gym_type, created_at,
             interest_level, remarks, telecaller_id, telecaller_name, telecaller_mobile) = row

            # Format created_at
            created_at_iso = None
            if created_at:
                if created_at.tzinfo is None:
                    created_at = ist_tz.localize(created_at)
                else:
                    created_at = created_at.astimezone(ist_tz)
                created_at_iso = created_at.isoformat()

            # Get converted status if applicable
            cs_key = f"{gym_id}_{telecaller_id}"
            cs = converted_status_map.get(cs_key)

            gym_item = OtherStatusGymListItem(
                gym_id=gym_id,
                gym_name=gym_name,
                contact_person=contact_person,
                contact_number=contact_phone,
                city=city,
                address=address,
                area=area,
                zone=zone,
                isprime=isprime,
                location=location,
                type=gym_type,
                call_date=created_at_iso,
                call_status=call_status,
                interest_level=interest_level,
                remarks=remarks,
                telecaller_id=telecaller_id,
                telecaller_name=telecaller_name,
                telecaller_mobile=telecaller_mobile,
                document_collected=cs.document_uploaded if cs else None,
                membership_collected=cs.membership_plan_created if cs else None,
                session_collected=cs.session_created if cs else None,
                daily_pass_collected=cs.daily_pass_created if cs else None,
                studio_images_collected=cs.gym_studio_images_uploaded if cs else None,
                agreement_collected=cs.agreement_signed if cs else None
            )
            gyms.append(gym_item)

            # Store cursor data
            next_cursor_data = {
                "created_at": created_at_iso,
                "gym_id": gym_id,
                "gym_name": gym_name
            }

        # Encode next cursor
        next_cursor = None
        if has_more and next_cursor_data:
            cursor_json = json.dumps(next_cursor_data)
            next_cursor = quote(base64.b64encode(cursor_json.encode()).decode())

        # Get total count only if requested
        total_count = None
        if include_total_count:
            count_query = select(func.count(GymDatabase.id)).select_from(
                GymDatabase
            ).join(
                GymCallLogs,
                GymDatabase.id == GymCallLogs.gym_id
            ).join(
                latest_log_subq,
                and_(
                    GymCallLogs.gym_id == latest_log_subq.c.gym_id,
                    GymCallLogs.created_at == latest_log_subq.c.max_created
                )
            ).join(
                Telecaller,
                GymCallLogs.telecaller_id == Telecaller.id
            ).where(
                and_(
                    Telecaller.manager_id == manager.id,
                    GymCallLogs.call_status == call_status
                )
            )

            # Apply same filters
            if telecaller_id:
                count_query = count_query.where(GymCallLogs.telecaller_id == telecaller_id)
            if type:
                count_query = count_query.where(GymDatabase.type == type)
            if status_date_filter and status_date_filter != "all":
                if status_date_filter == "today":
                    count_query = count_query.where(func.date(GymCallLogs.created_at) == today)
                elif status_date_filter == "this_week":
                    week_start = today - timedelta(days=today.weekday())
                    week_end = week_start + timedelta(days=6)
                    count_query = count_query.where(func.date(GymCallLogs.created_at).between(week_start, week_end))
                elif status_date_filter == "this_month":
                    count_query = count_query.where(
                        and_(
                            func.extract('year', GymCallLogs.created_at) == today.year,
                            func.extract('month', GymCallLogs.created_at) == today.month
                        )
                    )
                elif status_date_filter == "custom" and status_start_date and status_end_date:
                    count_query = count_query.where(func.date(GymCallLogs.created_at).between(status_start_date, status_end_date))
            if search_query:
                search_pattern = f"%{search_query}%"
                count_query = count_query.where(
                    or_(
                        GymDatabase.gym_name.ilike(search_pattern),
                        GymDatabase.contact_person.ilike(search_pattern)
                    )
                )

            count_result = await db.execute(count_query)
            total_count = count_result.scalar() or 0

        return OtherStatusCursorPaginatedResponse(
            gyms=gyms,
            next_cursor=next_cursor,
            has_more=has_more,
            page_size=page_size,
            total_count=total_count
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


# ============================================================================
# Statistics/Counts Endpoints
# ============================================================================

@router.get("/stats-counts")
async def get_manager_stats_counts(
    target_date_filter: Optional[str] = Query(None, description="Filter by target date for pending"),
    target_start_date: Optional[date] = Query(None, description="Target start date for custom filter"),
    target_end_date: Optional[date] = Query(None, description="Target end date for custom filter"),
    follow_up_filter: Optional[str] = Query(None, description="Filter by follow-up date"),
    follow_up_start_date: Optional[date] = Query(None, description="Follow-up start date for custom filter"),
    follow_up_end_date: Optional[date] = Query(None, description="Follow-up end date for custom filter"),
    converted_filter: Optional[str] = Query(None, description="Filter by converted date"),
    converted_start_date: Optional[date] = Query(None, description="Converted start date for custom filter"),
    converted_end_date: Optional[date] = Query(None, description="Converted end date for custom filter"),
    rejected_filter: Optional[str] = Query(None, description="Filter by rejected date"),
    rejected_start_date: Optional[date] = Query(None, description="Rejected start date for custom filter"),
    rejected_end_date: Optional[date] = Query(None, description="Rejected end date for custom filter"),
    no_response_filter: Optional[str] = Query(None, description="Filter by no response date"),
    no_response_start_date: Optional[date] = Query(None, description="No response start date for custom filter"),
    no_response_end_date: Optional[date] = Query(None, description="No response end date for custom filter"),
    out_of_service_filter: Optional[str] = Query(None, description="Filter by out of service date"),
    out_of_service_start_date: Optional[date] = Query(None, description="Out of service start date for custom filter"),
    out_of_service_end_date: Optional[date] = Query(None, description="Out of service end date for custom filter"),
    manager: Manager = Depends(get_current_manager),
    db: AsyncSession = Depends(get_async_db)
):
    """
    Get all status counts in a single optimized call.

    This replaces 6 separate API calls with one optimized query.
    Returns counts for: pending, follow_up, converted, rejected, no_response, out_of_service
    """
    try:
        today = date.today()
        ist_tz = pytz.timezone('Asia/Kolkata')

        # ============================================================================
        # 1. PENDING COUNT
        # ============================================================================
        pending_count_query = select(func.count(GymDatabase.id)).select_from(
            GymAssignment
        ).join(
            GymDatabase,
            GymAssignment.gym_id == GymDatabase.id
        ).join(
            Telecaller,
            GymAssignment.telecaller_id == Telecaller.id
        ).where(
            and_(
                Telecaller.manager_id == manager.id,
                GymAssignment.status == "active",
                ~GymAssignment.gym_id.in_(
                    select(GymCallLogs.gym_id).where(
                        and_(
                            GymCallLogs.telecaller_id == GymAssignment.telecaller_id,
                            GymCallLogs.call_status.in_(['follow_up', 'follow_up_required', 'converted', 'rejected', 'no_response', 'out_of_service', 'delegated'])
                        )
                    )
                )
            )
        )

        # Apply target date filter for pending
        if target_date_filter and target_date_filter != "all":
            if target_date_filter == "today":
                pending_count_query = pending_count_query.where(GymAssignment.target_date == today)
            elif target_date_filter == "this_week":
                week_start = today - timedelta(days=today.weekday())
                week_end = week_start + timedelta(days=6)
                pending_count_query = pending_count_query.where(GymAssignment.target_date.between(week_start, week_end))
            elif target_date_filter == "this_month":
                pending_count_query = pending_count_query.where(
                    and_(
                        func.extract('year', GymAssignment.target_date) == today.year,
                        func.extract('month', GymAssignment.target_date) == today.month
                    )
                )
            elif target_date_filter == "overdue":
                pending_count_query = pending_count_query.where(GymAssignment.target_date < today)
            elif target_date_filter == "custom" and target_start_date and target_end_date:
                pending_count_query = pending_count_query.where(GymAssignment.target_date.between(target_start_date, target_end_date))

        pending_result = await db.execute(pending_count_query)
        pending_count = pending_result.scalar() or 0

        # ============================================================================
        # 2. OTHER STATUS COUNTS (using latest log subquery)
        # ============================================================================
        latest_log_subq = select(
            GymCallLogs.gym_id,
            func.max(GymCallLogs.created_at).label('max_created')
        ).group_by(GymCallLogs.gym_id).subquery()

        # Base query for all statuses
        status_counts_query = select(
            GymCallLogs.call_status,
            func.count(GymDatabase.id).label('count')
        ).select_from(
            GymDatabase
        ).join(
            GymCallLogs,
            GymDatabase.id == GymCallLogs.gym_id
        ).join(
            latest_log_subq,
            and_(
                GymCallLogs.gym_id == latest_log_subq.c.gym_id,
                GymCallLogs.created_at == latest_log_subq.c.max_created
            )
        ).join(
            Telecaller,
            GymCallLogs.telecaller_id == Telecaller.id
        ).where(
            Telecaller.manager_id == manager.id
        ).group_by(GymCallLogs.call_status)

        status_result = await db.execute(status_counts_query)
        status_rows = status_result.all()

        # Initialize counts
        follow_up_count = 0
        converted_count = 0
        rejected_count = 0
        no_response_count = 0
        out_of_service_count = 0

        # Process status rows
        for call_status, count in status_rows:
            if call_status == 'follow_up' or call_status == 'follow_up_required' or call_status == 'delegated':
                follow_up_count += count
            elif call_status == 'converted':
                converted_count += count
            elif call_status == 'rejected':
                rejected_count += count
            elif call_status == 'no_response':
                no_response_count += count
            elif call_status == 'out_of_service':
                out_of_service_count += count

        return {
            "pending": pending_count,
            "follow_up": follow_up_count,
            "converted": converted_count,
            "rejected": rejected_count,
            "no_response": no_response_count,
            "out_of_service": out_of_service_count,
            "total": pending_count + follow_up_count + converted_count + rejected_count + no_response_count + out_of_service_count
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")
