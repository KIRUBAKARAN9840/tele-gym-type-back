"""
Optimized Async API endpoints for Gym Assignments.

This module provides fully async, optimized endpoints for:
1. Fetching gyms with proper backend pagination
2. Checking assignment status without N+1 queries
3. Getting locations with bulk queries

All endpoints use AsyncSession and avoid any blocking operations.
"""

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import and_, or_, func, not_, desc, select, literal_column
from sqlalchemy.orm import selectinload, joinedload
from typing import List, Optional, Dict, Any
from datetime import datetime, date
import pytz

from app.models.async_database import get_async_db
from app.models.telecaller_models import (
    Manager, Telecaller, GymAssignment, GymAssignmentHistory,
    GymCallLogs, ConvertedStatus, GymDatabase
)
from app.telecaller.dependencies import get_current_manager
from pydantic import BaseModel

router = APIRouter()

# ============================================================================
# Pydantic Models for Request/Response
# ============================================================================

class PaginatedGymsResponse(BaseModel):
    gyms: List[Dict[str, Any]]
    assignments: List[Dict[str, Any]]
    page: int
    limit: int
    total: int
    total_pages: int


class LocationsResponse(BaseModel):
    cities: List[str]
    city_areas: Dict[str, List[str]]
    area_stats: Dict[str, Dict[str, Any]]
    fully_assigned_areas: List[str]


# ============================================================================
# Optimized Gyms List Endpoint with Async and Pagination
# ============================================================================

@router.get("/gyms/all-optimized")
async def get_all_gyms_optimized(
    manager: Manager = Depends(get_current_manager),
    db: AsyncSession = Depends(get_async_db),
    page: int = Query(1, ge=1),
    limit: int = Query(50, ge=1, le=200),
    search: Optional[str] = None,
    city: Optional[str] = None,
    area: Optional[str] = None,
    type: Optional[str] = None
):
    
    try:
        offset = (page - 1) * limit
        ist_tz = pytz.timezone('Asia/Kolkata')

        # ============================================================================
        # STEP 1: Single query to fetch gyms WITH assignment status
        # Using LEFT JOIN to avoid N+1 queries
        # ============================================================================
        base_query = (
            select(
                GymDatabase.id,
                GymDatabase.gym_name,
                GymDatabase.contact_person,
                GymDatabase.contact_phone,
                GymDatabase.address,
                GymDatabase.area,
                GymDatabase.city,
                GymDatabase.state,
                GymDatabase.pincode,
                GymDatabase.zone,
                GymDatabase.approval_status,
                GymDatabase.location,
                GymDatabase.verified,
                GymDatabase.isprime,
                GymDatabase.type,
                # Assignment fields (will be NULL if not assigned)
                # GymAssignment has composite PK (gym_id, telecaller_id), no separate id
                GymAssignment.telecaller_id.label('assigned_telecaller_id'),
                GymAssignment.manager_id.label('assigned_manager_id'),
                GymAssignment.assigned_at.label('assignment_created_at'),
                GymAssignment.target_date,
                Telecaller.name.label('telecaller_name'),
                # Latest call log status (using subquery)
            )
            .outerjoin(
                GymAssignment,
                and_(
                    GymAssignment.gym_id == GymDatabase.id,
                    GymAssignment.status == 'active'
                )
            )
            .outerjoin(
                Telecaller,
                GymAssignment.telecaller_id == Telecaller.id
            )
        )

        # Apply filters at database level
        if search:
            search_pattern = f"%{search}%"
            base_query = base_query.where(
                or_(
                    GymDatabase.gym_name.ilike(search_pattern),
                    GymDatabase.area.ilike(search_pattern),
                    GymDatabase.contact_person.ilike(search_pattern),
                    GymDatabase.contact_phone.ilike(search_pattern)
                )
            )

        if city:
            base_query = base_query.where(GymDatabase.city.ilike(f"%{city}%"))

        if area:
            base_query = base_query.where(GymDatabase.area == area)

        if type:
            base_query = base_query.where(GymDatabase.type == type)

        # Get total count BEFORE pagination
        count_query = select(func.count()).select_from(base_query.subquery())
        total_result = await db.execute(count_query)
        total = total_result.scalar_one() or 0
        total_pages = (total + limit - 1) // limit if total > 0 else 1

        # Apply pagination and fetch results
        paginated_query = base_query.offset(offset).limit(limit)
        result = await db.execute(paginated_query)
        rows = result.all()

        # ============================================================================
        # STEP 2: Fetch latest call status for all gyms in current page
        # Using a single subquery instead of N+1 individual queries
        # ============================================================================

        if not rows:
            return {
                "gyms": [],
                "assignments": {},
                "page": page,
                "limit": limit,
                "total": 0,
                "total_pages": 0
            }

        gym_ids = [row.id for row in rows]

        # Single query to get latest call log for each gym
        latest_call_subquery = (
            select(
                GymCallLogs.gym_id,
                func.max(GymCallLogs.created_at).label('max_created')
            )
            .where(GymCallLogs.gym_id.in_(gym_ids))
            .group_by(GymCallLogs.gym_id)
            .subquery()
        )

        latest_calls_query = (
            select(
                GymCallLogs.gym_id,
                GymCallLogs.call_status,
                GymCallLogs.created_at,
                GymCallLogs.follow_up_date
            )
            .join(
                latest_call_subquery,
                and_(
                    GymCallLogs.gym_id == latest_call_subquery.c.gym_id,
                    GymCallLogs.created_at == latest_call_subquery.c.max_created
                )
            )
        )

        call_result = await db.execute(latest_calls_query)
        call_statuses = {row.gym_id: row for row in call_result.all()}

        # ============================================================================
        # STEP 3: Build response combining all data
        # ============================================================================

        gyms = []
        assignments = {}

        for row in rows:
            gym_data = {
                "id": row.id,
                "gym_name": row.gym_name,
                "contact_person": row.contact_person,
                "contact_phone": row.contact_phone,
                "contact_number": row.contact_phone,  # Alias for frontend
                "address": row.address,
                "area": row.area,
                "city": row.city,
                "state": row.state,
                "pincode": row.pincode,
                "zone": row.zone,
                "approval_status": row.approval_status,
                "location": row.location,
                "verified": row.verified,
                "is_prime": row.isprime == 1 if row.isprime is not None else False,
                "type": row.type
            }
            gyms.append(gym_data)

            # Determine assignment status
            is_assigned = row.assigned_telecaller_id is not None

            if is_assigned:
                # Gym is assigned via gym_assignment table
                current_call_status = 'pending'
                follow_up_date = None

                # Check if there's a call log that overrides this
                if row.id in call_statuses:
                    call_info = call_statuses[row.id]
                    current_call_status = call_info.call_status

                    if call_info.follow_up_date:
                        if call_info.follow_up_date.tzinfo is None:
                            follow_up_date = ist_tz.localize(call_info.follow_up_date).isoformat()
                        else:
                            follow_up_date = call_info.follow_up_date.astimezone(ist_tz).isoformat()

                assignments[str(row.id)] = {
                    'gym_id': row.id,
                    'telecaller_id': row.assigned_telecaller_id,
                    'telecaller_name': row.telecaller_name,
                    'manager_id': row.assigned_manager_id,
                    'assigned_at': row.assignment_created_at.isoformat() if row.assignment_created_at else None,
                    'target_date': row.target_date.isoformat() if row.target_date else None,
                    'status': 'active',
                    'current_call_status': current_call_status,
                    'follow_up_date': follow_up_date,
                    'source': 'gym_assignment'
                }
            else:
                # Check if gym is assigned via call_logs (has been called)
                if row.id in call_statuses:
                    call_info = call_statuses[row.id]
                    follow_up_date = None

                    if call_info.follow_up_date:
                        if call_info.follow_up_date.tzinfo is None:
                            follow_up_date = ist_tz.localize(call_info.follow_up_date).isoformat()
                        else:
                            follow_up_date = call_info.follow_up_date.astimezone(ist_tz).isoformat()

                    assignments[str(row.id)] = {
                        'gym_id': row.id,
                        'telecaller_id': None,  # Will be filled if we join telecaller
                        'telecaller_name': None,
                        'manager_id': None,
                        'assigned_at': call_info.created_at.isoformat() if call_info.created_at else None,
                        'target_date': None,
                        'status': 'active',
                        'current_call_status': call_info.call_status,
                        'follow_up_date': follow_up_date,
                        'source': 'gym_call_logs'
                    }

        return {
            "gyms": gyms,
            "assignments": assignments,
            "page": page,
            "limit": limit,
            "total": total,
            "total_pages": total_pages
        }

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch gyms: {str(e)}"
        )


# ============================================================================
# Optimized Assignment Status Check - No N+1 Queries
# ============================================================================

@router.get("/assignments/check-status-optimized")
async def check_assignments_optimized(
    manager: Manager = Depends(get_current_manager),
    db: AsyncSession = Depends(get_async_db),
    city: Optional[str] = None
):
    """
    Check assignment status for gyms with optimized queries.

    This is a replacement for the original check-status endpoint that:
    - Uses async operations
    - Eliminates N+1 queries with bulk fetching
    - Supports filtering by city (for checking fully assigned areas)
    - Returns minimal data required by frontend

    For city filtering: Only checks gyms in the specified city.
    This is used by the frontend to determine which areas are fully assigned.
    """
    try:
        ist_tz = pytz.timezone('Asia/Kolkata')

        # ============================================================================
        # STEP 1: Get latest call logs with gym and telecaller info in ONE query
        # ============================================================================

        latest_log_subquery = (
            select(
                GymCallLogs.gym_id,
                func.max(GymCallLogs.created_at).label('max_created')
            )
            .group_by(GymCallLogs.gym_id)
        )

        if city:
            city_clean = city.strip()
            latest_log_subquery = latest_log_subquery.join(
                GymDatabase,
                GymCallLogs.gym_id == GymDatabase.id
            ).where(GymDatabase.city == city_clean)

        latest_log_subquery = latest_log_subquery.subquery()

        call_logs_query = (
            select(
                GymCallLogs.gym_id,
                GymCallLogs.telecaller_id,
                GymCallLogs.call_status,
                GymCallLogs.created_at,
                GymDatabase.city,
                GymDatabase.area,
                Telecaller.name.label('telecaller_name'),
                Telecaller.manager_id,
            )
            .join(GymDatabase, GymCallLogs.gym_id == GymDatabase.id)
            .join(
                latest_log_subquery,
                and_(
                    GymCallLogs.gym_id == latest_log_subquery.c.gym_id,
                    GymCallLogs.created_at == latest_log_subquery.c.max_created
                )
            )
            .join(Telecaller, GymCallLogs.telecaller_id == Telecaller.id)
        )

        call_result = await db.execute(call_logs_query)
        call_log_assignments = {row.gym_id: row for row in call_result.all()}

        # ============================================================================
        # STEP 2: Get active assignments from gym_assignment table in ONE query
        # Only for gyms NOT already in call_logs
        # ============================================================================

        assignment_query = (
            select(
                GymAssignment.gym_id,
                GymAssignment.telecaller_id,
                GymAssignment.assigned_at,
                GymAssignment.target_date,
                GymAssignment.manager_id,
                GymDatabase.city,
                GymDatabase.area,
                Telecaller.name.label('telecaller_name')
            )
            .join(GymDatabase, GymAssignment.gym_id == GymDatabase.id)
            .join(Telecaller, GymAssignment.telecaller_id == Telecaller.id)
            .where(GymAssignment.status == 'active')
        )

        # Filter out gyms that are already in call_logs
        if call_log_assignments:
            assignment_query = assignment_query.where(
                not_(GymAssignment.gym_id.in_(call_log_assignments.keys()))
            )

        if city:
            assignment_query = assignment_query.where(GymDatabase.city == city_clean)

        assignment_result = await db.execute(assignment_query)
        gym_assignments = {row.gym_id: row for row in assignment_result.all()}

        # ============================================================================
        # STEP 3: Build unified assignments list
        # Priority: call_logs > gym_assignment
        # ============================================================================

        assignments = []
        assigned_gym_ids = set()

        # Add from call_logs (higher priority)
        for gym_id, row in call_log_assignments.items():
            assigned_gym_ids.add(gym_id)
            assignments.append({
                'gym_id': gym_id,
                'telecaller_id': row.telecaller_id,
                'telecaller_name': row.telecaller_name,
                'manager_id': row.manager_id,
                'assigned_at': row.created_at.isoformat() if row.created_at else None,
                'target_date': None,
                'status': 'active',
                'current_call_status': row.call_status,
                'source': 'gym_call_logs',
                'city': row.city,
                'area': row.area
            })

        # Add from gym_assignment (only if not in call_logs)
        for gym_id, row in gym_assignments.items():
            assigned_gym_ids.add(gym_id)
            assignments.append({
                'gym_id': gym_id,
                'telecaller_id': row.telecaller_id,
                'telecaller_name': row.telecaller_name,
                'manager_id': row.manager_id,
                'assigned_at': row.assigned_at.isoformat() if row.assigned_at else None,
                'target_date': row.target_date.isoformat() if row.target_date else None,
                'status': 'active',
                'current_call_status': 'pending',
                'source': 'gym_assignment',
                'city': row.city,
                'area': row.area
            })

        # If city is specified, calculate fully assigned areas
        fully_assigned_areas = []
        if city:
            # Group gyms by area and check if all are assigned
            area_stats = {}

            # Get all gyms in this city grouped by area
            gyms_by_area_query = (
                select(
                    GymDatabase.area,
                    func.count(GymDatabase.id).label('total_gyms')
                )
                .where(GymDatabase.city == city_clean)
                .where(GymDatabase.area.isnot(None))
                .where(GymDatabase.area != '')
                .group_by(GymDatabase.area)
            )
            gyms_by_area_result = await db.execute(gyms_by_area_query)
            area_stats = {row.area: {'total': row.total_gyms, 'assigned': 0} for row in gyms_by_area_result.all()}

            # Count assigned gyms per area
            for assignment in assignments:
                area = assignment.get('area')
                if area and area in area_stats:
                    area_stats[area]['assigned'] += 1

            # Find fully assigned areas
            fully_assigned_areas = [
                area for area, stats in area_stats.items()
                if stats['total'] > 0 and stats['total'] == stats['assigned']
            ]

        return {
            "assignments": assignments,
            "total": len(assignments),
            "fully_assigned_areas": fully_assigned_areas
        }

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to check assignments: {str(e)}"
        )


# ============================================================================
# Optimized Locations Endpoint with Bulk Queries
# ============================================================================

@router.get("/locations/all-optimized")
async def get_locations_optimized(
    manager: Manager = Depends(get_current_manager),
    db: AsyncSession = Depends(get_async_db)
):
    """
    Get all cities and areas with statistics in optimized bulk queries.

    This replaces the original locations/all endpoint which had N+1 queries.
    All statistics are calculated using aggregate functions and subqueries.
    """
    try:
        # ============================================================================
        # STEP 1: Get all unique cities and their areas in ONE query
        # ============================================================================

        locations_query = (
            select(
                GymDatabase.city,
                GymDatabase.area
            )
            .where(GymDatabase.city.isnot(None))
            .where(GymDatabase.city != '')
            .distinct()
            .order_by(GymDatabase.city, GymDatabase.area)
        )

        result = await db.execute(locations_query)
        locations = result.all()

        # Build city -> areas map
        city_areas_map: Dict[str, set] = {}
        all_cities = set()

        for city, area in locations:
            if city:
                city_clean = city.strip()
                all_cities.add(city_clean)
                if city_clean not in city_areas_map:
                    city_areas_map[city_clean] = set()
                if area:
                    area_clean = area.strip()
                    if area_clean:
                        city_areas_map[city_clean].add(area_clean)

        # Convert sets to sorted lists
        cities_array = sorted(list(all_cities))
        city_areas_array = {
            city: sorted(list(areas))
            for city, areas in city_areas_map.items()
        }

        # ============================================================================
        # STEP 2: Calculate area statistics using bulk aggregate queries
        # ============================================================================

        # Get total gyms per area (one query for all areas)
        total_gyms_query = (
            select(
                GymDatabase.city,
                GymDatabase.area,
                func.count(GymDatabase.id).label('total')
            )
            .where(GymDatabase.city.isnot(None))
            .where(GymDatabase.area.isnot(None))
            .where(GymDatabase.area != '')
            .group_by(GymDatabase.city, GymDatabase.area)
        )
        total_result = await db.execute(total_gyms_query)
        total_gyms_map = {
            (f"{row.city.strip()}__{row.area.strip()}"): row.total
            for row in total_result.all()
        }

        # Get prime gym areas (one query)
        prime_areas_query = (
            select(
                GymDatabase.city,
                GymDatabase.area
            )
            .where(GymDatabase.isprime == 1)
            .where(GymDatabase.city.isnot(None))
            .where(GymDatabase.area.isnot(None))
            .distinct()
        )
        prime_result = await db.execute(prime_areas_query)
        prime_areas = {
            f"{row.city.strip()}__{row.area.strip()}": True
            for row in prime_result.all()
        }

        # Get converted gyms using latest call log (subquery approach)
        latest_log_subquery = (
            select(
                GymCallLogs.gym_id,
                func.max(GymCallLogs.created_at).label('max_created')
            )
            .group_by(GymCallLogs.gym_id)
            .subquery()
        )

        converted_gyms_query = (
            select(
                GymDatabase.city,
                GymDatabase.area,
                func.count(GymCallLogs.gym_id).label('converted_count')
            )
            .join(
                latest_log_subquery,
                and_(
                    GymCallLogs.gym_id == latest_log_subquery.c.gym_id,
                    GymCallLogs.created_at == latest_log_subquery.c.max_created
                )
            )
            .join(
                GymDatabase,
                GymCallLogs.gym_id == GymDatabase.id
            )
            .where(GymCallLogs.call_status == 'converted')
            .where(GymDatabase.city.isnot(None))
            .where(GymDatabase.area.isnot(None))
            .group_by(GymDatabase.city, GymDatabase.area)
        )
        converted_result = await db.execute(converted_gyms_query)
        converted_gyms_map = {
            (f"{row.city.strip()}__{row.area.strip()}"): row.converted_count
            for row in converted_result.all()
        }

        # ============================================================================
        # STEP 3: Build area_stats map
        # ============================================================================

        area_stats = {}
        for city, areas in city_areas_map.items():
            for area in areas:
                key = f"{city}__{area}"
                area_stats[key] = {
                    "total": total_gyms_map.get(key, 0),
                    "converted": converted_gyms_map.get(key, 0),
                    "is_prime": key in prime_areas
                }

        return {
            "cities": cities_array,
            "city_areas": city_areas_array,
            "area_stats": area_stats
        }

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch locations: {str(e)}"
        )
