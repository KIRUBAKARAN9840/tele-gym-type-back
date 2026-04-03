# Backend Implementation for Gym Stats API
from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import func, or_, and_, desc, asc, case, select, distinct, String, cast
from typing import Optional, List
from app.models.fittbot_models import (
    Gym, GymOwner, Client, ClientFittbotAccess, TrainerProfile,
    FittbotPlans, GymPhoto, GymPlans, GymStudiosPic, GymOnboardingPics,
    SessionSetting, ClassSession, FittbotGymMembership
)
from app.models.marketingmodels import GymDatabase
from app.models.dailypass_models import DailyPassPricing
from app.models.async_database import get_async_db
import math
import io
import pandas as pd
from datetime import datetime

router = APIRouter(prefix="/api/admin/gym-stats", tags=["AdminGymStats"])

# Pydantic models for response
class GymStatsResponse(BaseModel):
    gym_id: int
    gym_name: str
    owner_name: str
    contact_number: str
    location: str
    total_clients: int
    active_clients: int
    retention_rate: float
    registered_users: int
    status: str
    created_at: str
    referal_id: Optional[str] = None
    fittbot_verified: bool = False

    class Config:
        from_attributes = True

class PaginatedGymsResponse(BaseModel):
    gyms: List[GymStatsResponse]
    total: int
    page: int
    limit: int
    totalPages: int
    hasNext: bool
    hasPrev: bool
    unverified_gyms_count: int

class GymStatsSummary(BaseModel):
    total_gyms: int
    active_gyms: int
    inactive_gyms: int
    total_clients_across_all_gyms: int
    average_retention_rate: float
    unverified_gyms_count: int

class GymUpdateRequest(BaseModel):
    name: Optional[str] = None
    location: Optional[str] = None
    max_clients: Optional[int] = None
    referal_id: Optional[str] = None
    fittbot_verified: Optional[bool] = None
    owner_contact_number: Optional[str] = None

def apply_client_range_filter_having(having_clause, client_count_column, range_value):
    """Apply client count range filter to HAVING clause"""
    if range_value == "0-50":
        return and_(having_clause, client_count_column >= 0, client_count_column <= 50)
    elif range_value == "51-100":
        return and_(having_clause, client_count_column >= 51, client_count_column <= 100)
    elif range_value == "101-150":
        return and_(having_clause, client_count_column >= 101, client_count_column <= 150)
    elif range_value == "151-200":
        return and_(having_clause, client_count_column >= 151, client_count_column <= 200)
    elif range_value == ">200":
        return and_(having_clause, client_count_column > 200)
    return having_clause

async def get_gym_stats_query(db: AsyncSession, verified_only=None):
    """Base query for gym stats with client counts

    Args:
        verified_only: None (all gyms), True (verified only), False (unverified only)
    """
    # Build the base query with all necessary joins and aggregations
    stmt = select(
        Gym.gym_id,
        Gym.name.label('gym_name'),
        Gym.location,
        Gym.created_at,
        Gym.referal_id,
        Gym.fittbot_verified,
        GymOwner.name.label('owner_name'),
        GymOwner.contact_number,
        Gym.door_no,
        Gym.building,
        Gym.street,
        Gym.area,
        Gym.city,
        Gym.state,
        Gym.pincode,
        func.count(func.distinct(Client.client_id)).label('registered_users'),
        func.count(Client.client_id).label('total_clients'),
        func.sum(case(
            (ClientFittbotAccess.access_status == 'active', 1),
            else_=0
        )).label('active_clients'),
        case(
            (func.count(Client.client_id) > 0,
             func.round(
                 (func.sum(case((ClientFittbotAccess.access_status == 'active', 1), else_=0)) * 100.0) /
                 func.count(Client.client_id), 2
             )),
            else_=0
        ).label('retention_rate'),
        case(
            (func.sum(case((ClientFittbotAccess.access_status == 'active', 1), else_=0)) > 0, 'active'),
            else_='inactive'
        ).label('status')
    ).outerjoin(
        GymOwner, Gym.owner_id == GymOwner.owner_id
    ).outerjoin(
        Client, Gym.gym_id == Client.gym_id
    ).outerjoin(
        ClientFittbotAccess, Client.client_id == ClientFittbotAccess.client_id
    ).group_by(
        Gym.gym_id,
        Gym.name,
        Gym.location,
        Gym.created_at,
        Gym.referal_id,
        Gym.fittbot_verified,
        GymOwner.name,
        GymOwner.contact_number,
        Gym.door_no,
        Gym.building,
        Gym.street,
        Gym.area,
        Gym.city,
        Gym.state,
        Gym.pincode
    )

    # Apply verification filter only if specified
    if verified_only is True:
        stmt = stmt.where(Gym.fittbot_verified == True)
    elif verified_only is False:
        stmt = stmt.where(Gym.fittbot_verified == False)

    return stmt

@router.get("")
@router.get("/")
async def get_gym_stats(
    page: int = Query(1, ge=1, description="Page number"),
    limit: int = Query(10, ge=1, le=100, description="Items per page"),
    search: Optional[str] = Query(None, description="Search by gym name, owner, mobile, or location"),
    status: Optional[str] = Query(None, description="Filter by gym status"),
    total_clients_range: Optional[str] = Query(None, description="Filter by total clients range"),
    active_clients_range: Optional[str] = Query(None, description="Filter by active clients range"),
    sort_order: str = Query("desc", description="Sort order for created_at"),
    has_session_plans: Optional[bool] = Query(None, description="Filter by session plans existence"),
    has_membership_plans: Optional[bool] = Query(None, description="Filter by membership plans existence"),
    has_daily_pass: Optional[bool] = Query(None, description="Filter by daily pass pricing existence"),
    price_sort: Optional[str] = Query(None, description="Sort by session price"),
    registered_users_filter: Optional[str] = Query(None, description="Filter by registered users count (e.g., '50', '100', '150')"),
    city: Optional[str] = Query(None, description="Filter by city"),
    state: Optional[str] = Query(None, description="Filter by state"),
    db: AsyncSession = Depends(get_async_db)
):
    """
    Optimized endpoint for fetching gym statistics with proper backend pagination.

    OPTIMIZATIONS:
    - Uses subqueries instead of N+1 queries for plan type checks
    - Pagination happens at database level (not in-memory)
    - Only fetches fields needed by the frontend
    - Search filtering is done at database level
    """
    try:
        # Get count of unverified gyms (separate query for stats)
        unverified_stmt = select(func.count()).select_from(Gym).where(Gym.fittbot_verified == False)
        unverified_result = await db.execute(unverified_stmt)
        unverified_count = unverified_result.scalar() or 0

        # Build subqueries for plan type existence checks (eliminates N+1 queries)
        session_exists_subq = select(1).where(
            SessionSetting.gym_id == Gym.gym_id
        ).correlate(Gym).exists()

        membership_exists_subq = select(1).where(
            GymPlans.gym_id == Gym.gym_id
        ).correlate(Gym).exists()

        daily_pass_exists_subq = select(1).where(
            DailyPassPricing.gym_id == cast(Gym.gym_id, String)
        ).correlate(Gym).exists()

        base_stmt = select(
            Gym.gym_id,
            Gym.name.label('gym_name'),
            Gym.location,
            Gym.created_at,
            Gym.fittbot_verified,
            GymOwner.name.label('owner_name'),
            GymOwner.contact_number,
            Gym.door_no,
            Gym.street,
            Gym.building,
            Gym.area,
            Gym.city,
            Gym.state,
            Gym.pincode,
            # Count distinct clients for registered_users (what frontend displays)
            func.count(func.distinct(Client.client_id)).label('registered_users'),
            # Plan type existence flags (from subqueries - no N+1!)
            session_exists_subq.label('has_session_plans'),
            membership_exists_subq.label('has_membership_plans'),
            daily_pass_exists_subq.label('has_daily_pass')
        ).outerjoin(
            GymOwner, Gym.owner_id == GymOwner.owner_id
        ).outerjoin(
            Client, Gym.gym_id == Client.gym_id
        ).group_by(
            Gym.gym_id,
            Gym.name,
            Gym.location,
            Gym.created_at,
            Gym.fittbot_verified,
            GymOwner.name,
            GymOwner.contact_number,
            Gym.door_no,
            Gym.street,
            Gym.building,
            Gym.area,
            Gym.city,
            Gym.state,
            Gym.pincode
        )

        # Apply search filter at database level (not in-memory)
        if search:
            search_term = f"%{search.lower()}%"
            base_stmt = base_stmt.where(
                or_(
                    func.lower(Gym.name).like(search_term),
                    func.lower(GymOwner.name).like(search_term),
                    GymOwner.contact_number.like(search_term),
                    func.lower(Gym.location).like(search_term),
                    func.lower(Gym.city).like(search_term)
                )
            )

        # Apply plan type filters using the subquery flags
        if has_session_plans is not None:
            base_stmt = base_stmt.having(session_exists_subq == has_session_plans)
        if has_membership_plans is not None:
            base_stmt = base_stmt.having(membership_exists_subq == has_membership_plans)
        if has_daily_pass is not None:
            base_stmt = base_stmt.having(daily_pass_exists_subq == has_daily_pass)

        # Apply registered users filter (from frontend dropdown: >50, >100, etc.)
        if registered_users_filter:
            try:
                min_users = int(registered_users_filter)
                base_stmt = base_stmt.having(
                    func.count(func.distinct(Client.client_id)) > min_users
                )
            except (ValueError, TypeError):
                pass  # Invalid filter, ignore it

        # Apply city filter — use TRIM + LOWER on both sides to handle spaces in DB values
        if city:
            base_stmt = base_stmt.where(
                func.lower(func.trim(Gym.city)) == city.strip().lower()
            )

        # Apply state filter — use TRIM + LOWER on both sides to handle spaces in DB values
        if state:
            base_stmt = base_stmt.where(
                func.lower(func.trim(Gym.state)) == state.strip().lower()
            )

        # Apply sorting
        if sort_order == "asc":
            base_stmt = base_stmt.order_by(asc(Gym.created_at))
        else:
            base_stmt = base_stmt.order_by(desc(Gym.created_at))

        # IMPORTANT: Get total count BEFORE pagination (for pagination info)
        # Use a subquery to count the filtered results
        count_stmt = select(func.count()).select_from(base_stmt.subquery())
        count_result = await db.execute(count_stmt)
        total_count = count_result.scalar() or 0

        # Apply pagination at database level
        offset = (page - 1) * limit
        base_stmt = base_stmt.offset(offset).limit(limit)

        # Execute the optimized query
        result = await db.execute(base_stmt)
        gyms_data = result.all()

        # Fetch distinct cities and states in two single async queries (no loops, no N+1)
        cities_stmt = (
            select(Gym.city)
            .where(Gym.city.isnot(None))
            .where(Gym.city != "")
            .where(Gym.city != "-")
            .where(Gym.city != "N/A")
            .distinct()
            .order_by(Gym.city)
        )
        cities_result = await db.execute(cities_stmt)
        all_cities = [row[0].strip() for row in cities_result.all() if row[0] and row[0].strip()]

        states_stmt = (
            select(Gym.state)
            .where(Gym.state.isnot(None))
            .where(Gym.state != "")
            .where(Gym.state != "-")
            .where(Gym.state != "N/A")
            .distinct()
            .order_by(Gym.state)
        )
        states_result = await db.execute(states_stmt)
        all_states = [row[0].strip() for row in states_result.all() if row[0] and row[0].strip()]

        # Build response list (only needed fields)
        gyms = []
        for row in gyms_data:
            gyms.append({
                "gym_id": row.gym_id,
                "gym_name": row.gym_name,
                "owner_name": row.owner_name or "N/A",
                "contact_number": row.contact_number or "N/A",
                "location": row.city or row.location or "N/A",
                "registered_users": row.registered_users or 0,
                "created_at": row.created_at.isoformat() if row.created_at else None,
                "fittbot_verified": row.fittbot_verified,
                "street_address": ", ".join(filter(None, [row.door_no, row.building, row.street, row.area, row.city, row.state, str(row.pincode) if row.pincode else None])) or "-",
                # Plan type flags
                "has_session_plans": row.has_session_plans or False,
                "has_membership_plans": row.has_membership_plans or False,
                "has_daily_pass": row.has_daily_pass or False,
            })

        # Calculate pagination info
        total_pages = math.ceil(total_count / limit)
        has_next = page < total_pages
        has_prev = page > 1

        return {
            "success": True,
            "data": {
                "gyms": gyms,
                "total": total_count,
                "page": page,
                "limit": limit,
                "totalPages": total_pages,
                "hasNext": has_next,
                "hasPrev": has_prev,
                "unverified_gyms_count": unverified_count,
                "cities": all_cities,
                "states": all_states
            },
            "message": "Gym statistics fetched successfully"
        }

    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error fetching gym statistics: {str(e)}")

@router.get("/summary")
async def get_gym_stats_summary(db: AsyncSession = Depends(get_async_db)):
    """Get overall gym statistics summary"""
    try:
        # Basic gym counts
        total_gyms_stmt = select(func.count()).select_from(Gym).where(Gym.fittbot_verified == True)
        total_gyms_result = await db.execute(total_gyms_stmt)
        total_gyms = total_gyms_result.scalar() or 0

        unverified_gyms_stmt = select(func.count()).select_from(Gym).where(Gym.fittbot_verified == False)
        unverified_gyms_result = await db.execute(unverified_gyms_stmt)
        unverified_gyms_count = unverified_gyms_result.scalar() or 0

        # Get gym stats for verified gyms
        stmt = await get_gym_stats_query(db, verified_only=True)
        result = await db.execute(stmt)
        gym_stats = result.all()

        active_gyms = sum(1 for gym in gym_stats if (gym.active_clients or 0) > 0)
        inactive_gyms = total_gyms - active_gyms

        total_clients_across_all_gyms = sum(gym.total_clients for gym in gym_stats)

        # Calculate average retention rate (only for gyms with active clients)
        gyms_with_clients = [gym for gym in gym_stats if (gym.active_clients or 0) > 0]
        average_retention_rate = (
            sum(gym.retention_rate for gym in gyms_with_clients) / len(gyms_with_clients)
            if gyms_with_clients else 0
        )

        return {
            "success": True,
            "data": {
                "total_gyms": total_gyms,
                "active_gyms": active_gyms,
                "inactive_gyms": inactive_gyms,
                "total_clients_across_all_gyms": total_clients_across_all_gyms,
                "average_retention_rate": round(average_retention_rate, 2),
                "unverified_gyms_count": unverified_gyms_count
            },
            "message": "Gym statistics summary fetched successfully"
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching gym summary: {str(e)}")



@router.get("/unverified")
async def get_unverified_gyms(
    page: int = Query(1, ge=1, description="Page number"),
    limit: int = Query(10, ge=1, le=100, description="Items per page"),
    search: Optional[str] = Query(None, description="Search by gym name, owner, mobile, or location"),
    status: Optional[str] = Query(None, description="Filter by gym status"),
    total_clients_range: Optional[str] = Query(None, description="Filter by total clients range"),
    active_clients_range: Optional[str] = Query(None, description="Filter by active clients range"),
    sort_order: str = Query("desc", description="Sort order for created_at"),
    db: AsyncSession = Depends(get_async_db)
):
    """Get unverified gyms with search and pagination"""
    try:
        # Base query for unverified gyms
        stmt = await get_gym_stats_query(db, verified_only=False)

        # Apply sorting
        if sort_order == "asc":
            stmt = stmt.order_by(asc(Gym.created_at))
        else:
            stmt = stmt.order_by(desc(Gym.created_at))

        # Execute query to get results
        result = await db.execute(stmt)
        all_results = result.all()

        # Apply filters
        filtered_gyms = []
        for result in all_results:
            gym_id_for_query = result.gym_id

            # Check if gym has session plans
            session_stmt = select(SessionSetting).where(SessionSetting.gym_id == gym_id_for_query).limit(1)
            session_result = await db.execute(session_stmt)
            has_session_plans = session_result.first() is not None

            # Check if gym has membership plans
            membership_stmt = select(GymPlans).where(GymPlans.gym_id == gym_id_for_query).limit(1)
            membership_result = await db.execute(membership_stmt)
            has_membership_plans = membership_result.first() is not None

            # Check if gym has daily pass pricing
            daily_pass_stmt = select(DailyPassPricing).where(
                DailyPassPricing.gym_id == str(result.gym_id)
            ).limit(1)
            daily_pass_result = await db.execute(daily_pass_stmt)
            has_daily_pass = daily_pass_result.first() is not None

            gym_data = {
                "gym_id": result.gym_id,
                "gym_name": result.gym_name,
                "owner_name": result.owner_name or "N/A",
                "contact_number": result.contact_number or "N/A",
                "location": result.location or "N/A",
                "total_clients": result.total_clients or 0,
                "active_clients": result.active_clients or 0,
                "retention_rate": float(result.retention_rate or 0),
                "registered_users": result.registered_users or 0,
                "status": result.status,
                "created_at": result.created_at.isoformat() if result.created_at else None,
                "referal_id": result.referal_id,
                "fittbot_verified": result.fittbot_verified,
                "has_session_plans": has_session_plans,
                "has_membership_plans": has_membership_plans,
                "has_daily_pass": has_daily_pass
            }

            # Apply search filter
            if search:
                search_term = search.lower()
                searchable_text = f"{result.gym_name or ''} {result.owner_name or ''} {result.contact_number or ''} {result.location or ''}".lower()
                if search_term not in searchable_text:
                    continue

            # Apply total clients range filter
            if total_clients_range and total_clients_range != "all":
                client_count = result.total_clients or 0
                if total_clients_range == "0-50" and not (0 <= client_count <= 50):
                    continue
                elif total_clients_range == "51-100" and not (51 <= client_count <= 100):
                    continue
                elif total_clients_range == "101-150" and not (101 <= client_count <= 150):
                    continue
                elif total_clients_range == "151-200" and not (151 <= client_count <= 200):
                    continue
                elif total_clients_range == ">200" and not (client_count > 200):
                    continue

            # Apply active clients range filter
            if active_clients_range and active_clients_range != "all":
                active_client_count = result.active_clients or 0
                if active_clients_range == "0-50" and not (0 <= active_client_count <= 50):
                    continue
                elif active_clients_range == "51-100" and not (51 <= active_client_count <= 100):
                    continue
                elif active_clients_range == "101-150" and not (101 <= active_client_count <= 150):
                    continue
                elif active_clients_range == "151-200" and not (151 <= active_client_count <= 200):
                    continue
                elif active_clients_range == ">200" and not (active_client_count > 200):
                    continue

            # Apply status filter
            if status and status != "all":
                active_count = result.active_clients or 0
                if status == "active" and active_count == 0:
                    continue
                elif status == "inactive" and active_count > 0:
                    continue

            filtered_gyms.append(gym_data)

        total_count = len(filtered_gyms)

        # Apply pagination
        start_idx = (page - 1) * limit
        end_idx = start_idx + limit
        paginated_gyms = filtered_gyms[start_idx:end_idx]

        # Calculate pagination info
        total_pages = math.ceil(total_count / limit)
        has_next = page < total_pages
        has_prev = page > 1

        return {
            "success": True,
            "data": {
                "gyms": paginated_gyms,
                "total": total_count,
                "page": page,
                "limit": limit,
                "totalPages": total_pages,
                "hasNext": has_next,
                "hasPrev": has_prev
            },
            "message": "Unverified gyms fetched successfully"
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching unverified gyms: {str(e)}")

@router.get("/photos")
async def get_gyms_by_photo_status(
    photo_type: Optional[str] = Query(None, description="Photo status: studio, onboard, or noUploads"),
    page: int = Query(1, ge=1, description="Page number"),
    limit: int = Query(10, ge=1, le=100, description="Items per page"),
    search: Optional[str] = Query(None, description="Search by gym name, owner, mobile, or city"),
    sort_order: str = Query("desc", description="Sort order for created_at"),
    db: AsyncSession = Depends(get_async_db)
):

    try:
        # Validate photo_type
        if not photo_type:
            raise HTTPException(
                status_code=400,
                detail="photo_type parameter is required. Must be 'studio', 'onboard', or 'noUploads'"
            )

        if photo_type not in ["studio", "onboard", "noUploads"]:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid photo_type. Must be 'studio', 'onboard', or 'noUploads'"
            )

        # Validate sort_order
        if sort_order not in ["asc", "desc"]:
            sort_order = "desc"

        # Get all gym IDs with studio photos
        studio_stmt = select(GymStudiosPic.gym_id)
        studio_result = await db.execute(studio_stmt)
        gym_ids_with_studio = set([row[0] for row in studio_result.all()])

        # Get all gym IDs with onboarding photos
        onboard_stmt = select(GymOnboardingPics.gym_id)
        onboard_result = await db.execute(onboard_stmt)
        gym_ids_with_onboarding = set([row[0] for row in onboard_result.all()])

        # Mutually exclusive categorization (matches dashboard logic)
        # studio: gyms with studio photos (priority)
        # onboard: gyms with ONLY onboarding photos (no studio photos)
        # noUploads: gyms with neither studio nor onboarding photos
        if photo_type == "studio":
            target_gym_ids = gym_ids_with_studio
        elif photo_type == "onboard":
            # Only onboarding photos (exclude those with studio)
            target_gym_ids = gym_ids_with_onboarding - gym_ids_with_studio
        elif photo_type == "noUploads":
            # Get all gym IDs first
            all_gyms_stmt = select(Gym.gym_id)
            all_gyms_result = await db.execute(all_gyms_stmt)
            all_gym_ids = set([row[0] for row in all_gyms_result.all()])
            # Gyms with neither studio nor onboarding
            gym_ids_with_any_photos = gym_ids_with_studio.union(gym_ids_with_onboarding)
            target_gym_ids = all_gym_ids - gym_ids_with_any_photos
        else:
            target_gym_ids = set()

        # Build base query with gym info and owner info
        stmt = select(
            Gym.gym_id,
            Gym.name.label('gym_name'),
            Gym.location,
            Gym.created_at,
            Gym.fittbot_verified,
            GymOwner.name.label('owner_name'),
            GymOwner.contact_number,
            Gym.door_no,
            Gym.building,
            Gym.street,
            Gym.area,
            Gym.city,
            Gym.state,
            Gym.pincode
        ).outerjoin(
            GymOwner, Gym.owner_id == GymOwner.owner_id
        ).where(
            Gym.gym_id.in_(target_gym_ids) if target_gym_ids else False
        )

        # Apply search filter
        if search and search.strip():
            search_term = f"%{search.strip()}%"
            stmt = stmt.where(
                or_(
                    Gym.name.ilike(search_term),
                    GymOwner.name.ilike(search_term),
                    GymOwner.contact_number.ilike(search_term),
                    Gym.location.ilike(search_term),
                    Gym.city.ilike(search_term),
                    Gym.area.ilike(search_term)
                )
            )

        # Apply sorting
        if sort_order == "asc":
            stmt = stmt.order_by(asc(Gym.created_at))
        else:
            stmt = stmt.order_by(desc(Gym.created_at))

        # Get total count
        count_stmt = select(func.count()).select_from(stmt.subquery())
        count_result = await db.execute(count_stmt)
        total_count = count_result.scalar() or 0

        # Apply pagination
        offset = (page - 1) * limit
        stmt = stmt.offset(offset).limit(limit)

        result = await db.execute(stmt)
        gyms = result.all()

        # Format results
        gym_list = []
        for gym in gyms:
            # Count registered users for this gym
            client_count_stmt = select(func.count()).select_from(Client).where(Client.gym_id == gym.gym_id)
            client_count_result = await db.execute(client_count_stmt)
            registered_users = client_count_result.scalar() or 0

            gym_list.append({
                "gym_id": gym.gym_id,
                "gym_name": gym.gym_name or "-",
                "owner_name": gym.owner_name or "-",
                "contact_number": gym.contact_number or "-",
                "location": gym.city or "-",
                "street_address": ", ".join(filter(None, [gym.door_no, gym.building, gym.street, gym.area, gym.city, gym.state, str(gym.pincode) if gym.pincode else None])) or "-",
                "registered_users": registered_users,
                "fittbot_verified": gym.fittbot_verified or False,
                "created_at": gym.created_at.isoformat() if gym.created_at else None,
                "photo_status": photo_type
            })

        # Calculate pagination info
        total_pages = math.ceil(total_count / limit)
        has_next = page < total_pages
        has_prev = page > 1

        return {
            "success": True,
            "data": {
                "gyms": gym_list,
                "total": total_count,
                "page": page,
                "limit": limit,
                "totalPages": total_pages,
                "hasNext": has_next,
                "hasPrev": has_prev,
                "photoType": photo_type
            },
            "message": "Gyms fetched successfully"
        }

    except HTTPException:
        raise
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(
            status_code=500,
            detail=f"Error fetching gyms: {str(e)}"
        )

@router.get("/{gym_id}")
async def get_gym_by_id(gym_id: int, db: AsyncSession = Depends(get_async_db)):
    """Get specific gym details by ID"""
    try:
        stmt = await get_gym_stats_query(db, verified_only=False)
        stmt = stmt.where(Gym.gym_id == gym_id)

        result = await db.execute(stmt)
        gym_result = result.first()

        if not gym_result:
            raise HTTPException(status_code=404, detail="Gym not found")

        gym_data = {
            "gym_id": gym_result.gym_id,
            "gym_name": gym_result.gym_name,
            "owner_name": gym_result.owner_name or "N/A",
            "contact_number": gym_result.contact_number or "N/A",
            "location": gym_result.location or "N/A",
            "total_clients": gym_result.total_clients or 0,
            "active_clients": gym_result.active_clients or 0,
            "retention_rate": float(gym_result.retention_rate or 0),
            "registered_users": gym_result.registered_users or 0,
            "status": gym_result.status,
            "created_at": gym_result.created_at.isoformat() if gym_result.created_at else None,
            "referal_id": gym_result.referal_id,
            "fittbot_verified": gym_result.fittbot_verified
        }

        return {
            "success": True,
            "data": gym_data,
            "message": "Gym details fetched successfully"
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching gym details: {str(e)}")

@router.get("/{gym_id}/clients")
async def get_gym_clients(
    gym_id: int,
    page: int = Query(1, ge=1),
    limit: int = Query(10, ge=1, le=100),
    status: Optional[str] = Query(None, description="Filter by client access status"),
    db: AsyncSession = Depends(get_async_db)
):
    try:
        # Check if gym exists
        gym_stmt = select(Gym).where(Gym.gym_id == gym_id)
        gym_result = await db.execute(gym_stmt)
        gym = gym_result.scalar_one_or_none()

        if not gym:
            raise HTTPException(status_code=404, detail="Gym not found")

        stmt = select(
            Client.client_id,
            Client.name,
            Client.email,
            Client.contact,
            Client.created_at,
            ClientFittbotAccess.access_status
        ).where(
            Client.gym_id == gym_id
        ).outerjoin(
            ClientFittbotAccess, Client.client_id == ClientFittbotAccess.client_id
        )

        if status and status != "all":
            stmt = stmt.where(ClientFittbotAccess.access_status == status)

        stmt = stmt.order_by(desc(Client.created_at))

        # Get total count
        count_stmt = select(func.count()).select_from(stmt.subquery())
        count_result = await db.execute(count_stmt)
        total_count = count_result.scalar() or 0

        # Apply pagination
        offset = (page - 1) * limit
        stmt = stmt.offset(offset).limit(limit)

        result = await db.execute(stmt)
        clients = result.all()

        clients_data = []
        for client in clients:
            client_data = {
                "client_id": client.client_id,
                "name": client.name,
                "email": client.email,
                "contact": client.contact,
                "access_status": client.access_status or "inactive",
                "created_at": client.created_at.isoformat() if client.created_at else None
            }
            clients_data.append(client_data)

        total_pages = math.ceil(total_count / limit)

        return {
            "success": True,
            "data": {
                "gym_name": gym.name,
                "clients": clients_data,
                "total": total_count,
                "page": page,
                "limit": limit,
                "totalPages": total_pages,
                "hasNext": page < total_pages,
                "hasPrev": page > 1
            },
            "message": "Gym clients fetched successfully"
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching gym clients: {str(e)}")

@router.get("/{gym_id}/details")
async def get_gym_details(gym_id: int, db: AsyncSession = Depends(get_async_db)):
    """Get complete gym details including trainers, clients, plans, and photos"""
    try:
        # Get basic gym info
        gym_stmt = select(
            Gym.gym_id,
            Gym.name,
            Gym.location,
            Gym.max_clients,
            Gym.logo,
            Gym.cover_pic,
            Gym.subscription_start_date,
            Gym.subscription_end_date,
            Gym.created_at,
            Gym.updated_at,
            Gym.referal_id,
            Gym.fittbot_verified,
            GymOwner.name.label('owner_name'),
            GymOwner.email.label('owner_email'),
            GymOwner.contact_number.label('owner_contact'),
            GymOwner.profile.label('owner_profile')
        ).outerjoin(
            GymOwner, Gym.owner_id == GymOwner.owner_id
        ).where(Gym.gym_id == gym_id)

        gym_result = await db.execute(gym_stmt)
        gym = gym_result.first()

        if not gym:
            raise HTTPException(status_code=404, detail="Gym not found")

        # Get trainers
        trainer_stmt = select(
            TrainerProfile.profile_id,
            TrainerProfile.trainer_id,
            TrainerProfile.full_name,
            TrainerProfile.email,
            TrainerProfile.specializations,
            TrainerProfile.experience,
            TrainerProfile.certifications,
            TrainerProfile.work_timings
        ).where(TrainerProfile.gym_id == gym_id)

        trainer_result = await db.execute(trainer_stmt)
        trainers = trainer_result.all()

        # Get clients with their plans
        client_stmt = select(
            Client.client_id,
            Client.name,
            Client.email,
            Client.contact,
            Client.profile,
            Client.location,
            Client.lifestyle,
            Client.medical_issues,
            Client.created_at,
            ClientFittbotAccess.access_status,
            ClientFittbotAccess.plan,
            ClientFittbotAccess.paid_date,
            ClientFittbotAccess.start_date,
            ClientFittbotAccess.days_left,
            ClientFittbotAccess.free_trial,
            FittbotPlans.plan_name.label('fittbot_plan_name'),
            FittbotPlans.duration.label('fittbot_plan_duration'),
            FittbotPlans.image_url.label('fittbot_plan_image'),
            FittbotPlans.package_identifier.label('fittbot_plan_package')
        ).where(Client.gym_id == gym_id
        ).outerjoin(
            ClientFittbotAccess, Client.client_id == ClientFittbotAccess.client_id
        ).outerjoin(
            FittbotPlans, ClientFittbotAccess.fittbot_plan == FittbotPlans.id
        )

        client_result = await db.execute(client_stmt)
        clients_rows = client_result.all()

        # Get gym photos
        photo_stmt = select(
            GymPhoto.photo_id,
            GymPhoto.area_type,
            GymPhoto.image_url,
            GymPhoto.file_name,
            GymPhoto.file_size,
            GymPhoto.created_at
        ).where(GymPhoto.gym_id == gym_id)

        photo_result = await db.execute(photo_stmt)
        photos_rows = photo_result.all()

        # Get gym plans
        gym_plan_stmt = select(GymPlans).where(GymPlans.gym_id == gym_id)
        gym_plan_result = await db.execute(gym_plan_stmt)
        gym_plans = gym_plan_result.scalars().all()

        # Format response
        gym_details = {
            "gym_info": {
                "gym_id": gym.gym_id,
                "name": gym.name,
                "location": gym.location,
                "max_clients": gym.max_clients,
                "logo": gym.logo,
                "cover_pic": gym.cover_pic,
                "subscription_start_date": gym.subscription_start_date.isoformat() if gym.subscription_start_date else None,
                "subscription_end_date": gym.subscription_end_date.isoformat() if gym.subscription_end_date else None,
                "created_at": gym.created_at.isoformat() if gym.created_at else None,
                "updated_at": gym.updated_at.isoformat() if gym.updated_at else None,
                "referal_id": gym.referal_id,
                "fittbot_verified": gym.fittbot_verified,
                "owner_info": {
                    "name": gym.owner_name,
                    "email": gym.owner_email,
                    "contact_number": gym.owner_contact,
                    "profile": gym.owner_profile
                }
            },
            "trainers": [
                {
                    "profile_id": trainer.profile_id,
                    "trainer_id": trainer.trainer_id,
                    "full_name": trainer.full_name,
                    "email": trainer.email,
                    "specializations": trainer.specializations,
                    "experience": trainer.experience,
                    "certifications": trainer.certifications,
                    "work_timings": trainer.work_timings
                } for trainer in trainers
            ],
            "clients": [
                {
                    "client_id": client[0],
                    "name": client[1],
                    "email": client[2],
                    "contact": client[3],
                    "profile": client[4],
                    "location": client[5],
                    "lifestyle": client[6],
                    "medical_issues": client[7],
                    "created_at": client[8].isoformat() if client[8] else None,
                    "access_status": client[9],
                    "gym_plan": client[10],
                    "paid_date": client[11].isoformat() if client[11] else None,
                    "start_date": client[12].isoformat() if client[12] else None,
                    "days_left": client[13],
                    "free_trial": client[14],
                    "fittbot_plan": {
                        "plan_name": client[15],
                        "duration": client[16],
                        "image_url": client[17],
                        "package_identifier": client[18]
                    } if client[15] else None
                } for client in clients_rows
            ],
            "photos": [
                {
                    "photo_id": photo[0],
                    "area_type": photo[1],
                    "image_url": photo[2],
                    "file_name": photo[3],
                    "file_size": photo[4],
                    "created_at": photo[5].isoformat() if photo[5] else None
                } for photo in photos_rows
            ],
            "gym_plans": [
                {
                    "plan_id": plan.id,
                    "plan_name": plan.plans,
                    "amount": plan.amount,
                    "duration": plan.duration,
                    "description": plan.description
                } for plan in gym_plans
            ],
            "stats": {
                "total_trainers": len(trainers),
                "total_clients": len(clients_rows),
                "active_clients": len([c for c in clients_rows if c[9] == 'active']),
                "total_photos": len(photos_rows),
                "total_gym_plans": len(gym_plans)
            }
        }

        return {
            "success": True,
            "data": gym_details,
            "message": "Gym details fetched successfully"
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching gym details: {str(e)}")

@router.put("/{gym_id}")
async def update_gym(
    gym_id: int,
    update_data: GymUpdateRequest,
    db: AsyncSession = Depends(get_async_db)
):
    """Update gym details including basic info and verification status"""
    try:
        # Check if gym exists
        gym_stmt = select(Gym).where(Gym.gym_id == gym_id)
        gym_result = await db.execute(gym_stmt)
        gym = gym_result.scalar_one_or_none()

        if not gym:
            raise HTTPException(status_code=404, detail="Gym not found")

        # Update gym fields if provided
        from sqlalchemy import update
        update_values = {}
        if update_data.name is not None:
            update_values['name'] = update_data.name
        if update_data.location is not None:
            update_values['location'] = update_data.location
        if update_data.max_clients is not None:
            update_values['max_clients'] = update_data.max_clients
        if update_data.referal_id is not None:
            update_values['referal_id'] = update_data.referal_id
        if update_data.fittbot_verified is not None:
            update_values['fittbot_verified'] = update_data.fittbot_verified

        # Update gym if there are fields to update
        if update_values:
            update_stmt = update(Gym).where(Gym.gym_id == gym_id).values(**update_values)
            await db.execute(update_stmt)

        # Update owner contact number if provided
        if update_data.owner_contact_number is not None and gym.owner_id:
            owner_update_stmt = update(GymOwner).where(GymOwner.owner_id == gym.owner_id).values(
                contact_number=update_data.owner_contact_number
            )
            await db.execute(owner_update_stmt)

        await db.commit()

        # Fetch updated gym details
        updated_gym_stmt = select(
            Gym.gym_id,
            Gym.name,
            Gym.location,
            Gym.max_clients,
            Gym.referal_id,
            Gym.fittbot_verified,
            Gym.updated_at,
            GymOwner.name.label("owner_name"),
            GymOwner.contact_number.label("owner_contact")
        ).outerjoin(
            GymOwner, Gym.owner_id == GymOwner.owner_id
        ).where(Gym.gym_id == gym_id)

        updated_gym_result = await db.execute(updated_gym_stmt)
        updated_gym = updated_gym_result.first()

        return {
            "success": True,
            "data": {
                "gym_id": updated_gym.gym_id,
                "name": updated_gym.name,
                "location": updated_gym.location,
                "max_clients": updated_gym.max_clients,
                "referal_id": updated_gym.referal_id,
                "fittbot_verified": updated_gym.fittbot_verified,
                "updated_at": updated_gym.updated_at.isoformat() if updated_gym.updated_at else None,
                "owner_name": updated_gym.owner_name,
                "owner_contact": updated_gym.owner_contact
            },
            "message": "Gym details updated successfully"
        }

    except HTTPException:
        raise
    except Exception as e:
        await db.rollback()
        raise HTTPException(status_code=500, detail=f"Error updating gym: {str(e)}")

@router.get("/{gym_id}/session-plans")
async def get_gym_session_plans(gym_id: int, db: AsyncSession = Depends(get_async_db)):
    
    try:
        # Try to get gym from fittbot_local.gyms first
        gym_stmt = select(Gym).where(Gym.gym_id == gym_id)
        gym_result = await db.execute(gym_stmt)
        gym = gym_result.scalar_one_or_none()

        # Fallback: Try to get gym from marketing_latest.gym_database
        gym_from_marketing = None
        if not gym:
            marketing_gym_stmt = select(GymDatabase).where(GymDatabase.id == gym_id)
            marketing_gym_result = await db.execute(marketing_gym_stmt)
            gym_from_marketing = marketing_gym_result.scalar_one_or_none()

            if not gym_from_marketing:
                raise HTTPException(status_code=404, detail=f"Gym with ID {gym_id} not found in either fittbot_local.gyms or marketing_latest.gym_database")

        # Query session_settings joined with all_sessions to get session names
        # Wrap in try-except to handle cases where ClassSession might not exist
        session_plans = []
        try:
            session_stmt = select(
                SessionSetting.id.label('setting_id'),
                SessionSetting.session_id,
                SessionSetting.gym_id,
                SessionSetting.trainer_id,
                SessionSetting.is_enabled,
                SessionSetting.base_price,
                SessionSetting.discount_percent,
                SessionSetting.final_price,
                SessionSetting.capacity,
                SessionSetting.booking_lead_minutes,
                SessionSetting.cancellation_cutoff_minutes,
                ClassSession.name.label('session_name'),
                ClassSession.image.label('session_image'),
                ClassSession.description.label('session_description'),
                ClassSession.timing.label('session_timing'),
                ClassSession.internal.label('session_internal')
            ).join(
                ClassSession, SessionSetting.session_id == ClassSession.id
            ).where(
                SessionSetting.gym_id == gym_id
            )

            session_result = await db.execute(session_stmt)
            session_plans = session_result.all()
        except Exception as session_error:
            # Continue without session plans
            session_plans = []

        # Format session plans response
        plans = []
        for plan in session_plans:
            plan_data = {
                "setting_id": plan[0],
                "session_id": plan[1],
                "gym_id": plan[2],
                "trainer_id": plan[3],
                "is_enabled": plan[4],
                "base_price": plan[5],
                "discount_percent": plan[6],
                "final_price": plan[7],
                "capacity": plan[8],
                "booking_lead_minutes": plan[9],
                "cancellation_cutoff_minutes": plan[10],
                "session_name": plan[11],
                "session_image": plan[12],
                "session_description": plan[13],
                "session_timing": plan[14],
                "session_internal": plan[15]
            }
            plans.append(plan_data)

        # Query gym membership plans from gym_plans table
        membership_stmt = select(GymPlans).where(GymPlans.gym_id == gym_id)
        membership_result = await db.execute(membership_stmt)
        membership_plans = membership_result.scalars().all()

        # Format membership plans response
        gym_membership_plans = []
        for plan in membership_plans:
            # Calculate discount percentage if original amount exists
            discount_percent = None
            if plan.original_amount and plan.original_amount > plan.amount:
                discount_percent = round(((plan.original_amount - plan.amount) / plan.original_amount) * 100)

            plan_data = {
                "id": plan.id,
                "plan_name": plan.plans,
                "amount": plan.amount,
                "original_amount": plan.original_amount,
                "discount_percent": discount_percent,
                "duration": plan.duration,
                "description": plan.description,
                "services": plan.services,
                "personal_training": plan.personal_training,
                "bonus": plan.bonus,
                "pause": plan.pause,
                "bonus_type": plan.bonus_type,
                "plan_for": plan.plan_for
            }
            gym_membership_plans.append(plan_data)

        # Query daily pass pricing for this gym
        daily_pass_stmt = select(DailyPassPricing).where(
            DailyPassPricing.gym_id == str(gym_id)
        )
        daily_pass_result = await db.execute(daily_pass_stmt)
        daily_pass_pricing = daily_pass_result.scalar_one_or_none()

        daily_pass_data = None
        if daily_pass_pricing:
            # Calculate discount percentage if not stored
            discount_percent = daily_pass_pricing.discount_percentage
            if discount_percent is None and daily_pass_pricing.price and daily_pass_pricing.discount_price:
                discount_percent = round(((daily_pass_pricing.price - daily_pass_pricing.discount_price) / daily_pass_pricing.price) * 100, 2)

            # Convert daily pass prices from paisa to rupees (divide by 100)
            daily_pass_data = {
                "price": daily_pass_pricing.price / 100 if daily_pass_pricing.price else None,
                "discount_price": daily_pass_pricing.discount_price / 100 if daily_pass_pricing.discount_price else None,
                "discount_percentage": discount_percent
            }

        # Query gym studio pictures
        # Logic: First check gym_studios_pic, if pics exist use those (Studio tag)
        # If no pics in gym_studios_pic, check gym_onboarding_pics (Onboarding tag)
        studio_pics_data = []

        try:
            # First check gym_studios_pic table
            studio_stmt = select(GymStudiosPic).where(GymStudiosPic.gym_id == gym_id)
            studio_result = await db.execute(studio_stmt)
            gym_studio_pics = studio_result.scalars().all()

            if gym_studio_pics and len(list(gym_studio_pics)) > 0:
                # Use gym_studios_pic - tag as "Studio"
                for pic in gym_studio_pics:
                    studio_pics_data.append({
                        "photo_id": str(pic.photo_id),
                        "type": pic.type,
                        "image_url": pic.image_url
                    })
            else:
                # Fallback to gym_onboarding_pics - tag as "Onboarding"
                onboarding_stmt = select(GymOnboardingPics).where(GymOnboardingPics.gym_id == gym_id)
                onboarding_result = await db.execute(onboarding_stmt)
                gym_onboarding_pics = onboarding_result.scalar_one_or_none()

                if gym_onboarding_pics:
                    # Extract all non-null image URLs from gym_onboarding_pics
                    image_columns = [
                        "machinery_1", "machinery_2", "treadmill_area",
                        "cardio_area", "dumbell_area", "reception_area"
                    ]

                    for idx, col in enumerate(image_columns):
                        image_url = getattr(gym_onboarding_pics, col, None)
                        if image_url:
                            studio_pics_data.append({
                                "photo_id": f"onboarding_{idx}",
                                "type": col.replace("_", " ").title(),
                                "image_url": image_url
                            })
        except Exception as pics_error:
            # Continue without studio pictures
            studio_pics_data = []

        # Get total clients count
        # Logic: Count clients from clients table matching gym_id, then count active gym memberships
        total_clients_count = 0
        inactive_clients_count = 0
        online_members_count = 0
        offline_members_count = 0

        try:
            gym_id_str = str(gym_id)

            # Get latest membership record for each client_id from fittbot_gym_membership
            # Fetch all memberships for this gym, excluding upcoming status
            all_memberships_stmt = select(
                FittbotGymMembership.id,
                FittbotGymMembership.client_id,
                FittbotGymMembership.type,
                FittbotGymMembership.status
            ).where(
                FittbotGymMembership.gym_id == gym_id_str,
                ~FittbotGymMembership.status.like('%upcoming%')
            ).order_by(
                FittbotGymMembership.client_id,
                FittbotGymMembership.id.desc()
            )

            all_memberships_result = await db.execute(all_memberships_stmt)
            all_memberships = all_memberships_result.all()

            # Get latest record per client_id by tracking seen clients
            seen_clients = set()
            latest_memberships = []

            for membership in all_memberships:
                client_id = membership[1]
                if client_id not in seen_clients:
                    seen_clients.add(client_id)
                    latest_memberships.append(membership)

            # Count online and offline members based on type
            for membership in latest_memberships:
                membership_type = membership[2] if membership[2] else ""
                if membership_type in ['admission_fees', 'normal']:
                    offline_members_count += 1
                else:
                    online_members_count += 1

            # Also get clients from the clients table for backward compatibility
            clients_stmt = select(Client.client_id).where(Client.gym_id == gym_id)
            clients_result = await db.execute(clients_stmt)
            client_ids = [str(row[0]) for row in clients_result.all()]

            inactive_clients_count = len(client_ids)  # Start with all clients as potentially inactive
            if client_ids:
                # Count active gym memberships for these client_ids and gym_id
                # Count distinct client_ids with active gym memberships
                # If same client has multiple active memberships for same gym, count as 1
                from sqlalchemy import distinct
                membership_count_stmt = select(func.count(distinct(FittbotGymMembership.client_id))).select_from(
                    FittbotGymMembership
                ).where(
                    FittbotGymMembership.gym_id == gym_id_str,
                    FittbotGymMembership.client_id.in_(client_ids),
                    FittbotGymMembership.status == "active"
                )
                membership_count_result = await db.execute(membership_count_stmt)
                total_clients_count = membership_count_result.scalar() or 0

                # Inactive clients = total clients - active clients
                inactive_clients_count = len(client_ids) - total_clients_count
        except Exception as clients_error:
            # Set default values
            total_clients_count = 0
            inactive_clients_count = 0
            online_members_count = 0
            offline_members_count = 0

        # Determine gym name and logo - prefer from Gym table, fallback to GymDatabase
        gym_name = gym.name if gym else (gym_from_marketing.gym_name if gym_from_marketing else f"Gym {gym_id}")
        gym_logo = gym.logo if gym else (gym_from_marketing.logo if gym_from_marketing else None)

        return {
            "success": True,
            "data": {
                "gym_id": gym_id,
                "gym_name": gym_name,
                "gym_logo": gym_logo,
                "session_plans": plans,
                "total_session_plans": len(plans),
                "membership_plans": gym_membership_plans,
                "total_membership_plans": len(gym_membership_plans),
                "daily_pass": daily_pass_data,
                "studio_pictures": studio_pics_data,
                "total_clients": total_clients_count,
                "inactive_clients": inactive_clients_count,
                "online_members": online_members_count,
                "offline_members": offline_members_count
            },
            "message": "Plans fetched successfully"
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching plans: {str(e)}")


@router.get("/{gym_id}/active-clients")
async def get_gym_active_clients(
    gym_id: int,
    page: int = Query(1, ge=1, description="Page number"),
    limit: int = Query(10, ge=1, le=100, description="Items per page"),
    search: Optional[str] = Query(None, description="Search by client name, email, or mobile"),
    sort_order: str = Query("desc", description="Sort order for joined date"),
    export: bool = Query(False, description="Export to Excel"),
    db: AsyncSession = Depends(get_async_db)
):
    
    try:
        # Verify gym exists
        gym_stmt = select(Gym).where(Gym.gym_id == gym_id)
        gym_result = await db.execute(gym_stmt)
        gym = gym_result.scalar_one_or_none()

        if not gym:
            raise HTTPException(status_code=404, detail="Gym not found")

        gym_id_str = str(gym_id)

        # Get client_ids with active gym memberships for this gym
        membership_stmt = select(
            distinct(FittbotGymMembership.client_id)
        ).where(
            FittbotGymMembership.gym_id == gym_id_str,
            FittbotGymMembership.status == "active"
        )
        membership_result = await db.execute(membership_stmt)
        # Get as strings from FittbotGymMembership.client_id (String type)
        active_client_ids_str = [row[0] for row in membership_result.all()]

        if not active_client_ids_str:
            return {
                "success": True,
                "data": {
                    "clients": [],
                    "total": 0,
                    "page": page,
                    "limit": limit,
                    "totalPages": 0,
                    "hasNext": False,
                    "hasPrev": False,
                    "gym_name": gym.name
                },
                "message": "Active clients fetched successfully"
            }

        # Convert to integers for querying Client table (client_id is Integer)
        # Filter out non-integer values like 'manual_6' before conversion
        active_client_ids = [int(cid) for cid in active_client_ids_str if cid.isdigit()]

        # Build subquery to get last purchase date for each client
        latest_purchase_subquery = select(
            func.cast(FittbotGymMembership.client_id, String).label('purchase_client_id'),
            func.max(FittbotGymMembership.joined_at).label('last_joined_at')
        ).group_by(
            func.cast(FittbotGymMembership.client_id, String)
        ).subquery('latest_purchase')

        # Build query to get client details with last purchase date
        stmt = select(
            Client.client_id,
            Client.name,
            Client.email,
            Client.contact,
            Client.gender,
            Client.created_at,
            Client.profile,
            latest_purchase_subquery.c.last_joined_at.label('last_purchase_date')
        ).outerjoin(
            latest_purchase_subquery, func.cast(Client.client_id, String) == latest_purchase_subquery.c.purchase_client_id
        ).where(
            Client.client_id.in_(active_client_ids)
        )

        # Apply search filter
        if search:
            search_term = f"%{search.lower()}%"
            stmt = stmt.where(
                or_(
                    func.lower(Client.name).like(search_term),
                    func.lower(Client.email).like(search_term),
                    Client.contact.like(search_term)
                )
            )

        # Apply sorting
        if sort_order == "asc":
            stmt = stmt.order_by(asc(Client.created_at))
        else:
            stmt = stmt.order_by(desc(Client.created_at))

        # Handle export to Excel
        if export:
            # For export, get all results without pagination
            export_result = await db.execute(stmt)
            export_clients = export_result.all()

            # Prepare data for Excel
            export_data = []
            for client in export_clients:
                last_purchase = client.last_purchase_date
                export_data.append({
                    "Client ID": client.client_id,
                    "Name": client.name or "N/A",
                    "Email": client.email or "N/A",
                    "Mobile": client.contact or "N/A",
                    "Gender": client.gender or "N/A",
                    "Joined Date": client.created_at.strftime("%Y-%m-%d %H:%M:%S") if client.created_at else "N/A",
                    "Last Purchase Date": last_purchase.strftime("%Y-%m-%d %H:%M:%S") if last_purchase else "N/A"
                })

            # Create Excel file
            df = pd.DataFrame(export_data)
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                df.to_excel(writer, index=False, sheet_name='Active Clients')
                # Auto-adjust column widths
                worksheet = writer.sheets['Active Clients']
                for idx, col in enumerate(df.columns, 1):
                    max_len = max(
                        df[col].astype(str).apply(len).max(),
                        len(str(col))
                    ) + 2
                    worksheet.column_dimensions[chr(64 + idx)].width = min(max_len, 50)

            output.seek(0)

            filename = f"active_clients_{gym.name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"

            return StreamingResponse(
                output,
                media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                headers={"Content-Disposition": f"attachment; filename={filename}"}
            )

        # Apply pagination
        offset = (page - 1) * limit
        stmt = stmt.offset(offset).limit(limit)

        # Execute query
        result = await db.execute(stmt)
        clients = result.all()

        # Get total count (using the active_client_ids length as base count)
        total_count = len(active_client_ids)

        # Format response
        clients_data = []
        for client in clients:
            last_purchase = client.last_purchase_date
            client_data = {
                "client_id": client.client_id,
                "name": client.name or "N/A",
                "email": client.email or "N/A",
                "mobile": client.contact or "N/A",
                "gender": client.gender,
                "profile_pic": client.profile,
                "joined_date": client.created_at.isoformat() if client.created_at else None,
                "last_purchase_date": last_purchase.isoformat() if last_purchase else None
            }
            clients_data.append(client_data)

        # Calculate pagination info
        total_pages = math.ceil(total_count / limit)
        has_next = page < total_pages
        has_prev = page > 1

        return {
            "success": True,
            "data": {
                "clients": clients_data,
                "total": total_count,
                "page": page,
                "limit": limit,
                "totalPages": total_pages,
                "hasNext": has_next,
                "hasPrev": has_prev,
                "gym_name": gym.name,
                "gym_logo": gym.logo
            },
            "message": "Active clients fetched successfully"
        }

    except HTTPException:
        raise
    except Exception as e:
        import traceback
        import sys
        traceback.print_exc(file=sys.stderr)
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error fetching active clients: {str(e)}")


@router.get("/{gym_id}/inactive-clients")
async def get_gym_inactive_clients(
    gym_id: int,
    page: int = Query(1, ge=1, description="Page number"),
    limit: int = Query(10, ge=1, le=100, description="Items per page"),
    search: Optional[str] = Query(None, description="Search by client name, email, or mobile"),
    sort_order: str = Query("desc", description="Sort order for joined date"),
    export: bool = Query(False, description="Export to Excel"),
    db: AsyncSession = Depends(get_async_db)
):
    """
    Get list of inactive clients for a specific gym.
    Inactive clients are those who don't have active gym memberships.
    """
    try:
        # Verify gym exists
        gym_stmt = select(Gym).where(Gym.gym_id == gym_id)
        gym_result = await db.execute(gym_stmt)
        gym = gym_result.scalar_one_or_none()

        if not gym:
            raise HTTPException(status_code=404, detail="Gym not found")

        gym_id_str = str(gym_id)

        # Get all client_ids for this gym
        all_clients_stmt = select(Client.client_id).where(Client.gym_id == gym_id)
        all_clients_result = await db.execute(all_clients_stmt)
        # Convert to strings to match with FittbotGymMembership.client_id (which is String type)
        all_client_ids = [str(row[0]) for row in all_clients_result.all()]

        if not all_client_ids:
            return {
                "success": True,
                "data": {
                    "clients": [],
                    "total": 0,
                    "page": page,
                    "limit": limit,
                    "totalPages": 0,
                    "hasNext": False,
                    "hasPrev": False,
                    "gym_name": gym.name
                },
                "message": "Inactive clients fetched successfully"
            }

        # Get client_ids with active gym memberships
        membership_stmt = select(
            distinct(FittbotGymMembership.client_id)
        ).where(
            FittbotGymMembership.gym_id == gym_id_str,
            FittbotGymMembership.status == "active"
        )
        membership_result = await db.execute(membership_stmt)
        active_client_ids = set(row[0] for row in membership_result.all())

        # Inactive clients = all clients - active clients
        # Both are now strings, so comparison will work correctly
        inactive_client_ids_str = [cid for cid in all_client_ids if cid not in active_client_ids]

        # Convert back to integers for querying Client table
        # Filter out non-integer values like 'manual_6' before conversion
        inactive_client_ids = [int(cid) for cid in inactive_client_ids_str if cid.isdigit()]

        # Build subquery to get last purchase date for each client
        latest_purchase_subquery = select(
            func.cast(FittbotGymMembership.client_id, String).label('purchase_client_id'),
            func.max(FittbotGymMembership.joined_at).label('last_joined_at')
        ).group_by(
            func.cast(FittbotGymMembership.client_id, String)
        ).subquery('latest_purchase')

        if not inactive_client_ids:
            return {
                "success": True,
                "data": {
                    "clients": [],
                    "total": 0,
                    "page": page,
                    "limit": limit,
                    "totalPages": 0,
                    "hasNext": False,
                    "hasPrev": False,
                    "gym_name": gym.name
                },
                "message": "Inactive clients fetched successfully"
            }

        # Build query to get client details with last purchase date
        stmt = select(
            Client.client_id,
            Client.name,
            Client.email,
            Client.contact,
            Client.gender,
            Client.created_at,
            Client.profile,
            latest_purchase_subquery.c.last_joined_at.label('last_purchase_date')
        ).outerjoin(
            latest_purchase_subquery, func.cast(Client.client_id, String) == latest_purchase_subquery.c.purchase_client_id
        ).where(
            Client.client_id.in_(inactive_client_ids)
        )

        # Apply search filter
        if search:
            search_term = f"%{search.lower()}%"
            stmt = stmt.where(
                or_(
                    func.lower(Client.name).like(search_term),
                    func.lower(Client.email).like(search_term),
                    Client.contact.like(search_term)
                )
            )

        # Apply sorting
        if sort_order == "asc":
            stmt = stmt.order_by(asc(Client.created_at))
        else:
            stmt = stmt.order_by(desc(Client.created_at))

        # Handle export to Excel
        if export:
            # For export, get all results without pagination
            export_result = await db.execute(stmt)
            export_clients = export_result.all()

            # Prepare data for Excel
            export_data = []
            for client in export_clients:
                last_purchase = client.last_purchase_date
                export_data.append({
                    "Client ID": client.client_id,
                    "Name": client.name or "N/A",
                    "Email": client.email or "N/A",
                    "Mobile": client.contact or "N/A",
                    "Gender": client.gender or "N/A",
                    "Joined Date": client.created_at.strftime("%Y-%m-%d %H:%M:%S") if client.created_at else "N/A",
                    "Last Purchase Date": last_purchase.strftime("%Y-%m-%d %H:%M:%S") if last_purchase else "N/A"
                })

            # Create Excel file
            df = pd.DataFrame(export_data)
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                df.to_excel(writer, index=False, sheet_name='Inactive Clients')
                # Auto-adjust column widths
                worksheet = writer.sheets['Inactive Clients']
                for idx, col in enumerate(df.columns, 1):
                    max_len = max(
                        df[col].astype(str).apply(len).max(),
                        len(str(col))
                    ) + 2
                    worksheet.column_dimensions[chr(64 + idx)].width = min(max_len, 50)

            output.seek(0)

            filename = f"inactive_clients_{gym.name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"

            return StreamingResponse(
                output,
                media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                headers={"Content-Disposition": f"attachment; filename={filename}"}
            )

        # Apply pagination
        offset = (page - 1) * limit
        stmt = stmt.offset(offset).limit(limit)

        # Execute query
        result = await db.execute(stmt)
        clients = result.all()

        # Get total count (using the inactive_client_ids length as base count)
        total_count = len(inactive_client_ids)

        # Format response
        clients_data = []
        for client in clients:
            last_purchase = client.last_purchase_date
            client_data = {
                "client_id": client.client_id,
                "name": client.name or "N/A",
                "email": client.email or "N/A",
                "mobile": client.contact or "N/A",
                "gender": client.gender,
                "profile_pic": client.profile,
                "joined_date": client.created_at.isoformat() if client.created_at else None,
                "last_purchase_date": last_purchase.isoformat() if last_purchase else None
            }
            clients_data.append(client_data)

        # Calculate pagination info
        total_pages = math.ceil(total_count / limit)
        has_next = page < total_pages
        has_prev = page > 1

        return {
            "success": True,
            "data": {
                "clients": clients_data,
                "total": total_count,
                "page": page,
                "limit": limit,
                "totalPages": total_pages,
                "hasNext": has_next,
                "hasPrev": has_prev,
                "gym_name": gym.name,
                "gym_logo": gym.logo
            },
            "message": "Inactive clients fetched successfully"
        }

    except HTTPException:
        raise
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error fetching inactive clients: {str(e)}")


@router.get("/{gym_id}/online-members")
async def get_gym_online_members(
    gym_id: int,
    page: int = Query(1, ge=1, description="Page number"),
    limit: int = Query(10, ge=1, le=100, description="Items per page"),
    search: Optional[str] = Query(None, description="Search by client name, email, or mobile"),
    sort_order: str = Query("desc", description="Sort order for joined date"),
    export: bool = Query(False, description="Export to Excel"),
    db: AsyncSession = Depends(get_async_db)
):
    
    try:
        # Verify gym exists
        gym_stmt = select(Gym).where(Gym.gym_id == gym_id)
        gym_result = await db.execute(gym_stmt)
        gym = gym_result.scalar_one_or_none()

        if not gym:
            raise HTTPException(status_code=404, detail="Gym not found")

        gym_id_str = str(gym_id)

        # Get all membership records for this gym, excluding upcoming status
        all_memberships_stmt = select(
            FittbotGymMembership.id,
            FittbotGymMembership.client_id,
            FittbotGymMembership.type,
            FittbotGymMembership.status
        ).where(
            FittbotGymMembership.gym_id == gym_id_str,
            ~FittbotGymMembership.status.like('%upcoming%')
        ).order_by(
            FittbotGymMembership.client_id,
            FittbotGymMembership.id.desc()
        )

        all_memberships_result = await db.execute(all_memberships_stmt)
        all_memberships = all_memberships_result.all()

        # Get latest record per client_id
        seen_clients = set()
        online_client_ids_str = []

        for membership in all_memberships:
            client_id = membership[1]
            if client_id not in seen_clients:
                seen_clients.add(client_id)
                membership_type = membership[2] if membership[2] else ""
                # Online members: type is NOT 'admission_fees' or 'normal'
                if membership_type not in ['admission_fees', 'normal']:
                    online_client_ids_str.append(client_id)

        if not online_client_ids_str:
            return {
                "success": True,
                "data": {
                    "clients": [],
                    "total": 0,
                    "page": page,
                    "limit": limit,
                    "totalPages": 0,
                    "hasNext": False,
                    "hasPrev": False,
                    "gym_name": gym.name,
                    "gym_logo": gym.logo
                },
                "message": "Online members fetched successfully"
            }

        # Convert to integers for querying Client table
        # Filter out non-integer values like 'manual_6' before conversion
        online_client_ids = [int(cid) for cid in online_client_ids_str if cid.isdigit()]

        # Build subquery to get last purchase date for each client
        latest_purchase_subquery = select(
            func.cast(FittbotGymMembership.client_id, String).label('purchase_client_id'),
            func.max(FittbotGymMembership.joined_at).label('last_joined_at')
        ).group_by(
            func.cast(FittbotGymMembership.client_id, String)
        ).subquery('latest_purchase')

        # Build query to get client details with last purchase date
        stmt = select(
            Client.client_id,
            Client.name,
            Client.email,
            Client.contact,
            Client.gender,
            Client.created_at,
            Client.profile,
            latest_purchase_subquery.c.last_joined_at.label('last_purchase_date')
        ).outerjoin(
            latest_purchase_subquery, func.cast(Client.client_id, String) == latest_purchase_subquery.c.purchase_client_id
        ).where(
            Client.client_id.in_(online_client_ids)
        )

        # Apply search filter
        if search:
            search_term = f"%{search.lower()}%"
            stmt = stmt.where(
                or_(
                    func.lower(Client.name).like(search_term),
                    func.lower(Client.email).like(search_term),
                    Client.contact.like(search_term)
                )
            )

        # Apply sorting
        if sort_order == "asc":
            stmt = stmt.order_by(asc(Client.created_at))
        else:
            stmt = stmt.order_by(desc(Client.created_at))

        # Handle export to Excel
        if export:
            # For export, get all results without pagination
            export_result = await db.execute(stmt)
            export_clients = export_result.all()

            # Prepare data for Excel
            export_data = []
            for client in export_clients:
                last_purchase = client.last_purchase_date
                export_data.append({
                    "Client ID": client.client_id,
                    "Name": client.name or "N/A",
                    "Email": client.email or "N/A",
                    "Mobile": client.contact or "N/A",
                    "Gender": client.gender or "N/A",
                    "Joined Date": client.created_at.strftime("%Y-%m-%d %H:%M:%S") if client.created_at else "N/A",
                    "Last Purchase Date": last_purchase.strftime("%Y-%m-%d %H:%M:%S") if last_purchase else "N/A"
                })

            # Create Excel file
            df = pd.DataFrame(export_data)
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                df.to_excel(writer, index=False, sheet_name='Online Members')
                # Auto-adjust column widths
                worksheet = writer.sheets['Online Members']
                for idx, col in enumerate(df.columns, 1):
                    max_len = max(
                        df[col].astype(str).apply(len).max(),
                        len(str(col))
                    ) + 2
                    worksheet.column_dimensions[chr(64 + idx)].width = min(max_len, 50)

            output.seek(0)

            filename = f"online_members_{gym.name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"

            return StreamingResponse(
                output,
                media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                headers={"Content-Disposition": f"attachment; filename={filename}"}
            )

        # Get total count before pagination
        count_result = await db.execute(select(func.count()).select_from(stmt.subquery()))
        total_count = count_result.scalar() or 0

        # Apply pagination
        offset = (page - 1) * limit
        stmt = stmt.offset(offset).limit(limit)

        # Execute query
        result = await db.execute(stmt)
        clients = result.all()

        # Format response
        clients_data = []
        for client in clients:
            last_purchase = client.last_purchase_date
            client_data = {
                "client_id": client.client_id,
                "name": client.name or "N/A",
                "email": client.email or "N/A",
                "mobile": client.contact or "N/A",
                "gender": client.gender,
                "profile_pic": client.profile,
                "joined_date": client.created_at.isoformat() if client.created_at else None,
                "last_purchase_date": last_purchase.isoformat() if last_purchase else None
            }
            clients_data.append(client_data)

        # Calculate pagination info
        total_pages = math.ceil(total_count / limit)
        has_next = page < total_pages
        has_prev = page > 1

        return {
            "success": True,
            "data": {
                "clients": clients_data,
                "total": total_count,
                "page": page,
                "limit": limit,
                "totalPages": total_pages,
                "hasNext": has_next,
                "hasPrev": has_prev,
                "gym_name": gym.name,
                "gym_logo": gym.logo
            },
            "message": "Online members fetched successfully"
        }

    except HTTPException:
        raise
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error fetching online members: {str(e)}")


@router.get("/{gym_id}/offline-members")
async def get_gym_offline_members(
    gym_id: int,
    page: int = Query(1, ge=1, description="Page number"),
    limit: int = Query(10, ge=1, le=100, description="Items per page"),
    search: Optional[str] = Query(None, description="Search by client name, email, or mobile"),
    sort_order: str = Query("desc", description="Sort order for joined date"),
    export: bool = Query(False, description="Export to Excel"),
    db: AsyncSession = Depends(get_async_db)
):
    
    try:
        # Verify gym exists
        gym_stmt = select(Gym).where(Gym.gym_id == gym_id)
        gym_result = await db.execute(gym_stmt)
        gym = gym_result.scalar_one_or_none()

        if not gym:
            raise HTTPException(status_code=404, detail="Gym not found")

        gym_id_str = str(gym_id)

        # Get all membership records for this gym, excluding upcoming status
        all_memberships_stmt = select(
            FittbotGymMembership.id,
            FittbotGymMembership.client_id,
            FittbotGymMembership.type,
            FittbotGymMembership.status
        ).where(
            FittbotGymMembership.gym_id == gym_id_str,
            ~FittbotGymMembership.status.like('%upcoming%')
        ).order_by(
            FittbotGymMembership.client_id,
            FittbotGymMembership.id.desc()
        )

        all_memberships_result = await db.execute(all_memberships_stmt)
        all_memberships = all_memberships_result.all()

        # Get latest record per client_id
        seen_clients = set()
        offline_client_ids_str = []

        for membership in all_memberships:
            client_id = membership[1]
            if client_id not in seen_clients:
                seen_clients.add(client_id)
                membership_type = membership[2] if membership[2] else ""
                # Offline members: type is 'admission_fees' or 'normal'
                if membership_type in ['admission_fees', 'normal']:
                    offline_client_ids_str.append(client_id)

        if not offline_client_ids_str:
            return {
                "success": True,
                "data": {
                    "clients": [],
                    "total": 0,
                    "page": page,
                    "limit": limit,
                    "totalPages": 0,
                    "hasNext": False,
                    "hasPrev": False,
                    "gym_name": gym.name,
                    "gym_logo": gym.logo
                },
                "message": "Offline members fetched successfully"
            }

        # Convert to integers for querying Client table
        # Filter out non-integer values like 'manual_6' before conversion
        offline_client_ids = [int(cid) for cid in offline_client_ids_str if cid.isdigit()]

        # Build subquery to get last purchase date for each client
        latest_purchase_subquery = select(
            func.cast(FittbotGymMembership.client_id, String).label('purchase_client_id'),
            func.max(FittbotGymMembership.joined_at).label('last_joined_at')
        ).group_by(
            func.cast(FittbotGymMembership.client_id, String)
        ).subquery('latest_purchase')

        # Build query to get client details with last purchase date
        stmt = select(
            Client.client_id,
            Client.name,
            Client.email,
            Client.contact,
            Client.gender,
            Client.created_at,
            Client.profile,
            latest_purchase_subquery.c.last_joined_at.label('last_purchase_date')
        ).outerjoin(
            latest_purchase_subquery, func.cast(Client.client_id, String) == latest_purchase_subquery.c.purchase_client_id
        ).where(
            Client.client_id.in_(offline_client_ids)
        )

        # Apply search filter
        if search:
            search_term = f"%{search.lower()}%"
            stmt = stmt.where(
                or_(
                    func.lower(Client.name).like(search_term),
                    func.lower(Client.email).like(search_term),
                    Client.contact.like(search_term)
                )
            )

        # Apply sorting
        if sort_order == "asc":
            stmt = stmt.order_by(asc(Client.created_at))
        else:
            stmt = stmt.order_by(desc(Client.created_at))

        # Handle export to Excel
        if export:
            # For export, get all results without pagination
            export_result = await db.execute(stmt)
            export_clients = export_result.all()

            # Prepare data for Excel
            export_data = []
            for client in export_clients:
                last_purchase = client.last_purchase_date
                export_data.append({
                    "Client ID": client.client_id,
                    "Name": client.name or "N/A",
                    "Email": client.email or "N/A",
                    "Mobile": client.contact or "N/A",
                    "Gender": client.gender or "N/A",
                    "Joined Date": client.created_at.strftime("%Y-%m-%d %H:%M:%S") if client.created_at else "N/A",
                    "Last Purchase Date": last_purchase.strftime("%Y-%m-%d %H:%M:%S") if last_purchase else "N/A"
                })

            # Create Excel file
            df = pd.DataFrame(export_data)
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                df.to_excel(writer, index=False, sheet_name='Offline Members')
                # Auto-adjust column widths
                worksheet = writer.sheets['Offline Members']
                for idx, col in enumerate(df.columns, 1):
                    max_len = max(
                        df[col].astype(str).apply(len).max(),
                        len(str(col))
                    ) + 2
                    worksheet.column_dimensions[chr(64 + idx)].width = min(max_len, 50)

            output.seek(0)

            filename = f"offline_members_{gym.name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"

            return StreamingResponse(
                output,
                media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                headers={"Content-Disposition": f"attachment; filename={filename}"}
            )

        # Get total count before pagination
        count_result = await db.execute(select(func.count()).select_from(stmt.subquery()))
        total_count = count_result.scalar() or 0

        # Apply pagination
        offset = (page - 1) * limit
        stmt = stmt.offset(offset).limit(limit)

        # Execute query
        result = await db.execute(stmt)
        clients = result.all()

        # Format response
        clients_data = []
        for client in clients:
            last_purchase = client.last_purchase_date
            client_data = {
                "client_id": client.client_id,
                "name": client.name or "N/A",
                "email": client.email or "N/A",
                "mobile": client.contact or "N/A",
                "gender": client.gender,
                "profile_pic": client.profile,
                "joined_date": client.created_at.isoformat() if client.created_at else None,
                "last_purchase_date": last_purchase.isoformat() if last_purchase else None
            }
            clients_data.append(client_data)

        # Calculate pagination info
        total_pages = math.ceil(total_count / limit)
        has_next = page < total_pages
        has_prev = page > 1

        return {
            "success": True,
            "data": {
                "clients": clients_data,
                "total": total_count,
                "page": page,
                "limit": limit,
                "totalPages": total_pages,
                "hasNext": has_next,
                "hasPrev": has_prev,
                "gym_name": gym.name,
                "gym_logo": gym.logo
            },
            "message": "Offline members fetched successfully"
        }

    except HTTPException:
        raise
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error fetching offline members: {str(e)}")


@router.get("/{gym_id}/recurring-subscribers")
async def get_gym_recurring_subscribers(
    gym_id: int,
    page: int = Query(1, ge=1, description="Page number"),
    limit: int = Query(10, ge=1, le=100, description="Items per page"),
    search: Optional[str] = Query(None, description="Search by client name, email, or mobile"),
    sort_order: str = Query("desc", description="Sort order for subscription count"),
    db: AsyncSession = Depends(get_async_db)
):
    
    try:
        # Verify gym exists
        gym_stmt = select(Gym).where(Gym.gym_id == gym_id)
        gym_result = await db.execute(gym_stmt)
        gym = gym_result.scalar_one_or_none()

        if not gym:
            raise HTTPException(status_code=404, detail="Gym not found")

        # Get all client_ids for this gym
        all_clients_stmt = select(Client.client_id).where(Client.gym_id == gym_id)
        all_clients_result = await db.execute(all_clients_stmt)
        all_client_ids = [row[0] for row in all_clients_result.all()]

        if not all_client_ids:
            return {
                "success": True,
                "data": {
                    "subscribers": [],
                    "total": 0,
                    "page": page,
                    "limit": limit,
                    "totalPages": 0,
                    "hasNext": False,
                    "hasPrev": False,
                    "gym_name": gym.name
                },
                "message": "Recurring subscribers fetched successfully"
            }

        # Count Fittbot subscription purchases for each client
        # Group by client_id and count records
        subscription_count_stmt = select(
            ClientFittbotAccess.client_id,
            func.count(ClientFittbotAccess.id).label('subscription_count')
        ).where(
            ClientFittbotAccess.client_id.in_(all_client_ids)
        ).group_by(
            ClientFittbotAccess.client_id
        ).having(
            func.count(ClientFittbotAccess.id) > 1
        )

        subscription_count_result = await db.execute(subscription_count_stmt)
        recurring_clients_data = subscription_count_result.all()

        if not recurring_clients_data:
            return {
                "success": True,
                "data": {
                    "subscribers": [],
                    "total": 0,
                    "page": page,
                    "limit": limit,
                    "totalPages": 0,
                    "hasNext": False,
                    "hasPrev": False,
                    "gym_name": gym.name
                },
                "message": "Recurring subscribers fetched successfully"
            }

        # Create a dict of client_id -> subscription_count
        client_subscription_counts = {row[0]: row[1] for row in recurring_clients_data}
        recurring_client_ids = list(client_subscription_counts.keys())

        # Build query to get client details
        stmt = select(
            Client.client_id,
            Client.name,
            Client.email,
            Client.contact,
            Client.gender,
            Client.created_at,
            Client.profile
        ).where(
            Client.client_id.in_(recurring_client_ids)
        )

        # Apply search filter
        if search:
            search_term = f"%{search.lower()}%"
            stmt = stmt.where(
                or_(
                    func.lower(Client.name).like(search_term),
                    func.lower(Client.email).like(search_term),
                    Client.contact.like(search_term)
                )
            )

        # Execute query to get all matching clients
        all_clients_result = await db.execute(stmt)
        all_clients = all_clients_result.all()

        # Filter clients based on search and add subscription count
        subscribers_data = []
        for client in all_clients:
            sub_count = client_subscription_counts.get(client.client_id, 0)
            client_data = {
                "client_id": client.client_id,
                "name": client.name or "N/A",
                "email": client.email or "N/A",
                "mobile": client.contact or "N/A",
                "gender": client.gender,
                "profile_pic": client.profile,
                "joined_date": client.created_at.isoformat() if client.created_at else None,
                "subscription_count": sub_count
            }
            subscribers_data.append(client_data)

        # Apply sorting by subscription count
        if sort_order == "asc":
            subscribers_data.sort(key=lambda x: x["subscription_count"])
        else:
            subscribers_data.sort(key=lambda x: x["subscription_count"], reverse=True)

        # Get total count
        total_count = len(subscribers_data)

        # Apply pagination
        start_idx = (page - 1) * limit
        end_idx = start_idx + limit
        paginated_subscribers = subscribers_data[start_idx:end_idx]

        # Calculate pagination info
        total_pages = math.ceil(total_count / limit)
        has_next = page < total_pages
        has_prev = page > 1

        return {
            "success": True,
            "data": {
                "subscribers": paginated_subscribers,
                "total": total_count,
                "page": page,
                "limit": limit,
                "totalPages": total_pages,
                "hasNext": has_next,
                "hasPrev": has_prev,
                "gym_name": gym.name,
                "gym_logo": gym.logo
            },
            "message": "Recurring subscribers fetched successfully"
        }

    except HTTPException:
        raise
    except Exception as e:
        import traceback
        import sys
        traceback.print_exc(file=sys.stderr)
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error fetching recurring subscribers: {str(e)}")


@router.get("/{gym_id}/recurring-gym-subscribers")
async def get_recurring_gym_subscribers(gym_id: int, db: AsyncSession = Depends(get_async_db)):
    """
    Get count of recurring gym subscribers for a specific gym.

    Logic:
    - Use fittbot_gym_membership table
    - Filter by gym_id
    - Consider only rows where client_id is pure integer (digits only)
    - Group by client_id
    - Count only client_ids with more than one membership entry for the same gym_id
    """
    try:
        # Verify gym exists
        gym_stmt = select(Gym).where(Gym.gym_id == gym_id)
        gym_result = await db.execute(gym_stmt)
        gym = gym_result.scalar_one_or_none()

        if not gym:
            raise HTTPException(status_code=404, detail=f"Gym with ID {gym_id} not found")

        # Query all memberships for this gym
        membership_stmt = select(
            func.cast(FittbotGymMembership.client_id, String).label('client_id_str')
        ).where(
            FittbotGymMembership.gym_id == str(gym_id)
        )

        membership_result = await db.execute(membership_stmt)
        all_client_ids = [row[0] for row in membership_result.all()]

        # Filter only pure integer client_ids (digits only)
        pure_integer_client_ids = []
        for client_id_str in all_client_ids:
            if client_id_str and client_id_str.isdigit():
                pure_integer_client_ids.append(client_id_str)

        # Count occurrences of each client_id
        from collections import Counter
        client_id_counts = Counter(pure_integer_client_ids)

        # Filter only client_ids with more than one membership (recurring)
        recurring_client_ids = [
            client_id for client_id, count in client_id_counts.items()
            if count > 1
        ]

        recurring_count = len(recurring_client_ids)

        return {
            "success": True,
            "data": {
                "total": recurring_count,
                "gym_id": gym_id,
                "gym_name": gym.name
            },
            "message": "Recurring gym subscribers fetched successfully"
        }

    except HTTPException:
        raise
    except Exception as e:
        import traceback
        import sys
        traceback.print_exc(file=sys.stderr)
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error fetching recurring gym subscribers: {str(e)}")


@router.get("/{gym_id}/recurring-dailypass-purchasers")
async def get_recurring_dailypass_purchasers(gym_id: int, db: AsyncSession = Depends(get_async_db)):
    """
    Get count of recurring daily pass purchasers for a specific gym.

    Logic:
    - Use dailypass.daily_passes table
    - Filter by gym_id
    - Consider only rows where client_id is pure integer (digits only)
    - Group by client_id
    - Count only client_ids with more than one purchase for the same gym_id
    """
    try:
        from app.models.dailypass_models import DailyPass

        # Verify gym exists
        gym_stmt = select(Gym).where(Gym.gym_id == gym_id)
        gym_result = await db.execute(gym_stmt)
        gym = gym_result.scalar_one_or_none()

        if not gym:
            raise HTTPException(status_code=404, detail=f"Gym with ID {gym_id} not found")

        # Query all daily passes for this gym
        dailypass_stmt = select(
            func.cast(DailyPass.client_id, String).label('client_id_str')
        ).where(
            DailyPass.gym_id == str(gym_id)
        )

        dailypass_result = await db.execute(dailypass_stmt)
        all_client_ids = [row[0] for row in dailypass_result.all()]

        # Filter only pure integer client_ids (digits only)
        pure_integer_client_ids = []
        for client_id_str in all_client_ids:
            if client_id_str and client_id_str.isdigit():
                pure_integer_client_ids.append(client_id_str)

        # Count occurrences of each client_id
        from collections import Counter
        client_id_counts = Counter(pure_integer_client_ids)

        # Filter only client_ids with more than one purchase (recurring)
        recurring_client_ids = [
            client_id for client_id, count in client_id_counts.items()
            if count > 1
        ]

        recurring_count = len(recurring_client_ids)

        return {
            "success": True,
            "data": {
                "total": recurring_count,
                "gym_id": gym_id,
                "gym_name": gym.name
            },
            "message": "Recurring daily pass purchasers fetched successfully"
        }

    except HTTPException:
        raise
    except Exception as e:
        import traceback
        import sys
        traceback.print_exc(file=sys.stderr)
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error fetching recurring daily pass purchasers: {str(e)}")


@router.get("/{gym_id}/recurring-session-purchasers")
async def get_recurring_session_purchasers(gym_id: int, db: AsyncSession = Depends(get_async_db)):
    """
    Get count of recurring session purchasers for a specific gym.

    Logic:
    - Use sessions.session_purchases table
    - Filter by gym_id
    - Consider only rows where client_id is pure integer (digits only)
    - A client is considered "recurring" if:
      1. They have more than 1 purchase record, OR
      2. Any single purchase has sessions_count > 1
    - Count unique such clients
    """
    try:
        from app.models.fittbot_models import SessionPurchase

        # Verify gym exists
        gym_stmt = select(Gym).where(Gym.gym_id == gym_id)
        gym_result = await db.execute(gym_stmt)
        gym = gym_result.scalar_one_or_none()

        if not gym:
            raise HTTPException(status_code=404, detail=f"Gym with ID {gym_id} not found")

        # Query all session purchases for this gym with client_id and sessions_count
        session_stmt = select(
            func.cast(SessionPurchase.client_id, String).label('client_id_str'),
            SessionPurchase.sessions_count
        ).where(
            SessionPurchase.gym_id == gym_id
        )

        session_result = await db.execute(session_stmt)
        all_purchases = session_result.all()

        # Filter only pure integer client_ids and group by client
        from collections import defaultdict
        client_data = defaultdict(lambda: {"purchase_count": 0, "max_sessions_count": 0})

        for client_id_str, sessions_count in all_purchases:
            if client_id_str and str(client_id_str).isdigit():
                client_id_str = str(client_id_str)
                client_data[client_id_str]["purchase_count"] += 1
                if sessions_count and sessions_count > client_data[client_id_str]["max_sessions_count"]:
                    client_data[client_id_str]["max_sessions_count"] = sessions_count

        # Filter only clients who are recurring:
        # - More than 1 purchase, OR
        # - Any purchase has sessions_count > 1
        recurring_client_ids = [
            client_id for client_id, data in client_data.items()
            if data["purchase_count"] > 1 or data["max_sessions_count"] > 1
        ]

        recurring_count = len(recurring_client_ids)

        return {
            "success": True,
            "data": {
                "total": recurring_count,
                "gym_id": gym_id,
                "gym_name": gym.name
            },
            "message": "Recurring session purchasers fetched successfully"
        }

    except HTTPException:
        raise
    except Exception as e:
        import traceback
        import sys
        traceback.print_exc(file=sys.stderr)
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error fetching recurring session purchasers: {str(e)}")

