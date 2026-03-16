# Verified Gyms API - Optimized Async Implementation
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import func, or_, desc, asc, select, cast, String
from typing import Optional, List
from app.models.fittbot_models import (
    Gym, GymOwner, Client, GymPlans, SessionSetting
)
from app.models.dailypass_models import DailyPassPricing
from app.models.async_database import get_async_db
import math

router = APIRouter(prefix="/api/admin/verified-gyms", tags=["VerifiedGyms"])

class VerifiedGymResponse(BaseModel):
    gym_id: int
    gym_name: str
    owner_name: Optional[str] = None
    contact_number: Optional[str] = None
    location: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    area: Optional[str] = None
    pincode: Optional[str] = None
    address: Optional[str] = None
    registered_users: int = 0
    type: Optional[str] = None
    created_at: Optional[str] = None

    class Config:
        from_attributes = True

@router.get("")
async def get_verified_gyms(
    page: int = Query(1, ge=1, description="Page number"),
    limit: int = Query(10, ge=1, le=100, description="Items per page"),
    search: Optional[str] = Query(None, description="Search by gym name, owner, mobile, or location"),
    sort_order: str = Query("desc", regex="^(asc|desc)$", description="Sort order for created_at"),
    has_session_plans: Optional[bool] = Query(None, description="Filter by session plans existence"),
    has_membership_plans: Optional[bool] = Query(None, description="Filter by membership plans existence"),
    has_daily_pass: Optional[bool] = Query(None, description="Filter by daily pass pricing existence"),
    registered_users_filter: Optional[str] = Query(None, description="Filter by registered users count (e.g., '50', '100', '150')"),
    city: Optional[str] = Query(None, description="Filter by city"),
    db: AsyncSession = Depends(get_async_db)
):
   
    try:
        # Subquery to count registered users per gym
        registered_users_subq = (
            select(func.count(Client.client_id))
            .where(Client.gym_id == Gym.gym_id)
            .correlate(Gym)
            .label('registered_users')
        )

        # Subqueries for plan type checks (executed as EXISTS in DB)
        session_exists_subq = (
            select(1)
            .where(SessionSetting.gym_id == Gym.gym_id)
            .correlate(Gym)
            .exists()
            .label('has_session_plans')
        )

        membership_exists_subq = (
            select(1)
            .where(GymPlans.gym_id == Gym.gym_id)
            .correlate(Gym)
            .exists()
            .label('has_membership_plans')
        )

        daily_pass_exists_subq = (
            select(1)
            .where(DailyPassPricing.gym_id == cast(Gym.gym_id, String))
            .correlate(Gym)
            .exists()
            .label('has_daily_pass')
        )

        # Main query - optimized with only necessary fields and joins
        main_stmt = select(
            Gym.gym_id,
            Gym.name.label('gym_name'),
            Gym.location.label('location'),
            Gym.created_at,
            Gym.type,
            GymOwner.name.label('owner_name'),
            GymOwner.contact_number,
            Gym.area,
            Gym.city,
            Gym.state,
            Gym.pincode,
            Gym.door_no,
            Gym.street,
            Gym.building,
            registered_users_subq,
            session_exists_subq,
            membership_exists_subq,
            daily_pass_exists_subq
        ).outerjoin(
            GymOwner, Gym.owner_id == GymOwner.owner_id
        ).where(
            Gym.type.like("%green%")
        )

        # Apply search filter at DB level
        if search:
            search_term = f"%{search.lower()}%"
            main_stmt = main_stmt.where(
                or_(
                    func.lower(Gym.name).like(search_term),
                    func.lower(GymOwner.name).like(search_term),
                    GymOwner.contact_number.like(search_term),
                    func.lower(Gym.location).like(search_term),
                    func.lower(Gym.area).like(search_term),
                    func.lower(Gym.city).like(search_term)
                )
            )

        # Apply plan type filters using EXISTS subqueries
        if has_session_plans is not None:
            main_stmt = main_stmt.where(session_exists_subq == has_session_plans)
        if has_membership_plans is not None:
            main_stmt = main_stmt.where(membership_exists_subq == has_membership_plans)
        if has_daily_pass is not None:
            main_stmt = main_stmt.where(daily_pass_exists_subq == has_daily_pass)

        # Apply registered users filter using HAVING (after aggregation)
        if registered_users_filter:
            try:
                min_users = int(registered_users_filter)
                main_stmt = main_stmt.having(registered_users_subq > min_users)
            except ValueError:
                pass

        # Apply city filter (case-insensitive and trimmed to match normalized city names)
        if city:
            main_stmt = main_stmt.where(
                func.trim(func.lower(Gym.city)) == city.lower().strip()
            )

        # Get all unique cities for the dropdown (before pagination)
        cities_stmt = select(Gym.city).where(
            Gym.type.like("%green%"),
            Gym.city.isnot(None),
            Gym.city != ""
        ).distinct().order_by(Gym.city)
        cities_result = await db.execute(cities_stmt)

        # Normalize city names: trim whitespace and convert to title case
        seen_cities = set()
        cities = []
        for row in cities_result.fetchall():
            if row[0]:
                normalized = row[0].strip().title()
                if normalized and normalized not in seen_cities:
                    seen_cities.add(normalized)
                    cities.append(normalized)

        # Get total count (with all filters applied)
        count_stmt = select(func.count()).select_from(main_stmt.subquery())
        count_result = await db.execute(count_stmt)
        total_count = count_result.scalar() or 0

        # Apply sorting at DB level
        if sort_order == "asc":
            main_stmt = main_stmt.order_by(asc(Gym.created_at))
        else:
            main_stmt = main_stmt.order_by(desc(Gym.created_at))

        # Apply pagination at DB level
        offset = (page - 1) * limit
        main_stmt = main_stmt.offset(offset).limit(limit)

        # Execute query
        result = await db.execute(main_stmt)
        gyms = result.all()

        # Format response
        gyms_data = []
        for gym in gyms:
            # Build street address from Gym table fields
            street_address_parts = []
            if gym.door_no:
                street_address_parts.append(gym.door_no)
            if gym.street:
                street_address_parts.append(gym.street)
            if gym.building:
                street_address_parts.append(gym.building)
            street_address = ", ".join(street_address_parts) if street_address_parts else "-"

            # Build full address
            address_parts = []
            if street_address and street_address != "-":
                address_parts.append(street_address)
            if gym.area and gym.area != "-":
                address_parts.append(gym.area)
            if gym.city and gym.city != "-":
                address_parts.append(gym.city)
            if gym.state and gym.state != "-":
                address_parts.append(gym.state)
            if gym.pincode and gym.pincode != "-":
                address_parts.append(gym.pincode)

            address = ", ".join(address_parts) if address_parts else (gym.location or "-")

            gyms_data.append({
                "gym_id": gym.gym_id,
                "gym_name": gym.gym_name or "-",
                "owner_name": gym.owner_name or "-",
                "contact_number": gym.contact_number or "-",
                "location": gym.location or "-",
                "address": address,
                "street_address": street_address,
                "area": gym.area or "-",
                "city": gym.city or "-",
                "state": gym.state or "-",
                "pincode": gym.pincode or "-",
                "registered_users": gym.registered_users or 0,
                "type": gym.type,
                "created_at": gym.created_at.isoformat() if gym.created_at else None,
                "has_session_plans": gym.has_session_plans or False,
                "has_membership_plans": gym.has_membership_plans or False,
                "has_daily_pass": gym.has_daily_pass or False,
            })

        # Calculate pagination metadata
        total_pages = math.ceil(total_count / limit) if total_count > 0 else 1
        has_next = page < total_pages
        has_prev = page > 1

        return {
            "success": True,
            "data": {
                "gyms": gyms_data,
                "total": total_count,
                "page": page,
                "limit": limit,
                "totalPages": total_pages,
                "hasNext": has_next,
                "hasPrev": has_prev,
                "cities": cities,
            },
            "message": "Verified gyms fetched successfully"
        }

    except Exception as e:
        import traceback
        import sys
        traceback.print_exc(file=sys.stderr)
        raise HTTPException(status_code=500, detail=f"Error fetching verified gyms: {str(e)}")
