# Unverified Gyms API - Optimized Async Implementation
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import func, or_, desc, asc, select, cast, String, text
from typing import Optional, List, Dict, Any
from app.models.fittbot_models import (
    Gym, GymOwner, Client, GymPlans, SessionSetting,
    AccountDetails, GymVerificationDocument, GymOnboardingPics
)
from app.models.dailypass_models import DailyPassPricing
from app.models.async_database import get_async_db
import math

router = APIRouter(prefix="/api/admin/unverified-gyms", tags=["UnverifiedGyms"])

class UnverifiedGymResponse(BaseModel):
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
async def get_unverified_gyms(
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
            or_(
                Gym.type.like("%hold%"),
                Gym.type.like("%red%")
            )
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
            or_(
                Gym.type.like("%hold%"),
                Gym.type.like("%red%")
            ),
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
            "message": "Unverified gyms fetched successfully"
        }

    except Exception as e:
        import traceback
        import sys
        traceback.print_exc(file=sys.stderr)
        raise HTTPException(status_code=500, detail=f"Error fetching unverified gyms: {str(e)}")


@router.get("/splitup")
async def get_unverified_splitup(
    page: int = Query(1, ge=1, description="Page number"),
    limit: int = Query(10, ge=1, le=100, description="Items per page"),
    type: str = Query("red", regex="^(red|hold)$", description="Type filter: red or hold"),
    search: Optional[str] = Query(None, description="Search by gym name, owner, mobile, or location"),
    city: Optional[str] = Query(None, description="Filter by city"),
    sort_order: str = Query("desc", regex="^(asc|desc)$", description="Sort order for created_at"),
    db: AsyncSession = Depends(get_async_db)
):
    """
    Get gyms by type (red or hold) for the Unverified Splitup page
    """
    try:
        # Subquery to count registered users per gym
        registered_users_subq = (
            select(func.count(Client.client_id))
            .where(Client.gym_id == Gym.gym_id)
            .correlate(Gym)
            .label('registered_users')
        )

        # Main query
        main_stmt = select(
            Gym.gym_id,
            Gym.name.label('gym_name'),
            Gym.location.label('location'),
            Gym.city,
            Gym.created_at,
            Gym.area,
            Gym.state,
            Gym.pincode,
            Gym.door_no,
            Gym.street,
            Gym.building,
            registered_users_subq
        ).where(
            Gym.type == type
        )

        # Apply city filter (case-insensitive and trimmed to match normalized city names)
        # This handles variations like "bangalore", "BANGALORE", " Bangalore ", etc.
        if city:
            main_stmt = main_stmt.where(
                func.trim(func.lower(Gym.city)) == city.lower().strip()
            )

        # Apply search filter
        if search:
            search_term = f"%{search.lower()}%"
            main_stmt = main_stmt.where(
                or_(
                    func.lower(Gym.name).like(search_term),
                    func.lower(Gym.location).like(search_term),
                    func.lower(Gym.area).like(search_term),
                    func.lower(Gym.city).like(search_term)
                )
            )

        # Get all unique cities for the dropdown (before pagination)
        cities_stmt = select(Gym.city).where(
            Gym.type == type,
            Gym.city.isnot(None),
            Gym.city != ""
        ).distinct().order_by(Gym.city)
        cities_result = await db.execute(cities_stmt)

        # Normalize city names: trim whitespace and convert to title case
        # This ensures variations like "bangalore", "BANGALORE", " Bangalore " all become "Bangalore"
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

        # Apply sorting
        if sort_order == "asc":
            main_stmt = main_stmt.order_by(asc(Gym.created_at))
        else:
            main_stmt = main_stmt.order_by(desc(Gym.created_at))

        # Apply pagination
        offset = (page - 1) * limit
        main_stmt = main_stmt.offset(offset).limit(limit)

        # Execute query
        result = await db.execute(main_stmt)
        gyms = result.all()

        # Collect gym_ids for batch fetching plans scores and registration steps
        gym_ids = [gym.gym_id for gym in gyms]

        # Batch fetch plans completion scores
        plans_scores = await _get_plans_completion_scores_batch(gym_ids, db)

        # Batch fetch registration steps
        registration_steps_batch = await _get_registration_steps_batch(gym_ids, db)

        # Format response
        gyms_data = []
        for gym in gyms:
            # Build street address
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

            # Get registration steps for this gym
            gym_registration_steps = registration_steps_batch.get(gym.gym_id, {})
            registration_completion = calculate_registration_completion_percentage(gym_registration_steps)

            # Get plans data for this gym
            gym_plans_data = plans_scores.get(gym.gym_id, {
                "completion_score": 0.0,
                "has_daily_pass": False,
                "has_session": False,
                "has_membership": False
            })

            gyms_data.append({
                "gym_id": gym.gym_id,
                "gym_name": gym.gym_name or "-",
                "location": gym.location or "-",
                "address": address,
                "street_address": street_address,
                "area": gym.area or "-",
                "city": gym.city or "-",
                "state": gym.state or "-",
                "pincode": gym.pincode or "-",
                "registered_users": gym.registered_users or 0,
                "created_at": gym.created_at.isoformat() if gym.created_at else None,
                "plans_completion_score": gym_plans_data.get("completion_score", 0.0),
                "has_daily_pass": gym_plans_data.get("has_daily_pass", False),
                "has_session": gym_plans_data.get("has_session", False),
                "has_membership": gym_plans_data.get("has_membership", False),
                "registration_steps": gym_registration_steps,
                "registration_completion": registration_completion,
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
            "message": f"{type.capitalize()} gyms fetched successfully"
        }

    except Exception as e:
        import traceback
        import sys
        traceback.print_exc(file=sys.stderr)
        raise HTTPException(status_code=500, detail=f"Error fetching {type} gyms: {str(e)}")


@router.get("/gym-plans/{gym_id}")
async def get_admin_gym_plans(
    gym_id: int,
    db: AsyncSession = Depends(get_async_db)
):
    """
    Get plans data for a specific gym (Daily Pass, Sessions, Gym Plans) - Admin endpoint
    Fully async with optimized queries
    """
    try:
        plans_data = await _get_plans_data(gym_id, db)
        return {
            "success": True,
            "data": plans_data,
            "message": "Successfully retrieved gym plans"
        }
    except Exception as e:
        import traceback
        import sys
        traceback.print_exc(file=sys.stderr)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch gym plans: {str(e)}"
        )


async def _get_plans_data(gym_id: int, async_db: AsyncSession) -> Dict[str, Any]:
    """
    Fetch plans data from dailypass, sessions, and fittbot_local schemas
    Optimized async implementation using SQLAlchemy ORM
    """
    # 1. Fetch Daily Pass data using SQLAlchemy
    daily_pass_stmt = select(DailyPassPricing).where(
        DailyPassPricing.gym_id == str(gym_id)
    )
    daily_pass_result = await async_db.execute(daily_pass_stmt)
    daily_pass_rows = daily_pass_result.scalars().all()

    daily_pass_data = []
    for pass_obj in daily_pass_rows:
        daily_pass_data.append({
            "id": pass_obj.id,
            "gym_id": pass_obj.gym_id,
            "price": pass_obj.price,
            "discount_price": pass_obj.discount_price,
            "discount_percentage": pass_obj.discount_percentage
        })

    # 2. Fetch Sessions data using SQLAlchemy
    sessions_stmt = select(func.count()).select_from(SessionSetting).where(
        SessionSetting.gym_id == gym_id,
        SessionSetting.is_enabled == True
    )
    sessions_result = await async_db.execute(sessions_stmt)
    sessions_count = sessions_result.scalar_one_or_none() or 0

    # Fetch session prices to find the lowest price
    lowest_session_price = None
    try:
        sessions_price_stmt = select(SessionSetting.final_price).where(
            SessionSetting.gym_id == gym_id,
            SessionSetting.is_enabled == True,
            SessionSetting.final_price.is_not(None)
        ).order_by(SessionSetting.final_price.asc()).limit(1)
        sessions_price_result = await async_db.execute(sessions_price_stmt)
        lowest_session_price = sessions_price_result.scalar_one_or_none()
    except Exception:
        lowest_session_price = None

    # 3. Fetch Gym Plans data using SQLAlchemy
    gym_plans_stmt = select(func.count()).select_from(GymPlans).where(
        GymPlans.gym_id == gym_id
    )
    gym_plans_result = await async_db.execute(gym_plans_stmt)
    gym_plans_count = gym_plans_result.scalar_one_or_none() or 0

    # Calculate weighted score
    # Daily Pass: 33.33%, Sessions: 33.33%, Gym Plans: 33.34%
    daily_pass_score = 33.33 if len(daily_pass_data) > 0 else 0
    sessions_score = 33.33 if sessions_count > 0 else 0
    gym_plans_score = 33.34 if gym_plans_count > 0 else 0

    total_score = daily_pass_score + sessions_score + gym_plans_score

    return {
        "gym_id": gym_id,
        "daily_pass": {
            "count": len(daily_pass_data),
            "entries": daily_pass_data
        },
        "sessions": {
            "count": sessions_count,
            "lowest_price": lowest_session_price
        },
        "gym_plans": {
            "count": gym_plans_count
        },
        "completion_score": round(total_score, 2),
        "max_score": 100
    }


async def _get_plans_completion_scores_batch(
    gym_ids: List[int], async_db: AsyncSession
) -> Dict[int, Dict[str, Any]]:
    """
    Get plans completion scores and existence status for multiple gyms using SQLAlchemy ORM.
    Returns a dictionary mapping gym_id to completion score and plan existence.
    Optimized to avoid N+1 query problems.
    """
    if not gym_ids:
        return {}

    try:
        # Batch query for daily pass counts using SQLAlchemy
        daily_pass_stmt = select(
            DailyPassPricing.gym_id,
            func.count().label('count')
        ).where(
            DailyPassPricing.gym_id.in_(str(gid) for gid in gym_ids)
        ).group_by(DailyPassPricing.gym_id)

        daily_pass_result = await async_db.execute(daily_pass_stmt)
        daily_pass_counts = {row[0]: row[1] for row in daily_pass_result.fetchall()}

        # Batch query for session counts using SQLAlchemy
        sessions_stmt = select(
            SessionSetting.gym_id,
            func.count().label('count')
        ).where(
            SessionSetting.gym_id.in_(gym_ids),
            SessionSetting.is_enabled == True
        ).group_by(SessionSetting.gym_id)

        sessions_result = await async_db.execute(sessions_stmt)
        sessions_counts = {row[0]: row[1] for row in sessions_result.fetchall()}

        # Batch query for gym plan counts using SQLAlchemy
        gym_plans_stmt = select(
            GymPlans.gym_id,
            func.count().label('count')
        ).where(
            GymPlans.gym_id.in_(gym_ids)
        ).group_by(GymPlans.gym_id)

        gym_plans_result = await async_db.execute(gym_plans_stmt)
        gym_plans_counts = {row[0]: row[1] for row in gym_plans_result.fetchall()}

        # Calculate scores and existence for each gym
        result = {}
        for gym_id in gym_ids:
            daily_pass_exists = daily_pass_counts.get(str(gym_id), 0) > 0
            sessions_exists = sessions_counts.get(gym_id, 0) > 0
            gym_plans_exists = gym_plans_counts.get(gym_id, 0) > 0

            daily_pass_score = 33.33 if daily_pass_exists else 0
            sessions_score = 33.33 if sessions_exists else 0
            gym_plans_score = 33.34 if gym_plans_exists else 0

            result[gym_id] = {
                "completion_score": round(daily_pass_score + sessions_score + gym_plans_score, 2),
                "has_daily_pass": daily_pass_exists,
                "has_session": sessions_exists,
                "has_membership": gym_plans_exists
            }

        return result
    except Exception as e:
        import traceback
        import sys
        traceback.print_exc(file=sys.stderr)
        return {
            gym_id: {
                "completion_score": 0.0,
                "has_daily_pass": False,
                "has_session": False,
                "has_membership": False
            } for gym_id in gym_ids
        }


@router.get("/gym-registration-status/{gym_id}")
async def get_admin_gym_registration_status(
    gym_id: int,
    db: AsyncSession = Depends(get_async_db)
):
    """
    Get registration steps for a specific gym by gym_id - Admin endpoint
    Fully async with optimized queries
    """
    try:
        # Fetch gym by gym_id using SQLAlchemy
        gym_stmt = select(Gym).where(Gym.gym_id == gym_id)
        gym_result = await db.execute(gym_stmt)
        gym = gym_result.scalar_one_or_none()

        if not gym:
            raise HTTPException(
                status_code=404,
                detail=f"Gym with gym_id {gym_id} not found"
            )

        # Get owner contact number
        owner_stmt = select(GymOwner.contact_number, GymOwner.email).where(
            GymOwner.owner_id == gym.owner_id
        )
        owner_result = await db.execute(owner_stmt)
        owner = owner_result.first()

        # Get registration steps for this gym (returns the dict directly)
        gym_registration_steps = await _get_registration_steps(gym_id, db)

        gym_info = {
            "gym_id": gym.gym_id,
            "gym_name": gym.name,
            "owner_id": gym.owner_id,
            "owner_contact_number": owner[0] if owner else None,
            "owner_email": owner[1] if owner else None,
            "location": gym.location,
            "contact_number": None,
            "created_at": gym.created_at.isoformat() if gym.created_at else None,
            "registration_steps": gym_registration_steps
        }

        return {
            "success": True,
            "data": gym_info,
            "message": "Successfully retrieved gym registration status"
        }

    except HTTPException:
        raise
    except Exception as e:
        import traceback
        import sys
        traceback.print_exc(file=sys.stderr)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch gym registration status: {str(e)}"
        )


async def _get_registration_steps(
    gym_id: int, async_db: AsyncSession
) -> Dict[str, Any]:
    """Get registration document steps status for a gym"""

    # 1. Check account_details table
    account_stmt = select(AccountDetails).where(AccountDetails.gym_id == gym_id)
    account_result = await async_db.execute(account_stmt)
    account_details = account_result.scalar_one_or_none()
    account_details_completed = account_details is not None

    # 2. Check gyms table for services and operating_hours
    gym_stmt = select(Gym).where(Gym.gym_id == gym_id)
    gym_result = await async_db.execute(gym_stmt)
    gym = gym_result.scalar_one_or_none()

    services_completed = False
    operating_hours_completed = False
    if gym:
        services_completed = gym.services is not None and len(gym.services) > 0 if gym.services else False
        operating_hours_completed = gym.operating_hours is not None and len(gym.operating_hours) > 0 if gym.operating_hours else False

    # 3. Check gym_verification_documents table
    doc_stmt = select(GymVerificationDocument).where(GymVerificationDocument.gym_id == gym_id)
    doc_result = await async_db.execute(doc_stmt)
    verification_doc = doc_result.scalar_one_or_none()

    # Agreement status
    agreement_completed = verification_doc.agreement if verification_doc and verification_doc.agreement else False

    # Pancard status (pan_url)
    pancard_completed = verification_doc.pan_url is not None and len(verification_doc.pan_url) > 0 if verification_doc else False

    # Passbook status (bankbook_url)
    passbook_completed = verification_doc.bankbook_url is not None and len(verification_doc.bankbook_url) > 0 if verification_doc else False

    # 4. Check gym_onboarding_pics table
    pics_stmt = select(GymOnboardingPics).where(GymOnboardingPics.gym_id == gym_id)
    pics_result = await async_db.execute(pics_stmt)
    onboarding_pics = pics_result.scalar_one_or_none()

    # Build documents list with pancard and passbook only
    documents = [
        {"pancard": pancard_completed},
        {"passbook": passbook_completed}
    ]

    # Build onboarding pics list separately
    onboarding_pics_status = []
    if onboarding_pics:
        pic_columns = [
            "machinery_1",
            "machinery_2",
            "treadmill_area",
            "cardio_area",
            "dumbell_area",
            "reception_area"
        ]
        for col in pic_columns:
            value = getattr(onboarding_pics, col, None)
            onboarding_pics_status.append({
                col: value is not None and len(value) > 0 if value else False
            })
    else:
        onboarding_pics_status = [
            {"machinery_1": False},
            {"machinery_2": False},
            {"treadmill_area": False},
            {"cardio_area": False},
            {"dumbell_area": False},
            {"reception_area": False}
        ]

    # Return registration_steps directly, not wrapped in another dict
    return {
        "account_details": account_details_completed,
        "services": services_completed,
        "operating_hours": operating_hours_completed,
        "agreement": agreement_completed,
        "documents": documents,
        "onboarding_pics": onboarding_pics_status
    }


async def _get_registration_steps_batch(
    gym_ids: List[int], async_db: AsyncSession
) -> Dict[int, Dict[str, Any]]:
    """
    Get registration steps for multiple gyms in batch queries.
    Returns a dictionary mapping gym_id to registration_steps dict.
    Optimized to avoid N+1 query problems.
    """
    if not gym_ids:
        return {}

    result = {}

    # 1. Batch fetch account details
    account_stmt = select(AccountDetails.gym_id).where(
        AccountDetails.gym_id.in_(gym_ids)
    )
    account_result = await async_db.execute(account_stmt)
    account_gyms = {row[0] for row in account_result.fetchall()}

    # 2. Batch fetch gym data (services, operating_hours)
    gym_stmt = select(
        Gym.gym_id,
        Gym.services,
        Gym.operating_hours
    ).where(Gym.gym_id.in_(gym_ids))
    gym_result = await async_db.execute(gym_stmt)
    gym_data = {}
    for row in gym_result.fetchall():
        gym_data[row[0]] = {
            "services": row[1],
            "operating_hours": row[2]
        }

    # 3. Batch fetch verification documents
    doc_stmt = select(
        GymVerificationDocument.gym_id,
        GymVerificationDocument.agreement,
        GymVerificationDocument.pan_url,
        GymVerificationDocument.bankbook_url
    ).where(GymVerificationDocument.gym_id.in_(gym_ids))
    doc_result = await async_db.execute(doc_stmt)
    doc_data = {}
    for row in doc_result.fetchall():
        doc_data[row[0]] = {
            "agreement": row[1],
            "pan_url": row[2],
            "bankbook_url": row[3]
        }

    # 4. Batch fetch onboarding pics
    pics_stmt = select(
        GymOnboardingPics.gym_id,
        GymOnboardingPics.machinery_1,
        GymOnboardingPics.machinery_2,
        GymOnboardingPics.treadmill_area,
        GymOnboardingPics.cardio_area,
        GymOnboardingPics.dumbell_area,
        GymOnboardingPics.reception_area
    ).where(GymOnboardingPics.gym_id.in_(gym_ids))
    pics_result = await async_db.execute(pics_stmt)
    pics_data = {}
    for row in pics_result.fetchall():
        pics_data[row[0]] = {
            "machinery_1": row[1],
            "machinery_2": row[2],
            "treadmill_area": row[3],
            "cardio_area": row[4],
            "dumbell_area": row[5],
            "reception_area": row[6]
        }

    # Build response for each gym
    for gym_id in gym_ids:
        # Account details
        account_details_completed = gym_id in account_gyms

        # Services and operating hours
        gym_info = gym_data.get(gym_id, {})
        services_completed = (
            gym_info.get("services") is not None and
            len(gym_info.get("services", "")) > 0
        )
        operating_hours_completed = (
            gym_info.get("operating_hours") is not None and
            len(gym_info.get("operating_hours", "")) > 0
        )

        # Documents
        doc_info = doc_data.get(gym_id, {})
        agreement_completed = doc_info.get("agreement", False) or False
        pancard_completed = (
            doc_info.get("pan_url") is not None and
            len(doc_info.get("pan_url", "")) > 0
        )
        passbook_completed = (
            doc_info.get("bankbook_url") is not None and
            len(doc_info.get("bankbook_url", "")) > 0
        )

        documents = [
            {"pancard": pancard_completed},
            {"passbook": passbook_completed}
        ]

        # Onboarding pics
        pics_info = pics_data.get(gym_id, {})
        pic_columns = [
            "machinery_1", "machinery_2", "treadmill_area",
            "cardio_area", "dumbell_area", "reception_area"
        ]
        onboarding_pics_status = []
        for col in pic_columns:
            value = pics_info.get(col)
            onboarding_pics_status.append({
                col: value is not None and len(value) > 0 if value else False
            })

        result[gym_id] = {
            "account_details": account_details_completed,
            "services": services_completed,
            "operating_hours": operating_hours_completed,
            "agreement": agreement_completed,
            "documents": documents,
            "onboarding_pics": onboarding_pics_status
        }

    return result


def calculate_registration_completion_percentage(registration_steps: Dict[str, Any]) -> float:
    """
    Calculate registration completion percentage.
    Base 40% + 10% for each completed step (max 100%).
    """
    if not registration_steps:
        return 40.0

    true_count = 0

    # Check main boolean fields
    if registration_steps.get("account_details"):
        true_count += 1
    if registration_steps.get("services"):
        true_count += 1
    if registration_steps.get("operating_hours"):
        true_count += 1
    if registration_steps.get("agreement"):
        true_count += 1

    # Check documents array - add 10% only if both documents are completed
    documents = registration_steps.get("documents", [])
    if documents:
        completed_docs_count = sum(
            1 for doc in documents
            if any(v is True for v in doc.values())
        )
        if completed_docs_count >= 2:
            true_count += 1

    # Check onboarding_pics array - add 10% only if at least 3 pics are completed
    onboarding_pics = registration_steps.get("onboarding_pics", [])
    if onboarding_pics:
        completed_pics_count = sum(
            1 for pic in onboarding_pics
            if any(v is True for v in pic.values())
        )
        if completed_pics_count >= 3:
            true_count += 1

    # Base 40% + 10% for each True value, max 100%
    return min(100.0, 40.0 + (true_count * 10.0))
