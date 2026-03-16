from typing import Dict, Any, List, Union, Optional
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy import select, text, and_, update, delete, or_, func
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Session

from app.models.async_database import get_async_db
from app.models.database import get_db
from app.models.fittbot_models import (
    Gym,
    GymOwner,
    AccountDetails,
    GymVerificationDocument,
    GymOnboardingPics,
    GymPrefilledAgreement,
    GymLocation,
)
from app.telecaller.dependencies import get_current_manager, get_current_telecaller
from app.models.telecaller_models import Manager, Telecaller, GymAssignment, ConvertedBy
from typing import List, Dict, Any, Optional

router = APIRouter(prefix="/status", tags=["Telecaller Status"])


# Pydantic models for pagination
class PaginatedGymResponse(BaseModel):
    status: int
    message: str
    data: List[Dict[str, Any]]
    pagination: Dict[str, Any]


@router.get("/gym-plans/{gym_id}")
async def get_gym_plans(
    gym_id: int,
    async_db: AsyncSession = Depends(get_async_db),
    current_manager: Manager = Depends(get_current_manager)
):
    """
    Get plans data for a specific gym (Daily Pass, Sessions, Gym Plans)
    """
    try:
        plans_data = await get_plans_data(gym_id, async_db)
        return {
            "status": 200,
            "message": "Successfully retrieved gym plans",
            "data": plans_data
        }
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch gym plans: {str(e)}"
        )


@router.get("/telecaller/gym-plans/{gym_id}")
async def get_telecaller_gym_plans(
    gym_id: int,
    async_db: AsyncSession = Depends(get_async_db),
    current_telecaller: Telecaller = Depends(get_current_telecaller)
):
    """
    Get plans data for a specific gym (Daily Pass, Sessions, Gym Plans) - Telecaller endpoint
    """
    try:
        
        plans_data = await get_plans_data(gym_id, async_db)
        return {
            "status": 200,
            "message": "Successfully retrieved gym plans",
            "data": plans_data
        }
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch gym plans: {str(e)}"
        )


async def get_plans_data(gym_id: int, async_db: AsyncSession) -> Dict[str, Any]:

    daily_pass_query = text("""
        SELECT id, gym_id, price, discount_price, discount_percentage
        FROM dailypass.dailypass_pricing
        WHERE gym_id = :gym_id
    """)

    daily_pass_result = await async_db.execute(daily_pass_query, {"gym_id": str(gym_id)})
    daily_pass_rows = daily_pass_result.fetchall()

    daily_pass_data = []
    for row in daily_pass_rows:
        daily_pass_data.append({
            "id": row[0],
            "gym_id": row[1],
            "price": row[2],
            "discount_price": row[3],
            "discount_percentage": row[4]
        })

    # 2. Fetch Sessions data from sessions schema
    sessions_query = text("""
        SELECT COUNT(*) as total_sessions
        FROM sessions.session_settings
        WHERE gym_id = :gym_id AND is_enabled = 1
    """)
    sessions_result = await async_db.execute(sessions_query, {"gym_id": gym_id})
    sessions_count = sessions_result.scalar_one_or_none() or 0


    lowest_session_price = None
    try:
        sessions_price_query = text("""
            SELECT final_price
            FROM sessions.session_settings
            WHERE gym_id = :gym_id AND is_enabled = 1 AND final_price IS NOT NULL
            ORDER BY final_price ASC
            LIMIT 1
        """)
        sessions_price_result = await async_db.execute(sessions_price_query, {"gym_id": gym_id})
        lowest_session_price = sessions_price_result.scalar_one_or_none()
    except Exception as e:

        lowest_session_price = None

   
    gym_plans_query = text("""
        SELECT COUNT(*) as total_plans
        FROM fittbot.gym_plans
        WHERE gym_id = :gym_id
    """)
    gym_plans_result = await async_db.execute(gym_plans_query, {"gym_id": gym_id})
    gym_plans_count = gym_plans_result.scalar_one_or_none() or 0

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


@router.get("/gyms-registration-status")
async def get_gyms_registration_status(
    page: int = Query(1, ge=1, description="Page number"),
    limit: int = Query(10, ge=1, le=100, description="Items per page"),
    search: Optional[str] = Query(None, description="Search by gym name, location, or phone number"),
    async_db: AsyncSession = Depends(get_async_db),
    current_manager: Manager = Depends(get_current_manager)
):
    """
    Get gyms registration status with pagination and search.
    Optimized with batch queries to avoid N+1 problem.
    """
    try:
        offset = (page - 1) * limit

        # Build WHERE clause for search
        search_clause = ""
        params = {"limit": limit, "offset": offset}
        if search:
            search_clause = "AND (g.name LIKE :search OR g.location LIKE :search OR g.contact_number LIKE :search OR g.owner_id IN (SELECT owner_id FROM gym_owners WHERE contact_number LIKE :search))"
            params["search"] = f"%{search}%"

        # Get total count for pagination
        count_query = text(f"""
            SELECT COUNT(*)
            FROM gyms g
            WHERE g.gym_id > 214 {search_clause}
        """)
        count_result = await async_db.execute(count_query, params)
        total_count = count_result.scalar_one_or_none() or 0

        if total_count == 0:
            return {
                "status": 200,
                "message": "No gyms found",
                "data": [],
                "pagination": {
                    "total": 0,
                    "limit": limit,
                    "page": page,
                    "totalPages": 0,
                    "hasNext": False,
                    "hasPrev": False
                }
            }

        # Fetch gyms for current page
        gym_query = text(f"""
            SELECT g.gym_id, g.owner_id, g.name, g.location
            FROM gyms g
            WHERE g.gym_id > 214 {search_clause}
            ORDER BY g.created_at DESC
            LIMIT :limit OFFSET :offset
        """)
        result = await async_db.execute(gym_query, params)
        gyms = result.fetchall()

        if not gyms:
            return {
                "status": 200,
                "message": "No gyms found",
                "data": [],
                "pagination": {
                    "total": total_count,
                    "limit": limit,
                    "page": page,
                    "totalPages": (total_count + limit - 1) // limit,
                    "hasNext": False,
                    "hasPrev": page > 1
                }
            }

        gym_ids = [gym[0] for gym in gyms]
        owner_ids = [gym[1] for gym in gyms]

        # Batch fetch all owner contacts
        owner_placeholders = ','.join([f':owner{i}' for i in range(len(owner_ids))])
        owner_params = {f'owner{i}': oid for i, oid in enumerate(owner_ids)}
        owner_query = text(f"""
            SELECT owner_id, contact_number
            FROM gym_owners
            WHERE owner_id IN ({owner_placeholders})
        """)
        owner_result = await async_db.execute(owner_query, owner_params)
        owner_contacts = {row[0]: row[1] for row in owner_result.fetchall()}

        # Batch fetch all converted_by data
        gym_placeholders = ','.join([f':gym{i}' for i in range(len(gym_ids))])
        gym_params = {f'gym{i}': gid for i, gid in enumerate(gym_ids)}
        converted_by_query = text(f"""
            SELECT gym_id, telecaller_id
            FROM telecaller.converted_by
            WHERE gym_id IN ({gym_placeholders})
        """)
        converted_by_result = await async_db.execute(converted_by_query, gym_params)
        converted_by_map = {row[0]: row[1] for row in converted_by_result.fetchall()}

        # Fetch telecaller names for converted_by
        telecaller_ids = list(set(converted_by_map.values())) if converted_by_map else []
        telecaller_names = {}
        if telecaller_ids:
            tc_placeholders = ','.join([f':tc{i}' for i in range(len(telecaller_ids))])
            tc_params = {f'tc{i}': tid for i, tid in enumerate(telecaller_ids)}
            telecaller_query = text(f"""
                SELECT id, name
                FROM telecaller.telecallers
                WHERE id IN ({tc_placeholders})
            """)
            telecaller_result = await async_db.execute(telecaller_query, tc_params)
            telecaller_names = {row[0]: row[1] for row in telecaller_result.fetchall()}

        # Batch fetch all registration steps data
        registration_steps_batch = await get_registration_steps_batch(gym_ids, async_db)

        # Batch fetch all plans completion scores
        plans_scores_batch = await get_plans_completion_scores_batch(gym_ids, async_db)

        # Build response
        gyms_data = []
        for gym in gyms:
            gym_id, owner_id, name, location = gym

            gym_info = {
                "gym_id": gym_id,
                "gym_name": name,
                "owner_contact_number": owner_contacts.get(owner_id),
                "location": location,
                "registration_steps": registration_steps_batch.get(gym_id, {}),
                "plans_completion_score": plans_scores_batch.get(gym_id, 0.0)
            }

            # Add converted_by data
            converted_by_telecaller_id = converted_by_map.get(gym_id)
            if converted_by_telecaller_id:
                gym_info["converted_by"] = {
                    "telecaller_id": converted_by_telecaller_id,
                    "telecaller_name": telecaller_names.get(converted_by_telecaller_id)
                }
            else:
                gym_info["converted_by"] = None

            gyms_data.append(gym_info)

        total_pages = (total_count + limit - 1) // limit
        has_next = page < total_pages
        has_prev = page > 1

        return {
            "status": 200,
            "message": f"Successfully retrieved {len(gyms_data)} gyms",
            "data": gyms_data,
            "pagination": {
                "total": total_count,
                "limit": limit,
                "page": page,
                "totalPages": total_pages,
                "hasNext": has_next,
                "hasPrev": has_prev
            }
        }

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch gyms registration status: {str(e)}"
        )


@router.get("/telecaller/gyms-registration-status")
async def get_telecaller_gyms_registration_status(
    page: int = Query(1, ge=1, description="Page number"),
    limit: int = Query(10, ge=1, le=100, description="Items per page"),
    search: Optional[str] = Query(None, description="Search by gym name, location, or phone number"),
    async_db: AsyncSession = Depends(get_async_db),
    current_telecaller: Telecaller = Depends(get_current_telecaller)
):
    """
    Get gyms registration status with pagination and search - Telecaller endpoint.
    Optimized with batch queries to avoid N+1 problem.
    """
    try:
        offset = (page - 1) * limit

        # Build WHERE clause for search
        search_clause = ""
        params = {"limit": limit, "offset": offset}
        if search:
            search_clause = "AND (g.name LIKE :search OR g.location LIKE :search OR g.contact_number LIKE :search OR g.owner_id IN (SELECT owner_id FROM gym_owners WHERE contact_number LIKE :search))"
            params["search"] = f"%{search}%"

        # Get total count for pagination
        count_query = text(f"""
            SELECT COUNT(*)
            FROM gyms g
            WHERE g.gym_id > 214 {search_clause}
        """)
        count_result = await async_db.execute(count_query, params)
        total_count = count_result.scalar_one_or_none() or 0

        if total_count == 0:
            return {
                "status": 200,
                "message": "No gyms found",
                "data": [],
                "pagination": {
                    "total": 0,
                    "limit": limit,
                    "page": page,
                    "totalPages": 0,
                    "hasNext": False,
                    "hasPrev": False
                }
            }

        # Fetch gyms for current page
        gym_query = text(f"""
            SELECT g.gym_id, g.owner_id, g.name, g.location
            FROM gyms g
            WHERE g.gym_id > 214 {search_clause}
            ORDER BY g.created_at DESC
            LIMIT :limit OFFSET :offset
        """)
        result = await async_db.execute(gym_query, params)
        gyms = result.fetchall()

        if not gyms:
            return {
                "status": 200,
                "message": "No gyms found",
                "data": [],
                "pagination": {
                    "total": total_count,
                    "limit": limit,
                    "page": page,
                    "totalPages": (total_count + limit - 1) // limit,
                    "hasNext": False,
                    "hasPrev": page > 1
                }
            }

        gym_ids = [gym[0] for gym in gyms]
        owner_ids = [gym[1] for gym in gyms]

        # Batch fetch all owner contacts
        owner_placeholders = ','.join([f':owner{i}' for i in range(len(owner_ids))])
        owner_params = {f'owner{i}': oid for i, oid in enumerate(owner_ids)}
        owner_query = text(f"""
            SELECT owner_id, contact_number
            FROM gym_owners
            WHERE owner_id IN ({owner_placeholders})
        """)
        owner_result = await async_db.execute(owner_query, owner_params)
        owner_contacts = {row[0]: row[1] for row in owner_result.fetchall()}

        # Batch fetch all converted_by data
        gym_placeholders = ','.join([f':gym{i}' for i in range(len(gym_ids))])
        gym_params = {f'gym{i}': gid for i, gid in enumerate(gym_ids)}
        converted_by_query = text(f"""
            SELECT gym_id, telecaller_id
            FROM telecaller.converted_by
            WHERE gym_id IN ({gym_placeholders})
        """)
        converted_by_result = await async_db.execute(converted_by_query, gym_params)
        converted_by_map = {row[0]: row[1] for row in converted_by_result.fetchall()}

        # Fetch telecaller names for converted_by
        telecaller_ids = list(set(converted_by_map.values())) if converted_by_map else []
        telecaller_names = {}
        if telecaller_ids:
            tc_placeholders = ','.join([f':tc{i}' for i in range(len(telecaller_ids))])
            tc_params = {f'tc{i}': tid for i, tid in enumerate(telecaller_ids)}
            telecaller_query = text(f"""
                SELECT id, name
                FROM telecaller.telecallers
                WHERE id IN ({tc_placeholders})
            """)
            telecaller_result = await async_db.execute(telecaller_query, tc_params)
            telecaller_names = {row[0]: row[1] for row in telecaller_result.fetchall()}

        # Batch fetch all registration steps data
        registration_steps_batch = await get_registration_steps_batch(gym_ids, async_db)

        # Batch fetch all plans completion scores
        plans_scores_batch = await get_plans_completion_scores_batch(gym_ids, async_db)

        # Build response
        gyms_data = []
        for gym in gyms:
            gym_id, owner_id, name, location = gym

            gym_info = {
                "gym_id": gym_id,
                "gym_name": name,
                "owner_contact_number": owner_contacts.get(owner_id),
                "location": location,
                "registration_steps": registration_steps_batch.get(gym_id, {}),
                "plans_completion_score": plans_scores_batch.get(gym_id, 0.0)
            }

            # Add converted_by data
            converted_by_telecaller_id = converted_by_map.get(gym_id)
            if converted_by_telecaller_id:
                gym_info["converted_by"] = {
                    "telecaller_id": converted_by_telecaller_id,
                    "telecaller_name": telecaller_names.get(converted_by_telecaller_id)
                }
            else:
                gym_info["converted_by"] = None

            gyms_data.append(gym_info)

        total_pages = (total_count + limit - 1) // limit
        has_next = page < total_pages
        has_prev = page > 1

        return {
            "status": 200,
            "message": f"Successfully retrieved {len(gyms_data)} gyms",
            "data": gyms_data,
            "pagination": {
                "total": total_count,
                "limit": limit,
                "page": page,
                "totalPages": total_pages,
                "hasNext": has_next,
                "hasPrev": has_prev
            }
        }

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch gyms registration status: {str(e)}"
        )


@router.get("/gym-registration-status/{gym_id}")
async def get_single_gym_registration_status(
    gym_id: int,
    async_db: AsyncSession = Depends(get_async_db),
    current_manager: Manager = Depends(get_current_manager)
):
    """
    Get registration steps for a specific gym by gym_id.
    """
    try:
        # Fetch gym by gym_id using raw SQL
        gym_query = text("""
            SELECT gym_id, owner_id, name, location, created_at
            FROM gyms
            WHERE gym_id = :gym_id
        """)
        result = await async_db.execute(gym_query, {"gym_id": gym_id})
        gym = result.fetchone()

        if not gym:
            raise HTTPException(
                status_code=404,
                detail=f"Gym with gym_id {gym_id} not found"
            )

        gym_id_val, owner_id, name, location, created_at = gym

        # Get owner contact number
        owner_query = text("""
            SELECT contact_number, email
            FROM gym_owners
            WHERE owner_id = :owner_id
        """)
        owner_result = await async_db.execute(owner_query, {"owner_id": owner_id})
        owner = owner_result.fetchone()

        # Get registration steps for this gym
        registration_steps = await get_registration_steps(gym_id, async_db)

        gym_info = {
            "gym_id": gym_id_val,
            "gym_name": name,
            "owner_id": owner_id,
            "owner_contact_number": owner[0] if owner else None,
            "owner_email": owner[1] if owner else None,
            "location": location,
            "contact_number": None,
            "created_at": created_at.isoformat() if created_at else None,
            "registration_steps": registration_steps
        }

        return {
            "status": 200,
            "message": "Successfully retrieved gym registration status",
            "data": gym_info
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch gym registration status: {str(e)}"
        )

async def get_plans_completion_score(
    gym_id: int, async_db: AsyncSession
) -> float:
    """
    Get plans completion score for a gym (Daily Pass, Sessions, Gym Plans)
    Returns the completion percentage (0-100)
    """
    try:
        # 1. Check Daily Pass
        daily_pass_query = text("""
            SELECT COUNT(*) as count
            FROM dailypass.dailypass_pricing
            WHERE gym_id = :gym_id
        """)
        daily_pass_result = await async_db.execute(daily_pass_query, {"gym_id": str(gym_id)})
        daily_pass_count = daily_pass_result.scalar_one_or_none() or 0
        daily_pass_score = 33.33 if daily_pass_count > 0 else 0

        # 2. Check Sessions
        sessions_query = text("""
            SELECT COUNT(*) as count
            FROM sessions.session_settings
            WHERE gym_id = :gym_id AND is_enabled = 1
        """)
        sessions_result = await async_db.execute(sessions_query, {"gym_id": gym_id})
        sessions_count = sessions_result.scalar_one_or_none() or 0
        sessions_score = 33.33 if sessions_count > 0 else 0

        # 3. Check Gym Plans
        gym_plans_query = text("""
            SELECT COUNT(*) as count
            FROM fittbot.gym_plans
            WHERE gym_id = :gym_id
        """)
        gym_plans_result = await async_db.execute(gym_plans_query, {"gym_id": gym_id})
        gym_plans_count = gym_plans_result.scalar_one_or_none() or 0
        gym_plans_score = 33.34 if gym_plans_count > 0 else 0

        # Calculate total score
        total_score = daily_pass_score + sessions_score + gym_plans_score
        return round(total_score, 2)
    except Exception as e:
        # print(f"Error fetching plans score for gym {gym_id}: {str(e)}")
        return 0.0


async def get_plans_completion_scores_batch(
    gym_ids: List[int], async_db: AsyncSession
) -> Dict[int, float]:
    """
    Get plans completion scores for multiple gyms in a single batch query.
    Returns a dictionary mapping gym_id to completion score.
    """
    if not gym_ids:
        return {}

    try:
        gym_placeholders = ','.join([f':gym{i}' for i in range(len(gym_ids))])
        gym_params = {f'gym{i}': gid for i, gid in enumerate(gym_ids)}

        # Batch query for daily pass counts
        # dailypass_pricing.gym_id is a varchar column, so pass string params and convert keys to int
        daily_pass_str_params = {f'gym{i}': str(gid) for i, gid in enumerate(gym_ids)}
        daily_pass_query = text(f"""
            SELECT gym_id, COUNT(*) as count
            FROM dailypass.dailypass_pricing
            WHERE gym_id IN ({gym_placeholders})
            GROUP BY gym_id
        """)
        daily_pass_result = await async_db.execute(daily_pass_query, daily_pass_str_params)
        daily_pass_counts = {int(row[0]): row[1] for row in daily_pass_result.fetchall()}

        # Batch query for session counts
        sessions_query = text(f"""
            SELECT gym_id, COUNT(*) as count
            FROM sessions.session_settings
            WHERE gym_id IN ({gym_placeholders}) AND is_enabled = 1
            GROUP BY gym_id
        """)
        sessions_result = await async_db.execute(sessions_query, gym_params)
        sessions_counts = {row[0]: row[1] for row in sessions_result.fetchall()}

        # Batch query for gym plan counts
        gym_plans_query = text(f"""
            SELECT gym_id, COUNT(*) as count
            FROM fittbot.gym_plans
            WHERE gym_id IN ({gym_placeholders})
            GROUP BY gym_id
        """)
        gym_plans_result = await async_db.execute(gym_plans_query, gym_params)
        gym_plans_counts = {row[0]: row[1] for row in gym_plans_result.fetchall()}

        # Calculate scores for each gym
        scores = {}
        for gym_id in gym_ids:
            daily_pass_score = 33.33 if daily_pass_counts.get(gym_id, 0) > 0 else 0
            sessions_score = 33.33 if sessions_counts.get(gym_id, 0) > 0 else 0
            gym_plans_score = 33.34 if gym_plans_counts.get(gym_id, 0) > 0 else 0
            scores[gym_id] = round(daily_pass_score + sessions_score + gym_plans_score, 2)

        return scores
    except Exception as e:
        # print(f"Error fetching batch plans scores: {str(e)}")
        return {gym_id: 0.0 for gym_id in gym_ids}


async def get_registration_steps(
    gym_id: int, async_db: AsyncSession
) -> Dict[str, Any]:
    """Get registration document steps status for a gym"""
    response = {}

    # 1. Check account_details table
    stmt = select(AccountDetails).where(AccountDetails.gym_id == gym_id)
    result = await async_db.execute(stmt)
    account_details = result.scalar_one_or_none()
    account_details_completed = account_details is not None

    # 2. Check gyms table for services and operating_hours
    stmt = select(Gym).where(Gym.gym_id == gym_id)
    result = await async_db.execute(stmt)
    gym = result.scalar_one_or_none()

    services_completed = False
    operating_hours_completed = False
    if gym:
        services_completed = gym.services is not None and len(gym.services) > 0 if gym.services else False
        operating_hours_completed = gym.operating_hours is not None and len(gym.operating_hours) > 0 if gym.operating_hours else False

    # 2.5. Check gym_location table for gym_pic (for gyms > 470)
    gym_location_completed = False
    if gym_id > 470:
        stmt = select(GymLocation).where(GymLocation.gym_id == gym_id)
        result = await async_db.execute(stmt)
        gym_location = result.scalar_one_or_none()
        gym_location_completed = gym_location is not None and gym_location.gym_pic is not None and len(gym_location.gym_pic) > 0

    # 3. Check gym_verification_documents table
    stmt = select(GymVerificationDocument).where(GymVerificationDocument.gym_id == gym_id)
    result = await async_db.execute(stmt)
    verification_doc = result.scalar_one_or_none()

    # Agreement status
    agreement_completed = verification_doc.agreement if verification_doc and verification_doc.agreement else False

    # Pancard status (pan_url)
    pancard_completed = verification_doc.pan_url is not None and len(verification_doc.pan_url) > 0 if verification_doc else False

    # Passbook status (bankbook_url)
    passbook_completed = verification_doc.bankbook_url is not None and len(verification_doc.bankbook_url) > 0 if verification_doc else False

    # 4. Check gym_onboarding_pics table
    stmt = select(GymOnboardingPics).where(GymOnboardingPics.gym_id == gym_id)
    result = await async_db.execute(stmt)
    onboarding_pics = result.scalar_one_or_none()

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

    # For gyms <= 470: use services, for gyms > 470: use gym_location
    if gym_id <= 470:
        response["registration_steps"] = {
            "account_details": account_details_completed,
            "services": services_completed,
            "operating_hours": operating_hours_completed,
            "agreement": agreement_completed,
            "documents": documents,
            "onboarding_pics": onboarding_pics_status
        }
    else:
        response["registration_steps"] = {
            "account_details": account_details_completed,
            "gym_location": gym_location_completed,
            "operating_hours": operating_hours_completed,
            "agreement": agreement_completed,
            "documents": documents,
            "onboarding_pics": onboarding_pics_status
        }

    return response


async def get_registration_steps_batch(
    gym_ids: List[int], async_db: AsyncSession
) -> Dict[int, Dict[str, Any]]:
    """
    Get registration steps for multiple gyms in batch queries.
    Returns a dictionary mapping gym_id to registration_steps dict.
    Optimized to avoid N+1 query problems.
    """
    if not gym_ids:
        return {}

    gym_placeholders = ','.join([f':gym{i}' for i in range(len(gym_ids))])
    gym_params = {f'gym{i}': gid for i, gid in enumerate(gym_ids)}

    result = {}

    # 1. Batch fetch account details (no schema prefix)
    account_query = text(f"""
        SELECT gym_id
        FROM account_details
        WHERE gym_id IN ({gym_placeholders})
    """)
    account_result = await async_db.execute(account_query, gym_params)
    account_gyms = {row[0] for row in account_result.fetchall()}

    # 2. Batch fetch gym data (services, operating_hours)
    gym_query = text(f"""
        SELECT gym_id, services, operating_hours
        FROM gyms
        WHERE gym_id IN ({gym_placeholders})
    """)
    gym_result = await async_db.execute(gym_query, gym_params)
    gym_data = {}
    for row in gym_result.fetchall():
        gym_data[row[0]] = {
            "services": row[1],
            "operating_hours": row[2]
        }

    # 3. Batch fetch verification documents (no schema prefix)
    doc_query = text(f"""
        SELECT gym_id, agreement, pan_url, bankbook_url
        FROM gym_verification_documents
        WHERE gym_id IN ({gym_placeholders})
    """)
    doc_result = await async_db.execute(doc_query, gym_params)
    doc_data = {}
    for row in doc_result.fetchall():
        doc_data[row[0]] = {
            "agreement": row[1],
            "pan_url": row[2],
            "bankbook_url": row[3]
        }

    # 4. Batch fetch onboarding pics (no schema prefix)
    pics_query = text(f"""
        SELECT gym_id, machinery_1, machinery_2, treadmill_area,
               cardio_area, dumbell_area, reception_area
        FROM gym_onboarding_pics
        WHERE gym_id IN ({gym_placeholders})
    """)
    pics_result = await async_db.execute(pics_query, gym_params)
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

    # 5. Batch fetch gym_location data for gym_pic (for gyms > 470)
    location_query = text(f"""
        SELECT gym_id, gym_pic
        FROM gym_location
        WHERE gym_id IN ({gym_placeholders})
    """)
    location_result = await async_db.execute(location_query, gym_params)
    location_data = {}
    for row in location_result.fetchall():
        location_data[row[0]] = row[1]

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

        # Gym pic (for gyms > 470)
        gym_pic_value = location_data.get(gym_id, "")
        gym_pic_completed = gym_pic_value is not None and len(gym_pic_value) > 0

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

        # For gyms <= 470: use services, for gyms > 470: use gym_location
        if gym_id <= 470:
            result[gym_id] = {
                "account_details": account_details_completed,
                "services": services_completed,
                "operating_hours": operating_hours_completed,
                "agreement": agreement_completed,
                "documents": documents,
                "onboarding_pics": onboarding_pics_status
            }
        else:
            result[gym_id] = {
                "account_details": account_details_completed,
                "gym_location": gym_pic_completed,
                "operating_hours": operating_hours_completed,
                "agreement": agreement_completed,
                "documents": documents,
                "onboarding_pics": onboarding_pics_status
            }

    return result


# Pydantic models for ConvertedBy API
class SetConvertedByRequest(BaseModel):
    gym_id: int
    telecaller_id: int


class ConvertedByResponse(BaseModel):
    id: int
    gym_id: int
    telecaller_id: int
    telecaller_name: Optional[str]=None
    created_at: str
    updated_at: str


@router.get("/converted-by")
async def get_all_converted_by(
    async_db: AsyncSession = Depends(get_async_db),
    current_manager: Manager = Depends(get_current_manager)
):
    """
    Get all converted_by mappings for the manager's team
    """
    try:
        # Get all telecallers under this manager
        telecaller_query = text("""
            SELECT id, name
            FROM telecaller.telecallers
            WHERE manager_id = :manager_id AND status = 'active'
        """)
        result = await async_db.execute(telecaller_query, {"manager_id": current_manager.id})
        telecallers = result.fetchall()
        telecaller_map = {t[0]: t[1] for t in telecallers}

        # Get all converted_by records for gyms
        converted_by_query = text("""
            SELECT id, gym_id, telecaller_id, created_at, updated_at
            FROM telecaller.converted_by
            WHERE telecaller_id IS NOT NULL
        """)
        result = await async_db.execute(converted_by_query)
        converted_by_records = result.fetchall()

        converted_by_data = []
        for record in converted_by_records:
            converted_by_data.append({
                "id": record[0],
                "gym_id": record[1],
                "telecaller_id": record[2],
                "telecaller_name": telecaller_map.get(record[2]),
                "created_at": record[3].isoformat() if record[3] else None,
                "updated_at": record[4].isoformat() if record[4] else None
            })

        return {
            "status": 200,
            "message": "Successfully retrieved converted_by data",
            "data": converted_by_data
        }
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch converted_by data: {str(e)}"
        )


@router.get("/converted-by/{gym_id}")
async def get_converted_by_gym(
    gym_id: int,
    async_db: AsyncSession = Depends(get_async_db),
    current_manager: Manager = Depends(get_current_manager)
):
    """
    Get converted_by data for a specific gym
    """
    try:
        query = text("""
            SELECT id, gym_id, telecaller_id, created_at, updated_at
            FROM telecaller.converted_by
            WHERE gym_id = :gym_id
        """)
        result = await async_db.execute(query, {"gym_id": gym_id})
        record = result.fetchone()

        if not record:
            return {
                "status": 200,
                "message": "No converted_by data found for this gym",
                "data": None
            }

        # Get telecaller name
        telecaller_name = None
        if record[2]:
            telecaller_query = text("""
                SELECT name
                FROM telecaller.telecallers
                WHERE id = :telecaller_id
            """)
            telecaller_result = await async_db.execute(telecaller_query, {"telecaller_id": record[2]})
            telecaller_name = telecaller_result.scalar_one_or_none()

        return {
            "status": 200,
            "message": "Successfully retrieved converted_by data",
            "data": {
                "id": record[0],
                "gym_id": record[1],
                "telecaller_id": record[2],
                "telecaller_name": telecaller_name,
                "created_at": record[3].isoformat() if record[3] else None,
                "updated_at": record[4].isoformat() if record[4] else None
            }
        }
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch converted_by data: {str(e)}"
        )


@router.post("/converted-by")
async def set_converted_by(
    data: SetConvertedByRequest,
    async_db: AsyncSession = Depends(get_async_db),
    current_manager: Manager = Depends(get_current_manager)
):
    """
    Set or update the telecaller who converted a gym
    """
    try:
        # Verify telecaller exists and is active
        telecaller_query = text("""
            SELECT id, name
            FROM telecaller.telecallers
            WHERE id = :telecaller_id AND status = 'active'
        """)
        result = await async_db.execute(telecaller_query, {
            "telecaller_id": data.telecaller_id
        })
        telecaller = result.fetchone()

        if not telecaller:
            raise HTTPException(
                status_code=400,
                detail="Invalid telecaller"
            )

        # Check if converted_by record already exists for this gym
        check_query = text("""
            SELECT id FROM telecaller.converted_by WHERE gym_id = :gym_id
        """)
        check_result = await async_db.execute(check_query, {"gym_id": data.gym_id})
        existing_record = check_result.fetchone()

        if existing_record:
            # Update existing record
            update_query = text("""
                UPDATE telecaller.converted_by
                SET telecaller_id = :telecaller_id,
                    updated_at = NOW()
                WHERE gym_id = :gym_id
            """)
            await async_db.execute(update_query, {
                "gym_id": data.gym_id,
                "telecaller_id": data.telecaller_id
            })
            await async_db.commit()
            # Fetch the updated record
            fetch_query = text("""
                SELECT id, gym_id, telecaller_id, created_at, updated_at
                FROM telecaller.converted_by
                WHERE gym_id = :gym_id
            """)
            result = await async_db.execute(fetch_query, {"gym_id": data.gym_id})
            record = result.fetchone()
        else:
            # Insert new record
            insert_query = text("""
                INSERT INTO telecaller.converted_by (gym_id, telecaller_id, created_at, updated_at)
                VALUES (:gym_id, :telecaller_id, NOW(), NOW())
            """)
            await async_db.execute(insert_query, {
                "gym_id": data.gym_id,
                "telecaller_id": data.telecaller_id
            })
            await async_db.commit()
            # Fetch the inserted record
            fetch_query = text("""
                SELECT id, gym_id, telecaller_id, created_at, updated_at
                FROM telecaller.converted_by
                WHERE gym_id = :gym_id
            """)
            result = await async_db.execute(fetch_query, {"gym_id": data.gym_id})
            record = result.fetchone()

        return {
            "status": 200,
            "message": "Successfully set converted_by",
            "data": {
                "id": record[0],
                "gym_id": record[1],
                "telecaller_id": record[2],
                "telecaller_name": telecaller[1],
                "created_at": record[3].isoformat() if record[3] else None,
                "updated_at": record[4].isoformat() if record[4] else None
            }
        }
    except HTTPException:
        raise
    except Exception as e:
        await async_db.rollback()
        raise HTTPException(
            status_code=500,
            detail=f"Failed to set converted_by: {str(e)}"
        )


@router.delete("/converted-by/{gym_id}")
async def delete_converted_by(
    gym_id: int,
    async_db: AsyncSession = Depends(get_async_db),
    current_manager: Manager = Depends(get_current_manager)
):
    """
    Remove converted_by assignment for a gym
    """
    try:
        delete_query = text("""
            DELETE FROM telecaller.converted_by
            WHERE gym_id = :gym_id
            RETURNING id
        """)
        result = await async_db.execute(delete_query, {"gym_id": gym_id})
        deleted_id = result.fetchone()

        await async_db.commit()

        if not deleted_id:
            raise HTTPException(
                status_code=404,
                detail=f"No converted_by record found for gym_id {gym_id}"
            )

        return {
            "status": 200,
            "message": "Successfully deleted converted_by",
            "data": {"deleted_id": deleted_id[0]}
        }
    except HTTPException:
        raise
    except Exception as e:
        await async_db.rollback()
        raise HTTPException(
            status_code=500,
            detail=f"Failed to delete converted_by: {str(e)}"
        )


@router.get("/telecallers")
async def get_team_telecallers(
    async_db: AsyncSession = Depends(get_async_db),
    current_manager: Manager = Depends(get_current_manager)
):

    try:
        query = text("""
            SELECT id, name
            FROM telecaller.telecallers
            WHERE status = 'active'
            ORDER BY name ASC
        """)
        result = await async_db.execute(query)
        telecallers = result.fetchall()

        telecallers_data = [{"id": t[0], "name": t[1]} for t in telecallers]

        return {
            "status": 200,
            "message": "Successfully retrieved telecallers",
            "data": telecallers_data
        }
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch telecallers: {str(e)}"
        )


@router.get("/telecallers/telecaller")
async def get_team_telecallers_for_telecaller(
    async_db: AsyncSession = Depends(get_async_db),
    current_telecaller: Telecaller = Depends(get_current_telecaller)
):
    """
    Get all active telecallers in the system (for dropdown)
    """
    try:
        query = text("""
            SELECT id, name
            FROM telecaller.telecallers
            WHERE status = 'active'
            ORDER BY name ASC
        """)
        result = await async_db.execute(query)
        telecallers = result.fetchall()

        telecallers_data = [{"id": t[0], "name": t[1]} for t in telecallers]

        return {
            "status": 200,
            "message": "Successfully retrieved telecallers",
            "data": telecallers_data
        }
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch telecallers: {str(e)}"
        )


@router.post("/telecaller/converted-by")
async def set_converted_by_telecaller(
    data: SetConvertedByRequest,
    async_db: AsyncSession = Depends(get_async_db),
    current_telecaller: Telecaller = Depends(get_current_telecaller)
):
    """
    Set or update the telecaller who converted a gym - Telecaller endpoint
    """
    try:
        # Verify the telecaller exists and is active
        telecaller_query = text("""
            SELECT id, name
            FROM telecaller.telecallers
            WHERE id = :telecaller_id AND status = 'active'
        """)
        result = await async_db.execute(telecaller_query, {
            "telecaller_id": data.telecaller_id
        })
        telecaller = result.fetchone()

        if not telecaller:
            raise HTTPException(
                status_code=400,
                detail="Invalid telecaller"
            )

        # Check if converted_by record already exists for this gym
        check_query = text("""
            SELECT id FROM telecaller.converted_by WHERE gym_id = :gym_id
        """)
        check_result = await async_db.execute(check_query, {"gym_id": data.gym_id})
        existing_record = check_result.fetchone()

        if existing_record:
            # Update existing record
            update_query = text("""
                UPDATE telecaller.converted_by
                SET telecaller_id = :telecaller_id,
                    updated_at = NOW()
                WHERE gym_id = :gym_id
            """)
            await async_db.execute(update_query, {
                "gym_id": data.gym_id,
                "telecaller_id": data.telecaller_id
            })
            await async_db.commit()
            # Fetch the updated record
            fetch_query = text("""
                SELECT id, gym_id, telecaller_id, created_at, updated_at
                FROM telecaller.converted_by
                WHERE gym_id = :gym_id
            """)
            result = await async_db.execute(fetch_query, {"gym_id": data.gym_id})
            record = result.fetchone()
        else:
            # Insert new record
            insert_query = text("""
                INSERT INTO telecaller.converted_by (gym_id, telecaller_id, created_at, updated_at)
                VALUES (:gym_id, :telecaller_id, NOW(), NOW())
            """)
            await async_db.execute(insert_query, {
                "gym_id": data.gym_id,
                "telecaller_id": data.telecaller_id
            })
            await async_db.commit()
            # Fetch the inserted record
            fetch_query = text("""
                SELECT id, gym_id, telecaller_id, created_at, updated_at
                FROM telecaller.converted_by
                WHERE gym_id = :gym_id
            """)
            result = await async_db.execute(fetch_query, {"gym_id": data.gym_id})
            record = result.fetchone()

        return {
            "status": 200,
            "message": "Successfully set converted_by",
            "data": {
                "id": record[0],
                "gym_id": record[1],
                "telecaller_id": record[2],
                "telecaller_name": telecaller[1],
                "created_at": record[3].isoformat() if record[3] else None,
                "updated_at": record[4].isoformat() if record[4] else None
            }
        }
    except HTTPException:
        raise
    except Exception as e:
        await async_db.rollback()
        raise HTTPException(
            status_code=500,
            detail=f"Failed to set converted_by: {str(e)}"
        )
