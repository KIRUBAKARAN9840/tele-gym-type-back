# Backend Implementation for Users API
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import func, or_, and_, desc, asc, case, literal_column, String, select, over, union_all, cast, DateTime as SQLDateTime, distinct
from typing import Optional, List
from datetime import datetime, timezone, timedelta
from app.models.fittbot_models import (
    Client,
    ClientFittbotAccess,
    FittbotPlans,
    Gym,
    SessionBookingDay,
    SessionBooking,
    SessionPurchase,
    ClassSession,
    FittbotGymMembership,
    ActiveUser,
)
from app.models.dailypass_models import DailyPass, get_dailypass_session
from app.models.async_database import get_async_db
from app.fittbot_api.v1.payments.models.subscriptions import Subscription
from app.fittbot_api.v1.payments.models.payments import Payment
from app.fittbot_api.v1.payments.models.orders import Order, OrderItem
import math
import calendar

# IST timezone
IST = timezone(timedelta(hours=5, minutes=30))

def get_plan_name_from_product_id(product_id: Optional[str]) -> Optional[str]:
    """Map product_id to plan name"""
    if not product_id:
        return None
    product_id_lower = product_id.lower()
    if product_id_lower.startswith("one_month_plan"):
        return "Gold"
    elif product_id_lower.startswith("six_month_plan"):
        return "Platinum"
    elif product_id_lower.startswith("twelve_month_plan"):
        return "Diamond"
    return None

def is_subscription_active(active_until, now) -> bool:
    """Check if subscription is active, handling string, timezone-naive and aware datetimes"""
    if active_until is None:
        return False
    # Handle string dates from database
    if isinstance(active_until, str):
        try:
            # Try parsing ISO format string
            active_until = datetime.fromisoformat(active_until.replace('Z', '+00:00'))
            # If no timezone info, assume IST
            if active_until.tzinfo is None:
                active_until = active_until.replace(tzinfo=IST)
        except (ValueError, AttributeError):
            # If parsing fails, treat as inactive
            return False
    # Make both datetimes comparable
    if active_until.tzinfo is None:
        # Assume naive datetime is in IST
        active_until = active_until.replace(tzinfo=IST)
    if now.tzinfo is None:
        now = now.replace(tzinfo=IST)
    return active_until >= now

def safe_isoformat(date_value) -> Optional[str]:
    """Safely convert date/datetime value to ISO format string, handling strings"""
    if date_value is None:
        return None
    if isinstance(date_value, str):
        return date_value  # Already a string, return as-is
    if hasattr(date_value, 'isoformat'):
        return date_value.isoformat()
    return str(date_value)

def safe_parse_datetime(date_value) -> Optional[datetime]:
    """Safely parse date/datetime value to datetime object, handling strings and datetime objects"""
    if date_value is None:
        return None
    if isinstance(date_value, datetime):
        return date_value
    if isinstance(date_value, str):
        try:
            # Try parsing ISO format string
            parsed = datetime.fromisoformat(date_value.replace('Z', '+00:00'))
            # If no timezone info, assume IST
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=IST)
            return parsed
        except (ValueError, AttributeError):
            # If parsing fails, return None
            return None
    # Handle date objects
    if hasattr(date_value, 'isoformat') and not isinstance(date_value, datetime):
        # It's a date object, convert to datetime
        return datetime.combine(date_value, datetime.min.time()).replace(tzinfo=IST)
    return None

router = APIRouter(prefix="/api/admin/users", tags=["AdminUsers"])

# Pydantic models for response
class UserResponse(BaseModel):
    client_id: int
    name: str
    contact: str
    email: str
    gym_name: Optional[str] = None
    access_status: str
    plan_name: Optional[str] = None
    created_at: str
    last_purchase_date: Optional[str] = None

    class Config:
        from_attributes = True

class PaginatedUsersResponse(BaseModel):
    users: List[UserResponse]
    total: int
    page: int
    limit: int
    totalPages: int
    hasNext: bool
    hasPrev: bool

class PlanResponse(BaseModel):
    id: int
    plan_name: str

    class Config:
        from_attributes = True

class GymResponse(BaseModel):
    gym_id: int
    name: str

    class Config:
        from_attributes = True

async def build_subscription_subquery(db: AsyncSession, now: datetime):
    """Build subquery for active subscription data"""
    # Cast active_until to timestamp to handle string values in database
    active_until_cast = cast(Subscription.active_until, SQLDateTime())

    # Subquery to get the latest active subscription for each user
    # Filter for google_play and razorpay_pg providers with active_until >= today
    active_sub_subquery = select(
        Subscription.customer_id,
        Subscription.product_id,
        Subscription.active_until,
        func.row_number().over(
            partition_by=Subscription.customer_id,
            order_by=active_until_cast.desc()
        ).label('rn')
    ).where(
        Subscription.provider.in_(['google_play', 'razorpay_pg']),
        active_until_cast >= now
    ).subquery()

    # Filter to get only the latest subscription (rn=1)
    latest_sub = select(
        active_sub_subquery.c.customer_id,
        active_sub_subquery.c.product_id,
        active_sub_subquery.c.active_until
    ).where(active_sub_subquery.c.rn == 1).subquery()

    return latest_sub

@router.get("")
@router.get("/")
async def get_users(
    page: int = Query(1, ge=1, description="Page number"),
    limit: int = Query(10, ge=1, le=100, description="Items per page"),
    search: Optional[str] = Query(None, description="Search by name, mobile, or gym"),
    status: Optional[str] = Query(None, description="Filter by access status (active/inactive)"),
    plan: Optional[str] = Query(None, description="Filter by plan name (Gold/Platinum/Diamond)"),
    gym: Optional[str] = Query(None, description="Filter by gym name"),
    sort_order: str = Query("desc", description="Sort order for created_at"),
    date_filter: Optional[str] = Query(None, description="Date filter: all, today, yesterday, week, month, custom"),
    custom_start_date: Optional[str] = Query(None, description="Custom start date (YYYY-MM-DD)"),
    custom_end_date: Optional[str] = Query(None, description="Custom end date (YYYY-MM-DD)"),
    db: AsyncSession = Depends(get_async_db)
):
    try:
        now = datetime.now(IST)

        # Build subquery for subscription data
        latest_sub = await build_subscription_subquery(db, now)

        # Build subquery for last purchase date from fittbot_gym_membership
        # Get the latest joined_at for each client_id
        latest_purchase_subquery = select(
            func.cast(FittbotGymMembership.client_id, String).label('purchase_client_id'),
            func.max(FittbotGymMembership.joined_at).label('last_joined_at')
        ).group_by(
            func.cast(FittbotGymMembership.client_id, String)
        ).subquery('latest_purchase')

        # Base query with subscription join and purchase join
        stmt = select(
            Client.client_id,
            Client.name,
            Client.contact,
            Client.email,
            Client.created_at,
            Gym.name.label('gym_name'),
            latest_sub.c.product_id.label('subscription_product_id'),
            latest_sub.c.active_until.label('subscription_active_until'),
            latest_purchase_subquery.c.last_purchase_date
        ).outerjoin(
            Gym, Client.gym_id == Gym.gym_id
        ).outerjoin(
            latest_sub, func.cast(Client.client_id, String) == latest_sub.c.customer_id
        ).outerjoin(
            latest_purchase_subquery, func.cast(Client.client_id, String) == latest_purchase_subquery.c.purchase_client_id
        )

        # Apply search filter
        if search:
            search_term = f"%{search.lower()}%"
            # Note: For ILIKE in async, we need to use .like() with lower()
            stmt = stmt.where(
                or_(
                    func.lower(Client.name).like(search_term),
                    Client.contact.like(search_term),
                    func.lower(Gym.name).like(search_term)
                )
            )

        # Apply status filter based on subscription active_until
        if status and status != "all":
            active_until_cast = cast(latest_sub.c.active_until, SQLDateTime())
            if status == "active":
                stmt = stmt.where(active_until_cast >= now)
            elif status == "inactive":
                stmt = stmt.where(or_(
                    latest_sub.c.active_until.is_(None),
                    active_until_cast < now
                ))

        # Apply plan filter based on product_id mapping
        if plan and plan != "all":
            plan_lower = plan.lower()
            if plan_lower == "gold":
                stmt = stmt.where(func.lower(latest_sub.c.product_id).like("one_month_plan%"))
            elif plan_lower == "platinum":
                stmt = stmt.where(func.lower(latest_sub.c.product_id).like("six_month_plan%"))
            elif plan_lower == "diamond":
                stmt = stmt.where(func.lower(latest_sub.c.product_id).like("twelve_month_plan%"))

        # Apply gym filter (for URL parameter)
        if gym:
            stmt = stmt.where(func.lower(Gym.name).like(f"%{gym.lower()}%"))

        # Apply date filter based on created_at (joined date)
        if date_filter and date_filter != "all":
            start_date = None
            end_date = None

            if date_filter == "today":
                start_date = now.replace(hour=0, minute=0, second=0, microsecond=0)
                end_date = now.replace(hour=23, minute=59, second=59, microsecond=999999)
            elif date_filter == "yesterday":
                yesterday = now - timedelta(days=1)
                start_date = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)
                end_date = yesterday.replace(hour=23, minute=59, second=59, microsecond=999999)
            elif date_filter == "week":
                start_date = now - timedelta(days=7)
                start_date = start_date.replace(hour=0, minute=0, second=0, microsecond=0)
                end_date = now.replace(hour=23, minute=59, second=59, microsecond=999999)
            elif date_filter == "month":
                start_date = now - timedelta(days=30)
                start_date = start_date.replace(hour=0, minute=0, second=0, microsecond=0)
                end_date = now.replace(hour=23, minute=59, second=59, microsecond=999999)
            elif date_filter == "custom":
                if custom_start_date and custom_end_date:
                    try:
                        start_date = datetime.strptime(custom_start_date, "%Y-%m-%d").replace(tzinfo=IST)
                        start_date = start_date.replace(hour=0, minute=0, second=0, microsecond=0)
                        end_date = datetime.strptime(custom_end_date, "%Y-%m-%d").replace(tzinfo=IST)
                        end_date = end_date.replace(hour=23, minute=59, second=59, microsecond=999999)
                    except ValueError:
                        pass  # Invalid date format, skip filter

            # Apply the date range filter
            if start_date and end_date:
                stmt = stmt.where(Client.created_at >= start_date, Client.created_at <= end_date)

        # Apply sorting
        if sort_order == "asc":
            stmt = stmt.order_by(asc(Client.created_at))
        else:
            stmt = stmt.order_by(desc(Client.created_at))

        # Get total count before pagination
        count_stmt = select(func.count()).select_from(stmt.subquery())
        count_result = await db.execute(count_stmt)
        total_count = count_result.scalar() or 0

        # Apply pagination
        offset = (page - 1) * limit
        stmt = stmt.offset(offset).limit(limit)

        # Execute query
        result = await db.execute(stmt)
        results = result.all()

        # Convert to response format
        users = []
        for result in results:
            # Determine access_status based on subscription active_until
            has_active_subscription = is_subscription_active(result.subscription_active_until, now)
            access_status = "active" if has_active_subscription else "inactive"

            # Map product_id to plan name
            plan_name = get_plan_name_from_product_id(result.subscription_product_id)

            user_data = {
                "client_id": result.client_id,
                "name": result.name,
                "contact": result.contact,
                "email": result.email,
                "gym_name": result.gym_name,
                "access_status": access_status,
                "plan_name": plan_name,
                "created_at": result.created_at.isoformat() if result.created_at else None,
                "last_purchase_date": result.last_purchase_date.isoformat() if result.last_purchase_date else None
            }
            users.append(user_data)

        # Calculate pagination info
        total_pages = math.ceil(total_count / limit)
        has_next = page < total_pages
        has_prev = page > 1

        return {
            "success": True,
            "data": {
                "users": users,
                "total": total_count,
                "page": page,
                "limit": limit,
                "totalPages": total_pages,
                "hasNext": has_next,
                "hasPrev": has_prev
            },
            "message": "Users fetched successfully"
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching users: {str(e)}")

@router.get("/plans")
@router.get("/plans/")
async def get_available_plans(db: AsyncSession = Depends(get_async_db)):
    """Get all available subscription plans for filter dropdown"""
    try:
        # Return the mapped plan names based on subscription product_id patterns
        plans_data = [
            {"id": 1, "plan_name": "Gold"},
            {"id": 2, "plan_name": "Platinum"},
            {"id": 3, "plan_name": "Diamond"}
        ]

        return {
            "success": True,
            "data": plans_data,
            "message": "Plans fetched successfully"
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching plans: {str(e)}")

@router.get("/gyms")
@router.get("/gyms/")
async def get_available_gyms(db: AsyncSession = Depends(get_async_db)):
    """Get all gyms that have clients for filter dropdown"""
    try:
        # Subquery to get distinct gym_ids from clients
        client_gym_subquery = select(Client.gym_id).where(Client.gym_id.isnot(None)).distinct().subquery()

        # Main query to get gyms
        stmt = select(Gym).where(Gym.gym_id.in_(select(client_gym_subquery.c.gym_id)))

        result = await db.execute(stmt)
        gyms = result.scalars().all()

        gyms_data = [{"gym_id": gym.gym_id, "name": gym.name} for gym in gyms]

        return {
            "success": True,
            "data": gyms_data,
            "message": "Gyms fetched successfully"
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching gyms: {str(e)}")


@router.get("/client-counts")
async def get_client_counts(
    db: AsyncSession = Depends(get_async_db)
):
    """
    Get overall active and inactive client counts.

    Flow:
    1. First check 'clients' table for client_id and gym_id (both must be present)
    2. Only for those with valid client_id and gym_id, come to 'fittbot_gym_membership' table
    3. Match with that client_id and gym_id (check the latest entry only by id DESC)
    4. Check status column:
       - If contains 'active' → Active count
       - If contains 'expired' OR 'upcoming' → Inactive count
    """
    try:
        # Step 1: Check clients table - get only rows where both client_id AND gym_id are present
        clients_stmt = select(Client.client_id, Client.gym_id).where(
            and_(
                Client.client_id.isnot(None),
                Client.gym_id.isnot(None)
            )
        )
        clients_result = await db.execute(clients_stmt)
        clients_data = clients_result.all()

        # Create a set of (client_id, gym_id) pairs from clients table
        # These are the ONLY pairs we should check in fittbot_gym_membership
        valid_client_gym_pairs = set()
        for row in clients_data:
            valid_client_gym_pairs.add((str(row.client_id), str(row.gym_id)))

        total_clients = len(valid_client_gym_pairs)

        if total_clients == 0:
            return {
                "success": True,
                "data": {
                    "active_clients": 0,
                    "inactive_clients": 0,
                    "total_clients": 0
                },
                "message": "Client counts fetched successfully"
            }

        # Step 2: For each valid (client_id, gym_id) pair from clients table,
        # find the latest membership and check its status
        # This is the CORRECT approach - iterate through clients, not memberships

        active_clients_count = 0
        inactive_clients_count = 0
        no_membership_count = 0

        # Get all memberships in one query (we'll filter in Python to ensure exact pair matching)
        all_membership_stmt = select(
            FittbotGymMembership.id,
            FittbotGymMembership.client_id,
            FittbotGymMembership.gym_id,
            FittbotGymMembership.status
        ).order_by(
            FittbotGymMembership.id.desc()
        )

        all_membership_result = await db.execute(all_membership_stmt)
        all_memberships = all_membership_result.all()

        # Create a dictionary to store the latest membership for each (client_id, gym_id) pair
        latest_memberships = {}
        for membership in all_memberships:
            if membership.client_id is None or membership.gym_id is None:
                continue

            client_id = str(membership.client_id)
            gym_id = str(membership.gym_id)
            pair_key = (client_id, gym_id)

            # Only store if this pair is in our valid clients list
            if pair_key in valid_client_gym_pairs:
                # Since we ordered by id DESC, the first one we encounter is the latest
                if pair_key not in latest_memberships:
                    latest_memberships[pair_key] = membership

        # Step 3: Check the status for each valid pair
        for pair_key in valid_client_gym_pairs:
            if pair_key in latest_memberships:
                # Has membership - check status
                membership = latest_memberships[pair_key]
                status = membership.status

                if status and 'active' in status.lower():
                    active_clients_count += 1
                elif status and ('expired' in status.lower() or 'upcoming' in status.lower()):
                    inactive_clients_count += 1
                else:
                    inactive_clients_count += 1
            else:
                # No membership found for this valid client
                no_membership_count += 1

        # Important: Only count clients that have a membership record
        # Clients without membership are NOT counted in active or inactive
        final_total = active_clients_count + inactive_clients_count

        return {
            "success": True,
            "data": {
                "active_clients": active_clients_count,
                "inactive_clients": inactive_clients_count,
                "total_clients": final_total  # Only clients with membership
            },
            "message": "Client counts fetched successfully"
        }

    except Exception as e:
        import traceback
        traceback.print_exc()
        return {
            "success": False,
            "message": f"Failed to fetch client counts: {str(e)}"
        }


@router.get("/online-offline-counts")
async def get_online_offline_counts(
    db: AsyncSession = Depends(get_async_db)
):
    """
    Get overall online and offline member counts.

    Flow:
    1. First check 'clients' table for client_id and gym_id (both must be present)
    2. Only for those with valid client_id and gym_id, come to 'fittbot_gym_membership' table
    3. Match with that client_id and gym_id (check the latest entry only by id DESC)
    4. Check type column:
       - If type IN ('admission_fees', 'normal') → Offline count
       - Otherwise (all other types) → Online count
    5. If (client_id, gym_id) combination is not in memberships → Leave it (don't count)
    """
    try:
        # Step 1: Check clients table - get only rows where both client_id AND gym_id are present
        clients_stmt = select(Client.client_id, Client.gym_id).where(
            and_(
                Client.client_id.isnot(None),
                Client.gym_id.isnot(None)
            )
        )
        clients_result = await db.execute(clients_stmt)
        clients_data = clients_result.all()

        # Create a set of (client_id, gym_id) pairs from clients table
        # These are the ONLY pairs we should check in fittbot_gym_membership
        valid_client_gym_pairs = set()
        for row in clients_data:
            valid_client_gym_pairs.add((str(row.client_id), str(row.gym_id)))

        total_clients = len(valid_client_gym_pairs)

        if total_clients == 0:
            return {
                "success": True,
                "data": {
                    "online_members": 0,
                    "offline_members": 0,
                    "total_members": 0
                },
                "message": "Online/offline member counts fetched successfully"
            }

        # Step 2: Get all memberships from fittbot_gym_membership
        all_membership_stmt = select(
            FittbotGymMembership.id,
            FittbotGymMembership.client_id,
            FittbotGymMembership.gym_id,
            FittbotGymMembership.type
        ).order_by(
            FittbotGymMembership.id.desc()
        )

        all_membership_result = await db.execute(all_membership_stmt)
        all_memberships = all_membership_result.all()

        # Step 3: Build dictionary of latest memberships for valid pairs only
        latest_memberships = {}
        for membership in all_memberships:
            if membership.client_id is None or membership.gym_id is None:
                continue

            client_id = str(membership.client_id)
            gym_id = str(membership.gym_id)
            pair_key = (client_id, gym_id)

            # Only store if this pair is in our valid clients list
            if pair_key in valid_client_gym_pairs:
                if pair_key not in latest_memberships:
                    latest_memberships[pair_key] = membership

        # Step 4: Count online and offline based on type
        online_members_count = 0
        offline_members_count = 0
        no_membership_count = 0

        offline_types = {'admission_fees', 'normal'}

        for pair_key in valid_client_gym_pairs:
            if pair_key in latest_memberships:
                # Has membership - check type
                membership = latest_memberships[pair_key]
                membership_type = membership.type

                if membership_type in offline_types:
                    # Type is 'admission_fees' or 'normal' → Offline
                    offline_members_count += 1
                else:
                    # All other types → Online
                    online_members_count += 1
            else:
                # No membership found - don't count
                no_membership_count += 1

        final_total = online_members_count + offline_members_count

        return {
            "success": True,
            "data": {
                "online_members": online_members_count,
                "offline_members": offline_members_count,
                "total_members": final_total  # Only clients with membership
            },
            "message": "Online/offline member counts fetched successfully"
        }

    except Exception as e:
        import traceback
        traceback.print_exc()
        return {
            "success": False,
            "message": f"Failed to fetch online/offline counts: {str(e)}"
        }


@router.get("/active-clients")
async def get_active_clients(
    page: int = Query(1, ge=1, description="Page number"),
    limit: int = Query(10, ge=1, le=100, description="Items per page"),
    search: Optional[str] = Query(None, description="Search by name, email, contact"),
    sort_order: str = Query("desc", regex="^(asc|desc)$", description="Sort order by joined date"),
    db: AsyncSession = Depends(get_async_db)
):
    """
    Get paginated list of active clients across all gyms.
    Active clients: Latest membership expires_at > current_date() AND type != 'imported' (same as overview/cards)
    """
    try:
        now = datetime.now(IST)
        offset = (page - 1) * limit

        # SQL-based counting logic (same as overview/cards)
        latest_membership_subquery = select(
            FittbotGymMembership.client_id,
            FittbotGymMembership.type,
            FittbotGymMembership.expires_at,
            func.row_number().over(
                partition_by=FittbotGymMembership.client_id,
                order_by=FittbotGymMembership.id.desc()
            ).label('rn')
        ).where(
            and_(
                FittbotGymMembership.client_id.isnot(None),
                FittbotGymMembership.gym_id.isnot(None),
                FittbotGymMembership.client_id.op('REGEXP')('^[0-9]+$')
            )
        ).subquery('latest_membership')

        # Get active clients count using SQL
        active_counts_stmt = select(
            func.count().label('total_active')
        ).where(
            and_(
                latest_membership_subquery.c.rn == 1,
                latest_membership_subquery.c.expires_at > func.current_date(),
                latest_membership_subquery.c.type != 'imported'
            )
        )

        active_counts_result = await db.execute(active_counts_stmt)
        total_active_count = active_counts_result.scalar() or 0

        # Get active client client_ids first (same query as SQL count, but returning IDs)
        active_ids_stmt = select(
            latest_membership_subquery.c.client_id
        ).where(
            and_(
                latest_membership_subquery.c.rn == 1,
                latest_membership_subquery.c.expires_at > func.current_date(),
                latest_membership_subquery.c.type != 'imported'
            )
        )

        active_ids_result = await db.execute(active_ids_stmt)
        active_client_ids_list = [str(row[0]) for row in active_ids_result.all()]

        # Build base query for fetching clients - filter by active client IDs
        clients_stmt = select(
            Client.client_id,
            Client.name,
            Client.contact,
            Client.email,
            Client.gender,
            Client.gym_id,
            Client.created_at
        ).where(
            and_(
                Client.client_id.isnot(None),
                Client.gym_id.isnot(None),
                func.cast(Client.client_id, String).in_(active_client_ids_list)
            )
        )

        # Apply search filter
        if search:
            search_term = f"%{search}%"
            clients_stmt = clients_stmt.where(
                or_(
                    Client.name.ilike(search_term),
                    Client.email.ilike(search_term),
                    Client.contact.ilike(search_term)
                )
            )

        # Apply sorting
        if sort_order == "asc":
            clients_stmt = clients_stmt.order_by(asc(Client.created_at))
        else:
            clients_stmt = clients_stmt.order_by(desc(Client.created_at))

        # Get total count (after search filter, if any)
        count_stmt = select(func.count()).select_from(clients_stmt.subquery())
        count_result = await db.execute(count_stmt)
        filtered_count = count_result.scalar() or 0

        # Apply pagination and execute query
        clients_stmt = clients_stmt.offset(offset).limit(limit)
        clients_result = await db.execute(clients_stmt)
        paginated_clients = clients_result.all()

        # Fetch gym names and last purchase dates
        client_ids = [c.client_id for c in paginated_clients]
        gym_ids = list(set([c.gym_id for c in paginated_clients if c.gym_id]))

        # Fetch gym names
        gyms = {}
        if gym_ids:
            gyms_stmt = select(Gym.gym_id, Gym.name).where(Gym.gym_id.in_(gym_ids))
            gyms_result = await db.execute(gyms_stmt)
            for gym in gyms_result:
                gyms[str(gym.gym_id)] = gym.name

        # Fetch last purchase dates
        last_purchases = {}
        if client_ids:
            # Check in DailyPass
            daily_pass_stmt = select(
                DailyPass.client_id,
                func.max(DailyPass.created_at).label('last_purchase')
            ).where(DailyPass.client_id.in_(client_ids)).group_by(DailyPass.client_id)

            daily_pass_result = await db.execute(daily_pass_stmt)
            for dp in daily_pass_result:
                last_purchases[str(dp.client_id)] = dp.last_purchase

            # Check in SessionPurchase
            session_purchase_stmt = select(
                SessionPurchase.client_id,
                func.max(SessionPurchase.created_at).label('last_purchase')
            ).where(SessionPurchase.client_id.in_(client_ids)).group_by(SessionPurchase.client_id)

            session_purchase_result = await db.execute(session_purchase_stmt)
            for sp in session_purchase_result:
                existing = last_purchases.get(str(sp.client_id))
                if not existing or sp.last_purchase > existing:
                    last_purchases[str(sp.client_id)] = sp.last_purchase

            # Check in subscriptions
            subscription_stmt = select(
                Subscription.customer_id,
                func.max(Subscription.created_at).label('last_purchase')
            ).where(Subscription.customer_id.in_(client_ids)).group_by(Subscription.customer_id)

            subscription_result = await db.execute(subscription_stmt)
            for sub in subscription_result:
                existing = last_purchases.get(str(sub.customer_id))
                if not existing or sub.last_purchase > existing:
                    last_purchases[str(sub.customer_id)] = sub.last_purchase

        # Build response
        clients_list = []
        for client in paginated_clients:
            clients_list.append({
                "client_id": client.client_id,
                "name": client.name or "-",
                "contact": client.contact or "-",
                "email": client.email or "-",
                "gender": client.gender,
                "gym_name": gyms.get(str(client.gym_id), "-"),
                "joined_date": client.created_at.isoformat() if client.created_at else None,
                "last_purchase_date": last_purchases.get(str(client.client_id))
            })

        return {
            "success": True,
            "data": {
                "clients": clients_list,
                "total": filtered_count,
                "page": page,
                "limit": limit
            }
        }

    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Failed to fetch active clients: {str(e)}")


@router.get("/inactive-clients")
async def get_inactive_clients(
    page: int = Query(1, ge=1, description="Page number"),
    limit: int = Query(10, ge=1, le=100, description="Items per page"),
    search: Optional[str] = Query(None, description="Search by name, email, mobile"),
    sort_order: str = Query("desc", regex="^(asc|desc)$", description="Sort order by joined date"),
    db: AsyncSession = Depends(get_async_db)
):
    """
    Get paginated list of inactive clients across all gyms.
    Inactive clients: Latest membership expires_at <= current_date() OR type = 'imported' (same as overview/cards logic)
    """
    try:
        now = datetime.now(IST)
        offset = (page - 1) * limit

        # SQL-based counting logic (same as overview/cards)
        latest_membership_subquery = select(
            FittbotGymMembership.client_id,
            FittbotGymMembership.type,
            FittbotGymMembership.expires_at,
            func.row_number().over(
                partition_by=FittbotGymMembership.client_id,
                order_by=FittbotGymMembership.id.desc()
            ).label('rn')
        ).where(
            and_(
                FittbotGymMembership.client_id.isnot(None),
                FittbotGymMembership.gym_id.isnot(None),
                FittbotGymMembership.client_id.op('REGEXP')('^[0-9]+$')
            )
        ).subquery('latest_membership')

        # Get total and active counts using SQL
        total_counts_stmt = select(
            func.count().label('total_clients'),
            func.sum(case(
                (and_(
                    latest_membership_subquery.c.expires_at > func.current_date(),
                    latest_membership_subquery.c.type != 'imported'
                ), 1),
                else_=0
            )).label('active_clients')
        ).where(
            latest_membership_subquery.c.rn == 1
        )

        counts_result = await db.execute(total_counts_stmt)
        counts_row = counts_result.first()

        total_clients_count = counts_row.total_clients or 0
        active_clients_count = counts_row.active_clients or 0
        inactive_clients_count = total_clients_count - active_clients_count

        # Get inactive client client_ids first (same query as SQL count, but returning IDs)
        # Inactive: expires_at <= current_date() OR type = 'imported'
        inactive_ids_stmt = select(
            latest_membership_subquery.c.client_id
        ).where(
            and_(
                latest_membership_subquery.c.rn == 1,
                or_(
                    latest_membership_subquery.c.expires_at <= func.current_date(),
                    latest_membership_subquery.c.type == 'imported'
                )
            )
        )

        inactive_ids_result = await db.execute(inactive_ids_stmt)
        inactive_client_ids_list = [str(row[0]) for row in inactive_ids_result.all()]

        # Build base query for fetching clients - filter by inactive client IDs
        clients_stmt = select(
            Client.client_id,
            Client.name,
            Client.contact,
            Client.email,
            Client.gender,
            Client.gym_id,
            Client.created_at
        ).where(
            and_(
                Client.client_id.isnot(None),
                Client.gym_id.isnot(None),
                func.cast(Client.client_id, String).in_(inactive_client_ids_list)
            )
        )

        # Apply search filter
        if search:
            search_term = f"%{search}%"
            clients_stmt = clients_stmt.where(
                or_(
                    Client.name.ilike(search_term),
                    Client.email.ilike(search_term),
                    Client.contact.ilike(search_term)
                )
            )

        # Apply sorting
        if sort_order == "asc":
            clients_stmt = clients_stmt.order_by(asc(Client.created_at))
        else:
            clients_stmt = clients_stmt.order_by(desc(Client.created_at))

        # Get total count (after search filter, if any)
        count_stmt = select(func.count()).select_from(clients_stmt.subquery())
        count_result = await db.execute(count_stmt)
        filtered_count = count_result.scalar() or 0

        # Apply pagination and execute query
        clients_stmt = clients_stmt.offset(offset).limit(limit)
        clients_result = await db.execute(clients_stmt)
        paginated_clients = clients_result.all()

        # Fetch gym names and last purchase dates
        client_ids = [c.client_id for c in paginated_clients]
        gym_ids = list(set([c.gym_id for c in paginated_clients if c.gym_id]))

        # Fetch gym names
        gyms = {}
        if gym_ids:
            gyms_stmt = select(Gym.gym_id, Gym.name).where(Gym.gym_id.in_(gym_ids))
            gyms_result = await db.execute(gyms_stmt)
            for gym in gyms_result:
                gyms[str(gym.gym_id)] = gym.name

        # Fetch last purchase dates
        last_purchases = {}
        if client_ids:
            # Check in DailyPass
            daily_pass_stmt = select(
                DailyPass.client_id,
                func.max(DailyPass.created_at).label('last_purchase')
            ).where(DailyPass.client_id.in_(client_ids)).group_by(DailyPass.client_id)

            daily_pass_result = await db.execute(daily_pass_stmt)
            for dp in daily_pass_result:
                last_purchases[str(dp.client_id)] = dp.last_purchase

            # Check in SessionPurchase
            session_purchase_stmt = select(
                SessionPurchase.client_id,
                func.max(SessionPurchase.created_at).label('last_purchase')
            ).where(SessionPurchase.client_id.in_(client_ids)).group_by(SessionPurchase.client_id)

            session_purchase_result = await db.execute(session_purchase_stmt)
            for sp in session_purchase_result:
                existing = last_purchases.get(str(sp.client_id))
                if not existing or sp.last_purchase > existing:
                    last_purchases[str(sp.client_id)] = sp.last_purchase

            # Check in subscriptions
            subscription_stmt = select(
                Subscription.customer_id,
                func.max(Subscription.created_at).label('last_purchase')
            ).where(Subscription.customer_id.in_(client_ids)).group_by(Subscription.customer_id)

            subscription_result = await db.execute(subscription_stmt)
            for sub in subscription_result:
                existing = last_purchases.get(str(sub.customer_id))
                if not existing or sub.last_purchase > existing:
                    last_purchases[str(sub.customer_id)] = sub.last_purchase

        # Build response
        clients_list = []
        for client in paginated_clients:
            clients_list.append({
                "client_id": client.client_id,
                "name": client.name or "-",
                "contact": client.contact or "-",
                "email": client.email or "-",
                "gender": client.gender,
                "gym_name": gyms.get(str(client.gym_id), "-"),
                "joined_date": client.created_at.isoformat() if client.created_at else None,
                "last_purchase_date": last_purchases.get(str(client.client_id))
            })

        return {
            "success": True,
            "data": {
                "clients": clients_list,
                "total": filtered_count,
                "page": page,
                "limit": limit
            }
        }

    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Failed to fetch inactive clients: {str(e)}")


@router.get("/online-members")
async def get_online_members(
    page: int = Query(1, ge=1, description="Page number"),
    limit: int = Query(10, ge=1, le=100, description="Items per page"),
    search: Optional[str] = Query(None, description="Search by name, email, mobile"),
    sort_order: str = Query("desc", regex="^(asc|desc)$", description="Sort order by joined date"),
    db: AsyncSession = Depends(get_async_db)
):
    """
    Get paginated list of online members across all gyms.
    Online members: Latest membership type NOT IN ('admission_fees', 'normal') (same as overview/cards SQL logic)
    """
    try:
        now = datetime.now(IST)
        offset = (page - 1) * limit

        # SQL-based counting logic (same as overview/cards)
        latest_membership_subquery = select(
            FittbotGymMembership.client_id,
            FittbotGymMembership.type,
            func.row_number().over(
                partition_by=FittbotGymMembership.client_id,
                order_by=FittbotGymMembership.id.desc()
            ).label('rn')
        ).where(
            and_(
                FittbotGymMembership.client_id.isnot(None),
                FittbotGymMembership.gym_id.isnot(None),
                FittbotGymMembership.client_id.op('REGEXP')('^[0-9]+$')
            )
        ).subquery('latest_membership')

        # Get online members count using SQL
        online_counts_stmt = select(
            func.count().label('total_online')
        ).where(
            and_(
                latest_membership_subquery.c.rn == 1,
                latest_membership_subquery.c.type.notin_(['normal', 'admission_fees'])
            )
        )

        online_counts_result = await db.execute(online_counts_stmt)
        total_online_count = online_counts_result.scalar() or 0

        # Get online member client_ids first (same query as SQL count, but returning IDs)
        online_ids_stmt = select(
            latest_membership_subquery.c.client_id
        ).where(
            and_(
                latest_membership_subquery.c.rn == 1,
                latest_membership_subquery.c.type.notin_(['normal', 'admission_fees'])
            )
        )

        online_ids_result = await db.execute(online_ids_stmt)
        online_member_ids_list = [str(row[0]) for row in online_ids_result.all()]

        # Build base query for fetching clients - filter by online member IDs
        clients_stmt = select(
            Client.client_id,
            Client.name,
            Client.contact,
            Client.email,
            Client.gender,
            Client.gym_id,
            Client.created_at
        ).where(
            and_(
                Client.client_id.isnot(None),
                Client.gym_id.isnot(None),
                func.cast(Client.client_id, String).in_(online_member_ids_list)
            )
        )

        # Apply search filter
        if search:
            search_term = f"%{search}%"
            clients_stmt = clients_stmt.where(
                or_(
                    Client.name.ilike(search_term),
                    Client.email.ilike(search_term),
                    Client.contact.ilike(search_term)
                )
            )

        # Apply sorting
        if sort_order == "asc":
            clients_stmt = clients_stmt.order_by(asc(Client.created_at))
        else:
            clients_stmt = clients_stmt.order_by(desc(Client.created_at))

        # Get total count (after search filter, if any)
        count_stmt = select(func.count()).select_from(clients_stmt.subquery())
        count_result = await db.execute(count_stmt)
        filtered_count = count_result.scalar() or 0

        # Apply pagination and execute query
        clients_stmt = clients_stmt.offset(offset).limit(limit)
        clients_result = await db.execute(clients_stmt)
        paginated_clients = clients_result.all()

        # Fetch gym names and last purchase dates
        client_ids = [c.client_id for c in paginated_clients]
        gym_ids = list(set([c.gym_id for c in paginated_clients if c.gym_id]))

        # Fetch gym names
        gyms = {}
        if gym_ids:
            gyms_stmt = select(Gym.gym_id, Gym.name).where(Gym.gym_id.in_(gym_ids))
            gyms_result = await db.execute(gyms_stmt)
            for gym in gyms_result:
                gyms[str(gym.gym_id)] = gym.name

        # Fetch last purchase dates
        last_purchases = {}
        if client_ids:
            # Check in DailyPass
            daily_pass_stmt = select(
                DailyPass.client_id,
                func.max(DailyPass.created_at).label('last_purchase')
            ).where(DailyPass.client_id.in_(client_ids)).group_by(DailyPass.client_id)

            daily_pass_result = await db.execute(daily_pass_stmt)
            for dp in daily_pass_result:
                last_purchases[str(dp.client_id)] = dp.last_purchase

            # Check in SessionPurchase
            session_purchase_stmt = select(
                SessionPurchase.client_id,
                func.max(SessionPurchase.created_at).label('last_purchase')
            ).where(SessionPurchase.client_id.in_(client_ids)).group_by(SessionPurchase.client_id)

            session_purchase_result = await db.execute(session_purchase_stmt)
            for sp in session_purchase_result:
                existing = last_purchases.get(str(sp.client_id))
                if not existing or sp.last_purchase > existing:
                    last_purchases[str(sp.client_id)] = sp.last_purchase

            # Check in subscriptions
            subscription_stmt = select(
                Subscription.customer_id,
                func.max(Subscription.created_at).label('last_purchase')
            ).where(Subscription.customer_id.in_(client_ids)).group_by(Subscription.customer_id)

            subscription_result = await db.execute(subscription_stmt)
            for sub in subscription_result:
                existing = last_purchases.get(str(sub.customer_id))
                if not existing or sub.last_purchase > existing:
                    last_purchases[str(sub.customer_id)] = sub.last_purchase

        # Build response
        clients_list = []
        for client in paginated_clients:
            clients_list.append({
                "client_id": client.client_id,
                "name": client.name or "-",
                "contact": client.contact or "-",
                "email": client.email or "-",
                "gender": client.gender,
                "gym_name": gyms.get(str(client.gym_id), "-"),
                "joined_date": client.created_at.isoformat() if client.created_at else None,
                "last_purchase_date": last_purchases.get(str(client.client_id))
            })

        return {
            "success": True,
            "data": {
                "clients": clients_list,
                "total": filtered_count,
                "page": page,
                "limit": limit
            }
        }

    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Failed to fetch online members: {str(e)}")


@router.get("/offline-members")
async def get_offline_members(
    page: int = Query(1, ge=1, description="Page number"),
    limit: int = Query(10, ge=1, le=100, description="Items per page"),
    search: Optional[str] = Query(None, description="Search by name, email, mobile"),
    sort_order: str = Query("desc", regex="^(asc|desc)$", description="Sort order by joined date"),
    db: AsyncSession = Depends(get_async_db)
):
    """
    Get paginated list of offline members across all gyms.
    Offline members: Latest membership type IN ('admission_fees', 'normal') (same as overview/cards SQL logic)
    """
    try:
        now = datetime.now(IST)
        offset = (page - 1) * limit

        # SQL-based counting logic (same as overview/cards)
        latest_membership_subquery = select(
            FittbotGymMembership.client_id,
            FittbotGymMembership.type,
            func.row_number().over(
                partition_by=FittbotGymMembership.client_id,
                order_by=FittbotGymMembership.id.desc()
            ).label('rn')
        ).where(
            and_(
                FittbotGymMembership.client_id.isnot(None),
                FittbotGymMembership.gym_id.isnot(None),
                FittbotGymMembership.client_id.op('REGEXP')('^[0-9]+$')
            )
        ).subquery('latest_membership')

        # Get offline members count using SQL
        offline_counts_stmt = select(
            func.count().label('total_offline')
        ).where(
            and_(
                latest_membership_subquery.c.rn == 1,
                latest_membership_subquery.c.type.in_(['normal', 'admission_fees'])
            )
        )

        offline_counts_result = await db.execute(offline_counts_stmt)
        total_offline_count = offline_counts_result.scalar() or 0

        # Get offline member client_ids first (same query as SQL count, but returning IDs)
        offline_ids_stmt = select(
            latest_membership_subquery.c.client_id
        ).where(
            and_(
                latest_membership_subquery.c.rn == 1,
                latest_membership_subquery.c.type.in_(['normal', 'admission_fees'])
            )
        )

        offline_ids_result = await db.execute(offline_ids_stmt)
        offline_member_ids_list = [str(row[0]) for row in offline_ids_result.all()]

        # Build base query for fetching clients - filter by offline member IDs
        clients_stmt = select(
            Client.client_id,
            Client.name,
            Client.contact,
            Client.email,
            Client.gender,
            Client.gym_id,
            Client.created_at
        ).where(
            and_(
                Client.client_id.isnot(None),
                Client.gym_id.isnot(None),
                func.cast(Client.client_id, String).in_(offline_member_ids_list)
            )
        )

        # Apply search filter
        if search:
            search_term = f"%{search}%"
            clients_stmt = clients_stmt.where(
                or_(
                    Client.name.ilike(search_term),
                    Client.email.ilike(search_term),
                    Client.contact.ilike(search_term)
                )
            )

        # Apply sorting
        if sort_order == "asc":
            clients_stmt = clients_stmt.order_by(asc(Client.created_at))
        else:
            clients_stmt = clients_stmt.order_by(desc(Client.created_at))

        # Get total count (after search filter, if any)
        count_stmt = select(func.count()).select_from(clients_stmt.subquery())
        count_result = await db.execute(count_stmt)
        filtered_count = count_result.scalar() or 0

        # Apply pagination and execute query
        clients_stmt = clients_stmt.offset(offset).limit(limit)
        clients_result = await db.execute(clients_stmt)
        paginated_clients = clients_result.all()

        # Fetch gym names and last purchase dates
        client_ids = [c.client_id for c in paginated_clients]
        gym_ids = list(set([c.gym_id for c in paginated_clients if c.gym_id]))

        # Fetch gym names
        gyms = {}
        if gym_ids:
            gyms_stmt = select(Gym.gym_id, Gym.name).where(Gym.gym_id.in_(gym_ids))
            gyms_result = await db.execute(gyms_stmt)
            for gym in gyms_result:
                gyms[str(gym.gym_id)] = gym.name

        # Fetch last purchase dates
        last_purchases = {}
        if client_ids:
            # Check in DailyPass
            daily_pass_stmt = select(
                DailyPass.client_id,
                func.max(DailyPass.created_at).label('last_purchase')
            ).where(DailyPass.client_id.in_(client_ids)).group_by(DailyPass.client_id)

            daily_pass_result = await db.execute(daily_pass_stmt)
            for dp in daily_pass_result:
                last_purchases[str(dp.client_id)] = dp.last_purchase

            # Check in SessionPurchase
            session_purchase_stmt = select(
                SessionPurchase.client_id,
                func.max(SessionPurchase.created_at).label('last_purchase')
            ).where(SessionPurchase.client_id.in_(client_ids)).group_by(SessionPurchase.client_id)

            session_purchase_result = await db.execute(session_purchase_stmt)
            for sp in session_purchase_result:
                existing = last_purchases.get(str(sp.client_id))
                if not existing or sp.last_purchase > existing:
                    last_purchases[str(sp.client_id)] = sp.last_purchase

            # Check in subscriptions
            subscription_stmt = select(
                Subscription.customer_id,
                func.max(Subscription.created_at).label('last_purchase')
            ).where(Subscription.customer_id.in_(client_ids)).group_by(Subscription.customer_id)

            subscription_result = await db.execute(subscription_stmt)
            for sub in subscription_result:
                existing = last_purchases.get(str(sub.customer_id))
                if not existing or sub.last_purchase > existing:
                    last_purchases[str(sub.customer_id)] = sub.last_purchase

        # Build response
        clients_list = []
        for client in paginated_clients:
            clients_list.append({
                "client_id": client.client_id,
                "name": client.name or "-",
                "contact": client.contact or "-",
                "email": client.email or "-",
                "gender": client.gender,
                "gym_name": gyms.get(str(client.gym_id), "-"),
                "joined_date": client.created_at.isoformat() if client.created_at else None,
                "last_purchase_date": last_purchases.get(str(client.client_id))
            })

        return {
            "success": True,
            "data": {
                "clients": clients_list,
                "total": filtered_count,
                "page": page,
                "limit": limit
            }
        }

    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Failed to fetch offline members: {str(e)}")

# USER PAGE API FOR ADMIN WEBSITE
@router.get("/overview")
async def get_users_overview(
    page: int = Query(1, ge=1, description="Page number"),
    limit: int = Query(10, ge=1, le=100, description="Items per page"),
    search: Optional[str] = Query(None, description="Search by name, mobile, or gym"),
    status: Optional[str] = Query(None, description="Filter by access status (active/inactive)"),
    plan: Optional[str] = Query(None, description="Filter by plan name (Gold/Platinum/Diamond)"),
    gym: Optional[str] = Query(None, description="Filter by gym name"),
    sort_order: str = Query("desc", description="Sort order for created_at"),
    date_filter: Optional[str] = Query(None, description="Date filter: all, today, yesterday, week, month, custom"),
    custom_start_date: Optional[str] = Query(None, description="Custom start date (YYYY-MM-DD)"),
    custom_end_date: Optional[str] = Query(None, description="Custom end date (YYYY-MM-DD)"),
    platform: Optional[str] = Query(None, description="Filter by platform (android/ios)"),
    db: AsyncSession = Depends(get_async_db)
):

    try:
        now = datetime.now(IST)

        # 1. Get available plans
        plans_data = [
            {"id": 1, "plan_name": "Gold"},
            {"id": 2, "plan_name": "Platinum"},
            {"id": 3, "plan_name": "Diamond"}
        ]

        # Create latest membership subquery (same as individual pages)
        latest_membership_subquery = select(
            FittbotGymMembership.client_id,
            FittbotGymMembership.type,
            FittbotGymMembership.expires_at,
            func.row_number().over(
                partition_by=FittbotGymMembership.client_id,
                order_by=FittbotGymMembership.id.desc()
            ).label('rn')
        ).where(
            and_(
                FittbotGymMembership.client_id.isnot(None),
                FittbotGymMembership.gym_id.isnot(None),
                FittbotGymMembership.client_id.op('REGEXP')('^[0-9]+$')  # Only numeric client_ids
            )
        ).subquery('latest_membership')

        # Get ONLINE member IDs first (same as /online-members page)
        online_ids_stmt = select(
            latest_membership_subquery.c.client_id
        ).where(
            and_(
                latest_membership_subquery.c.rn == 1,
                latest_membership_subquery.c.type.notin_(['normal', 'admission_fees'])
            )
        )
        online_ids_result = await db.execute(online_ids_stmt)
        online_ids_list = [str(row[0]) for row in online_ids_result.all()]

        # Get OFFLINE member IDs first (same as /offline-members page)
        offline_ids_stmt = select(
            latest_membership_subquery.c.client_id
        ).where(
            and_(
                latest_membership_subquery.c.rn == 1,
                latest_membership_subquery.c.type.in_(['normal', 'admission_fees'])
            )
        )
        offline_ids_result = await db.execute(offline_ids_stmt)
        offline_ids_list = [str(row[0]) for row in offline_ids_result.all()]

        # Get ACTIVE client IDs first (same as /active-clients page)
        active_ids_stmt = select(
            latest_membership_subquery.c.client_id
        ).where(
            and_(
                latest_membership_subquery.c.rn == 1,
                latest_membership_subquery.c.expires_at > func.current_date(),
                latest_membership_subquery.c.type != 'imported'
            )
        )
        active_ids_result = await db.execute(active_ids_stmt)
        active_ids_list = [str(row[0]) for row in active_ids_result.all()]

        # Get INACTIVE client IDs first (same as /inactive-clients page)
        inactive_ids_stmt = select(
            latest_membership_subquery.c.client_id
        ).where(
            and_(
                latest_membership_subquery.c.rn == 1,
                or_(
                    latest_membership_subquery.c.expires_at <= func.current_date(),
                    latest_membership_subquery.c.type == 'imported'
                )
            )
        )
        inactive_ids_result = await db.execute(inactive_ids_stmt)
        inactive_ids_list = [str(row[0]) for row in inactive_ids_result.all()]

        # Count clients that exist in Client table (same as individual pages)
        # Online members count
        online_members_count = 0
        if online_ids_list:
            online_count_stmt = select(func.count()).select_from(
                select(Client.client_id).where(
                    and_(
                        Client.client_id.isnot(None),
                        Client.gym_id.isnot(None),
                        func.cast(Client.client_id, String).in_(online_ids_list)
                    )
                ).subquery()
            )
            online_count_result = await db.execute(online_count_stmt)
            online_members_count = online_count_result.scalar() or 0

        # Offline members count
        offline_members_count = 0
        if offline_ids_list:
            offline_count_stmt = select(func.count()).select_from(
                select(Client.client_id).where(
                    and_(
                        Client.client_id.isnot(None),
                        Client.gym_id.isnot(None),
                        func.cast(Client.client_id, String).in_(offline_ids_list)
                    )
                ).subquery()
            )
            offline_count_result = await db.execute(offline_count_stmt)
            offline_members_count = offline_count_result.scalar() or 0

        # Active clients count
        active_clients_count = 0
        if active_ids_list:
            active_count_stmt = select(func.count()).select_from(
                select(Client.client_id).where(
                    and_(
                        Client.client_id.isnot(None),
                        Client.gym_id.isnot(None),
                        func.cast(Client.client_id, String).in_(active_ids_list)
                    )
                ).subquery()
            )
            active_count_result = await db.execute(active_count_stmt)
            active_clients_count = active_count_result.scalar() or 0

        # Inactive clients count
        inactive_clients_count = 0
        if inactive_ids_list:
            inactive_count_stmt = select(func.count()).select_from(
                select(Client.client_id).where(
                    and_(
                        Client.client_id.isnot(None),
                        Client.gym_id.isnot(None),
                        func.cast(Client.client_id, String).in_(inactive_ids_list)
                    )
                ).subquery()
            )
            inactive_count_result = await db.execute(inactive_count_stmt)
            inactive_clients_count = inactive_count_result.scalar() or 0

        total_all_clients = active_clients_count + inactive_clients_count

        # Platform counts (Android and iOS) - simple count from clients table
        android_count_stmt = select(func.count()).where(func.lower(Client.platform) == "android")
        android_count_result = await db.execute(android_count_stmt)
        android_count = android_count_result.scalar() or 0

        ios_count_stmt = select(func.count()).where(func.lower(Client.platform) == "ios")
        ios_count_result = await db.execute(ios_count_stmt)
        ios_count = ios_count_result.scalar() or 0

        total_platform_users = android_count + ios_count

        # Each card shows its own count as "total" (matching individual pages)
        client_counts_data = {
            "active_clients": active_clients_count,
            "inactive_clients": inactive_clients_count,
            "total_clients": total_all_clients,  # Overall total
            # Individual page totals (for cards to use)
            "active_clients_total": active_clients_count,  # Shows in active-clients card
            "inactive_clients_total": inactive_clients_count  # Shows in inactive-clients card
        }

        online_offline_counts_data = {
            "online_members": online_members_count,
            "offline_members": offline_members_count,
            "total_members": total_all_clients,  # Overall total
            # Individual page totals (for cards to use)
            "online_members_total": online_members_count,  # Shows in online-members card
            "offline_members_total": offline_members_count  # Shows in offline-members card
        }

        platform_counts_data = {
            "android": android_count,
            "ios": ios_count,
            "total_platform_users": total_platform_users
        }

        # 5. Calculate Active Users Metrics (Monthly and Weekly averages)
        # Uses same logic as financials API
        from calendar import monthrange

        today_utc = datetime.now(timezone.utc).date()

        # Get gym_id if gym name is provided
        filter_gym_id = None
        if gym:
            gym_stmt = select(Gym.gym_id).where(Gym.name == gym)
            gym_result = await db.execute(gym_stmt)
            filter_gym_id = gym_result.scalar_one_or_none()

        # Helper function to get active users count
        async def get_active_count(start_date, end_date):
            try:
                # Active users: users with at least 1 login in the date range
                conditions = [
                    func.date(ActiveUser.created_at) >= start_date,
                    func.date(ActiveUser.created_at) <= end_date,
                    Client.gym_id != 1
                ]

                if filter_gym_id is not None:
                    conditions.append(Client.gym_id == filter_gym_id)

                subquery = select(ActiveUser.client_id).join(
                    Client, ActiveUser.client_id == Client.client_id
                ).where(
                    and_(*conditions)
                )

                count_query = select(func.coalesce(func.count(distinct(ActiveUser.client_id)), 0)).where(
                    ActiveUser.client_id.in_(subquery)
                )

                count_result = await db.execute(count_query)
                return int(count_result.scalar() or 0)
            except Exception as e:
                print(f"[ACTIVE_USERS_METRICS] Error: {e}")
                return 0

        # Monthly Average Users (last 3 completed months)
        monthly_counts = []
        current_year = today_utc.year
        current_month = today_utc.month

        for i in range(3):
            month_index = current_month - 1 - i
            year = current_year

            while month_index < 0:
                month_index += 12
                year -= 1

            month_start = datetime(year, month_index + 1, 1).date()
            _, last_day = monthrange(year, month_index + 1)
            month_end = datetime(year, month_index + 1, last_day).date()

            count = await get_active_count(month_start, month_end)
            monthly_counts.append(count)

        monthly_average = sum(monthly_counts) / 3

        # Weekly Average Users (last 3 completed weeks)
        weekly_counts = []
        yesterday = today_utc - timedelta(days=1)

        for i in range(3):
            week_end = yesterday - timedelta(weeks=i)
            week_start = week_end - timedelta(days=6)
            count = await get_active_count(week_start, week_end)
            weekly_counts.append(count)

        weekly_average = sum(weekly_counts) / 3

        # Daily Average Users (last 3 completed days)
        # Each day is calculated separately, then averaged
        daily_counts = []
        for i in range(3):
            day = today_utc - timedelta(days=i + 1)  # Start from yesterday (i+1 to skip today)
            count = await get_active_count(day, day)
            daily_counts.append(count)

        daily_average = sum(daily_counts) / 3

        active_users_metrics_data = {
            "monthly_average_users": round(monthly_average, 0),
            "weekly_average_users": round(weekly_average, 0),
            "daily_average_users": round(daily_average, 0)
        }

        # 4. Get paginated users with all filters
        latest_sub = await build_subscription_subquery(db, now)

        # OPTIMIZED: First fetch paginated clients WITHOUT purchase dates
        # This avoids scanning all purchase tables for every request
        stmt = select(
            Client.client_id,
            Client.name,
            Client.contact,
            Client.email,
            Client.created_at,
            Client.platform,
            Gym.name.label('gym_name'),
            latest_sub.c.product_id.label('subscription_product_id'),
            latest_sub.c.active_until.label('subscription_active_until')
        ).outerjoin(
            Gym, Client.gym_id == Gym.gym_id
        ).outerjoin(
            latest_sub, func.cast(Client.client_id, String) == latest_sub.c.customer_id
        )

        # Apply filters
        if search:
            search_term = f"%{search.lower()}%"
            stmt = stmt.where(
                or_(
                    func.lower(Client.name).like(search_term),
                    Client.contact.like(search_term),
                    func.lower(Gym.name).like(search_term)
                )
            )

        if status and status != "all":
            active_until_cast = cast(latest_sub.c.active_until, SQLDateTime())
            if status == "active":
                stmt = stmt.where(active_until_cast >= now)
            elif status == "inactive":
                stmt = stmt.where(or_(
                    latest_sub.c.active_until.is_(None),
                    active_until_cast < now
                ))

        if plan and plan != "all":
            plan_lower = plan.lower()
            if plan_lower == "gold":
                stmt = stmt.where(func.lower(latest_sub.c.product_id).like("one_month_plan%"))
            elif plan_lower == "platinum":
                stmt = stmt.where(func.lower(latest_sub.c.product_id).like("six_month_plan%"))
            elif plan_lower == "diamond":
                stmt = stmt.where(func.lower(latest_sub.c.product_id).like("twelve_month_plan%"))

        if gym:
            stmt = stmt.where(func.lower(Gym.name).like(f"%{gym.lower()}%"))

        if platform and platform != "all":
            stmt = stmt.where(func.lower(Client.platform) == platform.lower())

        if date_filter and date_filter != "all":
            start_date = None
            end_date = None

            if date_filter == "today":
                start_date = now.replace(hour=0, minute=0, second=0, microsecond=0)
                end_date = now.replace(hour=23, minute=59, second=59, microsecond=999999)
            elif date_filter == "yesterday":
                yesterday = now - timedelta(days=1)
                start_date = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)
                end_date = yesterday.replace(hour=23, minute=59, second=59, microsecond=999999)
            elif date_filter == "week":
                start_date = now - timedelta(days=7)
                start_date = start_date.replace(hour=0, minute=0, second=0, microsecond=0)
                end_date = now.replace(hour=23, minute=59, second=59, microsecond=999999)
            elif date_filter == "month":
                start_date = now - timedelta(days=30)
                start_date = start_date.replace(hour=0, minute=0, second=0, microsecond=0)
                end_date = now.replace(hour=23, minute=59, second=59, microsecond=999999)
            elif date_filter == "custom":
                if custom_start_date and custom_end_date:
                    try:
                        start_date = datetime.strptime(custom_start_date, "%Y-%m-%d").replace(tzinfo=IST)
                        start_date = start_date.replace(hour=0, minute=0, second=0, microsecond=0)
                        end_date = datetime.strptime(custom_end_date, "%Y-%m-%d").replace(tzinfo=IST)
                        end_date = end_date.replace(hour=23, minute=59, second=59, microsecond=999999)
                    except ValueError:
                        pass

            if start_date and end_date:
                stmt = stmt.where(Client.created_at >= start_date, Client.created_at <= end_date)

        if sort_order == "asc":
            stmt = stmt.order_by(asc(Client.created_at))
        else:
            stmt = stmt.order_by(desc(Client.created_at))

        # Get total count before pagination
        count_stmt = select(func.count()).select_from(stmt.subquery())
        count_result = await db.execute(count_stmt)
        total_count = count_result.scalar() or 0

        # Apply pagination
        offset = (page - 1) * limit
        stmt = stmt.offset(offset).limit(limit)

        # Execute query (paginated - only fetches needed rows)
        result = await db.execute(stmt)
        results = result.all()

        # Build response without purchase details (purchase details now fetched separately via /last-purchases endpoint)
        users = []
        for result in results:
            has_active_subscription = is_subscription_active(result.subscription_active_until, now)
            access_status = "active" if has_active_subscription else "inactive"
            plan_name = get_plan_name_from_product_id(result.subscription_product_id)

            user_data = {
                "client_id": result.client_id,
                "name": result.name,
                "contact": result.contact,
                "email": result.email,
                "gym_name": result.gym_name,
                "platform":result.platform,
                "access_status": access_status,
                "plan_name": plan_name,
                "created_at": result.created_at
            }
            users.append(user_data)

        total_pages = math.ceil(total_count / limit)
        has_next = page < total_pages
        has_prev = page > 1

        return {
            "success": True,
            "data": {
                "users": users,
                "total": total_count,
                "page": page,
                "limit": limit,
                "totalPages": total_pages,
                "hasNext": has_next,
                "hasPrev": has_prev,
                "plans": plans_data,
                "clientCounts": client_counts_data,
                "onlineOfflineCounts": online_offline_counts_data,
                "platformCounts": platform_counts_data,
                "activeUsersMetrics": active_users_metrics_data
            },
            "message": "Users overview fetched successfully"
        }

    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error fetching users overview: {str(e)}")


@router.get("/{user_id}")
async def get_user_by_id(user_id: int, db: AsyncSession = Depends(get_async_db)):
    """Get specific user details by ID with comprehensive client information"""
    try:
        now = datetime.now(IST)

        # Build subquery for subscription data
        latest_sub = await build_subscription_subquery(db, now)

        # Main query with all client fields (without purchase date first)
        stmt = select(
            Client.client_id,
            Client.name,
            Client.contact,
            Client.email,
            Client.profile,
            Client.location,
            Client.age,
            Client.gender,
            Client.dob,
            Client.height,
            Client.weight,
            Client.bmi,
            Client.lifestyle,
            Client.medical_issues,
            Client.goals,
            Client.gym_id,
            Client.batch_id,
            Client.training_id,
            Client.joined_date,
            Client.status,
            Client.access,
            Client.expiry,
            Client.pincode,
            Client.uuid_client,
            Client.incomplete,
            Client.created_at,
            Client.updated_at,
            Gym.name.label('gym_name'),
            Gym.location.label('gym_location'),
            Gym.street.label('gym_street'),
            Gym.area.label('gym_area'),
            Gym.city.label('gym_city'),
            Gym.state.label('gym_state'),
            Gym.pincode.label('gym_pincode'),
            Gym.contact_number.label('gym_contact'),
            latest_sub.c.product_id.label('subscription_product_id'),
            latest_sub.c.active_until.label('subscription_active_until')
        ).outerjoin(
            Gym, Client.gym_id == Gym.gym_id
        ).outerjoin(
            latest_sub, func.cast(Client.client_id, String) == latest_sub.c.customer_id
        ).where(Client.client_id == user_id)

        result = await db.execute(stmt)
        result = result.first()

        if not result:
            raise HTTPException(status_code=404, detail="User not found")

        # Fetch last purchase date from all three tables for this single user
        user_id_str = str(user_id)
        last_purchase_date = None

        # Check DailyPass
        dp_result = await db.execute(
            select(func.max(DailyPass.created_at)).where(
                func.cast(DailyPass.client_id, String) == user_id_str
            )
        )
        dp_date = safe_parse_datetime(dp_result.scalar())
        if dp_date and (last_purchase_date is None or dp_date > last_purchase_date):
            last_purchase_date = dp_date

        # Check FittbotGymMembership (exclude 'normal' and 'admission_fees' types)
        gm_result = await db.execute(
            select(func.max(FittbotGymMembership.purchased_at)).where(
                and_(
                    func.cast(FittbotGymMembership.client_id, String) == user_id_str,
                    FittbotGymMembership.type.notin_(['normal', 'admission_fees'])
                )
            )
        )
        gm_date = safe_parse_datetime(gm_result.scalar())
        if gm_date and (last_purchase_date is None or gm_date > last_purchase_date):
            last_purchase_date = gm_date

        # Check SessionPurchase
        sp_result = await db.execute(
            select(func.max(SessionPurchase.created_at)).where(
                func.cast(SessionPurchase.client_id, String) == user_id_str
            )
        )
        sp_date = safe_parse_datetime(sp_result.scalar())
        if sp_date and (last_purchase_date is None or sp_date > last_purchase_date):
            last_purchase_date = sp_date

        # Check Subscription (exclude 'free_trial' and 'internal_manual' providers)
        sub_result = await db.execute(
            select(func.max(Subscription.created_at)).where(
                and_(
                    Subscription.customer_id == user_id_str,
                    Subscription.provider.notin_(['free_trial', 'internal_manual'])
                )
            )
        )
        sub_date = safe_parse_datetime(sub_result.scalar())
        if sub_date and (last_purchase_date is None or sub_date > last_purchase_date):
            last_purchase_date = sub_date

        # Determine access_status based on subscription active_until
        has_active_subscription = is_subscription_active(result.subscription_active_until, now)
        access_status = "active" if has_active_subscription else "inactive"

        # Map product_id to plan name
        plan_name = get_plan_name_from_product_id(result.subscription_product_id)

        # Calculate age from DOB if not stored
        calculated_age = result.age
        if result.dob and not result.age:
            today = datetime.now().date()
            birth_date = result.dob if isinstance(result.dob, datetime.date) else result.dob.date()
            calculated_age = today.year - birth_date.year - ((today.month, today.day) < (birth_date.month, birth_date.day))

        user_data = {
            "client_id": result.client_id,
            "name": result.name,
            "contact": result.contact,
            "email": result.email,
            "profile": result.profile,
            "location": result.location,
            "age": calculated_age,
            "gender": result.gender,
            "dob": result.dob.isoformat() if result.dob else None,
            "height": result.height,
            "weight": result.weight,
            "bmi": result.bmi,
            "lifestyle": result.lifestyle,
            "medical_issues": result.medical_issues,
            "goals": result.goals,
            "gym_id": result.gym_id,
            "batch_id": result.batch_id,
            "training_id": result.training_id,
            "joined_date": result.joined_date.isoformat() if result.joined_date else None,
            "status": result.status,
            "access": result.access,
            "expiry": result.expiry,
            "pincode": result.pincode,
            "uuid_client": result.uuid_client,
            "incomplete": result.incomplete,
            "gym_name": result.gym_name,
            "gym_location": result.gym_location,
            "gym_street": result.gym_street,
            "gym_area": result.gym_area,
            "gym_city": result.gym_city,
            "gym_state": result.gym_state,
            "gym_pincode": result.gym_pincode,
            "gym_contact": result.gym_contact,
            "access_status": access_status,
            "plan_name": plan_name,
            "subscription_active_until": safe_isoformat(result.subscription_active_until),
            "created_at": result.created_at.isoformat() if result.created_at else None,
            "updated_at": result.updated_at.isoformat() if result.updated_at else None,
            "last_purchase_date": last_purchase_date.isoformat() if last_purchase_date else None
        }

        return {
            "success": True,
            "data": user_data,
            "message": "User fetched successfully"
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching user: {str(e)}")

@router.get("/gym/{gym_id}")
async def get_users_by_gym(
    gym_id: int,
    page: int = Query(1, ge=1),
    limit: int = Query(10, ge=1, le=100),
    status: Optional[str] = Query(None),
    db: AsyncSession = Depends(get_async_db)
):
    """Get users filtered by specific gym ID"""
    try:
        now = datetime.now(IST)

        # Build subquery for subscription data
        latest_sub = await build_subscription_subquery(db, now)

        stmt = select(
            Client.client_id,
            Client.name,
            Client.contact,
            Client.email,
            Client.created_at,
            Gym.name.label('gym_name'),
            latest_sub.c.product_id.label('subscription_product_id'),
            latest_sub.c.active_until.label('subscription_active_until')
        ).outerjoin(
            Gym, Client.gym_id == Gym.gym_id
        ).outerjoin(
            latest_sub, func.cast(Client.client_id, String) == latest_sub.c.customer_id
        ).where(Client.gym_id == gym_id)

        # Apply status filter based on subscription active_until
        if status and status != "all":
            active_until_cast = cast(latest_sub.c.active_until, SQLDateTime())
            if status == "active":
                stmt = stmt.where(active_until_cast >= now)
            elif status == "inactive":
                stmt = stmt.where(or_(
                    latest_sub.c.active_until.is_(None),
                    active_until_cast < now
                ))

        stmt = stmt.order_by(desc(Client.created_at))

        # Get total count
        count_stmt = select(func.count()).select_from(stmt.subquery())
        count_result = await db.execute(count_stmt)
        total_count = count_result.scalar() or 0

        # Apply pagination
        offset_val = (page - 1) * limit
        stmt = stmt.offset(offset_val).limit(limit)

        result = await db.execute(stmt)
        results = result.all()

        users = []
        for result in results:
            # Determine access_status based on subscription active_until
            has_active_subscription = is_subscription_active(result.subscription_active_until, now)
            access_status = "active" if has_active_subscription else "inactive"

            # Map product_id to plan name
            plan_name = get_plan_name_from_product_id(result.subscription_product_id)

            user_data = {
                "client_id": result.client_id,
                "name": result.name,
                "contact": result.contact,
                "email": result.email,
                "gym_name": result.gym_name,
                "access_status": access_status,
                "plan_name": plan_name,
                "created_at": result.created_at.isoformat() if result.created_at else None
            }
            users.append(user_data)

        total_pages = math.ceil(total_count / limit)

        return {
            "success": True,
            "data": {
                "users": users,
                "total": total_count,
                "page": page,
                "limit": limit,
                "totalPages": total_pages,
                "hasNext": page < total_pages,
                "hasPrev": page > 1
            },
            "message": "Users fetched successfully"
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching gym users: {str(e)}")


@router.get("/export/all")
async def export_all_users(db: AsyncSession = Depends(get_async_db)):
    """Export all users data ordered by latest created first"""
    try:
        now = datetime.now(IST)

        # Build subquery for subscription data
        latest_sub = await build_subscription_subquery(db, now)

        # Query all clients ordered by latest created first
        stmt = select(
            Client.client_id,
            Client.name,
            Client.contact,
            Client.email,
            Client.created_at,
            Gym.name.label('gym_name'),
            latest_sub.c.product_id.label('subscription_product_id'),
            latest_sub.c.active_until.label('subscription_active_until')
        ).outerjoin(
            Gym, Client.gym_id == Gym.gym_id
        ).outerjoin(
            latest_sub, func.cast(Client.client_id, String) == latest_sub.c.customer_id
        ).order_by(desc(Client.created_at))

        result = await db.execute(stmt)
        results = result.all()

        # Convert to export format
        users = []
        for result in results:
            # Map product_id to plan name
            plan_name = get_plan_name_from_product_id(result.subscription_product_id)

            user_data = {
                "name": result.name,
                "contact": result.contact,
                "gym_name": result.gym_name,
                "plan_name": plan_name,
                "created_at": result.created_at.strftime("%Y-%m-%d") if result.created_at else None
            }
            users.append(user_data)

        return {
            "success": True,
            "data": users,
            "total": len(users),
            "message": "Users exported successfully"
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error exporting users: {str(e)}")


@router.get("/{user_id}/daily-pass-purchases")
async def get_user_daily_pass_purchases(
    user_id: int,
    db: AsyncSession = Depends(get_async_db)
):
    """Get daily pass purchases for a specific user filtered by client_id only"""
    dailypass_session = None
    try:
        # Get dailypass database session
        dailypass_session = get_dailypass_session()

        # Query daily passes directly by client_id - no gym_id check needed
        # The daily_passes table contains all the data we need
        user_id_str = str(user_id)
        user_id_int = int(user_id)

        # Try string match first
        daily_passes = (
            dailypass_session.query(DailyPass)
            .filter(DailyPass.client_id == user_id_str)
            .order_by(DailyPass.created_at.desc())
            .all()
        )

        # If no results with string, try integer
        if len(daily_passes) == 0:
            daily_passes = (
                dailypass_session.query(DailyPass)
                .filter(DailyPass.client_id == user_id_int)
                .order_by(DailyPass.created_at.desc())
                .all()
            )

        if not daily_passes:
            return {
                "success": True,
                "data": [],
                "message": "No daily pass purchases found for this user",
                "total": 0
            }

        # Collect unique gym_ids from the daily passes
        unique_gym_ids = set()
        for pass_record in daily_passes:
            if pass_record.gym_id:
                try:
                    unique_gym_ids.add(int(pass_record.gym_id))
                except (ValueError, TypeError):
                    pass

        # Fetch gym names for all unique gym_ids
        gym_names = {}
        if unique_gym_ids:
            gym_stmt = select(Gym.gym_id, Gym.name).where(Gym.gym_id.in_(unique_gym_ids))
            gym_result = await db.execute(gym_stmt)
            for gym_id, gym_name in gym_result.all():
                gym_names[gym_id] = gym_name

        # Format the response
        purchases = []
        for pass_record in daily_passes:
            gym_id_int = None
            if pass_record.gym_id:
                try:
                    gym_id_int = int(pass_record.gym_id)
                except (ValueError, TypeError):
                    pass

            purchases.append({
                "id": pass_record.id,
                "client_id": pass_record.client_id,
                "gym_id": pass_record.gym_id,
                "gym_name": gym_names.get(gym_id_int) if gym_id_int else None,
                "days_total": pass_record.days_total,
                "days_used": pass_record.days_used,
                "days_remaining": pass_record.days_total - pass_record.days_used if pass_record.days_total else 0,
                "valid_from": pass_record.valid_from.isoformat() if pass_record.valid_from else None,
                "valid_until": pass_record.valid_until.isoformat() if pass_record.valid_until else None,
                "amount_paid": pass_record.amount_paid,
                "selected_time": pass_record.selected_time,
                "status": pass_record.status,
                "created_at": pass_record.created_at.isoformat() if pass_record.created_at else None,
                "updated_at": pass_record.updated_at.isoformat() if pass_record.updated_at else None
            })

        return {
            "success": True,
            "data": purchases,
            "total": len(purchases),
            "message": "Daily pass purchases fetched successfully"
        }

    except Exception as e:
        import traceback
        traceback.print_exc()
        return {
            "success": False,
            "data": [],
            "message": f"Error fetching daily pass purchases: {str(e)}",
            "total": 0
        }
    finally:
        # Close the dailypass session
        if dailypass_session:
            try:
                dailypass_session.close()
            except:
                pass


@router.get("/{user_id}/session-bookings")
async def get_user_session_bookings(
    user_id: int,
    db: AsyncSession = Depends(get_async_db)
):
    """Get session bookings for a specific user filtered by client_id"""
    try:
        # Query session booking days filtered by client_id
        # SessionBookingDay is in the sessions schema and contains the actual booking instances
        # Join with SessionBooking to get price_paid from session_bookings table via schedule_id
        booking_stmt = (
            select(SessionBookingDay, SessionBooking)
            .join(SessionBooking, SessionBooking.schedule_id == SessionBookingDay.schedule_id, isouter=True)
            .where(SessionBookingDay.client_id == user_id)
            .order_by(SessionBookingDay.booking_date.desc(), SessionBookingDay.created_at.desc())
        )

        booking_result = await db.execute(booking_stmt)
        bookings = booking_result.all()

        if not bookings:
            return {
                "success": True,
                "data": [],
                "message": "No session bookings found for this user",
                "total": 0
            }

        # Get purchase_ids to fetch SessionPurchase amounts (same as purchases/all page)
        purchase_ids = list({b.SessionBookingDay.purchase_id for b in bookings if b.SessionBookingDay.purchase_id})
        purchase_amounts = {}
        if purchase_ids:
            purchase_stmt = (
                select(SessionPurchase)
                .where(SessionPurchase.id.in_(purchase_ids))
                .where(SessionPurchase.status == "paid")
            )
            purchase_result = await db.execute(purchase_stmt)
            purchases = purchase_result.scalars().all()
            # Create mapping: purchase_id -> payable_rupees
            for p in purchases:
                purchase_amounts[p.id] = p.payable_rupees

        # Get unique session_ids to fetch session names
        session_ids = list({b.SessionBookingDay.session_id for b in bookings})
        sessions_map = {}
        if session_ids:
            session_stmt = select(ClassSession.id, ClassSession.name, ClassSession.internal).where(
                ClassSession.id.in_(session_ids)
            )
            session_result = await db.execute(session_stmt)
            for session_id, session_name, session_internal in session_result.all():
                # Use internal name if available, otherwise use session name
                display_name = session_internal if session_internal else session_name
                # Handle personal_training special case
                if display_name == "personal_training_session":
                    display_name = "personal_training"
                sessions_map[session_id] = display_name

        # Get unique gym_ids to fetch gym names
        gym_ids = list({b.SessionBookingDay.gym_id for b in bookings if b.SessionBookingDay.gym_id})
        gym_names = {}
        if gym_ids:
            gym_stmt = select(Gym.gym_id, Gym.name).where(Gym.gym_id.in_(gym_ids))
            gym_result = await db.execute(gym_stmt)
            for gym_id, gym_name in gym_result.all():
                gym_names[gym_id] = gym_name

        # Format the response
        session_bookings = []
        for row in bookings:
            booking = row.SessionBookingDay
            booking_info = row.SessionBooking

            # Get amount from SessionPurchase.payable_rupees (same as purchases/all page)
            # If not found in SessionPurchase, fallback to SessionBooking.price_paid
            price_paid = purchase_amounts.get(booking.purchase_id) if booking.purchase_id in purchase_amounts else (booking_info.price_paid if booking_info else None)

            session_bookings.append({
                "id": booking.id,
                "purchase_id": booking.purchase_id,
                "session_id": booking.session_id,
                "session_name": sessions_map.get(booking.session_id, "Unknown Session"),
                "gym_id": booking.gym_id,
                "gym_name": gym_names.get(booking.gym_id) if booking.gym_id else None,
                "trainer_id": booking.trainer_id,
                "booking_date": booking.booking_date.isoformat() if booking.booking_date else None,
                "start_time": booking.start_time.strftime("%H:%M:%S") if booking.start_time else None,
                "end_time": booking.end_time.strftime("%H:%M:%S") if booking.end_time else None,
                "status": booking.status,
                "price_paid": price_paid,
                "created_at": booking.created_at.isoformat() if booking.created_at else None,
                "updated_at": booking.updated_at.isoformat() if booking.updated_at else None,
            })

        return {
            "success": True,
            "data": session_bookings,
            "total": len(session_bookings),
            "message": "Session bookings fetched successfully"
        }

    except Exception as e:
        import traceback
        traceback.print_exc()
        return {
            "success": False,
            "data": [],
            "message": f"Error fetching session bookings: {str(e)}",
            "total": 0
        }


@router.get("/{user_id}/fittbot-subscription")
async def get_user_fittbot_subscription(
    user_id: int,
    db: AsyncSession = Depends(get_async_db)
):
    """Get Fittbot subscription for a specific user using the same logic as recurring-subscribers"""
    try:
        subscriptions = []

        # FIRST CONDITION: Orders table -> Payments table
        # Step 1: Query orders table with filters
        # - customer_id = user_id
        # - status = 'paid'
        # - provider_order_id starts with 'sub_'
        order_stmt = (
            select(Order.customer_id, Order.id)
            .where(Order.customer_id == str(user_id))
            .where(Order.provider_order_id.like("sub_%"))
            .where(Order.status == "paid")
        )

        order_result = await db.execute(order_stmt)
        orders = order_result.all()

        # Step 2: Get order IDs and query payments table
        # Match payment.order_id with order.id
        # Multiple payment entries for same order_id = multiple subscriptions
        if orders:
            order_ids = [order.id for order in orders]

            # Query payments table using the order IDs
            # Extract: amount_minor, captured_at
            payment_from_order_stmt = (
                select(Payment.customer_id, Payment.id, Payment.amount_minor, Payment.captured_at, Payment.order_id)
                .where(Payment.customer_id == str(user_id))
                .where(Payment.order_id.in_(order_ids))
            )

            payment_from_order_result = await db.execute(payment_from_order_stmt)
            payments_from_orders = payment_from_order_result.all()

            for payment in payments_from_orders:
                subscriptions.append({
                    "id": payment.id,
                    "order_id": payment.order_id,
                    "customer_id": payment.customer_id,
                    "amount": payment.amount_minor,  # Using amount_minor from payments table
                    "captured_at": payment.captured_at.isoformat() if payment.captured_at else None,
                })

        # SECOND CONDITION: Direct query on payments table
        # Filters:
        # - customer_id = user_id
        # - provider = 'google_play'
        # - status = 'captured'
        # Extract: amount_minor, captured_at
        payment_stmt = (
            select(Payment.customer_id, Payment.id, Payment.amount_minor, Payment.captured_at, Payment.order_id)
            .where(Payment.customer_id == str(user_id))
            .where(Payment.provider == "google_play")
            .where(Payment.status == "captured")
        )

        payment_result = await db.execute(payment_stmt)
        payments = payment_result.all()

        # Deduplicate by payment ID and add
        existing_payment_ids = {sub["id"] for sub in subscriptions}

        for payment in payments:
            if payment.id not in existing_payment_ids:
                subscriptions.append({
                    "id": payment.id,
                    "order_id": payment.order_id,
                    "customer_id": payment.customer_id,
                    "amount": payment.amount_minor,  # Using amount_minor from payments table
                    "captured_at": payment.captured_at.isoformat() if payment.captured_at else None,
                })

        if not subscriptions:
            return {
                "success": True,
                "data": [],
                "message": "No Fittbot subscription found for this user",
                "total": 0
            }

        # Sort by captured_at descending (newest first)
        subscriptions.sort(key=lambda x: x["captured_at"] or "", reverse=True)

        return {
            "success": True,
            "data": subscriptions,
            "total": len(subscriptions),
            "message": "Fittbot subscription fetched successfully"
        }

    except Exception as e:
        import traceback
        traceback.print_exc()
        return {
            "success": False,
            "data": [],
            "message": f"Error fetching Fittbot subscription: {str(e)}",
            "total": 0
        }


@router.get("/{user_id}/gym-membership")
async def get_user_gym_membership(
    user_id: int,
    db: AsyncSession = Depends(get_async_db)
):
    """Get Gym Membership purchases for a specific user from payments and orders tables"""
    try:
        gym_memberships = []

        # Query payments table with filters
        payment_stmt = (
            select(Payment, Order)
            .join(Order, Order.id == Payment.order_id)
            .where(Payment.customer_id == str(user_id))
            .where(Payment.status == "captured")
            .where(Order.status == "paid")
            .order_by(Payment.captured_at.desc())
        )

        payment_result = await db.execute(payment_stmt)
        payments = payment_result.all()

        # Collect order IDs to fetch gym info
        order_ids = [row.Order.id for row in payments]

        # Fetch order items for these orders to get gym_ids
        gym_name_cache = {}
        if order_ids:
            order_items_stmt = (
                select(OrderItem)
                .where(OrderItem.order_id.in_(order_ids))
                .where(OrderItem.gym_id.isnot(None))
            )
            order_items_result = await db.execute(order_items_stmt)
            order_items = order_items_result.scalars().all()

            # Filter out items with empty gym_id strings and get unique gym_ids
            # When multiple rows exist for same order_id, prefer the one with valid gym_id
            gym_ids = list(set([item.gym_id for item in order_items if item.gym_id and item.gym_id.strip()]))

            # Fetch gym names
            if gym_ids:
                gym_ids_int = [int(gid) for gid in gym_ids if gid.isdigit()]
                if gym_ids_int:
                    gyms_stmt = (
                        select(Gym)
                        .where(Gym.gym_id.in_(gym_ids_int))
                    )
                    gyms_result = await db.execute(gyms_stmt)
                    gyms = gyms_result.scalars().all()

                    # Create cache mapping gym_id to gym_name
                    gym_name_cache = {gym.gym_id: gym.name for gym in gyms}

            # Create mapping from order_id to gym_id using order_items
            # Handle case where multiple rows exist for same order_id - use the one with valid gym_id
            order_gym_mapping = {}
            for item in order_items:
                if item.gym_id and item.gym_id.strip() and item.gym_id.isdigit():
                    # Only set if we don't have a mapping yet, or prefer this one (last valid one wins)
                    order_gym_mapping[item.order_id] = int(item.gym_id)

        # Process each payment
        for row in payments:
            payment = row.Payment
            order = row.Order

            # Check order_metadata for specific conditions
            if not order.order_metadata or not isinstance(order.order_metadata, dict):
                continue

            metadata = order.order_metadata

            # Condition 1: audit.source = "dailypass_checkout_api"
            condition1 = False
            if metadata.get("audit") and isinstance(metadata.get("audit"), dict):
                if metadata["audit"].get("source") == "dailypass_checkout_api":
                    condition1 = True

            # Condition 2: order_info.flow = "unified_gym_membership_with_sub"
            condition2 = False
            if metadata.get("order_info") and isinstance(metadata.get("order_info"), dict):
                if metadata["order_info"].get("flow") == "unified_gym_membership_with_sub":
                    condition2 = True

            # Condition 3: order_info.flow = "unified_gym_membership_with_free_fittbot"
            condition3 = False
            if metadata.get("order_info") and isinstance(metadata.get("order_info"), dict):
                if metadata["order_info"].get("flow") == "unified_gym_membership_with_free_fittbot":
                    condition3 = True

            # Only include if any condition matches
            if not (condition1 or condition2 or condition3):
                continue

            # Get gym name from order_items mapping
            gym_id = order_gym_mapping.get(order.id)
            gym_name = gym_name_cache.get(gym_id) if gym_id else None

            gym_memberships.append({
                "id": payment.id,
                "order_id": payment.order_id,
                "payment_id": payment.id,
                "customer_id": payment.customer_id,
                "amount": order.gross_amount_minor,
                "currency": payment.currency,
                "provider": payment.provider,
                "status": payment.status,
                "captured_at": payment.captured_at.isoformat() if payment.captured_at else None,
                "created_at": payment.created_at.isoformat() if payment.created_at else None,
                "order_status": order.status,
                "order_metadata": metadata,
                "gym_name": gym_name,
            })

        if not gym_memberships:
            return {
                "success": True,
                "data": [],
                "message": "No Gym Membership purchases found for this user",
                "total": 0
            }

        return {
            "success": True,
            "data": gym_memberships,
            "total": len(gym_memberships),
            "message": "Gym Membership purchases fetched successfully"
        }

    except Exception as e:
        import traceback
        traceback.print_exc()
        return {
            "success": False,
            "data": [],
            "message": f"Error fetching Gym Membership: {str(e)}",
            "total": 0
        }


@router.get("/{user_id}/last-purchases")
async def get_user_last_purchases(
    user_id: int,
    db: AsyncSession = Depends(get_async_db)
):
    """
    Get all four types of purchases for a specific user.
    Returns the most recent purchase for each of the four types:
    1. Daily Pass (latest)
    2. Session (latest)
    3. Gym Membership (latest, excluding 'normal' and 'admission_fees')
    4. Subscription (latest, excluding 'free_trial' and 'internal_manual' providers)
    """
    try:
        now = datetime.now(IST)
        user_id_str = str(user_id)

        purchases = {
            "daily_pass": None,
            "session": None,
            "membership": None,
            "subscription": None
        }

        # 1. Get latest Daily Pass
        try:
            dailypass_session = get_dailypass_session()
            daily_pass = dailypass_session.query(DailyPass).filter(
                DailyPass.client_id == user_id_str
            ).order_by(DailyPass.created_at.desc()).first()
            dailypass_session.close()

            if daily_pass:
                # Get gym name
                gym_name = None
                if daily_pass.gym_id:
                    try:
                        gym_stmt = select(Gym.name).where(Gym.gym_id == int(daily_pass.gym_id))
                        gym_result = await db.execute(gym_stmt)
                        gym_row = gym_result.first()
                        gym_name = gym_row[0] if gym_row else None
                    except (ValueError, TypeError):
                        pass

                purchases["daily_pass"] = {
                    "type": "Daily Pass",
                    "purchase_date": daily_pass.created_at.isoformat() if daily_pass.created_at else None,
                    "gym_name": gym_name,
                    "days_total": daily_pass.days_total,
                    "amount_paid": float(daily_pass.amount_paid) if daily_pass.amount_paid else None
                }
        except Exception as e:
            pass

        # 2. Get latest Session Purchase (only paid status)
        try:
            session_stmt = select(
                SessionPurchase,
                Gym.name.label('gym_name')
            ).outerjoin(
                Gym, SessionPurchase.gym_id == Gym.gym_id
            ).where(
                and_(
                    SessionPurchase.client_id == user_id,
                    SessionPurchase.status == "paid"
                )
            ).order_by(SessionPurchase.created_at.desc()).limit(1)

            session_result = await db.execute(session_stmt)
            session_row = session_result.first()

            if session_row:
                sp = session_row.SessionPurchase
                purchases["session"] = {
                    "type": "Session",
                    "purchase_date": sp.created_at.isoformat() if sp.created_at else None,
                    "gym_name": session_row.gym_name,
                    "sessions_count": sp.sessions_count,
                    "scheduled_sessions": sp.scheduled_sessions,
                    "payable_rupees": float(sp.payable_rupees) if sp.payable_rupees else None
                }
        except Exception as e:
            pass

        # 3. Get latest Gym Membership (excluding 'normal' and 'admission_fees')
        try:
            membership_stmt = select(
                FittbotGymMembership,
                Gym.name.label('gym_name')
            ).outerjoin(
                Gym, func.cast(FittbotGymMembership.gym_id, String) == func.cast(Gym.gym_id, String)
            ).where(
                and_(
                    func.cast(FittbotGymMembership.client_id, String) == user_id_str,
                    FittbotGymMembership.type.notin_(['normal', 'admission_fees'])
                )
            ).order_by(FittbotGymMembership.purchased_at.desc()).limit(1)

            membership_result = await db.execute(membership_stmt)
            membership_row = membership_result.first()

            if membership_row:
                gm = membership_row.FittbotGymMembership
                purchases["membership"] = {
                    "type": "Membership",
                    "purchase_date": gm.purchased_at.isoformat() if gm.purchased_at else None,
                    "gym_name": membership_row.gym_name,
                    "membership_type": gm.type,
                    "amount": float(gm.amount) if gm.amount else None
                }
        except Exception as e:
            pass

        # 4. Get latest Subscription (excluding 'free_trial' and 'internal_manual')
        try:
            sub_stmt = select(
                Subscription
            ).where(
                and_(
                    Subscription.customer_id == user_id_str,
                    Subscription.provider.notin_(['free_trial', 'internal_manual'])
                )
            ).order_by(Subscription.created_at.desc()).limit(1)

            sub_result = await db.execute(sub_stmt)
            sub = sub_result.scalar_one_or_none()

            if sub:
                # Get plan name from product_id
                plan_name = get_plan_name_from_product_id(sub.product_id)

                purchases["subscription"] = {
                    "type": "Subscription",
                    "purchase_date": sub.created_at.isoformat() if sub.created_at else None,
                    "gym_name": None,  # Subscriptions don't have gym-specific purchases
                    "product_id": sub.product_id,
                    "plan_name": plan_name,
                    "provider": sub.provider,
                    "status": sub.status,
                    "active_until": sub.active_until.isoformat() if sub.active_until else None,
                    "is_active": is_subscription_active(sub.active_until, now)
                }
        except Exception as e:
            pass

        return {
            "success": True,
            "data": purchases,
            "message": "Last purchases fetched successfully"
        }

    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error fetching last purchases: {str(e)}")

