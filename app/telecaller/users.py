# Backend Implementation for Users API (Telecaller/Manager Dashboard)
from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from pydantic import BaseModel, field_validator
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import func, or_, and_, desc, asc, case, literal_column, String, select, union_all
from typing import Optional, List, Union
from datetime import datetime, timezone, timedelta
from app.models.fittbot_models import (
    Client,
    Gym,
    FittbotGymMembership,
    SessionPurchase,
    SessionBookingDay,
)
from app.models.dailypass_models import DailyPass, get_dailypass_session
from app.models.async_database import get_async_db
from app.fittbot_api.v1.payments.models.subscriptions import Subscription
from app.fittbot_api.v1.payments.models.payments import Payment
from app.fittbot_api.v1.payments.models.orders import Order, OrderItem
from app.models.telecaller_models import Manager, Telecaller, UserConversion, ClientCallFeedback
import math

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
    """Check if subscription is active, handling timezone-naive and aware datetimes"""
    if active_until is None:
        return False
    # Convert naive datetime to aware if needed
    if active_until.tzinfo is None:
        active_until = active_until.replace(tzinfo=IST)
    if now.tzinfo is None:
        now = now.replace(tzinfo=IST)
    return active_until >= now

router = APIRouter(prefix="/users", tags=["telecaller-users"])

async def build_subscription_subquery(db: AsyncSession, now: datetime):
    """Build subquery for active subscription data"""
    # Subquery to get the latest active subscription for each user
    active_sub_subquery = select(
        Subscription.customer_id,
        Subscription.product_id,
        Subscription.active_until,
        func.row_number().over(
            partition_by=Subscription.customer_id,
            order_by=Subscription.active_until.desc()
        ).label('rn')
    ).where(
        Subscription.provider.in_(['google_play', 'razorpay_pg']),
        Subscription.active_until >= now
    ).subquery()

    # Filter to get only the latest subscription (rn=1)
    latest_sub = select(
        active_sub_subquery.c.customer_id,
        active_sub_subquery.c.product_id,
        active_sub_subquery.c.active_until
    ).where(active_sub_subquery.c.rn == 1).subquery()

    return latest_sub


async def get_current_user(
    request: Request,
    db: AsyncSession = Depends(get_async_db)
) -> Union[Manager, Telecaller]:
    """
    Get current user (Manager or Telecaller) from JWT token
    """
    from jose import jwt, JWTError
    from app.utils.security import SECRET_KEY, ALGORITHM

    # Get token from cookie
    access_token = request.cookies.get("access_token")
    if not access_token:
        # Fallback to Authorization header if no cookie
        auth_header = request.headers.get("Authorization")
        if auth_header and auth_header.startswith("Bearer "):
            access_token = auth_header.split(" ")[1]
        else:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Could not validate credentials"
            )

    try:
        payload = jwt.decode(access_token, SECRET_KEY, algorithms=[ALGORITHM])
        mobile_number: str = payload.get("sub")
        role: str = payload.get("role")
        user_id: int = payload.get("id")
        user_type: str = payload.get("type")

        if user_type != "telecaller":
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Could not validate credentials"
            )

        if role == "manager":
            # Get manager from database
            stmt = select(Manager).where(Manager.id == user_id)
            result = await db.execute(stmt)
            user = result.scalar_one_or_none()
        elif role == "telecaller":
            # Get telecaller from database
            stmt = select(Telecaller).where(Telecaller.id == user_id)
            result = await db.execute(stmt)
            user = result.scalar_one_or_none()
        else:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Could not validate credentials"
            )

        if user is None:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Could not validate credentials"
            )

        return user

    except jwt.ExpiredSignatureError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token expired"
        )
    except JWTError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Could not validate credentials"
        )


@router.get("/overview")
async def get_users_overview(
    request: Request,
    page: int = Query(1, ge=1, description="Page number"),
    limit: int = Query(10, ge=1, le=100, description="Items per page"),
    search: Optional[str] = Query(None, description="Search by name, mobile, or gym"),
    gym: Optional[str] = Query(None, description="Filter by gym name"),
    db: AsyncSession = Depends(get_async_db)
):
    """
    Get users overview with optimized queries.
    Purchase details are now fetched separately via /last-purchases endpoint.
    """
    # Verify authentication
    await get_current_user(request, db)

    try:
        now = datetime.now(IST)

        # Get paginated users with all filters
        latest_sub = await build_subscription_subquery(db, now)

        # Fetch paginated clients WITHOUT purchase details
        # Purchase details are now fetched on-demand via /last-purchases endpoint
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
        )

        # Apply gym filter
        if gym:
            stmt = stmt.where(Gym.name == gym)

        # Apply search filter
        if search:
            search_term = f"%{search.lower()}%"
            stmt = stmt.where(
                or_(
                    func.lower(Client.name).like(search_term),
                    Client.contact.like(search_term),
                    func.lower(Gym.name).like(search_term)
                )
            )

        # Order by created_at descending
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

        # Get client IDs from current page for fetching conversion details
        client_ids_on_page = [str(r.client_id) for r in results]

        # Fetch last purchase dates for clients on this page
        last_purchase_dates_map = {}
        if client_ids_on_page:
            # 1. Daily Pass - max(created_at)
            try:
                dailypass_session = get_dailypass_session()
                dp_dates = dailypass_session.query(
                    DailyPass.client_id,
                    func.max(DailyPass.created_at).label('last_date')
                ).filter(
                    DailyPass.client_id.in_(client_ids_on_page)
                ).group_by(DailyPass.client_id).all()
                dailypass_session.close()
                for client_id, last_date in dp_dates:
                    last_purchase_dates_map.setdefault(client_id, []).append(last_date)
            except Exception as e:
                print(f"[OVERVIEW] Error fetching Daily Pass dates: {e}")

            # 2. Session Purchase - max(created_at) where status = 'paid'
            try:
                sp_date_stmt = select(
                    func.cast(SessionPurchase.client_id, String).label('client_id'),
                    func.max(SessionPurchase.created_at).label('last_date')
                ).where(
                    and_(
                        func.cast(SessionPurchase.client_id, String).in_(client_ids_on_page),
                        SessionPurchase.status == "paid"
                    )
                ).group_by(func.cast(SessionPurchase.client_id, String))
                sp_date_result = await db.execute(sp_date_stmt)
                for client_id, last_date in sp_date_result.all():
                    last_purchase_dates_map.setdefault(str(client_id), []).append(last_date)
            except Exception as e:
                print(f"[OVERVIEW] Error fetching Session dates: {e}")

            # 3. Gym Membership - max(purchased_at)
            try:
                gm_date_stmt = select(
                    FittbotGymMembership.client_id,
                    func.max(FittbotGymMembership.purchased_at).label('last_date')
                ).where(
                    func.cast(FittbotGymMembership.client_id, String).in_(client_ids_on_page)
                ).group_by(FittbotGymMembership.client_id)
                gm_date_result = await db.execute(gm_date_stmt)
                for client_id, last_date in gm_date_result.all():
                    last_purchase_dates_map.setdefault(str(client_id), []).append(last_date)
            except Exception as e:
                print(f"[OVERVIEW] Error fetching Gym Membership dates: {e}")

        # Fetch conversion details for clients on this page
        conversion_details_map = {}
        if client_ids_on_page:
            conversion_stmt = select(
                UserConversion.client_id,
                UserConversion.telecaller_id,
                UserConversion.purchased_plan,
                Telecaller.name.label('telecaller_name')
            ).outerjoin(
                Telecaller, UserConversion.telecaller_id == Telecaller.id
            ).where(
                UserConversion.client_id.in_(client_ids_on_page)
            )

            conversion_results = await db.execute(conversion_stmt)
            for cr in conversion_results.all():
                conversion_details_map[str(cr.client_id)] = {
                    'telecaller_id': cr.telecaller_id,
                    'telecaller_name': cr.telecaller_name,
                    'purchased_plan': cr.purchased_plan
                }

        # Fetch last called by details for clients on this page
        last_called_map = {}
        if client_ids_on_page:
            latest_call_subq = (
                select(
                    ClientCallFeedback.client_id,
                    func.max(ClientCallFeedback.id).label("max_id"),
                )
                .where(ClientCallFeedback.client_id.in_([int(cid) for cid in client_ids_on_page]))
                .group_by(ClientCallFeedback.client_id)
                .subquery()
            )
            last_called_result = await db.execute(
                select(
                    ClientCallFeedback.client_id,
                    Telecaller.name.label("executive_name"),
                )
                .join(latest_call_subq, ClientCallFeedback.id == latest_call_subq.c.max_id)
                .join(Telecaller, Telecaller.id == ClientCallFeedback.executive_id)
            )
            last_called_map = {
                str(r.client_id): r.executive_name
                for r in last_called_result.all()
            }

        # Build response with conversion details (purchase details now fetched separately via /last-purchases endpoint)
        users = []
        for result in results:
            has_active_subscription = is_subscription_active(result.subscription_active_until, now)
            access_status = "active" if has_active_subscription else "inactive"
            plan_name = get_plan_name_from_product_id(result.subscription_product_id)
            client_id_str = str(result.client_id)

            # Calculate last purchased date (max of all purchase dates)
            dates = last_purchase_dates_map.get(client_id_str, [])
            last_purchased_date = max(dates) if dates else None

            user_data = {
                "client_id": result.client_id,
                "name": result.name,
                "contact": result.contact,
                "email": result.email,
                "gym_name": result.gym_name,
                "access_status": access_status,
                "plan_name": plan_name,
                "created_at": result.created_at.isoformat() if result.created_at else None,
                "conversion": conversion_details_map.get(client_id_str),  # Add conversion data
                "last_purchased_date": last_purchased_date.isoformat() if last_purchased_date else None,  # Add last purchase date
                "last_called_by": last_called_map.get(client_id_str)  # Add last called by
            }
            users.append(user_data)

        total_pages = math.ceil(total_count / limit)
        has_next = page < total_pages
        has_prev = page > 1

        # Fetch available gyms (only when not filtering by specific gym)
        available_gyms = []
        if not gym:
            gyms_stmt = select(Gym.name).where(Gym.name.isnot(None)).order_by(Gym.name).distinct()
            gyms_result = await db.execute(gyms_stmt)
            available_gyms = [g[0] for g in gyms_result.all() if g[0]]

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
                "available_gyms": available_gyms,
            },
            "message": "Users overview fetched successfully"
        }

    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error fetching users overview: {str(e)}")


# Pydantic models for conversion API
class ConvertUserRequest(BaseModel):
    client_id: Union[str, int]  # Accept both string and int for client_id
    telecaller_id: int
    purchased_plan: str

    @field_validator('client_id', mode='before')
    @classmethod
    def convert_client_id_to_str(cls, v):
        """Convert client_id to string for consistent database querying"""
        return str(v) if v is not None else None


@router.get("/telecallers")
async def get_telecallers_list(
    request: Request,
    db: AsyncSession = Depends(get_async_db)
):
    """
    Get list of telecallers for the conversion dropdown.
    Returns telecallers based on user role:
    - Manager: Returns their team's telecallers
    - Telecaller: Returns all telecallers in their team
    """
    current_user = await get_current_user(request, db)

    try:
        telecaller_list = []

        if isinstance(current_user, Manager):
            # Manager: Get their team's telecallers
            stmt = select(Telecaller).where(
                Telecaller.manager_id == current_user.id,
                Telecaller.status == "active"
            ).order_by(Telecaller.name)
            result = await db.execute(stmt)
            telecallers = result.scalars().all()

            telecaller_list = [
                {"id": tc.id, "name": tc.name, "mobile_number": tc.mobile_number}
                for tc in telecallers
            ]

        elif isinstance(current_user, Telecaller):
            # Telecaller: Get all telecallers in their team (including themselves)
            stmt = select(Telecaller).where(
                Telecaller.manager_id == current_user.manager_id,
                Telecaller.status == "active"
            ).order_by(Telecaller.name)
            result = await db.execute(stmt)
            telecallers = result.scalars().all()

            telecaller_list = [
                {"id": tc.id, "name": tc.name, "mobile_number": tc.mobile_number}
                for tc in telecallers
            ]

        return {
            "success": True,
            "data": {
                "telecallers": telecaller_list
            }
        }

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch telecallers: {str(e)}"
        )


@router.post("/convert")
async def convert_user(
    body_request: ConvertUserRequest,
    request: Request,
    db: AsyncSession = Depends(get_async_db)
):
    """
    Mark a user as converted by a telecaller
    """
    current_user = await get_current_user(request, db)

    try:
        # Check if conversion already exists
        existing_stmt = select(UserConversion).where(UserConversion.client_id == body_request.client_id)
        existing_result = await db.execute(existing_stmt)
        existing_conversion = existing_result.scalar_one_or_none()

        # Verify telecaller exists (and belongs to manager's team if manager is making the request)
        telecaller_stmt = select(Telecaller).where(Telecaller.id == body_request.telecaller_id)
        telecaller_result = await db.execute(telecaller_stmt)
        telecaller = telecaller_result.scalar_one_or_none()

        if not telecaller:
            raise HTTPException(
                status_code=404,
                detail="Telecaller not found"
            )

        # If manager, verify telecaller belongs to their team
        if isinstance(current_user, Manager):
            if telecaller.manager_id != current_user.id:
                raise HTTPException(
                    status_code=403,
                    detail="You can only assign conversions to your team's telecallers"
                )

        if existing_conversion:
            # Update existing conversion
            existing_conversion.telecaller_id = body_request.telecaller_id
            existing_conversion.purchased_plan = body_request.purchased_plan
            existing_conversion.updated_at = datetime.now(IST)
            await db.commit()
            await db.refresh(existing_conversion)

            return {
                "success": True,
                "message": "Conversion updated successfully",
                "data": {
                    "id": existing_conversion.id,
                    "client_id": existing_conversion.client_id,
                    "telecaller_id": existing_conversion.telecaller_id,
                    "telecaller_name": telecaller.name,
                    "purchased_plan": existing_conversion.purchased_plan,
                    "converted_at": existing_conversion.converted_at.isoformat() if existing_conversion.converted_at else None
                }
            }
        else:
            # Create new conversion
            new_conversion = UserConversion(
                client_id=body_request.client_id,
                telecaller_id=body_request.telecaller_id,
                purchased_plan=body_request.purchased_plan,
                converted_at=datetime.now(IST)
            )

            db.add(new_conversion)
            await db.commit()
            await db.refresh(new_conversion)

            return {
                "success": True,
                "message": "User marked as converted successfully",
                "data": {
                    "id": new_conversion.id,
                    "client_id": new_conversion.client_id,
                    "telecaller_id": new_conversion.telecaller_id,
                    "telecaller_name": telecaller.name,
                    "purchased_plan": new_conversion.purchased_plan,
                    "converted_at": new_conversion.converted_at.isoformat() if new_conversion.converted_at else None
                }
            }

    except HTTPException:
        raise
    except Exception as e:
        await db.rollback()
        raise HTTPException(
            status_code=500,
            detail=f"Failed to convert user: {str(e)}"
        )


# ============================================================================
# User Purchase Detail APIs (4 tabs: Daily Pass, Sessions, Subscription, Gym Membership)
# ============================================================================

@router.get("/{client_id}/daily-pass-purchases")
async def get_user_daily_pass_purchases(
    client_id: str,
    request: Request,
    db: AsyncSession = Depends(get_async_db)
):
    """Get daily pass purchases for a specific user"""
    # Verify authentication
    await get_current_user(request, db)

    dailypass_session = None
    try:

        # Get dailypass database session
        from app.models.dailypass_models import get_dailypass_session, DailyPass
        dailypass_session = get_dailypass_session()

        # Query daily passes by client_id
        daily_passes = (
            dailypass_session.query(DailyPass)
            .filter(DailyPass.client_id == str(client_id))
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
        raise HTTPException(
            status_code=500,
            detail=f"Error fetching daily pass purchases: {str(e)}"
        )
    finally:
        if dailypass_session:
            try:
                dailypass_session.close()
            except:
                pass


@router.get("/{client_id}/session-bookings")
async def get_user_session_bookings(
    client_id: str,
    request: Request,
    db: AsyncSession = Depends(get_async_db)
):
    """Get session bookings for a specific user"""
    # Verify authentication
    await get_current_user(request, db)

    try:

        # Query session booking days filtered by client_id
        from app.models.fittbot_models import ClassSession, SessionBooking

        booking_stmt = (
            select(SessionBookingDay, SessionBooking)
            .join(SessionBooking, SessionBooking.schedule_id == SessionBookingDay.schedule_id, isouter=True)
            .where(SessionBookingDay.client_id == int(client_id))
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

        # Get unique session_ids to fetch session names
        session_ids = list({b.SessionBookingDay.session_id for b in bookings})
        sessions_map = {}
        if session_ids:
            session_stmt = select(ClassSession.id, ClassSession.name, ClassSession.internal).where(
                ClassSession.id.in_(session_ids)
            )
            session_result = await db.execute(session_stmt)
            for session_id, session_name, session_internal in session_result.all():
                display_name = session_internal if session_internal else session_name
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
                "price_paid": booking_info.price_paid if booking_info else None,
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
        raise HTTPException(
            status_code=500,
            detail=f"Error fetching session bookings: {str(e)}"
        )


@router.get("/{client_id}/fittbot-subscription")
async def get_user_fittbot_subscription(
    client_id: str,
    request: Request,
    db: AsyncSession = Depends(get_async_db)
):
    """Get Fittbot subscription for a specific user"""
    # Verify authentication
    await get_current_user(request, db)

    try:

        subscriptions = []

        # Query payments with filters
        payment_stmt = (
            select(Payment, Order)
            .join(Order, Order.id == Payment.order_id)
            .where(Payment.customer_id == str(client_id))
            .where(Payment.provider == "google_play")
            .where(Payment.status == "captured")
            .where(Order.status == "paid")
            .order_by(Payment.captured_at.desc())
        )

        payment_result = await db.execute(payment_stmt)
        payments = payment_result.all()

        # Format payments data
        for row in payments:
            payment = row.Payment
            order = row.Order

            subscriptions.append({
                "id": payment.id,
                "order_id": payment.order_id,
                "customer_id": payment.customer_id,
                "amount": order.gross_amount_minor,
                "currency": payment.currency,
                "provider": payment.provider,
                "status": payment.status,
                "captured_at": payment.captured_at.isoformat() if payment.captured_at else None,
                "created_at": payment.created_at.isoformat() if payment.created_at else None,
                "order_status": order.status,
            })

        # Query orders table independently
        order_stmt = (
            select(Order)
            .where(Order.customer_id == str(client_id))
            .where(Order.provider_order_id.like("sub_%"))
            .where(Order.status == "paid")
            .order_by(Order.created_at.desc())
        )

        order_result = await db.execute(order_stmt)
        orders = order_result.scalars().all()

        # Format orders data (avoiding duplicates)
        existing_order_ids = {sub["order_id"] for sub in subscriptions}

        for order in orders:
            if order.id not in existing_order_ids:
                subscriptions.append({
                    "id": order.id,
                    "order_id": order.id,
                    "customer_id": order.customer_id,
                    "amount": order.gross_amount_minor,
                    "currency": order.currency,
                    "provider": order.provider,
                    "provider_order_id": order.provider_order_id,
                    "status": order.status,
                    "created_at": order.created_at.isoformat() if order.created_at else None,
                    "updated_at": order.updated_at.isoformat() if order.updated_at else None,
                })

        return {
            "success": True,
            "data": subscriptions,
            "total": len(subscriptions),
            "message": "Fittbot subscription fetched successfully"
        }

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error fetching Fittbot subscription: {str(e)}"
        )


@router.get("/{client_id}/gym-membership")
async def get_user_gym_membership(
    client_id: str,
    request: Request,
    db: AsyncSession = Depends(get_async_db)
):
    """Get Gym Membership purchases for a specific user"""
    # Verify authentication
    await get_current_user(request, db)

    try:

        gym_memberships = []

        # Query payments table with filters
        payment_stmt = (
            select(Payment, Order)
            .join(Order, Order.id == Payment.order_id)
            .where(Payment.customer_id == str(client_id))
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
        order_gym_mapping = {}
        if order_ids:
            order_items_stmt = (
                select(OrderItem)
                .where(OrderItem.order_id.in_(order_ids))
                .where(OrderItem.gym_id.isnot(None))
            )
            order_items_result = await db.execute(order_items_stmt)
            order_items = order_items_result.scalars().all()

            gym_ids = list(set([item.gym_id for item in order_items if item.gym_id and item.gym_id.strip()]))

            if gym_ids:
                gym_ids_int = [int(gid) for gid in gym_ids if gid.isdigit()]
                if gym_ids_int:
                    gyms_stmt = (
                        select(Gym)
                        .where(Gym.gym_id.in_(gym_ids_int))
                    )
                    gyms_result = await db.execute(gyms_stmt)
                    gyms = gyms_result.scalars().all()

                    gym_name_cache = {gym.gym_id: gym.name for gym in gyms}

            for item in order_items:
                if item.gym_id and item.gym_id.strip() and item.gym_id.isdigit():
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

            # Only include if either condition matches
            if not (condition1 or condition2):
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

        return {
            "success": True,
            "data": gym_memberships,
            "total": len(gym_memberships),
            "message": "Gym Membership purchases fetched successfully"
        }

    except Exception as e:
        print(f"[GYM_MEMBERSHIP_API] Error: {str(e)}")
        import traceback
        traceback.print_exc()
        raise HTTPException(
            status_code=500,
            detail=f"Error fetching Gym Membership: {str(e)}"
        )


# ============================================================================
# Last Purchases API (for card toggle in users list)
# ============================================================================

@router.get("/{user_id}/last-purchases")
async def get_user_last_purchases(
    user_id: str,
    request: Request,
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
    # Verify authentication
    await get_current_user(request, db)

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
            print(f"[LAST_PURCHASES] Error fetching Daily Pass: {e}")

        # 2. Get latest Session Purchase (only paid status)
        try:
            session_stmt = select(
                SessionPurchase,
                Gym.name.label('gym_name')
            ).outerjoin(
                Gym, SessionPurchase.gym_id == Gym.gym_id
            ).where(
                and_(
                    SessionPurchase.client_id == int(user_id),
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
            print(f"[LAST_PURCHASES] Error fetching Session: {e}")

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
                    FittbotGymMembership.type.notin_(['normal', 'admission_fees', 'imported'])
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
            print(f"[LAST_PURCHASES] Error fetching Membership: {e}")

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
            print(f"[LAST_PURCHASES] Error fetching Subscription: {e}")

        return {
            "success": True,
            "data": purchases,
            "message": "Last purchases fetched successfully"
        }

    except Exception as e:
        print(f"[LAST_PURCHASES] Error: {str(e)}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error fetching last purchases: {str(e)}")
