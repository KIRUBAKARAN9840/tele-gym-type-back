from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, distinct, func, desc, asc, or_
from datetime import datetime, timedelta
from typing import Optional
import math

from app.models.fittbot_models import Client, Gym
from app.fittbot_api.v1.payments.models.subscriptions import Subscription
from app.models.async_database import get_async_db

router = APIRouter(prefix="/api/admin/free-trial", tags=["AdminFreeTrial"])


@router.get("/list")
async def get_free_trial_users(
    page: int = Query(1, ge=1, description="Page number"),
    limit: int = Query(10, ge=1, le=100, description="Items per page"),
    search: Optional[str] = Query(None, description="Search by client name, email, or mobile"),
    sort_order: str = Query("desc", description="Sort order for subscription date"),
    db: AsyncSession = Depends(get_async_db)
):
    """
    Get list of clients who are currently on free trial
    Free trial users are identified by:
    - Subscription provider = 'free_trial'
    - Subscription active_until >= today
    """
    try:
        today = datetime.now().date()

        # Get all unique customer_ids with free trial subscriptions
        # This matches the dashboard card count exactly
        customer_ids_stmt = select(distinct(Subscription.customer_id)).where(
            Subscription.provider == 'free_trial',
            Subscription.active_until >= today
        )

        # Get the customer IDs as a list for the IN clause
        customer_ids_result = await db.execute(customer_ids_stmt)
        customer_ids = [row[0] for row in customer_ids_result.all()]

        if not customer_ids:
            # No free trial users found
            return {
                "success": True,
                "data": {
                    "users": [],
                    "total": 0,
                    "page": page,
                    "limit": limit,
                    "totalPages": 0,
                    "hasNext": False,
                    "hasPrev": False
                },
                "message": "Free trial users fetched successfully"
            }

        # Build query using GROUP BY and MAX to get most recent subscription per customer
        # This approach works with both MySQL and PostgreSQL

        # Subquery to get the most recent subscription for each customer
        most_recent_subq = (
            select(
                Subscription.customer_id,
                func.max(Subscription.active_from).label('max_active_from')
            )
            .where(Subscription.customer_id.in_(customer_ids))
            .where(Subscription.provider == 'free_trial')
            .where(Subscription.active_until >= today)
            .group_by(Subscription.customer_id)
            .subquery()
        )

        # Main query to get subscription details
        stmt = (
            select(
                Subscription.customer_id,
                Subscription.provider,
                Subscription.active_from,
                Subscription.active_until,
                Client.client_id,
                Client.name,
                Client.email,
                Client.contact,
                Client.gender,
                Client.created_at.label('client_created_at'),
                Gym.gym_id,
                Gym.name.label('gym_name'),
                Gym.logo,
                Gym.location,
                Gym.city
            )
            .select_from(Subscription)
            .join(most_recent_subq, (Subscription.customer_id == most_recent_subq.c.customer_id) & (Subscription.active_from == most_recent_subq.c.max_active_from))
            .outerjoin(Client, Client.client_id == Subscription.customer_id)
            .outerjoin(Gym, Gym.gym_id == Client.gym_id)
        )

        # Apply search filter
        if search:
            search_term = f"%{search.lower()}%"
            stmt = stmt.where(
                or_(
                    func.lower(Client.name).like(search_term),
                    func.lower(Client.email).like(search_term),
                    Client.contact.like(search_term),
                    func.lower(Gym.name).like(search_term)
                )
            )

        # Apply sorting
        if sort_order == "asc":
            stmt = stmt.order_by(asc(Subscription.active_from))
        else:
            stmt = stmt.order_by(desc(Subscription.active_from))

        # Apply pagination
        offset = (page - 1) * limit
        stmt = stmt.offset(offset).limit(limit)

        # Execute query
        result = await db.execute(stmt)
        users = result.all()

        # Get total count before pagination
        # Count query matching dashboard card logic - no joins, just count subscriptions
        # Use the length of customer_ids since we already fetched it
        total_count = len(customer_ids)

        # Note: Search filter is NOT applied to count query to match dashboard behavior
        # The count will show total free trial users, regardless of search

        print(f"[FREE-TRIAL] Count: {total_count}, today: {today}")
        print(f"[FREE-TRIAL] Users fetched: {len(users)}")

        # Format response
        free_trial_users = []
        for row in users:
            # Calculate days left in free trial
            # Convert datetime to date if needed
            active_until_date = row.active_until.date() if hasattr(row.active_until, 'date') else row.active_until
            days_left = (active_until_date - today).days if active_until_date else 0

            user_data = {
                "subscription_id": row.customer_id,
                "customer_id": row.customer_id,
                "client_id": row.client_id,
                "provider": row.provider,
                "name": row.name or "N/A",
                "email": row.email or "N/A",
                "mobile": row.contact or "N/A",
                "gender": row.gender,
                "gym_id": row.gym_id,
                "gym_name": row.gym_name or "N/A",
                "gym_location": row.location or row.city or "N/A",
                "gym_logo": row.logo,
                "trial_start_date": row.active_from.isoformat() if row.active_from else None,
                "trial_end_date": row.active_until.isoformat() if row.active_until else None,
                "days_left": days_left,
                "client_joined_date": row.client_created_at.isoformat() if row.client_created_at else None
            }
            free_trial_users.append(user_data)

        # Calculate pagination info
        total_pages = math.ceil(total_count / limit)
        has_next = page < total_pages
        has_prev = page > 1

        return {
            "success": True,
            "data": {
                "users": free_trial_users,
                "total": total_count,
                "page": page,
                "limit": limit,
                "totalPages": total_pages,
                "hasNext": has_next,
                "hasPrev": has_prev
            },
            "message": "Free trial users fetched successfully"
        }

    except Exception as e:
        import traceback
        traceback.print_exc()
        return {
            "success": False,
            "message": f"Error fetching free trial users: {str(e)}"
        }


@router.get("/stats")
async def get_free_trial_stats(
    db: AsyncSession = Depends(get_async_db)
):
    """
    Get overall free trial statistics
    """
    try:
        today = datetime.now().date()

        # Total active free trials
        total_stmt = select(func.count(distinct(Subscription.customer_id))).where(
            Subscription.provider == 'free_trial',
            Subscription.active_until >= today
        )
        total_result = await db.execute(total_stmt)
        total_free_trial = total_result.scalar() or 0

        # Free trials expiring in next 7 days
        next_week = today + timedelta(days=7)
        expiring_soon_stmt = select(func.count(distinct(Subscription.customer_id))).where(
            Subscription.provider == 'free_trial',
            Subscription.active_until >= today,
            Subscription.active_until <= next_week
        )
        expiring_result = await db.execute(expiring_soon_stmt)
        expiring_soon = expiring_result.scalar() or 0

        # New free trials started in last 7 days
        week_ago = today - timedelta(days=7)
        new_trials_stmt = select(func.count(distinct(Subscription.customer_id))).where(
            Subscription.provider == 'free_trial',
            func.date(Subscription.active_from) >= week_ago,
            func.date(Subscription.active_from) <= today
        )
        new_result = await db.execute(new_trials_stmt)
        new_trials = new_result.scalar() or 0

        return {
            "success": True,
            "data": {
                "total": total_free_trial,
                "expiring_soon": expiring_soon,
                "new_this_week": new_trials
            },
            "message": "Free trial stats fetched successfully"
        }

    except Exception as e:
        import traceback
        traceback.print_exc()
        return {
            "success": False,
            "message": f"Error fetching free trial stats: {str(e)}"
        }
