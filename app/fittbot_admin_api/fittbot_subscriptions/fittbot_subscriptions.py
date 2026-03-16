from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, distinct, func, desc, asc, or_
from datetime import datetime, timedelta
from typing import Optional
import math

from app.models.fittbot_models import Client, Gym
from app.fittbot_api.v1.payments.models.subscriptions import Subscription
from app.models.async_database import get_async_db

router = APIRouter(prefix="/api/admin/fittbot-subscriptions", tags=["AdminFittbotSubscriptions"])


@router.get("/list")
async def get_fittbot_subscription_users(
    page: int = Query(1, ge=1, description="Page number"),
    limit: int = Query(10, ge=1, le=100, description="Items per page"),
    search: Optional[str] = Query(None, description="Search by client name, email, or mobile"),
    sort_order: str = Query("desc", description="Sort order for subscription date"),
    db: AsyncSession = Depends(get_async_db)
):
    """
    Get list of clients with active Fittbot subscriptions (paid subscriptions)
    Fittbot subscription users are identified by:
    - Subscription provider IN ('razorpay_pg', 'google_play')
    - Subscription status != 'pending'
    - Subscription active_until >= today
    - Subscription product_id IN valid plan product IDs
    """
    try:
        now = datetime.now()
        today = datetime.now().date()

        # Define all plan product IDs (matching dashboard)
        gold_product_ids = ['one_month_plan:one-month-premium', 'one_month_plan:one-month-premium:rp']
        platinum_product_ids = ['six_month_plan:six-month-premium', 'six_month_plan:six-month-premium:rp']
        diamond_product_ids = ['twelve_month_plan:twelve-month-premium', 'twelve_month_plan:twelve-month-premium:rp']
        all_plan_product_ids = gold_product_ids + platinum_product_ids + diamond_product_ids

        # Get all unique customer_ids with fittbot subscriptions
        # This matches the dashboard card count exactly
        customer_ids_stmt = select(distinct(Subscription.customer_id)).where(
            Subscription.provider.in_(['razorpay_pg', 'google_play']),
            Subscription.status != 'pending',
            Subscription.active_until >= now,
            Subscription.product_id.in_(all_plan_product_ids)
        )

        # Get the customer IDs as a list for the IN clause
        customer_ids_result = await db.execute(customer_ids_stmt)
        customer_ids = [row[0] for row in customer_ids_result.all()]

        print(f"[FITTBOT-SUBSCRIPTIONS] Customer IDs with paid providers: {customer_ids}")

        if not customer_ids:
            # No fittbot subscription users found
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
                "message": "Fittbot subscription users fetched successfully"
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
            .where(Subscription.provider.in_(['razorpay_pg', 'google_play']))
            .where(Subscription.status != 'pending')
            .where(Subscription.active_until >= now)
            .where(Subscription.product_id.in_(all_plan_product_ids))
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

        print(f"[FITTBOT-SUBSCRIPTIONS] Count: {total_count}")
        print(f"[FITTBOT-SUBSCRIPTIONS] Users fetched: {len(users)}")

        # Format response
        fittbot_subscription_users = []
        for row in users:
            # Calculate days left in subscription
            # Convert datetime to date if needed
            active_until_date = row.active_until.date() if hasattr(row.active_until, 'date') else row.active_until
            days_left = (active_until_date - today).days if active_until_date else 0

            # Format provider name for display
            provider_display = "Razorpay" if row.provider == 'razorpay_pg' else "Google Play"

            user_data = {
                "subscription_id": row.customer_id,
                "customer_id": row.customer_id,
                "client_id": row.client_id,
                "provider": row.provider,
                "provider_display": provider_display,
                "name": row.name or "N/A",
                "email": row.email or "N/A",
                "mobile": row.contact or "N/A",
                "gender": row.gender,
                "gym_id": row.gym_id,
                "gym_name": row.gym_name or "N/A",
                "gym_location": row.location or row.city or "N/A",
                "gym_logo": row.logo,
                "subscription_start_date": row.active_from.isoformat() if row.active_from else None,
                "subscription_end_date": row.active_until.isoformat() if row.active_until else None,
                "days_left": days_left,
                "client_joined_date": row.client_created_at.isoformat() if row.client_created_at else None
            }
            fittbot_subscription_users.append(user_data)

        # Calculate pagination info
        total_pages = math.ceil(total_count / limit)
        has_next = page < total_pages
        has_prev = page > 1

        return {
            "success": True,
            "data": {
                "users": fittbot_subscription_users,
                "total": total_count,
                "page": page,
                "limit": limit,
                "totalPages": total_pages,
                "hasNext": has_next,
                "hasPrev": has_prev
            },
            "message": "Fittbot subscription users fetched successfully"
        }

    except Exception as e:
        import traceback
        traceback.print_exc()
        return {
            "success": False,
            "message": f"Error fetching fittbot subscription users: {str(e)}"
        }


@router.get("/stats")
async def get_fittbot_subscription_stats(
    db: AsyncSession = Depends(get_async_db)
):
    """
    Get overall fittbot subscription statistics
    """
    try:
        now = datetime.now()
        today = datetime.now().date()

        # Define all plan product IDs (matching dashboard)
        gold_product_ids = ['one_month_plan:one-month-premium', 'one_month_plan:one-month-premium:rp']
        platinum_product_ids = ['six_month_plan:six-month-premium', 'six_month_plan:six-month-premium:rp']
        diamond_product_ids = ['twelve_month_plan:twelve-month-premium', 'twelve_month_plan:twelve-month-premium:rp']
        all_plan_product_ids = gold_product_ids + platinum_product_ids + diamond_product_ids

        # Total active fittbot subscriptions
        total_stmt = select(func.count(distinct(Subscription.customer_id))).where(
            Subscription.provider.in_(['razorpay_pg', 'google_play']),
            Subscription.status != 'pending',
            Subscription.active_until >= now,
            Subscription.product_id.in_(all_plan_product_ids)
        )
        total_result = await db.execute(total_stmt)
        total_fittbot = total_result.scalar() or 0

        # Fittbot subscriptions expiring in next 7 days
        next_week = today + timedelta(days=7)
        expiring_soon_stmt = select(func.count(distinct(Subscription.customer_id))).where(
            Subscription.provider.in_(['razorpay_pg', 'google_play']),
            Subscription.status != 'pending',
            Subscription.active_until >= today,
            Subscription.active_until <= next_week,
            Subscription.product_id.in_(all_plan_product_ids)
        )
        expiring_result = await db.execute(expiring_soon_stmt)
        expiring_soon = expiring_result.scalar() or 0

        # New fittbot subscriptions started in last 7 days
        week_ago = today - timedelta(days=7)
        new_subs_stmt = select(func.count(distinct(Subscription.customer_id))).where(
            Subscription.provider.in_(['razorpay_pg', 'google_play']),
            Subscription.status != 'pending',
            func.date(Subscription.active_from) >= week_ago,
            func.date(Subscription.active_from) <= today,
            Subscription.product_id.in_(all_plan_product_ids)
        )
        new_result = await db.execute(new_subs_stmt)
        new_subs = new_result.scalar() or 0

        return {
            "success": True,
            "data": {
                "total": total_fittbot,
                "expiring_soon": expiring_soon,
                "new_this_week": new_subs
            },
            "message": "Fittbot subscription stats fetched successfully"
        }

    except Exception as e:
        import traceback
        traceback.print_exc()
        return {
            "success": False,
            "message": f"Error fetching fittbot subscription stats: {str(e)}"
        }
