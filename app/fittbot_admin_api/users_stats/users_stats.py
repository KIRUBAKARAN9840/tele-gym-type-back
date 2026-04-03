# Users Stats API - Total Users Count
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import func, select, case, literal_column, and_, or_
from typing import Dict, List, Optional
from pydantic import BaseModel
from datetime import datetime, timedelta

from app.models.async_database import get_async_db
from app.models.fittbot_models import Client, ActiveUser
from app.fittbot_api.v1.payments.models.payments import Payment
from app.fittbot_api.v1.payments.models.orders import Order, OrderItem

router = APIRouter(prefix="/api/admin/users-stats", tags=["AdminUsersStats"])


# Pydantic Schemas
class UsersStatsResponse(BaseModel):
    success: bool
    data: Dict
    message: str


class CityStatsItem(BaseModel):
    city: str
    users_count: int


class CityStatsResponse(BaseModel):
    success: bool
    data: List[CityStatsItem]
    next_cursor: Optional[int]
    has_more: bool
    message: str


@router.get("/data")
async def get_users_stats(
    db: AsyncSession = Depends(get_async_db)
):

    try:
        # Query 1: Count total clients (excluding gym_id = 1)
        # client_id is the primary key, so COUNT(*) gives us the total unique users
        total_query = select(func.count()).select_from(Client).where(Client.gym_id != 1)
        total_result = await db.execute(total_query)
        total_count = total_result.scalar() or 0

        # Query 2: Count distinct client_id from active_users where created_at >= 30 days ago
        # Active users: users with at least 1 login in the last 30 days
        # Exclude users from gym_id = 1
        thirty_days_ago = datetime.now() - timedelta(days=30)

        active_subquery = select(ActiveUser.client_id).join(
            Client, ActiveUser.client_id == Client.client_id
        ).where(
            and_(
                ActiveUser.created_at >= thirty_days_ago,
                Client.gym_id != 1
            )
        )

        active_query = select(
            func.coalesce(func.count(func.distinct(ActiveUser.client_id)), 0)
        ).where(
            ActiveUser.client_id.in_(active_subquery)
        )
        active_result = await db.execute(active_query)
        active_count = active_result.scalar() or 0

        # Query 3: Count distinct customer_id from payments table (paying users)
        # Exclude payments associated with gym_id = 1
        # Join: Payment -> Order -> OrderItem (to get gym_id)
        paying_subquery = select(Payment.customer_id).join(
            Order, Order.id == Payment.order_id
        ).join(
            OrderItem, OrderItem.order_id == Order.id
        ).where(
            and_(
                OrderItem.gym_id.isnot(None),
                OrderItem.gym_id != "1"
            )
        ).distinct().alias("paying_users_subquery")

        paying_query = select(func.count()).select_from(paying_subquery)
        paying_result = await db.execute(paying_query)
        paying_count = paying_result.scalar() or 0

        # Query 4: Count customers who appear more than once (repeat users)
        # Exclude payments associated with gym_id = 1
        repeat_subquery = select(
            Payment.customer_id
        ).join(
            Order, Order.id == Payment.order_id
        ).join(
            OrderItem, OrderItem.order_id == Order.id
        ).where(
            and_(
                OrderItem.gym_id.isnot(None),
                OrderItem.gym_id != "1"
            )
        ).group_by(
            Payment.customer_id
        ).having(
            func.count(Payment.customer_id) > 1
        ).alias("repeat_users_subquery")

        repeat_query = select(func.count()).select_from(repeat_subquery)
        repeat_result = await db.execute(repeat_query)
        repeat_count = repeat_result.scalar() or 0

        # Query 5: Get users per city with normalization
        # Excluding gym_id = 1
        # Using pure SQLAlchemy ORM - no raw SQL
        # Fetch all locations and filter in Python for better compatibility

        # Get all clients with valid locations (not null, not empty, not whitespace) and gym_id != 1
        clients_query = select(Client.location).where(
            and_(
                Client.location.isnot(None),
                func.trim(Client.location) != '',
                Client.gym_id != 1
            )
        )
        clients_result = await db.execute(clients_query)
        locations = [row[0] for row in clients_result.fetchall()]

        # Group by normalized location in Python and filter for valid city names
        # Valid city names must contain at least one letter
        city_counts = {}
        skipped_no_alpha = 0
        sample_skipped = []

        for loc in locations:
            if loc:
                normalized = loc.strip().lower()
                # Check if location contains at least one letter (a-z or A-Z)
                if normalized and any(c.isalpha() for c in normalized):
                    # Title case for display
                    display_city = normalized.title()
                    city_counts[display_city] = city_counts.get(display_city, 0) + 1
                else:
                    skipped_no_alpha += 1
                    if len(sample_skipped) < 10:
                        sample_skipped.append(loc)

        # Sort by count desc and take top 30
        city_stats = [
            {"city": city, "users_count": count}
            for city, count in sorted(city_counts.items(), key=lambda x: x[1], reverse=True)[:30]
        ]

        return {
            "success": True,
            "data": {
                "total_users": int(total_count),
                "active_users": int(active_count),
                "paying_users": int(paying_count),
                "repeat_users": int(repeat_count),
                "users_by_city": city_stats,
                "total_cities": len(city_counts)
            },
            "message": "Users stats fetched successfully"
        }

    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/cities")
async def get_cities_paginated(
    offset: int = Query(0, description="Number of cities to skip for pagination", ge=0),
    limit: int = Query(30, description="Number of cities to return per page", ge=1, le=100),
    db: AsyncSession = Depends(get_async_db)
):
    """
    Get cities with offset-based pagination.

    Returns cities ordered by users_count descending (excluding gym_id = 1).
    """
    try:
        # Subquery to get grouped and counted cities (excluding gym_id = 1)
        city_group_subquery = select(
            func.trim(func.lower(Client.location)).label("normalized_city"),
            func.count(Client.client_id).label("users_count")
        ).where(
            and_(
                Client.location.isnot(None),
                func.trim(Client.location) != '',
                Client.gym_id != 1
            )
        ).group_by(
            func.trim(func.lower(Client.location))
        ).order_by(
            func.count(Client.client_id).desc()
        ).alias("city_groups")

        # Fetch with offset and limit
        cities_query = select(
            city_group_subquery.c.normalized_city,
            city_group_subquery.c.users_count
        ).offset(offset).limit(limit)

        result = await db.execute(cities_query)
        rows = result.fetchall()

        # Also fetch one more to check if there are more results
        next_check_query = select(
            city_group_subquery.c.normalized_city,
            city_group_subquery.c.users_count
        ).offset(offset + limit).limit(1)

        next_check_result = await db.execute(next_check_query)
        has_more = len(next_check_result.fetchall()) > 0

        # Process results - filter for valid city names and format
        city_stats = []
        for row in rows:
            normalized = row[0]
            count = row[1]

            # Filter: must contain at least one letter
            if normalized and any(c.isalpha() for c in normalized):
                city_stats.append({
                    "city": normalized.title(),
                    "users_count": count
                })

        return {
            "success": True,
            "data": city_stats,
            "next_offset": offset + len(city_stats),
            "has_more": has_more,
            "message": "Cities fetched successfully"
        }

    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))
