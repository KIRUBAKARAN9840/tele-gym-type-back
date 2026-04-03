# Gyms API - Combined Gyms Data
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import func, or_, select, case, desc, text, and_, asc, String, exists
from typing import Dict, List, Optional
from pydantic import BaseModel

from app.models.async_database import get_async_db
from app.models.fittbot_models import Gym
from app.fittbot_api.v1.payments.models.orders import Order, OrderItem

router = APIRouter(prefix="/api/admin/gyms", tags=["AdminGyms"])


# Pydantic Schemas
class GymsDataResponse(BaseModel):
    success: bool
    data: Dict
    message: str


@router.get("/data")
async def get_gyms_data(
    limit: int = Query(100, ge=1, le=100, description="Maximum number of cities/states to return"),
    db: AsyncSession = Depends(get_async_db)
):
    """
    Get complete gyms data including:
    - Total gyms count (with type breakdown)
    - Active gyms count (with at least one paid order)
    - Gyms per city (with normalization)
    - Gyms per state (with normalization)

    All aggregation at database level - no N+1 patterns.
    Queries executed sequentially (AsyncSession doesn't support concurrent ops).
    Uses pure SQLAlchemy ORM - no raw SQL.
    """
    try:
        # Build condition: type IN ('green', 'red', 'hold') OR type IS NULL
        valid_types_condition = or_(
            Gym.type == "green",
            Gym.type == "red",
            Gym.type == "hold",
            Gym.type.is_(None)
        )

        # Query 1: Total count
        total_query = select(func.count()).select_from(Gym).where(valid_types_condition)
        total_result = await db.execute(total_query)
        total_count = total_result.scalar() or 0

        # Query 2: Breakdown by type - Single aggregated query using CASE
        breakdown_query = select(
            func.coalesce(func.sum(case((Gym.type == "green", 1), else_=0)), 0).label("green"),
            func.coalesce(func.sum(case((Gym.type == "red", 1), else_=0)), 0).label("red"),
            func.coalesce(func.sum(case((Gym.type == "hold", 1), else_=0)), 0).label("hold"),
            func.coalesce(func.sum(case((Gym.type.is_(None), 1), else_=0)), 0).label("null")
        )
        breakdown_result = await db.execute(breakdown_query)
        breakdown_row = breakdown_result.first()

        # Query 3: Active gyms count - Gyms with at least one PAID order
        # Using pure SQLAlchemy ORM with exists clause
        active_gyms_query = select(func.count(Gym.gym_id)).where(
            and_(
                valid_types_condition,
                exists(
                    select(1)
                    .select_from(OrderItem)
                    .join(Order, Order.id == OrderItem.order_id)
                    .where(
                        OrderItem.gym_id == func.cast(Gym.gym_id, String(100)),
                        Order.status == "paid"
                    )
                )
            )
        )
        active_result = await db.execute(active_gyms_query)
        active_count = active_result.scalar() or 0

        # Query 3.5: Total revenue from all active gyms (paid orders only)
        total_revenue_query = select(
            func.coalesce(func.sum(Order.gross_amount_minor), 0)
        ).select_from(
            Gym
        ).join(
            OrderItem, OrderItem.gym_id == func.cast(Gym.gym_id, String(100))
        ).join(
            Order, Order.id == OrderItem.order_id
        ).where(
            and_(
                valid_types_condition,
                Order.status == "paid"
            )
        )
        total_revenue_result = await db.execute(total_revenue_query)
        total_revenue_minor = total_revenue_result.scalar() or 0
        total_revenue = float(total_revenue_minor) / 100.0  # Convert to rupees

        # Query 4: Gyms per city - Database-level aggregation with normalization
        # Normalize city: trim, proper case, group and count in single query
        normalized_city = func.coalesce(
            func.concat(
                func.upper(func.substring(func.trim(func.coalesce(Gym.city, "")), 1, 1)),
                func.lower(func.substring(func.trim(func.coalesce(Gym.city, "")), 2, 100))
            ),
            "Unknown"
        ).label("city")

        city_query = select(
            normalized_city,
            func.count(Gym.gym_id).label("count")
        ).where(
            valid_types_condition
        ).group_by(
            normalized_city
        ).order_by(
            desc("count")
        ).limit(limit)

        city_result = await db.execute(city_query)
        city_data = city_result.all()

        # Format cities data - single pass, no loops with DB calls
        cities = [
            {"city": row.city, "count": int(row.count)}
            for row in city_data
            if row.city and row.city.strip()
        ]

        # Query 5: Gyms per state - Database-level aggregation with normalization
        # Normalize state: trim, proper case, group and count in single query
        normalized_state = func.coalesce(
            func.concat(
                func.upper(func.substring(func.trim(func.coalesce(Gym.state, "")), 1, 1)),
                func.lower(func.substring(func.trim(func.coalesce(Gym.state, "")), 2, 100))
            ),
            "Unknown"
        ).label("state")

        state_query = select(
            normalized_state,
            func.count(Gym.gym_id).label("count")
        ).where(
            valid_types_condition
        ).group_by(
            normalized_state
        ).order_by(
            desc("count")
        ).limit(limit)

        state_result = await db.execute(state_query)
        state_data = state_result.all()

        # Format states data - single pass, no loops with DB calls
        states = [
            {"state": row.state, "count": int(row.count)}
            for row in state_data
            if row.state and row.state.strip()
        ]

        return {
            "success": True,
            "data": {
                "total_gyms": int(total_count),
                "active_gyms": int(active_count),
                "total_revenue": round(total_revenue, 2),
                "breakdown": {
                    "green": int(breakdown_row.green or 0),
                    "red": int(breakdown_row.red or 0),
                    "hold": int(breakdown_row.hold or 0),
                    "null": int(breakdown_row.null or 0)
                },
                "cities": cities,
                "states": states
            },
            "message": "Gyms data fetched successfully"
        }

    except Exception as e:
        print(f"[GYMS] Error fetching gyms data: {str(e)}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/revenue-list")
async def get_revenue_per_gym_list(
    page: int = Query(1, ge=1, description="Page number"),
    per_page: int = Query(50, ge=1, le=100, description="Items per page"),
    search: Optional[str] = Query(None, description="Search by gym name or city"),
    sort_by: str = Query("amount", description="Sort field: amount, name, city"),
    sort_order: str = Query("desc", description="Sort order: asc, desc"),
    db: AsyncSession = Depends(get_async_db)
):
    """
    Get paginated list of gyms with revenue from paid orders.
    Backend pagination with filters - loads only current page data.

    Columns: Gym Name, City, Amount (gross_amount_minor / 100)

    All filtering, grouping, and aggregation at database level.
    No N1 queries, no in-memory processing.
    Uses pure SQLAlchemy ORM - no raw SQL.
    """
    try:
        # Build condition: type IN ('green', 'red', 'hold') OR type IS NULL
        valid_types_condition = or_(
            Gym.type == "green",
            Gym.type == "red",
            Gym.type == "hold",
            Gym.type.is_(None)
        )

        # Base conditions for the query
        base_conditions = and_(
            valid_types_condition,
            Order.status == "paid"
        )

        # Add search filter if provided - using SQLAlchemy ORM ilike
        if search and search.strip():
            search_pattern = f"%{search.strip()}%"
            base_conditions = and_(
                base_conditions,
                or_(
                    Gym.name.ilike(search_pattern),
                    Gym.city.ilike(search_pattern)
                )
            )

        # Determine sort column and direction - validate against whitelist
        sort_column_map = {
            "amount": "total_amount_minor",
            "name": "gym_name",
            "city": "gym_city"
        }
        sort_column = sort_column_map.get(sort_by, "total_amount_minor")

        # Calculate offset for pagination
        offset = (page - 1) * per_page

        # Main query with pagination - using SQLAlchemy ORM
        # Join: Gym -> OrderItem -> Order, group by gym, sum revenue
        query = select(
            Gym.gym_id,
            func.coalesce(Gym.name, "Unknown").label("gym_name"),
            func.coalesce(Gym.city, "Unknown").label("gym_city"),
            func.coalesce(func.sum(Order.gross_amount_minor), 0).label("total_amount_minor")
        ).join(
            OrderItem, OrderItem.gym_id == func.cast(Gym.gym_id, String(100))
        ).join(
            Order, Order.id == OrderItem.order_id
        ).where(
            base_conditions
        ).group_by(
            Gym.gym_id, Gym.name, Gym.city
        )

        # Apply sorting using ORM methods
        if sort_order.lower() == "asc":
            query = query.order_by(asc(text(sort_column)))
        else:
            query = query.order_by(desc(text(sort_column)))

        # Apply pagination using ORM methods
        paginated_query = query.limit(per_page).offset(offset)
        result = await db.execute(paginated_query)
        rows = result.all()

        # Count query for total records - apply same filters
        count_query = select(func.count(func.distinct(Gym.gym_id))).select_from(
            Gym
        ).join(
            OrderItem, OrderItem.gym_id == func.cast(Gym.gym_id, String(100))
        ).join(
            Order, Order.id == OrderItem.order_id
        ).where(
            base_conditions
        )

        count_result = await db.execute(count_query)
        total_records = count_result.scalar() or 0

        # Calculate total pages
        total_pages = (total_records + per_page - 1) // per_page if total_records > 0 else 1

        # Format response - single pass conversion
        gyms = [
            {
                "gym_id": row.gym_id,
                "gym_name": row.gym_name,
                "city": row.gym_city,
                "amount": float(row.total_amount_minor) / 100.0  # Convert minor to major
            }
            for row in rows
        ]

        return {
            "success": True,
            "data": {
                "gyms": gyms,
                "pagination": {
                    "page": page,
                    "per_page": per_page,
                    "total_records": int(total_records),
                    "total_pages": total_pages,
                    "has_next": page < total_pages,
                    "has_prev": page > 1
                }
            },
            "message": "Revenue per gym list fetched successfully"
        }

    except Exception as e:
        print(f"[GYMS] Error fetching revenue list: {str(e)}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))
