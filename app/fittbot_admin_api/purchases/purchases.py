# Purchases API for Admin Dashboard
from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import StreamingResponse
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, or_, cast, Integer, func, union_all, literal
from typing import Optional
import json
import io
from datetime import datetime, date
from openpyxl import Workbook
from openpyxl.styles import PatternFill, Font

from app.models.fittbot_models import Client, Gym, GymOwner, SessionPurchase, SessionBookingDay, FittbotGymMembership
from app.models.async_database import get_async_db
from app.models.dailypass_models import DailyPass, DailyPassDay
from app.fittbot_api.v1.payments.models.payments import Payment
from app.fittbot_api.v1.payments.models.orders import Order, OrderItem

router = APIRouter(prefix="/api/admin/purchases", tags=["AdminPurchases"])


@router.get("/all-purchases")
async def get_all_purchases(
    page: int = Query(1, ge=1, description="Page number"),
    limit: int = Query(10, ge=1, le=100, description="Items per page"),
    search: Optional[str] = Query(None, description="Search by client or gym name"),
    db: AsyncSession = Depends(get_async_db)
):
    try:
        search_pattern = f"%{search}%" if search else None

        # Build DailyPass subquery with type label
        daily_pass_query = (
            select(
                DailyPass.id.label("id"),
                DailyPass.client_id.label("client_id"),
                DailyPass.gym_id.label("gym_id"),
                literal("Daily Pass").label("type"),
                DailyPass.days_total.label("days_total"),
                literal(None).label("total_sessions"),
                literal(None).label("scheduled_sessions"),
                (Payment.amount_minor / 100.0).label("amount"),
                DailyPass.created_at.label("purchased_at"),
                Client.name.label("client_name"),
                Gym.name.label("gym_name"),
                Gym.contact_number.label("gym_contact"),
                GymOwner.contact_number.label("owner_contact"),
                GymOwner.name.label("owner_name"),
                Gym.area.label("gym_area"),
                Client.contact.label("client_contact")
            )
            .select_from(DailyPass)
            .outerjoin(Client, cast(DailyPass.client_id, Integer) == Client.client_id)
            .outerjoin(Gym, cast(DailyPass.gym_id, Integer) == Gym.gym_id)
            .outerjoin(GymOwner, Gym.owner_id == GymOwner.owner_id)
            .outerjoin(Payment, DailyPass.payment_id == Payment.provider_payment_id)
        )

        # Build SessionPurchase subquery with type label (only paid status)
        session_purchase_query = (
            select(
                SessionPurchase.id.label("id"),
                SessionPurchase.client_id.label("client_id"),
                SessionPurchase.gym_id.label("gym_id"),
                literal("Session").label("type"),
                literal(None).label("days_total"),
                SessionPurchase.sessions_count.label("total_sessions"),
                SessionPurchase.scheduled_sessions.label("scheduled_sessions"),
                SessionPurchase.payable_rupees.label("amount"),
                SessionPurchase.created_at.label("purchased_at"),
                Client.name.label("client_name"),
                Gym.name.label("gym_name"),
                Gym.contact_number.label("gym_contact"),
                GymOwner.contact_number.label("owner_contact"),
                GymOwner.name.label("owner_name"),
                Gym.area.label("gym_area"),
                Client.contact.label("client_contact")
            )
            .select_from(SessionPurchase)
            .outerjoin(Client, SessionPurchase.client_id == Client.client_id)
            .outerjoin(Gym, SessionPurchase.gym_id == Gym.gym_id)
            .outerjoin(GymOwner, Gym.owner_id == GymOwner.owner_id)
            .where(SessionPurchase.status == "paid")
        )

        # Apply search filters to both subqueries if search is provided
        if search:
            daily_pass_query = daily_pass_query.where(
                or_(
                    Client.name.ilike(search_pattern),
                    Gym.name.ilike(search_pattern)
                )
            )
            session_purchase_query = session_purchase_query.where(
                or_(
                    Client.name.ilike(search_pattern),
                    Gym.name.ilike(search_pattern)
                )
            )

        # Combine both queries with UNION ALL
        combined_query = union_all(daily_pass_query, session_purchase_query).alias("combined_purchases")

        # Count total records for pagination
        count_query = select(func.count()).select_from(combined_query)
        count_result = await db.execute(count_query)
        total = count_result.scalar() or 0

        # Early return if no results
        if total == 0:
            return {
                "success": True,
                "data": {
                    "purchases": [],
                    "pagination": {
                        "total": 0,
                        "page": page,
                        "limit": limit,
                        "totalPages": 0,
                        "hasNext": False,
                        "hasPrev": False
                    }
                }
            }

        # Build the final paginated query from the union result
        final_query = (
            select(
                combined_query.c.id,
                combined_query.c.client_id,
                combined_query.c.gym_id,
                combined_query.c.type,
                combined_query.c.days_total,
                combined_query.c.total_sessions,
                combined_query.c.scheduled_sessions,
                combined_query.c.amount,
                combined_query.c.purchased_at,
                combined_query.c.client_name,
                combined_query.c.gym_name,
                combined_query.c.gym_contact,
                combined_query.c.owner_contact,
                combined_query.c.owner_name,
                combined_query.c.gym_area,
                combined_query.c.client_contact
            )
            .select_from(combined_query)
            .order_by(combined_query.c.purchased_at.desc())
            .offset((page - 1) * limit)
            .limit(limit)
        )

        # Execute query (async, non-blocking)
        result = await db.execute(final_query)
        rows = result.all()

        # Process scheduled_sessions to get unique dates count
        def get_session_display(scheduled_sessions_json):
            """Process scheduled_sessions JSON to get 'X / Y sessions' format."""
            if not scheduled_sessions_json:
                return None
            try:
                # Parse JSON string if needed
                if isinstance(scheduled_sessions_json, str):
                    sessions = json.loads(scheduled_sessions_json)
                elif isinstance(scheduled_sessions_json, list):
                    sessions = scheduled_sessions_json
                else:
                    return None

                if not sessions:
                    return None

                total_sessions = len(sessions)
                unique_dates = len(set(s.get("date") for s in sessions if s.get("date")))
                return f"{unique_dates} / {total_sessions} sessions"
            except Exception:
                return None

        # Extract session schedule (date and start_time only)
        def get_session_schedule(scheduled_sessions_json):
            """Extract date and start_time from scheduled_sessions."""
            if not scheduled_sessions_json:
                return []
            try:
                # Parse JSON string if needed
                if isinstance(scheduled_sessions_json, str):
                    sessions = json.loads(scheduled_sessions_json)
                elif isinstance(scheduled_sessions_json, list):
                    sessions = scheduled_sessions_json
                else:
                    return []

                if not sessions:
                    return []

                return [
                    {"date": s.get("date"), "start_time": s.get("start_time")}
                    for s in sessions
                    if s.get("date") and s.get("start_time")
                ]
            except Exception:
                return []

        # Fetch scheduled dates for daily passes and sessions
        daily_pass_ids = [row.id for row in rows if row.type == "Daily Pass"]
        session_ids = [row.id for row in rows if row.type == "Session"]

        daily_pass_dates = {}
        if daily_pass_ids:
            dp_dates_query = (
                select(DailyPassDay.pass_id, DailyPassDay.scheduled_date)
                .where(DailyPassDay.pass_id.in_(daily_pass_ids))
                .order_by(DailyPassDay.scheduled_date)
            )
            dp_dates_result = await db.execute(dp_dates_query)
            for dp_row in dp_dates_result.all():
                daily_pass_dates.setdefault(dp_row.pass_id, []).append(
                    dp_row.scheduled_date.isoformat() if dp_row.scheduled_date else None
                )

        session_dates = {}
        if session_ids:
            sb_dates_query = (
                select(SessionBookingDay.purchase_id, SessionBookingDay.booking_date)
                .where(SessionBookingDay.purchase_id.in_(session_ids))
                .order_by(SessionBookingDay.booking_date)
            )
            sb_dates_result = await db.execute(sb_dates_query)
            for sb_row in sb_dates_result.all():
                session_dates.setdefault(sb_row.purchase_id, []).append(
                    sb_row.booking_date.isoformat() if sb_row.booking_date else None
                )

        # Format response
        purchases = []
        for row in rows:
            purchase = {
                "id": row.id,
                "client_id": row.client_id,
                "client_name": row.client_name or "N/A",
                "gym_id": row.gym_id,
                "gym_name": row.gym_name or "N/A",
                "amount": float(row.amount) if row.amount else 0.0,
                "purchased_at": row.purchased_at,
                "type": row.type,
                "gym_contact": row.gym_contact or None,
                "owner_contact": row.owner_contact or None,
                "owner_name": row.owner_name or "N/A",
                "gym_area": row.gym_area or "N/A",
                "client_contact": row.client_contact or None
            }

            if row.type == "Daily Pass":
                purchase["days_total"] = row.days_total
                purchase["session_display"] = None
                purchase["session_schedule"] = []
                purchase["scheduled_date"] = daily_pass_dates.get(row.id, [])
            else:  # Session
                purchase["days_total"] = None
                purchase["session_display"] = get_session_display(row.scheduled_sessions)
                purchase["session_schedule"] = get_session_schedule(row.scheduled_sessions)
                purchase["scheduled_date"] = session_dates.get(row.id, [])

            purchases.append(purchase)

        # Calculate pagination info
        total_pages = (total + limit - 1) // limit if total > 0 else 0
        has_next = page < total_pages
        has_prev = page > 1

        return {
            "success": True,
            "data": {
                "purchases": purchases,
                "pagination": {
                    "total": total,
                    "page": page,
                    "limit": limit,
                    "totalPages": total_pages,
                    "hasNext": has_next,
                    "hasPrev": has_prev
                }
            }
        }

    except HTTPException:
        raise
    except Exception as e:
        import logging
        logging.error(f"Error in get_all_purchases: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail="An error occurred while fetching purchases"
        )


@router.get("/today-schedule")
async def get_today_schedule(
    page: int = Query(1, ge=1, description="Page number"),
    limit: int = Query(10, ge=1, le=100, description="Items per page"),
    db: AsyncSession = Depends(get_async_db)
):
   
    try:
        today = date.today()

        # Build DailyPassDay subquery with type label
        daily_pass_query = (
            select(
                DailyPassDay.id,
                DailyPassDay.scheduled_date.label("booking_date"),
                DailyPassDay.status.label("day_status"),
                literal(None).label("checkin_at"),
                DailyPass.days_total,
                (Payment.amount_minor / 100.0).label("amount"),
                DailyPass.created_at.label("purchased_at"),
                Client.name.label("client_name"),
                Gym.name.label("gym_name"),
                literal("Daily Pass").label("type"),
            )
            .select_from(DailyPassDay)
            .join(DailyPass, DailyPassDay.pass_id == DailyPass.id)
            .outerjoin(Client, cast(DailyPassDay.client_id, Integer) == Client.client_id)
            .outerjoin(Gym, cast(DailyPassDay.gym_id, Integer) == Gym.gym_id)
            .outerjoin(Payment, DailyPass.payment_id == Payment.provider_payment_id)
            .where(DailyPassDay.scheduled_date == today)
            .where(DailyPassDay.gym_id != 1)  # Exclude gym_id = 1
        )

        # Build SessionBookingDay subquery with type label
        session_booking_query = (
            select(
                SessionBookingDay.id,
                SessionBookingDay.booking_date,
                SessionBookingDay.status.label("day_status"),
                SessionBookingDay.scanned_at.label("checkin_at"),
                literal(None).label("days_total"),
                SessionPurchase.payable_rupees.label("amount"),
                SessionPurchase.created_at.label("purchased_at"),
                Client.name.label("client_name"),
                Gym.name.label("gym_name"),
                literal("Session").label("type"),
            )
            .select_from(SessionBookingDay)
            .join(SessionPurchase, SessionBookingDay.purchase_id == SessionPurchase.id)
            .outerjoin(Client, SessionBookingDay.client_id == Client.client_id)
            .outerjoin(Gym, SessionBookingDay.gym_id == Gym.gym_id)
            .where(SessionBookingDay.booking_date == today)
            .where(SessionBookingDay.gym_id != 1)  # Exclude gym_id = 1
        )

        # Combine both queries with UNION ALL
        combined_query = union_all(daily_pass_query, session_booking_query).alias("combined_schedule")

        # Count total records for pagination
        count_query = select(func.count()).select_from(combined_query)
        count_result = await db.execute(count_query)
        total = count_result.scalar() or 0

        # Early return if no results
        if total == 0:
            return {
                "success": True,
                "data": {
                    "schedule": [],
                    "date": today.isoformat(),
                    "pagination": {
                        "total": 0,
                        "page": page,
                        "limit": limit,
                        "totalPages": 0,
                        "hasNext": False,
                        "hasPrev": False
                    }
                }
            }

        # Build the final paginated query from the union result
        final_query = (
            select(
                combined_query.c.id,
                combined_query.c.booking_date,
                combined_query.c.day_status,
                combined_query.c.checkin_at,
                combined_query.c.days_total,
                combined_query.c.amount,
                combined_query.c.purchased_at,
                combined_query.c.client_name,
                combined_query.c.gym_name,
                combined_query.c.type
            )
            .select_from(combined_query)
            .order_by(combined_query.c.booking_date.desc(), combined_query.c.purchased_at.desc())
            .offset((page - 1) * limit)
            .limit(limit)
        )

        result = await db.execute(final_query)
        rows = result.all()

        # Format response
        schedule = [
            {
                "id": row.id,
                "client_name": row.client_name or "N/A",
                "gym_name": row.gym_name or "N/A",
                "scheduled_date": row.booking_date.isoformat() if row.booking_date else None,
                "status": row.day_status,
                "checkin_at": row.checkin_at.isoformat() if row.checkin_at else None,
                "days_total": row.days_total,
                "amount": float(row.amount) if row.amount else 0.0,
                "purchased_at": row.purchased_at,
                "type": row.type
            }
            for row in rows
        ]

        # Calculate pagination info
        total_pages = (total + limit - 1) // limit if total > 0 else 0
        has_next = page < total_pages
        has_prev = page > 1

        return {
            "success": True,
            "data": {
                "schedule": schedule,
                "date": today.isoformat(),
                "pagination": {
                    "total": total,
                    "page": page,
                    "limit": limit,
                    "totalPages": total_pages,
                    "hasNext": has_next,
                    "hasPrev": has_prev
                }
            }
        }

    except HTTPException:
        raise
    except Exception as e:
        import logging
        logging.error(f"Error in get_today_schedule: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail="An error occurred while fetching today's schedule"
        )


@router.get("/gym-memberships")
async def get_gym_memberships(
    page: int = Query(1, ge=1, description="Page number"),
    limit: int = Query(10, ge=1, le=100, description="Items per page"),
    db: AsyncSession = Depends(get_async_db)
):
    """
    Get all gym memberships with pagination.
    Using same logic as Financials/Revenue Analytics APIs (Order-based approach).
    Excludes gym_id = 1, rows where client_id is not in clients table,
    and rows where gym_id is not in gyms table.
    """
    try:
        # Fetch all gym memberships using Order-based approach (same as Financials API)
        gym_membership_stmt = (
            select(Payment, Order)
            .join(Order, Order.id == Payment.order_id)
            .where(Payment.status == "captured")
            .where(Order.status == "paid")
        )
        gym_membership_result = await db.execute(gym_membership_stmt)
        all_payments = gym_membership_result.all()

        # Collect order IDs to fetch gym info from order_items (exclude gym_id = 1)
        order_ids = [row.Order.id for row in all_payments]

        # Fetch order items to get gym_ids (exclude gym_id = 1)
        order_gym_mapping = {}
        order_gym_id_mapping = {}  # Store actual gym_id values
        if order_ids:
            order_items_stmt = (
                select(OrderItem)
                .where(OrderItem.order_id.in_(order_ids))
                .where(OrderItem.gym_id.isnot(None))
                .where(OrderItem.gym_id != "1")
            )
            order_items_result = await db.execute(order_items_stmt)
            order_items = order_items_result.scalars().all()

            # Create mapping from order_id to gym_id
            for item in order_items:
                if item.gym_id and item.gym_id.strip() and item.gym_id.isdigit():
                    order_gym_mapping[item.order_id] = int(item.gym_id)
                    order_gym_id_mapping[item.order_id] = item.gym_id

        # Filter by metadata conditions and collect valid orders
        valid_orders = []
        for row in all_payments:
            payment = row.Payment
            order = row.Order

            # Check order_metadata for specific conditions (same as Financials/Revenue Analytics)
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

            # Only include if any condition matches AND order has valid gym_id (not gym_id = 1)
            if not (condition1 or condition2 or condition3):
                continue

            if order.id not in order_gym_mapping:
                continue

            valid_orders.append({
                "order": order,
                "payment": payment,
                "gym_id": order_gym_id_mapping[order.id]
            })

        # Get total count for pagination
        total = len(valid_orders)

        # Early return if no results
        if total == 0:
            return {
                "success": True,
                "data": {
                    "memberships": [],
                    "pagination": {
                        "total": 0,
                        "page": page,
                        "limit": limit,
                        "totalPages": 0,
                        "hasNext": False,
                        "hasPrev": False
                    }
                }
            }

        # Sort by purchased_at descending and apply pagination
        valid_orders.sort(key=lambda x: x["order"].created_at, reverse=True)

        start_idx = (page - 1) * limit
        end_idx = start_idx + limit
        paginated_orders = valid_orders[start_idx:end_idx]

        # Fetch additional details (Client, Gym, GymOwner) for paginated orders
        memberships = []
        for item in paginated_orders:
            order = item["order"]
            gym_id_str = item["gym_id"]

            # Fetch client details (using customer_id from Payment)
            # Skip row if client not found in clients table
            if not order.customer_id:
                continue

            client_stmt = select(Client).where(Client.client_id == int(order.customer_id))
            client_result = await db.execute(client_stmt)
            client = client_result.scalar_one_or_none()
            if not client:
                # Skip this row if client not found in clients table
                continue

            client_name = client.name or "N/A"
            client_contact = client.contact

            # Fetch gym details
            # Skip row if gym not found in gyms table
            if not gym_id_str or not gym_id_str.isdigit():
                continue

            gym_stmt = select(Gym).where(Gym.gym_id == int(gym_id_str))
            gym_result = await db.execute(gym_stmt)
            gym = gym_result.scalar_one_or_none()
            if not gym:
                # Skip this row if gym not found in gyms table
                continue

            gym_name = gym.name or "N/A"
            gym_contact = gym.contact_number
            gym_area = gym.area or "N/A"

            # Fetch gym owner details
            owner_name = "N/A"
            owner_contact = None
            if gym.owner_id:
                owner_stmt = select(GymOwner).where(GymOwner.owner_id == gym.owner_id)
                owner_result = await db.execute(owner_stmt)
                owner = owner_result.scalar_one_or_none()
                if owner:
                    owner_name = owner.name or "N/A"
                    owner_contact = owner.contact_number

            memberships.append({
                "id": order.id,
                "client_name": client_name,
                "gym_name": gym_name,
                "type": "gym_membership",
                "amount": float(order.gross_amount_minor / 100) if order.gross_amount_minor else 0.0,
                "purchased_at": order.created_at,
                "gym_contact": gym_contact,
                "owner_contact": owner_contact,
                "owner_name": owner_name,
                "gym_area": gym_area,
                "client_contact": client_contact
            })

        total_pages = (total + limit - 1) // limit if total > 0 else 0
        has_next = page < total_pages
        has_prev = page > 1

        return {
            "success": True,
            "data": {
                "memberships": memberships,
                "pagination": {
                    "total": total,
                    "page": page,
                    "limit": limit,
                    "totalPages": total_pages,
                    "hasNext": has_next,
                    "hasPrev": has_prev
                }
            }
        }

    except HTTPException:
        raise
    except Exception as e:
        import logging
        logging.error(f"Error in get_gym_memberships: {str(e)}")
        import traceback
        traceback.print_exc()
        raise HTTPException(
            status_code=500,
            detail="An error occurred while fetching gym memberships"
        )


@router.get("/export-purchases")
async def export_purchases(
    search: Optional[str] = Query(None, description="Search by client or gym name"),
    db: AsyncSession = Depends(get_async_db)
):
    """
    Export all purchases to CSV file.
    Returns all purchases (without pagination) for export purposes.
    """
    try:
        search_pattern = f"%{search}%" if search else None

        # Build DailyPass subquery with type label
        daily_pass_query = (
            select(
                DailyPass.id.label("id"),
                DailyPass.client_id.label("client_id"),
                DailyPass.gym_id.label("gym_id"),
                literal("Daily Pass").label("type"),
                DailyPass.days_total.label("days_total"),
                literal(None).label("total_sessions"),
                literal(None).label("scheduled_sessions"),
                (Payment.amount_minor / 100.0).label("amount"),
                DailyPass.created_at.label("purchased_at"),
                Client.name.label("client_name"),
                Gym.name.label("gym_name"),
                Gym.contact_number.label("gym_contact"),
                GymOwner.contact_number.label("owner_contact")
            )
            .select_from(DailyPass)
            .outerjoin(Client, cast(DailyPass.client_id, Integer) == Client.client_id)
            .outerjoin(Gym, cast(DailyPass.gym_id, Integer) == Gym.gym_id)
            .outerjoin(GymOwner, Gym.owner_id == GymOwner.owner_id)
            .outerjoin(Payment, DailyPass.payment_id == Payment.provider_payment_id)
        )

        # Build SessionPurchase subquery with type label (only paid status)
        session_purchase_query = (
            select(
                SessionPurchase.id.label("id"),
                SessionPurchase.client_id.label("client_id"),
                SessionPurchase.gym_id.label("gym_id"),
                literal("Session").label("type"),
                literal(None).label("days_total"),
                SessionPurchase.sessions_count.label("total_sessions"),
                SessionPurchase.scheduled_sessions.label("scheduled_sessions"),
                SessionPurchase.payable_rupees.label("amount"),
                SessionPurchase.created_at.label("purchased_at"),
                Client.name.label("client_name"),
                Gym.name.label("gym_name"),
                Gym.contact_number.label("gym_contact"),
                GymOwner.contact_number.label("owner_contact")
            )
            .select_from(SessionPurchase)
            .outerjoin(Client, SessionPurchase.client_id == Client.client_id)
            .outerjoin(Gym, SessionPurchase.gym_id == Gym.gym_id)
            .outerjoin(GymOwner, Gym.owner_id == GymOwner.owner_id)
            .where(SessionPurchase.status == "paid")
        )

        # Apply search filters to both subqueries if search is provided
        if search:
            daily_pass_query = daily_pass_query.where(
                or_(
                    Client.name.ilike(search_pattern),
                    Gym.name.ilike(search_pattern)
                )
            )
            session_purchase_query = session_purchase_query.where(
                or_(
                    Client.name.ilike(search_pattern),
                    Gym.name.ilike(search_pattern)
                )
            )

        # Combine both queries with UNION ALL
        combined_query = union_all(daily_pass_query, session_purchase_query).alias("combined_purchases")

        # Build the final query from the union result (no pagination for export)
        final_query = (
            select(
                combined_query.c.id,
                combined_query.c.client_id,
                combined_query.c.gym_id,
                combined_query.c.type,
                combined_query.c.days_total,
                combined_query.c.total_sessions,
                combined_query.c.scheduled_sessions,
                combined_query.c.amount,
                combined_query.c.purchased_at,
                combined_query.c.client_name,
                combined_query.c.gym_name,
                combined_query.c.gym_contact,
                combined_query.c.owner_contact
            )
            .select_from(combined_query)
            .order_by(combined_query.c.purchased_at.desc())
        )

        # Execute query
        result = await db.execute(final_query)
        rows = result.all()

        # Process scheduled_sessions to get display format
        def get_session_display(scheduled_sessions_json):
            """Process scheduled_sessions JSON to get 'X / Y sessions' format."""
            if not scheduled_sessions_json:
                return "N/A"
            try:
                if isinstance(scheduled_sessions_json, str):
                    sessions = json.loads(scheduled_sessions_json)
                elif isinstance(scheduled_sessions_json, list):
                    sessions = scheduled_sessions_json
                else:
                    return "N/A"

                if not sessions:
                    return "N/A"

                total_sessions = len(sessions)
                unique_dates = len(set(s.get("date") for s in sessions if s.get("date")))
                return f"{unique_dates} / {total_sessions} sessions"
            except Exception:
                return "N/A"

        # Format purchase date
        def format_date(date_obj):
            if date_obj:
                return date_obj.strftime("%d-%b-%Y")
            return "N/A"

        # Format amount
        def format_amount(amount):
            return f"₹{float(amount):.2f}" if amount else "₹0.00"

        # Create Excel workbook
        wb = Workbook()
        ws = wb.active
        ws.title = "Purchases"

        # Write header
        headers = ["Client Name", "Gym Name", "Type", "Days / Sessions", "Amount", "Purchased At"]
        ws.append(headers)

        # Style the header row
        header_fill = PatternFill(start_color="FF5757", end_color="FF5757", fill_type="solid")
        header_font = Font(bold=True, color="FFFFFF")
        for col_num, header in enumerate(headers, 1):
            cell = ws.cell(row=1, column=col_num)
            cell.font = header_font
            cell.fill = header_fill

        # Write data rows
        for row in rows:
            if row.type == "Daily Pass":
                days_sessions = str(row.days_total) if row.days_total else "N/A"
            else:  # Session
                days_sessions = get_session_display(row.scheduled_sessions)

            ws.append([
                row.client_name or "N/A",
                row.gym_name or "N/A",
                row.type,
                days_sessions,
                format_amount(row.amount),
                format_date(row.purchased_at)
            ])

        # Auto-adjust column widths
        for column in ws.columns:
            max_length = 0
            column_letter = column[0].column_letter
            for cell in column:
                try:
                    if len(str(cell.value)) > max_length:
                        max_length = len(str(cell.value))
                except:
                    pass
            adjusted_width = min(max_length + 2, 50)
            ws.column_dimensions[column_letter].width = adjusted_width

        # Save to bytes
        output = io.BytesIO()
        wb.save(output)
        output.seek(0)

        # Generate filename with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"purchases_export_{timestamp}.xlsx"

        # Return Excel file as streaming response
        return StreamingResponse(
            output,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={
                "Content-Disposition": f"attachment; filename={filename}"
            }
        )

    except HTTPException:
        raise
    except Exception as e:
        import logging
        logging.error(f"Error in export_purchases: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail="An error occurred while exporting purchases"
        )


@router.get("/export-today-schedule")
async def export_today_schedule(
    db: AsyncSession = Depends(get_async_db)
):
    """
    Export today's schedule to Excel file.
    Returns all daily pass days and session bookings scheduled for today (without pagination).
    """
    try:
        today = date.today()

        # Build DailyPassDay subquery with type label
        daily_pass_query = (
            select(
                DailyPassDay.id,
                DailyPassDay.scheduled_date.label("booking_date"),
                DailyPassDay.status.label("day_status"),
                literal(None).label("checkin_at"),
                DailyPass.days_total,
                (Payment.amount_minor / 100.0).label("amount"),
                DailyPass.created_at.label("purchased_at"),
                Client.name.label("client_name"),
                Gym.name.label("gym_name"),
                literal("Daily Pass").label("type"),
            )
            .select_from(DailyPassDay)
            .join(DailyPass, DailyPassDay.pass_id == DailyPass.id)
            .outerjoin(Client, cast(DailyPassDay.client_id, Integer) == Client.client_id)
            .outerjoin(Gym, cast(DailyPassDay.gym_id, Integer) == Gym.gym_id)
            .outerjoin(Payment, DailyPass.payment_id == Payment.provider_payment_id)
            .where(DailyPassDay.scheduled_date == today)
            .where(DailyPassDay.gym_id != 1)  # Exclude gym_id = 1
        )

        # Build SessionBookingDay subquery with type label
        session_booking_query = (
            select(
                SessionBookingDay.id,
                SessionBookingDay.booking_date,
                SessionBookingDay.status.label("day_status"),
                SessionBookingDay.scanned_at.label("checkin_at"),
                literal(None).label("days_total"),
                SessionPurchase.payable_rupees.label("amount"),
                SessionPurchase.created_at.label("purchased_at"),
                Client.name.label("client_name"),
                Gym.name.label("gym_name"),
                literal("Session").label("type"),
            )
            .select_from(SessionBookingDay)
            .join(SessionPurchase, SessionBookingDay.purchase_id == SessionPurchase.id)
            .outerjoin(Client, SessionBookingDay.client_id == Client.client_id)
            .outerjoin(Gym, SessionBookingDay.gym_id == Gym.gym_id)
            .where(SessionBookingDay.booking_date == today)
            .where(SessionBookingDay.gym_id != 1)  # Exclude gym_id = 1
        )

        # Combine both queries with UNION ALL
        combined_query = union_all(daily_pass_query, session_booking_query).alias("combined_schedule")

        # Build the final query from the union result (no pagination for export)
        final_query = (
            select(
                combined_query.c.id,
                combined_query.c.booking_date,
                combined_query.c.day_status,
                combined_query.c.checkin_at,
                combined_query.c.days_total,
                combined_query.c.amount,
                combined_query.c.purchased_at,
                combined_query.c.client_name,
                combined_query.c.gym_name,
                combined_query.c.type
            )
            .select_from(combined_query)
            .order_by(combined_query.c.booking_date.desc(), combined_query.c.purchased_at.desc())
        )

        # Execute query
        result = await db.execute(final_query)
        rows = result.all()

        # Format functions
        def format_date(date_obj):
            if date_obj:
                return date_obj.strftime("%d-%b-%Y")
            return "N/A"

        def format_datetime(date_obj):
            if date_obj:
                return date_obj.strftime("%d-%b-%Y %H:%M")
            return "N/A"

        def format_amount(amount):
            return f"₹{float(amount):.2f}" if amount else "₹0.00"

        # Create Excel workbook
        wb = Workbook()
        ws = wb.active
        ws.title = "Today's Schedule"

        # Write header
        headers = ["Client Name", "Gym Name", "Type", "Scheduled Date", "Status", "Check-in At", "Amount", "Purchased At"]
        ws.append(headers)

        # Style the header row
        header_fill = PatternFill(start_color="FF5757", end_color="FF5757", fill_type="solid")
        header_font = Font(bold=True, color="FFFFFF")
        for col_num, header in enumerate(headers, 1):
            cell = ws.cell(row=1, column=col_num)
            cell.font = header_font
            cell.fill = header_fill

        # Write data rows
        for row in rows:
            ws.append([
                row.client_name or "N/A",
                row.gym_name or "N/A",
                row.type,
                format_date(row.booking_date),
                row.day_status or "N/A",
                format_datetime(row.checkin_at),
                format_amount(row.amount),
                format_datetime(row.purchased_at)
            ])

        # Auto-adjust column widths
        for column in ws.columns:
            max_length = 0
            column_letter = column[0].column_letter
            for cell in column:
                try:
                    if len(str(cell.value)) > max_length:
                        max_length = len(str(cell.value))
                except:
                    pass
            adjusted_width = min(max_length + 2, 50)
            ws.column_dimensions[column_letter].width = adjusted_width

        # Save to bytes
        output = io.BytesIO()
        wb.save(output)
        output.seek(0)

        # Generate filename with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"today_schedule_{timestamp}.xlsx"

        # Return Excel file as streaming response
        return StreamingResponse(
            output,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={
                "Content-Disposition": f"attachment; filename={filename}"
            }
        )

    except HTTPException:
        raise
    except Exception as e:
        import logging
        logging.error(f"Error in export_today_schedule: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail="An error occurred while exporting today's schedule"
        )


@router.get("/export-gym-memberships")
async def export_gym_memberships(
    db: AsyncSession = Depends(get_async_db)
):
    """
    Export all gym memberships to Excel file.
    Using same logic as Financials/Revenue Analytics APIs (Order-based approach).
    Returns all memberships (without pagination) for export purposes.
    Excludes gym_id = 1, rows where client_id is not in clients table,
    and rows where gym_id is not in gyms table.
    """
    try:
        # Fetch all gym memberships using Order-based approach (same as Financials API)
        gym_membership_stmt = (
            select(Payment, Order)
            .join(Order, Order.id == Payment.order_id)
            .where(Payment.status == "captured")
            .where(Order.status == "paid")
        )
        gym_membership_result = await db.execute(gym_membership_stmt)
        all_payments = gym_membership_result.all()

        # Collect order IDs to fetch gym info from order_items (exclude gym_id = 1)
        order_ids = [row.Order.id for row in all_payments]

        # Fetch order items to get gym_ids (exclude gym_id = 1)
        order_gym_mapping = {}
        order_gym_id_mapping = {}  # Store actual gym_id values
        if order_ids:
            order_items_stmt = (
                select(OrderItem)
                .where(OrderItem.order_id.in_(order_ids))
                .where(OrderItem.gym_id.isnot(None))
                .where(OrderItem.gym_id != "1")
            )
            order_items_result = await db.execute(order_items_stmt)
            order_items = order_items_result.scalars().all()

            # Create mapping from order_id to gym_id
            for item in order_items:
                if item.gym_id and item.gym_id.strip() and item.gym_id.isdigit():
                    order_gym_mapping[item.order_id] = int(item.gym_id)
                    order_gym_id_mapping[item.order_id] = item.gym_id

        # Filter by metadata conditions and collect valid orders
        valid_orders = []
        for row in all_payments:
            payment = row.Payment
            order = row.Order

            # Check order_metadata for specific conditions (same as Financials/Revenue Analytics)
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

            # Only include if any condition matches AND order has valid gym_id (not gym_id = 1)
            if not (condition1 or condition2 or condition3):
                continue

            if order.id not in order_gym_mapping:
                continue

            valid_orders.append({
                "order": order,
                "gym_id": order_gym_id_mapping[order.id]
            })

        # Sort by purchased_at descending
        valid_orders.sort(key=lambda x: x["order"].created_at, reverse=True)

        # Format functions
        def format_date(date_obj):
            if date_obj:
                return date_obj.strftime("%d-%b-%Y")
            return "N/A"

        def format_amount(amount):
            return f"₹{float(amount):.2f}" if amount else "₹0.00"

        # Create Excel workbook
        wb = Workbook()
        ws = wb.active
        ws.title = "Gym Memberships"

        # Write header
        headers = ["Client Name", "Gym Name", "Type", "Amount", "Purchased At"]
        ws.append(headers)

        # Style the header row
        header_fill = PatternFill(start_color="FF5757", end_color="FF5757", fill_type="solid")
        header_font = Font(bold=True, color="FFFFFF")
        for col_num, header in enumerate(headers, 1):
            cell = ws.cell(row=1, column=col_num)
            cell.font = header_font
            cell.fill = header_fill

        # Write data rows
        for item in valid_orders:
            order = item["order"]
            gym_id_str = item["gym_id"]

            # Fetch client details
            # Skip row if client not found in clients table
            if not order.customer_id:
                continue

            try:
                client_stmt = select(Client).where(Client.client_id == int(order.customer_id))
                client_result = await db.execute(client_stmt)
                client = client_result.scalar_one_or_none()
                if not client:
                    # Skip this row if client not found in clients table
                    continue
                client_name = client.name or "N/A"
            except:
                continue

            # Fetch gym details
            # Skip row if gym not found in gyms table
            if not gym_id_str or not gym_id_str.isdigit():
                continue

            try:
                gym_stmt = select(Gym).where(Gym.gym_id == int(gym_id_str))
                gym_result = await db.execute(gym_stmt)
                gym = gym_result.scalar_one_or_none()
                if not gym:
                    # Skip this row if gym not found in gyms table
                    continue
                gym_name = gym.name or "N/A"
            except:
                continue

            ws.append([
                client_name,
                gym_name,
                "Gym Membership",
                format_amount((order.gross_amount_minor / 100) if order.gross_amount_minor else 0),
                format_date(order.created_at)
            ])

        # Auto-adjust column widths
        for column in ws.columns:
            max_length = 0
            column_letter = column[0].column_letter
            for cell in column:
                try:
                    if len(str(cell.value)) > max_length:
                        max_length = len(str(cell.value))
                except:
                    pass
            adjusted_width = min(max_length + 2, 50)
            ws.column_dimensions[column_letter].width = adjusted_width

        # Save to bytes
        output = io.BytesIO()
        wb.save(output)
        output.seek(0)

        # Generate filename with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"gym_memberships_{timestamp}.xlsx"

        # Return Excel file as streaming response
        return StreamingResponse(
            output,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={
                "Content-Disposition": f"attachment; filename={filename}"
            }
        )

    except HTTPException:
        raise
    except Exception as e:
        import logging
        logging.error(f"Error in export_gym_memberships: {str(e)}")
        import traceback
        traceback.print_exc()
        raise HTTPException(
            status_code=500,
            detail="An error occurred while exporting gym memberships"
        )
