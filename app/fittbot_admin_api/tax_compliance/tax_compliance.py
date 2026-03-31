# Tax & Compliance API
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from datetime import datetime, timedelta, date
from sqlalchemy import func, and_, or_, select, distinct, case, literal_column
from pydantic import BaseModel
from typing import Optional
from decimal import Decimal

from app.models.async_database import get_async_db
from app.models.dailypass_models import get_dailypass_session, DailyPass
from app.models.fittbot_models import SessionBookingDay, SessionBooking, SessionPurchase, Gym, ActiveUser
from app.fittbot_api.v1.payments.models.payments import Payment
from app.fittbot_api.v1.payments.models.orders import Order, OrderItem
from app.models.adminmodels import TaxCompliance

router = APIRouter(prefix="/api/admin/tax-compliance", tags=["TaxCompliance"])


class TaxComplianceData(BaseModel):
    month: str
    month_display: str
    gst_collected: float
    gst_paid: float
    gst_payable: float
    tds_collected: float
    tds_paid: float
    tds_payable: float


class TaxComplianceUpdate(BaseModel):
    month: str
    gst_paid: Optional[float] = None
    tds_paid: Optional[float] = None


class TaxComplianceResponse(BaseModel):
    success: bool
    data: list
    message: str


def calculate_membership_payout_deductions(revenue):
    """Calculate commission and TDS for membership revenue."""
    if revenue <= 0:
        return 0, 0

    revenue = Decimal(str(revenue))
    commission = revenue * Decimal("0.15")  # 15% commission
    amount_after_commission = revenue - commission
    tds_deduction = amount_after_commission * Decimal("0.02")  # 2% TDS

    return float(commission), float(tds_deduction)


def calculate_daily_pass_session_payout_deductions(revenue):
    """Calculate commission and TDS for daily pass or session revenue."""
    if revenue <= 0:
        return 0, 0

    revenue = Decimal(str(revenue))
    commission = revenue * Decimal("0.30")  # 30% commission
    amount_after_commission = revenue - commission
    tds_deduction = amount_after_commission * Decimal("0.02")  # 2% TDS

    return float(commission), float(tds_deduction)


async def get_monthly_gst_tds(
    db: AsyncSession,
    dailypass_session,
    year: int,
    month: int
):
    """
    Calculate GST and TDS collected for a specific month.
    Reuses the same logic as financials/overview API.
    """
    # Calculate month start and end dates
    month_start = datetime(year, month, 1).date()

    if month == 12:
        month_end = datetime(year + 1, 1, 1).date() - timedelta(days=1)
    else:
        month_end = datetime(year, month + 1, 1).date() - timedelta(days=1)

    return await get_monthly_gst_tds_optimized(db, dailypass_session, month_start, month_end)


async def get_monthly_gst_tds_optimized(
    db: AsyncSession,
    dailypass_session,
    month_start: date,
    month_end: date
):
    """
    Calculate GST and TDS collected for a date range.
    Optimized version with explicit date boundaries.
    """
    # Use month_start and month_end directly (already calculated)

    # 1. DAILY PASS REVENUE - Exclude gym_id = 1
    daily_pass_revenue = 0
    try:
        daily_pass_stmt = (
            select(func.coalesce(func.sum(DailyPass.amount_paid), 0))
            .where(func.date(DailyPass.created_at) >= month_start)
            .where(func.date(DailyPass.created_at) <= month_end)
            .where(DailyPass.gym_id != "1")
        )
        daily_pass_result = await db.execute(daily_pass_stmt)
        daily_pass_revenue = daily_pass_result.scalar() or 0
    except Exception as e:
        print(f"[TAX_COMPLIANCE] Error fetching Daily Pass: {e}")

    # 2. SESSIONS REVENUE - Exclude gym_id = 1
    # NOTE: SessionPurchase.payable_rupees is in RUPEES, need to convert to PAISA
    sessions_revenue_rupees = 0
    try:
        # Include if created within date range OR updated within date range
        created_in_range = and_(
            func.date(SessionPurchase.created_at) >= month_start,
            func.date(SessionPurchase.created_at) <= month_end
        )
        updated_in_range = and_(
            func.date(SessionPurchase.updated_at) >= month_start,
            func.date(SessionPurchase.updated_at) <= month_end
        )

        sessions_stmt = (
            select(func.coalesce(func.sum(SessionPurchase.payable_rupees), 0))
            .where(SessionPurchase.status == "paid")
            .where(SessionPurchase.gym_id != 1)
            .where(or_(created_in_range, updated_in_range))
        )
        sessions_result = await db.execute(sessions_stmt)
        sessions_revenue_rupees = sessions_result.scalar() or 0
    except Exception as e:
        print(f"[TAX_COMPLIANCE] Error fetching Sessions: {e}")

    # Convert sessions from RUPEES to PAISA for consistency with other revenue sources
    sessions_revenue = int(sessions_revenue_rupees * 100) if sessions_revenue_rupees else 0

    # 3. FITTBOT SUBSCRIPTION REVENUE - Exclude gym_id = 1
    fittbot_subscription_revenue = 0
    try:
        # Method 1: Payments + Orders join, exclude gym_id = 1
        fittbot_stmt_1 = (
            select(func.coalesce(func.sum(Order.gross_amount_minor), 0))
            .join(Payment, Payment.order_id == Order.id)
            .join(OrderItem, OrderItem.order_id == Order.id)
            .where(Payment.provider == "google_play")
            .where(Payment.status == "captured")
            .where(Order.status == "paid")
            .where(func.date(Payment.captured_at) >= month_start)
            .where(func.date(Payment.captured_at) <= month_end)
            .where(or_(OrderItem.gym_id != "1", OrderItem.gym_id.is_(None)))
        )
        fittbot_result_1 = await db.execute(fittbot_stmt_1)
        fittbot_subscription_revenue += fittbot_result_1.scalar() or 0

        # Method 2: Orders with provider_order_id like 'sub_%', exclude gym_id = 1
        fittbot_stmt_2 = (
            select(func.coalesce(func.sum(Order.gross_amount_minor), 0))
            .join(OrderItem, OrderItem.order_id == Order.id)
            .where(Order.provider_order_id.like("sub_%"))
            .where(Order.status == "paid")
            .where(func.date(Order.created_at) >= month_start)
            .where(func.date(Order.created_at) <= month_end)
            .where(or_(OrderItem.gym_id != "1", OrderItem.gym_id.is_(None)))
        )
        fittbot_result_2 = await db.execute(fittbot_stmt_2)
        fittbot_subscription_revenue += fittbot_result_2.scalar() or 0
    except Exception as e:
        print(f"[TAX_COMPLIANCE] Error fetching Fittbot Subscription: {e}")

    # 4. GYM MEMBERSHIP REVENUE - Using same logic as Financials/Revenue Analytics APIs
    # Query: payments joined with orders
    # Filters: status = 'captured', order.status = 'paid'
    # Metadata conditions: audit.source = "dailypass_checkout_api" OR
    #                      order_info.flow = "unified_gym_membership_with_sub" OR
    #                      order_info.flow = "unified_gym_membership_with_free_fittbot"
    # Amount: gross_amount_minor from orders table
    # Exclusion: gym_id != 1 (from order_items table)
    gym_membership_revenue = 0
    try:
        # Fetch payments and orders
        gym_membership_stmt = (
            select(Payment, Order)
            .join(Order, Order.id == Payment.order_id)
            .where(Payment.status == "captured")
            .where(Order.status == "paid")
            .where(func.date(Payment.captured_at) >= month_start)
            .where(func.date(Payment.captured_at) <= month_end)
        )
        gym_membership_result = await db.execute(gym_membership_stmt)
        payments = gym_membership_result.all()

        # Collect order IDs to fetch gym info from order_items (exclude gym_id = 1)
        order_ids = [row.Order.id for row in payments]

        # Fetch order items to get gym_ids (exclude gym_id = 1)
        order_gym_mapping = {}
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

        # Filter by metadata conditions and gym_id exclusion
        for row in payments:
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

            amount = order.gross_amount_minor or 0
            gym_membership_revenue += amount

    except Exception as e:
        print(f"[TAX_COMPLIANCE] Error fetching Gym Membership: {e}")
        import traceback
        traceback.print_exc()

    # Calculate GST Collected
    # Fymble Subscription: 18% GST on total revenue
    fittbot_subscription_gst = float(Decimal(str(fittbot_subscription_revenue)) * Decimal("0.18"))

    # Gym Membership: 18% GST on commission only
    membership_comm, _ = calculate_membership_payout_deductions(gym_membership_revenue)
    gym_membership_gst = float(Decimal(str(membership_comm)) * Decimal("0.18"))

    # Daily Pass: 18% GST on commission only
    daily_pass_comm, _ = calculate_daily_pass_session_payout_deductions(daily_pass_revenue)
    daily_pass_gst = float(Decimal(str(daily_pass_comm)) * Decimal("0.18"))

    # Sessions: 18% GST on commission only
    sessions_comm, _ = calculate_daily_pass_session_payout_deductions(sessions_revenue)
    sessions_gst = float(Decimal(str(sessions_comm)) * Decimal("0.18"))

    # Total GST Collected (in rupees, convert from minor units)
    total_gst_collected = (
        fittbot_subscription_gst +
        gym_membership_gst +
        daily_pass_gst +
        sessions_gst
    ) / 100  # Convert to rupees

    # Calculate TDS Collected
    # Membership TDS
    _, membership_tds = calculate_membership_payout_deductions(gym_membership_revenue)

    # Daily Pass TDS
    _, daily_pass_tds = calculate_daily_pass_session_payout_deductions(daily_pass_revenue)

    # Sessions TDS
    _, sessions_tds = calculate_daily_pass_session_payout_deductions(sessions_revenue)

    # Total TDS Collected (in rupees, convert from minor units)
    total_tds_collected = (
        membership_tds +
        daily_pass_tds +
        sessions_tds
    ) / 100  # Convert to rupees

    return {
        "gst_collected": round(total_gst_collected, 2),
        "tds_collected": round(total_tds_collected, 2)
    }


@router.get("/monthly-data")
async def get_monthly_tax_data(
    page: int = Query(1, description="Page number (starts from 1)", ge=1),
    page_size: int = Query(12, description="Number of records per page", ge=1, le=100),
    db: AsyncSession = Depends(get_async_db)
):
    """
    Get paginated month-wise tax compliance data.
    Backend pagination - fetches only the required page data.
    """
    try:
        import logging
        from datetime import date
        import calendar

        today = date.today()

        # Calculate offset
        offset = (page - 1) * page_size

        # Get dailypass session
        dailypass_session = get_dailypass_session()

        # Fetch ALL paid entries in a single query (no N+1)
        paid_query = select(TaxCompliance)
        paid_result = await db.execute(paid_query)
        all_paid_records = paid_result.scalars().all()

        # Create a dictionary for quick lookup: month -> (gst_paid, tds_paid)
        paid_dict = {
            record.month: (record.gst_paid or 0.0, record.tds_paid or 0.0)
            for record in all_paid_records
        }

        # Generate ONLY the months needed for the current page
        # Calculate the starting month offset
        start_month_offset = offset

        monthly_data = []

        # Generate only the page_size months needed
        for i in range(page_size):
            # Calculate the actual month index (from start)
            month_index = start_month_offset + i

            # Calculate month and year for this index
            # Go back month_index months from current month
            month = today.month - month_index
            year = today.year
            while month <= 0:
                month += 12
                year -= 1

            # Format month string (YYYY-MM)
            month_str = f"{year}-{month:02d}"

            # Get month start and end dates
            month_start = date(year, month, 1)
            last_day_of_month = calendar.monthrange(year, month)[1]
            month_end = date(year, month, last_day_of_month)

            # Get calculated GST and TDS collected (optimized query)
            calculated_data = await get_monthly_gst_tds_optimized(
                db, dailypass_session, month_start, month_end
            )
            gst_collected = calculated_data["gst_collected"]
            tds_collected = calculated_data["tds_collected"]

            # Get paid amounts from dictionary (no additional query!)
            gst_paid, tds_paid = paid_dict.get(month_str, (0.0, 0.0))

            # Calculate payable amounts
            gst_payable = round(gst_collected - gst_paid, 2)
            tds_payable = round(tds_collected - tds_paid, 2)

            # Format month display name (e.g., "January 2026")
            month_name = month_start.strftime("%B %Y")

            monthly_data.append({
                "month": month_str,
                "month_display": month_name,
                "gst_collected": gst_collected,
                "gst_paid": round(gst_paid, 2),
                "gst_payable": gst_payable,
                "tds_collected": tds_collected,
                "tds_paid": round(tds_paid, 2),
                "tds_payable": tds_payable
            })

        dailypass_session.close()

        # Calculate total records (all historical months from start)
        # Assuming data starts from 2020-01-01 (adjust as needed)
        start_year = 2025
        start_month = 1
        total_months = (today.year - start_year) * 12 + today.month - start_month + 1

        # Calculate total pages
        total_pages = (total_months + page_size - 1) // page_size

        return {
            "success": True,
            "data": monthly_data,
            "pagination": {
                "page": page,
                "page_size": page_size,
                "total_records": total_months,
                "total_pages": total_pages,
                "has_next_page": page < total_pages,
                "has_prev_page": page > 1
            },
            "message": "Monthly tax compliance data fetched successfully"
        }

    except Exception as e:
        logging.error(f"[TAX_COMPLIANCE] Error: {str(e)}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/update-paid-amounts")
async def update_paid_amounts(
    payload: TaxComplianceUpdate,
    db: AsyncSession = Depends(get_async_db)
):
    """
    Update GST Paid and TDS Paid amounts for a specific month.
    Creates a new record if it doesn't exist, updates if it does.
    """
    try:
        # Check if record exists
        existing_query = select(TaxCompliance).where(TaxCompliance.month == payload.month)
        existing_result = await db.execute(existing_query)
        existing_record = existing_result.scalar_one_or_none()

        if existing_record:
            # Update existing record
            if payload.gst_paid is not None:
                existing_record.gst_paid = payload.gst_paid
            if payload.tds_paid is not None:
                existing_record.tds_paid = payload.tds_paid
            existing_record.updated_at = datetime.now()
        else:
            # Create new record
            new_record = TaxCompliance(
                month=payload.month,
                gst_paid=payload.gst_paid if payload.gst_paid is not None else 0.0,
                tds_paid=payload.tds_paid if payload.tds_paid is not None else 0.0
            )
            db.add(new_record)

        await db.commit()

        return {
            "success": True,
            "message": f"Tax compliance data for {payload.month} updated successfully"
        }

    except Exception as e:
        await db.rollback()
        import logging
        logging.error(f"[TAX_COMPLIANCE] Error updating paid amounts: {str(e)}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))
    