# MRR (Monthly Recurring Revenue) API
# Based on financials.py revenue sources and net revenue calculation logic
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from datetime import datetime, date, timedelta
from sqlalchemy import func, and_, select, or_, literal
from typing import Dict, Any, List, Tuple
from decimal import Decimal
import calendar

from app.models.async_database import get_async_db
from app.models.dailypass_models import get_dailypass_session, DailyPass
from app.models.fittbot_models import SessionBookingDay, SessionBooking, GymPlans, FittbotGymMembership
from app.fittbot_api.v1.payments.models.payments import Payment
from app.fittbot_api.v1.payments.models.orders import Order, OrderItem
from app.fittbot_api.v1.payments.models.catalog import CatalogProduct

router = APIRouter(prefix="/api/admin/mrr", tags=["MRR"])

# SKU to Duration mapping for Fymble Subscriptions
PRODUCT_PLAN_MAPPING = {
    # Monthly subscriptions
    'FYMBLE_MONTHLY': 1,
    'APP_SUB_MONTHLY': 1,
    'FYMBLE_SUB_MONTHLY': 1,
    'MONTHLY_PREMIUM': 1,

    # Quarterly subscriptions (3 months)
    'FYMBLE_QUARTERLY': 3,
    'APP_SUB_QUARTERLY': 3,
    'FYMBLE_SUB_QUARTERLY': 3,
    'QUARTERLY_PREMIUM': 3,

    # Yearly subscriptions (12 months)
    'FYMBLE_YEARLY': 12,
    'APP_SUB_YEARLY': 12,
    'FYMBLE_SUB_YEARLY': 12,
    'YEARLY_PREMIUM': 12,
    'ANNUAL_PREMIUM': 12,
}


def calculate_net_revenue_for_mrr(
    fittbot_subscription_revenue: float,
    gym_membership_revenue: float,
    daily_pass_revenue: float,
    sessions_revenue: float,
    membership_comm: float,
    daily_pass_comm: float,
    sessions_comm: float
) -> Dict[str, Any]:
    """
    Calculate Net Revenue for all four income categories (same logic as financials.py).

    Logic:
    1. Fymble Subscription: Deduct 18% GST from total revenue
    2. Gym Membership: Deduct 18% GST on platform commission only
    3. Daily Pass: Deduct 18% GST on platform commission only
    4. Session: Deduct 18% GST on platform commission only

    Returns dict with individual net revenues and total net revenue in paise.
    """
    GST_RATE = Decimal("0.18")  # 18% GST

    # Convert all inputs to Decimal
    fittbot_subscription_revenue = Decimal(str(fittbot_subscription_revenue))
    gym_membership_revenue = Decimal(str(gym_membership_revenue))
    daily_pass_revenue = Decimal(str(daily_pass_revenue))
    sessions_revenue = Decimal(str(sessions_revenue))
    membership_comm = Decimal(str(membership_comm))
    daily_pass_comm = Decimal(str(daily_pass_comm))
    sessions_comm = Decimal(str(sessions_comm))

    # 1. Fymble Subscription Net Revenue
    fittbot_subscription_gst = fittbot_subscription_revenue * GST_RATE
    fittbot_subscription_net = fittbot_subscription_revenue - fittbot_subscription_gst

    # 2. Gym Membership Net Revenue
    gym_membership_gst_on_comm = membership_comm * GST_RATE
    gym_membership_net = gym_membership_revenue - gym_membership_gst_on_comm

    # 3. Daily Pass Net Revenue
    daily_pass_gst_on_comm = daily_pass_comm * GST_RATE
    daily_pass_net = daily_pass_revenue - daily_pass_gst_on_comm

    # 4. Session Net Revenue
    sessions_gst_on_comm = sessions_comm * GST_RATE
    sessions_net = sessions_revenue - sessions_gst_on_comm

    # Total Net Revenue
    total_net_revenue = (
        fittbot_subscription_net +
        gym_membership_net +
        daily_pass_net +
        sessions_net
    )

    return {
        "total": float(total_net_revenue),
        "fittbot_subscription": float(fittbot_subscription_net),
        "gym_membership": float(gym_membership_net),
        "daily_pass": float(daily_pass_net),
        "sessions": float(sessions_net)
    }


def calculate_membership_payout(membership_revenue: float) -> tuple:
    """Calculate gym payout for membership revenue (same as financials.py)."""
    if membership_revenue <= 0:
        return 0, 0, 0, 0

    # Convert to Decimal for proper calculation
    membership_revenue = Decimal(str(membership_revenue))

    commission = membership_revenue * Decimal("0.15")  # 15% commission
    pg_deduction = membership_revenue * Decimal("0.02")  # 2% PG on total
    amount_after_commission = membership_revenue - commission
    tds_deduction = amount_after_commission * Decimal("0.02")  # 2% TDS
    final_payout = membership_revenue - commission - pg_deduction - tds_deduction

    return float(max(0, final_payout)), float(commission), float(pg_deduction), float(tds_deduction)


def calculate_daily_pass_session_payout(revenue: float) -> tuple:
    """Calculate gym payout for daily pass or session revenue (same as financials.py)."""
    if revenue <= 0:
        return 0, 0, 0, 0

    # Convert to Decimal for proper calculation
    revenue = Decimal(str(revenue))

    commission = revenue * Decimal("0.30")  # 30% commission
    pg_deduction = revenue * Decimal("0.02")  # 2% PG on total
    amount_after_commission = revenue - commission
    tds_deduction = amount_after_commission * Decimal("0.02")  # 2% TDS
    final_payout = revenue - commission - pg_deduction - tds_deduction

    return float(max(0, final_payout)), float(commission), float(pg_deduction), float(tds_deduction)


async def get_revenue_with_amortization(
    db: AsyncSession,
    dailypass_session,
    target_month_start: date,
    target_month_end: date
) -> Dict[str, int]:
    """
    Calculate MRR for a specific month with proper recurring revenue logic.

    MRR Logic:
    - Daily Pass: Full amount for passes purchased in target month (one-time product)
    - Sessions: Full amount for sessions booked in target month (one-time product)
    - Fymble Subscription: Monthly amortized amount for ALL subscriptions active during target month
                          (active = purchased before or during target month AND validity extends into/through target month)
    - Gym Membership: Monthly amortized amount for ALL memberships active during target month
                       (active = purchased before or during target month AND validity extends into/through target month)

    For recurring products (Fymble Subscriptions & Gym Memberships):
    - Use purchased_at/payment date as start date
    - Calculate validity period based on duration (from plan_id for gym, from SKU mapping for Fymble)
    - Include monthly amortized amount if target month falls within validity period
    """

    # 1. DAILY PASS REVENUE - Only passes purchased in target month (one-time product)
    # Exclude gym_id = 1
    daily_pass_revenue = 0
    try:
        daily_pass_stmt = (
            select(func.coalesce(func.sum(DailyPass.amount_paid), 0))
            .where(func.date(DailyPass.created_at) >= target_month_start)
            .where(func.date(DailyPass.created_at) <= target_month_end)
            .where(DailyPass.gym_id != "1")
        )
        daily_pass_result = await db.execute(daily_pass_stmt)
        daily_pass_revenue = daily_pass_result.scalar() or 0
    except Exception as e:
        print(f"[MRR] Error fetching Daily Pass: {e}")

    # 2. SESSIONS REVENUE - Only sessions booked in target month (one-time product)
    # Exclude gym_id = 1
    sessions_revenue = 0
    try:
        sessions_stmt = (
            select(func.coalesce(func.sum(SessionBooking.price_paid), 0))
            .join(SessionBookingDay, SessionBooking.schedule_id == SessionBookingDay.schedule_id)
            .where(func.date(SessionBookingDay.booking_date) >= target_month_start)
            .where(func.date(SessionBookingDay.booking_date) <= target_month_end)
            .where(SessionBookingDay.gym_id != 1)
        )
        sessions_result = await db.execute(sessions_stmt)
        sessions_revenue = sessions_result.scalar() or 0
    except Exception as e:
        print(f"[MRR] Error fetching Sessions: {e}")

    # 3. FITTBOT SUBSCRIPTION REVENUE - All subscriptions ACTIVE during target month
    # Active = payment_date <= target_month_end AND (payment_date + duration_months) > target_month_start
    # Exclude gym_id = 1
    fittbot_subscription_revenue = 0
    try:
        # Fetch all relevant subscriptions with their payment dates and SKUs
        # We'll filter in Python since we need to calculate validity periods
        fittbot_stmt = (
            select(
                Order.gross_amount_minor,
                OrderItem.sku,
                Payment.captured_at,
                Order.created_at
            )
            .select_from(Order)
            .join(OrderItem, OrderItem.order_id == Order.id)
            .join(Payment, Payment.order_id == Order.id)
            .where(Order.status == "paid")
            .where(
                or_(
                    Payment.provider == "google_play",
                    Order.provider_order_id.like("sub_%"),
                    OrderItem.item_type == "app_subscription",
                    OrderItem.item_type == "fittbot_subscription"
                )
            )
            .where(or_(OrderItem.gym_id != "1", OrderItem.gym_id.is_(None)))
        )

        fittbot_result = await db.execute(fittbot_stmt)
        subscriptions = fittbot_result.all()

        for sub in subscriptions:
            amount = sub.gross_amount_minor or 0
            sku = sub.sku
            # Use captured_at if available, otherwise fall back to created_at
            payment_date = sub.captured_at or sub.created_at

            if not payment_date:
                continue

            payment_date_only = payment_date.date() if isinstance(payment_date, datetime) else payment_date
            duration_months = PRODUCT_PLAN_MAPPING.get(sku, 1)

            # Calculate validity end date: payment_date + duration_months
            # Example: Purchased Jan 15, 12 months -> valid from Jan 15 to Jan 15 next year
            validity_end_date = (
                date(payment_date_only.year, payment_date_only.month, 1) +
                timedelta(days=32 * duration_months)
            )
            validity_end_date = date(validity_end_date.year, validity_end_date.month, 1) - timedelta(days=1)

            # Check if target month overlaps with validity period
            # Active if: validity_end_date >= target_month_start AND payment_date <= target_month_end
            if validity_end_date >= target_month_start and payment_date_only <= target_month_end:
                mrr_contribution = amount / duration_months
                fittbot_subscription_revenue += mrr_contribution

    except Exception as e:
        print(f"[MRR] Error fetching Fittbot Subscription: {e}")
        fittbot_subscription_revenue = 0

    # 4. GYM MEMBERSHIP REVENUE - All memberships ACTIVE during target month
    # Using same logic as Financials/Revenue Analytics APIs (Order-based with metadata conditions)
    # Metadata conditions: audit.source = "dailypass_checkout_api" OR
    #                      order_info.flow = "unified_gym_membership_with_sub" OR
    #                      order_info.flow = "unified_gym_membership_with_free_fittbot"
    # Exclude gym_id = 1
    # Amortization: Default to 1 month duration (since Orders and FittbotGymMembership are in different schemas)
    gym_membership_revenue = 0
    try:
        # Fetch payments and orders (same as financials.py)
        gym_membership_stmt = (
            select(Payment, Order)
            .join(Order, Order.id == Payment.order_id)
            .where(Payment.status == "captured")
            .where(Order.status == "paid")
            .where(func.date(Payment.captured_at) >= target_month_start)
            .where(func.date(Payment.captured_at) <= target_month_end)
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

            # For MRR, use the amount directly with 1 month amortization (default duration)
            # Since Orders and FittbotGymMembership are in different schemas and can't be linked
            amount = order.gross_amount_minor or 0
            gym_membership_revenue += amount

    except Exception as e:
        print(f"[MRR] Error fetching Gym Membership: {e}")
        import traceback
        traceback.print_exc()
        gym_membership_revenue = 0

    # Keep as float to preserve decimal precision
    total_revenue = float(daily_pass_revenue) + float(sessions_revenue) + float(fittbot_subscription_revenue) + float(gym_membership_revenue)

    return {
        "total_revenue": total_revenue,
        "daily_pass": float(daily_pass_revenue),
        "sessions": float(sessions_revenue),
        "fittbot_subscription": float(fittbot_subscription_revenue),
        "gym_membership": float(gym_membership_revenue)
    }


def get_month_date_range(year: int, month: int) -> tuple[date, date]:
    """Get start and end date for a given month/year."""
    start_date = date(year, month, 1)
    last_day = calendar.monthrange(year, month)[1]
    end_date = date(year, month, last_day)
    return start_date, end_date


@router.get("/data")
async def get_mrr_data(
    db: AsyncSession = Depends(get_async_db)
):
    """
    Get MRR (Monthly Recurring Revenue) data including:
    - Current month MRR (net revenue after GST, with amortization)
    - Previous month MRR (net revenue after GST, with amortization)
    - ARR (Annual Recurring Revenue = Previous Month MRR × 12)

    Uses accrual basis accounting with:
    - Amortization for multi-month subscriptions (Fymble & Gym Membership)
    - Recurring revenue from ALL active subscriptions/memberships during the month
    - Net revenue calculation (after GST deductions) same as financials.py

    Recurring Logic:
    - Gym Membership: Uses purchased_at + plan duration to determine active period
    - Fymble Subscription: Uses payment date + SKU duration mapping to determine active period
    - Daily Pass & Sessions: One-time products, counted only in purchase/booked month
    """
    try:
        today = date.today()
        current_year = today.year
        current_month = today.month

        # Calculate previous month
        if current_month == 1:
            prev_year = current_year - 1
            prev_month = 12
        else:
            prev_year = current_year
            prev_month = current_month - 1

        # Get date ranges
        current_start, current_end = get_month_date_range(current_year, current_month)
        prev_start, prev_end = get_month_date_range(prev_year, prev_month)

        # Get dailypass session
        dailypass_session = get_dailypass_session()

        # Calculate CURRENT MONTH revenue with amortization
        current_revenue_data = await get_revenue_with_amortization(
            db, dailypass_session, current_start, current_end
        )

        # Calculate PREVIOUS MONTH revenue with amortization
        prev_revenue_data = await get_revenue_with_amortization(
            db, dailypass_session, prev_start, prev_end
        )

        dailypass_session.close()

        # Calculate commissions for net revenue
        # Current month
        current_membership_payout, current_membership_comm, _, _ = calculate_membership_payout(
            current_revenue_data["gym_membership"]
        )
        current_daily_pass_payout, current_daily_pass_comm, _, _ = calculate_daily_pass_session_payout(
            current_revenue_data["daily_pass"]
        )
        current_sessions_payout, current_sessions_comm, _, _ = calculate_daily_pass_session_payout(
            current_revenue_data["sessions"]
        )

        # Previous month
        prev_membership_payout, prev_membership_comm, _, _ = calculate_membership_payout(
            prev_revenue_data["gym_membership"]
        )
        prev_daily_pass_payout, prev_daily_pass_comm, _, _ = calculate_daily_pass_session_payout(
            prev_revenue_data["daily_pass"]
        )
        prev_sessions_payout, prev_sessions_comm, _, _ = calculate_daily_pass_session_payout(
            prev_revenue_data["sessions"]
        )

        # Calculate NET revenue (after GST) - using same logic as financials.py
        current_net_result = calculate_net_revenue_for_mrr(
            fittbot_subscription_revenue=current_revenue_data["fittbot_subscription"],
            gym_membership_revenue=current_revenue_data["gym_membership"],
            daily_pass_revenue=current_revenue_data["daily_pass"],
            sessions_revenue=current_revenue_data["sessions"],
            membership_comm=current_membership_comm,
            daily_pass_comm=current_daily_pass_comm,
            sessions_comm=current_sessions_comm
        )

        prev_net_result = calculate_net_revenue_for_mrr(
            fittbot_subscription_revenue=prev_revenue_data["fittbot_subscription"],
            gym_membership_revenue=prev_revenue_data["gym_membership"],
            daily_pass_revenue=prev_revenue_data["daily_pass"],
            sessions_revenue=prev_revenue_data["sessions"],
            membership_comm=prev_membership_comm,
            daily_pass_comm=prev_daily_pass_comm,
            sessions_comm=prev_sessions_comm
        )

        current_net_revenue = current_net_result["total"]
        prev_net_revenue = prev_net_result["total"]

        # ARR = Previous Month MRR × 12
        arr = prev_net_revenue * 12

        # Convert to rupees for display (exact values with 2 decimal places)
        current_month_revenue = current_net_revenue / 100
        previous_month_revenue = prev_net_revenue / 100
        arr_revenue = arr / 100

        # Helper function to format to exactly 2 decimal places (truncates, not rounds)
        def format_two_decimal(value):
            return float(f"{value:.2f}")

        return {
            "success": True,
            "data": {
                "currentMonthRevenue": format_two_decimal(current_month_revenue),
                "previousMonthRevenue": format_two_decimal(previous_month_revenue),
                "arr": format_two_decimal(arr_revenue),
                "breakdown": {
                    "current_month": {
                        "fittbot_subscription": format_two_decimal(current_net_result["fittbot_subscription"] / 100),
                        "gym_membership": format_two_decimal(current_net_result["gym_membership"] / 100),
                        "daily_pass": format_two_decimal(current_net_result["daily_pass"] / 100),
                        "sessions": format_two_decimal(current_net_result["sessions"] / 100),
                        "net_revenue": format_two_decimal(current_month_revenue)
                    },
                    "previous_month": {
                        "fittbot_subscription": format_two_decimal(prev_net_result["fittbot_subscription"] / 100),
                        "gym_membership": format_two_decimal(prev_net_result["gym_membership"] / 100),
                        "daily_pass": format_two_decimal(prev_net_result["daily_pass"] / 100),
                        "sessions": format_two_decimal(prev_net_result["sessions"] / 100),
                        "net_revenue": format_two_decimal(previous_month_revenue)
                    }
                },
                "filters": {
                    "currentMonth": {
                        "start": current_start.isoformat(),
                        "end": current_end.isoformat()
                    },
                    "previousMonth": {
                        "start": prev_start.isoformat(),
                        "end": prev_end.isoformat()
                    }
                }
            }


        }

    except Exception as e:
        print(f"[MRR] Error: {str(e)}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))
