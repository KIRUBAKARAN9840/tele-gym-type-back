from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from datetime import datetime, timedelta
from sqlalchemy import func, and_, select, distinct, or_
from app.models.async_database import get_async_db
from app.models.dailypass_models import get_dailypass_session, DailyPass
from app.models.fittbot_models import (
    SessionBookingDay, SessionBooking, SessionPurchase, Gym, ActiveUser, Client
)
from app.fittbot_api.v1.payments.models.payments import Payment
from app.fittbot_api.v1.payments.models.orders import Order, OrderItem
from app.models.adminmodels import Expenses

router = APIRouter(prefix="/api/admin/financials", tags=["AdminFinancials"])


async def get_revenue_breakdown_optimized(db: AsyncSession, dailypass_session, start_date, end_date):
    """
    Calculate revenue breakdown using optimized bulk queries.
    No loops with database calls - all queries are aggregated.

    NOTE: Sessions revenue is in RUPEES (from SessionPurchase.payable_rupees).
    All other revenues are in PAISA (minor units).
    Returns all values in PAISA for consistency (sessions is converted to paisa).
    """

    # 1. DAILY PASS REVENUE - Single aggregated query (in PAISA)
    daily_pass_revenue = 0
    try:
        daily_pass_stmt = (
            select(func.coalesce(func.sum(DailyPass.amount_paid), 0))
            .where(func.date(DailyPass.created_at) >= start_date)
            .where(func.date(DailyPass.created_at) <= end_date)
        )
        daily_pass_result = await db.execute(daily_pass_stmt)
        daily_pass_revenue = daily_pass_result.scalar() or 0
    except Exception as e:
        print(f"[FINANCIALS] Error fetching Daily Pass: {e}")

    # 2. SESSIONS REVENUE - Using session_purchases table only (in RUPEES)
    sessions_revenue_rupees = 0
    try:
        # Include if created within date range OR updated within date range
        created_in_range = and_(
            func.date(SessionPurchase.created_at) >= start_date,
            func.date(SessionPurchase.created_at) <= end_date
        )
        updated_in_range = and_(
            func.date(SessionPurchase.updated_at) >= start_date,
            func.date(SessionPurchase.updated_at) <= end_date
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
        print(f"[FINANCIALS] Error fetching Sessions: {e}")

    # Convert sessions from RUPEES to PAISA for consistency
    sessions_revenue = int(sessions_revenue_rupees * 100) if sessions_revenue_rupees else 0

    # 3. FITTBOT SUBSCRIPTION REVENUE - Two bulk aggregated queries (in PAISA)
    fittbot_subscription_revenue = 0
    try:
        # Method 1: Payments + Orders join
        fittbot_stmt_1 = (
            select(func.coalesce(func.sum(Order.gross_amount_minor), 0))
            .join(Payment, Payment.order_id == Order.id)
            .where(Payment.provider == "google_play")
            .where(Payment.status == "captured")
            .where(Order.status == "paid")
            .where(func.date(Payment.captured_at) >= start_date)
            .where(func.date(Payment.captured_at) <= end_date)
        )
        fittbot_result_1 = await db.execute(fittbot_stmt_1)
        fittbot_subscription_revenue += fittbot_result_1.scalar() or 0

        # Method 2: Orders with provider_order_id like 'sub_%'
        fittbot_stmt_2 = (
            select(func.coalesce(func.sum(Order.gross_amount_minor), 0))
            .where(Order.provider_order_id.like("sub_%"))
            .where(Order.status == "paid")
            .where(func.date(Order.created_at) >= start_date)
            .where(func.date(Order.created_at) <= end_date)
        )
        fittbot_result_2 = await db.execute(fittbot_stmt_2)
        fittbot_subscription_revenue += fittbot_result_2.scalar() or 0
    except Exception as e:
        print(f"[FINANCIALS] Error fetching Fittbot Subscription: {e}")

    # 4. GYM MEMBERSHIP REVENUE - Using same logic as Revenue Analytics API (in PAISA)
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
            .where(func.date(Payment.captured_at) >= start_date)
            .where(func.date(Payment.captured_at) <= end_date)
        )
        gym_membership_result = await db.execute(gym_membership_stmt)
        payments = gym_membership_result.all()

        # Collect order IDs to fetch gym info from order_items
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
            # When multiple rows exist for same order_id, prefer the one with valid gym_id
            for item in order_items:
                if item.gym_id and item.gym_id.strip() and item.gym_id.isdigit():
                    order_gym_mapping[item.order_id] = int(item.gym_id)

        # Filter by metadata conditions and gym_id exclusion
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

            # Only include if any condition matches AND order has valid gym_id (not gym_id = 1)
            if (condition1 or condition2 or condition3) and order.id in order_gym_mapping:
                amount = order.gross_amount_minor or 0
                gym_membership_revenue += amount

    except Exception as e:
        print(f"[FINANCIALS] Error fetching Gym Membership: {e}")

    total_revenue = daily_pass_revenue + sessions_revenue + fittbot_subscription_revenue + gym_membership_revenue

    return {
        "total_revenue": total_revenue,
        "daily_pass": daily_pass_revenue,
        "sessions": sessions_revenue,
        "fittbot_subscription": fittbot_subscription_revenue,
        "gym_membership": gym_membership_revenue
    }


def calculate_membership_payout(membership_revenue):
    """
    Calculate gym payout for membership revenue.
    Formula:
    1. 15% platform commission
    2. 2% PG deduction on M_total
    3. 2% TDS on amount after commission
    """
    from decimal import Decimal

    if membership_revenue <= 0:
        return 0, 0, 0, 0

    # Convert to Decimal if not already
    membership_revenue = Decimal(str(membership_revenue))

    commission = membership_revenue * Decimal("0.15")  # 15% commission
    pg_deduction = membership_revenue * Decimal("0.02")  # 2% PG on total
    amount_after_commission = membership_revenue - commission
    tds_deduction = amount_after_commission * Decimal("0.02")  # 2% TDS on post-commission amount
    final_payout = membership_revenue - commission - pg_deduction - tds_deduction

    return max(0, int(final_payout)), int(commission), int(pg_deduction), int(tds_deduction)


def calculate_daily_pass_session_payout(revenue):
    """
    Calculate gym payout for daily pass or session revenue.
    Formula:
    1. 30% platform commission
    2. 2% PG deduction on total
    3. 2% TDS on amount after commission
    """
    from decimal import Decimal

    if revenue <= 0:
        return 0, 0, 0, 0

    # Convert to Decimal if not already
    revenue = Decimal(str(revenue))

    commission = revenue * Decimal("0.30")  # 30% commission
    pg_deduction = revenue * Decimal("0.02")  # 2% PG on total
    amount_after_commission = revenue - commission
    tds_deduction = amount_after_commission * Decimal("0.02")  # 2% TDS on post-commission amount
    final_payout = revenue - commission - pg_deduction - tds_deduction

    return max(0, int(final_payout)), int(commission), int(pg_deduction), int(tds_deduction)


async def get_total_expenses(
    db: AsyncSession,
    start_date,
    end_date
):
    """
    Get total expenses from fittbot_admins.expenses table for a date range.
    Returns the sum of all expenses (both operational and marketing).
    """
    try:
        conditions = [
            Expenses.expense_date >= start_date,
            Expenses.expense_date <= end_date
        ]

        # Single aggregated query for total expenses
        total_query = select(func.coalesce(func.sum(Expenses.amount), 0))
        if conditions:
            total_query = total_query.where(and_(*conditions))

        total_result = await db.execute(total_query)
        grand_total = float(total_result.scalar() or 0)

        return grand_total
    except Exception as e:
        print(f"[FINANCIALS] Error fetching expenses: {e}")
        return 0.0


async def get_active_users_count(
    db: AsyncSession,
    start_date,
    end_date
):

    try:
        # Use the filter date range (start_date to end_date)
        # Only count users with 2+ distinct dates in the selected range
        # Exclude users from gym_id = 1
        subquery = select(ActiveUser.client_id).join(
            Client, ActiveUser.client_id == Client.client_id
        ).where(
            and_(
                func.date(ActiveUser.created_at) >= start_date,
                func.date(ActiveUser.created_at) <= end_date,
                Client.gym_id != 1
            )
        ).group_by(
            ActiveUser.client_id
        ).having(
            func.count(func.distinct(func.date(ActiveUser.created_at))) >= 2
        )

        # Count distinct client_ids that qualify (each qualifying client = 1)
        count_query = select(func.coalesce(func.count(distinct(ActiveUser.client_id)), 0)).where(
            ActiveUser.client_id.in_(subquery)
        )

        count_result = await db.execute(count_query)
        active_users_count = count_result.scalar() or 0

        return int(active_users_count)
    except Exception as e:
        print(f"[FINANCIALS] Error fetching active users: {e}")
        import traceback
        traceback.print_exc()
        return 0


async def get_paying_users_count(
    db: AsyncSession,
    start_date,
    end_date
):

    try:
        conditions = [
            func.date(Payment.created_at) >= start_date,
            func.date(Payment.created_at) <= end_date
        ]

        # Query to count distinct customer_id, excluding gym_id = 1
        # Join Payment -> Order -> OrderItem to filter by gym_id
        from app.fittbot_api.v1.payments.models.orders import Order, OrderItem

        paying_users_subquery = select(Payment.customer_id).join(
            Order, Order.id == Payment.order_id
        ).join(
            OrderItem, OrderItem.order_id == Order.id
        ).where(
            and_(
                OrderItem.gym_id.isnot(None),
                OrderItem.gym_id != "1"
            )
        )

        # Add date conditions
        for condition in conditions:
            paying_users_subquery = paying_users_subquery.where(condition)

        paying_users_subquery = paying_users_subquery.distinct().alias("paying_users")

        # Count the results
        count_query = select(func.count()).select_from(paying_users_subquery)
        count_result = await db.execute(count_query)
        paying_users_count = count_result.scalar() or 0

        return int(paying_users_count)
    except Exception as e:
        print(f"[FINANCIALS] Error fetching paying users: {e}")
        import traceback
        traceback.print_exc()
        return 0


def calculate_net_revenue(
    fittbot_subscription_revenue,
    gym_membership_revenue,
    daily_pass_revenue,
    sessions_revenue,
    membership_comm,
    daily_pass_comm,
    sessions_comm
):
    """
    Calculate Net Revenue for all four income categories.

    Logic:
    1. Fymble Subscription: Deduct 18% GST from total revenue
    2. Gym Membership: Deduct 18% GST on platform commission only
    3. Daily Pass: Deduct 18% GST on platform commission only
    4. Session: Deduct 18% GST on platform commission only

    Returns:
        - Individual net revenue for each category
        - Total net revenue
    """
    from decimal import Decimal
    GST_RATE = Decimal("0.18")  # 18% GST as Decimal

    # Convert all inputs to Decimal for consistent arithmetic
    fittbot_subscription_revenue = Decimal(str(fittbot_subscription_revenue))
    gym_membership_revenue = Decimal(str(gym_membership_revenue))
    daily_pass_revenue = Decimal(str(daily_pass_revenue))
    sessions_revenue = Decimal(str(sessions_revenue))
    membership_comm = Decimal(str(membership_comm))
    daily_pass_comm = Decimal(str(daily_pass_comm))
    sessions_comm = Decimal(str(sessions_comm))

    # 1. Fymble Subscription Net Revenue
    # Net = Total Revenue - 18% GST on total
    fittbot_subscription_gst = fittbot_subscription_revenue * GST_RATE
    fittbot_subscription_net = fittbot_subscription_revenue - fittbot_subscription_gst

    # 2. Gym Membership Net Revenue
    # Net = Total Revenue - 18% GST on platform commission only
    gym_membership_gst_on_comm = membership_comm * GST_RATE
    gym_membership_net = gym_membership_revenue - gym_membership_gst_on_comm

    # 3. Daily Pass Net Revenue
    # Net = Total Revenue - 18% GST on platform commission only
    daily_pass_gst_on_comm = daily_pass_comm * GST_RATE
    daily_pass_net = daily_pass_revenue - daily_pass_gst_on_comm

    # 4. Session Net Revenue
    # Net = Total Revenue - 18% GST on platform commission only
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
        "fittbot_subscription": {
            "revenue": float(fittbot_subscription_revenue),
            "gst": float(fittbot_subscription_gst),
            "net_revenue": float(fittbot_subscription_net)
        },
        "gym_membership": {
            "revenue": float(gym_membership_revenue),
            "commission": float(membership_comm),
            "gst_on_comm": float(gym_membership_gst_on_comm),
            "net_revenue": float(gym_membership_net)
        },
        "daily_pass": {
            "revenue": float(daily_pass_revenue),
            "commission": float(daily_pass_comm),
            "gst_on_comm": float(daily_pass_gst_on_comm),
            "net_revenue": float(daily_pass_net)
        },
        "sessions": {
            "revenue": float(sessions_revenue),
            "commission": float(sessions_comm),
            "gst_on_comm": float(sessions_gst_on_comm),
            "net_revenue": float(sessions_net)
        },
        "total_net_revenue": float(total_net_revenue)
    }


@router.get("/overview")
async def get_financials_overview(
    start_date: str = None,
    end_date: str = None,
    db: AsyncSession = Depends(get_async_db)
):
    """
    Get Financials Dashboard data including:
    - Total Revenue (all sources)
    - Actual Gym Payout (excludes Fymble Subscription)
    """
    try:
        # Parse dates
        if start_date:
            start_date_obj = datetime.strptime(start_date, "%Y-%m-%d").date()
        else:
            # Default to early date for overall data (matching Revenue Analytics behavior)
            start_date_obj = datetime(2020, 1, 1).date()

        if end_date:
            end_date_obj = datetime.strptime(end_date, "%Y-%m-%d").date()
        else:
            # Default to today
            end_date_obj = datetime.now().date()

        print(f"[FINANCIALS] Fetching from {start_date_obj} to {end_date_obj}")

        # Get dailypass session
        dailypass_session = get_dailypass_session()

        # Get revenue breakdown using optimized queries
        revenue_data = await get_revenue_breakdown_optimized(db, dailypass_session, start_date_obj, end_date_obj)

        dailypass_session.close()

        # Extract individual source revenues
        daily_pass_revenue = revenue_data["daily_pass"]
        sessions_revenue = revenue_data["sessions"]
        gym_membership_revenue = revenue_data["gym_membership"]
        fittbot_subscription_revenue = revenue_data["fittbot_subscription"]

        # Calculate Total Revenue (all sources)
        total_revenue = revenue_data["total_revenue"]

        # Calculate Actual Gym Payout (excluding Fymble Subscription)
        membership_payout, membership_comm, membership_pg, membership_tds = calculate_membership_payout(gym_membership_revenue)
        daily_pass_payout, daily_pass_comm, daily_pass_pg, daily_pass_tds = calculate_daily_pass_session_payout(daily_pass_revenue)
        sessions_payout, sessions_comm, sessions_pg, sessions_tds = calculate_daily_pass_session_payout(sessions_revenue)

        actual_gym_payout = membership_payout + daily_pass_payout + sessions_payout

        # Calculate total deductions
        total_commission = membership_comm + daily_pass_comm + sessions_comm
        total_pg = membership_pg + daily_pass_pg + sessions_pg
        total_tds = membership_tds + daily_pass_tds + sessions_tds
        total_deductions = total_commission + total_pg + total_tds

        # Calculate Net Revenue for all categories
        net_revenue_data = calculate_net_revenue(
            fittbot_subscription_revenue=fittbot_subscription_revenue,
            gym_membership_revenue=gym_membership_revenue,
            daily_pass_revenue=daily_pass_revenue,
            sessions_revenue=sessions_revenue,
            membership_comm=membership_comm,
            daily_pass_comm=daily_pass_comm,
            sessions_comm=sessions_comm
        )

        # Fymble Subscription: Same as net revenue (already calculated)
        fittbot_subscription_gross_profit = net_revenue_data["fittbot_subscription"]["net_revenue"]

        # Gym Membership: Commission - 18% GST on commission (already calculated as gst_on_comm)
        gym_membership_gross_profit = membership_comm - net_revenue_data["gym_membership"]["gst_on_comm"]

        # Daily Pass: Commission - 18% GST on commission (already calculated as gst_on_comm)
        daily_pass_gross_profit = daily_pass_comm - net_revenue_data["daily_pass"]["gst_on_comm"]

        # Sessions: Commission - 18% GST on commission (already calculated as gst_on_comm)
        sessions_gross_profit = sessions_comm - net_revenue_data["sessions"]["gst_on_comm"]

        # Total Gross Profit
        total_gross_profit = fittbot_subscription_gross_profit + gym_membership_gross_profit + daily_pass_gross_profit + sessions_gross_profit

        # Get Total Expenses from fittbot_admins.expenses table
        total_expenses = await get_total_expenses(db, start_date_obj, end_date_obj)

        # Get Active Users count from fittbot_local.active_users table
        active_users_count = await get_active_users_count(db, start_date_obj, end_date_obj)

        # Get Paying Users count from payments.payments table
        paying_users_count = await get_paying_users_count(db, start_date_obj, end_date_obj)

        gross_profit_rupees = total_gross_profit / 100
        ebita = gross_profit_rupees - total_expenses

        net_revenue_rupees = net_revenue_data["total_net_revenue"] / 100
        arpu = net_revenue_rupees / active_users_count if active_users_count > 0 else 0

        # Calculate ARPPU (Average Revenue Per Paying User)
        arppu = net_revenue_rupees / paying_users_count if paying_users_count > 0 else 0

        # Revenue source breakdown in rupees (with proper decimal precision)
        revenue_source_breakdown = {
            "daily_pass": round(daily_pass_revenue / 100, 2),
            "sessions": round(sessions_revenue / 100, 2),
            "fittbot_subscription": round(fittbot_subscription_revenue / 100, 2),
            "gym_membership": round(gym_membership_revenue / 100, 2),
            "total": round(total_revenue / 100, 2)
        }

        return {
            "success": True,
            "data": {
                "totalRevenue": round(total_revenue / 100, 2),  # Convert to rupees with 2 decimals
                "actualGymPayout": round(actual_gym_payout / 100, 2),  # Convert to rupees with 2 decimals
                "netRevenue": round(net_revenue_data["total_net_revenue"] / 100, 2),  # Total Net Revenue
                "revenueSourceBreakdown": revenue_source_breakdown,
                "payoutBreakdown": {
                    "membership": {
                        "revenue": round(gym_membership_revenue / 100, 2),
                        "payout": round(membership_payout / 100, 2),
                        "deductions": {
                            "commission": round(membership_comm / 100, 2),
                            "pg_deduction": round(membership_pg / 100, 2),
                            "tds_deduction": round(membership_tds / 100, 2)
                        }
                    },
                    "daily_pass": {
                        "revenue": round(daily_pass_revenue / 100, 2),
                        "payout": round(daily_pass_payout / 100, 2),
                        "deductions": {
                            "commission": round(daily_pass_comm / 100, 2),
                            "pg_deduction": round(daily_pass_pg / 100, 2),
                            "tds_deduction": round(daily_pass_tds / 100, 2)
                        }
                    },
                    "sessions": {
                        "revenue": round(sessions_revenue / 100, 2),
                        "payout": round(sessions_payout / 100, 2),
                        "deductions": {
                            "commission": round(sessions_comm / 100, 2),
                            "pg_deduction": round(sessions_pg / 100, 2),
                            "tds_deduction": round(sessions_tds / 100, 2)
                        }
                    }
                },
                "totalDeductions": {
                    "commission": round(total_commission / 100, 2),
                    "pg_deduction": round(total_pg / 100, 2),
                    "tds_deduction": round(total_tds / 100, 2),
                    "total": round(total_deductions / 100, 2)
                },
                "netRevenueBreakdown": {
                    "fittbot_subscription": {
                        "revenue": round(net_revenue_data["fittbot_subscription"]["revenue"] / 100, 2),
                        "gst": round(net_revenue_data["fittbot_subscription"]["gst"] / 100, 2),
                        "net_revenue": round(net_revenue_data["fittbot_subscription"]["net_revenue"] / 100, 2)
                    },
                    "gym_membership": {
                        "revenue": round(net_revenue_data["gym_membership"]["revenue"] / 100, 2),
                        "commission": round(net_revenue_data["gym_membership"]["commission"] / 100, 2),
                        "gst_on_comm": round(net_revenue_data["gym_membership"]["gst_on_comm"] / 100, 2),
                        "net_revenue": round(net_revenue_data["gym_membership"]["net_revenue"] / 100, 2)
                    },
                    "daily_pass": {
                        "revenue": round(net_revenue_data["daily_pass"]["revenue"] / 100, 2),
                        "commission": round(net_revenue_data["daily_pass"]["commission"] / 100, 2),
                        "gst_on_comm": round(net_revenue_data["daily_pass"]["gst_on_comm"] / 100, 2),
                        "net_revenue": round(net_revenue_data["daily_pass"]["net_revenue"] / 100, 2)
                    },
                    "sessions": {
                        "revenue": round(net_revenue_data["sessions"]["revenue"] / 100, 2),
                        "commission": round(net_revenue_data["sessions"]["commission"] / 100, 2),
                        "gst_on_comm": round(net_revenue_data["sessions"]["gst_on_comm"] / 100, 2),
                        "net_revenue": round(net_revenue_data["sessions"]["net_revenue"] / 100, 2)
                    },
                    "total_net_revenue": round(net_revenue_data["total_net_revenue"] / 100, 2)
                },
                "grossProfitBreakdown": {
                    "fittbot_subscription": {
                        "revenue": round(net_revenue_data["fittbot_subscription"]["revenue"] / 100, 2),
                        "gst": round(net_revenue_data["fittbot_subscription"]["gst"] / 100, 2),
                        "gross_profit": round(fittbot_subscription_gross_profit / 100, 2)
                    },
                    "gym_membership": {
                        "revenue": round(net_revenue_data["gym_membership"]["revenue"] / 100, 2),
                        "commission": round(net_revenue_data["gym_membership"]["commission"] / 100, 2),
                        "gst_on_comm": round(net_revenue_data["gym_membership"]["gst_on_comm"] / 100, 2),
                        "gross_profit": round(gym_membership_gross_profit / 100, 2)
                    },
                    "daily_pass": {
                        "revenue": round(net_revenue_data["daily_pass"]["revenue"] / 100, 2),
                        "commission": round(net_revenue_data["daily_pass"]["commission"] / 100, 2),
                        "gst_on_comm": round(net_revenue_data["daily_pass"]["gst_on_comm"] / 100, 2),
                        "gross_profit": round(daily_pass_gross_profit / 100, 2)
                    },
                    "sessions": {
                        "revenue": round(net_revenue_data["sessions"]["revenue"] / 100, 2),
                        "commission": round(net_revenue_data["sessions"]["commission"] / 100, 2),
                        "gst_on_comm": round(net_revenue_data["sessions"]["gst_on_comm"] / 100, 2),
                        "gross_profit": round(sessions_gross_profit / 100, 2)
                    },
                    "total_gross_profit": round(total_gross_profit / 100, 2)
                },
                "grossProfit": round(total_gross_profit / 100, 2),
                "ebita": {
                    "gross_profit": round(total_gross_profit / 100, 2),
                    "total_expenses": round(total_expenses, 2),
                    "ebita": round(ebita, 2)
                },
                "arpu": {
                    "net_revenue": round(net_revenue_rupees, 2),
                    "active_users": active_users_count,
                    "arpu": round(arpu, 2)
                },
                "arppu": {
                    "net_revenue": round(net_revenue_rupees, 2),
                    "paying_users": paying_users_count,
                    "arppu": round(arppu, 2)
                },
                "filters": {
                    "startDate": start_date_obj.isoformat(),
                    "endDate": end_date_obj.isoformat()
                }
            },
            
        }

    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"Invalid date format: {str(e)}")
    except Exception as e:
        print(f"[FINANCIALS] Error: {str(e)}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))
