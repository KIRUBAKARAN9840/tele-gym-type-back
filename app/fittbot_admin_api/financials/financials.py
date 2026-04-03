from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from datetime import datetime, timedelta, date
from sqlalchemy import func, and_, select, distinct
from decimal import Decimal

from app.models.async_database import get_async_db
<<<<<<< HEAD
from app.models.dailypass_models import get_dailypass_session, DailyPass
from app.models.fittbot_models import (
    SessionBookingDay, SessionBooking, Gym, ActiveUser, FittbotGymMembership
)
=======
from app.models.fittbot_models import ActiveUser, Client
>>>>>>> new-feature
from app.fittbot_api.v1.payments.models.payments import Payment
from app.fittbot_api.v1.payments.models.orders import Order, OrderItem
from app.models.adminmodels import Expenses

# Import centralized revenue service
from app.fittbot_admin_api.revenue_service import (
    get_revenue_breakdown,
    paise_to_rupees,
    paise_to_rupees_float
)

router = APIRouter(prefix="/api/admin/financials", tags=["AdminFinancials"])


<<<<<<< HEAD
async def get_revenue_breakdown_optimized(db: AsyncSession, dailypass_session, start_date, end_date):
    """
    Calculate revenue breakdown using optimized bulk queries.
    No loops with database calls - all queries are aggregated.
    """

    # 1. DAILY PASS REVENUE - Single aggregated query
    # Exclude gym_id = 1
    daily_pass_revenue = 0
    try:
        daily_pass_stmt = (
            select(func.coalesce(func.sum(DailyPass.amount_paid), 0))
            .where(func.date(DailyPass.created_at) >= start_date)
            .where(func.date(DailyPass.created_at) <= end_date)
            .where(DailyPass.gym_id != "1")
        )
        daily_pass_result = await db.execute(daily_pass_stmt)
        daily_pass_revenue = float(daily_pass_result.scalar() or 0)
    except Exception as e:
        print(f"[FINANCIALS] Error fetching Daily Pass: {e}")

    # 2. SESSIONS REVENUE - Single aggregated query
    # Exclude gym_id = 1
    sessions_revenue = 0
    try:
        sessions_stmt = (
            select(func.coalesce(func.sum(SessionBooking.price_paid), 0))
            .join(SessionBookingDay, SessionBooking.schedule_id == SessionBookingDay.schedule_id)
            .where(func.date(SessionBookingDay.booking_date) >= start_date)
            .where(func.date(SessionBookingDay.booking_date) <= end_date)
            .where(SessionBookingDay.gym_id != 1)
        )
        sessions_result = await db.execute(sessions_stmt)
        sessions_revenue = float(sessions_result.scalar() or 0)
    except Exception as e:
        print(f"[FINANCIALS] Error fetching Sessions: {e}")

    # 3. FITTBOT SUBSCRIPTION REVENUE - Two bulk aggregated queries
    # Exclude gym_id = 1
    fittbot_subscription_revenue = 0
    try:
        # Method 1: Payments + Orders join (with OrderItem for gym_id filter)
        fittbot_stmt_1 = (
            select(func.coalesce(func.sum(Order.gross_amount_minor), 0))
            .join(Payment, Payment.order_id == Order.id)
            .join(OrderItem, OrderItem.order_id == Order.id)
            .where(Payment.provider == "google_play")
            .where(Payment.status == "captured")
            .where(Order.status == "paid")
            .where(func.date(Payment.captured_at) >= start_date)
            .where(func.date(Payment.captured_at) <= end_date)
            .where(or_(OrderItem.gym_id != "1", OrderItem.gym_id.is_(None)))
        )
        fittbot_result_1 = await db.execute(fittbot_stmt_1)
        fittbot_subscription_revenue += float(fittbot_result_1.scalar() or 0)

        # Method 2: Orders with provider_order_id like 'sub_%' (with OrderItem for gym_id filter)
        fittbot_stmt_2 = (
            select(func.coalesce(func.sum(Order.gross_amount_minor), 0))
            .join(OrderItem, OrderItem.order_id == Order.id)
            .where(Order.provider_order_id.like("sub_%"))
            .where(Order.status == "paid")
            .where(func.date(Order.created_at) >= start_date)
            .where(func.date(Order.created_at) <= end_date)
            .where(or_(OrderItem.gym_id != "1", OrderItem.gym_id.is_(None)))
        )
        fittbot_result_2 = await db.execute(fittbot_stmt_2)
        fittbot_subscription_revenue += float(fittbot_result_2.scalar() or 0)
    except Exception as e:
        print(f"[FINANCIALS] Error fetching Fittbot Subscription: {e}")

    # 4. GYM MEMBERSHIP REVENUE - Direct table check (same as MRR API)
    # Exclude gym_id = 1
    gym_membership_revenue = 0
    try:
        gym_membership_stmt = (
            select(func.coalesce(func.sum(FittbotGymMembership.amount), 0))
            .where(FittbotGymMembership.type.in_(["gym_membership", "personal_training"]))
            .where(func.date(FittbotGymMembership.purchased_at) >= start_date)
            .where(func.date(FittbotGymMembership.purchased_at) <= end_date)
            .where(FittbotGymMembership.gym_id != "1")
        )
        gym_membership_result = await db.execute(gym_membership_stmt)
        # Amount is in rupees, convert to paise (convert to float to avoid Decimal type issues)
        gym_membership_revenue = float(gym_membership_result.scalar() or 0) * 100
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


=======
>>>>>>> new-feature
def calculate_membership_payout(membership_revenue):
    """
    Calculate gym payout for membership revenue.
    Formula:
    1. 15% platform commission
    2. 2% PG deduction on M_total
    3. 2% TDS on amount after commission
    """
    if membership_revenue <= 0:
        return 0, 0, 0, 0

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
    if revenue <= 0:
        return 0, 0, 0, 0

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
        # Active users: users with at least 1 login in the date range
        subquery = select(ActiveUser.client_id).join(
            Client, ActiveUser.client_id == Client.client_id
        ).where(
            and_(
                func.date(ActiveUser.created_at) >= start_date,
                func.date(ActiveUser.created_at) <= end_date,
                Client.gym_id != 1
            )
        )

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

        for condition in conditions:
            paying_users_subquery = paying_users_subquery.where(condition)

        paying_users_subquery = paying_users_subquery.distinct().alias("paying_users")

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
    GST_RATE = Decimal("0.18")  # 18% GST as Decimal

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
            start_date_obj = datetime(2020, 1, 1).date()

        if end_date:
            end_date_obj = datetime.strptime(end_date, "%Y-%m-%d").date()
        else:
            end_date_obj = datetime.now().date()

        print(f"[FINANCIALS] Fetching from {start_date_obj} to {end_date_obj}")

        # Use centralized revenue service
        revenue_data = await get_revenue_breakdown(
            db=db,
            start_date=start_date_obj,
            end_date=end_date_obj,
            exclude_gym_id_one=True
        )

        # Extract individual source revenues (all in PAISA)
        daily_pass_revenue = revenue_data.daily_pass
        sessions_revenue = revenue_data.sessions
        gym_membership_revenue = revenue_data.gym_membership
        fittbot_subscription_revenue = revenue_data.fittbot_subscription

        total_revenue = revenue_data.total_revenue

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

        # Calculate Gross Profit
        fittbot_subscription_gross_profit = net_revenue_data["fittbot_subscription"]["net_revenue"]
        gym_membership_gross_profit = membership_comm - net_revenue_data["gym_membership"]["gst_on_comm"]
        daily_pass_gross_profit = daily_pass_comm - net_revenue_data["daily_pass"]["gst_on_comm"]
        sessions_gross_profit = sessions_comm - net_revenue_data["sessions"]["gst_on_comm"]

        total_gross_profit = fittbot_subscription_gross_profit + gym_membership_gross_profit + daily_pass_gross_profit + sessions_gross_profit

        # Get Total Expenses
        total_expenses = await get_total_expenses(db, start_date_obj, end_date_obj)

        # Get Active Users and Paying Users counts
        active_users_count = await get_active_users_count(db, start_date_obj, end_date_obj)
        paying_users_count = await get_paying_users_count(db, start_date_obj, end_date_obj)

        gross_profit_rupees = paise_to_rupees(total_gross_profit)
        ebita = gross_profit_rupees - total_expenses

        net_revenue_rupees = paise_to_rupees(net_revenue_data["total_net_revenue"])
        arpu = net_revenue_rupees / active_users_count if active_users_count > 0 else 0
        arppu = net_revenue_rupees / paying_users_count if paying_users_count > 0 else 0

        return {
            "success": True,
            "data": {
                "totalRevenue": paise_to_rupees(total_revenue),
                "actualGymPayout": paise_to_rupees(actual_gym_payout),
                "netRevenue": paise_to_rupees(net_revenue_data["total_net_revenue"]),
                "revenueSourceBreakdown": {
                    "daily_pass": paise_to_rupees(daily_pass_revenue),
                    "sessions": paise_to_rupees(sessions_revenue),
                    "fittbot_subscription": paise_to_rupees(fittbot_subscription_revenue),
                    "gym_membership": paise_to_rupees(gym_membership_revenue),
                    "total": paise_to_rupees(total_revenue)
                },
                "payoutBreakdown": {
                    "membership": {
                        "revenue": paise_to_rupees(gym_membership_revenue),
                        "payout": paise_to_rupees(membership_payout),
                        "deductions": {
                            "commission": paise_to_rupees(membership_comm),
                            "pg_deduction": paise_to_rupees(membership_pg),
                            "tds_deduction": paise_to_rupees(membership_tds)
                        }
                    },
                    "daily_pass": {
                        "revenue": paise_to_rupees(daily_pass_revenue),
                        "payout": paise_to_rupees(daily_pass_payout),
                        "deductions": {
                            "commission": paise_to_rupees(daily_pass_comm),
                            "pg_deduction": paise_to_rupees(daily_pass_pg),
                            "tds_deduction": paise_to_rupees(daily_pass_tds)
                        }
                    },
                    "sessions": {
                        "revenue": paise_to_rupees(sessions_revenue),
                        "payout": paise_to_rupees(sessions_payout),
                        "deductions": {
                            "commission": paise_to_rupees(sessions_comm),
                            "pg_deduction": paise_to_rupees(sessions_pg),
                            "tds_deduction": paise_to_rupees(sessions_tds)
                        }
                    }
                },
                "totalDeductions": {
                    "commission": paise_to_rupees(total_commission),
                    "pg_deduction": paise_to_rupees(total_pg),
                    "tds_deduction": paise_to_rupees(total_tds),
                    "total": paise_to_rupees(total_deductions)
                },
                "netRevenueBreakdown": {
                    "fittbot_subscription": {
                        "revenue": paise_to_rupees(net_revenue_data["fittbot_subscription"]["revenue"]),
                        "gst": paise_to_rupees(net_revenue_data["fittbot_subscription"]["gst"]),
                        "net_revenue": paise_to_rupees(net_revenue_data["fittbot_subscription"]["net_revenue"])
                    },
                    "gym_membership": {
                        "revenue": paise_to_rupees(net_revenue_data["gym_membership"]["revenue"]),
                        "commission": paise_to_rupees(net_revenue_data["gym_membership"]["commission"]),
                        "gst_on_comm": paise_to_rupees(net_revenue_data["gym_membership"]["gst_on_comm"]),
                        "net_revenue": paise_to_rupees(net_revenue_data["gym_membership"]["net_revenue"])
                    },
                    "daily_pass": {
                        "revenue": paise_to_rupees(net_revenue_data["daily_pass"]["revenue"]),
                        "commission": paise_to_rupees(net_revenue_data["daily_pass"]["commission"]),
                        "gst_on_comm": paise_to_rupees(net_revenue_data["daily_pass"]["gst_on_comm"]),
                        "net_revenue": paise_to_rupees(net_revenue_data["daily_pass"]["net_revenue"])
                    },
                    "sessions": {
                        "revenue": paise_to_rupees(net_revenue_data["sessions"]["revenue"]),
                        "commission": paise_to_rupees(net_revenue_data["sessions"]["commission"]),
                        "gst_on_comm": paise_to_rupees(net_revenue_data["sessions"]["gst_on_comm"]),
                        "net_revenue": paise_to_rupees(net_revenue_data["sessions"]["net_revenue"])
                    },
                    "total_net_revenue": paise_to_rupees(net_revenue_data["total_net_revenue"])
                },
                "grossProfitBreakdown": {
                    "fittbot_subscription": {
                        "revenue": paise_to_rupees(net_revenue_data["fittbot_subscription"]["revenue"]),
                        "gst": paise_to_rupees(net_revenue_data["fittbot_subscription"]["gst"]),
                        "gross_profit": paise_to_rupees(fittbot_subscription_gross_profit)
                    },
                    "gym_membership": {
                        "revenue": paise_to_rupees(net_revenue_data["gym_membership"]["revenue"]),
                        "commission": paise_to_rupees(net_revenue_data["gym_membership"]["commission"]),
                        "gst_on_comm": paise_to_rupees(net_revenue_data["gym_membership"]["gst_on_comm"]),
                        "gross_profit": paise_to_rupees(gym_membership_gross_profit)
                    },
                    "daily_pass": {
                        "revenue": paise_to_rupees(net_revenue_data["daily_pass"]["revenue"]),
                        "commission": paise_to_rupees(net_revenue_data["daily_pass"]["commission"]),
                        "gst_on_comm": paise_to_rupees(net_revenue_data["daily_pass"]["gst_on_comm"]),
                        "gross_profit": paise_to_rupees(daily_pass_gross_profit)
                    },
                    "sessions": {
                        "revenue": paise_to_rupees(net_revenue_data["sessions"]["revenue"]),
                        "commission": paise_to_rupees(net_revenue_data["sessions"]["commission"]),
                        "gst_on_comm": paise_to_rupees(net_revenue_data["sessions"]["gst_on_comm"]),
                        "gross_profit": paise_to_rupees(sessions_gross_profit)
                    },
                    "total_gross_profit": paise_to_rupees(total_gross_profit)
                },
                "grossProfit": paise_to_rupees(total_gross_profit),
                "ebita": {
                    "gross_profit": paise_to_rupees(total_gross_profit),
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
