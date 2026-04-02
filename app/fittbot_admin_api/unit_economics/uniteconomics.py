# Unit Economics API - CAC Calculation
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from datetime import datetime, timedelta
from sqlalchemy import func, and_, select, distinct, case, literal_column
from pydantic import BaseModel

from app.models.async_database import get_async_db
from app.models.fittbot_models import Client, ActiveUser
from app.models.adminmodels import Expenses

router = APIRouter(prefix="/api/admin/unit-economics", tags=["UnitEconomics"])


class UnitEconomicsResponse(BaseModel):
    success: bool
    data: dict
    message: str


# @router.get("/cac")
# async def get_cac_analytics(
#     start_date: str = Query(None, description="Start date in YYYY-MM-DD format"),
#     end_date: str = Query(None, description="End date in YYYY-MM-DD format"),
#     db: AsyncSession = Depends(get_async_db)
# ):
#     """
#     Get CAC (Customer Acquisition Cost) analytics.

#     CAC = Total Expenses / Total New Users
#     - Total Expenses: SUM(amount) from expenses table where expense_date is in range
#     - Total New Users: COUNT(*) from clients table where created_at is in range
#     """
#     try:
#         import logging

#         # Parse dates if provided
#         if start_date:
#             start_date_obj = datetime.strptime(start_date, "%Y-%m-%d").date()
#         else:
#             # Default to early date for overall data
#             start_date_obj = datetime(2020, 1, 1).date()

#         if end_date:
#             end_date_obj = datetime.strptime(end_date, "%Y-%m-%d").date()
#         else:
#             # Default to today
#             end_date_obj = datetime.now().date()

#         # Adjust end_date to include the full day
#         end_date_inclusive = end_date_obj + timedelta(days=1)

#         # Step 1: Calculate Total Expenses from expenses table
#         total_expenses = 0
#         try:
#             expenses_query = select(func.coalesce(func.sum(Expenses.amount), 0)).where(
#                 and_(
#                     Expenses.expense_date >= start_date_obj,
#                     Expenses.expense_date < end_date_inclusive
#                 )
#             )
#             expenses_result = await db.execute(expenses_query)
#             total_expenses = expenses_result.scalar() or 0
#             logging.info(f"[CAC] Total expenses from {start_date_obj} to {end_date_obj}: {total_expenses}")
#         except Exception as e:
#             logging.error(f"[CAC] Error fetching total expenses: {str(e)}")
#             import traceback
#             traceback.print_exc()

#         # Step 2: Calculate Total New Users from clients table
#         total_new_users = 0
#         try:
#             users_query = select(func.count()).where(
#                 and_(
#                     Client.created_at >= start_date_obj,
#                     Client.created_at < end_date_inclusive
#                 )
#             )
#             users_result = await db.execute(users_query)
#             total_new_users = users_result.scalar() or 0
#             logging.info(f"[CAC] Total new users from {start_date_obj} to {end_date_obj}: {total_new_users}")
#         except Exception as e:
#             logging.error(f"[CAC] Error fetching total new users: {str(e)}")
#             import traceback
#             traceback.print_exc()

#         # Step 3: Calculate CAC (handle division by zero)
#         cac = 0
#         if total_new_users > 0:
#             cac = total_expenses / total_new_users
#         else:
#             cac = 0

#         logging.info(f"[CAC] CAC calculated: {cac} (expenses: {total_expenses}, users: {total_new_users})")

#         analytics_data = {
#             "cac": round(cac, 2),
#             "totalExpenses": round(total_expenses, 2),
#             "totalNewUsers": total_new_users,
#             "filters": {
#                 "startDate": start_date_obj.isoformat(),
#                 "endDate": end_date_obj.isoformat()
#             }
#         }

#         return {
#             "success": True,
#             "data": analytics_data,
#             "message": "CAC analytics fetched successfully"
#         }

#     except ValueError as e:
#         raise HTTPException(status_code=400, detail=f"Invalid date format: {str(e)}")
#     except Exception as e:
#         logging.error(f"[CAC] Error: {str(e)}")
#         import traceback
#         traceback.print_exc()
#         raise HTTPException(status_code=500, detail=str(e))


# @router.get("/ltv")
# async def get_ltv_analytics(
#     db: AsyncSession = Depends(get_async_db)
# ):
#     """
#     Get LTV (Lifetime Value) analytics.

#     LTV = 1 / churn_rate
#     churn_rate = retained_users_count / previous_month_active_users_count

#     Steps:
#     1. Get previous month active users (client_ids with 2+ distinct dates)
#     2. Get current month active users (client_ids with 2+ distinct dates)
#     3. Find retained users (client_ids present in both months)
#     4. Calculate churn_rate = retained_users / previous_month_active_users
#     5. Calculate LTV = 1 / churn_rate
#     """
#     try:
#         import logging

#         today = datetime.now().date()

#         # Calculate current month date range
#         first_day_of_current_month = today.replace(day=1)

#         # Calculate previous month date range
#         first_day_of_previous_month = (first_day_of_current_month - timedelta(days=1)).replace(day=1)
#         last_day_of_previous_month = first_day_of_current_month - timedelta(days=1)

#         logging.info(f"[LTV] Previous month: {first_day_of_previous_month} to {last_day_of_previous_month}")
#         logging.info(f"[LTV] Current month: {first_day_of_current_month} to {today}")

#         # Step 1: Get Previous Month Active Users (client_ids with 2+ distinct dates)
#         # Using ORM with date filter
#         prev_month_start = first_day_of_previous_month
#         prev_month_end = last_day_of_previous_month

#         # Subquery for previous month active users (2+ distinct dates)
#         prev_month_subquery = select(ActiveUser.client_id).where(
#             and_(
#                 func.date(ActiveUser.created_at) >= prev_month_start,
#                 func.date(ActiveUser.created_at) <= prev_month_end
#             )
#         ).group_by(
#             ActiveUser.client_id
#         ).having(
#             func.count(func.distinct(func.date(ActiveUser.created_at))) >= 2
#         )

#         prev_result = await db.execute(prev_month_subquery)
#         previous_month_client_ids = set([row[0] for row in prev_result.fetchall()])
#         previous_month_count = len(previous_month_client_ids)

#         logging.info(f"[LTV] Previous month active users: {previous_month_count}")
#         logging.info(f"[LTV] Previous month client_ids sample: {list(previous_month_client_ids)[:5]}")

#         # Step 2: Get Current Month Active Users (client_ids with 2+ distinct dates)
#         curr_month_start = first_day_of_current_month
#         curr_month_end = today

#         # Subquery for current month active users (2+ distinct dates)
#         curr_month_subquery = select(ActiveUser.client_id).where(
#             and_(
#                 func.date(ActiveUser.created_at) >= curr_month_start,
#                 func.date(ActiveUser.created_at) <= curr_month_end
#             )
#         ).group_by(
#             ActiveUser.client_id
#         ).having(
#             func.count(func.distinct(func.date(ActiveUser.created_at))) >= 2
#         )

#         curr_result = await db.execute(curr_month_subquery)
#         current_month_client_ids = set([row[0] for row in curr_result.fetchall()])
#         current_month_count = len(current_month_client_ids)

#         logging.info(f"[LTV] Current month active users: {current_month_count}")
#         logging.info(f"[LTV] Current month client_ids sample: {list(current_month_client_ids)[:5]}")

#         # Step 3: Find Retained Users (present in both months)
#         retained_client_ids = previous_month_client_ids.intersection(current_month_client_ids)
#         retained_count = len(retained_client_ids)

#         logging.info(f"[LTV] Retained users: {retained_count}")

#         # Step 4: Calculate Churn Rate
#         churn_rate = 0
#         if previous_month_count > 0:
#             churn_rate = retained_count / previous_month_count

#         logging.info(f"[LTV] Churn rate: {churn_rate}")

#         # Step 5: Calculate LTV
#         ltv = 0
#         if churn_rate > 0:
#             ltv = 1 / churn_rate
#         else:
#             ltv = 0

#         logging.info(f"[LTV] LTV calculated: {ltv}")

#         analytics_data = {
#             "ltv": round(ltv, 2),
#             "churnRate": round(churn_rate, 4),
#             "previousMonthActiveUsers": previous_month_count,
#             "currentMonthActiveUsers": current_month_count,
#             "retainedUsers": retained_count,
#         }

#         return {
#             "success": True,
#             "data": analytics_data,
#             "message": "LTV analytics fetched successfully"
#         }

#     except Exception as e:
#         logging.error(f"[LTV] Error: {str(e)}")
#         import traceback
#         traceback.print_exc()
#         raise HTTPException(status_code=500, detail=str(e))


@router.get("/data")
async def get_unit_economics(
    start_date: str = Query(None, description="Start date in YYYY-MM-DD format"),
    end_date: str = Query(None, description="End date in YYYY-MM-DD format"),
    db: AsyncSession = Depends(get_async_db)
):
    """
    Get Unit Economics analytics (CAC + LTV + D30 Retention).

    CAC = Total Expenses / Total New Users
    LTV = 1 / churn_rate
    D30 Retention = Users active 30 days ago who are still active today
    """
    import logging

    # ========== CAC CALCULATION ==========
    # Parse dates if provided
    if start_date:
        start_date_obj = datetime.strptime(start_date, "%Y-%m-%d").date()
    else:
        # Default to early date for overall data
        start_date_obj = datetime(2020, 1, 1).date()

    if end_date:
        end_date_obj = datetime.strptime(end_date, "%Y-%m-%d").date()
    else:
        # Default to today
        end_date_obj = datetime.now().date()

    # Adjust end_date to include the full day
    end_date_inclusive = end_date_obj + timedelta(days=1)

    # Step 1: Calculate Total Expenses from expenses table
    total_expenses = 0
    try:
        expenses_query = select(func.coalesce(func.sum(Expenses.amount), 0)).where(
            and_(
                Expenses.expense_date >= start_date_obj,
                Expenses.expense_date < end_date_inclusive
            )
        )
        expenses_result = await db.execute(expenses_query)
        total_expenses = expenses_result.scalar() or 0
        logging.info(f"[UnitEconomics] Total expenses from {start_date_obj} to {end_date_obj}: {total_expenses}")
    except Exception as e:
        logging.error(f"[UnitEconomics] Error fetching total expenses: {str(e)}")
        import traceback
        traceback.print_exc()

    # Step 2: Calculate Total New Users from clients table
    total_new_users = 0
    try:
        users_query = select(func.count()).where(
            and_(
                Client.created_at >= start_date_obj,
                Client.created_at < end_date_inclusive
            )
        )
        users_result = await db.execute(users_query)
        total_new_users = users_result.scalar() or 0
        logging.info(f"[UnitEconomics] Total new users from {start_date_obj} to {end_date_obj}: {total_new_users}")
    except Exception as e:
        logging.error(f"[UnitEconomics] Error fetching total new users: {str(e)}")
        import traceback
        traceback.print_exc()

    # Step 3: Calculate CAC (handle division by zero)
    cac = 0
    if total_new_users > 0:
        cac = total_expenses / total_new_users
    else:
        cac = 0

    logging.info(f"[UnitEconomics] CAC calculated: {cac} (expenses: {total_expenses}, users: {total_new_users})")

    # ========== LTV CALCULATION ==========
    today = datetime.now().date()

    # For retention calculation, we use the TWO PREVIOUS COMPLETED MONTHS
    # If today is in February 2026, we compare December 2025 vs January 2026
    # If today is in January 2026, we compare November 2025 vs December 2025

    # Get the first day of the current month
    first_day_of_current_month = today.replace(day=1)

    # The most recent completed month (month N-1)
    most_recent_completed_month_start = (first_day_of_current_month - timedelta(days=1)).replace(day=1)
    most_recent_completed_month_end = first_day_of_current_month - timedelta(days=1)

    # The month before that (month N-2)
    second_previous_month_start = (most_recent_completed_month_start - timedelta(days=1)).replace(day=1)
    second_previous_month_end = most_recent_completed_month_start - timedelta(days=1)

    logging.info(f"[UnitEconomics] Month N-2 (earlier): {second_previous_month_start} to {second_previous_month_end}")
    logging.info(f"[UnitEconomics] Month N-1 (recent completed): {most_recent_completed_month_start} to {most_recent_completed_month_end}")

    # Step 1: Get Month N-2 Active Users (users with 1+ login in the month)
    prev_month_start = second_previous_month_start
    prev_month_end = second_previous_month_end

    # Active users: users with at least 1 login in the month
    prev_result = await db.execute(
        select(func.count(distinct(ActiveUser.client_id))).where(
            and_(
                func.date(ActiveUser.created_at) >= prev_month_start,
                func.date(ActiveUser.created_at) <= prev_month_end
            )
        )
    )
    previous_month_count = prev_result.scalar() or 0

    logging.info(f"[UnitEconomics] Month N-2 active users: {previous_month_count}")

    # Step 2: Get Month N-1 Active Users (users with 1+ login in the month)
    curr_month_start = most_recent_completed_month_start
    curr_month_end = most_recent_completed_month_end

    # Active users: users with at least 1 login in the month
    curr_result = await db.execute(
        select(func.count(distinct(ActiveUser.client_id))).where(
            and_(
                func.date(ActiveUser.created_at) >= curr_month_start,
                func.date(ActiveUser.created_at) <= curr_month_end
            )
        )
    )
    current_month_count = curr_result.scalar() or 0

    logging.info(f"[UnitEconomics] Month N-1 active users: {current_month_count}")

    # Step 3: Find Retained Users (present in both months N-2 and N-1)
    # Note: With new logic (1+ login), retention calculation needs reconsideration
    # For now, we'll use intersection of unique users from both months
    # However, this requires tracking individual users, not just counts
    # To properly calculate retention, we need to get the actual user lists

    # Get user lists for retention calculation
    prev_users_result = await db.execute(
        select(ActiveUser.client_id).where(
            and_(
                func.date(ActiveUser.created_at) >= prev_month_start,
                func.date(ActiveUser.created_at) <= prev_month_end
            )
        ).distinct()
    )
    previous_month_client_ids = set([row[0] for row in prev_users_result.fetchall()])

    curr_users_result = await db.execute(
        select(ActiveUser.client_id).where(
            and_(
                func.date(ActiveUser.created_at) >= curr_month_start,
                func.date(ActiveUser.created_at) <= curr_month_end
            )
        ).distinct()
    )
    current_month_client_ids = set([row[0] for row in curr_users_result.fetchall()])

    retained_client_ids = previous_month_client_ids.intersection(current_month_client_ids)
    retained_count = len(retained_client_ids)

    logging.info(f"[UnitEconomics] Retained users: {retained_count}")

    # Step 4: Calculate Churn Rate
    churn_rate = 0
    if previous_month_count > 0:
        churn_rate = retained_count / previous_month_count

    logging.info(f"[UnitEconomics] Churn rate: {churn_rate}")

    # Step 5: Calculate LTV
    ltv = 0
    if churn_rate > 0:
        ltv = 1 / churn_rate
    else:
        ltv = 0

    logging.info(f"[UnitEconomics] LTV calculated: {ltv}")

    # ========== D7 RETENTION CALCULATION ==========
    # Same logic as D30 but with weeks instead of months
    # Week N-2 = 2nd previous completed week
    # Week N-1 = Most recent completed week

    # Get current day of week (0 = Monday, 6 = Sunday)
    current_weekday = today.weekday()

    # Calculate start of current week (Monday)
    current_week_start = today - timedelta(days=current_weekday)

    # Week N-1 (previous completed week)
    week_n1_start = current_week_start - timedelta(weeks=1)
    week_n1_end = current_week_start - timedelta(days=1)

    # Week N-2 (week before that)
    week_n2_start = current_week_start - timedelta(weeks=2)
    week_n2_end = week_n1_start - timedelta(days=1)

    logging.info(f"[UnitEconomics] Week N-2: {week_n2_start} to {week_n2_end}")
    logging.info(f"[UnitEconomics] Week N-1: {week_n1_start} to {week_n1_end}")

    # Step 1: Get Week N-2 Active Users
    week_n2_users_result = await db.execute(
        select(ActiveUser.client_id).where(
            and_(
                func.date(ActiveUser.created_at) >= week_n2_start,
                func.date(ActiveUser.created_at) <= week_n2_end
            )
        ).distinct()
    )
    week_n2_client_ids = set([row[0] for row in week_n2_users_result.fetchall()])
    week_n2_count = len(week_n2_client_ids)

    logging.info(f"[UnitEconomics] Week N-2 active users: {week_n2_count}")

    # Step 2: Get Week N-1 Active Users
    week_n1_users_result = await db.execute(
        select(ActiveUser.client_id).where(
            and_(
                func.date(ActiveUser.created_at) >= week_n1_start,
                func.date(ActiveUser.created_at) <= week_n1_end
            )
        ).distinct()
    )
    week_n1_client_ids = set([row[0] for row in week_n1_users_result.fetchall()])
    week_n1_count = len(week_n1_client_ids)

    logging.info(f"[UnitEconomics] Week N-1 active users: {week_n1_count}")

    # Step 3: Find D7 Retained Users (present in both Week N-2 and Week N-1)
    d7_retained_client_ids = week_n2_client_ids.intersection(week_n1_client_ids)
    d7_retained_count = len(d7_retained_client_ids)

    logging.info(f"[UnitEconomics] D7 Retained users: {d7_retained_count}")

    # Combine all data
    analytics_data = {
        # CAC Data
        "cac": round(cac, 2),
        "totalExpenses": round(total_expenses, 2),
        "totalNewUsers": total_new_users,
        # LTV Data (D30 Retention)
        "ltv": round(ltv, 2),
        "cohortRetentionRate": round(churn_rate, 4),
        "retainedUsers": retained_count,
        # D7 Retention Data
        "d7_retained_users": d7_retained_count,
        # Filters
        "filters": {
            "startDate": start_date_obj.isoformat(),
            "endDate": end_date_obj.isoformat()
        }
    }

    return {
        "success": True,
        "data": analytics_data,
        "message": "Unit economics analytics fetched successfully"
    }


