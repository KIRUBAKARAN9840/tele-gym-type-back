from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from datetime import datetime, date, timedelta
from sqlalchemy import func, and_, select
from decimal import Decimal
from pydantic import BaseModel
from typing import Optional
import calendar

from app.models.async_database import get_async_db
from app.fittbot_api.v1.payments.models.orders import Order
from app.models.adminmodels import Expenses, OpeningBalance

# Import centralized revenue service
from app.fittbot_admin_api.revenue_service import (
    get_revenue_breakdown,
    paise_to_rupees
)


# Pydantic model for Opening Balance
class OpeningBalanceCreate(BaseModel):
    financial_year: str  # Format: '2020-2021'
    amount: float


router = APIRouter(prefix="/api/admin/cash-flow", tags=["AdminCashFlow"])


def calculate_membership_payout(membership_revenue):
    """
    Calculate gym payout for membership revenue.
    Formula:
    1. 15% platform commission
    2. 2% PG deduction on total
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


@router.get("/overview")
async def get_last_month_outflow(
    month: Optional[str] = Query(None, description="Month in YYYY-MM format (e.g., 2025-02). If not provided, returns previous month."),
    db: AsyncSession = Depends(get_async_db)
):
    """
    Calculate total outflow for a specific month (or previous calendar month if not specified).

    Outflow components:
    1. Gym Payout (membership, daily pass, sessions)
    2. GST Payable (on commissions and subscriptions)
    3. TDS Payable (tax deducted at source)
    4. Expenses (operational and marketing expenses from fittbot_admins.expenses)

    Note: PG charges and commission are NOT part of outflow as they are
          retained revenue for the platform.
    """
    try:
        today = date.today()

        # Determine target month
        if month:
            year, month_num = map(int, month.split("-"))
            target_month_start = date(year, month_num, 1)
            last_day = calendar.monthrange(year, month_num)[1]
            target_month_end = date(year, month_num, last_day)
        else:
            # Use previous calendar month
            if today.month == 1:
                target_month_start = date(today.year - 1, 12, 1)
                target_month_end = date(today.year - 1, 12, 31)
            else:
                target_month_start = date(today.year, today.month - 1, 1)
                last_day = calendar.monthrange(today.year, today.month - 1)[1]
                target_month_end = date(today.year, today.month - 1, last_day)

        # Use centralized revenue service
        revenue_data = await get_revenue_breakdown(
            db=db,
            start_date=target_month_start,
            end_date=target_month_end,
            exclude_gym_id_one=False  # Include all gyms for cash flow
        )

        # Extract revenues (all in PAISA)
        daily_pass_revenue = revenue_data.daily_pass
        sessions_revenue = revenue_data.sessions
        gym_membership_revenue = revenue_data.gym_membership
        fittbot_subscription_revenue = revenue_data.fittbot_subscription

        # Calculate payouts and deductions for each category
        membership_payout, membership_comm, membership_pg, membership_tds = calculate_membership_payout(
            gym_membership_revenue
        )
        daily_pass_payout, daily_pass_comm, daily_pass_pg, daily_pass_tds = calculate_daily_pass_session_payout(
            daily_pass_revenue
        )
        sessions_payout, sessions_comm, sessions_pg, sessions_tds = calculate_daily_pass_session_payout(
            sessions_revenue
        )

        # Calculate totals
        total_gym_payout = membership_payout + daily_pass_payout + sessions_payout
        total_pg_charges = membership_pg + daily_pass_pg + sessions_pg

        # GST Payable
        gst_on_subscription_paise = int(Decimal(str(fittbot_subscription_revenue)) * Decimal("0.18"))
        gst_on_commission_paise = (
            int(Decimal(str(membership_comm)) * Decimal("0.18")) +
            int(Decimal(str(daily_pass_comm)) * Decimal("0.18")) +
            int(Decimal(str(sessions_comm)) * Decimal("0.18"))
        )
        total_gst_payable_paise = gst_on_subscription_paise + gst_on_commission_paise

        # TDS Payable
        total_tds_payable_paise = membership_tds + daily_pass_tds + sessions_tds

        # Expenses
        total_expenses_rupees = 0.0
        try:
            expenses_stmt = (
                select(func.coalesce(func.sum(Expenses.amount), 0))
                .where(Expenses.expense_date >= target_month_start)
                .where(Expenses.expense_date <= target_month_end)
            )
            expenses_result = await db.execute(expenses_stmt)
            total_expenses_rupees = expenses_result.scalar() or 0
        except Exception as e:
            print(f"[CASH_FLOW] Error fetching Expenses: {e}")

        total_expenses_paise = int(total_expenses_rupees * 100)

        # Calculate outflow (in paise)
        total_outflow_paise = total_gym_payout + total_gst_payable_paise + total_tds_payable_paise + total_expenses_paise

        # Calculate inflow (total gross revenue) - all in paise
        total_inflow_paise = daily_pass_revenue + sessions_revenue + gym_membership_revenue + fittbot_subscription_revenue

        # Calculate net cash flow
        net_cash_flow_paise = total_inflow_paise - total_outflow_paise

        return {
            "success": True,
            "data": {
                "totalInflow": round(total_inflow_paise / 100, 2),
                "totalOutflow": round(total_outflow_paise / 100, 2),
                "netCashFlow": round(net_cash_flow_paise / 100, 2),
                "inflowBreakdown": {
                    "daily_pass": round(daily_pass_revenue / 100, 2),
                    "sessions": round(sessions_revenue / 100, 2),
                    "gym_membership": round(gym_membership_revenue / 100, 2),
                    "fittbot_subscription": round(fittbot_subscription_revenue / 100, 2)
                },
                "outflowBreakdown": {
                    "gym_payout": round(total_gym_payout / 100, 2),
                    "gst_payable": round(total_gst_payable_paise / 100, 2),
                    "tds_payable": round(total_tds_payable_paise / 100, 2),
                    "expenses": round(total_expenses_paise / 100, 2)
                },
                "gymPayoutBreakdown": {
                    "membership": {
                        "revenue": round(gym_membership_revenue / 100, 2),
                        "payout": round(membership_payout / 100, 2),
                        "pg_charges": round(membership_pg / 100, 2),
                        "tds": round(membership_tds / 100, 2),
                        "gst_on_commission": round(int(Decimal(str(membership_comm)) * Decimal("0.18")) / 100, 2)
                    },
                    "daily_pass": {
                        "revenue": round(daily_pass_revenue / 100, 2),
                        "payout": round(daily_pass_payout / 100, 2),
                        "pg_charges": round(daily_pass_pg / 100, 2),
                        "tds": round(daily_pass_tds / 100, 2),
                        "gst_on_commission": round(int(Decimal(str(daily_pass_comm)) * Decimal("0.18")) / 100, 2)
                    },
                    "sessions": {
                        "revenue": round(sessions_revenue / 100, 2),
                        "payout": round(sessions_payout / 100, 2),
                        "pg_charges": round(sessions_pg / 100, 2),
                        "tds": round(sessions_tds / 100, 2),
                        "gst_on_commission": round(int(Decimal(str(sessions_comm)) * Decimal("0.18")) / 100, 2)
                    },
                    "fittbot_subscription": {
                        "revenue": round(fittbot_subscription_revenue / 100, 2),
                        "gst_on_revenue": round(gst_on_subscription_paise / 100, 2)
                    }
                },
                "month": target_month_start.strftime("%B %Y")
            }
        }

    except Exception as e:
        print(f"[CASH_FLOW] Error: {str(e)}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/monthly-trends")
async def get_monthly_cash_flow_trends(
    start_month: str = Query(None, description="Start month in YYYY-MM format"),
    end_month: str = Query(None, description="End month in YYYY-MM format"),
    db: AsyncSession = Depends(get_async_db)
):
    """
    Get monthly cash flow trends for a range of months.

    If no range provided, returns last 6 months of data.
    Includes opening balance tracking for the financial year.
    """
    try:
        today = date.today()

        # Determine month range
        if start_month and end_month:
            start_year, start_month_num = map(int, start_month.split("-"))
            end_year, end_month_num = map(int, end_month.split("-"))
            current_start = date(start_year, start_month_num, 1)
            current_end = date(end_year, end_month_num, calendar.monthrange(end_year, end_month_num)[1])
        else:
            # Default: last 6 months
            current_end = date(today.year, today.month, calendar.monthrange(today.year, today.month)[1])
            if today.month - 6 <= 0:
                current_start = date(today.year - 1, today.month - 6 + 12, 1)
            else:
                current_start = date(today.year, today.month - 6, 1)

        # Generate all months in range
        monthly_data = []
        current_date = current_start

        while current_date <= current_end:
            year = current_date.year
            month = current_date.month
            last_day = calendar.monthrange(year, month)[1]
            month_start = date(year, month, 1)
            month_end = date(year, month, last_day)

            # Get revenue data using centralized service
            revenue_data = await get_revenue_breakdown(
                db=db,
                start_date=month_start,
                end_date=month_end,
                exclude_gym_id_one=False
            )

            daily_pass_revenue = revenue_data.daily_pass
            sessions_revenue = revenue_data.sessions
            gym_membership_revenue = revenue_data.gym_membership
            fittbot_subscription_revenue = revenue_data.fittbot_subscription

            # Calculate payouts
            membership_payout, membership_comm, membership_pg, membership_tds = calculate_membership_payout(
                gym_membership_revenue
            )
            daily_pass_payout, daily_pass_comm, daily_pass_pg, daily_pass_tds = calculate_daily_pass_session_payout(
                daily_pass_revenue
            )
            sessions_payout, sessions_comm, sessions_pg, sessions_tds = calculate_daily_pass_session_payout(
                sessions_revenue
            )

            # Calculate totals
            total_gym_payout = membership_payout + daily_pass_payout + sessions_payout
            total_pg_charges = membership_pg + daily_pass_pg + sessions_pg

            # GST Payable
            gst_on_subscription_paise = int(Decimal(str(fittbot_subscription_revenue)) * Decimal("0.18"))
            gst_on_commission_paise = (
                int(Decimal(str(membership_comm)) * Decimal("0.18")) +
                int(Decimal(str(daily_pass_comm)) * Decimal("0.18")) +
                int(Decimal(str(sessions_comm)) * Decimal("0.18"))
            )
            total_gst_payable_paise = gst_on_subscription_paise + gst_on_commission_paise

            # TDS Payable
            total_tds_payable_paise = membership_tds + daily_pass_tds + sessions_tds

            # Expenses
            total_expenses_rupees = 0.0
            try:
                expenses_stmt = (
                    select(func.coalesce(func.sum(Expenses.amount), 0))
                    .where(Expenses.expense_date >= month_start)
                    .where(Expenses.expense_date <= month_end)
                )
                expenses_result = await db.execute(expenses_stmt)
                total_expenses_rupees = expenses_result.scalar() or 0
            except Exception as e:
                print(f"[CASH_FLOW] Error fetching Expenses: {e}")

            total_expenses_paise = int(total_expenses_rupees * 100)

            # Calculate inflow, outflow, and net cash flow (in rupees)
            total_inflow_paise = daily_pass_revenue + sessions_revenue + gym_membership_revenue + fittbot_subscription_revenue
            total_outflow_paise = total_gym_payout + total_gst_payable_paise + total_tds_payable_paise + total_expenses_paise
            net_cash_flow_paise = total_inflow_paise - total_outflow_paise

            monthly_data.append({
                "month": month_start.strftime("%B %Y"),
                "month_key": f"{year}-{month:02d}",
                "totalInflow": round(total_inflow_paise / 100, 2),
                "totalOutflow": round(total_outflow_paise / 100, 2),
                "netCashFlow": round(net_cash_flow_paise / 100, 2)
            })

            # Move to next month
            if month == 12:
                current_date = date(year + 1, 1, 1)
            else:
                current_date = date(year, month + 1, 1)

        return {
            "success": True,
            "data": monthly_data
        }

    except Exception as e:
        print(f"[CASH_FLOW] Error: {str(e)}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/opening-balance")
async def get_opening_balance(
    financial_year: Optional[str] = Query(None, description="Financial year in format YYYY-YYYY (e.g., 2024-2025)"),
    db: AsyncSession = Depends(get_async_db)
):
    """
    Get opening balance for a financial year.

    Financial year runs from April 1st to March 31st.
    Opening balance is the closing balance of March 31st of the previous year.
    """
    try:
        today = date.today()

        # Determine financial year
        if financial_year:
            start_year, end_year = map(int, financial_year.split("-"))
        else:
            # Current financial year
            if today.month >= 4:
                start_year = today.year
                end_year = today.year + 1
            else:
                start_year = today.year - 1
                end_year = today.year

        # Try to get stored opening balance
        balance_stmt = (
            select(OpeningBalance)
            .where(OpeningBalance.financial_year == f"{start_year}-{end_year}")
        )
        balance_result = await db.execute(balance_stmt)
        balance_record = balance_result.scalar_one_or_none()

        if balance_record:
            return {
                "success": True,
                "data": {
                    "financial_year": f"{start_year}-{end_year}",
                    "opening_balance": balance_record.amount,
                    "is_stored": True
                }
            }

        # If no stored balance, return zero
        return {
            "success": True,
            "data": {
                "financial_year": f"{start_year}-{end_year}",
                "opening_balance": 0.0,
                "is_stored": False
            }
        }

    except Exception as e:
        print(f"[CASH_FLOW] Error: {str(e)}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/opening-balance")
async def set_opening_balance(
    payload: OpeningBalanceCreate,
    db: AsyncSession = Depends(get_async_db)
):
    """
    Set or update opening balance for a financial year.
    """
    try:
        # Check if record exists
        existing_stmt = (
            select(OpeningBalance)
            .where(OpeningBalance.financial_year == payload.financial_year)
        )
        existing_result = await db.execute(existing_stmt)
        existing_record = existing_result.scalar_one_or_none()

        if existing_record:
            # Update existing
            existing_record.amount = payload.amount
            existing_record.updated_at = datetime.now()
        else:
            # Create new
            new_record = OpeningBalance(
                financial_year=payload.financial_year,
                amount=payload.amount
            )
            db.add(new_record)

        await db.commit()

        return {
            "success": True,
            "message": f"Opening balance for {payload.financial_year} updated successfully"
        }

    except Exception as e:
        await db.rollback()
        print(f"[CASH_FLOW] Error: {str(e)}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))
