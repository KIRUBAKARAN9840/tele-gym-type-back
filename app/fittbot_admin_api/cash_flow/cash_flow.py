from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from datetime import datetime, date, timedelta
from sqlalchemy import func, and_, select
from decimal import Decimal
from pydantic import BaseModel
from typing import Optional
import calendar

from app.models.async_database import get_async_db
from app.models.adminmodels import Expenses, OpeningBalance

# Import centralized revenue service
from app.fittbot_admin_api.revenue_service import (
    get_revenue_breakdown,
    paise_to_rupees,
    calculate_nutritionist_plan_net_revenue
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
        # Determine the month to calculate
        if month:
            # Parse the month parameter (YYYY-MM)
            parts = month.split("-")
            if len(parts) != 2:
                raise HTTPException(status_code=400, detail="Invalid month format. Expected: YYYY-MM")

            prev_year = int(parts[0])
            prev_month = int(parts[1])

            if prev_month < 1 or prev_month > 12:
                raise HTTPException(status_code=400, detail="Invalid month")
        else:
            # Calculate previous calendar month dates
            today = datetime.now().date()
            if today.month == 1:
                # January -> December of previous year
                prev_month = 12
                prev_year = today.year - 1
            else:
                prev_month = today.month - 1
                prev_year = today.year

        # First day of the month
        start_date = date(prev_year, prev_month, 1)

        # Last day of the month using calendar
        last_day_of_month = calendar.monthrange(prev_year, prev_month)[1]
        end_date = date(prev_year, prev_month, last_day_of_month)

        print(f"[CASH_FLOW] Calculating outflow for {start_date} to {end_date}")

        # Use centralized revenue service instead of get_revenue_for_month
        revenue_data = await get_revenue_breakdown(
            db=db,
            start_date=start_date,
            end_date=end_date,
            exclude_gym_id_one=False  # Include all gyms for cash flow
        )

        # Extract revenues (all in PAISA)
        daily_pass_revenue = revenue_data.daily_pass
        sessions_revenue = revenue_data.sessions
        gym_membership_revenue = revenue_data.gym_membership
        fittbot_subscription_revenue = revenue_data.fittbot_subscription

        # Calculate payouts and deductions for each category (using financials logic)
        # Membership: 15% commission, 2% PG, 2% TDS
        membership_payout, membership_comm, membership_pg, membership_tds = calculate_membership_payout(
            gym_membership_revenue
        )

        # Daily Pass: 30% commission, 2% PG, 2% TDS
        daily_pass_payout, daily_pass_comm, daily_pass_pg, daily_pass_tds = calculate_daily_pass_session_payout(
            daily_pass_revenue
        )

        # Sessions: 30% commission, 2% PG, 2% TDS
        sessions_payout, sessions_comm, sessions_pg, sessions_tds = calculate_daily_pass_session_payout(
            sessions_revenue
        )

        # Calculate totals
        # 1. Total Gym Payout (actual outflow to gyms)
        total_gym_payout = membership_payout + daily_pass_payout + sessions_payout

        # 2. Total PG Charges (payment gateway fees - kept by platform, not paid out)
        total_pg_charges = membership_pg + daily_pass_pg + sessions_pg

        # 3. Total GST Payable (in paise for consistency)
        # GST on subscription (using centralized Nutritionist Plan GST calculation)
        # Note: fittbot_subscription_revenue is already in paise from DB
        nutritionist_calc = calculate_nutritionist_plan_net_revenue(int(fittbot_subscription_revenue))
        gst_on_subscription_paise = nutritionist_calc["gst"]
        # GST on commissions (18% of commission)
        # Note: commissions are already in paise (int returned from calculate functions)
        gst_on_commission_paise = (
            int(Decimal(str(membership_comm)) * Decimal("0.18")) +
            int(Decimal(str(daily_pass_comm)) * Decimal("0.18")) +
            int(Decimal(str(sessions_comm)) * Decimal("0.18"))
        )
        total_gst_payable_paise = gst_on_subscription_paise + gst_on_commission_paise

        # 4. Total TDS Payable (tax deducted that needs to be paid to government)
        total_tds_payable_paise = membership_tds + daily_pass_tds + sessions_tds

        # 5. Total Expenses (from fittbot_admins.expenses table)
        # Note: Expenses are already in rupees (Float), need to convert to paise
        total_expenses_rupees = 0.0
        try:
            expenses_stmt = (
                select(func.coalesce(func.sum(Expenses.amount), 0))
                .where(Expenses.expense_date >= start_date)
                .where(Expenses.expense_date <= end_date)
            )
            expenses_result = await db.execute(expenses_stmt)
            total_expenses_rupees = expenses_result.scalar() or 0.0
        except Exception as e:
            print(f"[CASH_FLOW] Error fetching Expenses: {e}")

        # Convert expenses to paise for consistency
        total_expenses_paise = int(total_expenses_rupees * 100)

        # 6. Get Opening Balances
        opening_balances_data = []
        try:
            ob_stmt = select(OpeningBalance).order_by(OpeningBalance.financial_year.desc())
            ob_result = await db.execute(ob_stmt)
            opening_balances = ob_result.scalars().all()

            opening_balances_data = [
                {
                    "id": ob.id,
                    "financial_year": ob.financial_year,
                    "amount": ob.amount
                }
                for ob in opening_balances
            ]
        except Exception as e:
            print(f"[CASH_FLOW] Error fetching Opening Balances: {e}")

        # Calculate total outflow (actual cash leaving the business) - all in paise
        # Outflow = Gym Payout + GST Payable + TDS Payable + Expenses
        # Note: PG charges are deducted before payout, not a separate outflow
        # Commission is retained revenue, not an outflow
        total_outflow_paise = total_gym_payout + total_gst_payable_paise + total_tds_payable_paise + total_expenses_paise

        # Calculate total inflow (total gross revenue) - all in paise
        total_inflow_paise = daily_pass_revenue + sessions_revenue + gym_membership_revenue + fittbot_subscription_revenue

        # Calculate net cash flow
        net_cash_flow_paise = total_inflow_paise - total_outflow_paise

        return {
            "success": True,
            "data": {
                "month": {
                    "year": prev_year,
                    "month": prev_month,
                    "month_name": end_date.strftime("%B %Y"),
                    "start_date": start_date.isoformat(),
                    "end_date": end_date.isoformat()
                },
                "inflow": {
                    "total_revenue": round(total_inflow_paise / 100, 2)
                },
                "outflow": {
                    "gym_payout": round(total_gym_payout / 100, 2),
                    "gst_payable": round(total_gst_payable_paise / 100, 2),
                    "tds_payable": round(total_tds_payable_paise / 100, 2),
                    "expenses": round(total_expenses_paise / 100, 2),
                    "total_outflow": round(total_outflow_paise / 100, 2)
                },
                "net_cash_flow": round(net_cash_flow_paise / 100, 2),
                "opening_balances": opening_balances_data,
                "breakdown": {
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
                }
            }
        }

    except Exception as e:
        print(f"[CASH_FLOW] Error: {str(e)}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


# ==================== Opening Balance Endpoints ====================

@router.post("/opening-balance")
async def create_or_update_opening_balance(
    payload: OpeningBalanceCreate,
    db: AsyncSession = Depends(get_async_db)
):
    """
    Create or update opening balance for a financial year.
    """
    try:
        # Validate financial year format (YYYY-YYYY)
        if not validate_financial_year(payload.financial_year):
            raise HTTPException(
                status_code=400,
                detail="Invalid financial year format. Expected format: '2020-2021'"
            )

        # Check if record exists
        existing_stmt = select(OpeningBalance).where(OpeningBalance.financial_year == payload.financial_year)
        existing_result = await db.execute(existing_stmt)
        existing_record = existing_result.scalar_one_or_none()

        if existing_record:
            # Update existing record
            existing_record.amount = payload.amount
            existing_record.updated_at = datetime.now()
            await db.commit()

            return {
                "success": True,
                "message": f"Opening balance for {payload.financial_year} updated successfully",
                "data": {
                    "id": existing_record.id,
                    "financial_year": existing_record.financial_year,
                    "amount": existing_record.amount
                }
            }
        else:
            # Create new record
            new_record = OpeningBalance(
                financial_year=payload.financial_year,
                amount=payload.amount
            )
            db.add(new_record)
            await db.commit()

            return {
                "success": True,
                "message": f"Opening balance for {payload.financial_year} created successfully",
                "data": {
                    "id": new_record.id,
                    "financial_year": new_record.financial_year,
                    "amount": new_record.amount
                }
            }
    except HTTPException:
        raise
    except Exception as e:
        await db.rollback()
        print(f"[CASH_FLOW] Error saving opening balance: {str(e)}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/opening-balance/{financial_year}")
async def delete_opening_balance(
    financial_year: str,
    db: AsyncSession = Depends(get_async_db)
):
    """
    Delete opening balance for a financial year.
    """
    try:
        stmt = select(OpeningBalance).where(OpeningBalance.financial_year == financial_year)
        result = await db.execute(stmt)
        opening_balance = result.scalar_one_or_none()

        if not opening_balance:
            raise HTTPException(
                status_code=404,
                detail=f"Opening balance for {financial_year} not found"
            )

        await db.delete(opening_balance)
        await db.commit()

        return {
            "success": True,
            "message": f"Opening balance for {financial_year} deleted successfully"
        }
    except HTTPException:
        raise
    except Exception as e:
        await db.rollback()
        print(f"[CASH_FLOW] Error deleting opening balance: {str(e)}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


def validate_financial_year(financial_year: str) -> bool:
    """
    Validate financial year format (YYYY-YYYY).
    Example: '2020-2021', '2021-2022'
    """
    if not financial_year or len(financial_year) != 9:
        return False

    parts = financial_year.split("-")
    if len(parts) != 2:
        return False

    try:
        year1 = int(parts[0])
        year2 = int(parts[1])

        # Second year should be year1 + 1
        return year2 == year1 + 1
    except ValueError:
        return False


@router.get("/monthly-data")
async def get_monthly_cash_flow_data(
    page: int = Query(1, ge=1),
    page_size: int = Query(12, ge=1, le=100),
    db: AsyncSession = Depends(get_async_db)
):
    """
    Get paginated monthly cash flow data table.
    Returns data for multiple months including:
    - Opening Balance
    - Outflow (Gym Payout + GST + TDS + Expenses)
    - Net Cash Flow (Inflow - Outflow)
    - Closing Balance
    - Burn Rate
    - Runway
    """
    try:
        today = date.today()
        offset = (page - 1) * page_size

        # Fetch opening balances
        ob_stmt = select(OpeningBalance).order_by(OpeningBalance.financial_year.desc())
        ob_result = await db.execute(ob_stmt)
        opening_balances = ob_result.scalars().all()

        # Create opening balance lookup by financial year
        opening_balance_dict = {ob.financial_year: ob.amount for ob in opening_balances}

        # Get current financial year
        current_month = today.month
        current_year = today.year
        if current_month >= 4:
            current_fy = f"{current_year}-{current_year + 1}"
        else:
            current_fy = f"{current_year - 1}-{current_year}"

        # Get opening balance for current financial year
        current_fy_opening_balance = opening_balance_dict.get(current_fy, 0)

        # Calculate cumulative cash flow from April 1 to current date
        fy_start_month = 4  # April
        fy_start_year = current_year if current_month >= 4 else current_year - 1

        monthly_data = []

        # Generate only the months needed for the current page
        # Start from previous month to show complete financial years
        for i in range(page_size):
            month_index = offset + i

            # Calculate the month and year starting from previous month
            month = today.month - 1 - month_index
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

            # Use centralized revenue service instead of get_revenue_for_month
            revenue_data = await get_revenue_breakdown(
                db=db,
                start_date=month_start,
                end_date=month_end,
                exclude_gym_id_one=False  # Include all gyms for cash flow
            )

            # Extract revenues (all in PAISA)
            daily_pass_revenue = revenue_data.daily_pass
            sessions_revenue = revenue_data.sessions
            gym_membership_revenue = revenue_data.gym_membership
            fittbot_subscription_revenue = revenue_data.fittbot_subscription

            # Calculate payouts and deductions
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
                total_expenses_rupees = expenses_result.scalar() or 0.0
            except Exception as e:
                print(f"[CASH_FLOW] Error fetching Expenses: {e}")

            total_expenses_paise = int(total_expenses_rupees * 100)

            # Calculate inflow, outflow, and net cash flow (in rupees)
            total_inflow_paise = daily_pass_revenue + sessions_revenue + gym_membership_revenue + fittbot_subscription_revenue
            total_outflow_paise = total_gym_payout + total_gst_payable_paise + total_tds_payable_paise + total_expenses_paise
            net_cash_flow_paise = total_inflow_paise - total_outflow_paise

            # Calculate opening balance for this month
            # If month is April (4) or later, it belongs to current FY
            # If month is before April, it belongs to previous FY
            if month >= 4:
                month_fy = f"{year}-{year + 1}"
            else:
                month_fy = f"{year - 1}-{year}"

            month_opening_balance = opening_balance_dict.get(month_fy, 0)

            # Calculate closing balance (Opening Balance + Net Cash Flow)
            # Convert opening balance from rupees to paise for calculation
            closing_balance_paise = int(month_opening_balance * 100) + net_cash_flow_paise

            # Calculate burn rate (absolute value of negative cash flow)
            burn_rate_paise = abs(net_cash_flow_paise) if net_cash_flow_paise < 0 else 0

            # Calculate runway
            # Only if closing_balance > 0 AND net_cash_flow < 0
            closing_balance_rupees = closing_balance_paise / 100
            burn_rate_rupees = burn_rate_paise / 100

            if closing_balance_paise > 0 and net_cash_flow_paise < 0:
                runway = closing_balance_rupees / burn_rate_rupees
            else:
                runway = 0

            month_name = month_start.strftime("%B %Y")

            monthly_data.append({
                "month": month_str,
                "month_display": month_name,
                "financial_year": month_fy,
                "opening_balance": round(month_opening_balance, 2),
                "inflow": round(total_inflow_paise / 100, 2),
                "outflow": round(total_outflow_paise / 100, 2),
                "gym_payout": round(total_gym_payout / 100, 2),
                "gst_payable": round(total_gst_payable_paise / 100, 2),
                "tds_payable": round(total_tds_payable_paise / 100, 2),
                "expenses": round(total_expenses_paise / 100, 2),
                "net_cash_flow": round(net_cash_flow_paise / 100, 2),
                "closing_balance": round(closing_balance_rupees, 2),
                "burn_rate": round(burn_rate_rupees, 2),
                "runway": round(runway, 1)
            })

        # Calculate total months
        start_year = 2020
        start_month = 1
        total_months = (today.year - start_year) * 12 + today.month - start_month + 1
        total_pages = (total_months + page_size - 1) // page_size

        # Prepare opening balances data
        opening_balances_data = [
            {
                "id": ob.id,
                "financial_year": ob.financial_year,
                "amount": ob.amount
            }
            for ob in opening_balances
        ]

        # Reverse to show April -> March order within each financial year
        monthly_data.reverse()

        return {
            "success": True,
            "data": monthly_data,
            "opening_balances": opening_balances_data,
            "pagination": {
                "page": page,
                "page_size": page_size,
                "total_records": total_months,
                "total_pages": total_pages,
                "has_next_page": page < total_pages,
                "has_prev_page": page > 1
            }
        }

    except Exception as e:
        print(f"[CASH_FLOW] Error: {str(e)}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/export")
async def export_cash_flow_data(
    start_date: str = Query(..., description="Start date in YYYY-MM-DD format"),
    end_date: str = Query(..., description="End date in YYYY-MM-DD format"),
    db: AsyncSession = Depends(get_async_db)
):
    """
    Export cash flow data for a date range.
    Returns monthly data including opening balance, outflow breakdown, net cash flow, closing balance, burn rate, and runway.
    """
    try:
        # Parse dates
        start = datetime.strptime(start_date, "%Y-%m-%d").date()
        end = datetime.strptime(end_date, "%Y-%m-%d").date()

        if start > end:
            raise HTTPException(status_code=400, detail="Start date cannot be after end date")

        # Fetch opening balances
        ob_stmt = select(OpeningBalance).order_by(OpeningBalance.financial_year.desc())
        ob_result = await db.execute(ob_stmt)
        opening_balances = ob_result.scalars().all()

        # Create opening balance lookup by financial year
        opening_balance_dict = {ob.financial_year: ob.amount for ob in opening_balances}

        monthly_data = []

        # Generate all months in the date range
        current = start
        while current <= end:
            # Get month start and end
            month_start = date(current.year, current.month, 1)
            last_day_of_month = calendar.monthrange(current.year, current.month)[1]
            month_end = date(current.year, current.month, min(last_day_of_month, end.day))

            # Use centralized revenue service
            revenue_data = await get_revenue_breakdown(
                db=db,
                start_date=month_start,
                end_date=month_end,
                exclude_gym_id_one=False
            )

            # Extract revenues (all in PAISA)
            daily_pass_revenue = revenue_data.daily_pass
            sessions_revenue = revenue_data.sessions
            gym_membership_revenue = revenue_data.gym_membership
            fittbot_subscription_revenue = revenue_data.fittbot_subscription

            # Calculate payouts and deductions
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
                total_expenses_rupees = expenses_result.scalar() or 0.0
            except Exception as e:
                print(f"[CASH_FLOW] Error fetching Expenses: {e}")

            total_expenses_paise = int(total_expenses_rupees * 100)

            # Calculate inflow, outflow, and net cash flow (in rupees)
            total_inflow_paise = daily_pass_revenue + sessions_revenue + gym_membership_revenue + fittbot_subscription_revenue
            total_outflow_paise = total_gym_payout + total_gst_payable_paise + total_tds_payable_paise + total_expenses_paise
            net_cash_flow_paise = total_inflow_paise - total_outflow_paise

            # Calculate opening balance for this month
            month = current.month
            year = current.year
            if month >= 4:
                month_fy = f"{year}-{year + 1}"
            else:
                month_fy = f"{year - 1}-{year}"

            month_opening_balance = opening_balance_dict.get(month_fy, 0)

            # Calculate closing balance (Opening Balance + Net Cash Flow)
            closing_balance_paise = int(month_opening_balance * 100) + net_cash_flow_paise

            # Calculate burn rate (absolute value of negative cash flow)
            burn_rate_paise = abs(net_cash_flow_paise) if net_cash_flow_paise < 0 else 0

            # Calculate runway
            closing_balance_rupees = closing_balance_paise / 100
            burn_rate_rupees = burn_rate_paise / 100

            if closing_balance_paise > 0 and net_cash_flow_paise < 0:
                runway = closing_balance_rupees / burn_rate_rupees
            else:
                runway = 0

            month_name = month_start.strftime("%B %Y")

            monthly_data.append({
                "month": month_start.strftime("%Y-%m"),
                "month_display": month_name,
                "financial_year": month_fy,
                "opening_balance": round(month_opening_balance, 2),
                "outflow": round(total_outflow_paise / 100, 2),
                "gym_payout": round(total_gym_payout / 100, 2),
                "gst_payable": round(total_gst_payable_paise / 100, 2),
                "tds_payable": round(total_tds_payable_paise / 100, 2),
                "expenses": round(total_expenses_paise / 100, 2),
                "net_cash_flow": round(net_cash_flow_paise / 100, 2),
                "closing_balance": round(closing_balance_rupees, 2),
                "burn_rate": round(burn_rate_rupees, 2),
                "runway": round(runway, 1)
            })

            # Move to next month
            # Add one month to current date
            if current.month == 12:
                current = date(current.year + 1, 1, 1)
            else:
                current = date(current.year, current.month + 1, 1)

        # Reverse to match pagination display order (April -> March within each FY)
        monthly_data.reverse()

        return {
            "success": True,
            "data": monthly_data
        }

    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"Invalid date format: {str(e)}")
    except Exception as e:
        print(f"[CASH_FLOW] Error: {str(e)}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))
