# Tax & Compliance API
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from datetime import datetime, timedelta, date
from sqlalchemy import func, and_, select, distinct
from pydantic import BaseModel
from typing import Optional
from decimal import Decimal

from app.models.async_database import get_async_db
from app.models.adminmodels import TaxCompliance

# Import centralized revenue service
from app.fittbot_admin_api.revenue_service import (
    get_revenue_breakdown,
    paise_to_rupees
)

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


async def get_monthly_gst_tds_optimized(
    db: AsyncSession,
    month_start: date,
    month_end: date
):
    """
    Calculate GST and TDS collected for a date range.
    Uses centralized revenue service.
    """
    # Use centralized revenue service
    revenue_data = await get_revenue_breakdown(
        db=db,
        start_date=month_start,
        end_date=month_end,
        exclude_gym_id_one=True
    )

    # Extract revenues (all in PAISA)
    daily_pass_revenue = revenue_data.daily_pass
    sessions_revenue = revenue_data.sessions
    fittbot_subscription_revenue = revenue_data.fittbot_subscription
    gym_membership_revenue = revenue_data.gym_membership

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
        import calendar

        today = date.today()

        # Calculate offset
        offset = (page - 1) * page_size

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
        start_month_offset = offset

        monthly_data = []

        # Generate only the page_size months needed
        for i in range(page_size):
            month_index = start_month_offset + i

            # Calculate month and year
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

            # Get calculated GST and TDS collected (using centralized service)
            calculated_data = await get_monthly_gst_tds_optimized(
                db, month_start, month_end
            )
            gst_collected = calculated_data["gst_collected"]
            tds_collected = calculated_data["tds_collected"]

            # Get paid amounts from dictionary
            gst_paid, tds_paid = paid_dict.get(month_str, (0.0, 0.0))

            # Calculate payable amounts
            gst_payable = round(gst_collected - gst_paid, 2)
            tds_payable = round(tds_collected - tds_paid, 2)

            # Format month display name
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

        # Calculate total records
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
