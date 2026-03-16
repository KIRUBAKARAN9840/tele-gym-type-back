"""
Optimized Async API endpoints for Manager Report.

This module provides fully async, optimized endpoints for the manager report page:
1. Performance report for today's calls
2. Performance report for this week's calls
3. Performance report for this month's calls

Each endpoint returns performance stats for all telecallers under the manager.

Metrics:
- Total Calls: Count of gym_call_logs entries for telecaller_id in date range
- Converted: Count from converted_status table in date range
- Rejected: Count of latest entries per (telecaller_id, gym_id) with call_status='rejected'
- Follow-up: Count of latest entries per (telecaller_id, gym_id) with call_status='follow_up'
- No Response: Count of latest entries per (telecaller_id, gym_id) with call_status='no_response'
- Out of Service: Count of latest entries per (telecaller_id, gym_id) with call_status='out_of_service'

All endpoints use AsyncSession and avoid any blocking operations.
"""

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import and_, or_, func, not_, desc, select
from typing import List, Optional, Dict, Any, Tuple
from datetime import datetime, date, timedelta
import pytz

from app.models.async_database import get_async_db
from app.models.telecaller_models import (
    Manager, Telecaller, GymAssignment, GymCallLogs, GymDatabase, ConvertedStatus
)
from app.telecaller.dependencies import get_current_manager
from pydantic import BaseModel

router = APIRouter()

# ============================================================================
# Pydantic Models for Request/Response
# ============================================================================

class TelecallerPerformanceStats(BaseModel):
    telecaller_id: int
    telecaller_name: str
    total_calls: int
    converted: int
    rejected: int
    follow_up: int
    no_response: int
    out_of_service: int


class PerformanceReportResponse(BaseModel):
    telecallers: List[TelecallerPerformanceStats]
    period: str
    generated_at: str


# ============================================================================
# Helper Functions for Date Filtering
# ============================================================================

def get_today_date_range(ist_tz) -> Tuple[datetime, datetime]:
    """Get start and end dates for today in IST."""
    now_ist = datetime.now(ist_tz)
    start_date = now_ist.replace(hour=0, minute=0, second=0, microsecond=0)
    end_date = now_ist.replace(hour=23, minute=59, second=59, microsecond=999999)
    return start_date, end_date


def get_week_date_range(ist_tz) -> Tuple[datetime, datetime]:
    """Get start and end dates for this week in IST (Monday to today)."""
    now_ist = datetime.now(ist_tz)
    days_since_monday = now_ist.weekday()
    start_date = (now_ist - timedelta(days=days_since_monday)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    end_date = now_ist.replace(hour=23, minute=59, second=59, microsecond=999999)
    return start_date, end_date


def get_month_date_range(ist_tz) -> Tuple[datetime, datetime]:
    """Get start and end dates for this month in IST."""
    now_ist = datetime.now(ist_tz)
    start_date = now_ist.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    end_date = now_ist.replace(hour=23, minute=59, second=59, microsecond=999999)
    return start_date, end_date


# ============================================================================
# Core Function to Build Performance Report
# ============================================================================

async def build_performance_report(
    db: AsyncSession,
    manager_id: int,
    telecaller_ids: List[int],
    telecaller_names: Dict[int, str],
    start_date: datetime,
    end_date: datetime,
    period: str,
    ist_tz
) -> PerformanceReportResponse:
    """
    Core function to build performance report for given date range.

    Logic:
    1. Total Calls: Count gym_call_logs entries for telecaller_ids in date range
    2. Converted: Count converted_status entries in date range
    3. Rejected/Follow-up/No Response/Out of Service: Get latest entry per
       (telecaller_id, gym_id) and count by call_status

    Note: Database stores datetimes in IST (local server time), not UTC.
    So we query directly with IST dates.
    """

    # Database stores IST times, so use the dates directly (no UTC conversion)
    start_date_db = start_date.replace(tzinfo=None)
    end_date_db = end_date.replace(tzinfo=None)

    stats_list = []

    for tc_id in telecaller_ids:
        tc_name = telecaller_names[tc_id]

        # ====================================================================
        # STEP 1: Total Calls - Count gym_call_logs for this telecaller in date range
        # ====================================================================

        total_calls_query = select(
            func.count(GymCallLogs.id)
        ).where(
            and_(
                GymCallLogs.telecaller_id == tc_id,
                GymCallLogs.created_at >= start_date_db,
                GymCallLogs.created_at <= end_date_db
            )
        )

        total_calls_result = await db.execute(total_calls_query)
        total_calls = total_calls_result.scalar() or 0

        # ====================================================================
        # STEP 2: Converted - Count converted_status entries in date range
        # ====================================================================

        converted_query = select(
            func.count(ConvertedStatus.id)
        ).where(
            and_(
                ConvertedStatus.telecaller_id == tc_id,
                ConvertedStatus.created_at >= start_date_db,
                ConvertedStatus.created_at <= end_date_db
            )
        )

        converted_result = await db.execute(converted_query)
        converted = converted_result.scalar() or 0

        # ====================================================================
        # STEP 3: Get Latest Entry Per (telecaller_id, gym_id) for Status Counts
        # ====================================================================

        # Subquery to get the latest entry for each gym by this telecaller
        latest_log_subquery = (
            select(
                GymCallLogs.gym_id,
                func.max(GymCallLogs.created_at).label('max_created')
            )
            .where(GymCallLogs.telecaller_id == tc_id)
            .group_by(GymCallLogs.gym_id)
            .subquery()
        )

        # Get all latest logs for this telecaller
        latest_logs_query = (
            select(
                GymCallLogs.gym_id,
                GymCallLogs.call_status,
                GymCallLogs.created_at
            )
            .join(
                latest_log_subquery,
                and_(
                    GymCallLogs.gym_id == latest_log_subquery.c.gym_id,
                    GymCallLogs.created_at == latest_log_subquery.c.max_created
                )
            )
            .where(GymCallLogs.telecaller_id == tc_id)
        )

        latest_logs_result = await db.execute(latest_logs_query)
        latest_logs = latest_logs_result.all()

        # Now filter by date range and count by status
        # Database stores IST times, so compare directly with start_date_db and end_date_db
        rejected = 0
        follow_up = 0
        no_response = 0
        out_of_service = 0

        for log in latest_logs:
            # Check if this log falls within the date range
            # Database times are IST, start_date_db/end_date_db are also IST
            if start_date_db <= log.created_at <= end_date_db:
                if log.call_status == 'rejected':
                    rejected += 1
                elif log.call_status == 'follow_up':
                    follow_up += 1
                elif log.call_status == 'no_response':
                    no_response += 1
                elif log.call_status == 'out_of_service':
                    out_of_service += 1

        stats_list.append(
            TelecallerPerformanceStats(
                telecaller_id=tc_id,
                telecaller_name=tc_name,
                total_calls=total_calls,
                converted=converted,
                rejected=rejected,
                follow_up=follow_up,
                no_response=no_response,
                out_of_service=out_of_service
            )
        )

    return PerformanceReportResponse(
        telecallers=stats_list,
        period=period,
        generated_at=datetime.now(ist_tz).isoformat()
    )


# ============================================================================
# Today's Performance Report Endpoint
# ============================================================================

@router.get("/performance-report/today", response_model=PerformanceReportResponse)
async def get_today_performance_report(
    manager: Manager = Depends(get_current_manager),
    db: AsyncSession = Depends(get_async_db)
):
    """
    Get performance report for all telecallers for today's calls.

    Returns stats for calls made today and follow-ups scheduled for today.
    """
    try:
        ist_tz = pytz.timezone('Asia/Kolkata')
        start_date, end_date = get_today_date_range(ist_tz)

        # Get all telecallers under this manager
        telecallers_query = select(
            Telecaller.id,
            Telecaller.name
        ).where(
            Telecaller.manager_id == manager.id
        ).order_by(Telecaller.name)

        telecallers_result = await db.execute(telecallers_query)
        telecallers = telecallers_result.all()

        if not telecallers:
            return PerformanceReportResponse(
                telecallers=[],
                period='today',
                generated_at=datetime.now(ist_tz).isoformat()
            )

        telecaller_ids = [t.id for t in telecallers]
        telecaller_names = {t.id: t.name for t in telecallers}

        return await build_performance_report(
            db=db,
            manager_id=manager.id,
            telecaller_ids=telecaller_ids,
            telecaller_names=telecaller_names,
            start_date=start_date,
            end_date=end_date,
            period='today',
            ist_tz=ist_tz
        )

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch today's performance report: {str(e)}"
        )


# ============================================================================
# This Week's Performance Report Endpoint
# ============================================================================

@router.get("/performance-report/this-week", response_model=PerformanceReportResponse)
async def get_week_performance_report(
    manager: Manager = Depends(get_current_manager),
    db: AsyncSession = Depends(get_async_db)
):
    """
    Get performance report for all telecallers for this week's calls.

    Returns stats for calls made this week (Monday to today) and
    follow-ups scheduled for this week.
    """
    try:
        ist_tz = pytz.timezone('Asia/Kolkata')
        start_date, end_date = get_week_date_range(ist_tz)

        # Get all telecallers under this manager
        telecallers_query = select(
            Telecaller.id,
            Telecaller.name
        ).where(
            Telecaller.manager_id == manager.id
        ).order_by(Telecaller.name)

        telecallers_result = await db.execute(telecallers_query)
        telecallers = telecallers_result.all()

        if not telecallers:
            return PerformanceReportResponse(
                telecallers=[],
                period='this_week',
                generated_at=datetime.now(ist_tz).isoformat()
            )

        telecaller_ids = [t.id for t in telecallers]
        telecaller_names = {t.id: t.name for t in telecallers}

        return await build_performance_report(
            db=db,
            manager_id=manager.id,
            telecaller_ids=telecaller_ids,
            telecaller_names=telecaller_names,
            start_date=start_date,
            end_date=end_date,
            period='this_week',
            ist_tz=ist_tz
        )

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch this week's performance report: {str(e)}"
        )


# ============================================================================
# This Month's Performance Report Endpoint
# ============================================================================

@router.get("/performance-report/this-month", response_model=PerformanceReportResponse)
async def get_month_performance_report(
    manager: Manager = Depends(get_current_manager),
    db: AsyncSession = Depends(get_async_db)
):
    """
    Get performance report for all telecallers for this month's calls.

    Returns stats for calls made this month and follow-ups scheduled
    for this month.
    """
    try:
        ist_tz = pytz.timezone('Asia/Kolkata')
        start_date, end_date = get_month_date_range(ist_tz)

        # Get all telecallers under this manager
        telecallers_query = select(
            Telecaller.id,
            Telecaller.name
        ).where(
            Telecaller.manager_id == manager.id
        ).order_by(Telecaller.name)

        telecallers_result = await db.execute(telecallers_query)
        telecallers = telecallers_result.all()

        if not telecallers:
            return PerformanceReportResponse(
                telecallers=[],
                period='this_month',
                generated_at=datetime.now(ist_tz).isoformat()
            )

        telecaller_ids = [t.id for t in telecallers]
        telecaller_names = {t.id: t.name for t in telecallers}

        return await build_performance_report(
            db=db,
            manager_id=manager.id,
            telecaller_ids=telecaller_ids,
            telecaller_names=telecaller_names,
            start_date=start_date,
            end_date=end_date,
            period='this_month',
            ist_tz=ist_tz
        )

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch this month's performance report: {str(e)}"
        )
