from fastapi import APIRouter, Depends, Query, HTTPException
from typing import Optional
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, and_, case, literal
from datetime import datetime, timedelta
from calendar import monthrange
import pytz

from app.models.async_database import get_async_db
from app.models.telecaller_models import (
    Telecaller, Manager, GymCallLogs, ClientCallFeedback, GymDatabase
)
from app.models.fittbot_models import Client

import logging

logger = logging.getLogger(__name__)

IST = pytz.timezone("Asia/Kolkata")

router = APIRouter(
    prefix="/api/admin/telecaller-activity",
    tags=["AdminTelecallerActivity"]
)


def ist_date_to_utc(date_str: str, end_of_day: bool = False):
    """Convert an IST date string (YYYY-MM-DD) to UTC datetime."""
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    if end_of_day:
        dt = dt.replace(hour=23, minute=59, second=59, microsecond=999999)
    else:
        dt = dt.replace(hour=0, minute=0, second=0, microsecond=0)
    return IST.localize(dt).astimezone(pytz.utc).replace(tzinfo=None)


def resolve_date_range(date_filter, date, month, year, start_date, end_date, now):
    """
    Resolve the UTC start/end range based on filter mode.
    Returns (utc_start, utc_end, display_label).
    """
    if date_filter == "date":
        # Single date
        target = date or now.strftime("%Y-%m-%d")
        return (
            ist_date_to_utc(target, end_of_day=False),
            ist_date_to_utc(target, end_of_day=True),
            target,
        )

    elif date_filter == "month":
        # Full month: month=3&year=2026 → March 2026
        m = month or now.month
        y = year or now.year
        first_day = f"{y}-{m:02d}-01"
        last_day_num = monthrange(y, m)[1]
        last_day = f"{y}-{m:02d}-{last_day_num:02d}"
        return (
            ist_date_to_utc(first_day, end_of_day=False),
            ist_date_to_utc(last_day, end_of_day=True),
            f"{y}-{m:02d}",
        )

    elif date_filter == "custom":
        # Custom range: start_date & end_date required
        if not start_date or not end_date:
            raise HTTPException(
                status_code=400,
                detail="start_date and end_date are required for custom range (YYYY-MM-DD)."
            )
        return (
            ist_date_to_utc(start_date, end_of_day=False),
            ist_date_to_utc(end_date, end_of_day=True),
            f"{start_date} to {end_date}",
        )

    elif date_filter == "today":
        today = now.strftime("%Y-%m-%d")
        return (
            ist_date_to_utc(today, end_of_day=False),
            ist_date_to_utc(today, end_of_day=True),
            today,
        )

    elif date_filter == "week":
        end = now.strftime("%Y-%m-%d")
        start = (now - timedelta(days=7)).strftime("%Y-%m-%d")
        return (
            ist_date_to_utc(start, end_of_day=False),
            ist_date_to_utc(end, end_of_day=True),
            f"{start} to {end}",
        )

    else:
        # Default: today
        today = now.strftime("%Y-%m-%d")
        return (
            ist_date_to_utc(today, end_of_day=False),
            ist_date_to_utc(today, end_of_day=True),
            today,
        )


def utc_to_ist_time_str(utc_dt) -> str:
    """Convert UTC datetime to IST time string like '09:15 AM'."""
    if not utc_dt:
        return ""
    if utc_dt.tzinfo is None:
        utc_dt = pytz.utc.localize(utc_dt)
    ist_dt = utc_dt.astimezone(IST)
    return ist_dt.strftime("%I:%M %p")


def utc_to_ist_datetime_str(utc_dt) -> str:
    """Convert UTC datetime to IST datetime string."""
    if not utc_dt:
        return ""
    if utc_dt.tzinfo is None:
        utc_dt = pytz.utc.localize(utc_dt)
    ist_dt = utc_dt.astimezone(IST)
    return ist_dt.strftime("%Y-%m-%d %I:%M %p")


def utc_to_ist_date_str(utc_dt) -> str:
    """Convert UTC datetime to IST date string like '2026-03-04'."""
    if not utc_dt:
        return ""
    if utc_dt.tzinfo is None:
        utc_dt = pytz.utc.localize(utc_dt)
    ist_dt = utc_dt.astimezone(IST)
    return ist_dt.strftime("%Y-%m-%d")


@router.get("/daily-report")
async def get_daily_activity_report(
    date_filter: str = Query("today", description="Filter mode: today, date, week, month, custom"),
    date: Optional[str] = Query(None, description="Specific date (YYYY-MM-DD). Used when date_filter=date"),
    month: Optional[int] = Query(None, ge=1, le=12, description="Month number (1-12). Used when date_filter=month"),
    year: Optional[int] = Query(None, description="Year (e.g. 2026). Used when date_filter=month"),
    start_date: Optional[str] = Query(None, description="Range start (YYYY-MM-DD). Used when date_filter=custom"),
    end_date: Optional[str] = Query(None, description="Range end (YYYY-MM-DD). Used when date_filter=custom"),
    telecaller_id: Optional[int] = Query(None, description="Filter by specific telecaller ID"),
    db: AsyncSession = Depends(get_async_db)
):
    try:
        now = datetime.now(IST)

        try:
            utc_start, utc_end, date_label = resolve_date_range(
                date_filter, date, month, year, start_date, end_date, now
            )
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY-MM-DD.")

        # 1. Get all telecallers (with manager name)
        telecaller_stmt = select(
            Telecaller.id,
            Telecaller.name,
            Telecaller.mobile_number,
            Telecaller.status,
            Manager.name.label("manager_name")
        ).outerjoin(
            Manager, Telecaller.manager_id == Manager.id
        )

        if telecaller_id:
            telecaller_stmt = telecaller_stmt.where(Telecaller.id == telecaller_id)

        telecaller_result = await db.execute(telecaller_stmt)
        telecallers = telecaller_result.all()

        is_single_day = date_filter in ("today", "date")

        if not telecallers:
            return {
                "success": True,
                "data": {
                    "date_filter": date_filter,
                    "date_range": date_label,
                    "overall_summary": {
                        "total_telecallers": 0,
                        "active_in_range": 0,
                        "total_gym_calls": 0,
                        "total_client_calls": 0
                    },
                    "telecallers": []
                }
            }

        telecaller_ids = [t.id for t in telecallers]

        # 2. Fetch all GymCallLogs for the date + telecaller IDs
        gym_logs_stmt = select(
            GymCallLogs.id,
            GymCallLogs.telecaller_id,
            GymCallLogs.gym_id,
            GymCallLogs.call_status,
            GymCallLogs.remarks,
            GymCallLogs.follow_up_date,
            GymCallLogs.created_at,
            GymCallLogs.interest_level,
            GymCallLogs.total_members,
            GymCallLogs.new_contact_number,
            GymCallLogs.feature_explained,
            GymDatabase.gym_name,
        ).outerjoin(
            GymDatabase, GymCallLogs.gym_id == GymDatabase.id
        ).where(
            and_(
                GymCallLogs.telecaller_id.in_(telecaller_ids),
                GymCallLogs.created_at >= utc_start,
                GymCallLogs.created_at <= utc_end
            )
        ).order_by(GymCallLogs.created_at)

        gym_logs_result = await db.execute(gym_logs_stmt)
        gym_logs = gym_logs_result.all()

        # 3. Fetch all ClientCallFeedback for the date + telecaller IDs
        client_logs_stmt = select(
            ClientCallFeedback.id,
            ClientCallFeedback.executive_id,
            ClientCallFeedback.client_id,
            ClientCallFeedback.status,
            ClientCallFeedback.feedback,
            ClientCallFeedback.created_at,
            Client.name.label("client_name"),
            Client.contact.label("client_contact"),
        ).outerjoin(
            Client, ClientCallFeedback.client_id == Client.client_id
        ).where(
            and_(
                ClientCallFeedback.executive_id.in_(telecaller_ids),
                ClientCallFeedback.created_at >= utc_start,
                ClientCallFeedback.created_at <= utc_end
            )
        ).order_by(ClientCallFeedback.created_at)

        client_logs_result = await db.execute(client_logs_stmt)
        client_logs = client_logs_result.all()

        # 4. Group by telecaller
        gym_logs_by_tc = {}
        for log in gym_logs:
            gym_logs_by_tc.setdefault(log.telecaller_id, []).append(log)

        client_logs_by_tc = {}
        for log in client_logs:
            client_logs_by_tc.setdefault(log.executive_id, []).append(log)

        # 5. Build per-telecaller response
        overall_gym_calls = 0
        overall_client_calls = 0
        active_count = 0
        telecaller_reports = []

        for tc in telecallers:
            tc_gym_logs = gym_logs_by_tc.get(tc.id, [])
            tc_client_logs = client_logs_by_tc.get(tc.id, [])

            # --- Gym calls summary (by call_status) ---
            gym_status_counts = {}
            for log in tc_gym_logs:
                s = log.call_status or "unknown"
                gym_status_counts[s] = gym_status_counts.get(s, 0) + 1

            gym_calls_summary = {
                "total": len(tc_gym_logs),
                "interested": gym_status_counts.get("interested", 0),
                "converted": gym_status_counts.get("converted", 0),
                "rejected": gym_status_counts.get("rejected", 0),
                "follow_up": gym_status_counts.get("follow_up", 0) + gym_status_counts.get("follow_up_required", 0),
                "no_response": gym_status_counts.get("no_response", 0),
                "not_interested": gym_status_counts.get("not_interested", 0),
                "contacted": gym_status_counts.get("contacted", 0),
                "delegated": gym_status_counts.get("delegated", 0),
                "out_of_service": gym_status_counts.get("out_of_service", 0),
                "closed": gym_status_counts.get("closed", 0),
                "pending": gym_status_counts.get("pending", 0),
            }

            # --- Client calls summary (by status) ---
            client_status_counts = {}
            for log in tc_client_logs:
                s = log.status or "unknown"
                client_status_counts[s] = client_status_counts.get(s, 0) + 1

            client_calls_summary = {
                "total": len(tc_client_logs),
                "interested": client_status_counts.get("interested", 0),
                "not_interested": client_status_counts.get("not_interested", 0),
                "callback": client_status_counts.get("callback", 0),
                "no_answer": client_status_counts.get("no_answer", 0),
                "converted": client_status_counts.get("converted", 0),
                "follow_up": client_status_counts.get("follow_up", 0),
            }

            # --- Timeline (merge both, sort by created_at) ---
            timeline = []

            for log in tc_gym_logs:
                entry = {
                    "time": utc_to_ist_time_str(log.created_at),
                    "timestamp": utc_to_ist_datetime_str(log.created_at),
                    "type": "gym_call",
                    "name": log.gym_name or f"Gym #{log.gym_id}",
                    "status": log.call_status,
                    "remarks": log.remarks,
                    "interest_level": log.interest_level,
                    "follow_up_date": utc_to_ist_datetime_str(log.follow_up_date) if log.follow_up_date else None,
                }
                if not is_single_day:
                    entry["date"] = utc_to_ist_date_str(log.created_at)
                timeline.append(entry)

            for log in tc_client_logs:
                entry = {
                    "time": utc_to_ist_time_str(log.created_at),
                    "timestamp": utc_to_ist_datetime_str(log.created_at),
                    "type": "client_call",
                    "name": log.client_name or f"Client #{log.client_id}",
                    "client_contact": log.client_contact,
                    "status": log.status,
                    "remarks": log.feedback,
                }
                if not is_single_day:
                    entry["date"] = utc_to_ist_date_str(log.created_at)
                timeline.append(entry)

            # Sort timeline chronologically
            timeline.sort(key=lambda x: x["timestamp"])

            # First/last activity
            first_activity = timeline[0]["time"] if timeline else None
            last_activity = timeline[-1]["time"] if timeline else None

            is_active = len(timeline) > 0
            if is_active:
                active_count += 1

            overall_gym_calls += len(tc_gym_logs)
            overall_client_calls += len(tc_client_logs)

            telecaller_reports.append({
                "telecaller_id": tc.id,
                "name": tc.name,
                "mobile_number": tc.mobile_number,
                "manager_name": tc.manager_name,
                "status": tc.status,
                "is_active_today": is_active,
                "summary": {
                    "gym_calls": gym_calls_summary,
                    "client_calls": client_calls_summary,
                    "total_activities": len(timeline),
                    "first_activity": first_activity,
                    "last_activity": last_activity,
                },
                "timeline": timeline,
            })


        telecaller_reports.sort(
            key=lambda x: (-x["summary"]["total_activities"],)
        )

        return {
            "success": True,
            "data": {
                "date_filter": date_filter,
                "date_range": date_label,
                "overall_summary": {
                    "total_telecallers": len(telecallers),
                    "active_in_range": active_count,
                    "total_gym_calls": overall_gym_calls,
                    "total_client_calls": overall_client_calls,
                    "total_activities": overall_gym_calls + overall_client_calls,
                },
                "telecallers": telecaller_reports,
            },
            "message": "Activity report fetched successfully"
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"[TELECALLER-ACTIVITY] Error: {str(e)}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error fetching daily report: {str(e)}")
