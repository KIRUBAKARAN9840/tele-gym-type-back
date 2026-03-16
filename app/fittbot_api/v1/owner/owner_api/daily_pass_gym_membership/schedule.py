# app.py - Daily Pass Schedule API
from __future__ import annotations
from fastapi import APIRouter, Depends, HTTPException, Query
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo
from typing import List, Optional
from pydantic import BaseModel
from app.models.dailypass_models import (
    DailyPassDay,
    get_dailypass_session,
)
from sqlalchemy import select, func, and_

router = APIRouter(prefix="/fittbot_gym_schedule", tags=["daily pass schedule"])


class ScheduledDateData(BaseModel):
    expectedDate: str  # YYYY-MM-DD format
    expectedCount: int


class ScheduleResponse(BaseModel):
    status: int = 200
    gym_id: str
    start_date: str
    end_date: str
    schedule_data: List[ScheduledDateData]


@router.get("/api/daily-pass-schedule", response_model=ScheduleResponse, status_code=200)
async def get_daily_pass_schedule(
    gym_id: str = Query(..., description="Gym ID"),
    days: Optional[int] = Query(60, ge=1, le=180, description="Number of days to fetch (default: 60)"),
):
 
    ist = ZoneInfo("Asia/Kolkata")
    now_ist = datetime.now(ist)
    today = now_ist.date()

    # Calculate date range
    start_date = today
    end_date = today + timedelta(days=days)

    dbs = None
    try:
        dbs = get_dailypass_session()

        # Query to count scheduled daily pass days per date
        stmt = (
            select(
                DailyPassDay.scheduled_date,
                func.count(DailyPassDay.id).label("expected_count")
            )
            .where(
                and_(
                    DailyPassDay.gym_id == str(gym_id),
                    DailyPassDay.scheduled_date >= start_date,
                    DailyPassDay.scheduled_date < end_date,
                    DailyPassDay.status.in_(["available"])
                )
            )
            .group_by(DailyPassDay.scheduled_date)
            .order_by(DailyPassDay.scheduled_date)
        )

        results = dbs.execute(stmt).all()

        # Create a dict for easy lookup
        date_counts = {row.scheduled_date: row.expected_count for row in results}

        # Generate schedule data for all days in range
        schedule_data = []
        current_date = start_date

        while current_date < end_date:
            expected_count = date_counts.get(current_date, 0)
            schedule_data.append(
                ScheduledDateData(
                    expectedDate=current_date.isoformat(),
                    expectedCount=expected_count
                )
            )
            current_date += timedelta(days=1)

        return ScheduleResponse(
            status=200,
            gym_id=str(gym_id),
            start_date=start_date.isoformat(),
            end_date=end_date.isoformat(),
            schedule_data=schedule_data
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching schedule: {str(e)}")
    finally:
        if dbs:
            try:
                dbs.close()
            except Exception:
                pass
