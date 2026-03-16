from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import and_, func, desc, case, literal_column
from datetime import datetime, timedelta, date
from app.models.database import get_db
from app.models.telecaller_models import Telecaller, GymAssignment, GymCallLogs, GymDatabase
from app.telecaller.dependencies import get_current_telecaller
from typing import Dict, Any, List
import pytz

router = APIRouter()

@router.get("/stats")
async def get_telecaller_dashboard_stats(
    db: Session = Depends(get_db),
    current_telecaller: Telecaller = Depends(get_current_telecaller)
) -> Dict[str, Any]:
    """
    Get dashboard statistics for telecaller - Optimized version

    Uses efficient queries with subqueries for latest log detection.
    No blocking loops - all queries execute independently.
    """
    try:
        # Use IST timezone
        ist_tz = pytz.timezone('Asia/Kolkata')
        now_ist = datetime.now(ist_tz)
        today = now_ist.date()

        # Start and end of today in IST
        start_of_day_ist = ist_tz.localize(datetime.combine(today, datetime.min.time()))
        end_of_day_ist = ist_tz.localize(datetime.combine(today, datetime.max.time()))

        # Convert to UTC for database comparisons
        start_of_day_utc = start_of_day_ist.astimezone(pytz.UTC).replace(tzinfo=None)
        end_of_day_utc = end_of_day_ist.astimezone(pytz.UTC).replace(tzinfo=None)
        thirty_days_ago_utc = (now_ist - timedelta(days=30)).astimezone(pytz.UTC).replace(tzinfo=None)

        # ============================================================
        # OPTIMIZED: Calculate all stats using efficient queries
        # ============================================================

        # 1. Today's Target: Count of gyms assigned today that haven't been called yet
        today_assigned = db.query(
            func.count(GymAssignment.gym_id)
        ).filter(
            and_(
                GymAssignment.telecaller_id == current_telecaller.id,
                GymAssignment.target_date == today,
                GymAssignment.status == 'active'
            )
        ).scalar() or 0

        # Get unique gym_ids that were called today
        called_gyms_today = db.query(
            func.count(func.distinct(GymCallLogs.gym_id))
        ).filter(
            and_(
                GymCallLogs.telecaller_id == current_telecaller.id,
                GymCallLogs.created_at >= start_of_day_utc,
                GymCallLogs.created_at <= end_of_day_utc
            )
        ).scalar() or 0

        today_target = max(0, today_assigned - called_gyms_today)

        # 2. Followups Today: Count gyms where LATEST call status is follow_up and follow_up_date is today
        # Subquery to get latest call log for each gym
        latest_log_subquery = (
            db.query(
                GymCallLogs.gym_id,
                func.max(GymCallLogs.created_at).label('max_created')
            )
            .filter(GymCallLogs.telecaller_id == current_telecaller.id)
            .group_by(GymCallLogs.gym_id)
            .subquery()
        )

        followups_today = db.query(
            func.count(func.distinct(GymCallLogs.gym_id))
        ).join(
            latest_log_subquery,
            and_(
                GymCallLogs.gym_id == latest_log_subquery.c.gym_id,
                GymCallLogs.created_at == latest_log_subquery.c.max_created
            )
        ).filter(
            and_(
                GymCallLogs.call_status.in_(['follow_up', 'follow_up_required']),
                func.date(GymCallLogs.follow_up_date) == today
            )
        ).scalar() or 0

        # 3. Calls Today: Total count of call log entries for today
        calls_today = db.query(
            func.count(GymCallLogs.id)
        ).filter(
            and_(
                GymCallLogs.telecaller_id == current_telecaller.id,
                GymCallLogs.created_at >= start_of_day_utc,
                GymCallLogs.created_at <= end_of_day_utc
            )
        ).scalar() or 0

        # 4. Today's Converted: Count gyms where LATEST call was converted today
        converted_today = db.query(
            func.count(func.distinct(GymCallLogs.gym_id))
        ).join(
            latest_log_subquery,
            and_(
                GymCallLogs.gym_id == latest_log_subquery.c.gym_id,
                GymCallLogs.created_at == latest_log_subquery.c.max_created
            )
        ).filter(
            and_(
                GymCallLogs.call_status == 'converted',
                func.date(GymCallLogs.created_at) == today
            )
        ).scalar() or 0

        # 5. Today's Rejected: Count gyms where LATEST call was rejected today
        rejected_today = db.query(
            func.count(func.distinct(GymCallLogs.gym_id))
        ).join(
            latest_log_subquery,
            and_(
                GymCallLogs.gym_id == latest_log_subquery.c.gym_id,
                GymCallLogs.created_at == latest_log_subquery.c.max_created
            )
        ).filter(
            and_(
                GymCallLogs.call_status == 'rejected',
                func.date(GymCallLogs.created_at) == today
            )
        ).scalar() or 0

        # 6. Today's No Response: Count gyms where LATEST call was no_response today
        no_response_today = db.query(
            func.count(func.distinct(GymCallLogs.gym_id))
        ).join(
            latest_log_subquery,
            and_(
                GymCallLogs.gym_id == latest_log_subquery.c.gym_id,
                GymCallLogs.created_at == latest_log_subquery.c.max_created
            )
        ).filter(
            and_(
                GymCallLogs.call_status == 'no_response',
                func.date(GymCallLogs.created_at) == today
            )
        ).scalar() or 0

        # 7. 30-day Conversion Rate
        total_gyms_30_days = db.query(
            func.count(func.distinct(GymCallLogs.gym_id))
        ).filter(
            and_(
                GymCallLogs.telecaller_id == current_telecaller.id,
                GymCallLogs.created_at >= thirty_days_ago_utc
            )
        ).scalar() or 0

        converted_gyms_30_days = db.query(
            func.count(func.distinct(GymCallLogs.gym_id))
        ).filter(
            and_(
                GymCallLogs.telecaller_id == current_telecaller.id,
                GymCallLogs.call_status == 'converted',
                GymCallLogs.created_at >= thirty_days_ago_utc
            )
        ).scalar() or 0

        conversion_rate = (converted_gyms_30_days / total_gyms_30_days * 100) if total_gyms_30_days > 0 else 0

        return {
            "today_target": today_target,
            "followups_today": followups_today,
            "calls_today": calls_today,
            "todays_converted": converted_today,
            "todays_rejected": rejected_today,
            "todays_no_response": no_response_today,
            "conversion_rate": round(conversion_rate, 2),
        }

    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch dashboard stats: {str(e)}"
        )

@router.get("/followups/today")
async def get_today_followups(
    db: Session = Depends(get_db),
    current_telecaller: Telecaller = Depends(get_current_telecaller)
) -> List[Dict[str, Any]]:
    """
    Get today's follow-ups for the telecaller
    """
    try:
        # Use IST timezone
        ist_tz = pytz.timezone('Asia/Kolkata')
        now_ist = datetime.now(ist_tz)
        today = now_ist.date()
        start_of_day = ist_tz.localize(datetime.combine(today, datetime.min.time()))
        end_of_day = ist_tz.localize(datetime.combine(today, datetime.max.time()))

        # Convert to UTC for comparison since DB stores UTC
        start_of_day_utc = start_of_day.astimezone(pytz.UTC).replace(tzinfo=None)
        end_of_day_utc = end_of_day.astimezone(pytz.UTC).replace(tzinfo=None)

        # Get only the latest follow-up for each gym
        from sqlalchemy import func

        # Subquery to get the latest follow-up for each gym today
        subquery = (
            db.query(
                GymCallLogs.gym_id,
                func.max(GymCallLogs.created_at).label('max_created')
            )
            .filter(
                and_(
                    GymCallLogs.telecaller_id == current_telecaller.id,
                    GymCallLogs.call_status.in_(['follow_up', 'follow_up_required']),
                    GymCallLogs.follow_up_date >= start_of_day_utc,
                    GymCallLogs.follow_up_date <= end_of_day_utc
                )
            )
            .group_by(GymCallLogs.gym_id)
            .subquery()
        )

        # Get follow-ups with gym details (only latest per gym)
        followups = (
            db.query(GymCallLogs, GymAssignment)
            .join(GymAssignment, GymCallLogs.gym_id == GymAssignment.gym_id)
            .join(
                subquery,
                and_(
                    GymCallLogs.gym_id == subquery.c.gym_id,
                    GymCallLogs.created_at == subquery.c.max_created
                )
            )
            .filter(
                and_(
                    GymCallLogs.telecaller_id == current_telecaller.id,
                    GymCallLogs.call_status.in_(['follow_up', 'follow_up_required']),
                    GymCallLogs.follow_up_date >= start_of_day_utc,
                    GymCallLogs.follow_up_date <= end_of_day_utc
                )
            )
            .order_by(GymCallLogs.follow_up_date)
            .all()
        )

        result = []
        for call_log, assignment in followups:
            # Format follow-up time
            follow_up_time = call_log.follow_up_date.strftime("%I:%M %p") if call_log.follow_up_date else None

            result.append({
                "id": call_log.id,
                "gym_id": call_log.gym_id,
                "gym_name": f"Gym {call_log.gym_id}",  # Will be updated when we join with gym_database
                "contact_person": "Contact Person",  # Will be updated when we join with gym_database
                "phone": "Phone Number",  # Will be updated when we join with gym_database
                "follow_up_time": follow_up_time,
                "notes": call_log.remarks,
                "call_count": assignment.call_count or 0,
            })

        return result

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch today's follow-ups: {str(e)}"
        )

@router.get("/gyms/assigned")
async def get_assigned_gyms(
    page: int = 1,
    limit: int = 10,
    db: Session = Depends(get_db),
    current_telecaller: Telecaller = Depends(get_current_telecaller)
) -> Dict[str, Any]:
    """
    Get gyms assigned to the telecaller
    """
    try:
        offset = (page - 1) * limit

        # Get total count
        total = db.query(GymAssignment).filter(
            GymAssignment.telecaller_id == current_telecaller.id
        ).count()

        # Get assigned gyms with pagination
        assignments = db.query(GymAssignment).filter(
            GymAssignment.telecaller_id == current_telecaller.id
        ).offset(offset).limit(limit).all()

        result = []
        for assignment in assignments:
            # Get latest call log for this gym
            latest_call = db.query(GymCallLogs).filter(
                and_(
                    GymCallLogs.telecaller_id == current_telecaller.id,
                    GymCallLogs.gym_id == assignment.gym_id
                )
            ).order_by(GymCallLogs.created_at.desc()).first()

            result.append({
                "gym_id": assignment.gym_id,
                "assigned_at": assignment.assigned_at,
                "priority": assignment.priority,
                "last_called_at": latest_call.created_at if latest_call else None,
                "last_status": latest_call.call_status if latest_call else None,
                "call_count": assignment.call_count or 0,
            })

        return {
            "gyms": result,
            "total": total,
            "page": page,
            "limit": limit,
            "total_pages": (total + limit - 1) // limit
        }

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch assigned gyms: {str(e)}"
        )