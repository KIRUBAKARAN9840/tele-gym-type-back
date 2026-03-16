# app/api/v1/analytics/workout_insights.py
 
from datetime import datetime
from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
 
from app.models.database import get_db
from app.models.fittbot_models import (
    AggregatedInsights,
    MuscleAggregatedInsights,
    ClientWeeklyPerformance,
)
from app.utils.logging_utils import FittbotHTTPException
 
router = APIRouter(prefix="/workout_analysis", tags=["Client Analytics"])
 
 
@router.get("/get")
async def get_workout_insights(
    client_id: int,
    start_date: str = None,
    end_date: str = None,
    db: Session = Depends(get_db)
) -> dict:
 
    try:
        current_year = datetime.now().year
 
        # Determine date range for filtering
        if start_date and end_date:
            # Convert string dates to datetime objects
            start_datetime = datetime.strptime(start_date, "%Y-%m-%d")
            end_datetime = datetime.strptime(end_date, "%Y-%m-%d")
        else:
            # Default to current year if no date range provided
            start_datetime = datetime(current_year, 1, 1)
            end_datetime = datetime(current_year + 1, 1, 1)
 
        # ---- Filtered weekly aggregates (DESC for "latest then previous") ----
        year_data = (
            db.query(AggregatedInsights)
            .filter(
                AggregatedInsights.client_id == client_id,
                AggregatedInsights.week_start >= start_datetime,
                AggregatedInsights.week_start <= end_datetime,
            )
            .order_by(AggregatedInsights.week_start.desc())
            .all()
        )
 
        # ---- Comparison comments (latest vs. previous) ----
        if not year_data:
            comparison_comment = [
                "There is no data to compare. Please start workouts to get analysis."
            ]
        elif len(year_data) < 2:
            comparison_comment = [
                "Not enough data to compare the last two weeks."
            ]
        else:
            latest = year_data[0]
            previous = year_data[1]
 
            prev_total_volume = (previous.total_volume or 0)
            prev_avg_weight = (previous.avg_weight or 0)
            prev_avg_reps = (previous.avg_reps or 0)
 
            # Avoid division by zero and None
            volume_change = (
                round(((latest.total_volume or 0) - prev_total_volume) / prev_total_volume * 100, 2)
                if prev_total_volume
                else 0
            )
            avg_weight_change = (
                round(((latest.avg_weight or 0) - prev_avg_weight) / prev_avg_weight * 100, 2)
                if prev_avg_weight
                else 0
            )
            avg_reps_change = (
                round(((latest.avg_reps or 0) - prev_avg_reps) / prev_avg_reps * 100, 2)
                if prev_avg_reps
                else 0
            )
 
            comparison_comment = []
            if volume_change > 0:
                comparison_comment.append(f"Great job! Your total workout volume increased by {volume_change}%.")
            elif volume_change < 0:
                comparison_comment.append(f"Your total workout volume decreased by {abs(volume_change)}%. Focus on consistency.")
 
            if avg_weight_change > 0:
                comparison_comment.append(f"You're lifting heavier weights! Average weight increased by {avg_weight_change}%.")
            elif avg_weight_change < 0:
                comparison_comment.append(f"Average weight dropped by {abs(avg_weight_change)}%. Consider increasing the intensity.")
 
            if avg_reps_change > 0:
                comparison_comment.append(f"You're doing more reps! Average reps increased by {avg_reps_change}%.")
            elif avg_reps_change < 0:
                comparison_comment.append(f"Average reps decreased by {abs(avg_reps_change)}%. Focus on endurance.")
 
            if not comparison_comment:
                comparison_comment = ["Your performance is consistent with the previous week."]
 
        # ---- Aggregated per-muscle insights (all-time or all rows for client) ----
        muscle_data_aggregated = (
            db.query(MuscleAggregatedInsights)
            .filter(MuscleAggregatedInsights.client_id == client_id)
            .all()
        )
 
        aggregated_muscle_insights = {
            "total_volume": [],
            "avg_weight": [],
            "avg_reps": [],
            "max_weight": [],
            "max_reps": [],
            "rest_days": [],
        }
 
        for record in muscle_data_aggregated or []:
            mg = record.muscle_group
            aggregated_muscle_insights["total_volume"].append({"label": mg, "value": record.total_volume or 0})
            aggregated_muscle_insights["avg_weight"].append({"label": mg, "value": record.avg_weight or 0})
            aggregated_muscle_insights["avg_reps"].append({"label": mg, "value": record.avg_reps or 0})
            aggregated_muscle_insights["max_weight"].append({"label": mg, "value": record.max_weight or 0})
            aggregated_muscle_insights["max_reps"].append({"label": mg, "value": record.max_reps or 0})
            aggregated_muscle_insights["rest_days"].append({"label": mg, "value": record.rest_days or 0})
 
        # ---- Per-muscle weekly series within date range ----
        muscle_rows = (
            db.query(ClientWeeklyPerformance)
            .filter(
                ClientWeeklyPerformance.client_id == client_id,
                ClientWeeklyPerformance.week_start >= start_datetime,
                ClientWeeklyPerformance.week_start <= end_datetime,
            )
            .order_by(ClientWeeklyPerformance.week_start.asc())
            .all()
        )
 
        muscle_insights = {}
        muscle_group_list = []
        muscle_group_ids = {}
        next_id = 1
 
        for record in muscle_rows or []:
            mg = record.muscle_group
            if mg not in muscle_insights:
                muscle_insights[mg] = {
                    "weekly_data": {"total_volume": [], "avg_weight": [], "avg_reps": []}
                }
                muscle_group_ids[mg] = next_id
                muscle_group_list.append({"id": next_id, "name": mg})
                next_id += 1
 
            date_label = record.week_start.isoformat() if hasattr(record.week_start, "isoformat") else str(record.week_start)
            muscle_insights[mg]["weekly_data"]["total_volume"].append({
                "week_start": date_label, "value": record.total_volume or 0
            })
            muscle_insights[mg]["weekly_data"]["avg_weight"].append({
                "week_start": date_label, "value": record.avg_weight or 0
            })
            muscle_insights[mg]["weekly_data"]["avg_reps"].append({
                "week_start": date_label, "value": record.avg_reps or 0
            })
 
        # ---- Overall weekly data for current year ----
        overall_data = [
            {
                "week_start": (r.week_start.isoformat() if hasattr(r.week_start, "isoformat") else str(r.week_start)),
                "total_volume": r.total_volume or 0,
                "avg_weight": r.avg_weight or 0,
                "avg_reps": r.avg_reps or 0,
            }
            for r in (year_data or [])
        ]
 
        return {
            "status": 200,
            "message": "Workout insights fetched successfully",
            "data": {
                "overall_data": overall_data,
                "comparison_comment": comparison_comment,
                "aggregated_muscle_insights": aggregated_muscle_insights,
                "muscle_insights": muscle_insights,
                "muscle_group_list": muscle_group_list,
            },
        }
 
    except FittbotHTTPException:
        # Pass through structured, known errors.
        raise
    except Exception as e:
        # Normalize unexpected errors to your standard format.
        raise FittbotHTTPException(
            status_code=500,
            detail="Failed to fetch workout insights",
            error_code="CLIENT_WORKOUT_INSIGHTS_ERROR",
            log_data={"client_id": client_id, "error": str(e)},
        )
 
 