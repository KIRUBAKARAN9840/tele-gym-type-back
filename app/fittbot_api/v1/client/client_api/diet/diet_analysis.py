# app/api/v1/analytics/diet_analysis.py

from datetime import datetime
from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from app.models.database import get_db
from app.models.fittbot_models import ClientActualAggregated, ClientActualAggregatedWeekly
from app.utils.logging_utils import FittbotHTTPException

router = APIRouter(prefix="/diet_analysis", tags=["Client Analytics"])


@router.get("/get")
async def get_diet_analysis(client_id: int, db: Session = Depends(get_db)) -> dict:

    try:
        current_year = datetime.now().year

        year_data = (
            db.query(ClientActualAggregated)
            .filter(
                ClientActualAggregated.client_id == client_id,
                ClientActualAggregated.year == current_year,
            )
            .first()
        )

        # Safely coalesce Nones to 0 for macro calorie math
        if not year_data:
            avg_protein = 0
            avg_carbs = 0
            avg_fats = 0
        else:
            avg_protein = year_data.avg_protein or 0
            avg_carbs = year_data.avg_carbs or 0
            avg_fats = year_data.avg_fats or 0

        total_calories_from_protein = avg_protein * 4
        total_calories_from_carbs = avg_carbs * 4
        total_calories_from_fats = avg_fats * 9
        total_calories = (
            total_calories_from_protein
            + total_calories_from_carbs
            + total_calories_from_fats
        )

        if total_calories == 0:
            protein_percentage = 0.0
            carbs_percentage = 0.0
            fats_percentage = 0.0
        else:
            protein_percentage = round((total_calories_from_protein / total_calories) * 100, 2)
            carbs_percentage = round((total_calories_from_carbs / total_calories) * 100, 2)
            fats_percentage = round((total_calories_from_fats / total_calories) * 100, 2)

        macro_split = {
            "protein_percentage": protein_percentage,
            "carbs_percentage": carbs_percentage,
            "fats_percentage": fats_percentage,
        }

        # ---- Stats ----
        if not year_data:
            stats = {
                "no_of_days_calories_met": 0,
                "calories_surplus_days": 0,
                "calories_deficit_days": 0,
                "longest_streak": 0,
                "average_protein_target": 0,
                "average_carbs_target": 0,
                "average_fat_target": 0,
            }
        else:
            stats = {
                "no_of_days_calories_met": year_data.no_of_days_calories_met or 0,
                "calories_surplus_days": year_data.calories_surplus_days or 0,
                "calories_deficit_days": year_data.calories_deficit_days or 0,
                "longest_streak": year_data.longest_streak or 0,
                "average_protein_target": year_data.average_protein_target or 0,
                "average_carbs_target": year_data.average_carbs_target or 0,
                "average_fat_target": year_data.average_fat_target or 0,
            }

        # ---- Weekly charts for current year ----
        week_start_lower = datetime(current_year, 1, 1)
        week_start_upper = datetime(current_year + 1, 1, 1)

        weekly_rows = (
            db.query(ClientActualAggregatedWeekly)
            .filter(
                ClientActualAggregatedWeekly.client_id == client_id,
                ClientActualAggregatedWeekly.week_start >= week_start_lower,
                ClientActualAggregatedWeekly.week_start < week_start_upper,
            )
            .order_by(ClientActualAggregatedWeekly.week_start.asc())
            .all()
        )

        weekly_data = {"calories": [], "protein": [], "carbs": [], "fats": []}

        for r in weekly_rows or []:
            date_label = r.week_start.isoformat() if hasattr(r.week_start, "isoformat") else str(r.week_start)
            weekly_data["calories"].append({"label": "calories", "date": date_label, "value": r.avg_calories or 0})
            weekly_data["protein"].append({"label": "protein", "date": date_label, "value": r.avg_protein or 0})
            weekly_data["carbs"].append({"label": "carbs", "date": date_label, "value": r.avg_carbs or 0})
            weekly_data["fats"].append({"label": "fats", "date": date_label, "value": r.avg_fats or 0})

        return {
            "status": 200,
            "message": "Diet analysis fetched successfully",
            "data": {
                "macro_split": macro_split,
                "stats": stats,
                "weekly_data": weekly_data,
            },
        }

    except FittbotHTTPException:
        # Pass through structured, known errors
        raise
    except Exception as e:
        # Normalize all unexpected errors to your standard error format
        raise FittbotHTTPException(
            status_code=500,
            detail="Failed to fetch diet analysis",
            error_code="CLIENT_DIET_ANALYSIS_ERROR",
            log_data={"client_id": client_id, "error": str(e)},
        )
