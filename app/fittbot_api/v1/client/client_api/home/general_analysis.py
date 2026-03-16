# app/api/v1/analytics/client_general_analysis.py

from datetime import datetime
from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from app.models.database import get_db
from app.models.fittbot_models import ClientGeneralAnalysis, ClientActualAggregated
from app.utils.logging_utils import FittbotHTTPException

router = APIRouter(prefix="/general_analysis", tags=["Client Analytics"])


@router.get("/client")
async def get_client_general_analysis(client_id: int, db: Session = Depends(get_db)):

    try:
        # ---- Fetch monthly records (ascending by date) ----
        records = (
            db.query(ClientGeneralAnalysis)
            .filter(ClientGeneralAnalysis.client_id == client_id)
            .order_by(ClientGeneralAnalysis.date.asc())
            .all()
        )

        if not records:
            raise FittbotHTTPException(
                status_code=404,
                detail="No data available for the specified client.",
                error_code="CLIENT_ANALYSIS_NOT_FOUND",
            )

        monthly_data = {
            "weight": [],
            "water_taken": [],
            "attendance": [],
            "burnt_calories": [],
        }

        for r in records:
            label = r.date.isoformat() if hasattr(r.date, "isoformat") else str(r.date)
            monthly_data["weight"].append({"label": label, "value": r.weight or 0})
            monthly_data["water_taken"].append({"label": label, "value": r.water_taken or 0})
            monthly_data["burnt_calories"].append({"label": label, "value": r.burnt_calories or 0})
            monthly_data["attendance"].append({"label": label, "value": r.attendance or 0})

        # ---- Fetch current year aggregated actual gym time (in minutes) ----
        current_year = datetime.now().year
        aggregated = (
            db.query(ClientActualAggregated)
            .filter(
                ClientActualAggregated.client_id == client_id,
                ClientActualAggregated.year == current_year,
            )
            .first()
        )

        if not aggregated or not aggregated.gym_time:
            gym_time = {"hour": 0, "minutes": 0}
        else:
            total_minutes = int(aggregated.gym_time)
            gym_time = {"hour": total_minutes // 60, "minutes": total_minutes % 60}

        return {
            "status": 200,
            "message": "Client general analysis fetched successfully",
            "data": {
                "monthly_data": monthly_data,
                "total_gym_time": gym_time,
            },
        }

    except FittbotHTTPException:
        # Pass through known, structured errors.
        raise
    except Exception as e:
        # Normalize any unexpected error to your standard format.
        raise FittbotHTTPException(
            status_code=500,
            detail="Failed to fetch client general analysis",
            error_code="CLIENT_GENERAL_ANALYSIS_ERROR",
            log_data={"client_id": client_id, "error": str(e)},
        )
