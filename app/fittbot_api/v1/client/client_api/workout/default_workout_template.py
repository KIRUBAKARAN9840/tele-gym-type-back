# app/routers/default_workout_template_router.py

from fastapi import APIRouter, Depends, Request
from sqlalchemy.orm import Session

from app.models.database import get_db
from app.models.fittbot_models import DefaultWorkoutTemplates
from app.utils.logging_utils import FittbotHTTPException

router = APIRouter(
    prefix="/default_workout_template",
    tags=["Fittbot Workout Template"],
)


@router.get("/get_default_workout")
async def get_default_workout(
    request: Request,
    gender: str,
    level: str,
    goals: str,
    db: Session = Depends(get_db),
):
    try:
        template = (
            db.query(DefaultWorkoutTemplates)
            .filter(
                DefaultWorkoutTemplates.gender == gender,
                DefaultWorkoutTemplates.expertise_level == level,
                DefaultWorkoutTemplates.goals == goals,
            )
            .first()
        )

        return {
            "status": 200,
            "message": "Data retrieved successfully",
            "data": template.workout_json if template else [],
        }

    except FittbotHTTPException:
        # Already logged in the exception constructor
        raise
    except Exception as e:
        raise FittbotHTTPException(
            status_code=500,
            detail="Unable to retrieve workout template",
            error_code="DEFAULT_WORKOUT_TEMPLATE_FETCH_ERROR",
            log_data={
                "exc": repr(e),
                "gender": gender,
                "level": level,
                "goals": goals,
            },
        )
