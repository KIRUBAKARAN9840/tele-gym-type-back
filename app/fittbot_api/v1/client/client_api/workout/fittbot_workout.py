# app/api/v1/workouts/fittbot_workout.py

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from app.models.database import get_db
from app.models.fittbot_models import FittbotWorkout, Client
from app.utils.logging_utils import FittbotHTTPException

router = APIRouter(prefix="/fittbot_workout", tags=["template"])


@router.get("/get")
async def get_fittbot_workout(
    client_id: int,
    db: Session = Depends(get_db),
):
    """
    Returns:
      - muscle_groups: list of keys from exercise_data
      - exercise_data: raw exercise JSON from FittbotWorkout
      - client_weight: weight from Client table for the given client_id
    """
    try:
        workout_entry = db.query(FittbotWorkout).first()
        client = db.query(Client).filter(Client.client_id == client_id).first()

        if not workout_entry:
            raise FittbotHTTPException(
                status_code=404,
                detail="No workout data found.",
                error_code="FITT_BOT_WORKOUT_NOT_FOUND",
                log_data={"client_id": client_id},
            )

        exercise_data = workout_entry.exercise_data
        muscle_groups = list(exercise_data.keys())

        return {
            "status": 200,
            "data": {
                "muscle_groups": muscle_groups,
                "exercise_data": exercise_data,
                "client_weight": client.weight,  # same logic as provided
            },
        }

    except FittbotHTTPException:
        # Pass through known, structured errors.
        raise
    except Exception as e:
        # For any unexpected issue (e.g., missing client causing attribute error), normalize the error.
        # Keeping original logic—no additional guards added.
        try:
            db.rollback()
        except Exception:
            pass
        raise FittbotHTTPException(
            status_code=500,
            detail="Failed to fetch Fittbot workout",
            error_code="FITT_BOT_WORKOUT_ERROR",
            log_data={"client_id": client_id, "error": str(e)},
        )
