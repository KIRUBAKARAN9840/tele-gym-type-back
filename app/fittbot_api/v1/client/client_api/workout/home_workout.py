# app/api/v1/workouts/home_workout.py

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from app.models.database import get_db
from app.models.fittbot_models import HomeWorkout, Client
from app.utils.logging_utils import FittbotHTTPException

router = APIRouter(prefix="/home_workout", tags=["template"])


@router.get("/get")
async def get_home_workout(
    client_id: int,
    db: Session = Depends(get_db),
):

    try:
        
        workout_entry = db.query(HomeWorkout).first()
        client = db.query(Client).filter(Client.client_id == client_id).first()

        if not workout_entry:
            raise FittbotHTTPException(
                status_code=404,
                detail="No home workout data found.",
                error_code="HOME_WORKOUT_NOT_FOUND",
                log_data={"client_id": client_id},
            )

        exercise_data = workout_entry.home_workout
        muscle_groups = list(exercise_data.keys())

        return {
            "status": 200,
            "data": {
                "muscle_groups": muscle_groups,
                "exercise_data": exercise_data,
                "client_weight": client.weight,  
            },
        }

    except FittbotHTTPException:
    
        raise
    except Exception as e:

        try:
            db.rollback()
        except Exception:
            pass
        raise FittbotHTTPException(
            status_code=500,
            detail="Failed to fetch home workout",
            error_code="HOME_WORKOUT_ERROR",
            log_data={"client_id": client_id, "error": str(e)},
        )
