# app/routers/gym_workout_template.py

from typing import Dict

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel
from redis.asyncio import Redis
from sqlalchemy.orm import Session

from app.models.database import get_db
from app.models.fittbot_models import FittbotWorkout, TemplateWorkout
from app.utils.logging_utils import FittbotHTTPException
from app.utils.redis_config import get_redis

router = APIRouter(prefix="/gym_workout_template", tags=["Diet Plans"])


@router.get("/get_fittbot_workout")
def get_fittbot_workout(db: Session = Depends(get_db)):
    """
    Return the global Fittbot workout master (muscle groups & exercises).
    """
    try:
        workout_entry = db.query(FittbotWorkout).first()
        if not workout_entry:
            raise FittbotHTTPException(
                status_code=404,
                detail="No workout data found.",
                error_code="WORKOUT_MASTER_NOT_FOUND",
                log_data=None,
            )

        exercise_data = workout_entry.exercise_data or {}
        muscle_groups = list(exercise_data.keys())

        return {
            "status": 200,
            "data": {
                "muscle_groups": muscle_groups,
                "exercise_data": exercise_data,
            },
        }
    except FittbotHTTPException:
        raise
    except Exception as e:
        raise FittbotHTTPException(
            status_code=500,
            detail="Failed to fetch Fittbot workout master",
            error_code="WORKOUT_MASTER_FETCH_ERROR",
            log_data={"error": repr(e)},
        ) from e


class TemplateWorkoutRequest(BaseModel):
    name: str
    workoutPlan: Dict
    gym_id: int


@router.post("/addworkouttemplate")
async def addWorkOutTemplate(
    template: TemplateWorkoutRequest,
    db: Session = Depends(get_db),
    redis: Redis = Depends(get_redis),
):
    """
    Create a workout template for a gym and invalidate relevant caches.
    """
    try:
        new_template = TemplateWorkout(
            name=template.name,
            workoutPlan=template.workoutPlan,
            gym_id=template.gym_id,
        )
        db.add(new_template)
        db.commit()

        # Invalidate caches
        workout_redis_key = f"gym:{template.gym_id}:all_workouts"
        if await redis.exists(workout_redis_key):
            await redis.delete(workout_redis_key)

        client_redis_key = f"gym:{template.gym_id}:all_clients"
        if await redis.exists(client_redis_key):
            await redis.delete(client_redis_key)

        pattern = f"*:{template.gym_id}:assigned_plans"
        keys = await redis.keys(pattern)
        if keys:
            await redis.delete(*keys)

        return {"status": 200, "message": "Template added successfully"}
    except FittbotHTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise FittbotHTTPException(
            status_code=500,
            detail="Error adding template",
            error_code="WORKOUT_TEMPLATE_CREATE_ERROR",
            log_data={"error": repr(e), "gym_id": template.gym_id},
        ) from e


@router.get("/addworkouttemplate")
async def getWorkOutTemplate(
    gym_id: int = Query(...),
    db: Session = Depends(get_db),
):

    try:
        template_rows = (
            db.query(TemplateWorkout).filter(TemplateWorkout.gym_id == gym_id).all()
        )

        temp = []
        for t in template_rows:
            # Extract unique muscle groups from workoutPlan
            muscle_groups = []
            workout_plan = t.workoutPlan or {}

            for _, day_data in workout_plan.items():
                if isinstance(day_data, dict):
                    for muscle_group in day_data.keys():
                        if muscle_group not in muscle_groups:
                            muscle_groups.append(muscle_group)

            temp.append({
                "id": t.id,
                "name": t.name,
                "exercise_data": t.workoutPlan,
                "muscle_group": muscle_groups,
            })


        return {"status": 200, "message": "Templates retrieved successfully", "data": temp}
    except FittbotHTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise FittbotHTTPException(
            status_code=500,
            detail="Error retrieving templates",
            error_code="WORKOUT_TEMPLATE_LIST_ERROR",
            log_data={"error": repr(e), "gym_id": gym_id},
        ) from e


class UpdateWorkoutTemplateRequest(BaseModel):
    id: int
    gym_id: int
    workoutPlan: Dict


class editWorkoutTemplateRequest(BaseModel):
    id: int
    name: str
    gym_id: int


@router.put("/addworkouttemplate")
async def update_workout_template(
    request: UpdateWorkoutTemplateRequest,
    db: Session = Depends(get_db),
    redis: Redis = Depends(get_redis),
):
    """
    Update a workout template's plan JSON.
    """
    try:
        template = db.query(TemplateWorkout).filter(TemplateWorkout.id == request.id).first()
        if not template:
            raise FittbotHTTPException(
                status_code=404,
                detail="Template not found with the given ID",
                error_code="WORKOUT_TEMPLATE_NOT_FOUND",
                log_data={"id": request.id, "gym_id": request.gym_id},
            )

        template.workoutPlan = request.workoutPlan
        db.commit()
        db.refresh(template)

        # Invalidate caches
        workout_redis_key = f"gym:{request.gym_id}:all_workouts"
        if await redis.exists(workout_redis_key):
            await redis.delete(workout_redis_key)

        client_redis_key = f"gym:{request.gym_id}:all_clients"
        if await redis.exists(client_redis_key):
            await redis.delete(client_redis_key)

        pattern = f"*:{request.gym_id}:assigned_plans"
        keys = await redis.keys(pattern)
        if keys:
            await redis.delete(*keys)

        return {"status": 200, "message": "Template updated successfully"}
    except FittbotHTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise FittbotHTTPException(
            status_code=500,
            detail="Error updating template",
            error_code="WORKOUT_TEMPLATE_UPDATE_ERROR",
            log_data={"error": repr(e), "id": request.id, "gym_id": request.gym_id},
        ) from e


@router.delete("/addworkouttemplate")
async def delete_workout_template(
    id: int,
    gym_id: int,
    db: Session = Depends(get_db),
    redis: Redis = Depends(get_redis),
):
    """
    Delete a workout template and clear caches.
    """
    try:
        result = (
            db.query(TemplateWorkout)
            .filter(TemplateWorkout.id == id)
            .delete(synchronize_session=False)
        )
        if result == 0:
            db.rollback()
            raise FittbotHTTPException(
                status_code=404,
                detail="Template not found",
                error_code="WORKOUT_TEMPLATE_NOT_FOUND",
                log_data={"id": id, "gym_id": gym_id},
            )

        db.commit()

        # Invalidate caches
        workout_redis_key = f"gym:{gym_id}:all_workouts"
        if await redis.exists(workout_redis_key):
            await redis.delete(workout_redis_key)

        client_redis_key = f"gym:{gym_id}:all_clients"
        if await redis.exists(client_redis_key):
            await redis.delete(client_redis_key)

        pattern = f"*:{gym_id}:assigned_plans"
        keys = await redis.keys(pattern)
        if keys:
            await redis.delete(*keys)

        return {"status": 200, "message": "Template deleted successfully"}
    except FittbotHTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise FittbotHTTPException(
            status_code=500,
            detail="Error deleting workout template",
            error_code="WORKOUT_TEMPLATE_DELETE_ERROR",
            log_data={"error": repr(e), "id": id, "gym_id": gym_id},
        ) from e


@router.put("/editworkouttemplate")
async def edit_workout_template(
    input: editWorkoutTemplateRequest,  # keep param name for backward compatibility
    db: Session = Depends(get_db),
    redis: Redis = Depends(get_redis),
):
    """
    Rename a workout template and clear caches.
    """
    try:
        result = (
            db.query(TemplateWorkout)
            .filter(TemplateWorkout.id == input.id)
            .update({"name": input.name}, synchronize_session=False)
        )
        if not result:
            db.rollback()
            raise FittbotHTTPException(
                status_code=404,
                detail="Template not found",
                error_code="WORKOUT_TEMPLATE_NOT_FOUND",
                log_data={"id": input.id, "gym_id": input.gym_id},
            )

        db.commit()

        # Invalidate caches
        workout_redis_key = f"gym:{input.gym_id}:all_workouts"
        if await redis.exists(workout_redis_key):
            await redis.delete(workout_redis_key)

        client_redis_key = f"gym:{input.gym_id}:all_clients"
        if await redis.exists(client_redis_key):
            await redis.delete(client_redis_key)

        pattern = f"*:{input.gym_id}:assigned_plans"
        keys = await redis.keys(pattern)
        if keys:
            await redis.delete(*keys)

        return {"status": 200, "message": "Template updated successfully"}
    except FittbotHTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise FittbotHTTPException(
            status_code=500,
            detail="An unexpected error occurred while renaming template",
            error_code="WORKOUT_TEMPLATE_RENAME_ERROR",
            log_data={"error": repr(e), "id": input.id, "gym_id": input.gym_id},
        ) from e
