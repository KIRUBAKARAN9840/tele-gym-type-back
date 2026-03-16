# app/routers/gym_diet_template.py

from typing import Optional, Any, Dict

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from pydantic import BaseModel
from redis.asyncio import Redis

from app.models.database import get_db
from app.models.fittbot_models import TemplateDiet
from app.utils.redis_config import get_redis
from app.utils.logging_utils import FittbotHTTPException

router = APIRouter(prefix="/gym_diet_template", tags=["Diet Plans"])


class TemplateDietRequest(BaseModel):
    name: str
    dietPlan: dict
    gym_id: int


@router.post("/add_diet_template")
async def add_diet_template(
    template: TemplateDietRequest,
    db: Session = Depends(get_db),
    redis: Redis = Depends(get_redis),
):
    """
    Create a new diet template for a gym and invalidate related caches.
    """
    try:
        # Basic validation (keep original logic intact)
        if not template.name or not isinstance(template.dietPlan, dict):
            raise FittbotHTTPException(
                status_code=400,
                detail="Invalid template payload",
                error_code="INVALID_TEMPLATE_PAYLOAD",
                log_data={"gym_id": template.gym_id},
            )

        new_template = TemplateDiet(
            template_name=template.name,
            template_details=template.dietPlan,
            gym_id=template.gym_id,
        )
        db.add(new_template)
        db.commit()

        # Invalidate caches (best-effort)
        diet_redis_key = f"gym:{template.gym_id}:all_diets"
        if await redis.exists(diet_redis_key):
            await redis.delete(diet_redis_key)

        client_redis_key = f"gym:{template.gym_id}:all_clients"
        if await redis.exists(client_redis_key):
            await redis.delete(client_redis_key)

        pattern = f"*:{template.gym_id}:assigned_plans"
        keys = await redis.keys(pattern)
        if keys:
            await redis.delete(*keys)

        return {"status": 200, "message": "Diet template added successfully"}

    except FittbotHTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise FittbotHTTPException(
            status_code=500,
            detail="Error adding diet template",
            error_code="DIET_TEMPLATE_ADD_ERROR",
            log_data={"error": repr(e), "gym_id": template.gym_id},
        ) from e


@router.get("/get_diet_template")
async def getDietTemplate(gym_id: int, db: Session = Depends(get_db)):
    """
    Return all diet templates for a gym.
    """
    try:
        template_names = (
            db.query(TemplateDiet).filter(TemplateDiet.gym_id == gym_id).all()
        )

        temp = [
            {
                "id": template.template_id,
                "name": template.template_name,
                "dietPlan": template.template_details,
            }
            for template in template_names
        ]

        return {
            "status": 200,
            "message": "Templates retrieved successfully",
            "data": temp,
        }

    except FittbotHTTPException:
        raise
    except Exception as e:
        raise FittbotHTTPException(
            status_code=500,
            detail="Error retrieving templates",
            error_code="DIET_TEMPLATE_FETCH_ERROR",
            log_data={"error": repr(e), "gym_id": gym_id},
        ) from e


@router.get("/get_single_diettemplate")
async def getSingleDietTemplate(
    gym_id: int, template_id: int, db: Session = Depends(get_db)
):
    """
    Return a single diet template by id for a gym.
    """
    try:
        template = (
            db.query(TemplateDiet)
            .filter(
                TemplateDiet.gym_id == gym_id,
                TemplateDiet.template_id == template_id,
            )
            .first()
        )

        if not template:
            raise FittbotHTTPException(
                status_code=404,
                detail="Template not found",
                error_code="DIET_TEMPLATE_NOT_FOUND",
                log_data={"gym_id": gym_id, "template_id": template_id},
            )

        temp = {
            "id": template.template_id,
            "name": template.template_name,
            "dietPlan": template.template_details,
        }

        return {
            "status": 200,
            "message": "Templates retrieved successfully",
            "data": temp,
        }

    except FittbotHTTPException:
        raise
    except Exception as e:
        raise FittbotHTTPException(
            status_code=500,
            detail="Error retrieving template",
            error_code="DIET_TEMPLATE_SINGLE_FETCH_ERROR",
            log_data={"error": repr(e), "gym_id": gym_id, "template_id": template_id},
        ) from e


class UpdateDietTemplateRequest(BaseModel):
    id: int
    dietPlan: dict
    gym_id: int


@router.put("/update_diet_template")
async def update_diet_template(
    request: UpdateDietTemplateRequest,
    db: Session = Depends(get_db),
    redis: Redis = Depends(get_redis),
):
    """
    Update a diet template's plan and invalidate related caches.
    """
    try:
        template = (
            db.query(TemplateDiet).filter(TemplateDiet.template_id == request.id).first()
        )
        if not template:
            raise FittbotHTTPException(
                status_code=404,
                detail="Template not found with the given ID",
                error_code="DIET_TEMPLATE_NOT_FOUND",
                log_data={"template_id": request.id, "gym_id": request.gym_id},
            )

        if request.dietPlan is not None:
            template.template_details = request.dietPlan

        db.commit()
        db.refresh(template)

        # Invalidate caches
        diet_redis_key = f"gym:{request.gym_id}:all_diets"
        if await redis.exists(diet_redis_key):
            await redis.delete(diet_redis_key)

        client_redis_key = f"gym:{request.gym_id}:all_clients"
        if await redis.exists(client_redis_key):
            await redis.delete(client_redis_key)

        pattern = f"*:{request.gym_id}:assigned_plans"
        keys = await redis.keys(pattern)
        if keys:
            await redis.delete(*keys)

        return {"status": 200, "message": "Diet template updated successfully"}

    except FittbotHTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise FittbotHTTPException(
            status_code=500,
            detail="Error updating diet template",
            error_code="DIET_TEMPLATE_UPDATE_ERROR",
            log_data={"error": repr(e), "template_id": request.id, "gym_id": request.gym_id},
        ) from e


@router.delete("/delete_diet_template")
async def delete_diet_template(
    id: int,
    gym_id: int,
    db: Session = Depends(get_db),
    redis: Redis = Depends(get_redis),
):
    """
    Delete a diet template and invalidate related caches.
    """
    try:
        result = (
            db.query(TemplateDiet).filter(TemplateDiet.template_id == id).delete()
        )
        db.commit()

        # invalidate caches (best-effort)
        diet_redis_key = f"gym:{gym_id}:all_diets"
        if await redis.exists(diet_redis_key):
            await redis.delete(diet_redis_key)

        client_redis_key = f"gym:{gym_id}:all_clients"
        if await redis.exists(client_redis_key):
            await redis.delete(client_redis_key)

        pattern = f"*:{gym_id}:assigned_plans"
        keys = await redis.keys(pattern)
        if keys:
            await redis.delete(*keys)

        if result == 0:
            raise FittbotHTTPException(
                status_code=404,
                detail="Template not found with the given ID",
                error_code="DIET_TEMPLATE_NOT_FOUND",
                log_data={"template_id": id, "gym_id": gym_id},
            )

        return {"status": 200, "message": "Diet template deleted successfully"}

    except FittbotHTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise FittbotHTTPException(
            status_code=500,
            detail="Error deleting diet template",
            error_code="DIET_TEMPLATE_DELETE_ERROR",
            log_data={"error": repr(e), "template_id": id, "gym_id": gym_id},
        ) from e


class EditDietTemplateRequest(BaseModel):
    id: int
    name: str
    gym_id: int


@router.put("/edit_diet_template")
async def edit_diet_template(
    input: EditDietTemplateRequest,
    db: Session = Depends(get_db),
    redis: Redis = Depends(get_redis),
):
    """
    Edit a diet template's name and invalidate related caches.
    """
    try:
        result = (
            db.query(TemplateDiet)
            .filter(TemplateDiet.template_id == input.id)
            .update({"template_name": input.name})
        )
        db.commit()

        if not result:
            raise FittbotHTTPException(
                status_code=404,
                detail="Template not found",
                error_code="DIET_TEMPLATE_NOT_FOUND",
                log_data={"template_id": input.id, "gym_id": input.gym_id},
            )

        # invalidate caches
        diet_redis_key = f"gym:{input.gym_id}:all_diets"
        if await redis.exists(diet_redis_key):
            await redis.delete(diet_redis_key)

        client_redis_key = f"gym:{input.gym_id}:all_clients"
        if await redis.exists(client_redis_key):
            await redis.delete(client_redis_key)

        pattern = f"*:{input.gym_id}:assigned_plans"
        keys = await redis.keys(pattern)
        if keys:
            await redis.delete(*keys)

        return {"status": 200, "message": "Diet template name updated successfully"}

    except FittbotHTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise FittbotHTTPException(
            status_code=500,
            detail="Error updating diet template name",
            error_code="DIET_TEMPLATE_NAME_UPDATE_ERROR",
            log_data={"error": repr(e), "template_id": input.id, "gym_id": input.gym_id},
        ) from e
