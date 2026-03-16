# app/api/v1/workouts/workout_templates_manual.py

from typing import Dict, Optional
from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel
from sqlalchemy.orm import Session

from app.models.database import get_db
from app.models.fittbot_models import ClientWorkoutTemplate
from app.utils.logging_utils import FittbotHTTPException

router = APIRouter(prefix="/personal_template", tags=["template"])


# ---------- Schemas ----------
class AddWorkoutTemplateRequest(BaseModel):
    client_id: int
    template_name: str
    exercise_data: Dict[str, list]


class UpdateWorkoutTemplateRequest(BaseModel):
    id: int
    exercise_data: Dict


class EditWorkoutTemplateNameRequest(BaseModel):
    id: int
    template_name: str


# ---------- Endpoints ----------
@router.get("/get")
async def get_client_workout_templates(
    client_id: int = Query(...),
    db: Session = Depends(get_db),
):
    """
    List templates for a client.
    """
    try:
        templates = (
            db.query(ClientWorkoutTemplate)
            .filter(ClientWorkoutTemplate.client_id == client_id)
            .all()
        )

        temp = [
            {
                "id": template.id,
                "name": template.template_name,
                "exercise_data": template.exercise_data,
            }
            for template in templates
        ]

        print("Temppppp is", temp)

        return {"status": 200, "message": "Template listed successfully", "data": temp}

    except FittbotHTTPException:
        raise
    except Exception as e:
        db.rollback()
        print(f"Error listing template: {str(e)}")
        raise FittbotHTTPException(
            status_code=500,
            detail="Error listing template",
            error_code="TEMPLATE_LIST_ERROR",
            log_data={"client_id": client_id, "error": str(e)},
        )


@router.post("/add")
async def add_workout_template(
    request: AddWorkoutTemplateRequest,
    db: Session = Depends(get_db),
):
    """
    Add a new workout template for a client.
    """
    # Duplicate name check (same logic)
    existing = (
        db.query(ClientWorkoutTemplate)
        .filter(
            ClientWorkoutTemplate.client_id == request.client_id,
            ClientWorkoutTemplate.template_name == request.template_name,
        )
        .first()
    )
    if existing:
        raise FittbotHTTPException(
            status_code=400,
            detail=f"Template name '{request.template_name}' is already there",
            error_code="WORKOUT_TEMPLATE_DUPLICATE",
            log_data={
                "client_id": request.client_id,
                "template_name": request.template_name,
            },
        )

    try:
        new_template = ClientWorkoutTemplate(
            client_id=request.client_id,
            template_name=request.template_name,
            exercise_data=request.exercise_data,
        )
        db.add(new_template)
        db.commit()
        db.refresh(new_template)

        return {
            "status": 200,
            "message": "Workout template added successfully",
            "data": {
                "id": new_template.id,
                "name": new_template.template_name,
                "exercise_data": new_template.exercise_data,
                "client_id": new_template.client_id,
            },
        }

    except FittbotHTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise FittbotHTTPException(
            status_code=500,
            detail=f"Error adding workout template: {str(e)}",
            error_code="WORKOUT_TEMPLATE_ADD_ERROR",
            log_data={
                "client_id": request.client_id,
                "template_name": request.template_name,
                "error": str(e),
            },
        )


@router.put("/edit")
async def edit_workout_template(
    request: UpdateWorkoutTemplateRequest,
    db: Session = Depends(get_db),
):
    """
    Update exercise_data for a template by ID.
    """
    try:
        template = (
            db.query(ClientWorkoutTemplate)
            .filter(ClientWorkoutTemplate.id == request.id)
            .first()
        )
        if not template:
            raise FittbotHTTPException(
                status_code=404,
                detail="Template not found with the given ID",
                error_code="WORKOUT_TEMPLATE_NOT_FOUND",
                log_data={"id": request.id},
            )

        template.exercise_data = request.exercise_data
        db.commit()
        db.refresh(template)

        return {"status": 200, "message": "Exercise updated successfully"}

    except FittbotHTTPException:
        db.rollback()
        raise
    except Exception as e:
        db.rollback()
        raise FittbotHTTPException(
            status_code=500,
            detail=f"Error updating workout template: {str(e)}",
            error_code="WORKOUT_TEMPLATE_UPDATE_ERROR",
            log_data={"id": request.id, "error": str(e)},
        )


@router.put("/update")
async def update_workout_template_name(
    request: EditWorkoutTemplateNameRequest,
    db: Session = Depends(get_db),
):
    """
    Edit template name by ID.
    """
    try:
        result = (
            db.query(ClientWorkoutTemplate)
            .filter(ClientWorkoutTemplate.id == request.id)
            .update({"template_name": request.template_name})
        )
        db.commit()

        if not result:
            raise FittbotHTTPException(
                status_code=404,
                detail="Template not found",
                error_code="WORKOUT_TEMPLATE_NOT_FOUND",
                log_data={"id": request.id, "template_name": request.template_name},
            )

        return {"status": 200, "message": "Workout template name updated successfully"}

    except FittbotHTTPException:
        db.rollback()
        raise
    except Exception as e:
        db.rollback()
        raise FittbotHTTPException(
            status_code=500,
            detail=f"Error updating workout template name: {str(e)}",
            error_code="WORKOUT_TEMPLATE_RENAME_ERROR",
            log_data={"id": request.id, "template_name": request.template_name, "error": str(e)},
        )


@router.delete("/delete")
async def delete_workout_template(
    id: int,
    db: Session = Depends(get_db),
):
    """
    Delete a template by ID.
    """
    try:
        result = (
            db.query(ClientWorkoutTemplate)
            .filter(ClientWorkoutTemplate.id == id)
            .delete()
        )
        db.commit()

        if result == 0:
            raise FittbotHTTPException(
                status_code=404,
                detail="Template not found with the given ID",
                error_code="WORKOUT_TEMPLATE_NOT_FOUND",
                log_data={"id": id},
            )

        return {"status": 200, "message": "Workout template deleted successfully"}

    except FittbotHTTPException:
        db.rollback()
        raise
    except Exception as e:
        db.rollback()
        raise FittbotHTTPException(
            status_code=500,
            detail=f"Error deleting workout template: {str(e)}",
            error_code="WORKOUT_TEMPLATE_DELETE_ERROR",
            log_data={"id": id, "error": str(e)},
        )
