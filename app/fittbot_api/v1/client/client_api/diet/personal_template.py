# app/api/v1/diets/diet_templates.py

from typing import Dict, List
from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel
from sqlalchemy.orm import Session

from app.models.database import get_db
from app.models.fittbot_models import (
    ClientDietTemplate,
    ClientScheduler,
    TemplateDiet,
)
from app.utils.logging_utils import FittbotHTTPException

router = APIRouter(prefix="/diet_personal_template", tags=["Client Tokens"])

def _get_nutrient_value(food_item: dict, *keys: str) -> float:
                """Return the first matching nutrient value, handling legacy naming/casing."""
                for key in keys:
                    value = food_item.get(key)
                    if value not in (None, ""):
                        return value or 0
                return 0

# ---------- Helper Functions ----------
def calculate_template_totals(diet_data: List) -> Dict:
    """Calculate consolidated nutrition totals from diet_data"""
    totals = {
        "calories": 0,
        "protein": 0,
        "carbs": 0,
        "fats": 0,
        "fiber": 0,
        "sugar": 0,
        "calcium":0,
        "magnesium":0,
        "iron":0,
        "sodium":0,
        "potassium":0
    }

    if not diet_data or not isinstance(diet_data, list):
        return totals

    # Iterate through each meal in diet_data
    for meal in diet_data:
        if not isinstance(meal, dict):
            continue

        food_list = meal.get("foodList", [])
        if not isinstance(food_list, list):
            continue

        # Sum up nutrition from each food item
        for food_item in food_list:
            if not isinstance(food_item, dict):
                continue

            totals["calories"] += food_item.get("calories", 0) or 0
            totals["protein"] += food_item.get("protein", 0) or 0
            totals["carbs"] += food_item.get("carbs", 0) or 0
            totals["fats"] += food_item.get("fat", 0) or 0  # Note: could be "fat" or "fats"
            totals["fiber"] += food_item.get("fiber", 0) or 0
            totals["sugar"] += food_item.get("sugar", 0) or 0
            totals["calcium"] += _get_nutrient_value(food_item, "calcium")
            totals["magnesium"] += _get_nutrient_value(food_item, "magnesium")
            totals["sodium"] += _get_nutrient_value(food_item, "sodium")
            totals["potassium"] += _get_nutrient_value(food_item, "potassium")
            totals["iron"] += _get_nutrient_value(food_item, "iron")
# Round to 1 decimal place
    for key in totals:
            totals[key] = round(totals[key])

    print("total",totals)

    return totals


# ---------- Schemas ----------
class AddDietTemplateRequest(BaseModel):
    client_id: int
    template_name: str
    diet_data: List  # keep as list to match your current logic


class UpdateDietTemplateRequest(BaseModel):
    id: int
    diet_data: List


class EditDietTemplateNameRequest(BaseModel):
    id: int
    template_name: str


# ---------- Endpoints ----------
@router.get("/get")
async def get_client_diet_templates(
    method: str,
    client_id: int = Query(...),
    db: Session = Depends(get_db),
):
    try:
        if method == "personal":
            templates = (
                db.query(ClientDietTemplate)
                .filter(ClientDietTemplate.client_id == client_id)
                .all()
            )

            temp = [
                {
                    "id": t.id,
                    "name": t.template_name,
                    "diet_data": t.diet_data,
                    "nutrition_totals": calculate_template_totals(t.diet_data),
                }
                for t in templates
            ]

            print("temp is",temp)

            return {
                "status": 200,
                "message": "Template listed successfully",
                "data": temp,
            }

        elif method == "gym":
            client_scheduler = (
                db.query(ClientScheduler)
                .filter(ClientScheduler.client_id == client_id)
                .first()
            )
            if not client_scheduler or client_scheduler.assigned_dietplan is None:
                return {
                    "status": 200,
                    "message": "No diet has been assigned",
                    "data": [],
                }

            diet_plan = (
                db.query(TemplateDiet)
                .filter(
                    TemplateDiet.template_id
                    == client_scheduler.assigned_dietplan
                )
                .first()
            )
            if not diet_plan or not isinstance(diet_plan.template_details, dict):
                return {
                    "status": 200,
                    "message": "Assigned diet plan not found or invalid format",
                    "data": [],
                }

            output = [
                {
                    "id": idx,
                    "name": plan_name,
                    "diet_data": plan_sections,
                    "nutrition_totals": calculate_template_totals(plan_sections),
                }
                for idx, (plan_name, plan_sections) in enumerate(
                    diet_plan.template_details.items(), start=1
                )
            ]

            return {
                "status": 200,
                "message": "Template listed successfully",
                "data": output,
            }

    except FittbotHTTPException:
        # Pass through structured errors unchanged
        raise
    except Exception as e:
        db.rollback()
        # Keep your behavior (500) & message semantics; just normalize to FittbotHTTPException
        raise FittbotHTTPException(
            status_code=500,
            detail=f"Error listing template: {str(e)}",
            error_code="DIET_TEMPLATE_LIST_ERROR",
            log_data={"method": method, "client_id": client_id, "error": str(e)},
        )


@router.get("/get_single_diet_template")
async def get_single_template(id: int, db: Session = Depends(get_db)):
    try:
        template = (
            db.query(ClientDietTemplate)
            .filter(ClientDietTemplate.id == id)
            .first()
        )

        # Keep your original logic: if None, accessing attributes will raise -> caught below
        template_data = {
            "id": template.id,
            "name": template.template_name,
            "diet_data": template.diet_data,
        }
        return {
            "status": 200,
            "message": "Template retrived successfully",
            "data": template_data,
        }

    except FittbotHTTPException:
        raise
    except Exception as e:
        raise FittbotHTTPException(
            status_code=500,
            detail=f"An error Occured : {str(e)}",
            error_code="DIET_TEMPLATE_SINGLE_FETCH_ERROR",
            log_data={"id": id, "error": str(e)},
        )


@router.post("/add")
async def add_diet_template(
    request: AddDietTemplateRequest,
    db: Session = Depends(get_db),
):
    # Duplicate name check (same logic; just use FittbotHTTPException)
    existing = (
        db.query(ClientDietTemplate)
        .filter(
            ClientDietTemplate.client_id == request.client_id,
            ClientDietTemplate.template_name == request.template_name,
        )
        .first()
    )
    if existing:
        raise FittbotHTTPException(
            status_code=400,
            detail=f"Template name '{request.template_name}' is already there",
            error_code="DIET_TEMPLATE_DUPLICATE",
            log_data={
                "client_id": request.client_id,
                "template_name": request.template_name,
            },
        )

    try:
        new_template = ClientDietTemplate(
            client_id=request.client_id,
            template_name=request.template_name,
            diet_data=request.diet_data,
        )
        db.add(new_template)
        db.commit()
     
        data = {
            "id": new_template.id,
            "client_id": new_template.client_id,
            "template_name": new_template.template_name,
            "diet_data": new_template.diet_data,
        }
        return {
            "status": 200,
            "message": "Diet template added successfully",
            "data": data,
        }

    except FittbotHTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise FittbotHTTPException(
            status_code=500,
            detail=f"Error adding diet template: {str(e)}",
            error_code="DIET_TEMPLATE_ADD_ERROR",
            log_data={
                "client_id": request.client_id,
                "template_name": request.template_name,
                "error": str(e),
            },
        )


@router.put("/edit")
async def edit_diet_template(
    request: UpdateDietTemplateRequest, db: Session = Depends(get_db)
):
    try:

        print("request.diet_data",request.diet_data)
        template = (
            db.query(ClientDietTemplate)
            .filter(ClientDietTemplate.id == request.id)
            .first()
        )
        if not template:
            raise FittbotHTTPException(
                status_code=404,
                detail="Template not found with the given ID",
                error_code="DIET_TEMPLATE_NOT_FOUND",
                log_data={"id": request.id},
            )

        template.diet_data = request.diet_data
        db.commit()
        db.refresh(template)

        return {"status": 200, "message": "Diet template updated successfully"}

    except FittbotHTTPException:
        db.rollback()
        raise
    except Exception as e:
        db.rollback()
        raise FittbotHTTPException(
            status_code=500,
            detail=f"Error updating diet template: {str(e)}",
            error_code="DIET_TEMPLATE_UPDATE_ERROR",
            log_data={"id": request.id, "error": str(e)},
        )


@router.put("/update")
async def update_diet_template_name(
    request: EditDietTemplateNameRequest, db: Session = Depends(get_db)
):
    try:
        result = (
            db.query(ClientDietTemplate)
            .filter(ClientDietTemplate.id == request.id)
            .update({"template_name": request.template_name})
        )
        db.commit()

        if not result:
            raise FittbotHTTPException(
                status_code=404,
                detail="Template not found",
                error_code="DIET_TEMPLATE_NOT_FOUND",
                log_data={"id": request.id, "template_name": request.template_name},
            )

        return {"status": 200, "message": "Diet template name updated successfully"}

    except FittbotHTTPException:
        db.rollback()
        raise
    except Exception as e:
        db.rollback()
        raise FittbotHTTPException(
            status_code=500,
            detail=f"Error updating diet template name: {str(e)}",
            error_code="DIET_TEMPLATE_RENAME_ERROR",
            log_data={
                "id": request.id,
                "template_name": request.template_name,
                "error": str(e)},
        )


@router.delete("/delete")
async def delete_diet_template(id: int, db: Session = Depends(get_db)):
    try:
        result = (
            db.query(ClientDietTemplate)
            .filter(ClientDietTemplate.id == id)
            .delete()
        )
        db.commit()

        if result == 0:
            raise FittbotHTTPException(
                status_code=404,
                detail="Template not found with the given ID",
                error_code="DIET_TEMPLATE_NOT_FOUND",
                log_data={"id": id},
            )

        return {"status": 200, "message": "Diet template deleted successfully"}

    except FittbotHTTPException:
        db.rollback()
        raise
    
    except Exception as e:
        db.rollback()
        raise FittbotHTTPException(
            status_code=500,
            detail=f"Error deleting diet template: {str(e)}",
            error_code="DIET_TEMPLATE_DELETE_ERROR",
            log_data={"id": id, "error": str(e)},
        )
