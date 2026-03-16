# app/api/v1/food/consumed_foods.py

from typing import List
from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session
from app.models.database import get_db
from app.models.fittbot_models import Food
from app.utils.logging_utils import FittbotHTTPException

router = APIRouter(prefix="/common_food", tags=["Food - Consumed"])


@router.get("/consumed")
async def get_consumed_foods(db: Session = Depends(get_db)):
    try:
        food_ids: List[int] = [
            6257, 17201, 7, 17206, 20, 1556, 654, 48, 63, 771,
            1239, 10477, 110, 15746
        ]
        foods = db.query(Food).filter(Food.id.in_(food_ids)).all()

        food_list = (
            [
                {
                    "id": food.id,
                    "name": food.item,
                    "calories": food.calories,
                    "protein": food.protein,
                    "carbs": food.carbs,
                    "fat": food.fat,
                    "fiber": food.fiber,
                    "sugar": food.sugar,
                    "quantity": food.quantity,
                    "pic": food.pic,
                    "calcium":food.calcium,
                    "magnesium":food.magnesium,
                    "potassium":food.potassium,
                    "iron":food.iron,
                    "sodium":food.sodium
                }
                for food in foods
            ]
            if foods
            else []
        )
        print("food list",food_list)

        return {
            "status": 200,
            "data": food_list,
            "message": "Food data fetched successfully",
        }
    except FittbotHTTPException:
        raise
    except Exception as e:
        raise FittbotHTTPException(
            status_code=500,
            detail=f"An error occurred: {str(e)}",
            error_code="CONSUMED_FOODS_FETCH_ERROR",
            log_data={"error": str(e)},
        )


@router.get("/search")
async def search_consumed_food(
    query: str = Query(..., min_length=2, description="Search query, minimum 3 characters"),
    db: Session = Depends(get_db),
):
    try:
        query = query.strip()
        offset = 0
        limit = 25

        startswith_query = db.query(Food).filter(Food.item.ilike(f"{query}%"))
        startswith_count = startswith_query.count()

        if startswith_count == 0:
            foods = (
                db.query(Food)
                .filter(Food.item.ilike(f"%{query}%"))
                .offset(offset)
                .limit(limit)
                .all()
            )
        else:
            foods = startswith_query.offset(offset).limit(limit).all()

        food_list = [
            {
                "id": food.id,
                "name": food.item,
                "calories": food.calories,
                "protein": food.protein,
                "carbs": food.carbs,
                "fiber": food.fiber,
                "sugar": food.sugar,
                "fat": food.fat,
                "calcium":food.calcium,
                "magnesium":food.magnesium,
                "potassium":food.potassium,
                "iron":food.iron,
                "sodium":food.sodium,
                "quantity": food.quantity,
                "pic": food.pic,
            }
            for food in foods
        ]

        return {"status": 200, "data": food_list}
    except FittbotHTTPException:
        raise
    except Exception as e:
        raise FittbotHTTPException(
            status_code=500,
            detail=f"An error occurred: {str(e)}",
            error_code="SEARCH_CONSUMED_FOOD_ERROR",
            log_data={"query": query, "error": str(e)},
        )
