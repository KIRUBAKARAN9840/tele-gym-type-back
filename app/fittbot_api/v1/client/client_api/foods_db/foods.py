from typing import List
from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session
from sqlalchemy import func

from app.models.database import get_db
from app.models.fittbot_models import Food
from app.utils.logging_utils import FittbotHTTPException

router = APIRouter(prefix="/food", tags=["Food Catalog"])


@router.get("/get")
async def read_foods(
    page: int = Query(1, gt=0, description="Page number, starting at 1"),
    limit: int = Query(10, gt=0, description="Number of items per page"),
    db: Session = Depends(get_db),
):
    try:
        offset = (page - 1) * limit
        foods = db.query(Food).offset(offset).limit(limit).all()
        return {"status": 200, "data": foods}
    except FittbotHTTPException:
        raise
    except Exception as e:
        raise FittbotHTTPException(
            status_code=500,
            detail=f"An error occurred: {str(e)}",
            error_code="FOODS_LIST_ERROR",
            log_data={"page": page, "limit": limit, "error": str(e)},
        )


@router.get("/get_categories")
async def read_food_categories(
    db: Session = Depends(get_db),
):
    try:
        categories = [category[0] for category in db.query(Food.categories).distinct().all()]
        return {"status": 200, "data": categories}
    except FittbotHTTPException:
        raise
    except Exception as e:
        raise FittbotHTTPException(
            status_code=500,
            detail=f"An error occurred: {str(e)}",
            error_code="FOOD_CATEGORIES_ERROR",
            log_data={"error": str(e)},
        )


@router.get("/search")
async def search_food(
    query: str = Query(..., min_length=3, description="Search query, minimum 3 characters"),
    page: int = Query(1, gt=0, description="Page number, starting at 1"),
    limit: int = Query(10, gt=0, description="Number of items per page"),
    db: Session = Depends(get_db),
):
    try:
        offset = (page - 1) * limit

        startswith_query = db.query(Food).filter(Food.item.ilike(f"{query}%"))
        startswith_count = db.query(func.count()).filter(Food.item.ilike(f"{query}%")).scalar()

        if startswith_count == 0:
            contains_query = db.query(Food).filter(Food.item.ilike(f"%{query}%"))
            total_count = db.query(func.count()).filter(Food.item.ilike(f"%{query}%")).scalar()
            foods = contains_query.offset(offset).limit(limit).all()
        else:
            total_count = startswith_count
            foods = startswith_query.offset(offset).limit(limit).all()

        return {"status": 200, "data": foods, "total": total_count, "page": page, "limit": limit}
    except FittbotHTTPException:
        raise
    except Exception as e:
        raise FittbotHTTPException(
            status_code=500,
            detail=f"An error occurred: {str(e)}",
            error_code="FOOD_SEARCH_ERROR",
            log_data={"query": query, "page": page, "limit": limit, "error": str(e)},
        )


@router.get("/categories")
async def get_foods_by_category(
    categories: str = Query(..., description="Comma-separated list of categories"),
    page: int = Query(1, gt=0, description="Page number, starting at 1"),
    limit: int = Query(10, gt=0, description="Number of items per page"),
    db: Session = Depends(get_db),
):
    try:
        offset = (page - 1) * limit
        category_list: List[str] = [cat.strip() for cat in categories.split(",")]

        query = db.query(Food).filter(Food.categories.in_(category_list))
        total_count = query.count()
        foods = query.offset(offset).limit(limit).all()

        return {
            "status": 200,
            "data": foods,
            "total": total_count,
            "page": page,
            "limit": limit,
            "categories": category_list,
        }
    except FittbotHTTPException:
        raise
    except Exception as e:
        raise FittbotHTTPException(
            status_code=500,
            detail=f"An error occurred: {str(e)}",
            error_code="FOODS_BY_CATEGORY_ERROR",
            log_data={
                "categories": categories,
                "page": page,
                "limit": limit,
                "error": str(e),
            },
        )
