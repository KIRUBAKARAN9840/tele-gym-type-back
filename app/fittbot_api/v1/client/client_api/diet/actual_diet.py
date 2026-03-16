# app/api/v1/diets/actual_diet.py

from typing import List, Dict, Optional
from datetime import datetime, date, timedelta
from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy import asc
from sqlalchemy.orm import Session
from sqlalchemy.orm.attributes import flag_modified
from redis.asyncio import Redis

from app.utils.redis_config import get_redis
from app.models.database import get_db
from app.models.fittbot_models import (
    ActualDiet,
    ClientActual,
    ClientTarget,
    CalorieEvent,
    LeaderboardDaily,
    LeaderboardMonthly,
    LeaderboardOverall,
    Client,
    ClientNextXp,
    RewardGym,
    RewardPrizeHistory,
)
from app.utils.logging_utils import FittbotHTTPException
from app.fittbot_api.v1.client.client_api.side_bar.ratings import check_feedback_status

router = APIRouter(prefix="/actual_diet", tags=["diet"])

async def delete_keys_by_pattern(redis: Redis, pattern: str) -> None:
    keys = await redis.keys(pattern)
    if keys:
        print("keys are there deleting",keys)
        await redis.delete(*keys)

# -------------------- Schemas (unchanged) --------------------
class DietInput(BaseModel):
    client_id: int
    date: date
    diet_data: list
    gym_id: Optional[int] = None


class DieteditInput(BaseModel):
    record_id: int
    date: date
    client_id: int
    diet_data: list
    gym_id: Optional[int] = None




# -------------------- Helpers --------------------
def calculate_totals(diet_data: list) -> dict:
    """Calculate totals from diet data - supports both old format and new template format"""
    total_calories = 0
    total_protein = 0
    total_carbs = 0
    total_fats = 0
    total_fiber = 0
    total_sugar = 0
    total_calcium = 0
    total_magnesium = 0
    total_potassium = 0
    total_Iodine = 0
    total_Iron = 0

    for item in diet_data:
        # New template format (with meal categories)
        if isinstance(item, dict) and "foodList" in item:
            food_list = item.get("foodList", [])
            for food_item in food_list:
                total_calories += food_item.get("calories", 0) or 0
                total_protein += food_item.get("protein", 0) or 0
                total_carbs += food_item.get("carbs", 0) or 0
                total_fats += food_item.get("fat", 0) or 0
                total_fiber += food_item.get("fiber", 0) or 0
                total_sugar += food_item.get("sugar", 0) or 0
                total_calcium += food_item.get("calcium", 0) or 0
                total_magnesium += food_item.get("magnesium", 0) or 0
                total_potassium += food_item.get("potassium", 0) or 0
                total_Iodine += food_item.get("Iodine", 0) or 0
                total_Iron += food_item.get("Iron", 0) or 0
        else:
            # Legacy flat items
            total_calories += item.get("calories", 0) or 0
            total_protein += item.get("protein", 0) or 0
            total_carbs += item.get("carbs", 0) or 0
            total_fats += item.get("fat", 0) or 0
            total_fiber += item.get("fiber", 0) or 0
            total_sugar += item.get("sugar", 0) or 0
            total_calcium += item.get("calcium", 0) or 0
            total_magnesium += item.get("magnesium", 0) or 0
            total_potassium += item.get("potassium", 0) or 0
            total_Iodine += item.get("Iodine", 0) or 0
            total_Iron += item.get("Iron", 0) or 0

    return {
        "calories": total_calories,
        "protein": total_protein,
        "carbs": total_carbs,
        "fats": total_fats,
        "fiber": total_fiber,
        "sugar": total_sugar,
        "calcium": total_calcium,
        "magnesium": total_magnesium,
        "potassium": total_potassium,
        "Iodine": total_Iodine,
        "Iron": total_Iron
    }


def filter_meals_by_type(meals: list, meal_types: list = None) -> list:
    """Filter meals by selected meal types"""
    if not meal_types or len(meal_types) == 0:
        return meals

    filtered_meals = []
    for meal in meals:
        if isinstance(meal, dict):
            meal_title = meal.get("title", "").lower()
            for meal_type in meal_types:
                if meal_type.lower() in meal_title:
                    filtered_meals.append(meal)
                    break
    return filtered_meals


def _get_nutrient_value(food_item: dict, *keys: str) -> float:
    """Return the first matching nutrient value, handling legacy naming/casing."""
    for key in keys:
        value = food_item.get(key)
        if value not in (None, ""):
            return value or 0
    return 0


def _target_value(client_target, attr: str) -> float:
    if not client_target:
        return 0
    value = getattr(client_target, attr, 0)
    return value or 0

# -------- Template helpers (NEW) --------
def _default_template() -> list:
    return [
        {"id": "1", "title": "Pre workout", "tagline": "Energy boost", "foodList": [], "timeRange": "6:30-7:00 AM", "itemsCount": 0},
        {"id": "2", "title": "Post workout", "tagline": "Recovery fuel", "foodList": [], "timeRange": "7:30-8:00 AM", "itemsCount": 0},
        {"id": "3", "title": "Early morning Detox", "tagline": "Early morning nutrition", "foodList": [], "timeRange": "5:30-6:00 AM", "itemsCount": 0},
        {"id": "4", "title": "Pre-Breakfast / Pre-Meal Starter", "tagline": "Pre-breakfast fuel", "foodList": [], "timeRange": "7:00-7:30 AM", "itemsCount": 0},
        {"id": "5", "title": "Breakfast", "tagline": "Start your day right", "foodList": [], "timeRange": "8:30-9:30 AM", "itemsCount": 0},
        {"id": "6", "title": "Mid-Morning snack", "tagline": "Healthy meal", "foodList": [], "timeRange": "10:00-11:00 AM", "itemsCount": 0},
        {"id": "7", "title": "Lunch", "tagline": "Nutritious midday meal", "foodList": [], "timeRange": "1:00-2:00 PM", "itemsCount": 0},
        {"id": "8", "title": "Evening snack", "tagline": "Healthy meal", "foodList": [], "timeRange": "4:00-5:00 PM", "itemsCount": 0},
        {"id": "9", "title": "Dinner", "tagline": "End your day well", "foodList": [], "timeRange": "7:30-8:30 PM", "itemsCount": 0},
        {"id": "10", "title": "Bed time", "tagline": "Rest well", "foodList": [], "timeRange": "9:30-10:00 PM", "itemsCount": 0}
    ]


def _is_template_format(d: list) -> bool:
    return isinstance(d, list) and len(d) > 0 and isinstance(d[0], dict) and "foodList" in d[0]


def _find_meal(meals: list, target: dict) -> Optional[dict]:
    # Match by id first, else by title
    for m in meals:
        # Ensure consistent string comparison for IDs to prevent type issues
        m_id = str(m.get("id", "")) if m.get("id") is not None else ""
        target_id = str(target.get("id", "")) if target.get("id") is not None else ""

        print(f"Comparing meal IDs: '{m_id}' == '{target_id}' (types: {type(m.get('id'))}, {type(target.get('id'))})")

        if (m_id and target_id and m_id == target_id) or \
           (m.get("title") and target.get("title") and m["title"] == target["title"]):
            return m
    return None


def _expand_to_full_template_with(incoming_template: list) -> list:
    """Start with full default; overlay incoming meal's foodList/itemsCount into the matching category."""
    full = _default_template()
    for inc_meal in incoming_template:
        dest = _find_meal(full, inc_meal)
        if dest is None:
            # Ensure id is always a string to prevent type concatenation errors
            meal_id = inc_meal.get("id")
            id_value = str(meal_id) if meal_id is not None else str(len(full) + 1)
            print(f"Processing meal ID: {meal_id} -> {id_value} (type: {type(id_value)})")

            full.append({
                "id": id_value,
                "title": inc_meal.get("title", "Custom"),
                "tagline": inc_meal.get("tagline", ""),
                "foodList": inc_meal.get("foodList", []),
                "timeRange": inc_meal.get("timeRange", ""),
                "itemsCount": len(inc_meal.get("foodList", [])),
            })
        else:
            dest["foodList"] = inc_meal.get("foodList", [])
            dest["itemsCount"] = len(dest["foodList"])
    return full


def _merge_templates(existing: list, incoming: list) -> list:
    """
    Append incoming foods to matching categories; allow duplicate food items.
    Generate unique IDs for each food item to avoid React key conflicts.
    """
    import time
    import random

    for in_meal in incoming:
        ex_meal = _find_meal(existing, in_meal)
        if not ex_meal:
            new_foods = in_meal.get("foodList", []) or []
            # Ensure id is always a string to prevent type concatenation errors
            meal_id = in_meal.get("id")
            id_value = str(meal_id) if meal_id is not None else str(len(existing) + 1)
            print(f"Merging meal ID: {meal_id} -> {id_value} (type: {type(id_value)})")

            existing.append({
                "id": id_value,
                "title": in_meal.get("title", "Custom"),
                "tagline": in_meal.get("tagline", ""),
                "foodList": new_foods,
                "timeRange": in_meal.get("timeRange", ""),
                "itemsCount": len(new_foods),
            })
            continue

        ex_foods = ex_meal.get("foodList", []) or []

        # Append all incoming foods, generating unique IDs for each
        for nf in in_meal.get("foodList", []) or []:
            # Create a new unique ID for this food item to avoid duplicate keys
            unique_id = str(int(time.time() * 1000)) + str(random.randint(10000, 99999))
            new_food = nf.copy()
            new_food["id"] = unique_id
            ex_foods.append(new_food)

        ex_meal["foodList"] = ex_foods
        ex_meal["itemsCount"] = len(ex_foods)

    return existing


# -------------------- Endpoints --------------------
@router.get("/get")
async def get_actual_diet(
    client_id: int,
    date: date = None,
    start_date: date = None,
    end_date: date = None,
    meal_types: str = None,
    db: Session = Depends(get_db),
):
    try:
        # Validate input parameters
        if not date and not (start_date and end_date):
            raise FittbotHTTPException(
                status_code=400,
                detail="Either 'date' or both 'start_date' and 'end_date' must be provided",
                error_code="INVALID_PARAMETERS",
                log_data={"client_id": client_id, "date": str(date), "start_date": str(start_date), "end_date": str(end_date)},
            )

        # Parse meal types filter
        selected_meal_types = []
        if meal_types:
            selected_meal_types = [mt.strip() for mt in meal_types.split(',') if mt.strip()]

        # Get client target values for progress tracking
        client_target = (
            db.query(ClientTarget)
            .filter(ClientTarget.client_id == client_id)
            .first()
        )

        default_template = _default_template()

        if date:
            # Single date query
            record = (
                db.query(ActualDiet)
                .filter(ActualDiet.client_id == client_id, ActualDiet.date == date)
                .first()
            )

            if not record:
                # Return default template with empty progress data
                progress_data = {
                    "calories": {"actual": 0, "target": _target_value(client_target, "calories")},
                    "protein": {"actual": 0, "target": _target_value(client_target, "protein")},
                    "carbs": {"actual": 0, "target": _target_value(client_target, "carbs")},
                    "fat": {"actual": 0, "target": _target_value(client_target, "fat")},
                    "fiber": {"actual": 0, "target": _target_value(client_target, "fiber")},
                    "sugar": {"actual": 0, "target": _target_value(client_target, "sugar")},
                    "calcium": {"actual": 0, "target": _target_value(client_target, "calcium")},
                    "magnesium": {"actual": 0, "target": _target_value(client_target, "magnesium")},
                    "sodium": {"actual": 0, "target": _target_value(client_target, "sodium")},
                    "potassium": {"actual": 0, "target": _target_value(client_target, "potassium")},
                    "iron": {"actual": 0, "target": _target_value(client_target, "iron")},
                    "iodine": {"actual": 0, "target": _target_value(client_target, "iodine")},
                }

                return {
                    "status": 200,
                    "data": default_template,
                    "id": None,
                    "progress": progress_data
                }

            diet_data = record.diet_data or []

            # Calculate total macros from all foodLists in the diet data
            print("=" * 80)
            print(f"DEBUG [actual_diet.py /get]: Calculating macros for client_id={client_id}, date={date}")
            total_macros = {
                "calories": 0,
                "protein": 0,
                "carbs": 0,
                "fat": 0,
                "fiber": 0,
                "sugar": 0,
            }

            total_micro = {
                "calcium": 0,
                "magnesium": 0,
                "sodium": 0,
                "potassium": 0,
                "iron": 0,
                "iodine": 0,
            }

            if isinstance(diet_data, list):
                # Apply meal type filtering if specified
                filtered_meals = filter_meals_by_type(diet_data, selected_meal_types)
                print(f"DEBUG [actual_diet.py /get]: Found {len(filtered_meals)} meals in diet_data")

                for meal in filtered_meals:
                    if isinstance(meal, dict) and "foodList" in meal:
                        food_list = meal.get("foodList", [])
                        print(f"DEBUG [actual_diet.py /get]: Meal '{meal.get('title')}' has {len(food_list)} food items")
                        for food_item in food_list:
                            if isinstance(food_item, dict):
                                print(f"  - {food_item.get('name')}: cal={food_item.get('calories', 0)}, protein={food_item.get('protein', 0)}")
                                total_macros["calories"] += food_item.get("calories", 0) or 0
                                total_macros["protein"] += food_item.get("protein", 0) or 0
                                total_macros["carbs"] += food_item.get("carbs", 0) or 0
                                total_macros["fat"] += food_item.get("fat", 0) or 0
                                total_macros["fiber"] += food_item.get("fiber", 0) or 0
                                total_macros["sugar"] += food_item.get("sugar", 0) or 0
                                total_micro["calcium"] += _get_nutrient_value(food_item, "calcium")
                                total_micro["magnesium"] += _get_nutrient_value(food_item, "magnesium")
                                total_micro["sodium"] += _get_nutrient_value(food_item, "sodium")
                                total_micro["potassium"] += _get_nutrient_value(food_item, "potassium")
                                total_micro["iron"] += _get_nutrient_value(food_item, "iron")

            print(f"DEBUG [actual_diet.py /get]: ✅ CALCULATED total_macros from ActualDiet.diet_data:")
            print(f"  - Calories: {total_macros['calories']}")
            print(f"  - Protein: {total_macros['protein']}")
            print(f"  - Carbs: {total_macros['carbs']}")
            print(f"  - Fat: {total_macros['fat']}")
            print(f"  - Fiber: {total_macros['fiber']}")
            print(f"  - Sugar: {total_macros['sugar']}")
            print(f"  - Calcium: {total_micro['calcium']}")
            print(f"  - Magnesium: {total_micro['magnesium']}")
            print(f"  - Sodium: {total_micro['sodium']}")
            print(f"  - Potassium: {total_micro['potassium']}")
            print(f"  - Iron: {total_micro['iron']}")

            # Prepare progress data for frontend
            progress_data = {
                "calories": {"actual": total_macros["calories"], "target": _target_value(client_target, "calories")},
                "protein": {"actual": total_macros["protein"], "target": _target_value(client_target, "protein")},
                "carbs": {"actual": total_macros["carbs"], "target": _target_value(client_target, "carbs")},
                "fat": {"actual": total_macros["fat"], "target": _target_value(client_target, "fat")},
                "fiber": {"actual": total_macros["fiber"], "target": _target_value(client_target, "fiber")},
                "sugar": {"actual": total_macros["sugar"], "target": _target_value(client_target, "sugar")},
                "calcium": {"actual": total_micro["calcium"], "target": _target_value(client_target, "calcium")},
                "magnesium": {"actual": total_micro["magnesium"], "target": _target_value(client_target, "magnesium")},
                "sodium": {"actual": total_micro["sodium"], "target": _target_value(client_target, "sodium")},
                "potassium": {"actual": total_micro["potassium"], "target": _target_value(client_target, "potassium")},
                "iron": {"actual": total_micro["iron"], "target": _target_value(client_target, "iron")}
            }
            print(f"DEBUG [actual_diet.py /get]: Returning progress_data: {progress_data}")

            return {
                "status": 200,
                "data": diet_data,
                "id": record.record_id,
                "progress": progress_data
            }

        else:
            # Date range query
            records = (
                db.query(ActualDiet)
                .filter(
                    ActualDiet.client_id == client_id,
                    ActualDiet.date >= start_date,
                    ActualDiet.date <= end_date
                )
                .all()
            )

            all_diet_data = []
            total_macros = {
                "calories": 0,
                "protein": 0,
                "carbs": 0,
                "fat": 0,
                "fiber": 0,
                "sugar": 0
            }
            total_micro = {
                "calcium": 0,
                "magnesium": 0,
                "sodium": 0,
                "potassium": 0,
                "iron": 0,
                "iodine": 0,
            }
            day_count = 0

            # Process each date in the range
            current_date = start_date
            while current_date <= end_date:
                date_string = current_date.strftime("%Y-%m-%d")
                date_record = next((r for r in records if r.date == current_date), None)

                if date_record and date_record.diet_data:
                    diet_data = date_record.diet_data

                    # Add date field to each meal for display
                    for meal in diet_data:
                        if isinstance(meal, dict):
                            meal_with_date = meal.copy()
                            meal_with_date["date"] = date_string
                            all_diet_data.append(meal_with_date)

                    # Calculate macros using filtered meals for progress calculation
                    filtered_meals = filter_meals_by_type(diet_data, selected_meal_types)

                    has_filtered_data = False
                    for meal in filtered_meals:
                        if isinstance(meal, dict):
                            food_list = meal.get("foodList", [])
                            for food_item in food_list:
                                if isinstance(food_item, dict):
                                    total_macros["calories"] += food_item.get("calories", 0) or 0
                                    total_macros["protein"] += food_item.get("protein", 0) or 0
                                    total_macros["carbs"] += food_item.get("carbs", 0) or 0
                                    total_macros["fat"] += food_item.get("fat", 0) or 0
                                    total_macros["fiber"] += food_item.get("fiber", 0) or 0
                                    total_macros["sugar"] += food_item.get("sugar", 0) or 0
                                    total_micro["calcium"] += _get_nutrient_value(food_item, "calcium", "Calcium", "calcium_mg")
                                    total_micro["magnesium"] += _get_nutrient_value(food_item, "magnesium", "Magnesium", "magnesium_mg")
                                    total_micro["sodium"] += _get_nutrient_value(food_item, "sodium", "Sodium", "sodium_mg")
                                    total_micro["potassium"] += _get_nutrient_value(food_item, "potassium", "Potassium", "potassium_mg")
                                    total_micro["iron"] += _get_nutrient_value(food_item, "iron", "Iron", "iron_mg")
                                    total_micro["iodine"] += _get_nutrient_value(food_item, "iodine", "Iodine", "iodine_mcg")
                                    has_filtered_data = True

                    if has_filtered_data or not selected_meal_types:
                        day_count += 1
                else:
                    # Add empty template for days with no data
                    for template_meal in default_template:
                        meal_with_date = template_meal.copy()
                        meal_with_date["date"] = date_string
                        all_diet_data.append(meal_with_date)

                current_date = current_date + timedelta(days=1)

            # Calculate average macros
            if day_count > 0:
                avg_macros = {
                    "calories": {"actual": round(total_macros["calories"] / day_count), "target": _target_value(client_target, "calories")},
                    "protein": {"actual": round(total_macros["protein"] / day_count), "target": _target_value(client_target, "protein")},
                    "carbs": {"actual": round(total_macros["carbs"] / day_count), "target": _target_value(client_target, "carbs")},
                    "fat": {"actual": round(total_macros["fat"] / day_count), "target": _target_value(client_target, "fat")},
                    "fiber": {"actual": round(total_macros["fiber"] / day_count), "target": _target_value(client_target, "fiber")},
                    "sugar": {"actual": round(total_macros["sugar"] / day_count), "target": _target_value(client_target, "sugar")},
                    "calcium": {"actual": round(total_micro["calcium"] / day_count), "target": _target_value(client_target, "calcium")},
                    "magnesium": {"actual": round(total_micro["magnesium"] / day_count), "target": _target_value(client_target, "magnesium")},
                    "sodium": {"actual": round(total_micro["sodium"] / day_count), "target": _target_value(client_target, "sodium")},
                    "potassium": {"actual": round(total_micro["potassium"] / day_count), "target": _target_value(client_target, "potassium")},
                    "iron": {"actual": round(total_micro["iron"] / day_count), "target": _target_value(client_target, "iron")},
                    "iodine": {"actual": round(total_micro["iodine"] / day_count), "target": _target_value(client_target, "iodine")},
                }
            else:
                avg_macros = {
                    "calories": {"actual": 0, "target": _target_value(client_target, "calories")},
                    "protein": {"actual": 0, "target": _target_value(client_target, "protein")},
                    "carbs": {"actual": 0, "target": _target_value(client_target, "carbs")},
                    "fat": {"actual": 0, "target": _target_value(client_target, "fat")},
                    "fiber": {"actual": 0, "target": _target_value(client_target, "fiber")},
                    "sugar": {"actual": 0, "target": _target_value(client_target, "sugar")},
                    "calcium": {"actual": 0, "target": _target_value(client_target, "calcium")},
                    "magnesium": {"actual": 0, "target": _target_value(client_target, "magnesium")},
                    "sodium": {"actual": 0, "target": _target_value(client_target, "sodium")},
                    "potassium": {"actual": 0, "target": _target_value(client_target, "potassium")},
                    "iron": {"actual": 0, "target": _target_value(client_target, "iron")},
                    "iodine": {"actual": 0, "target": _target_value(client_target, "iodine")},
                }

            return {
                "status": 200,
                "data": all_diet_data,
                "progress": avg_macros,
                "date_range": {
                    "start_date": start_date.strftime("%Y-%m-%d"),
                    "end_date": end_date.strftime("%Y-%m-%d"),
                    "total_days": (end_date - start_date).days + 1,
                    "days_with_data": day_count
                }
            }

    except FittbotHTTPException:
        raise
    except Exception as e:
        raise FittbotHTTPException(
            status_code=500,
            detail=f"An unexpected error occurred {e}",
            error_code="ACTUAL_DIET_FETCH_ERROR",
            log_data={"client_id": client_id, "date": str(date), "start_date": str(start_date), "end_date": str(end_date), "error": str(e)},
        )


@router.post("/add")
async def create_or_append_diet(
    data: DietInput,
    db: Session = Depends(get_db),
    redis: Redis = Depends(get_redis),
):

    try:


        today = date.today()
        print("daata is,,,", data.diet_data)

        # Debug: Check types of IDs in incoming data
        for item in data.diet_data:
            if isinstance(item, dict):
                print(f"Item ID: {item.get('id')} (type: {type(item.get('id'))})")
                if "foodList" in item:
                    for food in item.get("foodList", []):
                        if isinstance(food, dict):
                            print(f"Food ID: {food.get('id')} (type: {type(food.get('id'))})")

        client = db.query(Client).filter(Client.client_id == data.client_id).first()
        gym_id = data.gym_id

        record = (
            db.query(ActualDiet)
            .filter(ActualDiet.client_id == data.client_id, ActualDiet.date == data.date)
            .first()
        )

        print("DEBUG: Checking if incoming data is template format...")
        try:
            incoming_is_template = _is_template_format(data.diet_data)
            print(f"DEBUG: incoming_is_template = {incoming_is_template}")
        except Exception as e:
            print(f"ERROR in _is_template_format: {e}")
            raise

        # Totals for ONLY the incoming payload (not the scaffold)
        print("=" * 80)
        print("DEBUG [actual_diet.py /add]: Calculating totals from INCOMING data...")
        print(f"DEBUG [actual_diet.py /add]: client_id={data.client_id}, date={data.date}")
        print(f"DEBUG [actual_diet.py /add]: Incoming diet_data structure:")
        print(f"DEBUG [actual_diet.py /add]: Type: {type(data.diet_data)}, Length: {len(data.diet_data) if isinstance(data.diet_data, list) else 'N/A'}")
        try:
            new_totals = calculate_totals(data.diet_data)
            print(f"DEBUG [actual_diet.py /add]: ✅ CALCULATED new_totals from INCOMING data:")
            print(f"  - Calories: {new_totals.get('calories', 0)}")
            print(f"  - Protein: {new_totals.get('protein', 0)}")
            print(f"  - Carbs: {new_totals.get('carbs', 0)}")
            print(f"  - Fat: {new_totals.get('fats', 0)}")
            print(f"  - Sugar: {new_totals.get('sugar', 0)}")
            print(f"  - Fiber: {new_totals.get('fiber', 0)}")
            print(f"  - Calcium: {new_totals.get('calcium', 0)}")
            print(f"  - Magnesium: {new_totals.get('magnesium', 0)}")
            print(f"  - Potassium: {new_totals.get('potassium', 0)}")
            print(f"  - Iodine: {new_totals.get('Iodine', 0)}")
            print(f"  - Iron: {new_totals.get('Iron', 0)}")
        except Exception as e:
            print(f"ERROR in calculate_totals: {e}")
            raise

        if record:
            if record.diet_data is None:
                # If nothing stored yet, initialize with default template if incoming is template format
                if incoming_is_template:
                    print("DEBUG: Expanding to full template...")
                    try:
                        record.diet_data = _expand_to_full_template_with(data.diet_data)
                        print("DEBUG: Template expansion successful")
                    except Exception as e:
                        print(f"ERROR in _expand_to_full_template_with: {e}")
                        raise
                else:
                    record.diet_data = data.diet_data
                flag_modified(record, "diet_data")
                db.commit()
                db.refresh(record)
            else:
                # Handle both old and new template formats when merging data
                existing_data = record.diet_data
                new_data = data.diet_data

                existing_is_template = _is_template_format(existing_data) if isinstance(existing_data, list) and existing_data else False

                if incoming_is_template and existing_is_template:
                    print("DEBUG: Merging templates...")
                    try:
                        merged = _merge_templates(existing_data, new_data)
                        record.diet_data = merged
                        flag_modified(record, "diet_data")
                        print("DEBUG: Template merging successful")
                    except Exception as e:
                        print(f"ERROR in _merge_templates: {e}")
                        raise
                    print("DEBUG: Continuing after template merge...")
                elif incoming_is_template and not existing_is_template:
                    # Migrate legacy day to template structure: start from full default + overlay incoming,
                    # then add legacy items under a "Legacy Import" category to preserve them.
                    base = _expand_to_full_template_with(new_data)
                    legacy_payload = existing_data if isinstance(existing_data, list) else [existing_data]
                    base.append({
                        "id": str(len(base) + 1),
                        "title": "Legacy Import",
                        "tagline": "Migrated items",
                        "foodList": legacy_payload,
                        "timeRange": "",
                        "itemsCount": len(legacy_payload),
                    })
                    record.diet_data = base
                    flag_modified(record, "diet_data")
                else:
                    # Legacy -> simple append
                    if isinstance(existing_data, list):
                        updated_list = existing_data + new_data
                        record.diet_data = updated_list
                    else:
                        record.diet_data = [existing_data] + new_data
                    flag_modified(record, "diet_data")

                print("DEBUG: About to commit record after template processing...")
                db.commit()
                print("DEBUG: Record committed successfully")
                db.refresh(record)
                print("DEBUG: Record refreshed successfully")
        else:
            # No existing record for the day
            if incoming_is_template:
                full_template = _expand_to_full_template_with(data.diet_data)
                record = ActualDiet(
                    client_id=data.client_id,
                    date=data.date,
                    diet_data=full_template,
                )
            else:
                record = ActualDiet(
                    client_id=data.client_id,
                    date=data.date,
                    diet_data=data.diet_data,
                )
            db.add(record)
            db.commit()
            db.refresh(record)

        # Aggregate new items into ClientActual
        print("=" * 80)
        print("DEBUG [actual_diet.py /add]: Starting ClientActual aggregation...")
        client_record = (
            db.query(ClientActual)
            .filter(ClientActual.client_id == data.client_id, ClientActual.date == data.date)
            .first()
        )
        print(f"DEBUG [actual_diet.py /add]: Found existing client_record: {client_record is not None}")
        if client_record:
            print(f"DEBUG [actual_diet.py /add]: EXISTING ClientActual values BEFORE update:")
            print(f"  - Calories: {client_record.calories}")
            print(f"  - Protein: {client_record.protein}")
            print(f"  - Carbs: {client_record.carbs}")
            print(f"  - Fat: {client_record.fats}")
            print(f"  - Sugar: {client_record.sugar}")
            print(f"  - Fiber: {client_record.fiber}")
            print(f"  - Calcium: {client_record.calcium}")
            print(f"  - Magnesium: {client_record.magnesium}")
            print(f"  - Potassium: {client_record.potassium}")
            print(f"  - Iodine: {client_record.Iodine}")
            print(f"  - Iron: {client_record.Iron}")

        client_target = db.query(ClientTarget).filter(ClientTarget.client_id == data.client_id).first()
        client_target_calories = client_target.calories if client_target else 0

        if client_record:
            print("DEBUG: Updating existing client_record...")
            try:
                # Convert existing values to float to prevent string concatenation errors
                current_calories = float(client_record.calories) if client_record.calories is not None else 0
                current_protein = float(client_record.protein) if client_record.protein is not None else 0
                current_carbs = float(client_record.carbs) if client_record.carbs is not None else 0
                current_fats = float(client_record.fats) if client_record.fats is not None else 0
                current_sugar = float(client_record.sugar) if client_record.sugar is not None else 0
                current_fiber = float(client_record.fiber) if client_record.fiber is not None else 0
                current_calcium = float(client_record.calcium) if client_record.calcium is not None else 0
                current_magnesium = float(client_record.magnesium) if client_record.magnesium is not None else 0
                current_potassium = float(client_record.potassium) if client_record.potassium is not None else 0
                current_iodine = float(client_record.Iodine) if client_record.Iodine is not None else 0
                current_iron = float(client_record.Iron) if client_record.Iron is not None else 0

                print(f"DEBUG: Current values - calories: {current_calories} ({type(current_calories)})")
                print(f"DEBUG: Adding values - calories: {new_totals['calories']} ({type(new_totals['calories'])})")

                client_record.calories = current_calories + new_totals["calories"]
                client_record.protein = current_protein + new_totals["protein"]
                client_record.carbs = current_carbs + new_totals["carbs"]
                client_record.fats = current_fats + new_totals["fats"]
                client_record.sugar = current_sugar + new_totals["sugar"]
                client_record.fiber = current_fiber + new_totals["fiber"]
                client_record.calcium = current_calcium + new_totals["calcium"]
                client_record.magnesium = current_magnesium + new_totals["magnesium"]
                client_record.potassium = current_potassium + new_totals["potassium"]
                client_record.Iodine = current_iodine + new_totals["Iodine"]
                client_record.Iron = current_iron + new_totals["Iron"]

                print("DEBUG [actual_diet.py /add]: ✅ Client record fields UPDATED (adding new_totals to existing):")
                print(f"  - Calories: {client_record.calories} (was {current_calories}, added {new_totals['calories']})")
                print(f"  - Protein: {client_record.protein} (was {current_protein}, added {new_totals['protein']})")
                print(f"  - Carbs: {client_record.carbs} (was {current_carbs}, added {new_totals['carbs']})")
                print(f"  - Fat: {client_record.fats} (was {current_fats}, added {new_totals['fats']})")
                print(f"  - Sugar: {client_record.sugar}")
                print(f"  - Fiber: {client_record.fiber}")
                print(f"  - Calcium: {client_record.calcium}")
                print(f"  - Magnesium: {client_record.magnesium}")
                print(f"  - Potassium: {client_record.potassium}")
                print(f"  - Iodine: {client_record.Iodine}")
                print(f"  - Iron: {client_record.Iron}")
                print("DEBUG [actual_diet.py /add]: Committing ClientActual update...")
                db.commit()
                print("DEBUG [actual_diet.py /add]: ✅ ClientActual committed successfully")
            except Exception as e:
                print(f"ERROR updating client_record: {e}")
                raise
        else:
            print("DEBUG [actual_diet.py /add]: Creating NEW ClientActual record...")
            target_record = client_target
            try:
                client_record = ClientActual(
                client_id=data.client_id,
                date=data.date,
                calories=new_totals["calories"],
                protein=new_totals["protein"],
                carbs=new_totals["carbs"],
                fats=new_totals["fats"],
                sugar=new_totals['sugar'],
                fiber=new_totals['fiber'],
                magnesium=new_totals['magnesium'],
                calcium=new_totals['calcium'],
                potassium=new_totals['potassium'],
                Iodine=new_totals['Iodine'],
                Iron=new_totals['Iron'],
                target_calories=target_record.calories if target_record else None,
                target_protein=target_record.protein if target_record else None,
                target_fat=target_record.fat if target_record else None,
                target_carbs=target_record.carbs if target_record else None,
                target_fiber=getattr(target_record, "fiber", None) if target_record else None,
                target_sugar=getattr(target_record, "sugar", None) if target_record else None,
                target_calcium=getattr(target_record, "calcium", None) if target_record else None,
                target_magnesium=getattr(target_record, "magnesium", None) if target_record else None,
                target_potassium=getattr(target_record, "potassium", None) if target_record else None,
                target_Iodine=getattr(target_record, "Iodine", None) if target_record else None,
                target_Iron=getattr(target_record, "Iron", None) if target_record else None,
                )
                print("DEBUG [actual_diet.py /add]: ✅ NEW ClientActual record created with values:")
                print(f"  - Calories: {new_totals['calories']}")
                print(f"  - Protein: {new_totals['protein']}")
                print(f"  - Carbs: {new_totals['carbs']}")
                print(f"  - Fat: {new_totals['fats']}")
                print(f"  - Sugar: {new_totals['sugar']}")
                print(f"  - Fiber: {new_totals['fiber']}")
                print(f"  - Calcium: {new_totals['calcium']}")
                print(f"  - Magnesium: {new_totals['magnesium']}")
                print(f"  - Potassium: {new_totals['potassium']}")
                print(f"  - Iodine: {new_totals['Iodine']}")
                print(f"  - Iron: {new_totals['Iron']}")
                db.add(client_record)
                db.commit()
                print("DEBUG [actual_diet.py /add]: ✅ New ClientActual record committed successfully")
            except Exception as e:
                print(f"ERROR creating new ClientActual record: {e}")
                raise

        # Leaderboard/xp logic unchanged
        print("DEBUG: Starting leaderboard/XP processing...")
        if data.date == today:
            print("DEBUG: Processing for today's date...")
            if client_target_calories > 0:
                ratio = new_totals["calories"] / client_target_calories
                if ratio > 1:
                    ratio = 1
            else:
                ratio = 0

            print(
                f"DEBUG: XP calc -> new_calories={new_totals['calories']}, "
                f"target_calories={client_target_calories}, ratio={ratio}"
            )

            calorie_points = int(round(ratio * 50))
            print(f"DEBUG: Initial calorie_points={calorie_points}")
            calorie_event = (
                db.query(CalorieEvent)
                .filter(
                    CalorieEvent.client_id == data.client_id,
                    CalorieEvent.event_date == today
                    
                )
                .first()
            )

            if not calorie_event:
                calorie = CalorieEvent(
                    client_id=data.client_id,
                    event_date=today,
                    calories_added=0,
                )
                db.add(calorie)
                db.commit()
                calorie_event = calorie

            if not calorie_event.calories_added:
                calorie_event.calories_added = 0

            added_calory = calorie_event.calories_added

            print(f"DEBUG: Existing calorie_event.calories_added={added_calory}")

            if added_calory < 50:
                if added_calory + calorie_points > 50:
                    calorie_points = 50 - added_calory
                    print(
                        "DEBUG: Capping calorie_points to avoid exceeding 50 -> "
                        f"capped_calorie_points={calorie_points}"
                    )

                print(
                    f"DEBUG: Awarding calorie_points={calorie_points} "
                    f"(before daily/monthly/overall updates)"
                )

                daily_record = (
                    db.query(LeaderboardDaily)
                    .filter(
                        LeaderboardDaily.client_id == data.client_id,
                        LeaderboardDaily.date == today,
                    )
                    .first()
                )

                if daily_record:
                    prev_daily_xp = daily_record.xp
                    daily_record.xp += calorie_points
                    print(
                        f"DEBUG: Updated daily XP from {prev_daily_xp} to {daily_record.xp} "
                        f"for client_id={data.client_id}"
                    )
                else:
                    print(
                        f"DEBUG: Creating new daily leaderboard entry "
                        f"with xp={calorie_points} for client_id={data.client_id}"
                    )
                    new_daily = LeaderboardDaily(
                        client_id=data.client_id,
                        xp=calorie_points,
                        date=today,
                    )
                    db.add(new_daily)

                month_date = today.replace(day=1)
                monthly_record = (
                    db.query(LeaderboardMonthly)
                    .filter(
                        LeaderboardMonthly.client_id == data.client_id,
                        LeaderboardMonthly.month == month_date,
                    )
                    .first()
                )

                if monthly_record:
                    prev_monthly_xp = monthly_record.xp
                    monthly_record.xp += calorie_points
                    print(
                        f"DEBUG: Updated monthly XP from {prev_monthly_xp} to {monthly_record.xp} "
                        f"for client_id={data.client_id}"
                    )
                else:
                    print(
                        f"DEBUG: Creating new monthly leaderboard entry "
                        f"with xp={calorie_points} for client_id={data.client_id}"
                    )
                    new_monthly = LeaderboardMonthly(
                        client_id=data.client_id,
                      
                        xp=calorie_points,
                        month=month_date,
                    )
                    db.add(new_monthly)

                overall_record = (
                    db.query(LeaderboardOverall)
                    .filter(
                        LeaderboardOverall.client_id == data.client_id
                    )
                    .first()
                )

                if overall_record:
                    prev_overall_xp = overall_record.xp
                    overall_record.xp += calorie_points
                    new_total = overall_record.xp
                    print(
                        f"DEBUG: Updated overall XP from {prev_overall_xp} to {overall_record.xp} "
                        f"for client_id={data.client_id}"
                    )

                    next_row = (
                        db.query(ClientNextXp)
                        .filter_by(client_id=data.client_id)
                        .with_for_update()
                        .one_or_none()
                    )

                    def _tier_after(xp: int):
                        return (
                            db.query(RewardGym)
                            .filter(RewardGym.xp > xp)
                            .order_by(asc(RewardGym.xp))
                            .first()
                        )

                    if gym_id is not None:
                        if next_row :
                            if new_total >= next_row.next_xp  and next_row.next_xp!=0:
                                print(
                                    f"DEBUG: Client {data.client_id} reached new XP milestone "
                                    f"-> next_xp={next_row.next_xp}"
                                )
                                client_details = client
                                db.add(
                                    RewardPrizeHistory(
                                        client_id=data.client_id,
                                        gym_id=gym_id,
                                        xp=next_row.next_xp,
                                        gift=next_row.gift,
                                        achieved_date=datetime.now(),
                                        client_name=client_details.name if client_details else None,
                                        is_given=False,
                                        profile=client_details.profile if client_details else None,
                                    )
                                )

                                next_tier = _tier_after(next_row.next_xp)
                                if next_tier:
                                    next_row.next_xp = next_tier.xp
                                    next_row.gift = next_tier.gift
                                else:
                                    next_row.next_xp = 0
                                    next_row.gift = None
                        else:
                            first_tier = (
                                db.query(RewardGym)
                                .order_by(asc(RewardGym.xp))
                                .first()
                            )
                            if first_tier:
                                db.add(
                                    ClientNextXp(
                                        client_id=data.client_id,
                                        next_xp=first_tier.xp,
                                        gift=first_tier.gift,
                                    )
                                )

                    db.commit()
                else:
                    print(
                        f"DEBUG: Creating new overall leaderboard entry "
                        f"with xp={calorie_points} for client_id={data.client_id}"
                    )
                    new_overall = LeaderboardOverall(
                        client_id=data.client_id, xp=calorie_points
                    )
                    db.add(new_overall)
                    db.commit()

                existing_event = (
                    db.query(CalorieEvent)
                    .filter(
                        CalorieEvent.client_id == data.client_id,
                        CalorieEvent.event_date == data.date,
                    )
                    .first()
                )

                if existing_event:
                    before_update = existing_event.calories_added
                    existing_event.calories_added += calorie_points
                    print(
                        f"DEBUG: Updated CalorieEvent from {before_update} to "
                        f"{existing_event.calories_added} for client_id={data.client_id}"
                    )
                else:
                    print(
                        f"DEBUG: Creating CalorieEvent with calories_added={calorie_points} "
                        f"for client_id={data.client_id}"
                    )
                    new_event = CalorieEvent(
                        client_id=data.client_id,
                        event_date=data.date,
                        calories_added=calorie_points,
                    )
                    db.add(new_event)

                db.commit()
            else:
                calorie_points = 0
        else:
            calorie_points = 0


        await delete_keys_by_pattern(redis, f"{data.client_id}:*:target_actual")
        await delete_keys_by_pattern(redis, f"{data.client_id}:*:chart")

        # Check feedback status
        show_feedback = check_feedback_status(db, data.client_id)

        # Check if actual calories exceed target calories
        client_target = db.query(ClientTarget).filter(ClientTarget.client_id == data.client_id).first()
        target_exceeded = False
        target_calories = None
        if client_target and client_target.calories:
            target_calories = client_target.calories
            # Get current actual diet record and calculate total calories from diet_data
            actual_diet_record = db.query(ActualDiet).filter(
                ActualDiet.client_id == data.client_id,
                ActualDiet.date == data.date
            ).first()

            if actual_diet_record and actual_diet_record.diet_data:
                # Calculate total calories from the diet_data
                total_calories_from_diet = calculate_totals(actual_diet_record.diet_data)
                actual_calories = total_calories_from_diet.get("calories", 0)

                if actual_calories > client_target.calories:
                    target_exceeded = True
                    print(f"DEBUG: Target exceeded! Actual: {actual_calories}, Target: {client_target.calories}")

        if target_exceeded and target_calories is not None:
            achievement_date = str(data.date)
            redis_key = f"diet_target_achieved:{data.client_id}:{target_calories}:{achievement_date}"
            # Only announce the achievement once per (client, target) pair
            if await redis.exists(redis_key):
                target_exceeded = False
            else:
                await redis.set(redis_key, "1", ex=86400)
                print(f"DEBUG: Created redis key {redis_key} for first-time target achievement")

        
        print("target_exceeded is",target_exceeded)

        

        
        return {
            "status": 200,
            "message": "Diet data appended and aggregated nutrition updated",
            "reward_point": calorie_points,
            "feedback": show_feedback,
            "target": target_exceeded,
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
            detail=f"An unexpected error occurred {e}",
            error_code="ACTUAL_DIET_CREATE_APPEND_ERROR",
            log_data={
                "client_id": data.client_id,
                "date": str(data.date),
                
                "error": str(e),
            },
        )


@router.put("/edit")
async def edit_diet(
    data: DieteditInput,
    db: Session = Depends(get_db),
    redis: Redis = Depends(get_redis),
):
    try:
        client = db.query(Client).filter(Client.client_id == data.client_id).first()
        gym_id = None

        if not data:
            record = db.query(ActualDiet).filter(ActualDiet.record_id == data.record_id).first()
            if not record:
                raise FittbotHTTPException(
                    status_code=404,
                    detail="Diet record not found",
                    error_code="ACTUAL_DIET_NOT_FOUND",
                    log_data={"record_id": getattr(data, 'record_id', None)},
                )

            db.delete(record)
            db.commit()

            client_actual_record = (
                db.query(ClientActual)
                .filter(ClientActual.client_id == data.client_id, ClientActual.date == data.date)
                .first()
            )
            if client_actual_record:
                db.delete(client_actual_record)
                db.commit()

            return {
                "status": 200,
                "message": "Diet record and corresponding aggregated client data deleted",
            }

        record = (
            db.query(ActualDiet)
            .filter(ActualDiet.record_id == data.record_id)
            .first()
        )

        if not record:
            raise FittbotHTTPException(
                status_code=404,
                detail="Record not found",
                error_code="ACTUAL_DIET_NOT_FOUND",
                log_data={"record_id": data.record_id},
            )

        record.diet_data = data.diet_data
        db.commit()

        totals = calculate_totals(record.diet_data)

        client_record = (
            db.query(ClientActual)
            .filter(ClientActual.client_id == record.client_id, ClientActual.date == record.date)
            .first()
        )

        if client_record:
            client_record.calories = totals["calories"]
            client_record.protein = totals["protein"]
            client_record.carbs = totals["carbs"]
            client_record.fats = totals["fats"]
            client_record.sugar = totals["sugar"]
            client_record.fiber = totals['fiber']
            client_record.calcium = totals['calcium']
            client_record.magnesium = totals["magnesium"]
            client_record.potassium = totals["potassium"]
            client_record.Iodine = totals["Iodine"]
            client_record.Iron = totals["Iron"]
            db.commit()
        else:
            client_record = ClientActual(
                client_id=record.client_id,
                date=record.date,
                calories=totals["calories"],
                protein=totals["protein"],
                carbs=totals["carbs"],
                fats=totals["fats"],
                sugar=totals["sugar"],
                fiber=totals['fiber'],
                calcium=totals['calcium'],
                magnesium=totals["magnesium"],
                potassium=totals["potassium"],
                Iodine=totals["Iodine"],
                Iron=totals["Iron"],
            )
            db.add(client_record)
            db.commit()

        if data.date == date.today():
            old_event = (
                db.query(CalorieEvent)
                .filter(
                    CalorieEvent.client_id == record.client_id,
                   
                    CalorieEvent.event_date == date.today(),
                )
                .first()
            )

            if old_event:
                old_calorie_points = old_event.calories_added
                daily_record = (
                    db.query(LeaderboardDaily)
                    .filter(
                        LeaderboardDaily.client_id == data.client_id,
                        
                        LeaderboardDaily.date == date.today(),
                    )
                    .first()
                )
                if daily_record:
                    daily_record.xp -= old_calorie_points

                month_date = date.today().replace(day=1)
                monthly_record = (
                    db.query(LeaderboardMonthly)
                    .filter(
                        LeaderboardMonthly.client_id == data.client_id,
                        
                        LeaderboardMonthly.month == month_date,
                    )
                    .first()
                )
                if monthly_record:
                    monthly_record.xp -= old_calorie_points

                overall_record = (
                    db.query(LeaderboardOverall)
                    .filter(
                        LeaderboardOverall.client_id == data.client_id,
                        
                    )
                    .first()
                )
                if overall_record:
                    overall_record.xp -= old_calorie_points

                old_event.calories_added = 0
            else:
                new_event = CalorieEvent(
                    client_id=data.client_id,
                   
                    event_date=record.date,
                    calories_added=0,
                )
                db.add(new_event)

            db.commit()

            if client_record.target_calories and client_record.target_calories > 0:
                ratio = totals["calories"] / client_record.target_calories
                if ratio > 1:
                    ratio = 1
            else:
                ratio = 0

            calorie_points = int(round(ratio * 50))
            today_val = date.today()

            daily_record = (
                db.query(LeaderboardDaily)
                .filter(
                    LeaderboardDaily.client_id == data.client_id,
                    
                    LeaderboardDaily.date == today_val,
                )
                .first()
            )
            if daily_record:
                daily_record.xp += calorie_points
            else:
                new_daily = LeaderboardDaily(
                    client_id=data.client_id,  xp=calorie_points, date=today_val
                )
                db.add(new_daily)

            month_date = today_val.replace(day=1)
            monthly_record = (
                db.query(LeaderboardMonthly)
                .filter(
                    LeaderboardMonthly.client_id == data.client_id,
                    
                    LeaderboardMonthly.month == month_date,
                )
                .first()
            )
            if monthly_record:
                monthly_record.xp += calorie_points
            else:
                new_monthly = LeaderboardMonthly(
                    client_id=data.client_id,
                  
                    xp=calorie_points,
                    month=month_date,
                )
                db.add(new_monthly)

            overall_record = (
                db.query(LeaderboardOverall)
                .filter(
                    LeaderboardOverall.client_id == data.client_id,
                    
                )
                .first()
            )
            if overall_record:
                overall_record.xp += calorie_points
                db.commit()
            else:
                new_overall = LeaderboardOverall(
                    client_id=data.client_id,  xp=calorie_points
                )
                db.add(new_overall)
                db.commit()

            existing_event = (
                db.query(CalorieEvent)
                .filter(
                    CalorieEvent.client_id == data.client_id,
                 
                    CalorieEvent.event_date == data.date,
                )
                .first()
            )
            if existing_event:
                existing_event.calories_added += calorie_points
            else:
                new_event = CalorieEvent(
                    client_id=data.client_id,
                    
                    event_date=date.today(),
                    calories_added=calorie_points,
                )
                db.add(new_event)
            db.commit()

        await delete_keys_by_pattern(redis, f"{data.client_id}:*:target_actual")
        await delete_keys_by_pattern(redis, f"{data.client_id}:*:chart")

        return {
            "status": 200,
            "message": "Diet data replaced and aggregated nutrition updated",
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
            detail=f"An unexpected error occurred {e}",
            error_code="ACTUAL_DIET_EDIT_ERROR",
            log_data={
                "record_id": getattr(data, "record_id", None),
                "client_id": data.client_id,
                
                "date": str(data.date),
                "error": str(e),
            },
        )


@router.delete("/delete")
async def delete_diet(
    record_id: int,
    client_id: int,
    
    date: date,
    gym_id: Optional[int]=None,
    db: Session = Depends(get_db),
    redis: Redis = Depends(get_redis),
):
    try:
        record = db.query(ActualDiet).filter(ActualDiet.record_id == record_id).first()
        if not record:
            raise FittbotHTTPException(
                status_code=404,
                detail="Diet record not found",
                error_code="ACTUAL_DIET_NOT_FOUND",
                log_data={"record_id": record_id},
            )

        db.delete(record)
        db.commit()

        client_actual_record = (
            db.query(ClientActual)
            .filter(ClientActual.client_id == client_id, ClientActual.date == date)
            .first()
        )
        if client_actual_record:
            db.delete(client_actual_record)
            db.commit()

        if date == date.today():
            old_event = (
                db.query(CalorieEvent)
                .filter(
                    CalorieEvent.client_id == client_id,
                  
                    CalorieEvent.event_date == date.today(),
                )
                .first()
            )

            if old_event:
                old_calorie_points = old_event.calories_added
                daily_record = (
                    db.query(LeaderboardDaily)
                    .filter(
                        LeaderboardDaily.client_id == client_id,
                       
                        LeaderboardDaily.date == date.today(),
                    )
                    .first()
                )
                if daily_record:
                    daily_record.xp -= old_calorie_points

                month_date = date.today().replace(day=1)
                monthly_record = (
                    db.query(LeaderboardMonthly)
                    .filter(
                        LeaderboardMonthly.client_id == client_id,
                        
                        LeaderboardMonthly.month == month_date,
                    )
                    .first()
                )
                if monthly_record:
                    monthly_record.xp -= old_calorie_points

                overall_record = (
                    db.query(LeaderboardOverall)
                    .filter(
                        LeaderboardOverall.client_id == client_id,
                        
                    )
                    .first()
                )
                if overall_record:
                    overall_record.xp -= old_calorie_points

                old_event.calories_added = 0
            else:
                new_event = CalorieEvent(
                    client_id=client_id,
               
                    event_date=date.today(),
                    calories_added=0,
                )
                db.add(new_event)

            db.commit()

        await delete_keys_by_pattern(redis, f"{client_id}:*:target_actual")


        return {
            "status": 200,
            "message": "Diet record and corresponding aggregated client data deleted",
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
            detail=f"An unexpected error occurred {e}",
            error_code="ACTUAL_DIET_DELETE_ERROR",
            log_data={
                "record_id": record_id,
                "client_id": client_id,
               
                "date": str(date),
                "error": str(e),
            },
        )
