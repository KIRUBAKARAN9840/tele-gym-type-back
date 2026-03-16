
import json
import asyncio
import random
import math
from typing import Dict, List, Optional, Set
from sqlalchemy.orm import Session
from sqlalchemy import or_, and_, func
from openai import AsyncOpenAI
from app.models.fittbot_models import IndianFoodMaster
from datetime import datetime
import uuid

# ============================================================================
# SECTION 1: DATABASE FOOD RETRIEVAL (Indian Food Database)
# ============================================================================

def get_meal_slot_column_for_slot_id(slot_id: str) -> str:
    """
    Map slot IDs (1-10) to database column names in indian_food_master
    """
    slot_map = {
        "1": "suitable_for_pre_workout",      # Pre workout
        "2": "suitable_for_post_workout",     # Post workout
        "3": "suitable_for_early_morning",    # Early morning Detox
        "4": "suitable_for_pre_breakfast",    # Pre-Breakfast Starter
        "5": "suitable_for_breakfast",        # Breakfast
        "6": "suitable_for_mid_morning",      # Mid-Morning snack
        "7": "suitable_for_lunch",            # Lunch
        "8": "suitable_for_evening_snack",    # Evening snack
        "9": "suitable_for_dinner",           # Dinner
        "10": "suitable_for_bedtime"          # Bed time
    }
    return slot_map.get(slot_id, "suitable_for_breakfast")


def build_diet_type_filter(diet_type: str):
    """Build SQLAlchemy filter for diet type with strict compliance"""
    diet_type_lower = diet_type.lower()

    if diet_type_lower == "vegetarian":
        return IndianFoodMaster.is_vegetarian == True
    elif diet_type_lower in ["non-vegetarian", "non_vegetarian", "nonveg"]:
        return or_(
            IndianFoodMaster.is_vegetarian == True,
            IndianFoodMaster.is_non_vegetarian == True,
            IndianFoodMaster.is_eggetarian == True
        )
    elif diet_type_lower == "vegan":
        return IndianFoodMaster.is_vegan == True
    elif diet_type_lower == "eggetarian":
        return or_(
            IndianFoodMaster.is_vegetarian == True,
            IndianFoodMaster.is_eggetarian == True
        )
    elif diet_type_lower == "jain":
        return IndianFoodMaster.is_jain == True
    elif diet_type_lower == "paleo":
        return IndianFoodMaster.is_paleo == True
    elif diet_type_lower in ["ketogenic", "keto"]:
        return IndianFoodMaster.is_ketogenic == True
    else:
        # Default to vegetarian
        return IndianFoodMaster.is_vegetarian == True


def get_diet_restricted_keywords(diet_type: str) -> List[str]:
    """
    Get list of keywords for foods that are strictly forbidden for each diet type
    Returns list of restricted food keywords
    """
    diet_type_lower = diet_type.lower()

    restrictions = {
        "jain": [
            'potato', 'aloo', 'batata',  # Potato variants
            'onion', 'pyaz', 'kanda',     # Onion variants
            'garlic', 'lehsun', 'lasun',  # Garlic variants
            'carrot', 'gajar',             # Carrot
            'radish', 'mooli',             # Radish
            'beetroot', 'chukandar',       # Beetroot
            'ginger', 'adrak',             # Ginger
            'turmeric', 'haldi',           # Turmeric
            'turnip', 'shalgam',           # Turnip
            'sweet potato', 'shakarkandi', # Sweet potato
            'yam', 'jimikand', 'suran',    # Yam
            'mushroom', 'khumbi'           # Mushroom
        ],

        "vegan": [
            'paneer', 'cheese', 'butter', 'ghee', 'dahi', 'curd', 'yogurt',
            'milk', 'doodh', 'cream', 'malai', 'khoya', 'mawa',
            'egg', 'anda', 'mayonnaise',
            'chicken', 'mutton', 'fish', 'meat', 'seafood', 'prawn', 'shrimp',
            'honey', 'shahad'
        ],

        "vegetarian": [
            'chicken', 'murgi', 'tandoori chicken',
            'mutton', 'lamb', 'goat', 'bakra',
            'fish', 'machli', 'salmon', 'tuna',
            'meat', 'beef', 'pork',
            'egg', 'anda', 'omelette', 'boiled egg',
            'seafood', 'prawn', 'shrimp', 'crab', 'lobster'
        ],

        "ketogenic": [
            'rice', 'chawal', 'biryani', 'pulao',
            'roti', 'chapati', 'paratha', 'naan', 'bread', 'puri', 'bhatura',
            'potato', 'aloo', 'batata',
            'banana', 'kela',
            'mango', 'aam',
            'grapes', 'angoor',
            'sugar', 'cheeni', 'gud', 'jaggery',
            'honey', 'shahad',
            'dal', 'lentil', 'rajma', 'chole', 'chickpea', 'bean'
        ],

        "paleo": [
            'rice', 'chawal',
            'wheat', 'gehun', 'roti', 'chapati', 'bread', 'naan',
            'dal', 'lentil', 'rajma', 'bean', 'legume',
            'paneer', 'cheese', 'milk', 'dahi', 'curd',
            'sugar', 'cheeni',
            'peanut', 'moongfali',
            'soy', 'tofu', 'soya'
        ],

        "eggetarian": [
            'chicken', 'murgi',
            'mutton', 'lamb', 'goat',
            'fish', 'machli',
            'meat', 'beef', 'pork',
            'seafood', 'prawn', 'shrimp'
        ]
    }

    return restrictions.get(diet_type_lower, [])


def is_food_diet_compliant(food_name: str, diet_type: str) -> bool:
    """
    Universal diet compliance validator for all diet types
    Checks food name against diet-specific restricted items

    Args:
        food_name: Name of the food item
        diet_type: Diet type (jain, vegan, vegetarian, ketogenic, paleo, eggetarian)

    Returns:
        True if food is compliant, False otherwise
    """
    food_name_lower = food_name.lower()
    restricted_keywords = get_diet_restricted_keywords(diet_type)

    # Check if any restricted keyword appears in food name
    for keyword in restricted_keywords:
        if keyword in food_name_lower:
            return False

    return True


def build_cuisine_filter(cuisine: Optional[str]):
    """Build SQLAlchemy filter for cuisine type"""
    if not cuisine:
        return None

    cuisine_lower = cuisine.lower()

    if "north" in cuisine_lower:
        return or_(
            IndianFoodMaster.cuisine_type == "North Indian",
            IndianFoodMaster.cuisine_type == "Common"
        )
    elif "south" in cuisine_lower:
        return or_(
            IndianFoodMaster.cuisine_type == "South Indian",
            IndianFoodMaster.cuisine_type == "Common"
        )
    else:
        return IndianFoodMaster.cuisine_type == "Common"


def get_indian_foods_from_db(
    db: Session,
    slot_id: str,
    diet_type: str = "vegetarian",
    cuisine: Optional[str] = None,
    health_condition: Optional[str] = None,
    target_calories: Optional[float] = None,
    limit: int = 50
) -> List[IndianFoodMaster]:
    """
    Get Indian foods from database for a specific meal slot

    Args:
        db: Database session
        slot_id: Meal slot ID (1-10)
        diet_type: vegetarian, non-vegetarian, vegan, etc.
        cuisine: North Indian, South Indian, Common
        health_condition: diabetes, weight loss, muscle gain, etc.
        target_calories: Target calories for the slot
        limit: Max foods to return

    Returns:
        List of IndianFoodMaster objects
    """

    # Get the appropriate column for this slot
    slot_column = get_meal_slot_column_for_slot_id(slot_id)

    # Base query
    query = db.query(IndianFoodMaster).filter(
        IndianFoodMaster.is_active == True,
        getattr(IndianFoodMaster, slot_column) == True
    )

    # Apply diet type filter
    diet_filter = build_diet_type_filter(diet_type)
    query = query.filter(diet_filter)

    # Apply cuisine filter
    cuisine_filter = build_cuisine_filter(cuisine)
    if cuisine_filter is not None:
        query = query.filter(cuisine_filter)

    # Apply health condition filters (as preference, not requirement)
    # We'll try with health filter first, if no results, we'll skip it
    health_query = query
    if health_condition:
        condition_lower = health_condition.lower()
        if "diabet" in condition_lower:
            health_query = health_query.filter(IndianFoodMaster.is_diabetic_friendly == True)
        elif "weight" in condition_lower and "loss" in condition_lower:
            health_query = health_query.filter(IndianFoodMaster.is_weight_loss_friendly == True)
        elif "muscle" in condition_lower or "gain" in condition_lower:
            health_query = health_query.filter(IndianFoodMaster.is_muscle_gain_friendly == True)
        elif "heart" in condition_lower:
            health_query = health_query.filter(IndianFoodMaster.is_heart_healthy == True)

    # Apply calorie filter with narrower range (±50%)
    if target_calories and target_calories > 0:
        calorie_min = target_calories * 0.5  # More restrictive minimum
        calorie_max = target_calories * 1.5  # More restrictive maximum
        health_query = health_query.filter(
            and_(
                IndianFoodMaster.calories >= calorie_min,
                IndianFoodMaster.calories <= calorie_max
            )
        )

    # Try to get foods with health filter
    foods = health_query.limit(limit).all()

    # If no foods found, try without health filter
    if not foods and health_condition:
        if target_calories and target_calories > 0:
            calorie_min = target_calories * 0.1
            calorie_max = target_calories * 1.5
            query = query.filter(
                and_(
                    IndianFoodMaster.calories >= calorie_min,
                    IndianFoodMaster.calories <= calorie_max
                )
            )
        foods = query.limit(limit).all()

    # If still no foods, skip calorie filter entirely
    if not foods:
        query = db.query(IndianFoodMaster).filter(
            IndianFoodMaster.is_active == True,
            getattr(IndianFoodMaster, slot_column) == True
        )
        query = query.filter(diet_filter)
        if cuisine_filter is not None:
            query = query.filter(cuisine_filter)
        foods = query.limit(limit).all()

    return foods


# ============================================================================
# SECTION 2: MEAL PLAN GENERATION
# ============================================================================

def calculate_slot_calories(slot_id: str, daily_calories: float) -> float:
    """Calculate target calories for a meal slot"""
    # Calorie distribution percentages
    percentages = {
        "1": 0.05,   # Pre workout - 5%
        "2": 0.05,   # Post workout - 5%
        "3": 0.03,   # Early morning - 3%
        "4": 0.05,   # Pre-breakfast - 5%
        "5": 0.25,   # Breakfast - 25%
        "6": 0.07,   # Mid-morning - 7%
        "7": 0.30,   # Lunch - 30%
        "8": 0.07,   # Evening snack - 7%
        "9": 0.20,   # Dinner - 20%
        "10": 0.03   # Bedtime - 3%
    }

    percentage = percentages.get(slot_id, 0.10)
    return daily_calories * percentage
def create_default_food_for_slot(slot_id: str, diet_type: str) -> Dict:
    """Create a default food item for a slot if no suitable food is found"""
    slot_defaults = {
    "1": {"name": "Sugar-Free Energy Drink (Caffeinated)", "calories": 10, "protein": 0, "carbs": 2, "fat": 0, "quantity": "250 ml"},
    "2": {"name": "Whey Protein Shake", "calories": 120, "protein": 24, "carbs": 4, "fat": 2, "quantity": "300 ml"},
    "3": {"name": "Warm Water with Lemon", "calories": 8, "protein": 0, "carbs": 2, "fat": 0, "quantity": "250 ml"},
    "4": {"name": "Mixed Nuts (Almonds, Walnuts, Cashews)", "calories": 160, "protein": 5, "carbs": 6, "fat": 14, "quantity": "25 grams"},
    "5": {"name": "Oatmeal with Milk", "calories": 200, "protein": 8, "carbs": 30, "fat": 5, "quantity": "50 grams oats + 150 ml milk"},
    "6": {"name": "Fresh Fruit Salad", "calories": 90, "protein": 1, "carbs": 22, "fat": 0, "quantity": "150 grams"},
    "7": {"name": "Steamed Mixed Vegetables", "calories": 110, "protein": 5, "carbs": 15, "fat": 4, "quantity": "200 grams"},
    "8": {"name": "Green Tea (Unsweetened)", "calories": 2, "protein": 0, "carbs": 0, "fat": 0, "quantity": "200 ml"},
    "9": {"name": "Vegetable Soup (Clear)", "calories": 80, "protein": 4, "carbs": 10, "fat": 3, "quantity": "250 ml"},
    "10": {"name": "Herbal Tea (Chamomile or Tulsi)", "calories": 5, "protein": 0, "carbs": 1, "fat": 0, "quantity": "200 ml"}
}
    
    default = slot_defaults.get(slot_id, {"name": "Default Food", "calories": 100, "protein": 5, "carbs": 15, "fat": 3, "quantity": "100 grams"})
    
    return {
        "id": uuid.uuid4().hex,  # <--- FIXED: Uses the locally generated ID
        "name": default["name"],
        "quantity": default["quantity"],
        "calories": default["calories"],
        "protein": default["protein"],
        "carbs": default["carbs"],
        "fat": default["fat"],
        "fiber": 2,
        "sugar": 5,
        "image_url": ""
    }

async def generate_indian_7_days_parallel_fast(
    profile: Dict,
    diet_type: str = "vegetarian",
    cuisine_type: Optional[str] = None,
    avoid_foods: Optional[List[str]] = None,
    prefer_foods: Optional[List[str]] = None,
    health_conditions: Optional[List[str]] = None,
    db: Session = None,
    oai: Optional[AsyncOpenAI] = None
) -> Dict:
    """
    Generate 7-day meal plan using Indian food database
    Fast generation in 10-20 seconds

    Compatible with existing chatbot - accepts same parameters as old generator

    Args:
        profile: User profile with target_calories, client_goal, etc.
        diet_type: vegetarian, non-vegetarian, vegan, etc.
        cuisine_type: North Indian, South Indian, Common
        avoid_foods: Foods to avoid (optional)
        prefer_foods: Preferred foods (optional)
        health_conditions: Health conditions list (optional)
        db: Database session
        oai: OpenAI client (optional, for AI-powered selection)

    Returns:
        7-day meal plan dictionary
    """

    # Extract daily calories from profile
    daily_calories = profile.get("target_calories", 2000)

    # Extract health condition from list if provided
    health_condition = None
    if health_conditions and len(health_conditions) > 0:
        health_condition = health_conditions[0]

    # Use cuisine_type instead of cuisine for consistency
    cuisine = cuisine_type

    print(f"🍛 Generating Indian 7-day meal plan from indian_food_master DB...")
    print(f"  Calories: {daily_calories}, Diet: {diet_type}, Cuisine: {cuisine or 'All'}")

    # Meal slots configuration
    meal_slots = [
        {"id": "1", "title": "Pre workout", "tagline": "Energy boost", "timeRange": "6:30-7:00 AM"},
        {"id": "2", "title": "Post workout", "tagline": "Recovery fuel", "timeRange": "7:30-8:00 AM"},
        {"id": "3", "title": "Early morning Detox", "tagline": "Early morning nutrition", "timeRange": "5:30-6:00 AM"},
        {"id": "4", "title": "Pre-Breakfast / Pre-Meal Starter", "tagline": "Pre-breakfast fuel", "timeRange": "7:00-7:30 AM"},
        {"id": "5", "title": "Breakfast", "tagline": "Start your day right", "timeRange": "8:30-9:30 AM"},
        {"id": "6", "title": "Mid-Morning snack", "tagline": "Healthy meal", "timeRange": "10:00-11:00 AM"},
        {"id": "7", "title": "Lunch", "tagline": "Nutritious midday meal", "timeRange": "1:00-2:00 PM"},
        {"id": "8", "title": "Evening snack", "tagline": "Healthy meal", "timeRange": "4:00-5:00 PM"},
        {"id": "9", "title": "Dinner", "tagline": "End your day well", "timeRange": "7:30-8:30 PM"},
        {"id": "10", "title": "Bed time", "tagline": "Rest well", "timeRange": "9:30-10:00 PM"}
    ]

    days = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]

    meal_plan = {}

    # Track used foods to ensure variety across days
    # Format: {slot_id: {day1: [food_ids], day2: [food_ids], ...}}
    used_foods_tracker: Dict[str, Dict[str, Set[int]]] = {str(i): {} for i in range(1, 11)}

    for day_index, day in enumerate(days):
        day_meals = []

        # Track foods used TODAY across ALL slots to prevent duplicates within same day
        today_used_foods: Set[int] = set()

        # CRITICAL FIX: Track drinks/beverages separately to prevent repetition
        # Common drink keywords to identify beverages
        today_used_drink_types: Set[str] = set()
        day_total_calories = 0  # Track total calories for the day
        for slot in meal_slots:
            slot_id = slot["id"]
            target_calories = calculate_slot_calories(slot_id, daily_calories)

            # Get foods for this slot (get more to have variety)
            foods = get_indian_foods_from_db(
                db=db,
                slot_id=slot_id,
                diet_type=diet_type,
                cuisine=cuisine,
                health_condition=health_condition,
                target_calories=target_calories,
                limit=50  # Get more options for variety
            )

            # Randomize the food list
            if foods:
                foods = list(foods)  # Convert to list if not already
                random.shuffle(foods)

            # Get previously used food IDs for this slot (from previous days)
            previously_used = set()
            for prev_day in range(day_index):
                prev_day_name = days[prev_day]
                if prev_day_name in used_foods_tracker[slot_id]:
                    previously_used.update(used_foods_tracker[slot_id][prev_day_name])

            # Build food list - prefer foods not used in previous days OR today
            food_list = []
            slot_total_calories = 0
            current_slot_foods = set()
            slot_has_drink = False  # NEW: Track if this slot already has a drink

            # First pass: try foods not used recently (last 2 days) AND not used today
            recent_used = set()
            for prev_day in range(max(0, day_index - 2), day_index):
                prev_day_name = days[prev_day]
                if prev_day_name in used_foods_tracker[slot_id]:
                    recent_used.update(used_foods_tracker[slot_id][prev_day_name])

            for food in foods:
                if len(food_list) >= 2:  # Max 2 items per slot
                    break
                if slot_total_calories >= target_calories:
                    break

                # CRITICAL: Skip if food already used today in another slot
                if food.id in today_used_foods:
                    continue

                # CRITICAL FIX: Universal diet compliance validation
                # This ensures strict adherence to ALL diet restrictions
                if not is_food_diet_compliant(food.food_name, diet_type):
                    continue  # Skip foods that violate diet restrictions

                # NEW: Use database is_liquid column to detect drinks/beverages
                is_drink = food.is_liquid

                if is_drink:
                    # CRITICAL FIX: Only allow ONE drink per slot
                    if slot_has_drink:
                        continue  # Skip this drink - slot already has a drink

                    # Extract drink type from food name for tracking across slots
                    food_name_lower = food.food_name.lower()
                    drink_type = None
                    for keyword in ['tea', 'coffee', 'chai', 'milk', 'juice', 'lassi',
                                   'buttermilk', 'smoothie', 'shake']:
                        if keyword in food_name_lower:
                            drink_type = keyword
                            break

                    # Skip if this drink type already used today in another slot
                    if drink_type and drink_type in today_used_drink_types:
                        continue

                # Prefer foods not used recently
                if food.id in recent_used:
                    continue

                food_item = {
                    "id": str(food.id),
                    "name": food.food_name,
                    "quantity": food.quantity,
                    "calories": round(food.calories, 1),
                    "protein": round(food.protein, 1),
                    "carbs": round(food.carbs, 1),
                    "fat": round(food.fat, 1),
                    "fiber": round(food.fiber, 1),
                    "sugar": round(food.sugar, 1),
                    "image_url": food.image_url or ""
                }

                food_list.append(food_item)
                slot_total_calories += food.calories
                current_slot_foods.add(food.id)
                today_used_foods.add(food.id)  # Track for whole day

                # Track drink type if it's a drink
                if is_drink and drink_type:
                    today_used_drink_types.add(drink_type)
                    slot_has_drink = True  # Mark slot as having a drink

            # Second pass: if we don't have enough foods, use any available (but still not used today)
            if len(food_list) < 2:
                for food in foods:
                    if len(food_list) >= 2:
                        break
                    if slot_total_calories >= target_calories:
                        break
                    if food.id in current_slot_foods:  # Already added in this slot
                        continue

                    # CRITICAL: Skip if food already used today in another slot
                    if food.id in today_used_foods:
                        continue

                    # CRITICAL FIX: Universal diet compliance validation in second pass
                    if not is_food_diet_compliant(food.food_name, diet_type):
                        continue

                    # NEW: Use database is_liquid column in second pass too
                    is_drink = food.is_liquid

                    if is_drink:
                        # CRITICAL FIX: Only allow ONE drink per slot
                        if slot_has_drink:
                            continue  # Skip this drink - slot already has a drink

                        # Extract drink type from food name for tracking across slots
                        food_name_lower = food.food_name.lower()
                        drink_type = None
                        for keyword in ['tea', 'coffee', 'chai', 'milk', 'juice', 'lassi',
                                       'buttermilk', 'smoothie', 'shake']:
                            if keyword in food_name_lower:
                                drink_type = keyword
                                break

                        if drink_type and drink_type in today_used_drink_types:
                            continue

                    food_item = {
                        "id": str(food.id),
                        "name": food.food_name,
                        "quantity": food.quantity,
                        "calories": round(food.calories, 1),
                        "protein": round(food.protein, 1),
                        "carbs": round(food.carbs, 1),
                        "fat": round(food.fat, 1),
                        "fiber": round(food.fiber, 1),
                        "sugar": round(food.sugar, 1),
                        "image_url": food.image_url or ""
                    }

                    food_list.append(food_item)
                    slot_total_calories += food.calories
                    current_slot_foods.add(food.id)
                    today_used_foods.add(food.id)  # Track for whole day

                    if is_drink and drink_type:
                        today_used_drink_types.add(drink_type)
                        slot_has_drink = True  # Mark slot as having a drink

            # Track used foods for this day and slot
            if not food_list:
                # Create a default food item for this slot
                default_food = create_default_food_for_slot(slot_id, diet_type)
                food_list.append(default_food)
                slot_total_calories += default_food["calories"]

            # Track used foods for this day and slot
            used_foods_tracker[slot_id][day] = current_slot_foods

            # Build meal slot
            meal_slot = {
                "id": slot_id,
                "title": slot["title"],
                "tagline": slot["tagline"],
                "timeRange": slot["timeRange"],
                "foodList": food_list,
                "itemsCount": len(food_list)
            }

            day_meals.append(meal_slot)
            day_total_calories += slot_total_calories

                # FIX: Adjust calories to match target
        day_total_target = daily_calories
        if day_total_calories > 0:
            adjustment_factor = day_total_target / day_total_calories
            # Only adjust if the difference is significant (more than 10%)
            if abs(adjustment_factor - 1.0) > 0.1:
                for meal_slot in day_meals:
                    for food_item in meal_slot["foodList"]:
                        food_item["calories"] = round(food_item["calories"] * adjustment_factor, 1)
                        food_item["protein"] = round(food_item["protein"] * adjustment_factor, 1)
                        food_item["carbs"] = round(food_item["carbs"] * adjustment_factor, 1)
                        food_item["fat"] = round(food_item["fat"] * adjustment_factor, 1)

        meal_plan[day] = day_meals

    print(f"✅ Generated 7-day Indian meal plan")
    return meal_plan


# ============================================================================
# HELPER FUNCTION FOR BACKWARDS COMPATIBILITY
# ============================================================================

def convert_indian_fast_results_to_meal_data(fast_results) -> Dict:
    """
    Convert fast results to the expected meal data format for chatbot

    Input can be:
    - Dict: {"monday": [...meal slots...], "tuesday": [...], ...} (full 7-day plan)
    - List: [...meal slots...] (single day's meal slots)

    Output format: {"meals": [...all meal slots...], "total_target_calories": X}
    """

    # Handle both list and dict inputs
    if isinstance(fast_results, list):
        # It's already a list of meal slots for one day
        meal_slots = fast_results
    elif isinstance(fast_results, dict):
        # It's a full week plan, get Monday's meals
        meal_slots = fast_results.get("monday", [])
    else:
        # Fallback
        meal_slots = []

    # Calculate total calories
    total_calories = 0
    all_meals = []

    for slot in meal_slots:
        # Convert to the format expected by convert_ai_meal_to_template
        foods = []
        slot_id = slot.get("id", "1")

        for food in slot.get("foodList", []):
            foods.append({
                "name": food.get("name", ""),
                "calories": food.get("calories", 0),
                "protein": food.get("protein", 0),
                "carbs": food.get("carbs", 0),
                "fat": food.get("fat", 0),
                "quantity": food.get("quantity", ""),
                "fiber": food.get("fiber", 0),
                "sugar": food.get("sugar", 0)
            })
            total_calories += food.get("calories", 0)

        all_meals.append({
            "slot_id": slot_id,
            "foods": foods
        })

    result = {
        "meals": all_meals,
        "total_target_calories": total_calories
    }

    # If input was full week, include it
    if isinstance(fast_results, dict) and "monday" in fast_results:
        result["full_week_plan"] = fast_results

    return result


# ============================================================================
# AI INTENT UNDERSTANDING (Compatible with existing chatbot)
# ============================================================================

async def ai_understand_indian_user_preferences(
    user_message: str,
    oai: AsyncOpenAI,
    current_profile: Optional[Dict] = None
) -> Dict:
    """
    Use AI to understand user's diet preferences from their message
    Compatible with existing chatbot flow

    DEPRECATED: Use ai_understand_indian_user_preferences_celery() for rate-limited Celery version
    """

    system_prompt = """You are a diet preference analyzer for Indian food.
Extract diet preferences from the user's message.

Respond ONLY with JSON in this exact format:
{
  "diet_type": "vegetarian|non-vegetarian|vegan|eggetarian|jain|paleo|ketogenic",
  "cuisine": "North Indian|South Indian|Common",
  "health_condition": "diabetes|weight loss|muscle gain|heart health|null",
  "daily_calories": number or null
}

Examples:
User: "I want vegetarian food" → {"diet_type": "vegetarian", "cuisine": "Common", "health_condition": null, "daily_calories": null}
User: "North Indian non-veg for muscle gain" → {"diet_type": "non-vegetarian", "cuisine": "North Indian", "health_condition": "muscle gain", "daily_calories": null}
"""

    try:
        response = await oai.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_message}
            ],
            response_format={"type": "json_object"},
            temperature=0
        )

        preferences = json.loads(response.choices[0].message.content)

        # Merge with current profile if provided
        if current_profile:
            preferences = {**current_profile, **preferences}

        return preferences

    except Exception as e:
        print(f"Error understanding preferences: {e}")
        return {
            "diet_type": "vegetarian",
            "cuisine": "Common",
            "health_condition": None,
            "daily_calories": None
        }


async def ai_understand_indian_user_preferences_celery(
    user_id: int,
    user_message: str,
    current_profile: Optional[Dict] = None
) -> Dict:
    """
    Use AI to understand user's diet preferences from their message
    Uses Celery+Redis for rate limiting and async processing

    Args:
        user_id: Client ID
        user_message: User's message containing diet preferences
        current_profile: Current profile to merge with extracted preferences

    Returns:
        dict: Extracted preferences (diet_type, cuisine, health_condition, daily_calories)
    """
    from app.tasks.meal_tasks import understand_user_preferences
    from celery.result import AsyncResult

    try:
        # Queue the task
        task = understand_user_preferences.delay(
            user_id=user_id,
            user_message=user_message,
            current_profile=current_profile or {}
        )

        print(f"🍛 Queued meal preference task {task.id} for user {user_id}")

        # Poll for result with timeout
        max_wait = 30  # 30 seconds timeout
        poll_interval = 0.3  # Check every 300ms
        elapsed = 0

        while elapsed < max_wait:
            celery_task = AsyncResult(task.id)

            if celery_task.ready():
                if celery_task.successful():
                    result = celery_task.result
                    print(f"✅ Meal preference task completed for user {user_id}")
                    return result
                else:
                    # Task failed, use fallback
                    print(f"❌ Meal preference task failed: {celery_task.info}")
                    break

            await asyncio.sleep(poll_interval)
            elapsed += poll_interval

        # Timeout or failure - return fallback
        print(f"⏱️ Meal preference task timeout/failed for user {user_id}, using fallback")
        fallback = {
            "diet_type": "vegetarian",
            "cuisine": "Common",
            "health_condition": None,
            "daily_calories": None
        }
        if current_profile:
            fallback = {**current_profile, **fallback}
        return fallback

    except Exception as e:
        print(f"Error in ai_understand_indian_user_preferences_celery: {e}")
        fallback = {
            "diet_type": "vegetarian",
            "cuisine": "Common",
            "health_condition": None,
            "daily_calories": None
        }
        if current_profile:
            fallback = {**current_profile, **fallback}
        return fallback


# ============================================================================
# FOOD ALTERNATIVES (For swapping)
# ============================================================================

async def generate_indian_food_alternatives(
    db: Session,
    food_name: str,
    diet_type: str = "vegetarian",
    cuisine: Optional[str] = None,
    oai: Optional[AsyncOpenAI] = None
) -> List[Dict]:
    """
    Generate food alternatives using the food swap engine
    """
    # Use the local FoodSwapEngine class (no external import needed)
    alternatives = find_food_alternatives(
        db=db,
        food_name=food_name,
        diet_type=diet_type,
        cuisine=cuisine,
        limit=5
    )

    return alternatives


# ============================================================================
# SECTION 4: FOOD SWAP ENGINE
# ============================================================================

class FoodSwapEngine:
    """
    Intelligent food swapping system
    Matches foods based on nutritional similarity, diet type, cuisine, and meal slot
    """

    def __init__(self, db: Session):
        self.db = db

    def calculate_nutritional_similarity(
        self,
        food1: IndianFoodMaster,
        food2: IndianFoodMaster
    ) -> float:
        """
        Calculate similarity score between two foods (0-100, higher is more similar)
        Based on:
        - Calorie difference (40% weight)
        - Protein difference (30% weight)
        - Carbs difference (15% weight)
        - Fat difference (15% weight)
        """

        # Calculate percentage differences
        cal_diff = abs(food1.calories - food2.calories) / max(food1.calories, food2.calories, 1)
        protein_diff = abs(food1.protein - food2.protein) / max(food1.protein, food2.protein, 1)
        carbs_diff = abs(food1.carbs - food2.carbs) / max(food1.carbs, food2.carbs, 1)
        fat_diff = abs(food1.fat - food2.fat) / max(food1.fat, food2.fat, 1)

        # Calculate similarity scores (inverse of difference)
        cal_similarity = max(0, 100 - (cal_diff * 100))
        protein_similarity = max(0, 100 - (protein_diff * 100))
        carbs_similarity = max(0, 100 - (carbs_diff * 100))
        fat_similarity = max(0, 100 - (fat_diff * 100))

        # Weighted average
        total_similarity = (
            cal_similarity * 0.40 +
            protein_similarity * 0.30 +
            carbs_similarity * 0.15 +
            fat_similarity * 0.15
        )

        return total_similarity

    def get_meal_slot_columns(self, food: IndianFoodMaster) -> List[str]:
        """Get all meal slots that this food is suitable for"""
        slots = []

        if food.suitable_for_early_morning:
            slots.append("suitable_for_early_morning")
        if food.suitable_for_pre_breakfast:
            slots.append("suitable_for_pre_breakfast")
        if food.suitable_for_breakfast:
            slots.append("suitable_for_breakfast")
        if food.suitable_for_mid_morning:
            slots.append("suitable_for_mid_morning")
        if food.suitable_for_lunch:
            slots.append("suitable_for_lunch")
        if food.suitable_for_evening_snack:
            slots.append("suitable_for_evening_snack")
        if food.suitable_for_pre_workout:
            slots.append("suitable_for_pre_workout")
        if food.suitable_for_post_workout:
            slots.append("suitable_for_post_workout")
        if food.suitable_for_dinner:
            slots.append("suitable_for_dinner")
        if food.suitable_for_bedtime:
            slots.append("suitable_for_bedtime")

        return slots

    def build_swap_query(
        self,
        original_food: IndianFoodMaster,
        diet_type: Optional[str] = None,
        cuisine: Optional[str] = None
    ):
        """Build query to find swap candidates"""

        query = self.db.query(IndianFoodMaster).filter(
            IndianFoodMaster.is_active == True,
            IndianFoodMaster.id != original_food.id  # Exclude the original food
        )

        # Match at least one meal slot
        meal_slots = self.get_meal_slot_columns(original_food)
        if meal_slots:
            slot_filters = []
            for slot in meal_slots:
                slot_filters.append(getattr(IndianFoodMaster, slot) == True)

            query = query.filter(or_(*slot_filters))

        # Match diet type compatibility
        if diet_type:
            diet_type_lower = diet_type.lower()

            if diet_type_lower == "vegetarian":
                query = query.filter(IndianFoodMaster.is_vegetarian == True)
            elif diet_type_lower in ["non-vegetarian", "non_vegetarian", "nonveg"]:
                query = query.filter(
                    or_(
                        IndianFoodMaster.is_vegetarian == True,
                        IndianFoodMaster.is_non_vegetarian == True,
                        IndianFoodMaster.is_eggetarian == True
                    )
                )
            elif diet_type_lower == "vegan":
                query = query.filter(IndianFoodMaster.is_vegan == True)
            elif diet_type_lower == "eggetarian":
                query = query.filter(
                    or_(
                        IndianFoodMaster.is_vegetarian == True,
                        IndianFoodMaster.is_eggetarian == True
                    )
                )
            elif diet_type_lower == "jain":
                query = query.filter(IndianFoodMaster.is_jain == True)
            elif diet_type_lower == "paleo":
                query = query.filter(IndianFoodMaster.is_paleo == True)
            elif diet_type_lower in ["ketogenic", "keto"]:
                query = query.filter(IndianFoodMaster.is_ketogenic == True)

        # Match cuisine if specified
        if cuisine:
            cuisine_lower = cuisine.lower()
            if "north" in cuisine_lower:
                query = query.filter(
                    or_(
                        IndianFoodMaster.cuisine_type == "North Indian",
                        IndianFoodMaster.cuisine_type == "Common"
                    )
                )
            elif "south" in cuisine_lower:
                query = query.filter(
                    or_(
                        IndianFoodMaster.cuisine_type == "South Indian",
                        IndianFoodMaster.cuisine_type == "Common"
                    )
                )

        # Match similar calories (±50%)
        calorie_min = original_food.calories * 0.5
        calorie_max = original_food.calories * 1.5

        query = query.filter(
            and_(
                IndianFoodMaster.calories >= calorie_min,
                IndianFoodMaster.calories <= calorie_max
            )
        )

        return query

    def find_food_swaps(
        self,
        food_id: int,
        diet_type: Optional[str] = None,
        cuisine: Optional[str] = None,
        limit: int = 5
    ) -> List[Dict]:
        """
        Find suitable food swaps for a given food item

        Args:
            food_id: ID of the food to swap
            diet_type: User's diet type
            cuisine: User's cuisine preference
            limit: Maximum number of alternatives to return

        Returns:
            List of alternative foods with similarity scores
        """

        # Get original food
        original_food = self.db.query(IndianFoodMaster).filter(
            IndianFoodMaster.id == food_id
        ).first()

        if not original_food:
            return []

        # Build query for swap candidates
        query = self.build_swap_query(
            original_food=original_food,
            diet_type=diet_type,
            cuisine=cuisine
        )

        # Get all candidates
        candidates = query.all()

        # Calculate similarity scores
        scored_candidates = []
        for candidate in candidates:
            similarity_score = self.calculate_nutritional_similarity(
                original_food,
                candidate
            )

            scored_candidates.append({
                "food": candidate,
                "similarity_score": similarity_score
            })

        # Sort by similarity score (highest first)
        scored_candidates.sort(key=lambda x: x["similarity_score"], reverse=True)

        # Format results
        results = []
        for item in scored_candidates[:limit]:
            food = item["food"]
            results.append({
                "id": food.id,
                "food_name": food.food_name,
                "food_name_hindi": food.food_name_hindi,
                "quantity": food.quantity,
                "calories": round(food.calories, 1),
                "protein": round(food.protein, 1),
                "carbs": round(food.carbs, 1),
                "fat": round(food.fat, 1),
                "fiber": round(food.fiber, 1),
                "sugar": round(food.sugar, 1),
                "cuisine_type": food.cuisine_type,
                "category": food.category,
                "similarity_score": round(item["similarity_score"], 1),
                "image_url": food.image_url or ""
            })

        return results

    def swap_food_in_meal_plan(
        self,
        meal_plan: Dict,
        day: str,
        meal_slot_id: str,
        old_food_id: int,
        new_food_id: int
    ) -> Dict:
        """
        Swap a food item in an existing meal plan

        Args:
            meal_plan: The complete meal plan dictionary
            day: Day of week (e.g., "monday")
            meal_slot_id: ID of the meal slot
            old_food_id: ID of food to replace
            new_food_id: ID of new food

        Returns:
            Updated meal plan
        """

        # Get new food details
        new_food = self.db.query(IndianFoodMaster).filter(
            IndianFoodMaster.id == new_food_id
        ).first()

        if not new_food:
            return meal_plan

        # Find and replace in meal plan
        if day in meal_plan:
            for meal_slot in meal_plan[day]:
                if meal_slot["id"] == meal_slot_id:
                    # Find and replace the food item
                    for i, food_item in enumerate(meal_slot["foodList"]):
                        if str(food_item["id"]) == str(old_food_id):
                            # Replace with new food
                            meal_slot["foodList"][i] = {
                                "id": str(new_food.id),
                                "name": new_food.food_name,
                                "quantity": new_food.quantity,
                                "calories": round(new_food.calories, 1),
                                "protein": round(new_food.protein, 1),
                                "carbs": round(new_food.carbs, 1),
                                "fat": round(new_food.fat, 1),
                                "fiber": round(new_food.fiber, 1),
                                "sugar": round(new_food.sugar, 1),
                                "image_url": new_food.image_url or ""
                            }

                            # Recalculate totals
                            total_calories = sum(f["calories"] for f in meal_slot["foodList"])
                            total_protein = sum(f["protein"] for f in meal_slot["foodList"])
                            total_carbs = sum(f["carbs"] for f in meal_slot["foodList"])
                            total_fat = sum(f["fat"] for f in meal_slot["foodList"])

                            meal_slot["totalCalories"] = round(total_calories, 1)
                            meal_slot["totalProtein"] = round(total_protein, 1)
                            meal_slot["totalCarbs"] = round(total_carbs, 1)
                            meal_slot["totalFat"] = round(total_fat, 1)

                            break

        return meal_plan


def find_food_alternatives(
    db: Session,
    food_name: str,
    diet_type: str = "Vegetarian",
    cuisine: Optional[str] = None,
    limit: int = 5
) -> List[Dict]:
    """
    Find food alternatives by food name

    Args:
        db: Database session
        food_name: Name of the food to find alternatives for
        diet_type: User's diet type
        cuisine: User's cuisine preference
        limit: Maximum number of alternatives

    Returns:
        List of alternative foods
    """

    # Find the food by name
    food = db.query(IndianFoodMaster).filter(
        IndianFoodMaster.food_name.ilike(f"%{food_name}%")
    ).first()

    if not food:
        return []

    # Use swap engine to find alternatives
    swap_engine = FoodSwapEngine(db)
    return swap_engine.find_food_swaps(
        food_id=food.id,
        diet_type=diet_type,
        cuisine=cuisine,
        limit=limit
    )


# ============================================================================
# SECTION 5: ADDITIONAL MEAL PLANNING UTILITIES
# ============================================================================

# Meal slot configuration with calorie distribution (for API endpoints)
MEAL_SLOTS_CONFIG = {
    "early_morning": {
        "title": "Early Morning Detox",
        "tagline": "Early morning nutrition",
        "timeRange": "5:30-6:00 AM",
        "calorie_percent": 3,
        "column": "suitable_for_early_morning"
    },
    "pre_breakfast": {
        "title": "Pre-Breakfast / Pre-Meal Starter",
        "tagline": "Pre-breakfast fuel",
        "timeRange": "7:00-7:30 AM",
        "calorie_percent": 5,
        "column": "suitable_for_pre_breakfast"
    },
    "breakfast": {
        "title": "Breakfast",
        "tagline": "Start your day right",
        "timeRange": "8:30-9:30 AM",
        "calorie_percent": 25,
        "column": "suitable_for_breakfast"
    },
    "mid_morning": {
        "title": "Mid-Morning snack",
        "tagline": "Healthy meal",
        "timeRange": "10:00-11:00 AM",
        "calorie_percent": 7,
        "column": "suitable_for_mid_morning"
    },
    "lunch": {
        "title": "Lunch",
        "tagline": "Nutritious midday meal",
        "timeRange": "1:00-2:00 PM",
        "calorie_percent": 30,
        "column": "suitable_for_lunch"
    },
    "evening_snack": {
        "title": "Evening snack",
        "tagline": "Healthy meal",
        "timeRange": "4:00-5:00 PM",
        "calorie_percent": 7,
        "column": "suitable_for_evening_snack"
    },
    "pre_workout": {
        "title": "Pre workout",
        "tagline": "Energy boost",
        "timeRange": "6:30-7:00 PM",
        "calorie_percent": 5,
        "column": "suitable_for_pre_workout"
    },
    "post_workout": {
        "title": "Post workout",
        "tagline": "Recovery fuel",
        "timeRange": "7:30-8:00 PM",
        "calorie_percent": 5,
        "column": "suitable_for_post_workout"
    },
    "dinner": {
        "title": "Dinner",
        "tagline": "End your day well",
        "timeRange": "8:30-9:00 PM",
        "calorie_percent": 20,
        "column": "suitable_for_dinner"
    },
    "bedtime": {
        "title": "Bed time",
        "tagline": "Rest well",
        "timeRange": "9:30-10:00 PM",
        "calorie_percent": 3,
        "column": "suitable_for_bedtime"
    }
}

DAYS_OF_WEEK = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]


# ============================================================================
# WRAPPER FUNCTIONS FOR API COMPATIBILITY
# ============================================================================

def generate_indian_meal_plan(
    db: Session,
    daily_calories: float = 2000,
    diet_type: str = "Vegetarian",
    cuisine: Optional[str] = "Common",
    health_condition: Optional[str] = None
) -> Dict:
    """
    Wrapper function for API compatibility
    Generates 7-day Indian meal plan (synchronous version)

    This is a compatibility wrapper around generate_indian_7_days_parallel_fast
    for API endpoints that expect a synchronous function

    Args:
        db: Database session
        daily_calories: Target daily calories (default: 2000)
        diet_type: Diet type (Vegetarian, Non-Vegetarian, Vegan, Eggetarian, Jain, Paleo, Ketogenic)
        cuisine: Cuisine preference (North Indian, South Indian, Common)
        health_condition: Health condition (diabetes, weight loss, muscle gain, heart health, etc.)

    Returns:
        Dict with 7-day meal plan (keys: Monday, Tuesday, etc.)
    """

    import asyncio

    profile = {
        "target_calories": daily_calories,
        "client_goal": health_condition or "general"
    }

    health_conditions = [health_condition] if health_condition else None

    # Run the async function synchronously
    meal_plan = asyncio.run(generate_indian_7_days_parallel_fast(
        profile=profile,
        diet_type=diet_type,
        cuisine_type=cuisine,
        health_conditions=health_conditions,
        db=db
    ))

    # Convert day keys from lowercase to capitalized (Monday, Tuesday, etc.)
    formatted_plan = {}
    for day_key, day_meals in meal_plan.items():
        formatted_plan[day_key.capitalize()] = day_meals

    return formatted_plan