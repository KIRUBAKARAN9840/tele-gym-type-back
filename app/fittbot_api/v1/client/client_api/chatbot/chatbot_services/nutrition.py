from typing import Dict, Any, List

# Enhanced food database with regional foods and categories
FOOD_DB: Dict[str, Dict[str, Any]] = {
    # Indian Breakfast Foods
    "chapati": {"unit":"pc","calories":120,"protein":3,"carbs":22,"fat":2,"fiber":2,"sugar":1,"category":"bread","region":"all"},
    "idli":    {"unit":"pc","calories":58, "protein":2,"carbs":12,"fat":0.3,"fiber":0.8,"sugar":0.2,"category":"breakfast","region":"south"},
    "dosa":    {"unit":"pc","calories":133,"protein":3,"carbs":26,"fat":3,"fiber":1,"sugar":1,"category":"breakfast","region":"south"},
    "poha":    {"unit":"cup","calories":250,"protein":5,"carbs":45,"fat":5,"fiber":3,"sugar":2,"category":"breakfast","region":"west"},
    "upma":    {"unit":"cup","calories":280,"protein":6,"carbs":40,"fat":8,"fiber":4,"sugar":3,"category":"breakfast","region":"south"},
    "paratha": {"unit":"pc","calories":220,"protein":5,"carbs":30,"fat":9,"fiber":3,"sugar":2,"category":"breakfast","region":"north"},
    
    # Main Courses
    "biryani": {"unit":"cup","calories":350,"protein":15,"carbs":50,"fat":12,"fiber":3,"sugar":4,"category":"main","region":"all"},
    "butter chicken": {"unit":"cup","calories":450,"protein":25,"carbs":20,"fat":30,"fiber":2,"sugar":8,"category":"main","region":"north"},
    "palak paneer": {"unit":"cup","calories":280,"protein":18,"carbs":15,"fat":18,"fiber":4,"sugar":5,"category":"main","region":"north"},
    "sambar rice": {"unit":"cup","calories":320,"protein":12,"carbs":55,"fat":8,"fiber":6,"sugar":7,"category":"main","region":"south"},
    "dal rice": {"unit":"cup","calories":300,"protein":10,"carbs":50,"fat":6,"fiber":5,"sugar":3,"category":"main","region":"all"},
    
    # Snacks
    "vada":    {"unit":"pc","calories":110,"protein":3,"carbs":10,"fat":7,"fiber":1,"sugar":0.5,"category":"snack","region":"south"},
    "samosa":  {"unit":"pc","calories":262,"protein":4,"carbs":31,"fat":13,"fiber":3,"sugar":2,"category":"snack","region":"all"},
    "bhel puri": {"unit":"cup","calories":180,"protein":4,"carbs":30,"fat":5,"fiber":4,"sugar":6,"category":"snack","region":"west"},
    "pakora": {"unit":"pc","calories":150,"protein":3,"carbs":15,"fat":8,"fiber":2,"sugar":2,"category":"snack","region":"north"},
    
    # International
    "pizza": {"unit":"slice","calories":285,"protein":12,"carbs":36,"fat":10,"fiber":2,"sugar":4,"category":"fastfood","region":"international"},
    "burger": {"unit":"pc","calories":354,"protein":17,"carbs":35,"fat":16,"fiber":2,"sugar":6,"category":"fastfood","region":"international"},
    "pasta": {"unit":"cup","calories":220,"protein":8,"carbs":35,"fat":5,"fiber":3,"sugar":4,"category":"main","region":"international"},
    
    # Fruits
    "banana": {"unit":"pc","calories":105,"protein":1,"carbs":27,"fat":0.4,"fiber":3,"sugar":14,"category":"fruit","region":"all"},
    "apple":  {"unit":"pc","calories":95,"protein":0.5,"carbs":25,"fat":0.3,"fiber":4,"sugar":19,"category":"fruit","region":"all"},
    "orange": {"unit":"pc","calories":62,"protein":1.2,"carbs":15,"fat":0.2,"fiber":3,"sugar":12,"category":"fruit","region":"all"},
    "mango": {"unit":"pc","calories":150,"protein":1.4,"carbs":35,"fat":0.6,"fiber":3,"sugar":31,"category":"fruit","region":"all"},
    
    # Dairy
    "milk": {"unit":"cup","calories":150,"protein":8,"carbs":12,"fat":8,"fiber":0,"sugar":12,"category":"dairy","region":"all"},
    "curd": {"unit":"cup","calories":150,"protein":8,"carbs":11,"fat":8,"fiber":0,"sugar":11,"category":"dairy","region":"all"},
    "paneer": {"unit":"100g","calories":265,"protein":18,"carbs":4,"fat":20,"fiber":0,"sugar":4,"category":"dairy","region":"all"},
}

# Enhanced macros with portion sizes
MACROS = ["calories", "protein", "carbs", "fat", "fiber", "sugar"]
MICRONUTRIENTS = ["iron", "calcium", "vitamin_c", "vitamin_d", "sodium", "potassium"]

# Food categories for better filtering
FOOD_CATEGORIES = {
    "breakfast": ["idli", "dosa", "poha", "upma", "paratha", "pongal"],
    "main": ["biryani", "curry", "rice", "roti", "naan", "dal"],
    "snacks": ["vada", "samosa", "pakora", "bhel", "chaat"],
    "drinks": ["lassi", "juice", "smoothie", "tea", "coffee"],
    "fruits": ["banana", "apple", "orange", "mango", "papaya"],
    "dairy": ["milk", "curd", "paneer", "yogurt"]
}

# Common portion sizes
PORTION_SIZES = {
    "small": 0.7,
    "medium": 1.0,
    "large": 1.3,
    "extra_large": 1.6
}

def get_foods_by_category(category: str) -> List[str]:
    """Get all foods in a specific category"""
    return [food for food, data in FOOD_DB.items() if data.get("category") == category]

def get_foods_by_region(region: str) -> List[str]:
    """Get all foods from a specific region"""
    return [food for food, data in FOOD_DB.items() if data.get("region") == region]

def get_food_nutrition(food_name: str, quantity: float = 1.0) -> Dict[str, Any]:
    """Get nutrition information for food with quantity"""
    food_data = FOOD_DB.get(food_name.lower())
    if not food_data:
        return {}
    
    nutrition = {}
    for macro in MACROS:
        if macro in food_data:
            nutrition[macro] = round(food_data[macro] * quantity, 2)
    
    return {
        **nutrition,
        "food": food_name,
        "quantity": quantity,
        "unit": food_data.get("unit", "pc")
    }