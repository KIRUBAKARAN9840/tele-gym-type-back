# app/utils/food_validator.py
"""
Food Validation Utility - Uses indian_food_master database + fuzzy matching
Validates food inputs before sending to AI to reject gibberish and accept valid foods
"""
import re
import logging
from difflib import SequenceMatcher
from typing import List, Dict, Tuple, Optional, Set
from functools import lru_cache

logger = logging.getLogger(__name__)

# Common international foods not in Indian database (fallback list)
COMMON_INTERNATIONAL_FOODS = {
    # Dairy
    "cheese", "cheese block", "cheese slice", "cheddar", "mozzarella", "parmesan",
    "butter", "cream", "yogurt", "milk", "cottage cheese", "cream cheese",

    # Meats
    "chicken", "chicken breast", "grilled chicken", "fried chicken", "chicken drumstick",
    "drumstick", "chicken leg", "chicken wing", "chicken thigh", "chicken curry",
    "mutton", "lamb", "beef", "pork", "bacon", "ham", "sausage", "salami",
    "fish", "salmon", "tuna", "cod", "prawns", "shrimp", "crab", "lobster",
    "turkey", "duck", "goat meat", "keema", "mince",

    # Eggs
    "egg", "boiled egg", "fried egg", "scrambled egg", "omelette", "egg white", "egg yolk",

    # Vegetables
    "potato", "tomato", "onion", "carrot", "broccoli", "spinach", "cabbage",
    "cauliflower", "capsicum", "bell pepper", "cucumber", "lettuce", "celery",
    "mushroom", "corn", "peas", "beans", "green beans", "eggplant", "brinjal",
    "beetroot", "radish", "turnip", "zucchini", "asparagus", "artichoke",
    "lady finger", "okra", "bhindi", "drumstick vegetable", "moringa",

    # Fruits
    "apple", "banana", "orange", "mango", "grape", "watermelon", "papaya",
    "pineapple", "strawberry", "blueberry", "raspberry", "kiwi", "pomegranate",
    "guava", "litchi", "lychee", "peach", "plum", "pear", "cherry", "fig",
    "dates", "coconut", "avocado", "dragon fruit", "passion fruit",

    # Grains & Cereals
    "rice", "brown rice", "white rice", "basmati rice", "fried rice",
    "wheat", "oats", "oatmeal", "cornflakes", "cereal", "muesli", "granola",
    "quinoa", "barley", "millet", "ragi", "jowar", "bajra",

    # Breads & Bakery
    "bread", "white bread", "brown bread", "whole wheat bread", "toast",
    "roti", "chapati", "paratha", "naan", "kulcha", "puri", "bhatura",
    "croissant", "bagel", "muffin", "donut", "cake", "pastry", "cookie", "biscuit",

    # Legumes & Pulses
    "dal", "lentils", "chickpeas", "chana", "rajma", "kidney beans",
    "black beans", "moong", "urad", "toor dal", "masoor dal", "chana dal",
    "sprouts", "soybean", "tofu", "tempeh",

    # Indian Foods
    "idli", "dosa", "vada", "sambar", "rasam", "upma", "poha", "pongal",
    "uttapam", "appam", "puttu", "idiyappam", "pesarattu",
    "biryani", "pulao", "khichdi", "curd rice", "lemon rice", "tamarind rice",
    "paneer", "palak paneer", "paneer butter masala", "shahi paneer",
    "butter chicken", "chicken tikka", "tandoori chicken", "kebab",
    "samosa", "pakora", "bhaji", "vada pav", "pav bhaji", "chole bhature",
    "aloo paratha", "gobi paratha", "paneer paratha", "thepla", "dhokla",
    "kachori", "jalebi", "gulab jamun", "rasgulla", "barfi", "ladoo", "halwa",
    "payasam", "kheer", "kulfi", "lassi", "chaas", "buttermilk",
    "raita", "pickle", "chutney", "papad",

    # Chinese/Asian
    "noodles", "fried noodles", "chow mein", "hakka noodles", "ramen",
    "spring roll", "manchurian", "chilli chicken", "sweet corn soup",
    "dim sum", "momos", "dumpling", "sushi", "pad thai",

    # Italian
    "pizza", "pasta", "spaghetti", "macaroni", "lasagna", "risotto",
    "garlic bread", "bruschetta", "tiramisu",

    # Mexican
    "burrito", "taco", "quesadilla", "nachos", "guacamole", "salsa",

    # Fast Food
    "burger", "hamburger", "cheeseburger", "veggie burger",
    "french fries", "fries", "potato chips", "chips",
    "hot dog", "sandwich", "sub", "wrap", "roll",

    # Beverages
    "coffee", "tea", "green tea", "chai", "milk tea", "smoothie",
    "juice", "orange juice", "apple juice", "mango juice", "coconut water",
    "lemonade", "soda", "cola", "milkshake", "protein shake",

    # Snacks
    "popcorn", "nuts", "almonds", "cashews", "peanuts", "walnuts", "pistachios",
    "raisins", "dried fruits", "trail mix", "energy bar", "protein bar",

    # Desserts & Sweets
    "ice cream", "chocolate", "candy", "brownie", "pudding", "custard",
    "cheesecake", "pie", "tart", "mousse",

    # Condiments & Others
    "honey", "sugar", "salt", "pepper", "ketchup", "mayonnaise", "mustard",
    "olive oil", "coconut oil", "ghee", "butter oil",
}

# Words that should NEVER be accepted (harmful/non-food)
BLOCKED_WORDS = {
    "poison", "kerosene", "bleach", "detergent", "soap", "shampoo",
    "chemical", "pesticide", "insecticide", "fuel", "petrol", "diesel",
    "paint", "thinner", "acid", "alkali", "plastic", "paper", "metal",
    "wood", "glass", "rubber", "drug", "medicine", "tablet", "pill",
    "injection", "cocaine", "heroin", "meth", "weed", "marijuana",
}

# Minimum word length to consider (filters out single random characters)
MIN_WORD_LENGTH = 2

# Fuzzy match threshold (0.0 to 1.0) - higher = stricter
FUZZY_THRESHOLD = 0.75


class FoodValidator:
    """Validates food inputs using database + fuzzy matching"""

    def __init__(self, db_session=None):
        self.db_session = db_session
        self._db_foods: Optional[Set[str]] = None
        self._all_foods: Optional[Set[str]] = None

    def _load_db_foods(self) -> Set[str]:
        """Load food names from indian_food_master and fittbot_food tables"""
        if self._db_foods is not None:
            return self._db_foods

        self._db_foods = set()

        if self.db_session:
            # Load from indian_food_master table
            try:
                from app.models.fittbot_models import IndianFoodMaster

                foods = self.db_session.query(
                    IndianFoodMaster.food_name,
                    IndianFoodMaster.food_name_hindi,
                    IndianFoodMaster.food_name_regional
                ).filter(
                    IndianFoodMaster.is_active == True
                ).all()

                for food in foods:
                    if food.food_name:
                        self._db_foods.add(food.food_name.lower().strip())
                    if food.food_name_hindi:
                        self._db_foods.add(food.food_name_hindi.lower().strip())
                    if food.food_name_regional:
                        self._db_foods.add(food.food_name_regional.lower().strip())

                logger.info(f"[FoodValidator] Loaded {len(self._db_foods)} foods from indian_food_master")
            except Exception as e:
                logger.warning(f"[FoodValidator] Could not load from indian_food_master: {e}")

            # Load from fittbot_food table
            try:
                from app.models.fittbot_models import Food

                fittbot_foods = self.db_session.query(Food.item).all()

                fittbot_count = 0
                for food in fittbot_foods:
                    if food.item:
                        self._db_foods.add(food.item.lower().strip())
                        fittbot_count += 1

                logger.info(f"[FoodValidator] Loaded {fittbot_count} foods from fittbot_food")
            except Exception as e:
                logger.warning(f"[FoodValidator] Could not load from fittbot_food: {e}")

            logger.info(f"[FoodValidator] Total unique foods from database: {len(self._db_foods)}")

        return self._db_foods

    def _get_all_foods(self) -> Set[str]:
        """Get combined set of database foods + international foods"""
        if self._all_foods is not None:
            return self._all_foods

        db_foods = self._load_db_foods()
        self._all_foods = db_foods | {f.lower() for f in COMMON_INTERNATIONAL_FOODS}
        return self._all_foods

    def _normalize_text(self, text: str) -> str:
        """Normalize food text for matching"""
        # Lowercase, remove extra spaces, remove special chars except space
        text = text.lower().strip()
        text = re.sub(r'[^\w\s]', '', text)
        text = re.sub(r'\s+', ' ', text)
        return text

    def _fuzzy_match(self, input_text: str, known_foods: Set[str]) -> Tuple[bool, Optional[str], float]:
        """
        Check if input fuzzy matches any known food
        Returns: (is_match, matched_food, confidence)
        """
        normalized = self._normalize_text(input_text)

        # First, check exact match
        if normalized in known_foods:
            return True, normalized, 1.0

        # Check if input contains a known food
        for food in known_foods:
            if food in normalized or normalized in food:
                return True, food, 0.95

        # Fuzzy match using SequenceMatcher
        best_match = None
        best_score = 0.0

        for food in known_foods:
            # Skip very short foods for fuzzy matching
            if len(food) < 3:
                continue

            score = SequenceMatcher(None, normalized, food).ratio()
            if score > best_score:
                best_score = score
                best_match = food

        if best_score >= FUZZY_THRESHOLD:
            return True, best_match, best_score

        return False, None, best_score

    def _is_blocked(self, text: str) -> bool:
        """Check if text contains blocked/harmful words"""
        normalized = self._normalize_text(text)
        words = normalized.split()

        for word in words:
            if word in BLOCKED_WORDS:
                return True

        return False

    def _is_gibberish(self, text: str) -> bool:
        """
        Detect if text is likely gibberish
        - Too short
        - No vowels
        - Repeated characters
        - Random pattern
        """
        normalized = self._normalize_text(text)

        # Too short
        if len(normalized) < MIN_WORD_LENGTH:
            return True

        # Only numbers or special chars
        if not re.search(r'[a-z]', normalized):
            return True

        # Check for vowels (most real words have vowels)
        vowels = set('aeiou')
        words = normalized.split()
        for word in words:
            if len(word) > 3 and not any(c in vowels for c in word):
                return True

        # Repeated characters (like "aaaa", "xxxx")
        if re.search(r'(.)\1{3,}', normalized):
            return True

        # Check for common gibberish patterns
        gibberish_patterns = [
            r'^[bcdfghjklmnpqrstvwxyz]{4,}$',  # Only consonants
            r'^[aeiou]{4,}$',  # Only vowels (4+)
            r'^\d+$',  # Only digits
        ]
        for pattern in gibberish_patterns:
            for word in words:
                if re.match(pattern, word):
                    return True

        return False

    def validate_food(self, food_name: str) -> Dict:
        """
        Validate a single food item
        Returns: {
            "is_valid": bool,
            "original": str,
            "matched_food": str or None,
            "confidence": float,
            "reason": str
        }
        """
        original = food_name.strip()

        # Check if blocked
        if self._is_blocked(original):
            return {
                "is_valid": False,
                "original": original,
                "matched_food": None,
                "confidence": 0.0,
                "reason": "blocked_harmful"
            }

        # Check if gibberish
        if self._is_gibberish(original):
            return {
                "is_valid": False,
                "original": original,
                "matched_food": None,
                "confidence": 0.0,
                "reason": "gibberish"
            }

        # Get all known foods
        all_foods = self._get_all_foods()

        # Fuzzy match against known foods
        is_match, matched_food, confidence = self._fuzzy_match(original, all_foods)

        if is_match:
            return {
                "is_valid": True,
                "original": original,
                "matched_food": matched_food,
                "confidence": confidence,
                "reason": "valid_food"
            }

        # If no match found but confidence is somewhat close, still reject
        return {
            "is_valid": False,
            "original": original,
            "matched_food": None,
            "confidence": confidence,
            "reason": "unknown_food"
        }

    def validate_food_items(self, food_items: List[Dict]) -> Tuple[List[Dict], List[Dict]]:
        """
        Validate multiple food items
        Returns: (valid_items, invalid_items)
        """
        valid_items = []
        invalid_items = []

        for item in food_items:
            name = item.get("name", "")
            result = self.validate_food(name)

            if result["is_valid"]:
                valid_items.append(item)
            else:
                invalid_items.append({
                    **item,
                    "validation_result": result
                })

        logger.info(f"[FoodValidator] Validated {len(food_items)} items: {len(valid_items)} valid, {len(invalid_items)} invalid")

        return valid_items, invalid_items


# Singleton instance (without DB - uses only fallback list)
_validator_no_db = None

def get_food_validator(db_session=None) -> FoodValidator:
    """Get food validator instance"""
    global _validator_no_db

    if db_session:
        # Create new instance with DB session
        return FoodValidator(db_session)
    else:
        # Return singleton without DB
        if _validator_no_db is None:
            _validator_no_db = FoodValidator()
        return _validator_no_db


def validate_food_name(food_name: str, db_session=None) -> Dict:
    """Quick validation of a single food name"""
    validator = get_food_validator(db_session)
    return validator.validate_food(food_name)


def filter_valid_foods(food_items: List[Dict], db_session=None) -> Tuple[List[Dict], List[Dict]]:
    """Filter food items to valid/invalid"""
    validator = get_food_validator(db_session)
    return validator.validate_food_items(food_items)
