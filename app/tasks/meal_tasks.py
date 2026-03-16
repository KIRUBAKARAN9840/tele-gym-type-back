# app/tasks/meal_tasks.py
"""
Celery tasks for AI meal planning
Production-ready with error handling and progress updates

Uses SYNC OpenAI client for gevent compatibility.
"""
import json
import logging
from typing import Dict, Optional
from app.celery_app import celery_app
from app.utils.openai_sync import get_sync_openai_client, sync_openai_call
from app.utils.redis_config import get_redis_sync

# Production logging configuration
logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] [%(name)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)


def publish_progress(task_id: str, data: dict):
    """Publish progress to Redis pub/sub for SSE streaming"""
    try:
        redis_client = get_redis_sync()
        redis_client.publish(
            f"task:{task_id}",
            json.dumps(data)
        )
        logger.debug(f"Task {task_id}: Published progress - {data.get('status')} ({data.get('progress', 0)}%)")
    except Exception as e:
        logger.error(f"Task {task_id}: Failed to publish progress - {e}")


@celery_app.task(bind=True, name="app.tasks.meal_tasks.understand_user_preferences")
def understand_user_preferences(
    self,
    user_id: int,
    user_message: str,
    current_profile: dict = None
):
    """
    Use AI to understand user's diet preferences from their message

    Args:
        user_id: Client ID
        user_message: User's message containing diet preferences
        current_profile: Current profile to merge with extracted preferences

    Returns:
        dict: Extracted preferences (diet_type, cuisine, health_condition, daily_calories)
    """
    task_id = self.request.id

    try:
        publish_progress(task_id, {
            "status": "progress",
            "progress": 20,
            "message": "Analyzing diet preferences..."
        })

        logger.info(f"Task {task_id}: Understanding preferences for user {user_id}")

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

        oai_client = get_sync_openai_client()

        # Run async OpenAI call
        
        

        

        publish_progress(task_id, {
            "status": "progress",
            "progress": 50,
            "message": "Processing with AI..."
        })

        response = sync_openai_call(
            oai_client,
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_message}
                ],
                response_format={"type": "json_object"},
            temperature=0
        )

        content = (response.choices[0].message.content or "").strip()
        

        # Parse JSON response
        try:
            preferences = json.loads(content)
        except json.JSONDecodeError:
            logger.warning(f"Task {task_id}: Failed to parse AI response as JSON: {content[:100]}")
            preferences = {
                "diet_type": "vegetarian",
                "cuisine": "Common",
                "health_condition": None,
                "daily_calories": None
            }

        # Merge with current profile if provided
        if current_profile:
            preferences = {**current_profile, **preferences}

        result = {
            "status": "completed",
            "progress": 100,
            "result": preferences
        }

        publish_progress(task_id, result)

        logger.info(f"Task {task_id}: Preferences extracted for user {user_id} - diet={preferences.get('diet_type')}")
        return preferences

    except Exception as e:
        logger.error(f"Task {task_id}: Preference extraction failed for user {user_id} - {type(e).__name__}: {e}")
        import traceback
        logger.error(f"Task {task_id}: Traceback:\n{traceback.format_exc()}")

        # Return fallback preferences
        fallback_preferences = {
            "diet_type": "vegetarian",
            "cuisine": "Common",
            "health_condition": None,
            "daily_calories": None
        }

        # Merge with current profile if provided
        if current_profile:
            fallback_preferences = {**current_profile, **fallback_preferences}

        result = {
            "status": "completed",
            "progress": 100,
            "result": fallback_preferences
        }
        publish_progress(task_id, result)

        return fallback_preferences


@celery_app.task(bind=True, name="app.tasks.meal_tasks.translate_text")
def translate_text(
    self,
    user_id: int,
    text: str
):
    """
    Translate text to English using AI

    Args:
        user_id: Client ID
        text: Text to translate

    Returns:
        dict: {"lang": "detected_language", "english": "translated_text"}
    """
    task_id = self.request.id

    try:
        publish_progress(task_id, {
            "status": "progress",
            "progress": 20,
            "message": "Translating..."
        })

        logger.info(f"Task {task_id}: Translating text for user {user_id}")

        system_prompt = (
            "You are a translator. Output ONLY JSON like "
            "{\"lang\":\"xx\",\"english\":\"...\"} "
            "Detect source language code (ISO-639-1 if possible). "
            "Translate to natural English. Do not add extra words. "
            "Keep food names recognizable; use common transliterations if needed."
        )

        oai_client = get_sync_openai_client()

        
        

        

        publish_progress(task_id, {
            "status": "progress",
            "progress": 50,
            "message": "Processing translation..."
        })

        response = sync_openai_call(
            oai_client,
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": text}
                ],
                response_format={"type": "json_object"},
            temperature=0
        )

        content = (response.choices[0].message.content or "").strip()
        

        try:
            data = json.loads(content)
            result_data = {
                "lang": (data.get("lang") or "unknown").strip(),
                "english": (data.get("english") or text).strip()
            }
        except json.JSONDecodeError:
            logger.warning(f"Task {task_id}: Failed to parse translation response: {content[:100]}")
            result_data = {"lang": "unknown", "english": text}

        result = {
            "status": "completed",
            "progress": 100,
            "result": result_data
        }

        publish_progress(task_id, result)

        logger.info(f"Task {task_id}: Translation completed for user {user_id} - lang={result_data['lang']}")
        return result_data

    except Exception as e:
        logger.error(f"Task {task_id}: Translation failed for user {user_id} - {type(e).__name__}: {e}")
        import traceback
        logger.error(f"Task {task_id}: Traceback:\n{traceback.format_exc()}")

        fallback = {"lang": "unknown", "english": text}

        result = {
            "status": "completed",
            "progress": 100,
            "result": fallback
        }
        publish_progress(task_id, result)

        return fallback


@celery_app.task(bind=True, name="app.tasks.meal_tasks.classify_meal_intent")
def classify_meal_intent(
    self,
    user_id: int,
    user_input: str,
    current_state: str
):
    """
    AI-driven intent classifier for meal planning chatbot

    Args:
        user_id: Client ID
        user_input: User's message
        current_state: Current conversation state

    Returns:
        dict: Intent classification result
    """
    task_id = self.request.id

    try:
        publish_progress(task_id, {
            "status": "progress",
            "progress": 20,
            "message": "Classifying intent..."
        })

        logger.info(f"Task {task_id}: Classifying meal intent for user {user_id}")

        system_prompt = f"""You are an intent classifier for a meal planning chatbot.
Current conversation state: {current_state}

Classify the user's intent into ONE of these categories:

1. **diet_preference**: User is specifying their diet type
   - Extract: diet_type (vegetarian, non-vegetarian, vegan, eggetarian, jain, ketogenic, paleo)

2. **cuisine_preference**: User is specifying cuisine preference
   - Extract: cuisine_type (north_indian, south_indian, commonly_available)
   - Note: "simple", "basic", "common", "everyday" → commonly_available

3. **food_allergy**: User mentions food allergies or items to avoid
   - Extract: allergens (list of foods/ingredients to avoid)

4. **food_removal**: User wants to remove specific foods
   - Extract: foods_to_remove (list of food items)

5. **food_alternate**: User wants alternatives for specific foods
   - Extract: foods_to_alternate (list of food items to find alternatives for)

6. **health_condition_change**: User mentions a health condition or dietary need
   - Extract: health_conditions (list: diabetic, pregnancy, pcos, hypertension, thyroid, etc.)

7. **save_template**: User wants to save or finalize the template (save, done, finish, etc.)

8. **unclear**: User input is unclear or doesn't match any intent

IMPORTANT: Be flexible with typos, informal language, and variations.
Examples:
- "im allergick to nuts" → food_allergy, allergens: ["nuts"]
- "rmove dairy plz" → food_removal, foods_to_remove: ["dairy"]
- "give me alternate for paneer" → food_alternate, foods_to_alternate: ["paneer"]
- "alternative to eggs" → food_alternate, foods_to_alternate: ["eggs"]
- "i have diabetes" → health_condition_change, health_conditions: ["diabetic"]
- "im pregnant" → health_condition_change, health_conditions: ["pregnancy"]
- "i have pcos and thyroid" → health_condition_change, health_conditions: ["pcos", "thyroid"]
- "save it" → save_template
- "done" → save_template
- "non vej" → diet_preference, diet_type: "non-vegetarian"
- "south indain cuisne" → cuisine_preference, cuisine_type: "south_indian"

Return ONLY valid JSON in this format:
{{
    "intent": "intent_name",
    "confidence": 0.95,
    "extracted_data": {{
        "key": "value"
    }},
    "normalized_input": "corrected version of user input"
}}"""

        oai_client = get_sync_openai_client()

        
        

        

        publish_progress(task_id, {
            "status": "progress",
            "progress": 50,
            "message": "Processing with AI..."
        })

        response = sync_openai_call(
            oai_client,
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_input}
                ],
                response_format={"type": "json_object"},
            temperature=0.3
        )

        content = (response.choices[0].message.content or "").strip()
        

        try:
            result_data = json.loads(content)
        except json.JSONDecodeError:
            logger.warning(f"Task {task_id}: Failed to parse intent response: {content[:100]}")
            result_data = {
                "intent": "unclear",
                "confidence": 0.0,
                "extracted_data": {},
                "normalized_input": user_input
            }

        result = {
            "status": "completed",
            "progress": 100,
            "result": result_data
        }

        publish_progress(task_id, result)

        logger.info(f"Task {task_id}: Intent classified for user {user_id} - intent={result_data.get('intent')}")
        return result_data

    except Exception as e:
        logger.error(f"Task {task_id}: Intent classification failed for user {user_id} - {type(e).__name__}: {e}")
        import traceback
        logger.error(f"Task {task_id}: Traceback:\n{traceback.format_exc()}")

        fallback = {
            "intent": "unclear",
            "confidence": 0.0,
            "extracted_data": {},
            "normalized_input": user_input
        }

        result = {
            "status": "completed",
            "progress": 100,
            "result": fallback
        }
        publish_progress(task_id, result)

        return fallback
