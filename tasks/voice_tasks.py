# app/tasks/voice_tasks.py
"""
Celery tasks for voice message processing
Uses API key pool for rotation across 3-4 keys
"""
import os
import json
import httpx
import logging
from celery import current_task
from app.celery_app import celery_app
from app.utils.openai_pool import get_openai_client
from app.utils.redis_config import get_redis_sync
from app.utils.async_openai import async_openai_call

# Production logging configuration
logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] [%(name)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# Import your existing functions
from app.fittbot_api.v1.client.client_api.chatbot.chatbot_services.asr import (
    transcribe_groq,
    backoff_call,
    get_groq_api_key,
    GROQ_BASE_URL,
    GROQ_ASR_MODEL
)
# NOTE: Imports from food_log moved inside functions to avoid circular import

from dotenv import load_dotenv
load_dotenv()


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


@celery_app.task(bind=True, name="app.tasks.voice_tasks.process_voice_message")
def process_voice_message(self, user_id: int, audio_bytes: bytes, meal: str = None):
    
    # Lazy import to avoid circular dependency
    from app.fittbot_api.v1.client.client_api.chatbot.codes.food_log import (
        extract_food_info_using_ai,
        calculate_nutrition_using_ai,
        store_diet_data_to_db
    )

    task_id = self.request.id

    try:
        # Update progress: Starting
        publish_progress(task_id, {
            "status": "progress",
            "progress": 10,
            "message": "Processing audio..."
        })

        # Step 1: Transcribe with Groq (2-4s)
        logger.info(f"Task {task_id}: Starting voice processing for user {user_id} (audio_size={len(audio_bytes)} bytes, meal={meal})")
        transcript = transcribe_audio_sync(audio_bytes)

        if not transcript:
            raise ValueError("Empty transcript from Groq")

        logger.info(f"Task {task_id}: Audio transcribed successfully - '{transcript[:100]}{'...' if len(transcript) > 100 else ''}'")

        # Update progress: Transcription done
        publish_progress(task_id, {
            "status": "progress",
            "progress": 30,
            "message": "Analyzing food items..."
        })

        # Step 2: Extract food info with OpenAI (uses key pool!)
        logger.debug(f"Task {task_id}: Extracting food info from transcript using OpenAI")
        oai_client = get_openai_client()  # Gets next key from pool

        # Run async function in sync context
        import asyncio
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        food_info = loop.run_until_complete(
            extract_food_info_using_ai(transcript, oai_client)
        )

        foods = food_info.get("foods", [])
        if not foods:
            raise ValueError("No food items identified")

        food_names = [f['name'] for f in foods[:5]]  # First 5 items
        logger.info(f"Task {task_id}: Extracted {len(foods)} food items - {food_names}{'...' if len(foods) > 5 else ''}")

        # Update progress: Food extraction done
        publish_progress(task_id, {
            "status": "progress",
            "progress": 60,
            "message": "Calculating nutrition..."
        })

        # Step 3: Calculate nutrition for foods without it
        for food in foods:
            if food.get("quantity") is not None and food.get("calories") is None:
                oai_client = get_openai_client()  # Get next key
                nutrition = loop.run_until_complete(
                    calculate_nutrition_using_ai(
                        food["name"],
                        food["quantity"],
                        food["unit"],
                        oai_client
                    )
                )
                food.update(nutrition)

        logger.debug(f"Task {task_id}: Nutrition calculated for all food items")

        # Update progress: Almost done
        publish_progress(task_id, {
            "status": "progress",
            "progress": 80,
            "message": "Saving to database..."
        })

        # Step 4: Save to database (need DB session)
        from app.models.database import get_db_sync
        from datetime import datetime
        import pytz

        db = next(get_db_sync())
        try:
            IST = pytz.timezone("Asia/Kolkata")
            today_date = datetime.now(IST).strftime("%Y-%m-%d")

            # Create Redis client for cache invalidation
            redis_client = get_redis_sync()

            if meal:
                loop.run_until_complete(
                    store_diet_data_to_db(db, redis_client, user_id, today_date, foods, meal)
                )
        finally:
            db.close()

        loop.close()

        logger.info(f"Task {task_id}: Voice food logging completed successfully for user {user_id} - logged {len(foods)} items")

        # Final result
        result = {
            "status": "completed",
            "progress": 100,
            "result": {
                "type": "food_log",
                "status": "logged",
                "is_log": True,
                "message": f"✅ Logged {len(foods)} food items successfully!",
                "foods": foods,
                "transcript": transcript
            }
        }

        # Publish completion
        publish_progress(task_id, result)

        return result["result"]

    except Exception as e:
        logger.error(f"Task {task_id}: Voice processing failed for user {user_id} - {type(e).__name__}: {e}")
        import traceback
        logger.error(f"Task {task_id}: Traceback:\n{traceback.format_exc()}")

        # Publish error
        error_result = {
            "status": "error",
            "progress": 0,
            "result": {
                "type": "error",
                "message": f"Failed to process voice: {str(e)}"
            }
        }
        publish_progress(task_id, error_result)

        raise  # Re-raise for Celery retry


def transcribe_audio_sync(audio_bytes: bytes) -> str:
    """Synchronous wrapper for Groq transcription"""
    # Lazy import to avoid circular dependency
    from app.fittbot_api.v1.client.client_api.chatbot.codes.food_log import (
        FOOD_LOG_TRANSCRIPTION_PROMPT
    )

    import io
    from fastapi import UploadFile

    # Create fake UploadFile from bytes
    audio_file = io.BytesIO(audio_bytes)

    # Create httpx client
    with httpx.Client(timeout=60.0) as client:
        # Prepare multipart form data
        files = {"file": ("audio.wav", audio_file, "audio/wav")}
        data = {"model": GROQ_ASR_MODEL}
        if FOOD_LOG_TRANSCRIPTION_PROMPT:
            data["prompt"] = FOOD_LOG_TRANSCRIPTION_PROMPT

        headers = {"Authorization": f"Bearer {get_groq_api_key()}"}
        url = f"{GROQ_BASE_URL}/openai/v1/audio/transcriptions"

        # Call Groq API
        response = client.post(url, data=data, files=files, headers=headers)
        response.raise_for_status()

        result = response.json()
        return (result.get("text") or "").strip()


@celery_app.task(bind=True, name="app.tasks.voice_tasks.extract_food_from_text")
def extract_food_from_text(
    self,
    user_id: int,
    text: str
):
    """
    Extract food info from TEXT (no transcription needed)
    ONLY makes the OpenAI call - all state management stays in FastAPI

    Args:
        user_id: Client ID
        text: Text input from user

    Returns:
        dict: Food extraction result {"foods": [...]}
    """
    # Lazy import to avoid circular dependency
    from app.fittbot_api.v1.client.client_api.chatbot.codes.food_log import (
        extract_food_info_using_ai
    )

    task_id = self.request.id

    try:
        publish_progress(task_id, {
            "status": "progress",
            "progress": 30,
            "message": "Analyzing food items..."
        })

        logger.info(f"Task {task_id}: Extracting food from text for user {user_id} - '{text[:100]}{'...' if len(text) > 100 else ''}'")

        oai_client = get_openai_client()  # Gets next key from pool
        logger.debug(f"Task {task_id}: OpenAI client acquired from pool")

        # Run async function in sync context
        import asyncio
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        food_info = loop.run_until_complete(
            extract_food_info_using_ai(text, oai_client)
        )

        loop.close()

        result = {
            "status": "completed",
            "progress": 100,
            "result": food_info
        }

        publish_progress(task_id, result)

        foods = food_info.get('foods', [])
        logger.info(f"Task {task_id}: Text food extraction completed for user {user_id} - extracted {len(foods)} items")
        return result["result"]

    except Exception as e:
        logger.error(f"Task {task_id}: Text food extraction failed for user {user_id} - {type(e).__name__}: {e}")
        import traceback
        logger.error(f"Task {task_id}: Traceback:\n{traceback.format_exc()}")

        error_result = {
            "status": "error",
            "progress": 0,
            "result": {
                "foods": [],
                "error": str(e)
            }
        }
        publish_progress(task_id, error_result)

        raise


@celery_app.task(bind=True, name="app.tasks.voice_tasks.transcribe_and_translate")
def transcribe_and_translate(self, user_id: int, audio_bytes: bytes, context: str = "general"):
    """
    Transcribe audio with Groq and translate to English with OpenAI
    ONLY handles AI calls - no business logic

    Args:
        user_id: Client ID for logging and traceability
        audio_bytes: Audio file bytes
        context: Context for translation ("food", "workout", "general")

    Returns:
        dict: {
            "transcript": "original transcription",
            "lang": "detected language code",
            "english": "english translation"
        }
    """
    task_id = self.request.id

    try:
        # Step 1: Transcribe with Groq
        publish_progress(task_id, {
            "status": "progress",
            "progress": 30,
            "message": "Transcribing audio..."
        })

        logger.info(f"Task {task_id}: Starting transcription for user {user_id} (audio_size={len(audio_bytes)} bytes, context={context})")
        transcript = transcribe_audio_sync(audio_bytes)

        if not transcript:
            raise ValueError("Empty transcript from Groq")

        logger.info(f"Task {task_id}: Transcription completed - '{transcript[:100]}{'...' if len(transcript) > 100 else ''}'")

        # Step 2: Translate with OpenAI
        publish_progress(task_id, {
            "status": "progress",
            "progress": 70,
            "message": "Translating to English..."
        })

        logger.debug(f"Task {task_id}: Translating transcript to English (context={context})")
        oai_client = get_openai_client()

        # Context-specific system prompts
        system_prompts = {
            "food": (
                "You are a translator. Output ONLY JSON like "
                "{\"lang\":\"xx\",\"english\":\"...\"}. "
                "Detect source language code (ISO-639-1 if possible). "
                "Translate to natural English. Do not add extra words. "
                "Keep food names recognizable; use common transliterations if needed."
            ),
            "workout": (
                "You are a translator. Output ONLY JSON like "
                "{\"lang\":\"xx\",\"english\":\"...\"}. "
                "Detect source language code (ISO-639-1 if possible). "
                "Translate to natural English. Do not add extra words. "
                "Keep exercise names recognizable; use common transliterations if needed."
            ),
            "general": (
                "You are a translator. Output ONLY JSON like "
                "{\"lang\":\"xx\",\"english\":\"...\"}. "
                "Detect source language code (ISO-639-1 if possible). "
                "Translate to natural English. Do not add extra words."
            )
        }

        system_prompt = system_prompts.get(context, system_prompts["general"])

        # Run async OpenAI call in sync context
        import asyncio
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        try:
            response = loop.run_until_complete(
                async_openai_call(
                    oai_client,
                    model="gpt-4o-mini",
                    messages=[
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": transcript}
                    ],
                    response_format={"type": "json_object"},
                    temperature=0
                )
            )

            import json
            translation_data = json.loads(response.choices[0].message.content)
            lang = (translation_data.get("lang") or "unknown").strip()
            english = (translation_data.get("english") or transcript).strip()

        except Exception as e:
            logger.warning(f"Task {task_id}: Translation failed, using original transcript - {e}")
            lang = "unknown"
            english = transcript
        finally:
            loop.close()

        logger.info(f"Task {task_id}: Translation completed (lang={lang})")

        # Final result
        result = {
            "status": "completed",
            "progress": 100,
            "result": {
                "transcript": transcript,
                "lang": lang,
                "english": english
            }
        }

        publish_progress(task_id, result)

        logger.info(f"Task {task_id}: Transcribe+translate completed successfully for user {user_id} (lang={lang})")
        return result["result"]

    except Exception as e:
        logger.error(f"Task {task_id}: Transcribe+translate failed for user {user_id} - {type(e).__name__}: {e}")
        import traceback
        logger.error(f"Task {task_id}: Traceback:\n{traceback.format_exc()}")

        error_result = {
            "status": "error",
            "progress": 0,
            "result": {
                "transcript": "",
                "lang": "unknown",
                "english": "",
                "error": str(e)
            }
        }
        publish_progress(task_id, error_result)

        raise


@celery_app.task(bind=True, name="app.tasks.voice_tasks.process_food_log_success_voice")
def process_food_log_success_voice(self, user_id: int, target_calories: int):
    """
    Process FoodLog success voice message via Celery
    Similar to process_voice_message but for food logging success
    """
    from app.models.fittbot_models import VoicePreference
    from app.utils.redis_config import get_redis_sync
    import uuid
    from datetime import datetime
    from app.models.database import get_db_sync

    task_id = self.request.id

    # VOICE_DEBUG: Comprehensive logging
    logger.info(f"VOICE_DEBUG: Celery task started for user {user_id}, task_id: {task_id}, target_calories: {target_calories}")

    try:
        redis_client = get_redis_sync()
        logger.info(f"VOICE_DEBUG: Redis connection established for task {task_id}")

        # Double-check voice preference within the task (safety layer)
        db = get_db_sync()
        try:
            voice_pref = db.query(VoicePreference).filter(VoicePreference.client_id == user_id).first()
            if voice_pref and voice_pref.preference == "0":
                logger.info(f"VOICE_DEBUG: Voice disabled for user {user_id}, skipping voice message generation")
                return {
                    "status": "skipped",
                    "task_id": task_id,
                    "reason": "Voice preference disabled"
                }
        except Exception as db_error:
            logger.warning(f"VOICE_DEBUG: Error checking voice preference in task for user {user_id}: {db_error}")
            # Continue with voice generation if preference check fails (default to enabled)

        # Create personalized success message
        voice_message = "Food logged successfully! Tap View Food Logs to see your logged items."
        logger.info(f"VOICE_DEBUG: Voice message created: '{voice_message}' for task {task_id}")

        # Generate task ID for WebSocket delivery
        message_id = str(uuid.uuid4())
        logger.info(f"VOICE_DEBUG: Generated message_id: {message_id} for task {task_id}")

        # Store voice message in Redis for WebSocket delivery
        voice_data = {
            "type": "food_log_success_voice",
            "user_id": user_id,
            "message": voice_message,
            "target_calories": target_calories,
            "timestamp": datetime.utcnow().isoformat()
        }
        logger.info(f"VOICE_DEBUG: Voice data prepared for task {task_id}: {voice_data}")

        # Store in Redis with TTL (5 minutes)
        redis_client.setex(f"voice_message:{message_id}", 300, json.dumps(voice_data))
        logger.info(f"VOICE_DEBUG: Voice message stored in Redis for task {task_id}, message_id: {message_id}, redis_key: voice_message:{message_id}")

        # Publish to WebSocket channel
        channel_name = f"user_channel:{user_id}"
        websocket_message = {
            "type": "voice_message",
            "task_id": task_id,
            "message_id": message_id,
            "data": voice_data
        }

        logger.info(f"VOICE_DEBUG: Publishing to WebSocket channel '{channel_name}' for task {task_id}, channel: {channel_name}")

        redis_client.publish(channel_name, json.dumps(websocket_message))
        logger.info(f"VOICE_DEBUG: Successfully published to WebSocket channel '{channel_name}' for task {task_id}")

        logger.info(f"VOICE_DEBUG: FoodLog success voice task {task_id}: Published voice message for user {user_id}")

        return {
            "status": "success",
            "task_id": task_id,
            "message_id": message_id,
            "message": voice_message
        }

    except Exception as e:
        logger.error(f"FoodLog success voice task {task_id} failed for user {user_id} - {type(e).__name__}: {e}")
        import traceback
        logger.error(f"FoodLog success voice task {task_id}: Traceback:\n{traceback.format_exc()}")

        return {
            "status": "error",
            "task_id": task_id,
            "error": str(e)
        }


@celery_app.task(bind=True, name="app.tasks.voice_tasks.process_food_scanner_voice")
def process_food_scanner_voice(self, user_id: int, scan_results: dict):
    """
    Process Food Scanner voice message via Celery
    Generates natural, friendly voice output for food scan results with category-specific messages
    """
    from app.models.fittbot_models import VoicePreference
    from app.utils.redis_config import get_redis_sync
    import uuid
    from datetime import datetime
    from app.models.database import get_db_sync

    task_id = self.request.id

    # VOICE_DEBUG: Comprehensive logging
    logger.info(f"VOICE_DEBUG: Food Scanner Celery task started for user {user_id}, task_id: {task_id}")
    logger.info(f"VOICE_DEBUG: Scan results: {scan_results}")

    try:
        redis_client = get_redis_sync()
        logger.info(f"VOICE_DEBUG: Redis connection established for food scanner task {task_id}")

        # Double-check voice preference within the task (safety layer)
        db = get_db_sync()
        try:
            voice_pref = db.query(VoicePreference).filter(VoicePreference.client_id == user_id).first()
            if voice_pref and voice_pref.preference == "0":
                logger.info(f"VOICE_DEBUG: Voice disabled for user {user_id}, skipping food scanner voice message generation")
                return {
                    "status": "skipped",
                    "task_id": task_id,
                    "reason": "Voice preference disabled"
                }
        except Exception as db_error:
            logger.warning(f"VOICE_DEBUG: Error checking voice preference in food scanner task for user {user_id}: {db_error}")
            # Continue with voice generation if preference check fails (default to enabled)

        # Extract scan data
        items = scan_results.get("items", [])

        # Generate natural, friendly voice message based on scan results
        voice_message = generate_natural_food_message(items)

        logger.info(f"VOICE_DEBUG: Food scanner voice message created: '{voice_message}' for task {task_id}")

        # Generate task ID for WebSocket delivery
        message_id = str(uuid.uuid4())
        logger.info(f"VOICE_DEBUG: Generated food scanner message_id: {message_id} for task {task_id}")

        # Store voice message in Redis for WebSocket delivery
        voice_data = {
            "type": "food_scanner_voice",
            "user_id": user_id,
            "message": voice_message,
            "items_count": len(items),
            "category": determine_food_category(items) if items else "unknown",
            "timestamp": datetime.utcnow().isoformat()
        }
        logger.info(f"VOICE_DEBUG: Food scanner voice data prepared for task {task_id}: {voice_data}")

        # Store in Redis with TTL (5 minutes like other voice tasks)
        redis_client.setex(f"voice_message:{message_id}", 300, json.dumps(voice_data))
        logger.info(f"VOICE_DEBUG: Food scanner voice message stored in Redis for task {task_id}, message_id: {message_id}, redis_key: voice_message:{message_id}")

        # Publish to WebSocket channel
        channel_name = f"user_channel:{user_id}"
        websocket_message = {
            "type": "voice_message",
            "task_id": task_id,
            "message_id": message_id,
            "data": voice_data
        }

        logger.info(f"VOICE_DEBUG: Publishing to WebSocket channel '{channel_name}' for task {task_id}, channel: {channel_name}")

        redis_client.publish(channel_name, json.dumps(websocket_message))
        logger.info(f"VOICE_DEBUG: Successfully published to WebSocket channel '{channel_name}' for task {task_id}")

        logger.info(f"VOICE_DEBUG: Food scanner voice task {task_id}: Published voice message for user {user_id}")

        return {
            "status": "success",
            "task_id": task_id,
            "message_id": message_id,
            "message": voice_message
        }

    except Exception as e:
        logger.error(f"Food scanner voice task {task_id} failed for user {user_id} - {type(e).__name__}: {e}")
        import traceback
        logger.error(f"Food scanner voice task {task_id}: Traceback:\n{traceback.format_exc()}")

        return {
            "status": "error",
            "task_id": task_id,
            "error": str(e)
        }


def generate_natural_food_message(items: list) -> str:
    """Generate natural, friendly voice message based on food items"""
    if not items:
        return "Great job tracking your nutrition! Keep it up!"

    # Determine food category
    category = determine_food_category(items)
    food_text = " ".join(items).lower()

    # Generate natural message based on category
    if category == "juice":
        return "Enjoy your refreshing drink! Staying hydrated is important for your fitness journey."

    elif category == "fast_food":
        return "Enjoy your meal! Remember to balance it with healthy choices throughout the day."

    elif category == "chips":
        return "Enjoy your snack! Perfect for satisfying those cravings."

    elif category == "snacks":
        return "Enjoy your tasty snack! Smart choices keep you energized."

    elif category == "regular_food":
        # Check if it's a balanced meal
        protein_keywords = ['chicken', 'fish', 'egg', 'dal', 'beans', 'tofu']
        veg_keywords = ['vegetable', 'salad', 'spinach', 'broccoli', 'carrot']

        has_protein = any(keyword in food_text for keyword in protein_keywords)
        has_veggies = any(keyword in food_text for keyword in veg_keywords)

        if has_protein and has_veggies:
            return "Enjoy your nutritious meal! You've got a great balance of protein and vegetables."
        elif has_protein:
            return "Enjoy your protein-rich meal! Great for muscle recovery and growth."
        elif has_veggies:
            return "Enjoy your healthy meal! Those vegetables are packed with vitamins and minerals."
        else:
            return "Enjoy your delicious homemade meal! Every healthy choice counts."

    else:
        return "Great job logging your food! You're doing amazing on your fitness journey."


def determine_food_category(items: list) -> str:
    """Determine food category based on detected food items"""
    if not items:
        return "unknown"

    # Convert to lowercase for analysis
    food_text = " ".join(items).lower()

    # Juice and liquid categories
    juice_keywords = ['juice', 'milk', 'water', 'tea', 'coffee', 'smoothie', 'lassi', 'shake', 'drink', 'coconut water']
    if any(keyword in food_text for keyword in juice_keywords):
        return "juice"

    # Fast food categories
    fast_food_keywords = ['pizza', 'burger', 'cheeseburger', 'fries', 'sandwich', 'pasta', 'noodles', 'doughnut']
    if any(keyword in food_text for keyword in fast_food_keywords):
        return "fast_food"

    # Chip categories
    chip_keywords = ['chips', 'potato chips', 'corn chips']
    if any(keyword in food_text for keyword in chip_keywords):
        return "chips"

    # Snack categories
    snack_keywords = ['cookie', 'biscuit', 'cake', 'pastry', 'candy', 'chocolate', 'nuts']
    if any(keyword in food_text for keyword in snack_keywords):
        return "snacks"

    # Default to regular food
    return "regular_food"


@celery_app.task(bind=True, name="app.tasks.voice_tasks.process_workout_log_success_voice")
def process_workout_log_success_voice(self, user_id: int, duration_minutes: int, total_calories: float, exercises_count: int):
    """
    Process WorkoutLog success voice message via Celery
    Similar to process_food_log_success_voice but for workout logging success
    """
    from app.models.fittbot_models import VoicePreference
    from app.utils.redis_config import get_redis_sync
    import uuid
    from datetime import datetime
    from app.models.database import get_db_sync

    task_id = self.request.id

    # VOICE_DEBUG: Comprehensive logging
    logger.info(f"VOICE_DEBUG: Workout Celery task started for user {user_id}, task_id: {task_id}, duration: {duration_minutes}, calories: {total_calories}, exercises: {exercises_count}")

    try:
        redis_client = get_redis_sync()
        logger.info(f"VOICE_DEBUG: Redis connection established for workout task {task_id}")

        # Double-check voice preference within the task (safety layer)
        db = get_db_sync()
        try:
            voice_pref = db.query(VoicePreference).filter(VoicePreference.client_id == user_id).first()
            if voice_pref and voice_pref.preference == "0":
                logger.info(f"VOICE_DEBUG: Voice disabled for user {user_id}, skipping workout voice message generation")
                return {
                    "status": "skipped",
                    "task_id": task_id,
                    "reason": "Voice preference disabled"
                }
        except Exception as db_error:
            logger.warning(f"VOICE_DEBUG: Error checking voice preference in workout task for user {user_id}: {db_error}")
            # Continue with voice generation if preference check fails (default to enabled)

        # Create personalized workout success message
        voice_message = f"Great job! Workout logged successfully"
        logger.info(f"VOICE_DEBUG: Workout voice message created: '{voice_message}' for task {task_id}")

        # Generate task ID for WebSocket delivery
        message_id = str(uuid.uuid4())
        logger.info(f"VOICE_DEBUG: Generated workout message_id: {message_id} for task {task_id}")

        # Store voice message in Redis for WebSocket delivery
        voice_data = {
            "type": "workout_log_success_voice",
            "user_id": user_id,
            "message": voice_message,
            "duration_minutes": duration_minutes,
            "total_calories": round(total_calories),
            "exercises_count": exercises_count,
            "timestamp": datetime.utcnow().isoformat()
        }
        logger.info(f"VOICE_DEBUG: Workout voice data prepared for task {task_id}: {voice_data}")

        # Store in Redis with TTL (5 minutes like food log)
        redis_client.setex(f"voice_message:{message_id}", 300, json.dumps(voice_data))
        logger.info(f"VOICE_DEBUG: Workout voice message stored in Redis for task {task_id}, message_id: {message_id}, redis_key: voice_message:{message_id}")

        # Publish to WebSocket channel
        channel_name = f"user_channel:{user_id}"
        websocket_message = {
            "type": "voice_message",
            "task_id": task_id,
            "message_id": message_id,
            "data": voice_data
        }

        logger.info(f"VOICE_DEBUG: Publishing workout voice to WebSocket channel '{channel_name}' for task {task_id}")

        redis_client.publish(channel_name, json.dumps(websocket_message))
        logger.info(f"VOICE_DEBUG: Successfully published workout voice to WebSocket channel '{channel_name}' for task {task_id}")

        logger.info(f"VOICE_DEBUG: WorkoutLog success voice task {task_id}: Published voice message for user {user_id}")

        return {
            "status": "success",
            "task_id": task_id,
            "message_id": message_id,
            "message": voice_message
        }

    except Exception as e:
        logger.error(f"WorkoutLog success voice task {task_id} failed for user {user_id} - {type(e).__name__}: {e}")
        import traceback
        logger.error(f"WorkoutLog success voice task {task_id}: Traceback:\n{traceback.format_exc()}")

        return {
            "status": "error",
            "task_id": task_id,
            "error": str(e)
        }
def process_food_template_voice(self, client_id: int, voice_type: str):
    """
    Process food template voice notifications using text-to-speech.
    Handles both template creation and meal plan saved voice messages.
    Stores voice message in Redis and publishes to WebSocket for real-time delivery.
    Follows the same pattern as Food Log and Workout Log voice systems.

    Args:
        client_id: Client ID for personalized voice generation
        voice_type: Type of voice message ("template_creation" or "meal_plan_saved")
    """
    import redis
    import json
    import uuid
    from datetime import datetime
    from app.config.settings import get_settings

    settings = get_settings()
    redis_client = redis.Redis.from_url(settings.REDIS_URL)

    # Determine voice message based on type
    voice_messages = {
        "template_creation": "Here is your diet template.",
        "meal_plan_saved": "Your diet plan saved"
    }

    voice_message = voice_messages.get(voice_type, "Here is your diet template.")

    try:
        # Skip ElevenLabs API call - use text message for frontend TTS (following Food Log pattern)
        # This is more reliable and consistent with the existing voice system

        # Generate task ID for WebSocket delivery (following Food Log pattern)
        message_id = str(uuid.uuid4())

        # Create voice data following the working Food Log pattern
        voice_data = {
            "type": "food_template_success_voice",
            "user_id": client_id,
            "client_id": client_id,
            "message": voice_message,
            "voice_type": voice_type,
            "timestamp": datetime.utcnow().isoformat()
        }

        # Store in Redis with TTL (5 minutes like other voice messages)
        redis_client.setex(f"voice_message:{message_id}", 300, json.dumps(voice_data))

        # Prepare WebSocket message following the working pattern
        websocket_message = {
            "type": "voice_message",
            "task_id": self.request.id,
            "message_id": message_id,
            "data": voice_data
        }

        # Publish to WebSocket channel for real-time delivery (using correct channel)
        redis_client.publish(f"user_channel:{client_id}", json.dumps(websocket_message))

        return {
            "status": "success",
            "message_id": message_id,
            "voice_type": voice_type,
            "message": voice_message
        }

    except Exception as e:
        # Log error but don't fail the task (graceful degradation)
        logger.error(f"Food template {voice_type} voice generation failed for client {client_id}: {str(e)}")
        self.update_state(state='FAILURE', meta={'error': str(e)})


@celery_app.task(bind=True, name="app.tasks.voice_tasks.process_workout_template_voice")
def process_workout_template_voice(self, client_id: int, voice_type: str):
    """
    Process workout template voice notifications using text-to-speech.
    Handles both template creation and workout plan saved voice messages.
    Stores voice message in Redis and publishes to WebSocket for real-time delivery.
    Follows the same pattern as Food Log and Food Template voice systems.

    Args:
        client_id: Client ID for personalized voice generation
        voice_type: Type of voice message ("template_creation" or "workout_saved")
    """
    import redis
    import json
    import uuid
    from datetime import datetime
    from app.config.settings import get_settings

    settings = get_settings()
    redis_client = redis.Redis.from_url(settings.REDIS_URL)

    # Determine voice message based on type
    voice_messages = {
        "template_creation": "Here is your workout plan",
        "workout_saved": "Your workout plan has been saved"
    }

    voice_message = voice_messages.get(voice_type, "Here is your workout plan")

    try:
        # Skip ElevenLabs API call - use text message for frontend TTS (following Food Log pattern)
        # This is more reliable and consistent with the existing voice system

        # Generate task ID for WebSocket delivery (following Food Log pattern)
        message_id = str(uuid.uuid4())

        # Create voice data following the working Food Log pattern
        voice_data = {
            "type": "workout_template_success_voice",
            "user_id": client_id,
            "client_id": client_id,
            "message": voice_message,
            "voice_type": voice_type,
            "timestamp": datetime.utcnow().isoformat()
        }

        # Store in Redis with TTL (5 minutes like other voice messages)
        redis_client.setex(f"voice_message:{message_id}", 300, json.dumps(voice_data))

        # Prepare WebSocket message following the working pattern
        websocket_message = {
            "type": "voice_message",
            "task_id": self.request.id,
            "message_id": message_id,
            "data": voice_data
        }

        # Publish to WebSocket channel for real-time delivery (using correct channel)
        redis_client.publish(f"user_channel:{client_id}", json.dumps(websocket_message))

        return {
            "status": "success",
            "message_id": message_id,
            "voice_type": voice_type,
            "message": voice_message
        }

    except Exception as e:
        # Log error but don't fail the task (graceful degradation)
        logger.error(f"Workout template {voice_type} voice generation failed for client {client_id}: {str(e)}")
        self.update_state(state='FAILURE', meta={'error': str(e)})


# Report analysis voice task removed - no voice for report analysis


@celery_app.task(bind=True, name="app.tasks.voice_tasks.process_meal_selector_voice")
def process_meal_selector_voice(self, user_id: int):
    """
    Process meal selector voice message via Celery
    Triggered when meal selector modal opens
    """
    from app.models.fittbot_models import VoicePreference
    from app.utils.redis_config import get_redis_sync
    import uuid
    from datetime import datetime
    from app.models.database import get_db_sync

    task_id = self.request.id

    # VOICE_DEBUG: Comprehensive logging
    logger.info(f"VOICE_DEBUG: Meal selector voice task started for user {user_id}, task_id: {task_id}")

    try:
        redis_client = get_redis_sync()
        logger.info(f"VOICE_DEBUG: Redis connection established for meal selector task {task_id}")

        # Double-check voice preference within the task (safety layer)
        db = get_db_sync()
        try:
            voice_pref = db.query(VoicePreference).filter(VoicePreference.client_id == user_id).first()
            if voice_pref and voice_pref.preference == "0":
                logger.info(f"VOICE_DEBUG: Voice disabled for user {user_id}, skipping meal selector voice message")
                return {
                    "status": "skipped",
                    "task_id": task_id,
                    "reason": "Voice preference disabled"
                }
        except Exception as db_error:
            # Continue with voice generation if preference check fails
            logger.warning(f"VOICE_DEBUG: Voice preference check failed for user {user_id}, continuing with voice generation: {db_error}")

        # Create meal selector voice message
        voice_message = "Select your meal slot"
        logger.info(f"VOICE_DEBUG: Created meal selector voice message for user {user_id}: '{voice_message}'")

        # Generate message ID
        message_id = str(uuid.uuid4())
        logger.info(f"VOICE_DEBUG: Generated message_id: {message_id} for meal selector task {task_id}")

        # Store voice message in Redis for WebSocket delivery
        voice_data = {
            "type": "meal_selector_voice",
            "user_id": user_id,
            "message": voice_message,
            "timestamp": datetime.utcnow().isoformat()
        }
        logger.info(f"VOICE_DEBUG: Meal selector voice data prepared for task {task_id}: {voice_data}")

        # Store in Redis with TTL (5 minutes)
        redis_client.setex(f"voice_message:{message_id}", 300, json.dumps(voice_data))
        logger.info(f"VOICE_DEBUG: Meal selector voice message stored in Redis for task {task_id}, message_id: {message_id}, redis_key: voice_message:{message_id}")

        # Publish to WebSocket channel
        channel_name = f"user_channel:{user_id}"
        websocket_message = {
            "type": "voice_message",
            "task_id": task_id,
            "message_id": message_id,
            "data": voice_data
        }

        logger.info(f"VOICE_DEBUG: Publishing meal selector voice to WebSocket channel '{channel_name}' for task {task_id}")

        redis_client.publish(channel_name, json.dumps(websocket_message))
        logger.info(f"VOICE_DEBUG: Successfully published meal selector voice to WebSocket channel '{channel_name}' for task {task_id}")

        logger.info(f"VOICE_DEBUG: Meal selector voice task {task_id}: Published voice message for user {user_id}")

        return {
            "status": "success",
            "task_id": task_id,
            "message_id": message_id,
            "message": voice_message
        }

    except Exception as e:
        logger.error(f"Meal selector voice task {task_id} failed for user {user_id} - {type(e).__name__}: {e}")
        import traceback
        logger.error(f"Meal selector voice task {task_id}: Traceback:\n{traceback.format_exc()}")

        return {
            "status": "error",
            "task_id": task_id,
            "error": str(e)
        }
