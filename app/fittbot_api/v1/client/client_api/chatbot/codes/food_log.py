#food_log
from fastapi import APIRouter, HTTPException, Query, Depends, UploadFile, File
from fastapi.responses import StreamingResponse
from fastapi_limiter.depends import RateLimiter
from pydantic import BaseModel
import pytz, os, hashlib, orjson, re, json
from datetime import datetime
from sqlalchemy.orm import Session
from app.models.database import get_db
from app.models.deps import get_http, get_oai, get_mem
from app.utils.async_openai import async_openai_call
from openai import AsyncOpenAI
import google.generativeai as genai
import json, re, os, asyncio

# Celery task import for voice processing
from app.tasks.voice_tasks import process_voice_message
from app.models.fittbot_models import (
    ActualDiet, VoicePreference, Client, CalorieEvent,
    LeaderboardDaily, LeaderboardMonthly, LeaderboardOverall,
    ClientNextXp, RewardGym, RewardPrizeHistory, ClientTarget
)
from sqlalchemy import asc
from datetime import date
from app.fittbot_api.v1.client.client_api.chatbot.chatbot_services.asr import transcribe_audio as generic_transcribe_audio
from app.utils.redis_config import get_redis
from redis.asyncio import Redis

from app.fittbot_api.v1.client.client_api.chatbot.chatbot_services.llm_helpers import (
    PlainTextStreamFilter, oai_chat_stream, GENERAL_SYSTEM, TOP_K,
    build_messages, heuristic_confidence, gpt_extract_items, first_missing_quantity,OPENAI_MODEL,
    sse_json, sse_escape, gpt_small_route, _scale_macros, is_yes, is_no,is_fit_chat,
    has_action_verb, food_hits,ensure_per_unit_macros, is_fittbot_meta_query,normalize_food, explicit_log_command, STYLE_PLAN, is_plan_request,STYLE_CHAT_FORMAT,pretty_plan
)



router = APIRouter(prefix="/food_log", tags=["food_log"])

async def delete_keys_by_pattern(redis: Redis, pattern: str) -> None:
    keys = await redis.keys(pattern)
    if keys:
        print("keys are there deleting",keys)
        await redis.delete(*keys)

APP_ENV = os.getenv("APP_ENV", "prod")
TZNAME = os.getenv("TZ", "Asia/Kolkata")
IST = pytz.timezone(TZNAME)
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

# Validate API keys
if not OPENAI_API_KEY:
    raise ValueError("OPENAI_API_KEY environment variable is not set")
if not GEMINI_API_KEY:
    raise ValueError("GEMINI_API_KEY environment variable is not set")

# Configure Gemini
genai.configure(api_key=GEMINI_API_KEY)
gemini_model = genai.GenerativeModel("gemini-2.5-flash")

# Food-specific transcription prompt - simple and focused on food vocabulary
# The AI already handles misspellings, so we just need reasonable transcription
# Even if "log" becomes "lock", the AI will understand from context
FOOD_LOG_TRANSCRIPTION_PROMPT = """I ate 2 idli, 3 dosa, 5 vada, biryani, pulao, rice, sambar, dal, curry, paneer, chicken, fish, egg, curd, yogurt, milk, apple, banana, mango, juice, roti, chapati, naan, vada. I had 1 plate, 2 bowls, 3 pieces, 100 grams, 200 ml, half cup, 1 spoon, 2 tablespoons. Numbers: one, two, three, four, five, six, seven, eight, nine, ten."""

async def get_voice_preference(db: Session, client_id: int) -> str:
    """Get voice preference for a client - returns '1' for enabled, '0' for disabled"""
    try:
        voice_pref = db.query(VoicePreference).filter(VoicePreference.client_id == client_id).first()
        if voice_pref:
            return voice_pref.preference
        return "1"  # Default to enabled
    except Exception as e:
        print(f"Error getting voice preference for client {client_id}: {e}")
        return "1"  # Default to enabled on error

async def transcribe_audio_for_food_log(audio: UploadFile, http):
    """
    Food-specific transcription - works exactly like text input
    The AI handles misspellings, so transcription doesn't need to be perfect
    """
    transcript = await generic_transcribe_audio(audio, http=http, prompt=FOOD_LOG_TRANSCRIPTION_PROMPT)
    print(f"[FOOD_LOG_TRANSCRIBE] Audio → Text: '{transcript}'")
    return transcript

async def trigger_food_log_success_voice(user_id: int, target_calories: int, db: Session):
    """Trigger voice notification via Celery task for food logging success"""
    try:
        # Check voice preference using existing async helper
        voice_pref = await get_voice_preference(db, user_id)

        if voice_pref == "1":  # Voice enabled
            from app.tasks.voice_tasks import process_food_log_success_voice
            # Trigger Celery task for non-blocking voice processing
            process_food_log_success_voice.delay(user_id, target_calories)
            print(f"[VOICE_TRIGGER] Voice notification triggered for user {user_id}, target_calories: {target_calories}")
        else:
            print(f"[VOICE_TRIGGER] Voice disabled for user {user_id}, skipping notification")
    except Exception as e:
        print(f"[VOICE_TRIGGER] Error triggering voice notification: {e}")
        # Don't fail food logging if voice notification fails

async def trigger_meal_selector_voice(user_id: int, db: Session):
    """Trigger voice notification via Celery task for meal selector modal"""
    try:
        # Check voice preference using existing async helper
        voice_pref = await get_voice_preference(db, user_id)

        if voice_pref == "1":  # Voice enabled
            from app.tasks.voice_tasks import process_meal_selector_voice
            # Trigger Celery task for non-blocking voice processing
            process_meal_selector_voice.delay(user_id)
            print(f"[VOICE_TRIGGER] Meal selector voice notification triggered for user {user_id}")
        else:
            print(f"[VOICE_TRIGGER] Voice disabled for user {user_id}, skipping meal selector voice notification")
    except Exception as e:
        print(f"[VOICE_TRIGGER] Error triggering meal selector voice notification: {e}")
        # Don't fail meal selector if voice notification fails

@router.get("/healthz")
async def healthz():
    return {"ok": True, "env": APP_ENV, "tz": TZNAME}

@router.post("/voice/transcribe")
async def voice_transcribe(
    audio: UploadFile = File(...),
    http = Depends(get_http),
    oai = Depends(get_oai),
):
    """Transcribe audio to text and translate to English - food-specific recognition"""
    transcript = await transcribe_audio_for_food_log(audio, http=http)
    if not transcript:
        raise HTTPException(400, "empty transcript")

    print(f"[VOICE_TRANSCRIBE] Transcribed: '{transcript}'")

    async def _translate_to_english(text: str) -> dict:
        try:
            sys = (
                "You are a translator. Output ONLY JSON like "
                "{\"lang\":\"xx\",\"english\":\"...\"}. "
                "Detect source language code (ISO-639-1 if possible). "
                "Translate to natural English. Do not add extra words. "
                "Keep food names recognizable; use common transliterations if needed."
            )
            resp = await async_openai_call(oai,
                model=OPENAI_MODEL,
                messages=[{"role":"system","content":sys},{"role":"user","content":text}],
                response_format={"type":"json_object"},
                temperature=0
            )
            return orjson.loads(resp.choices[0].message.content)
        except Exception as e:
            print(f"Translation error: {e}")
            return {"lang": "unknown", "english": text}

    result = await _translate_to_english(transcript)
    return {
        "transcript": transcript,
        "detected_language": result.get("lang", "unknown"),
        "english_text": result.get("english", transcript)
    }

@router.post("/voice/stream")
async def voice_stream_sse(
    user_id: int,
    audio: UploadFile = File(...),
    meal: str = Query(None),
    redis: Redis = Depends(get_redis),
):
    """
    Process voice using Celery queue - Production ready!
    NO APP CHANGES - Same SSE streaming interface!
    """
    async def _stream_with_celery():
        try:
            audio_bytes = await audio.read()

            if not audio_bytes:
                yield sse_json({"type": "error", "message": "Empty audio file"})
                yield "event: done\ndata: [DONE]\n\n"
                return

            # Immediate response - connection established
            yield sse_json({
                "type": "status",
                "message": "🎤 Processing voice message..."
            })

            # Queue job to Celery worker
            task = process_voice_message.delay(
                user_id=user_id,
                audio_bytes=audio_bytes,
                meal=meal
            )

            task_id = task.id
            print(f"[Voice Stream] Queued task {task_id} for user {user_id}")

            # Subscribe to Redis pub/sub for real-time updates
            pubsub = redis.pubsub()
            await pubsub.subscribe(f"task:{task_id}")

            # Stream updates from Celery worker
            timeout_seconds = 120  # 2 minutes timeout
            start_time = asyncio.get_event_loop().time()

            try:
                async for message in pubsub.listen():
                    # Check timeout
                    if asyncio.get_event_loop().time() - start_time > timeout_seconds:
                        yield sse_json({
                            "type": "error",
                            "message": "Processing timeout. Please try again."
                        })
                        break

                    if message['type'] == 'message':
                        data = json.loads(message['data'])
                        status = data.get('status')

                        if status == 'progress':
                            # Progress update from worker
                            yield sse_json({
                                "type": "progress",
                                "message": data.get('message', 'Processing...'),
                                "progress": data.get('progress', 0)
                            })

                        elif status == 'completed':
                            # Final result - same format as before!
                            result = data.get('result', {})
                            yield sse_json(result)
                            yield "event: done\ndata: [DONE]\n\n"
                            break

                        elif status == 'error':
                            # Error from worker
                            yield sse_json({
                                "type": "error",
                                "message": data.get('message', 'Processing failed')
                            })
                            yield "event: done\ndata: [DONE]\n\n"
                            break

            finally:
                # Cleanup
                await pubsub.unsubscribe(f"task:{task_id}")
                await pubsub.close()

        except Exception as e:
            print(f"[Voice Stream] Error: {e}")
            import traceback
            traceback.print_exc()

            yield sse_json({
                "type": "error",
                "message": f"Failed to process voice: {str(e)}"
            })
            yield "event: done\ndata: [DONE]\n\n"

    return StreamingResponse(
        _stream_with_celery(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive"
        }
    )

def is_unit_keyword(text):
    """
    Check if the given text is a standalone measuring unit
    Returns True if the text is a unit, False otherwise
    """
    if not text:
        return False

    text_lower = text.lower().strip()

    # List of common measuring units (standalone)
    unit_keywords = {
        # Volume units
        'glass', 'glasses', 'cup', 'cups', 'ml', 'liter', 'liters', 'litre', 'litres',
        # Weight units
        'g', 'gram', 'grams', 'kg', 'kilogram', 'kilograms',
        # Counting/Container units
        'piece', 'pieces', 'plate', 'plates', 'bowl', 'bowls', 'spoon', 'spoons',
        'tablespoon', 'tablespoons', 'tbsp', 'teaspoon', 'teaspoons', 'tsp',
        'slice', 'slices', 'can', 'cans', 'packet', 'packets',
        # Indian specific units
        'katori', 'kadai', 'handi'
    }

    # Check if it's exactly a unit keyword
    return text_lower in unit_keywords

# REMOVED: get_smart_unit_and_question function - users can now enter any food without database restrictions
def normalize_unit_with_context(unit, food_name):
    """Simplified unit normalization - trust AI decisions more"""
    if not unit:
        return 'pieces'  # Simple fallback
    
    unit_lower = unit.lower().strip()
    
    # Standard unit mappings - expanded for spoons
    unit_map = {
        'g': 'grams', 'gram': 'grams', 'gms': 'grams',
        'kg': 'kg', 'kilogram': 'kg', 'kilograms': 'kg',
        'piece': 'pieces', 'pcs': 'pieces', 'pc': 'pieces',
        'slice': 'slices', 'slc': 'slices',
        'plate': 'plates', 'bowl': 'bowls',
        'cup': 'cups', 'glass': 'glasses',
        'ml': 'ml', 'milliliter': 'ml', 'milliliters': 'ml',
        'liter': 'liters', 'litre': 'liters', 'l': 'liters',
        'can': 'cans', 'tin': 'cans', 'packet': 'packets',
        'tablespoon': 'tablespoons', 'tbsp': 'tablespoons',
        'teaspoon': 'teaspoons', 'tsp': 'teaspoons',
        'spoon': 'tablespoons', 'spoons': 'tablespoons'  # Added spoon mapping
    }
    
    normalized = unit_map.get(unit_lower, unit_lower)
    
    # Accept any reasonable unit without overriding
    common_food_units = [
        'pieces', 'plates', 'bowls', 'cups', 'glasses', 'grams', 'ml', 'slices', 
        'tablespoons', 'teaspoons', 'kg', 'liters', 'cans', 'packets'
    ]
    
    if normalized in common_food_units:
        # print(f"DEBUG: Using unit '{normalized}' for {food_name}")
        return normalized
    else:
        # print(f"DEBUG: Unknown unit '{unit}', defaulting to pieces")
        return 'pieces'


def normalize_unit(unit):
    """Legacy function - kept for backward compatibility"""
    if not unit:
        return 'pieces'
    return normalize_unit_with_context(unit, 'unknown')

def get_unit_hint(unit):
    """Generate unit hint for the specified unit"""
    unit_hints = {
        'pieces': 'How many pieces?',
        'plates': 'How many plates or grams?',
        'bowls': 'How many bowls or grams?',
        'cups': 'How many cups or ml?',
        'glasses': 'How many glasses or ml?',
        'slices': 'How many slices?',
        'ml': 'How much ml or cups?',
        'grams': 'How many grams?',
        'kg': 'How many kg?'
    }
    return unit_hints.get(unit, f'How many {unit}?')

def convert_food_to_item_format(food):
    """Convert food object to items format"""
    return {
        "food": food.get('name', ''),
        "unit": food.get('unit', 'pieces'),
        "quantity": food.get('quantity'),
        "calories": food.get('calories', 0),
        "protein": food.get('protein', 0),
        "carbs": food.get('carbs', 0),
        "fat": food.get('fat', 0),
        "fiber": food.get('fiber', 0),
        "sugar": food.get('sugar', 0),
        "calcium": food.get('calcium', 0),
        "magnesium": food.get('magnesium', 0),
        "sodium": food.get('sodium', 0),
        "potassium": food.get('potassium', 0),
        "iron": food.get('iron', 0),
        "iodine": food.get('iodine', 0),
        "unit_hint": get_unit_hint(food.get('unit', 'pieces')),
        "ask": food.get('quantity') is None,
        "qty_from": "provided" if food.get('quantity') is not None else "ask"
    }

def get_enhanced_ai_prompt(text):
    """Generate comprehensive AI prompt that handles all edge cases"""
    return f"""
    Analyze this text and extract food information: "{text}"

    CRITICAL FOOD IDENTIFICATION RULES:
    1. COMPOUND FOODS: Treat compound words as SINGLE dishes:
       - "curdrice" = "curd rice" (one dish, not separate curd and rice)
          note: if curd,rice are separate with space/comma, treat as two dishes
       - "lemonrice" = "lemon rice" (one dish)
       - "masalatea" = "masala tea" (one dish)

    2. PREPOSITIONAL PHRASES (CRITICAL): Handle "with", "and", "&" correctly:
       - "potato chips with ketchup" = TWO separate foods: "potato chips" AND "ketchup"
       - "fries with ketchup" = TWO separate foods: "fries" AND "ketchup"
       - "sandwich with mayonnaise" = TWO separate foods: "sandwich" AND "mayonnaise"
       - "rice with curry" = TWO separate foods: "rice" AND "curry"
       - KEY: Split on "with", "and", "&" to get separate food items

    3. CONDIMENTS AND SAUCES: These ARE valid foods and MUST be tracked:
       - ketchup, mayonnaise, mustard, sauce, chutney, pickles
       - DO NOT ignore condiments - they have nutritional value
       - Assign appropriate units: tablespoons for most condiments

    4. FOOD DETECTION: Extract ALL foods/drinks, handle misspellings liberally
    5. CONTEXT AWARENESS: Consider Indian cuisine context for units and dishes

    QUANTITY EXTRACTION - CRITICAL:
    - ALWAYS look for numbers/quantities in the text, even without action verbs
    - "2 idli" → quantity: 2 (extract the number!)
    - "3 apples" → quantity: 3 (extract the number!)
    - "500ml juice" → quantity: 500, unit: ml
    - "I ate 2 idli" → quantity: 2 (same as "2 idli")
    - If truly no number is present, ONLY THEN set quantity to null
    - Don't ignore quantities just because there's no "ate" or "had"

    INTELLIGENT UNIT ASSIGNMENT:
    - When quantity is provided AND a unit is specified in the text, USE THE SPECIFIED UNIT.
    - When quantity is provided BUT NO unit is specified, choose the MOST LOGICAL unit based on:

    INDIAN RICE DISHES (default unit: plates/bowls):
    - Any rice dish: biryani, pulao, fried rice, lemon rice, curd rice → plates
    - Curries, dal, sambar → bowls

    MEASUREMENT CONTEXT:
    - "spoon", "spoons" → tablespoons (NOT grams)
    - Small countable items (idli, dosa, apple, banana) → pieces
    - Liquids → ml, cups, glasses (PREFER glasses for water only when water is explicitly mentioned)
    - Large servings → plates, bowls
    - Precise measurements (grams, kg) → use specified unit
    - IMPORTANT: Standalone units like "glass", "cup", "spoon" should be ignored unless associated with food

    EXAMPLES OF CORRECT INTERPRETATION:
    - "2 idli" → {{"name": "idli", "quantity": 2, "unit": "pieces", ...}}
    - "3 apples" → {{"name": "apple", "quantity": 3, "unit": "pieces", ...}}
    - "500ml juice" → {{"name": "juice", "quantity": 500, "unit": "ml", ...}}
    - "I ate 2 idli" → {{"name": "idli", "quantity": 2, "unit": "pieces", ...}}
    - "curdrice" → {{"name": "curd rice", "quantity": null, "unit": "plates"}}
    - "3 spoon curd" → {{"name": "curd", "quantity": 3, "unit": "tablespoons"}}
    - "500g biryani" → {{"name": "biryani", "quantity": 500, "unit": "grams"}}
    - "lemonrice" → {{"name": "lemon rice", "quantity": null, "unit": "plates"}}
    - "water 2" → {{"name": "water", "quantity": 2, "unit": "glasses", ...}}
    - "2 water" → {{"name": "water", "quantity": 2, "unit": "glasses", ...}}
    - "3 glasses of water" → {{"name": "water", "quantity": 3, "unit": "glasses", ...}}
    - "500ml water" → {{"name": "water", "quantity": 500, "unit": "ml", ...}}
    - "biryani, apple, glass" → [{{"name": "biryani", "quantity": null, "unit": "plates"}}, {{"name": "apple", "quantity": null, "unit": "pieces"}}] (ignore standalone "glass")
    - "glass" → [] (empty array - ignore standalone units)

    # IMPORTANT: Handle "with" phrases and condiments correctly
    - "potato chips with ketchup" → [{{"name": "potato chips", "quantity": null, "unit": "pieces"}}, {{"name": "ketchup", "quantity": null, "unit": "tablespoons"}}]
    - "fries with ketchup" → [{{"name": "fries", "quantity": null, "unit": "pieces"}}, {{"name": "ketchup", "quantity": null, "unit": "tablespoons"}}]
    - "sandwich with mayonnaise" → [{{"name": "sandwich", "quantity": null, "unit": "pieces"}}, {{"name": "mayonnaise", "quantity": null, "unit": "tablespoons"}}]
    - "chips with ketchup" → [{{"name": "chips", "quantity": null, "unit": "pieces"}}, {{"name": "ketchup", "quantity": null, "unit": "tablespoons"}}]
    - "rice with curry" → [{{"name": "rice", "quantity": null, "unit": "plates"}}, {{"name": "curry", "quantity": null, "unit": "bowls"}}]

    # CRITICAL: Condiments and sauces ARE foods and should be tracked
    - "ketchup" → {{"name": "ketchup", "quantity": null, "unit": "tablespoons"}} (DO NOT ignore!)
    - "mayonnaise" → {{"name": "mayonnaise", "quantity": null, "unit": "tablespoons"}} (DO NOT ignore!)
    - "sauce" → {{"name": "sauce", "quantity": null, "unit": "tablespoons"}} (DO NOT ignore!)
    - "chutney" → {{"name": "chutney", "quantity": null, "unit": "tablespoons"}} (DO NOT ignore!)

    NUTRITION CALCULATION (when quantity provided):
    - Use REALISTIC conversions:
      * 1 plate = 300g for rice dishes
      * 1 tablespoon = 15g for dense foods, 15ml for liquids
      * 1 piece idli = 39g, 1 piece dosa = 80g
      * 1 piece apple = 182g, 1 piece banana = 118g
    - Calculate accurate nutrition for exact quantity
    - If quantity is null, omit nutrition fields (set to null)
    - If quantity IS provided, MUST calculate and include macro (calories, protein, carbs, fat, fiber, sugar) AND micro nutrients (calcium, magnesium, sodium, potassium, iron, iodine)

    Return ONLY valid JSON array:
    [
        {{
            "name": "properly_formatted_food_name",
            "quantity": number_or_null,
            "unit": "contextually_appropriate_unit",
            "calories": number_or_null,
            "protein": number_or_null,
            "carbs": number_or_null,
            "fat": number_or_null,
            "fiber": number_or_null,
            "sugar": number_or_null,
            "calcium": number_or_null,
            "magnesium": number_or_null,
            "sodium": number_or_null,
            "potassium": number_or_null,
            "iron": number_or_null,
            "iodine": number_or_null
        }}
    ]

    CRITICAL:
    - ALWAYS extract quantities when present, regardless of action verbs
    - Use cultural context for Indian foods
    - Don't split compound food words
    - Calculate nutrition when quantity is provided
    """

async def extract_food_info_using_ai(text: str, oai):
    """AI-driven food extraction with comprehensive prompt"""
    
    reasoning_prompt = get_enhanced_ai_prompt(text)

    # Try OpenAI first
    try:
        response = await async_openai_call(oai,
            model="gpt-4o-mini",
            messages=[
                {
                    "role": "system",
                    "content": """You are a nutrition expert specializing in Indian cuisine.
                    CRITICAL RULES:
                    1. Compound food words are SINGLE dishes (curdrice = curd rice as one dish)
                    2. Use culturally appropriate units (plates for rice dishes, tablespoons for spoons)
                    3. Never split single dishes into multiple foods
                    4. Respect user's measurement context (3 spoon = 3 tablespoons, not grams)"""
                },
                {"role": "user", "content": reasoning_prompt}
            ],
            max_tokens=1000,
            temperature=0.1
        )

        result = response.choices[0].message.content.strip()
        result = re.sub(r"^```json\s*", "", result)
        result = re.sub(r"\s*```$", "", result)
        
        foods = json.loads(result)
        if not isinstance(foods, list):
            foods = [foods] if isinstance(foods, dict) else []
        
        # print(f"DEBUG: AI extracted foods: {[(f.get('name'), f.get('quantity'), f.get('unit')) for f in foods]}")
        return {"foods": foods}

    except Exception as e:
        print(f"OpenAI extraction error: {e}, trying Gemini fallback...")
        
        # Try Gemini fallback
        try:
            response = gemini_model.generate_content(reasoning_prompt)
            
            if response.text:
                result = response.text.strip()
                result = re.sub(r"^```json\s*", "", result)
                result = re.sub(r"\s*```$", "", result)
                
                foods = json.loads(result)
                if not isinstance(foods, list):
                    foods = [foods] if isinstance(foods, dict) else []
                
                # print(f"DEBUG: Gemini extracted foods: {[(f.get('name'), f.get('quantity'), f.get('unit')) for f in foods]}")
                return {"foods": foods}
                
        except Exception as gemini_error:
            print(f"Gemini extraction error: {gemini_error}")
        
        # Simple fallback parsing
        print("Both AI models failed, using fallback parsing...")
        return parse_food_with_smart_units(text)


def parse_food_with_smart_units(text):
    """Enhanced fallback parsing with smart unit assignment"""
    text_lower = text.lower().strip()
    
    # Handle common misspellings
    corrections = {
        'avacado': 'avocado', 'avacadojuice': 'avocado juice',
        'sugarcanjuice': 'sugar cane juice', 'orangejuice': 'orange juice'
    }
    
    for wrong, correct in corrections.items():
        text_lower = text_lower.replace(wrong, correct)
    
    # Split multiple foods by comma
    food_items = [item.strip() for item in text_lower.split(',')]
    foods = []

    for item in food_items:
        # NEW: Skip standalone unit keywords
        if is_unit_keyword(item):
            print(f"[parse_food_with_smart_units] Skipping standalone unit: '{item}'")
            continue
        # Extract quantity and unit
        quantity_match = re.search(r'(\d+(?:\.\d+)?)\s*(\w+)?', item)

        if quantity_match:
            quantity = float(quantity_match.group(1))
            unit_part = quantity_match.group(2)

            # Check if the word after quantity is a unit or the food name
            if unit_part and unit_part in ['g', 'grams', 'kg', 'plates', 'bowls', 'pieces', 'pcs', 'ml', 'glasses', 'cups', 'tablespoons', 'tbsp', 'teaspoons', 'tsp', 'spoons']:
                # It's a unit, so remove quantity and unit to get food name
                food_name = re.sub(r'\d+(?:\.\d+)?\s*\w+\s*', '', item).strip()
            else:
                # The word after quantity is likely the food name, so set unit_part to None
                unit_part = None
                food_name = re.sub(r'\d+(?:\.\d+)?\s*', '', item).strip()
        else:
            quantity = None
            unit_part = None
            food_name = item
        
        if food_name:
            # Smart unit assignment - no database matching
            if unit_part:
                unit = normalize_unit_with_context(unit_part, food_name)
            else:
                # Default unit heuristics for common food types
                food_lower = food_name.lower().strip()
                if any(rice_word in food_lower for rice_word in ['rice', 'pulao', 'biryani', 'fried rice', 'jeera rice', 'lemon rice', 'pongal']):
                    unit = 'plates'  # Rice dishes default to plates
                elif any(liquid_word in food_lower for liquid_word in ['juice', 'milk', 'water', 'tea', 'coffee', 'soup', 'curry', 'dal']):
                    unit = 'bowls'   # Liquids/curries default to bowls
                elif any(grain_word in food_lower for grain_word in ['dosa', 'idli', 'roti', 'chapati', 'naan', 'bread']):
                    unit = 'pieces'  # Grains default to pieces
                else:
                    unit = 'pieces'  # Default fallback

            foods.append({
                "name": food_name,
                "quantity": quantity,
                "unit": unit,
                "calories": None,
                "protein": None,
                "carbs": None,
                "fat": None,
                "fiber": None,
                "sugar": None,
                "calcium": None,
                "magnesium": None,
                "sodium": None,
                "potassium": None,
                "iron": None,
                "iodine": None
            })
    
    return {"foods": foods}

NUTRITION_KEYS = [
    "calories",
    "protein",
    "carbs",
    "fat",
    "fiber",
    "sugar",
    "calcium",
    "magnesium",
    "sodium",
    "potassium",
    "iron",
    "iodine",
]
NUTRITION_DEFAULTS = {key: 0.0 for key in NUTRITION_KEYS}

def _to_float(value):
    """Best-effort float coercion used for nutrition fields."""
    if value is None:
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    try:
        return float(str(value).strip())
    except (TypeError, ValueError):
        return 0.0

def ensure_nutrition_fields(nutrition: dict) -> dict:
    """Ensure all macro and micro nutrient keys exist with numeric values."""
    cleaned = NUTRITION_DEFAULTS.copy()
    if isinstance(nutrition, dict):
        for key in NUTRITION_KEYS:
            cleaned[key] = _to_float(nutrition.get(key, cleaned[key]))
    return cleaned

async def calculate_nutrition_using_ai(food_name, quantity, unit, oai):
    """Enhanced nutrition calculation with better unit handling"""
    try:
        prompt = f"""
        Calculate nutrition for: {quantity} {unit} of {food_name}
        
        Use these REALISTIC conversions:
        - 1 plate (rice dishes) = 300 grams
        - 1 tablespoon = 15 grams (solids) or 15 ml (liquids)
        - 1 teaspoon = 5 grams (solids) or 5 ml (liquids)
        - 1 cup = 200 grams (solids) or 200 ml (liquids)
        - 1 bowl = 200 grams
        - 1 glass = 200 ml
        - 1 piece varies by food type (estimate appropriately)
        
        For spoon measurements, consider the food density:
        - Curd/yogurt: 1 tablespoon ≈ 15g
        - Ghee/oil: 1 tablespoon ≈ 14g
        - Rice: 1 tablespoon ≈ 12g
    
        Examples:
        - "3 tablespoons of curd" = 45g of curd
        - "1 plate of lemon rice" = 300g of lemon rice
        - "2 pieces of dosa" = 160g total (80g each)
        
        Return ONLY valid JSON with realistic values:
        {{
            "calories": number,
            "protein": number,
            "carbs": number,
            "fat": number,
            "fiber": number,
            "sugar": number,
            "calcium": number (in mg),
            "magnesium": number (in mg),
            "sodium": number (in mg),
            "potassium": number (in mg),
            "iron": number (in mg),
            "iodine": number (in mcg)
        }}
        """
        
        # print(f"DEBUG: Calculating nutrition for {quantity} {unit} of {food_name}")
        
        # Try OpenAI first
        try:
            response = await async_openai_call(oai,
                model="gpt-4o-mini",
                messages=[
                    {
                        "role": "system",
                        "content": "You are a nutrition expert. Always provide realistic nutrition values based on the specified quantity and unit. Use the conversions provided."
                    },
                    {"role": "user", "content": prompt}
                ],
                max_tokens=300,
                temperature=0
            )
            
            result = response.choices[0].message.content.strip()
            # print(f"DEBUG: OpenAI nutrition response: {result}")
            
            result = re.sub(r"^```json\s*", "", result)
            result = re.sub(r"\s*```$", "", result)
            
            nutrition = json.loads(result)
            # print(f"DEBUG: Parsed nutrition: {nutrition}")
            
            return ensure_nutrition_fields(nutrition)
            
        except Exception as openai_error:
            print(f"OpenAI nutrition error: {openai_error}, trying Gemini...")
            
            # Gemini fallback
            response = gemini_model.generate_content(prompt)
            if response.text:
                result = response.text.strip()
                result = re.sub(r"^```json\s*", "", result)
                result = re.sub(r"\s*```$", "", result)
                
                nutrition = json.loads(result)
                return ensure_nutrition_fields(nutrition)

    except Exception as e:
        print(f"AI nutrition calculation failed: {e}, using fallback values")
        return get_fallback_nutrition(food_name, quantity, unit)

def get_fallback_nutrition(food_name, quantity, unit):
    """Enhanced fallback with better unit conversion"""
    food_lower = food_name.lower()
    
    # Convert to approximate grams for calculation
    if unit == 'plates':
        grams = quantity * 300
    elif unit == 'bowls':
        grams = quantity * 200
    elif unit == 'cups':
        grams = quantity * 200
    elif unit == 'tablespoons':
        if 'oil' in food_lower or 'ghee' in food_lower:
            grams = quantity * 14  # Fat is denser
        elif 'curd' in food_lower or 'yogurt' in food_lower:
            grams = quantity * 15
        else:
            grams = quantity * 12  # Rice, etc.
    elif unit == 'teaspoons':
        grams = quantity * 5
    elif unit == 'pieces':
        if 'dosa' in food_lower:
            grams = quantity * 80
        elif 'chicken' in food_lower:
            grams = quantity * 100
        else:
            grams = quantity * 50
    elif unit == 'grams':
        grams = quantity
    elif unit == 'ml':
        grams = quantity  # For liquids
    else:
        grams = quantity * 50  # Default
    
    # Basic nutrition per 100g with better estimates
    if 'curd' in food_lower and 'rice' in food_lower:
        per_100g = {"calories": 110, "protein": 3, "carbs": 20, "fat": 2, "fiber": 1, "sugar": 3}
    elif 'lemon rice' in food_lower:
        per_100g = {"calories": 150, "protein": 3, "carbs": 28, "fat": 4, "fiber": 1, "sugar": 1}
    elif 'curd' in food_lower:
        per_100g = {"calories": 60, "protein": 3.5, "carbs": 4.5, "fat": 3.5, "fiber": 0, "sugar": 4.5}
    elif 'rice' in food_lower:
        per_100g = {"calories": 130, "protein": 2.7, "carbs": 28, "fat": 0.3, "fiber": 0.4, "sugar": 0.1}
    else:
        per_100g = {"calories": 100, "protein": 3, "carbs": 15, "fat": 2, "fiber": 1, "sugar": 2}
    
    # Calculate for actual portion
    ratio = grams / 100.0
    nutrition = {}
    for key, value in per_100g.items():
        nutrition[key] = round(value * ratio, 1)
    
    # print(f"DEBUG: Fallback nutrition for {grams}g of {food_name}: {nutrition}")
    return ensure_nutrition_fields(nutrition)

def is_food_related(text):
    """Enhanced food detection - detects food names, quantities, and action verbs"""
    if not text:
        return False

    text_lower = text.lower().strip()

    # NEW: Check if the text is a standalone unit keyword first
    if is_unit_keyword(text_lower):
        print(f"[is_food_related] Ignoring standalone unit: '{text}'")
        return False

    # Check for common non-food greetings/commands first
    non_food_patterns = {
        'hello', 'hi', 'hey', 'thanks', 'thank you', 'help', 'what', 'how', 'when', 'where',
        'good morning', 'good evening', 'bye', 'goodbye'
    }

    if text_lower in non_food_patterns:
        print(f"[is_food_related] Matched non-food pattern: '{text}'")
        return False

    # Check for action verbs/meal indicators
    food_indicators = [
        'ate', 'eat', 'eating', 'had', 'drink', 'drinking', 'consumed', 'meal',
        'breakfast', 'lunch', 'dinner', 'snack'
    ]

    if any(indicator in text_lower for indicator in food_indicators):
        print(f"[is_food_related] Matched action verb: '{text}'")
        return True

    # Check for common food names (Indian and international)
    common_foods = [
        'idli', 'dosa', 'roti', 'chapati', 'naan', 'rice', 'biryani', 'pulao',
        'sambar', 'dal', 'curry', 'paneer', 'chicken', 'fish', 'egg', 'meat',
        'curd', 'yogurt', 'milk', 'juice', 'coffee', 'tea', 'water',
        'vada', 'vadai', 'upma', 'poha', 'paratha', 'uttapam', 'pongal',
        'samosa', 'pakora', 'chaat', 'tikka', 'kebab',
        'apple', 'banana', 'mango', 'orange', 'grapes', 'fruit',
        'vegetable', 'salad', 'soup', 'bread', 'butter', 'ghee', 'rasam',
        # Added snacks and condiments
        'chips', 'potato chips', 'fries', 'french fries', 'ketchup', 'tomato ketchup',
        'mayonnaise', 'sandwich', 'burger', 'pizza', 'pasta', 'noodles',
        'biscuit', 'cookies', 'cake', 'chocolate', 'ice cream', 'sauce',
        'mustard', 'relish', 'pickles', 'chutney', 'dip'
    ]

    # If any food name is found, consider it food-related
    matched_foods = [food for food in common_foods if food in text_lower]
    if matched_foods:
        print(f"[is_food_related] Matched food names {matched_foods}: '{text}'")
        return True

    # Check for "with" phrases - these are almost always food combinations
    if ' with ' in text_lower:
        words = text_lower.split(' with ')
        if len(words) == 2 and len(words[0].split()) <= 3 and len(words[1].split()) <= 3:
            print(f"[is_food_related] Detected 'with' phrase pattern: '{text}'")
            return True

    # Check for quantity patterns (number + word pattern)
    # This catches "two dosa", "3 apples", "500ml juice", "one vada and two idli", etc.
    quantity_patterns = [
        r'\d+\s*\w+',  # "2 dosa", "500ml", "3 apples"
        r'(one|two|three|four|five|six|seven|eight|nine|ten|half|quarter)\s+\w+',  # "two dosa"
        r'\w+\s+and\s+\w+',  # "dosa and vada"
    ]

    for pattern in quantity_patterns:
        if re.search(pattern, text_lower):
            words = text_lower.split()
            if len(words) <= 10:
                print(f"[is_food_related] Matched quantity pattern '{pattern}': '{text}'")
                return True

    # Default: if input is short (<=6 words) and doesn't match non-food patterns, assume it might be food
    # But exclude if it's a standalone unit keyword
    words = text_lower.split()
    if len(words) <= 6 and not is_unit_keyword(text_lower):
        print(f"[is_food_related] Short input (<= 6 words), assuming food: '{text}'")
        return True

    print(f"[is_food_related] No match found, NOT food-related: '{text}'")
    return False

def create_food_log_response_with_message(logged_foods, db=None, user_id=None):
    """Create food log response with summary message and nutrition totals"""
    items = [convert_food_to_item_format(food) for food in logged_foods]
    
    # Calculate total nutrition
    macro_keys = ["calories", "protein", "carbs", "fat", "fiber", "sugar"]
    micro_keys = [key for key in NUTRITION_KEYS if key not in macro_keys]
    macro_totals = {key: 0.0 for key in macro_keys}
    micro_totals = {key: 0.0 for key in micro_keys}
    
    # Create food summary and calculate totals
    food_summaries = []
    for food in logged_foods:
        quantity = food.get('quantity', 0)
        unit = food.get('unit', 'pieces')
        name = food.get('name', '')
        
        # Add to summary
        food_summaries.append(f"{quantity} {unit} of {name}")
        
        # Add to totals
        for nutrient in macro_keys:
            macro_totals[nutrient] += _to_float(food.get(nutrient, 0))
        for nutrient in micro_keys:
            micro_totals[nutrient] += _to_float(food.get(nutrient, 0))
    
    # Round totals to 1 decimal place
    for nutrient in macro_totals:
        macro_totals[nutrient] = round(macro_totals[nutrient], 1)
    for nutrient in micro_totals:
        micro_totals[nutrient] = round(micro_totals[nutrient], 1)
    
    # Create message
    if len(food_summaries) == 1:
        message = f"✅ Logged {food_summaries[0]}! "
    elif len(food_summaries) == 2:
        message = f"✅ Logged {food_summaries[0]} and {food_summaries[1]}! "
    else:
        message = f"✅ Logged {', '.join(food_summaries[:-1])} and {food_summaries[-1]}! "
    
    # Add nutrition info to message
    message += f"\n📊 Nutrition: {macro_totals['calories']} calories, {macro_totals['protein']}g protein, {macro_totals['carbs']}g carbs, {macro_totals['fat']}g fat"

    return {
        "type": "food_log",
        "status": "logged",
        "is_log": True,
        "message": message,
        "items": items,
        "totals": macro_totals,
        "micro_nutrients": micro_totals,
    }

async def handle_quantity_question(food_name, oai, unit=None):
    """Generate AI-driven quantity question"""

    # Rice-specific formatting: use "How many plates of {food} did you have?" format
    food_lower = food_name.lower().strip()
    if any(rice_word in food_lower for rice_word in ['rice', 'pulao', 'biryani', 'fried rice', 'jeera rice', 'lemon rice', 'pongal']):
        return f"How many plates of {food_name} did you have?"

    try:
        prompt = f"""
        Generate a natural quantity question for "{food_name}".
        
        Consider the most common way this food is measured:
        - Rice dishes (biryani, fried rice, lemon rice): plates, bowls
        - Small items (dosa, roti): pieces
        - Liquids: glasses, cups, ml
        - Condiments, curd: spoons, tablespoons
        - Vegetables: pieces, grams
        
        Return a friendly question like:
        "How many plates of biryani did you have?"
        "How many pieces of dosa?"
        "How many tablespoons of curd?"
        
        Return ONLY the question text, nothing else.
        """

        response = await async_openai_call(oai,
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "Generate natural food quantity questions based on cultural context."},
                {"role": "user", "content": prompt}
            ],
            max_tokens=50,
            temperature=0.3
        )
        
        question = response.choices[0].message.content.strip().strip('"')
        # print(f"DEBUG: AI generated question: {question}")
        return question
        
    except Exception as e:
        print(f"AI question generation failed: {e}")
        return f"How much {food_name} did you have?"


@router.get("/chat/stream_test")
async def chat_stream(
    user_id: int,
    text: str = Query(None),
    meal: str = Query(None),
    mem = Depends(get_mem),
    oai = Depends(get_oai),
    db: Session = Depends(get_db),
    redis: Redis = Depends(get_redis),
    
):
    try:
        if not text:
            async def _welcome():
                welcome_msg = "Hello! I'm your food logging assistant. What would you like to log today?"
                yield f"data: {json.dumps({'message': welcome_msg, 'type': 'welcome'})}\n\n"
                yield "event: done\ndata: [DONE]\n\n"
            
            return StreamingResponse(_welcome(), media_type="text/event-stream",
                                headers={"Cache-Control":"no-cache","X-Accel-Buffering":"no"})
        
        text = text.strip()
        # print(f"DEBUG: Processing text: '{text}'")

        # Check for pending state
        pending_state = await mem.get_pending(user_id)
        # print(f"DEBUG: Current pending state: {pending_state}")

        # Handle navigation confirmation
        if pending_state and pending_state.get("state") == "awaiting_nav_confirm":
            # print("DEBUG: In awaiting_nav_confirm state")

            if is_yes(text):
                # print("DEBUG: User said yes to navigation")
                await mem.clear_pending(user_id)
                async def _nav_yes():
                    yield sse_json({"type":"nav","is_navigation": True,
                                    "prompt":"Thanks for your confirmation. Redirecting to today's diet logs"})
                    yield "event: done\ndata: [DONE]\n\n"
                return StreamingResponse(_nav_yes(), media_type="text/event-stream",
                                        headers={"Cache-Control":"no-cache","X-Accel-Buffering":"no"})
            
            elif is_no(text):
                # print("DEBUG: User said no to navigation")
                await mem.clear_pending(user_id)
                async def _nav_no():
                    yield sse_json({"type":"nav","is_navigation": False,
                                    "prompt":"Thanks for your response. You can continue chatting here."})
                    yield "event: done\ndata: [DONE]\n\n"
                return StreamingResponse(_nav_no(), media_type="text/event-stream",
                                        headers={"Cache-Control":"no-cache","X-Accel-Buffering":"no"})
            
            elif is_food_related(text):
                # print("DEBUG: User entered food during nav confirmation, clearing state")
                await mem.clear_pending(user_id)
                # Continue to food processing below
            
            else:
                # print("DEBUG: User input not recognized, asking for nav confirmation again")
                async def _nav_clar():
                    yield f"data: {json.dumps({'message': 'Do you want to go to your diet log? Please say Yes or No.', 'type': 'nav_confirm'})}\n\n"
                    yield "event: done\ndata: [DONE]\n\n"
                return StreamingResponse(_nav_clar(), media_type="text/event-stream",
                                     headers={"Cache-Control":"no-cache","X-Accel-Buffering":"no"})

        # Handle pending food confirmation
        elif pending_state and pending_state.get("state") == "awaiting_pending_confirm":
            # print("DEBUG: In awaiting_pending_confirm state")
            pending_foods = pending_state.get("pending_foods", [])
            logged_foods = pending_state.get("logged_foods", [])

            if is_yes(text):
                # print("DEBUG: User wants to log pending foods")
                first_pending = pending_foods[0] if pending_foods else None
                if first_pending:
                    # Use context-aware question
                    ask_message = await handle_quantity_question(first_pending['name'], oai)
                                        
                    await mem.set_pending(user_id, {
                        "state": "awaiting_quantity",
                        "foods": pending_foods,
                        "current_food_index": 0,
                        "logged_foods": logged_foods,
                        "original_input": pending_state.get("original_input", "")
                    })
                    
                    async def _ask_pending():
                        yield f"data: {json.dumps({'message': ask_message, 'type': 'ask_quantity', 'food': first_pending['name']})}\n\n"
                        yield "event: done\ndata: [DONE]\n\n"
                    
                    return StreamingResponse(_ask_pending(), media_type="text/event-stream",
                                            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})
            
            elif is_no(text):
                # print("DEBUG: User doesn't want pending foods")
                await mem.clear_pending(user_id)
                
                if logged_foods:
                    async def _final_logged():
                        # Store to database
                        today_date = datetime.now(IST).strftime("%Y-%m-%d")
                        if meal:
                            await store_diet_data_to_db(db, redis, user_id, today_date, logged_foods, meal)

                        response_data = create_food_log_response_with_message(logged_foods, db, user_id)
                        yield sse_json(response_data)

                        # Trigger voice notification via Celery
                        try:
                            from app.models.fittbot_models import ClientTarget
                            client_target = db.query(ClientTarget).filter(ClientTarget.client_id == user_id).first()
                            target_calories = client_target.calories if client_target else 0
                            await trigger_food_log_success_voice(user_id, target_calories, db)
                        except Exception as e:
                            print(f"[VOICE_TRIGGER] Error in first trigger point: {e}")

                        yield "event: ping\ndata: {}\n\n"
                        yield "event: done\ndata: [DONE]\n\n"
                        await mem.set_pending(user_id, {"state":"awaiting_nav_confirm"})
                    return StreamingResponse(_final_logged(), media_type="text/event-stream",
                                            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})
                else:
                    async def _no_logged():
                        yield f"data: {json.dumps({'message': 'Okay, nothing logged. What else would you like to log?', 'type': 'response'})}\n\n"
                        yield "event: done\ndata: [DONE]\n\n"
                    
                    return StreamingResponse(_no_logged(), media_type="text/event-stream",
                                            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})
            
            elif is_food_related(text):
                # print("DEBUG: User entered food during pending confirmation")
                await mem.clear_pending(user_id)
                # Continue to food processing below
            
            else:
                # print("DEBUG: Asking for pending confirmation again")
                pending_names = [food['name'] for food in pending_foods]
                ask_message = f"Do you want to log these foods: {', '.join(pending_names)}? Please say Yes or No."
                
                async def _ask_confirm_again():
                    yield f"data: {json.dumps({'message': ask_message, 'type': 'confirm_pending'})}\n\n"
                    yield "event: done\ndata: [DONE]\n\n"
                
                return StreamingResponse(_ask_confirm_again(), media_type="text/event-stream",
                                        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})

        # Handle response to "Do you want to log remaining items?" confirmation
        elif pending_state and pending_state.get("state") == "awaiting_remaining_confirm":
            # print("DEBUG: In awaiting_remaining_confirm state")
            original_pending_foods = pending_state.get("original_pending_foods", [])
            new_pending_foods = pending_state.get("new_pending_foods", [])
            logged_foods = pending_state.get("logged_foods", [])
            original_index = pending_state.get("current_food_index", 0)

            if is_yes(text):
                # User wants to log remaining items + new items
                combined_pending = original_pending_foods + new_pending_foods

                if combined_pending:
                    # Continue from where we left off, not from beginning
                    next_index = original_index
                    if next_index >= len(original_pending_foods):
                        # If we were at the end of original foods, start with new foods
                        next_index = 0

                    next_food = combined_pending[next_index]
                    question = await handle_quantity_question(next_food['name'], oai)

                    await mem.set_pending(user_id, {
                        "state": "awaiting_quantity",
                        "foods": combined_pending,
                        "current_food_index": next_index,
                        "logged_foods": logged_foods,
                        "original_input": pending_state.get("original_input", "")
                    })

                    ask_message = question
                    # Remove logged summary - just ask the next question directly

                    async def _ask_combined():
                        yield f"data: {json.dumps({'message': ask_message, 'type': 'ask_quantity', 'food': next_food['name']})}\n\n"
                        yield "event: done\ndata: [DONE]\n\n"

                    return StreamingResponse(_ask_combined(), media_type="text/event-stream",
                                            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})

            elif is_no(text):
                # User doesn't want to log remaining items, only new items

                if new_pending_foods:
                    next_food = new_pending_foods[0]
                    question = await handle_quantity_question(next_food['name'], oai)

                    await mem.set_pending(user_id, {
                        "state": "awaiting_quantity",
                        "foods": new_pending_foods,
                        "current_food_index": 0,
                        "logged_foods": logged_foods,
                        "original_input": pending_state.get("original_input", "")
                    })

                    ask_message = question
                    # Remove logged summary - just ask the next question directly

                    async def _ask_new_only():
                        yield f"data: {json.dumps({'message': ask_message, 'type': 'ask_quantity', 'food': next_food['name']})}\n\n"
                        yield "event: done\ndata: [DONE]\n\n"

                    return StreamingResponse(_ask_new_only(), media_type="text/event-stream",
                                            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})

                else:
                    # No new foods to ask about, just log what we have
                    # Clear state and log all logged foods
                    await mem.clear_pending(user_id)

                    async def _final_log():
                        today_date = datetime.now(IST).strftime("%Y-%m-%d")
                        if meal:
                            await store_diet_data_to_db(db, redis, user_id, today_date, logged_foods, meal)

                        response_data = create_food_log_response_with_message(logged_foods, db, user_id)
                        yield sse_json(response_data)

                        # Trigger voice notification via Celery
                        try:
                            from app.models.fittbot_models import ClientTarget
                            client_target = db.query(ClientTarget).filter(ClientTarget.client_id == user_id).first()
                            target_calories = client_target.calories if client_target else 0
                            await trigger_food_log_success_voice(user_id, target_calories, db)
                        except Exception as e:
                            print(f"[VOICE_TRIGGER] Error in third trigger point: {e}")

                        yield "event: ping\ndata: {}\n\n"
                        yield "event: done\ndata: [DONE]\n\n"
                        await mem.set_pending(user_id, {"state": "awaiting_nav_confirm"})

                    return StreamingResponse(_final_log(), media_type="text/event-stream",
                                                     headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})

            else:
                # Invalid response, ask again - show only foods that still need quantities
                foods_needing_quantities = [f for f in original_pending_foods if f.get("quantity") is None]
                ask_message = f"Please answer Yes or No. Do you want to log the remaining items ({', '.join([f['name'] for f in foods_needing_quantities])})?"

                async def _ask_again():
                    yield f"data: {json.dumps({'message': ask_message, 'type': 'confirm_remaining'})}\n\n"
                    yield "event: done\ndata: [DONE]\n\n"

                return StreamingResponse(_ask_again(), media_type="text/event-stream",
                                        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})

        # Handle quantity input
        elif pending_state and pending_state.get("state") == "awaiting_quantity":
            # print("DEBUG: In awaiting_quantity state")
            try:
                foods = pending_state.get("foods", [])
                current_index = pending_state.get("current_food_index", 0)
                current_food = foods[current_index] if current_index < len(foods) else None
                logged_foods = pending_state.get("logged_foods", [])
                
                if not current_food:
                    # print("DEBUG: No current food found, clearing state")
                    await mem.clear_pending(user_id)
                    raise Exception("No current food found")
                
                # Check if user entered a new food instead of quantity
                # Enhanced logic: Check if food-related even if it looks like quantity input
                if is_food_related(text):
                    # Check if this is actually a new food entry (not just quantity for current food)
                    # by looking for food names in the text
                    new_food_info = await extract_food_info_using_ai(text, oai)
                    new_foods = new_food_info.get("foods", [])

                    # If AI finds food items and they're different from current food, treat as new food entry
                    if new_foods and any(f.get('name', '').lower().strip() != current_food['name'].lower().strip() for f in new_foods):
                        # This is a new food entry, proceed with new food logic
                        # print("DEBUG: User entered new food during quantity request")

                        if new_foods:
                            # Separate foods with and without quantities from new input
                            new_foods_with_quantity = [f for f in new_foods if f.get("quantity") is not None]
                            new_foods_without_quantity = [f for f in new_foods if f.get("quantity") is None]

                            # Get current pending foods (original batch)
                            current_pending_foods = foods
                            current_logged_foods = logged_foods

                            # Log foods with quantities immediately
                            immediate_logged = current_logged_foods + new_foods_with_quantity

                            # If there are pending foods from original batch, ask for confirmation
                            if current_pending_foods:
                                # Store new foods separately for later processing, preserving current food index
                                await mem.set_pending(user_id, {
                                    "state": "awaiting_remaining_confirm",
                                    "original_pending_foods": current_pending_foods,
                                    "new_pending_foods": new_foods_without_quantity,
                                    "logged_foods": immediate_logged,
                                    "original_input": pending_state.get("original_input", ""),
                                    "current_food_index": current_index  # Preserve where we left off
                                })

                                # Ask for confirmation - show only foods that still need quantities
                                foods_needing_quantities = [f for f in current_pending_foods if f.get("quantity") is None]
                                remaining_items = ", ".join([f['name'] for f in foods_needing_quantities])
                                ask_message = f"Do you want to log the remaining items ({remaining_items})?"

                            async def _ask_confirmation():
                                yield f"data: {json.dumps({'message': ask_message, 'type': 'confirm_remaining'})}\n\n"
                                yield "event: done\ndata: [DONE]\n\n"

                            return StreamingResponse(_ask_confirmation(), media_type="text/event-stream",
                                                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})

                            # No original pending foods, continue normally with new foods only
                        else:
                                updated_foods = list(new_foods_without_quantity)

                                next_index = None
                                for idx, item in enumerate(updated_foods):
                                    if item.get("quantity") is None:
                                        next_index = idx
                                        break

                                if next_index is None:
                                    # No foods without quantity, log everything immediately
                                    await mem.clear_pending(user_id)

                                    async def _final_log():
                                        today_date = datetime.now(IST).strftime("%Y-%m-%d")
                                        if meal:
                                            await store_diet_data_to_db(db, redis, user_id, today_date, immediate_logged, meal)

                                        response_data = create_food_log_response_with_message(immediate_logged, db, user_id)
                                        yield sse_json(response_data)

                                        # Trigger voice notification via Celery
                                        try:
                                            from app.models.fittbot_models import ClientTarget
                                            client_target = db.query(ClientTarget).filter(ClientTarget.client_id == user_id).first()
                                            target_calories = client_target.calories if client_target else 0
                                            await trigger_food_log_success_voice(user_id, target_calories, db)
                                        except Exception as e:
                                            print(f"[VOICE_TRIGGER] Error in second trigger point: {e}")

                                        yield "event: ping\ndata: {}\n\n"
                                        yield "event: done\ndata: [DONE]\n\n"
                                        await mem.set_pending(user_id, {"state": "awaiting_nav_confirm"})

                                    return StreamingResponse(_final_log(), media_type="text/event-stream",
                                                             headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})

                                message_parts = []
                                if new_foods_with_quantity:
                                    summary = ", ".join(
                                        f"{food['quantity']} {food['unit']} of {food['name']}"
                                        for food in new_foods_with_quantity
                                    )
                                    message_parts.append(f"Logged: {summary}")

                                question = await handle_quantity_question(updated_foods[next_index]['name'], oai)
                                message_parts.append(question)
                                ask_message = "\n\n".join(message_parts)

                                await mem.set_pending(user_id, {
                                    "state": "awaiting_quantity",
                                    "foods": updated_foods,
                                    "current_food_index": next_index,
                                    "logged_foods": immediate_logged,
                                    "original_input": pending_state.get("original_input", text)
                                })

                                async def _ask_updated():
                                    yield f"data: {json.dumps({'message': ask_message, 'type': 'ask_quantity', 'food': updated_foods[next_index]['name']})}\n\n"
                                    yield "event: done\ndata: [DONE]\n\n"

                                return StreamingResponse(_ask_updated(), media_type="text/event-stream",
                                                         headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})
                    else:
                        # AI found food items but they're the same as current food,
                        pass

                # Process as quantity input
                print(f"DEBUG: Processing '{text}' as quantity for {current_food['name']}")
                quantity_value, parsed_unit = parse_quantity_and_unit(text, current_food['name'], current_food.get('unit'))
                print(f"DEBUG: Parsed -> quantity: {quantity_value}, unit: {parsed_unit}")
                
                if quantity_value is not None:
                    # print(f"DEBUG: Parsed quantity: {quantity_value} {parsed_unit}")
                    # Use parsed unit or default to food's preferred unit
                    final_unit = parsed_unit
                    
                    # Update food
                    foods[current_index]["quantity"] = quantity_value
                    foods[current_index]["unit"] = final_unit
                    
                    # Calculate nutrition using AI
                    nutrition = await calculate_nutrition_using_ai(
                        current_food['name'], quantity_value, final_unit, oai)
                    foods[current_index].update(nutrition)
                    
                    # Move completed food to logged
                    logged_foods.append(foods[current_index])
                    
                    # Check for next food needing quantity
                    next_food_index = -1
                    for i in range(current_index + 1, len(foods)):
                        if foods[i].get("quantity") is None:
                            next_food_index = i
                            break
                    
                    if next_food_index != -1:
                        next_food = foods[next_food_index]
                        # Use context-aware question
                        ask_message = await handle_quantity_question(next_food['name'], oai)
                        
                        await mem.set_pending(user_id, {
                            "state": "awaiting_quantity",
                            "foods": foods,
                            "current_food_index": next_food_index,
                            "logged_foods": logged_foods,
                            "original_input": pending_state.get("original_input", text)
                        })
                        
                        async def _ask_next():
                            yield f"data: {json.dumps({'message': ask_message, 'type': 'ask_quantity', 'food': next_food['name']})}\n\n"
                            yield "event: done\ndata: [DONE]\n\n"
                        
                        return StreamingResponse(_ask_next(), media_type="text/event-stream",
                                                headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})
                    else:
                        # All foods processed, log everything
                        # print("DEBUG: All foods processed, logging")
                        await mem.clear_pending(user_id)
                        
                        async def _logged_then_nav():
                            # Store to database
                            today_date = datetime.now(IST).strftime("%Y-%m-%d")
                            if meal:
                                await store_diet_data_to_db(db, redis, user_id, today_date, logged_foods, meal)

                            response_data = create_food_log_response_with_message(logged_foods, db, user_id)

                            # Send only the complete food log data (includes the message)
                            yield sse_json(response_data)

                            # Trigger voice notification via Celery
                            try:
                                from app.models.fittbot_models import ClientTarget
                                client_target = db.query(ClientTarget).filter(ClientTarget.client_id == user_id).first()
                                target_calories = client_target.calories if client_target else 0
                                await trigger_food_log_success_voice(user_id, target_calories, db)
                            except Exception as e:
                                print(f"[VOICE_TRIGGER] Error in third trigger point: {e}")

                            yield "event: ping\ndata: {}\n\n"
                            yield "event: done\ndata: [DONE]\n\n"
                            await mem.set_pending(user_id, {"state":"awaiting_nav_confirm"})

                        return StreamingResponse(_logged_then_nav(), media_type="text/event-stream",
                                                headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})
                else:
                    # Ask again with better guidance
                    ask_message = f"Please enter a number for {current_food['name']}. For example: '2', '1.5', or '500g'"
                    
                    async def _ask_again():
                        yield f"data: {json.dumps({'message': ask_message, 'type': 'ask_quantity', 'food': current_food['name']})}\n\n"
                        yield "event: done\ndata: [DONE]\n\n"
                    
                    return StreamingResponse(_ask_again(), media_type="text/event-stream",
                                            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})
                                            
            except Exception as e:
                print(f"Error processing quantity: {e}")
                await mem.clear_pending(user_id)

        # Normal food processing (no pending state or cleared above)
        print(f"DEBUG: Processing as normal food input: '{text}'")

        if not is_food_related(text):
            print(f"DEBUG: Text is not food related: '{text}'")
            async def _not_food():
                response = "I'm here to help you log food. What did you eat or drink?"
                yield f"data: {json.dumps({'message': response, 'type': 'response'})}\n\n"
                yield "event: done\ndata: [DONE]\n\n"

            return StreamingResponse(_not_food(), media_type="text/event-stream",
                                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})

        # Extract food information using AI (via Celery task)
        # print(f"DEBUG: Extracting food info from: '{text}'")
        from app.tasks.voice_tasks import extract_food_from_text
        from celery.result import AsyncResult

        # Queue task to Celery (non-blocking)
        task = extract_food_from_text.delay(user_id=user_id, text=text)

        # Wait for result (async polling, doesn't block event loop)
        max_wait = 60
        poll_interval = 0.5
        elapsed = 0
        food_info = None

        while elapsed < max_wait:
            celery_task = AsyncResult(task.id)
            if celery_task.ready():
                if celery_task.successful():
                    food_info = celery_task.result
                    break
                else:
                    raise HTTPException(status_code=500, detail=f"Food extraction failed: {celery_task.info}")
            await asyncio.sleep(poll_interval)
            elapsed += poll_interval

        if food_info is None:
            raise HTTPException(status_code=504, detail="Food extraction timed out")

        foods = food_info.get("foods", [])
        for food in foods:
            if food.get("quantity") is not None:
                nutrition_fields = ensure_nutrition_fields(food)
                for key, value in nutrition_fields.items():
                    food[key] = value
            else:
                for key in NUTRITION_KEYS:
                    food.setdefault(key, None)
        # print(f"DEBUG: Extracted foods: {foods}")
        
        if not foods:
            async def _no_food():
                response = "I couldn't identify any food. Could you tell me what you ate? For example: 'rice', '2 apples', or 'orange juice'"
                yield f"data: {json.dumps({'message': response, 'type': 'response'})}\n\n"
                yield "event: done\ndata: [DONE]\n\n"
            
            return StreamingResponse(_no_food(), media_type="text/event-stream",
                                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})

        # Separate foods with and without quantities
        foods_with_quantity = [f for f in foods if f.get("quantity") is not None]
        foods_without_quantity = [f for f in foods if f.get("quantity") is None]
        # print(f"DEBUG: Foods with quantity: {foods_with_quantity}")
        # print(f"DEBUG: Foods without quantity: {foods_without_quantity}")

        # Foods with quantities are already processed by AI (have nutrition)
        logged_foods = foods_with_quantity

        if foods_without_quantity:
            next_food = foods_without_quantity[0]
            question = await handle_quantity_question(next_food['name'], oai)

            if logged_foods:
                summary = ", ".join(
                    f"{food['quantity']} {food['unit']} of {food['name']}"
                    for food in logged_foods
                )
                ask_message = f"Logged: {summary}\n\n{question}"
            else:
                ask_message = question

            await mem.set_pending(user_id, {
                "state": "awaiting_quantity",
                "foods": foods_without_quantity,
                "current_food_index": 0,
                "logged_foods": logged_foods,
                "original_input": text
            })

            async def _ask_quantity():
                yield f"data: {json.dumps({'message': ask_message, 'type': 'ask_quantity', 'food': next_food['name']})}\n\n"
                yield "event: done\ndata: [DONE]\n\n"

            return StreamingResponse(_ask_quantity(), media_type="text/event-stream",
                                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})
        else:
            # All foods have quantities, log immediately
            # print("DEBUG: All foods have quantities, logging immediately")
            
            async def _logged_then_nav():
                # Store to database
                today_date = datetime.now(IST).strftime("%Y-%m-%d")
                if meal:
                    await store_diet_data_to_db(db, redis, user_id, today_date, logged_foods, meal)

                response_data = create_food_log_response_with_message(logged_foods, db, user_id)

                # Send only the complete food log data (includes the message)
                yield sse_json(response_data)

                # Trigger voice notification via Celery
                try:
                    from app.models.fittbot_models import ClientTarget
                    client_target = db.query(ClientTarget).filter(ClientTarget.client_id == user_id).first()
                    target_calories = client_target.calories if client_target else 0
                    await trigger_food_log_success_voice(user_id, target_calories, db)
                except Exception as e:
                    print(f"[VOICE_TRIGGER] Error in fourth trigger point: {e}")

                yield "event: ping\ndata: {}\n\n"
                yield "event: done\ndata: [DONE]\n\n"
                await mem.set_pending(user_id, {"state":"awaiting_nav_confirm"})
            return StreamingResponse(_logged_then_nav(), media_type="text/event-stream",
                                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})
    
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        print(f"Full traceback: {traceback.format_exc()}")
        
        try:
            await mem.clear_pending(user_id)
        except:
            pass
        
        async def _error():
            yield f"data: {json.dumps({'message': 'Sorry, I encountered an error. Please try again.', 'type': 'error'})}\n\n"
            yield "event: done\ndata: [DONE]\n\n"
        
        return StreamingResponse(_error(), media_type="text/event-stream",
                                headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


def is_quantity_input(text):
    """Check if the input is a quantity (number with optional unit)"""
    if not text:
        return False
    
    text_lower = text.strip().lower()
    
    # Check for pure numbers or numbers with units
    quantity_patterns = [
        r'^\d+(?:\.\d+)?',  # Just numbers: 2, 1.5, 100
        r'^\d+(?:\.\d+)?\s*(g|grams?|kg|plates?|bowls?|pieces?|ml|glasses?|cups?|slices?)'
                          # Numbers with units
    ]
    
    return any(re.match(pattern, text_lower) for pattern in quantity_patterns)
    
def parse_quantity_and_unit(text, food_name, existing_unit=None):
    """Smart parsing that handles numbers, words, and fractions"""
    text_lower = text.lower().strip()

    # NEW: Word-to-number mapping for common quantities
    word_to_number = {
        'half': 0.5, 'a half': 0.5,
        'quarter': 0.25, 'a quarter': 0.25, 'one quarter': 0.25,
        'one fourth': 0.25, 'one-fourth': 0.25,
        'three quarters': 0.75, 'three-quarters': 0.75,
        'one': 1, 'a': 1, 'an': 1, 'one and half': 1.5, 'one and a half': 1.5,
        'two': 2, 'three': 3, 'four': 4, 'five': 5,
        'six': 6, 'seven': 7, 'eight': 8, 'nine': 9, 'ten': 10,
        'couple': 2, 'few': 3
          
    }


    # NEW: Check for word-based quantities FIRST
    for word_phrase, numeric_value in word_to_number.items():
        # Check if the phrase exists in text (with word boundaries)
        pattern = r'\b' + re.escape(word_phrase) + r'\b'
        if re.search(pattern, text_lower):
            # Check if there's a unit mentioned
            unit_pattern = r'\b(?:' + re.escape(word_phrase) + r')\s*(spoons?|tablespoons?|tbsp|teaspoons?|tsp|g|grams?|kg|kilograms?|plates?|bowls?|pieces?|pcs?|pc|ml|milliliters?|glasses?|cups?|slices?|slc)\b'
            unit_match = re.search(unit_pattern, text_lower)
            
            if unit_match:
                unit = unit_match.group(1)
                if unit in ['kg', 'kilogram', 'kilograms']:
                    numeric_value = numeric_value * 1000
                    unit = 'grams'
                else:
                    unit = normalize_unit_with_context(unit, food_name)
                print(f"DEBUG: Word quantity with unit - '{text}' = {numeric_value} {unit}")
                return numeric_value, unit
            else:
                # No unit specified, use existing/default
                final_unit = existing_unit if existing_unit else 'pieces'
                print(f"DEBUG: Word quantity without unit - '{text}' = {numeric_value} {final_unit}")
                return numeric_value, final_unit

    # EXISTING: Pattern matching for numeric quantities with units
    patterns_with_units = [
        r'(\d+(?:\.\d+)?)\s*(tablespoons?|tbsp|teaspoons?|tsp|spoons?)',
        r'(\d+(?:\.\d+)?)\s*(kilograms?|kg|grams?|g\b)',
        r'(\d+(?:\.\d+)?)\s*(plates?|bowls?)',
        r'(\d+(?:\.\d+)?)\s*(pieces?|pcs?|pc)',
        r'(\d+(?:\.\d+)?)\s*(milliliters?|ml|glasses?|cups?)',
        r'(\d+(?:\.\d+)?)\s*(slices?|slc)',
    ]

    user_provided_unit = False
    for pattern in patterns_with_units:
        match = re.search(pattern, text_lower)
        if match:
            quantity = float(match.group(1))
            unit = match.group(2)
            user_provided_unit = True

            if unit in ['kg', 'kilogram', 'kilograms']:
                quantity = quantity * 1000
                unit = 'grams'
            else:
                unit = normalize_unit_with_context(unit, food_name)

            print(f"DEBUG: Numeric with unit - '{text}' = {quantity} {unit}")
            return quantity, unit

    # EXISTING: Check for just numbers
    number_match = re.search(r'(\d+(?:\.\d+)?)', text_lower)
    if number_match:
        quantity = float(number_match.group(1))
        

        # Smart unit detection based on quantity and food type
        if existing_unit == 'ml' and quantity < 10:
            # If existing unit is ml but user entered small number (< 10),
            # interpret as glasses/cups instead
            if is_liquid_food(food_name):
                final_unit = 'glasses'
                print(f"DEBUG: Small number for liquid - '{text}' = {quantity} {final_unit} (converted from ml assumption)")
            else:
                final_unit = existing_unit
        elif existing_unit == 'ml' and quantity >= 10:
            # If user enters 10 or more with ml as existing unit, keep it as ml
            final_unit = 'ml'
            print(f"DEBUG: Large number for liquid - '{text}' = {quantity} {final_unit}")
        elif existing_unit == 'glasses' and is_liquid_food(food_name):
            # For liquids with glasses as default, keep using glasses
            final_unit = 'glasses'
            print(f"DEBUG: Liquid with glasses default - '{text}' = {quantity} {final_unit}")
        elif is_liquid_food(food_name) and not existing_unit:
            # For liquids without existing unit, default to glasses (not pieces)
            final_unit = 'glasses'
            print(f"DEBUG: Liquid without unit - '{text}' = {quantity} {final_unit}")
        else:
            final_unit = existing_unit if existing_unit else 'pieces'
            print(f"DEBUG: Just number - '{text}' = {quantity} {final_unit}")

        return quantity, final_unit

    print(f"DEBUG: Could not parse quantity from '{text}'")
    return None, None

def is_liquid_food(food_name):
    """Determine if a food is primarily liquid"""
    food_lower = food_name.lower()
    liquid_keywords = [
        'juice', 'milk', 'water', 'tea', 'coffee', 'drink', 'smoothie', 
        'shake', 'lassi', 'soup', 'broth', 'wine', 'beer', 'soda'
    ]
    return any(keyword in food_lower for keyword in liquid_keywords)

def is_liquid_food(food_name):
    """Determine if a food is primarily liquid"""
    food_lower = food_name.lower()
    liquid_keywords = [
        'juice', 'milk', 'water', 'tea', 'coffee', 'drink', 'smoothie', 
        'shake', 'lassi', 'soup', 'broth', 'wine', 'beer', 'soda'
    ]
    return any(keyword in food_lower for keyword in liquid_keywords)

# ADD THESE FUNCTIONS HERE (starting around line 634)
async def store_diet_data_to_db(db: Session, redis: Redis, client_id: int, date_str: str, logged_foods: list, meal: str):
    """Store logged food data in actual_diet table, award XP, and invalidate cache"""
    try:
        # Check if entry exists for this client and date
        existing_entry = db.query(ActualDiet).filter(
            ActualDiet.client_id == client_id,
            ActualDiet.date == date_str
        ).first()

        # Create the food item structure for the meal
        food_items = []
        total_calories = 0
        for food in logged_foods:
            calories = food.get('calories', 0)
            total_calories += calories
            food_item = {
                "id": str(int(datetime.now().timestamp() * 1000)),  # Generate unique ID
                "name": food.get('name', ''),
                "quantity": f"{food.get('quantity', 0)} {food.get('unit', 'serving')}",
                "calories": calories,
                "protein": food.get('protein', 0),
                "carbs": food.get('carbs', 0),
                "fat": food.get('fat', 0),
                "fiber": food.get('fiber', 0),
                "sugar": food.get('sugar', 0),
                "calcium": food.get('calcium', 0),
                "magnesium": food.get('magnesium', 0),
                "sodium": food.get('sodium', 0),
                "potassium": food.get('potassium', 0),
                "iron": food.get('iron', 0),
                "iodine": food.get('iodine', 0),
                "image_url": ""  # Empty as requested
            }
            food_items.append(food_item)

        if existing_entry:
            # Parse existing diet_data
            diet_data = existing_entry.diet_data if existing_entry.diet_data else []
            

            # Find the meal category and update it
            meal_found = False
            for meal_category in diet_data:
                # print(meal_category.get("title", "").lower(), meal.lower())
                if meal_category.get("title", "").lower() == meal.lower():
                    # Append to existing foodList
                    meal_category["foodList"].extend(food_items)
                    meal_category["itemsCount"] = len(meal_category["foodList"])
                    meal_found = True
                    break

            if not meal_found:
                # If meal category doesn't exist, this shouldn't happen with your predefined structure
                # But we can handle it by finding the right meal from the template
                default_structure = get_default_diet_structure()
                for default_meal in default_structure:
                    if default_meal.get("title", "").lower() == meal.lower():
                        default_meal["foodList"] = food_items
                        default_meal["itemsCount"] = len(food_items)
                        diet_data.append(default_meal)
                        break
            from sqlalchemy.orm import attributes
            attributes.flag_modified(existing_entry, "diet_data")
            # Update existing record
            existing_entry.diet_data = diet_data
            db.commit()

        else:
            # Create new entry with full structure
            diet_data = get_default_diet_structure()

            # Update the specific meal
            for meal_category in diet_data:
                # print(str(meal_category.get("title", "")).lower(), meal.lower())
                if meal_category.get("title", "").lower() == meal.lower():
                    meal_category["foodList"] = food_items
                    meal_category["itemsCount"] = len(food_items)
                    break

            # Create new record
            new_entry = ActualDiet(
                client_id=client_id,
                date=date_str,
                diet_data=diet_data
            )
            db.add(new_entry)
            db.commit()

        # ============== XP REWARD CALCULATION (copied from actual_diet.py) ==============
        # Get client and gym_id
        client = db.query(Client).filter(Client.client_id == client_id).first()
        gym_id = client.gym_id if client else None

        # Get ClientTarget for target_calories
        client_target = db.query(ClientTarget).filter(ClientTarget.client_id == client_id).first()
        client_target_calories = client_target.calories if client_target else 0

        # Leaderboard/xp logic
        print("DEBUG: Starting leaderboard/XP processing...")
        today = date.today()
        if date_str == str(today):
            print("DEBUG: Processing for today's date...")
            if client_target_calories > 0:
                ratio = total_calories / client_target_calories
                if ratio > 1:
                    ratio = 1
            else:
                ratio = 0

            print(
                f"DEBUG: XP calc -> new_calories={total_calories}, "
                f"target_calories={client_target_calories}, ratio={ratio}"
            )

            calorie_points = int(round(ratio * 50))
            print(f"DEBUG: Initial calorie_points={calorie_points}")
            calorie_event = (
                db.query(CalorieEvent)
                .filter(
                    CalorieEvent.client_id == client_id,
                    CalorieEvent.event_date == today

                )
                .first()
            )

            if not calorie_event:
                calorie = CalorieEvent(
                    client_id=client_id,
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
                        LeaderboardDaily.client_id == client_id,
                        LeaderboardDaily.date == today,
                    )
                    .first()
                )

                if daily_record:
                    prev_daily_xp = daily_record.xp
                    daily_record.xp += calorie_points
                    print(
                        f"DEBUG: Updated daily XP from {prev_daily_xp} to {daily_record.xp} "
                        f"for client_id={client_id}"
                    )
                else:
                    print(
                        f"DEBUG: Creating new daily leaderboard entry "
                        f"with xp={calorie_points} for client_id={client_id}"
                    )
                    new_daily = LeaderboardDaily(
                        client_id=client_id,
                        xp=calorie_points,
                        date=today,
                    )
                    db.add(new_daily)

                month_date = today.replace(day=1)
                monthly_record = (
                    db.query(LeaderboardMonthly)
                    .filter(
                        LeaderboardMonthly.client_id == client_id,
                        LeaderboardMonthly.month == month_date,
                    )
                    .first()
                )

                if monthly_record:
                    prev_monthly_xp = monthly_record.xp
                    monthly_record.xp += calorie_points
                    print(
                        f"DEBUG: Updated monthly XP from {prev_monthly_xp} to {monthly_record.xp} "
                        f"for client_id={client_id}"
                    )
                else:
                    print(
                        f"DEBUG: Creating new monthly leaderboard entry "
                        f"with xp={calorie_points} for client_id={client_id}"
                    )
                    new_monthly = LeaderboardMonthly(
                        client_id=client_id,

                        xp=calorie_points,
                        month=month_date,
                    )
                    db.add(new_monthly)

                overall_record = (
                    db.query(LeaderboardOverall)
                    .filter(
                        LeaderboardOverall.client_id == client_id
                    )
                    .first()
                )

                if overall_record:
                    prev_overall_xp = overall_record.xp
                    overall_record.xp += calorie_points
                    new_total = overall_record.xp
                    print(
                        f"DEBUG: Updated overall XP from {prev_overall_xp} to {overall_record.xp} "
                        f"for client_id={client_id}"
                    )

                    next_row = (
                        db.query(ClientNextXp)
                        .filter_by(client_id=client_id)
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
                                    f"DEBUG: Client {client_id} reached new XP milestone "
                                    f"-> next_xp={next_row.next_xp}"
                                )
                                client_details = client
                                db.add(
                                    RewardPrizeHistory(
                                        client_id=client_id,
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
                                        client_id=client_id,
                                        next_xp=first_tier.xp,
                                        gift=first_tier.gift,
                                    )
                                )

                    db.commit()
                else:
                    print(
                        f"DEBUG: Creating new overall leaderboard entry "
                        f"with xp={calorie_points} for client_id={client_id}"
                    )
                    new_overall = LeaderboardOverall(
                        client_id=client_id, xp=calorie_points
                    )
                    db.add(new_overall)
                    db.commit()

                existing_event = (
                    db.query(CalorieEvent)
                    .filter(
                        CalorieEvent.client_id == client_id,
                        CalorieEvent.event_date == today,
                    )
                    .first()
                )

                if existing_event:
                    before_update = existing_event.calories_added
                    existing_event.calories_added += calorie_points
                    print(
                        f"DEBUG: Updated CalorieEvent from {before_update} to "
                        f"{existing_event.calories_added} for client_id={client_id}"
                    )
                else:
                    print(
                        f"DEBUG: Creating CalorieEvent with calories_added={calorie_points} "
                        f"for client_id={client_id}"
                    )
                    new_event = CalorieEvent(
                        client_id=client_id,
                        event_date=today,
                        calories_added=calorie_points,
                    )
                    db.add(new_event)

                db.commit()
            else:
                calorie_points = 0
        else:
            calorie_points = 0


        await delete_keys_by_pattern(redis, f"{client_id}:*:target_actual")
        await delete_keys_by_pattern(redis, f"{client_id}:*:chart")

        print(f"Successfully stored diet data for client {client_id}, date {date_str}, meal {meal}")
        return calorie_points

    except Exception as e:
        print(f"Error storing diet data: {e}")
        db.rollback()
        return 0

def get_default_diet_structure():
    """Return the default diet structure as shown in your example"""
    return [
        {
            "id": "1",
            "title": "Pre workout",
            "tagline": "Energy boost",
            "foodList": [],
            "timeRange": "6:30-7:00 AM",
            "itemsCount": 0
        },
        {
            "id": "2",
            "title": "Post workout",
            "tagline": "Recovery fuel",
            "foodList": [],
            "timeRange": "7:30-8:00 AM",
            "itemsCount": 0
        },
        {
            "id": "3",
            "title": "Early morning Detox",
            "tagline": "Early morning nutrition",
            "foodList": [],
            "timeRange": "5:30-6:00 AM",
            "itemsCount": 0
        },
        {
            "id": "4",
            "title": "Pre-Breakfast / Pre-Meal Starter",
            "tagline": "Pre-breakfast fuel",
            "foodList": [],
            "timeRange": "7:00-7:30 AM",
            "itemsCount": 0
        },
        {
            "id": "5",
            "title": "Breakfast",
            "tagline": "Start your day right",
            "foodList": [],
            "timeRange": "8:30-9:30 AM",
            "itemsCount": 0
        },
        {
            "id": "6",
            "title": "Mid-Morning snack",
            "tagline": "Healthy meal",
            "foodList": [],
            "timeRange": "10:00-11:00 AM",
            "itemsCount": 0
        },
        {
            "id": "7",
            "title": "Lunch",
            "tagline": "Nutritious midday meal",
            "foodList": [],
            "timeRange": "1:00-2:00 PM",
            "itemsCount": 0
        },
        {
            "id": "8",
            "title": "Evening snack",
            "tagline": "Healthy meal",
            "foodList": [],
            "timeRange": "4:00-5:00 PM",
            "itemsCount": 0
        },
        {
            "id": "9",
            "title": "Dinner",
            "tagline": "End your day well",
            "foodList": [],
            "timeRange": "7:30-8:30 PM",
            "itemsCount": 0
        },
        {
            "id": "10",
            "title": "Bed time",
            "tagline": "Rest well",
            "foodList": [],
            "timeRange": "9:30-10:00 PM",
            "itemsCount": 0
        }
    ]


def get_food_max_reasonable_serving(food_name, unit):
    """Get maximum reasonable serving size for different foods"""
    food_lower = food_name.lower()
    
    # Define reasonable maximums by food type and unit
    max_servings = {
        # Rice dishes
        'rice': {'plates': 3, 'bowls': 4, 'grams': 400},
        'biryani': {'plates': 2, 'bowls': 3, 'grams': 600},
        'pongal': {'plates': 2, 'bowls': 3, 'grams': 400},
        
        # Liquids
        'juice': {'ml': 500, 'glasses': 3, 'cups': 3},
        'milk': {'ml': 500, 'glasses': 3, 'cups': 3},
        'water': {'ml': 1000, 'glasses': 5, 'cups': 5},
        
        # Default maximums by unit
        'default': {'plates': 3, 'bowls': 4, 'pieces': 10, 'grams': 500, 'ml': 1000}
    }
    
    # Check specific food first
    for food_key in max_servings:
        if food_key in food_lower and food_key != 'default':
            return max_servings[food_key].get(unit, max_servings['default'].get(unit, 10))
    
    # Use default
    return max_servings['default'].get(unit, 10)



class Userid(BaseModel):
    user_id: int

@router.get("/debug_pending")
async def debug_pending(
    user_id: int,
    mem = Depends(get_mem),
):
    pending_state = await mem.get_pending(user_id)
    return {"user_id": user_id, "pending_state": pending_state}

@router.post("/delete_chat")
async def chat_close(
    req: Userid,
    mem = Depends(get_mem),
):
    print(f"Deleting chat history for user {req.user_id}")
    history_key = f"chat:{req.user_id}:history"
    pending_key = f"chat:{req.user_id}:pending"
    deleted = await mem.r.delete(history_key, pending_key)
    return {"status": 200}

@router.post("/clear_pending")
async def clear_pending_state(
    req: Userid,
    mem = Depends(get_mem),
):
    await mem.clear_pending(req.user_id)
    return {"status": "cleared", "user_id": req.user_id}


# Pydantic model for meal selector voice request
class MealSelectorVoiceRequest(BaseModel):
    user_id: int

@router.post("/meal-selector/voice")
async def meal_selector_voice(
    request: MealSelectorVoiceRequest,
    db: Session = Depends(get_db)
):
    """Trigger voice notification when meal selector modal opens"""
    try:
        user_id = request.user_id

        print(f"[MEAL_SELECTOR_VOICE] Received request for user {user_id}")

        # Trigger voice notification
        await trigger_meal_selector_voice(user_id, db)

        return {
            "status": "success",
            "message": "Meal selector voice notification triggered",
            "user_id": user_id
        }

    except Exception as e:
        print(f"[MEAL_SELECTOR_VOICE] Error: {e}")
        return {
            "status": "error",
            "message": f"Failed to trigger voice notification: {str(e)}"
        }
