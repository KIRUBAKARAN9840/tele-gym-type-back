from fastapi import APIRouter, HTTPException, Query, Depends
from fastapi.responses import StreamingResponse
from fastapi_limiter.depends import RateLimiter
from pydantic import BaseModel
import pytz, os, hashlib, orjson, re, json
from datetime import datetime
from sqlalchemy.orm import Session
from app.models.database import get_db
from app.models.deps import get_http, get_oai, get_mem
from app.models.fittbot_models import WeightJourney,Client,ClientTarget, VoicePreference
import json, re, os, random, uuid, traceback
import httpx
import asyncio
from fastapi import APIRouter, HTTPException, UploadFile, File, Query, Depends

from app.models.fittbot_models import ClientDietTemplate
from app.fittbot_api.v1.client.client_api.chatbot.chatbot_services.asr import transcribe_audio
import orjson 

from app.fittbot_api.v1.client.client_api.chatbot.chatbot_services.llm_helpers import (
   PlainTextStreamFilter, oai_chat_stream, GENERAL_SYSTEM, TOP_K,
   build_messages, heuristic_confidence, gpt_extract_items, first_missing_quantity,OPENAI_MODEL,
   sse_json, sse_escape, gpt_small_route, _scale_macros, is_yes, is_no, is_fit_chat,
   has_action_verb, food_hits,ensure_per_unit_macros, is_fittbot_meta_query,normalize_food,
   explicit_log_command, STYLE_PLAN, is_plan_request,STYLE_CHAT_FORMAT,pretty_plan
)

# Import Indian fast meal generator (database-powered, 10-20s generation)
# Uses indian_food_master database with 5,231+ Indian food items
from app.fittbot_api.v1.client.client_api.chatbot.chatbot_services.indian_fast_meal_generator import (
    generate_indian_7_days_parallel_fast as generate_7_days_parallel_fast,
    convert_indian_fast_results_to_meal_data as convert_fast_results_to_meal_data,
    ai_understand_indian_user_preferences as ai_understand_user_preferences,  # DEPRECATED - use Celery version
    ai_understand_indian_user_preferences_celery as ai_understand_user_preferences_celery,  # Celery+Redis rate-limited
    generate_indian_food_alternatives as generate_food_alternatives
)


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


async def trigger_food_template_voice(client_id: int, voice_type: str, db: Session):
    """Trigger voice notification via Celery task for food template events"""
    try:
        # Check voice preference using existing async helper
        voice_pref = await get_voice_preference(db, client_id)

        if voice_pref == "1":  # Voice enabled
            from app.tasks.voice_tasks import process_food_template_voice
            # Trigger Celery task for non-blocking voice processing
            process_food_template_voice.delay(client_id, voice_type)
            print(f"[FOOD_TEMPLATE_VOICE_TRIGGER] {voice_type} voice notification triggered for client {client_id}")
        else:
            print(f"[FOOD_TEMPLATE_VOICE_TRIGGER] Voice disabled for client {client_id}, skipping {voice_type} voice notification")

    except Exception as e:
        print(f"[FOOD_TEMPLATE_VOICE_TRIGGER] Error triggering {voice_type} voice notification: {e}")


def sse_data(content: str) -> bytes:
    """
    Properly format content for SSE transmission with UTF-8 Unicode support.
    SSE requires 'data: ' prefix and double newline. Content is sent as plain UTF-8.
    Returns bytes to prevent buffering.
    """
    if isinstance(content, bytes):
        content = content.decode('utf-8', errors='replace')

    lines = content.split('\n')
    if len(lines) == 1:
        result = f"data: {content}\n\n"
    else:
        result = ''.join(f"data: {line}\n" for line in lines) + "\n"

    return result.encode('utf-8')


def sse_json_bytes(obj: dict) -> bytes:
    """
    Format JSON object as SSE event and return as bytes to prevent buffering.
    """
    json_str = json.dumps(obj, ensure_ascii=False)
    return f"data: {json_str}\n\n".encode('utf-8')


def sse_done() -> bytes:
    """Return SSE done event as bytes"""
    return b"event: done\ndata: [DONE]\n\n"


router = APIRouter(prefix="/food_template", tags=["food_template"])


APP_ENV = os.getenv("APP_ENV", "prod")
TZNAME = os.getenv("TZ", "Asia/Kolkata")
IST = pytz.timezone(TZNAME)
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

# Enable fast meal generation (database-powered, 10-20s vs 3-4 minutes)
USE_FAST_GENERATION = os.getenv("USE_FAST_GENERATION", "true").lower() == "true"


@router.get("/healthz")
async def healthz():
   return {"ok": True, "env": APP_ENV, "tz": TZNAME}


@router.get("/test_stream")
async def test_stream():
    """Test endpoint to verify SSE streaming works"""
    async def generate():
        print("🧪 Starting test stream...")
        for i in range(5):
            print(f"📤 Sending message {i+1}...")
            yield sse_data(f"Message {i+1} - Current time: {datetime.now()}")
            await asyncio.sleep(0)  # Force flush
            print(f"✅ Message {i+1} sent")
            await asyncio.sleep(1)  # Wait 1 second between messages

        print("🎉 Test stream complete")
        yield b"event: done\ndata: [DONE]\n\n"

    return StreamingResponse(
        generate(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"}
    )

@router.post("/voice/transcribe")
async def voice_transcribe(
    audio: UploadFile = File(...),
    user_id: int = Query(0, description="User ID for rate limiting"),
    http = Depends(get_http),
):
    """Transcribe audio to text and translate to English (uses Celery for rate limiting)"""
    try:
        # Use the transcribe function that's already imported from asr.py
        transcript = await transcribe_audio(audio, http=http)
        if not transcript:
            raise HTTPException(400, "empty transcript")

        # Use Celery for rate-limited translation
        tinfo = await translate_text_celery(user_id, transcript)
        transcript_en = tinfo["english"]
        lang_code = tinfo["lang"]

        return {
            "transcript": transcript_en,
            "lang": lang_code,
            "english": transcript_en,
            "raw_transcript": transcript  # Include original for debugging
        }

    except Exception as e:
        print(f"Voice transcribe error: {e}")
        raise HTTPException(500, f"Transcription failed: {str(e)}")

def _fetch_profile(db: Session, client_id: int):
   """Fetch complete client profile including weight journey and calorie targets"""
   try:
       # Get latest weight journey
       
       weight_delta_text = None
       goal_type = "maintain"
      

      
       # Get client details
       c = db.query(Client).where(Client.client_id == client_id).first()
       client_goal = (getattr(c, "goals", None) or getattr(c, "goal", None) or "muscle gain") if c else "muscle gain"
       lifestyle= c.lifestyle if c else "moderate"
      
       # Get calorie target
       ct = db.query(ClientTarget).where(ClientTarget.client_id == client_id).first()
       target_calories = float(ct.calories) if ct and ct.calories else 2000.0
       target_weight=ct.weight if ct.weight else None
       current_weight=c.weight if c.weight else None

       if current_weight is not None and target_weight is not None:
           diff = round(target_weight - current_weight, 1)
           if diff > 0:
               weight_delta_text = f"Gain {abs(diff)} kg (from {current_weight} → {target_weight})"
               goal_type = "weight_gain"
           elif diff < 0:
               weight_delta_text = f"Lose {abs(diff)} kg (from {current_weight} → {target_weight})"
               goal_type = "weight_loss"
           else:
               weight_delta_text = f"Maintain {current_weight} kg"
               goal_type = "maintain"


      
       return {
           "client_id": client_id,
           "current_weight": current_weight,
           "target_weight": target_weight,
           "weight_delta_text": weight_delta_text,
           "client_goal": client_goal,
           "goal_type": goal_type,
           "target_calories": target_calories,
           "lifestyle": lifestyle,
           "days_per_week": 6,  # Mon–Sat
       }
  
   except Exception as e:
       print(f"Error fetching profile for client {client_id}: {e}")
       print(f"Profile fetch traceback: {traceback.format_exc()}")
       # Return default profile for testing
       return {
           "client_id": client_id,
           "current_weight": 70.0,
           "target_weight": 65.0,
           "weight_delta_text": "Lose 5.0 kg (from 70.0 → 65.0)",
           "client_goal": "weight loss",
           "goal_type": "weight_loss",
           "target_calories": 1800.0,
           "lifestyle": "moderate",
           "days_per_week": 6,
       }


def get_meal_template():
   """Get the base meal template structure"""
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


_food_id_counter = 0

def generate_food_id():
   """Generate a unique food ID similar to the example format"""
   global _food_id_counter
   import time
   # Create a unique ID using timestamp + counter + random number to ensure uniqueness
   timestamp_ms = int(time.time() * 1000)
   _food_id_counter += 1
   random_suffix = random.randint(100, 999)
   return str(timestamp_ms + _food_id_counter + random_suffix)


def get_food_image_url(food_name):
   """Generate food image URL based on food name"""
   # Clean food name for URL
   clean_name = food_name.replace(' ', '+')
   return f"add_image{clean_name}.png"


def create_food_item(name, calories, protein, carbs, fat, quantity, fiber=0, sugar=0):
   """Create a food item in the required format with proper quantity"""
   return {
       "id": generate_food_id(),
       "fat": fat,
       "name": name,
       "carbs": carbs,
       "fiber": fiber,
       "sugar": sugar,
       "protein": protein,
       "calories": calories,
       "quantity": quantity,
       "image_url": ""
   }


def save_meal_plan_to_database(client_id: int, meal_plan: dict, db: Session, replace_all: bool = False):
    """Save meal plan to database - only replaces templates with matching names"""
    try:
        # Standard day mapping
        standard_day_names = {
            'monday': 'Monday', 'tuesday': 'Tuesday', 'wednesday': 'Wednesday',
            'thursday': 'Thursday', 'friday': 'Friday', 'saturday': 'Saturday', 
            'sunday': 'Sunday'
        }
        
        if replace_all:
            # Full replacement - delete all existing records (only when explicitly requested)
            deleted_count = db.query(ClientDietTemplate).filter(ClientDietTemplate.client_id == client_id).delete()
        else:
            # Selective replacement - only delete records that will be replaced
            names_to_replace = []
            for day_key in meal_plan.keys():
                day_name = standard_day_names.get(day_key.lower(), day_key.replace('_', ' ').title())
                names_to_replace.append(day_name)

            if names_to_replace:
                deleted_count = db.query(ClientDietTemplate).filter(
                    ClientDietTemplate.client_id == client_id,
                    ClientDietTemplate.template_name.in_(names_to_replace)
                ).delete(synchronize_session=False)
            else:
                deleted_count = 0
        
        saved_count = 0
        
        # Save each day
        for day_key, day_data in meal_plan.items():
            try:
                # Handle custom day names
                day_name = standard_day_names.get(day_key.lower(), day_key.replace('_', ' ').title())
                
                # FIX: Store raw Python objects in JSON column, not JSON strings
                if isinstance(day_data, str):
                    # If it's a JSON string, parse it back to Python object
                    try:
                        diet_data_obj = json.loads(day_data)
                    except json.JSONDecodeError:
                        # If parsing fails, treat as plain text (shouldn't happen for meal plans)
                        diet_data_obj = {"error": "Invalid JSON", "raw_data": day_data}
                else:
                    # If it's already a Python object, use directly
                    diet_data_obj = day_data

                new_record = ClientDietTemplate(
                    client_id=client_id,
                    template_name=day_name,
                    diet_data=diet_data_obj
                )
                db.add(new_record)
                saved_count += 1
                
            except Exception as day_error:
                print(f"ERROR: Failed to save day {day_key}: {day_error}")
                continue
        
        # Commit all changes
        db.commit()

        return {
            'success': True, 
            'saved_count': saved_count,
            'deleted_count': deleted_count,
            'replace_all': replace_all,
            'message': f'Saved {saved_count} meal templates (deleted {deleted_count} old ones)'
        }
        
    except Exception as e:
        print(f"ERROR: Database save failed: {e}")
        print(f"ERROR: Full traceback: {traceback.format_exc()}")
        try:
            db.rollback()
        except Exception as rollback_error:
            print(f"ERROR: Rollback failed: {rollback_error}")
        
        return {
            'success': False, 
            'error': str(e),
            'message': f'Database save failed: {str(e)}'
        }


def has_custom_day_names(meal_plan: dict):
    """Check if meal plan contains custom day names (not standard Monday-Sunday)"""
    standard_keys = {'monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday'}
    actual_keys = {key.lower().replace('_', ' ').replace(' ', '') for key in meal_plan.keys()}
    
    # Check if any key doesn't match standard day names
    for key in meal_plan.keys():
        normalized_key = key.lower().replace('_', '').replace(' ', '')
        if normalized_key not in standard_keys:
            return True
    return False


def get_standard_quantity_for_food(food_name):
    """Return standard quantity with exact gram/ml measurements plus cup/bowl descriptions"""
    food_name_lower = food_name.lower()
    
    # Liquids - use ml with cup measurements
    if any(liquid in food_name_lower for liquid in ['water', 'juice', 'coconut water']):
        if 'water' in food_name_lower:
            return "250 ml | 1 cup | 1 large glass"
        else:
            return "200 ml | 3/4 cup | 1 medium glass"
    
    if any(liquid in food_name_lower for liquid in ['lassi', 'buttermilk']):
        return "200 ml | 3/4 cup | 1 glass"
    
    # Hot beverages
    if any(drink in food_name_lower for drink in ['tea', 'coffee', 'chai']):
        return "150 ml | 2/3 cup | 1 small glass"
    
    # Milk
    if 'milk' in food_name_lower:
        return "200 ml | 3/4 cup | 1 glass"
    
    # Bread items
    if any(bread in food_name_lower for bread in ['roti', 'chapati']):
        return "60 grams | 2 medium pieces"
    if any(bread in food_name_lower for bread in ['naan', 'kulcha']):
        return "80 grams | 1 medium piece"
    if 'paratha' in food_name_lower:
        return "70 grams | 1 medium piece"
    if any(bread in food_name_lower for bread in ['bread', 'toast']):
        return "50 grams | 2 slices"
    
    # Rice and grains
    if 'rice' in food_name_lower:
        return "150 grams | 3/4 cup cooked | 1 bowl"
    if any(grain in food_name_lower for grain in ['upma', 'poha', 'daliya']):
        return "150 grams | 3/4 cup | 1 bowl"
    if 'oats' in food_name_lower:
        return "50 grams | 1/2 cup dry | 1 bowl cooked"
    if 'cornflakes' in food_name_lower:
        return "30 grams | 1 cup | 1 bowl"
    
    # South Indian items
    if 'idli' in food_name_lower:
        return "120 grams | 3 pieces"
    if 'dosa' in food_name_lower:
        return "150 grams | 1 medium piece"
    if 'uttapam' in food_name_lower:
        return "120 grams | 1 piece"
    
    # Dal and curries
    if any(dal in food_name_lower for dal in ['dal', 'sambar', 'rasam']):
        return "150 grams | 3/4 cup | 1 bowl"
    if 'curry' in food_name_lower or 'sabzi' in food_name_lower:
        return "100 grams | 1/2 cup | 3/4 bowl"
    
    # Paneer dishes
    if 'paneer' in food_name_lower:
        return "100 grams | 1/2 cup cubes | 3/4 bowl"
    
    # Non-veg items
    if any(meat in food_name_lower for meat in ['chicken', 'mutton', 'fish']):
        return "100 grams | 1/2 cup | 3-4 pieces"
    if 'egg' in food_name_lower:
        if 'boiled' in food_name_lower or 'fried' in food_name_lower:
            return "100 grams | 2 pieces"
        else:
            return "50 grams | 1 piece"
    
    # Vegetables
    if any(veg in food_name_lower for veg in ['vegetables', 'aloo', 'gobi', 'bhindi', 'palak']):
        return "100 grams | 1/2 cup | 3/4 bowl"
    
    # Dairy
    if 'curd' in food_name_lower or 'raita' in food_name_lower:
        return "100 grams | 1/2 cup | 3/4 bowl"
    
    # Fruits
    if 'banana' in food_name_lower:
        return "100 grams | 1 medium piece"
    if 'apple' in food_name_lower:
        return "150 grams | 1 medium piece"
    if 'orange' in food_name_lower:
        return "120 grams | 1 medium piece"
    if any(fruit in food_name_lower for fruit in ['mango', 'papaya']):
        return "100 grams | 1/2 cup sliced | 3/4 bowl"
    if 'fruit' in food_name_lower:
        return "100 grams | 1/2 cup mixed | 3/4 bowl"
    
    # Nuts and snacks
    if any(nut in food_name_lower for nut in ['almonds', 'walnuts', 'cashews']):
        return "30 grams | 1/4 cup | 20-25 pieces"
    if 'nuts' in food_name_lower:
        return "30 grams | 1/4 cup | handful"
    if any(snack in food_name_lower for snack in ['mixture', 'namkeen', 'bhujia']):
        return "30 grams | 1/4 cup | small bowl"
    
    # Chutneys and condiments
    if 'chutney' in food_name_lower:
        return "20 grams | 2 tablespoons | 1/4 cup"
    if 'pickle' in food_name_lower:
        return "10 grams | 1 tablespoon"
    
    # Papad and similar
    if 'papad' in food_name_lower:
        return "5 grams | 1 piece"
    
    # Salad
    if 'salad' in food_name_lower:
        return "80 grams | 1/2 cup | 1 small bowl"
    
    # Oil and ghee
    if any(fat in food_name_lower for fat in ['oil', 'ghee', 'butter']):
        return "10 grams | 2 teaspoons"
    
    # Sugar and jaggery
    if any(sweet in food_name_lower for sweet in ['sugar', 'jaggery', 'honey']):
        return "15 grams | 1 tablespoon"
    
    # Soups
    if 'soup' in food_name_lower:
        return "200 ml | 3/4 cup | 1 bowl"
    
    # Dry fruits
    if any(dry_fruit in food_name_lower for dry_fruit in ['dates', 'raisins', 'figs']):
        return "30 grams | 1/4 cup | 8-10 pieces"
    
    # Cereals and pulses (raw)
    if any(pulse in food_name_lower for pulse in ['moong', 'chana', 'rajma', 'lentil']):
        return "30 grams | 1/4 cup | 2 tablespoons dry"
    
    # Sweets and desserts
    if any(sweet in food_name_lower for sweet in ['laddu', 'barfi', 'halwa', 'kheer']):
        return "50 grams | 1/4 cup | 1 small piece"
    
    # Biscuits and cookies
    if any(biscuit in food_name_lower for biscuit in ['biscuit', 'cookie', 'rusk']):
        return "25 grams | 3-4 pieces"
    
    # Additional items
    if any(drink in food_name_lower for drink in ['smoothie', 'shake']):
        return "200 ml | 3/4 cup | 1 glass"
    
    if any(grain in food_name_lower for grain in ['quinoa', 'millet', 'barley']):
        return "150 grams | 3/4 cup cooked | 1 bowl"
    
    # Default fallback
    return "100 grams | 1/2 cup | 1 serving"


async def _store_meal_template(mem, db, user_id, meal_plan, name):
    """Store meal template using the async method"""
    try:
        await mem.set(f"meal_template:{user_id}", {
            "template": meal_plan,
            "name": name,
            "client_id": user_id,
            "created_at": datetime.now().isoformat()
        })
        print(f"Stored meal template '{name}' for user {user_id}")
        return True
    except Exception as e:
        print(f"Failed to store meal template: {e}")
        return False



def format_meal_plan_for_user_display(meal_plan, diet_type, cuisine_type, target_calories):
    """Format meal plan data into an attractive user-friendly display"""
    
    formatted_display = {
        "type": "meal_plan_display",
        "diet_type": diet_type,
        "cuisine_type": cuisine_type.replace('_', ' ').title(),
        "target_calories_per_day": target_calories,
        "total_days": len(meal_plan),
        "days": []
    }
    
    # Process each day
    for day_name, day_meals in meal_plan.items():
        day_display = {
            "day_name": day_name.replace('_', ' ').title(),
            "day_key": day_name,
            "total_day_calories": 0,
            "total_day_protein": 0,
            "total_day_carbs": 0,
            "total_day_fat": 0,
            "meal_slots": []
        }
        
        # Process each meal slot for this day
        for meal_slot in day_meals:
            slot_display = {
                "slot_id": meal_slot.get("id"),
                "title": meal_slot.get("title", ""),
                "time_range": meal_slot.get("timeRange", ""),
                "foods": [],
                "slot_calories": 0,
                "slot_protein": 0,
                "slot_carbs": 0,
                "slot_fat": 0,
                "food_count": meal_slot.get("itemsCount", 0)
            }
            
            # Process each food item in this slot
            for food_item in meal_slot.get("foodList", []):
                food_display = {
                    "name": food_item.get("name", ""),
                    "quantity": food_item.get("quantity", ""),
                    "calories": food_item.get("calories", 0) or 0,
                    "protein": food_item.get("protein", 0) or 0,
                    "carbs": food_item.get("carbs", 0) or 0,
                    "fat": food_item.get("fat", 0) or 0,
                    "date": food_item.get("date", ""),
                    "editable": True,
                    "food_id": food_item.get("id", "")
                }
                
                slot_display["foods"].append(food_display)
                
                # Add to slot totals
                slot_display["slot_calories"] += food_display["calories"]
                slot_display["slot_protein"] += food_display["protein"]
                slot_display["slot_carbs"] += food_display["carbs"]
                slot_display["slot_fat"] += food_display["fat"]
            
            # Round slot totals
            slot_display["slot_calories"] = round(slot_display["slot_calories"])
            slot_display["slot_protein"] = round(slot_display["slot_protein"], 1)
            slot_display["slot_carbs"] = round(slot_display["slot_carbs"], 1)
            slot_display["slot_fat"] = round(slot_display["slot_fat"], 1)
            
            day_display["meal_slots"].append(slot_display)
            
            # Add to day totals
            day_display["total_day_calories"] += slot_display["slot_calories"]
            day_display["total_day_protein"] += slot_display["slot_protein"]
            day_display["total_day_carbs"] += slot_display["slot_carbs"]
            day_display["total_day_fat"] += slot_display["slot_fat"]
        
        # Round day totals
        day_display["total_day_calories"] = round(day_display["total_day_calories"])
        day_display["total_day_protein"] = round(day_display["total_day_protein"], 1)
        day_display["total_day_carbs"] = round(day_display["total_day_carbs"], 1)
        day_display["total_day_fat"] = round(day_display["total_day_fat"], 1)
        
        formatted_display["days"].append(day_display)
    
    return formatted_display
def get_meal_emoji(meal_title):
    """Get emoji for meal slot - mobile friendly"""
    title_lower = meal_title.lower()
    
    if "early morning" in title_lower:
        return "🌅"
    elif "breakfast" in title_lower:
        return "🍳"
    elif "morning" in title_lower:
        return "🥨"
    elif "lunch" in title_lower:
        return "🍽️"
    elif "evening" in title_lower:
        return "☕"
    elif "dinner" in title_lower:
        return "🌙"
    else:
        return "🍴"

def format_day_loader_json(day_name):
    """Format a loader JSON event for generating a specific day with animation"""
    day_display_name = day_name.replace('_', ' ').title()
    return {
        "type": "loader",
        "is_loader": True,
        "day_name": day_display_name,
        "message": f"Generating {day_display_name} meal plan..."
    }


def format_single_day_for_streaming(day_name, day_meals):
    """Format a single day's meal plan for streaming (appends to existing message)"""

    message_parts = []
    day_display_name = day_name.replace('_', ' ').title()
    message_parts.append(f"📅 {day_display_name.upper()}")
    message_parts.append("─" * 15)

    day_total_calories = 0

    for meal_slot in day_meals:
        title = meal_slot.get("title", "")
        time_range = meal_slot.get("timeRange", "")
        foods = meal_slot.get("foodList", [])

        if foods:
            meal_emoji = get_meal_emoji(title)
            message_parts.append(f"\n{meal_emoji} {title}")
            message_parts.append(f"⏰ {time_range}")

            slot_calories = 0
            for food in foods:
                name = food.get("name", "")
                quantity = food.get("quantity", "")
                calories = food.get("calories", 0)
                protein = food.get("protein", 0)
                carbs = food.get("carbs", 0)
                fat = food.get("fat", 0)

                message_parts.append(f"  • {name}")
                message_parts.append(f"    Qty: {quantity}")
                message_parts.append(f"    {calories}cal | {protein}g protein | {carbs}g carbs | {fat}g fat")

                slot_calories += calories
                day_total_calories += calories

            message_parts.append(f"Total: {slot_calories} cal")

    message_parts.append(f"\n  Day Total: {day_total_calories} calories")
    message_parts.append("=" * 22 + "\n")

    return "\n".join(message_parts)


def create_user_friendly_meal_plan_message(meal_plan, diet_type, cuisine_type, target_calories):
    """Create a formatted text message showing the meal plan structure"""

    message_parts = []
    message_parts.append("🍽️ YOUR MEAL PLAN")
    message_parts.append(f"Diet: {diet_type.title()}")
    message_parts.append(f"Style: {cuisine_type.replace('_', ' ').title()}")
    message_parts.append(f"Daily Goal: {target_calories} cal")
    message_parts.append("")
    
    for day_name, day_meals in meal_plan.items():
        day_display_name = day_name.replace('_', ' ').title()
        message_parts.append(f"📅 {day_display_name.upper()}")
        message_parts.append("─" * 6)
        
        day_total_calories = 0
        
        for meal_slot in day_meals:
            title = meal_slot.get("title", "")
            time_range = meal_slot.get("timeRange", "")
            foods = meal_slot.get("foodList", [])
            
            if foods:
                meal_emoji = get_meal_emoji(title)
                message_parts.append(f"{meal_emoji} {title}")
                message_parts.append(f"⏰ {time_range}")
                
                slot_calories = 0
                for food in foods:
                    name = food.get("name", "")
                    quantity = food.get("quantity", "")
                    calories = food.get("calories", 0)
                    protein = food.get("protein", 0)
                    carbs = food.get("carbs", 0)
                    fat = food.get("fat", 0)
                    
                    message_parts.append(f"  • {name}")
                    message_parts.append(f"    Qty: {quantity}")
                    message_parts.append(f"    {calories}cal | {protein}g protein | {carbs}g carbs | {fat}g fat")
                    
                    slot_calories += calories
                
                message_parts.append(f"Total: {slot_calories} cal")
                message_parts.append("")
        
        # message_parts.append(f"  Day Total: {day_total_calories} calories")
        message_parts.append("=" * 22 + "\n")
    
    message_parts.append("You can edit any food item by telling me what you'd like to change!")
    message_parts.append("Continue with day name customization or finalization when ready.")
    
    return "\n".join(message_parts)

def detect_food_edit_request(text):
    """Detect if user wants to edit a specific food item"""
    text_lower = text.lower().strip()
    
    edit_patterns = [
        r'change\s+(\w+)\s+(\w+)\s+to\s+(.+)',
        r'replace\s+(.+)\s+with\s+(.+)',
        r'edit\s+(\w+)\s+(\w+)',
        r'modify\s+(.+)',
        r'update\s+(.+)',
    ]
    
    for pattern in edit_patterns:
        match = re.search(pattern, text_lower)
        if match:
            return {
                'is_edit_request': True,
                'original_text': text,
                'pattern_match': match.groups()
            }
    
    return {'is_edit_request': False}

def detect_food_restrictions(text):
    """Detect food allergies/restrictions from user input"""
    text = text.lower().strip()
    
    # Skip simple action words that aren't about allergies
    simple_actions = ['remove', 'change', 'edit', 'modify', 'update', 'replace']
    if text in simple_actions:
        return None
    
    # Common allergen patterns
    allergen_patterns = {
        'peanuts': [r'\bpeanut\b', r'\bpeanuts\b', r'\bgroundnut\b', r'\bgroundnuts\b'],
        'tree_nuts': [r'\bnuts?\b', r'\balmond\b', r'\bwalnut\b', r'\bcashew\b', r'\bpistachio\b'],
        'dairy': [r'\bdairy\b', r'\bmilk\b', r'\bcheese\b', r'\bpaneer\b', r'\bbutter\b', r'\bghee\b', r'\byogurt\b', r'\bcurd\b'],
        'gluten': [r'\bgluten\b', r'\bwheat\b', r'\bbread\b', r'\broti\b', r'\bchapati\b'],
        'eggs': [r'\begg\b', r'\beggs\b'],
        'fish': [r'\bfish\b', r'\bseafood\b'],
        'shellfish': [r'\bshrimp\b', r'\bcrab\b', r'\blobster\b', r'\bshellfish\b'],
        'soy': [r'\bsoy\b', r'\bsoya\b', r'\btofu\b'],
        'sesame': [r'\bsesame\b', r'\btil\b'],
        'coconut': [r'\bcoconut\b', r'\bnariyal\b'],
        'onion_garlic': [r'\bonion\b', r'\bgarlic\b', r'\bpyaz\b', r'\blahsun\b']
    }
    
    # Restriction trigger words - must have both trigger AND specific food
    restriction_triggers = [
        r'\ballerg\w*\s+to\b',  # allergic to
        r'\bremove.*\b(all|any)\b',  # remove all/any
        r'\bavoid.*\b(all|any)\b',  # avoid all/any  
        r'\bcan\'?t\s+eat\b',
        r'\bdont?\s+eat\b',
        r'\bdon\'?t\s+eat\b',
        r'\bnot\s+allowed\b',
        r'\brestrict\w*\s+from\b',
        r'\bintoleran\w*\s+to\b',
        r'\bsensitiv\w*\s+to\b',
        r'\bexclude.*from\b',
        r'\bi\s+am\s+allergic\b',
        r'\bi\s+have.*allergy\b'
    ]
    
    # Check if text contains meaningful restriction triggers
    has_restriction_trigger = any(re.search(pattern, text) for pattern in restriction_triggers)
    
    # Also check for "no [food]" patterns
    no_food_pattern = r'\bno\s+(\w+)'
    no_food_matches = re.findall(no_food_pattern, text)
    
    if not has_restriction_trigger and not no_food_matches:
        return None
    
    # Find specific allergens/foods mentioned
    found_restrictions = []
    for allergen, patterns in allergen_patterns.items():
        if any(re.search(pattern, text) for pattern in patterns):
            found_restrictions.append(allergen)
    
    # Also extract other food names mentioned with restrictions
    other_foods = []
    if no_food_matches:
        other_foods.extend(no_food_matches)
    
    # Look for foods mentioned after restriction triggers
    words = text.split()
    for i, word in enumerate(words):
        if any(re.search(trigger, ' '.join(words[i:i+3])) for trigger in restriction_triggers):
            for j in range(i+1, min(i+5, len(words))):
                next_word = words[j].strip('.,!?')
                if len(next_word) > 2 and next_word not in ['the', 'and', 'or', 'any', 'from', 'plan', 'all']:
                    other_foods.append(next_word)
    
    result = {
        'found_allergens': found_restrictions,
        'other_foods': list(set(other_foods)),  # Remove duplicates
        'raw_text': text
    }
    
    return result if found_restrictions or other_foods else None


# REMOVED: regenerate_meal_plan_without_allergens() - Using DB generation with avoid_foods parameter
# The entire 211-line function has been removed as it's replaced by generate_7_days_parallel_fast() with avoid_foods parameter


def simple_food_removal(meal_plan, foods_to_remove, diet_type="vegetarian", cuisine_type=None, db=None):
    """
    Smart food removal with automatic replacement using FoodSwapEngine

    Args:
        meal_plan: The meal plan dict with days and meal slots
        foods_to_remove: List of food names to remove
        diet_type: User's diet type for replacement selection
        cuisine_type: User's cuisine preference for replacement
        db: Database session for finding replacements from IndianFoodMaster
    """
    try:
        if not foods_to_remove:
            return meal_plan

        # Convert foods to lowercase for matching
        remove_foods_lower = [food.lower().strip() for food in foods_to_remove]

        updated_meal_plan = {}
        removed_items_count = 0

        # Initialize swap engine if db is available
        swap_engine = FoodSwapEngine(db) if db else None

        for day_key, day_data in meal_plan.items():
            updated_day_data = []

            for meal_slot in day_data:
                updated_food_list = []
                slot_calories_removed = 0
                removed_food_id = None
                slot_id = meal_slot.get('id', '1')

                for food_item in meal_slot.get('foodList', []):
                    food_name = food_item.get('name', '').lower()

                    # Check if this food should be removed
                    should_remove = any(remove_food in food_name for remove_food in remove_foods_lower)

                    if should_remove:
                        slot_calories_removed += food_item.get('calories', 0) or 0
                        removed_items_count += 1
                        # Store the food ID for finding similar replacement
                        if 'id' in food_item:
                            removed_food_id = food_item.get('id')
                    else:
                        updated_food_list.append(food_item)

                # Update the meal slot
                updated_slot = meal_slot.copy()
                updated_slot['foodList'] = updated_food_list
                updated_slot['itemsCount'] = len(updated_food_list)

                # Smart replacement using FoodSwapEngine
                if slot_calories_removed > 20:  # Lower threshold for better coverage
                    replacement_added = False

                    # Try to get intelligent replacement from database
                    if swap_engine and removed_food_id:
                        try:
                            # Find similar foods from database
                            alternatives = swap_engine.find_food_swaps(
                                food_id=int(removed_food_id),
                                diet_type=diet_type,
                                cuisine=cuisine_type,
                                limit=3
                            )

                            if alternatives:
                                # Use the best match (highest similarity)
                                best_match = alternatives[0]
                                replacement_food = {
                                    "id": str(best_match['id']),
                                    "name": best_match['food_name'],
                                    "quantity": best_match['quantity'],
                                    "calories": best_match['calories'],
                                    "protein": best_match['protein'],
                                    "carbs": best_match['carbs'],
                                    "fat": best_match['fat'],
                                    "fiber": best_match.get('fiber', 0),
                                    "sugar": best_match.get('sugar', 0),
                                    "image_url": best_match.get('image_url', '')
                                }
                                updated_slot['foodList'].append(replacement_food)
                                updated_slot['itemsCount'] = len(updated_slot['foodList'])
                                replacement_added = True
                                print(f"✅ Replaced with {best_match['food_name']} (similarity: {best_match.get('similarity_score', 0)})")
                        except Exception as swap_error:
                            print(f"Warning: FoodSwapEngine failed: {swap_error}")

                    # Fallback: Use generic replacement if slot is empty and no DB replacement worked
                    if not replacement_added and len(updated_slot['foodList']) == 0:
                        replacement_food = create_food_item(
                            name="Mixed vegetables",
                            calories=min(slot_calories_removed, 100),  # Cap at 100 cal
                            protein=3,
                            carbs=8,
                            fat=2,
                            quantity="100 grams | 1/2 cup | 3/4 bowl",
                            fiber=3,
                            sugar=5
                        )
                        updated_slot['foodList'].append(replacement_food)
                        updated_slot['itemsCount'] = 1

                updated_day_data.append(updated_slot)

            updated_meal_plan[day_key] = updated_day_data

        print(f"🔄 Removed {removed_items_count} food items from meal plan")
        return updated_meal_plan

    except Exception as e:
        print(f"ERROR: Simple food removal failed: {e}")
        print(f"ERROR: Simple removal traceback: {traceback.format_exc()}")
        # Return original plan if removal fails
        return meal_plan


def calculate_meal_calories_distribution(target_calories):
   """Calculate calorie distribution across meal slots"""
   distributions = {
       "1": 0.05,  # Pre workout - 5%
       "2": 0.08,  # Post workout - 8%
       "3": 0.02,  # Early morning Detox - 2%
       "4": 0.05,  # Pre-Breakfast Starter - 5%
       "5": 0.25,  # Breakfast - 25%
       "6": 0.10,  # Mid-Morning Snack - 10%
       "7": 0.25,  # Lunch - 25%
       "8": 0.08,  # Evening Snack - 8%
       "9": 0.20,  # Dinner - 20%
       "10": 0.02  # Bed time - 2%
   }

   # Allow ±10% flexibility per slot
   slot_calories = {}
   for slot_id, percentage in distributions.items():
       base_calories = target_calories * percentage
       slot_calories[slot_id] = {
           "target": round(base_calories),
           "min": round(base_calories * 0.9),
           "max": round(base_calories * 1.1)
       }

   return slot_calories


def validate_and_adjust_calories(meal_data, target_calories, max_attempts=3):
   """Validate total calories and adjust if needed"""
  
   def calculate_total_calories(meal_data):
       total = 0
       for meal_slot in meal_data.get("meals", []):
           for food in meal_slot.get("foods", []):
               total += food.get("calories", 0) or 0
       return total
  
   def scale_meal_calories(meal_data, scale_factor):
       """Scale all calories in the meal plan by a factor"""
       for meal_slot in meal_data.get("meals", []):
           for food in meal_slot.get("foods", []):
               food["calories"] = round(food["calories"] * scale_factor)
               food["protein"] = round(food["protein"] * scale_factor, 1)
               food["carbs"] = round(food["carbs"] * scale_factor, 1)
               food["fat"] = round(food["fat"] * scale_factor, 1)
       return meal_data
  
   current_calories = calculate_total_calories(meal_data)
   tolerance = target_calories * 0.13 # Allow 15% tolerance

   if abs(current_calories - target_calories) <= tolerance:
       return meal_data

   # Calculate adjustment factor
   if current_calories > 0:
       scale_factor = target_calories / current_calories
       return scale_meal_calories(meal_data, scale_factor)
  
   return meal_data


# OLD AI GENERATION FUNCTIONS REMOVED - NOW USING DB-BASED GENERATION ONLY
# See indian_fast_meal_generator.py for current meal generation logic


# REMOVED: get_curry_suggestions() - was only used in old AI prompts


def get_slot_name(slot_id):
   """Get readable name for meal slot"""
   slot_names = {
       "1": "Pre workout",
       "2": "Post workout",
       "3": "Early morning Detox",
       "4": "Pre-Breakfast / Pre-Meal Starter",
       "5": "Breakfast",
       "6": "Mid-Morning snack",
       "7": "Lunch",
       "8": "Evening snack",
       "9": "Dinner",
       "10": "Bed time"
   }
   return slot_names.get(slot_id, f"Slot {slot_id}")


def convert_ai_meal_to_template(meal_data):
   """Convert AI generated meal data to template format"""
   template = get_meal_template()
  
   for meal_slot in meal_data.get("meals", []):
       slot_id = meal_slot.get("slot_id")
       foods = meal_slot.get("foods", [])
      
       if slot_id and foods:
           slot_index = int(slot_id) - 1
           if 0 <= slot_index < len(template):
               food_list = []
               for food in foods:
                   # Get quantity - either from AI response or use standard quantity
                   quantity = food.get("quantity", get_standard_quantity_for_food(food.get("name", "")))
                  
                   # FIX: Include fiber and sugar from the food data
                   food_item = create_food_item(
                       name=food.get("name", "Unknown Food"),
                       calories=food.get("calories", 0),
                       protein=food.get("protein", 0),
                       carbs=food.get("carbs", 0),
                       fat=food.get("fat", 0),
                       quantity=quantity,
                       fiber=food.get("fiber", 0),  
                       sugar=food.get("sugar", 0)  
                   )
                   food_list.append(food_item)
              
               template[slot_index]["foodList"] = food_list
               template[slot_index]["itemsCount"] = len(food_list)
  
   return template


def extract_main_meals(meal_data):
   """Extract main meal names for tracking variety"""
   main_meals = {
       'breakfasts': [],
       'lunches': [],
       'dinners': []
   }

   for meal_slot in meal_data.get("meals", []):
       slot_id = meal_slot.get("slot_id")
       foods = meal_slot.get("foods", [])

       if slot_id == "5":  # Breakfast
           main_meals['breakfasts'].extend([food.get("name", "") for food in foods])
       elif slot_id == "7":  # Lunch
           main_meals['lunches'].extend([food.get("name", "") for food in foods])
       elif slot_id == "9":  # Dinner
           main_meals['dinners'].extend([food.get("name", "") for food in foods])

   return main_meals


# REMOVED: generate_7_day_meal_plan() - Using DB generation only (generate_7_days_parallel_fast)


def detect_diet_preference(text):
    """Detect diet preference from user input with comprehensive diet options"""
    text = text.lower().strip()

    # Ketogenic patterns - check first (most specific)
    keto_patterns = [
        r'\bketo\b',
        r'\bketogenic\b',
        r'\bketo diet\b',
        r'\bketogenic diet\b',
        r'\blow carb\b',
        r'\bno carb\b',
        r'\bhigh fat low carb\b',
        r'\bhflc\b',
        r'\blchf\b'  # low carb high fat
    ]
    
    # Paleo patterns
    paleo_patterns = [
        r'\bpaleo\b',
        r'\bpalaeolithic\b',
        r'\bpaleo diet\b',
        r'\bpaleolithic diet\b',
        r'\bcaveman diet\b',
        r'\bstone age diet\b',
        r'\bprimal\b',
        r'\bprimal diet\b'
    ]
    
    # Vegan patterns (more restrictive than vegetarian)
    vegan_patterns = [
        r'\bvegan\b',
        r'\bplant based\b',
        r'\bplant-based\b',
        r'\bno dairy\b.*\bno eggs\b',
        r'\bno animal products\b',
        r'\bstrictly plant\b',
        r'\bonly plants\b',
        r'\bvegan diet\b'
    ]
    
    # Jain diet patterns
    jain_patterns = [
        r'\bjain\b',
        r'\bjain diet\b',
        r'\bjainism\b',
        r'\bno root vegetables\b',
        r'\bno onion garlic\b',
        r'\bno underground\b',
        r'\bjain food\b',
        r'\bjain meal\b'
    ]
    
    # Eggetarian patterns (vegetarian + eggs)
    eggetarian_patterns = [
        r'\beggetarian\b',
        r'\begg vegetarian\b',
        r'\bvegetarian with eggs\b',
        r'\bveg with egg\b',
        r'\bveg plus egg\b',
        r'\beggs allowed\b.*\bveg\b',
        r'\bveg.*\beggs ok\b',
        r'\bovo vegetarian\b'
    ]
    
    # Non-vegetarian patterns
    non_veg_patterns = [
        r'\bnon\s*-?\s*veg\b',
        r'\bnon\s*-?\s*vegetarian\b',
        r'\bmeat\b',
        r'\bchicken\b',
        r'\bfish\b',
        r'\bbeef\b',
        r'\bmutton\b',
        r'\bpork\b',
        r'\bseafood\b',
        r'\bomni\b',
        r'\bomnivore\b',
        r'\bi eat meat\b',
        r'\bi eat chicken\b',
        r'\bi eat fish\b',
        r'\bnon vegetarian\b',
        r'\bnon veg\b',
        r'\bnon-veg\b',
        r'\bnon-vegetarian\b'
    ]
    
    # Pure vegetarian patterns (most restrictive vegetarian)
    veg_patterns = [
        r'(?<!non\s)(?<!non\s-)(?<!non-)(?<!non)\bveg\b',
        r'(?<!non\s)(?<!non\s-)(?<!non-)(?<!non)\bvegetarian\b',
        r'\bpure veg\b',
        r'\bonly veg\b',
        r'\bno meat\b',
        r'\bno chicken\b',
        r'\bno fish\b',
        r'\bplant vegetarian\b',
        r'\bi am veg\b',
        r'\bi am vegetarian\b'
    ]
    
    # Check in order of specificity (most specific first)
    
    # 1. Ketogenic (most specific dietary restriction)
    for pattern in keto_patterns:
        if re.search(pattern, text):
            return "ketogenic"

    # 2. Paleo
    for pattern in paleo_patterns:
        if re.search(pattern, text):
            return "paleo"

    # 3. Jain diet (very specific restrictions)
    for pattern in jain_patterns:
        if re.search(pattern, text):
            return "jain"

    # 4. Vegan (more restrictive than vegetarian)
    for pattern in vegan_patterns:
        if re.search(pattern, text):
            return "vegan"

    # 5. Eggetarian (vegetarian + eggs)
    for pattern in eggetarian_patterns:
        if re.search(pattern, text):
            return "eggetarian"

    # 6. Non-vegetarian
    for pattern in non_veg_patterns:
        if re.search(pattern, text):
            return "non-vegetarian"

    # 7. Vegetarian (least specific)
    for pattern in veg_patterns:
        if re.search(pattern, text):
            return "vegetarian"

    return None
  


def detect_cuisine_preference(text):
   """Detect cuisine preference from user input"""
   text = text.lower().strip()
  
   # North Indian patterns
   north_indian_patterns = [
       r'\bnorth\s*indian\b',
       r'\bnorthern\s*indian\b',
       r'\bnorth\b',
       r'\bpunjabi\b',
       r'\bdelhi\b',
       r'\bmughlai\b',
       r'\broti\b',
       r'\bchapati\b',
       r'\bparatha\b',
       r'\bnaan\b',
       r'\brajasthani\b',
       r'\bharyanvi\b',
       r'\bnorthside\b',
       r'\bnorth\s*side\b'
   ]
  
   # South Indian patterns
   south_indian_patterns = [
       r'\bsouth\s*indian\b',
       r'\bsouthern\s*indian\b',
       r'\bsouth\b',
       r'\btamil\b',
       r'\bkerala\b',
       r'\bkarnataka\b',
       r'\bandhra\b',
       r'\btelugu\b',
       r'\bidli\b',
       r'\bdosa\b',
       r'\bsambar\b',
       r'\bcoconut\b',
       r'\brice\b.*\bfocus\b',
       r'\bfilter\s*coffee\b'
   ]
  
   # Commonly available patterns
   common_patterns = [
       r'\bcommon\b',
       r'\bcommonly\b',
       r'\bavailable\b',
       r'\bsimple\b',
       r'\bbasic\b',
       r'\beveryday\b',
       r'\bregular\b',
       r'\bnormal\b',
       r'\bany\b',
       r'\banything\b',
       r'\bmixed\b',
       r'\bgeneral\b'
       r'\bcommonlyavailable\b',
       r'\bcommonavailable\b',
       r'\bcommomly\s*available\b',
       r'\bcommon\s*available\b',
       r'\ball\b',
   ]
  
   # Check for North Indian
   for pattern in north_indian_patterns:
       if re.search(pattern, text):
           return "north_indian"

   # Check for South Indian
   for pattern in south_indian_patterns:
       if re.search(pattern, text):
           return "south_indian"

   # Check for commonly available
   for pattern in common_patterns:
       if re.search(pattern, text):
           return "commonly_available"

   return None

def get_diet_specific_restrictions(diet_type):
    """Get specific food restrictions and guidelines for each diet type"""
    
    restrictions = {
        "vegetarian": {
            "avoid": ["meat", "chicken", "fish", "beef", "mutton", "pork", "seafood", "eggs"],
            "allow": ["dairy", "milk", "paneer", "ghee", "vegetables", "grains", "legumes", "fruits"],
            "special_notes": "No meat, fish, or eggs. Dairy products allowed."
        },
        
        "non-vegetarian": {
            "avoid": [],
            "allow": ["meat", "chicken", "fish", "eggs", "dairy", "vegetables", "grains", "legumes"],
            "special_notes": "All foods allowed including meat, fish, eggs, and dairy."
        },
        
        "eggetarian": {
            "avoid": ["meat", "chicken", "fish", "beef", "mutton", "pork", "seafood"],
            "allow": ["eggs", "dairy", "milk", "paneer", "ghee", "vegetables", "grains", "legumes"],
            "special_notes": "Vegetarian diet that includes eggs and dairy products."
        },
        
        "vegan": {
            "avoid": ["meat", "chicken", "fish", "eggs", "dairy", "milk", "paneer", "ghee", "butter", "curd", "cheese", "honey"],
            "allow": ["vegetables", "fruits", "grains", "legumes", "nuts", "seeds", "plant-based milk"],
            "special_notes": "Strictly plant-based. No animal products including dairy, eggs, or honey."
        },
        
        "jain": {
            "avoid": ["meat", "fish", "eggs", "onion", "garlic", "potato", "carrot", "radish", "beetroot", 
                     "ginger", "turmeric", "mushrooms", "yeast", "alcohol", "root vegetables", "underground vegetables"],
            "allow": ["dairy", "milk", "paneer", "above-ground vegetables", "fruits", "grains", "legumes"],
            "special_notes": "Vegetarian diet avoiding all root/underground vegetables, onion, garlic. No violence to plants with multiple lives."
        },
        
        "ketogenic": {
            "avoid": ["rice", "wheat", "bread", "roti", "chapati", "potato", "banana", "mango", "grapes", 
                     "sugar", "honey", "jaggery", "high-carb fruits", "legumes", "beans", "lentils"],
            "allow": ["meat", "fish", "eggs", "cheese", "butter", "ghee", "coconut oil", "avocado", 
                     "leafy greens", "cauliflower", "broccoli", "nuts", "seeds"],
            "special_notes": "Very low carb (under 20g/day), high fat, moderate protein. Focus on ketosis."
        },
        
        "paleo": {
            "avoid": ["grains", "rice", "wheat", "legumes", "dairy", "processed foods", "sugar", 
                     "artificial sweeteners", "vegetable oils", "beans", "lentils", "peanuts"],
            "allow": ["meat", "fish", "eggs", "vegetables", "fruits", "nuts", "seeds", "coconut", "olive oil"],
            "special_notes": "Foods available to paleolithic humans. No grains, legumes, or dairy."
        }
    }

    return restrictions.get(diet_type, restrictions["vegetarian"])


# REMOVED: Multiple unused suggestion functions that were only used in old AI prompts:
# - get_protein_suggestions()
# - get_vegetable_suggestions()
# - get_fat_suggestions()
# - get_early_morning_suggestions()
# - get_pre_breakfast_suggestions()
# - get_snack_suggestions()
# - get_staple_suggestions()
# These are no longer needed as we use DB-based generation

# REMOVED: get_diet_specific_meal_guidelines() - was only used in old AI prompts
# REMOVED: validate_diet_compliance() - validation now done in indian_fast_meal_generator.py


# REMOVED: generate_single_day_with_restrictions() - Using DB generation only (generate_7_days_parallel_fast with avoid_foods)


async def regenerate_with_health_conditions_helper(
    pending_state, health_conditions, db, oai, mem, user_id, next_state
):
    """
    Reusable helper to regenerate meal plan with health conditions
    Eliminates code duplication between different chat states
    """
    try:
        meal_plan = pending_state.get("meal_plan")
        diet_type = pending_state.get("diet_type")
        cuisine_type = pending_state.get("cuisine_type")
        profile = pending_state.get("profile")

        conditions_text = ', '.join(health_conditions)
        cuisine_display = cuisine_type.replace('_', ' ').title()

        header = f"🏥 I understand you have: **{conditions_text}**.\n\n"
        header += f"Regenerating your meal plan with foods suitable for {conditions_text}...\n\n"
        header += f"🍽️ YOUR UPDATED MEAL PLAN\n"
        header += f"Diet: {diet_type.title()}\n"
        header += f"Style: {cuisine_display}\n"
        header += f"Daily Goal: {profile['target_calories']} cal\n"
        header += f"Health Focus: {conditions_text.title()}\n\n"

        yield sse_data(header)
        await asyncio.sleep(0)

        days = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]

        # Use fast generation with health conditions
        if USE_FAST_GENERATION:
            print(f"⚡ Regenerating with health conditions: {health_conditions}")

            fast_results = await generate_7_days_parallel_fast(
                profile=profile,
                diet_type=diet_type,
                cuisine_type=cuisine_type,
                avoid_foods=[],
                prefer_foods=[],
                health_conditions=health_conditions,
                db=db,
                oai=oai
            )

            # Stream each day
            new_meal_plan = {}
            for day in days:
                day_key = day.lower()
                slot_results = fast_results.get(day_key, [])

                if slot_results:
                    meal_data = convert_fast_results_to_meal_data(slot_results)
                    template = convert_ai_meal_to_template(meal_data)
                    new_meal_plan[day_key] = template

                    day_content = format_single_day_for_streaming(day, template)
                    yield sse_data(day_content)
                    await asyncio.sleep(0)

            if new_meal_plan and len(new_meal_plan) == 7:
                # Send meal plan data with is_save flag for frontend (buttons will appear)
                yield sse_json_bytes({
                    "type": "meal_plan_complete",
                    "client_id": pending_state.get("client_id"),
                    "profile": profile,
                    "diet_type": diet_type,
                    "cuisine_type": cuisine_type,
                    "meal_plan": new_meal_plan,
                    "health_conditions": health_conditions,
                    "is_save": True
                })

                # Update pending state with the specified next state
                await mem.set_pending(user_id, {
                    "state": next_state,
                    "client_id": pending_state.get("client_id"),
                    "profile": profile,
                    "diet_type": diet_type,
                    "cuisine_type": cuisine_type,
                    "meal_plan": new_meal_plan,
                    "health_conditions": health_conditions
                })

                yield sse_done()
            else:
                yield sse_data("Sorry, I had trouble regenerating. Please try again.\n")
                yield sse_done()

    except Exception as e:
        print(f"Error regenerating with health conditions: {e}")
        yield sse_data(f"❌ Error: {str(e)}\n")
        yield sse_done()


async def ai_intent_classifier_sync(user_input: str, current_state: str, oai) -> dict:
    """
    AI-driven intent classifier that understands user intent flexibly.
    Handles typos, natural language, and context-aware interpretation.
    Uses pure async architecture with AsyncOpenAI for non-blocking operations.

    Args:
        user_input: The user's message (can contain typos, be informal)
        current_state: Current conversation state
        oai: AsyncOpenAI client

    Returns:
        dict with 'intent' and extracted parameters
    """

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

    try:
        response = await oai.chat.completions.create(
            model=OPENAI_MODEL,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_input}
            ],
            response_format={"type": "json_object"},
            temperature=0.3,
            max_tokens=300
        )

        result = orjson.loads(response.choices[0].message.content)
        return result

    except Exception as e:
        print(f"ERROR: AI intent classification failed: {e}")
        return {
            "intent": "unclear",
            "confidence": 0.0,
            "extracted_data": {},
            "normalized_input": user_input
        }


async def ai_intent_classifier(user_input: str, current_state: str, oai) -> dict:
    """
    Async wrapper: AI-driven intent classifier using pure async architecture.
    Uses AsyncOpenAI for non-blocking operations.

    DEPRECATED: Use ai_intent_classifier_celery() for rate-limited Celery version
    """
    # Pure async - no executor needed
    return await ai_intent_classifier_sync(user_input, current_state, oai)


# ===== Celery-backed async wrappers (rate-limited) =====

async def translate_text_celery(user_id: int, text: str) -> dict:
    """
    Translate text to English using Celery+Redis for rate limiting.

    Args:
        user_id: Client ID
        text: Text to translate

    Returns:
        dict: {"lang": "detected_language", "english": "translated_text"}
    """
    import asyncio
    from app.tasks.meal_tasks import translate_text
    from celery.result import AsyncResult

    try:
        task = translate_text.delay(user_id=user_id, text=text)
        print(f"🌐 Queued translation task {task.id} for user {user_id}")

        max_wait = 30
        poll_interval = 0.3
        elapsed = 0

        while elapsed < max_wait:
            celery_task = AsyncResult(task.id)
            if celery_task.ready():
                if celery_task.successful():
                    return celery_task.result
                break
            await asyncio.sleep(poll_interval)
            elapsed += poll_interval

        return {"lang": "unknown", "english": text}

    except Exception as e:
        print(f"Error in translate_text_celery: {e}")
        return {"lang": "unknown", "english": text}


async def ai_intent_classifier_celery(user_id: int, user_input: str, current_state: str) -> dict:
    """
    AI-driven intent classifier using Celery+Redis for rate limiting.

    Args:
        user_id: Client ID
        user_input: User's message
        current_state: Current conversation state

    Returns:
        dict: Intent classification result
    """
    import asyncio
    from app.tasks.meal_tasks import classify_meal_intent
    from celery.result import AsyncResult

    try:
        task = classify_meal_intent.delay(
            user_id=user_id,
            user_input=user_input,
            current_state=current_state
        )
        print(f"🎯 Queued intent classification task {task.id} for user {user_id}")

        max_wait = 30
        poll_interval = 0.3
        elapsed = 0

        while elapsed < max_wait:
            celery_task = AsyncResult(task.id)
            if celery_task.ready():
                if celery_task.successful():
                    return celery_task.result
                break
            await asyncio.sleep(poll_interval)
            elapsed += poll_interval

        return {
            "intent": "unclear",
            "confidence": 0.0,
            "extracted_data": {},
            "normalized_input": user_input
        }

    except Exception as e:
        print(f"Error in ai_intent_classifier_celery: {e}")
        return {
            "intent": "unclear",
            "confidence": 0.0,
            "extracted_data": {},
            "normalized_input": user_input
        }


def extract_diet_from_ai_intent(intent_result: dict) -> str:
    """Extract and normalize diet preference from AI intent result"""
    if intent_result.get("intent") != "diet_preference":
        return None

    extracted = intent_result.get("extracted_data", {})
    diet = extracted.get("diet_type", "").lower()

    # Normalize variations
    diet_map = {
        "veg": "vegetarian",
        "vegetarian": "vegetarian",
        "non-veg": "non-vegetarian",
        "nonveg": "non-vegetarian",
        "non-vegetarian": "non-vegetarian",
        "non vegetarian": "non-vegetarian",
        "eggetarian": "eggetarian",
        "egg": "eggetarian",
        "vegan": "vegan",
        "jain": "jain",
        "keto": "ketogenic",
        "ketogenic": "ketogenic",
        "paleo": "paleo"
    }

    return diet_map.get(diet)


def extract_cuisine_from_ai_intent(intent_result: dict) -> str:
    """Extract and normalize cuisine preference from AI intent result"""
    if intent_result.get("intent") != "cuisine_preference":
        return None

    extracted = intent_result.get("extracted_data", {})
    cuisine = extracted.get("cuisine_type", "").lower().replace(" ", "_")

    # Normalize variations
    cuisine_map = {
        "north_indian": "north_indian",
        "northindian": "north_indian",
        "north": "north_indian",
        "south_indian": "south_indian",
        "southindian": "south_indian",
        "south": "south_indian",
        "common": "commonly_available",
        "commonly_available": "commonly_available",
        "simple": "commonly_available",
        "basic": "commonly_available"
    }

    return cuisine_map.get(cuisine)


def extract_food_restrictions_from_ai(intent_result: dict) -> dict:
    """Extract food restrictions from AI intent result"""
    if intent_result.get("intent") not in ["food_allergy", "food_removal"]:
        return None

    extracted = intent_result.get("extracted_data", {})

    if intent_result.get("intent") == "food_allergy":
        allergens = extracted.get("allergens", [])
        return {
            'found_allergens': [],
            'other_foods': allergens if isinstance(allergens, list) else [allergens],
            'raw_text': intent_result.get("normalized_input", ""),
            'ai_detected': True
        }
    else:  # food_removal
        foods = extracted.get("foods_to_remove", [])
        return {
            'found_allergens': [],
            'other_foods': foods if isinstance(foods, list) else [foods],
            'raw_text': intent_result.get("normalized_input", ""),
            'simple_removal': True,
            'ai_detected': True
        }


@router.get("/chat/stream")
async def chat_stream(
   user_id: int,
   client_id: int = Query(..., description="Client ID for whom to create meal plan"),
   text: str = Query(None),
   audio_transcript: str = Query(None, description="Transcribed audio text"),
   mem = Depends(get_mem),
   oai = Depends(get_oai),
   db: Session = Depends(get_db),
):
   try:
       if not text:
           # Fetch client profile and start conversation
           try:
               profile = _fetch_profile(db, client_id)

               async def _welcome():
                   print("🎬 Sending welcome message...")
                   welcome_msg = f"""Hello! I'm your meal template assistant.


I can see your profile:
• Current Weight: {profile['current_weight']} kg
• Target Weight: {profile['target_weight']} kg
• Goal: {profile['weight_delta_text']}
• Daily Calorie Target: {profile['target_calories']} calories


I'll create a personalized 7-day meal template for you. First, are you vegetarian or non-vegetarian or eggetarian or vegan or ketogic or paleo or jain?"""

                   await mem.set_pending(user_id, {
                       "state": "awaiting_diet_preference",
                       "client_id": client_id,
                       "profile": profile
                   })

                   yield sse_json_bytes({'message': welcome_msg, 'type': 'welcome'})
                   await asyncio.sleep(0)
                   yield sse_done()
                   print("✅ Welcome message sent")
              
               return StreamingResponse(_welcome(), media_type="text/event-stream",
                                       headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})
                                      
           except Exception as e:
               print(f"Error fetching profile: {e}")
               print(f"Profile fetch full traceback: {traceback.format_exc()}")
               async def _profile_error():
                   yield sse_json_bytes({'message': 'Error fetching client profile. Please check the client ID and try again.', 'type': 'error'})
                   yield sse_done()
              
               return StreamingResponse(_profile_error(), media_type="text/event-stream",
                                       headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})
      
       text = text.strip() if text else ""

       # Handle voice input
       if audio_transcript and not text:
            text = audio_transcript.strip()
      
       # Get pending state from memory
       try:
           pending_state = await mem.get_pending(user_id)
       except Exception as e:
           print(f"Error getting pending state: {e}")
           pending_state = None
      
       # Handle diet preference selection
       if pending_state and pending_state.get("state") == "awaiting_diet_preference":
           # Use AI intent classification via Celery (rate-limited)
           ai_intent = await ai_intent_classifier_celery(user_id, text, "awaiting_diet_preference")
           diet_type = extract_diet_from_ai_intent(ai_intent)

           # Fallback to regex patterns if AI fails
           if not diet_type:
               diet_type = detect_diet_preference(text)

           if diet_type:
               # Move to cuisine preference selection
               await mem.set_pending(user_id, {
                   "state": "awaiting_cuisine_preference",
                   "client_id": pending_state.get("client_id"),
                   "profile": pending_state.get("profile"),
                   "diet_type": diet_type
               })

               async def _ask_cuisine():
                   cuisine_msg = f"""Great! You've selected {diet_type} 🎉

Which cuisine do you prefer?

🍛 North Indian
🥥 South Indian
🍽️ Commonly Available

Please choose: North Indian, South Indian, or Commonly Available"""

                   print("📤 Sending cuisine selection prompt...")
                   yield sse_json_bytes({'message': cuisine_msg, 'type': 'cuisine_selection'})
                   await asyncio.sleep(0)
                   yield sse_done()
                   print("✅ Cuisine prompt sent")

               return StreamingResponse(_ask_cuisine(), media_type="text/event-stream",
                                       headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})
           else:
               async def _ask_diet_again():
                   msg = """I didn't understand your preference. Please be more specific:


• Type "vegetarian" or "veg" if you don't eat meat, fish, chicken, eggs
• Type "non-vegetarian" or "non-veg" if you eat meat, chicken, fish, eggs


Examples:
- "I am vegetarian"
- "non-veg"
- "I eat chicken and fish"
- "veg only" """
                   yield sse_json_bytes({'message': msg, 'type': 'clarification'})
                   yield sse_done()

               return StreamingResponse(_ask_diet_again(), media_type="text/event-stream",
                                       headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})
      
       # Handle cuisine preference selection
       elif pending_state and pending_state.get("state") == "awaiting_cuisine_preference":
            # Use AI intent classification via Celery (rate-limited)
            ai_intent = await ai_intent_classifier_celery(user_id, text, "awaiting_cuisine_preference")
            cuisine_type = extract_cuisine_from_ai_intent(ai_intent)

            # Fallback to regex patterns if AI fails
            if not cuisine_type:
                cuisine_type = detect_cuisine_preference(text)

            if cuisine_type:
                # Generate meal plan with both diet and cuisine preferences
                profile = pending_state.get("profile")
                diet_type = pending_state.get("diet_type")

                async def _generate_plan():
                    try:
                        print(f"🚀 Starting meal plan generation for user {user_id}")
                        print(f"⚡ Fast generation mode: {USE_FAST_GENERATION}")
                        cuisine_display = cuisine_type.replace('_', ' ').title()

                        # Start with header - stream as plain text
                        header = f"""🍽️ YOUR MEAL PLAN
Diet: {diet_type.title()}
Style: {cuisine_display}
Daily Goal: {profile['target_calories']} cal

"""
                        print(f"📤 Sending header...")
                        yield sse_data(header)
                        await asyncio.sleep(0)
                        print(f"✅ Header sent")

                        days = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
                        meal_plan = {}

                        # ===== FAST GENERATION PATH (NEW - 10-20 seconds) =====
                        if USE_FAST_GENERATION:
                            print("⚡ Using fast database-powered generation...")

                            # Generate ALL 7 days in parallel using database (NO LOADERS - it's fast!)
                            fast_results = await generate_7_days_parallel_fast(
                                profile=profile,
                                diet_type=diet_type,
                                cuisine_type=cuisine_type,
                                avoid_foods=[],  # Can add user restrictions here
                                prefer_foods=[],  # Can add user preferences here
                                health_conditions=[],  # Will be extracted from user messages
                                db=db,
                                oai=oai
                            )

                            # Convert and stream each day (instant - no loaders needed)
                            for day in days:
                                day_key = day.lower()
                                slot_results = fast_results.get(day_key, [])

                                if slot_results:
                                    # Convert to template format
                                    meal_data = convert_fast_results_to_meal_data(slot_results)
                                    template = convert_ai_meal_to_template(meal_data)
                                    meal_plan[day_key] = template

                                    # Stream day content (instant - no loader removal needed)
                                    day_content = format_single_day_for_streaming(day, template)
                                    yield sse_data(day_content)
                                    await asyncio.sleep(0)
                                    print(f"✅ [{day}] Streamed (fast mode)")

                        # Validate meal plan has content
                        if not meal_plan or len(meal_plan) == 0:
                            print("ERROR: Empty meal plan generated")
                            yield sse_data('Sorry, I encountered an issue generating your meal plan. Please try again.')
                            yield sse_done()
                            return

                        # Trigger voice notification for template creation
                        try:
                            client_id = pending_state.get("client_id")
                            if client_id:
                                await trigger_food_template_voice(client_id, "template_creation", db)
                        except Exception as e:
                            print(f"Error triggering template creation voice: {e}")

                        # Send meal plan data with is_save flag for frontend (buttons will appear)
                        print("💾 Sending meal plan complete event...")
                        yield sse_json_bytes({
                            "type": "meal_plan_complete",
                            "client_id": pending_state.get("client_id"),
                            "profile": profile,
                            "diet_type": diet_type,
                            "cuisine_type": cuisine_type,
                            "meal_plan": meal_plan,
                            "is_save": True
                        })
                        await asyncio.sleep(0)
                        print("✅ Meal plan complete event sent")

                        # Set pending state
                        await mem.set_pending(user_id, {
                            "state": "awaiting_name_change_or_edit",
                            "client_id": pending_state.get("client_id"),
                            "profile": profile,
                            "diet_type": diet_type,
                            "cuisine_type": cuisine_type,
                            "meal_plan": meal_plan
                        })

                        print("🎉 Meal plan generation complete!")
                        yield sse_done()

                    except Exception as e:
                        print(f"Error generating meal plan: {e}")
                        print(f"Meal generation full traceback: {traceback.format_exc()}")
                        yield sse_data('Sorry, there was an error creating your meal plan. Please try again or contact support.')
                        yield sse_done()
                
                return StreamingResponse(_generate_plan(), media_type="text/event-stream",
                                        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})
            else:
                # Handle unrecognized cuisine input - ask for clarification
                async def _ask_cuisine_again():
                    cuisine_msg = """I didn't recognize that cuisine preference. Please choose from:

• North Indian - Roti, dal, curry-based dishes
• South Indian - Rice, sambar, coconut-based dishes
• Commonly Available - Simple, everyday foods

Please type one of: North Indian, South Indian, or Commonly Available"""

                    yield sse_json_bytes({'message': cuisine_msg, 'type': 'cuisine_clarification'})
                    yield sse_done()

                return StreamingResponse(_ask_cuisine_again(), media_type="text/event-stream",
                                        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})

       # Handle name change and allergy check
       elif pending_state and pending_state.get("state") == "awaiting_name_change_or_edit":
            # Use AI intent classification via Celery (rate-limited)
            ai_intent = await ai_intent_classifier_celery(user_id, text, "awaiting_name_change_or_edit")
            intent_type = ai_intent.get("intent")

            # Handle food alternate requests
            if intent_type == "food_alternate":
                # Extract foods to find alternatives for
                extracted_data = ai_intent.get("extracted_data", {})
                foods_to_alternate = extracted_data.get("foods_to_alternate", [])

                if foods_to_alternate:
                    # Generate alternatives
                    async def _show_alternatives():
                        try:
                            meal_plan = pending_state.get("meal_plan")
                            diet_type = pending_state.get("diet_type")
                            cuisine_type = pending_state.get("cuisine_type")
                            profile = pending_state.get("profile")

                            food_to_replace = foods_to_alternate[0]  # Handle first food

                            header = f"🔄 Finding alternatives for **{food_to_replace}**...\n\n"
                            yield sse_data(header)
                            await asyncio.sleep(0)

                            # Generate alternatives using fast generator
                            alternatives = await generate_food_alternatives(
                                food_to_replace=food_to_replace,
                                meal_plan=meal_plan,
                                diet_type=diet_type,
                                cuisine_type=cuisine_type,
                                profile=profile,
                                db=db,
                                oai=oai
                            )

                            if alternatives:
                                alt_text = f"✅ Here are some great alternatives for **{food_to_replace}**:\n\n"
                                for i, alt in enumerate(alternatives, 1):
                                    alt_text += f"{i}. **{alt['name']}** - {alt['calories']} cal, {alt['protein']}g protein\n"

                                alt_text += f"\n💡 To replace {food_to_replace}, just say:\n"
                                alt_text += f"- 'remove {food_to_replace}' (I'll regenerate without it)\n"
                                alt_text += f"- Or type 'save' to keep your current plan\n"

                                yield sse_data(alt_text)
                            else:
                                no_alt = f"😔 Sorry, I couldn't find good alternatives for {food_to_replace} in the database.\n\n"
                                no_alt += f"You can still say 'remove {food_to_replace}' and I'll regenerate without it!\n"
                                yield sse_data(no_alt)

                            yield sse_done()

                        except Exception as e:
                            print(f"Error generating alternatives: {e}")
                            error_msg = f"❌ Sorry, I had trouble finding alternatives. You can say 'remove {food_to_replace}' to regenerate without it.\n"
                            yield sse_data(error_msg)
                            yield sse_done()

                    return StreamingResponse(_show_alternatives(), media_type="text/event-stream",
                                            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})
                else:
                    # No food extracted - ask for clarification
                    async def _ask_which_food():
                        msg = "🤔 Which food would you like alternatives for? Please specify the food name.\n"
                        yield sse_data(msg)
                        yield sse_done()

                    return StreamingResponse(_ask_which_food(), media_type="text/event-stream",
                                            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})

            # Handle health condition changes (e.g., "I have diabetes")
            elif intent_type == "health_condition_change":
                extracted_data = ai_intent.get("extracted_data", {})
                health_conditions = extracted_data.get("health_conditions", [])

                if health_conditions:
                    # Use the reusable helper function
                    return StreamingResponse(
                        regenerate_with_health_conditions_helper(
                            pending_state, health_conditions, db, oai, mem, user_id,
                            next_state="awaiting_name_change_or_edit"
                        ),
                        media_type="text/event-stream",
                        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"}
                    )
                else:
                    # No health conditions extracted
                    async def _ask_clarification():
                        msg = "🤔 I want to help! Can you please specify your health condition? (e.g., diabetes, pregnancy, PCOS, hypertension)\n"
                        yield sse_data(msg)
                        yield sse_done()

                    return StreamingResponse(_ask_clarification(), media_type="text/event-stream",
                                            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})

            # Handle food allergies/removal
            elif intent_type in ["food_allergy", "food_removal"]:
                restrictions = extract_food_restrictions_from_ai(ai_intent)

                # Fallback to regex if AI didn't extract anything useful
                if not restrictions or not restrictions.get('other_foods'):
                    restrictions = detect_food_restrictions(text)

            elif intent_type in ["save_template"] or text.lower() in ['finalize', 'finish', 'done', 'save']:
                # Finalize with default names
                meal_plan = pending_state.get("meal_plan")
                client_id = pending_state.get("client_id")
                template_name = "Meal Template (Mon-Sun)"

                # Store template in memory first
                await _store_meal_template(mem, db, client_id, meal_plan, template_name)

                # Save to database directly
                save_result = save_meal_plan_to_database(client_id, meal_plan, db, replace_all=False)

                await mem.clear_pending(user_id)

                async def _finalize_default():
                    if save_result and save_result.get('success'):
                        if save_result.get('merge_mode'):
                            message = f'✅ Your meal plan is updated! Custom day names added as new templates. Total templates: {save_result.get("saved_count", 0)}'
                        else:
                            message = '✅ Your 7-day meal plan is finalized and saved!'
                    else:
                        message = '⚠️ Your 7-day meal plan is finalized but save failed!'

                    # Trigger voice notification for meal plan saved
                    if save_result and save_result.get('success'):
                        try:
                            await trigger_food_template_voice(user_id, "meal_plan_saved", db)
                        except Exception as e:
                            print(f"Error triggering meal plan saved voice: {e}")

                    yield sse_json_bytes({
                        "type": "meal_template",
                        "status": "stored" if (save_result and save_result.get('success')) else "error",
                        "template_name": template_name,
                        "meal_plan": meal_plan,
                        "day_names": ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"],
                        "message": message,
                        "is_nav": True
                    })
                    yield sse_done()

                return StreamingResponse(_finalize_default(), media_type="text/event-stream",
                                        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})

            else:
                # Check for restrictions using fallback regex
                restrictions = detect_food_restrictions(text)

            if restrictions:
                # Regenerate meal plan without allergens
                async def _regenerate_with_restrictions():
                    try:
                        avoid_items = restrictions.get('found_allergens', []) + restrictions.get('other_foods', [])
                        avoid_text = ', '.join(avoid_items)

                        diet_type = pending_state.get("diet_type")
                        cuisine_type = pending_state.get("cuisine_type")
                        profile = pending_state.get("profile")

                        # Stream header FIRST with "understanding" message
                        cuisine_display = cuisine_type.replace('_', ' ').title()
                        header = f"""I understand you want to avoid: {avoid_text}.

Regenerating your 7-day meal plan (this will be quick!)...

🍽️ YOUR UPDATED MEAL PLAN
Diet: {diet_type.title()}
Style: {cuisine_display}
Daily Goal: {profile['target_calories']} cal
Avoided: {avoid_text}

"""
                        yield sse_data(header)
                        await asyncio.sleep(0)

                        # Prepare avoid foods list
                        allergen_foods_map = {
                            'peanuts': ['peanut', 'groundnut', 'peanut butter', 'groundnut oil'],
                            'tree_nuts': ['almond', 'walnut', 'cashew', 'pistachio', 'nuts', 'mixed nuts'],
                            'dairy': ['milk', 'cheese', 'paneer', 'butter', 'ghee', 'yogurt', 'curd', 'lassi', 'buttermilk'],
                            'gluten': ['wheat', 'bread', 'roti', 'chapati', 'naan', 'paratha', 'biscuit'],
                            'eggs': ['egg', 'eggs', 'boiled egg', 'fried egg', 'scrambled egg'],
                            'fish': ['fish', 'fish curry', 'fried fish', 'fish fry'],
                            'shellfish': ['shrimp', 'crab', 'lobster', 'prawns'],
                            'soy': ['soy', 'soya', 'tofu', 'soybean'],
                            'sesame': ['sesame', 'til', 'sesame oil'],
                            'coconut': ['coconut', 'coconut oil', 'coconut milk', 'coconut chutney'],
                            'onion_garlic': ['onion', 'garlic', 'pyaz', 'lahsun']
                        }

                        avoid_foods = []
                        for allergen in restrictions.get('found_allergens', []):
                            avoid_foods.extend(allergen_foods_map.get(allergen, []))
                        avoid_foods.extend(restrictions.get('other_foods', []))
                        avoid_foods = [food.lower() for food in avoid_foods]

                        days = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]

                        # ===== USE FAST GENERATION (same as initial generation) =====
                        if USE_FAST_GENERATION:
                            print("⚡ Using fast database-powered regeneration...")

                            # Generate ALL 7 days in parallel using database (FAST!)
                            fast_results = await generate_7_days_parallel_fast(
                                profile=profile,
                                diet_type=diet_type,
                                cuisine_type=cuisine_type,
                                avoid_foods=avoid_foods,  # Pass user restrictions
                                prefer_foods=[],
                                health_conditions=[],  # TODO: Extract from user messages
                                db=db,
                                oai=oai
                            )

                            # Convert and stream each day
                            new_meal_plan = {}
                            for day in days:
                                day_key = day.lower()
                                slot_results = fast_results.get(day_key, [])

                                if slot_results:
                                    meal_data = convert_fast_results_to_meal_data(slot_results)
                                    template = convert_ai_meal_to_template(meal_data)
                                    new_meal_plan[day_key] = template

                                    # Stream day content (no loaders needed - it's instant!)
                                    day_content = format_single_day_for_streaming(day, template)
                                    yield sse_data(day_content)
                                    await asyncio.sleep(0)

                        # Validate meal plan has 7 days
                        if new_meal_plan and len(new_meal_plan) == 7:  # Should have 7 days

                            # Send meal plan data with is_save flag for frontend (buttons will appear)
                            yield sse_json_bytes({
                                "type": "meal_plan_complete",
                                "client_id": pending_state.get("client_id"),
                                "profile": profile,
                                "diet_type": diet_type,
                                "cuisine_type": cuisine_type,
                                "meal_plan": new_meal_plan,
                                "avoided_foods": avoid_items,
                                "is_save": True
                            })

                            # Update pending state with new meal plan
                            await mem.set_pending(user_id, {
                                "state": "awaiting_name_change_after_allergy",
                                "client_id": pending_state.get("client_id"),
                                "profile": profile,
                                "diet_type": diet_type,
                                "cuisine_type": cuisine_type,
                                "meal_plan": new_meal_plan,
                                "avoided_foods": avoid_items
                            })

                            yield sse_done()
                            
                        else:
                            yield sse_json_bytes({'message': 'Sorry, I had trouble regenerating the complete meal plan. Would you like to continue with the original plan and customize day names instead?', 'type': 'regeneration_error'})
                            yield sse_done()
                            
                    except Exception as e:
                        print(f"Error in allergy regeneration: {e}")
                        print(f"Allergy regeneration traceback: {traceback.format_exc()}")
                        yield sse_json_bytes({'message': 'Error updating meal plan. Would you like to continue with day name customization?', 'type': 'error'})
                        yield sse_done()
                
                return StreamingResponse(_regenerate_with_restrictions(), media_type="text/event-stream",
                                        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})

            

            # Handle unclear intents
            if not restrictions:
                # Handle unrecognized input in name change state
                async def _clarify_options():
                    msg = """Please say "save" to save the plan, or specify foods to remove like "remove dairy"."""

                    yield sse_json_bytes({'message': msg, 'type': 'prompt'})
                    yield sse_done()

                return StreamingResponse(_clarify_options(), media_type="text/event-stream",
                                        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})

       # Handle name change after allergy update
       elif pending_state and pending_state.get("state") == "awaiting_name_change_after_allergy":
            # FIRST check if user wants to remove MORE foods or get alternatives (via Celery)
            ai_intent = await ai_intent_classifier_celery(user_id, text, "awaiting_name_change_after_allergy")
            intent_type = ai_intent.get("intent")

            # Handle food alternate requests (same as in awaiting_name_change_or_edit)
            if intent_type == "food_alternate":
                # Extract foods to find alternatives for
                extracted_data = ai_intent.get("extracted_data", {})
                foods_to_alternate = extracted_data.get("foods_to_alternate", [])

                if foods_to_alternate:
                    # Generate alternatives
                    async def _show_alternatives():
                        try:
                            meal_plan = pending_state.get("meal_plan")
                            diet_type = pending_state.get("diet_type")
                            cuisine_type = pending_state.get("cuisine_type")
                            profile = pending_state.get("profile")

                            food_to_replace = foods_to_alternate[0]  # Handle first food

                            header = f"🔄 Finding alternatives for **{food_to_replace}**...\n\n"
                            yield sse_data(header)
                            await asyncio.sleep(0)

                            # Generate alternatives using fast generator
                            alternatives = await generate_food_alternatives(
                                food_to_replace=food_to_replace,
                                meal_plan=meal_plan,
                                diet_type=diet_type,
                                cuisine_type=cuisine_type,
                                profile=profile,
                                db=db,
                                oai=oai
                            )

                            if alternatives:
                                alt_text = f"✅ Here are some great alternatives for **{food_to_replace}**:\n\n"
                                for i, alt in enumerate(alternatives, 1):
                                    alt_text += f"{i}. **{alt['name']}** - {alt['calories']} cal, {alt['protein']}g protein\n"

                                alt_text += f"\n💡 To replace {food_to_replace}, just say:\n"
                                alt_text += f"- 'remove {food_to_replace}' (I'll regenerate without it)\n"
                                alt_text += f"- Or type 'save' to keep your current plan\n"

                                yield sse_data(alt_text)
                            else:
                                no_alt = f"😔 Sorry, I couldn't find good alternatives for {food_to_replace} in the database.\n\n"
                                no_alt += f"You can still say 'remove {food_to_replace}' and I'll regenerate without it!\n"
                                yield sse_data(no_alt)

                            yield sse_done()

                        except Exception as e:
                            print(f"Error generating alternatives: {e}")
                            error_msg = f"❌ Sorry, I had trouble finding alternatives. You can say 'remove {food_to_replace}' to regenerate without it.\n"
                            yield sse_data(error_msg)
                            yield sse_done()

                    return StreamingResponse(_show_alternatives(), media_type="text/event-stream",
                                            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})
                else:
                    # No food extracted - ask for clarification
                    async def _ask_which_food():
                        msg = "🤔 Which food would you like alternatives for? Please specify the food name.\n"
                        yield sse_data(msg)
                        yield sse_done()

                    return StreamingResponse(_ask_which_food(), media_type="text/event-stream",
                                            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})

            # Handle health condition changes in after_allergy state too
            elif intent_type == "health_condition_change":
                extracted_data = ai_intent.get("extracted_data", {})
                health_conditions = extracted_data.get("health_conditions", [])

                if health_conditions:
                    # Use the reusable helper function
                    return StreamingResponse(
                        regenerate_with_health_conditions_helper(
                            pending_state, health_conditions, db, oai, mem, user_id,
                            next_state="awaiting_name_change_after_allergy"
                        ),
                        media_type="text/event-stream",
                        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"}
                    )

            elif intent_type in ["food_allergy", "food_removal"]:
                # User wants to remove MORE items - regenerate again
                restrictions = extract_food_restrictions_from_ai(ai_intent)

                # Fallback to regex if AI didn't extract anything useful
                if not restrictions or not restrictions.get('other_foods'):
                    restrictions = detect_food_restrictions(text)

                if restrictions:
                    # Regenerate meal plan without additional allergens
                    async def _regenerate_with_more_restrictions():
                        try:
                            avoid_items = restrictions.get('found_allergens', []) + restrictions.get('other_foods', [])
                            avoid_text = ', '.join(avoid_items)

                            diet_type = pending_state.get("diet_type")
                            cuisine_type = pending_state.get("cuisine_type")
                            profile = pending_state.get("profile")

                            # Stream header FIRST with "understanding" message
                            cuisine_display = cuisine_type.replace('_', ' ').title()
                            header = f"""I understand you want to avoid: {avoid_text}.

Regenerating your 7-day meal plan...

🍽️ YOUR UPDATED MEAL PLAN
Diet: {diet_type.title()}
Style: {cuisine_display}
Daily Goal: {profile['target_calories']} cal
Avoided: {avoid_text}

"""
                            yield sse_data(header)
                            await asyncio.sleep(0)  # Force immediate flush

                            # Prepare restrictions
                            allergen_foods_map = {
                                'peanuts': ['peanut', 'groundnut', 'peanut butter', 'groundnut oil'],
                                'tree_nuts': ['almond', 'walnut', 'cashew', 'pistachio', 'nuts', 'mixed nuts'],
                                'dairy': ['milk', 'cheese', 'paneer', 'butter', 'ghee', 'yogurt', 'curd', 'lassi', 'buttermilk'],
                                'gluten': ['wheat', 'bread', 'roti', 'chapati', 'naan', 'paratha', 'biscuit'],
                                'eggs': ['egg', 'eggs', 'boiled egg', 'fried egg', 'scrambled egg'],
                                'fish': ['fish', 'fish curry', 'fried fish', 'fish fry'],
                                'shellfish': ['shrimp', 'crab', 'lobster', 'prawns'],
                                'soy': ['soy', 'soya', 'tofu', 'soybean'],
                                'sesame': ['sesame', 'til', 'sesame oil'],
                                'coconut': ['coconut', 'coconut oil', 'coconut milk', 'coconut chutney'],
                                'onion_garlic': ['onion', 'garlic', 'pyaz', 'lahsun']
                            }

                            avoid_foods = []
                            for allergen in restrictions.get('found_allergens', []):
                                avoid_foods.extend(allergen_foods_map.get(allergen, []))
                            avoid_foods.extend(restrictions.get('other_foods', []))
                            avoid_foods = [food.lower() for food in avoid_foods]

                            days = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]

                            # ===== USE FAST GENERATION (same as initial generation) =====
                            if USE_FAST_GENERATION:
                                print("⚡ Using fast database-powered regeneration...")

                                # Generate ALL 7 days in parallel using database (FAST!)
                                fast_results = await generate_7_days_parallel_fast(
                                    profile=profile,
                                    diet_type=diet_type,
                                    cuisine_type=cuisine_type,
                                    avoid_foods=avoid_foods,  # Pass user restrictions
                                    prefer_foods=[],
                                    health_conditions=[],  # TODO: Extract from user messages
                                    db=db,
                                    oai=oai
                                )

                                # Convert and stream each day (no loaders - instant!)
                                new_meal_plan = {}
                                for day in days:
                                    day_key = day.lower()
                                    slot_results = fast_results.get(day_key, [])

                                    if slot_results:
                                        meal_data = convert_fast_results_to_meal_data(slot_results)
                                        template = convert_ai_meal_to_template(meal_data)
                                        new_meal_plan[day_key] = template

                                        # Stream day content (instant - no loaders)
                                        day_content = format_single_day_for_streaming(day, template)
                                        yield sse_data(day_content)
                                        await asyncio.sleep(0)

                            # Validate meal plan
                            if new_meal_plan and len(new_meal_plan) == 7:  # Should have 7 days

                                # Send meal plan data with is_save flag for frontend (buttons will appear)
                                yield sse_json_bytes({
                                    "type": "meal_plan_complete",
                                    "client_id": pending_state.get("client_id"),
                                    "profile": profile,
                                    "diet_type": diet_type,
                                    "cuisine_type": cuisine_type,
                                    "meal_plan": new_meal_plan,
                                    "avoided_foods": avoid_items,
                                    "is_save": True
                                })

                                # Update pending state with new meal plan
                                await mem.set_pending(user_id, {
                                    "state": "awaiting_name_change_after_allergy",
                                    "client_id": pending_state.get("client_id"),
                                    "profile": profile,
                                    "diet_type": diet_type,
                                    "cuisine_type": cuisine_type,
                                    "meal_plan": new_meal_plan,
                                    "avoided_foods": avoid_items
                                })

                            yield sse_done()

                        except Exception as e:
                            print(f"Error in regeneration: {e}")
                            print(f"Traceback: {traceback.format_exc()}")
                            yield sse_data(f"⚠️ Error regenerating meal plan: {str(e)}")
                            yield sse_done()

                    return StreamingResponse(_regenerate_with_more_restrictions(), media_type="text/event-stream",
                                            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})

            elif text.lower() in ['save', 'done', 'finalize', 'finish']:
                # Finalize with default names
                meal_plan = pending_state.get("meal_plan")
                client_id = pending_state.get("client_id")
                template_name = "Meal Template (Mon-Sun)"

                # Store template in memory first
                await _store_meal_template(mem, db, client_id, meal_plan, template_name)

                # Save to database directly
                save_result = save_meal_plan_to_database(client_id, meal_plan, db, replace_all=False)
                await mem.clear_pending(user_id)

                async def _finalize_default():
                    if save_result['success']:
                        message = "Your 7-day meal plan is finalized and saved!"
                    else:
                        message = "Your 7-day meal plan is finalized! (Save failed)"

                    # Trigger voice notification for meal plan saved
                    if save_result['success']:
                        try:
                            await trigger_food_template_voice(client_id, "meal_plan_saved", db)
                        except Exception as e:
                            print(f"Error triggering meal plan saved voice: {e}")

                    yield sse_json_bytes({
                        "type": "meal_plan_final",
                        "status": "completed",
                        "diet_type": pending_state.get("diet_type"),
                        "meal_plan": meal_plan,
                        "day_names": ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"],
                        "message": message,
                        "is_nav": True
                    })
                    yield sse_done()

                return StreamingResponse(_finalize_default(), media_type="text/event-stream",
                                        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})
            
            else:
                # Unrecognized input, just acknowledge
                async def _acknowledge():
                    yield sse_json_bytes({'message': 'Please say \"save\" to save the plan, or specify foods to remove like \"remove dairy\".', 'type': 'prompt'})
                    yield sse_done()

                return StreamingResponse(_acknowledge(), media_type="text/event-stream",
                                        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})

       # Handle case where no pending state exists but user is sending text (FIRST MESSAGE HANDLER)
       else:
           # Automatically start new template creation for any message
           # Treat as first message and show welcome
           profile = _fetch_profile(db, client_id)

           async def _welcome_with_greeting():
               # Then show the welcome message with profile info
               welcome_msg = f"""👋 Hello! I can see your profile:

⚖️ Current Weight: {profile['current_weight']} kg
🎯 Target Weight: {profile['target_weight']} kg
🏆 Goal: {profile['weight_delta_text']}
🍽️ Daily Calorie Target: {profile['target_calories']} kcal

🥗 I'll create a personalized 7-day meal plan just for you!  
First, please tell me your diet preference:  
🌱 Vegetarian  
🍗 Non-Vegetarian  
🥚 Eggetarian  
🌿 Vegan  
🥩 Ketogenic  
🥓 Paleo  
🙏 Jain"""

               await mem.set_pending(user_id, {
                   "state": "awaiting_diet_preference",
                   "client_id": client_id,
                   "profile": profile
               })

               yield sse_json_bytes({'message': welcome_msg, 'type': 'welcome'})
               yield sse_done()

           return StreamingResponse(_welcome_with_greeting(), media_type="text/event-stream",
                                   headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})
  
   except Exception as e:
       print(f"Critical error in chat_stream: {e}")
       print(f"Full critical traceback: {traceback.format_exc()}")
      
       try:
           await mem.clear_pending(user_id)
       except Exception as cleanup_error:
           print(f"Error clearing pending state: {cleanup_error}")
      
       async def _critical_error():
           yield sse_json_bytes({'message': 'Sorry, I encountered a technical error. Please try starting over. If the issue persists, contact support.', 'type': 'critical_error'})
           yield sse_done()
      
       return StreamingResponse(_critical_error(), media_type="text/event-stream",
                               headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


class UserId(BaseModel):
   user_id: int




@router.get("/debug_pending")
async def debug_pending(
   user_id: int,
   mem = Depends(get_mem),
):
   """Debug endpoint to check pending state"""
   try:
       pending_state = await mem.get_pending(user_id)
       return {"user_id": user_id, "pending_state": pending_state, "status": "success"}
   except Exception as e:
       return {"user_id": user_id, "pending_state": None, "error": str(e), "status": "error"}




@router.post("/clear_pending")
async def clear_pending_state(
   req: UserId,
   mem = Depends(get_mem),
):
   """Clear pending state for a user"""
   try:
       await mem.clear_pending(req.user_id)
       return {"status": "cleared", "user_id": req.user_id}
   except Exception as e:
       return {"status": "error", "user_id": req.user_id, "error": str(e)}




@router.post("/delete_chat")
async def delete_chat(
   req: UserId,
   mem = Depends(get_mem),
):
   """Delete chat history and pending state for a user"""
   try:
       print(f"Deleting template chat history for user {req.user_id}")
       history_key = f"template_chat:{req.user_id}:history"
       pending_key = f"template_chat:{req.user_id}:pending"
       deleted = await mem.r.delete(history_key, pending_key)
       return {"status": "deleted", "user_id": req.user_id, "keys_deleted": deleted}
   except Exception as e:
       print(f"Error deleting chat for user {req.user_id}: {e}")
       return {"status": "error", "user_id": req.user_id, "error": str(e)}


@router.post("/exit_chat")
async def exit_chat(
   req: UserId,
   mem = Depends(get_mem),
):
   """Called when user exits the chatbot - clears conversation state and chat history"""
   try:
       print(f"User {req.user_id} exiting food template chatbot - clearing all chat data")

       # Clear both chat history and pending state
       await mem.clear_chat_on_exit(req.user_id)

       return {
           "status": "success",
           "user_id": req.user_id,
           "message": "Chat history and state cleared successfully"
       }
   except Exception as e:
       print(f"Error clearing chat for user {req.user_id}: {e}")
       return {"status": "error", "user_id": req.user_id, "error": str(e)}




@router.get("/get_saved_template/{client_id}")
async def get_saved_template(client_id: int, db: Session = Depends(get_db)):
    """Get saved meal plan template for a client"""
    try:
        templates = db.query(ClientDietTemplate).filter(
            ClientDietTemplate.client_id == client_id
        ).order_by(ClientDietTemplate.template_name).all()
        
        if not templates:
            return {"status": "error", "message": "No saved templates found"}
        
        meal_plan = {}
        for template in templates:
            day_key = template.template_name.lower()
            # FIX: No need to json.loads() since diet_data is already a Python object from JSON column
            day_data = template.diet_data
            meal_plan[day_key] = day_data
        
        return {"status": "success", "client_id": client_id, "meal_plan": meal_plan, "total_days": len(templates)}
        
    except Exception as e:
        return {"status": "error", "message": str(e)}

@router.delete("/delete_saved_template/{client_id}")
async def delete_saved_template(client_id: int, db: Session = Depends(get_db)):
    """Delete saved meal plan templates for a client"""
    try:
        count = db.query(ClientDietTemplate).filter(ClientDietTemplate.client_id == client_id).delete()
        db.commit()
        
        if count == 0:
            return {"status": "error", "message": "No templates found to delete"}
        
        return {"status": "success", "message": f"Deleted {count} templates", "deleted_count": count}
        
    except Exception as e:
        db.rollback()
        return {"status": "error", "message": str(e)}


@router.post("/structurize_and_save")
async def structurize_and_save_meal_template(
    request: dict,
    db: Session = Depends(get_db)
):
    """Structurize and save meal template to database"""
    try:
        client_id = request.get("client_id")
        meal_plan = request.get("template")
        
        if not client_id or not meal_plan:
            return {"status": "error", "message": "Missing client_id or template"}

        # Delete existing templates
        db.query(ClientDietTemplate).filter(ClientDietTemplate.client_id == client_id).delete()
        
        # Day mapping
        day_names = {
            'monday': 'Monday', 'tuesday': 'Tuesday', 'wednesday': 'Wednesday',
            'thursday': 'Thursday', 'friday': 'Friday', 'saturday': 'Saturday', 'sunday': 'Sunday'
        }
        
        # Save each day
        templates_created = 0
        for day_key, day_data in meal_plan.items():
            day_name = day_names.get(day_key.lower(), day_key.title())
            
            # FIX: Store raw Python objects in JSON column, not JSON strings
            if isinstance(day_data, str):
                # If it's a JSON string, parse it back to Python object
                try:
                    diet_data_obj = json.loads(day_data)
                except json.JSONDecodeError:
                    # If parsing fails, treat as plain text
                    diet_data_obj = {"error": "Invalid JSON", "raw_data": day_data}
            else:
                # If it's already a Python object, use directly
                diet_data_obj = day_data

            new_template = ClientDietTemplate(
                client_id=client_id,
                template_name=day_name,
                diet_data=diet_data_obj
            )
            db.add(new_template)
            templates_created += 1

        db.commit()

        return {
            "status": "success", 
            "message": f"Saved {templates_created} meal templates",
            "templates_created": templates_created
        }
        
    except Exception as e:
        print(f"ERROR: Structurize and save failed: {e}")
        db.rollback()
        return {"status": "error", "message": str(e)}


# ============================================================================
# INDIAN MEAL PLAN GENERATOR ENDPOINTS
# ============================================================================

# All functions now in indian_fast_meal_generator.py (combined file)
# This replaces imports from: indian_meal_generator.py and food_swap_engine.py
from app.fittbot_api.v1.client.client_api.chatbot.chatbot_services.indian_fast_meal_generator import (
    FoodSwapEngine,
    find_food_alternatives,
    generate_indian_meal_plan,
    MEAL_SLOTS_CONFIG,
    DAYS_OF_WEEK
)


@router.post("/indian/generate-meal-plan")
async def api_generate_indian_meal_plan(
    request: dict,
    db: Session = Depends(get_db)
):
    """
    Generate personalized 7-day Indian meal plan

    Request body:
    {
        "client_id": 123,
        "daily_calories": 2000,
        "diet_type": "Vegetarian",  # Vegetarian, Non-Vegetarian, Vegan, Eggetarian, Jain, Paleo, Ketogenic
        "cuisine": "North Indian",   # North Indian, South Indian, Common
        "health_condition": "weight loss"  # Optional: diabetes, weight loss, muscle gain, heart health
    }
    """
    try:
        client_id = request.get("client_id")
        daily_calories = request.get("daily_calories", 2000)
        diet_type = request.get("diet_type", "Vegetarian")
        cuisine = request.get("cuisine", "Common")
        health_condition = request.get("health_condition")

        if not client_id:
            return {"status": "error", "message": "client_id is required"}

        print(f"Generating Indian meal plan for client {client_id}")
        print(f"  Calories: {daily_calories}, Diet: {diet_type}, Cuisine: {cuisine}")

        # Generate the meal plan
        meal_plan = generate_indian_meal_plan(
            db=db,
            daily_calories=float(daily_calories),
            diet_type=diet_type,
            cuisine=cuisine,
            health_condition=health_condition
        )

        # Format for display
        formatted_plan = format_meal_plan_for_user_display(
            meal_plan=meal_plan,
            diet_type=diet_type,
            cuisine_type=cuisine,
            target_calories=daily_calories
        )

        return {
            "status": "success",
            "message": f"Generated {len(meal_plan)}-day Indian meal plan",
            "meal_plan": meal_plan,
            "formatted_display": formatted_plan,
            "config": {
                "client_id": client_id,
                "daily_calories": daily_calories,
                "diet_type": diet_type,
                "cuisine": cuisine,
                "health_condition": health_condition
            }
        }

    except Exception as e:
        print(f"ERROR: Indian meal plan generation failed: {e}")
        import traceback
        traceback.print_exc()
        return {"status": "error", "message": str(e)}


@router.get("/indian/food-alternatives/{food_id}")
async def api_get_indian_food_alternatives(
    food_id: int,
    diet_type: str = "Vegetarian",
    cuisine: str = None,
    limit: int = 5,
    db: Session = Depends(get_db)
):
    """
    Get alternative foods for swapping from Indian food database

    Query parameters:
    - food_id: ID of the food to find alternatives for
    - diet_type: User's diet type (Vegetarian, Non-Vegetarian, etc.)
    - cuisine: User's cuisine preference (North Indian, South Indian, Common)
    - limit: Maximum number of alternatives (default: 5)
    """
    try:
        swap_engine = FoodSwapEngine(db)
        alternatives = swap_engine.find_food_swaps(
            food_id=food_id,
            diet_type=diet_type,
            cuisine=cuisine,
            limit=limit
        )

        return {
            "status": "success",
            "food_id": food_id,
            "alternatives": alternatives,
            "count": len(alternatives)
        }

    except Exception as e:
        print(f"ERROR: Get food alternatives failed: {e}")
        return {"status": "error", "message": str(e)}


@router.post("/indian/search-food")
async def api_search_indian_food(
    request: dict,
    db: Session = Depends(get_db)
):
    """
    Search for Indian foods by name and get alternatives

    Request body:
    {
        "food_name": "Aloo Paratha",
        "diet_type": "Vegetarian",
        "cuisine": "North Indian",
        "limit": 5
    }
    """
    try:
        food_name = request.get("food_name")
        diet_type = request.get("diet_type", "Vegetarian")
        cuisine = request.get("cuisine")
        limit = request.get("limit", 5)

        if not food_name:
            return {"status": "error", "message": "food_name is required"}

        alternatives = find_food_alternatives(
            db=db,
            food_name=food_name,
            diet_type=diet_type,
            cuisine=cuisine,
            limit=limit
        )

        return {
            "status": "success",
            "search_term": food_name,
            "alternatives": alternatives,
            "count": len(alternatives)
        }

    except Exception as e:
        print(f"ERROR: Search Indian food failed: {e}")
        return {"status": "error", "message": str(e)}


@router.post("/indian/swap-food")
async def api_swap_indian_food(
    request: dict,
    db: Session = Depends(get_db)
):
    """
    Swap a food item in an existing meal plan

    Request body:
    {
        "client_id": 123,
        "day": "Monday",
        "meal_slot_id": "5",
        "old_food_id": 456,
        "new_food_id": 789
    }
    """
    try:
        client_id = request.get("client_id")
        day = request.get("day")
        meal_slot_id = request.get("meal_slot_id")
        old_food_id = request.get("old_food_id")
        new_food_id = request.get("new_food_id")

        if not all([client_id, day, meal_slot_id, old_food_id, new_food_id]):
            return {"status": "error", "message": "Missing required parameters"}

        # Get current meal plan from database
        templates = db.query(ClientDietTemplate).filter(
            ClientDietTemplate.client_id == client_id
        ).all()

        if not templates:
            return {"status": "error", "message": "No meal plan found for client"}

        # Reconstruct meal plan
        meal_plan = {}
        for template in templates:
            meal_plan[template.template_name] = template.diet_data

        # Perform swap
        swap_engine = FoodSwapEngine(db)
        updated_plan = swap_engine.swap_food_in_meal_plan(
            meal_plan=meal_plan,
            day=day,
            meal_slot_id=str(meal_slot_id),
            old_food_id=int(old_food_id),
            new_food_id=int(new_food_id)
        )

        # Save updated plan back to database
        for template in templates:
            if template.template_name == day:
                template.diet_data = updated_plan[day]

        db.commit()

        return {
            "status": "success",
            "message": "Food swapped successfully",
            "updated_day": day,
            "meal_plan": updated_plan
        }

    except Exception as e:
        print(f"ERROR: Swap food failed: {e}")
        db.rollback()
        return {"status": "error", "message": str(e)}


@router.post("/indian/save-meal-plan")
async def api_save_indian_meal_plan(
    request: dict,
    db: Session = Depends(get_db)
):
    """
    Save Indian meal plan to database

    Request body:
    {
        "client_id": 123,
        "meal_plan": {...},  # The meal plan dict from generate-meal-plan
        "replace_all": true  # Optional: replace all existing templates
    }
    """
    try:
        client_id = request.get("client_id")
        meal_plan = request.get("meal_plan")
        replace_all = request.get("replace_all", True)

        if not client_id or not meal_plan:
            return {"status": "error", "message": "Missing client_id or meal_plan"}

        # Use the existing save function
        result = save_meal_plan_to_database(
            client_id=client_id,
            meal_plan=meal_plan,
            db=db,
            replace_all=replace_all
        )

        return {
            "status": "success" if result.get("success") else "error",
            "message": result.get("message"),
            "saved_count": result.get("saved_count", 0),
            "deleted_count": result.get("deleted_count", 0)
        }

    except Exception as e:
        print(f"ERROR: Save Indian meal plan failed: {e}")
        db.rollback()
        return {"status": "error", "message": str(e)}