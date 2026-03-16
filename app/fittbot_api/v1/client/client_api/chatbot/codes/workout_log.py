from fastapi import APIRouter, HTTPException, Query, Depends
from fastapi.responses import StreamingResponse
from fastapi_limiter.depends import RateLimiter
from pydantic import BaseModel
import asyncio
import pytz, os, hashlib, orjson, re, json
from datetime import datetime
from sqlalchemy.orm import Session
from app.models.database import get_db
from app.models.deps import get_http, get_oai, get_mem
from app.utils.async_openai import async_openai_call
from app.models.fittbot_models import ActualWorkout
import openai
from openai import AsyncOpenAI
import json, re, os
import time
import logging

# Import Celery tasks for exercise extraction
from app.tasks.workout_tasks import (
    extract_exercises as extract_exercises_task,
    extract_exercises_with_details as extract_exercises_with_details_task,
    parse_sets_reps as parse_sets_reps_task
)
from celery.result import AsyncResult

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/workout_log", tags=["workout_log"])

APP_ENV = os.getenv("APP_ENV", "prod")
TZNAME = os.getenv("TZ", "Asia/Kolkata")
IST = pytz.timezone(TZNAME)
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

@router.get("/healthz")
async def healthz():
    return {"ok": True, "env": APP_ENV, "tz": TZNAME}

def get_exercise_muscle_groups():
    """Database of exercises and their corresponding muscle groups"""
    return {
        # Chest exercises
        'push up': 'Chest', 'pushup': 'Chest', 'push ups': 'Chest',
        'bench press': 'Chest', 'dumbell bench press': 'Chest', 'dumbbell bench press': 'Chest',
        'flat bench press': 'Chest', 'incline bench press': 'Chest', 'decline bench press': 'Chest',
        'dumbell flat bench press': 'Chest', 'dumbell inclined bench press': 'Chest',
        'dumbbell flat bench press': 'Chest', 'dumbbell inclined bench press': 'Chest',
        'chest fly': 'Chest', 'chest flyes': 'Chest', 'dumbbell fly': 'Chest',
        'chest dips': 'Chest', 'dips': 'Chest',
        
        # Back exercises  
        'pull up': 'Back', 'pullup': 'Back', 'pull ups': 'Back', 'pullups': 'Back',
        'chin up': 'Back', 'chinup': 'Back', 'chin ups': 'Back',
        'lat pulldown': 'Back', 'lat pull down': 'Back',
        'bent over row': 'Back', 'barbell row': 'Back', 'dumbbell row': 'Back',
        'seated row': 'Back', 'cable row': 'Back',
        'deadlift': 'Back', 'deadlifts': 'Back',
        't bar row': 'Back', 't-bar row': 'Back',
        
        # Shoulders
        'shoulder press': 'Shoulders', 'overhead press': 'Shoulders',
        'military press': 'Shoulders', 'dumbell press': 'Shoulders', 'dumbbell press': 'Shoulders',
        'lateral raise': 'Shoulders', 'lateral raises': 'Shoulders', 'side raise': 'Shoulders',
        'front raise': 'Shoulders', 'front raises': 'Shoulders',
        'rear delt fly': 'Shoulders', 'reverse fly': 'Shoulders',
        'upright row': 'Shoulders', 'shrugs': 'Shoulders', 'shoulder shrugs': 'Shoulders',
        
        # Arms - Biceps
        'bicep curl': 'Arms', 'biceps curl': 'Arms', 'barbell curl': 'Arms',
        'dumbbell curl': 'Arms', 'dumbell curl': 'Arms', 'hammer curl': 'Arms',
        'concentration curl': 'Arms', 'preacher curl': 'Arms',
        
        # Arms - Triceps  
        'tricep dip': 'Arms', 'triceps dip': 'Arms', 'tricep dips': 'Arms',
        'tricep extension': 'Arms', 'triceps extension': 'Arms',
        'overhead tricep extension': 'Arms', 'skull crusher': 'Arms', 'skull crushers': 'Arms',
        'close grip bench press': 'Arms', 'diamond push up': 'Arms', 'diamond pushup': 'Arms',
        
        # Legs - Quads/Glutes
        'squat': 'Legs', 'squats': 'Legs', 'goblet squat': 'Legs',
        'leg press': 'Legs', 'lunge': 'Legs', 'lunges': 'Legs',
        'bulgarian split squat': 'Legs', 'split squat': 'Legs',
        'leg extension': 'Legs', 'quad extension': 'Legs',
        
        # Legs - Hamstrings/Glutes
        'romanian deadlift': 'Legs', 'rdl': 'Legs',
        'leg curl': 'Legs', 'hamstring curl': 'Legs',
        'hip thrust': 'Legs', 'glute bridge': 'Legs',
        
        # Legs - Calves
        'calf raise': 'Legs', 'calf raises': 'Legs',
        'standing calf raise': 'Legs', 'seated calf raise': 'Legs',
        
        # Core/Abs
        'plank': 'Core', 'planks': 'Core', 'side plank': 'Core',
        'crunch': 'Core', 'crunches': 'Core', 'sit up': 'Core', 'sit ups': 'Core',
        'russian twist': 'Core', 'russian twists': 'Core',
        'mountain climber': 'Core', 'mountain climbers': 'Core',
        'leg raise': 'Core', 'leg raises': 'Core',
        'bicycle crunch': 'Core', 'bicycle crunches': 'Core',
        
        # Cardio
        'running': 'Cardio', 'jogging': 'Cardio', 'walking': 'Cardio',
        'treadmill': 'Cardio', 'cycling': 'Cardio', 'bike': 'Cardio',
        'elliptical': 'Cardio', 'rowing': 'Cardio', 'burpee': 'Cardio', 'burpees': 'Cardio',
    }

def get_muscle_group_for_exercise(exercise_name):
    """Get muscle group for an exercise, with fuzzy matching"""
    exercise_lower = exercise_name.lower().strip()
    exercise_groups = get_exercise_muscle_groups()
    
    # Direct match
    if exercise_lower in exercise_groups:
        return exercise_groups[exercise_lower]
    
    # Partial match - check if exercise contains any key
    for key, group in exercise_groups.items():
        if key in exercise_lower or exercise_lower in key:
            return group
    
    # Default to 'Other' if no match found
    return 'Other'

def is_cardio_exercise(exercise_name):
    """Check if an exercise is cardio-based and should be measured by duration instead of sets/reps"""
    exercise_lower = exercise_name.lower().strip()
    
    cardio_keywords = [
        'running', 'jogging', 'walking', 'treadmill', 'cycling', 'bike', 
        'elliptical', 'rowing', 'swimming', 'cardio', 'sprint', 'jog',
        'stationary bike', 'exercise bike', 'spin', 'stair climber',
        'cross trainer', 'step machine'
    ]
    
    return any(keyword in exercise_lower for keyword in cardio_keywords)

def parse_cardio_duration(text: str):
    """Parse cardio duration from text"""
    text_lower = text.lower().strip()
    
    # Handle different formats
    hour_match = re.search(r'(\d+(?:\.\d+)?)\s*(hr|hrs|hour|hours)', text_lower)
    if hour_match:
        return float(hour_match.group(1)) * 60
    
    minute_match = re.search(r'(\d+(?:\.\d+)?)\s*(min|mins|minute|minutes)', text_lower)
    if minute_match:
        return float(minute_match.group(1))
    
    # Handle HH:MM format
    time_match = re.search(r'(\d+):(\d+)', text_lower)
    if time_match:
        hours = int(time_match.group(1))
        minutes = int(time_match.group(2))
        return hours * 60 + minutes
    
    # Just numbers - assume minutes for cardio
    number_match = re.search(r'^\d+(?:\.\d+)?$', text_lower)
    if number_match:
        return float(number_match.group(0))
    
    return None

def create_cardio_sets(exercise_name, duration_minutes, weight_kg):
    """Create a single 'set' for cardio exercises based on duration"""
    met_value = calculate_met_value(exercise_name)
    calories = calculate_calories_per_set(weight_kg, met_value, duration_minutes)

    return [{
        "setNumber": 1,
        "startTime": "",
        "endTime": "",
        "reps": int(duration_minutes),  # Store duration as "reps" for consistency
        "weight": 0,
        "duration": duration_minutes,
        "MET": met_value,
        "calories": calories
    }]

def calculate_met_value(exercise_name):
    """Calculate MET value based on exercise type - always returns moderate intensity values"""
    exercise_lower = exercise_name.lower()

    # Moderate MET values for different exercise types
    moderate_mets = {
        'cardio': 6.0,
        'strength': 6.0,
        'bodyweight': 5.0
    }

    # Determine exercise category
    cardio_keywords = ['running', 'jogging', 'walking', 'cycling', 'bike', 'treadmill',
                      'elliptical', 'rowing', 'burpee', 'mountain climber']
    bodyweight_keywords = ['push up', 'pull up', 'plank', 'squat', 'lunge', 'dip',
                          'crunch', 'sit up']

    if any(keyword in exercise_lower for keyword in cardio_keywords):
        category = 'cardio'
    elif any(keyword in exercise_lower for keyword in bodyweight_keywords):
        category = 'bodyweight'
    else:
        category = 'strength'

    # Always return moderate MET value
    return moderate_mets[category]

def calculate_calories_per_set(weight_kg, met_value, duration_minutes):
    """Calculate calories burned per set"""
    # Formula: Calories = MET × weight(kg) × time(hours)
    calories = met_value * weight_kg * (duration_minutes / 60)*2
    return round(calories, 1)


# =============================================================================
# CELERY WRAPPERS - Route OpenAI calls through Celery for rate limiting
# =============================================================================

async def extract_exercises_celery(user_id: int, text: str, timeout: float = 30.0) -> list:
    """
    Celery wrapper for extract_exercises_using_openai
    Queues task and polls for result with timeout
    """
    try:
        task = extract_exercises_task.delay(user_id, text)
        #logger.info(f"Queued extract_exercises task {task.id} for user {user_id}")

        start_time = time.time()
        while time.time() - start_time < timeout:
            result = AsyncResult(task.id)
            if result.ready():
                if result.successful():
                    return result.result or []
                else:
                    logger.error(f"Task {task.id} failed: {result.result}")
                    return []
            await asyncio.sleep(0.3)

        logger.warning(f"Task {task.id} timed out after {timeout}s")
        return []

    except Exception as e:
        logger.error(f"extract_exercises_celery error for user {user_id}: {e}")
        return []


async def extract_exercises_with_details_celery(user_id: int, text: str, timeout: float = 30.0) -> list:
    """
    Celery wrapper for extract_exercises_with_details
    Queues task and polls for result with timeout
    """
    try:
        task = extract_exercises_with_details_task.delay(user_id, text)
        #logger.info(f"Queued extract_exercises_with_details task {task.id} for user {user_id}")

        start_time = time.time()
        while time.time() - start_time < timeout:
            result = AsyncResult(task.id)
            if result.ready():
                if result.successful():
                    return result.result or []
                else:
                    logger.error(f"Task {task.id} failed: {result.result}")
                    return []
            await asyncio.sleep(0.3)

        logger.warning(f"Task {task.id} timed out after {timeout}s")
        return []

    except Exception as e:
        logger.error(f"extract_exercises_with_details_celery error for user {user_id}: {e}")
        return []


async def parse_sets_reps_celery(user_id: int, text: str, exercise_name: str = "", timeout: float = 30.0) -> dict:
    """
    Celery wrapper for parse_reps_and_sets
    Queues task and polls for result with timeout
    """
    try:
        task = parse_sets_reps_task.delay(user_id, text, exercise_name)
        #ogger.info(f"Queued parse_sets_reps task {task.id} for user {user_id}")

        start_time = time.time()
        while time.time() - start_time < timeout:
            result = AsyncResult(task.id)
            if result.ready():
                if result.successful():
                    return result.result
                else:
                    logger.error(f"Task {task.id} failed: {result.result}")
                    return parse_reps_and_sets_fallback(text)
            await asyncio.sleep(0.3)

        logger.warning(f"Task {task.id} timed out after {timeout}s, using fallback")
        return parse_reps_and_sets_fallback(text)

    except Exception as e:
        logger.error(f"parse_sets_reps_celery error for user {user_id}: {e}")
        return parse_reps_and_sets_fallback(text)


# =============================================================================
# WORKOUT VOICE FUNCTIONS
# =============================================================================

async def get_voice_preference(db: Session, client_id: int) -> str:
    """Get voice preference for a client - returns '1' for enabled, '0' for disabled"""
    try:
        from app.models.fittbot_models import VoicePreference
        voice_pref = db.query(VoicePreference).filter(VoicePreference.client_id == client_id).first()
        if voice_pref:
            return voice_pref.preference
        else:
            return "1"  # Default to enabled
    except Exception as e:
        logger.error(f"Error getting voice preference for client {client_id}: {e}")
        return "1"  # Default to enabled on error


async def trigger_workout_log_success_voice(user_id: int, duration_minutes: int, total_calories: float, exercises_count: int, db: Session):
    """Trigger voice notification via Celery task for workout logging success"""
    try:
        # Check voice preference using existing async helper
        voice_pref = await get_voice_preference(db, user_id)

        if voice_pref == "1":  # Voice enabled
            from app.tasks.voice_tasks import process_workout_log_success_voice
            # Trigger Celery task for non-blocking voice processing
            process_workout_log_success_voice.delay(user_id, duration_minutes, total_calories, exercises_count)
            #logger.info(f"[WORKOUT_VOICE_TRIGGER] Voice notification triggered for user {user_id}, duration: {duration_minutes}, calories: {total_calories}, exercises: {exercises_count}")
        else:
            logger.info(f"[WORKOUT_VOICE_TRIGGER] Voice disabled for user {user_id}, skipping workout voice notification")

    except Exception as e:
        logger.error(f"[WORKOUT_VOICE_TRIGGER] Error triggering workout voice notification: {e}")


# =============================================================================
# ORIGINAL OPENAI FUNCTIONS (kept for reference/fallback)
# =============================================================================

async def extract_exercises_using_openai(text: str, client):
    """Extract exercises from user input using OpenAI"""
    try:
        prompt = f"""
        Extract exercise names from this text: "{text}"

        RULES:
        1. Extract ANY exercise or physical activity mentioned
        2. Handle common misspellings and variations
        3. Normalize exercise names to standard form
        4. Be very permissive - if it sounds like exercise, include it

        EXAMPLES:
        - "pushup" → "Push Up"
        - "dumbell bench pres" → "Dumbbell Bench Press"
        - "bicep curls" → "Bicep Curl"
        - "squats and lunges" → ["Squat", "Lunge"]

        Return JSON:
        {{
            "exercises": ["Exercise Name 1", "Exercise Name 2"]
        }}
        """

        response = await async_openai_call(client,
            model="gpt-4o",
            messages=[
                {"role": "system", "content": "You are an exercise recognition expert. Extract and normalize exercise names."},
                {"role": "user", "content": prompt}
            ],
            max_tokens=300,
            temperature=0.1
        )

        result = response.choices[0].message.content.strip()
        result = re.sub(r"^```json\s*", "", result)
        result = re.sub(r"\s*```$", "", result)

        parsed = json.loads(result)
        exercises = parsed.get("exercises", [])

        return [ex.strip() for ex in exercises if ex.strip()]

    except Exception as e:
        print(f"OpenAI exercise extraction error: {e}")

        # Fallback: simple text parsing
        text_lower = text.lower().strip()
        exercise_db = get_exercise_muscle_groups()

        found_exercises = []
        for exercise_key in exercise_db.keys():
            if exercise_key in text_lower:
                # Capitalize properly
                exercise_name = ' '.join(word.capitalize() for word in exercise_key.split())
                if exercise_name not in found_exercises:
                    found_exercises.append(exercise_name)

        return found_exercises[:5]  # Limit to 5 exercises

async def extract_exercises_with_details(text: str, client):
    """
    Extract exercises along with sets/reps if provided in the initial message.
    Returns a list of dicts with exercise name and optionally sets_reps_data or duration.
    """
    try:
        prompt = f"""
        Parse workout information from this text: "{text}"

        Extract ALL exercises mentioned and check if sets, reps, or duration are provided for each.

        EXAMPLES:
        - "I did pushups and pullups" → [{{"exercise": "Push Up", "has_sets_reps": false}}, {{"exercise": "Pull Up", "has_sets_reps": false}}]
        - "I did 3 sets of pushups" → [{{"exercise": "Push Up", "has_sets_reps": true, "sets": 3, "reps": null}}]
        - "I did 3x10 pushups" → [{{"exercise": "Push Up", "has_sets_reps": true, "sets": 3, "reps": 10}}]
        - "I did 3x10 pushups and 4x8 pullups" → [{{"exercise": "Push Up", "has_sets_reps": true, "sets": 3, "reps": 10}}, {{"exercise": "Pull Up", "has_sets_reps": true, "sets": 4, "reps": 8}}]
        - "30 minutes of running" → [{{"exercise": "Running", "has_duration": true, "duration_minutes": 30}}]
        - "I did pushups 3 sets of 15 reps" → [{{"exercise": "Push Up", "has_sets_reps": true, "sets": 3, "reps": 15}}]

        Return JSON array:
        [
            {{
                "exercise": "Exercise Name",
                "has_sets_reps": true/false,
                "sets": number or null,
                "reps": number or null,
                "has_duration": true/false,
                "duration_minutes": number or null
            }}
        ]

        RULES:
        - If sets or reps are mentioned for an exercise, set has_sets_reps to true
        - If only sets mentioned (e.g., "3 sets of pushups"), include sets but set reps to null
        - If only reps mentioned, set sets to null
        - For cardio exercises with duration, set has_duration to true
        - If no sets/reps/duration mentioned, set has_sets_reps and has_duration to false
        """

        response = await async_openai_call(client,
            model="gpt-4o",
            messages=[
                {"role": "system", "content": "You are an exercise recognition expert. Extract exercises and their sets/reps/duration details."},
                {"role": "user", "content": prompt}
            ],
            max_tokens=500,
            temperature=0.1
        )

        result = response.choices[0].message.content.strip()
        result = re.sub(r"^```json\s*", "", result)
        result = re.sub(r"\s*```$", "", result)

        parsed = json.loads(result)

        # Ensure it's a list
        if not isinstance(parsed, list):
            parsed = [parsed]

        return parsed

    except Exception as e:
        print(f"OpenAI detailed extraction error: {e}")

        # Fallback: extract exercises only
        exercises = await extract_exercises_using_openai(text, client)
        return [{"exercise": ex, "has_sets_reps": False, "sets": None, "reps": None, "has_duration": False, "duration_minutes": None} for ex in exercises]

async def parse_reps_and_sets(text: str, client, exercise_name: str = ""):
    """Enhanced parsing that handles various formats including variable reps per set"""
    try:
        # Try AI-powered parsing first
        prompt = f"""
        Parse sets and reps from this text: "{text}"
        Exercise: {exercise_name}
        
        Handle these formats:
        - "3 sets 10 reps" → {{"sets": 3, "reps": 10, "format": "uniform"}}
        - "30 in first, 40 in 2nd set" → {{"sets": 2, "reps": [30, 40], "format": "variable"}}
        - "3x12" → {{"sets": 3, "reps": 12, "format": "uniform"}}
        - "15, 12, 10" → {{"sets": 3, "reps": [15, 12, 10], "format": "variable"}}
        
        Return JSON: {{"sets": number, "reps": number_or_array, "format": "uniform"|"variable"}}
        """

        response = await async_openai_call(client,
            model="gpt-4o",
            messages=[
                {"role": "system", "content": "Extract workout sets/reps data. Return valid JSON only."},
                {"role": "user", "content": prompt}
            ],
            max_tokens=150,
            temperature=0.1
        )
        
        result = response.choices[0].message.content.strip()
        result = re.sub(r"^```json\s*", "", result)
        result = re.sub(r"\s*```$", "", result)
        
        parsed = json.loads(result)
        return parsed
        
    except Exception as e:
        print(f"AI parsing failed: {e}")
        return parse_reps_and_sets_fallback(text)
    

def estimate_exercise_duration_per_set(exercise_name, reps):
    """Estimate actual exercise time per set (excluding rest)"""
    exercise_lower = exercise_name.lower()
    
    # Time per rep in seconds based on exercise type
    if any(keyword in exercise_lower for keyword in ['plank', 'wall sit', 'hold']):
        # Static holds - reps usually represent seconds
        return reps  # seconds
    elif any(keyword in exercise_lower for keyword in ['running', 'cycling', 'walking', 'cardio']):
        # Cardio exercises - different calculation
        return max(60, reps * 2)  # minimum 1 minute, or 2 seconds per "rep"
    elif any(keyword in exercise_lower for keyword in ['push up', 'pull up', 'squat', 'burpee']):
        # Bodyweight exercises - typically faster
        return reps * 2  # 2 seconds per rep
    elif any(keyword in exercise_lower for keyword in ['bench press', 'deadlift', 'squat', 'press']):
        # Heavy compound movements - slower
        return reps * 4  # 4 seconds per rep
    else:
        # Default isolation exercises
        return reps * 3  # 3 seconds per rep

def calculate_total_exercise_time(sets_data, exercise_name):
    """Calculate total actual exercise time (excluding rest)"""
    total_exercise_seconds = 0
    
    for set_data in sets_data:
        reps = set_data.get('reps', 0)
        exercise_time_seconds = estimate_exercise_duration_per_set(exercise_name, reps)
        total_exercise_seconds += exercise_time_seconds
    
    return total_exercise_seconds / 60  # Convert to minutes

def create_exercise_sets(exercise_name, sets_reps_data, weight_kg, total_duration_minutes=None, total_sets_in_workout=1):
    """Create sets data with improved duration-based calorie calculations"""
    if not sets_reps_data:
        return []
    
    sets_count = sets_reps_data["sets"]
    reps = sets_reps_data["reps"]
    format_type = sets_reps_data.get("format", "uniform")
    
    sets_data = []
    met_value = calculate_met_value(exercise_name)
    
    # First, create sets with reps information
    for i in range(sets_count):
        if format_type == "variable" and isinstance(reps, list):
            set_reps = reps[i] if i < len(reps) else reps[-1]
        else:
            set_reps = reps
        
        set_data = {
            "setNumber": i + 1,
            "startTime": "",
            "endTime": "",
            "reps": set_reps,
            "weight": 0,
            "duration": 0,  # Will be calculated below
            "MET": met_value,
            "calories": 0.0  # Will be calculated below
        }
        sets_data.append(set_data)
    
    # Update sets with calculated duration and calories
    for i, set_data in enumerate(sets_data):
        # Use individual set duration based on reps
        individual_reps = set_data['reps']
        individual_duration = estimate_exercise_duration_per_set(exercise_name, individual_reps) / 60  # Convert to minutes
        
        set_data["duration"] = round(individual_duration, 1)
        
        # Calculate calories based on actual exercise time (not including rest)
        calories = calculate_calories_per_set(weight_kg, met_value, individual_duration)
        set_data["calories"] = calories
    
    return sets_data





def calculate_total_exercise_time(sets_data, exercise_name):
    """Calculate total actual exercise time (excluding rest)"""
    total_exercise_seconds = 0
    
    for set_data in sets_data:
        reps = set_data.get('reps', 0)
        exercise_time_seconds = estimate_exercise_duration_per_set(exercise_name, reps)
        total_exercise_seconds += exercise_time_seconds
    
    return total_exercise_seconds / 60  # Convert to minutes


def parse_reps_and_sets_fallback(text: str):
    """Rule-based fallback parsing"""
    text_lower = text.lower().strip()
    numbers = [int(x) for x in re.findall(r'\d+', text)]
    
    if not numbers:
        return None
    
    # Pattern: "X sets Y reps"
    sets_reps_match = re.search(r'(\d+)\s*sets?\s*(?:of\s*)?(\d+)\s*reps?', text_lower)
    if sets_reps_match:
        return {
            "sets": int(sets_reps_match.group(1)),
            "reps": int(sets_reps_match.group(2)),
            "format": "uniform"
        }
    
    # Pattern: "XxY"
    x_format = re.search(r'(\d+)\s*x\s*(\d+)', text_lower)
    if x_format:
        return {
            "sets": int(x_format.group(1)),
            "reps": int(x_format.group(2)),
            "format": "uniform"
        }
    
    # Pattern: Variable reps - look for ordinal indicators or commas
    if any(word in text_lower for word in ['first', 'second', '1st', '2nd', 'set']) and len(numbers) >= 2:
        # Extract reps mentioned for each set
        reps_list = []
        
        # Look for pattern like "X in first, Y in second"
        first_match = re.search(r'(\d+)\s*(?:in\s*)?(?:first|1st)', text_lower)
        second_match = re.search(r'(\d+)\s*(?:in\s*)?(?:second|2nd)', text_lower)
        third_match = re.search(r'(\d+)\s*(?:in\s*)?(?:third|3rd)', text_lower)
        
        if first_match and second_match:
            reps_list = [int(first_match.group(1)), int(second_match.group(1))]
            if third_match:
                reps_list.append(int(third_match.group(1)))
        elif len(numbers) >= 2:
            reps_list = numbers[:4]  # Take first 4 numbers as reps per set
        
        if reps_list:
            return {
                "sets": len(reps_list),
                "reps": reps_list,
                "format": "variable"
            }
    
    # Pattern: Comma-separated numbers
    if ',' in text and len(numbers) >= 2:
        return {
            "sets": len(numbers),
            "reps": numbers,
            "format": "variable"
        }
    
    # Default patterns
    if len(numbers) == 1:
        return {"sets": 3, "reps": numbers[0], "format": "uniform"}
    elif len(numbers) == 2:
        return {"sets": numbers[0], "reps": numbers[1], "format": "uniform"}
    
    return None

def is_exercise_related(text):
    """Check if text is exercise-related"""
    if not text:
        return False
    
    text_lower = text.lower().strip()
    
    # Exercise indicators
    exercise_keywords = [
        'workout', 'exercise', 'gym', 'training', 'lift', 'run', 'walk', 'cycle',
        'push', 'pull', 'squat', 'press', 'curl', 'raise', 'extension', 'fly'
    ]
    
    if any(keyword in text_lower for keyword in exercise_keywords):
        return True
    
    # Check against exercise database
    exercise_db = get_exercise_muscle_groups()
    return any(exercise_key in text_lower for exercise_key in exercise_db.keys())


def is_duration_input(text):
    """Check if input is duration"""
    if not text:
        return False
    
    text_lower = text.lower().strip()
    
    # Check for duration patterns
    duration_patterns = [
        r'\d+\s*(min|mins|minute|minutes)',
        r'\d+\s*(hr|hrs|hour|hours)',
        r'\d+:\d+',  # 1:30 format
        r'^\d+$'     # Just numbers (assume minutes)
    ]
    
    return any(re.search(pattern, text_lower) for pattern in duration_patterns)

def parse_duration(text):
    """Parse duration from text and return minutes"""
    text_lower = text.lower().strip()
    
    # Handle different formats
    hour_match = re.search(r'(\d+)\s*(hr|hrs|hour|hours)', text_lower)
    if hour_match:
        return int(hour_match.group(1)) * 60
    
    minute_match = re.search(r'(\d+)\s*(min|mins|minute|minutes)', text_lower)
    if minute_match:
        return int(minute_match.group(1))
    
    # Handle HH:MM format
    time_match = re.search(r'(\d+):(\d+)', text_lower)
    if time_match:
        hours = int(time_match.group(1))
        minutes = int(time_match.group(2))
        return hours * 60 + minutes
    
    # Just numbers - assume minutes
    number_match = re.search(r'^\d+$', text_lower)
    if number_match:
        return int(number_match.group(0))
    
    return None

def save_workout_to_database(db: Session, client_id: int, workout_details: list, gym_id: int = 0):
    """Save workout data using the same logic as /actual_workout/add"""
    from datetime import date

    try:
        # print(f"DEBUG: save_workout_to_database called with client_id={client_id}")

        today = date.today()

        # Check if record exists (same logic as actual_workout.py line 89-93)
        record = (
            db.query(ActualWorkout)
            .filter(ActualWorkout.client_id == client_id, ActualWorkout.date == today)
            .first()
        )

        if record:
            # print(f"DEBUG: Found existing workout, appending data")
            # Append to existing workout (same logic as actual_workout.py line 95-103)
            if record.workout_details is None:
                record.workout_details = workout_details
            else:
                if isinstance(record.workout_details, list):
                    updated_list = record.workout_details + workout_details
                    record.workout_details = updated_list
                else:
                    record.workout_details = [record.workout_details] + workout_details
            db.commit()
            db.refresh(record)
            # print(f"✅ Updated existing workout with record_id: {record.record_id}")
            return record.record_id
        else:
            # print(f"DEBUG: Creating new workout record")
            # Create new record (same logic as actual_workout.py line 107-114)
            record = ActualWorkout(
                client_id=client_id,
                date=today,
                workout_details=workout_details,
            )
            db.add(record)
            db.commit()
            db.refresh(record)
            # print(f"✅ Created new workout with record_id: {record.record_id}")
            return record.record_id

    except Exception as e:
        print(f"❌ Error saving workout to database: {e}")
        import traceback
        print(f"Full traceback: {traceback.format_exc()}")
        db.rollback()
        return None

def create_workout_response(client_id, exercises_data, current_weight, total_duration_minutes=None):
    """Create workout response with proper time distribution for both cardio and strength exercises"""
    from datetime import datetime

    workout_by_muscle = {}

    for exercise_data in exercises_data:
        exercise_name = exercise_data['name']
        muscle_group = get_muscle_group_for_exercise(exercise_name)

        if muscle_group not in workout_by_muscle:
            workout_by_muscle[muscle_group] = []

        # Handle cardio vs strength exercises differently
        if exercise_data.get('is_cardio', False):
            # Create cardio sets
            sets_data = create_cardio_sets(
                exercise_name,
                exercise_data['duration_minutes'],
                current_weight
            )
        else:
            # Create regular strength sets
            sets_data = create_exercise_sets(
                exercise_name,
                exercise_data['sets_reps_data'],
                current_weight,
                total_duration_minutes,
                1
            )

        exercise_entry = {
            "name": exercise_name,
            "sets": sets_data
        }

        workout_by_muscle[muscle_group].append(exercise_entry)

    # Convert to required format
    workout_details = []
    for muscle_group, exercises in workout_by_muscle.items():
        workout_details.append({muscle_group: exercises})

    return {
        "client_id": client_id,
        "date": datetime.now().strftime("%Y-%m-%d"),
        "workout_details": workout_details,
        "live_status": False,
        "gym_id": 0
    }


def sse_json(data):
    """Helper function to format SSE JSON response"""
    return f"data: {json.dumps(data)}\n\n"

def is_yes(text):
    """Check if user said yes"""
    if not text:
        return False
    text_lower = text.lower().strip()
    return text_lower in ['yes', 'y', 'yeah', 'yep', 'ok', 'okay', 'sure']

def is_no(text):
    """Check if user said no"""
    if not text:
        return False
    text_lower = text.lower().strip()
    return text_lower in ['no', 'n', 'nope', 'nah']

async def workout_chat_stream_internal(
    user_id: int,
    text: str,
    mem,
    db,
    oai=Depends(get_oai),
    fresh_start: bool = False,
):
    """Internal version of workout_chat_stream for calling from ai_chatbot.py"""
    client_id = user_id
    try:
        # Get client profile for current weight
        profile = _fetch_profile(db, client_id)
        current_weight = profile.get('current_weight', 70.0)

        # Clear pending state if it's a fresh start or no text provided
        if not text or fresh_start:
            await mem.set_pending(client_id, None)

        if not text:
            async def _welcome():
                welcome_msg = "Hello! I'm your workout logging assistant. What exercises did you do today?"
                await mem.add(client_id, "assistant", welcome_msg)
                yield f"data: {orjson.dumps({'message': welcome_msg, 'type': 'response'}).decode()}\n\n"
                yield "event: done\ndata: [DONE]\n\n"
            return StreamingResponse(_welcome(), media_type="text/event-stream")

        # Rest of the logic will be the same as the main function...
        # Let me copy the main logic here
        await mem.add(client_id, "user", text.strip())

        # Get current pending state
        pending_state = await mem.get_pending(client_id)

        # Handle sets and reps input
        if pending_state and pending_state.get("state") == "awaiting_sets_reps":
            exercises = pending_state.get("exercises", [])
            current_index = pending_state.get("current_exercise_index", 0)
            exercises_data = pending_state.get("exercises_data", [])
            needs_input = pending_state.get("needs_input", [])

            # Find the current item that needs input
            current_need = None
            current_need_idx = None
            for idx, need in enumerate(needs_input):
                if need["index"] == current_index:
                    current_need = need
                    current_need_idx = idx
                    break

            if current_need:
                current_exercise = current_need["exercise"]

                # Check if current exercise needs duration (cardio)
                if current_need["type"] == "duration":
                    # Parse duration instead of sets/reps
                    duration_minutes = parse_cardio_duration(text)

                    if duration_minutes:
                        # Remove any existing incomplete entry for this exercise
                        exercises_data = [ex for ex in exercises_data if not (
                            ex.get('name') == current_exercise and
                            ex.get('needs_duration') is True
                        )]

                        # Store cardio data
                        exercise_data = {
                            'name': current_exercise,
                            'is_cardio': True,
                            'duration_minutes': duration_minutes,
                            'intensity': 'moderate'
                        }
                        exercises_data.append(exercise_data)

                        # Remove this item from needs_input
                        needs_input.pop(current_need_idx)

                        # Check if there are more exercises that need input
                        if needs_input:
                            next_need = needs_input[0]
                            next_exercise = next_need["exercise"]

                            if next_need["type"] == "duration":
                                ask_message = f"How long did you do {next_exercise}? (e.g., '30 minutes', '45', or '1 hour')"
                            else:
                                ask_message = f"How many sets and reps did you do for {next_exercise}? (e.g., '3 sets 10 reps' or '3x10')"

                            await mem.add(client_id, "assistant", ask_message)

                            await mem.set_pending(client_id, {
                                "state": "awaiting_sets_reps",
                                "exercises": exercises,
                                "current_exercise_index": next_need["index"],
                                "exercises_data": exercises_data,
                                "needs_input": needs_input,
                                "current_weight": current_weight
                            })

                            async def _ask_next():
                                yield f"data: {orjson.dumps({'message': ask_message, 'type': 'ask_sets_reps'}).decode()}\n\n"
                                yield "event: done\ndata: [DONE]\n\n"

                            return StreamingResponse(_ask_next(), media_type="text/event-stream")
                        else:
                            # All exercises done, set default intensity and proceed to workout duration
                            # Set all exercises to moderate intensity by default
                            for exercise_data in exercises_data:
                                exercise_data['intensity'] = 'moderate'

                            # Ask for total workout duration
                            duration_msg = "What was your total workout duration? (e.g., '45 minutes', '1 hour 30 minutes', or '90')"
                            await mem.add(client_id, "assistant", duration_msg)

                            await mem.set_pending(client_id, {
                                "state": "awaiting_duration",
                                "exercises_data": exercises_data,
                                "current_weight": current_weight
                            })

                            async def _ask_duration():
                                yield f"data: {orjson.dumps({'message': duration_msg, 'type': 'ask_duration'}).decode()}\n\n"
                                yield "event: done\ndata: [DONE]\n\n"

                            return StreamingResponse(_ask_duration(), media_type="text/event-stream")
                    else:
                        # Ask for valid duration
                        ask_message = f"Please specify how long you did {current_exercise}. Examples: '30 minutes', '45', '1 hour', or '1:30'"
                        await mem.add(client_id, "assistant", ask_message)

                        async def _ask_duration_again():
                            yield f"data: {orjson.dumps({'message': ask_message, 'type': 'ask_duration'}).decode()}\n\n"
                            yield "event: done\ndata: [DONE]\n\n"

                        return StreamingResponse(_ask_duration_again(), media_type="text/event-stream")

                else:
                    # Handle regular exercises with sets/reps
                    # Use Celery wrapper for rate-limited OpenAI calls
                    sets_reps_data = await parse_sets_reps_celery(client_id, text, current_exercise)

                    if sets_reps_data:
                        # Remove any existing incomplete entry for this exercise
                        exercises_data = [ex for ex in exercises_data if not (
                            ex.get('name') == current_exercise and
                            ex.get('needs_sets_reps') is True
                        )]

                        exercise_data = {
                            'name': current_exercise,
                            'sets_reps_data': sets_reps_data,
                            'is_cardio': False,
                            'intensity': 'moderate'
                        }
                        exercises_data.append(exercise_data)

                        # Remove this item from needs_input
                        needs_input.pop(current_need_idx)

                        # Check if there are more exercises that need input
                        if needs_input:
                            next_need = needs_input[0]
                            next_exercise = next_need["exercise"]

                            if next_need["type"] == "duration":
                                ask_message = f"How long did you do {next_exercise}? (e.g., '30 minutes', '45', or '1 hour')"
                            else:
                                ask_message = f"How many sets and reps did you do for {next_exercise}? (e.g., '3 sets 10 reps' or '3x10')"

                            await mem.add(client_id, "assistant", ask_message)

                            await mem.set_pending(client_id, {
                                "state": "awaiting_sets_reps",
                                "exercises": exercises,
                                "current_exercise_index": next_need["index"],
                                "exercises_data": exercises_data,
                                "needs_input": needs_input,
                                "current_weight": current_weight
                            })

                            async def _ask_next():
                                yield f"data: {orjson.dumps({'message': ask_message, 'type': 'ask_sets_reps'}).decode()}\n\n"
                                yield "event: done\ndata: [DONE]\n\n"

                            return StreamingResponse(_ask_next(), media_type="text/event-stream")
                        else:
                            # All exercises done, set default intensity and proceed to workout duration
                            # Set all exercises to moderate intensity by default
                            for exercise_data in exercises_data:
                                exercise_data['intensity'] = 'moderate'

                            # Ask for total workout duration
                            duration_msg = "What was your total workout duration? (e.g., '45 minutes', '1 hour 30 minutes', or '90')"
                            await mem.add(client_id, "assistant", duration_msg)

                            await mem.set_pending(client_id, {
                                "state": "awaiting_duration",
                                "exercises_data": exercises_data,
                                "current_weight": current_weight
                            })

                            async def _ask_duration():
                                yield f"data: {orjson.dumps({'message': duration_msg, 'type': 'ask_duration'}).decode()}\n\n"
                                yield "event: done\ndata: [DONE]\n\n"

                            return StreamingResponse(_ask_duration(), media_type="text/event-stream")
                    else:
                        # Ask again with better guidance
                        ask_message = f"Please specify sets and reps for {current_exercise}. For example: '3 sets 10 reps', '3x10', or '4 sets 12 reps'"
                        await mem.add(client_id, "assistant", ask_message)

                        async def _ask_sets_reps_again():
                            yield f"data: {orjson.dumps({'message': ask_message, 'type': 'ask_sets_reps'}).decode()}\n\n"
                            yield "event: done\ndata: [DONE]\n\n"

                        return StreamingResponse(_ask_sets_reps_again(), media_type="text/event-stream")
            else:
                # Fallback: if we can't find what we need, set default intensity and move to duration
                if exercises_data:
                    # Set all exercises to moderate intensity by default
                    for exercise_data in exercises_data:
                        exercise_data['intensity'] = 'moderate'

                    # Ask for total workout duration
                    duration_msg = "What was your total workout duration? (e.g., '45 minutes', '1 hour 30 minutes', or '90')"
                    await mem.add(client_id, "assistant", duration_msg)

                    await mem.set_pending(client_id, {
                        "state": "awaiting_duration",
                        "exercises_data": exercises_data,
                        "current_weight": current_weight
                    })

                    async def _ask_duration():
                        yield f"data: {orjson.dumps({'message': duration_msg, 'type': 'ask_duration'}).decode()}\n\n"
                        yield "event: done\ndata: [DONE]\n\n"

                    return StreamingResponse(_ask_duration(), media_type="text/event-stream")

  
        # Handle duration input and finalize workout
        elif pending_state and pending_state.get("state") == "awaiting_duration":
            exercises_data = pending_state.get("exercises_data", [])

            # Parse duration
            duration_minutes = parse_cardio_duration(text)
            if not duration_minutes:
                duration_minutes = 10  # Default

            # Clear pending state
            await mem.set_pending(client_id, None)

            # Filter out incomplete exercises before creating workout
            complete_exercises_data = [
                ex for ex in exercises_data
                if not ex.get('needs_sets_reps') and not ex.get('needs_duration')
            ]

            # Create and save workout
            workout_response = create_workout_response(user_id, complete_exercises_data, current_weight, duration_minutes)
            record_id = save_workout_to_database(db, user_id, workout_response['workout_details'])

            # Calculate total calories
            total_calories = 0
            for muscle_group_data in workout_response['workout_details']:
                for muscle_group, exercises_list in muscle_group_data.items():
                    for exercise in exercises_list:
                        for set_data in exercise['sets']:
                            total_calories += set_data['calories']

            success_message = f"✅ Workout logged successfully! Total duration: {duration_minutes} minutes, Estimated calories burned: {round(total_calories, 1)}"
            await mem.add(client_id, "assistant", success_message)

            async def _workout_complete():
                workout_response['message'] = success_message
                workout_response['type'] = 'workout_logged'
                workout_response['status'] = 'logged'
                workout_response['is_log'] = True
                workout_response['total_duration_minutes'] = duration_minutes
                workout_response['total_calories'] = round(total_calories, 1)
                yield f"data: {orjson.dumps(workout_response).decode()}\n\n"
                yield "event: done\ndata: [DONE]\n\n"

            return StreamingResponse(_workout_complete(), media_type="text/event-stream")

        # Initial exercise detection with detailed extraction
        else:
            # Extract exercises with details (sets/reps/duration if provided)
            # Use Celery wrapper for rate-limited OpenAI calls
            exercises_with_details = await extract_exercises_with_details_celery(client_id, text)

            if not exercises_with_details:
                error_msg = "I couldn't identify any exercises. Please tell me what exercises you did (e.g., 'pushups', 'squats', 'running')."
                await mem.add(client_id, "assistant", error_msg)

                async def _no_exercises():
                    yield f"data: {orjson.dumps({'message': error_msg, 'type': 'error'}).decode()}\n\n"
                    yield "event: done\ndata: [DONE]\n\n"

                return StreamingResponse(_no_exercises(), media_type="text/event-stream")

            # Process exercises and build initial exercises_data with what we already know
            exercises = []
            exercises_data = []
            needs_input = []  # Track which exercises need sets/reps/duration

            for idx, ex_detail in enumerate(exercises_with_details):
                exercise_name = ex_detail.get("exercise", "")
                exercises.append(exercise_name)

                # Set all exercises to moderate intensity by default
                normalized_intensity = 'moderate'

                # Check if it's a cardio exercise
                if is_cardio_exercise(exercise_name):
                    # Check if duration was provided
                    if ex_detail.get("has_duration") and ex_detail.get("duration_minutes"):
                        exercises_data.append({
                            'name': exercise_name,
                            'is_cardio': True,
                            'duration_minutes': ex_detail.get("duration_minutes"),
                            'intensity': normalized_intensity  # Include extracted intensity
                        })
                    else:
                        # Need to ask for duration
                        needs_input.append({"index": idx, "type": "duration", "exercise": exercise_name})
                        # Add to exercises_data with moderate intensity
                        exercises_data.append({
                            'name': exercise_name,
                            'is_cardio': True,
                            'intensity': 'moderate',
                            'needs_duration': True  # Flag that duration is still needed
                        })
                else:
                    # Regular exercise - check if sets/reps were provided
                    if ex_detail.get("has_sets_reps"):
                        sets = ex_detail.get("sets")
                        reps = ex_detail.get("reps")
                        has_duration = ex_detail.get("has_duration")
                        duration_minutes = ex_detail.get("duration_minutes")

                        # Check if exercise has both complete sets/reps AND complete info (with or without duration)
                        if sets and reps:
                            # Complete exercise - handle optional duration
                            exercise_data = {
                                'name': exercise_name,
                                'sets_reps_data': {"sets": sets, "reps": reps, "format": "uniform"},
                                'is_cardio': False,
                                'intensity': normalized_intensity
                            }

                            # Add duration if provided (strength exercises can have duration too!)
                            if has_duration and duration_minutes:
                                exercise_data['duration_minutes'] = duration_minutes

                            exercises_data.append(exercise_data)
                        # If we only have partial info, still need to ask
                        else:
                            needs_input.append({"index": idx, "type": "sets_reps", "exercise": exercise_name})
                            # Add to exercises_data with moderate intensity
                            exercises_data.append({
                                'name': exercise_name,
                                'sets_reps_data': None,
                                'is_cardio': False,
                                'intensity': 'moderate',
                                'needs_sets_reps': True
                            })
                    else:
                        # Need to ask for sets and reps
                        needs_input.append({"index": idx, "type": "sets_reps", "exercise": exercise_name})
                        # Still add to exercises_data with intensity if available
                        if normalized_intensity:
                            exercises_data.append({
                                'name': exercise_name,
                                'sets_reps_data': None,
                                'is_cardio': False,
                                'intensity': normalized_intensity,
                                'needs_sets_reps': True
                            })
                        else:
                            exercises_data.append({
                                'name': exercise_name,
                                'sets_reps_data': None,
                                'is_cardio': False,
                                'intensity': 'moderate',
                                'needs_sets_reps': True
                            })

                # Note: Intensity is optional, so we don't track it as a requirement
                # Only track sets/reps/duration as mandatory requirements

            # Check if all exercises have complete information
            if not needs_input:
                # All exercises have sets/reps/duration - check if we already have total duration
                # If any exercise has duration, use the maximum as total workout duration
                total_duration = None
                for exercise in exercises_data:
                    if exercise.get('duration_minutes'):
                        total_duration = max(total_duration or 0, exercise['duration_minutes'])

                if total_duration:
                    # Use the extracted duration as total workout duration and complete the workout
                    await mem.set_pending(client_id, None)

                    # Filter out incomplete exercises before creating workout
                    complete_exercises_data = [
                        ex for ex in exercises_data
                        if not ex.get('needs_sets_reps') and not ex.get('needs_duration')
                    ]

                    # Create and save workout
                    workout_response = create_workout_response(user_id, complete_exercises_data, current_weight, total_duration)
                    record_id = save_workout_to_database(db, user_id, workout_response['workout_details'])

                    # Calculate total calories
                    total_calories = 0
                    for muscle_group_data in workout_response['workout_details']:
                        for muscle_group, exercises_list in muscle_group_data.items():
                            for exercise in exercises_list:
                                for set_data in exercise['sets']:
                                    total_calories += set_data['calories']

                    success_message = f"✅ Workout logged successfully! Total duration: {total_duration} minutes, Estimated calories burned: {round(total_calories, 1)}"
                    await mem.add(client_id, "assistant", success_message)

                    async def _workout_complete():
                        workout_response['message'] = success_message
                        workout_response['type'] = 'workout_logged'
                        workout_response['status'] = 'logged'
                        workout_response['is_log'] = True
                        workout_response['total_duration_minutes'] = total_duration
                        workout_response['total_calories'] = round(total_calories, 1)
                        yield f"data: {orjson.dumps(workout_response).decode()}\n\n"
                        yield "event: done\ndata: [DONE]\n\n"

                    return StreamingResponse(_workout_complete(), media_type="text/event-stream")
                else:
                    # No duration provided, ask for total workout duration
                    duration_msg = "What was your total workout duration? (e.g., '45 minutes', '1 hour 30 minutes', or '90')"
                    await mem.add(client_id, "assistant", duration_msg)

                    await mem.set_pending(client_id, {
                        "state": "awaiting_duration",
                        "exercises_data": exercises_data,
                        "current_weight": current_weight
                    })

                    async def _ask_duration():
                        yield f"data: {orjson.dumps({'message': duration_msg, 'type': 'ask_duration'}).decode()}\n\n"
                        yield "event: done\ndata: [DONE]\n\n"

                    return StreamingResponse(_ask_duration(), media_type="text/event-stream")

            # If we still need sets/reps/duration, ask for those first

            # Otherwise, ask for the first missing information
            first_needed = needs_input[0]
            first_exercise = first_needed["exercise"]

            if first_needed["type"] == "duration":
                ask_message = f"Great! How long did you do {first_exercise}? (e.g., '30 minutes', '45', or '1 hour')"
            else:
                ask_message = f"Great! How many sets and reps did you do for {first_exercise}? (e.g., '3 sets 10 reps' or '3x10')"

            await mem.add(client_id, "assistant", ask_message)

            await mem.set_pending(client_id, {
                "state": "awaiting_sets_reps",
                "exercises": exercises,
                "current_exercise_index": first_needed["index"],
                "exercises_data": exercises_data,
                "needs_input": needs_input,
                "current_weight": current_weight
            })

            async def _ask_sets_reps():
                yield f"data: {orjson.dumps({'message': ask_message, 'type': 'ask_sets_reps'}).decode()}\n\n"
                yield "event: done\ndata: [DONE]\n\n"

            return StreamingResponse(_ask_sets_reps(), media_type="text/event-stream")

    except Exception as e:
        print(f"Error in workout logging: {e}")
        await mem.set_pending(client_id, None)

        error_msg = "Something went wrong. Please try again."
        await mem.add(client_id, "assistant", error_msg)

        async def _error():
            yield f"data: {orjson.dumps({'message': error_msg, 'type': 'error'}).decode()}\n\n"
            yield "event: done\ndata: [DONE]\n\n"

        return StreamingResponse(_error(), media_type="text/event-stream")

@router.get("/chat/stream")
# @router.get("/chat/stream", dependencies=[Depends(RateLimiter(times=30, seconds=60))])
async def workout_chat_stream(
    user_id: int,
    text: str = Query(None),
    fresh_start: bool = Query(False),  # Add this parameter
    mem = Depends(get_mem),
    oai=Depends(get_oai),
    db: Session = Depends(get_db),
):
    client_id = user_id
    try:
        # Get client profile for current weight
        profile = _fetch_profile(db, client_id)
        current_weight = profile.get('current_weight', 70.0)
        
        # Clear pending state if it's a fresh start or no text provided
        if not text or fresh_start:
            await mem.clear_pending(client_id)
        
        if not text:
            async def _welcome():
                welcome_msg = "Hello! I'm your workout logging assistant. What exercises did you do today?"
                yield f"data: {json.dumps({'message': welcome_msg, 'type': 'welcome'})}\n\n"
                yield "event: done\ndata: [DONE]\n\n"
            
            return StreamingResponse(_welcome(), media_type="text/event-stream",
                                headers={"Cache-Control":"no-cache","X-Accel-Buffering":"no"})
        
        text = text.strip()
        # print(f"DEBUG: Processing text: '{text}'")
        
        # Get pending state
        pending_state = await mem.get_pending(client_id)
        # print(f"DEBUG: Current pending state: {pending_state}")

        # Check for navigation confirmation state
        if pending_state and pending_state.get("state") == "awaiting_nav_confirm":
            if is_yes(text):
                await mem.clear_pending(client_id)
                async def _nav_yes():
                    yield sse_json({
                        "type": "nav",
                        "is_navigation": True,
                        "prompt": "Thanks for your confirmation. Redirecting to your workout logs"
                    })
                    yield "event: done\ndata: [DONE]\n\n"
                return StreamingResponse(_nav_yes(), media_type="text/event-stream",
                                        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})
            elif is_no(text):
                await mem.clear_pending(client_id)
                async def _nav_no():
                    yield sse_json({
                        "type": "nav",
                        "is_navigation": False
                        # Removed prompt to avoid disrupting the workflow
                    })
                    yield "event: done\ndata: [DONE]\n\n"
                return StreamingResponse(_nav_no(), media_type="text/event-stream",
                                        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})
            else:
                # Ask again with better guidance
                ask_message = "Would you like to view your workout logs? Please respond with 'yes' or 'no'"

                async def _ask_nav_confirm_again():
                    yield f"data: {json.dumps({'message': ask_message, 'type': 'ask_nav_confirm'})}\n\n"
                    yield "event: done\ndata: [DONE]\n\n"

                return StreamingResponse(_ask_nav_confirm_again(), media_type="text/event-stream",
                                        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})

        # Handle asking for sets and reps for each exercise
        elif pending_state and pending_state.get("state") == "awaiting_sets_reps":
            exercises = pending_state.get("exercises", [])
            current_index = pending_state.get("current_exercise_index", 0)
            exercises_data = pending_state.get("exercises_data", [])
            needs_input = pending_state.get("needs_input", [])

            # Find the current item that needs input
            current_need = None
            current_need_idx = None
            for idx, need in enumerate(needs_input):
                if need["index"] == current_index:
                    current_need = need
                    current_need_idx = idx
                    break

            if current_need:
                current_exercise = current_need["exercise"]

                # Check if current exercise needs duration (cardio)
                if current_need["type"] == "duration":
                    # Parse duration instead of sets/reps
                    duration_minutes = parse_cardio_duration(text)

                    if duration_minutes:
                        # Remove any existing incomplete entry for this exercise
                        exercises_data = [ex for ex in exercises_data if not (
                            ex.get('name') == current_exercise and
                            ex.get('needs_duration') is True
                        )]

                        # Store cardio data
                        exercise_data = {
                            'name': current_exercise,
                            'is_cardio': True,
                            'duration_minutes': duration_minutes,
                            'intensity': 'moderate'
                        }
                        exercises_data.append(exercise_data)

                        # Remove this item from needs_input
                        needs_input.pop(current_need_idx)

                        # Check if there are more exercises that need input
                        if needs_input:
                            next_need = needs_input[0]
                            next_exercise = next_need["exercise"]

                            if next_need["type"] == "duration":
                                ask_message = f"How long did you do {next_exercise}? (e.g., '30 minutes', '45', or '1 hour')"
                            else:
                                ask_message = f"How many sets and reps did you do for {next_exercise}? (e.g., '3 sets 10 reps' or '3x10')"

                            await mem.set_pending(client_id, {
                                "state": "awaiting_sets_reps",
                                "exercises": exercises,
                                "current_exercise_index": next_need["index"],
                                "exercises_data": exercises_data,
                                "needs_input": needs_input
                            })

                            async def _ask_next():
                                yield f"data: {json.dumps({'message': ask_message, 'type': 'ask_sets_reps'})}\n\n"
                                yield "event: done\ndata: [DONE]\n\n"

                            return StreamingResponse(_ask_next(), media_type="text/event-stream",
                                                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})
                        else:
                            # All exercises done, set default intensity and proceed to workout duration
                            # Set all exercises to moderate intensity by default
                            for exercise_data in exercises_data:
                                exercise_data['intensity'] = 'moderate'

                            # Ask for total workout duration
                            ask_message = "What was your total workout duration? (e.g., '45 minutes', '1 hour 30 minutes', or '90')"

                            await mem.set_pending(client_id, {
                                "state": "awaiting_duration",
                                "exercises_data": exercises_data
                            })

                            async def _ask_duration():
                                yield f"data: {json.dumps({'message': ask_message, 'type': 'ask_duration'})}\n\n"
                                yield "event: done\ndata: [DONE]\n\n"

                            return StreamingResponse(_ask_duration(), media_type="text/event-stream",
                                                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})
                    else:
                        # Ask for valid duration
                        ask_message = f"Please specify how long you did {current_exercise}. Examples: '30 minutes', '45', '1 hour', or '1:30'"

                        async def _ask_duration_again():
                            yield f"data: {json.dumps({'message': ask_message, 'type': 'ask_duration'})}\n\n"
                            yield "event: done\ndata: [DONE]\n\n"

                        return StreamingResponse(_ask_duration_again(), media_type="text/event-stream",
                                                headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})

                else:
                    # Handle regular exercises with sets/reps
                    # Use Celery wrapper for rate-limited OpenAI calls
                    sets_reps_data = await parse_sets_reps_celery(client_id, text, current_exercise)

                    if sets_reps_data:
                        # Remove any existing incomplete entry for this exercise
                        exercises_data = [ex for ex in exercises_data if not (
                            ex.get('name') == current_exercise and
                            ex.get('needs_sets_reps') is True
                        )]

                        exercise_data = {
                            'name': current_exercise,
                            'sets_reps_data': sets_reps_data,
                            'is_cardio': False,
                            'intensity': 'moderate'
                        }
                        exercises_data.append(exercise_data)

                        # Remove this item from needs_input
                        needs_input.pop(current_need_idx)

                        # Check if there are more exercises that need input
                        if needs_input:
                            next_need = needs_input[0]
                            next_exercise = next_need["exercise"]

                            if next_need["type"] == "duration":
                                ask_message = f"How long did you do {next_exercise}? (e.g., '30 minutes', '45', or '1 hour')"
                            else:
                                ask_message = f"How many sets and reps did you do for {next_exercise}? (e.g., '3 sets 10 reps' or '3x10')"

                            await mem.set_pending(client_id, {
                                "state": "awaiting_sets_reps",
                                "exercises": exercises,
                                "current_exercise_index": next_need["index"],
                                "exercises_data": exercises_data,
                                "needs_input": needs_input
                            })

                            async def _ask_next():
                                yield f"data: {json.dumps({'message': ask_message, 'type': 'ask_sets_reps'})}\n\n"
                                yield "event: done\ndata: [DONE]\n\n"

                            return StreamingResponse(_ask_next(), media_type="text/event-stream",
                                                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})
                        else:
                            # All exercises done, set default intensity and proceed to workout duration
                            # Set all exercises to moderate intensity by default
                            for exercise_data in exercises_data:
                                exercise_data['intensity'] = 'moderate'

                            # Ask for total workout duration
                            ask_message = "What was your total workout duration? (e.g., '45 minutes', '1 hour 30 minutes', or '90')"

                            await mem.set_pending(client_id, {
                                "state": "awaiting_duration",
                                "exercises_data": exercises_data
                            })

                            async def _ask_duration():
                                yield f"data: {json.dumps({'message': ask_message, 'type': 'ask_duration'})}\n\n"
                                yield "event: done\ndata: [DONE]\n\n"

                            return StreamingResponse(_ask_duration(), media_type="text/event-stream",
                                                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})
                    else:
                        # Ask again with better guidance
                        ask_message = f"Please specify sets and reps for {current_exercise}. For example: '3 sets 10 reps', '3x10', or '4 sets 12 reps'"

                        async def _ask_sets_reps_again():
                            yield f"data: {json.dumps({'message': ask_message, 'type': 'ask_sets_reps'})}\n\n"
                            yield "event: done\ndata: [DONE]\n\n"

                        return StreamingResponse(_ask_sets_reps_again(), media_type="text/event-stream",
                                                headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})
            else:
                # Fallback: if we can't find what we need, set default intensity and move to duration
                if exercises_data:
                    # Set all exercises to moderate intensity by default
                    for exercise_data in exercises_data:
                        exercise_data['intensity'] = 'moderate'

                    ask_message = "What was your total workout duration? (e.g., '45 minutes', '1 hour 30 minutes', or '90')"

                    await mem.set_pending(client_id, {
                        "state": "awaiting_duration",
                        "exercises_data": exercises_data
                    })

                    async def _ask_duration():
                        yield f"data: {json.dumps({'message': ask_message, 'type': 'ask_duration'})}\n\n"
                        yield "event: done\ndata: [DONE]\n\n"

                    return StreamingResponse(_ask_duration(), media_type="text/event-stream",
                                            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})
        
              
        # Handle duration input
        elif pending_state and pending_state.get("state") == "awaiting_duration":
            exercises_data = pending_state.get("exercises_data", [])
            
            duration_minutes = parse_duration(text)
            
            if duration_minutes:
                # Clear pending state and create final response
                await mem.clear_pending(client_id)

                # Filter out incomplete exercises before creating workout
                complete_exercises_data = [
                    ex for ex in exercises_data
                    if not ex.get('needs_sets_reps') and not ex.get('needs_duration')
                ]

                # Create workout log response
                workout_response = create_workout_response(client_id, complete_exercises_data, current_weight, duration_minutes)

                # Save workout using existing API
                try:
                    # print(f"DEBUG: Attempting to save workout for client_id: {client_id}")
                    # print(f"DEBUG: Workout data: {workout_response['workout_details']}")
                    record_id = save_workout_to_database(db, client_id, workout_response['workout_details'], workout_response.get('gym_id', 0))
                    if record_id:
                        # print(f"✅ Workout saved to database with record_id: {record_id}")
                        pass
                    else:
                        # print("❌ Failed to save workout to database")
                        pass
                except Exception as e:
                    print(f"❌ Error saving workout: {e}")
                    import traceback
                    print(f"Full traceback: {traceback.format_exc()}")

                # Calculate total calories burned
                total_calories = 0
                for muscle_group_data in workout_response['workout_details']:
                    for muscle_group, exercises in muscle_group_data.items():
                        for exercise in exercises:
                            for set_data in exercise['sets']:
                                total_calories += set_data['calories']

                success_message = f"✅ Workout logged successfully! Total duration: {duration_minutes} minutes, Estimated calories burned: {round(total_calories, 1)}"

                # Clear pending state to allow immediate next workout logging
                await mem.clear_pending(client_id)

                async def _workout_logged():
                    # Send single comprehensive workout logged response
                    workout_response['message'] = success_message
                    workout_response['type'] = 'workout_logged'
                    workout_response['status'] = 'logged'
                    workout_response['is_log'] = True
                    workout_response['total_duration_minutes'] = duration_minutes
                    workout_response['total_calories'] = round(total_calories, 1)

                    # Calculate total exercises for voice feedback
                    total_exercises = sum(
                        len(muscle_group_data.get('exercises', []))
                        for muscle_group_data in workout_response.get('workout_details', [])
                        for muscle_group_data in [muscle_group_data]
                    )

                    # Add total exercises to response for frontend voice feedback
                    workout_response['total_exercises'] = total_exercises

                    # Extract user_id for voice trigger (same pattern as food log)
                    try:
                        # Parse client_id to get user_id (same pattern as food log)
                        import re
                        match = re.search(r'user_(\d+)_.*', client_id)
                        if match:
                            user_id = int(match.group(1))

                            # Trigger voice notification asynchronously
                            await trigger_workout_log_success_voice(
                                user_id=user_id,
                                duration_minutes=duration_minutes,
                                total_calories=round(total_calories, 1),
                                exercises_count=total_exercises,
                                db=db
                            )
                    except Exception as e:
                        logger.error(f"[WORKOUT_VOICE_TRIGGER] Error triggering workout voice notification: {str(e)}")

                    yield sse_json(workout_response)
                    yield "event: done\ndata: [DONE]\n\n"

                return StreamingResponse(_workout_logged(), media_type="text/event-stream",
                                        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})
            else:
                # Ask for valid duration
                ask_message = "Please specify your workout duration. Examples: '45 minutes', '1 hour', '90', or '1:30'"
                
                async def _ask_duration_again():
                    yield f"data: {json.dumps({'message': ask_message, 'type': 'ask_duration'})}\n\n"
                    yield "event: done\ndata: [DONE]\n\n"
                
                return StreamingResponse(_ask_duration_again(), media_type="text/event-stream",
                                        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})
        
        # Initial exercise extraction
        else:
            if not is_exercise_related(text):
                async def _not_exercise():
                    response = "I'm here to help you log workouts. What exercises did you do today?"
                    yield f"data: {json.dumps({'message': response, 'type': 'response'})}\n\n"
                    yield "event: done\ndata: [DONE]\n\n"

                return StreamingResponse(_not_exercise(), media_type="text/event-stream",
                                        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})

            # Extract exercises with details (sets/reps/duration if provided)
            # Use Celery wrapper for rate-limited OpenAI calls
            exercises_with_details = await extract_exercises_with_details_celery(client_id, text)

            if not exercises_with_details:
                async def _no_exercises():
                    response = "I couldn't identify any exercises. Could you tell me what exercises you did? For example: 'push ups and squats' or 'bench press'"
                    yield f"data: {json.dumps({'message': response, 'type': 'response'})}\n\n"
                    yield "event: done\ndata: [DONE]\n\n"

                return StreamingResponse(_no_exercises(), media_type="text/event-stream",
                                        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})

            # Process exercises and build initial exercises_data with what we already know
            exercises = []
            exercises_data = []
            needs_input = []  # Track which exercises need sets/reps/duration

            for idx, ex_detail in enumerate(exercises_with_details):
                exercise_name = ex_detail.get("exercise", "")
                exercises.append(exercise_name)

                # Set all exercises to moderate intensity by default
                normalized_intensity = 'moderate'

                # Check if it's a cardio exercise
                if is_cardio_exercise(exercise_name):
                    # Check if duration was provided
                    if ex_detail.get("has_duration") and ex_detail.get("duration_minutes"):
                        exercises_data.append({
                            'name': exercise_name,
                            'is_cardio': True,
                            'duration_minutes': ex_detail.get("duration_minutes"),
                            'intensity': normalized_intensity  # Include extracted intensity
                        })
                    else:
                        # Need to ask for duration
                        needs_input.append({"index": idx, "type": "duration", "exercise": exercise_name})
                        # Add to exercises_data with moderate intensity
                        exercises_data.append({
                            'name': exercise_name,
                            'is_cardio': True,
                            'intensity': 'moderate',
                            'needs_duration': True  # Flag that duration is still needed
                        })
                else:
                    # Regular exercise - check if sets/reps were provided
                    if ex_detail.get("has_sets_reps"):
                        sets = ex_detail.get("sets")
                        reps = ex_detail.get("reps")
                        has_duration = ex_detail.get("has_duration")
                        duration_minutes = ex_detail.get("duration_minutes")

                        # Check if exercise has both complete sets/reps AND complete info (with or without duration)
                        if sets and reps:
                            # Complete exercise - handle optional duration
                            exercise_data = {
                                'name': exercise_name,
                                'sets_reps_data': {"sets": sets, "reps": reps, "format": "uniform"},
                                'is_cardio': False,
                                'intensity': normalized_intensity
                            }

                            # Add duration if provided (strength exercises can have duration too!)
                            if has_duration and duration_minutes:
                                exercise_data['duration_minutes'] = duration_minutes

                            exercises_data.append(exercise_data)
                        # If we only have partial info, still need to ask
                        else:
                            needs_input.append({"index": idx, "type": "sets_reps", "exercise": exercise_name})
                            # Add to exercises_data with moderate intensity
                            exercises_data.append({
                                'name': exercise_name,
                                'sets_reps_data': None,
                                'is_cardio': False,
                                'intensity': 'moderate',
                                'needs_sets_reps': True
                            })
                    else:
                        # Need to ask for sets and reps
                        needs_input.append({"index": idx, "type": "sets_reps", "exercise": exercise_name})
                        # Still add to exercises_data with intensity if available
                        if normalized_intensity:
                            exercises_data.append({
                                'name': exercise_name,
                                'sets_reps_data': None,
                                'is_cardio': False,
                                'intensity': normalized_intensity,
                                'needs_sets_reps': True
                            })
                        else:
                            exercises_data.append({
                                'name': exercise_name,
                                'sets_reps_data': None,
                                'is_cardio': False,
                                'intensity': 'moderate',
                                'needs_sets_reps': True
                            })

                # Note: Intensity is optional, so we don't track it as a requirement
                # Only track sets/reps/duration as mandatory requirements

            # Check if all exercises have complete information
            if not needs_input:
                # All exercises have sets/reps/duration - check if we already have total duration
                # If any exercise has duration, use the maximum as total workout duration
                total_duration = None
                for exercise in exercises_data:
                    if exercise.get('duration_minutes'):
                        total_duration = max(total_duration or 0, exercise['duration_minutes'])

                if total_duration:
                    # Use the extracted duration as total workout duration and complete the workout
                    await mem.set_pending(client_id, None)

                    # Filter out incomplete exercises before creating workout
                    complete_exercises_data = [
                        ex for ex in exercises_data
                        if not ex.get('needs_sets_reps') and not ex.get('needs_duration')
                    ]

                    # Create and save workout
                    workout_response = create_workout_response(user_id, complete_exercises_data, current_weight, total_duration)
                    record_id = save_workout_to_database(db, user_id, workout_response['workout_details'])

                    # Calculate total calories
                    total_calories = 0
                    for muscle_group_data in workout_response['workout_details']:
                        for muscle_group, exercises_list in muscle_group_data.items():
                            for exercise in exercises_list:
                                for set_data in exercise['sets']:
                                    total_calories += set_data['calories']

                    success_message = f"✅ Workout logged successfully! Total duration: {total_duration} minutes, Estimated calories burned: {round(total_calories, 1)}"
                    await mem.add(client_id, "assistant", success_message)

                    async def _workout_complete():
                        workout_response['message'] = success_message
                        workout_response['type'] = 'workout_logged'
                        workout_response['status'] = 'logged'
                        workout_response['is_log'] = True
                        workout_response['total_duration_minutes'] = total_duration
                        workout_response['total_calories'] = round(total_calories, 1)
                        yield f"data: {json.dumps(workout_response)}\n\n"
                        yield "event: done\ndata: [DONE]\n\n"

                    return StreamingResponse(_workout_complete(), media_type="text/event-stream",
                                            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})
                else:
                    # No duration provided, ask for total workout duration
                    duration_msg = "What was your total workout duration? (e.g., '45 minutes', '1 hour 30 minutes', or '90')"
                    await mem.add(client_id, "assistant", duration_msg)

                    await mem.set_pending(client_id, {
                        "state": "awaiting_duration",
                        "exercises_data": exercises_data,
                        "current_weight": current_weight
                    })

                    async def _ask_duration():
                        yield f"data: {json.dumps({'message': duration_msg, 'type': 'ask_duration'})}\n\n"
                        yield "event: done\ndata: [DONE]\n\n"

                    return StreamingResponse(_ask_duration(), media_type="text/event-stream",
                                            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})

            # If we still need sets/reps/duration, ask for those first

            # Otherwise, ask for the first missing information
            first_needed = needs_input[0]
            first_exercise = first_needed["exercise"]

            if first_needed["type"] == "duration":
                ask_message = f"Great! How long did you do {first_exercise}? (e.g., '30 minutes', '45', or '1 hour')"
            else:
                ask_message = f"Great! How many sets and reps did you do for {first_exercise}? (e.g., '3 sets 10 reps' or '3x10')"

            await mem.add(client_id, "assistant", ask_message)

            await mem.set_pending(client_id, {
                "state": "awaiting_sets_reps",
                "exercises": exercises,
                "current_exercise_index": first_needed["index"],
                "exercises_data": exercises_data,
                "needs_input": needs_input,
                "current_weight": current_weight
            })

            async def _ask_sets_reps():
                yield f"data: {json.dumps({'message': ask_message, 'type': 'ask_sets_reps'})}\n\n"
                yield "event: done\ndata: [DONE]\n\n"

            return StreamingResponse(_ask_sets_reps(), media_type="text/event-stream",
                                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})
    
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        print(f"Full traceback: {traceback.format_exc()}")
        
        try:
            await mem.clear_pending(client_id)
        except:
            pass
        
        async def _error():
            yield f"data: {json.dumps({'message': 'Sorry, I encountered an error. Please try again.', 'type': 'error'})}\n\n"
            yield "event: done\ndata: [DONE]\n\n"
        
        return StreamingResponse(_error(), media_type="text/event-stream",
                                headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


def _fetch_profile(db: Session, client_id: int):
    """Fetch complete client profile including weight journey and calorie targets"""
    try:
        # Get latest weight journey
        from app.models.fittbot_models import WeightJourney,Client,ClientTarget
        w = (
            db.query(WeightJourney)
            .where(WeightJourney.client_id == client_id)
            .order_by(WeightJourney.id.desc())
            .first()
        )
       
        current_weight = float(w.actual_weight) if w and w.actual_weight is not None else 70.0
        target_weight = float(w.target_weight) if w and w.target_weight is not None else 65.0
       
        weight_delta_text = None
        goal_type = "maintain"
       
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
       
        # Get client details
        c = db.query(Client).where(Client.client_id == client_id).first()
        client_goal = (getattr(c, "goals", None) or getattr(c, "goal", None) or "muscle gain") if c else "muscle gain"
        lifestyle = c.lifestyle if c else "moderate"
       
        # Get calorie target
        ct = db.query(ClientTarget).where(ClientTarget.client_id == client_id).first()
        target_calories = float(ct.calories) if ct and ct.calories else 2000.0
       
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
        print(f"Error fetching profile: {e}")
        return {
            "client_id": client_id,
            "current_weight": 70.0,
            "target_weight": 65.0,
            "weight_delta_text": "Maintain 70.0 kg",
            "client_goal": "muscle gain",
            "goal_type": "maintain",
            "target_calories": 2000.0,
            "lifestyle": "moderate",
            "days_per_week": 6,
        }


class ClientId(BaseModel):
    client_id: int


@router.get("/debug_pending")
async def debug_pending(
    client_id: int,
    mem = Depends(get_mem),
):
    """Debug endpoint to check pending state"""
    pending_state = await mem.get_pending(client_id)
    return {"client_id": client_id, "pending_state": pending_state}


@router.post("/clear_pending")
async def clear_pending_state(
    req: ClientId,
    mem = Depends(get_mem),
):
    """Clear pending state for a client"""
    await mem.clear_pending(req.client_id)
    return {"status": "cleared", "client_id": req.client_id}


@router.post("/delete_chat")
async def chat_close(
    req: ClientId,
    mem = Depends(get_mem),
):
    """Delete chat history for a client"""
    print(f"Deleting chat history for client {req.client_id}")
    history_key = f"workout_chat:{req.client_id}:history"
    pending_key = f"workout_chat:{req.client_id}:pending"
    deleted = await mem.r.delete(history_key, pending_key)
    return {"status": 200, "deleted_keys": deleted}


@router.get("/get_workouts/{client_id}")
async def get_stored_workouts(
    client_id: int,
    limit: int = 10,
    db: Session = Depends(get_db),
):
    """Get stored workouts for a client (for testing purposes)"""
    try:
        workouts = (
            db.query(ActualWorkout)
            .filter(ActualWorkout.client_id == client_id)
            .order_by(ActualWorkout.date.desc())
            .limit(limit)
            .all()
        )

        result = []
        for workout in workouts:
            result.append({
                "record_id": workout.record_id,
                "client_id": workout.client_id,
                "date": workout.date.isoformat() if workout.date else None,
                "workout_details": workout.workout_details
            })

        return {"status": "success", "workouts": result, "count": len(result)}

    except Exception as e:
        print(f"Error retrieving workouts: {e}")
        return {"status": "error", "message": str(e)}