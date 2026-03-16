# chatbot_services/analysis_helpers.py
from __future__ import annotations

import re, ast, orjson, json
from typing import Any, Dict, List, Optional
from datetime import datetime, timedelta, date

from sqlalchemy.orm import Session
from sqlalchemy import func

from fastapi import APIRouter, HTTPException, UploadFile, File, Query, Depends
from fastapi.responses import StreamingResponse
from fastapi_limiter.depends import RateLimiter

# models
from app.models.fittbot_models import (
    WeightJourney,
    ClientTarget,
    ClientActualAggregatedWeekly,
    ActualDiet,
    ActualWorkout,
    AggregatedInsights,
    MuscleAggregatedInsights,
)

# reused LLM/config helpers
from app.fittbot_api.v1.client.client_api.chatbot.chatbot_services.llm_helpers import (
    GENERAL_SYSTEM, STYLE_CHAT_FORMAT, OPENAI_MODEL, sse_json
)

# ASR for voice transcription
from app.fittbot_api.v1.client.client_api.chatbot.chatbot_services.asr import transcribe_audio

# Voice preference helper
from app.fittbot_api.v1.client.client_api.chatbot.codes.food_log import get_voice_preference

# ===== public constants/styles (used by the route) =====

STYLE_INSIGHT_REPORT = (
    "You are KyraAI, a caring fitness coach. Build ONE beautifully formatted, mobile-friendly report from the JSON dataset.\n"
    "Tone: warm, concise, encouraging. Use emojis generously to make it visually appealing and easy to scan.\n"
    "\n"
    "CRITICAL FORMATTING RULES:\n"
    "- NO markdown syntax whatsoever (no **, no ##, no _, no bold, no italics, no tables)\n"
    "- Use emojis for visual hierarchy and engagement\n"
    "- MANDATORY: Add TWO blank lines between major sections for visual breathing room\n"
    "- Add ONE blank line between subsections within a section\n"
    "- Use simple bullets with • or relevant emojis\n"
    "- Keep lines short for mobile readability (max 50-60 chars per line when possible)\n"
    "- Add proper indentation (2-4 spaces) for nested content\n"
    "\n"
    "REPORT STRUCTURE (follow this EXACT spacing):\n"
    "\n"
    "1) GREETING (1-2 lines)\n"
    "   Start with 👋 and mention any recent foods/exercises if present\n"
    "   Example:\n"
    "   '👋 Hey there!\n"
    "   I see you've been enjoying idli and hitting the gym - nice!'\n"
    "   [ADD 2 BLANK LINES AFTER]\n"
    "\n"
    "2) WEIGHT SNAPSHOT (if target set, skip if no target)\n"
    "   Use ⚖️ emoji as section header\n"
    "   IMPORTANT: Always use dataset.current_weight for Current weight and dataset.target_weight for Target weight\n"
    "   NEVER use avg_weight from weekly data for the weight snapshot section\n"
    "   Example:\n"
    "   '⚖️ Weight Journey\n"
    "   \n"
    "   Current: 75kg\n"
    "   Target: 70kg\n"
    "   To go: 5kg 🎯'\n"
    "   [ADD 2 BLANK LINES AFTER]\n"
    "\n"
    "3) MACROS STORY (main nutrition section)\n"
    "   Start with section header: '📊 Nutrition Check-in'\n"
    "   [1 BLANK LINE]\n"
    "   Then for EACH macro (Calories, Protein, Carbs, Fat) use:\n"
    "   - Emoji: 🔥 Calories, 💪 Protein, 🍚 Carbs, 🥑 Fat\n"
    "   - Show Target | Avg on one line\n"
    "   - Visual bar (10 chars using █ and ░) with percentage and status emoji:\n"
    "     ✅ (90-110% on track), ⚠️ (75-89% or 111-125% off), 🔴 (<75% or >125% very off)\n"
    "   - One friendly sentence explaining the gap\n"
    "   - [1 BLANK LINE between each macro]\n"
    "   Example:\n"
    "   '📊 Nutrition Check-in\n"
    "   \n"
    "   🔥 Calories\n"
    "   Target: 2000 | Avg: 1850\n"
    "   ████████░░ 92% ✅\n"
    "   About one snack short each day\n"
    "   \n\n"
    "   💪 Protein\n"
    "   Target: 150g | Avg: 145g\n"
    "   █████████░ 97% ✅\n"
    "   You're nailing this!'\n"
    "   [ADD 2 BLANK LINES AFTER]\n"
    "\n"
    "4) TRAINING STORY\n"
    "   Start with: '🏋️ Training Highlights'\n"
    "   [1 BLANK LINE]\n"
    "   Show top 2-3 muscle groups with trend emojis:\n"
    "   📈 (increasing), ➡️ (stable/maintaining), 📉 (decreasing)\n"
    "   Example:\n"
    "   '🏋️ Training Highlights\n"
    "   \n"
    "   • Chest: 12,500kg 📈 +8% from last week\n"
    "   • Back: 10,200kg ➡️ holding steady\n"
    "   • Legs: 8,000kg 📉 dropped a bit\n"
    "   \n"
    "   Your upper body is crushing it!'\n"
    "   [ADD 2 BLANK LINES AFTER]\n"
    "\n"
    "5) QUICK WINS (3 items)\n"
    "   Start with: '✨ What's Working'\n"
    "   [1 BLANK LINE]\n"
    "   List 3 wins from their actual data\n"
    "   Example:\n"
    "   '✨ What's Working\n"
    "   \n"
    "   • Your protein at breakfast is on point!\n"
    "   • Consistent gym sessions this week\n"
    "   • Great variety in your meals'\n"
    "   [ADD 2 BLANK LINES AFTER]\n"
    "\n"
    "6) THIS WEEK'S FOCUS (3-5 items)\n"
    "   Start with: '🎯 This Week's Focus'\n"
    "   [1 BLANK LINE]\n"
    "   Very specific, quantified actions with emojis\n"
    "   Example:\n"
    "   '🎯 This Week's Focus\n"
    "   \n"
    "   • Add 20-30g protein at lunch (paneer/eggs/chicken)\n"
    "   • Hit 3 leg sessions with 12-15 reps each\n"
    "   • Bump up calories by 150-200 (add 1 banana or handful nuts)\n"
    "   • Stay hydrated: 3L water daily\n"
    "   • Get 7-8 hours sleep for recovery'\n"
    "   [ADD 2 BLANK LINES AFTER]\n"
    "\n"
    "7) MICRO-HABITS (2-3 items)\n"
    "   Start with: '🌱 Tiny Habits (Easy Wins)'\n"
    "   [1 BLANK LINE]\n"
    "   Super small, easy actions\n"
    "   Example:\n"
    "   '🌱 Tiny Habits (Easy Wins)\n"
    "   \n"
    "   • Drink water right after waking up\n"
    "   • Keep protein snacks visible (eggs/paneer in fridge front)'\n"
    "   [ADD 2 BLANK LINES AFTER]\n"
    "\n"
    "8) OVERALL SUMMARY\n"
    "   Start with: '📋 Your 3-Step Action Plan'\n"
    "   [1 BLANK LINE]\n"
    "   Exactly 3 bullets:\n"
    "   1. Diet action (specific quantities)\n"
    "   2. Workout action (frequency/sets/reps, focus on weak muscle groups)\n"
    "   3. One personalized data-driven tip from their recent foods/workouts/macros\n"
    "   Example:\n"
    "   '📋 Your 3-Step Action Plan\n"
    "   \n"
    "   • Nutrition: Add 150g paneer or 4 eggs daily to hit protein\n"
    "   • Training: Hit legs 2x/week - 3 sets x 12 reps squats & lunges\n"
    "   • Keep it up: Your evening snack timing is perfect for recovery!'\n"
    "   [ADD 2 BLANK LINES AFTER]\n"
    "\n"
    "9) CLOSING\n"
    "   One line with 💙, short and motivating\n"
    "   Example:\n"
    "   '💙 I'm with you every step - let's make this week count!'\n"
    "\n"
    "ADDITIONAL GUIDELINES:\n"
    "- If data is missing or extreme/erroneous, skip that section gracefully\n"
    "- Use local Indian food examples: idli, dosa, chapati, paratha, paneer, eggs, dal, sambar, rice, curd\n"
    "- Keep total report under 400 words (but don't sacrifice clarity)\n"
    "- SPACING IS CRITICAL: 2 blank lines between sections, 1 blank line within sections\n"
    "- Make it feel personal and data-driven, never generic\n"
    "- Numbers should be easy to scan (use | separators, clear labels)\n"
)

# ===== mode + artifacts (public) =====

ANALYSIS_MODE_TTL = 60 * 60  # 1h

async def set_mode(mem, user_id: int, mode: Optional[str], ttl: int = ANALYSIS_MODE_TTL):
    key = f"chat:{user_id}:mode"
    if mode:
        await mem.r.set(key, mode, ex=ttl)
    else:
        await mem.r.delete(key)

async def get_mode(mem, user_id: int) -> str:
    v = await mem.r.get(f"chat:{user_id}:mode")
    return v.decode() if isinstance(v, (bytes, bytearray)) else (v or "")

async def set_analysis_artifacts(mem, user_id: int, dataset: Optional[dict], summary: Optional[str]):
    if dataset is not None:
        await mem.r.set(f"analysis:{user_id}:dataset", orjson.dumps(dataset), ex=7*24*3600)
    if summary is not None:
        await mem.r.set(f"analysis:{user_id}:summary", summary, ex=7*24*3600)

async def get_analysis_artifacts(mem, user_id: int):
    dj = await mem.r.get(f"analysis:{user_id}:dataset")
    sj = await mem.r.get(f"analysis:{user_id}:summary")
    dataset = orjson.loads(dj) if dj else None
    summary = sj.decode() if isinstance(sj, (bytes, bytearray)) else (sj or None)
    return dataset, summary

# ===== light intent + follow-up detector (public) =====

def is_analysis_intent(t: str) -> bool:
    lt = (t or "").strip().lower()

    # Common phrases that indicate user wants a fresh analysis report
    analysis_phrases = [
        "analyse", "analyze",
        "report", "my report", "give my", "generate report",
        "fitness report", "summary", "my summary",
        "progress report", "my progress", "how am i doing",
        "check my", "review my"
    ]

    return any(lt.startswith(phrase) or f" {phrase}" in lt for phrase in analysis_phrases)

_ANALYSIS_FOLLOWUP_HINTS = {
    "why","how","increase","decrease","raise","lower","fix","improve",
    "protein","calories","carbs","fat","macro","adherence","target",
    "volume","sets","reps","rest","muscle","back","chest","legs","shoulder","biceps","cardio",
}
def is_followup_question(text: str) -> bool:
    t = (text or "").lower()
    return any(k in t for k in _ANALYSIS_FOLLOWUP_HINTS)

# ===== small utils (internal) =====

def _to_iso(v) -> Optional[str]:
    if v is None: return None
    try:
        if isinstance(v, str):
            return v if "T" in v or "-" in v else str(v)
        return v.isoformat()
    except Exception:
        return str(v)

def _mean(vals: List[Optional[float]]) -> Optional[float]:
    xs = [float(v) for v in vals if isinstance(v, (int, float))]
    return round(sum(xs)/len(xs), 2) if xs else None

def _pct(actual: Optional[float], target: Optional[float]) -> Optional[float]:
    if actual is None or not target or float(target) == 0.0:
        return None
    return round(100.0 * float(actual) / float(target), 1)

def _safe_json_load(s):
    try:
        if s is None: return []
        if isinstance(s, (list, dict)): return s
        if isinstance(s, (bytes, bytearray)): s = s.decode("utf-8", "ignore")
        s = s.strip()
        if not s: return []
        try:
            return orjson.loads(s)
        except Exception:
            pass
        try:
            s_py = s.replace("true","True").replace("false","False").replace("null","None")
            obj = ast.literal_eval(s_py)
            if isinstance(obj, (list, dict)): return obj
        except Exception:
            pass
        s_fix = re.sub(r"(?<!\KATEX_INLINE_CLOSE'", '"', s).replace("None","null").replace("True","true").replace("False","false")
        return orjson.loads(s_fix)
    except Exception:
        return []

def _as_list(x):
    if isinstance(x, list): return x
    if isinstance(x, dict): return [x]
    return []

def sse_data(content: str) -> str:
    """
    Properly format content for SSE transmission with UTF-8 Unicode support.
    SSE requires 'data: ' prefix and double newline. Content is sent as plain UTF-8.
    Skips empty content to avoid sending blank messages.
    """
    if isinstance(content, bytes):
        content = content.decode('utf-8', errors='replace')

    # Skip empty or whitespace-only content
    if not content or not content.strip():
        return ""

    lines = content.split('\n')
    if len(lines) == 1:
        return f"data: {content}\n\n"
    else:
        return ''.join(f"data: {line}\n" for line in lines) + "\n"

# ===== date range helpers (public) =====

def get_default_date_range() -> tuple[date, date]:
    """
    Returns the default date range: last 4 weeks from today.
    Returns (start_date, end_date) as date objects.
    """
    today = date.today()
    four_weeks_ago = today - timedelta(weeks=4)
    return four_weeks_ago, today

def format_date_range(start: date, end: date) -> str:
    """Format date range for display."""
    return f"{start.strftime('%b %d, %Y')} to {end.strftime('%b %d, %Y')}"

def validate_date_range(start: date, end: date) -> tuple[bool, Optional[str]]:
    """
    Validate date range.
    Returns (is_valid, error_message).
    """
    if start > end:
        return False, "Start date cannot be after end date"

    if end > date.today():
        return False, "End date cannot be in the future"

    days_diff = (end - start).days
    if days_diff > 365:
        return False, "Date range cannot exceed 1 year (365 days)"

    if days_diff < 1:
        return False, "Date range must be at least 1 day"

    return True, None

# ===== dataset builder (public) =====

def build_analysis_dataset_dict(
    db: Session,
    client_id: int,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None
) -> Dict[str, Any]:
    # Use default date range if not provided (last 4 weeks)
    if start_date is None or end_date is None:
        start_date, end_date = get_default_date_range()

    # 1) weight
    w = (
        db.query(WeightJourney)
        .filter(WeightJourney.client_id == client_id)
        .order_by(WeightJourney.id.desc())
        .first()
    )
    current_weight = float(w.actual_weight) if w and w.actual_weight is not None else None
    target_weight  = float(w.target_weight) if w and w.target_weight is not None else None

    # 2) targets (handle NULLs properly for MySQL)
    t = (
        db.query(ClientTarget)
        .filter(ClientTarget.client_id == client_id)
        .order_by(
            (ClientTarget.updated_at.is_(None)).asc(),
            ClientTarget.updated_at.desc(),
            ClientTarget.target_id.desc(),
        )
        .first()
    )
    t = t or object()
    targets = {
    "calories": float(getattr(t,"calories",None)) if getattr(t,"calories",None) is not None else None,
    "protein":  float(getattr(t,"protein",None))  if getattr(t,"protein",None)  is not None else None,
    "carbs":    float(getattr(t,"carbs",None))    if getattr(t,"carbs",None)    is not None else None,
    "fat":      float(getattr(t,"fat",None))      if getattr(t,"fat",None)      is not None else None,
    "steps":    float(getattr(t,"steps",None))    if getattr(t,"steps",None)    is not None else None,
    "calories_to_burn": float(getattr(t,"calories_to_burn",None)) if getattr(t,"calories_to_burn",None) is not None else None,
    "water_intake":     float(getattr(t,"water_intake",None))     if getattr(t,"water_intake",None)     is not None else None,
    "sleep_hours":      float(getattr(t,"sleep_hours",None))      if getattr(t,"sleep_hours",None)      is not None else None,
    "updated_at": _to_iso(getattr(t,"updated_at",None)),
}

    # 3) weekly macros (within date range)
    weekly_rows = (
        db.query(ClientActualAggregatedWeekly)
        .filter(
            ClientActualAggregatedWeekly.client_id == client_id,
            ClientActualAggregatedWeekly.week_start >= start_date,
            ClientActualAggregatedWeekly.week_start <= end_date
        )
        .order_by(ClientActualAggregatedWeekly.week_start.desc())
        .all()
    )
    weekly_macros: List[Dict[str, Any]] = []
    for r in weekly_rows:
        weekly_macros.append({
            "week": _to_iso(r.week_start),
            "avg_calories": float(r.avg_calories) if r.avg_calories is not None else None,
            "avg_protein":  float(r.avg_protein)  if r.avg_protein  is not None else None,
            "avg_carbs":    float(r.avg_carbs)    if r.avg_carbs    is not None else None,
            "avg_fat":      float(r.avg_fats)     if r.avg_fats     is not None else None,
            "avg_weekly_weight": float(r.avg_weight)   if r.avg_weight   is not None else None,  # Renamed to avoid confusion with current_weight
            "total_steps":  float(r.total_steps)  if r.total_steps  is not None else None,
            "total_burnt_calories": float(r.total_burnt_calories) if r.total_burnt_calories is not None else None,
        })
    avg_actual = {
        "avg_calories": _mean([w.get("avg_calories") for w in weekly_macros]),
        "avg_protein":  _mean([w.get("avg_protein")  for w in weekly_macros]),
        "avg_carbs":    _mean([w.get("avg_carbs")    for w in weekly_macros]),
        "avg_fat":      _mean([w.get("avg_fat")      for w in weekly_macros]),
        # Note: avg_weight removed to avoid confusion with current_weight
    }

    print(f"[REPORT_ANALYSIS] DEBUG: Weekly macros found: {len(weekly_macros)} weeks")
    print(f"[REPORT_ANALYSIS] DEBUG: Averages from weekly data: {avg_actual}")

    # Fallback: Calculate from ActualDiet records if weekly data is insufficient
    # More aggressive fallback: trigger if most averages are None or if no weekly data exists
    none_count = sum(1 for v in avg_actual.values() if v is None)
    has_fallback_macros = False  # Track if we successfully got macros from ActualDiet

    if none_count >= 3 or len(weekly_macros) == 0:
        print(f"[REPORT_ANALYSIS] Weekly data insufficient, calculating from ActualDiet for user {client_id}")
        print(f"[REPORT_ANALYSIS] DEBUG: Searching for ActualDiet data from {start_date} to {end_date}")

        daily_macros = []
        diet_rows = (
            db.query(ActualDiet)
            .filter(
                ActualDiet.client_id == client_id,
                ActualDiet.date >= start_date,
                ActualDiet.date <= end_date,
                ActualDiet.diet_data.isnot(None)
            )
            .all()
        )

        print(f"[REPORT_ANALYSIS] DEBUG: Found {len(diet_rows)} ActualDiet records for date range")

        for record in diet_rows:
            print(f"[REPORT_ANALYSIS] DEBUG: Processing ActualDiet record for date: {record.date}")
            if record.diet_data:
                try:
                    diet_items = _as_list(_safe_json_load(record.diet_data))
                    print(f"[REPORT_ANALYSIS] DEBUG: Found {len(diet_items)} diet items for {record.date}")
                    daily_totals = {"calories": 0, "protein": 0, "carbs": 0, "fat": 0}

                    for i, item in enumerate(diet_items):
                        if isinstance(item, dict):
                            print(f"[REPORT_ANALYSIS] DEBUG: Diet item {i} keys: {list(item.keys())}")
                            print(f"[REPORT_ANALYSIS] DEBUG: Diet item {i} title: {item.get('title', 'N/A')}")

                            # The actual food items are in the 'foodList' array
                            food_list = item.get("foodList", [])
                            print(f"[REPORT_ANALYSIS] DEBUG: Food items in {item.get('title', 'N/A')}: {len(food_list)} items")

                            for food_item in food_list:
                                if isinstance(food_item, dict):
                                    # Extract macros from the food item
                                    calories = float(food_item.get("calories", 0))
                                    protein = float(food_item.get("protein", 0))
                                    carbs = float(food_item.get("carbs", 0))
                                    fat = float(food_item.get("fat", 0))

                                    daily_totals["calories"] += calories
                                    daily_totals["protein"]  += protein
                                    daily_totals["carbs"]    += carbs
                                    daily_totals["fat"]      += fat

                                    print(f"[REPORT_ANALYSIS] DEBUG: Added {food_item.get('name', 'unknown')}: {calories} cal, {protein}g protein, {carbs}g carbs, {fat}g fat")

                    print(f"[REPORT_ANALYSIS] DEBUG: Daily totals for {record.date}: {daily_totals}")
                    if daily_totals["calories"] > 0:  # Only include days with actual data
                        daily_macros.append(daily_totals)
                except Exception as e:
                    print(f"[REPORT_ANALYSIS] Error processing diet data for {record.date}: {e}")
                    import traceback
                    print(f"[REPORT_ANALYSIS] Traceback: {traceback.format_exc()}")
                    continue

        # Calculate averages from daily data
        if daily_macros:
            avg_actual = {
                "avg_calories": _mean([d["calories"] for d in daily_macros]),
                "avg_protein":  _mean([d["protein"]  for d in daily_macros]),
                "avg_carbs":    _mean([d["carbs"]    for d in daily_macros]),
                "avg_fat":      _mean([d["fat"]      for d in daily_macros]),
            }
            has_fallback_macros = True  # Mark that we successfully calculated macros from fallback
            print(f"[REPORT_ANALYSIS] SUCCESS: Calculated averages from {len(daily_macros)} days of ActualDiet data: {avg_actual}")
        else:
            print(f"[REPORT_ANALYSIS] ERROR: No valid daily_macros found for user {client_id} in date range")

    adherence = {
        "calories_pct": _pct(avg_actual["avg_calories"], targets["calories"]),
        "protein_pct":  _pct(avg_actual["avg_protein"],  targets["protein"]),
        "carbs_pct":    _pct(avg_actual["avg_carbs"],    targets["carbs"]),
        "fat_pct":      _pct(avg_actual["avg_fat"],      targets["fat"]),
    }
    macros = {"targets": targets, "weeks": weekly_macros, "avg_actual": avg_actual, "adherence_pct": adherence}

    # 4) last 2 non-empty diet days (within date range)
    diet_rows = (
        db.query(ActualDiet)
        .filter(
            ActualDiet.client_id == client_id,
            ActualDiet.date >= start_date,
            ActualDiet.date <= end_date
        )
        .order_by(ActualDiet.date.desc())
        .all()
    )
    last7_foods = []
    for r in diet_rows:
        items = _as_list(_safe_json_load(r.diet_data))
        if items:
            last7_foods.append({"date": _to_iso(r.date), "items": items})
        if len(last7_foods) >= 2: break
    if not last7_foods:
        last7_foods = [{"date": _to_iso(r.date), "items": []} for r in diet_rows[:2]]

    # 5) weekly workout volume (within date range)
    agg_rows = (
        db.query(AggregatedInsights)
        .filter(
            AggregatedInsights.client_id == client_id,
            AggregatedInsights.week_start >= start_date,
            AggregatedInsights.week_start <= end_date
        )
        .order_by(AggregatedInsights.week_start.desc())
        .all()
    )
    print(f"[REPORT_ANALYSIS] DEBUG: Found {len(agg_rows)} weekly workout records for date range {start_date} to {end_date}")

    weekly_workouts = {
        "weeks": [
            {
                "week": _to_iso(r.week_start),
                "total_volume": float(r.total_volume) if r.total_volume is not None else None,
                "avg_workout_weight": float(r.avg_weight)   if r.avg_weight   is not None else None,  # Renamed to avoid confusion
                "avg_reps":     float(r.avg_reps)     if r.avg_reps     is not None else None,
            } for r in agg_rows
        ]
    }

    # Fallback: Calculate from actual workout records if weekly data is insufficient
    if len(agg_rows) == 0:
        print(f"[REPORT_ANALYSIS] No weekly workout data found, looking for actual workout records")
        print(f"[REPORT_ANALYSIS] DEBUG: Searching for ActualWorkout data from {start_date} to {end_date}")

        workout_rows = (
            db.query(ActualWorkout)
            .filter(
                ActualWorkout.client_id == client_id,
                ActualWorkout.date >= start_date,
                ActualWorkout.date <= end_date,
                ActualWorkout.workout_details.isnot(None)
            )
            .order_by(ActualWorkout.date.desc())
            .all()
        )

        print(f"[REPORT_ANALYSIS] DEBUG: Found {len(workout_rows)} ActualWorkout records for date range")

        total_volume = 0
        total_weight = 0
        total_reps = 0
        workout_count = 0

        for record in workout_rows:
            print(f"[REPORT_ANALYSIS] DEBUG: Processing ActualWorkout record for date: {record.date}")
            if record.workout_details:
                try:
                    workout_data = _safe_json_load(record.workout_details)
                    print(f"[REPORT_ANALYSIS] DEBUG: Workout data for {record.date}: {type(workout_data)}")

                    # Handle different possible workout data structures
                    if isinstance(workout_data, list):
                        print(f"[REPORT_ANALYSIS] DEBUG: Workout data is a list with {len(workout_data)} items")

                        for i, exercise in enumerate(workout_data):
                            if isinstance(exercise, dict):
                                # Each exercise has one muscle group as key
                                for muscle_group, exercise_data in exercise.items():
                                    print(f"[REPORT_ANALYSIS] DEBUG: Exercise {i} - Muscle group: {muscle_group}")

                                    # exercise_data is a list of exercise objects
                                    if isinstance(exercise_data, list):
                                        for exercise_obj in exercise_data:
                                            if isinstance(exercise_obj, dict) and "sets" in exercise_obj:
                                                sets_data = exercise_obj.get("sets", [])
                                                print(f"[REPORT_ANALYSIS] DEBUG: Found {len(sets_data)} sets for {muscle_group}")

                                                for set_data in sets_data:
                                                    if isinstance(set_data, dict):
                                                        reps = float(set_data.get("reps", 0))
                                                        weight = float(set_data.get("weight", 0))
                                                        set_num = float(set_data.get("setNumber", 1))
                                                        calories = float(set_data.get("calories", 0))

                                                        # Calculate volume (weight x reps)
                                                        volume = weight * reps

                                                        total_volume += volume
                                                        total_weight += volume  # Total weight lifted (same as volume for this calculation)
                                                        total_reps += reps
                                                        workout_count += 1

                                                        print(f"[REPORT_ANALYSIS] DEBUG: Added {muscle_group} set {set_num}: volume={volume}, weight={weight}, reps={reps}, calories={calories}")

                                    # Handle the old format just in case
                                    elif isinstance(exercise_data, list) and len(exercise_data) >= 2:
                                        # Second item contains the actual sets
                                        sets_data = exercise_data[1].get("sets", [])
                                        print(f"[REPORT_ANALYSIS] DEBUG: Found {len(sets_data)} sets for {muscle_group} (old format)")

                                        for set_data in sets_data:
                                            if isinstance(set_data, dict):
                                                reps = float(set_data.get("reps", 0))
                                                weight = float(set_data.get("weight", 0))
                                                volume = weight * reps

                                                total_volume += volume
                                                total_weight += volume
                                                total_reps += reps
                                                workout_count += 1

                                # The exercise dict processing is done inside the muscle group loop above

                    elif isinstance(workout_data, dict):
                        print(f"[REPORT_ANALYSIS] DEBUG: Workout data is a dict with keys: {list(workout_data.keys())}")
                        print(f"[REPORT_ANALYSIS] DEBUG: Workout data sample: {dict(list(workout_data.items())[:8])}")  # First 8 items

                        # Handle single workout structure
                        total_volume += float(workout_data.get("volume", 0))
                        total_weight += float(workout_data.get("total_weight", 0))
                        total_reps += float(workout_data.get("total_reps", 0))
                        workout_count += 1

                except Exception as e:
                    print(f"[REPORT_ANALYSIS] Error processing workout data for {record.date}: {e}")
                    import traceback
                    print(f"[REPORT_ANALYSIS] Traceback: {traceback.format_exc()}")
                    continue

        if workout_count > 0:
            # Create a synthetic weekly record from actual workout data
            avg_workout_weight = total_weight / workout_count if workout_count > 0 else 0
            avg_reps = total_reps / workout_count if workout_count > 0 else 0

            weekly_workouts = {
                "weeks": [
                    {
                        "week": _to_iso(start_date),
                        "total_volume": total_volume,
                        "avg_workout_weight": avg_workout_weight,  # Renamed to avoid confusion
                        "avg_reps": avg_reps,
                    }
                ]
            }
            print(f"[REPORT_ANALYSIS] SUCCESS: Created workout summary from {len(workout_rows)} workout records: volume={total_volume}, avg_workout_weight={avg_workout_weight}, avg_reps={avg_reps}")
        else:
            print(f"[REPORT_ANALYSIS] ERROR: No valid workout data found for user {client_id} in date range")

    # 6) muscle summary (latest per group)
    # Try to get muscle data within date range from aggregated insights first
    mus_rows = (
        db.query(MuscleAggregatedInsights)
        .filter(
            MuscleAggregatedInsights.client_id == client_id,
            # Add date range filter if the table has date columns
        )
        .order_by(
            func.coalesce(MuscleAggregatedInsights.updated_at, MuscleAggregatedInsights.created_at).desc(),
            MuscleAggregatedInsights.id.desc()
        )
        .all()
    )
    seen = set()
    muscle_by_group: List[Dict[str, Any]] = []
    for r in mus_rows:
        mg = r.muscle_group or ""
        if mg in seen: continue
        seen.add(mg)
        muscle_by_group.append({
            "muscle_group": mg,
            "total_volume": float(r.total_volume) if r.total_volume is not None else None,
            "avg_muscle_weight": float(r.avg_weight)   if r.avg_weight   is not None else None,  # Renamed to avoid confusion
            "avg_reps":     float(r.avg_reps)     if r.avg_reps     is not None else None,
            "max_weight":   float(r.max_weight)   if r.max_weight   is not None else None,
            "max_reps":     float(r.max_reps)     if r.max_reps     is not None else None,
            "rest_days":    float(r.rest_days)    if r.rest_days    is not None else None,
            "updated_at":   _to_iso(r.updated_at or r.created_at),
        })

    # Fallback: Calculate muscle group data from ActualWorkout records if no aggregated data
    has_fallback_muscle_data = False
    if len(muscle_by_group) == 0:
        print(f"[REPORT_ANALYSIS] No muscle aggregated data found, calculating from ActualWorkout for user {client_id}")
        print(f"[REPORT_ANALYSIS] DEBUG: Searching for ActualWorkout data from {start_date} to {end_date}")

        workout_rows = (
            db.query(ActualWorkout)
            .filter(
                ActualWorkout.client_id == client_id,
                ActualWorkout.date >= start_date,
                ActualWorkout.date <= end_date,
                ActualWorkout.workout_details.isnot(None)
            )
            .order_by(ActualWorkout.date.desc())
            .all()
        )

        # Calculate muscle group volumes from ActualWorkout
        muscle_volumes = {}  # muscle_group -> total_volume
        for record in workout_rows:
            if record.workout_details:
                try:
                    workout_data = _safe_json_load(record.workout_details)
                    # The workout data structure may contain exercises with muscle group info
                    if isinstance(workout_data, dict) and "data" in workout_data:
                        for exercise_group in workout_data["data"]:
                            if isinstance(exercise_group, dict) and "data" in exercise_group:
                                for exercise in exercise_group["data"]:
                                    if isinstance(exercise, dict):
                                        muscle_group = exercise.get("muscleGroup", "Unknown")
                                        volume = 0

                                        # Calculate volume: sets × reps × weight
                                        if "data" in exercise:
                                            for set_data in exercise["data"]:
                                                if isinstance(set_data, dict):
                                                    weight = float(set_data.get("weight", 0))
                                                    reps = float(set_data.get("reps", 0))
                                                    volume += weight * reps

                                        if muscle_group not in muscle_volumes:
                                            muscle_volumes[muscle_group] = 0
                                        muscle_volumes[muscle_group] += volume
                except Exception as e:
                    print(f"[REPORT_ANALYSIS] Error processing muscle data for {record.date}: {e}")
                    continue

        # Convert muscle_volumes to muscle_by_group format
        if muscle_volumes:
            has_fallback_muscle_data = True
            for muscle_group, total_volume in sorted(muscle_volumes.items(), key=lambda x: x[1], reverse=True):
                muscle_by_group.append({
                    "muscle_group": muscle_group,
                    "total_volume": float(total_volume),
                    "avg_weight": None,  
                    "avg_reps": None,   
                    "max_weight": None,  
                    "max_reps": None,   
                    "rest_days": None,
                    "updated_at": _to_iso(end_date),
                })
            print(f"[REPORT_ANALYSIS] SUCCESS: Calculated muscle data from {len(workout_rows)} workouts: {len(muscle_by_group)} muscle groups")

    muscle_load = {"by_group": muscle_by_group}

    # Calculate actual timeframe description
    timeframe = format_date_range(start_date, end_date)
    days_in_range = (end_date - start_date).days

    # Data completeness check
    # Check if we have workout data from either weekly aggregated or fallback mechanism
    has_workout_from_weekly = len(agg_rows) > 0
    has_workout_from_fallback = len(weekly_workouts.get("weeks", [])) > 0 and len(agg_rows) == 0

    data_completeness = {
        "has_macro_data": len(weekly_macros) > 0 or has_fallback_macros,  # Include fallback macro data
        "has_workout_data": has_workout_from_weekly or has_workout_from_fallback,
        "has_diet_data": len(last7_foods) > 0,
        "has_muscle_data": len(muscle_by_group) > 0,
        "weeks_with_macros": len(weekly_macros),
        "has_fallback_macros": has_fallback_macros,  # Track if we used ActualDiet fallback
        "has_fallback_muscle_data": has_fallback_muscle_data,  # Track if we used ActualWorkout fallback
        "weeks_with_workouts": len(weekly_workouts.get("weeks", [])),  # Use fallback data if available
        "days_with_diet": len([d for d in last7_foods if d.get("items")]),
        "muscle_groups_tracked": len(muscle_by_group),
    }

    print(f"[REPORT_ANALYSIS] DEBUG: Data completeness: {data_completeness}")
    print(f"[REPORT_ANALYSIS] DEBUG: Weekly workouts data: {weekly_workouts}")

    result = {
        "user_id": client_id,
        "current_weight": current_weight,
        "target_weight": target_weight,
        "timeframe": timeframe,
        "start_date": _to_iso(start_date),
        "end_date": _to_iso(end_date),
        "days_in_range": days_in_range,
        "data_completeness": data_completeness,
        "macros": macros,
        "muscle_load": muscle_load,
        "weekly_workouts": weekly_workouts,
        "last7_foods": last7_foods,
        "last7_workouts": [],  # workouts-by-day not needed for the report, weekly is enough
        "notes": None,
    }

    # Debug final dataset being returned
    print(f"[REPORT_ANALYSIS] DEBUG: Final dataset being returned:")
    print(f"  current_weight: {result['current_weight']} (type: {type(result['current_weight'])})")
    print(f"  target_weight: {result['target_weight']} (type: {type(result['target_weight'])})")
    print(f"  timeframe: {result['timeframe']}")
    print(f"  days_in_range: {result['days_in_range']}")

    return result

# ===== data validation and empty data handling (public) =====

def check_data_availability(dataset: dict) -> tuple[bool, Optional[str]]:
    """
    Check if there's enough data to generate a meaningful report.
    Returns (has_data, message).
    """
    completeness = dataset.get("data_completeness", {})
    timeframe = dataset.get("timeframe", "the requested period")

    has_macro = completeness.get("has_macro_data", False)
    has_workout = completeness.get("has_workout_data", False)
    has_diet = completeness.get("has_diet_data", False)

    # If absolutely no data exists
    if not has_macro and not has_workout and not has_diet:
        return False, (
            f"👋 Hey there!\n\n"
            f"I don't see any data entered for {timeframe}.\n\n"
            f"To generate your fitness report, I need:\n"
            f"• Diet entries (meals, macros)\n"
            f"• Workout data (exercises, sets, reps)\n\n"
            f"Start logging your meals and workouts, and I'll create an amazing report for you! 💪"
        )

    # If minimal data - give warning but allow report
    missing = []
    if not has_macro:
        missing.append("nutrition/macro data")
    if not has_workout:
        missing.append("workout data")
    if not has_diet:
        missing.append("meal entries")

    if missing:
        weeks_macro = completeness.get("weeks_with_macros", 0)
        weeks_workout = completeness.get("weeks_with_workouts", 0)
        days_diet = completeness.get("days_with_diet", 0)
        has_fallback_macros = completeness.get("has_fallback_macros", False)

        partial_msg = (
            f"📊 Data Available for {timeframe}:\n\n"
        )
        # Check for nutrition data from either weekly aggregated or fallback
        if weeks_macro > 0:
            partial_msg += f"✅ {weeks_macro} week(s) of nutrition data\n"
        elif has_fallback_macros and "nutrition/macro data" in missing:
            # Remove nutrition/macro from missing since we have fallback data
            missing.remove("nutrition/macro data")
            partial_msg += f"✅ Nutrition data available for this period\n"
        if weeks_workout > 0:
            partial_msg += f"✅ {weeks_workout} week(s) of workout data\n"
        if days_diet > 0:
            partial_msg += f"✅ {days_diet} day(s) with meal entries\n"

        if missing:
            partial_msg += f"\n⚠️ Missing: {', '.join(missing)}\n\n"
            partial_msg += "I'll generate a report with the data I have, but it will be more comprehensive once you add the missing information."

        # Return True but with warning message (will be added to dataset)
        return True, partial_msg

    return True, None

# ===== personalization hints for the LLM (public) =====

def recent_foods_for_hints(dataset: dict, k=3):
    out = []
    for day in (dataset or {}).get("last7_foods", []) or []:
        for it in (day.get("items") or []):
            nm = (it.get("food") or it.get("name") or it.get("item") or "").strip()
            if nm: out.append(nm)
    # dedupe preserve
    seen=set(); res=[]
    for n in out:
        l=n.lower()
        if l in seen: continue
        seen.add(l); res.append(n)
    return res[:k]

def build_summary_hints(dataset: dict) -> dict:
    cw = dataset.get("current_weight")
    tw = dataset.get("target_weight")
    remaining_kg = None
    if isinstance(cw,(int,float)) and isinstance(tw,(int,float)):
        remaining_kg = round(tw - cw, 1)
    adh = (dataset.get("macros") or {}).get("adherence_pct") or {}

    # Debug weight data being sent to AI
    print(f"[REPORT_ANALYSIS] DEBUG: Weight data in build_summary_hints:")
    print(f"  current_weight from dataset: {cw} (type: {type(cw)})")
    print(f"  target_weight from dataset: {tw} (type: {type(tw)})")
    print(f"  calculated remaining_kg: {remaining_kg}")

    return {
        "remaining_kg": remaining_kg,
        "adherence_pct": {
            "calories": adh.get("calories_pct"),
            "protein":  adh.get("protein_pct"),
            "carbs":    adh.get("carbs_pct"),
            "fat":      adh.get("fat_pct"),
        },
        "recent_foods": recent_foods_for_hints(dataset),
    }

def pretty_plan_report(markdown: str) -> str:
    """
    Clean up the report output while preserving emojis and formatting.
    Removes markdown syntax but keeps the visual structure intact.
    """
    if not markdown:
        return ""

    txt = markdown.replace("\r\n", "\n").replace("\r", "\n")

    # Remove markdown heading markers (##, ###, etc.)
    txt = re.sub(r'^\s*#{1,6}\s+', '', txt, flags=re.M)

    # Remove **bold** -> plain text (but preserve emojis)
    txt = re.sub(r'\*\*([^\*]+?)\*\*', r'\1', txt)

    # Remove *italic* -> plain text
    txt = re.sub(r'\*([^\*]+?)\*', r'\1', txt)

    # Remove __underline__ if present
    txt = re.sub(r'__([^_]+?)__', r'\1', txt)
    txt = re.sub(r'_([^_]+?)_', r'\1', txt)

    # Clean up list formatting
    # Keep bullets as • (already in text from emojis)
    txt = re.sub(r'^\s*[-]\s+', '• ', txt, flags=re.M)

    # Numbered lists: "1. Foo" -> "1) Foo" or keep as is
    txt = re.sub(r'^\s*(\d+)\.\s+', r'\1) ', txt, flags=re.M)

    # Collapse 3+ blank lines to max 2 (for better spacing)
    txt = re.sub(r'\n{3,}', '\n\n', txt)

    # Remove trailing whitespace from each line
    txt = "\n".join(line.rstrip() for line in txt.split("\n"))

    # Remove any remaining backticks or code formatting
    txt = txt.replace('`', '')

    return txt.strip()

# ===== main async generator to run analysis (public) =====

async def run_analysis_generator(
    db: Session,
    mem,
    user_id: int,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None
):
    # Don't send initial status messages - they were being displayed as chat bubbles
    # Just start building the dataset directly
    print(f"[REPORT_ANALYSIS_MAIN] ===== STARTED: Report generation started for user {user_id} =====")

    dataset = build_analysis_dataset_dict(db, user_id, start_date, end_date)
    print(f"[REPORT_ANALYSIS_MAIN] Dataset built for user {user_id}")

    # Check data availability
    has_data, data_msg = check_data_availability(dataset)

    # Voice trigger removed - no voice for report analysis

    if not has_data:
        # No data at all - send message and end (no voice)
        if data_msg and data_msg.strip():
            yield sse_data(data_msg)
        yield "event: done\ndata: [DONE]\n\n"
        return

    await set_analysis_artifacts(mem, user_id, dataset, None)
    await set_mode(mem, user_id, "analysis")

    # If partial data, show warning first
    if data_msg and data_msg.strip():
        yield sse_data(data_msg + "\n\n")  # Combine message with newlines in one yield

    hints = build_summary_hints(dataset)

    # Use Celery for rate-limited report generation
    result = await generate_analysis_report_celery(
        user_id=user_id,
        dataset=dataset,
        hints=hints
    )

    pretty = result.get("report", "")
    await set_analysis_artifacts(mem, user_id, dataset, pretty)

    # Send SSE message to trigger frontend voice (working pattern)
    # print(f"[REPORT_ANALYSIS_MAIN] DEBUG: About to send voice trigger for user {user_id}")
    # try:
    #     # Send SSE message to trigger frontend voice (working pattern)
    #     frontend_voice_trigger = {"is_log": True, "type": "report_analysis"}
    #     print(f"[REPORT_ANALYSIS_MAIN] DEBUG: Voice trigger data: {frontend_voice_trigger}")
    #     yield sse_data(json.dumps(frontend_voice_trigger))
    #     print(f"[REPORT_ANALYSIS_MAIN] DEBUG: SSE voice trigger sent for user {user_id}")
    # except Exception as e:
    #     print(f"[REPORT_ANALYSIS_MAIN] DEBUG: SSE voice trigger error: {e}")
    #     import traceback
    #     print(f"[REPORT_ANALYSIS_MAIN] DEBUG: Full traceback: {traceback.format_exc()}")

    # Send line by line as separate SSE events (skip empty lines)
    for line in pretty.split('\n'):
        if line.strip():  # Only send non-empty lines
            yield sse_data(line)

    yield "event: done\ndata: [DONE]\n\n"


# ===== AI-powered date extraction =====

async def extract_date_range_from_text(text: str, oai) -> Optional[tuple[date, date]]:
    """
    Use AI to extract date range from user's natural language text.
    Returns (start_date, end_date) or None if no date range found.

    DEPRECATED: Use extract_date_range_from_text_celery() for rate-limited Celery version
    """
    if not text or not text.strip():
        return None

    today = date.today()

    # Create a prompt for the AI to extract dates
    extraction_prompt = f"""Today's date is {today.strftime('%Y-%m-%d')}.

Analyze the following user request and extract the date range they want for their fitness report.

User request: "{text}"

If the user specifies a date range (like "last 7 days", "from Jan 1 to Jan 31", "this month", "last month", etc.),
respond with ONLY a JSON object in this exact format:
{{"start_date": "YYYY-MM-DD", "end_date": "YYYY-MM-DD"}}

If NO date range is mentioned, respond with ONLY:
{{"start_date": null, "end_date": null}}

Examples:
- "analyze last 7 days" -> {{"start_date": "{(today - timedelta(days=7)).strftime('%Y-%m-%d')}", "end_date": "{today.strftime('%Y-%m-%d')}"}}
- "report from 2024-01-01 to 2024-01-31" -> {{"start_date": "2024-01-01", "end_date": "2024-01-31"}}
- "this week" -> {{"start_date": "{(today - timedelta(days=today.weekday())).strftime('%Y-%m-%d')}", "end_date": "{today.strftime('%Y-%m-%d')}"}}
- "last month" -> calculate last month's first and last day
- "analyze my progress" -> {{"start_date": null, "end_date": null}}

Respond with ONLY the JSON, nothing else."""

    try:
        resp = await oai.chat.completions.create(
            model=OPENAI_MODEL,
            messages=[{"role": "user", "content": extraction_prompt}],
            stream=False,
            temperature=0
        )

        result = (resp.choices[0].message.content or "").strip()

        # Parse the JSON response
        parsed = orjson.loads(result)

        if parsed.get("start_date") and parsed.get("end_date"):
            start = datetime.strptime(parsed["start_date"], '%Y-%m-%d').date()
            end = datetime.strptime(parsed["end_date"], '%Y-%m-%d').date()

            # Validate the date range
            is_valid, error = validate_date_range(start, end)
            if is_valid:
                return start, end
            else:
                # Invalid range, return None
                return None

        return None

    except Exception as e:
        # If AI extraction fails, return None (will use default)
        return None


# ===== Celery-backed async wrappers (rate-limited) =====

import asyncio

async def extract_date_range_from_text_celery(user_id: int, text: str) -> Optional[tuple[date, date]]:
    """
    Use AI to extract date range from user's natural language text.
    Uses Celery+Redis for rate limiting.

    Args:
        user_id: Client ID
        text: User's text containing date information

    Returns:
        (start_date, end_date) tuple or None if no date range found
    """
    from app.tasks.analysis_tasks import extract_date_range
    from celery.result import AsyncResult

    if not text or not text.strip():
        return None

    try:
        task = extract_date_range.delay(user_id=user_id, text=text)
        print(f"📅 Queued date extraction task {task.id} for user {user_id}")

        max_wait = 30
        poll_interval = 0.3
        elapsed = 0

        while elapsed < max_wait:
            celery_task = AsyncResult(task.id)
            if celery_task.ready():
                if celery_task.successful():
                    result = celery_task.result
                    start_str = result.get("start_date")
                    end_str = result.get("end_date")
                    if start_str and end_str:
                        start = datetime.strptime(start_str, '%Y-%m-%d').date()
                        end = datetime.strptime(end_str, '%Y-%m-%d').date()
                        is_valid, _ = validate_date_range(start, end)
                        if is_valid:
                            return start, end
                    return None
                break
            await asyncio.sleep(poll_interval)
            elapsed += poll_interval

        return None

    except Exception as e:
        print(f"Error in extract_date_range_from_text_celery: {e}")
        return None


async def generate_analysis_report_celery(
    user_id: int,
    dataset: dict,
    hints: dict
) -> dict:
    """
    Generate fitness analysis report using Celery+Redis for rate limiting.

    Args:
        user_id: Client ID
        dataset: Analysis dataset
        hints: Summary hints for personalization

    Returns:
        dict with 'report' and 'raw_content' keys
    """
    from app.tasks.analysis_tasks import generate_analysis_report
    from celery.result import AsyncResult

    try:
        task = generate_analysis_report.delay(
            user_id=user_id,
            dataset=dataset,
            hints=hints
        )
        print(f"📊 Queued analysis report task {task.id} for user {user_id}")

        max_wait = 120  # 2 minutes for report generation
        poll_interval = 0.5
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
            "report": "Report generation timed out. Please try again.",
            "raw_content": "",
            "error": "timeout"
        }

    except Exception as e:
        print(f"Error in generate_analysis_report_celery: {e}")
        return {
            "report": "I'm having trouble generating your report right now. Please try again.",
            "raw_content": "",
            "error": str(e)
        }


async def generate_followup_response_celery(
    user_id: int,
    user_text: str,
    summary: str,
    dataset: dict = None,
    is_followup: bool = False
) -> str:
    """
    Generate follow-up response using Celery+Redis for rate limiting.

    Args:
        user_id: Client ID
        user_text: User's follow-up question
        summary: Previous analysis summary
        dataset: Original dataset for context
        is_followup: Whether this needs full context

    Returns:
        Generated response string
    """
    from app.tasks.analysis_tasks import generate_followup_response
    from celery.result import AsyncResult

    try:
        task = generate_followup_response.delay(
            user_id=user_id,
            user_text=user_text,
            summary=summary,
            dataset=dataset or {},
            is_followup=is_followup
        )
        print(f"💬 Queued followup response task {task.id} for user {user_id}")

        max_wait = 60
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

        return "I'm having trouble responding right now. Could you try again?"

    except Exception as e:
        print(f"Error in generate_followup_response_celery: {e}")
        return "I'm having trouble answering that right now. Could you try rephrasing your question?"

# ===== FastAPI Router =====

from fastapi import APIRouter, HTTPException, Query, Depends, UploadFile, File
from fastapi.responses import StreamingResponse
from sqlalchemy.orm import Session
from app.models.database import get_db
from app.models.deps import get_oai, get_mem, get_http

router = APIRouter(prefix="/analysis", tags=["analysis"])

@router.get("/chat/stream")
async def analysis_chat_stream(
    user_id: int,
    text: str = Query(None),
    start_date: Optional[str] = Query(None, description="Start date in YYYY-MM-DD format"),
    end_date: Optional[str] = Query(None, description="End date in YYYY-MM-DD format"),
    mem = Depends(get_mem),
    oai = Depends(get_oai),
    db: Session = Depends(get_db),
):
    """
    Analysis chatbot endpoint for fitness data analysis
    - If text is None or empty: triggers automatic analysis for last 4 weeks
    - If text is provided with date info: AI extracts date range and generates report
    - If text is provided without date info: handles follow-up questions
    - Optional start_date/end_date query params override AI extraction
    """
    # DEBUG: Print immediately to verify this code is loaded
    print("🚀 DEBUG: Enhanced analysis_chat_stream function is ACTIVE! v2")

    if not user_id:
        raise HTTPException(400, "user_id required")

    mode = await get_mode(mem, user_id)

    # Parse explicit date parameters if provided
    parsed_start = None
    parsed_end = None

    if start_date and end_date:
        try:
            parsed_start = datetime.strptime(start_date, '%Y-%m-%d').date()
            parsed_end = datetime.strptime(end_date, '%Y-%m-%d').date()

            is_valid, error = validate_date_range(parsed_start, parsed_end)
            if not is_valid:
                raise HTTPException(400, f"Invalid date range: {error}")
        except ValueError:
            raise HTTPException(400, "Invalid date format. Use YYYY-MM-DD")

    # If no text provided or user explicitly requests analysis, run the analysis
    analysis_intent = is_analysis_intent(text or "") if text and text.strip() else False
    print(f"[DEBUG] text='{text}', analysis_intent={analysis_intent}, not_text={not text or not text.strip()}")

    if not text or not text.strip() or analysis_intent:
        print(f"[DEBUG] Taking fresh analysis path for user {user_id}")
        # Try to extract date range from text using Celery (if not already provided)
        if not parsed_start and text and text.strip():
            date_range = await extract_date_range_from_text_celery(user_id, text)
            if date_range:
                parsed_start, parsed_end = date_range
                print(f"[DEBUG] Date range extracted: {parsed_start} to {parsed_end}")

        # Run the analysis generator (uses Celery for OpenAI calls)
        return StreamingResponse(
            run_analysis_generator(db, mem, user_id, parsed_start, parsed_end),
            media_type="text/event-stream; charset=utf-8",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"}
        )

    # Try to detect if user is requesting a report with specific date range (uses Celery)
    print(f"[DEBUG] Checking for date range in text: '{text}'")
    date_range = await extract_date_range_from_text_celery(user_id, text)
    if date_range:
        parsed_start, parsed_end = date_range
        print(f"[DEBUG] Date range found, taking analysis path: {parsed_start} to {parsed_end}")
        # User is requesting analysis for a specific period
        return StreamingResponse(
            run_analysis_generator(db, mem, user_id, parsed_start, parsed_end),
            media_type="text/event-stream; charset=utf-8",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"}
        )

    # Handle follow-up questions about the analysis
    dataset, summary = await get_analysis_artifacts(mem, user_id)
    print(f"[DEBUG] Taking followup path - dataset exists: {dataset is not None}, summary exists: {summary is not None}")

    if not dataset or not summary:
        # No previous analysis, run a new one
        print(f"[DEBUG] No cached analysis found, running fresh analysis")
        return StreamingResponse(
            run_analysis_generator(db, mem, user_id, parsed_start, parsed_end),
            media_type="text/event-stream; charset=utf-8",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"}
        )

    print(f"[DEBUG] Using cached followup response - cached weights: current={dataset.get('current_weight')}, target={dataset.get('target_weight')}")
    # Use Celery for rate-limited follow-up response
    async def _followup():
        response = await generate_followup_response_celery(
            user_id=user_id,
            user_text=text,
            summary=summary,
            dataset=dataset,
            is_followup=is_followup_question(text)
        )

        # Send line by line as separate SSE events (skip empty lines)
        for line in response.split('\n'):
            if line.strip():
                yield sse_data(line)

        yield "event: done\ndata: [DONE]\n\n"

    return StreamingResponse(
        _followup(),
        media_type="text/event-stream; charset=utf-8",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"}
    )


@router.post("/voice/transcribe")
async def voice_transcribe_analysis(
    audio: UploadFile = File(...),
    http = Depends(get_http),
):
    """
    Transcribe audio for analysis chatbot
    Returns the transcribed text that can be used with /chat/stream
    """
    transcript = await transcribe_audio(audio, http=http)
    if not transcript:
        raise HTTPException(400, "empty transcript")

    return {"transcript": transcript, "success": True}


@router.get("/voice/test")
async def test_report_voice_direct(user_id: int, db: Session = Depends(get_db)):
    """Bypass all logic, directly test voice notification"""
    print(f"[VOICE_TEST] Starting direct voice test for user {user_id}")

    try:
        # Test 1: Voice preference check
        voice_pref = await get_voice_preference(db, client_id=user_id)
        print(f"[VOICE_TEST] Voice preference: {voice_pref}")

        # Test 2: Direct Celery task trigger
        if voice_pref == "1":
            from app.tasks.voice_tasks import process_report_analysis_voice
            task = process_report_analysis_voice.delay(user_id)
            print(f"[VOICE_TEST] Celery task triggered: {task.id}")

            return {
                "status": "test_triggered",
                "user_id": user_id,
                "voice_pref": voice_pref,
                "task_id": task.id,
                "message": "Voice test triggered - check logs and WebSocket"
            }
        else:
            return {
                "status": "voice_disabled",
                "user_id": user_id,
                "voice_pref": voice_pref,
                "message": "Voice is disabled for this user"
            }

    except Exception as e:
        print(f"[VOICE_TEST] Error in voice test: {e}")
        import traceback
        print(f"[VOICE_TEST] Traceback: {traceback.format_exc()}")

        return {
            "status": "error",
            "user_id": user_id,
            "error": str(e),
            "message": "Voice test failed - check logs"
        }


@router.get("/voice/websocket-test")
async def test_websocket_connection(user_id: int):
    """Test WebSocket connectivity and message reception"""
    import json
    import uuid
    from datetime import datetime
    from app.utils.redis_config import get_redis_sync

    print(f"[WEBSOCKET_TEST] Starting WebSocket test for user {user_id}")

    try:
        redis_client = get_redis_sync()

        # Send test message directly to WebSocket channel
        message_id = str(uuid.uuid4())
        test_message = {
            "type": "voice_message",
            "task_id": "test_manual",
            "message_id": message_id,
            "data": {
                "type": "report_analysis_success_voice",
                "user_id": user_id,
                "message": "Test voice message - this should play on frontend",
                "voice_type": "test_websocket",
                "timestamp": datetime.utcnow().isoformat()
            }
        }

        redis_client.publish(f"user_channel:{user_id}", json.dumps(test_message))
        print(f"[WEBSOCKET_TEST] Published test message to channel user_channel:{user_id}")

        return {
            "status": "test_message_sent",
            "user_id": user_id,
            "channel": f"user_channel:{user_id}",
            "message_id": message_id,
            "message": "Test voice message sent to WebSocket - check frontend console"
        }

    except Exception as e:
        print(f"[WEBSOCKET_TEST] Error in WebSocket test: {e}")
        import traceback
        print(f"[WEBSOCKET_TEST] Traceback: {traceback.format_exc()}")

        return {
            "status": "error",
            "user_id": user_id,
            "error": str(e),
            "message": "WebSocket test failed - check logs"
        }
