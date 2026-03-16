# app/routers/diet_router.py
 
from fastapi import FastAPI, APIRouter, Depends
from sqlalchemy.orm import Session
from pydantic import BaseModel
from app.models.fittbot_models import (
    CalorieEvent, ClientNextXp, LeaderboardOverall, LeaderboardDaily, LeaderboardMonthly,
    RewardGym, AggregatedInsights, Notification, Message, ClientWorkoutTemplate, FittbotWorkout,
    ActualWorkout, ActualDiet, ClientGeneralAnalysis, ClientActualAggregated, ClientActualAggregatedWeekly,
    ClientWeeklyPerformance, MuscleAggregatedInsights, Client, Attendance, TemplateDiet, ClientTarget,
    FeeHistory, Expenditure, ClientScheduler, DietTemplate, WorkoutTemplate, ClientActual, GymHourlyAgg,
    Gym, GymAnalysis, GymMonthlyData, GymPlans, GymBatches, Trainer, TemplateWorkout, Post, Comment, Like,
    ClientDietTemplate, GymLocation, QRCode, Feedback, Participant, Food, JoinProposal, New_Session, Avatar,
    Report, BlockedUsers, RewardPrizeHistory
)
from app.models.database import get_db
from app.utils.redis_config import get_redis
from redis.asyncio import Redis
 
from datetime import datetime, timedelta, date, time
from typing import Optional, List
import uuid
 
from sqlalchemy import or_, and_, asc, desc
 
# Logging & errors
from app.utils.logging_setup import jlog
from app.utils.logging_utils import (
    FittbotHTTPException,
    SecuritySeverity,   # kept for parity with other modules
    EventType,
)
from app.fittbot_api.v1.client.client_api.side_bar.ratings import check_feedback_status
 
app = FastAPI()
router = APIRouter(prefix="/diet", tags=["Clients"])
 
 
# -----------------------------
# Domain logger (diet)
# -----------------------------
class _DietLogger:
    """
    Lightweight structured logger facade for the Diet domain.
    Mirrors the _AuthLogger style. Keeps logs concise, JSON, one-line.
    """
    def __init__(self):
        self.request_id = None
 
    def set_request_context(self, context: Optional[object] = None) -> str:
        # Optionally pull X-Request-ID from headers if available in your stack.
        self.request_id = uuid.uuid4().hex
        return self.request_id
 
    def _log(self, level: str, log_type: str, **payload):
        payload.setdefault("timestamp", datetime.utcnow().isoformat() + "Z")
        payload.setdefault("request_id", self.request_id)
        payload.setdefault("domain", "diet")
        payload.setdefault("type", log_type)
        jlog(level, payload)
 
    # Debug/Info
    def debug(self, msg: str, **kv): self._log("debug", "debug", msg=msg, **kv)
    def info(self, msg: str, **kv):  self._log("info", "info", msg=msg, **kv)
 
    # Warnings/Errors
    def warning(self, msg: str, **kv): self._log("warning", "warn", msg=msg, **kv)
    def error(self, msg: str, **kv):   self._log("error", "error", msg=msg, **kv)
 
    # Domain helpers
    def business_event(self, name: str, **kv):
        self._log("info", EventType.BUSINESS, event=name, **kv)
 
    def api_access(self, method: str, endpoint: str, response_time_ms: int, **kv):
        self._log("info", EventType.API, method=method, endpoint=endpoint,
                  response_time_ms=int(response_time_ms), **kv)
 
 
# Keep the variable name used throughout the original file
client_logger = _DietLogger()
 
 
# -----------------------------
# Schemas
# -----------------------------
class DietInput(BaseModel):
    client_id: int
    date: date
    diet_data: list
    gym_id: Optional[int]=None
 
class DieteditInput(BaseModel):
    record_id: int
    date: date
    client_id: int
    diet_data: list
    gym_id: Optional[int]=None
 
class AIDietItem(BaseModel):
    food:      str
    quantity:  float
    unit:      str
    calories:  float
    protein:   float
    carbs:     float
    fat:       float
    fiber:     Optional[float] = None
    sugar:     Optional[float] = None
    sodium:    Optional[float] = None
    calcium:   Optional[float] = None
    magnesium: Optional[float] = None
    potassium: Optional[float] = None
    Iodine:    Optional[float] = None
    Iron:      Optional[float] = None
 
class AIDietPayload(BaseModel):
    client_id: int
    
    date:      date
    meal_category: str
    diet_data: Optional[List] = None
    scanner_data: Optional[dict] = None
    template_data: Optional[List] = None
    gym_id:    Optional[int]=None
    type: str
 
 

async def delete_keys_by_pattern(redis: Redis, pattern: str) -> None:
    keys = await redis.keys(pattern)
    if keys:
        await redis.delete(*keys) 
# -----------------------------
# Helpers
# -----------------------------
def calculate_totals(diet_data: list) -> dict:
    """Calculate totals from diet data - supports both old format and new template format"""
    total_calories = 0
    total_protein = 0
    total_carbs = 0
    total_fats = 0
    total_fiber = 0
    total_sugar = 0
    total_sodium = 0
    total_calcium = 0
    total_magnesium = 0
    total_potassium = 0
    total_Iodine = 0
    total_Iron = 0
 
    for item in diet_data:
        # Check if this is the new template format (with meal categories)
        if isinstance(item, dict) and "foodList" in item:
            # New template format - extract from foodList
            food_list = item.get("foodList", [])
            for food_item in food_list:
                total_calories += food_item.get("calories", 0) or 0
                total_protein += food_item.get("protein", 0) or 0
                total_carbs += food_item.get("carbs", 0) or 0
                total_fats += food_item.get("fat", 0) or 0
                total_fiber += food_item.get("fiber", 0) or 0
                total_sugar += food_item.get("sugar", 0) or 0
                total_sodium += food_item.get("sodium", 0) or 0
                total_calcium += food_item.get("calcium", 0) or 0
                total_magnesium += food_item.get("magnesium", 0) or 0
                total_potassium += food_item.get("potassium", 0) or 0
                total_Iodine += food_item.get("Iodine", 0) or 0
                total_Iron += food_item.get("Iron", 0) or 0
        else:
            # Old format - direct food items
            total_calories += item.get("calories", 0) or 0
            total_protein += item.get("protein", 0) or 0
            total_carbs += item.get("carbs", 0) or 0
            total_fats += item.get("fat", 0) or 0
            total_fiber += item.get("fiber", 0) or 0
            total_sugar += item.get("sugar", 0) or 0
            total_sodium += item.get("sodium", 0) or 0
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
        "sodium": total_sodium,
        "calcium": total_calcium,
        "magnesium": total_magnesium,
        "potassium": total_potassium,
        "Iodine": total_Iodine,
        "Iron": total_Iron
    }
 
 
def _mask(s: Optional[str]) -> str:
    if not s:
        return "****"
    return s[:3] + "****" + s[-2:] if len(s) > 5 else "****"
 
def _to_legacy(item, log_date: date, check) -> dict:
    now_str = datetime.now().strftime("%H:%M")
    uid     = f"{uuid.uuid4().hex}-{int(datetime.now().timestamp()*1000)}"
 
    if check == "scanner":
        data = item
        food_name = "+".join(data.get("items", []))
        totals = data.get("totals", {})
        return {
            "id":        uid,
            "name":      food_name,
            "calories":  totals.get("calories", 0),
            "protein":   totals.get("protein_g", 0),
            "carbs":     totals.get("carbs_g", 0),
            "fat":       totals.get("fat_g", 0),
            "sodium":    totals.get("sodium_mg", 0),
            "calcium":   totals.get("calcium_mg", 0),
            "magnesium": totals.get("magnesium_mg", 0),
            "potassium": totals.get("potassium_mg", 0),
            "Iodine":    totals.get("Iodine_mcg", 0),
            "Iron":      totals.get("Iron_mg", 0),
            "quantity":  1,
            "pic":       "",
            "date":      log_date.isoformat(),
            "timeAdded": now_str,
            "fiber":     totals.get("fibre_g", 0),
            "sugar":     totals.get("sugar_g", 0),
        }
    else:
        # chatbot payload
        #client_logger.info("Processing AI diet item", food=item.get("food", "unknown"))
        return {
            "id":        uid,
            "name":      item["food"],
            "calories":  item["calories"],
            "protein":   item["protein"],
            "carbs":     item["carbs"],
            "fat":       item["fat"],
            "sodium":    item.get("sodium"),
            "calcium":   item.get("calcium"),
            "magnesium": item.get("magnesium"),
            "potassium": item.get("potassium"),
            "Iodine":    item.get("Iodine"),
            "Iron":      item.get("Iron"),
            "quantity":  item["quantity"],
            "pic":       "",
            "date":      log_date.isoformat(),
            "timeAdded": now_str,
            "fiber":     item.get("fiber"),
            "sugar":     item.get("sugar"),
        }
 
 
# -----------------------------
# Routes
# -----------------------------
@router.get("/get_actual_diet")
async def get_actual_diet(client_id: int, date: date, db: Session = Depends(get_db)):
    req_id = client_logger.set_request_context({"client_id": client_id, "date": str(date)})
    #client_logger.info("Fetching actual diet", client_id=client_id, date=str(date))
    try:
        record = db.query(ActualDiet).filter(
            ActualDiet.client_id == client_id,
            ActualDiet.date == date
        ).first()
 
        if not record:
            #client_logger.info("No diet record found", client_id=client_id, date=str(date))
            # Return default template structure with correct timings
            default_template = [
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
            # Calculate empty totals for default template
            default_totals = {
                "calories": 0, "protein": 0, "carbs": 0, "fats": 0,
                "fiber": 0, "sugar": 0, "sodium": 0, "calcium": 0,
                "magnesium": 0, "potassium": 0, "Iodine": 0, "Iron": 0
            }
            return {"status": 200, "data": default_template, "id": None, "format": "template", "totals": default_totals}
 
        diet_data = record.diet_data or []
 
        # Check if data is in new template format
        is_template_format = (isinstance(diet_data, list) and diet_data and
                            isinstance(diet_data[0], dict) and "foodList" in diet_data[0])
 

 
        # Calculate totals including all minerals
        totals = calculate_totals(diet_data)

        return {
            "status": 200,
            "data": diet_data,
            "id": record.record_id,
            "format": "template" if is_template_format else "legacy",
            "totals": totals
        }
 
    except FittbotHTTPException:
        raise
    except Exception as e:
        raise FittbotHTTPException(
            status_code=500,
            detail="Failed to fetch actual diet",
            error_code="DIET_FETCH_ERROR",
            log_data={"exc": repr(e), "client_id": client_id, "date": str(date), "request_id": req_id},
        )
 
 
@router.post("/create_actual_diet")
async def create_or_append_diet(
    gym_id:int,
    data: DietInput,
    db: Session = Depends(get_db),
    redis: Redis = Depends(get_redis),
):
    req_id = client_logger.set_request_context(data.model_dump())
    #client_logger.info("Diet data received", client_id=data.client_id, items=len(data.diet_data), date=str(data.date))
    try:
        today = date.today()
        record = db.query(ActualDiet).filter(
            ActualDiet.client_id == data.client_id,
            ActualDiet.date == data.date
        ).first()
 
        # Append or create record - handle both old and new template formats
        if record:
            print("""diet data""",data.diet_data)
           
            record.diet_data = data.diet_data
            from sqlalchemy.orm.attributes import flag_modified
            flag_modified(record, 'diet_data')
            db.commit()  
            
        else:
            record = ActualDiet(
                client_id=data.client_id,
                date=data.date,
                diet_data=data.diet_data
            )
            db.add(record)
            db.commit()
 
        # Aggregate totals for new items
        print("=" * 80)
        print(f"DEBUG [crud_diet.py /add]: client_id={data.client_id}, date={data.date}")
        new_totals = calculate_totals(data.diet_data)
        print(f"DEBUG [crud_diet.py /add]: CALCULATED: cal={new_totals.get('calories')}, protein={new_totals.get('protein')}")

        client_record = db.query(ClientActual).filter(
            ClientActual.client_id == data.client_id,
            ClientActual.date == data.date
        ).first()
        print(f"DEBUG [crud_diet.py /add]: Found existing ClientActual: {client_record is not None}")

        client_target_calories = db.query(ClientTarget).filter(ClientTarget.client_id == data.client_id).first()
        client_target_calories = client_target_calories.calories if client_target_calories else 0

        if client_record:
            print(f"DEBUG [crud_diet.py /add]: BEFORE: cal={client_record.calories}")
            prev_calories = client_record.calories
            client_record.calories = (client_record.calories or 0) + new_totals["calories"]
            client_record.protein  = (client_record.protein  or 0) + new_totals["protein"]
            client_record.carbs    = (client_record.carbs    or 0) + new_totals["carbs"]
            client_record.fats     = (client_record.fats     or 0) + new_totals["fats"]
            print(f"DEBUG [crud_diet.py /add]: ⚠️ Only updating 4 fields")
            print(f"DEBUG [crud_diet.py /add]: AFTER: cal={client_record.calories}")
            db.commit()
        else:
            target_record = db.query(ClientTarget).filter(ClientTarget.client_id == data.client_id).first()
            prev_calories = new_totals["calories"]
            client_record = ClientActual(
                client_id=data.client_id,
                date=data.date,
                calories=new_totals["calories"],
                protein=new_totals["protein"],
                carbs=new_totals["carbs"],
                fats=new_totals["fats"],
                target_calories=target_record.calories if target_record else None,
                target_protein=target_record.protein if target_record else None,
                target_fat=target_record.fat if target_record else None,
                target_carbs=target_record.carbs if target_record else None,
            )
            print(f"DEBUG [crud_diet.py /add]: NEW ClientActual: cal={new_totals['calories']}")
            db.add(client_record)
            db.commit()
 
        # Leaderboard/xp logic (unchanged)
        if data.date == today:
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

            calorie_event = db.query(CalorieEvent).filter(
                CalorieEvent.client_id == data.client_id,
                CalorieEvent.event_date == today
            ).first()

            if not calorie_event:
                calorie = CalorieEvent(client_id=data.client_id,
                                       event_date=today, calories_added=0)
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

                # Daily
                daily_record = db.query(LeaderboardDaily).filter(
                    LeaderboardDaily.client_id == data.client_id,
                    LeaderboardDaily.date == today
                ).first()
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
                    db.add(LeaderboardDaily(
                        client_id=data.client_id,
                        xp=calorie_points, date=today
                    ))

                # Monthly
                month_date = today.replace(day=1)
                monthly_record = db.query(LeaderboardMonthly).filter(
                    LeaderboardMonthly.client_id == data.client_id,
                    LeaderboardMonthly.month == month_date
                ).first()
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
                    db.add(LeaderboardMonthly(
                        client_id=data.client_id,
                        xp=calorie_points, month=month_date
                    ))

                # Overall
                overall_record = db.query(LeaderboardOverall).filter(
                    LeaderboardOverall.client_id == data.client_id,
                ).first()
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
                        if next_row:

                            if new_total >= next_row.next_xp and next_row.next_xp != 0:
                                
                                client = db.query(Client).filter(Client.client_id == data.client_id).first()
                                
                                db.add(
                                    RewardPrizeHistory(
                                        client_id=data.client_id,
                                        gym_id=gym_id,
                                        xp=next_row.next_xp,
                                        gift=next_row.gift,
                                        achieved_date=datetime.now(),
                                        client_name=client.name if client else None,
                                        is_given=False,
                                        profile=client.profile if client else None
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
                                db.add(ClientNextXp(
                                    client_id=data.client_id,
                                    next_xp=first_tier.xp,
                                    gift=first_tier.gift,
                                ))
                        
                        
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

                existing_event = db.query(CalorieEvent).filter(
                    CalorieEvent.client_id == data.client_id,
                    CalorieEvent.event_date == data.date
                ).first()
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
                    db.add(CalorieEvent(
                        client_id=data.client_id, 
                        event_date=data.date, calories_added=calorie_points
                    ))
                db.commit()
            else:
                print(
                    f"DEBUG: CalorieEvent already at cap ({added_calory}), "
                    "skipping additional XP award."
                )
                calorie_points = 0
        else:
            calorie_points = 0
 
        # Bust caches

        await delete_keys_by_pattern(redis, f"{data.client_id}:*:target_actual")
        await delete_keys_by_pattern(redis, f"{data.client_id}:*:chart")

        # Check if actual calories exceed target calories
        client_target_record = db.query(ClientTarget).filter(ClientTarget.client_id == data.client_id).first()
        target_exceeded = False
        target_calories = None
        if client_target_record and client_target_record.calories:
            target_calories = client_target_record.calories
            # Get current actual diet record and calculate total calories from diet_data
            actual_diet_record = db.query(ActualDiet).filter(
                ActualDiet.client_id == data.client_id,
                ActualDiet.date == data.date
            ).first()

            if actual_diet_record and actual_diet_record.diet_data:
                # Calculate total calories from the diet_data
                total_calories_from_diet = calculate_totals(actual_diet_record.diet_data)
                actual_calories = total_calories_from_diet.get("calories", 0)

                if actual_calories > client_target_record.calories:
                    target_exceeded = True
                    print(f"DEBUG [crud_diet]: Target exceeded! Actual: {actual_calories}, Target: {client_target_record.calories}")

   
        
        if target_exceeded and target_calories is not None:
            achievement_date = str(data.date)
            redis_key = f"diet_target_achieved:{data.client_id}:{target_calories}:{achievement_date}"
            if await redis.exists(redis_key):
               
                target_exceeded = False
            else:
                await redis.set(redis_key, "1", ex=86400)
               
                client_logger.debug("Created redis key for first-time diet target achievement",
                                    redis_key=redis_key, client_id=data.client_id, target=target_calories)


        return {
            "status": 200,
            "message": "Diet data appended and aggregated nutrition updated",
            "reward_point": calorie_points,
            "target": target_exceeded
        }
 
    except FittbotHTTPException:
        raise
    except Exception as e:
        # Do not mask the original behavior; just structure the error.
        raise FittbotHTTPException(
            status_code=500,
            detail=f"An unexpected error occurred {e}",
            error_code="DIET_CREATE_ERROR",
            log_data={"exc": repr(e), "client_id": data.client_id,"request_id": req_id},
        )
 
 
@router.put("/edit_actual_diet")
async def edit_diet(
    data: DieteditInput,
    db: Session = Depends(get_db),
    redis: Redis = Depends(get_redis)
):
    req_id = client_logger.set_request_context(data.model_dump())

    try:
        # Preserve original branch (though it never triggers as written)
        if not data:
            record = db.query(ActualDiet).filter(ActualDiet.record_id == data.record_id).first()
            if not record:
                raise FittbotHTTPException(
                    status_code=404,
                    detail="Diet record not found",
                    error_code="DIET_RECORD_NOT_FOUND",
                    log_level="warning",
                    log_data={"record_id": data.record_id, "request_id": req_id},
                )
            db.delete(record)
            db.commit()
 
            client_actual_record = db.query(ClientActual).filter(
                ClientActual.client_id == data.client_id,
                ClientActual.date == data.date
            ).first()
            if client_actual_record:
                db.delete(client_actual_record)
                db.commit()
 
            client_logger.business_event("diet_deleted_via_edit_branch",
                                         record_id=data.record_id, client_id=data.client_id)
            return {"status": 200, "message": "Diet record and corresponding aggregated client data deleted"}
 
        # Normal edit flow
        record = db.query(ActualDiet).filter(ActualDiet.record_id == data.record_id).first()
        if not record:
            raise FittbotHTTPException(
                status_code=404,
                detail="Record not found",
                error_code="DIET_RECORD_NOT_FOUND",
                log_level="warning",
                log_data={"record_id": data.record_id, "request_id": req_id},
            )
 
        record.diet_data = data.diet_data
        db.commit()
 
        totals = calculate_totals(record.diet_data)
 
        client_record = db.query(ClientActual).filter(
            ClientActual.client_id == record.client_id,
            ClientActual.date == record.date
        ).first()
 
        if client_record:
            client_record.calories = totals["calories"]
            client_record.protein  = totals["protein"]
            client_record.carbs    = totals["carbs"]
            client_record.fats     = totals["fats"]
            db.commit()
        else:
            client_record = ClientActual(
                client_id=record.client_id,
                date=record.date,
                calories=totals["calories"],
                protein=totals["protein"],
                carbs=totals["carbs"],
                fats=totals["fats"]
            )
            db.add(client_record)
            db.commit()
 
        if data.date == date.today():
            old_event = db.query(CalorieEvent).filter(
                CalorieEvent.client_id == record.client_id,
                CalorieEvent.event_date == date.today()
            ).first()
 
            if old_event:
                old_calorie_points = old_event.calories_added
                daily_record = db.query(LeaderboardDaily).filter(
                    LeaderboardDaily.client_id == data.client_id,
                    LeaderboardDaily.date == date.today()
                ).first()
                if daily_record:
                    daily_record.xp -= old_calorie_points
 
                month_date = date.today().replace(day=1)
                monthly_record = db.query(LeaderboardMonthly).filter(
                    LeaderboardMonthly.client_id == data.client_id,
                    LeaderboardMonthly.month == month_date
                ).first()
                if monthly_record:
                    monthly_record.xp -= old_calorie_points
 
                overall_record = db.query(LeaderboardOverall).filter(
                    LeaderboardOverall.client_id == data.client_id,
                ).first()
                if overall_record:
                    overall_record.xp -= old_calorie_points
 
                old_event.calories_added = 0
            else:
                db.add(CalorieEvent(
                    client_id=data.client_id,
                    event_date=record.date,
                    calories_added=0
                ))
            db.commit()
 
            if client_record.target_calories and client_record.target_calories > 0:
                ratio = totals["calories"] / client_record.target_calories
                if ratio > 1:
                    ratio = 1
            else:
                ratio = 0

            print(
                f"DEBUG: (edit) XP calc -> new_calories={totals['calories']}, "
                f"target_calories={client_record.target_calories}, ratio={ratio}"
            )

            calorie_points = int(round(ratio * 50))
            print(f"DEBUG: (edit) Initial calorie_points={calorie_points}")

            today_d = date.today()

            calorie_event = db.query(CalorieEvent).filter(
                CalorieEvent.client_id == data.client_id,
                CalorieEvent.event_date == today_d
            ).first()

            if not calorie_event:
                db.add(
                    CalorieEvent(
                        client_id=data.client_id,
                        event_date=today_d,
                        calories_added=0,
                    )
                )
                db.commit()
                calorie_event = db.query(CalorieEvent).filter(
                    CalorieEvent.client_id == data.client_id,
                    CalorieEvent.event_date == today_d
                ).first()

            if not calorie_event.calories_added:
                calorie_event.calories_added = 0

            added_calory = calorie_event.calories_added
            print(f"DEBUG: (edit) Existing calorie_event.calories_added={added_calory}")

            if added_calory < 50:
                if added_calory + calorie_points > 50:
                    calorie_points = 50 - added_calory
                    print(
                        "DEBUG: (edit) Capping calorie_points to avoid exceeding 50 -> "
                        f"capped_calorie_points={calorie_points}"
                    )

                print(
                    f"DEBUG: (edit) Awarding calorie_points={calorie_points} "
                    f"(before daily/monthly/overall updates)"
                )

                daily_record = db.query(LeaderboardDaily).filter(
                    LeaderboardDaily.client_id == data.client_id,
                    LeaderboardDaily.date == today_d
                ).first()
                if daily_record:
                    prev_daily_xp = daily_record.xp
                    daily_record.xp += calorie_points
                    print(
                        f"DEBUG: (edit) Updated daily XP from {prev_daily_xp} to {daily_record.xp} "
                        f"for client_id={data.client_id}"
                    )
                else:
                    print(
                        f"DEBUG: (edit) Creating new daily leaderboard entry "
                        f"with xp={calorie_points} for client_id={data.client_id}"
                    )
                    db.add(LeaderboardDaily(
                        client_id=data.client_id,
                        xp=calorie_points, date=today_d
                    ))

                month_date = today_d.replace(day=1)
                monthly_record = db.query(LeaderboardMonthly).filter(
                    LeaderboardMonthly.client_id == data.client_id,
                    LeaderboardMonthly.month == month_date
                ).first()
                if monthly_record:
                    prev_monthly_xp = monthly_record.xp
                    monthly_record.xp += calorie_points
                    print(
                        f"DEBUG: (edit) Updated monthly XP from {prev_monthly_xp} to {monthly_record.xp} "
                        f"for client_id={data.client_id}"
                    )
                else:
                    print(
                        f"DEBUG: (edit) Creating new monthly leaderboard entry "
                        f"with xp={calorie_points} for client_id={data.client_id}"
                    )
                    db.add(LeaderboardMonthly(
                        client_id=data.client_id, 
                        xp=calorie_points, month=month_date
                    ))

                overall_record = db.query(LeaderboardOverall).filter(
                    LeaderboardOverall.client_id == data.client_id,
                ).first()
                if overall_record:
                    prev_overall_xp = overall_record.xp
                    overall_record.xp += calorie_points
                    print(
                        f"DEBUG: (edit) Updated overall XP from {prev_overall_xp} to {overall_record.xp} "
                        f"for client_id={data.client_id}"
                    )
                    db.commit()
                else:
                    print(
                        f"DEBUG: (edit) Creating new overall leaderboard entry "
                        f"with xp={calorie_points} for client_id={data.client_id}"
                    )
                    new_overall = LeaderboardOverall(
                        client_id=data.client_id,  xp=calorie_points
                    )
                    db.add(new_overall)
                    db.commit()

                existing_event = db.query(CalorieEvent).filter(
                    CalorieEvent.client_id == data.client_id,
                    CalorieEvent.event_date == today_d
                ).first()
                if existing_event:
                    before_update = existing_event.calories_added
                    existing_event.calories_added += calorie_points
                    print(
                        f"DEBUG: (edit) Updated CalorieEvent from {before_update} to "
                        f"{existing_event.calories_added} for client_id={data.client_id}"
                    )
                db.commit()
            else:
                print(
                    f"DEBUG: (edit) CalorieEvent already at cap ({added_calory}), "
                    "skipping additional XP award."
                )
 
        # Invalidate cache
        await delete_keys_by_pattern(redis, f"{data.client_id}:*:target_actual")

        return {"status": 200, "message": "Diet data replaced and aggregated nutrition updated"}
 
    except FittbotHTTPException:
        raise
    except Exception as e:
        raise FittbotHTTPException(
            status_code=500,
            detail=f"An unexpected error occurred {e}",
            error_code="DIET_EDIT_ERROR",
            log_data={"exc": repr(e), "record_id": getattr(data, 'record_id', None), "request_id": req_id},
        )
 
 
@router.delete("/delete_actual_diet")
async def delete_diet(
    record_id: int,
    client_id: int,
    date: date,
    db: Session = Depends(get_db),
    redis: Redis = Depends(get_redis)
):
    req_id = client_logger.set_request_context(
        {"record_id": record_id, "client_id": client_id, "date": str(date)}
    )
    try:
        record = db.query(ActualDiet).filter(ActualDiet.record_id == record_id).first()
        if not record:
            raise FittbotHTTPException(
                status_code=404,
                detail="Diet record not found",
                error_code="DIET_RECORD_NOT_FOUND",
                log_level="warning",
                log_data={"record_id": record_id, "request_id": req_id},
            )
 
        db.delete(record)
        db.commit()
 
        client_actual_record = db.query(ClientActual).filter(
            ClientActual.client_id == client_id,
            ClientActual.date == date
        ).first()
        if client_actual_record:
            db.delete(client_actual_record)
            db.commit()
 
        if date == date.today():
            old_event = db.query(CalorieEvent).filter(
                CalorieEvent.client_id == client_id,
                CalorieEvent.event_date == date.today()
            ).first()
 
            if old_event:
                old_calorie_points = old_event.calories_added
                daily_record = db.query(LeaderboardDaily).filter(
                    LeaderboardDaily.client_id == client_id,
                    LeaderboardDaily.date == date.today()
                ).first()
                if daily_record:
                    daily_record.xp -= old_calorie_points
 
                month_date = date.today().replace(day=1)
                monthly_record = db.query(LeaderboardMonthly).filter(
                    LeaderboardMonthly.client_id == client_id,
                    LeaderboardMonthly.month == month_date
                ).first()
                if monthly_record:
                    monthly_record.xp -= old_calorie_points
 
                overall_record = db.query(LeaderboardOverall).filter(
                    LeaderboardOverall.client_id == client_id,
                ).first()
                if overall_record:
                    overall_record.xp -= old_calorie_points
 
                old_event.calories_added = 0
            else:
                db.add(CalorieEvent(
                    client_id=client_id,
                    event_date=date.today(),
                    calories_added=0
                ))
            db.commit()
 
        # Invalidate cache
        await delete_keys_by_pattern(redis, f"{client_id}:*:target_actual")
        
        return {"status": 200, "message": "Diet record and corresponding aggregated client data deleted"}
 
    except FittbotHTTPException:
        raise
    except Exception as e:
        raise FittbotHTTPException(
            status_code=500,
            detail=f"An unexpected error occurred {e}",
            error_code="DIET_DELETE_ERROR",
            log_data={"exc": repr(e), "record_id": record_id, "client_id": client_id, "request_id": req_id},
        )
 
 
########### AI FOOD LOGGING #############
 
@router.post("/create_ai_diet")
async def ai_create_diet(
    payload: AIDietPayload,
    db: Session = Depends(get_db),
    redis: Redis = Depends(get_redis),
):
    req_id = client_logger.set_request_context(payload.model_dump())



    try:

        print("diet data",payload.scanner_data)

        reward_point=None
        gym_id=payload.gym_id
        # Check if we have template_data (new structure) OR if we have meal_category (scanner without template_data)
        if payload.template_data or (payload.meal_category and payload.type == "scanner"):
            # Handle new template structure
     
 
            # Get template data - either from payload or fetch current from database
            if payload.template_data:
                template_data = payload.template_data
            else:
                # Fetch current template from database
                existing_record = db.query(ActualDiet).filter(
                    ActualDiet.client_id == payload.client_id,
                    ActualDiet.date == payload.date
                ).first()
 
                if existing_record and existing_record.diet_data:
                    template_data = existing_record.diet_data
                else:
                    # Use default template structure
                    template_data = [
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
 
            meal_category = payload.meal_category
 
            # Create food item to add based on type
            if payload.type == "scanner":
                # Create food item from scanner data
                scanner_data = payload.scanner_data
                food_item = {
                    "id": f"{int(datetime.now().timestamp()*1000)}",
                    "name": "+".join(scanner_data.get("items", [])),
                    "calories": scanner_data.get("totals", {}).get("calories", 0),
                    "protein": scanner_data.get("totals", {}).get("protein_g", 0),
                    "carbs": scanner_data.get("totals", {}).get("carbs_g", 0),
                    "fat": scanner_data.get("totals", {}).get("fat_g", 0),
                    "fiber": scanner_data.get("totals", {}).get("fibre_g", 0),
                    "sugar": scanner_data.get("totals", {}).get("sugar_g", 0),
                    "sodium": scanner_data.get("micro_nutrients", {}).get("sodium_mg", 0),
                    "calcium": scanner_data.get("micro_nutrients", {}).get("calcium_mg", 0),
                    "magnesium": scanner_data.get("micro_nutrients", {}).get("magnesium_mg", 0),
                    "potassium": scanner_data.get("micro_nutrients", {}).get("potassium_mg", 0),
                    "iron": scanner_data.get("micro_nutrients", {}).get("Iron_mg", 0),
                    "quantity": "1 serving",
                    "image_url": ""
                }

                print("food item is",food_item)
            else:
                # Handle chatbot or other types
                diet_item = payload.diet_data[0] if payload.diet_data else {}
                food_item = {
                    "id": f"{int(datetime.now().timestamp()*1000)}",
                    "name": diet_item.get("food", ""),
                    "calories": diet_item.get("calories", 0),
                    "protein": diet_item.get("protein", 0),
                    "carbs": diet_item.get("carbs", 0),
                    "fat": diet_item.get("fat", 0),
                    "fiber": diet_item.get("fiber", 0),
                    "sugar": diet_item.get("sugar", 0),
                    "sodium": diet_item.get("sodium", 0),
                    "calcium": diet_item.get("calcium", 0),
                    "magnesium": diet_item.get("magnesium", 0),
                    "potassium": diet_item.get("potassium", 0),
                    "Iodine": diet_item.get("Iodine", 0),
                    "Iron": diet_item.get("Iron", 0),
                    "quantity": f"{diet_item.get('quantity', 1)} {diet_item.get('unit', 'serving')}",
                    "image_url": ""
                }
 
            # Find and update the correct meal category
            for meal in template_data:
                if meal.get("title") == meal_category:
                    meal["foodList"].append(food_item)
                    meal["itemsCount"] = len(meal["foodList"])
                    break
 
            # Store the complete template data
            diet_input = DietInput(
                client_id=payload.client_id,
                date=payload.date,
                diet_data=template_data
            )
 
        else:
            # Handle legacy format (old structure)

            if payload.type == "chatbot":
                legacy_items = [_to_legacy(it, payload.date, payload.type) for it in (payload.diet_data or [])]
            else:
                legacy_items = [_to_legacy(payload.scanner_data, payload.date, payload.type)]
 
            diet_input = DietInput(
                client_id=payload.client_id,
                date=payload.date,
                diet_data=legacy_items
            )
 
     
        response= await create_or_append_diet(gym_id,data=diet_input, db=db, redis=redis)

        show_feedback = check_feedback_status(db, payload.client_id)
      
        return {
            "status": 200,
            "reward_point": response["reward_point"],
            "feedback": show_feedback,
            "target": response.get("target")
        }
 
    except FittbotHTTPException:
        raise
    except Exception as e:
        client_logger.error("Failed to log AI diet data", error=repr(e), client_id=payload.client_id)
        raise FittbotHTTPException(
            status_code=500,
            detail=f"Failed to log AI diet data: {e}",
            error_code="DIET_AI_CREATE_ERROR",
            log_data={"exc": repr(e), "client_id": payload.client_id,  "request_id": req_id},
        )
 
 
