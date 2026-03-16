# app/routers/client_router.py

import os
import time
import json
import random
import requests
from datetime import datetime, timedelta, date
from typing import Dict, List, Any, Optional

from fastapi import FastAPI, APIRouter, Depends, Request,HTTPException,Query
from pydantic import BaseModel
from sqlalchemy import or_, and_, asc,desc,func,extract
from sqlalchemy.orm import Session, class_mapper
from app.utils.http_retry import http_get_with_retry
from app.utils.otp import generate_otp, async_send_verification_sms

from app.fittbot_api.v1.websockets.websocket import live_gym_manager, session_update_manager  
from app.models.database import get_db
from app.models.fittbot_models import (
    VoicePreference,
    GymFees,
    AttendanceGym,
    CalorieEvent,
    GBMessage,
    ClientNextXp,
    SmartWatch,
    FittbotMuscleGroup,
    LiveCount,
    DefaultWorkoutTemplates,
    FittbotDietTemplate,
    DailyGymHourlyAgg,
    LeaderboardOverall,
    LeaderboardDaily,
    RewardQuest,
    LeaderboardMonthly,
    RewardGym,
    RewardBadge,
    RewardClientHistory,
    GymOwner,
    RejectedProposal,
    AggregatedInsights,
    Notification,
    Message,
    ClientWorkoutTemplate,
    FittbotWorkout,
    ActualWorkout,
    ActualDiet,
    ClientGeneralAnalysis,
    ClientActualAggregated,
    ClientActualAggregatedWeekly,
    ClientWeeklyPerformance,
    MuscleAggregatedInsights,
    Client,
    Attendance,
    TemplateDiet,
    ClientTarget,
    FeeHistory,
    Expenditure,
    ClientScheduler,
    DietTemplate,
    WorkoutTemplate,
    ClientActual,
    GymHourlyAgg,
    Gym,
    GymAnalysis,
    GymMonthlyData,
    GymPlans,
    GymBatches,
    Trainer,
    TemplateWorkout,
    Post,
    Comment,
    Like,
    ClientDietTemplate,
    GymLocation,
    QRCode,
    Feedback,
    Participant,
    Food,
    JoinProposal,
    New_Session,
    Avatar,
    Report,
    BlockedUsers,
    RewardPrizeHistory,
    EquipmentWorkout,
    Preference,
)
from app.utils.redis_config import get_redis
from redis.asyncio import Redis
from app.utils.logging_utils import FittbotHTTPException
from app.utils.hashing import verify_password,hash_password
from app.utils.security import get_password_hash

app = FastAPI()
router = APIRouter(prefix="/client", tags=["Clients"])


def serialize_sqlalchemy_object(obj):
    result = {}
    for column in class_mapper(obj.__class__).columns:
        value = getattr(obj, column.key)
        if isinstance(value, date):
            value = value.isoformat()
        result[column.key] = value
    return result

async def _load_equipment_catalog(db: Session) -> Dict[str, Any]:
    records = db.query(EquipmentWorkout).order_by(EquipmentWorkout.id.asc()).all()
    if not records:
        raise FittbotHTTPException(
            status_code=404,
            detail="Equipment catalog not configured.",
            error_code="EQUIPMENT_NOT_CONFIGURED",
        )

    catalog: Dict[str, Any] = {}
    for record in records:
        payload = record.equipment
        if isinstance(payload, str):
            try:
                payload = json.loads(payload)
            except json.JSONDecodeError:
                continue
        if isinstance(payload, dict):
            catalog.update(payload)

    if not catalog:
        raise FittbotHTTPException(
            status_code=404,
            detail="Equipment catalog is empty.",
            error_code="EQUIPMENT_CATALOG_EMPTY",
        )

    return catalog


async def get_gym_analytics_key(gym_id: int, db: Session, redis: Redis):
    
    try:
        analytics_key = f"gym:{gym_id}:analytics"
        today = datetime.now().date()
        current_clients = (
            db.query(
                Attendance.client_id,
                Attendance.in_time,
                Client.name,
                Client.training_id,
                Client.goals,
                Client.profile,
                Attendance.muscle,
            )
            .join(Client, Attendance.client_id == Client.client_id)
            .filter(
                Attendance.date == today,
                Attendance.out_time.is_(None),
                Client.gym_id == gym_id,
            )
            .all()
        )

        goals_summary: Dict[str, Dict[str, Any]] = {}
        training_type_summary: Dict[str, Dict[str, Any]] = {}

        for client in current_clients:
            if client.goals not in goals_summary:
                goals_summary[client.goals] = {"count": 0, "clients": []}
            goals_summary[client.goals]["count"] += 1
            goals_summary[client.goals]["clients"].append(client.name)

            training = db.query(GymPlans).filter(GymPlans.id == client.training_id).first()
            training_type = training.plans if training else None
            if training_type not in training_type_summary:
                training_type_summary[training_type] = {"count": 0, "clients": []}
            training_type_summary[training_type]["count"] += 1
            training_type_summary[training_type]["clients"].append(client.name)

        analytics_summary = {
            "goals_summary": goals_summary,
            "training_type_summary": training_type_summary,
        }

        await redis.hset(analytics_key, mapping={"details": json.dumps(analytics_summary)})
        await redis.expire(analytics_key, 86400)

        return analytics_summary

    except Exception as e:
        return {"error": str(e)}


class ClientHomeRequest(BaseModel):
    gym_id: int
    client_id: int


@router.get("/check-client-target")
async def check_client_target(
    request: Request, client_id: int, db: Session = Depends(get_db), redis: Redis = Depends(get_redis)
):
    try:
        client_status_key = f"client{client_id}:initialstatus"
        client_status = await redis.hget(client_status_key, "initialstatus")
        actual_weight = await redis.hget(client_status_key, "actual_weight")
        target_weight = await redis.hget(client_status_key, "target_weight")
        start_weight = await redis.hget(client_status_key, "start_weight")
        height = await redis.hget(client_status_key, "height")
        goals = await redis.hget(client_status_key, "goals")
        age = await redis.hget(client_status_key, "age")
        lifestyle = await redis.hget(client_status_key, "lifestyle")

        if client_status is None:
            client = db.query(Client).filter(Client.client_id == client_id).first()
            target_data = db.query(ClientTarget).filter(ClientTarget.client_id == client_id).first()
            if not client:
                raise FittbotHTTPException(
                    status_code=404,
                    detail="User not found",
                    error_code="CLIENT_NOT_FOUND",
                    log_data={"client_id": client_id},
                )

            client_status = client.status
            height = client.height if client.height is not None else ""
            age = client.age if client.age is not None else ""
            lifestyle = client.lifestyle if client.lifestyle is not None else ""
            goals = client.goals if client.goals is not None else ""
            actual_weight = client.weight if client.weight is not None else None
            target_weight = target_data.weight if target_data and target_data.weight is not None else 0
            start_weight = target_data.start_weight if target_data and target_data.start_weight is not None else 0

            await redis.hset(
                client_status_key,
                mapping={
                    "initialstatus": client_status,
                    "actual_weight": actual_weight,
                    "target_weight": target_weight,
                    "start_weight": start_weight,
                    "goals": goals,
                    "height": height,
                    "age": age,
                    "lifestyle": lifestyle,
                },
            )
            await redis.expire(client_status_key, 86400)

            def ensure_str(value):
                if value is None:
                    return ""
                if isinstance(value, bytes):
                    return value.decode()
                return value

            client_status = ensure_str(client_status)
            goals = ensure_str(goals)
            height = ensure_str(height)
            age = ensure_str(age)
            lifestyle = ensure_str(lifestyle)

        weight = True
        if any([target_weight == "0", target_weight is None, target_weight == 0]):
            weight = False

        target_actual_key = f"client{client_id}:initial_target_actual"
        target_actual_data = await redis.get(target_actual_key)

        if target_actual_data:
            target_actual = json.loads(target_actual_data)
        else:
            today_dt = datetime.now().date()
            target_data = db.query(ClientTarget).filter(ClientTarget.client_id == client_id).first()
            actual_data = (
                db.query(ClientActual)
                .filter(ClientActual.client_id == client_id, ClientActual.date == today_dt)
                .first()
            )

            target_actual = {
                "calories": {
                    "target": target_data.calories if target_data else None,
                    "actual": actual_data.calories if actual_data else 0,
                },
                "protein": {
                    "target": target_data.protein if target_data else None,
                    "actual": actual_data.protein if actual_data else 0,
                },
                "carbs": {
                    "target": target_data.carbs if target_data else None,
                    "actual": actual_data.carbs if actual_data else 0,
                },
                "fat": {
                    "target": target_data.fat if target_data else None,
                    "actual": actual_data.fats if actual_data else 0,
                },
                "fiber": {  
                    "target": target_data.fiber if target_data else None,
                    "actual": actual_data.fiber if actual_data else 0,
                },
                "sugar": {  
                    "target": target_data.sugar if target_data else None,
                    "actual": actual_data.sugar if actual_data else 0,
                },
                "calcium": {  
                    "target": target_data.calcium if target_data else None,
                    "actual": actual_data.calcium if actual_data else 0,
                },
                "magnesium": {  
                    "target": target_data.magnesium if target_data else None,
                    "actual": actual_data.magnesium if actual_data else 0,
                },
                "potassium": {  
                    "target": target_data.potassium if target_data else None,
                    "actual": actual_data.potassium if actual_data else 0,
                },
                "Iodine": {  
                    "target": target_data.Iodine if target_data else None,
                    "actual": actual_data.Iodine if actual_data else 0,
                },
                "Iron": {  
                    "target": target_data.Iron if target_data else None,
                    "actual": actual_data.Iron if actual_data else 0,
                },
 
            }

            await redis.set(target_actual_key, json.dumps(target_actual))
            await redis.expire(target_actual_key, 86400)

        calories = True
        if any(
            [
                target_actual["calories"]["target"] is None,
                target_actual["protein"]["target"] is None,
                target_actual["carbs"]["target"] is None,
                target_actual["fat"]["target"] is None,
                target_actual["fiber"]["target"] is None,
                target_actual["sugar"]["target"] is None,
                target_actual["calcium"]["target"] is None,
                target_actual["magnesium"]["target"] is None,
                target_actual["potassium"]["target"] is None,
                target_actual["Iodine"]["target"] is None,
                target_actual["Iron"]["target"] is None,
            ]
        ):
            calories = False

        return {
            "status": 200,
            "message": "Data fetched successfully",
            "data": {
                "client": {
                    "actual_weight": actual_weight,
                    "target_weight": target_weight,
                    "start_weight": start_weight,
                    "height": height,
                    "age": age,
                    "lifestyle": lifestyle,
                    "goals": goals,
                },
                "target_actual": target_actual,
            },
            "weight": weight,
            "calories": calories,
        }

    except FittbotHTTPException:
        raise
    except Exception as e:
        raise FittbotHTTPException(
            status_code=500,
            detail="Unable to retrieve client information",
            error_code="CHECK_CLIENT_TARGET_ERROR",
            log_data={"exc": repr(e), "client_id": client_id},
        )



@router.get("/home")
async def get_clients_home(
    gym_id: int,
    client_id: int,
    request: Request,
    db: Session = Depends(get_db),
    redis: Redis = Depends(get_redis),
):
    try:
        analytics_key = f"gym:{gym_id}:client_analytics"
        analytics_data = await redis.hget(analytics_key, "details")
        if analytics_data:
            _ = json.loads(analytics_data)
        else:
            _ = await get_gym_analytics_key(gym_id, db, redis)

        client_status_key = f"{client_id}:{gym_id}:status"
        client_status = await redis.hget(client_status_key, "status")
        client_profile = await redis.hget(client_status_key, "profile")
        client_name = await redis.hget(client_status_key, "name")
        expiry = await redis.hget(client_status_key, "expiry")
        joined_date_str = await redis.hget(client_status_key, "joined_date")
        bmi = await redis.hget(client_status_key, "bmi")
        height = await redis.hget(client_status_key, "height")
        goals = await redis.hget(client_status_key, "goals")
        age = await redis.hget(client_status_key, "age")
        lifestyle = await redis.hget(client_status_key, "lifestyle")
        actual_weight = await redis.hget(client_status_key, "actual_weight")
        target_weight = await redis.hget(client_status_key, "target_weight")
        start_weight = await redis.hget(client_status_key, "start_weight")
        progress = await redis.hget(client_status_key, "progress")

        if client_status is None:
            client = db.query(Client).filter(Client.client_id == client_id).first()
            target_data = db.query(ClientTarget).filter(ClientTarget.client_id == client_id).first()
            if not client:
                return {"status": 404, "message": "Client not found"}

            client_status = client.status
            bmi = client.bmi if client.bmi is not None else ""
            height = client.height if client.height is not None else ""
            age = client.age if client.age is not None else ""
            lifestyle = client.lifestyle if client.lifestyle is not None else ""
            goals = client.goals if client.goals is not None else ""
            actual_weight = client.weight if client.weight is not None else None
            target_weight = target_data.weight if target_data and target_data.weight is not None else 0
            start_weight = target_data.start_weight if target_data and target_data.start_weight is not None else 0

            progress_val = 0
            if actual_weight is not None and target_weight is not None and start_weight is not None:
                if goals.lower() == "weight_gain":
                    if actual_weight < start_weight:
                        progress_val = 0
                    else:
                        progress_val = (
                            ((actual_weight - start_weight) / (target_weight - start_weight)) * 100
                            if (target_weight - start_weight) > 0
                            else 0
                        )
                elif goals.lower() == "weight_loss":
                    if actual_weight > start_weight:
                        progress_val = 0
                    else:
                        progress_val = (
                            ((start_weight - actual_weight) / (start_weight - target_weight)) * 100
                            if (start_weight - target_weight) > 0
                            else 0
                        )
                else:
                    progress_val = 0
            progress = min(progress_val, 100)

            client_profile = client.profile or ""
            client_name = client.name or ""
            expiry = client.expiry or ""
            joined_date_str = client.joined_date.isoformat() if client.joined_date else None

            await redis.hset(
                client_status_key,
                mapping={
                    "status": client_status,
                    "progress": progress,
                    "bmi": bmi,
                    "goals": goals,
                    "height": height,
                    "age": age,
                    "lifestyle": lifestyle,
                    "expiry": expiry,
                    "joined_date": joined_date_str,
                    "actual_weight": actual_weight,
                    "target_weight": target_weight,
                    "profile": client_profile,
                    "name": client_name,
                    "start_weight": start_weight,
                },
            )
            await redis.expire(client_status_key, 86400)

        def ensure_str(value):
            if value is None:
                return ""
            if isinstance(value, bytes):
                return value.decode()
            return value

        client_status = ensure_str(client_status)
        expiry = ensure_str(expiry)
        bmi = ensure_str(bmi)
        goals = ensure_str(goals)
        height = ensure_str(height)
        age = ensure_str(age)
        lifestyle = ensure_str(lifestyle)
        joined_date = datetime.fromisoformat(joined_date_str).date() if joined_date_str else None

        if client_status == "inactive":
            days_left_for_expiry: Any = "expired"
        else:
            today = datetime.now().date()
            if expiry == "start_of_the_month":
                first_of_next_month = (today.replace(day=1) + timedelta(days=31)).replace(day=1)
                days_left_for_expiry = (first_of_next_month - today).days
            elif expiry == "joining_date" and joined_date:
                joined_day = joined_date.day
                this_month_expiry = today.replace(day=joined_day)
                if today <= this_month_expiry:
                    days_left_for_expiry = (this_month_expiry - today).days
                else:
                    next_month_expiry = (this_month_expiry + timedelta(days=31)).replace(day=joined_day)
                    days_left_for_expiry = (next_month_expiry - today).days
            else:
                days_left_for_expiry = "unknown"

        assigned_plans_key = f"{client_id}:{gym_id}:assigned_plans"
        assigned_plans_data = await redis.get(assigned_plans_key)
        trainer_name = None

        if assigned_plans_data:
            assigned_plans = json.loads(assigned_plans_data)
        else:
            client_scheduler = (
                db.query(ClientScheduler)
                .filter(ClientScheduler.gym_id == gym_id, ClientScheduler.client_id == client_id)
                .first()
            )

            if client_scheduler:
                trainer = db.query(Trainer).filter(Trainer.trainer_id == client_scheduler.assigned_trainer).first()
                trainer_name = "" if not trainer else trainer.full_name

            
            dietplan = None
            if client_scheduler and client_scheduler.assigned_dietplan is not None:
                diet_plan = (
                    db.query(TemplateDiet).filter(TemplateDiet.template_id == client_scheduler.assigned_dietplan).first()
                )
                dietplan = diet_plan.template_details if diet_plan else "No diet has been assigned"

            
            workout_plan = None
            if client_scheduler and client_scheduler.assigned_workoutplan is not None:
                workout_plan_obj = (
                    db.query(TemplateWorkout).filter(TemplateWorkout.id == client_scheduler.assigned_workoutplan).first()
                )
                workout_plan = workout_plan_obj.workoutPlan if workout_plan_obj else "No workout has been assigned"

            assigned_plans = {"trainer_name": trainer_name or None, "diet_plan": dietplan or [], "workout_plan": workout_plan or []}

            await redis.set(assigned_plans_key, json.dumps(assigned_plans))
            await redis.expire(assigned_plans_key, 86400)

        target_actual_key = f"{client_id}:{gym_id}:target_actual"
        target_actual_data = await redis.get(target_actual_key)
        today = datetime.now().date()

        if target_actual_data:
            target_actual = json.loads(target_actual_data)
        else:
            target_data = db.query(ClientTarget).filter(ClientTarget.client_id == client_id).first()
            actual_data = (
                db.query(ClientActual)
                .filter(ClientActual.client_id == client_id, ClientActual.date == today)
                .first()
            )
            target_actual = {
                "calories": {
                    "target": target_data.calories if target_data else None,
                    "actual": actual_data.calories if actual_data else 0,
                },
                "protein": {
                    "target": target_data.protein if target_data else None,
                    "actual": actual_data.protein if actual_data else 0,
                },
                "carbs": {
                    "target": target_data.carbs if target_data else None,
                    "actual": actual_data.carbs if actual_data else 0,
                },
                "fat": {
                    "target": target_data.fat if target_data else None,
                    "actual": actual_data.fats if actual_data else 0
                },
                "fiber": {
                    "target": target_data.fiber if target_data else None,
                    "actual": actual_data.fiber if actual_data else 0,
                },
                "sugar": {
                    "target": target_data.sugar if target_data else None,
                    "actual": actual_data.sugar if actual_data else 0,
                },
                "calcium": {
                    "target": target_data.calcium if target_data else None,
                    "actual": actual_data.calcium if actual_data else 0,
                },
                "magnesium": {
                    "target": target_data.magnesium if target_data else None,
                    "actual": actual_data.magnesium if actual_data else 0,
                },
                "potassium": {
                    "target": target_data.potassium if target_data else None,
                    "actual": actual_data.potassium if actual_data else 0,      
                },
                "Iodine": {
                    "target": target_data.Iodine if target_data else None,
                    "actual": actual_data.Iodine if actual_data else 0,      
                },
                "Iron": {
                    "target": target_data.Iron if target_data else None,
                    "actual": actual_data.Iron if actual_data else 0,      
                },
 
                "calories_burnt": {
                    "target": target_data.calories_to_burn if target_data else None,
                    "actual": actual_data.burnt_calories if actual_data else 0,
                },
                "water_intake": {
                    "target": target_data.water_intake if target_data else None,
                    "actual": actual_data.water_intake if actual_data else 0,
                },
                "weight": {"target": target_data.weight if target_data else None, "actual": actual_data.weight if actual_data else 0},
            }

            await redis.set(target_actual_key, json.dumps(target_actual))
            await redis.expire(target_actual_key, 86400)

        chart_key = f"{client_id}:{gym_id}:chart"
        chart_data = await redis.get(chart_key)

        if chart_data:
            chart = json.loads(chart_data)
        else:
            first_day_of_month = today.replace(day=1)
            actual_data = (
                db.query(ClientActual)
                .filter(
                    ClientActual.client_id == client_id,
                    ClientActual.date >= first_day_of_month,
                    ClientActual.date <= today,
                )
                .order_by(ClientActual.date)
                .all()
            )

            if not actual_data:
                chart = {"weight": [], "calories": [], "calories_burnt": [], "protein": [], "fat": [], "carbs": [], "fiber":[],"sugar":[], "calcium":[], "water_intake": [], "magnesium":[], "potassium":[], "Iodine":[], "Iron":[]}
            else:
                dates = [entry.date for entry in actual_data]
                if len(dates) <= 7:
                    selected_records = actual_data
                else:
                    selected_records = [actual_data[0], actual_data[-1]]
                    step = len(dates) // 6
                    for i in range(1, 6):
                        index = i * step
                        if index < len(actual_data) - 1:
                            selected_records.append(actual_data[index])

                chart = {
                    "weight": [],
                    "calories": [],
                    "calories_burnt": [],
                    "protein": [],
                    "fat": [],
                    "carbs": [],
                    "fiber": [],
                    "sugar": [],
                    "water_intake": [],
                    "calcium": [],
                    "magnesium": [],
                    "potassium": [],
                    "Iodine": [],
                    "Iron": [],
                }
                for record in selected_records:
                    formatted_date = record.date.isoformat()
                    chart["weight"].append({"date": formatted_date, "weight": record.weight})
                    chart["calories"].append({"date": formatted_date, "calories": record.calories})
                    chart["calories_burnt"].append({"date": formatted_date, "calories_burnt": record.burnt_calories})
                    chart["protein"].append({"date": formatted_date, "protein": record.protein})
                    chart["fiber"].append({"date": formatted_date, "fiber": record.fiber})
                    chart["sugar"].append({"date": formatted_date, "sugar": record.sugar})
                    chart["fat"].append({"date": formatted_date, "fat": record.fats})
                    chart["carbs"].append({"date": formatted_date, "carbs": record.carbs})
                    chart["water_intake"].append({"date": formatted_date, "water_intake": record.water_intake})
                    chart["calcium"].append({"date": formatted_date, "calcium": record.calcium})
                    chart["magnesium"].append({"date": formatted_date, "magnesium": record.magnesium})
                    chart["potassium"].append({"date": formatted_date, "potassium": record.potassium})
                    chart["Iodine"].append({"date": formatted_date, "Iodine": record.Iodine})
                    chart["Iron"].append({"date": formatted_date, "Iron": record.Iron})

            await redis.set(chart_key, json.dumps(chart))
            await redis.expire(chart_key, 86400)

        fees_redis_key = f"{client_id}:{gym_id}:fees"
        fee_data = await redis.get(fees_redis_key)
        if fee_data:
            fee_history = json.loads(fee_data)
        else:
            fee_history_records = db.query(FeeHistory).filter(FeeHistory.gym_id == gym_id, FeeHistory.client_id == client_id).all()
            fee_history = [
                {"payment_date": record.payment_date.strftime("%Y-%m-%d"), "fees_paid": record.fees_paid}
                for record in fee_history_records
            ]
            await redis.set(fees_redis_key, json.dumps(fee_history), ex=86400)

        return {
            "status": 200,
            "message": "Data fetched successfully",
            "data": {
                "client": {
                    "goals": goals,
                    "bmi": bmi,
                    "progress": progress,
                    "days_left_for_expiry": days_left_for_expiry,
                    "fee_history": fee_history,
                    "height": height,
                    "age": age,
                    "lifestyle": lifestyle,
                    "actual_weight": actual_weight,
                    "target_weight": target_weight,
                    "start_weight": start_weight,
                    "profile": client_profile,
                    "name": client_name,
                },
                "target_actual": target_actual,
                "assigned_plans": assigned_plans,
                "chart": chart,
            },
        }

    except FittbotHTTPException:
        raise
    except Exception as e:
        raise FittbotHTTPException(
            status_code=500,
            detail="Unable to retrieve home information",
            error_code="CLIENT_HOME_FETCH_ERROR",
            log_data={"exc": repr(e), "client_id": client_id, "gym_id": gym_id},
        )


@router.get("/get-unpaid-home")
async def get_unpaid_home(client_id: int, db: Session = Depends(get_db)):
    try:
        client = db.query(Client).filter(Client.client_id == client_id).one()
        gym_name = None
        gym_id = None
        joined = False
        gym_location = None

        if client.gym_id:
            gym = db.query(Gym).filter(Gym.gym_id == client.gym_id).one()
            gym_name = gym.name
            gym_id = gym.gym_id
            gym_location = gym.location
            joined = True

        return {
            "status": 200,
            "message": "Data retrived successfully",
            "data": {
                "client_name": client.name,
                "gym_id": gym_id,
                "gym_name": gym_name,
                "gym_location": gym_location,
                "joined": joined,
            },
        }
    except FittbotHTTPException:
        raise
    except Exception as e:
        raise FittbotHTTPException(
            status_code=500,
            detail=f"An error occurred while fetching unpaid home data",
            error_code="UNPAID_HOME_FETCH_ERROR",
            log_data={"exc": repr(e), "client_id": client_id},
        )


class InPunchRequest(BaseModel):
    gym_id: int
    client_id: int
    muscle: List


@router.post("/in_punch")
async def in_punch(
    http_request: Request,
    request: InPunchRequest,
    db: Session = Depends(get_db),
    redis: Redis = Depends(get_redis),
):
    try:
        current_date = date.today()
        current_time = datetime.now().time()

        client_id = request.client_id
        gym_id = request.gym_id

        existing_record = (
            db.query(Attendance)
            .filter(
                Attendance.client_id == request.client_id,
                Attendance.gym_id == request.gym_id,
                Attendance.date == current_date,
            )
            .first()
        )

        if existing_record:
            if existing_record.out_time is None:
                raise FittbotHTTPException(
                    status_code=400,
                    detail="Already punched in (1st). Punch out first.",
                    error_code="ALREADY_PUNCHED_IN_1ST",
                    log_data={"client_id": client_id, "gym_id": gym_id},
                )

            if existing_record.in_time_2 is None:
                existing_record.in_time_2 = current_time
                existing_record.muscle_2 = request.muscle

            elif existing_record.out_time_2 is None:
                raise FittbotHTTPException(
                    status_code=400,
                    detail="Already punched in (2nd). Punch out first.",
                    error_code="ALREADY_PUNCHED_IN_2ND",
                    log_data={"client_id": client_id, "gym_id": gym_id},
                )

            elif existing_record.in_time_3 is None:
                existing_record.in_time_3 = current_time
                existing_record.muscle_3 = request.muscle

            elif existing_record.out_time_3 is None:
                raise FittbotHTTPException(
                    status_code=400,
                    detail="Already punched in (3rd). Punch out first.",
                    error_code="ALREADY_PUNCHED_IN_3RD",
                    log_data={"client_id": client_id, "gym_id": gym_id},
                )

            else:
                raise FittbotHTTPException(
                    status_code=400,
                    detail="Maximum three punch-ins reached for today.",
                    error_code="MAX_PUNCH_INS_REACHED",
                    log_data={"client_id": client_id, "gym_id": gym_id},
                )

            db.commit()
            points = 0  

        else:
            
            new_attendance = Attendance(
                gym_id=request.gym_id,
                client_id=request.client_id,
                date=current_date,
                in_time=current_time,
                muscle=request.muscle,
            )
            db.add(new_attendance)
            db.commit()

            hour = current_time.hour
            hourly_agg_record = (
                db.query(DailyGymHourlyAgg)
                .filter(DailyGymHourlyAgg.gym_id == request.gym_id, DailyGymHourlyAgg.agg_date == current_date)
                .first()
            )
            if not hourly_agg_record:
                hourly_agg_record = DailyGymHourlyAgg(gym_id=request.gym_id, agg_date=current_date)
                db.add(hourly_agg_record)
                db.commit()
                db.refresh(hourly_agg_record)

            if 4 <= hour < 6:
                hourly_agg_record.col_4_6 += 1
            elif 6 <= hour < 8:
                hourly_agg_record.col_6_8 += 1
            elif 8 <= hour < 10:
                hourly_agg_record.col_8_10 += 1
            elif 10 <= hour < 12:
                hourly_agg_record.col_10_12 += 1
            elif 12 <= hour < 14:
                hourly_agg_record.col_12_14 += 1
            elif 14 <= hour < 16:
                hourly_agg_record.col_14_16 += 1
            elif 16 <= hour < 18:
                hourly_agg_record.col_16_18 += 1
            elif 18 <= hour < 20:
                hourly_agg_record.col_18_20 += 1
            elif 20 <= hour < 22:
                hourly_agg_record.col_20_22 += 1
            elif 22 <= hour < 24:
                hourly_agg_record.col_22_24 += 1
            db.commit()

            points = 50
            today = date.today()

            daily_record = (
                db.query(LeaderboardDaily)
                .filter(
                    LeaderboardDaily.client_id == client_id,
                    LeaderboardDaily.date == today,
                )
                .first()
            )
            if daily_record:
                daily_record.xp += points
            else:
                db.add(LeaderboardDaily(client_id=client_id, xp=points, date=today))

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
                monthly_record.xp += points
            else:
                db.add(LeaderboardMonthly(client_id=client_id, xp=points, month=month_date))

            overall_record = (
                db.query(LeaderboardOverall)
                .filter(LeaderboardOverall.client_id == client_id)
                .first()
            )
            if overall_record:
                overall_record.xp += points
                new_total = overall_record.xp

                next_row = (
                    db.query(ClientNextXp).filter_by(client_id=client_id).with_for_update().one_or_none()
                )

                def _tier_after(xp: int):
                    return (
                        db.query(RewardGym)
                        .filter_by(gym_id=gym_id)
                        .filter(RewardGym.xp > xp)
                        .order_by(asc(RewardGym.xp))
                        .first()
                    )

                if next_row and next_row.next_xp != 0:
                    if new_total >= next_row.next_xp:
                        client = db.query(Client).filter(Client.client_id == client_id).first()
                        db.add(
                            RewardPrizeHistory(
                                gym_id=gym_id,
                                client_id=client_id,
                                xp=next_row.next_xp,
                                gift=next_row.gift,
                                achieved_date=datetime.now(),
                                client_name=client.name if client else "",
                                is_given=False,
                                profile=client.profile if client else "",
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
                        db.query(RewardGym).filter_by(gym_id=gym_id).order_by(asc(RewardGym.xp)).first()
                    )
                    if first_tier:
                        if next_row:
                            next_row.next_xp = first_tier.xp
                            next_row.gift = first_tier.gift
                        else:
                            db.add(
                                ClientNextXp(
                                    client_id=client_id,
                                    next_xp=first_tier.xp,
                                    gift=first_tier.gift,
                                )
                            )
                db.commit()
            else:
                db.add(LeaderboardOverall(client_id=client_id, xp=points))
                db.commit()

            record_date = date.today()
            month_start_date = date(record_date.year, record_date.month, 1)
            analysis_record = (
                db.query(ClientGeneralAnalysis)
                .filter(
                    ClientGeneralAnalysis.client_id == client_id,
                    ClientGeneralAnalysis.date == month_start_date,
                )
                .first()
            )
            if analysis_record:
                analysis_record.attendance = (analysis_record.attendance or 0) + 1
                db.commit()
            else:
                db.add(
                    ClientGeneralAnalysis(client_id=client_id, date=month_start_date, attendance=1)
                )
                db.commit()

        # Invalidate today's attendance cache
        today = date.today()
        attendance_key = f"gym:{request.gym_id}:attendance:{today.strftime('%Y-%m-%d')}"
        analytics_key = f"gym:{request.gym_id}:analytics"
        client_analytics_key = f"gym:{request.gym_id}:client_analytics"
        if await redis.exists(attendance_key):
            await redis.delete(attendance_key)
        if await redis.exists(analytics_key):
            await redis.delete(analytics_key)
        if await redis.exists(client_analytics_key):
            await redis.delete(client_analytics_key)

        gym_count_record = db.query(LiveCount).filter(LiveCount.gym_id == request.gym_id).first()
        if not gym_count_record:
            gym_count_record = LiveCount(gym_id=request.gym_id, count=0)
            db.add(gym_count_record)
            db.commit()
            db.refresh(gym_count_record)
        gym_count_record.count += 1
        db.commit()
        db.refresh(gym_count_record)

        current_clients = (
            db.query(
                Attendance.client_id,
                Attendance.in_time,
                Attendance.in_time_2,
                Attendance.in_time_3,
                Client.name,
                Client.training_id,
                Client.goals,
                Client.profile,
                Attendance.muscle,
                Attendance.muscle_2,
                Attendance.muscle_3,
            )
            .join(Client, Attendance.client_id == Client.client_id)
            .filter(
                Attendance.date == date.today(),
                Client.gym_id == gym_id,
                or_(
                    and_(Attendance.in_time.isnot(None), Attendance.out_time.is_(None)),
                    and_(Attendance.in_time_2.isnot(None), Attendance.out_time_2.is_(None)),
                    and_(Attendance.in_time_3.isnot(None), Attendance.out_time_3.is_(None)),
                ),
            )
            .all()
        )

        goals_summary: Dict[str, Dict[str, Any]] = {}
        training_type_summary: Dict[str, Dict[str, Any]] = {}
        muscle_summary: Dict[str, Dict[str, Any]] = {}
        present_clients: List[Dict[str, Any]] = []

        for c in current_clients:
            goal_key = c.goals or "Unknown"
            goals_summary.setdefault(goal_key, {"count": 0, "clients": []})
            goals_summary[goal_key]["count"] += 1
            goals_summary[goal_key]["clients"].append(c.name)

            training_type = db.query(GymPlans.plans).filter(GymPlans.id == c.training_id).scalar()
            training_key = training_type or "Unknown"
            training_type_summary.setdefault(training_key, {"count": 0, "clients": []})
            training_type_summary[training_key]["count"] += 1
            training_type_summary[training_key]["clients"].append(c.name)

            for muscle in (c.muscle or []):
                muscle_summary.setdefault(muscle, {"count": 0, "clients": []})
                muscle_summary[muscle]["count"] += 1
                muscle_summary[muscle]["clients"].append(c.name)

            present_clients.append({"name": c.name, "profile": c.profile})

        top_goal = max(goals_summary.items(), key=lambda x: x[1]["count"], default=(None, {}))[0]
        top_training_type = max(
            training_type_summary.items(), key=lambda x: x[1]["count"], default=(None, {})
        )[0]
        top_muscle = max(muscle_summary.items(), key=lambda x: x[1]["count"], default=(None, {}))[0]

        male_url = female_url = ""
        if top_muscle:
            pics = (
                db.query(FittbotMuscleGroup.gender, FittbotMuscleGroup.url)
                .filter(FittbotMuscleGroup.muscle_group == top_muscle)
                .all()
            )
            if pics:
                pics_map = {g: u for g, u in pics}
                male_url = pics_map.get("male", "")
                female_url = pics_map.get("female", "")

        message = {
            "action": "get_initial_data",
            "live_count": gym_count_record.count,
            "total_present": len(current_clients),
            "goals_summary": goals_summary,
            "training_type_summary": training_type_summary,
            "muscle_summary": muscle_summary,
            "top_goal": top_goal,
            "top_training_type": top_training_type,
            "top_muscle": top_muscle,
            "present_clients": present_clients,
            "male_url": male_url,
            "female_url": female_url,
        }

        await http_request.app.state.live_hub.publish(request.gym_id, message)

        today = date.today()
        attendance = (
            db.query(AttendanceGym)
            .filter(AttendanceGym.gym_id == request.gym_id, AttendanceGym.date == today)
            .first()
        )
        if attendance:
            attendance.attendance_count += 1
        else:
            attendance = AttendanceGym(gym_id=gym_id, date=today, attendance_count=1)
            db.add(attendance)
        db.commit()
        db.refresh(attendance)

        redis_key = f"gym:{request.gym_id}:gym_attendance"
        if await redis.exists(redis_key):
            await redis.delete(redis_key)

        return {"status": 200, "message": "In-punch recorded successfully", "reward_point": points}

    except FittbotHTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise FittbotHTTPException(
            status_code=500,
            detail=f"An unexpected error occurred during in-punch",
            error_code="IN_PUNCH_ERROR",
            log_data={"exc": repr(e), "client_id": request.client_id, "gym_id": request.gym_id},
        )


class OutPunchRequest(BaseModel):
    gym_id: int
    client_id: int
    

@router.post("/out_punch")
async def out_punch(http_request: Request,request: OutPunchRequest, db: Session = Depends(get_db), redis: Redis = Depends(get_redis)):


    try:
        current_date = date.today()
        current_time = datetime.now().time()

        attendance_record = db.query(Attendance).filter(
            Attendance.client_id == request.client_id,
            Attendance.gym_id == request.gym_id,
            Attendance.date == current_date 
        ).first()

        if not attendance_record:

            raise HTTPException(
                status_code=404,
                detail="No in-punch record found for today, or client has already punched out."
            )

        today=date.today()

        if attendance_record.out_time is None:
            attendance_record.out_time = current_time
            in_dt  = datetime.combine(today, attendance_record.in_time)

        elif attendance_record.in_time_2 and attendance_record.out_time_2 is None:
            attendance_record.out_time_2 = current_time
            in_dt  = datetime.combine(today, attendance_record.in_time_2)

        elif attendance_record.in_time_3 and attendance_record.out_time_3 is None:
            attendance_record.out_time_3 = current_time
            in_dt  = datetime.combine(today, attendance_record.in_time_3)

        else:
            raise HTTPException(status_code=400,
                                detail="All three sessions already punched out.")


        db.commit()
        # Invalidate today's attendance cache
        today = date.today()
        attendance_key = f"gym:{request.gym_id}:attendance:{today.strftime('%Y-%m-%d')}"
        analytics_key = f"gym:{request.gym_id}:analytics"
        client_analytics_key = f"gym:{request.gym_id}:client_analytics"



        if await redis.exists(attendance_key):
            await redis.delete(attendance_key)
            
        if await redis.exists(analytics_key):
            await redis.delete(analytics_key)

        if await redis.exists(client_analytics_key):
            await redis.delete(client_analytics_key)


        gym_count_record = db.query(LiveCount).filter(LiveCount.gym_id == request.gym_id).first()
        if not gym_count_record:
            raise HTTPException(status_code=404, detail="Gym live count not found.")

        if gym_count_record.count > 0:
            gym_count_record.count -= 1
        else:
            pass

        db.commit()
        db.refresh(gym_count_record)

        current_clients = (
        db.query(
            Attendance.client_id,
            Attendance.in_time,
            Attendance.in_time_2,
            Attendance.in_time_3,
            Client.name,
            Client.training_id,
            Client.goals,
            Client.profile,
            Attendance.muscle,
            Attendance.muscle_2,
            Attendance.muscle_3,
        )
        .join(Client, Attendance.client_id == Client.client_id)
        .filter(
            Attendance.date == date.today(),
            Client.gym_id == request.gym_id,
            or_(
                and_(Attendance.in_time.isnot(None),   Attendance.out_time.is_(None)),
                and_(Attendance.in_time_2.isnot(None), Attendance.out_time_2.is_(None)),
                and_(Attendance.in_time_3.isnot(None), Attendance.out_time_3.is_(None)),
            )
        )
        .all())

        goals_summary = {}
        training_type_summary = {}
        muscle_summary = {}
        present_clients = []

        for client in current_clients:
            goal_key = client.goals or "Unknown"
            goals_summary.setdefault(goal_key, {"count": 0, "clients": []})
            goals_summary[goal_key]["count"] += 1
            goals_summary[goal_key]["clients"].append(client.name)

            training_type = (
                db.query(GymPlans.plans)
                .filter(GymPlans.id == client.training_id)
                .scalar()
            )
            training_key = training_type or "Unknown"
            training_type_summary.setdefault(training_key, {"count": 0, "clients": []})
            training_type_summary[training_key]["count"] += 1
            training_type_summary[training_key]["clients"].append(client.name)

            for muscle in client.muscle:
                muscle_summary.setdefault(muscle, {"count": 0, "clients": []})
                muscle_summary[muscle]["count"] += 1
                muscle_summary[muscle]["clients"].append(client.name)

            present_clients.append({"name": client.name, "profile": client.profile})

        top_goal = max(goals_summary.items(), key=lambda x: x[1]["count"], default=(None, {}))[0]
        top_training_type = max(
            training_type_summary.items(), key=lambda x: x[1]["count"], default=(None, {})
        )[0]
        top_muscle = max(muscle_summary.items(), key=lambda x: x[1]["count"], default=(None, {}))[0]

        male_url   = female_url = None       
        if top_muscle:
            pics = (
                db.query(FittbotMuscleGroup.gender, FittbotMuscleGroup.url)
                .filter(FittbotMuscleGroup.muscle_group == top_muscle)
                .all()
            )
            if pics:
                pics_map = {g: u for g, u in pics}
                male_url   = pics_map.get("male")
                female_url = pics_map.get("female")
            else:
                male_url   = ""
                female_url = ""


        else:
            male_url   = ""
            female_url = ""



        message = {
            "action": "get_initial_data",
            "live_count": gym_count_record.count,
            "total_present": len(current_clients),
            "goals_summary": goals_summary,
            "training_type_summary": training_type_summary,
            "muscle_summary": muscle_summary,
            "top_goal": top_goal,
            "top_training_type": top_training_type,
            "top_muscle": top_muscle,
            "present_clients": present_clients,
            "male_url":male_url,
            "female_url":female_url
            
        }

        await http_request.app.state.live_hub.publish(request.gym_id, message)

        today=date.today()
        current_time  = datetime.now().time() 
        out_dt = datetime.combine(date.today(),current_time )
        duration_minutes = (out_dt - in_dt).total_seconds() / 60

        current_year = today.year
        agg = (
            db.query(ClientActualAggregated)
              .filter(
                  ClientActualAggregated.client_id == request.client_id,
                  ClientActualAggregated.year      == current_year
              )
              .first()
        )


        if agg:
            if agg.gym_time:

                agg.gym_time  = int((agg.gym_time + duration_minutes) / 2)

            else:
                agg.gym_time  = int(duration_minutes)

        else:
            agg = ClientActualAggregated(
                client_id   = request.client_id,
                year        = current_year,
                gym_time    = int(duration_minutes),
                created_at  = datetime.now(),
                updated_at  = datetime.now()
            )
            db.add(agg)
        db.commit()



        return {"status": 200, "message": "Out-punch recorded successfully","data":current_time}


    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred {e}")



@router.get("/workout_insights")
async def get_workout_insights(client_id: int, db: Session = Depends(get_db)):
    try:
        
        current_year = datetime.now().year
        year_data = db.query(AggregatedInsights).filter(
            AggregatedInsights.client_id == client_id,
            AggregatedInsights.week_start >= datetime(current_year, 1, 1),
            AggregatedInsights.week_start < datetime(current_year + 1, 1, 1)
        ).order_by(AggregatedInsights.week_start.desc()).all()

        if not year_data:
            comparison_comment = ["There is no data to compare, Please start workout to get analysis"]

        if len(year_data) < 2:
            comparison_comment = ["Not enough data to compare the last two weeks."]
        else:
            latest = year_data[0]
            previous = year_data[1]
            volume_change = round(((latest.total_volume - previous.total_volume) / previous.total_volume) * 100, 2) if previous.total_volume else 0
            avg_weight_change = round(((latest.avg_weight - previous.avg_weight) / previous.avg_weight) * 100, 2) if previous.avg_weight else 0
            avg_reps_change = round(((latest.avg_reps - previous.avg_reps) / previous.avg_reps) * 100, 2) if previous.avg_reps else 0
            comparison_comment = []
            if volume_change > 0:
                comparison_comment.append(f"Great job! Your total workout volume increased by {volume_change}%.")
            elif volume_change < 0:
                comparison_comment.append(f"Your total workout volume decreased by {abs(volume_change)}%. Focus on consistency.")

            if avg_weight_change > 0:
                comparison_comment.append(f"You're lifting heavier weights! Average weight increased by {avg_weight_change}%.")
            elif avg_weight_change < 0:
                comparison_comment.append(f"Average weight dropped by {abs(avg_weight_change)}%. Consider increasing the intensity.")

            if avg_reps_change > 0:
                comparison_comment.append(f"You're doing more reps! Average reps increased by {avg_reps_change}%.")
            elif avg_reps_change < 0:
                comparison_comment.append(f"Average reps decreased by {abs(avg_reps_change)}%. Focus on endurance.")

            if not comparison_comment:
                comparison_comment = ["Your performance is consistent with the previous week."]


        muscle_data_aggregated = db.query(MuscleAggregatedInsights).filter(
            MuscleAggregatedInsights.client_id == client_id
        ).all()

        aggregated_muscle_insights = {
            "total_volume": [],
            "avg_weight": [],
            "avg_reps": [],
            "max_weight": [],
            "max_reps": [],
            "rest_days": []
        }

        if not muscle_data_aggregated:
            aggregated_muscle_insights = {
                "total_volume": [],
                "avg_weight": [],
                "avg_reps": [],
                "max_weight": [],
                "max_reps": [],
                "rest_days": []
            }
        else:
            for record in muscle_data_aggregated:
                muscle_group = record.muscle_group
                aggregated_muscle_insights["total_volume"].append({"label": muscle_group, "value": record.total_volume})
                aggregated_muscle_insights["avg_weight"].append({"label": muscle_group, "value": record.avg_weight})
                aggregated_muscle_insights["avg_reps"].append({"label": muscle_group, "value": record.avg_reps})
                aggregated_muscle_insights["max_weight"].append({"label": muscle_group, "value": record.max_weight})
                aggregated_muscle_insights["max_reps"].append({"label": muscle_group, "value": record.max_reps})
                aggregated_muscle_insights["rest_days"].append({"label": muscle_group, "value": record.rest_days})


        muscle_data = db.query(ClientWeeklyPerformance).filter(
            ClientWeeklyPerformance.client_id == client_id,
            ClientWeeklyPerformance.week_start >= datetime(current_year, 1, 1),
            ClientWeeklyPerformance.week_start < datetime(current_year + 1, 1, 1)
        ).all()

        muscle_insights = {}
        muscle_group_list = []
        muscle_group_ids = {}
        next_id = 1

        if not muscle_data:
            muscle_insights = {}
            muscle_group_list = []
            muscle_group_ids = {}
        else:
            for record in muscle_data:
                muscle_group = record.muscle_group

                if muscle_group not in muscle_insights:
                    muscle_insights[muscle_group] = {
                        "weekly_data": {
                            "total_volume": [],
                            "avg_weight": [],
                            "avg_reps": []
                        }
                    }
                    muscle_group_ids[muscle_group] = next_id
                    muscle_group_list.append({"id": next_id, "name": muscle_group})
                    next_id += 1

                muscle_insights[muscle_group]["weekly_data"]["total_volume"].append({
                    "week_start": record.week_start.isoformat(),
                    "value": record.total_volume
                })
                muscle_insights[muscle_group]["weekly_data"]["avg_weight"].append({
                    "week_start": record.week_start.isoformat(),
                    "value": record.avg_weight
                })
                muscle_insights[muscle_group]["weekly_data"]["avg_reps"].append({
                    "week_start": record.week_start.isoformat(),
                    "value": record.avg_reps
                })

        response = {
            "status":200,
            "data":{
                "overall_data": [
                {
                    "week_start": record.week_start.isoformat(),
                    "total_volume": record.total_volume,
                    "avg_weight": record.avg_weight,
                    "avg_reps": record.avg_reps
                }
                for record in year_data
            ],
            "comparison_comment": comparison_comment,
            "aggregated_muscle_insights": aggregated_muscle_insights,
            "muscle_insights": muscle_insights,
            "muscle_group_list": muscle_group_list
            }  
        }
        return response

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occured: {str(e)}")




@router.get("/diet_analysis")
async def get_diet_analysis(client_id: int, db: Session = Depends(get_db)):
    try:
        current_year = datetime.now().year
        year_data = db.query(ClientActualAggregated).filter(
            ClientActualAggregated.client_id == client_id,
            ClientActualAggregated.year == current_year
        ).first()

        if not year_data:
            total_calories_from_protein = 0
            total_calories_from_carbs = 0
            total_calories_from_fats = 0
            total_calories = (
                total_calories_from_protein +
                total_calories_from_carbs +
                total_calories_from_fats
            )
        else:
            total_calories_from_protein = year_data.avg_protein * 4
            total_calories_from_carbs = year_data.avg_carbs * 4
            total_calories_from_fats = year_data.avg_fats * 9
            total_calories = (
                total_calories_from_protein +
                total_calories_from_carbs +
                total_calories_from_fats
            )

        if total_calories == 0:
            protein_percentage = 0
            carbs_percentage = 0
            fats_percentage = 0
        else:
            protein_percentage = round((total_calories_from_protein / total_calories) * 100, 2)
            carbs_percentage = round((total_calories_from_carbs / total_calories) * 100, 2)
            fats_percentage = round((total_calories_from_fats / total_calories) * 100, 2)

        macro_split = {
            "protein_percentage": protein_percentage,
            "carbs_percentage": carbs_percentage,
            "fats_percentage": fats_percentage
        }

        if not year_data:
            stats = {
                "no_of_days_calories_met": 0,
                "calories_surplus_days": 0,
                "calories_deficit_days": 0,
                "longest_streak": 0,
                "average_protein_target": 0,
                "average_carbs_target": 0,
                "average_fat_target": 0
            }
        else:
            stats = {
                "no_of_days_calories_met": year_data.no_of_days_calories_met,
                "calories_surplus_days": year_data.calories_surplus_days,
                "calories_deficit_days": year_data.calories_deficit_days,
                "longest_streak": year_data.longest_streak,
                "average_protein_target": year_data.average_protein_target,
                "average_carbs_target": year_data.average_carbs_target,
                "average_fat_target": year_data.average_fat_target
            }

        
        weekly_data = db.query(ClientActualAggregatedWeekly).filter(
            ClientActualAggregatedWeekly.client_id == client_id,
            ClientActualAggregatedWeekly.week_start >= datetime(current_year, 1, 1),
            ClientActualAggregatedWeekly.week_start < datetime(current_year + 1, 1, 1)
        ).order_by(ClientActualAggregatedWeekly.week_start.asc()).all()

        weekly_chart_data = {
            "calories": [],
            "protein": [],
            "carbs": [],
            "fats": [],
            "fiber":[],
            "sugar":[],
        }

        if not weekly_data:
            weekly_chart_data = {
                "calories": [],
                "protein": [],
                "carbs": [],
                "fats": [],
                "fiber":[],
                "sugar":[],
            }
        else:
            for record in weekly_data:
                weekly_chart_data["calories"].append({
                    "label": "calories",
                    "date": record.week_start.isoformat(),
                    "value": record.avg_calories
                })
                weekly_chart_data["protein"].append({
                    "label": "protein",
                    "date": record.week_start.isoformat(),
                    "value": record.avg_protein
                })
                weekly_chart_data["carbs"].append({
                    "label": "carbs",
                    "date": record.week_start.isoformat(),
                    "value": record.avg_carbs
                })
                weekly_chart_data["fats"].append({
                    "label": "fats",
                    "date": record.week_start.isoformat(),
                    "value": record.avg_fats
                })
                weekly_chart_data["fiber"].append({
                    "label": "fiber",
                    "date": record.week_start.isoformat(),
                    "value": record.avg_fiber if record.avg_fiber else 0
                })
                weekly_chart_data["sugar"].append({
                    "label": "sugar",
                    "date": record.week_start.isoformat(),
                    "value": record.avg_sugar if record.avg_sugar else 0
                })

        
        response = {
            "status":200,
            "data":{
                "macro_split": macro_split,
                "stats": stats,
                "weekly_data": weekly_chart_data
            }   
        }

        return response

    except Exception as e:
        return {"error": str(e)}




@router.get("/client_general_analysis")
async def get_client_general_analysis(client_id: int, db: Session = Depends(get_db)):
    try:
        data = db.query(ClientGeneralAnalysis).filter(
            ClientGeneralAnalysis.client_id == client_id
        ).order_by(ClientGeneralAnalysis.date.asc()).all()

        if not data:
            raise HTTPException(status_code=404, detail="No data available for the specified client.")

        monthly_data = {
            "weight": [],
            "water_taken": [],
            "attendance":[],
            "burnt_calories":[]
        }

        for record in data:
            month_name = record.date  
            monthly_data["weight"].append({
                "label": month_name,
                "value": record.weight if record.weight else 0
            })
            monthly_data["water_taken"].append({
                "label": month_name,
                "value": record.water_taken if record.water_taken else  0
            })
            monthly_data["burnt_calories"].append({
                "label": month_name,
                "value": record.burnt_calories if record.burnt_calories else 0
            })
            monthly_data["attendance"].append({
                "label": month_name,
                "value": record.attendance if record.attendance else 0
            })
            

        current_year = datetime.now().year
        aggregated_data = db.query(ClientActualAggregated).filter(
            ClientActualAggregated.client_id == client_id,
            ClientActualAggregated.year == current_year
        ).first()

        if not aggregated_data:
            gym_time={"hour":0,"minutes":0}
        
        else:
            actual_gym_time=aggregated_data.gym_time

            if actual_gym_time:

                if actual_gym_time >= 60:
                    hours   = actual_gym_time // 60
                    minutes = actual_gym_time % 60
                    gym_time = {"hour": hours, "minutes": minutes}
                else:

                    gym_time={"hour":0,"minutes":actual_gym_time}
            
            else:
                gym_time={"hour":0,"minutes":0}



        response = {
            "status":200,
            "data":{
            "monthly_data": monthly_data,
            "total_gym_time": gym_time
            }
        }
        return response
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")
  

@router.get("/get_client_report")
async def get_client_data( client_id: int, date:str,db: Session = Depends(get_db)):
    try:
        try:
            input_date = datetime.strptime(date, "%Y-%m-%d").date()
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid date format. Use 'YYYY-MM-DD'.")

        response = {}

        attendance = db.query(Attendance).filter(
            Attendance.client_id == client_id,
            Attendance.date == input_date
        ).first()


        attendance_dict = {
            "in_time": None, "out_time": None,
            "in_time_2": None, "out_time_2": None,
            "in_time_3": None, "out_time_3": None,
        }
        total_minutes = 0

        if attendance:
            for suf in ["", "_2", "_3"]:
                in_attr  = getattr(attendance, f"in_time{suf}")
                out_attr = getattr(attendance, f"out_time{suf}")
                in_key   = f"in_time{suf}"  if suf else "in_time"
                out_key  = f"out_time{suf}" if suf else "out_time"


                attendance_dict[in_key]  = in_attr.strftime("%I:%M %p") if in_attr else None
                attendance_dict[out_key] = out_attr.strftime("%I:%M %p") if out_attr else None


                if in_attr and out_attr:
                    diff = datetime.combine(input_date, out_attr) - datetime.combine(input_date, in_attr)
                    total_minutes += diff.total_seconds() // 60

        if total_minutes:
            if total_minutes < 60:
                time_spent = f"{int(total_minutes)} mins"
            else:
                hrs, mins = divmod(int(total_minutes), 60)
                time_spent = f"{hrs}h {mins}mins"
        else:
            time_spent = None

        response["attendance"]   = attendance_dict
        response["time_spent"]=time_spent

        client_actual = db.query(ClientActual).filter(
            ClientActual.client_id == client_id,
            ClientActual.date == input_date
        ).first()

        client_target= db.query(ClientTarget).filter(ClientTarget.client_id == client_id).first()

        if client_actual:
            response["client_actual"] = {
                "calories": {
                    "actual": client_actual.calories,
                    "target": client_target.calories
                },
                "protein": {
                    "actual": client_actual.protein,
                    "target": client_target.protein
                },
                "fat": {
                    "actual": client_actual.fats,
                    "target": client_target.fat
                },
                "carbs": {
                    "actual": client_actual.carbs,
                    "target": client_target.carbs
                },
                "fiber": {"actual": client_actual.fiber, "target": client_target.fiber} if client_target and client_target.fiber is not None else {"actual": client_actual.fiber, "target": None},
                "sugar": {"actual": client_actual.sugar, "target": client_target.sugar} if client_target and client_target.sugar is not None else {"actual": client_actual.sugar, "target": None},
                "calcium": {"actual": client_actual.calcium, "target": client_target.calcium} if client_target and client_target.calcium is not None else {"actual": client_actual.calcium, "target": None},
                "magnesium": {"actual": client_actual.magnesium, "target": client_target.magnesium} if client_target and client_target.magnesium is not None else {"actual": client_actual.magnesium, "target": None},
                "potassium": {"actual": client_actual.potassium, "target": client_target.potassium} if client_target and client_target.potassium is not None else {"actual": client_actual.potassium, "target": None},
                "Iodine": {"actual": client_actual.Iodine, "target": client_target.Iodine} if client_target and client_target.Iodine is not None else {"actual": client_actual.Iodine, "target": None},
                "Iron": {"actual": client_actual.Iron, "target": client_target.Iron} if client_target and client_target.Iron is not None else {"actual": client_actual.Iron, "target": None},
            }
        else:
            if client_target:
                response["client_actual"] = {
                    "calories": {"actual": None, "target": client_target.calories},
                    "protein": {"actual": None, "target": client_target.protein},
                    "fat": {"actual": None, "target": client_target.fat},
                    "carbs": {"actual": None, "target": client_target.carbs},
                    "fiber": {"actual": None, "target": client_target.fiber} if client_target.fiber is not None else {"actual": None, "target": None},
                    "sugar": {"actual": None, "target": client_target.sugar} if client_target.sugar is not None else {"actual": None, "target": None},
                    "sugar": {"actual": None, "target": client_target.sugar} if client_target.sugar is not None else {"actual": None, "target": None},
                    "calcium": {"actual": None, "target": client_target.calcium} if client_target.calcium is not None else {"actual": None, "target": None},
                    "magnesium": {"actual": None, "target": client_target.magnesium} if client_target.magnesium is not None else {"actual": None, "target": None},
                    "potassium": {"actual": None, "target": client_target.potassium} if client_target.potassium is not None else {"actual": None, "target": None},
                    "Iodine": {"actual": None, "target": client_target.Iodine} if client_target.Iodine is not None else {"actual": None, "target": None},
                    "Iron": {"actual": None, "target": client_target.Iron} if client_target.Iron is not None else {"actual": None, "target": None},
                }
            else:
                response["client_actual"] = {
                    "calories": {"actual": None, "target": None},
                    "protein": {"actual": None, "target": None},
                    "fat": {"actual": None, "target": None},
                    "carbs": {"actual": None, "target": None},
                    "fiber": {"actual": None, "target": None},
                    "sugar": {"actual": None, "target": None},
                    "calcium": {"actual": None, "target": None},
                    "magnesium": {"actual": None, "target": None},
                    "potassium": {"actual": None, "target": None},
                    "Iodine": {"actual": None, "target": None},
                    "Iron": {"actual": None, "target": None},
                }

        diet_data = db.query(ActualDiet).filter(
            ActualDiet.client_id == client_id,
            ActualDiet.date == input_date
        ).first()

        if diet_data:
            response["diet_data"] = diet_data.diet_data
        else:
            response["diet_data"] = None

        workout_data = db.query(ActualWorkout).filter(
            ActualWorkout.client_id == client_id,
            ActualWorkout.date == input_date
        ).first()

        if workout_data:
            data=workout_data.workout_details
            exercise_names = set()
            for record in data:
                for category in record.values():
                    for exercise in category:
                        exercise_names.add(exercise["name"])

            unique_exercise_count = len(exercise_names)


            response["workout"] = {
                "workout_details": workout_data.workout_details if workout_data else None,
                "muscle_group": attendance.muscle if attendance else [],
                "count":unique_exercise_count,
                "duration":time_spent or 0

            }
        else:
            response["workout"]=None

        target= client_target.water_intake if client_target else None
        actual= client_actual.water_intake if client_actual else 0
        water_intake= {"target":target, "actual":actual}
        response["water_intake"]=water_intake

        client = db.query(Client).filter(Client.client_id == client_id).first()
        if client:
            leaderboard_entry = db.query(LeaderboardOverall).filter(
                LeaderboardOverall.client_id == client_id,
                LeaderboardOverall.gym_id == client.gym_id
            ).first()
            
            if leaderboard_entry:
                position = db.query(LeaderboardOverall).filter(
                    LeaderboardOverall.gym_id == leaderboard_entry.gym_id,
                    LeaderboardOverall.xp > leaderboard_entry.xp
                ).count() + 1
                
                total_participants = db.query(LeaderboardOverall).filter(
                    LeaderboardOverall.gym_id == leaderboard_entry.gym_id
                ).count()
                
                badge = db.query(RewardBadge).filter(
                    RewardBadge.min_points <= leaderboard_entry.xp,
                    RewardBadge.max_points >= leaderboard_entry.xp
                ).first()
                
                badge_details = None
                if badge:
                    badge_details = {
                        "badge_name": badge.badge,
                        "level": badge.level,
                        "image_url": badge.image_url,
                        "min_points": badge.min_points,
                        "max_points": badge.max_points
                    }
                
                response["leaderboard"] = {
                    "position": position,
                    "total_participants": total_participants,
                    "xp": leaderboard_entry.xp,
                    "badge": badge_details
                }
            else:
                response["leaderboard"] = {
                    "position": None,
                    "total_participants": 0,
                    "xp": 0,
                    "badge": None
                }
        else:
            response["leaderboard"] = {
                "position": None,
                "total_participants": 0,
                "xp": 0,
                "badge": None
            }

        client = db.query(Client).filter(Client.client_id == client_id).first()
        response["bmi"] = client.bmi if client and client.bmi else None

        current_year = input_date.year
        client_aggregated = db.query(ClientActualAggregated).filter(
            ClientActualAggregated.client_id == client_id,
            ClientActualAggregated.year == current_year
        ).first()
        
        if client_aggregated:
            response["diet_streak"] = {
                "current_streak": client_aggregated.current_streak,
                "longest_streak": client_aggregated.longest_streak
            }
        else:
            response["diet_streak"] = {
                "current_streak": 0,
                "longest_streak": 0
            }

        if workout_data and workout_data.workout_details:
            muscle_group_performance = {}
            
            for record in workout_data.workout_details:
                for muscle_group, exercises in record.items():
                    if muscle_group not in muscle_group_performance:
                        muscle_group_performance[muscle_group] = {
                            "total_weight": 0,
                            "total_reps": 0,
                            "total_sets": 0,
                            "exercise_count": 0
                        }
                    
                    for exercise in exercises:
                        muscle_group_performance[muscle_group]["exercise_count"] += 1
                        
                        if "sets" in exercise:
                            for set_data in exercise["sets"]:
                                if "weight" in set_data and set_data["weight"]:
                                    muscle_group_performance[muscle_group]["total_weight"] += set_data["weight"]
                                if "reps" in set_data and set_data["reps"]:
                                    muscle_group_performance[muscle_group]["total_reps"] += set_data["reps"]
                                muscle_group_performance[muscle_group]["total_sets"] += 1
            
            top_muscle_group = None
            max_score = 0
            
            for muscle_group, performance in muscle_group_performance.items():
                volume = performance["total_weight"] * performance["total_reps"]
                score = volume + (performance["total_sets"] * 10) + (performance["exercise_count"] * 5)
                
                if score > max_score:
                    max_score = score
                    top_muscle_group = muscle_group
            
            response["top_performing_muscle_group"] = {
                "muscle_group": top_muscle_group,
                "performance_details": muscle_group_performance.get(top_muscle_group, {}) if top_muscle_group else {}
            }
        else:
            response["top_performing_muscle_group"] = {
                "muscle_group": None,
                "performance_details": {}
            }

        return {"status": 200, "message": "Data fetched successfully", "data": response}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")



@router.get("/manual_client_workout_template")
async def get_client_workout_templates(
    client_id: int = Query(...), db: Session = Depends(get_db)
):
    try:
        templates = db.query(ClientWorkoutTemplate).filter(ClientWorkoutTemplate.client_id == client_id).all()
        
        temp = [
            {"id": template.id, "name": template.template_name, "exercise_data": template.exercise_data}
            for template in templates
        ]
        
        return {"status": 200, "message": "Template listed successfully", "data": temp}
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Error adding template: {str(e)}")



class AddWorkoutTemplateRequest(BaseModel):
    client_id: int
    template_name: str
    exercise_data: Dict[str, list]  

class UpdateWorkoutTemplateRequest(BaseModel):
    id: int
    exercise_data: Dict

class EditWorkoutTemplateNameRequest(BaseModel):
    id: int
    template_name: str




@router.post("/manual/add_workout_template")
async def add_workout_template(
    request: AddWorkoutTemplateRequest, 
    db: Session = Depends(get_db)
):

    existing = (
        db.query(ClientWorkoutTemplate)
          .filter(
              ClientWorkoutTemplate.client_id == request.client_id,
              ClientWorkoutTemplate.template_name == request.template_name
          )
          .first()
    )
    if existing:
        raise HTTPException(
            status_code=400, 
            detail=f"Template name '{request.template_name}' is already there"
        )

    try:
        new_template = ClientWorkoutTemplate(
            client_id      = request.client_id,
            template_name  = request.template_name,
            exercise_data  = request.exercise_data
        )
        db.add(new_template)
        db.commit()
        db.refresh(new_template)

        return {
            "status": 200,
            "message": "Workout template added successfully",
            "data": {
                "id":            new_template.id,
                "name":          new_template.template_name,
                "exercise_data": new_template.exercise_data,
                "client_id":     new_template.client_id
            }
        }
    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=500, 
            detail=f"Error adding workout template: {str(e)}"
        )



@router.put("/manual/update_workout_template")
async def update_workout_template(
    request: UpdateWorkoutTemplateRequest, db: Session = Depends(get_db)
):
    try:
        template = db.query(ClientWorkoutTemplate).filter(ClientWorkoutTemplate.id == request.id).first()
        if not template:
            raise HTTPException(status_code=404, detail="Template not found with the given ID")

        template.exercise_data = request.exercise_data
        db.commit()
        db.refresh(template)

        return {"status": 200, "message": "Exercise updated successfully"}
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Error updating workout template: {str(e)}")


@router.put("/manual/edit_workout_template_name")
async def edit_workout_template_name(
    request: EditWorkoutTemplateNameRequest, db: Session = Depends(get_db)
):

    try:
        result = db.query(ClientWorkoutTemplate).filter(
            ClientWorkoutTemplate.id == request.id
        ).update({"template_name": request.template_name})
        db.commit()

        if not result:
            raise HTTPException(status_code=404, detail="Template not found")

        return {"status": 200, "message": "Workout template name updated successfully"}
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Error updating workout template name: {str(e)}")


@router.delete("/manual/delete_workout_template")
async def delete_workout_template(
    id: int, db: Session = Depends(get_db)
):

    try:
        result = db.query(ClientWorkoutTemplate).filter(
            ClientWorkoutTemplate.id == id
        ).delete()
        db.commit()

        if result == 0:
            raise HTTPException(status_code=404, detail="Template not found with the given ID")

        return {"status": 200, "message": "Workout template deleted successfully"}
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Error deleting workout template: {str(e)}")
    

@router.get("/get_fittbot_workout")
def get_fittbot_workout( client_id:int, db: Session = Depends(get_db)):
 
    workout_entry = db.query(FittbotWorkout).first()
    client=db.query(Client).filter(Client.client_id == client_id).first()
 
    if not workout_entry:
        raise HTTPException(status_code=404, detail="No workout data found.")
 
    exercise_data = workout_entry.exercise_data
    muscle_groups = list(exercise_data.keys())  
 
    return {
        "status":200,
        "data":{
            "muscle_groups": muscle_groups,
            "exercise_data": exercise_data,
            'client_weight':client.weight
        }
    }
 
 
@router.get("/attendance_status")
async def check_attendance_status(client_id: int, db: Session = Depends(get_db)):

    try:
        date = datetime.now().date()

        record = db.query(Attendance).filter(
            Attendance.client_id == client_id,
            Attendance.date == date
        ).first()

        if record and record.in_time and not record.out_time:
            attendance_status = True
        elif record and record.in_time_2 and not record.out_time_2:
            attendance_status = True
        elif record and record.in_time_3 and not record.out_time_3:

            attendance_status = True
        else:
            attendance_status = False
        

        return {"status":200,"attendance_status":attendance_status}  

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching attendance: {str(e)}")
    


class AddDietTemplateRequest(BaseModel):
    client_id: int
    template_name: str
    diet_data: list  

class UpdateDietTemplateRequest(BaseModel):
    id: int
    diet_data: list

class EditDietTemplateNameRequest(BaseModel):
    id: int
    template_name: str

@router.get("/get_diet_template")
async def get_client_diet_templates(
    method:str, client_id: int = Query(...), db: Session = Depends(get_db)
   
):
    try:
        if method=="personal":
            templates = db.query(ClientDietTemplate).filter(ClientDietTemplate.client_id == client_id).all()
           
            temp = [
                {"id": template.id, "name": template.template_name, "diet_data": template.diet_data}
                for template in templates
            ]
           
            return {"status": 200, "message": "Template listed successfully", "data": temp}
 
        elif method == "gym":
            client_scheduler = (
                db.query(ClientScheduler)
                .filter(ClientScheduler.client_id == client_id)
                .first()
            )
            if not client_scheduler or client_scheduler.assigned_dietplan is None:
                return {
                    "status": 200,
                    "message": "No diet has been assigned",
                    "data": []
                }
 
            diet_plan = (
                db.query(TemplateDiet)
                .filter(TemplateDiet.template_id == client_scheduler.assigned_dietplan)
                .first()
            )
            if not diet_plan or not isinstance(diet_plan.template_details, dict):
                return {
                    "status": 200,
                    "message": "Assigned diet plan not found or invalid format",
                    "data": []
                }
 
 
            output = [
                {
                    "id": idx,
                    "name": plan_name,
                    "diet_data": plan_sections
                }
                for idx, (plan_name, plan_sections) in enumerate(
                    diet_plan.template_details.items(), start=1
                )
            ]

            return {
                "status": 200,
                "message": "Template listed successfully",
                "data": output
            }
 
       
    except Exception as e:
        db.rollback()
        print(f"Error listing template: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error listing template: {str(e)}")
 
@router.get('/get_single_diet_template')
async def get_single_template(id:int, db:Session=Depends(get_db)):
    try:
        template = db.query(ClientDietTemplate).filter(ClientDietTemplate.id == id).first()
 
        template_data={
            "id":template.id,
            "name":template.template_name,
            "diet_data":template.diet_data
        }
        return{
            "status":200,
            "message":"Template retrived successfully",
            "data":template_data
        }
    except Exception as e:
        print(str(e))
        raise HTTPException(status_code=500, detail=f'An error Occured : {str(e)}')
 

@router.post("/add_diet_template")
async def add_diet_template(
    request: AddDietTemplateRequest, 
    db: Session = Depends(get_db)
):

    existing = (
        db.query(ClientDietTemplate)
          .filter(
              ClientDietTemplate.client_id   == request.client_id,
              ClientDietTemplate.template_name == request.template_name
          )
          .first()
    )
    if existing:
        raise HTTPException(
            status_code=400,
            detail=f"Template name '{request.template_name}' is already there"
        )

    try:
        new_template = ClientDietTemplate(
            client_id     = request.client_id,
            template_name = request.template_name,
            diet_data     = request.diet_data
        )
        db.add(new_template)
        db.commit()
        db.refresh(new_template)

        data = {
            "id":            new_template.id,
            "client_id":     new_template.client_id,
            "template_name": new_template.template_name,
            "diet_data":     new_template.diet_data
        }
        return {
            "status":  200,
            "message": "Diet template added successfully",
            "data":    data
        }
    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=500,
            detail=f"Error adding diet template: {str(e)}"
        )

@router.put("/update_diet_template")
async def update_diet_template(request: UpdateDietTemplateRequest, db: Session = Depends(get_db)):
    try:
        template = db.query(ClientDietTemplate).filter(ClientDietTemplate.id == request.id).first()
        if not template:
            raise HTTPException(status_code=404, detail="Template not found with the given ID")

        template.diet_data = request.diet_data
        db.commit()
        db.refresh(template)

        return {"status": 200, "message": "Diet template updated successfully"}
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Error updating diet template: {str(e)}")

@router.put("/edit_diet_template_name")
async def edit_diet_template_name(request: EditDietTemplateNameRequest, db: Session = Depends(get_db)):
    try:
        result = db.query(ClientDietTemplate).filter(ClientDietTemplate.id == request.id).update({"template_name": request.template_name})
        db.commit()

        if not result:
            raise HTTPException(status_code=404, detail="Template not found")

        return {"status": 200, "message": "Diet template name updated successfully"}
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Error updating diet template name: {str(e)}")

@router.delete("/delete_diet_template")
async def delete_diet_template(id: int, db: Session = Depends(get_db)):
    try:
        result = db.query(ClientDietTemplate).filter(ClientDietTemplate.id == id).delete()
        db.commit()

        if result == 0:
            raise HTTPException(status_code=404, detail="Template not found with the given ID")

        return {"status": 200, "message": "Diet template deleted successfully"}
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Error deleting diet template: {str(e)}")
    

@router.get("/get_actual_workout")
async def get_actual_workout(client_id: int, date: date, db: Session = Depends(get_db)):

    try:
        record = db.query(ActualWorkout).filter(
            ActualWorkout.client_id == client_id,
            ActualWorkout.date == date
        ).first()
        if not record:
            return {"status": 200, "data": []}
        return {"status": 200, "data": record}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred {e}")


class WorkoutInput(BaseModel):
    client_id: int
    date: date
    workout_details: list
    gym_id :int
    live_status:bool

class WorkoutEditInput(BaseModel):
    client_id: int

    gym_id :int
    record_id: int
    workout_details: list

@router.post("/create_actual_workout")
async def create_or_append_workout(data: WorkoutInput, db: Session = Depends(get_db),redis: Redis = Depends(get_redis)):

    try:
        total_burnt_calories = 0
        if data.workout_details:
            for muscle_group in data.workout_details:
                for exercises in muscle_group.values():
                    for exercise in exercises:
                        for set_detail in exercise.get("sets", []):
                            total_burnt_calories += set_detail.get("calories", 0) or 0
    
        record = db.query(ActualWorkout).filter(
            ActualWorkout.client_id == data.client_id,
            ActualWorkout.date == data.date
        ).first()
    
        if record:
            if record.workout_details is None:
                record.workout_details = data.workout_details
            else:
                if isinstance(record.workout_details, list):
                    updated_list = record.workout_details + data.workout_details
                    record.workout_details = updated_list
                else:
                    record.workout_details = [record.workout_details] + data.workout_details
            db.commit()
            db.refresh(record)
        else:
            record = ActualWorkout(
                client_id=data.client_id,
                date=data.date,
                workout_details=data.workout_details
            )
            db.add(record)
            db.commit()
            db.refresh(record)
    
        client_actual = db.query(ClientActual).filter(
            ClientActual.client_id == data.client_id,
            ClientActual.date == data.date
        ).first()
    
        if client_actual:
            client_actual.burnt_calories = (client_actual.burnt_calories or 0) + total_burnt_calories
        else:
            client_actual = ClientActual(
                client_id=data.client_id,
                date=data.date,
                burnt_calories=total_burnt_calories
            )
            db.add(client_actual)

        target_actual_key = f"{data.client_id}:{data.gym_id}:target_actual"
        if await redis.exists(target_actual_key):
            await redis.delete(target_actual_key)
        
        chart_key = f"{data.client_id}:{data.gym_id}:chart"
        if await redis.exists(chart_key):
            await redis.delete(chart_key)
    
        db.commit()
        db.refresh(client_actual)

        if data.live_status:
                total_sets = 0
                for workout in data.workout_details:
                    for muscle_group, exercises in workout.items():
                        for exercise in exercises:
                            sets = exercise.get("sets", [])
                            total_sets += len(sets)
                
                calculated_credits = total_sets * 3
                credits = calculated_credits if calculated_credits <= 50 else 50
                today = date.today()
                calorie_event = db.query(CalorieEvent).filter(
                    CalorieEvent.client_id == data.client_id,
                    CalorieEvent.gym_id == data.gym_id,
                    CalorieEvent.event_date == today
                ).first()

                if not calorie_event:
                    calorie_event = CalorieEvent(
                        client_id=data.client_id,
                        gym_id=data.gym_id,
                        event_date=today,
                        workout_added=0,
                    )
                    db.add(calorie_event)
                    db.commit()
                    calorie_event=calorie_event

                if True:

                    if not calorie_event.workout_added:
                        calorie_event.workout_added=0
                    target_sets = calorie_event.workout_added
                    if target_sets<50:

                        if target_sets+credits>50:
                            credits=50-target_sets
                        today=date.today()
                        daily_record = db.query(LeaderboardDaily).filter(
                            LeaderboardDaily.client_id == data.client_id,
                            LeaderboardDaily.date == today
                        ).first()

                        if daily_record:
                            daily_record.xp += credits
                        else:
                            new_daily = LeaderboardDaily(
                                client_id=data.client_id,
                                xp=credits,
                                date=today
                            )
                            db.add(new_daily)

                        month_date = today.replace(day=1)
                        monthly_record = db.query(LeaderboardMonthly).filter(
                            LeaderboardMonthly.client_id == data.client_id,
                            LeaderboardMonthly.month == month_date
                        ).first()

                        if monthly_record:
                            monthly_record.xp += credits
                        else:
                            new_monthly = LeaderboardMonthly(
                                client_id=data.client_id,
                                xp=credits,
                                month=month_date
                            )
                            db.add(new_monthly)

                        overall_record = db.query(LeaderboardOverall).filter(
                            LeaderboardOverall.client_id == data.client_id
                        ).first()

                        if overall_record:
                            overall_record.xp += credits
                            new_total = overall_record.xp

                            next_row = (
                                db.query(ClientNextXp)
                                .filter_by(client_id=data.client_id)
                                .with_for_update()
                                .one_or_none()
                            )

                            def _tier_after(xp: int):
                                return (
                                    db.query(RewardGym)
                                    .filter_by(gym_id=data.gym_id)
                                    .filter(RewardGym.xp > xp)
                                    .order_by(asc(RewardGym.xp))
                                    .first()
                                )


                            if next_row and next_row.next_xp != 0:
                                if new_total >= next_row.next_xp:
                                    client = db.query(Client).filter(Client.client_id==data.client_id).first()
                                    db.add(
                                        RewardPrizeHistory(
                                            gym_id        = data.gym_id,
                                            client_id     = data.client_id,
                                            xp            = next_row.next_xp,
                                            gift          = next_row.gift,
                                            achieved_date = datetime.now(),
                                            client_name   = client.name,
                                            is_given      = False,
                                            profile=client.profile
                                        )
                                    )

                                    next_tier = _tier_after(next_row.next_xp)
                                    if next_tier:
                                        next_row.next_xp = next_tier.xp
                                        next_row.gift    = next_tier.gift
                                    else:

                                        next_row.next_xp = 0
                                        next_row.gift    = None


                            else:
                                first_tier = (
                                    db.query(RewardGym)
                                    .filter_by(gym_id=data.gym_id)
                                    .order_by(asc(RewardGym.xp))
                                    .first()
                                )
                                if first_tier:
                                    if next_row:
                                        next_row.next_xp = first_tier.xp
                                        next_row.gift    = first_tier.gift
                                    else:
                                        db.add(
                                            ClientNextXp(
                                                client_id = data.client_id,
                                                next_xp   = first_tier.xp,
                                                gift      = first_tier.gift,
                                            )
                                        )

                            db.commit()
                            db.refresh(overall_record)
                        else:
                            new_overall = LeaderboardOverall(
                                client_id=data.client_id,
                                xp=credits
                            )
                            db.add(new_overall)
                            db.commit()
                            db.refresh(new_overall)

                        existing_event = db.query(CalorieEvent).filter(
                        CalorieEvent.client_id == data.client_id,
                        CalorieEvent.gym_id == data.gym_id,
                        CalorieEvent.event_date == data.date
                        ).first()

                        if existing_event:
                            existing_event.workout_added += credits
                        else:
                            new_event = CalorieEvent(
                                client_id=data.client_id,
                                gym_id=data.gym_id,
                                event_date=data.date,
                                workout_added=credits
                            )
                            db.add(new_event)

                        db.commit()
                   
                    
                    else:
                        credits=0
        else:
            credits=0   
        return {
            "status": 200,
            "message": "Workout data appended and updated",
            "record_id": record.record_id,
            "workout_details": record.workout_details,
            "total_burnt_calories": total_burnt_calories,
            "reward_point":credits
        }
    except Exception as e:
        print(e)
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred {e}")

 
@router.put("/edit_actual_workout")
async def edit_workout(data: WorkoutEditInput, db: Session = Depends(get_db),redis: Redis = Depends(get_redis)):
    record = db.query(ActualWorkout).filter(
        ActualWorkout.record_id == data.record_id
    ).first()
 
    if not record:
        raise HTTPException(status_code=404, detail="Workout record not found")
 
    previous_total_burnt_calories = 0
    if record.workout_details:
        for muscle_group in record.workout_details:
            for exercises in muscle_group.values():
                for exercise in exercises:
                    for set_detail in exercise.get("sets", []):
                        previous_total_burnt_calories += set_detail.get("calories", 0) or 0
 
    if data.workout_details == []:
        db.delete(record)
        db.commit()
 
        client_actual = db.query(ClientActual).filter(
            ClientActual.client_id == record.client_id,
            ClientActual.date == record.date
        ).first()
 
        if client_actual:
            client_actual.burnt_calories = max((client_actual.burnt_calories or 0) - previous_total_burnt_calories, 0)
            db.commit()
            db.refresh(client_actual)
        return {
            "status": 200,
            "message": "Workout record deleted as workout_details is empty"
        }
 
    new_total_burnt_calories = 0
    if data.workout_details:
        for muscle_group in data.workout_details:
            for exercises in muscle_group.values():
                for exercise in exercises:
                    for set_detail in exercise.get("sets", []):
                        new_total_burnt_calories += set_detail.get("calories", 0) or 0
 
    record.workout_details = data.workout_details
    db.commit()
    db.refresh(record)
 
    client_actual = db.query(ClientActual).filter(
        ClientActual.client_id == record.client_id,
        ClientActual.date == record.date
    ).first()
 
    if client_actual:
        client_actual.burnt_calories = max((client_actual.burnt_calories or 0) - previous_total_burnt_calories + new_total_burnt_calories, 0)
        db.commit()
        db.refresh(client_actual)

    
    target_actual_key = f"{data.client_id}:{data.gym_id}:target_actual"
    if await redis.exists(target_actual_key):
        await redis.delete(target_actual_key)
 
    return {
        "status": 200,
        "message": "Workout data replaced",
        "record_id": record.record_id,
        "workout_details": record.workout_details
    }

 
@router.delete("/delete_all_actual_workout")
async def delete_actual_workout(record_id: int, client_id:int, gym_id:int,db: Session = Depends(get_db),redis: Redis = Depends(get_redis)):
    workout = db.query(ActualWorkout).filter(ActualWorkout.record_id == record_id).first()
    if not workout:
        raise HTTPException(status_code=404, detail="Workout record not found")
   
    client_actual = db.query(ClientActual).filter(
        ClientActual.client_id == client_id,
        ClientActual.date == datetime.now().date()
    ).first()
 
    if client_actual:
        client_actual.burnt_calories = 0
        db.commit()
        db.refresh(client_actual)
   
    db.delete(workout)
    db.commit()
    target_actual_key = f"{client_id}:{gym_id}:target_actual"
    if await redis.exists(target_actual_key):
        await redis.delete(target_actual_key)
    return {"status":200,"message": "Workout record deleted successfully"}



@router.get("/get_actual_diet")
async def get_actual_diet(client_id: int, date: date, db: Session = Depends(get_db)):

    try:
        record = db.query(ActualDiet).filter(
            ActualDiet.client_id == client_id,
            ActualDiet.date == date
        ).first()

        if not record:
            return {
            "status":200,
            "data": [],
            "id":None
        }

        return {
            "status":200,
            "data": record.diet_data,
            "id":record.record_id
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred {e}")


class DietInput(BaseModel):
    client_id: int
    date: date
    diet_data: list
    gym_id:int

class DieteditInput(BaseModel):
    record_id: int
    date: date
    client_id : int
    diet_data: list
    gym_id : int

def calculate_totals(diet_data: list) -> dict:
    total_calories = 0
    total_protein = 0
    total_carbs = 0
    total_fats = 0
    total_sugar = 0
    total_fiber = 0
    total_calcium = 0
    total_magnesium = 0
    total_potassium = 0
    total_Iodine = 0
    total_Iron = 0
 
    # Handle new nested structure with meal categories
    for meal_category in diet_data:
        if isinstance(meal_category, dict) and "foodList" in meal_category:
            # New structure: iterate through foodList in each meal category
            for food_item in meal_category.get("foodList", []):
                total_calories += food_item.get("calories", 0) or 0
                total_protein += food_item.get("protein", 0) or 0
                total_carbs += food_item.get("carbs", 0) or 0
                total_fats += food_item.get("fat", 0) or 0
                total_sugar += food_item.get("sugar", 0) or 0
                total_fiber += food_item.get("fiber", 0) or 0
                total_calcium += food_item.get("calcium", 0) or 0
                total_magnesium += food_item.get("magnesium", 0) or 0
                total_potassium += food_item.get("potassium", 0) or 0
                total_Iodine += food_item.get("Iodine", 0) or 0
                total_Iron += food_item.get("Iron", 0) or 0
        else:
            # Old structure: direct food items
            total_calories += meal_category.get("calories", 0) or 0
            total_protein += meal_category.get("protein", 0) or 0
            total_carbs += meal_category.get("carbs", 0) or 0
            total_fats += meal_category.get("fat", 0) or 0
            total_sugar += meal_category.get("sugar", 0) or 0
            total_fiber += meal_category.get("fiber", 0) or 0
            total_calcium += meal_category.get("calcium", 0) or 0
            total_magnesium += meal_category.get("magnesium", 0) or 0
            total_potassium += meal_category.get("potassium", 0) or 0
            total_Iodine += meal_category.get("Iodine", 0) or 0
            total_Iron += meal_category.get("Iron", 0) or 0
 
    return {
        "calories": total_calories,
        "protein": total_protein,
        "carbs": total_carbs,
        "fats": total_fats,
        "sugar": total_sugar,
        "fiber": total_fiber,
        "calcium": total_calcium,
        "magnesium": total_magnesium,
        "potassium": total_potassium,
        "Iodine": total_Iodine,
        "Iron": total_Iron
    }
 


@router.post("/create_actual_diet")
async def create_or_append_diet(data: DietInput, db: Session = Depends(get_db),redis: Redis = Depends(get_redis)):
 
    try:

        today=date.today()
        record = db.query(ActualDiet).filter(
            ActualDiet.client_id == data.client_id,
            ActualDiet.date == data.date
        ).first()
 
        if record:
         
            if record.diet_data is None:
                record.diet_data = data.diet_data
            else:
                if isinstance(record.diet_data, list):
               
                    updated_list = record.diet_data + data.diet_data
                    record.diet_data = updated_list
                else:
                    record.diet_data = [record.diet_data] + data.diet_data
            db.commit()
            db.refresh(record)
        else:
 
            record = ActualDiet(
                client_id=data.client_id,
                date=data.date,
                diet_data=data.diet_data
            )
            db.add(record)
            db.commit()
            db.refresh(record)
 
 
        new_totals = calculate_totals(data.diet_data)
        client_record = db.query(ClientActual).filter(
            ClientActual.client_id == data.client_id,
            ClientActual.date == data.date
        ).first()
 
        client_target_calories=db.query(ClientTarget).filter(ClientTarget.client_id==data.client_id).first()
        client_target_calories=client_target_calories.calories
 
        if client_record:
            prev_calories=client_record.calories
            client_record.calories = (client_record.calories or 0) + new_totals["calories"] 
            client_record.protein  = (client_record.protein or 0)  + new_totals["protein"]
            client_record.carbs    = (client_record.carbs or 0)    + new_totals["carbs"]
            client_record.fats     = (client_record.fats or 0)     + new_totals["fats"]
            client_record.sugar    = (client_record.sugar or 0)    + new_totals["sugar"]
            client_record.fiber    = (client_record.fiber or 0)    + new_totals["fiber"]
            client_record.calcium  = (client_record.calcium or 0)  + new_totals["calcium"]
            client_record.magnesium= (client_record.magnesium or 0)+ new_totals["magnesium"]
            client_record.potassium= (client_record.potassium or 0)+ new_totals["potassium"]
            client_record.Iodine   = (client_record.Iodine or 0)   + new_totals["Iodine"]
            client_record.Iron     = (client_record.Iron or 0)     + new_totals["Iron"]
            db.commit()
            db.refresh(client_record)
        else:
         
            target_record=db.query(ClientTarget).filter(ClientTarget.client_id==data.client_id).first()
            prev_calories=new_totals["calories"]
            client_record = ClientActual(
                client_id=data.client_id,
                date=data.date,
                calories=new_totals["calories"],
                protein=new_totals["protein"],
                carbs=new_totals["carbs"],
                fats=new_totals["fats"],
                target_calories=target_record.calories,
                target_protein=target_record.protein,
                target_fat=target_record.fat,
                target_carbs=target_record.carbs,
                sugar=new_totals["sugar"],
                fiber=new_totals["fiber"],
                calcium=new_totals["calcium"],
                magnesium=new_totals["magnesium"],
                potassium=new_totals["potassium"],
                Iodine=new_totals["Iodine"],    
                Iron=new_totals["Iron"],
                target_sugar=target_record.sugar,
                target_fiber=target_record.fiber,
                target_calcium=target_record.calcium,
                target_magnesium=target_record.magnesium,
                target_potassium=target_record.potassium,
                target_Iodine=target_record.Iodine,
                target_Iron=target_record.Iron
 
            )
            db.add(client_record)
            db.commit()
            db.refresh(client_record)
 
        if data.date== date.today():
 
            if client_target_calories > 0:
                ratio = new_totals["calories"] / client_target_calories
                if ratio > 1:
                    ratio = 1
            else:
                ratio = 0

            calorie_points = int(round(ratio * 50))
            calorie_event = db.query(CalorieEvent).filter(
                            CalorieEvent.client_id == data.client_id,
                            CalorieEvent.gym_id == data.gym_id,
                            CalorieEvent.event_date == today
                                ).first()
 
            if not calorie_event:
                calorie=CalorieEvent(client_id=data.client_id,gym_id=data.gym_id,event_date=date.today(),calories_added=0)
                db.add(calorie)
                db.commit()
                calorie_event = calorie
 
            if not calorie_event.calories_added:
                calorie_event.calories_added=0
 
           
            added_calory=calorie_event.calories_added
 
            if added_calory<50:
                if added_calory+calorie_points>50:
                    calorie_points=50-added_calory
                   
                today=date.today()
                daily_record = db.query(LeaderboardDaily).filter(
                    LeaderboardDaily.client_id == data.client_id,
                    LeaderboardDaily.date == today
                ).first()

                if daily_record:
                    daily_record.xp += calorie_points
                else:
                    new_daily = LeaderboardDaily(
                        client_id=data.client_id,
                        xp=calorie_points,
                        date=today
                    )
                    db.add(new_daily)

                month_date = today.replace(day=1)
                monthly_record = db.query(LeaderboardMonthly).filter(
                    LeaderboardMonthly.client_id == data.client_id,
                    LeaderboardMonthly.month == month_date
                ).first()

                if monthly_record:
                    monthly_record.xp += calorie_points
                else:
                    new_monthly = LeaderboardMonthly(
                        client_id=data.client_id,
                        xp=calorie_points,
                        month=month_date
                    )
                    db.add(new_monthly)

                overall_record = db.query(LeaderboardOverall).filter(
                    LeaderboardOverall.client_id == data.client_id
                ).first()
 
                if overall_record:
                    overall_record.xp += calorie_points
                    new_total = overall_record.xp

                    next_row = (
                        db.query(ClientNextXp)
                        .filter_by(client_id=data.client_id)
                        .with_for_update()
                        .one_or_none()
                    )

                    def _tier_after(xp: int):
                        return (
                            db.query(RewardGym)
                            .filter_by(gym_id=data.gym_id)
                            .filter(RewardGym.xp > xp)
                            .order_by(asc(RewardGym.xp))
                            .first()
                        )


                    if next_row and next_row.next_xp != 0:
                        if new_total >= next_row.next_xp:
                            client = db.query(Client).filter(Client.client_id==data.client_id).first()

                            db.add(
                                RewardPrizeHistory(
                                    gym_id        = data.gym_id,
                                    client_id     = data.client_id,
                                    xp            = next_row.next_xp,
                                    gift          = next_row.gift,
                                    achieved_date = datetime.now(),
                                    client_name   = client.name,
                                    is_given      = False,
                                    profile=client.profile
                                )
                            )

                            next_tier = _tier_after(next_row.next_xp)
                            if next_tier:
                                next_row.next_xp = next_tier.xp
                                next_row.gift    = next_tier.gift
                            else:

                                next_row.next_xp = 0
                                next_row.gift    = None


                    else:
                        first_tier = (
                            db.query(RewardGym)
                            .filter_by(gym_id=data.gym_id)
                            .order_by(asc(RewardGym.xp))
                            .first()
                        )
                        if first_tier:
                            if next_row:
                                next_row.next_xp = first_tier.xp
                                next_row.gift    = first_tier.gift
                            else:
                                db.add(
                                    ClientNextXp(
                                        client_id = data.client_id,
                                        next_xp   = first_tier.xp,
                                        gift      = first_tier.gift,
                                    )
                                )



                    db.commit()
                    db.refresh(overall_record)
                else:
                    new_overall = LeaderboardOverall(
                        client_id=data.client_id,
                        xp=calorie_points
                    )
                    db.add(new_overall)
                    db.commit()
                    db.refresh(new_overall)
 
                existing_event = db.query(CalorieEvent).filter(
                CalorieEvent.client_id == data.client_id,
                CalorieEvent.gym_id == data.gym_id,
                CalorieEvent.event_date == data.date
                ).first()
 
                if existing_event:
                    existing_event.calories_added += calorie_points
                else:
                    new_event = CalorieEvent(
                        client_id=data.client_id,
                        gym_id=data.gym_id,
                        event_date=data.date,
                        calories_added=calorie_points
                    )
                    db.add(new_event)
 
                db.commit()
           
       
            else:
                calorie_points=0
                   
 
        else:
            calorie_points=0

        target_actual_key = f"{data.client_id}:{data.gym_id}:target_actual"
        if await redis.exists(target_actual_key):
            await redis.delete(target_actual_key)

        chart_key = f"{data.client_id}:{data.gym_id}:chart"
        if await redis.exists(chart_key):
            await redis.delete(chart_key)
 
        return {
            "status":200,
            "message": "Diet data appended and aggregated nutrition updated",
            "reward_point":calorie_points
        }
 
    except Exception as e:
        print(e)
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred {e}")


@router.put("/edit_actual_diet")
async def edit_diet(data: DieteditInput, db: Session = Depends(get_db),redis: Redis = Depends(get_redis)):
    try:

        if not data:
            record = db.query(ActualDiet).filter(ActualDiet.record_id == data.record_id).first()
            if not record:
                raise HTTPException(status_code=404, detail="Diet record not found")

            db.delete(record)
            db.commit()

            client_actual_record = db.query(ClientActual).filter(
                ClientActual.client_id == data.client_id,
                ClientActual.date == data.date
            ).first()
            if client_actual_record:
                db.delete(client_actual_record)
                db.commit()

            return {
                "status":200,
                "message": "Diet record and corresponding aggregated client data deleted"
            }


        record = db.query(ActualDiet).filter(
            ActualDiet.record_id == data.record_id,
        ).first()

        if not record:
            raise HTTPException(status_code=404, detail="Record not found")

    
        record.diet_data = data.diet_data
        db.commit()
        db.refresh(record)

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
            client_record.sugar    = totals["sugar"]
            client_record.fiber    = totals["fiber"]
            client_record.calcium  = totals["calcium"]
            client_record.magnesium= totals["magnesium"]
            client_record.potassium= totals["potassium"]
            client_record.Iodine   = totals["Iodine"]
            client_record.Iron     = totals["Iron"]
            db.commit()
            db.refresh(client_record)
        else:
            client_record = ClientActual(
                client_id=record.client_id,
                date=record.date,
                calories=totals["calories"],
                protein=totals["protein"],
                carbs=totals["carbs"],
                fats=totals["fats"],
                sugar=totals["sugar"],
                fiber=totals["fiber"],
                calcium=totals["calcium"],
                magnesium=totals["magnesium"],
                potassium=totals["potassium"],
                Iodine=totals["Iodine"],    
                Iron=totals["Iron"],
            )
            db.add(client_record)
            db.commit()
            db.refresh(client_record)

        if data.date== date.today(): 
            old_event = db.query(CalorieEvent).filter(
                CalorieEvent.client_id == record.client_id,
                CalorieEvent.gym_id == data.gym_id,
                CalorieEvent.event_date == date.today()
            ).first()

            if old_event:
                old_calorie_points = old_event.calories_added
                daily_record = db.query(LeaderboardDaily).filter(
                    LeaderboardDaily.client_id == data.client_id,
                    LeaderboardDaily.gym_id == data.gym_id,
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
                    LeaderboardOverall.client_id == data.client_id
                ).first()
                if overall_record:
                    overall_record.xp -= old_calorie_points

                old_event.calories_added = 0
            else:
                new_event = CalorieEvent(
                    client_id=data.client_id,
                    gym_id=data.gym_id,
                    event_date=record.date,
                    calories_added=0
                )
                db.add(new_event)

            db.commit() 

            if client_record.target_calories and client_record.target_calories > 0:
                ratio = totals["calories"] / client_record.target_calories
                if ratio > 1:
                    ratio = 1
            else:
                ratio = 0

            calorie_points = int(round(ratio * 50))
            today=date.today()


            daily_record = db.query(LeaderboardDaily).filter(
                LeaderboardDaily.client_id == data.client_id,
                LeaderboardDaily.date == today
            ).first()

            if daily_record:
                daily_record.xp += calorie_points
            else:
                new_daily = LeaderboardDaily(
                    client_id=data.client_id,
                    xp=calorie_points,
                    date=today
                )
                db.add(new_daily)

            month_date = today.replace(day=1)
            monthly_record = db.query(LeaderboardMonthly).filter(
                LeaderboardMonthly.client_id == data.client_id,
                LeaderboardMonthly.month == month_date
            ).first()

            if monthly_record:
                monthly_record.xp += calorie_points
            else:
                new_monthly = LeaderboardMonthly(
                    client_id=data.client_id,
                    xp=calorie_points,
                    month=month_date
                )
                db.add(new_monthly)

            overall_record = db.query(LeaderboardOverall).filter(
                LeaderboardOverall.client_id == data.client_id
            ).first()

            if overall_record:
                overall_record.xp += calorie_points
                db.commit()
                db.refresh(overall_record)
            else:
                new_overall = LeaderboardOverall(
                    client_id=data.client_id,
                    xp=calorie_points
                )
                db.add(new_overall)
                db.commit()
                db.refresh(new_overall)
                
            existing_event = db.query(CalorieEvent).filter(
            CalorieEvent.client_id == data.client_id,
            CalorieEvent.gym_id == data.gym_id,
            CalorieEvent.event_date == data.date
            ).first()

            if existing_event:
                existing_event.calories_added += calorie_points
            else:
                new_event = CalorieEvent(
                    client_id=data.client_id,
                    gym_id=data.gym_id,
                    event_date=date.today(),
                    calories_added=calorie_points
                )
                db.add(new_event)
            db.commit()

            # rewards = db.query(RewardGym).filter_by(gym_id=data.gym_id).order_by(RewardGym.xp.asc()).all()
            # client=db.query(Client).filter(Client.client_id == data.client_id).first()
            # achieved_xps = (
            #     db.query(RewardPrizeHistory.xp)
            #     .filter_by(client_id=data.client_id, gym_id=data.gym_id)
            #     .all()
            # )
            # achieved_xps = {row[0] for row in achieved_xps}  
            
            # for reward in rewards:
            #     if not overall_record:
            #         if reward.xp > new_overall.xp:
            #             break  

            #     else:
            #         if reward.xp > overall_record.xp:
            #             break
                
            #     if reward.xp not in achieved_xps:  
            #         achievement = RewardPrizeHistory(
            #             client_id=data.client_id,
            #             gym_id=data.gym_id,
            #             xp=reward.xp,
            #             gift=reward.gift,
            #             achieved_date=datetime.now(),
            #             is_given=False,
            #             client_name=client.name
            #         )
            #         db.add(achievement)
            #         db.commit()

        target_actual_key = f"{data.client_id}:{data.gym_id}:target_actual"
        if await redis.exists(target_actual_key):
            await redis.delete(target_actual_key)


        return {
            "status":200,
            "message": "Diet data replaced and aggregated nutrition updated",
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred {e}")
    
@router.delete("/delete_actual_diet")
async def delete_diet(record_id: int, client_id: int, gym_id:int, date: date, db: Session = Depends(get_db), redis: Redis = Depends(get_redis)):

    try:
        record = db.query(ActualDiet).filter(ActualDiet.record_id == record_id).first()
        if not record:
            raise HTTPException(status_code=404, detail="Diet record not found")

        db.delete(record)
        db.commit()

        client_actual_record = db.query(ClientActual).filter(
            ClientActual.client_id == client_id,
            ClientActual.date == date
        ).first()
        if client_actual_record:
            db.delete(client_actual_record)
            db.commit()

        if date==date.today():

            old_event = db.query(CalorieEvent).filter(
                CalorieEvent.client_id == client_id,
                CalorieEvent.gym_id == gym_id,
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
                    LeaderboardOverall.client_id == client_id
                ).first()
                if overall_record:
                    overall_record.xp -= old_calorie_points
                old_event.calories_added = 0
            else:
                new_event = CalorieEvent(
                    client_id=client_id,
                    gym_id=gym_id,
                    event_date=date.today(),
                    calories_added=0
                )
                db.add(new_event)

            db.commit() 
        target_actual_key = f"{client_id}:{gym_id}:target_actual"
        if await redis.exists(target_actual_key):
            await redis.delete(target_actual_key)

        return {
            "status":200,
            "message": "Diet record and corresponding aggregated client data deleted"
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred {e}")

    


@router.get("/attendance_status_with_location")
async def check_attendance_status(client_id: int, gym_id: int, db: Session = Depends(get_db)):

    try:

        print("client_id is",client_id)
        print("gym_id is",gym_id)
        today = datetime.now().date()
        record = db.query(Attendance).filter(
            Attendance.client_id == client_id,
            Attendance.date == today
        ).first()

        latitude = None
        longitude = None

        if record and record.in_time and not record.out_time:
            attendance_status = True
            in_time=record.in_time
        elif record and record.in_time_2 and not record.out_time_2:

            attendance_status = True
            in_time=record.in_time_2
        elif record and record.in_time_3 and not record.out_time_3:
            

            attendance_status = True
            in_time=record.in_time_3
        else:
            attendance_status = False
            in_time=None

        gym_location = db.query(GymLocation).filter(GymLocation.gym_id == gym_id).first()
        if gym_location:
            latitude = float(gym_location.latitude)
            longitude = float(gym_location.longitude)

        
        return {
            "status": 200,
            "attendance_status": attendance_status,
            "in_time":in_time,
            "gym_location": {
                "latitude": latitude,
                "longitude": longitude
            }
        }
    except Exception as e:
        print(str(e))
        raise HTTPException(status_code=500, detail=f"Error fetching attendance:{str(e)}")



class IdsRequest(BaseModel):
    ids: List[int]

@router.post("/fetch_qr_exercises")
def get_grouped_exercises(ids_request: IdsRequest, db: Session = Depends(get_db)):
    try:
        ids = ids_request.ids
        records = db.query(QRCode).filter(QRCode.id.in_(ids)).all()
        if not records:
            raise HTTPException(status_code=404, detail="No records found with given ids.")
 
        response_data = {}
       
        for record in records:
            group = record.muscle_group
            if group not in response_data:
                response_data[group] = {}
                response_data[group]["exercises"]=[]
                response_data[group]["isMuscleGroup"]=False
                response_data[group]["isCardio"]=False
                response_data[group]["isBodyWeight"]=False
                response_data[group]['gifUrl']=''
            response_data[group]["exercises"].append(record.exercises)
            response_data[group]["isMuscleGroup"]=record.isMuscleGroup
            response_data[group]["isCardio"]=record.isCardio
            response_data[group]["isCardio"]=record.isBodyWeight
            response_data[group]["gifUrl"]=record.gifUrl
 
        return {"status": 200, "data": response_data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching grouped exercises:{str(e)}")

class FetchMessagesResponse(BaseModel):
    inbox: List[dict]
    sent: List[dict]
 
class StoreFcmTokenRequest(BaseModel):
    user_id: int
    gym_id: int
    fcm_token: str
 
class FetchNotificationsResponse(BaseModel):
    notifications: List[dict]
 
class MarkAllReadRequest(BaseModel):
    user_id: int
    gym_id: int
 
 
 
class SendMessageRequest(BaseModel):
    sender_id: int
    gym_id: int
    message: str
 
class ConversationResponse(BaseModel):
    user_id: int
    latest_message: str
    last_message_time: datetime
 
 
@router.post("/send_message")
async def send_message(request: SendMessageRequest, db: Session = Depends(get_db)):
    try:
 
        owner_id= db.query(Gym).filter(Gym.gym_id==request.gym_id).first()
        recipient_id=owner_id.owner_id
        sender_role="client"
        recipient_role="owner"
       
        new_message = Message(
            sender_id=request.sender_id,
            recipient_id=recipient_id,
            gym_id=request.gym_id,
            sender_role=sender_role,
            recipient_role=recipient_role,
            message=request.message,
            sent_at=datetime.now()
        )
        db.add(new_message)
        db.commit()
        return {"success": True, "message": "Message sent successfully","status": 200}
 
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred {e}")

@router.get("/conversations")
async def get_home_conversations(
    gym_id: int,
    user_id: int,
    role: str,
    db: Session = Depends(get_db),
):
 
    try:
        all_messages = (
            db.query(
                Message.sender_id,
                Message.recipient_id,
                Message.sender_role,
                Message.recipient_role,
                Message.message,
                Message.is_read,
                Message.sent_at,
            )
            .filter(
                Message.gym_id == gym_id,
                or_(
                    and_(
                        Message.sender_id == user_id,
                        Message.sender_role == role,
                    ),
                    and_(
                        Message.recipient_id == user_id,
                        Message.recipient_role == role,
                    ),
                )
            )
            .order_by(Message.sent_at.desc())
            .all()
        )
 
        grouped_conversations = {}
        for msg in all_messages:
            conversation_key = tuple(sorted([(msg.sender_id, msg.sender_role), (msg.recipient_id, msg.recipient_role)]))
            if conversation_key not in grouped_conversations:
                grouped_conversations[conversation_key] = msg
 
        conversations = [
            {
                "sender_id": msg.sender_id,
                "recipient_id": msg.recipient_id,
                "sender_role": msg.sender_role,
                "recipient_role": msg.recipient_role,
                "message": msg.message,
                "is_read": msg.is_read,
                "sent_at": msg.sent_at.isoformat(),
            }
            for msg in grouped_conversations.values()
        ]
 
        return {"status": 200, "data": conversations}
 
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching conversations: {str(e)}")
   
@router.get("/messages")
async def get_messages(
    gym_id: int,
    user_id: int,
    db: Session = Depends(get_db),
):
 
    try:
        owner_id= db.query(Gym).filter(Gym.gym_id==gym_id).first()
        conversation_user_id=owner_id.owner_id
        user_role="client"
        conv_user_role="owner"
 
        query = db.query(Message).filter(
            Message.gym_id == gym_id,
            (
                (Message.sender_id == user_id) &
                (Message.recipient_id == conversation_user_id) &
                (Message.sender_role == user_role) &
                (Message.recipient_role == conv_user_role)
            )
            |
            (
                (Message.sender_id == conversation_user_id) &
                (Message.recipient_id == user_id) &
                (Message.sender_role == conv_user_role) &
                (Message.recipient_role == user_role)
            )
        )
 
        messages = query.order_by(Message.sent_at.asc()).all()
 
        return {
            "status": 200,
            "data": [
                {
                    "message_id": msg.message_id,
                    "sender_id": msg.sender_id,
                    "recipient_id": msg.recipient_id,
                    "message": msg.message,
                    "sent_at": msg.sent_at.isoformat(),
                    "is_self": msg.sender_id == user_id and msg.sender_role == user_role,
                }
                for msg in messages
            ],
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching messages: {str(e)}")
     
class EditMessageModel(BaseModel):
    message: str
    message_id:int
 
 
@router.put("/edit_message")
async def edit_message( data: EditMessageModel, db: Session = Depends(get_db)):
    try:
        message_id=data.message_id
        message = db.query(Message).filter(Message.message_id == message_id).first()
        if not message:
            raise HTTPException(status_code=404, detail="Message not found.")
        message.message = data.message
        db.commit()
        return {"status": 200, "message": "Message updated successfully."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error editing message: {str(e)}")
   
class deletemsg(BaseModel):
    message_ids: List[Any]
 

@router.delete("/delete_messages")
async def delete_messages( req:deletemsg, db: Session = Depends(get_db)):
    try:
        message_ids=req.message_ids
        messages = db.query(Message).filter(Message.message_id.in_(message_ids)).all()
        if not messages:
            raise HTTPException(status_code=404, detail="Messages not found.")
        for message in messages:
            db.delete(message)
        db.commit()
        return {"status": 200, "message": "Messages deleted successfully."}
   
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Error deleting messages: {str(e)}")


class FeedbackCreate(BaseModel):
    gym_id: int
    client_id: int
    tag: str
    ratings: int
    feedback: Optional[str] = None
 
 
@router.post("/feedback")
async def create_feedback(feedback_data: FeedbackCreate, db: Session = Depends(get_db)):
    try:
        new_feedback = Feedback(
            gym_id=feedback_data.gym_id,
            client_id=feedback_data.client_id,
            tag=feedback_data.tag,
            ratings=feedback_data.ratings,
            feedback=feedback_data.feedback,
        )
       
        db.add(new_feedback)
        db.commit()  
        db.refresh(new_feedback)  
 
        return {
            "status":200,
            "message": "Feedback submitted successfully",
            "feedback_id": new_feedback.id
        }
 
    except Exception as e:
        await db.rollback()  
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")
   
 
class UpdateProfileRequest(BaseModel):
    name: Optional[str] = None
    location: Optional[str] = None
    email: Optional[str] = None
    contact: Optional[str] = None
    dob: Optional[str] = None
    newPassword: Optional[str] = None
    oldPassword: Optional[str] = None
    height:Optional[float]=None
    client_id: int
    lifestyle: Optional [str]=None
    medical_issues: Optional[str]=None
    goals: Optional[str] =None
    gender: Optional[str]=None
    role: str
    method: str

class ClientSchema(BaseModel):
    name: str
    location: Optional[str] = None
    email: str
    contact: str
    height:float
    lifestyle: Optional[str] = None
    medical_issues: Optional[str] = None
    goals: Optional[str] = None
    gender: str
    dob: Optional[date] = None
    profile:str

    class Config:
        from_attributes = True



LIFESTYLE_CHOICES = {
    'sedentary':'Sedentary' ,
    'lightly_active': 'Lightly Active' ,
    'moderately_active' : 'Moderately Active' ,
    'very_active': 'Very Active' ,
    'super_active' : 'Super Active'
}
 
GOALS_CHOICES = {
    "weight_loss": "Weight Loss",
    "weight_gain": "Weigth Gain",
    "maintain": "Body Recomposition"
}
 
 
@router.get("/profile_data")
async def get_data(
    client_id: int,
    db: Session = Depends(get_db),
    redis: Redis = Depends(get_redis),
):
    try:
        client = (
            db.query(Client)
            .filter(Client.client_id == client_id)
            .first()
            if client_id
            else None
        )
 
        if not client:
            raise HTTPException(status_code=404, detail="Client not found")
 
        client_data = ClientSchema.model_validate(client)
        # client_data.lifestyle = LIFESTYLE_CHOICES.get(client.lifestyle, "Unknown")
        # client_data.goals = GOALS_CHOICES.get(client.goals, "Unknown")
        client_data.lifestyle = client.lifestyle
        client_data.goals = client.goals
 
        gym = db.query(Gym).filter(Gym.gym_id == client.gym_id).first()
        batch = db.query(GymBatches).filter(GymBatches.batch_id == client.batch_id).first()
        training = db.query(GymPlans).filter(GymPlans.id == client.training_id).first()
 
        return {
            "status": 200,
            "success": True,
            "message": "Data retrieved successfully",
            "data": {
                "client_data": client_data,
                "gym_data": {
                    "gym_location": gym.location,
                    "gym_name": gym.name,
                    "gym_logo":gym.logo,
                    "gym_cover_pic":gym.cover_pic,
                    "batch_name": batch.batch_name,
                    "batch_timing": batch.timing,
                    "training_plans": training.plans,
                    "training_duration": training.duration if training.duration else None,
                    "training_amount": training.amount if training.amount else None
                }
            }
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching profile: {str(e)}")


@router.put("/update_profile")
async def update_profile(request: UpdateProfileRequest, db: Session = Depends(get_db),redis: Redis = Depends(get_redis)):
 
    try:
        if request.method == "profile":
            if request.role != "client":
                raise HTTPException(status_code=400, detail="Only clients can update profiles.")
   
            client = db.query(Client).filter(Client.client_id == request.client_id).first()
            if not client:
                raise HTTPException(status_code=404, detail="Client not found.")
           
            is_changed=False
            if request.contact:
                if not client.contact == request.contact:
                    existing_owner = db.query(GymOwner).filter(
                        (GymOwner.contact_number == request.contact)
                    ).first()
           
                    existing_client = db.query(Client).filter(
                    (Client.contact == request.contact)
                    ).first()
                    if existing_client or existing_owner:
                        raise HTTPException(status_code=400, detail="Mobile number already registered with different account")
                    client.verification= '{"mobile": false, "password" : true}'
                    is_changed=True
                client.contact = request.contact

            if request.dob:
                if not client.dob == request.dob:
                    today=date.today()  
                    dob = datetime.strptime(str(request.dob), "%Y-%m-%d").date()
                    age=today.year - dob.year - ((today.month, today.day) < (dob.month, dob.day))
                    client.age = age
                client.dob = request.dob

            if request.email:
                if not client.email == request.email:
                    existing_owner = db.query(GymOwner).filter(
                        (GymOwner.email == request.email)
                    ).first()
           
                    existing_client = db.query(Client).filter(
                    (Client.email == request.email)
                    ).first()
                    if existing_client or existing_owner:
                            raise HTTPException(status_code=400, detail="Email already registered with different account")
                client.email = request.email
 
            if request.name:
                client.name = request.name
            if request.location:
                client.location=request.location
            if request.lifestyle:
                client.lifestyle=request.lifestyle
            if request.medical_issues:
                client.medical_issues=request.medical_issues
            if request.goals:
                client.goals=request.goals
            if request.gender:
                client.gender=request.gender
            if request.height:
                client.height = request.height
 
            gym_id=client.gym_id
            try:
                db.commit()
                bmr = calculate_bmr(client.weight, client.height, client.age)
    
                tdee = bmr * activity_multipliers[client.lifestyle]
        
                if client.goals == "weight_loss":
                    tdee -= 500
                elif client.goals == "weight_gain":
                    tdee += 500
        
                protein, carbs, fat = calculate_macros(tdee, client.goals)
        
                client_target = db.query(ClientTarget).filter(ClientTarget.client_id == client.client_id).first()

                if client_target:
                    client_target.calories = int(tdee)
                    client_target.protein = protein
                    client_target.carbs = carbs
                    client_target.fat = fat
                    client_target.sugar=25
                    client_target.fiber=30
                    client_target.calcium=1000
                    client_target.magnesium=450
                    client_target.potassium=4000
                    client_target.Iodine=150
                    client_target.Iron=14
                    client_target.updated_at = datetime.now()
        
                    db.commit()
        
                else:
                    client_target = ClientTarget(
                        client_id=client.client_id,
                        calories=int(tdee),
                        protein=protein,
                        carbs=carbs,
                        fat=fat,
                        sugar=25,
                        fiber=30,
                        calcium=1000,
                        magnesium=450,  
                        potassium=4000,
                        Iodine=150,
                        Iron=14,
                        updated_at=datetime.now(),
                    )
        
                    db.add(client_target)
                    db.commit()
        
                target_actual_keys_pattern = "*:initial_target_actual"
                target_actual_keys = await redis.keys(target_actual_keys_pattern)
                if target_actual_keys:
                    await redis.delete(*target_actual_keys)

                client_status_key_pattern = "*:initialstatus"
                client_status_key = await redis.keys(client_status_key_pattern)
                if client_status_key:
                    await redis.delete(*client_status_key)

                pattern = "*:target_actual"
                keys = await redis.keys(pattern)
                if keys:
                    await redis.delete(*keys)
    
                
            except Exception as e:
                db.rollback()
                raise HTTPException(status_code=500, detail=f"Error updating profile: {str(e)}")
           
   
        elif request.method == "password":
            if request.role != "client":
                raise HTTPException(status_code=400, detail="Only client can change passwords.")
   
            client = db.query(Client).filter(Client.client_id == request.client_id).first()
            if not client:
                raise HTTPException(status_code=404, detail="Client not found.")
 
            if not verify_password(request.oldPassword, client.password):
                raise HTTPException(status_code=400, detail="Incorrect old password.")
            else:
                hashed_password = get_password_hash(request.newPassword)
                client.password = hashed_password
   
            try:
                db.commit()
            except Exception as e:
                db.rollback()
                raise HTTPException(status_code=500, detail=f"Error updating password: {str(e)}")
            
            gym_id=client.gym_id
            is_changed=False
   
        client_status_key = f"{request.client_id}:{gym_id}:status"


        if is_changed:
            mobile_otp=generate_otp()
            await redis.set(f"otp:{client.contact}", mobile_otp, ex=300)
            if await async_send_verification_sms(client.contact, mobile_otp):
                print(f"Verification OTP send successfully to {client.contact}")
 
        if await redis.exists(client_status_key):
            await redis.delete(client_status_key)
       
        post_key=f"gym:{gym_id}:posts"
        if await redis.exists(post_key):
            await redis.delete(post_key)

        client_data=f"gym:{gym_id}:clientdata"
        if await redis.exists(client_data):
            await redis.delete(client_data)
        
       
        return {
            "status":200,
            "message": "Profile updated successfully.",
            "is_changed":is_changed,
            "data":{ 
                "verification":json.loads(client.verification),
                "contact":client.contact, 
                "id":client.client_id
                }
            }
 
    except Exception as e:
        print(e)
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred {e}")



class InputData(BaseModel):
    type: str
    client_id: int
    target_weight: Optional[float] = None
    actual_weight: Optional[float] = None
    start_weight: Optional[float] = None
    actual_water:Optional[float] = None
    target_water:Optional[float] = None
    calories: Optional[int] = None
    protein: Optional[int] = None
    carbs: Optional[int] = None
    fat: Optional[int] = None
    fiber: Optional[int] = None
    sugar: Optional[int] = None
    calcium: Optional[int] = None
    magnesium: Optional[int] = None
    potassium: Optional[int] = None
    Iodine: Optional[int] = None
    Iron: Optional[int] = None
    


@router.post("/add_inputs")
async def add_input(request: InputData, db: Session = Depends(get_db),redis: Redis = Depends(get_redis)):
    try:
        input_type = request.type.lower()
        if input_type == "weight":
            actual_weight = request.actual_weight
            target_weight = request.target_weight
            start_weight= request.start_weight
           
           
            if request.client_id is None:
                raise HTTPException(
                    status_code=400,
                    detail="client_id and weight are required for weight update"
                )
            record_date = date.today()
 
            if actual_weight:
 
                existing_record = db.query(ClientActual).filter(
                    ClientActual.client_id == request.client_id,
                    ClientActual.date == record_date
                ).first()
 
                if existing_record:
                    existing_record.weight = request.actual_weight
                else:
                   
                    new_actual = ClientActual(
                        client_id=request.client_id,
                        date=record_date,
                        weight=actual_weight
                    )
                    db.add(new_actual)
 
                db.commit()
 
                weight= db.query(Client).filter(Client.client_id==request.client_id).first()
                weight.weight=request.actual_weight
                db.commit()
 
 
                month_start_date = date(record_date.year, record_date.month, 1)
                analysis_record = db.query(ClientGeneralAnalysis).filter(
                    ClientGeneralAnalysis.client_id == request.client_id,
                    ClientGeneralAnalysis.date == month_start_date
                ).first()
               
                if analysis_record:
                    if analysis_record.weight is not None:
                        analysis_record.weight = (analysis_record.weight + actual_weight) / 2
                    else:
                        analysis_record.weight = actual_weight
                else:
                    new_analysis = ClientGeneralAnalysis(
                        client_id=request.client_id,
                        date=month_start_date,
                        weight=actual_weight
                    )
                    db.add(new_analysis)
               
                db.commit()
               
           
            if target_weight:
                existing_target = db.query(ClientTarget).filter(
                ClientTarget.client_id == request.client_id
                ).first()
 
                if existing_target:
                    existing_target.weight = target_weight
                    db.commit()
                   
                else:
               
                    new_target = ClientTarget(
                        client_id=request.client_id,
                        weight=target_weight
                       
                    )
                    db.add(new_target)
                    db.commit()
 
            if start_weight:
 
                existing_target = db.query(ClientTarget).filter(
                ClientTarget.client_id == request.client_id
                ).first()
 
                if existing_target:
                   
                    existing_target.start_weight = start_weight
                    db.commit()
                   
                else:
               
                    new_target = ClientTarget(
                        client_id=request.client_id,
                        start_weight=request.start_weight
                       
                    )
                    db.add(new_target)
                    db.commit()
            
            target_actual_key = f"client{request.client_id}:initial_target_actual"
            if await redis.exists(target_actual_key):
                await redis.delete(target_actual_key)

            client_status_key = f"client{request.client_id}:initialstatus"
            if await redis.exists(client_status_key):
                await redis.delete(client_status_key)
 
            client_status_key_pattern = "*:status"
            client_status_key = await redis.keys(client_status_key_pattern)
            if client_status_key:
                await redis.delete(*client_status_key)
 
            analytics_key_pattern = "*:analytics"
            analytics_key = await redis.keys(analytics_key_pattern)
            if analytics_key:
                await redis.delete(*analytics_key)
           
 
            pattern = "*:target_actual"
            keys = await redis.keys(pattern)
            if keys:
                await redis.delete(*keys)

 
            chart_key_pattern = "*:chart"
            chart_key = await redis.keys(chart_key_pattern)
            if chart_key:
                await redis.delete(*chart_key)
                   
 
 
            return{"status":200,"message":"weight added successfully"}
 
        elif input_type == "calories":
            if request.client_id is None:
                raise HTTPException(
                    status_code=400,
                    detail="client_id is required for calorie target update"
                )
 
            existing_target = db.query(ClientTarget).filter(
                ClientTarget.client_id == request.client_id
            ).first()
 
            if existing_target:
               
                existing_target.calories = request.calories
                existing_target.protein = request.protein
                existing_target.carbs = request.carbs
                existing_target.fat = request.fat
                existing_target.fiber=request.fiber
                existing_target.sugar=request.sugar
                existing_target.calcium=request.calcium
                existing_target.magnesium=request.magnesium
                existing_target.potassium=request.potassium
                existing_target.Iodine=request.Iodine
                existing_target.Iron=request.Iron
                db.commit()
               
 
            else:
               
                new_target = ClientTarget(
                    client_id=request.client_id,
                    calories=request.calories,
                    protein=request.protein,
                    carbs=request.carbs,
                    fat=request.fat,
                    fiber=request.fiber,
                    sugar=request.sugar,
                    calcium=request.calcium,
                    magnesium=request.magnesium,
                    potassium=request.potassium,
                    Iodine=request.Iodine,
                    Iron=request.Iron
                   
                )
                db.add(new_target)
                db.commit()

            target_actual_key = f"client{request.client_id}:initial_target_actual"
            if await redis.exists(target_actual_key):
                await redis.delete(target_actual_key)

            client_status_key = f"client{request.client_id}:initialstatus"
            if await redis.exists(client_status_key):
                await redis.delete(client_status_key)
 
            client_status_key_pattern = "*:status"
            client_status_key = await redis.keys(client_status_key_pattern)
            if client_status_key:
                await redis.delete(*client_status_key)
 
            analytics_key_pattern = "*:analytics"
            analytics_key = await redis.keys(analytics_key_pattern)
            if analytics_key:
                await redis.delete(*analytics_key)
           
 
            pattern = "*:target_actual"
            keys = await redis.keys(pattern)
            if keys:
                await redis.delete(*keys)

 
            chart_key_pattern = "*:chart"
            chart_key = await redis.keys(chart_key_pattern)
            if chart_key:
                await redis.delete(*chart_key)
               
            return{"status":200,"message":"Calories added successfully"}
 
 
        elif input_type == "water":
            actual_water=request.actual_water
            target_water=request.target_water
            if actual_water:
                if actual_water>-1:
                    if request.client_id is None or actual_water is None:
                        raise HTTPException(
                            status_code=400,
                            detail="client_id and water_intake are required for water update"
                        )
                    record_date = date.today()
   
                    existing_actual = db.query(ClientActual).filter(
                        ClientActual.client_id == request.client_id,
                        ClientActual.date == record_date
                    ).first()
   
                    if existing_actual:
                        existing_actual.water_intake = actual_water
                        db.commit()
                    else:
                        new_actual = ClientActual(
                            client_id=request.client_id,
                            date=record_date,
                            water_intake=actual_water
                        )
                        db.add(new_actual)
                        db.commit()
 
 
            if target_water:
 
                existing_target = db.query(ClientTarget).filter(
                    ClientTarget.client_id == request.client_id
                ).first()
                if existing_target:
                    existing_target.water_intake = request.target_water
                    db.commit()
                else:
                    new_target = ClientTarget(
                        client_id=request.client_id,
                        water_intake=request.target_water
                    )
                    db.add(new_target)
                    db.commit()
            
            target_actual_key = f"client{request.client_id}:initial_target_actual"
            if await redis.exists(target_actual_key):
                await redis.delete(target_actual_key)

            client_status_key = f"client{request.client_id}:initialstatus"
            if await redis.exists(client_status_key):
                await redis.delete(client_status_key)

            client_status_key_pattern = "*:status"
            client_status_key = await redis.keys(client_status_key_pattern)
            if client_status_key:
                await redis.delete(*client_status_key)
 
            analytics_key_pattern = "*:analytics"
            analytics_key = await redis.keys(analytics_key_pattern)
            if analytics_key:
                await redis.delete(*analytics_key)
           
 
            pattern = "*:target_actual"
            keys = await redis.keys(pattern)
            if keys:
                await redis.delete(*keys)

 
            chart_key_pattern = "*:chart"
            chart_key = await redis.keys(chart_key_pattern)
            if chart_key:
                await redis.delete(*chart_key)
           
            return{"status":200,"message":"Water qty added successfully"}
 
    except Exception as e:
        print(f"Unexpected error: {str(e)}")
        raise HTTPException(status_code=500, detail="An unexpected error occurred.")
 
    else:
        raise HTTPException(
            status_code=500,
            detail="Invalid type provided. Please use 'weight' or 'calories'."
        ) 




def get_sessions_data(gym_id: int, db: Session) -> list:
    now = datetime.now()
    today_date = now.date()
    current_time = now.time()
    sessions_query = (
        db.query(New_Session, Client.name.label("host_name"), Client.profile.label("host_profile"))
          .join(Client, New_Session.host_id == Client.client_id)
          .filter(New_Session.gym_id == gym_id)
          .filter(
              or_(
                  New_Session.session_date > today_date,
                  and_(
                      New_Session.session_date == today_date,
                      New_Session.session_time >= current_time
                  )
              )
          )
          .all()
    )
   
    sessions_data = []
    for session, host_name, host_profile in sessions_query:
        participants_query = (
            db.query(Participant,
                     Client.name.label("participant_name"),
                     Client.gender.label("participant_gender"),
                     Client.profile.label("participant_profile"))
              .join(Client, Participant.user_id == Client.client_id)
              .filter(Participant.session_id == session.session_id)
              .all()
        )
 
        participants_list = []
        for participant, participant_name, participant_gender, participant_profile in participants_query:
            participants_list.append({
                "participant_id": participant.participant_id,
                "user_id": participant.user_id,
                "participant_name": participant_name,
                "gender": participant_gender,
                "participant_profile": participant_profile,
                "proposed_time": participant.proposed_time  # assumed to be datetime
            })
 
        proposals_query = (
            db.query(JoinProposal, Client.name.label("proposer_name"), Client.profile.label("proposer_profile"))
              .join(Client, JoinProposal.proposer_id == Client.client_id)
              .filter(JoinProposal.session_id == session.session_id)
              .all()
        )
        join_proposals_list = []
        for proposal, proposer_name, proposer_profile in proposals_query:
            join_proposals_list.append({
                "proposal_id": proposal.proposal_id,
                "proposer_id": proposal.proposer_id,
                "proposer_name": proposer_name,
                "proposer_profile": proposer_profile,
                "proposal_time": proposal.proposed_time  # assumed to be datetime
            })
 
        participant_count = len(participants_list)
        rejected_rows = db.query(RejectedProposal.user_id).filter(RejectedProposal.session_id == session.session_id).all()
        rejected_user_ids = [row[0] for row in rejected_rows] if rejected_rows else [] 

        session_data = {
            "session_id": session.session_id,
            "gym_id": session.gym_id,
            "workout_type": session.workout_type,
            "session_date": session.session_date,    # date object
            "session_time": session.session_time,      # time object (if applicable)
            "host_id": session.host_id,
            "participant_limit": session.participant_limit,
            "gender_preference": session.gender_preference,
            "host_name": host_name,
            "host_profile": host_profile,
            "participant_count": participant_count,
            "participants": participants_list,
            "requests": join_proposals_list,
            "rejected": rejected_user_ids
        }
       
        sessions_data.append(session_data)
    return sessions_data


from datetime import date, time as dt_time

class SessionCreate(BaseModel):
    gym_id:int
    workout_type: List[str]
    session_date: date
    session_time: dt_time
    host_id: int
    participant_limit: int
    gender_preference: str
 
class JoinProposalCreate(BaseModel):
    session_id: int
    gym_id:int
    proposer_id: int
    proposed_time: dt_time


@router.post("/gym_buddy/create_session")
async def create_session(http_request: Request,session: SessionCreate, db: Session = Depends(get_db)):
    try:


        if not session.workout_type or len(session.workout_type) == 0:
            raise HTTPException(
                status_code=400, 
                detail="At least one muscle group must be selected"
            )
        db_session = New_Session(
            workout_type=session.workout_type,
            gym_id=session.gym_id,
            session_time=session.session_time,
            session_date=session.session_date,
            host_id=session.host_id,
            participant_limit=session.participant_limit,
            gender_preference=session.gender_preference
        )
        db.add(db_session)
        db.commit()
        db.refresh(db_session)


        sessions_data = get_sessions_data(session.gym_id, db)
       
        message = json.dumps({
            "action": "update_sessions",
            "data": sessions_data
        }, default=str)
        
        await http_request.app.state.session_hub.publish(session.gym_id, message)

        return {"status": 200, "data": db_session}
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Error creating session: {str(e)}")
  
@router.get("/gym_buddy/get_sessions")
async def get_sessions(gym_id: int, db: Session = Depends(get_db)):
 
    try:
        now = datetime.now()
        today_date = now.date()
        current_time = now.time()
        sessionkey = f"gym:{gym_id}:buddysessions" 
        sessions_query = (
            db.query(New_Session, Client.name.label("host_name"), Client.profile.label("host_profile"))
              .join(Client, New_Session.host_id == Client.client_id)
              .filter(New_Session.gym_id == gym_id)
              .filter(
                  or_(
                      New_Session.session_date > today_date,
                      and_(
                          New_Session.session_date == today_date,
                          New_Session.session_time >= current_time
                      )
                  )
              )
              .all()
        )
       
        sessions_data = []
 
        for session, host_name, host_profile in sessions_query:

            participants_query = (
                db.query(Participant,
                         Client.name.label("participant_name"),
                         Client.gender.label("participant_gender"),
                         Client.profile.label("participant_profile"))
                  .join(Client, Participant.user_id == Client.client_id)
                  .filter(Participant.session_id == session.session_id)
                  .all()
            )
 
            participants_list = []
            for participant, participant_name, participant_gender, participant_profile in participants_query:
                participants_list.append({
                    "participant_id": participant.participant_id,
                    "user_id":participant.user_id,
                    "participant_name": participant_name,
                    "gender": participant_gender,
                    "participant_profile":participant_profile,
                    "proposed_time": participant.proposed_time  
                })
 
 
            proposals_query = (
                db.query(JoinProposal, Client.name.label("proposer_name"), Client.profile.label("proposer_profile"))
                  .join(Client, JoinProposal.proposer_id == Client.client_id)
                  .filter(JoinProposal.session_id == session.session_id)
                  .all()
            )
            join_proposals_list = []
            for proposal, proposer_name, proposer_profile in proposals_query:
                join_proposals_list.append({
                    "proposal_id": proposal.proposal_id,
                    "proposer_id":proposal.proposer_id,
                    "proposer_name": proposer_name,
                    "proposer_profile":proposer_profile,
                    "proposal_time": proposal.proposed_time

                })
 
            participant_count = len(participants_list)
            rejected_rows = db.query(RejectedProposal.user_id).filter(RejectedProposal.session_id == session.session_id).all()
            rejected_user_ids = [row[0] for row in rejected_rows] if rejected_rows else [] 
            

            session_data = {
                "session_id": session.session_id,
                "gym_id": session.gym_id,
                "workout_type": session.workout_type,
                "session_date": session.session_date,
                "session_time": session.session_time,
                "host_id": session.host_id,
                "participant_limit": session.participant_limit,
                "gender_preference": session.gender_preference,
                "host_name": host_name,
                "host_profile":host_profile,
                "participant_count": participant_count,
                "participants": participants_list,      
                "requests": join_proposals_list ,
                "rejected":rejected_user_ids  

            }
           
            sessions_data.append(session_data)
        print(sessions_data)
       
        return {"status": 200, "data": sessions_data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching sessions: {str(e)}")
    
@router.post("/gym_buddy/join_proposal")
async def create_join_proposal(http_request:Request,proposal: JoinProposalCreate, db: Session = Depends(get_db)):
    try:        

        proposer = db.query(Client).filter(Client.client_id == proposal.proposer_id).first()
        if not proposer:
            raise HTTPException(status_code=404, detail="Proposer not found")
        
        session_obj = db.query(New_Session).filter(New_Session.session_id == proposal.session_id).first()
        if not session_obj:
            
            raise HTTPException(status_code=404, detail="Session not found")
 
        if session_obj.gender_preference.lower() != "any":
            if proposer.gender.lower() != session_obj.gender_preference.lower():
            
                raise HTTPException(
                    status_code=400,
                    detail="You cannot participate: Your gender does not match the session's preference.")
        
        db_proposal = JoinProposal(
            session_id=proposal.session_id,
            proposer_id=proposal.proposer_id,
            proposed_time=proposal.proposed_time
        )
        db.add(db_proposal)
        db.commit()
        db.refresh(db_proposal)
        
        rejected=db.query(RejectedProposal).filter(RejectedProposal.session_id==proposal.session_id,RejectedProposal.user_id==proposal.proposer_id).all()
        if rejected:
            for item in rejected:  
                db.delete(item)  
            db.commit()

        sessions_data = get_sessions_data(proposal.gym_id, db)
        message = json.dumps({
            "action": "update_sessions",
            "data": sessions_data
        }, default=str)
        

        await http_request.app.state.session_hub.publish(proposal.gym_id, message)

        return {"status": 200, "message": "successfuly created join proposal"}
    
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Error creating join proposal: {str(e)}")


class AcceptProposalCreate(BaseModel):
    session_id: int
    proposal_id: int
    gym_id:int

@router.post("/gym_buddy/accept_proposal")
async def accept_proposal(http_request:Request,request:AcceptProposalCreate, db: Session = Depends(get_db)):
    try:
        session_id=request.session_id
        proposal_id=request.proposal_id
        session = db.query(New_Session).filter(New_Session.session_id == session_id).first()
        if not session:
            raise HTTPException(status_code=404, detail="Session not found")
    
        current_participants = db.query(Participant).filter(Participant.session_id == session_id).count()
        if current_participants >= session.participant_limit:
            raise HTTPException(status_code=400, detail="Session is full")
    
        proposal = db.query(JoinProposal).filter(JoinProposal.proposal_id == proposal_id).first()
        if not proposal:
            raise HTTPException(status_code=404, detail="Proposal not found")
    
        db_participant = Participant(session_id=session_id, user_id=proposal.proposer_id,proposed_time=proposal.proposed_time)
        db.add(db_participant)
        db.delete(proposal)
        db.commit()
        rejected=db.query(RejectedProposal).filter(RejectedProposal.session_id==session_id,RejectedProposal.user_id==proposal.proposer_id).all()
        if rejected:
            for item in rejected:  
                db.delete(item)  
            
            db.commit()

        sessions_data = get_sessions_data(request.gym_id, db)
        message = json.dumps({
            "action": "update_sessions",
            "data": sessions_data
        }, default=str)

        await http_request.app.state.session_hub.publish(request.gym_id, message)

        
        return {"status": 200, "message": "Proposal accepted and user added to session"}

    except Exception as e:
        db.rollback()
        print("error is,",e)
        raise HTTPException(status_code=500, detail=f"Error accepting proposal:{str(e)}")
    

@router.delete("/gym_buddy/delete_proposal")
async def reject_proposal(http_request:Request,session_id:int,proposal_id: int,proposer_id:int,gym_id:int, db: Session = Depends(get_db)):
    try:
        proposal = db.query(JoinProposal).filter(JoinProposal.proposal_id == proposal_id).first()
        if not proposal:
            raise HTTPException(status_code=404, detail="Proposal not found")
    
        db.delete(proposal)
        db.commit()

        Rejected=RejectedProposal(session_id=session_id,user_id=proposer_id)
        db.add(Rejected)
        db.commit()
        sessions_data = get_sessions_data(gym_id, db)
        message = json.dumps({
            "action": "update_sessions",
            "data": sessions_data
        }, default=str)

        await http_request.app.state.session_hub.publish(gym_id, message)

        return {"status": 200, "message": "Proposal rejected"}

    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Error rejecting proposal: {str(e)}")

 
@router.delete("/gym_buddy/delete_session")
async def reject_proposal(session_id:int,gym_id:int, db: Session = Depends(get_db)):
    try:
        proposal = db.query(New_Session).filter(New_Session.session_id == session_id).first()
        if not proposal:
            raise HTTPException(status_code=404, detail="session not found")
    
        db.delete(proposal)
        db.commit()
        sessions_data = get_sessions_data(gym_id, db)
        message = json.dumps({
            "action": "update_sessions",
            "data": sessions_data
        }, default=str)
        
        await session_update_manager.broadcast(gym_id, message)
        return {"status": 200, "message": "Session deleted"}

    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Error deleting session: {str(e)}")

@router.get("/gym_buddy/session_details")
async def get_session_details(session_id: int, db: Session = Depends(get_db)):
    try:
 
        participants_query = (
            db.query(Participant, Client.name.label("participant_name"))
              .join(Client, Participant.user_id == Client.client_id)
              .filter(Participant.session_id == session_id)
              .all()
        )
       
        participants = []
        for participant, participant_name in participants_query:
            participants.append({
                "participant_id": participant.participant_id,
                "participant_name": participant_name
            })
 
        proposals_query = (
            db.query(JoinProposal, Client.name.label("proposer_name"))
              .join(Client, JoinProposal.proposer_id == Client.client_id)
              .filter(JoinProposal.session_id == session_id)
              .all()
        )
       
        join_proposals = []
        for proposal, proposer_name in proposals_query:
            join_proposals.append({
                "proposal_id": proposal.proposal_id,
                "proposer_name": proposer_name
            })
       
        return {
            "status": 200,
            "data": {
                "participants": participants,
                "join_proposals": join_proposals
            }
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching session details: {str(e)}")
 




 
@router.get("/foods")
async def read_foods(
    page: int = Query(1, gt=0, description="Page number, starting at 1"),
    limit: int = Query(10, gt=0, description="Number of items per page"),
    db: Session = Depends(get_db)
):
    try:
        offset = (page - 1) * limit
        foods = db.query(Food).offset(offset).limit(limit).all()

        return {"status":200,"data":foods}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")

@router.get("/food_categories")
async def read_foods(
    db: Session = Depends(get_db)
):
    try:
        categories = [category[0] for category in db.query(Food.categories).distinct().all()]

        return {"status":200,"data":categories}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")

@router.get("/search")
async def search_food(
    query: str = Query(..., min_length=3, description="Search query, minimum 3 characters"),
    page: int = Query(1, gt=0, description="Page number, starting at 1"),
    limit: int = Query(10, gt=0, description="Number of items per page"),
    db: Session = Depends(get_db)
):
    try:
        offset = (page - 1) * limit

        startswith_query = db.query(Food).filter(Food.item.ilike(f"{query}%"))
        startswith_count = db.query(func.count()).filter(Food.item.ilike(f"{query}%")).scalar()
        
        if startswith_count == 0: 
            contains_query = db.query(Food).filter(Food.item.ilike(f"%{query}%"))
            total_count = db.query(func.count()).filter(Food.item.ilike(f"%{query}%")).scalar()
            foods = contains_query.offset(offset).limit(limit).all()
        else:
            total_count = startswith_count
            foods = startswith_query.offset(offset).limit(limit).all()

        return {"status": 200, "data": foods, "total": total_count, "page": page, "limit": limit}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")
    
@router.get("/foods/categories")
async def get_foods_by_category(
    categories: str = Query(..., description="Comma-separated list of categories"),
    page: int = Query(1, gt=0, description="Page number, starting at 1"),
    limit: int = Query(10, gt=0, description="Number of items per page"),
    db: Session = Depends(get_db)
):
    try:
        offset = (page - 1) * limit
        category_list = [cat.strip() for cat in categories.split(',')]
        
        query = db.query(Food).filter(Food.categories.in_(category_list))
        total_count = query.count()
    
        foods = query.offset(offset).limit(limit).all()
        
        return {
            "status": 200,
            "data": foods,
            "total": total_count,
            "page": page,
            "limit": limit,
            "categories": category_list
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")


def calculate_bmr(weight, height, age, gender="male"):
    try:
        if gender == "male":
            return 10 * weight + 6.25 * height - 5 * age + 5
        else:
            return 10 * weight + 6.25 * height - 5 * age - 161

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred {e}")
 
activity_multipliers = {
    "sedentary": 1.2,
    "lightly_active": 1.375,
    "moderately_active": 1.55,
    "very_active": 1.725,
    "super_active": 1.9,
}
 
def calculate_macros(calories, goals):

    try:
        if goals == "weight_loss":
            carbs=calories*0.30
            carbs_grams=round(carbs/4)
    
            protein=calories*0.45
            protein_grams=round(protein/4)
    
            fat=calories*0.2
            fat_grams=round(fat/9)
    
        elif goals == "weight_gain":
            carbs=calories*0.45
            carbs_grams=round(carbs/4)
    
            protein=calories*0.35
            protein_grams=round(protein/4)
    
            fat=calories*0.2
            fat_grams=round(fat/9)
    
        else:
            carbs=calories*0.35
            carbs_grams=round(carbs/4)
    
            protein=calories*0.35
            protein_grams=round(protein/4)
    
            fat=calories*0.3
            fat_grams=round(fat/9)
    
        return protein_grams, carbs_grams, fat_grams

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred {e}")
 
class CaloriesData(BaseModel):
    client_id:int
    height: float
    weight: float
    age: int
    goals:str
    lifestyle:str

@router.post("/calculate-calories")
async def calculate_calories(data:CaloriesData, db: Session = Depends(get_db),redis: Redis = Depends(get_redis)):
    try:

        bmr = calculate_bmr(data.weight, data.height, data.age)
 
        tdee = bmr * activity_multipliers[data.lifestyle]
 
        if data.goals == "weight_loss":
            tdee -= 500
        elif data.goals == "weight_gain":
            tdee += 500
 
        protein, carbs, fat = calculate_macros(tdee, data.goals)
 
        client_target = db.query(ClientTarget).filter(ClientTarget.client_id == data.client_id).first()
        client=db.query(Client).filter(Client.client_id == data.client_id).first()
        client.weight=data.weight
        client.height=data.height
        client.age=data.age
        client.goals=data.goals
        client.lifestyle=data.lifestyle
        db.commit()
        db.refresh(client)

        if client_target:
            client_target.calories = int(tdee)
            client_target.protein = protein
            client_target.carbs = carbs
            client_target.fat = fat
            client_target.sugar=25
            client_target.fiber=30
            client_target.calcium=1000
            client_target.magnesium=450
            client_target.potassium=4000
            client_target.Iodine=150
            client_target.Iron=18
            client_target.updated_at = datetime.now()
   
            db.commit()
 
        else:
            client_target = ClientTarget(
                client_id=data.client_id,
                calories=int(tdee),
                protein=protein,
                carbs=carbs,
                fat=fat,
                sugar=25,
                fiber=30,
                calcium=1000,
                magnesium=450,
                potassium=4000,
                Iodine=150,
                Iron=18,
                updated_at=datetime.now(),
            )
 
            db.add(client_target)
            db.commit()
 
        target_actual_keys_pattern = "*:initial_target_actual"
        target_actual_keys = await redis.keys(target_actual_keys_pattern)
        if target_actual_keys:
            await redis.delete(*target_actual_keys)

        client_status_key_pattern = "*:initialstatus"
        client_status_key = await redis.keys(client_status_key_pattern)
        if client_status_key:
            await redis.delete(*client_status_key)

        pattern = "*:target_actual"
        keys = await redis.keys(pattern)
        if keys:
            await redis.delete(*keys)
        return {
            "status": 200,
            "message": "Calories calculated successfully",
            "data":{
                "client_id": data.client_id,
                "calories": int(tdee),
                "protein": protein,
                "carbs": carbs,
                "fat": fat,
                "sugar":25,
                "fiber":30,
                "calcium":1000,
                "magnesium":450,
                "potassium":4000,
                "Iodine":150,
                "Iron":18
            }
           
        }
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=400, detail=f"Error: {str(e)}")   





@router.get("/watertracker")
async def get_clients_water(
    gym_id:int,
    client_id:int,
    db: Session = Depends(get_db),
    redis: Redis = Depends(get_redis)
):
    try:

        today=datetime.now().date()         
        target_data = db.query(ClientTarget).filter(ClientTarget.client_id == client_id).first()
        actual_data = db.query(ClientActual).filter(
            ClientActual.client_id == client_id, ClientActual.date == today
        ).first()
        target_actual = {
            "water_intake": {
                "target": target_data.water_intake if target_data else 0,
                "actual": actual_data.water_intake if actual_data else 0,
            },
        }

        target_actual_key = f"{client_id}:{gym_id}:target_actual"
        if await redis.exists(target_actual_key):
            await redis.delete(target_actual_key)

        return {
            "status": 200,
            "message": "Data fetched successfully",
            "data": {
                "target_actual": target_actual,
            }
        }
    except Exception as e:
        return {
        "status": 500,
        "message": f"An error occurred: {str(e)}",
        }
    
def get_badge_for_xp(xp: int, db: Session):
    try:
 
        badge_record = (
            db.query(RewardBadge)
            .filter(RewardBadge.min_points <= xp, RewardBadge.max_points > xp)
            .first()
        )
        if badge_record:
            return {"badge": badge_record.badge, "level": badge_record.level}
        return {"badge": None, "level": None}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred {e}")
 
def record_to_dict(record, db: Session):

    try:
        badge_info = get_badge_for_xp(record.xp, db)
        client=db.query(Client).filter(Client.client_id==record.client_id).first()
        return {
            "client_id": record.client_id or None,
            "client_name": client.name if client else None,
            "profile": client.profile if client else None,
            "xp": record.xp or None,
            "badge": badge_info["badge"] or None,
            "level": badge_info["level"] or None
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred {e}")

 
@router.get("/leaderboard")
async def get_leaderboard(gym_id: int, db: Session = Depends(get_db)):
 
    try:
        today = date.today()       
        daily_records = (
            db.query(LeaderboardDaily)
            .filter(LeaderboardDaily.gym_id == gym_id, LeaderboardDaily.date == today)
            .order_by(desc(LeaderboardDaily.xp))
            .all()
        ) or []

        monthly_records = (
            db.query(LeaderboardMonthly)
            .filter(
                LeaderboardMonthly.gym_id == gym_id,
                extract('year', LeaderboardMonthly.month) == today.year,
                extract('month', LeaderboardMonthly.month) == today.month
            )
            .order_by(desc(LeaderboardMonthly.xp))
            .all()
        ) or []

        overall_records = (
            db.query(LeaderboardOverall)
            .filter(LeaderboardOverall.gym_id == gym_id)
            .order_by(desc(LeaderboardOverall.xp))
            .all()
        ) or []

        if not daily_records:
            daily_list=[]
        else:
            daily_list = [record_to_dict(rec, db) for rec in daily_records]

        if not monthly_records:
            monthly_list=[]
        else:
            monthly_list = [record_to_dict(rec, db) for rec in monthly_records]
         
        if not overall_records:
            overall_list=[]
        else:
            overall_list = [record_to_dict(rec, db) for rec in overall_records]
           
        return {
            "status":200,
            "data":{
            "today": daily_list,
            "month": monthly_list,
            "overall": overall_list
            }
        }
   
    except Exception as e:
        print(str(e))
        raise HTTPException(status_code=500, detail="An unexpected error occurred.")
 

@router.get("/show_rewards_page")
async def show_rewards_page(client_id: int, gym_id: int, db: Session = Depends(get_db)):
    try:
        overall = (
            db.query(LeaderboardOverall)
            .filter(
                LeaderboardOverall.client_id == client_id,
                LeaderboardOverall.gym_id == gym_id
            )
            .first()
        )
        if not overall:
            badge_record=None
            client_xp=0
        else:
        
            client_xp = overall.xp
    
            badge_record = (
                db.query(RewardBadge)
                .filter(RewardBadge.min_points <= client_xp, RewardBadge.max_points > client_xp)
                .first()
            )
    
        if badge_record:
            next_level = (
                db.query(RewardBadge)
                .filter(RewardBadge.min_points > client_xp)
                .order_by(asc(RewardBadge.min_points))
                .first()
            )
            next_level_start = next_level.min_points if next_level else None
 
            client_badge = {
                "badge": badge_record.badge,
                "image_url": badge_record.image_url,
                "level":badge_record.level,
                "next_level_start": next_level_start,
                "client_xp": client_xp
            }
        else:
            client_badge = {
                "badge": "Beginner",
                "image_url": None,
                "next_level_start": 500,
                "client_xp": client_xp
            }
 
 
        client_history = (
            db.query(RewardClientHistory)
            .filter(RewardClientHistory.client_id == client_id)
            .all()
        ) or []
 
        quests = db.query(RewardQuest).all()
 
        gym_rewards = (
            db.query(RewardGym)
            .filter(RewardGym.gym_id == gym_id)
            .all()
        ) or []
 
        monthly_leaderboard = (
            db.query(LeaderboardMonthly)
            .filter(LeaderboardMonthly.gym_id == gym_id,LeaderboardMonthly.client_id==client_id)
            .order_by(desc(LeaderboardMonthly.xp))
            .all()
        ) or []
 
        def to_dict_list(items: List) -> List[dict]:
            return [item.__dict__ for item in items if hasattr(item, '__dict__')]
 
        return {
            "status": 200,
            "data":{
            "client_badge": client_badge,
            "client_history": to_dict_list(client_history),
            "quest": to_dict_list(quests),
            "gym_rewards": to_dict_list(gym_rewards),
            "monthly_leaderboard": to_dict_list(monthly_leaderboard)
            }
           
        }
    except Exception as e:
            print(str(e))
            raise HTTPException(status_code=500, detail=f"An unexpected error occurred {e}")
  
 

TAG_LINES: list[str] = [
    "Push past limits—your future self will thank you!",
    "Every rep fuels progress—stay relentless and finish strong!",
    "Sweat now, shine later—victory loves disciplined hearts!",
    "Consistency crushes excuses—show up and conquer goals today!",
    "Your grind inspires others—keep pushing and lead greatness!",
    "Transform sweat into strength—momentum builds champions!",
    "No shortcuts to power—earn progress rep by rep!",
    "Commit to today's workout—tomorrow's victories depend on it!",
    "Rise early, train hard—success waits for persistent warriors!",
    "Fuel determination with sweat—each session sculpts confidence!",
    "Beat yesterday's limits—progress is built on daily commitment!",
    "Strength grows silently—show up and let actions roar!",
]


@router.get("/get_xp")
async def get_client_xp(client_id: int, db: Session = Depends(get_db), redis: Redis = Depends(get_redis)):
 
    try:
        profile = db.query(Client).filter(Client.client_id == client_id).first()
        xp_row  = db.query(LeaderboardOverall).filter(LeaderboardOverall.client_id == client_id).first()
        tag_line = random.choice(TAG_LINES)


        rediskey = f'logo:{profile.gym_id}:profileData'
        cached_gym = await redis.get(rediskey)
        gym_data={
            'name':'',
            'logo':''
        }
 
        if cached_gym:
            gym_data=json.loads(cached_gym)
        else:
            gym = db.query(Gym).filter(Gym.gym_id == profile.gym_id).first()
            gym_data["logo"] = gym.logo if gym.logo else ''
            gym_data['name'] = gym.name if gym.name else ''
 
            await redis.set(rediskey, json.dumps(gym_data), ex=8600)
 
        if not xp_row:
            return {
                "status": 200,
                "data": 0,
                "profile": profile.profile,
                "name": profile.name,
                'gym':gym_data,
                "badge": "https://fittbot-uploads.s3.ap-south-2.amazonaws.com/Badges/BEGINNER.png",
                "progress": 0,
                "email":profile.email,
                "tag":tag_line
 
            }
 
        overall_xp = xp_row.xp
 
        current_row = (
            db.query(RewardBadge)
            .filter(
                RewardBadge.min_points <= overall_xp,
                RewardBadge.max_points >= overall_xp,
            )
            .order_by(asc(RewardBadge.min_points))
            .first()
        )
        if not current_row:
 
            return {
                "status": 200,
                "data": overall_xp,
                "profile": profile.profile,
                "name": profile.name,
                "badge": None,
                'gym':gym_data,
                "progress": 0,
                "email":profile.email,
                "tag":tag_line
            }
 
 
        badge_rows = (
            db.query(RewardBadge)
            .filter(RewardBadge.badge == current_row.badge)
            .order_by(asc(RewardBadge.min_points))
            .all()
        )
 
        start_xp = badge_rows[0].min_points
        end_xp   = badge_rows[-1].max_points
 
 
        if end_xp > start_xp:
            progress = (overall_xp - start_xp) / (end_xp - start_xp)
            progress = max(0.0, min(progress, 1.0))
        else:
            progress = 1.0
 
        return {
            "status": 200,
            "data": {"client_id": client_id, "gym_id": profile.gym_id, "xp": overall_xp},
            "profile": profile.profile,
            "name": profile.name,
            "email":profile.email,
            "badge": current_row.image_url,
            'gym':gym_data,
            "progress": round(progress, 4),
            "start_xp": start_xp,
            "end_xp": end_xp,
            "tag":tag_line
        }
 
    except Exception as e:
            print(str(e))
            raise HTTPException(status_code=500, detail=f"An unexpected error occurred {e}")
 



QR_LINK_CONFIG: Dict[str, Dict[str, Any]] = {
    "https://qr1.be/JKBC": {
        "equipment": "Rods Exercises",
        "ids":       [29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44],
    },
    "https://qr1.be/P6Z3": {
        "equipment": "Static Bench Exercises",
        "ids":       [45,46,47,48,49,50,51,52,53],
    },
    "https://qr1.be/74QP": {
        "equipment": "Dumbbell Exercises",
        "ids":       [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28],
    },
    "https://qr1.be/PJYA": {
        "equipment": "Cardio Exercises",
        "ids":       [54,55,56,57,58,59,60,61,62,63,64,65,66],
    },
    "https://qr1.be/AG98": {
        "equipment": "Spin Bike",
        "ids":       [72],
    },
    "https://qr1.be/B52G": {
        "equipment": "Recumbent Bike",
        "ids":       [71],
    },
    "https://qr1.be/EF7D": {
        "equipment": "Upright Bike",
        "ids":       [73],
    },
    "https://qr1.be/DVBL": {
        "equipment": "Treadmill",
        "ids":       [74],
    },
    "https://qr1.be/9HCX": {
        "equipment": "Chest Press Machine",
        "ids":       [97],
    },
    "https://qr1.be/ULBM": {
        "equipment": "Chest Dips Machine",
        "ids":       [68,69],
    },
    "https://qr1.be/VOLC": {
        "equipment": "Seated Calf Raise Machine",
        "ids":       [67],
    },
    "https://qr1.be/YUC9": {
        "equipment": "Wrist Curls Machine",
        "ids":       [70],
    },
    "https://qr1.be/MQJL": {
        "equipment": "Barbell Chest Press Machine",
        "ids":       [98,99],
    },
    "https://qr1.be/AKGI": {
        "equipment": "Lat Pull Down Machine",
        "ids":       [83,84,85,86,87],
    },
    "https://qr1.be/8KUG": {
        "equipment": "Machine Leg Curls",
        "ids":       [100],
    },
    "https://qr1.be/8PL6": {
        "equipment": "Cable Crossover Machine",
        "ids":       [75,76,77,78,79,80,81,82],
    },
    "https://qr1.be/J2BJ": {
        "equipment": "Leg Press Machine",
        "ids":       [93],
    },
    "https://qr1.be/G52K": {
        "equipment": "Smith Machine",
        "ids":       [88,89,90,91,92],
    },
    "https://qr1.be/8H16": {
        "equipment": "V-Squats Machine",
        "ids":       [94],
    },
    "https://qr1.be/K1SB": {
        "equipment": "Fly/Pec Machine",
        "ids":       [95,96],
    },
}


class scanqr(BaseModel):
    link : str
 
@router.post("/scan_qr")
def get_grouped_exercises(req:scanqr, db: Session = Depends(get_db)):
    try:
  
        link = req.link
        print("link is", link)

        config = QR_LINK_CONFIG.get(link)
        if not config:
            raise HTTPException(
                status_code=404,
                detail=f"No equipment configured for link {link!r}"
            )


        ids = config["ids"]
 
        records = db.query(QRCode).filter(QRCode.id.in_(ids)).all()
        if not records:
            raise HTTPException(status_code=404, detail="No records found with given ids.")
 
        response_data = {}
       
        for record in records:
            group = record.muscle_group
            if group not in response_data:
                response_data[group] = {}
                response_data[group]["exercises"]=[]
                response_data[group]["isMuscleGroup"]=False
                response_data[group]["isCardio"]=False
                response_data[group]['gifUrl']=''
            response_data[group]["exercises"].append({"name":record.exercises , "gifPath":record.gifUrl})
            response_data[group]["isMuscleGroup"]=record.isMuscleGroup
            response_data[group]["isCardio"]=record.isCardio
            # response_data[group]["gifUrl"]=record.gifUrl
        print(response_data)
 
        return {"status": 200, "data": response_data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching grouped exercises:{str(e)}")


@router.get("/catalog")
async def get_equipment_catalog(db: Session = Depends(get_db)):
   
    try:
        catalog = _load_equipment_catalog(db)
        equipment_list = []
        for idx, (name, meta) in enumerate(
            sorted(catalog.items(), key=lambda item: item[0].lower()),
            start=1
        ):
            image_url = meta.get("image") if isinstance(meta, dict) else None
            equipment_list.append({
                "id": idx,
                "name": name,
                "image": image_url,
            })

        if not equipment_list:
            raise FittbotHTTPException(
                status_code=404,
                detail="No equipment entries available.",
                error_code="EQUIPMENT_LIST_EMPTY",
            )

        return {"status": 200, "data": equipment_list}

    except FittbotHTTPException:
        raise
    except Exception as exc:
        raise FittbotHTTPException(
            status_code=500,
            detail="Failed to load equipment catalog.",
            error_code="EQUIPMENT_LIST_ERROR",
            log_data={"error": str(exc)},
        )


@router.get("/equipment/exercises")
async def get_equipment_exercises(
    equipment_name: str = Query(..., min_length=1, description="Equipment name to fetch exercises for"),
    db: Session = Depends(get_db),
):
    """Return the exercise list for a specific equipment."""
    try:
        catalog = _load_equipment_catalog(db)
        matched_key = next(
            (key for key in catalog.keys() if key.lower() == equipment_name.lower()),
            None,
        )

        if not matched_key:
            raise FittbotHTTPException(
                status_code=404,
                detail=f"Equipment '{equipment_name}' not found.",
                error_code="EQUIPMENT_NOT_FOUND",
                log_data={"equipment_name": equipment_name},
            )

        details = catalog.get(matched_key, {})
        exercises = details.get("exercises") if isinstance(details, dict) else []
        if not isinstance(exercises, list):
            exercises = []

        return {"status": 200, "data": exercises}

    except FittbotHTTPException:
        raise
    except Exception as exc:
        raise FittbotHTTPException(
            status_code=500,
            detail="Failed to load equipment exercises.",
            error_code="EQUIPMENT_EXERCISE_ERROR",
            log_data={"equipment_name": equipment_name, "error": str(exc)},
        )

class SendGBMessageRequest(BaseModel):
    client_id: int
    session_id: int
    message: str

@router.post("/send_gb_message")
async def send_message(request: SendGBMessageRequest, db: Session = Depends(get_db)):
    try:
        new_message = GBMessage(
            client_id=request.client_id,
            session_id=request.session_id,
            message=request.message,
            sent_at=datetime.now()
        )
        
        db.add(new_message)
        db.commit()
        db.refresh(new_message)

        return {"success": True, "message": "Message sent successfully", "status": 200, "data": {
            "id": new_message.id,
            "client_id": new_message.client_id,
            "session_id": new_message.session_id,
            "message": new_message.message,
            "sent_at": new_message.sent_at.isoformat()
        }}
    
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Error sending message: {e}")




@router.get("/get_gb_messages")
async def get_messages(session_id: int, db: Session = Depends(get_db)):
    try:
       
        messages = (
            db.query(GBMessage, Client.name.label("client_name"))
            .join(Client, GBMessage.client_id == Client.client_id)
            .filter(GBMessage.session_id == session_id)
            .order_by(GBMessage.sent_at.asc())
            .all()
        )

        return {
            "status": 200,
            "data": [
                {
                    "id": msg.GBMessage.id,
                    "client_id": msg.GBMessage.client_id,
                    "client_name": msg.client_name,  
                    "session_id": msg.GBMessage.session_id,
                    "message": msg.GBMessage.message,
                    "sent_at": msg.GBMessage.sent_at.isoformat(),
                }
                for msg in messages
            ],
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching messages: {str(e)}")


class EditGBMessageModel(BaseModel):
    message_id: int
    message: str


@router.put("/edit_gb_message")
async def edit_message(data: EditGBMessageModel, db: Session = Depends(get_db)):
    try:
        message = db.query(GBMessage).filter(GBMessage.id == data.message_id).first()
        if not message:
            raise HTTPException(status_code=404, detail="Message not found.")

        message.message = data.message
        db.commit()
        return {"status": 200, "message": "Message updated successfully."}
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Error editing message: {str(e)}")



class DeleteGBMessagesModel(BaseModel):
    message_ids: List[int]


@router.delete("/delete_gb_messages")
async def delete_messages(req: DeleteGBMessagesModel, db: Session = Depends(get_db)):
    try:
        messages = db.query(GBMessage).filter(GBMessage.id.in_(req.message_ids)).all()
        
        if not messages:
            raise HTTPException(status_code=404, detail="Messages not found.")

        for message in messages:
            db.delete(message)

        db.commit()
        return {"status": 200, "message": "Messages deleted successfully."}
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Error deleting messages: {str(e)}")


@router.get("/get_fittbot_avatars")
def get_fittbot_avatars(client_id: int, db: Session = Depends(get_db)):

    client = db.query(Client).filter(Client.client_id == client_id).first()

    if not client:
        raise HTTPException(status_code=404, detail="Client not found.")
    avatars = db.query(Avatar).filter(Avatar.gender == client.gender).all()

    avatar_list = [{"id": avatar.id, "avatarurl": avatar.avatarurl} for avatar in avatars]

    return {
        "status": 200,
        "data": avatar_list
    }


class updateAvatarRequest(BaseModel):
    client_id : int
    profile : str
   
@router.put("/update_avatar")
async def update_avatar(req:updateAvatarRequest, db:Session=Depends(get_db),redis: Redis = Depends(get_redis)
 ):
    try:
        client= db.query(Client).filter(Client.client_id == req.client_id).first()
 
        if not client:
            raise HTTPException(status_code=400, detail="Client not found")
       
        client.profile=req.profile
        db.commit()
        client_status_key = f"{client.client_id}:{client.gym_id}:status"
        if await redis.exists(client_status_key, "profile"):
            await redis.delete(client_status_key, "profile")
        if await redis.exists(client_status_key, "status"):
            await redis.delete(client_status_key, "status")
 
        return {
            "status":200,
            "message":"Avatar updated successfully"
        }
    except Exception as e:
        db.rollback()
        print(str(e))
        raise HTTPException(status_code=500, detail=f"Error Updating Avatar: {str(e)}")


@router.get("/consumed_foods")
async def get_consumed_foods(db: Session = Depends(get_db)):

    food_ids = [ 3614, 3615, 3616, 3617, 1000, 2429, 1636, 1272, 1629, 3568, 1729, 1847, 842, 998, 109, 1725, 3618, 1467, 987,763, 504,]
    foods = db.query(Food).filter(Food.id.in_(food_ids)).all()

    food_list = [
        {
            "id": food.id,  
            "name": food.item,  
            "calories": food.calories,  
            "protein": food.protein,  
            "carbs": food.carbs,  
            "fat": food.fat,  
            "fiber": food.fiber,
            "sugar": food.sugar,
            "quantity": food.quantity,
            "calcium":food.calcium,
            "magensium":food.magnesium,
            "potassium":food.potassium,
            "Iron":food.iron,
            "pic":food.pic
        } for food in foods
    ] if foods else [] 

    return {
        "status": 200,
        "data": food_list,
        "message": "Food data fetched successfully"
    }


@router.get("/search_consumed_food")
async def search_consumed_food(
    query: str = Query(..., min_length=2, description="Search query, minimum 3 characters"),
    db: Session = Depends(get_db)
):
    try:
        query = query.strip()  
        offset = 0 
        limit = 25 
 

        startswith_query = db.query(Food).filter(Food.item.ilike(f"{query}%"))
        startswith_count = startswith_query.count() 
 
        if startswith_count == 0:
            foods = (
                db.query(Food)
                .filter(Food.item.ilike(f"%{query}%"))
                .offset(offset)
                .limit(limit)
                .all()
            )
        else:
            foods = startswith_query.offset(offset).limit(limit).all()

        food_list = [
            {
                "id": food.id,
                "name": food.item,
                "calories": food.calories,
                "protein": food.protein,
                "carbs": food.carbs,
                "fat": food.fat,
                "fiber": food.fiber,
                "sugar": food.sugar,
                "quantity": food.quantity,
                "calcium":food.calcium,
                "magensium":food.magnesium,
                "potassium":food.potassium,
                "iron":food.iron,
                "pic":food.pic
            }
            for food in foods
        ]
 
        return {"status": 200, "data": food_list}
 
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")
    



class reportUserRequest(BaseModel):
    user_id:Optional[int] = None
    user_role:str
    post_id:int
    reason:str

@router.post("/report_user")
async def report_user(request:reportUserRequest, db: Session=Depends(get_db)):
    try:
        post = db.query(Post).filter(Post.post_id == request.post_id).first()
 
        if not post:
            raise HTTPException(status_code=400, detail="Post not found")
       
        if not post.client_id:
            gym=db.query(Gym).filter(Gym.gym_id == post.gym_id).first()
            reported_id=gym.owner_id
            reported_role="owner"
       
        else:
            reported_id = post.client_id
            reported_role = "client"
 
        new_report=Report(
            user_id = request.user_id,
            user_role = request.user_role,
            reported_id = reported_id ,
            reported_role = reported_role,
            post_id = request.post_id,
            reason = request.reason,
            post_content= post.content,
            status = False
        )
 
        db.add(new_report)
        db.commit()
        return{
            "status":200,
            "message":"Report submitted successfully"
        }
   
    except Exception as e:
        print(str(e))
        raise HTTPException(status_code= 500, detail=f"Unexpected Error occured: {str(e)}")
    
    
class BlockUserRequest(BaseModel):
    user_id: int
    user_role: str
    post_id:int
 
@router.post("/block_user")
async def block_user(request: BlockUserRequest, db: Session = Depends(get_db),redis: Redis = Depends(get_redis)):
    try:
        
        post = db.query(Post).filter(Post.post_id == request.post_id).first()
        redis_key = f"gym:{post.gym_id}:posts"
        if await redis.exists(redis_key):
            await redis.delete(redis_key)

        async for key in redis.scan_iter("post:*:comment_count"):
            print(key)
            await redis.delete(key)

        if not post:
            raise HTTPException(status_code=400, detail="Post not found")
       
        if not post.client_id:
            print("owner")
            gym=db.query(Gym).filter(Gym.gym_id == post.gym_id).first()
            blocked_id=gym.gym_id
            blocked_role="owner"
       
        else:
            blocked_id = post.client_id
            blocked_role = "client"
 
 
        blocked_entry = db.query(BlockedUsers).filter(
            BlockedUsers.user_id == request.user_id,
            BlockedUsers.user_role == request.user_role
        ).first()
 
        if blocked_entry:

            blocked_data = blocked_entry.blocked_user_id
            print("blocked_data before processing:", blocked_data)
            if isinstance(blocked_data, str):
                try:
                    blocked_data = json.loads(blocked_data)
                except json.JSONDecodeError:
                    print("Error decoding JSON, resetting to empty dict")
                    blocked_data = {}

            if not isinstance(blocked_data, dict):
                print("noo")
                blocked_data = {}

            if "owner" not in blocked_data:
                blocked_data["owner"] = []
            if "client" not in blocked_data:
                blocked_data["client"] = []

            if blocked_id not in blocked_data[blocked_role]:
                print("Adding new blocked_id")
                blocked_data[blocked_role].append(blocked_id)
            else:
                print("blocked_id already exists, skipping addition")

            print("Final blocked_data before commit:", blocked_data)

            blocked_entry.blocked_user_id = json.dumps(blocked_data)

            db.commit()
            db.refresh(blocked_entry)
            return {"status": 200, "message": "Blocked user updated successfully"}


        else:
            if blocked_role=="client":
                opp="owner"
            else:
                opp="client"
            new_record = BlockedUsers(
                user_id=request.user_id,
                user_role=request.user_role,
                blocked_user_id=json.dumps({blocked_role: [blocked_id], opp: []})
            )
            db.add(new_record)
            db.commit()
            db.refresh(new_record)
            return {"status":200,"message": "Blocked user created successfully"}
    
    
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    



@router.get("/show-client-qr")
async def show_client_qr(client_id:int, db:Session=Depends(get_db)):
    try:

        client = db.query(Client).filter(Client.client_id == client_id).one()

        if not client:
            raise HTTPException(status_code=404, detail="User not found")
        
        gym_name = None
        gym_id = None

        if client.gym_id:
            gym = db.query(Gym).filter(Gym.gym_id == client.gym_id).one()
            gym_name= gym.name
            gym_id = gym.gym_id
        
        # encrypted_uuid = encrypt_uuid(str(client.uuid_client))
        encrypted_uuid = str(client.uuid_client)
        row = (
                db.query(
                    GymPlans.plans.label("plan_name"),
                    GymPlans.amount,
                    GymPlans.duration,
                    Client.joined_date
                )
                .join(Client, GymPlans.id == Client.training_id)
                .filter(Client.client_id == client_id)
                .first()
                )

        if not row:
            
            plans={}

        else:

            plan_name, amount, duration, joined_date = row
            days=db.query(GymFees).filter(GymFees.client_id == client_id).first()
            expiration_date = days.end_date
            days_left = (expiration_date - date.today()).days
            expiry = days_left if days_left > 0 else 0

            plans= {
                "plan_name":   plan_name,
                "amount":      amount,
                "duration":    duration,
                "joined_date": joined_date,
                "expiry":expiry

            }

        
        response={
            "goals":client.goals,
            "bmi":client.bmi,
            "height":client.height,
            "age":client.age,
            "lifestyle":client.lifestyle,
            "weight":client.weight,
            "profile":client.profile,
            "name":client.name,
            "client_id":client.client_id,
            "contact":client.contact,
            "email":client.email,
            "medical_issues":client.medical_issues,
            "gender":client.gender,
            "dob":client.dob,
            "uuid":encrypted_uuid,
            "gym_id":gym_id,
            "gym_name":gym_name,
            "plans":plans
        }

        return{
            "status":200,
            "message":"Data retrived successfully",
            "data":response
        }
    
    except Exception as e:
        print(str(e))
        raise HTTPException(status_code=500, detail=f"An error occured : {str(e)}")
    

@router.get("/fetch-preferences")
async def fetch_preferences(client_id:int, db:Session=Depends(get_db)):
    try:
        preference = db.query(Preference).filter(Preference.client_id == client_id).first()

        if not preference:
            return{
                "status":200,
                "preferences":{
                    "notifications": False,
                    "reminders": False,
                    "dataSharing": False,
                    "newsletters": False,
                    "promos": False
                }
            }
        
        else:
            return{
                "status":200,
                "preferences":{
                    "notifications": preference.notifications,
                    "reminders": preference.remainders,
                    "dataSharing": preference.data_sharing,
                    "newsletters": preference.newsletters,
                    "promos": preference.promos_and_offers
                }
            }
    except Exception as e:
        print(str(e))
        raise HTTPException(status_code=500, detail=f"An error occured: {str(e)}")

class PreferenceRequest(BaseModel):
    client_id : int
    preferences : dict

@router.post("/update-preference")
async def update_preferences(request:PreferenceRequest, db:Session=Depends(get_db)):
    try:
        preference = db.query(Preference).filter(Preference.client_id == request.client_id).first()
        data = request.preferences

        if not preference:
            new_record =Preference(
                client_id = request.client_id,
                notifications = data["notifications"],
                remainders =data["reminders"],
                data_sharing = data["dataSharing"],
                newsletters = data["newsletters"],
                promos_and_offers = data["promos"]
            )
            db.add(new_record)
            db.commit()
        
        else:
            preference.notifications = data["notifications"]
            preference.remainders =data["reminders"]
            preference.data_sharing = data["dataSharing"]
            preference.newsletters = data["newsletters"]
            preference.promos_and_offers = data["promos"]

            db.commit()

        return{
            "status":200,
            "message":"Preferences updated successfully"
        }

    except Exception as e:
        print(str(e))
        raise HTTPException(status_code=500, detail=f"An error occured: {str(e)}")

    
class ExpoTokenPayload(BaseModel):
    client_id: int
    expo_token: str
    
@router.post("/update_expo_token")
def update_expo_token(payload: ExpoTokenPayload, db: Session = Depends(get_db)):
    client = db.query(Client).filter(Client.client_id == payload.client_id).first()
    if not client:
        raise HTTPException(status_code=400, detail="Client not found")

    current_tokens = client.expo_token if client.expo_token else []

    if not isinstance(current_tokens, list):
        current_tokens = [current_tokens]


    if payload.expo_token in current_tokens:
        print("already exists")
        return {"status":200,"message": "Expo token already exists"}

    print(payload.expo_token)

    current_tokens.append(payload.expo_token)
    print("current_tokens",current_tokens)
    
    client.expo_token = current_tokens

    db.commit()
    client = db.query(Client).filter(Client.client_id == payload.client_id).first()
    print("client.expo_token",client.expo_token)

    print("addedededed")
    return {"status":200,"message": "Expo token added successfully"}



 
 
@router.get('/get_fittbot_diet_template')
async def get_fittbot_diet_template(db:Session=Depends(get_db), redis:Redis=Depends(get_redis)):
    try:
        redis_key = "fittbotDefaultDietKey"
        cached_data = await redis.get(redis_key)
        if cached_data:
            default_diet= json.loads(cached_data)
        else:
            default = db.query(FittbotDietTemplate).all()
            default_diet=[{
                'id':template.id,
                'template_json':template.template_json,
                'template_name':template.template_name
            }for template in default]
            await redis.set(redis_key, json.dumps(default_diet), ex=86400)
        return{
            'status':200,
            'message':"Data retrived Successfully",
            "data":default_diet
        }
    except Exception as e:
        print(str(e))
        raise HTTPException(status_code=500, detail=f"An error occured : {str(e)}")
 
 

@router.get('/gym_workout_template')
async def get_gym_workout_template(client_id : int , db:Session=Depends(get_db)):
    try:
        schedule = db.query(ClientScheduler).filter(ClientScheduler.client_id == client_id).first()
 
        template= db.query(TemplateWorkout).filter(TemplateWorkout.id == schedule.assigned_workoutplan).first()
        template_data = []
        if template:
            template_data=[{"id": template.id, "name": template.name, "exercise_data": template.workoutPlan}]
 
        print(template_data)
        return{
            'status':200,
            'message':'Data retrived sunccessfully',
            'data':template_data
        }
    except Exception as e:
        print(str(e))
        raise HTTPException(status_code=500, detail=f'An error occured ,{str(e)}')
 
 
@router.get('/smart-watch')
async def get_smart_watch_intrest(client_id : int, db:Session = Depends(get_db)):
    try:
        data = db.query(SmartWatch).filter(SmartWatch.client_id == client_id).first()
        interest = data.interested if data else False
 
        return{
            'status':200,
            'message':'Data retrived successfully',
            'data':interest
        }
   
    except Exception as e:
        print(str(e))
        raise HTTPException(status_code=500, detail=f'An error occured, {str(e)}')
   
class InterestRequest(BaseModel):
    client_id : int
    interest:bool
   
@router.post('/smart-watch')
async def add_interest(request:InterestRequest, db:Session = Depends(get_db)):
    try:
        client_id = request.client_id
        interest = request.interest
        existing =db.query(SmartWatch).filter(SmartWatch.client_id == client_id).first()
        if existing:
            existing.interested = True
        else:
            data = SmartWatch(
                client_id=client_id,
                interested =  interest
            )
            db.add(data)
       
        db.commit()
 
        return{
            'status':200,
            'message':'Thank you for showing the interest'
        }
    except Exception as e:
        print(str(e))
        raise HTTPException(status_code=500, detail=f'An error occured, {str(e)}')
 
 
 
@router.get('/get_single_fittbot_template')
async def get_fittbot_diet_template(client_id:int, cousine:str, goal_type:str,expertise_level:str, db:Session=Depends(get_db)):
    try:
        client = db.query(Client).filter(Client.client_id == client_id).first()

        default = db.query(FittbotDietTemplate).filter(FittbotDietTemplate.gender == client.gender, FittbotDietTemplate.goals == goal_type, FittbotDietTemplate.cousine == cousine, FittbotDietTemplate.expertise_level == expertise_level).first()

        if default:
            default_diet={
                'id':default.id,
                'template_json':default.template_json,
                'template_name':default.template_name,
                'gender':default.gender.lower(),
                'cousine':default.cousine,
                'expertise_level':default.expertise_level,
                'goal':default.goals,
                'tip':default.tip
            }
        else:
            default_diet={}

        return{
            'status':200,
            'message':"Data retrived Successfully",
            "data":default_diet
        }
    except Exception as e:
        print(str(e))
        raise HTTPException(status_code=500, detail=f"An error occured : {str(e)}")
 

@router.get('/get_default_workout')
async def get_default_workout(gender:str, level:str, goals:str, db:Session=Depends(get_db)):
    try:
        workouts = db.query(DefaultWorkoutTemplates).filter(DefaultWorkoutTemplates.gender == gender, DefaultWorkoutTemplates.expertise_level == level,DefaultWorkoutTemplates.goals == goals).first()
        return{
            'status':200,
            'message':"Data retrived Successfully",
            "data":workouts.workout_json if workouts else []
        }
    except Exception as e:
        print(str(e))
        raise HTTPException(status_code=500, detail=f"An error occured : {str(e)}")


       



@router.get("/voice-preference")
async def get_voice_preference(client_id: int, db: Session = Depends(get_db)):
    """Get voice preference for a client"""
    try:
        voice_pref = db.query(VoicePreference).filter(VoicePreference.client_id == client_id).first()

        if not voice_pref:
            # Return default preference (1 = ON)
            return {
                "status": 200,
                "voice_preference": "1"  # Default to ON
            }

        return {
            "status": 200,
            "voice_preference": voice_pref.preference
        }
    except Exception as e:
        print(str(e))
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")


class VoicePreferenceRequest(BaseModel):
    client_id: int
    preference: str  # "1" for ON, "0" for OFF


@router.post("/voice-preference")
async def update_voice_preference(request: VoicePreferenceRequest, db: Session = Depends(get_db)):
    """Update voice preference for a client"""
    try:
        # Validate preference value
        if request.preference not in ["0", "1"]:
            raise HTTPException(status_code=400, detail="Preference must be '0' or '1'")

        voice_pref = db.query(VoicePreference).filter(VoicePreference.client_id == request.client_id).first()

        if not voice_pref:
            # Create new voice preference record
            new_voice_pref = VoicePreference(
                client_id=request.client_id,
                preference=request.preference
            )
            db.add(new_voice_pref)
            db.commit()
        else:
            # Update existing record
            voice_pref.preference = request.preference
            db.commit()

        return {
            "status": 200,
            "message": "Voice preference updated successfully",
            "voice_preference": request.preference
        }
    except Exception as e:
        print(str(e))
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")
