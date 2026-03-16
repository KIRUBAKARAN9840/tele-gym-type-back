# app/api/v1/analytics/client_report.py
 
from datetime import datetime, date as date_type, time as time_type, timedelta
from typing import Any, Dict, Optional, List
from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from app.utils.check_subscriptions import get_client_tier_async

from app.models.async_database import get_async_db

from app.models.fittbot_models import (
    Attendance,
    ClientActual,
    ClientTarget,
    ActualDiet,
    ActualWorkout,
    Client,
    LeaderboardOverall,
    RewardBadge,
    ClientActualAggregated,
)

from app.fittbot_api.v1.client.client_api.home.my_progress import get_workout_data
from app.utils.logging_utils import FittbotHTTPException
 
router = APIRouter(prefix="/client_report", tags=["Client Analytics"])
 
 
def _fmt_time(t: Optional[time_type]) -> Optional[str]:
    """Return 'HH:MM AM/PM' or None."""
    return t.strftime("%I:%M %p") if t else None
 
 
def _minutes_between(d: date_type, start: Optional[time_type], end: Optional[time_type]) -> int:
    """Safe minutes between two times on the same date (0 if invalid or missing)."""
    if not start or not end:
        return 0
    try:
        delta = datetime.combine(d, end) - datetime.combine(d, start)
        mins = int(delta.total_seconds() // 60)
        return mins if mins > 0 else 0
    except Exception:
        return 0
 
 
def _duration_label(total_minutes: int) -> Optional[str]:
    if total_minutes <= 0:
        return None
    if total_minutes < 60:
        return f"{total_minutes} mins"
    hrs, mins = divmod(total_minutes, 60)
    return f"{hrs}h {mins}mins" if mins else f"{hrs}h"
 
 
@router.get("/get")
async def get_client_data(
    client_id: int,
    date: str = None,
    start_date: str = None,
    end_date: str = None,
    db: AsyncSession = Depends(get_async_db)
):
    try:

        tier = await get_client_tier_async(db, client_id)
        
        if not date and not (start_date and end_date):
            raise FittbotHTTPException(
                status_code=400,
                detail="Either 'date' or both 'start_date' and 'end_date' must be provided",
                error_code="INVALID_PARAMETERS",
                log_data={"client_id": client_id, "date": date, "start_date": start_date, "end_date": end_date},
            )
 
        if date:
            
            try:
                input_date = datetime.strptime(date, "%Y-%m-%d").date()
            except ValueError:
                raise FittbotHTTPException(
                    status_code=400,
                    detail="Invalid date format. Use 'YYYY-MM-DD'.",
                    error_code="INVALID_DATE_FORMAT",
                    log_data={"client_id": client_id, "date": date},
                )
 
            response = {}
 
            attendance_result = await db.execute(
                select(Attendance).where(
                    Attendance.client_id == client_id,
                    Attendance.date == input_date
                )
            )
            attendance = attendance_result.scalars().first()
 
            attendance_dict = {
                "in_time": None,
                "out_time": None,
                "in_time_2": None,
                "out_time_2": None,
                "in_time_3": None,
                "out_time_3": None,
            }
            total_minutes = 0
 
            if attendance:
                for suf in ["", "_2", "_3"]:
                    in_attr = getattr(attendance, f"in_time{suf}")
                    out_attr = getattr(attendance, f"out_time{suf}")
                    in_key = f"in_time{suf}" if suf else "in_time"
                    out_key = f"out_time{suf}" if suf else "out_time"
 
                    attendance_dict[in_key] = (
                        in_attr.strftime("%I:%M %p") if in_attr else None
                    )
                    attendance_dict[out_key] = (
                        out_attr.strftime("%I:%M %p") if out_attr else None
                    )
 
                    if in_attr and out_attr:
                        diff = datetime.combine(input_date, out_attr) - datetime.combine(
                            input_date, in_attr
                        )
                        total_minutes += diff.total_seconds() // 60
 
            if total_minutes:
                if total_minutes < 60:
                    time_spent = f"{int(total_minutes)} mins"
                else:
                    hrs, mins = divmod(int(total_minutes), 60)
                    time_spent = f"{hrs}h {mins}mins"
            else:
                time_spent = None
 
            response["attendance"] = attendance_dict
            response["time_spent"] = time_spent
 
            client_actual_result = await db.execute(
                select(ClientActual).where(
                    ClientActual.client_id == client_id,
                    ClientActual.date == input_date
                )
            )
            client_actual = client_actual_result.scalars().first()

            client_target_result = await db.execute(
                select(ClientTarget).where(ClientTarget.client_id == client_id)
            )
            client_target = client_target_result.scalars().first()

            if client_actual:
 
                response["client_actual"] = {
                    "calories": {
                        "actual": client_actual.calories,
                        "target": client_target.calories if client_target else 0,
                    },
                    "protein": {
                        "actual": client_actual.protein,
                        "target": client_target.protein if client_target else 0,
                    },
                    "fat": {"actual": client_actual.fats, "target": client_target.fat if client_target else 0},
                    "carbs": {
                        "actual": client_actual.carbs,
                        "target": client_target.carbs if client_target else 0,
                    },
                    "fiber": {
                        "actual": client_actual.fiber,
                        "target": client_target.fiber if client_target and client_target.fiber is not None else None,
                    },
                    "sugar": {
                        "actual": client_actual.sugar,
                        "target": client_target.sugar if client_target and client_target.sugar is not None else None,
                    },
                    "calcium": {
                        "actual": client_actual.calcium,
                        "target": client_target.calcium if client_target and client_target.calcium is not None else None,
                    },
                    "magnesium": {
                        "actual": client_actual.magnesium,
                        "target": client_target.magnesium if client_target and client_target.magnesium is not None else None,
                    },
                    "potassium": {
                        "actual": client_actual.potassium,
                        "target": client_target.potassium if client_target and client_target.potassium is not None else None,
                    },
                    "Iodine": {
                        "actual": client_actual.Iodine,
                        "target": client_target.Iodine if client_target and client_target.Iodine is not None else None,
                    },
                    "Iron": {
                        "actual": client_actual.Iron,
                        "target": client_target.Iron if client_target and client_target.Iron is not None else None,
                    },
                }
            else:
                if client_target:
                    response["client_actual"] = {
                        "calories": {"actual": None, "target": client_target.calories},
                        "protein": {"actual": None, "target": client_target.protein},
                        "fat": {"actual": None, "target": client_target.fat},
                        "carbs": {"actual": None, "target": client_target.carbs},
                        "fiber": {"actual": None, "target": client_target.fiber if client_target.fiber is not None else None},
                        "sugar": {"actual": None, "target": client_target.sugar if client_target.sugar is not None else None},
                        "calcium": {"actual": None, "target": client_target.calcium if client_target.calcium is not None else None},
                        "magnesium": {"actual": None, "target": client_target.magnesium if client_target.magnesium is not None else None},
                        "potassium": {"actual": None, "target": client_target.potassium if client_target.potassium is not None else None},
                        "Iodine": {"actual": None, "target": client_target.Iodine if client_target.Iodine is not None else None},
                        "Iron": {"actual": None, "target": client_target.Iron if client_target.Iron is not None else None},
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
 
            diet_data_result = await db.execute(
                select(ActualDiet).where(
                    ActualDiet.client_id == client_id,
                    ActualDiet.date == input_date
                )
            )
            diet_data = diet_data_result.scalars().first()
 
            if diet_data:
                response["diet_data"] = diet_data.diet_data
            else:
                response["diet_data"] = None
 
            workout_data_result = await db.execute(
                select(ActualWorkout).where(
                    ActualWorkout.client_id == client_id,
                    ActualWorkout.date == input_date
                )
            )
            workout_data = workout_data_result.scalars().first()
 
            if workout_data:
                data = workout_data.workout_details
                exercise_names = set()
                for record in data:
                    for category in record.values():
                        for exercise in category:
                            exercise_names.add(exercise["name"])
 
                unique_exercise_count = len(exercise_names)
 
                response["workout"] = {
                    "workout_details": workout_data.workout_details if workout_data else None,
                    "muscle_group": attendance.muscle if attendance else [],
                    "count": unique_exercise_count,
                    "duration": time_spent or 0,
                }
            else:
                response["workout"] = None

            response["workout_data"] = await get_workout_data(db, client_id, input_date)

            target = client_target.water_intake if client_target else None
            actual = client_actual.water_intake if client_actual else 0
            water_intake = {"target": target, "actual": actual}
            response["water_intake"] = water_intake
 
            client_result = await db.execute(
                select(Client).where(Client.client_id == client_id)
            )
            client = client_result.scalars().first()
            if client:
                leaderboard_entry_result = await db.execute(
                    select(LeaderboardOverall).where(
                        LeaderboardOverall.client_id == client_id
                    )
                )
                leaderboard_entry = leaderboard_entry_result.scalars().first()
 
                if leaderboard_entry:


                    if tier=="freemium_gym" or tier=="premium_gym":

                        from sqlalchemy import func
                        total_participants_result = await db.execute(
                            select(func.count(LeaderboardOverall.id)).where(
                                LeaderboardOverall.gym_id == leaderboard_entry.gym_id
                            )
                        )
                        total_participants = total_participants_result.scalar()

                        total_participants="NA"
 
                    badge_result = await db.execute(
                        select(RewardBadge).where(
                            RewardBadge.min_points <= leaderboard_entry.xp,
                            RewardBadge.max_points >= leaderboard_entry.xp
                        )
                    )
                    badge = badge_result.scalars().first()
 
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
                        # "position": position,
                        "total_participants": total_participants,
                        "xp": leaderboard_entry.xp,
                        "badge": badge_details
                    }
                else:
                    response["leaderboard"] = {
                        "position": "NA",
                        "total_participants": "NA",
                        "xp": 0,
                        "badge": {
                            "badge_name": "Beginner",
                            "level": "Silver",
                            "image_url": "https://fittbot-uploads.s3.ap-south-2.amazonaws.com/New_badges/Beginner.png",
                            "min_points": 0,
                            "max_points": 500
                        }
                    }
            else:
                response["leaderboard"] = {
                    "position": None,
                    "total_participants": 0,
                    "xp": 0,
                    "badge": None
                }


            response["bmi"] = client.bmi if client and client.bmi else None
 
            # Add diet streak functionality
            current_year = input_date.year
            client_aggregated_result = await db.execute(
                select(ClientActualAggregated).where(
                    ClientActualAggregated.client_id == client_id,
                    ClientActualAggregated.year == current_year
                )
            )
            client_aggregated = client_aggregated_result.scalars().first()
 
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

            # Add total gym time (in hours and minutes)
            if client_aggregated and client_aggregated.gym_time:
                total_minutes = int(client_aggregated.gym_time)
                response["total_gym_time"] = {"hour": total_minutes // 60, "minutes": total_minutes % 60}
            else:
                response["total_gym_time"] = {"hour": 0, "minutes": 0}


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
 
        else:
            # Date range query (new functionality)
            try:
                start_date_obj = datetime.strptime(start_date, "%Y-%m-%d").date()
                end_date_obj = datetime.strptime(end_date, "%Y-%m-%d").date()
            except ValueError:
                raise FittbotHTTPException(
                    status_code=400,
                    detail="Invalid date format. Use 'YYYY-MM-DD'.",
                    error_code="INVALID_DATE_FORMAT",
                    log_data={"client_id": client_id, "start_date": start_date, "end_date": end_date},
                )

            # Get client target
            client_target_result = await db.execute(
                select(ClientTarget).where(ClientTarget.client_id == client_id)
            )
            client_target = client_target_result.scalars().first()

            # Try to get client actual records first
            client_actual_records_result = await db.execute(
                select(ClientActual).where(
                    ClientActual.client_id == client_id,
                    ClientActual.date >= start_date_obj,
                    ClientActual.date <= end_date_obj
                )
            )
            client_actual_records = client_actual_records_result.scalars().all()

            # Calculate average macronutrients across the date range
            total_macros = {
                "calories": 0,
                "protein": 0,
                "carbs": 0,
                "fat": 0,
                "fiber": 0,
                "sugar": 0
            }
            day_count = 0
 
            # If we have ClientActual records, use them
            if client_actual_records:
                for record in client_actual_records:
                    if record:  # Check if record is not None
                        total_macros["calories"] += getattr(record, 'calories', 0) or 0
                        total_macros["protein"] += getattr(record, 'protein', 0) or 0
                        total_macros["carbs"] += getattr(record, 'carbs', 0) or 0
                        total_macros["fat"] += getattr(record, 'fats', 0) or 0
                        total_macros["fiber"] += getattr(record, 'fiber', 0) or 0
                        total_macros["sugar"] += getattr(record, 'sugar', 0) or 0
                        day_count += 1  # Only count valid records
            else:
                # Fallback: Calculate from ActualDiet records if ClientActual records are missing
                print(f"No ClientActual records found, falling back to ActualDiet calculation for client {client_id}")

                diet_records_result = await db.execute(
                    select(ActualDiet).where(
                        ActualDiet.client_id == client_id,
                        ActualDiet.date >= start_date_obj,
                        ActualDiet.date <= end_date_obj
                    )
                )
                diet_records = diet_records_result.scalars().all()
 
                current_date = start_date_obj
                while current_date <= end_date_obj:
                    # Find diet record for this date
                    date_record = next((r for r in diet_records if r.date == current_date), None)
 
                    if date_record and date_record.diet_data:
                        print(f"Processing diet data for {current_date}")
                        diet_data = date_record.diet_data
                        daily_macros = {
                            "calories": 0,
                            "protein": 0,
                            "carbs": 0,
                            "fat": 0,
                            "fiber": 0,
                            "sugar": 0
                        }
 
                        # Calculate macros from diet data - supports both old and new formats
                        for meal in diet_data:
                            if isinstance(meal, dict) and "foodList" in meal:
                                # New structure: meal categories with foodList arrays
                                food_list = meal.get("foodList", [])
                                if food_list:  # Only process if there are actual food items
                                    print(f"Processing meal: {meal.get('title')} with {len(food_list)} food items")
                                    for food_item in food_list:
                                        if isinstance(food_item, dict):
                                            daily_macros["calories"] += food_item.get("calories", 0) or 0
                                            daily_macros["protein"] += food_item.get("protein", 0) or 0
                                            daily_macros["carbs"] += food_item.get("carbs", 0) or 0
                                            daily_macros["fat"] += food_item.get("fat", 0) or 0
                                            daily_macros["fiber"] += food_item.get("fiber", 0) or 0
                                            daily_macros["sugar"] += food_item.get("sugar", 0) or 0
                            else:
                                # Old structure: direct food items
                                if isinstance(meal, dict):
                                    print(f"Processing legacy food item")
                                    daily_macros["calories"] += meal.get("calories", 0) or 0
                                    daily_macros["protein"] += meal.get("protein", 0) or 0
                                    daily_macros["carbs"] += meal.get("carbs", 0) or 0
                                    daily_macros["fat"] += meal.get("fat", 0) or 0
                                    daily_macros["fiber"] += meal.get("fiber", 0) or 0
                                    daily_macros["sugar"] += meal.get("sugar", 0) or 0
 
                        # Only count days that have actual food data
                        if any(daily_macros[key] > 0 for key in daily_macros):
                            print(f"Daily macros for {current_date}: {daily_macros}")
                            # Add to total
                            for key in total_macros:
                                total_macros[key] += daily_macros[key]
                            day_count += 1
 
                    current_date = current_date + timedelta(days=1)
 
                print(f"Total macros after processing: {total_macros}, days with data: {day_count}")
 
            # Calculate averages
            if day_count > 0:
                avg_macros = {
                    "calories": {"actual": round(total_macros["calories"] / day_count), "target": client_target.calories if client_target else 0},
                    "protein": {"actual": round(total_macros["protein"] / day_count), "target": client_target.protein if client_target else 0},
                    "carbs": {"actual": round(total_macros["carbs"] / day_count), "target": client_target.carbs if client_target else 0},
                    "fat": {"actual": round(total_macros["fat"] / day_count), "target": client_target.fat if client_target else 0},
                    "fiber": {"actual": round(total_macros["fiber"] / day_count), "target": client_target.fiber if client_target else 0},
                    "sugar": {"actual": round(total_macros["sugar"] / day_count), "target": client_target.sugar if client_target else 0},
                }
            else:
                avg_macros = {
                    "calories": {"actual": 0, "target": client_target.calories if client_target else 0},
                    "protein": {"actual": 0, "target": client_target.protein if client_target else 0},
                    "carbs": {"actual": 0, "target": client_target.carbs if client_target else 0},
                    "fat": {"actual": 0, "target": client_target.fat if client_target else 0},
                    "fiber": {"actual": 0, "target": client_target.fiber if client_target else 0},
                    "sugar": {"actual": 0, "target": client_target.sugar if client_target else 0},
                }
 
            return {
                "status": 200,
                "message": "Data fetched successfully",
                "data": {
                    "client_actual": avg_macros,
                    "date_range": {
                        "start_date": start_date,
                        "end_date": end_date,
                        "total_days": (end_date_obj - start_date_obj).days + 1,
                        "days_with_data": day_count
                    }
                }
            }
 
    except FittbotHTTPException:
        raise
    except Exception as e:
        print("error", e)
        raise FittbotHTTPException(
            status_code=500,
            detail=f"An error occurred: {str(e)}",
            error_code="GET_CLIENT_REPORT_ERROR",
            log_data={"client_id": client_id, "date": date, "start_date": start_date, "end_date": end_date, "error": str(e)},
        )
 
 
 
