# app/api/v1/workouts/actual_workout.py

from typing import List,Optional
from app.utils.redis_config import get_redis
from datetime import datetime, date
from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy import asc
from sqlalchemy.orm import Session
from redis.asyncio import Redis

from app.models.database import get_db
from app.models.fittbot_models import (
    ActualWorkout,
    ClientActual,
    CalorieEvent,
    LeaderboardDaily,
    LeaderboardMonthly,
    LeaderboardOverall,
    ClientNextXp,
    RewardGym,
    RewardPrizeHistory,
    Client,
)
from app.utils.logging_utils import FittbotHTTPException
from app.fittbot_api.v1.client.client_api.side_bar.ratings import check_feedback_status

router = APIRouter(prefix="/actual_workout", tags=["workout"])

async def delete_keys_by_pattern(redis: Redis, pattern: str) -> None:
    keys = await redis.keys(pattern)
    if keys:
        await redis.delete(*keys) 
        
# -------------------- Schemas (unchanged logic) --------------------
class WorkoutInput(BaseModel):
    client_id: int
    date: date
    workout_details: list
    gym_id: Optional[int]=None
    live_status: bool


class WorkoutEditInput(BaseModel):
    client_id: int
    gym_id: Optional[int]=None
    record_id: int
    workout_details: list


# -------------------- Endpoints --------------------
@router.get("/get")
async def get_actual_workout(
    client_id: int,
    date: date,
    db: Session = Depends(get_db),
):
    try:
        record = (
            db.query(ActualWorkout)
            .filter(ActualWorkout.client_id == client_id, ActualWorkout.date == date)
            .first()
        )
        print("record",record)
        print("date",date)
        if not record:
            return {"status": 200, "data": []}
        return {"status": 200, "data": record}

    except FittbotHTTPException:
        raise
    except Exception as e:
        raise FittbotHTTPException(
            status_code=500,
            detail=f"An unexpected error occurred {e}",
            error_code="ACTUAL_WORKOUT_FETCH_ERROR",
            log_data={"client_id": client_id, "date": str(date), "error": str(e)},
        )


@router.post("/add")
async def create_or_append_workout(
    data: WorkoutInput,
    db: Session = Depends(get_db),
    redis: Redis = Depends(get_redis),
):
    try:
        print(f"DEBUG: Processing workout data: {data.workout_details}")
        total_burnt_calories = 0
        if data.workout_details:
            for muscle_group in data.workout_details:
                for exercises in muscle_group.values():
                    for exercise in exercises:
                        for set_detail in exercise.get("sets", []):
                            calories = set_detail.get("calories", 0)
                            print(f"DEBUG: Set calories: {calories} ({type(calories)})")
                            # Ensure calories is numeric to prevent type errors
                            try:
                                numeric_calories = float(calories) if calories is not None else 0
                                total_burnt_calories += numeric_calories
                            except (ValueError, TypeError) as e:
                                print(f"ERROR converting calories {calories} to float: {e}")
                                # Skip this set's calories if conversion fails
                                continue

        print(f"DEBUG: Total burnt calories calculated: {total_burnt_calories} ({type(total_burnt_calories)})")

        record = (
            db.query(ActualWorkout)
            .filter(ActualWorkout.client_id == data.client_id, ActualWorkout.date == data.date)
            .first()
        )

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
            
        else:
            record = ActualWorkout(
                client_id=data.client_id,
                date=data.date,
                workout_details=data.workout_details,
            )
            db.add(record)
            db.commit()
        

        client_actual = (
            db.query(ClientActual)
            .filter(ClientActual.client_id == data.client_id, ClientActual.date == data.date)
            .first()
        )

        if client_actual:
            # Convert existing burnt_calories to float to prevent string concatenation errors
            current_burnt_calories = float(client_actual.burnt_calories) if client_actual.burnt_calories is not None else 0
            print(f"DEBUG: Current burnt_calories: {current_burnt_calories} ({type(current_burnt_calories)})")
            print(f"DEBUG: Adding burnt_calories: {total_burnt_calories} ({type(total_burnt_calories)})")
            client_actual.burnt_calories = current_burnt_calories + total_burnt_calories
        else:
            client_actual = ClientActual(
                client_id=data.client_id,
                date=data.date,
                burnt_calories=total_burnt_calories,
            )
            db.add(client_actual)

        await delete_keys_by_pattern(redis, f"{data.client_id}:*:target_actual")
        

        await delete_keys_by_pattern(redis, f"{data.client_id}:*:chart")


        db.commit()
        

        if data.live_status:
            print("live status is", data.live_status)
            total_sets = 0
            for workout in data.workout_details:
                for muscle_group, exercises in workout.items():
                    for exercise in exercises:
                        sets = exercise.get("sets", [])
                        total_sets += len(sets)

            print("hiii")
            calculated_credits = total_sets * 3
            credits = calculated_credits if calculated_credits <= 50 else 50
            today = date.today()
            calorie_event = (
                db.query(CalorieEvent)
                .filter(
                    CalorieEvent.client_id == data.client_id,
                    CalorieEvent.event_date == today,
                )
                .first()
            )

            if not calorie_event:
                calorie_event = CalorieEvent(
                    client_id=data.client_id,
                    event_date=today,
                    workout_added=0,
                )
                db.add(calorie_event)
                db.commit()
                calorie_event = calorie_event

            if True:
                if not calorie_event.workout_added:
                    calorie_event.workout_added = 0
                target_sets = calorie_event.workout_added
                if target_sets < 50:
                    if target_sets + credits > 50:
                        credits = 50 - target_sets
                    today = date.today()
                    daily_record = (
                        db.query(LeaderboardDaily)
                        .filter(
                            LeaderboardDaily.client_id == data.client_id,
                            LeaderboardDaily.date == today,
                        )
                        .first()
                    )

                    if daily_record:
                        daily_record.xp += credits
                    else:
                        new_daily = LeaderboardDaily(
                            client_id=data.client_id,
                            xp=credits,
                            date=today,
                        )
                        db.add(new_daily)

                    month_date = today.replace(day=1)
                    monthly_record = (
                        db.query(LeaderboardMonthly)
                        .filter(
                            LeaderboardMonthly.client_id == data.client_id,
                            LeaderboardMonthly.month == month_date,
                        )
                        .first()
                    )

                    if monthly_record:
                        monthly_record.xp += credits
                    else:
                        new_monthly = LeaderboardMonthly(
                            client_id=data.client_id,
                            
                            xp=credits,
                            month=month_date,
                        )
                        db.add(new_monthly)

                    overall_record = (
                        db.query(LeaderboardOverall)
                        .filter(
                            LeaderboardOverall.client_id == data.client_id,
                        )
                        .first()
                    )

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
                                .filter(RewardGym.xp > xp)
                                .order_by(asc(RewardGym.xp))
                                .first()
                            )

                        gym_id = data.gym_id
                        print("gym id is",gym_id)
                        if gym_id is not None:
                            if next_row:
                               
                                if new_total >= next_row.next_xp and next_row.next_xp != 0:
                                    client = (
                                        db.query(Client)
                                        .filter(Client.client_id == data.client_id)
                                        .first()
                                    )
                                    db.add(
                                        RewardPrizeHistory(
                                            client_id=data.client_id,
                                            gym_id=gym_id,
                                            xp=next_row.next_xp,
                                            gift=next_row.gift,
                                            achieved_date=datetime.now(),
                                            client_name=client.name,
                                            is_given=False,
                                            profile=client.profile,
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
                                            client_id=data.client_id,
                                            next_xp=first_tier.xp,
                                            gift=first_tier.gift,
                                        )
                                    )

                        db.commit()
                        
                    else:
                        new_overall = LeaderboardOverall(
                            client_id=data.client_id, xp=credits
                        )
                        db.add(new_overall)
                        db.commit()
                      

                    existing_event = (
                        db.query(CalorieEvent)
                        .filter(
                            CalorieEvent.client_id == data.client_id,
                            CalorieEvent.event_date == data.date,
                        )
                        .first()
                    )

                    if existing_event:
                        existing_event.workout_added += credits
                    else:
                        new_event = CalorieEvent(
                            client_id=data.client_id,
                            event_date=data.date,
                            workout_added=credits,
                        )
                        db.add(new_event)

                    db.commit()
                else:
                    credits = 0
        else:
            credits = 0

        # Check feedback status
        show_feedback = check_feedback_status(db, data.client_id)

        return {
            "status": 200,
            "message": "Workout data appended and updated",
            "record_id": record.record_id,
            "workout_details": record.workout_details,
            "total_burnt_calories": total_burnt_calories,
            "reward_point": credits,
            "feedback": show_feedback,
        }

    except FittbotHTTPException:
        # do not change logic; just bubble structured errors
        raise
    except Exception as e:
        try:
            db.rollback()
        except Exception:
            pass
        print(e)
        raise FittbotHTTPException(
            status_code=500,
            detail=f"An unexpected error occurred {e}",
            error_code="ACTUAL_WORKOUT_CREATE_APPEND_ERROR",
            log_data={
                "client_id": data.client_id,
                "date": str(data.date),
                "error": str(e),
            },
        )


@router.put("/edit")
async def edit_workout(
    data: WorkoutEditInput,
    db: Session = Depends(get_db),
    redis: Redis = Depends(get_redis),
):
    try:
        record = db.query(ActualWorkout).filter(ActualWorkout.record_id == data.record_id).first()

        if not record:
            raise FittbotHTTPException(
                status_code=404,
                detail="Workout record not found",
                error_code="ACTUAL_WORKOUT_NOT_FOUND",
                log_data={"record_id": data.record_id},
            )

        previous_total_burnt_calories = 0
        if record.workout_details:
            for muscle_group in record.workout_details:
                for exercises in muscle_group.values():
                    for exercise in exercises:
                        for set_detail in exercise.get("sets", []):
                            previous_total_burnt_calories += set_detail.get("calories", 0)

        if data.workout_details == []:
            db.delete(record)
            db.commit()

            client_actual = (
                db.query(ClientActual)
                .filter(ClientActual.client_id == record.client_id, ClientActual.date == record.date)
                .first()
            )

            if client_actual:
                # Convert existing burnt_calories to float to prevent string concatenation errors
                current_burnt_calories = float(client_actual.burnt_calories) if client_actual.burnt_calories is not None else 0
                client_actual.burnt_calories = max(current_burnt_calories - previous_total_burnt_calories, 0)
                db.commit()
              

            return {
                "status": 200,
                "message": "Workout record deleted as workout_details is empty",
            }

        new_total_burnt_calories = 0
        if data.workout_details:
            for muscle_group in data.workout_details:
                for exercises in muscle_group.values():
                    for exercise in exercises:
                        for set_detail in exercise.get("sets", []):
                            new_total_burnt_calories += set_detail.get("calories", 0)

        record.workout_details = data.workout_details
        db.commit()
        

        client_actual = (
            db.query(ClientActual)
            .filter(ClientActual.client_id == record.client_id, ClientActual.date == record.date)
            .first()
        )

        if client_actual:
            # Convert existing burnt_calories to float to prevent string concatenation errors
            current_burnt_calories = float(client_actual.burnt_calories) if client_actual.burnt_calories is not None else 0
            client_actual.burnt_calories = max(current_burnt_calories - previous_total_burnt_calories + new_total_burnt_calories, 0)
            db.commit()

        await delete_keys_by_pattern(redis, f"{data.client_id}:*:target_actual")

        return {
            "status": 200,
            "message": "Workout data replaced",
            "record_id": record.record_id,
            "workout_details": record.workout_details,
        }

    except FittbotHTTPException:
        raise
    except Exception as e:
        try:
            db.rollback()
        except Exception:
            pass
        raise FittbotHTTPException(
            status_code=500,
            detail=f"An unexpected error occurred {e}",
            error_code="ACTUAL_WORKOUT_EDIT_ERROR",
            log_data={
                "record_id": data.record_id,
                "client_id": data.client_id,
                "error": str(e),
            },
        )


@router.delete("/delete")
async def delete_actual_workout(
    record_id: int,
    client_id: int,
    gym_id: Optional[int]=None,
    db: Session = Depends(get_db),
    redis: Redis = Depends(get_redis),
):
    try:
        workout = db.query(ActualWorkout).filter(ActualWorkout.record_id == record_id).first()
        if not workout:
            raise FittbotHTTPException(
                status_code=404,
                detail="Workout record not found",
                error_code="ACTUAL_WORKOUT_NOT_FOUND",
                log_data={"record_id": record_id},
            )

        client_actual = (
            db.query(ClientActual)
            .filter(ClientActual.client_id == client_id, ClientActual.date == datetime.now().date())
            .first()
        )

        if client_actual:
            client_actual.burnt_calories = 0
            db.commit()

        db.delete(workout)
        db.commit()

        await delete_keys_by_pattern(redis, f"{client_id}:*:target_actual")

        return {"status": 200, "message": "Workout record deleted successfully"}

    except FittbotHTTPException:
        raise
    except Exception as e:
        try:
            db.rollback()
        except Exception:
            pass
        raise FittbotHTTPException(
            status_code=500,
            detail=f"An unexpected error occurred {e}",
            error_code="ACTUAL_WORKOUT_DELETE_ERROR",
            log_data={"record_id": record_id, "client_id": client_id, "error": str(e)},
        )
