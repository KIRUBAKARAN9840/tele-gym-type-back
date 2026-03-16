
from __future__ import annotations
from database import SessionLocal
from models import ClientBirthday,Gym, Client,DailyGymHourlyAgg, GymHourlyAgg,ActualWorkout,AggregatedInsights,GymAnalysis,ClientTarget,ClientActual,ClientGeneralAnalysis,ClientActualAggregatedWeekly,MuscleAggregatedInsights,Attendance,ClientActualAggregated,ClientWeeklyPerformance
from datetime import datetime, timedelta,date,time
from typing import Dict,List,Any
from redis_config import get_redis
import asyncio
from sqlalchemy import text,func
import json
from datetime import date, datetime, timedelta
from typing import Optional
from sqlalchemy.orm import Session
from sqlalchemy import desc, and_
from models import (             
    Client, GymFees, Gym, ClientGym,
    FeesReceipt, AccountDetails, AboutToExpire
)


T_MET_PCT  = 0.05      
T_GOAL_PCT = 0.15      
FLOOR_MET  = 100       
FLOOR_GOAL = 500       


def _bands(target: int) -> tuple[int, int]:
    tol  = max(FLOOR_MET,  round(T_MET_PCT  * target))   
    band = max(FLOOR_GOAL, round(T_GOAL_PCT * target))  
    return tol, band

def _bucket(goal: str, delta: int, tol: int, band: int) -> str:

    if abs(delta) <= tol:
        return True


def _streak_ok(goal: str, bucket: str) -> bool:
    if bucket=="met":
        return True
    return ((goal == "weight_loss" and bucket == "deficit") or
            (goal == "weight_gain" and bucket == "surplus"))



def get_workout_combined_summary():
    from datetime import date, timedelta, datetime
    from typing import Dict, List, Any
    import traceback

    def _dbg_bin(op: str,
                 left,
                 right,
                 label: str,
                 cid: int,
                 cname: str):
        """Safe arithmetic print-and-continue."""
        print(f"DBG  [{label}]  client {cid} ({cname})  |  L={left!r}  R={right!r}")
        try:
            if op == "add":
                res = (left or 0) + (right or 0)
            else:  # "avg"
                if left is None and right is None:
                    res = 0
                elif left is None:
                    res = right
                elif right is None:
                    res = left
                else:
                    res = (left + right) / 2
            print(f"DBG  [{label}]  → {res!r}")
            return res
        except Exception:               # ultra-defensive – should never hit
            print("🚨  ", label, "| client", cid, cname)
            traceback.print_exc()
            return left if left is not None else (right or 0)

    # ──────────────────────────────────────────────────────────────────────
    try:
        db = SessionLocal()
        clients = db.query(Client).all()

        for client in clients:
            client_id   = client.client_id
            client_name = client.name
            print("\n────", client_name, "(", client_id, ") ────")

            yesterday     = date.today() - timedelta(days=1)
            week_start    = yesterday - timedelta(days=yesterday.weekday())
            today_weekday = date.today().weekday()

            records = db.query(ActualWorkout).filter(
                ActualWorkout.client_id == client_id,
                ActualWorkout.date == yesterday
            ).all()

            attendance_record = db.query(Attendance).filter(
                Attendance.client_id == client_id,
                Attendance.date == yesterday
            ).first()

            # ======================= WORKOUT BLOCK ==========================
            if records:
                overall_volume = overall_reps = overall_weight = 0
                overall_sets = overall_time = 0
                muscle_stats: Dict[str, Dict[str, float]] = {}

                for record in records:
                    details: List[Dict[str, Any]] = record.workout_details or []
                    for workout in details:
                        for muscle_group, exercises in workout.items():
                            muscle_stats.setdefault(
                                muscle_group,
                                {
                                    "total_volume": 0,
                                    "total_reps": 0,
                                    "total_weight": 0,
                                    "total_sets": 0,
                                    "duration": 0,
                                },
                            )
                            for exercise in exercises:
                                for set_item in exercise.get("sets", []):
                                    reps     = set_item.get("reps", 0)
                                    weight   = set_item.get("weight", 0)
                                    duration = set_item.get("duration", 0)

                                    overall_volume += reps * weight
                                    overall_reps   += reps
                                    overall_weight += weight
                                    overall_sets   += 1
                                    overall_time   += duration

                                    st = muscle_stats[muscle_group]
                                    st["total_volume"] += reps * weight
                                    st["total_reps"]   += reps
                                    st["total_weight"] += weight
                                    st["total_sets"]   += 1
                                    st["duration"]     += duration

                for st in muscle_stats.values():
                    ts = st["total_sets"] or 1
                    st["avg_reps"]   = st["total_reps"]   / ts
                    st["avg_weight"] = st["total_weight"] / ts

                workout_time_minutes = overall_time / 60

                if attendance_record and attendance_record.in_time and attendance_record.out_time:
                    dt_in  = datetime.combine(yesterday, attendance_record.in_time)
                    dt_out = datetime.combine(yesterday, attendance_record.out_time)
                    attend_minutes = round((dt_out - dt_in).total_seconds() / 60)
                    rest_time_minutes = max(0, round(attend_minutes - workout_time_minutes))
                else:
                    rest_time_minutes = 0

                aggregated_entry = db.query(ClientActualAggregated).filter(
                    ClientActualAggregated.client_id == client_id
                ).first()

                if aggregated_entry:
                    aggregated_entry.workout_time = _dbg_bin(
                        "avg",
                        aggregated_entry.workout_time,
                        workout_time_minutes,
                        "workout_time",
                        client_id,
                        client_name,
                    )
                    aggregated_entry.rest_time = _dbg_bin(
                        "avg",
                        aggregated_entry.rest_time,
                        rest_time_minutes,
                        "rest_time",
                        client_id,
                        client_name,
                    )
                    db.commit()
                else:
                    db.add(
                        ClientActualAggregated(
                            client_id=client_id,
                            workout_time=workout_time_minutes,
                            rest_time=rest_time_minutes,
                        )
                    )
                    db.commit()

                # ─ per-muscle aggregates ─────────────────────────────────
                for mgrp, st in muscle_stats.items():
                    cur_vol, cur_wt, cur_reps = st["total_volume"], st["avg_weight"], st["avg_reps"]

                    mi = db.query(MuscleAggregatedInsights).filter(
                        MuscleAggregatedInsights.client_id == client_id,
                        MuscleAggregatedInsights.muscle_group == mgrp,
                    ).first()

                    if mi:
                        mi.total_volume = _dbg_bin("avg", mi.total_volume, cur_vol,
                                                   "total_volume", client_id, client_name)
                        mi.avg_weight   = _dbg_bin("avg", mi.avg_weight,   cur_wt,
                                                   "avg_weight",   client_id, client_name)
                        mi.avg_reps     = _dbg_bin("avg", mi.avg_reps,     cur_reps,
                                                   "avg_reps",     client_id, client_name)
                    else:
                        db.add(
                            MuscleAggregatedInsights(
                                client_id=client_id,
                                muscle_group=mgrp,
                                total_volume=cur_vol,
                                avg_weight=cur_wt,
                                avg_reps=cur_reps,
                            )
                        )
                    db.commit()

                    # ─ weekly muscle performance ────────────────────────
                    if today_weekday == 1:  # Tuesday
                        db.add(
                            ClientWeeklyPerformance(
                                client_id=client_id,
                                week_start=week_start,
                                muscle_group=mgrp,
                                total_volume=cur_vol,
                                avg_weight=cur_wt,
                                avg_reps=cur_reps,
                            )
                        )
                        db.commit()
                    else:
                        wi = db.query(ClientWeeklyPerformance).filter(
                            ClientWeeklyPerformance.client_id == client_id,
                            ClientWeeklyPerformance.week_start == week_start,
                            ClientWeeklyPerformance.muscle_group == mgrp,
                        ).first()
                        if wi:
                            wi.total_volume = _dbg_bin("add", wi.total_volume, cur_vol,
                                                       "weekly.total_volume", client_id, client_name)
                            wi.avg_weight   = _dbg_bin("avg", wi.avg_weight,   cur_wt,
                                                       "weekly.avg_weight",   client_id, client_name)
                            wi.avg_reps     = _dbg_bin("avg", wi.avg_reps,     cur_reps,
                                                       "weekly.avg_reps",     client_id, client_name)
                            db.commit()
                        else:
                            db.add(
                                ClientWeeklyPerformance(
                                    client_id=client_id,
                                    week_start=week_start,
                                    muscle_group=mgrp,
                                    total_volume=cur_vol,
                                    avg_weight=cur_wt,
                                    avg_reps=cur_reps,
                                )
                            )
                            db.commit()

                # ─ overall weekly aggregate ─────────────────────────────
                total_volume = round(sum(st["total_volume"] for st in muscle_stats.values()))
                overall_avg_reps = round(sum(st["avg_reps"] for st in muscle_stats.values()) /
                                         len(muscle_stats)) if muscle_stats else 0
                overall_avg_weight = round(sum(st["avg_weight"] for st in muscle_stats.values()) /
                                           len(muscle_stats)) if muscle_stats else 0

                ai = db.query(AggregatedInsights).filter(
                    AggregatedInsights.client_id == client_id,
                    AggregatedInsights.week_start == week_start,
                ).first()

                if today_weekday == 1 or not ai:
                    db.add(
                        AggregatedInsights(
                            client_id=client_id,
                            week_start=week_start,
                            total_volume=total_volume,
                            avg_weight=overall_avg_weight,
                            avg_reps=overall_avg_reps,
                        )
                    )
                else:
                    ai.total_volume = _dbg_bin("add", ai.total_volume, total_volume,
                                               "Agg.total_volume", client_id, client_name)
                    ai.avg_weight   = _dbg_bin("avg", ai.avg_weight,   overall_avg_weight,
                                               "Agg.avg_weight",   client_id, client_name)
                    ai.avg_reps     = _dbg_bin("avg", ai.avg_reps,     overall_avg_reps,
                                               "Agg.avg_reps",     client_id, client_name)
                db.commit()

            # ======================= DIET DAILY =============================
            print("yesterday is",yesterday)
            diet_record = db.query(ClientActual).filter(
                ClientActual.client_id == client_id,
                ClientActual.date == yesterday,
            ).first()


            client_target = db.query(ClientTarget).filter(
                ClientTarget.client_id == client_id
            ).first()

            if diet_record and diet_record.calories:
                aggregated = db.query(ClientActualAggregated).filter(
                    ClientActualAggregated.client_id == client_id
                ).first() or ClientActualAggregated(client_id=client_id)

                for fld, new_val in (
                    ("avg_calories", diet_record.calories),
                    ("avg_protein",  diet_record.protein),
                    ("avg_fats",     diet_record.fats),
                    ("avg_carbs",    diet_record.carbs),
                ):
                    current = getattr(aggregated, fld)
                    setattr(
                        aggregated,
                        fld,
                        _dbg_bin("avg" if current is not None else "add",
                                 current, new_val, fld, client_id, client_name),
                    )
                db.add(aggregated)
                db.commit()

                # ─ target / streak logic ───────────────────────────────
                client = db.query(Client).filter(Client.client_id == client_id).first()
                goal = (
                    "weight_loss"
                    if client and client.goals and "weight_loss" in client.goals
                    else ("weight_gain" if client and client.goals else "maintenance")
                )
                target_cals = client_target.calories if client_target else 0
                if target_cals:
                    tol, band = _bands(target_cals)           # helper from your codebase
                    delta     = diet_record.calories - target_cals
                    bucket    = _bucket(goal, delta, tol, band)  # helper evaluates “met / surplus / deficit”

                    if bucket == "met":
                        aggregated.no_of_days_calories_met = (aggregated.no_of_days_calories_met or 0) + 1
                    elif bucket == "surplus":
                        aggregated.calories_surplus_days   = (aggregated.calories_surplus_days or 0) + 1
                    elif bucket == "deficit":
                        aggregated.calories_deficit_days   = (aggregated.calories_deficit_days or 0) + 1

                    if _streak_ok(goal, bucket):              # your helper for streak continuation
                        aggregated.current_streak = (aggregated.current_streak or 0) + 1
                        if (aggregated.longest_streak or 0) < aggregated.current_streak:
                            aggregated.longest_streak = aggregated.current_streak
                    else:
                        aggregated.current_streak = 0
                    db.commit()

            # ================ WEEKLY DIET ROLL-UP ===========================
            if diet_record and diet_record.calories:
                adw = db.query(ClientActualAggregatedWeekly).filter(
                    ClientActualAggregatedWeekly.client_id == client_id,
                    ClientActualAggregatedWeekly.week_start == week_start,
                ).first()

                if today_weekday == 1:
                    if not adw:
                        adw = ClientActualAggregatedWeekly(
                            client_id=client_id,
                            week_start=week_start,
                            avg_weight=diet_record.weight,
                            avg_calories=diet_record.calories,
                            avg_protein=diet_record.protein,
                            avg_carbs=diet_record.carbs,
                            avg_fats=diet_record.fats,
                        )
                        db.add(adw)
                        db.commit()
                    else:
                        for fld, new_val in (
                            ("avg_calories", diet_record.calories),
                            ("avg_protein",  diet_record.protein),
                            ("avg_carbs",    diet_record.carbs),
                            ("avg_fats",     diet_record.fats),
                            ("avg_water_intake", diet_record.water_intake),
                            ("avg_sleep_hours",  diet_record.sleep_hours),
                        ):
                            setattr(
                                adw,
                                fld,
                                _dbg_bin("add", getattr(adw, fld), new_val,
                                         f"weekly.{fld}", client_id, client_name),
                            )
                        db.commit()
                else:
                    if adw:
                        for fld, new_val in (
                            ("avg_calories", diet_record.calories),
                            ("avg_protein",  diet_record.protein),
                            ("avg_carbs",    diet_record.carbs),
                            ("avg_fats",     diet_record.fats),
                        ):
                            current = getattr(adw, fld)
                            setattr(
                                adw,
                                fld,
                                _dbg_bin("add", current, new_val,
                                         f"weekly.{fld}", client_id, client_name)
                                if current is not None else new_val,
                            )
                        db.commit()
                    else:
                        db.add(
                            ClientActualAggregatedWeekly(
                                client_id=client_id,
                                week_start=week_start,
                                avg_weight=diet_record.weight,
                                avg_calories=diet_record.calories,
                                avg_protein=diet_record.protein,
                                avg_carbs=diet_record.carbs,
                                avg_fats=diet_record.fats,
                            )
                        )
                        db.commit()

            # ================= WATER & WEIGHT MONTHLY =======================
            month_start = date.today().replace(day=1)
            ga = db.query(ClientGeneralAnalysis).filter(
                ClientGeneralAnalysis.client_id == client_id,
                ClientGeneralAnalysis.date == month_start,
            ).first()

            yesterday_rec = db.query(ClientActual).filter(
                ClientActual.client_id == client_id,
                ClientActual.date == yesterday,
            ).first()
            today_rec = db.query(ClientActual).filter(
                ClientActual.client_id == client_id,
                ClientActual.date == date.today(),
            ).first()

            actual_water  = yesterday_rec.water_intake if yesterday_rec else None
            actual_weight = client.weight or 0
            latest_weight = yesterday_rec.weight if yesterday_rec else None

            if ga:
                ga.water_taken = _dbg_bin("avg", ga.water_taken, actual_water,
                                          "water_taken", client_id, client_name)
                if ga.weight is None:
                    ga.weight = actual_weight
                db.commit()
            elif actual_water is not None:
                db.add(
                    ClientGeneralAnalysis(
                        client_id=client_id,
                        date=month_start,
                        water_taken=actual_water,
                        weight=actual_weight,
                    )
                )
                db.commit()

            if today_rec:
                today_rec.weight = latest_weight if latest_weight else actual_weight
            else:
                db.add(
                    ClientActual(
                        client_id=client_id,
                        date=date.today(),
                        weight=latest_weight if latest_weight else actual_weight,
                    )
                )
            db.commit()

    except Exception as e:
        print("FATAL error in get_workout_combined_summary():", e)
        raise
    finally:
        db.close()


def get_all_gym_analysis():

    try:
        db = SessionLocal()
        gyms = db.query(Gym).all()
        default_analysis = {
            "gender": {},
            "goal_data": {},
            "expenditure": {},
            "goal_income": {},
            "training_data": {},
            "training_income": {},
            "expenditure_data": {}
        }
        if gyms: 
        
            for gym in gyms:
                gym_id = gym.gym_id   
                print("gym id is",gym_id) 
                
                analysis_record = db.query(GymAnalysis).filter(
                    GymAnalysis.gym_id == gym_id
                ).first()
                
                if not analysis_record:
                    analysis_record = GymAnalysis(
                        gym_id=gym_id,
                        analysis=default_analysis
                    )
                    db.add(analysis_record)
                    db.commit()
                    db.refresh(analysis_record)


                raw_analysis = analysis_record.analysis
                analysis_data: dict = {}

                if raw_analysis:                       
                    if isinstance(raw_analysis, dict):         
                        analysis_data = raw_analysis
                    elif isinstance(raw_analysis, str):        
                        try:
                            analysis_data = json.loads(raw_analysis)
                        except json.JSONDecodeError:
                            print("Gym %s – invalid JSON in analysis column: %s",
                                        analysis_record.gym_id, raw_analysis)
                            analysis_data = {}
                else:
                    analysis_data = {}


                print("analsis tupe",analysis_data)

                training_dict = analysis_data.get("training_data", {})
                print("training_dict",training_dict)


                total_training = sum(training_dict.values())
                if total_training > 0:

                    sorted_training = sorted(training_dict.items(), key=lambda x: x[1], reverse=True)
                    top_2 = sorted_training[:2]
                    others = sorted_training[2:]
                    training_breakdown = {}
                    for k, v in top_2:
                        training_breakdown[k] = round((v / total_training) * 100, 2)
                    if len(sorted_training) > 2:
                        others_sum = sum(x[1] for x in others)
                        training_breakdown["others"] = round((others_sum / total_training) * 100, 2)
                    analysis_data["training_income"] = training_breakdown
                else:
                    analysis_data["training_income"] = {}


                goal_dict = analysis_data.get("goal_data", {})
                print("goal_dict",goal_dict)


                total_goals = sum(goal_dict.values())
                if total_goals > 0:
                    sorted_goals = sorted(goal_dict.items(), key=lambda x: x[1], reverse=True)
                    top_2 = sorted_goals[:2]
                    others = sorted_goals[2:]
                    goal_breakdown = {}
                    for k, v in top_2:
                        goal_breakdown[k] = round((v / total_goals) * 100, 2)
                    if len(sorted_goals) > 2:
                        others_sum = sum(x[1] for x in others)
                        goal_breakdown["others"] = round((others_sum / total_goals) * 100, 2)
                    analysis_data["goal_income"] = goal_breakdown
                else:
                    analysis_data["goal_income"] = {}


                exp_data = analysis_data.get("expenditure_data", {})
                print("exp_data",exp_data)

                total_exp = sum(exp_data.values())
                if total_exp > 0:
                    sorted_exp = sorted(exp_data.items(), key=lambda x: x[1], reverse=True)
                    top_2 = sorted_exp[:2]
                    others = sorted_exp[2:]
                    exp_breakdown = {}
                    for k, v in top_2:
                        exp_breakdown[k] = round((v / total_exp) * 100, 2)
                    if len(sorted_exp) > 2:
                        others_sum = sum(x[1] for x in others)
                        exp_breakdown["others"] = round((others_sum / total_exp) * 100, 2)
                    analysis_data["expenditure"] = exp_breakdown
                else:
                    analysis_data["expenditure"] = {}


            
                clients = db.query(Client).filter(Client.gym_id == gym_id).all()
                total_clients = len(clients)
                if total_clients > 0:
                    gender_counts = {"Male": 0, "Female": 0}
                    for cl in clients:
                        key = cl.gender if cl.gender in gender_counts else "other"
                        gender_counts[key] += 1
                    gender_breakdown = {k: round((v / total_clients) * 100, 2) for k, v in gender_counts.items()}
                    analysis_data["gender"] = gender_breakdown
                else:
                    analysis_data["gender"] = {}
        
            
                analysis_record.analysis = analysis_data
                print("analysis data is",analysis_data)
                db.commit()
        
        
                yesterday = date.today() - timedelta(days=1)
        
                daily_record = db.query(DailyGymHourlyAgg).filter(
                    DailyGymHourlyAgg.gym_id == gym_id,
                    DailyGymHourlyAgg.agg_date == yesterday
                ).first()
            
                if daily_record:
                
                    aggregated_record = db.query(GymHourlyAgg).filter(
                        GymHourlyAgg.gym_id == gym_id
                    ).first()
                
                    if aggregated_record:
                        aggregated_record.col_4_6   = round((aggregated_record.col_4_6   + daily_record.col_4_6)   / 2)
                        aggregated_record.col_6_8   = round((aggregated_record.col_6_8   + daily_record.col_6_8)   / 2)
                        aggregated_record.col_8_10  = round((aggregated_record.col_8_10  + daily_record.col_8_10)  / 2)
                        aggregated_record.col_10_12 = round((aggregated_record.col_10_12 + daily_record.col_10_12) / 2)
                        aggregated_record.col_12_14 = round((aggregated_record.col_12_14 + daily_record.col_12_14) / 2)
                        aggregated_record.col_14_16 = round((aggregated_record.col_14_16 + daily_record.col_14_16) / 2)
                        aggregated_record.col_16_18 = round((aggregated_record.col_16_18 + daily_record.col_16_18) / 2)
                        aggregated_record.col_18_20 = round((aggregated_record.col_18_20 + daily_record.col_18_20) / 2)
                        aggregated_record.col_20_22 = round((aggregated_record.col_20_22 + daily_record.col_20_22) / 2)
                        aggregated_record.col_22_24 = round((aggregated_record.col_22_24 + daily_record.col_22_24) / 2)

                    
                    else:
                    
                        aggregated_record = GymHourlyAgg(
                            gym_id      = gym_id,
                            col_4_6     = daily_record.col_4_6,
                            col_6_8     = daily_record.col_6_8,
                            col_8_10    = daily_record.col_8_10,
                            col_10_12   = daily_record.col_10_12,
                            col_12_14   = daily_record.col_12_14,
                            col_14_16   = daily_record.col_14_16,
                            col_16_18   = daily_record.col_16_18,
                            col_18_20   = daily_record.col_18_20,
                            col_20_22   = daily_record.col_20_22,
                            col_22_24   = daily_record.col_22_24,
                        )
                        db.add(aggregated_record)
                
                    db.commit()

    
    
    except Exception as e:
        print("error is",e)

    finally:
        db.close()
    

async def redis_keys_deleting():
    redis_client = await get_redis()
    pattern = "*:target_actual"
    keys = await redis_client.keys(pattern)
    if keys:
        await redis_client.delete(*keys)
        print(f"Deleted keys matching pattern: {pattern}")
    else:
        print(f"No keys found matching pattern: {pattern}")

    await redis_client.aclose()



# OLD VERSION - Commented out due to bugs:
# - Missing queued = 0 reset (stuck reminders if Lambda fails)
# - No condition to check if reminder was sent (resets unsent reminders)
# - NULL intimation_start_time destroys reminder_time
# - Timezone issue with CURDATE()
#
# def cleanse_reminders() -> None:
#
#     with SessionLocal() as session:
#         session.execute(
#             text(
#                 """
#                 DELETE FROM reminders
#                 WHERE is_recurring = 0
#                 AND (queued = 1 OR reminder_Sent = 1)
#                 """
#             )
#         )
#         session.execute(
#             text(
#                 """
#                 UPDATE reminders
#                 SET    reminder_Sent     = 0,
#                        reminder_time     = CASE
#                                              WHEN reminder_mode = 'others' THEN TIME(others_time)
#                                              ELSE intimation_start_time
#                                            END,
#                        others_time       = CASE
#                                              WHEN reminder_mode = 'others' AND others_time IS NOT NULL
#                                              THEN CONCAT(CURDATE(), ' ', TIME(others_time))
#                                              ELSE others_time
#                                            END
#                 WHERE  is_recurring      = 1
#                 """
#             )
#         )
#
#         session.commit()


def cleanse_reminders() -> None:

    with SessionLocal() as session:
        
        session.execute(
            text(
                """
                DELETE FROM reminders
                WHERE is_recurring = 0
                  AND (queued = 1 OR reminder_Sent = 1)
                """
            )
        )

        session.execute(
            text(
                """
                UPDATE reminders
                SET    reminder_Sent = 0,
                       queued        = 0,
                       reminder_time = CASE
                                         WHEN reminder_mode = 'others' THEN TIME(others_time)
                                         WHEN intimation_start_time IS NOT NULL THEN intimation_start_time
                                         ELSE reminder_time
                                       END,
                       others_time   = CASE
                                         WHEN reminder_mode = 'others' AND others_time IS NOT NULL
                                         THEN CONCAT(DATE(CONVERT_TZ(NOW(), 'UTC', 'Asia/Kolkata')), ' ', TIME(others_time))
                                         ELSE others_time
                                       END
                WHERE  is_recurring = 1
                  AND  (reminder_Sent = 1 OR queued = 1)
                """
            )
        )

        session.commit()




def _last_receipt(db: Session, client_id: int, gym_id: int) -> Optional[FeesReceipt]:
    return (
        db.query(FeesReceipt)
          .filter(and_(FeesReceipt.client_id == client_id,
                       FeesReceipt.gym_id    == gym_id))
          .order_by(FeesReceipt.created_at.desc())
          .first()
    )


def upsert_about_to_expire(db: Session, client: Client, today: date) -> None:

    fees = (
        db.query(GymFees)
          .filter(GymFees.client_id == client.client_id)
          .order_by(GymFees.start_date.desc())
          .first()
    )
    if not fees or not fees.end_date:
        return   
                                       
    diff_days = (fees.end_date - today).days
    print("diff_days",diff_days)

    about = (
        db.query(AboutToExpire)
          .filter(AboutToExpire.client_id == client.client_id)
          .first()
    )

    if diff_days in (1, 2, 3):
        gym   = db.query(Gym).filter(Gym.gym_id == client.gym_id).first()
        cg_id = db.query(ClientGym).filter(ClientGym.client_id == client.client_id).first()
        acct  = db.query(AccountDetails).filter(AccountDetails.gym_id == client.gym_id).first()
        rcpt  = _last_receipt(db, client.client_id, client.gym_id)

        payload = dict(
            client_id         = client.client_id,
            gym_id            = client.gym_id,
            gym_client_id     = cg_id.gym_client_id   if cg_id else None,
            admission_number = cg_id.admission_number if cg_id else None,
            expires_in        = diff_days,
            client_name       = client.name,
            gym_name          = gym.name   if gym else None,
            gym_logo          = gym.logo   if gym else None,
            gym_contact       = None,                     
            gym_location      = gym.location if gym else None,
            plan_id           = rcpt.plan_id        if rcpt else None,
            plan_description  = rcpt.plan_description if rcpt else None,
            fees              = rcpt.fees           if rcpt else None,
            discount          = rcpt.discount       if rcpt else None,
            discounted_fees   = rcpt.discounted_fees if rcpt else None,
            due_date          = fees.end_date,
            invoice_number    = rcpt.invoice_number if rcpt else None,
            client_contact    = client.contact,
            bank_details      = acct.account_number if acct else None,
            ifsc_code         = acct.account_ifsccode if acct else None,
            bank_name         = acct.bank_name      if acct else None,
            upi_id            = acct.upi_id         if acct else None,
            branch            = acct.account_branch if acct else None,
            account_holder_name = acct.account_holdername if acct else None,
            gst_number        = acct.gst_number     if acct else None,
            mail_status       = False,
            expired           = False,
            email             = client.email,
            updated_at        = datetime.now(),
        )

        if about:                                  
            for k, v in payload.items():
                setattr(about, k, v)
        else:                                      
            db.add(AboutToExpire(**payload))

    elif diff_days < 0 and about and not about.expired:
        about.expired     = True
        about.expires_in  = 0
        about.updated_at  = datetime.now()

        if client.status != "inactive":
            client.status = "inactive"


def process_all_clients() -> None:

    today = date.today()
    db = SessionLocal()
    for client in (
        db.query(Client)
          .filter(Client.status == "active")
          .all()
    ):
        print("client name",client.name)
        upsert_about_to_expire(db, client, today)

    db.commit()


def birthday_job() -> None:
    today = date.today()                            
    with SessionLocal() as db:

        db.execute(text("TRUNCATE TABLE client_birthdays"))
        db.commit() 

        birthday_clients = (
            db.query(Client)
              .filter(func.month(Client.dob) == today.month,
                      func.day(Client.dob)   == today.day)
              .all()
        )

        for cl in birthday_clients:

            cl.age = today.year - cl.dob.year
            print("age",cl.age)
            if cl.status == "active":
                exists = (
                    db.query(ClientBirthday)
                      .filter(ClientBirthday.client_id == cl.client_id)
                      .first()
                )
                if not exists:
                    db.add(
                        ClientBirthday(
                            client_id   = cl.client_id,
                            client_name = cl.name,
                            expo_token  = cl.expo_token
                        )
                    )

        db.commit()




if __name__ == "__main__":
    asyncio.run(redis_keys_deleting())
    # get_workout_combined_summary()
    # get_all_gym_analysis()
    cleanse_reminders()
    # process_all_clients()
    # birthday_job()

