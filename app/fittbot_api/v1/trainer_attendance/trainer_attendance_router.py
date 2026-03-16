# app/routers/trainer_attendance_router.py

from typing import Optional, List, Dict, Any
import json
from datetime import datetime, date
from dateutil.relativedelta import relativedelta

from fastapi import APIRouter, Depends, Request
from pydantic import BaseModel

from sqlalchemy.orm import Session
from sqlalchemy.orm.attributes import flag_modified
from sqlalchemy import and_, or_, extract, func

from redis.asyncio import Redis

from app.models.database import get_db
from app.utils.redis_config import get_redis
from app.utils.logging_utils import FittbotHTTPException

from app.models.fittbot_models import (
    TrainerAttendance,
    Trainer,
    TrainerProfile,
    GymLocation,
    Gym,
)

router = APIRouter(prefix="/trainer-attendance", tags=["Trainer Attendance"])


async def clear_trainer_attendance_cache(gym_id: int, redis: Redis):
    """Clear all trainer attendance related cache keys for a gym"""
    # Clear specific cache keys including today's date-based keys
    today = date.today()
    cache_keys_to_clear = [
        f"gym:{gym_id}:trainer_attendance",
        f"gym:{gym_id}:trainer_attendance_summary:{today.strftime('%Y-%m-%d')}",
        f"gym:{gym_id}:trainer_attendance:today",
        f"gym:{gym_id}:members"  # This includes trainer counts in home API
    ]
    
    for cache_key in cache_keys_to_clear:
        if await redis.exists(cache_key):
            await redis.delete(cache_key)
    
    # Clear pattern-based cache keys (monthly attendance caches)
    monthly_pattern = f"gym:{gym_id}:trainer_monthly:*"
    monthly_keys = await redis.keys(monthly_pattern)
    if monthly_keys:
        await redis.delete(*monthly_keys)


class TrainerPunchInRequest(BaseModel):
    trainer_id: int
    gym_id: int
    location: Optional[str] = None


class TrainerPunchOutRequest(BaseModel):
    trainer_id: int
    gym_id: int
    location: Optional[str] = None


@router.get("/check-status")
async def check_trainer_attendance_status(
    trainer_id: int, 
    gym_id: int,
    db: Session = Depends(get_db)
):
    """Check if trainer is currently punched in for a specific gym today"""
    try:
        today = datetime.now().date()
        record = db.query(TrainerAttendance).filter(
            TrainerAttendance.trainer_id == trainer_id,
            TrainerAttendance.gym_id == gym_id,
            TrainerAttendance.date == today
        ).first()

        if not record or not record.punch_sessions:
            return {
                "status": 200,
                "is_punched_in": False,
                "current_session": None
            }

        # Check if any session is currently active (punch_in without punch_out)
        for session in record.punch_sessions:
            if session.get("punch_in") and not session.get("punch_out"):
                return {
                    "status": 200,
                    "is_punched_in": True,
                    "current_session": session
                }

        return {
            "status": 200,
            "is_punched_in": False,
            "current_session": None,
            "completed_sessions": len(record.punch_sessions)
        }

    except Exception as e:
        raise FittbotHTTPException(
            status_code=500,
            detail="Error checking trainer attendance status",
            error_code="TRAINER_ATTENDANCE_STATUS_ERROR",
            log_data={"trainer_id": trainer_id, "gym_id": gym_id, "error": str(e)},
        )


@router.get("/check-status-with-location")
async def check_trainer_attendance_with_location(
    trainer_id: int,
    gym_id: int,
    db: Session = Depends(get_db)
):
    """Check trainer attendance status and return gym location for proximity validation"""
    try:
        today = datetime.now().date()
        record = db.query(TrainerAttendance).filter(
            TrainerAttendance.trainer_id == trainer_id,
            TrainerAttendance.gym_id == gym_id,
            TrainerAttendance.date == today
        ).first()

        is_punched_in = False
        current_session = None
        
        if record and record.punch_sessions:
            for session in record.punch_sessions:
                if session.get("punch_in") and not session.get("punch_out"):
                    is_punched_in = True
                    current_session = session
                    break

        # Get gym location
        gym_location = db.query(GymLocation).filter(GymLocation.gym_id == gym_id).first()
        latitude = longitude = None
        if gym_location:
            latitude = float(gym_location.latitude)
            longitude = float(gym_location.longitude)

        return {
            "status": 200,
            "is_punched_in": is_punched_in,
            "current_session": current_session,
            "gym_location": {
                "latitude": latitude,
                "longitude": longitude
            }
        }

    except Exception as e:
        raise FittbotHTTPException(
            status_code=500,
            detail=f"Error fetching trainer attendance with location: {str(e)}",
            error_code="TRAINER_ATTENDANCE_LOCATION_ERROR",
            log_data={"trainer_id": trainer_id, "gym_id": gym_id, "error": str(e)},
        )


@router.post("/punch-in")
async def trainer_punch_in(
    request: TrainerPunchInRequest,
    db: Session = Depends(get_db),
    redis: Redis = Depends(get_redis),
):
    """Record trainer punch-in for a specific gym"""
    try:
        today = date.today()
        current_time = datetime.now()

        # Verify trainer profile exists for this gym
        trainer_profile = db.query(TrainerProfile).filter(
            TrainerProfile.trainer_id == request.trainer_id,
            TrainerProfile.gym_id == request.gym_id
        ).first()

        if not trainer_profile:
            raise FittbotHTTPException(
                status_code=404,
                detail="Trainer profile not found for this gym",
                error_code="TRAINER_PROFILE_NOT_FOUND",
                log_data={"trainer_id": request.trainer_id, "gym_id": request.gym_id},
            )

        # Get or create attendance record for today
        attendance_record = db.query(TrainerAttendance).filter(
            TrainerAttendance.trainer_id == request.trainer_id,
            TrainerAttendance.gym_id == request.gym_id,
            TrainerAttendance.date == today
        ).first()

        if not attendance_record:
            attendance_record = TrainerAttendance(
                trainer_id=request.trainer_id,
                gym_id=request.gym_id,
                date=today,
                punch_sessions=[],
                total_hours=0.0,
                status="active"
            )
            db.add(attendance_record)
            db.flush()  # Get the ID

        # Check if trainer is already punched in (has an active session)
        if attendance_record.punch_sessions:
            for session in attendance_record.punch_sessions:
                if session.get("punch_in") and not session.get("punch_out"):
                    raise FittbotHTTPException(
                        status_code=400,
                        detail="Trainer is already punched in. Please punch out first.",
                        error_code="ALREADY_PUNCHED_IN",
                        log_data={"trainer_id": request.trainer_id, "gym_id": request.gym_id},
                    )

        # Add new punch-in session
        new_session = {
            "punch_in": current_time.isoformat(),
            "punch_out": None,
            "location_in": request.location,
            "location_out": None
        }

        if not attendance_record.punch_sessions:
            attendance_record.punch_sessions = []
        
        attendance_record.punch_sessions.append(new_session)
        attendance_record.updated_at = current_time
        
        # Mark the punch_sessions field as modified for SQLAlchemy to detect changes
        flag_modified(attendance_record, 'punch_sessions')
        
        db.commit()

        # Clear all trainer attendance related cache
        await clear_trainer_attendance_cache(request.gym_id, redis)

        return {
            "status": 200,
            "message": "Trainer punched in successfully",
            "punch_in_time": current_time.isoformat(),
            "session_count": len(attendance_record.punch_sessions)
        }

    except FittbotHTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise FittbotHTTPException(
            status_code=500,
            detail="An unexpected error occurred during trainer punch-in",
            error_code="TRAINER_PUNCH_IN_ERROR",
            log_data={"exc": repr(e), "trainer_id": request.trainer_id, "gym_id": request.gym_id},
        )


@router.post("/punch-out")
async def trainer_punch_out(
    request: TrainerPunchOutRequest,
    db: Session = Depends(get_db),
    redis: Redis = Depends(get_redis),
):
    """Record trainer punch-out for a specific gym"""
    try:
        today = date.today()
        current_time = datetime.now()

        # Get attendance record for today
        attendance_record = db.query(TrainerAttendance).filter(
            TrainerAttendance.trainer_id == request.trainer_id,
            TrainerAttendance.gym_id == request.gym_id,
            TrainerAttendance.date == today
        ).first()

        if not attendance_record or not attendance_record.punch_sessions:
            raise FittbotHTTPException(
                status_code=404,
                detail="No punch-in record found for today",
                error_code="NO_PUNCH_IN_RECORD",
                log_data={"trainer_id": request.trainer_id, "gym_id": request.gym_id},
            )

        # Find the active session (punch_in without punch_out)
        active_session_found = False
        completed_session = None
        for session in attendance_record.punch_sessions:
            if session.get("punch_in") and not session.get("punch_out"):
                session["punch_out"] = current_time.isoformat()
                session["location_out"] = request.location
                
                # Calculate session duration in hours
                punch_in_time = datetime.fromisoformat(session["punch_in"])
                session_hours = (current_time - punch_in_time).total_seconds() / 3600
                session["duration_hours"] = round(session_hours, 2)
                
                completed_session = session  # Store reference to the completed session
                active_session_found = True
                break

        if not active_session_found:
            raise FittbotHTTPException(
                status_code=400,
                detail="No active punch-in session found",
                error_code="NO_ACTIVE_SESSION",
                log_data={"trainer_id": request.trainer_id, "gym_id": request.gym_id},
            )

        # Calculate total hours for the day
        total_hours = 0.0
        for session in attendance_record.punch_sessions:
            if session.get("duration_hours"):
                total_hours += session["duration_hours"]

        attendance_record.total_hours = round(total_hours, 2)
        attendance_record.updated_at = current_time
        
        # Mark the punch_sessions field as modified for SQLAlchemy to detect changes
        flag_modified(attendance_record, 'punch_sessions')
        
        # Check if all sessions are completed
        all_completed = all(
            session.get("punch_out") is not None 
            for session in attendance_record.punch_sessions
        )
        if all_completed:
            attendance_record.status = "completed"

        db.commit()

        # Clear all trainer attendance related cache
        await clear_trainer_attendance_cache(request.gym_id, redis)

        return {
            "status": 200,
            "message": "Trainer punched out successfully",
            "punch_out_time": current_time.isoformat(),
            "session_duration_hours": completed_session.get("duration_hours", 0),
            "total_hours_today": attendance_record.total_hours,
            "punch_out_details": {
                "punch_in_time": completed_session.get("punch_in"),
                "punch_out_time": completed_session.get("punch_out"),
                "location_in": completed_session.get("location_in"),
                "location_out": completed_session.get("location_out"),
                "duration_hours": completed_session.get("duration_hours", 0)
            }
        }

    except FittbotHTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise FittbotHTTPException(
            status_code=500,
            detail="An unexpected error occurred during trainer punch-out",
            error_code="TRAINER_PUNCH_OUT_ERROR",
            log_data={"exc": repr(e), "trainer_id": request.trainer_id, "gym_id": request.gym_id},
        )


@router.get("/today/{gym_id}")
async def get_today_trainer_attendance(
    gym_id: int,
    db: Session = Depends(get_db),
    redis: Redis = Depends(get_redis)
):
    """Get today's attendance for all trainers in a specific gym"""
    try:
        today = date.today()

        # Check cache first with date-specific key
        cache_key = f"gym:{gym_id}:trainer_attendance:today:{today.strftime('%Y-%m-%d')}"
        cached_data = await redis.get(cache_key)
        if cached_data:
            return json.loads(cached_data)

        # Get all trainer profiles for this gym
        trainer_profiles = db.query(TrainerProfile).filter(
            TrainerProfile.gym_id == gym_id
        ).all()

        trainer_attendance_data = []
        total_present = 0
        total_hours = 0.0

        for profile in trainer_profiles:
            attendance_record = db.query(TrainerAttendance).filter(
                TrainerAttendance.trainer_id == profile.trainer_id,
                TrainerAttendance.gym_id == gym_id,
                TrainerAttendance.date == today
            ).first()

            trainer_data = {
                "trainer_id": profile.trainer_id,
                "full_name": profile.full_name,
                "email": profile.email,
                "specialization": profile.specializations,
                "profile_image": profile.profile_image,
                "is_present": False,
                "is_currently_active": False,
                "punch_sessions": [],
                "total_hours": 0.0,
                "status": "absent"
            }

            if attendance_record and attendance_record.punch_sessions:
                trainer_data["is_present"] = True
                trainer_data["punch_sessions"] = attendance_record.punch_sessions
                trainer_data["total_hours"] = attendance_record.total_hours
                trainer_data["status"] = attendance_record.status
                
                # Check if currently active (has unpunched session)
                for session in attendance_record.punch_sessions:
                    if session.get("punch_in") and not session.get("punch_out"):
                        trainer_data["is_currently_active"] = True
                        break

                total_present += 1
                total_hours += attendance_record.total_hours

            trainer_attendance_data.append(trainer_data)

        result = {
            "status": 200,
            "date": today.isoformat(),
            "gym_id": gym_id,
            "summary": {
                "total_trainers": len(trainer_profiles),
                "present_today": total_present,
                "currently_active": sum(1 for t in trainer_attendance_data if t["is_currently_active"]),
                "total_hours": round(total_hours, 2),
                "average_hours": round(total_hours / max(total_present, 1), 2)
            },
            "trainers": trainer_attendance_data
        }

        # Cache for 5 minutes
        await redis.setex(cache_key, 300, json.dumps(result, default=str))
        return result

    except Exception as e:
        raise FittbotHTTPException(
            status_code=500,
            detail="Error fetching today's trainer attendance",
            error_code="TODAY_TRAINER_ATTENDANCE_ERROR",
            log_data={"gym_id": gym_id, "error": str(e)},
        )


@router.get("/monthly/{trainer_id}")
async def get_monthly_trainer_attendance(
    trainer_id: int,
    gym_id: int,
    month: Optional[int] = None,
    year: Optional[int] = None,
    db: Session = Depends(get_db)
):
    """Get monthly attendance for a specific trainer"""
    try:
        if not month:
            month = datetime.now().month
        if not year:
            year = datetime.now().year

        # Get all attendance records for the trainer in the specified month
        attendance_records = db.query(TrainerAttendance).filter(
            TrainerAttendance.trainer_id == trainer_id,
            TrainerAttendance.gym_id == gym_id,
            extract('month', TrainerAttendance.date) == month,
            extract('year', TrainerAttendance.date) == year
        ).all()

        # Get trainer info
        trainer = db.query(Trainer).filter(Trainer.trainer_id == trainer_id).first()
        if not trainer:
            raise FittbotHTTPException(
                status_code=404,
                detail="Trainer not found",
                error_code="TRAINER_NOT_FOUND",
            )

        daily_records = []
        total_days_present = 0
        total_hours = 0.0
        total_sessions = 0

        for record in attendance_records:
            if record.punch_sessions:
                total_days_present += 1
                total_hours += record.total_hours
                total_sessions += len(record.punch_sessions)

            daily_records.append({
                "date": record.date.isoformat(),
                "punch_sessions": record.punch_sessions or [],
                "total_hours": record.total_hours,
                "status": record.status,
                "session_count": len(record.punch_sessions) if record.punch_sessions else 0
            })

        # Calculate working days in month (assuming 30 days for simplicity)
        working_days = 30  # You might want to calculate actual working days

        return {
            "status": 200,
            "trainer_id": trainer_id,
            "trainer_name": trainer.full_name,
            "gym_id": gym_id,
            "month": month,
            "year": year,
            "summary": {
                "total_days_present": total_days_present,
                "total_working_days": working_days,
                "attendance_percentage": round((total_days_present / working_days) * 100, 2),
                "total_hours": round(total_hours, 2),
                "total_sessions": total_sessions,
                "average_hours_per_day": round(total_hours / max(total_days_present, 1), 2)
            },
            "daily_records": daily_records
        }

    except FittbotHTTPException:
        raise
    except Exception as e:
        raise FittbotHTTPException(
            status_code=500,
            detail="Error fetching monthly trainer attendance",
            error_code="MONTHLY_TRAINER_ATTENDANCE_ERROR",
            log_data={"trainer_id": trainer_id, "gym_id": gym_id, "error": str(e)},
        )


@router.get("/monthly/gym/{gym_id}")
async def get_monthly_gym_trainer_attendance(
    gym_id: int,
    month: Optional[int] = None,
    year: Optional[int] = None,
    db: Session = Depends(get_db)
):
    """Get monthly attendance for all trainers in a specific gym"""
    try:
        if not month:
            month = datetime.now().month
        if not year:
            year = datetime.now().year

        # Get all trainer profiles for this gym
        trainer_profiles = db.query(TrainerProfile).filter(
            TrainerProfile.gym_id == gym_id
        ).all()

        trainers_data = []
        gym_total_hours = 0.0
        gym_total_days_present = 0

        for profile in trainer_profiles:
            # Get attendance records for this trainer in the specified month
            attendance_records = db.query(TrainerAttendance).filter(
                TrainerAttendance.trainer_id == profile.trainer_id,
                TrainerAttendance.gym_id == gym_id,
                extract('month', TrainerAttendance.date) == month,
                extract('year', TrainerAttendance.date) == year
            ).all()

            trainer_total_hours = 0.0
            trainer_days_present = 0
            trainer_total_sessions = 0

            for record in attendance_records:
                if record.punch_sessions:
                    trainer_days_present += 1
                    trainer_total_hours += record.total_hours
                    trainer_total_sessions += len(record.punch_sessions)

            working_days = 30  # You might want to calculate actual working days
            attendance_percentage = round((trainer_days_present / working_days) * 100, 2) if working_days > 0 else 0

            trainer_data = {
                "trainer_id": profile.trainer_id,
                "full_name": profile.full_name,
                "email": profile.email,
                "specialization": profile.specializations,
                "days_present": trainer_days_present,
                "total_hours": round(trainer_total_hours, 2),
                "total_sessions": trainer_total_sessions,
                "attendance_percentage": attendance_percentage,
                "average_hours_per_day": round(trainer_total_hours / max(trainer_days_present, 1), 2)
            }

            trainers_data.append(trainer_data)
            gym_total_hours += trainer_total_hours
            gym_total_days_present += trainer_days_present

        return {
            "status": 200,
            "gym_id": gym_id,
            "month": month,
            "year": year,
            "gym_summary": {
                "total_trainers": len(trainer_profiles),
                "total_gym_hours": round(gym_total_hours, 2),
                "total_trainer_days": gym_total_days_present,
                "average_attendance_percentage": round(
                    sum(t["attendance_percentage"] for t in trainers_data) / max(len(trainers_data), 1), 2
                )
            },
            "trainers": trainers_data
        }

    except Exception as e:
        raise FittbotHTTPException(
            status_code=500,
            detail="Error fetching monthly gym trainer attendance",
            error_code="MONTHLY_GYM_TRAINER_ATTENDANCE_ERROR",
            log_data={"gym_id": gym_id, "error": str(e)},
        )


@router.get("/overall/{trainer_id}")
async def get_overall_trainer_attendance(
    trainer_id: int,
    gym_id: int,
    db: Session = Depends(get_db)
):
    """Get overall attendance statistics for a specific trainer"""
    try:
        # Get all attendance records for the trainer
        attendance_records = db.query(TrainerAttendance).filter(
            TrainerAttendance.trainer_id == trainer_id,
            TrainerAttendance.gym_id == gym_id
        ).all()

        # Get trainer info
        trainer = db.query(Trainer).filter(Trainer.trainer_id == trainer_id).first()
        if not trainer:
            raise FittbotHTTPException(
                status_code=404,
                detail="Trainer not found",
                error_code="TRAINER_NOT_FOUND",
            )

        total_days_present = 0
        total_hours = 0.0
        total_sessions = 0
        first_attendance_date = None
        last_attendance_date = None

        monthly_stats = {}

        for record in attendance_records:
            if record.punch_sessions:
                total_days_present += 1
                total_hours += record.total_hours
                total_sessions += len(record.punch_sessions)

                if not first_attendance_date or record.date < first_attendance_date:
                    first_attendance_date = record.date
                if not last_attendance_date or record.date > last_attendance_date:
                    last_attendance_date = record.date

                # Monthly aggregation
                month_key = f"{record.date.year}-{record.date.month:02d}"
                if month_key not in monthly_stats:
                    monthly_stats[month_key] = {
                        "days": 0,
                        "hours": 0.0,
                        "sessions": 0
                    }
                monthly_stats[month_key]["days"] += 1
                monthly_stats[month_key]["hours"] += record.total_hours
                monthly_stats[month_key]["sessions"] += len(record.punch_sessions)

        # Calculate tenure in months
        if first_attendance_date and last_attendance_date:
            tenure_months = ((last_attendance_date.year - first_attendance_date.year) * 12 + 
                           last_attendance_date.month - first_attendance_date.month) + 1
        else:
            tenure_months = 0

        return {
            "status": 200,
            "trainer_id": trainer_id,
            "trainer_name": trainer.full_name,
            "gym_id": gym_id,
            "overall_summary": {
                "total_days_present": total_days_present,
                "total_hours": round(total_hours, 2),
                "total_sessions": total_sessions,
                "first_attendance": first_attendance_date.isoformat() if first_attendance_date else None,
                "last_attendance": last_attendance_date.isoformat() if last_attendance_date else None,
                "tenure_months": tenure_months,
                "average_hours_per_day": round(total_hours / max(total_days_present, 1), 2),
                "average_sessions_per_day": round(total_sessions / max(total_days_present, 1), 2)
            },
            "monthly_breakdown": {
                month: {
                    "days_present": stats["days"],
                    "total_hours": round(stats["hours"], 2),
                    "total_sessions": stats["sessions"],
                    "average_hours_per_day": round(stats["hours"] / max(stats["days"], 1), 2)
                }
                for month, stats in sorted(monthly_stats.items())
            }
        }

    except FittbotHTTPException:
        raise
    except Exception as e:
        raise FittbotHTTPException(
            status_code=500,
            detail="Error fetching overall trainer attendance",
            error_code="OVERALL_TRAINER_ATTENDANCE_ERROR",
            log_data={"trainer_id": trainer_id, "gym_id": gym_id, "error": str(e)},
        )


@router.get("/overall/gym/{gym_id}")
async def get_overall_gym_trainer_attendance(
    gym_id: int,
    db: Session = Depends(get_db)
):
    """Get overall attendance statistics for all trainers in a specific gym"""
    try:
        # Get all trainer profiles for this gym
        trainer_profiles = db.query(TrainerProfile).filter(
            TrainerProfile.gym_id == gym_id
        ).all()

        trainers_data = []
        gym_total_hours = 0.0
        gym_total_days = 0
        gym_total_sessions = 0

        for profile in trainer_profiles:
            # Get all attendance records for this trainer
            attendance_records = db.query(TrainerAttendance).filter(
                TrainerAttendance.trainer_id == profile.trainer_id,
                TrainerAttendance.gym_id == gym_id
            ).all()

            trainer_total_hours = 0.0
            trainer_days_present = 0
            trainer_total_sessions = 0
            first_attendance = None
            last_attendance = None

            for record in attendance_records:
                if record.punch_sessions:
                    trainer_days_present += 1
                    trainer_total_hours += record.total_hours
                    trainer_total_sessions += len(record.punch_sessions)

                    if not first_attendance or record.date < first_attendance:
                        first_attendance = record.date
                    if not last_attendance or record.date > last_attendance:
                        last_attendance = record.date

            # Calculate tenure
            if first_attendance and last_attendance:
                tenure_months = ((last_attendance.year - first_attendance.year) * 12 + 
                               last_attendance.month - first_attendance.month) + 1
            else:
                tenure_months = 0

            trainer_data = {
                "trainer_id": profile.trainer_id,
                "full_name": profile.full_name,
                "email": profile.email,
                "specialization": profile.specializations,
                "days_present": trainer_days_present,
                "total_hours": round(trainer_total_hours, 2),
                "total_sessions": trainer_total_sessions,
                "tenure_months": tenure_months,
                "first_attendance": first_attendance.isoformat() if first_attendance else None,
                "last_attendance": last_attendance.isoformat() if last_attendance else None,
                "average_hours_per_day": round(trainer_total_hours / max(trainer_days_present, 1), 2),
                "average_sessions_per_day": round(trainer_total_sessions / max(trainer_days_present, 1), 2)
            }

            trainers_data.append(trainer_data)
            gym_total_hours += trainer_total_hours
            gym_total_days += trainer_days_present
            gym_total_sessions += trainer_total_sessions

        return {
            "status": 200,
            "gym_id": gym_id,
            "gym_overall_summary": {
                "total_trainers": len(trainer_profiles),
                "total_gym_hours": round(gym_total_hours, 2),
                "total_trainer_days": gym_total_days,
                "total_sessions": gym_total_sessions,
                "average_hours_per_trainer": round(gym_total_hours / max(len(trainer_profiles), 1), 2),
                "average_days_per_trainer": round(gym_total_days / max(len(trainer_profiles), 1), 2)
            },
            "trainers": trainers_data
        }

    except Exception as e:
        raise FittbotHTTPException(
            status_code=500,
            detail="Error fetching overall gym trainer attendance",
            error_code="OVERALL_GYM_TRAINER_ATTENDANCE_ERROR",
            log_data={"gym_id": gym_id, "error": str(e)},
        )