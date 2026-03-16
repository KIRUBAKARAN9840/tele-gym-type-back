# app/routers/check_attendance_router.py

from typing import Optional, List, Dict, Any
import json
from datetime import datetime, date

from fastapi import APIRouter, Depends, Request
from pydantic import BaseModel

from sqlalchemy.orm import Session
from sqlalchemy import and_, or_, asc

from redis.asyncio import Redis

from app.models.database import get_db
from app.utils.redis_config import get_redis
from app.utils.logging_utils import FittbotHTTPException

from app.models.fittbot_models import (
    ClientGeneralAnalysis,
    GymLocation,
    Client,
    Attendance,
    ManualAttendance,
    ManualClient,
    ImportClientAttendance,
    GymImportData,
    LiveCount,
    ClientActualAggregated,
    DailyGymHourlyAgg,
    LeaderboardDaily,
    LeaderboardMonthly,
    LeaderboardOverall,
    ClientNextXp,
    RewardGym,
    RewardPrizeHistory,
    AttendanceGym,
    GymPlans,
    FittbotMuscleGroup,
)




router = APIRouter(prefix="/attendance", tags=["Client Tokens"])


@router.get("/check")
async def check_attendance_status(
    client_id: Optional[int] = None,
    gym_id: Optional[int] = None,
    manual_client_id: Optional[int] = None,
    import_client_id: Optional[int] = None,
    db: Session = Depends(get_db)
):
    try:
        if not client_id and not manual_client_id and not import_client_id:
            raise FittbotHTTPException(
                status_code=400,
                detail="client_id, manual_client_id or import_client_id is required",
                error_code="CLIENT_ID_REQUIRED",
            )
        date = datetime.now().date()
        if manual_client_id:
            record = (
                db.query(ManualAttendance)
                .filter(
                    ManualAttendance.manual_client_id == manual_client_id,
                    ManualAttendance.date == date,
                    ManualAttendance.gym_id == gym_id if gym_id is not None else ManualAttendance.gym_id,
                )
                .first()
            )

        elif import_client_id:
            record = (
                db.query(ImportClientAttendance)
                .filter(
                    ImportClientAttendance.import_client_id == import_client_id,
                    ImportClientAttendance.date == date,
                    ImportClientAttendance.gym_id == gym_id if gym_id is not None else ImportClientAttendance.gym_id,
                )
                .first()
            )

        else:
            if gym_id is not None:
                record = (
                    db.query(Attendance)
                    .filter(Attendance.client_id == client_id, Attendance.date == date, Attendance.gym_id==gym_id)
                    .first()
                )

            else:
                record = (
                    db.query(Attendance)
                    .filter(Attendance.client_id == client_id, Attendance.date == date)
                    .first()
                )


        if record and record.in_time and not record.out_time:
            attendance_status = True
        elif record and record.in_time_2 and not record.out_time_2:
            attendance_status = True
        elif record and record.in_time_3 and not record.out_time_3:
            attendance_status = True
        else:
            attendance_status = False

        return {"status": 200, "attendance_status": attendance_status}

    except FittbotHTTPException:
        # Pass through known structured errors
        raise
    except Exception as e:
        # Normalize unexpected errors to our pattern without changing logic
        raise FittbotHTTPException(
            status_code=500,
            detail="Error fetching attendance",
            error_code="ATTENDANCE_STATUS_ERROR",
            log_data={"client_id": client_id, "error": str(e)},
        )
    

@router.get("/attendance_status_with_location")
async def check_attendance_status(client_id: int, gym_id: int, db: Session = Depends(get_db)):
    try:

        print("client id",client_id)
        print("gym_id id",gym_id)
        today = datetime.now().date()
        record = db.query(Attendance).filter(
            Attendance.client_id == client_id,
            Attendance.gym_id == gym_id,
            Attendance.date == today
        ).first()

        latitude = None
        longitude = None
        if record and record.in_time and not record.out_time:
            print("hi 1")
            attendance_status = True
            in_time = record.in_time
        elif record and record.in_time_2 and not record.out_time_2:
            print("hi 2")
            attendance_status = True
            in_time = record.in_time_2
        elif record and record.in_time_3 and not record.out_time_3:
            print("hi 3")
            attendance_status = True
            in_time = record.in_time_3
        else:
            print("hi 4")
            attendance_status = False
            in_time = None

        gym_location = db.query(GymLocation).filter(GymLocation.gym_id == gym_id).first()
        if gym_location:
            latitude = float(gym_location.latitude)
            longitude = float(gym_location.longitude)

        print("attendance status", attendance_status)
        print("in_time", in_time)
        return {
            "status": 200,
            "attendance_status": attendance_status,
            "in_time": in_time,
            "gym_location": {
                "latitude": latitude,
                "longitude": longitude
            }
        }
    except FittbotHTTPException:
        raise
    except Exception as e:
        print(str(e))
        raise FittbotHTTPException(
            status_code=500,
            detail=f"Error fetching attendance:{str(e)}",
            error_code="ATTENDANCE_STATUS_WITH_LOCATION_ERROR",
            log_data={"client_id": client_id, "gym_id": gym_id, "error": str(e)},
        )


class InPunchRequest(BaseModel):
    gym_id: int
    client_id: Optional[int] = None
    manual_client_id: Optional[int] = None
    import_client_id: Optional[int] = None
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
        manual_client_id = request.manual_client_id
        import_client_id = request.import_client_id
        gym_id = request.gym_id

        if not client_id and not manual_client_id and not import_client_id:
            raise FittbotHTTPException(
                status_code=400,
                detail="client_id, manual_client_id or import_client_id is required",
                error_code="CLIENT_ID_REQUIRED",
                log_data={"gym_id": gym_id},
            )

        if manual_client_id:
            manual_client = (
                db.query(ManualClient)
                .filter(ManualClient.id == manual_client_id, ManualClient.gym_id == gym_id)
                .first()
            )
            if not manual_client:
                raise FittbotHTTPException(
                    status_code=404,
                    detail="Manual client not found for this gym",
                    error_code="MANUAL_CLIENT_NOT_FOUND",
                    log_data={"manual_client_id": manual_client_id, "gym_id": gym_id},
                )

            existing_manual = (
                db.query(ManualAttendance)
                .filter(
                    ManualAttendance.manual_client_id == manual_client_id,
                    ManualAttendance.gym_id == gym_id,
                    ManualAttendance.date == current_date,
                )
                .first()
            )

            if existing_manual and existing_manual.in_time and not existing_manual.out_time:
                raise FittbotHTTPException(
                    status_code=400,
                    detail="Already punched in.",
                    error_code="MANUAL_ALREADY_PUNCHED_IN",
                    log_data={"manual_client_id": manual_client_id, "gym_id": gym_id},
                )

            if not existing_manual:
                existing_manual = ManualAttendance(
                    manual_client_id=manual_client_id,
                    gym_id=gym_id,
                    date=current_date,
                    in_time=current_time,
                    punched_by="owner",
                )
                db.add(existing_manual)
            else:
                existing_manual.in_time = current_time
                existing_manual.out_time = None

            db.commit()

            # Invalidate gym attendance cache
            redis_key = f"gym:{gym_id}:gym_attendance"
            if await redis.exists(redis_key):
                await redis.delete(redis_key)

            attendance_cache_key = f"gym:{gym_id}:attendance:{current_date.strftime('%Y-%m-%d')}"
            if await redis.exists(attendance_cache_key):
                await redis.delete(attendance_cache_key)

            # Publish lightweight live update including manual + regular clients currently in
            present_regular = (
                db.query(Attendance, Client.name, Client.profile)
                .join(Client, Attendance.client_id == Client.client_id)
                .filter(
                    Attendance.gym_id == gym_id,
                    Attendance.date == current_date,
                    or_(
                        and_(Attendance.in_time.isnot(None), Attendance.out_time.is_(None)),
                        and_(Attendance.in_time_2.isnot(None), Attendance.out_time_2.is_(None)),
                        and_(Attendance.in_time_3.isnot(None), Attendance.out_time_3.is_(None)),
                    ),
                )
                .all()
            )
            present_manual = (
                db.query(ManualAttendance, ManualClient.name, ManualClient.dp.label("profile"))
                .join(ManualClient, ManualAttendance.manual_client_id == ManualClient.id)
                .filter(
                    ManualAttendance.gym_id == gym_id,
                    ManualAttendance.date == current_date,
                    ManualAttendance.in_time.isnot(None),
                    ManualAttendance.out_time.is_(None),
                )
                .all()
            )

            present_clients = [
                {"client_id": row.Attendance.client_id, "name": row.name, "profile": row.profile}
                for row in present_regular
            ] + [
                {
                    "client_id": f"manual_{row.ManualAttendance.manual_client_id}",
                    "manual_client_id": row.ManualAttendance.manual_client_id,
                    "name": row.name,
                    "profile": row.profile,
                }
                for row in present_manual
            ]

            message = {
                "action": "get_initial_data",
                "live_count": len(present_clients),
                "total_present": len(present_clients),
                "present_clients": present_clients,
                "goals_summary": {},
                "training_type_summary": {},
                "muscle_summary": {},
                "top_goal": "NA",
                "top_training_type": "NA",
                "top_muscle": "NA",
                "male_url": None,
                "female_url": None,
            }
            await http_request.app.state.live_hub.publish(gym_id, message)

            return {"status": 200, "message": "In-punch recorded successfully"}

        # Import client attendance flow
        if import_client_id:
            import_client = (
                db.query(GymImportData)
                .filter(GymImportData.import_id == import_client_id, GymImportData.gym_id == gym_id)
                .first()
            )
            if not import_client:
                raise FittbotHTTPException(
                    status_code=404,
                    detail="Import client not found for this gym",
                    error_code="IMPORT_CLIENT_NOT_FOUND",
                    log_data={"import_client_id": import_client_id, "gym_id": gym_id},
                )

            existing_import = (
                db.query(ImportClientAttendance)
                .filter(
                    ImportClientAttendance.import_client_id == import_client_id,
                    ImportClientAttendance.gym_id == gym_id,
                    ImportClientAttendance.date == current_date,
                )
                .first()
            )

            if existing_import and existing_import.in_time and not existing_import.out_time:
                raise FittbotHTTPException(
                    status_code=400,
                    detail="Already punched in.",
                    error_code="IMPORT_ALREADY_PUNCHED_IN",
                    log_data={"import_client_id": import_client_id, "gym_id": gym_id},
                )

            if not existing_import:
                existing_import = ImportClientAttendance(
                    import_client_id=import_client_id,
                    gym_id=gym_id,
                    date=current_date,
                    in_time=current_time,
                    punched_by="owner",
                )
                db.add(existing_import)
            else:
                existing_import.in_time = current_time
                existing_import.out_time = None

            db.commit()

            # Invalidate gym attendance cache
            redis_key = f"gym:{gym_id}:gym_attendance"
            if await redis.exists(redis_key):
                await redis.delete(redis_key)

            attendance_cache_key = f"gym:{gym_id}:attendance:{current_date.strftime('%Y-%m-%d')}"
            if await redis.exists(attendance_cache_key):
                await redis.delete(attendance_cache_key)

            # Publish lightweight live update including import + manual + regular clients currently in
            present_regular = (
                db.query(Attendance, Client.name, Client.profile)
                .join(Client, Attendance.client_id == Client.client_id)
                .filter(
                    Attendance.gym_id == gym_id,
                    Attendance.date == current_date,
                    or_(
                        and_(Attendance.in_time.isnot(None), Attendance.out_time.is_(None)),
                        and_(Attendance.in_time_2.isnot(None), Attendance.out_time_2.is_(None)),
                        and_(Attendance.in_time_3.isnot(None), Attendance.out_time_3.is_(None)),
                    ),
                )
                .all()
            )
            present_manual = (
                db.query(ManualAttendance, ManualClient.name, ManualClient.dp.label("profile"))
                .join(ManualClient, ManualAttendance.manual_client_id == ManualClient.id)
                .filter(
                    ManualAttendance.gym_id == gym_id,
                    ManualAttendance.date == current_date,
                    ManualAttendance.in_time.isnot(None),
                    ManualAttendance.out_time.is_(None),
                )
                .all()
            )
            present_import = (
                db.query(ImportClientAttendance, GymImportData.client_name.label("name"))
                .join(GymImportData, ImportClientAttendance.import_client_id == GymImportData.import_id)
                .filter(
                    ImportClientAttendance.gym_id == gym_id,
                    ImportClientAttendance.date == current_date,
                    ImportClientAttendance.in_time.isnot(None),
                    ImportClientAttendance.out_time.is_(None),
                )
                .all()
            )

            present_clients = [
                {"client_id": row.Attendance.client_id, "name": row.name, "profile": row.profile}
                for row in present_regular
            ] + [
                {
                    "client_id": f"manual_{row.ManualAttendance.manual_client_id}",
                    "manual_client_id": row.ManualAttendance.manual_client_id,
                    "name": row.name,
                    "profile": row.profile,
                }
                for row in present_manual
            ] + [
                {
                    "client_id": f"import_{row.ImportClientAttendance.import_client_id}",
                    "import_client_id": row.ImportClientAttendance.import_client_id,
                    "name": row.name,
                    "profile": None,
                }
                for row in present_import
            ]

            message = {
                "action": "get_initial_data",
                "live_count": len(present_clients),
                "total_present": len(present_clients),
                "present_clients": present_clients,
                "goals_summary": {},
                "training_type_summary": {},
                "muscle_summary": {},
                "top_goal": "NA",
                "top_training_type": "NA",
                "top_muscle": "NA",
                "male_url": None,
                "female_url": None,
            }
            await http_request.app.state.live_hub.publish(gym_id, message)

            return {"status": 200, "message": "In-punch recorded successfully"}

        # Validate that client belongs to this gym
        client = db.query(Client).filter(Client.client_id == client_id).first()
        if not client:
            raise FittbotHTTPException(
                status_code=404,
                detail="Client not found",
                error_code="CLIENT_NOT_FOUND",
                log_data={"client_id": client_id},
            )

        if client.gym_id != gym_id:
            raise FittbotHTTPException(
                status_code=403,
                detail="Client is Inactive dont punch in",
                error_code="CLIENT_INACTIVE_WRONG_GYM",
                log_data={"client_id": client_id, "client_gym_id": client.gym_id, "requested_gym_id": gym_id},
            )

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
            # Multiple session logic
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
            points = 0  # No points for subsequent sessions

        else:
            # First session today
            new_attendance = Attendance(
                gym_id=request.gym_id,
                client_id=request.client_id,
                date=current_date,
                in_time=current_time,
                muscle=request.muscle,
            )
            db.add(new_attendance)
            db.commit()

            # Update daily hourly aggregation
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

            # Reward points & leaderboards for first punch-in
            points = 50
            today = date.today()

            # Daily
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

            # Monthly
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

            # Overall & next tier logic
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

            # Update monthly analysis attendance
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

            # Gym attendance counter - ONLY for first punch-in
            gym_attendance = (
                db.query(AttendanceGym)
                .filter(AttendanceGym.gym_id == request.gym_id, AttendanceGym.date == current_date)
                .first()
            )
            if gym_attendance:
                gym_attendance.attendance_count += 1
            else:
                gym_attendance = AttendanceGym(gym_id=gym_id, date=current_date, attendance_count=1)
                db.add(gym_attendance)
            db.commit()

        # Invalidate caches
        today = date.today()
        attendance_key = f"gym:{request.gym_id}:attendance:{today.strftime('%Y-%m-%d')}"
        analytics_key = f"gym:{request.gym_id}:analytics"
        client_analytics_key = f"gym:{request.gym_id}:client_analytics"
        daily_hourlyagg_key = f"gym:{request.gym_id}:daily_hourlyagg:{today.strftime('%Y-%m-%d')}"
        if await redis.exists(attendance_key):
            await redis.delete(attendance_key)
        if await redis.exists(analytics_key):
            await redis.delete(analytics_key)
        if await redis.exists(client_analytics_key):
            await redis.delete(client_analytics_key)
        if await redis.exists(daily_hourlyagg_key):
            await redis.delete(daily_hourlyagg_key)

        # Live count
        gym_count_record = db.query(LiveCount).filter(LiveCount.gym_id == request.gym_id).first()
        if not gym_count_record:
            gym_count_record = LiveCount(gym_id=request.gym_id, count=0)
            db.add(gym_count_record)
            db.commit()
            db.refresh(gym_count_record)
        gym_count_record.count += 1
        db.commit()
        db.refresh(gym_count_record)

        # Build live summary for websocket broadcast
        current_clients = (
            db.query(
                Attendance.client_id,
                Attendance.in_time,
                Attendance.in_time_2,
                Attendance.in_time_3,
                Attendance.out_time,
                Attendance.out_time_2,
                Attendance.out_time_3,
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
                Attendance.gym_id == gym_id,
                or_(
                    and_(Attendance.in_time.isnot(None), Attendance.out_time.is_(None)),
                    and_(Attendance.in_time_2.isnot(None), Attendance.out_time_2.is_(None)),
                    and_(Attendance.in_time_3.isnot(None), Attendance.out_time_3.is_(None)),
                ),
            )
            .all()
        )

        manual_present = (
            db.query(
                ManualAttendance.manual_client_id,
                ManualClient.name,
                ManualClient.dp.label("profile"),
            )
            .join(ManualClient, ManualAttendance.manual_client_id == ManualClient.id)
            .filter(
                ManualAttendance.date == current_date,
                ManualAttendance.gym_id == request.gym_id,
                ManualAttendance.in_time.isnot(None),
                ManualAttendance.out_time.is_(None),
            )
            .all()
        )

        goals_summary: Dict[str, Dict[str, Any]] = {}
        training_type_summary: Dict[str, Dict[str, Any]] = {}
        muscle_summary: Dict[str, Dict[str, Any]] = {}
        present_clients: List[Dict[str, Any]] = []

        for c in current_clients:
            # goal summary
            goal_key = c.goals or "Unknown"
            goals_summary.setdefault(goal_key, {"count": 0, "clients": []})
            goals_summary[goal_key]["count"] += 1
            goals_summary[goal_key]["clients"].append(c.name)

            # training summary
            training_type = db.query(GymPlans.plans).filter(GymPlans.id == c.training_id).scalar()
            training_key = training_type or "Unknown"
            training_type_summary.setdefault(training_key, {"count": 0, "clients": []})
            training_type_summary[training_key]["count"] += 1
            training_type_summary[training_key]["clients"].append(c.name)

            # Determine which punch-in is active and use the corresponding muscle group
            # Check from latest to earliest to find the active (not punched out) session
            active_muscle = None
            if c.in_time_3 is not None and c.out_time_3 is None:
                # Third punch-in is active
                active_muscle = c.muscle_3
            elif c.in_time_2 is not None and c.out_time_2 is None:
                # Second punch-in is active
                active_muscle = c.muscle_2
            elif c.in_time is not None and c.out_time is None:
                # First punch-in is active
                active_muscle = c.muscle

            if active_muscle:
                for muscle in active_muscle:
                    muscle_summary.setdefault(muscle, {"count": 0, "clients": []})
                    muscle_summary[muscle]["count"] += 1
                    muscle_summary[muscle]["clients"].append(c.name)

            present_clients.append(
                {
                    "client_id": c.client_id,
                    "name": c.name,
                    "profile": c.profile,
                }
            )

        # Include manual attendance in live view
        for m in manual_present:
            present_clients.append(
                {
                    "client_id": f"manual_{m.manual_client_id}",
                    "manual_client_id": m.manual_client_id,
                    "name": m.name,
                    "profile": m.profile,
                }
            )

        # top categories
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
            "live_count": len(current_clients) + len(manual_present),
            "total_present": len(current_clients) + len(manual_present),
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

        # Invalidate gym attendance cache
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
    client_id: Optional[int] = None
    manual_client_id: Optional[int] = None
    import_client_id: Optional[int] = None



@router.post("/out_punch")
async def out_punch(
    http_request: Request,
    request: OutPunchRequest,
    db: Session = Depends(get_db),
    redis: Redis = Depends(get_redis),
):
    try:
        current_date = date.today()
        current_time = datetime.now().time()

        if not request.client_id and not request.manual_client_id and not request.import_client_id:
            raise FittbotHTTPException(
                status_code=400,
                detail="client_id, manual_client_id or import_client_id is required",
                error_code="CLIENT_ID_REQUIRED",
                log_data={"gym_id": request.gym_id},
            )

        if request.manual_client_id:
            manual_record = (
                db.query(ManualAttendance)
                .filter(
                    ManualAttendance.manual_client_id == request.manual_client_id,
                    ManualAttendance.gym_id == request.gym_id,
                    ManualAttendance.date == current_date,
                )
                .first()
            )

            if not manual_record or not manual_record.in_time or manual_record.out_time:
                raise FittbotHTTPException(
                    status_code=404,
                    detail="No in-punch record found for today, or client has already punched out.",
                    error_code="OUT_PUNCH_NOT_FOUND",
                    log_data={
                        "manual_client_id": request.manual_client_id,
                        "gym_id": request.gym_id,
                        "date": str(current_date),
                    },
                )

            manual_record.out_time = current_time
            db.commit()

            redis_key = f"gym:{request.gym_id}:gym_attendance"
            if await redis.exists(redis_key):
                await redis.delete(redis_key)

            attendance_cache_key = f"gym:{request.gym_id}:attendance:{current_date.strftime('%Y-%m-%d')}"
            if await redis.exists(attendance_cache_key):
                await redis.delete(attendance_cache_key)

            # Publish live update for manual client out-punch
            present_regular = (
                db.query(
                    Attendance.client_id,
                    Attendance.in_time,
                    Attendance.in_time_2,
                    Attendance.in_time_3,
                    Attendance.out_time,
                    Attendance.out_time_2,
                    Attendance.out_time_3,
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
                    Attendance.date == current_date,
                    Attendance.gym_id == request.gym_id,
                    or_(
                        and_(Attendance.in_time.isnot(None), Attendance.out_time.is_(None)),
                        and_(Attendance.in_time_2.isnot(None), Attendance.out_time_2.is_(None)),
                        and_(Attendance.in_time_3.isnot(None), Attendance.out_time_3.is_(None)),
                    ),
                )
                .all()
            )
            present_manual = (
                db.query(ManualAttendance, ManualClient.name, ManualClient.dp.label("profile"), ManualClient.goal)
                .join(ManualClient, ManualAttendance.manual_client_id == ManualClient.id)
                .filter(
                    ManualAttendance.gym_id == request.gym_id,
                    ManualAttendance.date == current_date,
                    ManualAttendance.in_time.isnot(None),
                    ManualAttendance.out_time.is_(None),
                )
                .all()
            )

            goals_summary: Dict[str, Dict[str, Any]] = {}
            training_type_summary: Dict[str, Dict[str, Any]] = {}
            muscle_summary: Dict[str, Dict[str, Any]] = {}
            present_clients: List[Dict[str, Any]] = []

            for c in present_regular:
                goal_key = c.goals or "Unknown"
                goals_summary.setdefault(goal_key, {"count": 0, "clients": []})
                goals_summary[goal_key]["count"] += 1
                goals_summary[goal_key]["clients"].append(c.name)

                training_type = db.query(GymPlans.plans).filter(GymPlans.id == c.training_id).scalar()
                training_key = training_type or "Unknown"
                training_type_summary.setdefault(training_key, {"count": 0, "clients": []})
                training_type_summary[training_key]["count"] += 1
                training_type_summary[training_key]["clients"].append(c.name)

                active_muscle = None
                if c.in_time_3 is not None and c.out_time_3 is None:
                    active_muscle = c.muscle_3
                elif c.in_time_2 is not None and c.out_time_2 is None:
                    active_muscle = c.muscle_2
                elif c.in_time is not None and c.out_time is None:
                    active_muscle = c.muscle

                if active_muscle:
                    for muscle in active_muscle:
                        muscle_summary.setdefault(muscle, {"count": 0, "clients": []})
                        muscle_summary[muscle]["count"] += 1
                        muscle_summary[muscle]["clients"].append(c.name)

                present_clients.append({"client_id": c.client_id, "name": c.name, "profile": c.profile})

            for m in present_manual:
                if m.goal:
                    goals_summary.setdefault(m.goal, {"count": 0, "clients": []})
                    goals_summary[m.goal]["count"] += 1
                    goals_summary[m.goal]["clients"].append(m.name)

                present_clients.append({
                    "client_id": f"manual_{m.ManualAttendance.manual_client_id}",
                    "manual_client_id": m.ManualAttendance.manual_client_id,
                    "name": m.name,
                    "profile": m.profile,
                })

            top_goal = max(goals_summary.items(), key=lambda x: x[1]["count"], default=(None, {}))[0]
            top_training_type = max(training_type_summary.items(), key=lambda x: x[1]["count"], default=(None, {}))[0]
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
                "live_count": len(present_clients),
                "total_present": len(present_clients),
                "present_clients": present_clients,
                "goals_summary": goals_summary,
                "training_type_summary": training_type_summary,
                "muscle_summary": muscle_summary,
                "top_goal": top_goal,
                "top_training_type": top_training_type,
                "top_muscle": top_muscle,
                "male_url": male_url,
                "female_url": female_url,
            }
            await http_request.app.state.live_hub.publish(request.gym_id, message)

            in_dt = datetime.combine(current_date, manual_record.in_time)
            return {
                "status": 200,
                "message": "Punch-out recorded successfully.",
                "time_spent": (datetime.combine(current_date, manual_record.out_time) - in_dt).seconds,
            }

        # Import client out-punch flow
        if request.import_client_id:
            import_record = (
                db.query(ImportClientAttendance)
                .filter(
                    ImportClientAttendance.import_client_id == request.import_client_id,
                    ImportClientAttendance.gym_id == request.gym_id,
                    ImportClientAttendance.date == current_date,
                )
                .first()
            )

            if not import_record or not import_record.in_time or import_record.out_time:
                raise FittbotHTTPException(
                    status_code=404,
                    detail="No in-punch record found for today, or client has already punched out.",
                    error_code="OUT_PUNCH_NOT_FOUND",
                    log_data={
                        "import_client_id": request.import_client_id,
                        "gym_id": request.gym_id,
                        "date": str(current_date),
                    },
                )

            import_record.out_time = current_time
            db.commit()

            redis_key = f"gym:{request.gym_id}:gym_attendance"
            if await redis.exists(redis_key):
                await redis.delete(redis_key)

            attendance_cache_key = f"gym:{request.gym_id}:attendance:{current_date.strftime('%Y-%m-%d')}"
            if await redis.exists(attendance_cache_key):
                await redis.delete(attendance_cache_key)

            # Publish live update for import client out-punch
            present_regular = (
                db.query(
                    Attendance.client_id,
                    Attendance.in_time,
                    Attendance.in_time_2,
                    Attendance.in_time_3,
                    Attendance.out_time,
                    Attendance.out_time_2,
                    Attendance.out_time_3,
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
                    Attendance.date == current_date,
                    Attendance.gym_id == request.gym_id,
                    or_(
                        and_(Attendance.in_time.isnot(None), Attendance.out_time.is_(None)),
                        and_(Attendance.in_time_2.isnot(None), Attendance.out_time_2.is_(None)),
                        and_(Attendance.in_time_3.isnot(None), Attendance.out_time_3.is_(None)),
                    ),
                )
                .all()
            )
            present_manual = (
                db.query(ManualAttendance, ManualClient.name, ManualClient.dp.label("profile"), ManualClient.goal)
                .join(ManualClient, ManualAttendance.manual_client_id == ManualClient.id)
                .filter(
                    ManualAttendance.gym_id == request.gym_id,
                    ManualAttendance.date == current_date,
                    ManualAttendance.in_time.isnot(None),
                    ManualAttendance.out_time.is_(None),
                )
                .all()
            )
            present_import = (
                db.query(ImportClientAttendance, GymImportData.client_name.label("name"))
                .join(GymImportData, ImportClientAttendance.import_client_id == GymImportData.import_id)
                .filter(
                    ImportClientAttendance.gym_id == request.gym_id,
                    ImportClientAttendance.date == current_date,
                    ImportClientAttendance.in_time.isnot(None),
                    ImportClientAttendance.out_time.is_(None),
                )
                .all()
            )

            goals_summary: Dict[str, Dict[str, Any]] = {}
            training_type_summary: Dict[str, Dict[str, Any]] = {}
            muscle_summary: Dict[str, Dict[str, Any]] = {}
            present_clients: List[Dict[str, Any]] = []

            for c in present_regular:
                goal_key = c.goals or "Unknown"
                goals_summary.setdefault(goal_key, {"count": 0, "clients": []})
                goals_summary[goal_key]["count"] += 1
                goals_summary[goal_key]["clients"].append(c.name)

                training_type = db.query(GymPlans.plans).filter(GymPlans.id == c.training_id).scalar()
                training_key = training_type or "Unknown"
                training_type_summary.setdefault(training_key, {"count": 0, "clients": []})
                training_type_summary[training_key]["count"] += 1
                training_type_summary[training_key]["clients"].append(c.name)

                active_muscle = None
                if c.in_time_3 is not None and c.out_time_3 is None:
                    active_muscle = c.muscle_3
                elif c.in_time_2 is not None and c.out_time_2 is None:
                    active_muscle = c.muscle_2
                elif c.in_time is not None and c.out_time is None:
                    active_muscle = c.muscle

                if active_muscle:
                    for muscle in active_muscle:
                        muscle_summary.setdefault(muscle, {"count": 0, "clients": []})
                        muscle_summary[muscle]["count"] += 1
                        muscle_summary[muscle]["clients"].append(c.name)

                present_clients.append({"client_id": c.client_id, "name": c.name, "profile": c.profile})

            for m in present_manual:
                if m.goal:
                    goals_summary.setdefault(m.goal, {"count": 0, "clients": []})
                    goals_summary[m.goal]["count"] += 1
                    goals_summary[m.goal]["clients"].append(m.name)

                present_clients.append({
                    "client_id": f"manual_{m.ManualAttendance.manual_client_id}",
                    "manual_client_id": m.ManualAttendance.manual_client_id,
                    "name": m.name,
                    "profile": m.profile,
                })

            for i in present_import:
                present_clients.append({
                    "client_id": f"import_{i.ImportClientAttendance.import_client_id}",
                    "import_client_id": i.ImportClientAttendance.import_client_id,
                    "name": i.name,
                    "profile": None,
                })

            top_goal = max(goals_summary.items(), key=lambda x: x[1]["count"], default=(None, {}))[0]
            top_training_type = max(training_type_summary.items(), key=lambda x: x[1]["count"], default=(None, {}))[0]
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
                "live_count": len(present_clients),
                "total_present": len(present_clients),
                "present_clients": present_clients,
                "goals_summary": goals_summary,
                "training_type_summary": training_type_summary,
                "muscle_summary": muscle_summary,
                "top_goal": top_goal,
                "top_training_type": top_training_type,
                "top_muscle": top_muscle,
                "male_url": male_url,
                "female_url": female_url,
            }
            await http_request.app.state.live_hub.publish(request.gym_id, message)

            in_dt = datetime.combine(current_date, import_record.in_time)
            return {
                "status": 200,
                "message": "Punch-out recorded successfully.",
                "time_spent": (datetime.combine(current_date, import_record.out_time) - in_dt).seconds,
            }

        attendance_record = db.query(Attendance).filter(
            Attendance.client_id == request.client_id,
            Attendance.gym_id == request.gym_id,
            Attendance.date == current_date
        ).first()

        if not attendance_record:
            raise FittbotHTTPException(
                status_code=404,
                detail="No in-punch record found for today, or client has already punched out.",
                error_code="OUT_PUNCH_NOT_FOUND",
                log_data={"client_id": request.client_id, "gym_id": request.gym_id, "date": str(current_date)},
            )

        today = date.today()

        if attendance_record.out_time is None:
            attendance_record.out_time = current_time
            in_dt = datetime.combine(today, attendance_record.in_time)
            print("hi1")

        elif attendance_record.in_time_2 and attendance_record.out_time_2 is None:
            attendance_record.out_time_2 = current_time
            in_dt = datetime.combine(today, attendance_record.in_time_2)
            print("hi2")

        elif attendance_record.in_time_3 and attendance_record.out_time_3 is None:
            attendance_record.out_time_3 = current_time
            in_dt = datetime.combine(today, attendance_record.in_time_3)
            print("hi3")

        else:
            raise FittbotHTTPException(
                status_code=400,
                detail="All three sessions already punched out.",
                error_code="ALL_SESSIONS_PUNCHED_OUT",
                log_data={"client_id": request.client_id, "gym_id": request.gym_id},
            )

        db.commit()

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
            raise FittbotHTTPException(
                status_code=404,
                detail="Gym live count not found.",
                error_code="GYM_LIVE_COUNT_NOT_FOUND",
                log_data={"gym_id": request.gym_id},
            )

        if gym_count_record.count > 0:
            gym_count_record.count -= 1
        db.commit()
        db.refresh(gym_count_record)
        print("live count is", gym_count_record.count)

        current_clients = (
            db.query(
                Attendance.client_id,
                Attendance.in_time,
                Attendance.in_time_2,
                Attendance.in_time_3,
                Attendance.out_time,
                Attendance.out_time_2,
                Attendance.out_time_3,
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
                Attendance.gym_id == request.gym_id,
                or_(
                    and_(Attendance.in_time.isnot(None), Attendance.out_time.is_(None)),
                    and_(Attendance.in_time_2.isnot(None), Attendance.out_time_2.is_(None)),
                    and_(Attendance.in_time_3.isnot(None), Attendance.out_time_3.is_(None)),
                ),
            )
            .all()
        )

        manual_present = (
            db.query(
                ManualAttendance.manual_client_id,
                ManualClient.name,
                ManualClient.dp.label("profile"),
            )
            .join(ManualClient, ManualAttendance.manual_client_id == ManualClient.id)
            .filter(
                ManualAttendance.date == date.today(),
                ManualAttendance.gym_id == request.gym_id,
                ManualAttendance.in_time.isnot(None),
                ManualAttendance.out_time.is_(None),
            )
            .all()
        )

        goals_summary = {}
        training_type_summary = {}
        muscle_summary = {}
        present_clients = []

        for client in current_clients:
            goal_key = client.goals or "Unknown"
            goals_summary.setdefault(goal_key, {"count": 0, "clients": []})
            goals_summary[goal_key]["count"] += 1
            goals_summary[goal_key]["clients"].append(client.name)

            training_type = db.query(GymPlans.plans).filter(GymPlans.id == client.training_id).scalar()
            training_key = training_type or "Unknown"
            training_type_summary.setdefault(training_key, {"count": 0, "clients": []})
            training_type_summary[training_key]["count"] += 1
            training_type_summary[training_key]["clients"].append(client.name)

            # Determine which punch-in is active and use the corresponding muscle group
            # Check from latest to earliest to find the active (not punched out) session
            active_muscle = None
            if client.in_time_3 is not None and client.out_time_3 is None:
                # Third punch-in is active
                active_muscle = client.muscle_3
            elif client.in_time_2 is not None and client.out_time_2 is None:
                # Second punch-in is active
                active_muscle = client.muscle_2
            elif client.in_time is not None and client.out_time is None:
                # First punch-in is active
                active_muscle = client.muscle

            if active_muscle:
                for muscle in active_muscle:
                    muscle_summary.setdefault(muscle, {"count": 0, "clients": []})
                    muscle_summary[muscle]["count"] += 1
                    muscle_summary[muscle]["clients"].append(client.name)

            present_clients.append({"client_id": client.client_id, "name": client.name, "profile": client.profile})

        # Append manual present clients
        for m in manual_present:
            present_clients.append(
                {
                    "client_id": f"manual_{m.manual_client_id}",
                    "manual_client_id": m.manual_client_id,
                    "name": m.name,
                    "profile": m.profile,
                }
            )

        top_goal = max(goals_summary.items(), key=lambda x: x[1]["count"], default=(None, {}))[0]
        top_training_type = max(training_type_summary.items(), key=lambda x: x[1]["count"], default=(None, {}))[0]
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
            "live_count": len(current_clients) + len(manual_present),
            "total_present": len(current_clients) + len(manual_present),
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
        current_time = datetime.now().time()
        out_dt = datetime.combine(date.today(), current_time)
        print("In date is", in_dt)
        duration_minutes = (out_dt - in_dt).total_seconds() / 60

        current_year = today.year
        agg = (
            db.query(ClientActualAggregated)
            .filter(
                ClientActualAggregated.client_id == request.client_id,
                ClientActualAggregated.year == current_year,
            )
            .first()
        )

      
        if agg:
            if agg.gym_time:
                print("agg.gym_time", agg.gym_time)
                print("duration_minutes", duration_minutes)
                agg.gym_time = int((agg.gym_time + duration_minutes) / 2)
               
            else:
                agg.gym_time = int(duration_minutes)
        else:
            agg = ClientActualAggregated(
                client_id=request.client_id,
                year=current_year,
                gym_time=int(duration_minutes),
                created_at=datetime.now(),
                updated_at=datetime.now(),
            )
            db.add(agg)
           
        db.commit()

        return {"status": 200, "message": "Out-punch recorded successfully", "data": current_time}

    except FittbotHTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise FittbotHTTPException(
            status_code=500,
            detail="An unexpected error occurred during out-punch",
            error_code="OUT_PUNCH_ERROR",
            log_data={"exc": repr(e), "client_id": request.client_id, "gym_id": request.gym_id},
        )
