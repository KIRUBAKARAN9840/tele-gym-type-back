# app/routers/owner_home.py

import json
from datetime import datetime, date, timedelta
from typing import Dict, Any

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from redis.asyncio import Redis
from sqlalchemy import desc, or_, exists, func, select, update, cast, String
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.async_database import get_async_db
from app.utils.redis_config import get_redis
from app.utils.logging_utils import FittbotHTTPException
from app.models.fittbot_models import (
    Client,
    Attendance,
    ManualAttendance,
    ManualClient,
    AttendanceGym,
    TrainerProfile,
    TrainerAttendance,
    AboutToExpire,
    GymEnquiry,
    OldGymData,
    GymImportData,
    FittbotGymMembership,
    ClientGym,
    GymPlans,
    OwnerHomePoster,
    NoCostEmi,
    SessionSetting,
    AccountDetails,
    Gym,
    GymVerificationDocument,
    GymOnboardingPics,
    GymPrefilledAgreement,
    ManualPoster,
    ManualClient,
    SessionBookingDay,
    GymLocation,
)
from app.models.dailypass_models import DailyPassDay, DailyPassPricing

router = APIRouter(prefix="/owner/home", tags=["Gymowner"])


class GymHomeRequest(BaseModel):
    gym_id: int


async def get_attendance_data(
    gym_id: int, response: Dict[str, Any], redis: Redis, db: AsyncSession
) -> Dict[str, Any]:
    today = date.today()

    attendance_key = f"gym:{gym_id}:attendance:{today.strftime('%Y-%m-%d')}"
    attendance_data = await redis.hgetall(attendance_key)
    
    if not attendance_data:
        stmt = (
            select(
                Attendance.client_id,
                Attendance.in_time,
                Attendance.out_time,
                Attendance.in_time_2,
                Attendance.out_time_2,
                Attendance.in_time_3,
                Attendance.out_time_3,
                Attendance.muscle,
                Attendance.muscle_2,
                Attendance.muscle_3,
                Client.name,
                Client.profile,
            )
            .select_from(Attendance)
            .join(Client, Attendance.client_id == Client.client_id)
            .where(Attendance.date == today, Attendance.gym_id == gym_id)
        )
        result = await db.execute(stmt)
        attended_clients = result.all()

        # Manual attendance records
        manual_stmt = (
            select(
                ManualAttendance.manual_client_id,
                ManualAttendance.in_time,
                ManualAttendance.out_time,
                ManualClient.name,
                ManualClient.dp.label("profile"),
            )
            .select_from(ManualAttendance)
            .join(ManualClient, ManualAttendance.manual_client_id == ManualClient.id)
            .where(ManualAttendance.date == today, ManualAttendance.gym_id == gym_id)
        )
        manual_result = await db.execute(manual_stmt)
        manual_clients = manual_result.all()

        attendance_details = []
        for c in attended_clients:
            sessions = [
                {
                    "in_time": c.in_time.strftime("%H:%M") if c.in_time else None,
                    "out_time": c.out_time.strftime("%H:%M") if c.out_time else None,
                    "muscle": c.muscle,
                },
                {
                    "in_time": c.in_time_2.strftime("%H:%M") if c.in_time_2 else None,
                    "out_time": c.out_time_2.strftime("%H:%M") if c.out_time_2 else None,
                    "muscle": c.muscle_2,
                },
                {
                    "in_time": c.in_time_3.strftime("%H:%M") if c.in_time_3 else None,
                    "out_time": c.out_time_3.strftime("%H:%M") if c.out_time_3 else None,
                    "muscle": c.muscle_3,
                },
            ]
            sessions = [s for s in sessions if s["in_time"] or s["out_time"]]
            attendance_details.append(
                {
                    "client_id": c.client_id,
                    "name": c.name,
                    "profile_pic": c.profile,
                    "sessions": sessions
                }
            )

        for m in manual_clients:
            attendance_details.append(
                {
                    "client_id": f"manual_{m.manual_client_id}",
                    "name": m.name,
                    "profile_pic": m.profile,
                    "sessions": [
                        {
                            "in_time": m.in_time.strftime("%H:%M") if m.in_time else None,
                            "out_time": m.out_time.strftime("%H:%M") if m.out_time else None,
                            "muscle": None,
                        }
                    ],
                }
            )

        current_count = len(attended_clients) + len(manual_clients)

        # Count from FittbotGymMembership
        expected_count_result = await db.execute(
            select(func.count(func.distinct(FittbotGymMembership.client_id)))
            .where(
                FittbotGymMembership.gym_id == str(gym_id),
                FittbotGymMembership.expires_at >= today,
            )
        )
        expected_membership_count = expected_count_result.scalar() or 0

        # Include manual clients who have active status but no active membership record
        manual_expected_result = await db.execute(
            select(func.count()).select_from(ManualClient).where(
                ManualClient.gym_id == gym_id,
                ManualClient.status == "active",
                or_(ManualClient.expires_at == None, ManualClient.expires_at >= today),
                ~exists(
                    select(FittbotGymMembership.id).where(
                        FittbotGymMembership.gym_id == str(gym_id),
                        FittbotGymMembership.client_id == func.concat(
                            "manual_", cast(ManualClient.id, String)
                        ),
                        FittbotGymMembership.expires_at >= today
                    )
                )
            )
        )
        manual_expected = manual_expected_result.scalar() or 0
        expected_count = int(expected_membership_count) + int(manual_expected)

        await redis.hset(
            attendance_key,
            mapping={
                "current_count": current_count,
                "expected_count": expected_count,
                "details": json.dumps(attendance_details),
            },
        )
        await redis.expire(attendance_key, 86400)
    else:
        # Convert bytes → proper types
        try:
            details_raw = attendance_data.get("details")
            attendance_details = json.loads(details_raw) if details_raw else []
        except Exception:
            attendance_details = []
        current_count = int(attendance_data.get("current_count", 0))
        expected_count = int(attendance_data.get("expected_count", 0))

    response["attendance"] = {
        "current_count": current_count,
        "expected_count": expected_count,
        "details": attendance_details,
    }
    return response


async def get_count_data(
    gym_id: int, response: Dict[str, Any], redis: Redis, db: AsyncSession
) -> Dict[str, Any]:
    members_key = f"gym:{gym_id}:members"
    members_data = await redis.hgetall(members_key)

    if not members_data:
        today = date.today()

        members_result = await db.execute(
            select(func.count()).select_from(Client).where(Client.gym_id == gym_id)
        )
        members = members_result.scalar() or 0

        old_members_result = await db.execute(
            select(func.count()).select_from(OldGymData).where(OldGymData.gym_id == gym_id)
        )
        old_members = old_members_result.scalar() or 0

        imported_members_result = await db.execute(
            select(func.count()).select_from(GymImportData).where(GymImportData.gym_id == gym_id)
        )
        imported_members = imported_members_result.scalar() or 0

        manual_members_result = await db.execute(
            select(func.count()).select_from(ManualClient).where(ManualClient.gym_id == gym_id)
        )
        manual_members = manual_members_result.scalar() or 0

        total_members = members + old_members + imported_members + manual_members


        active_members_result = await db.execute(
            select(func.count(func.distinct(FittbotGymMembership.client_id)))
            .where(
                FittbotGymMembership.gym_id == str(gym_id),
                FittbotGymMembership.expires_at >= today
            )
        )
        active_membership_count = active_members_result.scalar() or 0

        # Include manual clients who have active status but no active membership record
        manual_active_result = await db.execute(
            select(func.count()).select_from(ManualClient).where(
                ManualClient.gym_id == gym_id,
                ManualClient.status == "active",
                or_(ManualClient.expires_at == None, ManualClient.expires_at >= today),
                ~exists(
                    select(FittbotGymMembership.id).where(
                        FittbotGymMembership.gym_id == str(gym_id),
                        FittbotGymMembership.client_id == func.concat(
                            "manual_", cast(ManualClient.id, String)
                        ),
                        FittbotGymMembership.expires_at >= today
                    )
                )
            )
        )
        manual_active = manual_active_result.scalar() or 0
        active_members = active_membership_count + manual_active

        total_trainers_result = await db.execute(
            select(func.count()).select_from(TrainerProfile).where(TrainerProfile.gym_id == gym_id)
        )
        total_trainers = total_trainers_result.scalar() or 0

        total_pending_result = await db.execute(
            select(func.count()).select_from(GymEnquiry)
            .where(
                GymEnquiry.gym_id == gym_id,
                or_(GymEnquiry.status == "Pending", GymEnquiry.status == "Follow Up"),
            )
        )
        total_pending = total_pending_result.scalar() or 0

        await redis.hset(
            members_key,
            mapping={
                "total_members": int(total_members),
                "active_members": int(active_members),
                "total_trainers": int(total_trainers),
                "total_pending_enquiries": int(total_pending),
            },
        )
        await redis.expire(members_key, 86400)

        members_payload = {
            "total_members": total_members,
            "active_members": active_members,
            "total_trainers": total_trainers,
            "total_pending_enquiries": total_pending,
        }
    else:
        members_payload = {
            "total_members": int(members_data.get("total_members", 0)),
            "active_members": int(members_data.get("active_members", 0)),
            "total_trainers": int(members_data.get("total_trainers", 0)),
            "total_pending_enquiries": int(
                members_data.get("total_pending_enquiries", 0)
            ),
        }

    response["members"] = members_payload
    return response


async def get_booking_payment_counts(
    gym_id: int, response: Dict[str, Any], redis: Redis, db: AsyncSession
) -> Dict[str, Any]:
    """Aggregate booking/payment related counts for the home screen."""
    cache_key = f"gym:{gym_id}:home_booking_counts:v2"
    cached = await redis.get(cache_key)

    if cached:
        try:
            counts = json.loads(cached)
        except Exception:
            counts = None
    else:
        counts = None

    if counts is None:
        target_date = date.today()

        dailypass_result = await db.execute(
            select(func.count())
            .select_from(DailyPassDay)
            .where(
                DailyPassDay.gym_id == gym_id,
                DailyPassDay.scheduled_date == target_date,
                DailyPassDay.status.in_(["scheduled", "attended", "available"]),
            )
        )
        dailypass_count = dailypass_result.scalar() or 0

        pt_result = await db.execute(
            select(func.count())
            .select_from(SessionBookingDay)
            .where(
                SessionBookingDay.gym_id == gym_id,
                SessionBookingDay.session_id == 2,
                SessionBookingDay.booking_date == target_date,
                SessionBookingDay.status.in_(["booked", "attended"]),
            )
        )
        pt_sessions_count = pt_result.scalar() or 0

        other_sessions_result = await db.execute(
            select(func.count())
            .select_from(SessionBookingDay)
            .where(
                SessionBookingDay.gym_id == gym_id,
                SessionBookingDay.session_id > 2,
                SessionBookingDay.booking_date == target_date,
                SessionBookingDay.status.in_(["booked", "attended"]),
            )
        )
        other_sessions_count = other_sessions_result.scalar() or 0

        membership_result = await db.execute(
            select(func.count())
            .select_from(FittbotGymMembership)
            .where(
                FittbotGymMembership.gym_id == str(gym_id),
                FittbotGymMembership.type=="gym_membership",
                FittbotGymMembership.status.in_(["upcoming", "active"]),
                FittbotGymMembership.purchased_at >= datetime.combine(target_date, datetime.min.time()),
                FittbotGymMembership.purchased_at < datetime.combine(target_date + timedelta(days=1), datetime.min.time()),
            )
        )
        membership_count = membership_result.scalar() or 0

        all_sessions_count = pt_sessions_count + other_sessions_count
        counts = {
            "dailypass": int(dailypass_count),
            "sessions": int(all_sessions_count),
            "membership": int(membership_count)
        }

        await redis.set(cache_key, json.dumps(counts), ex=600)

    response["booking_counts"] = counts
    return response


async def expiry_summary(
    gym_id: int, response: Dict[str, Any], redis: Redis, db: AsyncSession
) -> Dict[str, Any]:
    today = date.today()
    five_days_later = today + timedelta(days=5)

    about_key = f"gym:{gym_id}:about_to_expire:{today.strftime('%Y-%m-%d')}"
    expired_key = f"gym:{gym_id}:expired:{today.strftime('%Y-%m-%d')}"

    about_json = await redis.get(about_key)
    expired_json = await redis.get(expired_key)

    if about_json and expired_json:
        response["expiry_list"] = {
            "about_to_expire": json.loads(about_json),
            "expired": json.loads(expired_json),
        }
        return response

    # Query memberships expiring within next 5 days (exclude upcoming status)
    about_stmt = (
        select(
            Client.name,
            Client.contact.label("number"),
            ClientGym.gym_client_id,
            GymPlans.plans.label("plan_description"),
            FittbotGymMembership.expires_at,
            Client.profile.label("dp"),
        )
        .select_from(FittbotGymMembership)
        .join(Client, Client.client_id == FittbotGymMembership.client_id)
        .join(ClientGym, (ClientGym.client_id == Client.client_id) & (ClientGym.gym_id == gym_id), isouter=True)
        .join(GymPlans, GymPlans.id == FittbotGymMembership.plan_id, isouter=True)
        .where(
            FittbotGymMembership.gym_id == str(gym_id),
            FittbotGymMembership.status != "upcoming",
            FittbotGymMembership.expires_at > today,
            FittbotGymMembership.expires_at <= five_days_later
        )
        .order_by(FittbotGymMembership.expires_at.asc())
        .limit(3)
    )
    about_result = await db.execute(about_stmt)
    about_q = about_result.all()

    # First, update any expired memberships that still have active status
    update_stmt = (
        update(FittbotGymMembership)
        .where(
            FittbotGymMembership.gym_id == str(gym_id),
            FittbotGymMembership.expires_at < today,
            FittbotGymMembership.status == "active"
        )
        .values(status="expired")
    )
    await db.execute(update_stmt)
    await db.commit()

    # Query expired memberships (expires_at < today, exclude upcoming)
    expired_stmt = (
        select(
            Client.name,
            Client.contact.label("number"),
            ClientGym.gym_client_id,
            GymPlans.plans.label("plan_description"),
            Client.profile.label("dp"),
        )
        .select_from(FittbotGymMembership)
        .join(Client, Client.client_id == FittbotGymMembership.client_id)
        .join(ClientGym, (ClientGym.client_id == Client.client_id) & (ClientGym.gym_id == gym_id), isouter=True)
        .join(GymPlans, GymPlans.id == FittbotGymMembership.plan_id, isouter=True)
        .where(
            FittbotGymMembership.gym_id == str(gym_id),
            FittbotGymMembership.status != "upcoming",
            FittbotGymMembership.expires_at < today
        )
        .order_by(FittbotGymMembership.expires_at.desc())
        .limit(3)
    )
    expired_result = await db.execute(expired_stmt)
    expired_q = expired_result.all()

    def serialize_about(rows):
        return [
            {
                "name": r.name,
                "number": r.number,
                "gym_client_id": r.gym_client_id if r.gym_client_id else None,
                "plan_description": r.plan_description if r.plan_description else None,
                "dp": r.dp,
                "expires_in": (r.expires_at - today).days,
            }
            for r in rows
        ]

    def serialize_expired(rows):
        return [
            {
                "name": r.name,
                "number": r.number,
                "gym_client_id": r.gym_client_id if r.gym_client_id else None,
                "plan_description": r.plan_description if r.plan_description else None,
                "dp": r.dp,
            }
            for r in rows
        ]

    about_list = serialize_about(about_q)
    expired_list = serialize_expired(expired_q)

    await redis.set(about_key, json.dumps(about_list), ex=86400)  # Cache for 24 hours
    await redis.set(expired_key, json.dumps(expired_list), ex=86400)

    response["expiry_list"] = {"about_to_expire": about_list, "expired": expired_list}
    return response


async def get_attendance(
    gym_id: int, response: Dict[str, Any], redis: Redis, db: AsyncSession
) -> Dict[str, Any]:
    # Get current month's start date
    today = date.today()
    month_start = date(today.year, today.month, 1)

    stmt = (
        select(AttendanceGym.date, AttendanceGym.attendance_count)
        .where(
            AttendanceGym.gym_id == gym_id,
            AttendanceGym.date >= month_start
        )
        .order_by(AttendanceGym.date.asc())
    )
    result = await db.execute(stmt)
    rows = result.all()

    if not rows:
        response["attendance_chart"] = []
        return response

    result_list = [{"date": r.date.isoformat(), "attendance_count": r.attendance_count} for r in rows]
    response["attendance_chart"] = result_list
    return response


async def get_birthday_clients(
    gym_id: int, response: Dict[str, Any], redis: Redis, db: AsyncSession
) -> Dict[str, Any]:
    today = date.today()
    bday_key = f"gym:{gym_id}:bday_clients:{today.strftime('%Y-%m-%d')}"

    bday_data = await redis.get(bday_key)

    if bday_data:
        bday_clients = json.loads(bday_data)
    else:
        # Query clients whose birthday (date and month) matches today
        stmt = (
            select(Client.name, Client.contact)
            .where(
                Client.gym_id == gym_id,
                func.extract('month', Client.dob) == today.month,
                func.extract('day', Client.dob) == today.day
            )
        )
        result = await db.execute(stmt)
        bday_clients_query = result.all()

        bday_clients = [
            {"name": client.name, "mobile_number": client.contact}
            for client in bday_clients_query
        ]

        # Cache until end of day
        await redis.set(bday_key, json.dumps(bday_clients), ex=86400)

    response["bday_clients"] = bday_clients
    return response


async def get_trainer_attendance_data(
    gym_id: int, response: Dict[str, Any], redis: Redis, db: AsyncSession
) -> Dict[str, Any]:
    today = date.today()
    trainer_attendance_key = f"gym:{gym_id}:trainer_attendance_summary:{today.strftime('%Y-%m-%d')}"
    trainer_data = await redis.hgetall(trainer_attendance_key)

    if not trainer_data:

        # Get today's trainer attendance
        stmt = (
            select(
                TrainerAttendance.trainer_id,
                TrainerAttendance.punch_sessions,
                TrainerAttendance.total_hours,
                TrainerAttendance.status,
                TrainerProfile.full_name,
                TrainerProfile.profile_image,
                TrainerProfile.specializations
            )
            .select_from(TrainerAttendance)
            .join(TrainerProfile, TrainerAttendance.trainer_id == TrainerProfile.trainer_id)
            .where(
                TrainerAttendance.gym_id == gym_id,
                TrainerAttendance.date == today
            )
        )
        result = await db.execute(stmt)
        today_attendance = result.all()

        # Get all trainers for this gym
        trainers_stmt = select(TrainerProfile).where(TrainerProfile.gym_id == gym_id)
        trainers_result = await db.execute(trainers_stmt)
        all_trainers = trainers_result.scalars().all()

        total_trainers = len(all_trainers)
        present_trainers = len(today_attendance)
        currently_active = 0
        total_hours_today = 0.0

        trainer_details = []

        # Process present trainers
        for attendance in today_attendance:
            is_currently_active = False
            if attendance.punch_sessions:
                for session in attendance.punch_sessions:
                    if session.get("punch_in") and not session.get("punch_out"):
                        is_currently_active = True
                        currently_active += 1
                        break

            total_hours_today += attendance.total_hours or 0.0

            trainer_details.append({
                "trainer_id": attendance.trainer_id,
                "name": attendance.full_name,
                "profile_image": attendance.profile_image,
                "specializations": attendance.specializations,
                "total_hours": attendance.total_hours or 0.0,
                "session_count": len(attendance.punch_sessions) if attendance.punch_sessions else 0,
                "status": attendance.status,
                "is_currently_active": is_currently_active,
                "punch_sessions": attendance.punch_sessions or []
            })

        # Add absent trainers
        present_trainer_ids = {t.trainer_id for t in today_attendance}
        for trainer in all_trainers:
            if trainer.trainer_id not in present_trainer_ids:
                trainer_details.append({
                    "trainer_id": trainer.trainer_id,
                    "name": trainer.full_name,
                    "profile_image": trainer.profile_image,
                    "specializations": trainer.specializations,
                    "total_hours": 0.0,
                    "session_count": 0,
                    "status": "absent",
                    "is_currently_active": False,
                    "punch_sessions": []
                })

        trainer_summary = {
            "total_trainers": total_trainers,
            "present_today": present_trainers,
            "currently_active": currently_active,
            "absent_today": total_trainers - present_trainers,
            "total_hours_today": round(total_hours_today, 2),
            "average_hours": round(total_hours_today / max(present_trainers, 1), 2)
        }

        await redis.hset(
            trainer_attendance_key,
            mapping={
                "summary": json.dumps(trainer_summary),
                "details": json.dumps(trainer_details[:5])  # Limit to 5 for home screen
            }
        )
        await redis.expire(trainer_attendance_key, 1800)  # Cache for 30 minutes

    else:
        try:
            trainer_summary = json.loads(trainer_data.get("summary", "{}"))
            trainer_details = json.loads(trainer_data.get("details", "[]"))
        except Exception:
            trainer_summary = {}
            trainer_details = []

    response["trainer_attendance"] = {
        "summary": trainer_summary,
        "recent_attendance": trainer_details
    }
    return response


async def get_owner_posters(
    gym_id: int, response: Dict[str, Any], async_db: AsyncSession
) -> Dict[str, Any]:
    """Get posters for owner home based on missing setup items"""
    posters = []

    # Get all poster URLs from database
    all_posters_result = await async_db.execute(select(OwnerHomePoster))
    all_posters = all_posters_result.scalars().all()
    poster_map = {p.type: p.url for p in all_posters}

    # 1. Check if no clients for this gym
    client_count_result = await async_db.execute(
        select(func.count()).select_from(Client).where(Client.gym_id == gym_id)
    )
    client_count = client_count_result.scalar() or 0
    if client_count == 0 and "clients" in poster_map:
        posters.append({"url": poster_map["clients"], "type": "clients", "redirect": False})

    # 2. Check if no couple membership plan created
    couple_plan_result = await async_db.execute(
        select(GymPlans).where(GymPlans.gym_id == gym_id, GymPlans.plan_for == "couple")
    )
    couple_plan = couple_plan_result.scalars().first()
    if not couple_plan and "couplemembership" in poster_map:
        posters.append({"url": poster_map["couplemembership"], "type": "couplemembership", "redirect": False})

    # 3. Check if no dailypass pricing set
    dailypass_result = await async_db.execute(
        select(DailyPassPricing).where(DailyPassPricing.gym_id == str(gym_id))
    )
    dailypass_pricing = dailypass_result.scalars().first()
    if not dailypass_pricing and "dailypass" in poster_map:
        posters.append({"url": poster_map["dailypass"], "type": "dailypass", "redirect": False})

    # 4. Check if no plans created
    plans_count_result = await async_db.execute(
        select(func.count()).select_from(GymPlans).where(GymPlans.gym_id == gym_id)
    )
    plans_count = plans_count_result.scalar() or 0
    if plans_count == 0 and "plans" in poster_map:
        posters.append({"url": poster_map["plans"], "type": "plans", "redirect": False})

    # 5. Check if no_cost_emi not set
    no_cost_emi_result = await async_db.execute(
        select(NoCostEmi).where(NoCostEmi.gym_id == gym_id, NoCostEmi.no_cost_emi == True)
    )
    no_cost_emi = no_cost_emi_result.scalars().first()
    if not no_cost_emi and "nocostemi" in poster_map:
        posters.append({"url": poster_map["nocostemi"], "type": "nocostemi", "redirect": False})

    # 6. Check if no personal training plan
    pt_plan_result = await async_db.execute(
        select(GymPlans).where(GymPlans.gym_id == gym_id, GymPlans.personal_training == True)
    )
    pt_plan = pt_plan_result.scalars().first()
    if not pt_plan and "pt" in poster_map:
        posters.append({"url": poster_map["pt"], "type": "pt", "redirect": False})

    # 7. Check if no session created
    session_result = await async_db.execute(
        select(SessionSetting).where(SessionSetting.gym_id == gym_id)
    )
    session_setting = session_result.scalars().first()
    if not session_setting and "session" in poster_map:
        posters.append({"url": poster_map["session"], "type": "session", "redirect": False})

    # 8. Always add rewards poster with redirect True
    # if "rewards" in poster_map:
    #     posters.append({"url": poster_map["rewards"], "type": "rewards", "redirect": True})

    # response["posters"] = posters

    return response


async def get_registration_steps(
    gym_id: int, response: Dict[str, Any], async_db: AsyncSession
) -> Dict[str, Any]:
    """Get registration document steps status for a gym"""


    stmt = select(AccountDetails).where(AccountDetails.gym_id == gym_id)
    result = await async_db.execute(stmt)
    rows = result.scalars().all()
    account_details = rows[0] if rows else None
    account_details_completed = account_details is not None


    stmt = select(Gym).where(Gym.gym_id == gym_id)
    result = await async_db.execute(stmt)
    rows = result.scalars().all()
    gym = rows[0] if rows else None

    services_completed = False
    operating_hours_completed = False

    if gym:
        try:
            services_data = json.loads(gym.services) if isinstance(gym.services, str) else gym.services
            services_completed = services_data is not None and len(services_data) > 0
        except:
            services_completed = False

        try:
            operating_hours_data = json.loads(gym.operating_hours) if isinstance(gym.operating_hours, str) else gym.operating_hours
            operating_hours_completed = operating_hours_data is not None and len(operating_hours_data) > 0
        except:
            operating_hours_completed = False

    # Check gym_location table for gym_pic
    stmt = select(GymLocation).where(GymLocation.gym_id == gym_id)
    result = await async_db.execute(stmt)
    gym_location = result.scalar_one_or_none()
    gym_pic_completed = gym_location is not None and gym_location.gym_pic is not None and len(gym_location.gym_pic) > 0

    # 3. Check gym_verification_documents table
    stmt = select(GymVerificationDocument).where(GymVerificationDocument.gym_id == gym_id)
    result = await async_db.execute(stmt)
    rows = result.scalars().all()
    verification_doc = rows[0] if rows else None

    # Agreement status
    agreement_completed = verification_doc.agreement if verification_doc and verification_doc.agreement else False

    # Pancard status (pan_url)
    pancard_completed = verification_doc.pan_url is not None and len(verification_doc.pan_url) > 0 if verification_doc else False

    # Passbook status (bankbook_url)
    passbook_completed = verification_doc.bankbook_url is not None and len(verification_doc.bankbook_url) > 0 if verification_doc else False

    # 4. Check gym_onboarding_pics table
    stmt = select(GymOnboardingPics).where(GymOnboardingPics.gym_id == gym_id)
    result = await async_db.execute(stmt)
    rows = result.scalars().all()
    onboarding_pics = rows[0] if rows else None

    # Build documents list with pancard and passbook only
    documents = [
        {"pancard": pancard_completed},
        {"passbook": passbook_completed}
    ]

    # Build onboarding pics list separately
    onboarding_pics_status = []
    
    if onboarding_pics:
        pic_columns = [
            "machinery_1",
            "machinery_2",
            "treadmill_area",
            "cardio_area",
            "dumbell_area",
            "reception_area"
        ]
        for col in pic_columns:
            value = getattr(onboarding_pics, col, None)
            onboarding_pics_status.append({
                col: value is not None and len(value) > 0 if value else False
            })
    
    else:
        onboarding_pics_status = [
            {"machinery_1": False},
            {"machinery_2": False},
            {"treadmill_area": False},
            {"cardio_area": False},
            {"dumbell_area": False},
            {"reception_area": False}
        ]


    if gym_id <= 470:
        response["services_old"] = True
        response["registration_steps"] = {
            "account_details": account_details_completed,
            "services": services_completed,
            "operating_hours": operating_hours_completed,
            "agreement": agreement_completed,
            "documents": documents,
            "onboarding_pics": onboarding_pics_status
        }
    else:
        response["services_old"] = False
        response["registration_steps"] = {
            "account_details": account_details_completed,
            "gym_pic": gym_pic_completed,
            "operating_hours": operating_hours_completed,
            "agreement": agreement_completed,
            "documents": documents,
            "onboarding_pics": onboarding_pics_status
        }

    return response


async def get_prefilled_agreement_for_home(
    gym_id: int, response: Dict[str, Any], async_db: AsyncSession
) -> Dict[str, Any]:
    """Get prefilled agreement for home page - only show if is_clicked is False"""

    stmt = select(GymPrefilledAgreement).where(GymPrefilledAgreement.gym_id == gym_id)
    result = await async_db.execute(stmt)
    rows = result.scalars().all()
    prefilled_agreement = rows[0] if rows else None

    if prefilled_agreement and not prefilled_agreement.is_clicked:
        response["prefilled_agreement"] = {
            "show": True,
            "s3_link": prefilled_agreement.s3_link,
            "updated_at": prefilled_agreement.updated_at.isoformat() if prefilled_agreement.updated_at else None
        }
    else:
        response["prefilled_agreement"] = {
            "show": False,
            "s3_link": None,
            "updated_at": None
        }

    return response


async def get_manual_posters_info(
    response: Dict[str, Any], async_db: AsyncSession
) -> Dict[str, Any]:

    stmt = select(ManualPoster).where(ManualPoster.show == True)
    result = await async_db.execute(stmt)
    active_record = result.scalars().first()

    if active_record and active_record.urls:
        posters = active_record.urls if isinstance(active_record.urls, list) else []
        if posters:
            response["use_manual_posters"] = True
            response["manual_posters"] = posters
            return response

    response["use_manual_posters"] = False
    response["manual_posters"] = []
    return response


@router.post("/mark-agreement-clicked")
async def mark_agreement_clicked(
    gym_id: int,
    async_db: AsyncSession = Depends(get_async_db)
):

    try:
        stmt = select(GymPrefilledAgreement).where(GymPrefilledAgreement.gym_id == gym_id)
        result = await async_db.execute(stmt)
        prefilled_agreement = result.scalar_one_or_none()

        if not prefilled_agreement:
            raise HTTPException(status_code=404, detail="Prefilled agreement not found for this gym")

        prefilled_agreement.is_clicked = True
        await async_db.commit()

        return {
            "status": 200,
            "message": "Agreement marked as clicked successfully",
            "data": {
                "gym_id": gym_id,
                "is_clicked": True
            }
        }

    except HTTPException:
        raise
    except Exception as e:
        await async_db.rollback()
        raise FittbotHTTPException(
            status_code=500,
            detail="Failed to mark agreement as clicked",
            error_code="MARK_AGREEMENT_CLICKED_ERROR",
            log_data={"gym_id": gym_id, "error": repr(e)},
        ) from e


@router.get("/all")
async def get_gym_home_data(
    gym_id: int,
    async_db: AsyncSession = Depends(get_async_db),
    redis: Redis = Depends(get_redis)
):
    try:
        response: Dict[str, Any] = {}
        response = await get_attendance_data(gym_id, response, redis, async_db)
        response = await get_count_data(gym_id, response, redis, async_db)
        response = await get_booking_payment_counts(gym_id, response, redis, async_db)
        response = await expiry_summary(gym_id, response, redis, async_db)
        response = await get_attendance(gym_id, response, redis, async_db)
        response = await get_trainer_attendance_data(gym_id, response, redis, async_db)
        response = await get_birthday_clients(gym_id, response, redis, async_db)
        response = await get_owner_posters(gym_id, response, async_db)
        response = await get_registration_steps(gym_id, response, async_db)
        response = await get_prefilled_agreement_for_home(gym_id, response, async_db)
        response = await get_manual_posters_info(response, async_db)


        if gym_id==145:
            response["old_gym_data"] = False

        elif gym_id<=165:
            response["old_gym_data"] = True
   
        else:
            response["old_gym_data"] = False

    
        return {"status": 200, "data": response}

    except HTTPException:
        raise
    
    except Exception as e:
        raise FittbotHTTPException(
            status_code=500,
            detail="Failed to fetch gym home data",
            error_code="GYM_HOME_FETCH_ERROR",
            log_data={"gym_id": gym_id, "error": repr(e)},
        ) from e
