from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, desc
from datetime import datetime, timezone, timedelta
import traceback

from app.models.async_database import get_async_db
from app.models.fittbot_models import Client, Gym
from app.models.telecaller_models import (
    Manager,
    Telecaller,
    TelecallerNotificationCursor,
)
from app.telecaller.dependencies import get_current_telecaller, get_current_manager

# IST timezone
IST = timezone(timedelta(hours=5, minutes=30))

router = APIRouter(prefix="/notifications", tags=["telecaller-notifications"])


# ─── Pydantic Schemas ───────────────────────────────────────────────

class MarkSeenRequest(BaseModel):
    count: int  # The max created_at from the batch they viewed


# ─── GET /notifications/new-users ────────────────────────────────────

@router.get("/new-users")
async def get_new_users(
    page: int = Query(1, ge=1),
    limit: int = Query(20, ge=1, le=100),
    db: AsyncSession = Depends(get_async_db),
    current_telecaller: Telecaller = Depends(get_current_telecaller),
):
    try:
        telecaller_id = current_telecaller.id

        # Get the telecaller's cursor (last_seen_at)
        cursor_stmt = select(TelecallerNotificationCursor).where(
            TelecallerNotificationCursor.telecaller_id == telecaller_id
        )
        cursor_result = await db.execute(cursor_stmt)
        cursor = cursor_result.scalar_one_or_none()

        # If no cursor exists, create one with now() – only future registrations will show
        if cursor is None:
            now = datetime.now(IST).replace(tzinfo=None)
            new_cursor = TelecallerNotificationCursor(
                telecaller_id=telecaller_id,
                last_seen_at=now,
            )
            db.add(new_cursor)
            await db.commit()
            last_seen_at = now
        else:
            last_seen_at = cursor.last_seen_at

        # Count total unseen users
        count_stmt = select(func.count(Client.client_id)).where(
            Client.created_at > last_seen_at
        )
        count_result = await db.execute(count_stmt)
        total_unseen = count_result.scalar() or 0

        # Fetch paginated unseen users (newest first)
        offset = (page - 1) * limit
        users_stmt = (
            select(
                Client.client_id,
                Client.name,
                Client.contact,
                Client.email,
                Client.gym_id,
                Client.gender,
                Client.created_at,
                Gym.gym_name,
            )
            .outerjoin(Gym, Gym.gym_id == Client.gym_id)
            .where(Client.created_at > last_seen_at)
            .order_by(desc(Client.created_at))
            .offset(offset)
            .limit(limit)
        )
        users_result = await db.execute(users_stmt)
        rows = users_result.all()

        new_users = []
        latest_created_at = None

        for row in rows:
            user_data = {
                "client_id": row.client_id,
                "name": row.name,
                "contact": row.contact,
                "email": row.email,
                "gym_id": row.gym_id,
                "gym_name": row.gym_name,
                "gender": row.gender,
                "registered_at": row.created_at.isoformat() if row.created_at else None,
            }
            new_users.append(user_data)

            # Track the latest created_at for the mark-seen cursor
            if row.created_at and (latest_created_at is None or row.created_at > latest_created_at):
                latest_created_at = row.created_at

        total_pages = (total_unseen + limit - 1) // limit if total_unseen > 0 else 1

        return {
            "success": True,
            "data": {
                "new_users": new_users,
                "unseen_count": total_unseen,
                "latest_created_at": latest_created_at.isoformat() if latest_created_at else None,
                "pagination": {
                    "page": page,
                    "limit": limit,
                    "total": total_unseen,
                    "total_pages": total_pages,
                },
            },
        }

    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error fetching new user notifications: {str(e)}")


# ─── GET /notifications/unseen-count ─────────────────────────────────

@router.get("/unseen_count")
async def get_unseen_count(
    db: AsyncSession = Depends(get_async_db),
    current_telecaller: Telecaller = Depends(get_current_telecaller),
):
    try:
        telecaller_id = current_telecaller.id
        print("teleccaler id",telecaller_id)
        cursor_stmt = select(TelecallerNotificationCursor).where(
            TelecallerNotificationCursor.telecaller_id == telecaller_id
        )
        cursor_result = await db.execute(cursor_stmt)
        cursor = cursor_result.scalar_one_or_none()

        # If no cursor exists, create one with IST now – only future registrations will count
        if cursor is None:
            now = datetime.now(IST).replace(tzinfo=None)
            new_cursor = TelecallerNotificationCursor(
                telecaller_id=telecaller_id,
                last_seen_at=now,
                last_count=0,
            )
            db.add(new_cursor)
            await db.commit()
            return {
                "status": 200,
                "unseen_count": 0,
                "play_sound": False,
            }

        last_seen_at = cursor.last_seen_at

        count_stmt = select(func.count(Client.client_id)).where(
            Client.created_at > last_seen_at
        )
        count_result = await db.execute(count_stmt)
        unseen_count = count_result.scalar() or 0

        # Play sound only if count increased since last poll
        play_sound = unseen_count > cursor.last_count

        # Update last_count so next poll won't sound again for same count
        cursor.last_count = unseen_count
        await db.commit()

        return {
            "status": 200,
            "unseen_count": unseen_count,
            "play_sound": play_sound,
        }

    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error fetching unseen count: {str(e)}")


# ─── POST /notifications/mark-seen ───────────────────────────────────

@router.post("/mark_seen")
async def mark_seen(
    body: MarkSeenRequest,
    db: AsyncSession = Depends(get_async_db),
    current_telecaller: Telecaller = Depends(get_current_telecaller),
):
    try:
        telecaller_id = current_telecaller.id

        cursor_stmt = select(TelecallerNotificationCursor).where(
            TelecallerNotificationCursor.telecaller_id == telecaller_id
        )
        cursor_result = await db.execute(cursor_stmt)
        cursor = cursor_result.scalar_one_or_none()

        if cursor is not None:
            now = datetime.now(IST).replace(tzinfo=None)
            cursor.last_seen_at = now
            cursor.last_count = 0


        await db.commit()

        return {
            "status": 200,
        }

    except Exception as e:
        traceback.print_exc()
        await db.rollback()
        raise HTTPException(status_code=500, detail=f"Error marking notifications as seen: {str(e)}")


# ─── Manager Endpoints ───────────────────────────────────────────────

@router.get("/manager/new-users")
async def manager_get_new_users(
    page: int = Query(1, ge=1),
    limit: int = Query(20, ge=1, le=100),
    db: AsyncSession = Depends(get_async_db),
    current_manager: Manager = Depends(get_current_manager),
):
    try:
        last_seen_at = current_manager.last_login_at or current_manager.created_at

        # Count total unseen users
        count_stmt = select(func.count(Client.client_id)).where(
            Client.created_at > last_seen_at
        )
        count_result = await db.execute(count_stmt)
        total_unseen = count_result.scalar() or 0

        # Fetch paginated unseen users
        offset = (page - 1) * limit
        users_stmt = (
            select(
                Client.client_id,
                Client.name,
                Client.contact,
                Client.email,
                Client.gym_id,
                Client.gender,
                Client.created_at,
                Gym.gym_name,
            )
            .outerjoin(Gym, Gym.gym_id == Client.gym_id)
            .where(Client.created_at > last_seen_at)
            .order_by(desc(Client.created_at))
            .offset(offset)
            .limit(limit)
        )
        users_result = await db.execute(users_stmt)
        rows = users_result.all()

        new_users = []
        latest_created_at = None

        for row in rows:
            user_data = {
                "client_id": row.client_id,
                "name": row.name,
                "contact": row.contact,
                "email": row.email,
                "gym_id": row.gym_id,
                "gym_name": row.gym_name,
                "gender": row.gender,
                "registered_at": row.created_at.isoformat() if row.created_at else None,
            }
            new_users.append(user_data)

            if row.created_at and (latest_created_at is None or row.created_at > latest_created_at):
                latest_created_at = row.created_at

        total_pages = (total_unseen + limit - 1) // limit if total_unseen > 0 else 1

        return {
            "success": True,
            "data": {
                "new_users": new_users,
                "unseen_count": total_unseen,
                "latest_created_at": latest_created_at.isoformat() if latest_created_at else None,
                "pagination": {
                    "page": page,
                    "limit": limit,
                    "total": total_unseen,
                    "total_pages": total_pages,
                },
            },
        }

    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error fetching manager new user notifications: {str(e)}")
