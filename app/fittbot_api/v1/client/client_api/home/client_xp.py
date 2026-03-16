# app/routers/client_xp_router.py

from fastapi import APIRouter, Depends, Request
from sqlalchemy.orm import Session
from sqlalchemy import asc
from datetime import datetime, date

from app.models.database import get_db
from app.models.fittbot_models import LeaderboardOverall, RewardBadge, Client
from app.utils.logging_utils import FittbotHTTPException

router = APIRouter(prefix="/client_xp", tags=["Client_xp"])


@router.get("/get_xp")
async def get_client_xp(request: Request, client_id: int, db: Session = Depends(get_db)):
    try:
        # Ensure client exists
        profile = db.query(Client).filter(Client.client_id == client_id).first()
        if profile is None:
            raise FittbotHTTPException(
                status_code=404,
                detail=f"Client with id {client_id} not found",
                error_code="CLIENT_NOT_FOUND",
                log_data={"client_id": client_id},
            )

        xp_row = (
            db.query(LeaderboardOverall)
            .filter(LeaderboardOverall.client_id == client_id)
            .first()
        )

        # If no XP row yet, return zeros and profile info
        if not xp_row:
            return {
                "status": 200,
                "data": 0,
                "profile": profile.profile,
                "name": profile.name,
                "badge": None,
                "progress": 0,
                "email": profile.email,
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

        # No badge tier matches current XP
        if not current_row:
            return {
                "status": 200,
                "data": overall_xp,
                "profile": profile.profile,
                "name": profile.name,
                "badge": None,
                "progress": 0,
                "email": profile.email,
            }

        # Determine progress within the current badge band
        badge_rows = (
            db.query(RewardBadge)
            .filter(RewardBadge.badge == current_row.badge)
            .order_by(asc(RewardBadge.min_points))
            .all()
        )
        start_xp = badge_rows[0].min_points
        end_xp = badge_rows[-1].max_points

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
            "email": profile.email,
            "badge": current_row.image_url,
            "progress": round(progress, 4),
            "start_xp": start_xp,
            "end_xp": end_xp,
        }

    except FittbotHTTPException:
        raise
    except Exception as e:
        raise FittbotHTTPException(
            status_code=500,
            detail="Unable to retrieve client XP information",
            error_code="CLIENT_XP_FETCH_ERROR",
            log_data={"exc": repr(e), "client_id": client_id},
        )
