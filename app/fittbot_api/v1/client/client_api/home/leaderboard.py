from datetime import date
from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from sqlalchemy import desc, extract

from app.models.database import get_db
from app.utils.logging_utils import FittbotHTTPException
from app.models.fittbot_models import (
    RewardBadge,
    Client,
    LeaderboardDaily,
    LeaderboardMonthly,
    LeaderboardOverall,
)

router = APIRouter(prefix="/leaderboard",tags=["Leaderboard"])


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
    except FittbotHTTPException:
        raise
    except Exception as e:
        raise FittbotHTTPException(
            status_code=500,
            detail=f"An unexpected error occurred {e}",
            error_code="BADGE_LOOKUP_ERROR",
            log_data={"xp": xp, "error": str(e)},
        )


def record_to_dict(record, db: Session, client_map=None):
    try:
        # Handle case where record might be None
        if record is None:
            return {
                "client_id": None,
                "client_name": None,
                "profile": None,
                "xp": 0,
                "badge": None,
                "level": None,
            }

        # Get XP value safely
        xp_value = getattr(record, 'xp', 0) or 0
        badge_info = get_badge_for_xp(xp_value, db)

        # Get client from map or database
        client = None
        client_id = getattr(record, 'client_id', None)

        if client_id and client_map is not None:
            client = client_map.get(client_id)

        if client is None and client_id:
            client = db.query(Client).filter(Client.client_id == client_id).first()

        # Safely extract client data
        return {
            "client_id": client_id,
            "client_name": getattr(client, 'name', None) if client else None,
            "profile": getattr(client, 'profile', None) if client else None,
            "xp": xp_value,
            "badge": badge_info.get("badge") if badge_info else None,
            "level": badge_info.get("level") if badge_info else None,
        }

    except FittbotHTTPException:
        raise
    except Exception as e:
        raise FittbotHTTPException(
            status_code=500,
            detail=f"An unexpected error occurred {e}",
            error_code="LEADERBOARD_RECORD_FORMAT_ERROR",
            log_data={"client_id": getattr(record, 'client_id', None), "error": str(e)},
        )


@router.get("/get")
async def get_leaderboard(gym_id: int, db: Session = Depends(get_db)):
    try:
        today = date.today()

        # Fetch all clients for the gym
        clients = (
            db.query(Client)
            .filter(Client.gym_id == gym_id)
            .all()
        ) or []

        # Filter out None clients and clients without client_id, then build maps
        valid_clients = [c for c in clients if c is not None and hasattr(c, 'client_id') and c.client_id is not None]
        client_ids = [client.client_id for client in valid_clients]
        client_map = {client.client_id: client for client in valid_clients}

        # Fetch leaderboard records
        daily_records = (
            db.query(LeaderboardDaily)
            .filter(
                LeaderboardDaily.client_id.in_(client_ids),
                LeaderboardDaily.date == today,
            )
            .order_by(desc(LeaderboardDaily.xp))
            .all()
        ) if client_ids else []

        monthly_records = (
            db.query(LeaderboardMonthly)
            .filter(
                LeaderboardMonthly.client_id.in_(client_ids),
                extract("year", LeaderboardMonthly.month) == today.year,
                extract("month", LeaderboardMonthly.month) == today.month,
            )
            .order_by(desc(LeaderboardMonthly.xp))
            .all()
        ) if client_ids else []

        overall_records = (
            db.query(LeaderboardOverall)
            .filter(LeaderboardOverall.client_id.in_(client_ids))
            .order_by(desc(LeaderboardOverall.xp))
            .all()
        ) if client_ids else []

        # Convert to dict
        daily_list = [] if not daily_records else [record_to_dict(rec, db, client_map) for rec in daily_records]
        monthly_list = [] if not monthly_records else [record_to_dict(rec, db, client_map) for rec in monthly_records]
        overall_list = [] if not overall_records else [record_to_dict(rec, db, client_map) for rec in overall_records]

        return {
            "status": 200,
            "data": {
                "today": daily_list,
                "month": monthly_list,
                "overall": overall_list,
            },
        }

    except FittbotHTTPException:
        raise
    except Exception as e:
        raise FittbotHTTPException(
            status_code=500,
            detail="An unexpected error occurred.",
            error_code="LEADERBOARD_FETCH_ERROR",
            log_data={"gym_id": gym_id, "error": str(e)},
        )
