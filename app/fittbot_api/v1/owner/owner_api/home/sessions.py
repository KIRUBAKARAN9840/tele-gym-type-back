# Sessions listing for owner home
from typing import Any, Iterable, Set, List

from fastapi import APIRouter, Depends
from sqlalchemy import desc
from sqlalchemy.orm import Session
from pydantic import BaseModel

from app.models.database import get_db
from app.models.fittbot_models import ClassSession, GymSession
from app.utils.logging_utils import FittbotHTTPException

router = APIRouter(prefix="/owner/home", tags=["Gymowner"])

DAILY_PASS_SESSION_ID = 1
HIDDEN_SESSION_IDS = {7, 8, 10, 11, 14}


class GymSessionPayload(BaseModel):
    gym_id: int
    sessions: List[int]  


def _coerce_active_ids(raw_sessions: Any) -> Set[int]:

    if not raw_sessions:
        return set()

    ids: Iterable[Any]
    if isinstance(raw_sessions, dict):
        ids = [k for k, v in raw_sessions.items() if v]
    elif isinstance(raw_sessions, (list, tuple, set)):
        ids = raw_sessions
    elif isinstance(raw_sessions, str):
        ids = [part.strip() for part in raw_sessions.split(",") if part.strip()]
    else:
        ids = []

    normalized = set()
    for val in ids:
        try:
            normalized.add(int(val))
        except (TypeError, ValueError):
            continue
    return normalized


@router.get("/sessions")
async def get_sessions(
    gym_id: int,
    db: Session = Depends(get_db),
):
    try:
        if not isinstance(gym_id, int) or gym_id <= 0:
            raise FittbotHTTPException(
                status_code=400,
                detail="Invalid gym_id",
                error_code="INVALID_GYM_ID",
                log_data={"gym_id": gym_id},
            )

        all_sessions = db.query(ClassSession).order_by(ClassSession.id).all()

        latest_mapping = (
            db.query(GymSession.sessions)
            .filter(GymSession.gym_id == gym_id)
            .order_by(desc(GymSession.id))
            .first()
        )
        active_ids = (
            _coerce_active_ids(latest_mapping.sessions) if latest_mapping else set()
        )

        payload = [
            {
                "id": s.id,
                "name": s.name,
                "image": s.image,
                "description": s.description,
                "internal":s.internal,
                "timing": s.timing,
                "isActive": s.id == DAILY_PASS_SESSION_ID or s.id in active_ids,
            }
            for s in all_sessions
            if s.id not in HIDDEN_SESSION_IDS
        ]

        return {"status": 200, "data": payload}

    except FittbotHTTPException:
        raise
    except Exception as exc:  # pragma: no cover - defensive
        raise FittbotHTTPException(
            status_code=500,
            detail="Failed to fetch sessions",
            error_code="SESSION_FETCH_ERROR",
            log_data={"gym_id": gym_id, "error": repr(exc)},
        )


@router.post("/add_session")
async def set_sessions(
    payload: GymSessionPayload,
    db: Session = Depends(get_db),
):
    try:
        if not isinstance(payload.gym_id, int) or payload.gym_id <= 0:
            raise FittbotHTTPException(
                status_code=400,
                detail="Invalid gym_id",
                error_code="INVALID_GYM_ID",
                log_data={"gym_id": payload.gym_id},
            )

        normalized_ids = sorted(_coerce_active_ids(payload.sessions))

        existing = (
            db.query(GymSession)
            .filter(GymSession.gym_id == payload.gym_id)
            .order_by(desc(GymSession.id))
            .first()
        )

        if existing:
            existing.sessions = normalized_ids
        else:
            db.add(
                GymSession(
                    gym_id=payload.gym_id,
                    sessions=normalized_ids,
                )
            )

        db.commit()

        return {"status": 200, "message": "Sessions saved", "gym_id": payload.gym_id, "sessions": normalized_ids}

    except FittbotHTTPException:
        raise
    except Exception as exc:  # pragma: no cover - defensive
        db.rollback()
        raise FittbotHTTPException(
            status_code=500,
            detail="Failed to save sessions",
            error_code="SESSION_SAVE_ERROR",
            log_data={"gym_id": payload.gym_id, "error": repr(exc)},
        )

