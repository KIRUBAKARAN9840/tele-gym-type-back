# app/fittbot_api/v1/owner/owner_api/general_modal/modal.py

from datetime import datetime, date, timedelta
from typing import List
from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from app.models.database import get_db
from app.models.fittbot_models import (
    OwnerModalTracker,
    NoCostEmi,
    SessionSetting,
    Gym,
)
from app.utils.redis_config import get_redis
from app.utils.logging_utils import FittbotHTTPException

router = APIRouter(prefix="/owner_general_modal", tags=["Owner General Modal"])

# All possible modal types for owners
ALL_MODAL_TYPES = ["no_cost_emi", "session", "dailypass"]


def get_seconds_until_midnight() -> int:
    """Calculate seconds remaining until midnight."""
    now = datetime.now()
    midnight = datetime.combine(now.date() + timedelta(days=1), datetime.min.time())
    return int((midnight - now).total_seconds())


def get_missing_features(db: Session, gym_id: int) -> List[str]:

    missing = []


    no_cost_emi_record = db.query(NoCostEmi).filter(NoCostEmi.gym_id == gym_id).first()
    if not no_cost_emi_record or not no_cost_emi_record.no_cost_emi:
        missing.append("no_cost_emi")

    try:
        enabled_session = db.query(SessionSetting).filter(
            SessionSetting.gym_id == gym_id,
            SessionSetting.is_enabled == True
        ).first()
        if not enabled_session:
            missing.append("session")
    except Exception:
        missing.append("session")

    gym = db.query(Gym).filter(Gym.gym_id == gym_id).first()
    if not gym or not gym.dailypass:
        missing.append("dailypass")

    return missing


@router.get("/check")
async def check_owner_modal(gym_id: int, page: str, db: Session = Depends(get_db)):

    try:
        redis = await get_redis()
        today = date.today().isoformat()
        ttl_seconds = get_seconds_until_midnight()

        # Separate Redis keys for different pages
        redis_key_session_dailypass = f"owner_modal_shown:{gym_id}:{today}:session_dailypass"
        redis_key_no_cost_emi = f"owner_modal_shown:{gym_id}:{today}:no_cost_emi"

        # Default response - all False
        response = {
            "status": 200,
            "no_cost_emi": False,
            "session": False,
            "dailypass": False
        }

        # Check which modals were already shown today
        session_dailypass_shown = await redis.get(redis_key_session_dailypass)
        no_cost_emi_shown = await redis.get(redis_key_no_cost_emi)

        missing_features = get_missing_features(db, gym_id)

        # Handle no_cost_emi - shown on plan page
        if not no_cost_emi_shown and "no_cost_emi" in missing_features:
            response["no_cost_emi"] = True
            # Only set Redis (mark as shown) if on plan page
            if page == "client":
                await redis.set(redis_key_no_cost_emi, "1", ex=ttl_seconds)

        # Handle session/dailypass - shown on client page
        session_dailypass_missing = [f for f in missing_features if f in ["session", "dailypass"]]

        if not session_dailypass_shown and session_dailypass_missing:
            # Get or create tracker for session/dailypass rotation
            tracker = db.query(OwnerModalTracker).filter(
                OwnerModalTracker.gym_id == gym_id
            ).first()

            if not tracker:
                tracker = OwnerModalTracker(
                    gym_id=gym_id,
                    last_modal_index=0,
                    last_shown_date=date.today()
                )
                db.add(tracker)
                db.commit()
                db.refresh(tracker)
                current_index = 0
            else:
                current_index = (tracker.last_modal_index + 1) % len(session_dailypass_missing)
                # Only update tracker if on client page
                if page == "plan":
                    tracker.last_modal_index = current_index
                    tracker.last_shown_date = date.today()
                    db.commit()

            # Get the session/dailypass modal to show today
            modal_to_show = session_dailypass_missing[current_index % len(session_dailypass_missing)]
            response[modal_to_show] = True

            # Only set Redis (mark as shown) if on client page
            if page == "plan":
                await redis.set(redis_key_session_dailypass, "1", ex=ttl_seconds)


        return response

    except FittbotHTTPException:
        raise
    except Exception as e:
        raise FittbotHTTPException(
            status_code=500,
            detail="Failed to check owner modal status",
            error_code="OWNER_MODAL_CHECK_ERROR",
            log_data={"gym_id": gym_id, "error": str(e)}
        )
