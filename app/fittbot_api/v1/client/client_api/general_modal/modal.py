# app/fittbot_api/v1/client/client_api/general_modal/modal.py

from datetime import datetime, date, timedelta
from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from app.models.database import get_db
from app.models.fittbot_models import ClientModalTracker
from app.utils.redis_config import get_redis
from app.utils.logging_utils import FittbotHTTPException
from typing import Optional

router = APIRouter(prefix="/general_modal", tags=["General Modal"])

# Modal types in cycle order
MODAL_TYPES = ["no_cost_emi", "bnpl", "session", "dailypass"]


def get_seconds_until_midnight() -> int:
    """Calculate seconds remaining until midnight IST."""
    now = datetime.now()
    midnight = datetime.combine(now.date() + timedelta(days=1), datetime.min.time())
    return int((midnight - now).total_seconds())


@router.get("/check")
async def check_modal(client_id: int,from_contest: Optional[bool]=False,db: Session = Depends(get_db)):

    try:
        redis = await get_redis()
        today = date.today().isoformat()
        redis_key = f"modal_shown:{client_id}:{today}"


        already_shown = await redis.get(redis_key)
        
        if from_contest:
            return {
                "status": 200,
                "show_modal": False,
                "modal_type": None
            
            }
        
        if already_shown:
            return {
                "status": 200,
                "show_modal": False,
                "modal_type": None
            
            }

        # Get or create tracker for this client
        tracker = db.query(ClientModalTracker).filter(
            ClientModalTracker.client_id == client_id
        ).first()

        if not tracker:
            # First time - create tracker starting at index 0
            tracker = ClientModalTracker(
                client_id=client_id,
                last_modal_index=0,
                last_shown_date=date.today()
            )
            db.add(tracker)
            db.commit()
            db.refresh(tracker)
            current_modal_index = 0
        else:
            # Get next modal in cycle
            current_modal_index = (tracker.last_modal_index + 1) % len(MODAL_TYPES)

            # Update tracker
            tracker.last_modal_index = current_modal_index
            tracker.last_shown_date = date.today()
            db.commit()

        # Set Redis key with TTL until midnight
        ttl_seconds = get_seconds_until_midnight()
        await redis.set(redis_key, "1", ex=ttl_seconds)

        modal_type = MODAL_TYPES[current_modal_index]

        return {
            "status": 200,

            "show_modal": True,
            "modal_type": modal_type
            
        }

    except FittbotHTTPException:
        raise
    except Exception as e:
        raise FittbotHTTPException(
            status_code=500,
            detail="Failed to check modal status",
            error_code="MODAL_CHECK_ERROR",
            log_data={"client_id": client_id, "error": str(e)}
        )
