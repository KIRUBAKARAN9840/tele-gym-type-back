# app/routers/owner_rewards.py

from typing import List, Dict, Any

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from app.models.database import get_db
from app.models.fittbot_models import RewardQuest
from app.utils.logging_utils import FittbotHTTPException

router = APIRouter(prefix="/owner/rewards", tags=["Gymowner"])


@router.get("/quest")
async def get_quest(db: Session = Depends(get_db)) -> Dict[str, Any]:
    """
    Return all reward quests.
    """
    try:
        quests: List[RewardQuest] = db.query(RewardQuest).all()

        data = [
            {
                "id": q.id,
                "xp": q.xp,
                "about": q.about,
                "description": q.description,
                "tag": q.tag,
            }
            for q in quests
        ]

        return {
            "status": 200,
            "message": "Quests retrieved successfully",
            "data": data,
        }

    except FittbotHTTPException:
        # Pass through structured exceptions
        raise
    except Exception as e:
        # Wrap any unexpected errors in our structured exception
        raise FittbotHTTPException(
            status_code=500,
            detail="An unexpected error occurred while fetching quests",
            error_code="REWARD_QUEST_LIST_ERROR",
            log_data={"error": repr(e)},
        )
