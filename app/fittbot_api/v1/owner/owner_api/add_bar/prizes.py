# app/routers/gym_prizes.py

import datetime
from typing import Optional

from fastapi import APIRouter, Depends
from sqlalchemy import asc, desc
from sqlalchemy.orm import Session
from redis.asyncio import Redis

from app.models.database import get_db
from app.utils.redis_config import get_redis
from app.models.fittbot_models import RewardPrizeHistory, ClientGym
from pydantic import BaseModel
from app.utils.logging_utils import FittbotHTTPException

router = APIRouter(prefix="/gym_prizes", tags=["prizes"])


@router.get("/get_prizes")
async def get_prizes(
    gym_id: int,
    status: str,
    db: Session = Depends(get_db),
    redis: Redis = Depends(get_redis),  # kept for parity with existing signature
):
    """
    Fetch prizes for a given gym based on status ("pending" or "given").
    Keeps original API and logic intact; adds robust error handling.
    """
    try:
        status_norm = (status or "").strip().lower()
        if status_norm not in {"pending", "given"}:
            raise FittbotHTTPException(
                status_code=400,
                detail="Invalid 'status' value. Must be 'pending' or 'given'.",
                error_code="INVALID_PRIZE_STATUS",
                log_data={"gym_id": gym_id, "status": status},
            )

        query = (
            db.query(RewardPrizeHistory)
            .filter(RewardPrizeHistory.gym_id == gym_id)
            .filter(RewardPrizeHistory.is_given == (status_norm == "given"))
        )

        if status_norm == "pending":
            query = query.order_by(asc(RewardPrizeHistory.achieved_date))
        else:
            query = query.order_by(desc(RewardPrizeHistory.given_date))

        prizes = query.all()

        data = []
        for p in prizes:
            # Original logic only filtered by client_id
            gym_client = (
                db.query(ClientGym).filter(ClientGym.client_id == p.client_id).first()
            )

            achieved_iso = (
                p.achieved_date.isoformat() if p.achieved_date is not None else None
            )
            given_iso = p.given_date.isoformat() if p.given_date is not None else None

            data.append(
                {
                    "id": p.id,
                    "gym_id": p.gym_id,
                    "client_id": p.client_id,
                    "client_name": p.client_name,
                    "xp": p.xp,
                    "gift": p.gift,
                    "achieved_date": achieved_iso,
                    "given_date": given_iso,
                    "is_given": p.is_given,
                    "gym_client_id": gym_client.gym_client_id if gym_client else "",
                    "image_url": p.profile,
                }
            )

        return {"status": 200, "data": data}

    except FittbotHTTPException:
        raise
    except Exception as e:
        raise FittbotHTTPException(
            status_code=500,
            detail="Failed to fetch prizes",
            error_code="PRIZES_FETCH_ERROR",
            log_data={"error": repr(e), "gym_id": gym_id, "status": status},
        ) from e


class PrizeRequest(BaseModel):
    reward_id: int


@router.put("/give_prize")
async def give_prize(Request: PrizeRequest, db: Session = Depends(get_db)):
    """
    Mark a prize as given. Keeps original API and logic intact.
    """
    try:
        prize_id = Request.reward_id
        prize = (
            db.query(RewardPrizeHistory)
            .filter(RewardPrizeHistory.id == prize_id)
            .first()
        )

        if not prize:
            raise FittbotHTTPException(
                status_code=404,
                detail="Prize not found",
                error_code="PRIZE_NOT_FOUND",
                log_data={"reward_id": prize_id},
            )

        # Original behavior: set to given unconditionally
        prize.is_given = True
        prize.given_date = datetime.datetime.now()
        db.commit()
        db.refresh(prize)

        return {"status": 200, "message": "prize updated"}

    except FittbotHTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise FittbotHTTPException(
            status_code=500,
            detail="Failed to update prize",
            error_code="PRIZE_UPDATE_ERROR",
            log_data={"error": repr(e), "reward_id": getattr(Request, 'reward_id', None)},
        ) from e
