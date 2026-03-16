# app/api/v1/community/moderation.py

import json
from typing import Optional
from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy.orm import Session
from redis.asyncio import Redis
from app.models.database import get_db
from app.utils.redis_config import get_redis
from app.utils.logging_utils import FittbotHTTPException
from app.models.fittbot_models import (
    Post,
    Gym,
    Report,
    BlockedUsers,
)

router = APIRouter(prefix="/community", tags=["Reports & Moderation"])

# ---------------------- Schemas ----------------------
class ReportUserRequest(BaseModel):
    user_id: Optional[int] = None
    user_role: str
    post_id: int
    reason: str


class BlockUserRequest(BaseModel):
    user_id: int
    user_role: str
    post_id: int


# ---------------------- Endpoints ----------------------
@router.post("/report_user")
async def report_user(request: ReportUserRequest, db: Session = Depends(get_db)):
    try:
        post = db.query(Post).filter(Post.post_id == request.post_id).first()

        if not post:
            # Keep logic the same, just normalize error handling
            raise FittbotHTTPException(
                status_code=400,
                detail="Post not found",
                error_code="REPORT_POST_NOT_FOUND",
                log_data={"post_id": request.post_id},
            )

        if not post.client_id:
            gym = db.query(Gym).filter(Gym.gym_id == post.gym_id).first()
            reported_id = gym.owner_id
            reported_role = "owner"
        else:
            reported_id = post.client_id
            reported_role = "client"

        new_report = Report(
            user_id=request.user_id,
            user_role=request.user_role,
            reported_id=reported_id,
            reported_role=reported_role,
            post_id=request.post_id,
            reason=request.reason,
            post_content=post.content,
            status=False,
        )

        db.add(new_report)
        db.commit()

        return {"status": 200, "message": "Report submitted successfully"}

    except FittbotHTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise FittbotHTTPException(
            status_code=500,
            detail=f"Unexpected Error occurred: {str(e)}",
            error_code="REPORT_USER_ERROR",
            log_data={
                "post_id": request.post_id,
                "user_id": request.user_id,
                "user_role": request.user_role,
                "error": str(e),
            },
        )


@router.post("/block_user")
async def block_user(
    request: BlockUserRequest,
    db: Session = Depends(get_db),
    redis: Redis = Depends(get_redis),
):
    try:
        post = db.query(Post).filter(Post.post_id == request.post_id).first()

        # Keep original order/logic intact (even though it might access post before None check)
        redis_key = f"gym:{post.gym_id}:posts"
        if await redis.exists(redis_key):
            await redis.delete(redis_key)

        async for key in redis.scan_iter("post:*:comment_count"):
            await redis.delete(key)

        if not post:
            raise FittbotHTTPException(
                status_code=400,
                detail="Post not found",
                error_code="BLOCK_POST_NOT_FOUND",
                log_data={"post_id": request.post_id},
            )

        if not post.client_id:
            # Owner post
            gym = db.query(Gym).filter(Gym.gym_id == post.gym_id).first()
            blocked_id = gym.gym_id
            blocked_role = "owner"
        else:
            blocked_id = post.client_id
            blocked_role = "client"

        blocked_entry = (
            db.query(BlockedUsers)
            .filter(
                BlockedUsers.user_id == request.user_id,
                BlockedUsers.user_role == request.user_role,
            )
            .first()
        )

        if blocked_entry:
            blocked_data = blocked_entry.blocked_user_id
            if isinstance(blocked_data, str):
                try:
                    blocked_data = json.loads(blocked_data)
                except json.JSONDecodeError:
                    blocked_data = {}

            if not isinstance(blocked_data, dict):
                blocked_data = {}

            if "owner" not in blocked_data:
                blocked_data["owner"] = []
            if "client" not in blocked_data:
                blocked_data["client"] = []

            if blocked_id not in blocked_data[blocked_role]:
                blocked_data[blocked_role].append(blocked_id)

            blocked_entry.blocked_user_id = json.dumps(blocked_data)
            db.commit()
            db.refresh(blocked_entry)
            return {"status": 200, "message": "Blocked user updated successfully"}

        else:
            opp = "owner" if blocked_role == "client" else "client"
            new_record = BlockedUsers(
                user_id=request.user_id,
                user_role=request.user_role,
                blocked_user_id=json.dumps({blocked_role: [blocked_id], opp: []}),
            )
            db.add(new_record)
            db.commit()
            db.refresh(new_record)
            return {"status": 200, "message": "Blocked user created successfully"}

    except FittbotHTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise FittbotHTTPException(
            status_code=500,
            detail=str(e),
            error_code="BLOCK_USER_ERROR",
            log_data={
                "post_id": request.post_id,
                "user_id": request.user_id,
                "user_role": request.user_role,
                "error": str(e),
            },
        )
