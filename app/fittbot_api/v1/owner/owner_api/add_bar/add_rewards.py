# app/routers/rewards_section.py

import time
import json
import boto3
from typing import List, Optional

from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy.orm import Session
from redis.asyncio import Redis

from app.models.database import get_db
from app.utils.redis_config import get_redis
from app.utils.logging_utils import FittbotHTTPException
from app.models.fittbot_models import (
    RewardGym,
    Gym,
    Client,
    LeaderboardOverall,
    ClientNextXp,
)

AWS_REGION = "ap-south-2"
BUCKET_NAME = "fittbot-uploads"
PREFIX = "Reward_Pics/"
MAX_SIZE_BYTES = 1 * 1024 * 1024  # 1MB

_s3 = boto3.client("s3", region_name=AWS_REGION)

router = APIRouter(prefix="/rewards_section", tags=["Rewards"])


def _gen_presigned(
    gym_id: int,
    reward_id: int,
    extension: str,
    content_type: str = "image/jpeg",
):
    """
    Generate an S3 presigned POST for uploading a reward image.
    Centralized error/validation via FittbotHTTPException.
    """
    if not content_type or not content_type.startswith("image/"):
        raise FittbotHTTPException(
            status_code=400,
            detail="Invalid content type – must start with image/",
            error_code="INVALID_CONTENT_TYPE",
            log_data={"content_type": content_type},
        )
    if not extension:
        raise FittbotHTTPException(
            status_code=400,
            detail="File extension is required",
            error_code="MISSING_FILE_EXTENSION",
        )

    key = f"{PREFIX}gym-{gym_id}-reward-{reward_id}.{extension}"
    version = int(time.time() * 1000)

    fields = {"Content-Type": content_type}
    conditions = [
        {"Content-Type": content_type},
        ["content-length-range", 1, MAX_SIZE_BYTES],
    ]

    try:
        presigned = _s3.generate_presigned_post(
            Bucket=BUCKET_NAME,
            Key=key,
            Fields=fields,
            Conditions=conditions,
            ExpiresIn=600,
        )
    except Exception as e:
        raise FittbotHTTPException(
            status_code=500,
            detail="Failed to generate presigned upload",
            error_code="S3_PRESIGNED_POST_ERROR",
            log_data={"error": repr(e), "key": key},
        ) from e

    presigned["url"] = f"https://{BUCKET_NAME}.s3.{AWS_REGION}.amazonaws.com/"
    cdn_url = f"{presigned['url']}{key}?v={version}"
    return presigned, cdn_url


def _pick_next_reward(ladder: List[RewardGym], current_xp: int):
    for tier in ladder:
        if tier.xp > current_xp:
            return tier
    return None


def _is_custom_image_url(image_url: Optional[str]) -> bool:
    if not image_url:
        return False
    return image_url.startswith(("http://", "https://"))


def _is_default_image_id(image_url: Optional[str]) -> bool:
    if not image_url:
        return False
    return image_url.startswith("default_")


class RewardCreateBody(BaseModel):
    gym_id: int
    xp: int
    gift: str
    image_url: Optional[str] = None          # default ID or existing URL; None allowed
    extension: Optional[str] = None          # required only for custom uploads
    content_type: Optional[str] = "image/jpeg"  # required only for custom uploads


@router.post("/create_rewards")
async def create_rewards(
    request: RewardCreateBody,
    db: Session = Depends(get_db),
    redis: Redis = Depends(get_redis),
):
    """
    Create a reward tier. If a custom image upload is requested (by providing extension/content_type),
    return a presigned POST and CDN URL for the client to upload, otherwise store image_url directly.
    """
    try:
        gym = db.query(Gym).filter(Gym.gym_id == request.gym_id).first()
        if not gym:
            raise FittbotHTTPException(
                status_code=400,
                detail="There is no gym for this gym_id",
                error_code="GYM_NOT_FOUND",
                log_data={"gym_id": request.gym_id},
            )

        # Determine whether we need to generate an upload (custom image)
        final_image_url = request.image_url
        needs_upload = False
        if request.extension and request.content_type and not _is_custom_image_url(request.image_url):
            final_image_url = ""  # placeholder; will be set after upload confirmation
            needs_upload = True

        reward = RewardGym(
            gym_id=request.gym_id,
            xp=request.xp,
            gift=request.gift,
            image=final_image_url,
        )
        db.add(reward)
        db.commit()
        db.refresh(reward)

        # Recompute next reward snapshot for all active clients
        ladder: List[RewardGym] = (
            db.query(RewardGym)
            .filter(RewardGym.gym_id == request.gym_id)
            .order_by(RewardGym.xp.asc())
            .all()
        )

        active_client_ids = [
            cid
            for (cid,) in db.query(Client.client_id)
            .filter(Client.gym_id == request.gym_id, Client.status == "active")
            .all()
        ]

        # FIX: robust xp_map creation
        xp_map = dict(
            db.query(LeaderboardOverall.client_id, LeaderboardOverall.xp)
            .filter(LeaderboardOverall.client_id.in_(active_client_ids))
            .all()
        )

        for cid in active_client_ids:
            cur_xp = xp_map.get(cid, 0)
            tier = _pick_next_reward(ladder, cur_xp)
            new_next_xp = tier.xp if tier else 0
            new_gift = tier.gift if tier else None

            db.query(ClientNextXp).filter(ClientNextXp.client_id == cid).delete(
                synchronize_session=False
            )
            db.add(ClientNextXp(client_id=cid, next_xp=new_next_xp, gift=new_gift))

        db.commit()

        # Invalidate cache (best effort)
        try:
            redis_key = f"gym:{request.gym_id}:gymRewards"
            if await redis.exists(redis_key):
                await redis.delete(redis_key)
        except Exception:
            pass

        # If custom image upload requested, provide presigned payload to client
        if needs_upload:
            presigned, cdn_url = _gen_presigned(
                gym_id=request.gym_id,
                reward_id=reward.id,
                extension=request.extension,  # type: ignore[arg-type]
                content_type=request.content_type or "image/jpeg",
            )
            return {
                "status": 200,
                "message": "Reward created – upload URL generated",
                "data": {"reward_id": reward.id, "presigned": presigned, "cdn_url": cdn_url},
            }

        # Default/static image case
        return {
            "status": 200,
            "message": "Reward created successfully",
            "data": {"reward_id": reward.id},
        }

    except FittbotHTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise FittbotHTTPException(
            status_code=500,
            detail="An unexpected error occurred while creating reward",
            error_code="REWARD_CREATE_ERROR",
            log_data={"error": repr(e), "gym_id": request.gym_id},
        ) from e


class RewardConfirmBody(BaseModel):
    reward_id: int
    cdn_url: str


@router.post("/confirm_reward_image")
async def confirm_reward_image(
    body: RewardConfirmBody,
    db: Session = Depends(get_db),
    redis: Redis = Depends(get_redis),
):
    """
    Confirm upload and persist the CDN URL for a reward image.
    """
    try:
        reward = db.query(RewardGym).filter(RewardGym.id == body.reward_id).first()
        if not reward:
            raise FittbotHTTPException(
                status_code=404,
                detail="Reward not found",
                error_code="REWARD_NOT_FOUND",
                log_data={"reward_id": body.reward_id},
            )

        reward.image = body.cdn_url
        db.commit()
        db.refresh(reward)

        try:
            await redis.delete(f"gym:{reward.gym_id}:gymRewards")
        except Exception:
            pass

        return {"status": 200, "message": "Reward image saved", "data": reward.image}

    except FittbotHTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise FittbotHTTPException(
            status_code=500,
            detail="An unexpected error occurred while confirming reward image",
            error_code="REWARD_IMAGE_CONFIRM_ERROR",
            log_data={"error": repr(e), "reward_id": body.reward_id},
        ) from e


class UpdateRewardData(BaseModel):
    gym_id: int
    record_id: int
    updated_reward: dict


@router.put("/update_rewards")
async def update_rewards(
    request: UpdateRewardData,
    db: Session = Depends(get_db),
    redis: Redis = Depends(get_redis),
):
    """
    Update a reward (xp/gift and/or image). If a new custom image is requested via
    extension+content_type, return a presigned POST for upload and clear image URL
    until confirmation.
    """
    redis_key = f"gym:{request.gym_id}:gymRewards"

    try:
        gym = db.query(Gym).filter(Gym.gym_id == request.gym_id).first()
        if not gym:
            raise FittbotHTTPException(
                status_code=404,
                detail="No gym found for this gym_id",
                error_code="GYM_NOT_FOUND",
                log_data={"gym_id": request.gym_id},
            )

        reward = db.query(RewardGym).filter(RewardGym.id == request.record_id).first()
        if not reward:
            raise FittbotHTTPException(
                status_code=404,
                detail="No reward found for this record_id",
                error_code="REWARD_NOT_FOUND",
                log_data={"reward_id": request.record_id},
            )

        payload = request.updated_reward or {}

        # Step 1: simple field updates
        if "xp" in payload:
            reward.xp = payload["xp"]
        if "gift" in payload:
            reward.gift = payload["gift"]

        # Step 2: image logic
        presigned_url: Optional[dict] = None
        cdn_url: Optional[str] = None

        if "image_url" in payload:
            new_image_url = payload["image_url"]
            # default ID or null -> store directly
            if new_image_url is None or _is_default_image_id(new_image_url):
                reward.image = new_image_url
            # already a custom URL -> accept as-is
            elif _is_custom_image_url(new_image_url):
                reward.image = new_image_url

        elif "extension" in payload and "content_type" in payload:
            presigned_url, cdn_url = _gen_presigned(
                gym_id=request.gym_id,
                reward_id=request.record_id,
                extension=payload["extension"],
                content_type=payload["content_type"],
            )
            # clear image until client confirms via /confirm_reward_image
            reward.image = ""

        db.commit()

        # Step 3: recompute next reward snapshot for all active clients
        ladder: List[RewardGym] = (
            db.query(RewardGym)
            .filter(RewardGym.gym_id == request.gym_id)
            .order_by(RewardGym.xp.asc())
            .all()
        )
        active_client_ids = [
            cid
            for (cid,) in db.query(Client.client_id)
            .filter(Client.gym_id == request.gym_id, Client.status == "active")
            .all()
        ]
        # FIX: robust xp_map creation
        xp_map = dict(
            db.query(LeaderboardOverall.client_id, LeaderboardOverall.xp)
            .filter(LeaderboardOverall.client_id.in_(active_client_ids))
            .all()
        )

        for cid in active_client_ids:
            cur_xp = xp_map.get(cid, 0)
            tier = _pick_next_reward(ladder, cur_xp)
            next_xp = tier.xp if tier else 0
            next_gift = tier.gift if tier else None

            db.query(ClientNextXp).filter(ClientNextXp.client_id == cid).delete(
                synchronize_session=False
            )
            db.add(ClientNextXp(client_id=cid, next_xp=next_xp, gift=next_gift))
        db.commit()

        # Step 4: invalidate cache (best effort)
        try:
            if await redis.exists(redis_key):
                await redis.delete(redis_key)
        except Exception:
            pass

        # Step 5: response
        response: dict = {"status": 200, "message": "Reward updated successfully"}
        if presigned_url and cdn_url:
            response["data"] = {
                "reward_id": request.record_id,
                "presigned": presigned_url,
                "cdn_url": cdn_url,
            }
        return response

    except FittbotHTTPException:
        raise
    except Exception as exc:
        db.rollback()
        raise FittbotHTTPException(
            status_code=500,
            detail="An unexpected error occurred while updating reward",
            error_code="REWARD_UPDATE_ERROR",
            log_data={
                "error": repr(exc),
                "gym_id": request.gym_id,
                "reward_id": request.record_id,
            },
        ) from exc


@router.delete("/delete_rewards")
async def delete_rewards(
    reward_id: int,
    gym_id: int,
    db: Session = Depends(get_db),
    redis: Redis = Depends(get_redis),
):
    """
    Delete a reward and recompute all clients' next reward snapshots.
    """
    try:
        reward = db.query(RewardGym).filter(RewardGym.id == reward_id).first()
        if not reward:
            raise FittbotHTTPException(
                status_code=400,
                detail="Reward not found",
                error_code="REWARD_NOT_FOUND",
                log_data={"reward_id": reward_id},
            )

        db.delete(reward)
        db.commit()

        ladder: List[RewardGym] = (
            db.query(RewardGym)
            .filter(RewardGym.gym_id == gym_id)
            .order_by(RewardGym.xp.asc())
            .all()
        )
        active_client_ids = [
            cid
            for (cid,) in db.query(Client.client_id)
            .filter(Client.gym_id == gym_id, Client.status == "active")
            .all()
        ]
        # FIX: robust xp_map creation
        xp_map = dict(
            db.query(LeaderboardOverall.client_id, LeaderboardOverall.xp)
            .filter(LeaderboardOverall.client_id.in_(active_client_ids))
            .all()
        )

        for cid in active_client_ids:
            cur_xp = xp_map.get(cid, 0)
            tier = _pick_next_reward(ladder, cur_xp)
            new_next_xp = tier.xp if tier else 0
            new_gift = tier.gift if tier else None

            db.query(ClientNextXp).filter(ClientNextXp.client_id == cid).delete(
                synchronize_session=False
            )
            db.add(ClientNextXp(client_id=cid, next_xp=new_next_xp, gift=new_gift))
        db.commit()

        # Invalidate cache (best effort)
        try:
            redis_key = f"gym:{gym_id}:gymRewards"
            if await redis.exists(redis_key):
                await redis.delete(redis_key)
        except Exception:
            pass

        return {"status": 200, "message": "Reward Deleted successfully."}

    except FittbotHTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise FittbotHTTPException(
            status_code=500,
            detail="An unexpected error occurred while deleting reward",
            error_code="REWARD_DELETE_ERROR",
            log_data={"error": repr(e), "reward_id": reward_id, "gym_id": gym_id},
        ) from e


@router.get("/get_rewards")
async def get_rewards(
    gym_id: int,
    db: Session = Depends(get_db),
    redis: Redis = Depends(get_redis),
):
    """
    Get all rewards for a gym. Cached in Redis for 24 hours.
    """
    redis_key = f"gym:{gym_id}:gymRewards"

    try:
        cached_data = await redis.get(redis_key)
        if cached_data:
            return {
                "status": 200,
                "message": "Rewards data retrieved successfully from cache.",
                "data": json.loads(cached_data),
            }

        rewards_q = (
            db.query(RewardGym)
            .filter(RewardGym.gym_id == gym_id)
            .order_by(RewardGym.xp.asc())
            .all()
        )

        reward_list = [
            {
                "id": r.id,
                "gym_id": r.gym_id,
                "xp": r.xp,
                "gift": r.gift,
                "image_url": r.image,
            }
            for r in rewards_q
        ]

        await redis.set(redis_key, json.dumps(reward_list), ex=86400)

        return {
            "status": 200,
            "message": "Rewards data fetched successfully",
            "data": reward_list,
        }

    except FittbotHTTPException:
        raise
    except Exception as e:
        raise FittbotHTTPException(
            status_code=500,
            detail="An unexpected error occurred while fetching rewards data",
            error_code="REWARD_LIST_ERROR",
            log_data={"error": repr(e), "gym_id": gym_id},
        ) from e
